//! ProcessManager —— 单所有者 Actor 模型。
//!
//! 状态由单个 task 独占；IPC handler / supervisor / 定时器通过 mpsc 发命令，
//! oneshot 回结果，避免「持锁跨 await」。

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot, Notify};

use crate::common::errors::{OwlError, Result};
use crate::common::paths;
use crate::common::state::StateFile;
use crate::common::sysprobe;
use crate::ipc::message::StartOptions;
use crate::log::process_log;
use crate::process::entry::{
    HealthCheckConfig, HealthState, PersistedApp, ProcessInfo, ProcessStatus, RestartStrategy,
};
use crate::process::health;
use crate::process::monitor::Monitor;

const KILL_TIMEOUT: Duration = Duration::from_secs(5);
const BASE_DELAY: Duration = Duration::from_secs(1);
const MAX_DELAY: Duration = Duration::from_secs(16);
const MIN_UPTIME: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_secs(2);
/// 后台监控 tick 间隔（懒监控，见 9.3）。
const MONITOR_INTERVAL: Duration = Duration::from_secs(5);
/// 单个应用组允许的最大实例数，避免错误配置或暴露的 API 耗尽本机资源。
const MAX_INSTANCES: u32 = 1024;

/// 子进程环境白名单（env_clear 后按需注入，保证可复现）。
const ENV_WHITELIST: &[&str] = &[
    "PATH", "HOME", "LANG", "LC_ALL", "LC_CTYPE", "TERM", "USER", "SHELL", "TZ",
];

#[derive(Debug, Clone, Copy)]
pub enum ExitStatusDetail {
    Code(i32),
    Signal(i32),
    Unknown,
}

impl std::fmt::Display for ExitStatusDetail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExitStatusDetail::Code(c) => write!(f, "exit code {}", c),
            ExitStatusDetail::Signal(s) => write!(f, "signal {}", s),
            ExitStatusDetail::Unknown => write!(f, "unknown"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Intent {
    None,
    Stop,
    Restart,
    Delete,
}

/// 单个受管进程的运行时状态。
struct App {
    cfg: PersistedApp,
    status: ProcessStatus,
    pid: Option<u32>,
    start_time: Option<u64>,
    start_instant: Option<Instant>,
    restarts: u32,
    consecutive_crashes: u32,
    intent: Intent,
    reattached: bool,
    cpu_percent: f32,
    memory_bytes: u64,
    health: HealthState,
    health_failures: u32,
    health_cancel: Option<Arc<Notify>>,
    /// 每次 spawn 递增，用于忽略重启前旧健康任务的过期结果。
    generation: u64,
}

impl App {
    fn from_cfg(cfg: PersistedApp) -> Self {
        App {
            cfg,
            status: ProcessStatus::Stopped,
            pid: None,
            start_time: None,
            start_instant: None,
            restarts: 0,
            consecutive_crashes: 0,
            intent: Intent::None,
            reattached: false,
            cpu_percent: 0.0,
            memory_bytes: 0,
            health: HealthState::Unknown,
            health_failures: 0,
            health_cancel: None,
            generation: 0,
        }
    }

    /// 取消健康检查任务并清状态。
    fn cancel_health(&mut self) {
        if let Some(n) = self.health_cancel.take() {
            n.notify_waiters();
        }
        self.health = HealthState::Unknown;
        self.health_failures = 0;
    }

    fn to_info(&self) -> ProcessInfo {
        let uptime_secs = match (self.status, self.start_instant) {
            (ProcessStatus::Online, Some(inst)) => inst.elapsed().as_secs(),
            _ => 0,
        };
        ProcessInfo {
            id: self.cfg.id,
            name: self.cfg.name.clone(),
            instance_index: self.cfg.instance_index,
            command: self.cfg.command.clone(),
            args: self.cfg.args.clone(),
            pid: self.pid,
            status: self.status,
            health: self.health,
            port: self.cfg.port,
            restarts: self.restarts,
            max_restarts: self.cfg.max_restarts,
            uptime_secs,
            cpu_percent: self.cpu_percent,
            memory_bytes: self.memory_bytes,
            max_memory: self.cfg.max_memory,
            created_at: self.cfg.created_at,
            restart_strategy: self.cfg.restart_strategy,
        }
    }
}

/// Actor 命令。
pub enum Cmd {
    Start(Box<StartOptions>, oneshot::Sender<Result<ProcessInfo>>),
    Stop(String, oneshot::Sender<Result<String>>),
    Restart(String, oneshot::Sender<Result<String>>),
    Delete(String, oneshot::Sender<Result<String>>),
    List(oneshot::Sender<Vec<ProcessInfo>>),
    Info(String, oneshot::Sender<Result<ProcessInfo>>),
    HealthConfig(u32, oneshot::Sender<Result<Option<HealthCheckConfig>>>),
    LogPath(String, oneshot::Sender<Result<PathBuf>>),
    Flush(String, oneshot::Sender<Result<String>>),
    Reset(String, oneshot::Sender<Result<String>>),
    Apply {
        apps: Vec<StartOptions>,
        prune: bool,
        dry_run: bool,
        reply: oneshot::Sender<Result<String>>,
    },
    Scale {
        target: String,
        n: u32,
        reply: oneshot::Sender<Result<String>>,
    },
    Save {
        file: Option<String>,
        reply: oneshot::Sender<Result<String>>,
    },
    Resurrect {
        file: Option<String>,
        reply: oneshot::Sender<Result<String>>,
    },
    // 内部事件
    Exited {
        id: u32,
        reason: ExitStatusDetail,
    },
    EscalateKill {
        id: u32,
    },
    RestartNow {
        id: u32,
    },
    HealthResult {
        id: u32,
        generation: u64,
        healthy: bool,
    },
    Tick,
    Shutdown(oneshot::Sender<()>),
}

/// 对外句柄：克隆便宜，线程安全。
#[derive(Clone)]
pub struct ManagerHandle {
    tx: mpsc::UnboundedSender<Cmd>,
}

impl ManagerHandle {
    pub async fn start(&self, opts: StartOptions) -> Result<ProcessInfo> {
        let (tx, rx) = oneshot::channel();
        self.send(Cmd::Start(Box::new(opts), tx))?;
        rx.await.map_err(recv_err)?
    }

    pub async fn stop(&self, target: String) -> Result<String> {
        let (tx, rx) = oneshot::channel();
        self.send(Cmd::Stop(target, tx))?;
        rx.await.map_err(recv_err)?
    }

    pub async fn restart(&self, target: String) -> Result<String> {
        let (tx, rx) = oneshot::channel();
        self.send(Cmd::Restart(target, tx))?;
        rx.await.map_err(recv_err)?
    }

    pub async fn delete(&self, target: String) -> Result<String> {
        let (tx, rx) = oneshot::channel();
        self.send(Cmd::Delete(target, tx))?;
        rx.await.map_err(recv_err)?
    }

    pub async fn list(&self) -> Result<Vec<ProcessInfo>> {
        let (tx, rx) = oneshot::channel();
        self.send(Cmd::List(tx))?;
        rx.await.map_err(recv_err)
    }

    pub async fn info(&self, target: String) -> Result<ProcessInfo> {
        let (tx, rx) = oneshot::channel();
        self.send(Cmd::Info(target, tx))?;
        rx.await.map_err(recv_err)?
    }

    pub async fn health_config(&self, id: u32) -> Result<Option<HealthCheckConfig>> {
        let (tx, rx) = oneshot::channel();
        self.send(Cmd::HealthConfig(id, tx))?;
        rx.await.map_err(recv_err)?
    }

    pub async fn log_path(&self, target: String) -> Result<PathBuf> {
        let (tx, rx) = oneshot::channel();
        self.send(Cmd::LogPath(target, tx))?;
        rx.await.map_err(recv_err)?
    }

    pub async fn flush(&self, target: String) -> Result<String> {
        let (tx, rx) = oneshot::channel();
        self.send(Cmd::Flush(target, tx))?;
        rx.await.map_err(recv_err)?
    }

    pub async fn reset(&self, target: String) -> Result<String> {
        let (tx, rx) = oneshot::channel();
        self.send(Cmd::Reset(target, tx))?;
        rx.await.map_err(recv_err)?
    }

    pub async fn apply(
        &self,
        apps: Vec<StartOptions>,
        prune: bool,
        dry_run: bool,
    ) -> Result<String> {
        let (tx, rx) = oneshot::channel();
        self.send(Cmd::Apply {
            apps,
            prune,
            dry_run,
            reply: tx,
        })?;
        rx.await.map_err(recv_err)?
    }

    pub async fn scale(&self, target: String, n: u32) -> Result<String> {
        let (tx, rx) = oneshot::channel();
        self.send(Cmd::Scale {
            target,
            n,
            reply: tx,
        })?;
        rx.await.map_err(recv_err)?
    }

    pub async fn save(&self, file: Option<String>) -> Result<String> {
        let (tx, rx) = oneshot::channel();
        self.send(Cmd::Save { file, reply: tx })?;
        rx.await.map_err(recv_err)?
    }

    pub async fn resurrect(&self, file: Option<String>) -> Result<String> {
        let (tx, rx) = oneshot::channel();
        self.send(Cmd::Resurrect { file, reply: tx })?;
        rx.await.map_err(recv_err)?
    }

    /// 返回某 target（id 或 name）的所有实例 (id, port)，按 instance_index 排序。
    pub async fn instance_ids(&self, target: String) -> Result<Vec<(u32, Option<u16>)>> {
        let infos = self.list().await?;
        let mut group: Vec<(u32, u32, Option<u16>)> = infos
            .into_iter()
            .filter(|i| i.name == target || i.id.to_string() == target)
            .map(|i| (i.instance_index, i.id, i.port))
            .collect();
        group.sort_by_key(|(idx, _, _)| *idx);
        Ok(group.into_iter().map(|(_, id, port)| (id, port)).collect())
    }

    pub async fn shutdown(&self) {
        let (tx, rx) = oneshot::channel();
        if self.send(Cmd::Shutdown(tx)).is_ok() {
            let _ = rx.await;
        }
    }

    fn send(&self, cmd: Cmd) -> Result<()> {
        self.tx
            .send(cmd)
            .map_err(|_| OwlError::Other("ProcessManager 已停止".into()))
    }
}

fn recv_err(_: oneshot::error::RecvError) -> OwlError {
    OwlError::Other("ProcessManager 未响应".into())
}

/// 启动 ProcessManager Actor，恢复持久化状态，返回句柄。
pub fn start_manager() -> Result<ManagerHandle> {
    let (tx, rx) = mpsc::unbounded_channel();
    let mut mgr = Manager::new(tx.clone(), rx);
    mgr.recover()?;
    // 后台监控 ticker：统一一个定时器，懒触发（见 9.3）。
    let tick_tx = tx.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(MONITOR_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if tick_tx.send(Cmd::Tick).is_err() {
                break;
            }
        }
    });
    tokio::spawn(mgr.run());
    Ok(ManagerHandle { tx })
}

struct Manager {
    apps: HashMap<u32, App>,
    next_id: u32,
    tx: mpsc::UnboundedSender<Cmd>,
    rx: mpsc::UnboundedReceiver<Cmd>,
    monitor: Monitor,
}

impl Manager {
    fn new(tx: mpsc::UnboundedSender<Cmd>, rx: mpsc::UnboundedReceiver<Cmd>) -> Self {
        Manager {
            apps: HashMap::new(),
            next_id: 0,
            tx,
            rx,
            monitor: Monitor::new(),
        }
    }

    /// 从 state.json 恢复：重接管仍存活的进程（方案 B），其余标记 Stopped。
    fn recover(&mut self) -> Result<()> {
        let state = StateFile::load()?;
        self.next_id = state.next_id;
        for cfg in state.apps {
            validate_app_name(&cfg.name)?;
            let id = cfg.id;
            let mut app = App::from_cfg(cfg);
            if let Some(pid) = app.cfg.last_pid {
                if sysprobe::validate(pid, app.cfg.last_pid_start_time) {
                    app.pid = Some(pid);
                    app.start_time = app.cfg.last_pid_start_time;
                    app.start_instant = Some(Instant::now());
                    app.status = ProcessStatus::Online;
                    app.reattached = true;
                    spawn_poll_watcher(&self.tx, id, pid, app.start_time);
                    owl_logger::info!("重接管进程 [{id}] {} (pid={pid})", app.cfg.name);
                }
            }
            self.apps.insert(id, app);
        }
        if let Some(maxid) = self.apps.keys().copied().max() {
            if self.next_id <= maxid {
                self.next_id = maxid + 1;
            }
        }
        Ok(())
    }

    async fn run(mut self) {
        while let Some(cmd) = self.rx.recv().await {
            match cmd {
                Cmd::Start(opts, reply) => {
                    let _ = reply.send(self.handle_start(*opts));
                }
                Cmd::Stop(target, reply) => {
                    let _ = reply.send(self.handle_batch(&target, "stop", |m, id| m.stop_one(id)));
                }
                Cmd::Restart(target, reply) => {
                    let _ = reply
                        .send(self.handle_batch(&target, "restart", |m, id| m.restart_one(id)));
                }
                Cmd::Delete(target, reply) => {
                    let _ =
                        reply.send(self.handle_batch(&target, "delete", |m, id| m.delete_one(id)));
                }
                Cmd::List(reply) => {
                    // 读取后台 tick 采样值（规整间隔保证 CPU% 准确）。
                    let mut list: Vec<ProcessInfo> =
                        self.apps.values().map(|a| a.to_info()).collect();
                    list.sort_by_key(|i| i.id);
                    let _ = reply.send(list);
                }
                Cmd::Info(target, reply) => {
                    let _ = reply.send(self.handle_info(&target));
                }
                Cmd::HealthConfig(id, reply) => {
                    let result = self
                        .apps
                        .get(&id)
                        .map(|app| app.cfg.health_check.clone())
                        .ok_or_else(not_found);
                    let _ = reply.send(result);
                }
                Cmd::LogPath(target, reply) => {
                    let _ = reply.send(self.handle_log_path(&target));
                }
                Cmd::Flush(target, reply) => {
                    let _ =
                        reply.send(self.handle_batch(&target, "flush", |m, id| m.flush_one(id)));
                }
                Cmd::Reset(target, reply) => {
                    let _ =
                        reply.send(self.handle_batch(&target, "reset", |m, id| m.reset_one(id)));
                }
                Cmd::Apply {
                    apps,
                    prune,
                    dry_run,
                    reply,
                } => {
                    let _ = reply.send(self.handle_apply(apps, prune, dry_run));
                }
                Cmd::Scale { target, n, reply } => {
                    let _ = reply.send(self.handle_scale(&target, n));
                }
                Cmd::Save { file, reply } => {
                    let _ = reply.send(self.handle_save(file));
                }
                Cmd::Resurrect { file, reply } => {
                    let _ = reply.send(self.handle_resurrect(file));
                }
                Cmd::Tick => self.handle_tick(),
                Cmd::HealthResult {
                    id,
                    generation,
                    healthy,
                } => self.handle_health(id, generation, healthy),
                Cmd::Exited { id, reason } => self.handle_exited(id, reason),
                Cmd::EscalateKill { id } => self.handle_escalate(id),
                Cmd::RestartNow { id } => {
                    let _ = self.start_existing(id);
                    self.persist();
                }
                Cmd::Shutdown(reply) => {
                    owl_logger::info!("ProcessManager 收到关闭信号，子进程脱离存活");
                    self.persist();
                    let _ = reply.send(());
                    break;
                }
            }
        }
    }

    fn handle_start(&mut self, opts: StartOptions) -> Result<ProcessInfo> {
        let name = opts
            .name
            .clone()
            .unwrap_or_else(|| derive_name(&opts.command));
        if self.apps.values().any(|a| a.cfg.name == name) {
            return Err(OwlError::AlreadyExists(name));
        }
        let count = opts.instances.max(1);
        let ports = parse_port_range(opts.port.as_deref())?;
        validate_start_spec(&name, &opts, ports)?;
        for index in 0..count {
            if let Some(port) = instance_port(ports, index)? {
                if let Some(holder) = self.port_in_use(port, None) {
                    return Err(OwlError::Invalid(format!(
                        "端口 {port} 已被进程 [{holder}] 占用"
                    )));
                }
            }
        }
        let now = chrono::Utc::now().timestamp();
        let tx = self.tx.clone();
        let start_id = self.next_id;
        let mut started = Vec::with_capacity(count as usize);

        for i in 0..count {
            let id = start_id
                .checked_add(i)
                .ok_or_else(|| OwlError::Invalid("进程 ID 已耗尽".into()))?;
            let cfg = build_cfg(id, now, name.clone(), &opts, i, ports)?;
            let mut app = App::from_cfg(cfg);
            if let Err(e) = spawn_proc(&tx, &mut app) {
                rollback_spawned(&mut started);
                return Err(e);
            }
            started.push(app);
        }
        let first_info = started
            .first()
            .map(App::to_info)
            .ok_or_else(|| OwlError::Other("启动了 0 个实例".into()))?;
        self.next_id = start_id
            .checked_add(count)
            .ok_or_else(|| OwlError::Invalid("进程 ID 已耗尽".into()))?;
        for app in started {
            self.apps.insert(app.cfg.id, app);
        }
        self.persist();
        let _ = self.tx.send(Cmd::Tick);
        Ok(first_info)
    }

    /// 某端口是否已被受管进程占用（排除 `exclude` 这个 id）。返回占用者 id。
    fn port_in_use(&self, port: u16, exclude: Option<u32>) -> Option<u32> {
        self.apps
            .values()
            .find(|a| a.cfg.port == Some(port) && Some(a.cfg.id) != exclude)
            .map(|a| a.cfg.id)
    }

    /// 调整进程组实例数（见 7.9）。
    fn handle_scale(&mut self, target: &str, n: u32) -> Result<String> {
        let group_name = target
            .parse::<u32>()
            .ok()
            .and_then(|id| self.apps.get(&id).map(|app| app.cfg.name.clone()))
            .unwrap_or_else(|| target.to_string());
        // 取该组所有实例（按 instance_index 排序）。
        let mut group: Vec<(u32, u32)> = self
            .apps
            .values()
            .filter(|a| a.cfg.name == group_name)
            .map(|a| (a.cfg.instance_index, a.cfg.id))
            .collect();
        if group.is_empty() {
            return Err(OwlError::NotFound(target.to_string()));
        }
        group.sort_unstable();
        let current = group.len() as u32;
        if n == 0 || n > MAX_INSTANCES {
            return Err(OwlError::Invalid(format!(
                "实例数需介于 1 和 {MAX_INSTANCES}（删除请用 delete）"
            )));
        }
        if n == current {
            return Ok(format!("scale: {target} 已是 {n} 实例，无变化"));
        }

        if n > current {
            let template = self.apps[&group[0].1].cfg.clone();
            let ports = match (template.port_base, template.port_max) {
                (Some(base), Some(max)) => Some(PortRange { base, max }),
                (Some(base), None) => Some(PortRange {
                    base,
                    max: u16::MAX,
                }),
                _ => None,
            };
            for i in current..n {
                if let Some(port) = instance_port(ports, i)? {
                    if let Some(holder) = self.port_in_use(port, None) {
                        return Err(OwlError::Invalid(format!(
                            "端口 {port} 已被进程 [{holder}] 占用"
                        )));
                    }
                }
            }
            let now = chrono::Utc::now().timestamp();
            let tx = self.tx.clone();
            let start_id = self.next_id;
            let mut started = Vec::with_capacity((n - current) as usize);
            for i in current..n {
                let offset = i - current;
                let id = start_id
                    .checked_add(offset)
                    .ok_or_else(|| OwlError::Invalid("进程 ID 已耗尽".into()))?;
                let mut cfg = template.clone();
                cfg.id = id;
                cfg.instance_index = i;
                cfg.created_at = now;
                cfg.last_pid = None;
                cfg.last_pid_start_time = None;
                cfg.port = instance_port(ports, i)?;
                cfg.port_base = ports.map(|p| p.base);
                cfg.port_max = ports.map(|p| p.max);
                let mut app = App::from_cfg(cfg);
                if let Err(e) = spawn_proc(&tx, &mut app) {
                    rollback_spawned(&mut started);
                    return Err(e);
                }
                started.push(app);
            }
            self.next_id = start_id
                .checked_add(n - current)
                .ok_or_else(|| OwlError::Invalid("进程 ID 已耗尽".into()))?;
            for app in started {
                self.apps.insert(app.cfg.id, app);
            }
            self.persist();
            let _ = self.tx.send(Cmd::Tick);
            Ok(format!(
                "scale: {target} {current} -> {n}（+{}）",
                n - current
            ))
        } else {
            // 缩容：移除最高 index 的实例。
            let remove: Vec<u32> = group
                .iter()
                .rev()
                .take((current - n) as usize)
                .map(|(_, id)| *id)
                .collect();
            for id in &remove {
                let _ = self.delete_one(*id);
            }
            self.persist();
            Ok(format!(
                "scale: {target} {current} -> {n}（-{}）",
                current - n
            ))
        }
    }

    /// 声明式收敛（见 7.13）。返回人类可读的动作报告。
    fn handle_apply(
        &mut self,
        specs: Vec<StartOptions>,
        prune: bool,
        dry_run: bool,
    ) -> Result<String> {
        use std::collections::HashSet;
        let mut report: Vec<String> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();

        // 先完成所有纯校验，避免读到第二个 app 才发现重复名称而留下部分变更。
        let mut prepared = Vec::with_capacity(specs.len());
        for mut opts in specs {
            let name = opts
                .name
                .clone()
                .unwrap_or_else(|| derive_name(&opts.command));
            let ports = parse_port_range(opts.port.as_deref())?;
            validate_start_spec(&name, &opts, ports)?;
            if !seen.insert(name.clone()) {
                return Err(OwlError::Invalid(format!("配置中存在重复应用名: {name}")));
            }
            opts.name = Some(name.clone());
            prepared.push((name, opts, ports));
        }

        for (name, opts, ports) in prepared {
            let mut group: Vec<(u32, u32)> = self
                .apps
                .values()
                .filter(|a| a.cfg.name == name)
                .map(|a| (a.cfg.instance_index, a.cfg.id))
                .collect();
            group.sort_unstable();

            if group.is_empty() {
                report.push(format!("+ start   {name} ({} 实例)", opts.instances.max(1)));
                if !dry_run {
                    self.handle_start(opts)?;
                }
                continue;
            }

            let current = group.len() as u32;
            let desired = opts.instances.max(1);
            let group_ids: HashSet<u32> = group.iter().map(|(_, id)| *id).collect();
            for index in 0..desired {
                if let Some(port) = instance_port(ports, index)? {
                    if let Some(holder) = self.port_in_use(port, None) {
                        if !group_ids.contains(&holder) {
                            return Err(OwlError::Invalid(format!(
                                "应用 {name} 所需端口 {port} 已被进程 [{holder}] 占用"
                            )));
                        }
                    }
                }
            }

            let survivor_count = current.min(desired) as usize;
            let mut planned = Vec::with_capacity(survivor_count);
            for (_, id) in group.iter().take(survivor_count) {
                let old = &self.apps[id].cfg;
                planned.push((
                    *id,
                    build_cfg(
                        *id,
                        old.created_at,
                        name.clone(),
                        &opts,
                        old.instance_index,
                        ports,
                    )?,
                ));
            }
            let key_changed = planned
                .iter()
                .any(|(id, cfg)| key_fields_differ(&self.apps[id].cfg, cfg));
            let non_key_changed = planned
                .iter()
                .any(|(id, cfg)| non_key_differ(&self.apps[id].cfg, cfg));

            if key_changed {
                report.push(format!("~ restart {name} (全部现存实例的关键字段变更)"));
            } else if non_key_changed {
                report.push(format!("= update  {name} (全部现存实例原地更新)"));
            } else {
                report.push(format!("  ok      {name} (配置字段无变化)"));
            }
            if current != desired {
                report.push(format!("~ scale   {name} {current} -> {desired}"));
            }

            if !dry_run {
                for (id, mut cfg) in planned {
                    let app = self.apps.get_mut(&id).ok_or_else(not_found)?;
                    cfg.last_pid = app.cfg.last_pid;
                    cfg.last_pid_start_time = app.cfg.last_pid_start_time;
                    app.cfg = cfg;
                }
                if key_changed {
                    for (_, id) in group.iter().take(survivor_count) {
                        self.restart_one(*id)?;
                    }
                }
                if current != desired {
                    self.handle_scale(&name, desired)?;
                }
            }
        }

        let unlisted: Vec<(u32, String)> = self
            .apps
            .values()
            .filter(|a| !seen.contains(&a.cfg.name))
            .map(|a| (a.cfg.id, a.cfg.name.clone()))
            .collect();
        for (id, nm) in unlisted {
            if prune {
                report.push(format!("- prune   {nm}"));
                if !dry_run {
                    let _ = self.delete_one(id);
                }
            } else {
                report.push(format!("  keep    {nm} (未在配置中，保留)"));
            }
        }

        if !dry_run {
            self.persist();
        }
        let header = if dry_run {
            "apply --dry-run 预览："
        } else {
            "apply 完成："
        };
        Ok(format!("{header}\n{}", report.join("\n")))
    }

    /// 保存当前进程拓扑快照（按 name 聚合实例）。
    fn handle_save(&self, file: Option<String>) -> Result<String> {
        let path = snapshot_path(file.as_deref());
        let snap = SnapshotFile {
            schema_version: 1,
            apps: snapshot_specs(&self.apps),
        };
        let bytes = serde_json::to_vec_pretty(&snap)
            .map_err(|e| OwlError::Other(format!("序列化快照失败: {e}")))?;
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        std::fs::write(&path, bytes)
            .map_err(|e| OwlError::Other(format!("写入快照失败({}): {e}", path.display())))?;
        Ok(format!(
            "已保存 {} 个应用到 {}",
            snap.apps.len(),
            path.display()
        ))
    }

    /// 从快照恢复（等价 apply，默认不 prune）。
    fn handle_resurrect(&mut self, file: Option<String>) -> Result<String> {
        let path = snapshot_path(file.as_deref());
        let text = std::fs::read_to_string(&path)
            .map_err(|e| OwlError::Other(format!("读取快照失败({}): {e}", path.display())))?;
        let snap: SnapshotFile = serde_json::from_str(&text)
            .map_err(|e| OwlError::Other(format!("解析快照失败({}): {e}", path.display())))?;
        let mut msg = self.handle_apply(snap.apps, false, false)?;
        msg.push_str(&format!("\n(来源: {})", path.display()));
        Ok(msg)
    }

    fn handle_batch<F>(&mut self, target: &str, verb: &str, mut f: F) -> Result<String>
    where
        F: FnMut(&mut Self, u32) -> Result<()>,
    {
        let ids = self.resolve(target);
        if ids.is_empty() {
            return Err(OwlError::NotFound(target.to_string()));
        }
        let mut ok = 0u32;
        let mut errs = Vec::new();
        for id in ids {
            match f(self, id) {
                Ok(()) => ok += 1,
                Err(e) => errs.push(e.to_string()),
            }
        }
        self.persist();
        if errs.is_empty() {
            Ok(format!("{verb}: {ok} 个进程"))
        } else {
            Ok(format!(
                "{verb}: {ok} 成功, {} 失败: {}",
                errs.len(),
                errs.join("; ")
            ))
        }
    }

    fn stop_one(&mut self, id: u32) -> Result<()> {
        let app = self.apps.get_mut(&id).ok_or_else(not_found)?;
        match app.pid {
            None => {
                owl_logger::info!("正在停止进程 [{}] {} (未运行)", id, app.cfg.name);
                app.status = ProcessStatus::Stopped;
                Ok(())
            }
            Some(pid) => {
                owl_logger::info!("正在停止进程 [{}] {} (pid={})", id, app.cfg.name, pid);
                app.cancel_health();
                app.intent = Intent::Stop;
                app.status = ProcessStatus::Stopping;
                let sig = sysprobe::parse_signal(app.cfg.kill_signal.as_deref());
                sysprobe::send_signal_validated(pid, app.start_time, sig);
                schedule(&self.tx, KILL_TIMEOUT, Cmd::EscalateKill { id });
                Ok(())
            }
        }
    }

    fn restart_one(&mut self, id: u32) -> Result<()> {
        let app = self.apps.get_mut(&id).ok_or_else(not_found)?;
        match app.pid {
            Some(pid) => {
                owl_logger::info!("正在重启进程 [{}] {} (pid={})", id, app.cfg.name, pid);
                app.cancel_health();
                app.intent = Intent::Restart;
                app.status = ProcessStatus::Stopping;
                let sig = sysprobe::parse_signal(app.cfg.kill_signal.as_deref());
                sysprobe::send_signal_validated(pid, app.start_time, sig);
                schedule(&self.tx, KILL_TIMEOUT, Cmd::EscalateKill { id });
                Ok(())
            }
            None => {
                owl_logger::info!("正在启动进程 [{}] {} (当前未运行)", id, app.cfg.name);
                self.start_existing(id)
            }
        }
    }

    fn delete_one(&mut self, id: u32) -> Result<()> {
        let app = self.apps.get_mut(&id).ok_or_else(not_found)?;
        match app.pid {
            Some(pid) => {
                owl_logger::info!("正在删除进程 [{}] {} (pid={})", id, app.cfg.name, pid);
                app.cancel_health();
                app.intent = Intent::Delete;
                app.status = ProcessStatus::Stopping;
                let sig = sysprobe::parse_signal(app.cfg.kill_signal.as_deref());
                sysprobe::send_signal_validated(pid, app.start_time, sig);
                schedule(&self.tx, KILL_TIMEOUT, Cmd::EscalateKill { id });
                Ok(())
            }
            None => {
                owl_logger::info!("正在删除进程 [{}] {} (未运行)", id, app.cfg.name);
                self.apps.remove(&id);
                Ok(())
            }
        }
    }

    fn handle_health(&mut self, id: u32, generation: u64, healthy: bool) {
        let restart = {
            let app = match self.apps.get_mut(&id) {
                Some(a) => a,
                None => return,
            };
            if app.generation != generation || app.status != ProcessStatus::Online {
                return; // 过期结果或进程已非在线
            }
            if healthy {
                app.health = HealthState::Healthy;
                app.health_failures = 0;
                false
            } else {
                app.health = HealthState::Unhealthy;
                app.health_failures += 1;
                let max = app
                    .cfg
                    .health_check
                    .as_ref()
                    .map(|h| h.max_failures)
                    .unwrap_or(3);
                if app.health_failures >= max {
                    app.health_failures = 0;
                    true
                } else {
                    false
                }
            }
        };
        if restart {
            owl_logger::warn!("进程 [{id}] 健康检查连续失败超阈值，触发重启");
            let _ = self.restart_one(id);
        }
    }

    fn flush_one(&mut self, id: u32) -> Result<()> {
        let app = self.apps.get(&id).ok_or_else(not_found)?;
        let path = paths::proc_log(&app.cfg.name, app.cfg.id);
        let _ = std::fs::write(&path, b"");
        Ok(())
    }

    fn reset_one(&mut self, id: u32) -> Result<()> {
        let app = self.apps.get_mut(&id).ok_or_else(not_found)?;
        app.restarts = 0;
        app.consecutive_crashes = 0;
        if app.status == ProcessStatus::Errored {
            app.status = ProcessStatus::Stopped;
        }
        Ok(())
    }

    fn handle_info(&self, target: &str) -> Result<ProcessInfo> {
        let ids = self.resolve(target);
        match ids.first() {
            Some(id) => Ok(self.apps[id].to_info()),
            None => Err(OwlError::NotFound(target.to_string())),
        }
    }

    fn handle_log_path(&self, target: &str) -> Result<PathBuf> {
        let ids = self.resolve(target);
        match ids.first() {
            Some(id) => {
                let app = &self.apps[id];
                Ok(paths::proc_log(&app.cfg.name, app.cfg.id))
            }
            None => Err(OwlError::NotFound(target.to_string())),
        }
    }

    /// 重新拉起一个已存在的 app（重启路径），递增 restarts 计数。
    fn start_existing(&mut self, id: u32) -> Result<()> {
        let tx = self.tx.clone();
        let app = self.apps.get_mut(&id).ok_or_else(not_found)?;
        app.restarts += 1;
        spawn_proc(&tx, app)
    }

    fn handle_exited(&mut self, id: u32, reason: ExitStatusDetail) {
        enum Next {
            Remove,
            Settled,
            RestartNow,
            Schedule(Duration),
        }

        let success = matches!(reason, ExitStatusDetail::Code(0));
        let old_pid = self.apps.get(&id).and_then(|a| a.pid);

        let next = {
            let app = match self.apps.get_mut(&id) {
                Some(a) => a,
                None => return,
            };
            // 长稳运行则清零连续崩溃计数。
            if let Some(inst) = app.start_instant {
                if inst.elapsed() >= MIN_UPTIME {
                    app.consecutive_crashes = 0;
                }
            }
            app.pid = None;
            app.start_instant = None;
            app.reattached = false;
            app.cpu_percent = 0.0;
            app.memory_bytes = 0;
            app.cancel_health();

            match app.intent {
                Intent::Delete => {
                    owl_logger::info!("进程 [{}] {} 已停止并删除 ({})", id, app.cfg.name, reason);
                    Next::Remove
                }
                Intent::Stop => {
                    owl_logger::info!("进程 [{}] {} 已停止 ({})", id, app.cfg.name, reason);
                    app.status = ProcessStatus::Stopped;
                    app.intent = Intent::None;
                    Next::Settled
                }
                Intent::Restart => {
                    owl_logger::info!(
                        "进程 [{}] {} 已退出 ({})，正在重新拉起...",
                        id,
                        app.cfg.name,
                        reason
                    );
                    app.intent = Intent::None;
                    Next::RestartNow
                }
                Intent::None => {
                    let should = match app.cfg.restart_strategy {
                        RestartStrategy::Never => false,
                        RestartStrategy::OnFailure => !success,
                        RestartStrategy::Always => true,
                    };
                    if !should {
                        owl_logger::warn!(
                            "进程 [{}] {} (pid={:?}) 异常退出 ({})，无重启策略，状态标记为 Stopped",
                            id,
                            app.cfg.name,
                            old_pid,
                            reason
                        );
                        app.status = ProcessStatus::Stopped;
                        Next::Settled
                    } else {
                        app.consecutive_crashes += 1;
                        let delay = compute_delay(app);
                        if let Some(max) = app.cfg.max_restarts {
                            if app.consecutive_crashes > max {
                                app.status = ProcessStatus::Errored;
                                owl_logger::error!(
                                    "进程 [{}] {} (pid={:?}) 连续崩溃次数 ({}) 超过 max_restarts({})，标记为 Errored",
                                    id,
                                    app.cfg.name,
                                    old_pid,
                                    app.consecutive_crashes,
                                    max
                                );
                                Next::Settled
                            } else {
                                owl_logger::warn!(
                                    "进程 [{}] {} (pid={:?}) 异常退出 ({})，将于 {:?} 后自动重启 ({}/{})",
                                    id,
                                    app.cfg.name,
                                    old_pid,
                                    reason,
                                    delay,
                                    app.consecutive_crashes,
                                    max
                                );
                                app.status = ProcessStatus::Launching;
                                Next::Schedule(delay)
                            }
                        } else {
                            owl_logger::warn!(
                                "进程 [{}] {} (pid={:?}) 异常退出 ({})，将于 {:?} 后自动重启 (连续崩溃第 {} 次)",
                                id,
                                app.cfg.name,
                                old_pid,
                                reason,
                                delay,
                                app.consecutive_crashes
                            );
                            app.status = ProcessStatus::Launching;
                            Next::Schedule(delay)
                        }
                    }
                }
            }
        };

        match next {
            Next::Remove => {
                self.apps.remove(&id);
            }
            Next::Settled => {}
            Next::RestartNow => {
                let _ = self.start_existing(id);
            }
            Next::Schedule(delay) => {
                schedule(&self.tx, delay, Cmd::RestartNow { id });
            }
        }
        self.persist();
    }

    /// 后台监控 tick：以规整间隔采样**所有在线进程**（保证 CPU% 差值口径一致），
    /// 写回 cpu/内存，并对配了 `max_memory` 的进程做阈值检查（超限重启）。
    /// 无在线进程时不采样 → 空闲近零开销。
    fn handle_tick(&mut self) {
        let pids: Vec<u32> = self
            .apps
            .values()
            .filter(|a| a.status == ProcessStatus::Online)
            .filter_map(|a| a.pid)
            .collect();
        if pids.is_empty() {
            return;
        }
        let samples = self.monitor.sample(&pids);
        let mut oom_ids = Vec::new();
        let mut has_limit = false;
        for app in self.apps.values_mut() {
            if let Some(pid) = app.pid {
                if let Some(s) = samples.get(&pid) {
                    app.cpu_percent = s.cpu_percent;
                    app.memory_bytes = s.memory_bytes;
                    if let Some(limit) = app.cfg.max_memory {
                        has_limit = true;
                        if s.memory_bytes > limit {
                            oom_ids.push((app.cfg.id, app.cfg.name.clone(), s.memory_bytes, limit));
                        }
                    }
                }
            }
        }
        for (id, name, used, limit) in oom_ids {
            owl_logger::warn!("进程 [{id}] {name} 内存超限 ({used} > {limit} bytes)，触发重启");
            let _ = self.restart_one(id);
        }
        if has_limit {
            self.persist();
        }
    }

    fn handle_escalate(&mut self, id: u32) {
        if let Some(app) = self.apps.get(&id) {
            if app.status == ProcessStatus::Stopping {
                if let Some(pid) = app.pid {
                    sysprobe::send_signal_validated(pid, app.start_time, sysprobe::Signal::SIGKILL);
                }
            }
        }
    }

    fn resolve(&self, target: &str) -> Vec<u32> {
        if target == "all" {
            let mut ids: Vec<u32> = self.apps.keys().copied().collect();
            ids.sort_unstable();
            return ids;
        }
        if let Ok(id) = target.parse::<u32>() {
            if self.apps.contains_key(&id) {
                return vec![id];
            }
        }
        self.apps
            .values()
            .filter(|a| a.cfg.name == target)
            .map(|a| a.cfg.id)
            .collect()
    }

    fn persist(&mut self) {
        // 同步运行时已知 pid 到持久化配置。
        for app in self.apps.values_mut() {
            app.cfg.last_pid = app.pid;
            app.cfg.last_pid_start_time = app.start_time;
        }
        let mut apps: Vec<PersistedApp> = self.apps.values().map(|a| a.cfg.clone()).collect();
        apps.sort_by_key(|a| a.id);
        let sf = StateFile {
            schema_version: crate::common::state::SCHEMA_VERSION,
            next_id: self.next_id,
            apps,
        };
        if let Err(e) = sf.save() {
            owl_logger::warn!("持久化 state.json 失败: {e}");
        }
    }
}

fn not_found() -> OwlError {
    OwlError::NotFound("进程".into())
}

/// 计算退避延迟：固定 restart_delay 优先，否则指数退避。
fn compute_delay(app: &App) -> Duration {
    if let Some(ms) = app.cfg.restart_delay_ms {
        return Duration::from_millis(ms);
    }
    let n = app.consecutive_crashes.saturating_sub(1).min(16);
    let mult = 1u64 << n.min(20);
    let delay = BASE_DELAY.saturating_mul(mult as u32);
    delay.min(MAX_DELAY)
}

/// 由命令路径派生默认名称（basename 去扩展名）。
fn derive_name(command: &str) -> String {
    std::path::Path::new(command)
        .file_stem()
        .and_then(|s| s.to_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("app")
        .to_string()
}

/// 回滚尚未纳入 Manager 的新实例。子进程 watcher 随后上报的 Exited 事件会因
/// 对应 id 尚不存在而被安全忽略。
fn rollback_spawned(apps: &mut [App]) {
    for app in apps {
        app.cancel_health();
        if let Some(pid) = app.pid {
            let _ = sysprobe::send_signal_validated(pid, app.start_time, sysprobe::Signal::SIGKILL);
        }
    }
}

/// 派生子进程，挂日志采集 + supervisor。free 函数以避免与 `&mut self` 双借用冲突。
fn spawn_proc(tx: &mpsc::UnboundedSender<Cmd>, app: &mut App) -> Result<()> {
    let _ = paths::ensure_dirs();
    let log_path = paths::proc_log(&app.cfg.name, app.cfg.id);

    let mut std_cmd = std::process::Command::new(&app.cfg.command);
    std_cmd.args(&app.cfg.args);
    std_cmd.env_clear();
    for key in ENV_WHITELIST {
        if let Ok(v) = std::env::var(key) {
            std_cmd.env(key, v);
        }
    }
    for (k, v) in &app.cfg.env {
        std_cmd.env(k, v);
    }
    std_cmd.env("OWL_INSTANCE_ID", app.cfg.instance_index.to_string());
    std_cmd.env("NODE_APP_INSTANCE", app.cfg.instance_index.to_string());
    if let Some(port) = app.cfg.port {
        std_cmd.env("PORT", port.to_string());
    }
    if let Some(cwd) = &app.cfg.cwd {
        if !std::path::Path::new(cwd).is_dir() {
            return Err(OwlError::Invalid(format!("cwd 不存在: {cwd}")));
        }
        std_cmd.current_dir(cwd);
    }
    std_cmd.stdin(Stdio::null());
    std_cmd.stdout(Stdio::piped());
    std_cmd.stderr(Stdio::piped());

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        // setsid：脱离控制终端，使子进程在 Daemon 退出后存活（方案 B）。
        unsafe {
            std_cmd.pre_exec(|| {
                nix::unistd::setsid()
                    .map(|_| ())
                    .map_err(|e| std::io::Error::from_raw_os_error(e as i32))
            });
        }
    }

    let mut cmd = tokio::process::Command::from(std_cmd);
    cmd.kill_on_drop(false);

    let mut child = cmd
        .spawn()
        .map_err(|e| OwlError::Other(format!("启动失败 ({}): {e}", app.cfg.command)))?;

    let pid = child
        .id()
        .ok_or_else(|| OwlError::Other("无法获取子进程 PID".into()))?;

    let log_tx = process_log::spawn_writer(log_path);
    if let Some(out) = child.stdout.take() {
        process_log::spawn_collector(out, log_tx.clone(), false);
    }
    if let Some(err) = child.stderr.take() {
        process_log::spawn_collector(err, log_tx.clone(), true);
    }
    drop(log_tx);

    let tx2 = tx.clone();
    let id = app.cfg.id;
    tokio::spawn(async move {
        let reason = match child.wait().await {
            Ok(status) => {
                if let Some(code) = status.code() {
                    ExitStatusDetail::Code(code)
                } else {
                    #[cfg(unix)]
                    {
                        use std::os::unix::process::ExitStatusExt;
                        if let Some(sig) = status.signal() {
                            ExitStatusDetail::Signal(sig)
                        } else {
                            ExitStatusDetail::Unknown
                        }
                    }
                    #[cfg(not(unix))]
                    ExitStatusDetail::Unknown
                }
            }
            Err(_) => ExitStatusDetail::Unknown,
        };
        let _ = tx2.send(Cmd::Exited { id, reason });
    });

    owl_logger::info!(
        "进程 [{}] {} (pid={}) 已启动",
        app.cfg.id,
        app.cfg.name,
        pid
    );

    app.pid = Some(pid);
    app.start_time = sysprobe::pid_start_time(pid);
    app.start_instant = Some(Instant::now());
    app.status = ProcessStatus::Online;
    app.intent = Intent::None;
    app.cfg.last_pid = Some(pid);
    app.cfg.last_pid_start_time = app.start_time;
    app.generation += 1;
    app.health = HealthState::Unknown;
    app.health_failures = 0;

    // 健康检查任务（若已配置）。
    if let Some(hc) = app.cfg.health_check.clone() {
        let cancel = Arc::new(Notify::new());
        app.health_cancel = Some(cancel.clone());
        spawn_health(tx, app.cfg.id, app.generation, hc, app.cfg.port, cancel);
    }
    Ok(())
}

/// 周期健康探针任务；通过 `cancel` Notify 优雅停止。
fn spawn_health(
    tx: &mpsc::UnboundedSender<Cmd>,
    id: u32,
    generation: u64,
    cfg: HealthCheckConfig,
    port: Option<u16>,
    cancel: Arc<Notify>,
) {
    let tx = tx.clone();
    let interval = Duration::from_secs(cfg.interval_secs.max(1));
    let timeout = Duration::from_secs(cfg.timeout_secs.max(1));
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                _ = cancel.notified() => break,
            }
            let healthy = match health::probe(&cfg, port).await {
                Some(h) => h,
                None => match port {
                    Some(p) => health::tcp_probe("127.0.0.1", p, timeout).await,
                    None => true,
                },
            };
            if tx
                .send(Cmd::HealthResult {
                    id,
                    generation,
                    healthy,
                })
                .is_err()
            {
                break;
            }
        }
    });
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PortRange {
    base: u16,
    max: u16,
}

/// 由 `StartOptions` 构造持久化配置。调用方负责先解析并校验完整端口范围。
fn build_cfg(
    id: u32,
    created_at: i64,
    name: String,
    opts: &StartOptions,
    instance_index: u32,
    ports: Option<PortRange>,
) -> Result<PersistedApp> {
    Ok(PersistedApp {
        id,
        name,
        instance_index,
        command: opts.command.clone(),
        args: opts.args.clone(),
        cwd: opts.cwd.clone(),
        env: opts.env.clone(),
        port: instance_port(ports, instance_index)?,
        port_base: ports.map(|p| p.base),
        port_max: ports.map(|p| p.max),
        max_memory: opts.max_memory,
        max_restarts: opts.max_restarts,
        restart_strategy: opts.restart_strategy,
        restart_delay_ms: opts.restart_delay_ms,
        kill_signal: opts.kill_signal.clone(),
        health_check: opts.health_check.clone(),
        created_at,
        last_pid: None,
        last_pid_start_time: None,
    })
}

/// 关键字段差异（变更需重启生效，见 7.13）。
fn key_fields_differ(a: &PersistedApp, b: &PersistedApp) -> bool {
    a.command != b.command
        || a.args != b.args
        || a.cwd != b.cwd
        || a.env != b.env
        || a.port != b.port
        || a.port_base != b.port_base
        || a.port_max != b.port_max
}

/// 非关键字段差异（原地更新即可，无需重启）。
fn non_key_differ(a: &PersistedApp, b: &PersistedApp) -> bool {
    a.max_restarts != b.max_restarts
        || a.restart_delay_ms != b.restart_delay_ms
        || a.restart_strategy != b.restart_strategy
        || a.kill_signal != b.kill_signal
        || a.max_memory != b.max_memory
        || a.health_check != b.health_check
}

/// 解析端口范围，支持单端口或 `auto:5000-5100` / `5000-5100`。
/// 单端口是基准端口，最多可分配到 65535。
fn parse_port_range(s: Option<&str>) -> Result<Option<PortRange>> {
    let s = match s {
        None => return Ok(None),
        Some(s) => s.trim(),
    };
    if s.is_empty() {
        return Ok(None);
    }
    let body = s.strip_prefix("auto:").unwrap_or(s);
    let (start, end) = match body.split_once('-') {
        Some((start, end)) => (start.trim(), Some(end.trim())),
        None => (body.trim(), None),
    };
    let base = start
        .parse::<u16>()
        .map_err(|_| OwlError::Invalid(format!("无法解析端口: {s}")))?;
    let max = match end {
        Some(end) => end
            .parse::<u16>()
            .map_err(|_| OwlError::Invalid(format!("无法解析端口范围: {s}")))?,
        None => u16::MAX,
    };
    if base > max {
        return Err(OwlError::Invalid(format!("端口范围起点不能大于终点: {s}")));
    }
    Ok(Some(PortRange { base, max }))
}

fn instance_port(ports: Option<PortRange>, index: u32) -> Result<Option<u16>> {
    let Some(ports) = ports else {
        return Ok(None);
    };
    let index = u16::try_from(index)
        .map_err(|_| OwlError::Invalid(format!("实例序号超出端口可表示范围: {index}")))?;
    let port = ports
        .base
        .checked_add(index)
        .ok_or_else(|| OwlError::Invalid("端口分配超出 65535".into()))?;
    if port > ports.max {
        return Err(OwlError::Invalid(format!(
            "实例 #{index} 所需端口 {port} 超出声明范围 {}-{}",
            ports.base, ports.max
        )));
    }
    Ok(Some(port))
}

fn validate_start_spec(name: &str, opts: &StartOptions, ports: Option<PortRange>) -> Result<()> {
    validate_app_name(name)?;
    if opts.command.trim().is_empty() {
        return Err(OwlError::Invalid("command 不能为空".into()));
    }
    let count = opts.instances.max(1);
    if count > MAX_INSTANCES {
        return Err(OwlError::Invalid(format!(
            "实例数不能超过 {MAX_INSTANCES}: {count}"
        )));
    }
    // 预先验证末实例端口，确保启动/扩容时不会在中途失败。
    let _ = instance_port(ports, count - 1)?;
    Ok(())
}

fn validate_app_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 128 || name == "." || name == ".." {
        return Err(OwlError::Invalid(
            "进程名不能为空、`.`、`..`，且长度不能超过 128".into(),
        ));
    }
    if name.contains('/')
        || name.contains(std::path::MAIN_SEPARATOR)
        || name.chars().any(|c| c.is_control())
    {
        return Err(OwlError::Invalid(
            "进程名不能包含路径分隔符或控制字符".into(),
        ));
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct SnapshotFile {
    schema_version: u32,
    apps: Vec<StartOptions>,
}

fn snapshot_path(file: Option<&str>) -> PathBuf {
    match file {
        Some(p) => PathBuf::from(p),
        None => crate::common::paths::owl_home().join("saved.json"),
    }
}

/// 将当前 apps 按 name 聚合成可复用的 StartOptions（instances>1）。
fn snapshot_specs(apps: &HashMap<u32, App>) -> Vec<StartOptions> {
    #[derive(Default)]
    struct Group {
        count: u32,
        template: Option<PersistedApp>,
    }
    let mut groups: HashMap<String, Group> = HashMap::new();
    for app in apps.values() {
        let g = groups.entry(app.cfg.name.clone()).or_default();
        g.count += 1;
        if g.template.is_none() || app.cfg.instance_index == 0 {
            g.template = Some(app.cfg.clone());
        }
    }
    let mut out = Vec::new();
    let mut names: Vec<String> = groups.keys().cloned().collect();
    names.sort();
    for name in names {
        if let Some(g) = groups.get(&name) {
            if let Some(t) = &g.template {
                out.push(StartOptions {
                    name: Some(name.clone()),
                    command: t.command.clone(),
                    args: t.args.clone(),
                    cwd: t.cwd.clone(),
                    env: t.env.clone(),
                    instances: g.count.max(1),
                    port: match (t.port_base, t.port_max) {
                        (Some(base), Some(max)) if max != u16::MAX => Some(format!("{base}-{max}")),
                        (Some(base), _) => Some(base.to_string()),
                        _ => None,
                    },
                    max_memory: t.max_memory,
                    max_restarts: t.max_restarts,
                    restart_delay_ms: t.restart_delay_ms,
                    restart_strategy: t.restart_strategy,
                    kill_signal: t.kill_signal.clone(),
                    health_check: t.health_check.clone(),
                    wait_ready: false,
                    ready_timeout_secs: None,
                });
            }
        }
    }
    out
}

/// 轮询探测重接管进程的存活，消失则上报 Exited。
fn spawn_poll_watcher(tx: &mpsc::UnboundedSender<Cmd>, id: u32, pid: u32, start_time: Option<u64>) {
    let tx = tx.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(POLL_INTERVAL).await;
            if !sysprobe::validate(pid, start_time) {
                let _ = tx.send(Cmd::Exited {
                    id,
                    reason: ExitStatusDetail::Unknown,
                });
                break;
            }
        }
    });
}

/// 延迟后投递一个内部命令。
fn schedule(tx: &mpsc::UnboundedSender<Cmd>, delay: Duration, cmd: Cmd) {
    let tx = tx.clone();
    tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        let _ = tx.send(cmd);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn opts(instances: u32) -> StartOptions {
        StartOptions {
            name: Some("web".into()),
            command: "sleep".into(),
            args: vec!["60".into()],
            cwd: None,
            env: HashMap::new(),
            instances,
            port: Some("5000-5002".into()),
            max_memory: None,
            max_restarts: None,
            restart_delay_ms: Some(10),
            restart_strategy: RestartStrategy::OnFailure,
            kill_signal: None,
            health_check: None,
            wait_ready: false,
            ready_timeout_secs: None,
        }
    }

    #[test]
    fn port_range_enforces_upper_bound() {
        let ports = parse_port_range(Some("auto:5000-5001")).unwrap();
        assert_eq!(instance_port(ports, 0).unwrap(), Some(5000));
        assert_eq!(instance_port(ports, 1).unwrap(), Some(5001));
        assert!(instance_port(ports, 2).is_err());
        assert!(parse_port_range(Some("5002-5001")).is_err());
    }

    #[test]
    fn app_name_cannot_escape_log_directory() {
        assert!(validate_app_name("../escape").is_err());
        assert!(validate_app_name("nested/app").is_err());
        assert!(validate_app_name("web-api_1").is_ok());
    }

    #[test]
    fn apply_dry_run_reconciles_the_whole_group_and_instance_count() {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut manager = Manager::new(tx, rx);
        let mut existing = opts(2);
        existing.restart_delay_ms = None;
        for (id, index) in [(0, 0), (1, 1)] {
            let cfg = build_cfg(
                id,
                0,
                "web".into(),
                &existing,
                index,
                parse_port_range(Some("5000-5002")).unwrap(),
            )
            .unwrap();
            manager.apps.insert(id, App::from_cfg(cfg));
        }

        let report = manager.handle_apply(vec![opts(3)], false, true).unwrap();
        assert!(report.contains("全部现存实例原地更新"));
        assert!(report.contains("scale   web 2 -> 3"));
    }
}
