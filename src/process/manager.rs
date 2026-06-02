//! ProcessManager —— 单所有者 Actor 模型。
//!
//! 状态由单个 task 独占；IPC handler / supervisor / 定时器通过 mpsc 发命令，
//! oneshot 回结果，避免「持锁跨 await」。

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};

use crate::common::errors::{OwlError, Result};
use crate::common::paths;
use crate::common::state::StateFile;
use crate::common::sysprobe;
use crate::log::process_log;
use crate::ipc::message::StartOptions;
use crate::process::entry::{
    HealthState, PersistedApp, ProcessInfo, ProcessStatus, RestartStrategy,
};

const KILL_TIMEOUT: Duration = Duration::from_secs(5);
const BASE_DELAY: Duration = Duration::from_secs(1);
const MAX_DELAY: Duration = Duration::from_secs(16);
const MIN_UPTIME: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// 子进程环境白名单（env_clear 后按需注入，保证可复现）。
const ENV_WHITELIST: &[&str] = &[
    "PATH", "HOME", "LANG", "LC_ALL", "LC_CTYPE", "TERM", "USER", "SHELL", "TZ",
];

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
        }
    }

    fn to_info(&self) -> ProcessInfo {
        let uptime_secs = match (self.status, self.start_instant) {
            (ProcessStatus::Online, Some(inst)) => inst.elapsed().as_secs(),
            _ => 0,
        };
        ProcessInfo {
            id: self.cfg.id,
            name: self.cfg.name.clone(),
            command: self.cfg.command.clone(),
            args: self.cfg.args.clone(),
            pid: self.pid,
            status: self.status,
            health: HealthState::Unknown,
            port: self.cfg.port,
            restarts: self.restarts,
            max_restarts: self.cfg.max_restarts,
            uptime_secs,
            cpu_percent: 0.0,
            memory_bytes: 0,
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
    LogPath(String, oneshot::Sender<Result<PathBuf>>),
    Flush(String, oneshot::Sender<Result<String>>),
    Reset(String, oneshot::Sender<Result<String>>),
    // 内部事件
    Exited { id: u32, success: bool },
    EscalateKill { id: u32 },
    RestartNow { id: u32 },
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
pub fn start_manager() -> ManagerHandle {
    let (tx, rx) = mpsc::unbounded_channel();
    let mut mgr = Manager::new(tx.clone(), rx);
    mgr.recover();
    tokio::spawn(mgr.run());
    ManagerHandle { tx }
}

struct Manager {
    apps: HashMap<u32, App>,
    next_id: u32,
    tx: mpsc::UnboundedSender<Cmd>,
    rx: mpsc::UnboundedReceiver<Cmd>,
}

impl Manager {
    fn new(tx: mpsc::UnboundedSender<Cmd>, rx: mpsc::UnboundedReceiver<Cmd>) -> Self {
        Manager {
            apps: HashMap::new(),
            next_id: 0,
            tx,
            rx,
        }
    }

    /// 从 state.json 恢复：重接管仍存活的进程（方案 B），其余标记 Stopped。
    fn recover(&mut self) {
        let state = StateFile::load();
        self.next_id = state.next_id;
        for cfg in state.apps {
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
                    let _ =
                        reply.send(self.handle_batch(&target, "restart", |m, id| m.restart_one(id)));
                }
                Cmd::Delete(target, reply) => {
                    let _ =
                        reply.send(self.handle_batch(&target, "delete", |m, id| m.delete_one(id)));
                }
                Cmd::List(reply) => {
                    let mut list: Vec<ProcessInfo> =
                        self.apps.values().map(|a| a.to_info()).collect();
                    list.sort_by_key(|i| i.id);
                    let _ = reply.send(list);
                }
                Cmd::Info(target, reply) => {
                    let _ = reply.send(self.handle_info(&target));
                }
                Cmd::LogPath(target, reply) => {
                    let _ = reply.send(self.handle_log_path(&target));
                }
                Cmd::Flush(target, reply) => {
                    let _ = reply.send(self.handle_batch(&target, "flush", |m, id| m.flush_one(id)));
                }
                Cmd::Reset(target, reply) => {
                    let _ = reply.send(self.handle_batch(&target, "reset", |m, id| m.reset_one(id)));
                }
                Cmd::Exited { id, success } => self.handle_exited(id, success),
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
        let id = self.next_id;
        self.next_id += 1;
        let cfg = PersistedApp {
            id,
            name,
            command: opts.command,
            args: opts.args,
            cwd: opts.cwd,
            env: opts.env,
            port: None,
            max_memory: opts.max_memory,
            max_restarts: opts.max_restarts,
            restart_strategy: opts.restart_strategy,
            restart_delay_ms: opts.restart_delay_ms,
            kill_signal: opts.kill_signal,
            health_check: opts.health_check,
            created_at: chrono::Utc::now().timestamp(),
            last_pid: None,
            last_pid_start_time: None,
        };
        let mut app = App::from_cfg(cfg);
        let tx = self.tx.clone();
        spawn_proc(&tx, &mut app)?;
        let info = app.to_info();
        self.apps.insert(id, app);
        self.persist();
        Ok(info)
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
            Ok(format!("{verb}: {ok} 成功, {} 失败: {}", errs.len(), errs.join("; ")))
        }
    }

    fn stop_one(&mut self, id: u32) -> Result<()> {
        let app = self.apps.get_mut(&id).ok_or_else(not_found)?;
        match app.pid {
            None => {
                app.status = ProcessStatus::Stopped;
                Ok(())
            }
            Some(pid) => {
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
                app.intent = Intent::Restart;
                app.status = ProcessStatus::Stopping;
                let sig = sysprobe::parse_signal(app.cfg.kill_signal.as_deref());
                sysprobe::send_signal_validated(pid, app.start_time, sig);
                schedule(&self.tx, KILL_TIMEOUT, Cmd::EscalateKill { id });
                Ok(())
            }
            None => self.start_existing(id),
        }
    }

    fn delete_one(&mut self, id: u32) -> Result<()> {
        let app = self.apps.get_mut(&id).ok_or_else(not_found)?;
        match app.pid {
            Some(pid) => {
                app.intent = Intent::Delete;
                app.status = ProcessStatus::Stopping;
                let sig = sysprobe::parse_signal(app.cfg.kill_signal.as_deref());
                sysprobe::send_signal_validated(pid, app.start_time, sig);
                schedule(&self.tx, KILL_TIMEOUT, Cmd::EscalateKill { id });
                Ok(())
            }
            None => {
                self.apps.remove(&id);
                Ok(())
            }
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

    fn handle_exited(&mut self, id: u32, success: bool) {
        enum Next {
            Remove,
            Settled,
            RestartNow,
            Schedule(Duration),
        }

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

            match app.intent {
                Intent::Delete => Next::Remove,
                Intent::Stop => {
                    app.status = ProcessStatus::Stopped;
                    app.intent = Intent::None;
                    Next::Settled
                }
                Intent::Restart => {
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
                        app.status = ProcessStatus::Stopped;
                        Next::Settled
                    } else {
                        app.consecutive_crashes += 1;
                        if let Some(max) = app.cfg.max_restarts {
                            if app.consecutive_crashes > max {
                                app.status = ProcessStatus::Errored;
                                owl_logger::error!(
                                    "进程 [{id}] {} 超过 max_restarts({max})，标记 Errored",
                                    app.cfg.name
                                );
                                Next::Settled
                            } else {
                                app.status = ProcessStatus::Launching;
                                Next::Schedule(compute_delay(app))
                            }
                        } else {
                            app.status = ProcessStatus::Launching;
                            Next::Schedule(compute_delay(app))
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
    std_cmd.env("OWL_INSTANCE_ID", "0");
    std_cmd.env("NODE_APP_INSTANCE", "0");
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

    if let Some(out) = child.stdout.take() {
        process_log::spawn_collector(out, log_path.clone(), false);
    }
    if let Some(err) = child.stderr.take() {
        process_log::spawn_collector(err, log_path.clone(), true);
    }

    let tx2 = tx.clone();
    let id = app.cfg.id;
    tokio::spawn(async move {
        let success = child.wait().await.map(|s| s.success()).unwrap_or(false);
        let _ = tx2.send(Cmd::Exited { id, success });
    });

    app.pid = Some(pid);
    app.start_time = sysprobe::pid_start_time(pid);
    app.start_instant = Some(Instant::now());
    app.status = ProcessStatus::Online;
    app.intent = Intent::None;
    app.cfg.last_pid = Some(pid);
    app.cfg.last_pid_start_time = app.start_time;
    Ok(())
}

/// 轮询探测重接管进程的存活，消失则上报 Exited。
fn spawn_poll_watcher(
    tx: &mpsc::UnboundedSender<Cmd>,
    id: u32,
    pid: u32,
    start_time: Option<u64>,
) {
    let tx = tx.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(POLL_INTERVAL).await;
            if !sysprobe::validate(pid, start_time) {
                let _ = tx.send(Cmd::Exited { id, success: true });
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
