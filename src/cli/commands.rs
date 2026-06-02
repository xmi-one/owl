//! CLI 子命令定义 (clap derive)。

use clap::{Parser, Subcommand};

use crate::ipc::message::StartOptions;
use crate::process::entry::{HealthCheckConfig, RestartStrategy};

#[derive(Parser, Debug)]
#[command(
    name = "owl",
    version,
    about = "Owl —— 高性能、语言无关的进程管理器 (Rust 版 PM2)",
    propagate_version = true
)]
pub struct Cli {
    /// 机器可读 JSON 输出
    #[arg(long, global = true)]
    pub json: bool,

    /// 关闭彩色输出（也尊重 NO_COLOR 环境变量与非 TTY）
    #[arg(long, global = true)]
    pub no_color: bool,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
#[allow(clippy::large_enum_variant)] // StartArgs 为 CLI 解析结构，仅单实例，size 无关紧要
pub enum Commands {
    /// 启动进程：owl start [选项] -- <命令> [参数...]（直接 exec，不经 shell）
    Start(StartArgs),
    /// 停止进程（支持 all）
    Stop { target: String },
    /// 重启进程（支持 all）
    Restart { target: String },
    /// 删除进程（支持 all）
    #[command(alias = "rm")]
    Delete { target: String },
    /// 列出所有进程
    #[command(alias = "ls", alias = "ps")]
    List,
    /// 查看单个进程详情
    Info { target: String },
    /// 查看日志
    Logs {
        target: String,
        /// 显示末尾行数
        #[arg(short = 'n', long, default_value_t = 20)]
        lines: usize,
        /// 持续跟随输出
        #[arg(short, long)]
        follow: bool,
    },
    /// 清空进程日志
    Flush { target: String },
    /// 清零重启计数
    Reset { target: String },
    /// 运行时调整 Daemon 自身日志级别
    LogLevel { level: String },
    /// 终止 Daemon（在线子进程脱离存活）
    Kill,
    /// [内部] 以 Daemon 模式运行
    #[command(hide = true)]
    Daemon,
}

#[derive(clap::Args, Debug)]
pub struct StartArgs {
    /// 进程名（缺省由命令名派生）
    #[arg(long)]
    pub name: Option<String>,

    /// 工作目录
    #[arg(long)]
    pub cwd: Option<String>,

    /// 环境变量 KEY=VALUE（可重复）
    #[arg(long = "env", value_parser = parse_kv)]
    pub env: Vec<(String, String)>,

    /// 实例数（Phase 1 暂按 1 处理）
    #[arg(long, default_value_t = 1)]
    pub instances: u32,

    /// 端口（Phase 2 生效）
    #[arg(long)]
    pub port: Option<String>,

    /// 内存上限，如 512M / 1G（Phase 2 生效）
    #[arg(long, value_parser = parse_size)]
    pub max_memory: Option<u64>,

    /// 最大连续重启次数
    #[arg(long)]
    pub max_restarts: Option<u32>,

    /// 固定重启间隔（ms，设置后禁用指数退避）
    #[arg(long)]
    pub restart_delay: Option<u64>,

    /// 重启策略：always / on-failure / never
    #[arg(long = "restart-strategy", default_value = "on-failure", value_parser = parse_strategy)]
    pub restart_strategy: RestartStrategy,

    /// 停止信号，默认 SIGTERM
    #[arg(long = "kill-signal")]
    pub kill_signal: Option<String>,

    /// HTTP 健康检查 URL（支持 {port} 占位符，仅 http://）
    #[arg(long = "health-url")]
    pub health_url: Option<String>,

    /// 脚本健康检查（退出码 0 为健康）
    #[arg(long = "health-script")]
    pub health_script: Option<String>,

    /// 健康检查间隔（秒）
    #[arg(long = "health-interval", default_value_t = 30)]
    pub health_interval: u64,

    /// 健康检查超时（秒）
    #[arg(long = "health-timeout", default_value_t = 5)]
    pub health_timeout: u64,

    /// 连续失败多少次判定不健康并重启
    #[arg(long = "health-retries", default_value_t = 3)]
    pub health_retries: u32,

    /// 阻塞等待进程就绪后再返回（见 7.14）
    #[arg(long = "wait-ready")]
    pub wait_ready: bool,

    /// --wait-ready 的超时（秒）
    #[arg(long = "ready-timeout", default_value_t = 30)]
    pub ready_timeout: u64,

    /// 命令与参数（位于 `--` 之后）
    #[arg(trailing_var_arg = true, allow_hyphen_values = true, required = true, num_args = 1..)]
    pub cmd: Vec<String>,
}

impl StartArgs {
    pub fn into_options(self) -> StartOptions {
        let mut iter = self.cmd.into_iter();
        let command = iter.next().unwrap_or_default();
        let args: Vec<String> = iter.collect();
        let health_check = if self.health_url.is_some() || self.health_script.is_some() {
            Some(HealthCheckConfig {
                url: self.health_url,
                script: self.health_script,
                interval_secs: self.health_interval.max(1),
                timeout_secs: self.health_timeout.max(1),
                max_failures: self.health_retries.max(1),
            })
        } else {
            None
        };
        StartOptions {
            name: self.name,
            command,
            args,
            cwd: self.cwd,
            env: self.env.into_iter().collect(),
            instances: self.instances.max(1),
            port: self.port,
            max_memory: self.max_memory,
            max_restarts: self.max_restarts,
            restart_delay_ms: self.restart_delay,
            restart_strategy: self.restart_strategy,
            kill_signal: self.kill_signal,
            health_check,
            wait_ready: self.wait_ready,
            ready_timeout_secs: Some(self.ready_timeout),
        }
    }
}

fn parse_kv(s: &str) -> Result<(String, String), String> {
    match s.split_once('=') {
        Some((k, v)) => Ok((k.to_string(), v.to_string())),
        None => Err(format!("环境变量需为 KEY=VALUE 格式: {s}")),
    }
}

fn parse_strategy(s: &str) -> Result<RestartStrategy, String> {
    s.parse()
}

/// 解析人类可读大小，如 `512M` / `1G` / `1048576`。
fn parse_size(s: &str) -> Result<u64, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("空的大小".into());
    }
    let upper = s.to_ascii_uppercase();
    let (num_part, mult): (&str, u64) = if let Some(n) = upper.strip_suffix("GB") {
        (n, 1024 * 1024 * 1024)
    } else if let Some(n) = upper.strip_suffix("MB") {
        (n, 1024 * 1024)
    } else if let Some(n) = upper.strip_suffix("KB") {
        (n, 1024)
    } else if let Some(n) = upper.strip_suffix('G') {
        (n, 1024 * 1024 * 1024)
    } else if let Some(n) = upper.strip_suffix('M') {
        (n, 1024 * 1024)
    } else if let Some(n) = upper.strip_suffix('K') {
        (n, 1024)
    } else if let Some(n) = upper.strip_suffix('B') {
        (n, 1)
    } else {
        (upper.as_str(), 1)
    };
    let value: f64 = num_part
        .trim()
        .parse()
        .map_err(|_| format!("无法解析大小: {s}"))?;
    Ok((value * mult as f64) as u64)
}
