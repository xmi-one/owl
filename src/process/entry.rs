//! 进程相关的核心数据结构：持久化配置、运行时状态、对外展示视图。

use serde::{Deserialize, Serialize};

/// 重启策略。
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RestartStrategy {
    /// 任何退出都重启。
    Always,
    /// 仅非零退出码重启。
    #[default]
    OnFailure,
    /// 不自动重启。
    Never,
}

impl std::str::FromStr for RestartStrategy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().replace('_', "-").as_str() {
            "always" => Ok(RestartStrategy::Always),
            "on-failure" | "onfailure" => Ok(RestartStrategy::OnFailure),
            "never" => Ok(RestartStrategy::Never),
            other => Err(format!("未知的重启策略: {other}")),
        }
    }
}

/// 健康检查配置。
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct HealthCheckConfig {
    /// HTTP 探针 URL，支持 `{port}` 占位符。
    pub url: Option<String>,
    /// 脚本路径。
    pub script: Option<String>,
    pub interval_secs: u64,
    pub timeout_secs: u64,
    pub max_failures: u32,
}

/// 进程运行状态。
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessStatus {
    Launching,
    Online,
    Stopping,
    Stopped,
    /// 超过 max_restarts，放弃重启。
    Errored,
}

impl std::fmt::Display for ProcessStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            ProcessStatus::Launching => "launching",
            ProcessStatus::Online => "online",
            ProcessStatus::Stopping => "stopping",
            ProcessStatus::Stopped => "stopped",
            ProcessStatus::Errored => "errored",
        };
        f.write_str(s)
    }
}

/// 健康检查结果，与运行状态正交。
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthState {
    Unknown,
    Healthy,
    Unhealthy,
}

/// 持久化到 `state.json` 的用户意图与配置。
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PersistedApp {
    pub id: u32,
    pub name: String,
    /// 同名进程组内的实例序号（0..instances）。
    #[serde(default)]
    pub instance_index: u32,
    pub command: String,
    pub args: Vec<String>,
    pub cwd: Option<String>,
    #[serde(default)]
    pub env: std::collections::HashMap<String, String>,
    pub port: Option<u16>,
    /// 进程组基准端口；实例 i 的端口 = port_base + i（用于 scale 扩容时分配）。
    #[serde(default)]
    pub port_base: Option<u16>,
    /// 进程组可分配的最大端口。`None` 表示未配置端口。
    ///
    /// 旧 state.json 中没有此字段；恢复时会将已有 `port_base` 迁移为
    /// `u16::MAX`，保持旧版本“单端口可顺延扩容”的语义。
    #[serde(default)]
    pub port_max: Option<u16>,
    pub max_memory: Option<u64>,
    pub max_restarts: Option<u32>,
    #[serde(default)]
    pub restart_strategy: RestartStrategy,
    pub restart_delay_ms: Option<u64>,
    pub kill_signal: Option<String>,
    pub health_check: Option<HealthCheckConfig>,
    /// Unix epoch 秒。
    pub created_at: i64,
    /// 重接管校验：上次已知 PID 及其启动时刻。
    pub last_pid: Option<u32>,
    pub last_pid_start_time: Option<u64>,
}

/// 对外（IPC / 展示）的合并视图。
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProcessInfo {
    pub id: u32,
    pub name: String,
    pub instance_index: u32,
    pub command: String,
    pub args: Vec<String>,
    pub pid: Option<u32>,
    pub status: ProcessStatus,
    pub health: HealthState,
    pub port: Option<u16>,
    pub restarts: u32,
    pub max_restarts: Option<u32>,
    pub uptime_secs: u64,
    pub cpu_percent: f32,
    pub memory_bytes: u64,
    pub max_memory: Option<u64>,
    pub created_at: i64,
    pub restart_strategy: RestartStrategy,
}
