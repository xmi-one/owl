//! IPC 线缆协议：握手、请求、响应。

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::process::entry::{HealthCheckConfig, ProcessInfo, RestartStrategy};

/// 协议版本：CLI 与 Daemon 必须一致。每次破坏性修改递增。
pub const PROTOCOL_VERSION: u32 = 1;

/// 每个连接的第一帧：握手。
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Handshake {
    pub protocol_version: u32,
    pub client_version: String,
}

impl Handshake {
    pub fn current() -> Self {
        Handshake {
            protocol_version: PROTOCOL_VERSION,
            client_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

/// 启动选项。Phase 1 仅消费 command/args/cwd/env/restart_* 字段，
/// 其余（instances/port/max_memory/health_check/wait_ready）为 Phase 2 预留。
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StartOptions {
    pub name: Option<String>,
    /// 可执行文件名/路径，**直接 exec，不经 shell**。
    pub command: String,
    pub args: Vec<String>,
    pub cwd: Option<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    pub instances: u32,
    pub port: Option<String>,
    pub max_memory: Option<u64>,
    pub max_restarts: Option<u32>,
    pub restart_delay_ms: Option<u64>,
    pub restart_strategy: RestartStrategy,
    pub kill_signal: Option<String>,
    pub health_check: Option<HealthCheckConfig>,
    pub wait_ready: bool,
    pub ready_timeout_secs: Option<u64>,
}

/// CLI → Daemon 请求。
///
/// `target` 接受：精确 id（数字）、name、或字面量 `"all"`。
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Request {
    Start(Box<StartOptions>),
    Stop {
        target: String,
    },
    Restart {
        target: String,
    },
    Delete {
        target: String,
    },
    List,
    Info {
        target: String,
    },
    Logs {
        target: String,
        lines: usize,
        follow: bool,
    },
    Flush {
        target: String,
    },
    Reset {
        target: String,
    },
    Apply {
        apps: Vec<StartOptions>,
        prune: bool,
        dry_run: bool,
    },
    Scale {
        target: String,
        n: u32,
    },
    Reload {
        target: String,
    },
    Save {
        file: Option<String>,
    },
    Resurrect {
        file: Option<String>,
    },
    SetLogLevel {
        level: String,
    },
    Kill,
}

/// Daemon → CLI 响应。一条连接可发送多帧（用于 logs --follow）。
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Response {
    Ok(String),
    ProcessList(Vec<ProcessInfo>),
    ProcessDetail(ProcessInfo),
    LogLines(Vec<String>),
    LogChunk(Vec<String>),
    /// --wait-ready 等待期间的进度推送。
    Progress(String),
    /// --wait-ready 判定就绪。
    Ready(ProcessInfo),
    StreamEnd,
    VersionMismatch {
        daemon_version: u32,
    },
    Error(String),
}
