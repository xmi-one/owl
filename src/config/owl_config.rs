//! `owl.toml` 解析。
//!
//! 示例：
//! ```toml
//! [[apps]]
//! name = "web"
//! command = "node"
//! args = ["server.js"]
//! cwd = "/srv/web"
//! env = { NODE_ENV = "production" }
//! port = 3000              # 整数或字符串均可
//! max_memory = "512M"      # 整数(字节)或带单位字符串
//! max_restarts = 10
//! restart_strategy = "on-failure"
//! restart_delay_ms = 1000
//! kill_signal = "SIGTERM"
//!
//! [apps.health_check]
//! url = "http://127.0.0.1:{port}/health"
//! interval_secs = 30
//! timeout_secs = 5
//! max_failures = 3
//! ```

use std::collections::HashMap;

use serde::Deserialize;

use crate::common::errors::{OwlError, Result};
use crate::ipc::message::StartOptions;
use crate::process::entry::{HealthCheckConfig, RestartStrategy};

#[derive(Deserialize, Debug)]
struct OwlConfig {
    #[serde(default)]
    apps: Vec<AppSpec>,
}

#[derive(Deserialize, Debug)]
struct AppSpec {
    name: Option<String>,
    command: String,
    #[serde(default)]
    args: Vec<String>,
    cwd: Option<String>,
    #[serde(default)]
    env: HashMap<String, String>,
    #[serde(default = "one")]
    instances: u32,
    port: Option<Scalar>,
    max_memory: Option<Scalar>,
    max_restarts: Option<u32>,
    restart_delay_ms: Option<u64>,
    restart_strategy: Option<String>,
    kill_signal: Option<String>,
    health_check: Option<HealthSpec>,
}

#[derive(Deserialize, Debug)]
struct HealthSpec {
    url: Option<String>,
    script: Option<String>,
    #[serde(default = "thirty")]
    interval_secs: u64,
    #[serde(default = "five")]
    timeout_secs: u64,
    #[serde(default = "three")]
    max_failures: u32,
}

/// 标量：兼容 TOML 整数与字符串（用于 port / max_memory）。
#[derive(Deserialize, Debug)]
#[serde(untagged)]
enum Scalar {
    Int(i64),
    Str(String),
}

impl Scalar {
    fn as_string(&self) -> String {
        match self {
            Scalar::Int(i) => i.to_string(),
            Scalar::Str(s) => s.clone(),
        }
    }
}

fn one() -> u32 {
    1
}
fn thirty() -> u64 {
    30
}
fn five() -> u64 {
    5
}
fn three() -> u32 {
    3
}

/// 读取并解析 owl.toml，转换为 `StartOptions` 列表。
pub fn load_file(path: &str) -> Result<Vec<StartOptions>> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| OwlError::Invalid(format!("读取配置 {path} 失败: {e}")))?;
    let cfg: OwlConfig =
        toml::from_str(&text).map_err(|e| OwlError::Invalid(format!("解析 {path} 失败: {e}")))?;
    if cfg.apps.is_empty() {
        return Err(OwlError::Invalid(format!("{path} 中没有 [[apps]] 定义")));
    }
    cfg.apps.into_iter().map(spec_to_options).collect()
}

fn spec_to_options(spec: AppSpec) -> Result<StartOptions> {
    let restart_strategy = match &spec.restart_strategy {
        Some(s) => s
            .parse::<RestartStrategy>()
            .map_err(OwlError::Invalid)?,
        None => RestartStrategy::default(),
    };
    let max_memory = match &spec.max_memory {
        Some(s) => Some(parse_size(&s.as_string()).map_err(OwlError::Invalid)?),
        None => None,
    };
    let health_check = spec.health_check.map(|h| HealthCheckConfig {
        url: h.url,
        script: h.script,
        interval_secs: h.interval_secs.max(1),
        timeout_secs: h.timeout_secs.max(1),
        max_failures: h.max_failures.max(1),
    });
    Ok(StartOptions {
        name: spec.name,
        command: spec.command,
        args: spec.args,
        cwd: spec.cwd,
        env: spec.env,
        instances: spec.instances.max(1),
        port: spec.port.map(|p| p.as_string()),
        max_memory,
        max_restarts: spec.max_restarts,
        restart_delay_ms: spec.restart_delay_ms,
        restart_strategy,
        kill_signal: spec.kill_signal,
        health_check,
        wait_ready: false,
        ready_timeout_secs: None,
    })
}

/// 解析人类可读大小，如 `512M` / `1G` / `1048576`。
pub fn parse_size(s: &str) -> std::result::Result<u64, String> {
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
