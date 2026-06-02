//! PM2 `ecosystem.config.json` 兼容解析（常见字段子集）。

use std::collections::HashMap;

use serde::Deserialize;

use crate::common::errors::{OwlError, Result};
use crate::config::parse_size;
use crate::ipc::message::StartOptions;
use crate::process::entry::RestartStrategy;

#[derive(Deserialize, Debug)]
struct Ecosystem {
    #[serde(default)]
    apps: Vec<EcoApp>,
}

#[derive(Deserialize, Debug)]
struct EcoApp {
    name: Option<String>,
    script: Option<String>,
    #[serde(default)]
    args: Vec<String>,
    cwd: Option<String>,
    #[serde(default)]
    env: HashMap<String, String>,
    instances: Option<u32>,
    exec_mode: Option<String>,
    port: Option<serde_json::Value>,
    max_memory_restart: Option<serde_json::Value>,
    max_restarts: Option<u32>,
    restart_delay: Option<u64>,
    autorestart: Option<bool>,
    kill_timeout: Option<u64>,
}

pub fn load_file(path: &str) -> Result<Vec<StartOptions>> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| OwlError::Invalid(format!("读取配置 {path} 失败: {e}")))?;
    let cfg: Ecosystem = serde_json::from_str(&text)
        .map_err(|e| OwlError::Invalid(format!("解析 {path} 失败: {e}")))?;
    if cfg.apps.is_empty() {
        return Err(OwlError::Invalid(format!("{path} 中没有 apps 定义")));
    }
    cfg.apps.into_iter().map(to_start_options).collect()
}

fn to_start_options(app: EcoApp) -> Result<StartOptions> {
    let command = app
        .script
        .ok_or_else(|| OwlError::Invalid("ecosystem app 缺少 script".into()))?;
    let restart_strategy = match app.autorestart {
        Some(false) => RestartStrategy::Never,
        _ => RestartStrategy::OnFailure,
    };
    let max_memory = match app.max_memory_restart {
        None => None,
        Some(v) => Some(parse_memory(v)?),
    };
    let port = match app.port {
        None => None,
        Some(v) => Some(value_to_string(v)?),
    };

    let mut args = app.args;
    // PM2 里 args 也可能是单字符串；这里约定 JSON 数组，字符串场景后续可扩展。
    if app.exec_mode.as_deref() == Some("cluster") && app.instances.is_none() {
        // PM2 cluster 若未给 instances，默认 1，避免歧义。
        args.shrink_to_fit();
    }

    Ok(StartOptions {
        name: app.name,
        command,
        args,
        cwd: app.cwd,
        env: app.env,
        instances: app.instances.unwrap_or(1).max(1),
        port,
        max_memory,
        max_restarts: app.max_restarts,
        restart_delay_ms: app.restart_delay,
        restart_strategy,
        kill_signal: app
            .kill_timeout
            .map(|_| "SIGTERM".to_string()), // 先保守映射，后续可加 timeout->graceful stop
        health_check: None,
        wait_ready: false,
        ready_timeout_secs: None,
    })
}

fn parse_memory(v: serde_json::Value) -> Result<u64> {
    match v {
        serde_json::Value::Number(n) => n
            .as_u64()
            .ok_or_else(|| OwlError::Invalid("max_memory_restart 非法数字".into())),
        serde_json::Value::String(s) => parse_size(&s).map_err(OwlError::Invalid),
        _ => Err(OwlError::Invalid(
            "max_memory_restart 仅支持数字或字符串".into(),
        )),
    }
}

fn value_to_string(v: serde_json::Value) -> Result<String> {
    match v {
        serde_json::Value::String(s) => Ok(s),
        serde_json::Value::Number(n) => Ok(n.to_string()),
        _ => Err(OwlError::Invalid("port 仅支持数字或字符串".into())),
    }
}
