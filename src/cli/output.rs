//! 终端输出格式化：彩色状态、人类可读表格、JSON。

use colored::Colorize;
use tabled::settings::Style;
use tabled::{Table, Tabled};

use crate::process::entry::{HealthState, ProcessInfo, ProcessStatus};

#[derive(Tabled)]
struct Row {
    #[tabled(rename = "id")]
    id: u32,
    #[tabled(rename = "name")]
    name: String,
    #[tabled(rename = "status")]
    status: String,
    #[tabled(rename = "pid")]
    pid: String,
    #[tabled(rename = "restarts")]
    restarts: u32,
    #[tabled(rename = "uptime")]
    uptime: String,
    #[tabled(rename = "cpu")]
    cpu: String,
    #[tabled(rename = "mem")]
    mem: String,
    #[tabled(rename = "health")]
    health: String,
}

/// 是否应启用彩色：受 `--no-color`、`NO_COLOR`、TTY 共同控制。
pub fn color_enabled(no_color_flag: bool) -> bool {
    if no_color_flag || std::env::var_os("NO_COLOR").is_some() {
        return false;
    }
    std::io::IsTerminal::is_terminal(&std::io::stdout())
}

fn health_label(health: HealthState, color: bool) -> String {
    let s = match health {
        HealthState::Unknown => "-",
        HealthState::Healthy => "healthy",
        HealthState::Unhealthy => "unhealthy",
    };
    if !color {
        return s.to_string();
    }
    match health {
        HealthState::Unknown => s.dimmed().to_string(),
        HealthState::Healthy => s.green().to_string(),
        HealthState::Unhealthy => s.red().to_string(),
    }
}

fn colorize_status(status: ProcessStatus, color: bool) -> String {
    let s = status.to_string();
    if !color {
        return s;
    }
    match status {
        ProcessStatus::Online => s.green().to_string(),
        ProcessStatus::Errored => s.red().bold().to_string(),
        ProcessStatus::Stopping | ProcessStatus::Launching => s.yellow().to_string(),
        ProcessStatus::Stopped => s.dimmed().to_string(),
    }
}

/// 渲染进程列表为表格。
pub fn render_list(list: &[ProcessInfo], _color: bool) -> String {
    if list.is_empty() {
        return "（无进程）".to_string();
    }
    let rows: Vec<Row> = list
        .iter()
        .map(|p| Row {
            id: p.id,
            name: p.name.clone(),
            // `tabled` 在不同终端对 ANSI 宽度处理不一致，可能导致列错位；
            // 列表表格统一使用无颜色文本，保证对齐稳定。
            status: colorize_status(p.status, false),
            pid: p.pid.map(|v| v.to_string()).unwrap_or_else(|| "-".into()),
            restarts: p.restarts,
            uptime: human_duration(p.uptime_secs),
            cpu: if p.status == ProcessStatus::Online {
                format!("{:.1}%", p.cpu_percent)
            } else {
                "-".to_string()
            },
            mem: if p.memory_bytes == 0 {
                "-".to_string()
            } else {
                human_size(p.memory_bytes)
            },
            health: health_label(p.health, false),
        })
        .collect();
    Table::new(rows).with(Style::rounded()).to_string()
}

/// 渲染单个进程详情。
pub fn render_info(p: &ProcessInfo, color: bool) -> String {
    let mut out = String::new();
    let line = |out: &mut String, k: &str, v: String| {
        out.push_str(&format!("{:<14} {}\n", k, v));
    };
    line(&mut out, "id", p.id.to_string());
    line(&mut out, "name", p.name.clone());
    line(&mut out, "status", colorize_status(p.status, color));
    if p.health != HealthState::Unknown {
        line(&mut out, "health", health_label(p.health, color));
    }
    line(
        &mut out,
        "command",
        format!("{} {}", p.command, p.args.join(" ")),
    );
    line(
        &mut out,
        "pid",
        p.pid.map(|v| v.to_string()).unwrap_or_else(|| "-".into()),
    );
    line(&mut out, "restarts", p.restarts.to_string());
    line(
        &mut out,
        "max_restarts",
        p.max_restarts
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".into()),
    );
    line(&mut out, "uptime", human_duration(p.uptime_secs));
    if p.status == ProcessStatus::Online {
        line(&mut out, "cpu", format!("{:.1}%", p.cpu_percent));
    }
    if p.memory_bytes > 0 {
        line(&mut out, "memory", human_size(p.memory_bytes));
    }
    if let Some(limit) = p.max_memory {
        line(&mut out, "max_memory", human_size(limit));
    }
    if let Some(port) = p.port {
        line(&mut out, "port", port.to_string());
    }
    line(&mut out, "restart", format!("{:?}", p.restart_strategy));
    out
}

pub fn human_size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit < UNITS.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{size:.1} {}", UNITS[unit])
    }
}

pub fn human_duration(secs: u64) -> String {
    if secs == 0 {
        return "0s".to_string();
    }
    let d = secs / 86400;
    let h = (secs % 86400) / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if d > 0 {
        format!("{d}d {h}h")
    } else if h > 0 {
        format!("{h}h {m}m")
    } else if m > 0 {
        format!("{m}m {s}s")
    } else {
        format!("{s}s")
    }
}
