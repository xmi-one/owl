//! 终端输出格式化：彩色状态、人类可读表格、JSON。

use colored::Colorize;
use tabled::settings::object::Rows;
use tabled::settings::{Remove, Style};
use tabled::{Table, Tabled};

use crate::process::entry::{HealthState, ProcessInfo, ProcessStatus};

#[derive(Tabled)]
struct Row {
    #[tabled(rename = "id")]
    id: u32,
    #[tabled(rename = "name")]
    name: String,
    #[tabled(rename = "mode")]
    mode: String,
    #[tabled(rename = "pid")]
    pid: String,
    #[tabled(rename = "uptime")]
    uptime: String,
    #[tabled(rename = "↺")]
    restarts: String,
    #[tabled(rename = "status")]
    status: String,
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
        HealthState::Unhealthy => s.red().bold().to_string(),
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
pub fn render_list(list: &[ProcessInfo], color: bool) -> String {
    if list.is_empty() {
        return "（无进程）".to_string();
    }
    let rows: Vec<Row> = list
        .iter()
        .map(|p| {
            let name_str = if color {
                p.name.green().bold().to_string()
            } else {
                p.name.clone()
            };
            let mode_str = if color {
                "fork".green().to_string()
            } else {
                "fork".to_string()
            };
            let restarts_str = if color {
                if p.restarts == 0 {
                    "0".green().to_string()
                } else if p.restarts < 10 {
                    p.restarts.to_string().yellow().to_string()
                } else {
                    p.restarts.to_string().red().bold().to_string()
                }
            } else {
                p.restarts.to_string()
            };
            let cpu_str = if p.status == ProcessStatus::Online {
                let s = format!("{:.1}%", p.cpu_percent);
                if color {
                    s.green().to_string()
                } else {
                    s
                }
            } else {
                "-".to_string()
            };
            let mem_str = if p.memory_bytes == 0 {
                "-".to_string()
            } else {
                let s = human_size(p.memory_bytes);
                if color {
                    s.green().to_string()
                } else {
                    s
                }
            };
            let uptime_str = {
                let s = human_duration(p.uptime_secs);
                if color {
                    s.green().to_string()
                } else {
                    s
                }
            };

            Row {
                id: p.id,
                name: name_str,
                mode: mode_str,
                pid: p.pid.map(|v| v.to_string()).unwrap_or_else(|| "-".into()),
                uptime: uptime_str,
                restarts: restarts_str,
                status: colorize_status(p.status, color),
                cpu: cpu_str,
                mem: mem_str,
                health: health_label(p.health, color),
            }
        })
        .collect();
    Table::new(rows).with(Style::modern()).to_string()
}

/// 渲染单个进程详情。
pub fn render_info(p: &ProcessInfo, color: bool) -> String {
    #[derive(Tabled)]
    struct InfoRow {
        #[tabled(rename = "key")]
        key: String,
        #[tabled(rename = "value")]
        value: String,
    }

    let mut rows = Vec::new();
    let mut add_row = |key: &str, value: String| {
        let key_str = if color {
            key.cyan().bold().to_string()
        } else {
            key.to_string()
        };
        rows.push(InfoRow {
            key: key_str,
            value,
        });
    };

    add_row("status", colorize_status(p.status, color));
    add_row(
        "name",
        if color {
            p.name.green().bold().to_string()
        } else {
            p.name.clone()
        },
    );
    add_row("id", p.id.to_string());
    add_row(
        "mode",
        if color {
            "fork".green().to_string()
        } else {
            "fork".to_string()
        },
    );
    add_row(
        "pid",
        p.pid.map(|v| v.to_string()).unwrap_or_else(|| "-".into()),
    );

    let restarts_str = if color {
        if p.restarts == 0 {
            "0".green().to_string()
        } else if p.restarts < 10 {
            p.restarts.to_string().yellow().to_string()
        } else {
            p.restarts.to_string().red().bold().to_string()
        }
    } else {
        p.restarts.to_string()
    };
    add_row("restarts", restarts_str);

    add_row(
        "max restarts",
        p.max_restarts
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".into()),
    );

    add_row("uptime", {
        let s = human_duration(p.uptime_secs);
        if color {
            s.green().to_string()
        } else {
            s
        }
    });

    if p.status == ProcessStatus::Online {
        add_row("cpu", {
            let s = format!("{:.1}%", p.cpu_percent);
            if color {
                s.green().to_string()
            } else {
                s
            }
        });
    }

    if p.memory_bytes > 0 {
        add_row("memory", {
            let s = human_size(p.memory_bytes);
            if color {
                s.green().to_string()
            } else {
                s
            }
        });
    }

    if let Some(limit) = p.max_memory {
        add_row("max memory", human_size(limit));
    }

    if let Some(port) = p.port {
        add_row("port", port.to_string());
    }

    add_row("restart strategy", format!("{:?}", p.restart_strategy));

    if p.health != HealthState::Unknown {
        add_row("health", health_label(p.health, color));
    }

    add_row("command", format!("{} {}", p.command, p.args.join(" ")));

    // 添加日志路径
    let log_path = crate::common::paths::proc_log(&p.name, p.id);
    add_row("log path", log_path.to_string_lossy().to_string());

    let table = Table::new(rows)
        .with(Style::modern())
        .with(Remove::row(Rows::first()))
        .to_string();

    let title = if color {
        format!(
            "Describing process with id {} - name {}",
            p.id.to_string().cyan().bold(),
            p.name.green().bold()
        )
    } else {
        format!("Describing process with id {} - name {}", p.id, p.name)
    };

    format!("{title}\n{table}\n")
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
