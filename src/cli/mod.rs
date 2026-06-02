//! CLI 模块：命令定义、客户端、Daemon 拉起、输出格式化与编排。

pub mod client;
pub mod commands;
pub mod daemon_launcher;
pub mod output;
pub mod service;

use colored::Colorize;
use crossterm::event::{self, Event, KeyCode};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use clap::CommandFactory;
use clap_complete::{generate, shells};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Paragraph, Row, Table, TableState};

use crate::common::paths;
use crate::ipc::message::{Request, Response};
use crate::process::entry::{ProcessInfo, RestartStrategy};
use client::Client;
use commands::{Cli, Commands, CompletionShell, ServiceCommands, ServiceTarget, StartArgs};

/// CLI 退出码约定（见方案 7.16）。
const EXIT_OK: i32 = 0;
const EXIT_ERR: i32 = 1;
const EXIT_NOT_FOUND: i32 = 3;
const EXIT_DAEMON_UNREACHABLE: i32 = 4;

/// 执行 CLI 命令，返回进程退出码。
pub async fn run(cli: Cli) -> i32 {
    let color = output::color_enabled(cli.no_color);
    let json = cli.json;

    match cli.command {
        Commands::Daemon => EXIT_OK, // 由 main 处理，不会走到这里
        Commands::Kill => kill().await,
        Commands::Start(args) => {
            if let Some(target) = start_existing_shorthand(&args) {
                return simple(Request::Restart { target }, color).await;
            }
            let opts = args.into_options();
            let wait = opts.wait_ready;
            let req = Request::Start(Box::new(opts));
            if wait {
                start_wait(req, json, color).await
            } else {
                match one_shot(req).await {
                    Ok(Response::ProcessDetail(info)) => {
                        if json {
                            println!(
                                "{}",
                                serde_json::to_string_pretty(&info).unwrap_or_default()
                            );
                        } else {
                            let msg = format!("已启动 [{}] {}", info.id, info.name);
                            println!("{}", if color { msg.green().to_string() } else { msg });
                        }
                        EXIT_OK
                    }
                    Ok(resp) => print_simple(resp, color),
                    Err(code) => code,
                }
            }
        }
        Commands::List => match one_shot(Request::List).await {
            Ok(Response::ProcessList(list)) => {
                if json {
                    println!("{}", serde_json::to_string_pretty(&list).unwrap_or_default());
                } else {
                    println!("{}", output::render_list(&list, color));
                }
                EXIT_OK
            }
            Ok(resp) => print_simple(resp, color),
            Err(code) => code,
        },
        Commands::Info { target } => match one_shot(Request::Info { target }).await {
            Ok(Response::ProcessDetail(info)) => {
                if json {
                    println!("{}", serde_json::to_string_pretty(&info).unwrap_or_default());
                } else {
                    print!("{}", output::render_info(&info, color));
                }
                EXIT_OK
            }
            Ok(resp) => print_simple(resp, color),
            Err(code) => code,
        },
        Commands::Stop { target } => simple(Request::Stop { target }, color).await,
        Commands::Restart { target } => simple(Request::Restart { target }, color).await,
        Commands::Delete { target } => simple(Request::Delete { target }, color).await,
        Commands::Flush { target } => simple(Request::Flush { target }, color).await,
        Commands::Reset { target } => simple(Request::Reset { target }, color).await,
        Commands::Apply {
            file,
            prune,
            dry_run,
        } => {
            let apps = match crate::config::load_file_auto(&file) {
                Ok(a) => a,
                Err(e) => {
                    eprintln!("{}", if color { e.to_string().red().to_string() } else { e.to_string() });
                    return EXIT_ERR;
                }
            };
            simple(
                Request::Apply {
                    apps,
                    prune,
                    dry_run,
                },
                color,
            )
            .await
        }
        Commands::LogLevel { level } => simple(Request::SetLogLevel { level }, color).await,
        Commands::Logs {
            target,
            lines,
            follow,
        } => logs(target, lines, follow, color).await,
        Commands::Scale { target, n } => {
            simple(Request::Scale { target, n }, color).await
        }
        Commands::Reload { target } => {
            reload_stream(target, color).await
        }
        Commands::Monit { interval, count } => monit(interval, count, json, color).await,
        Commands::Save { file } => {
            simple(Request::Save { file }, color).await
        }
        Commands::Resurrect { file } => {
            simple(Request::Resurrect { file }, color).await
        }
        Commands::Completions { shell } => {
            output_completions(shell);
            EXIT_OK
        }
        Commands::Service { command } => match command {
            ServiceCommands::Generate {
                target,
                output,
                name,
            } => {
                let rendered = match target {
                    ServiceTarget::Systemd => service::render_systemd(&name),
                    ServiceTarget::Launchd => service::render_launchd(&name),
                };
                match rendered.and_then(|s| service::write_or_print(&s, output.as_deref())) {
                    Ok(msg) => {
                        println!("{}", if color { msg.green().to_string() } else { msg });
                        EXIT_OK
                    }
                    Err(e) => {
                        eprintln!("{}", if color { e.red().to_string() } else { e });
                        EXIT_ERR
                    }
                }
            }
        },
    }
}

/// 兼容 `owl start <id|name>`：若仅提供单个 token 且未携带其它启动选项，
/// 将其视为“拉起已存在（通常是 stopped）的进程”。
fn start_existing_shorthand(args: &StartArgs) -> Option<String> {
    if args.name.is_some()
        || args.cwd.is_some()
        || !args.env.is_empty()
        || args.instances != 1
        || args.port.is_some()
        || args.max_memory.is_some()
        || args.max_restarts.is_some()
        || args.restart_delay.is_some()
        || args.restart_strategy != RestartStrategy::OnFailure
        || args.kill_signal.is_some()
        || args.health_url.is_some()
        || args.health_script.is_some()
        || args.wait_ready
        || args.ready_timeout != 30
    {
        return None;
    }
    if args.cmd.len() != 1 {
        return None;
    }
    let token = args.cmd[0].trim();
    if token.is_empty() || token.contains('/') || token.contains(std::path::MAIN_SEPARATOR) {
        return None;
    }
    Some(token.to_string())
}

/// 确保 Daemon 运行 + 连接 + 发送 + 取首个响应。
async fn one_shot(req: Request) -> Result<Response, i32> {
    if let Err(e) = daemon_launcher::ensure_daemon().await {
        eprintln!("{e}");
        return Err(EXIT_DAEMON_UNREACHABLE);
    }
    let mut client = match Client::connect().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            return Err(EXIT_DAEMON_UNREACHABLE);
        }
    };
    if let Err(e) = client.send(&req).await {
        eprintln!("{e}");
        return Err(EXIT_ERR);
    }
    match client.recv().await {
        Ok(Some(resp)) => Ok(resp),
        Ok(None) => {
            eprintln!("Daemon 未返回响应");
            Err(EXIT_ERR)
        }
        Err(e) => {
            eprintln!("{e}");
            Err(EXIT_ERR)
        }
    }
}

/// `start --wait-ready`：流式接收 Progress / Ready / Error。
async fn start_wait(req: Request, json: bool, color: bool) -> i32 {
    if let Err(e) = daemon_launcher::ensure_daemon().await {
        eprintln!("{e}");
        return EXIT_DAEMON_UNREACHABLE;
    }
    let mut client = match Client::connect().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            return EXIT_DAEMON_UNREACHABLE;
        }
    };
    if let Err(e) = client.send(&req).await {
        eprintln!("{e}");
        return EXIT_ERR;
    }
    use std::io::Write;
    loop {
        match client.recv().await {
            Ok(Some(Response::Progress(msg))) => {
                if !json {
                    print!("{msg}");
                    let _ = std::io::stdout().flush();
                }
            }
            Ok(Some(Response::Ready(info))) => {
                if json {
                    println!("{}", serde_json::to_string_pretty(&info).unwrap_or_default());
                } else {
                    let msg = format!("\n已就绪 [{}] {}", info.id, info.name);
                    println!("{}", if color { msg.green().to_string() } else { msg });
                }
                return EXIT_OK;
            }
            Ok(Some(Response::ProcessDetail(info))) => {
                if !json {
                    println!("已启动 [{}] {}", info.id, info.name);
                }
                return EXIT_OK;
            }
            Ok(Some(Response::Error(e))) => {
                eprintln!("\n{}", if color { e.red().to_string() } else { e.clone() });
                return EXIT_ERR;
            }
            Ok(Some(_)) => {}
            Ok(None) => return EXIT_OK,
            Err(e) => {
                eprintln!("{e}");
                return EXIT_ERR;
            }
        }
    }
}

async fn simple(req: Request, color: bool) -> i32 {
    match one_shot(req).await {
        Ok(resp) => print_simple(resp, color),
        Err(code) => code,
    }
}

fn print_simple(resp: Response, color: bool) -> i32 {
    match resp {
        Response::Ok(msg) => {
            println!("{}", if color { msg.green().to_string() } else { msg });
            EXIT_OK
        }
        Response::Error(e) => {
            eprintln!("{}", if color { e.red().to_string() } else { e.clone() });
            if e.contains("未找到") {
                EXIT_NOT_FOUND
            } else {
                EXIT_ERR
            }
        }
        Response::Progress(msg) => {
            print!("{msg}");
            EXIT_OK
        }
        other => {
            println!("{other:?}");
            EXIT_OK
        }
    }
}

fn output_completions(shell: CompletionShell) {
    let mut cmd = Cli::command();
    match shell {
        CompletionShell::Bash => generate(shells::Bash, &mut cmd, "owl", &mut std::io::stdout()),
        CompletionShell::Zsh => generate(shells::Zsh, &mut cmd, "owl", &mut std::io::stdout()),
        CompletionShell::Fish => generate(shells::Fish, &mut cmd, "owl", &mut std::io::stdout()),
        CompletionShell::Elvish => {
            generate(shells::Elvish, &mut cmd, "owl", &mut std::io::stdout())
        }
        CompletionShell::Powershell => {
            generate(shells::PowerShell, &mut cmd, "owl", &mut std::io::stdout())
        }
    }
}

async fn reload_stream(target: String, color: bool) -> i32 {
    if let Err(e) = daemon_launcher::ensure_daemon().await {
        eprintln!("{e}");
        return EXIT_DAEMON_UNREACHABLE;
    }
    let mut client = match Client::connect().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            return EXIT_DAEMON_UNREACHABLE;
        }
    };
    if let Err(e) = client.send(&Request::Reload { target }).await {
        eprintln!("{e}");
        return EXIT_ERR;
    }
    loop {
        match client.recv().await {
            Ok(Some(Response::Progress(msg))) => print!("{msg}"),
            Ok(Some(Response::Ok(msg))) => {
                println!();
                println!("{}", if color { msg.green().to_string() } else { msg });
                return EXIT_OK;
            }
            Ok(Some(Response::Error(e))) => {
                eprintln!("{}", if color { e.red().to_string() } else { e });
                return EXIT_ERR;
            }
            Ok(Some(_)) => {}
            Ok(None) => return EXIT_OK,
            Err(e) => {
                eprintln!("{e}");
                return EXIT_ERR;
            }
        }
    }
}

async fn logs(target: String, lines: usize, follow: bool, color: bool) -> i32 {
    if let Err(e) = daemon_launcher::ensure_daemon().await {
        eprintln!("{e}");
        return EXIT_DAEMON_UNREACHABLE;
    }
    let mut client = match Client::connect().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            return EXIT_DAEMON_UNREACHABLE;
        }
    };
    let req = Request::Logs {
        target,
        lines,
        follow,
    };
    if let Err(e) = client.send(&req).await {
        eprintln!("{e}");
        return EXIT_ERR;
    }
    loop {
        match client.recv().await {
            Ok(Some(Response::LogLines(ls))) | Ok(Some(Response::LogChunk(ls))) => {
                for l in ls {
                    print_log_line(&l, color);
                }
            }
            Ok(Some(Response::StreamEnd)) => return EXIT_OK,
            Ok(Some(Response::Error(e))) => {
                eprintln!("{}", if color { e.red().to_string() } else { e.clone() });
                return if e.contains("未找到") { EXIT_NOT_FOUND } else { EXIT_ERR };
            }
            Ok(Some(_)) => {}
            Ok(None) => return EXIT_OK,
            Err(e) => {
                eprintln!("{e}");
                return EXIT_ERR;
            }
        }
    }
}

async fn monit(interval_secs: u64, count: Option<u32>, json: bool, color: bool) -> i32 {
    if !json && count.is_none() {
        return monit_tui(interval_secs).await;
    }
    monit_plain(interval_secs, count, json, color).await
}

async fn monit_plain(interval_secs: u64, count: Option<u32>, json: bool, color: bool) -> i32 {
    let sleep = std::time::Duration::from_secs(interval_secs.max(1));
    let mut n = 0u32;
    loop {
        match one_shot(Request::List).await {
            Ok(Response::ProcessList(list)) => {
                // 清屏并回到左上角（ANSI）
                if !json {
                    print!("\x1b[2J\x1b[H");
                    println!("owl monit  刷新间隔: {}s  (Ctrl+C 退出)", sleep.as_secs());
                    println!("{}", output::render_list(&list, color));
                } else {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&serde_json::json!({
                            "timestamp": chrono::Utc::now().timestamp(),
                            "data": list
                        }))
                        .unwrap_or_default()
                    );
                }
            }
            Ok(resp) => {
                let code = print_simple(resp, color);
                if code != EXIT_OK {
                    return code;
                }
            }
            Err(code) => return code,
        }

        n += 1;
        if let Some(c) = count {
            if n >= c {
                return EXIT_OK;
            }
        }
        tokio::time::sleep(sleep).await;
    }
}

async fn monit_tui(interval_secs: u64) -> i32 {
    if let Err(e) = enable_raw_mode() {
        eprintln!("无法启用终端 raw 模式: {e}");
        return EXIT_ERR;
    }
    let mut stdout = std::io::stdout();
    if let Err(e) = execute!(stdout, EnterAlternateScreen) {
        let _ = disable_raw_mode();
        eprintln!("无法进入备用屏幕: {e}");
        return EXIT_ERR;
    }
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = match Terminal::new(backend) {
        Ok(t) => t,
        Err(e) => {
            let _ = disable_raw_mode();
            eprintln!("初始化 TUI 失败: {e}");
            return EXIT_ERR;
        }
    };

    let mut last: Vec<ProcessInfo> = Vec::new();
    let mut selected: usize = 0;
    let refresh_every = std::time::Duration::from_secs(interval_secs.max(1));
    let mut next_refresh = std::time::Instant::now();
    let mut last_error: Option<String> = None;

    loop {
        let _ = terminal.draw(|f| {
            let size = f.area();
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Min(8), Constraint::Length(8), Constraint::Length(1)])
                .split(size);

            let header = Row::new(vec![
                "id", "name", "status", "pid", "uptime", "cpu", "mem", "health",
            ])
            .style(Style::default().add_modifier(Modifier::BOLD));
            let rows: Vec<Row> = last
                .iter()
                .map(|p| {
                    let status_color = match p.status.to_string().as_str() {
                        "online" => Color::Green,
                        "errored" => Color::Red,
                        "launching" | "stopping" => Color::Yellow,
                        _ => Color::DarkGray,
                    };
                    Row::new(vec![
                        p.id.to_string(),
                        p.name.clone(),
                        p.status.to_string(),
                        p.pid.map(|x| x.to_string()).unwrap_or_else(|| "-".into()),
                        output::human_duration(p.uptime_secs),
                        if p.status.to_string() == "online" {
                            format!("{:.1}%", p.cpu_percent)
                        } else {
                            "-".into()
                        },
                        if p.memory_bytes == 0 {
                            "-".into()
                        } else {
                            output::human_size(p.memory_bytes)
                        },
                        format!("{:?}", p.health).to_lowercase(),
                    ])
                    .style(Style::default().fg(status_color))
                })
                .collect();

            let table = Table::new(
                rows,
                [
                    Constraint::Length(4),
                    Constraint::Length(16),
                    Constraint::Length(10),
                    Constraint::Length(8),
                    Constraint::Length(8),
                    Constraint::Length(8),
                    Constraint::Length(10),
                    Constraint::Length(10),
                ],
            )
            .header(header)
            .block(Block::default().title("owl monit").borders(Borders::ALL))
            .row_highlight_style(Style::default().bg(Color::DarkGray))
            .highlight_symbol(">> ");
            let mut table_state = TableState::default();
            if !last.is_empty() {
                if selected >= last.len() {
                    selected = last.len() - 1;
                }
                table_state.select(Some(selected));
            }
            f.render_stateful_widget(table, chunks[0], &mut table_state);

            let detail = if let Some(p) = last.get(selected) {
                format!(
                    "name: {}\ncmd: {} {}\nrestarts: {}\nport: {}\nmax_memory: {}",
                    p.name,
                    p.command,
                    p.args.join(" "),
                    p.restarts,
                    p.port.map(|x| x.to_string()).unwrap_or_else(|| "-".into()),
                    p.max_memory
                        .map(output::human_size)
                        .unwrap_or_else(|| "-".into()),
                )
            } else {
                "暂无进程".to_string()
            };
            let detail_widget =
                Paragraph::new(detail).block(Block::default().title("detail").borders(Borders::ALL));
            f.render_widget(detail_widget, chunks[1]);

            let tip = if let Some(e) = &last_error {
                format!("q:退出  ↑/↓:选择  r:立即刷新   error: {e}")
            } else {
                "q:退出  ↑/↓:选择  r:立即刷新".to_string()
            };
            f.render_widget(Paragraph::new(Line::from(tip)), chunks[2]);
        });

        if event::poll(std::time::Duration::from_millis(80)).unwrap_or(false) {
            if let Ok(Event::Key(k)) = event::read() {
                match k.code {
                    KeyCode::Char('q') => break,
                    KeyCode::Down => {
                        if !last.is_empty() {
                            selected = (selected + 1).min(last.len() - 1);
                        }
                    }
                    KeyCode::Up => {
                        selected = selected.saturating_sub(1);
                    }
                    KeyCode::Char('r') => match one_shot(Request::List).await {
                        Ok(Response::ProcessList(list)) => {
                            last = list;
                            last_error = None;
                        }
                        Ok(other) => last_error = Some(format!("unexpected: {other:?}")),
                        Err(code) => last_error = Some(format!("request failed: {code}")),
                    },
                    _ => {}
                }
            }
        }

        if std::time::Instant::now() >= next_refresh {
            match one_shot(Request::List).await {
                Ok(Response::ProcessList(list)) => {
                    last = list;
                    last_error = None;
                }
                Ok(other) => last_error = Some(format!("unexpected: {other:?}")),
                Err(code) => last_error = Some(format!("request failed: {code}")),
            }
            next_refresh = std::time::Instant::now() + refresh_every;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let _ = disable_raw_mode();
    let _ = execute!(terminal.backend_mut(), LeaveAlternateScreen);
    let _ = terminal.show_cursor();
    EXIT_OK
}

fn print_log_line(line: &str, color: bool) {
    if color && line.starts_with("[err] ") {
        println!("{}", line.red());
    } else {
        println!("{line}");
    }
}

/// `owl kill`：不自动拉起 Daemon。
async fn kill() -> i32 {
    if std::os::unix::net::UnixStream::connect(paths::socket_path()).is_err() {
        println!("Daemon 未运行");
        return EXIT_OK;
    }
    let mut client = match Client::connect().await {
        Ok(c) => c,
        Err(_) => {
            println!("Daemon 未运行");
            return EXIT_OK;
        }
    };
    if client.send(&Request::Kill).await.is_err() {
        return EXIT_ERR;
    }
    match client.recv().await {
        Ok(Some(Response::Ok(msg))) => {
            println!("{msg}");
            EXIT_OK
        }
        _ => EXIT_OK,
    }
}
