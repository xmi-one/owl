//! CLI 模块：命令定义、客户端、Daemon 拉起、输出格式化与编排。

pub mod client;
pub mod commands;
pub mod daemon_launcher;
pub mod output;
pub mod service;

use colored::Colorize;
use clap::CommandFactory;
use clap_complete::{generate, shells};

use crate::common::paths;
use crate::ipc::message::{Request, Response};
use client::Client;
use commands::{Cli, Commands, CompletionShell, ServiceCommands, ServiceTarget};

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
            let apps = match crate::config::load_file(&file) {
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
