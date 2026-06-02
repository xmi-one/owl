//! CLI 模块：命令定义、客户端、Daemon 拉起、输出格式化与编排。

pub mod client;
pub mod commands;
pub mod daemon_launcher;
pub mod output;

use colored::Colorize;

use crate::common::paths;
use crate::ipc::message::{Request, Response};
use client::Client;
use commands::{Cli, Commands};

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
            let req = Request::Start(Box::new(args.into_options()));
            match one_shot(req).await {
                Ok(Response::ProcessDetail(info)) => {
                    if json {
                        println!("{}", serde_json::to_string_pretty(&info).unwrap_or_default());
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
        Commands::LogLevel { level } => simple(Request::SetLogLevel { level }, color).await,
        Commands::Logs {
            target,
            lines,
            follow,
        } => logs(target, lines, follow, color).await,
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
        other => {
            println!("{other:?}");
            EXIT_OK
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
