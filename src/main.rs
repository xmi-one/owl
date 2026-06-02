//! Owl 进程管理器入口：根据子命令分发到 CLI 客户端或 Daemon。

mod cli;
mod common;
mod config;
mod daemon;
mod ipc;
mod log;
mod process;

use clap::Parser;

use cli::commands::{Cli, Commands};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Daemon => {
            // 保活 owl-logger guard 直到 Daemon 退出。
            let _guard = log::daemon_log::init();
            if let Err(e) = daemon::server::run().await {
                owl_logger::error!("Daemon 异常退出: {e}");
                std::process::exit(1);
            }
        }
        _ => {
            let code = cli::run(cli).await;
            std::process::exit(code);
        }
    }
}
