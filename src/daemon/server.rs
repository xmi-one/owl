//! Daemon UDS Server：绑定、陈旧 socket 处理、accept 事件循环、握手、优雅退出。

use std::io::Write;

use tokio::io::BufReader;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;

use crate::common::errors::{OwlError, Result};
use crate::common::paths;
use crate::daemon::handler;
use crate::ipc::message::{Handshake, Request, Response, PROTOCOL_VERSION};
use crate::ipc::protocol::{read_frame, write_frame};
use crate::process::manager::{self, ManagerHandle};

/// 运行 Daemon 主循环。阻塞直到收到 Kill 或 SIGTERM/SIGINT。
pub async fn run() -> Result<()> {
    paths::ensure_dirs()?;
    write_pid_file()?;

    let listener = bind_listener()?;
    let mgr = manager::start_manager();

    owl_logger::info!(
        "Owl Daemon 启动 (pid={}, proto={PROTOCOL_VERSION})",
        std::process::id()
    );

    // 关闭信号汇聚：Kill 请求 / SIGTERM / SIGINT。
    let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(4);
    spawn_signal_listener(shutdown_tx.clone());

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _addr)) => {
                        let mgr = mgr.clone();
                        let shutdown_tx = shutdown_tx.clone();
                        tokio::spawn(async move {
                            if let Err(e) = serve_conn(stream, mgr, shutdown_tx).await {
                                owl_logger::debug!("连接处理结束: {e}");
                            }
                        });
                    }
                    Err(e) => {
                        owl_logger::warn!("accept 失败: {e}");
                    }
                }
            }
            _ = shutdown_rx.recv() => {
                break;
            }
        }
    }

    owl_logger::info!("Daemon 优雅退出中…");
    mgr.shutdown().await;
    cleanup();
    Ok(())
}

/// 处理单个客户端连接：先校验握手，再循环处理请求帧。
async fn serve_conn(
    stream: UnixStream,
    mgr: ManagerHandle,
    shutdown_tx: mpsc::Sender<()>,
) -> Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // 第一帧：握手。
    let hs: Option<Handshake> = read_frame(&mut reader).await?;
    let hs = match hs {
        Some(h) => h,
        None => return Ok(()),
    };
    if hs.protocol_version != PROTOCOL_VERSION {
        write_frame(
            &mut write_half,
            &Response::VersionMismatch {
                daemon_version: PROTOCOL_VERSION,
            },
        )
        .await?;
        return Ok(());
    }
    // 握手确认。
    write_frame(&mut write_half, &Response::Ok("owl-daemon".into())).await?;

    loop {
        let req: Option<Request> = read_frame(&mut reader).await?;
        let req = match req {
            Some(r) => r,
            None => break, // 客户端关闭
        };
        let should_kill = handler::handle(&mgr, req, &mut write_half).await?;
        if should_kill {
            let _ = shutdown_tx.send(()).await;
            break;
        }
    }
    Ok(())
}

/// 绑定 UDS，处理陈旧 socket（connect 探测失败则 unlink 重绑），并设 0600 权限。
fn bind_listener() -> Result<UnixListener> {
    let sock = paths::socket_path();
    if sock.exists() {
        // 尝试连接旧 socket；连得上说明已有 daemon，拒绝重复绑定。
        match std::os::unix::net::UnixStream::connect(&sock) {
            Ok(_) => {
                return Err(OwlError::Other(
                    "已有 Daemon 在运行（socket 可连接）".into(),
                ));
            }
            Err(_) => {
                // 陈旧 socket，清理。
                let _ = std::fs::remove_file(&sock);
            }
        }
    }
    let listener = UnixListener::bind(&sock)?;
    // 仅属主可访问。
    set_socket_perms(&sock);
    Ok(listener)
}

#[cfg(unix)]
fn set_socket_perms(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;
    let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
}

#[cfg(not(unix))]
fn set_socket_perms(_path: &std::path::Path) {}

fn write_pid_file() -> Result<()> {
    let pid = std::process::id();
    let mut f = std::fs::File::create(paths::pid_path())?;
    writeln!(f, "{pid}")?;
    Ok(())
}

fn cleanup() {
    let _ = std::fs::remove_file(paths::socket_path());
    let _ = std::fs::remove_file(paths::pid_path());
}

#[cfg(unix)]
fn spawn_signal_listener(tx: mpsc::Sender<()>) {
    use tokio::signal::unix::{signal, SignalKind};
    tokio::spawn(async move {
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(_) => return,
        };
        let mut int = match signal(SignalKind::interrupt()) {
            Ok(s) => s,
            Err(_) => return,
        };
        tokio::select! {
            _ = term.recv() => {}
            _ = int.recv() => {}
        }
        let _ = tx.send(()).await;
    });
}

#[cfg(not(unix))]
fn spawn_signal_listener(_tx: mpsc::Sender<()>) {}
