//! RPC 请求分发：把 `Request` 映射到 ProcessManager 调用并产生 `Response`。

use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::common::errors::OwlError;
use crate::ipc::message::{Request, Response};
use crate::ipc::protocol::write_frame;
use crate::log::daemon_log;
use crate::log::process_log;
use crate::process::manager::ManagerHandle;

/// 处理单个请求。返回 `Ok(true)` 表示请求已要求 Daemon 关闭（Kill）。
///
/// 对流式请求（logs --follow）会直接向 `writer` 连续写入多帧。
pub async fn handle<W>(
    mgr: &ManagerHandle,
    req: Request,
    writer: &mut W,
) -> Result<bool, OwlError>
where
    W: AsyncWrite + Unpin,
{
    match req {
        Request::Start(opts) => {
            let wait = opts.wait_ready;
            let timeout = opts.ready_timeout_secs.unwrap_or(30);
            let hc = opts.health_check.clone();
            match mgr.start(*opts).await {
                Ok(info) => {
                    if wait {
                        wait_ready(mgr, info, hc, timeout, writer).await?;
                    } else {
                        write_frame(writer, &Response::ProcessDetail(info)).await?;
                    }
                }
                Err(e) => {
                    write_frame(writer, &Response::Error(e.to_string())).await?;
                }
            }
            Ok(false)
        }
        Request::Stop { target } => reply_result(writer, mgr.stop(target).await).await,
        Request::Restart { target } => reply_result(writer, mgr.restart(target).await).await,
        Request::Delete { target } => reply_result(writer, mgr.delete(target).await).await,
        Request::Reset { target } => reply_result(writer, mgr.reset(target).await).await,
        Request::Flush { target } => reply_result(writer, mgr.flush(target).await).await,
        Request::Apply {
            apps,
            prune,
            dry_run,
        } => reply_result(writer, mgr.apply(apps, prune, dry_run).await).await,
        Request::Scale { target, n } => reply_result(writer, mgr.scale(target, n).await).await,
        Request::Reload { target } => {
            reload_rolling(mgr, &target, writer).await?;
            Ok(false)
        }
        Request::Save { file } => reply_result(writer, mgr.save(file).await).await,
        Request::Resurrect { file } => reply_result(writer, mgr.resurrect(file).await).await,
        Request::List => {
            let resp = match mgr.list().await {
                Ok(list) => Response::ProcessList(list),
                Err(e) => Response::Error(e.to_string()),
            };
            write_frame(writer, &resp).await?;
            Ok(false)
        }
        Request::Info { target } => {
            let resp = match mgr.info(target).await {
                Ok(info) => Response::ProcessDetail(info),
                Err(e) => Response::Error(e.to_string()),
            };
            write_frame(writer, &resp).await?;
            Ok(false)
        }
        Request::SetLogLevel { level } => {
            let resp = match daemon_log::set_level(&level) {
                Ok(()) => Response::Ok(format!("日志级别已设为 {level}")),
                Err(e) => Response::Error(e),
            };
            write_frame(writer, &resp).await?;
            Ok(false)
        }
        Request::Logs {
            target,
            lines,
            follow,
        } => {
            handle_logs(mgr, &target, lines, follow, writer).await?;
            Ok(false)
        }
        Request::Kill => {
            write_frame(writer, &Response::Ok("Daemon 正在退出".into())).await?;
            let _ = writer.flush().await;
            Ok(true)
        }
    }
}

async fn reply_result<W>(
    writer: &mut W,
    result: Result<String, OwlError>,
) -> Result<bool, OwlError>
where
    W: AsyncWrite + Unpin,
{
    let resp = match result {
        Ok(msg) => Response::Ok(msg),
        Err(e) => Response::Error(e.to_string()),
    };
    write_frame(writer, &resp).await?;
    Ok(false)
}

async fn handle_logs<W>(
    mgr: &ManagerHandle,
    target: &str,
    lines: usize,
    follow: bool,
    writer: &mut W,
) -> Result<(), OwlError>
where
    W: AsyncWrite + Unpin,
{
    let path = match mgr.log_path(target.to_string()).await {
        Ok(p) => p,
        Err(e) => {
            write_frame(writer, &Response::Error(e.to_string())).await?;
            return Ok(());
        }
    };

    let tail = process_log::read_last_lines(&path, lines).await;
    write_frame(writer, &Response::LogLines(tail)).await?;

    if !follow {
        write_frame(writer, &Response::StreamEnd).await?;
        return Ok(());
    }

    // follow 模式：使用增量 Seek 与读取，支持轮转兜底
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    let mut file = match tokio::fs::File::open(&path).await {
        Ok(f) => f,
        Err(e) => {
            write_frame(writer, &Response::Error(format!("无法打开日志文件: {e}"))).await?;
            return Ok(());
        }
    };
    let mut offset = file.metadata().await.map(|m| m.len()).unwrap_or(0);
    let _ = file.seek(std::io::SeekFrom::Start(offset)).await;
    let mut buf = vec![0u8; 8192];

    loop {
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;

        let path_len = tokio::fs::metadata(&path).await.map(|m| m.len()).unwrap_or(0);
        if path_len < offset {
            // 文件被截断/轮转：先把旧文件 descriptor 读到 EOF 以防数据丢失
            let mut remaining = Vec::new();
            loop {
                match file.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => remaining.extend_from_slice(&buf[..n]),
                    Err(_) => break,
                }
            }
            if !remaining.is_empty() {
                let text = String::from_utf8_lossy(&remaining);
                let fresh: Vec<String> = text.lines().map(|s| s.to_string()).collect();
                if !fresh.is_empty() && write_frame(writer, &Response::LogChunk(fresh)).await.is_err() {
                    break;
                }
            }

            // 重新打开新文件，重置 offset
            if let Ok(new_file) = tokio::fs::File::open(&path).await {
                file = new_file;
                offset = 0;
            } else {
                break;
            }
        }

        let metadata = match file.metadata().await {
            Ok(m) => m,
            Err(_) => break,
        };
        let len = metadata.len();

        if len > offset {
            let mut read_bytes = Vec::new();
            let _ = file.seek(std::io::SeekFrom::Start(offset)).await;
            loop {
                match file.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => read_bytes.extend_from_slice(&buf[..n]),
                    Err(_) => break,
                }
            }
            if !read_bytes.is_empty() {
                let text = String::from_utf8_lossy(&read_bytes);
                let fresh: Vec<String> = text.lines().map(|s| s.to_string()).collect();
                if !fresh.is_empty() && write_frame(writer, &Response::LogChunk(fresh)).await.is_err() {
                    break;
                }
            }
            offset = len;
        }
    }
    Ok(())
}

/// 等待进程就绪并流式回报（见 7.14）。就绪优先级：health_check > TCP port > 最小存活时长。
async fn wait_ready<W>(
    mgr: &ManagerHandle,
    info: crate::process::entry::ProcessInfo,
    hc: Option<crate::process::entry::HealthCheckConfig>,
    timeout_secs: u64,
    writer: &mut W,
) -> Result<(), OwlError>
where
    W: AsyncWrite + Unpin,
{
    use crate::process::entry::ProcessStatus;
    use crate::process::health;
    use std::time::{Duration, Instant};

    let id = info.id;
    let deadline = Instant::now() + Duration::from_secs(timeout_secs.max(1));
    let probe_timeout = Duration::from_secs(2);

    write_frame(writer, &Response::Progress(format!("等待 [{id}] {} 就绪…", info.name))).await?;

    loop {
        if Instant::now() >= deadline {
            write_frame(
                writer,
                &Response::Error(format!("等待就绪超时（{timeout_secs}s）")),
            )
            .await?;
            return Ok(());
        }

        match mgr.info(id.to_string()).await {
            Ok(ci) => match ci.status {
                ProcessStatus::Online => {
                    let ready = if let Some(hc) = &hc {
                        health::probe(hc, ci.port).await.unwrap_or(false)
                    } else if let Some(port) = ci.port {
                        health::tcp_probe("127.0.0.1", port, probe_timeout).await
                    } else {
                        ci.uptime_secs >= 1
                    };
                    if ready {
                        write_frame(writer, &Response::Ready(ci)).await?;
                        return Ok(());
                    }
                }
                ProcessStatus::Errored | ProcessStatus::Stopped => {
                    write_frame(
                        writer,
                        &Response::Error("进程在就绪前已退出".into()),
                    )
                    .await?;
                    return Ok(());
                }
                _ => {}
            },
            Err(_) => {
                write_frame(writer, &Response::Error("进程不存在".into())).await?;
                return Ok(());
            }
        }

        write_frame(writer, &Response::Progress(".".into())).await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// 无停机滚动重启：按实例顺序依次重启 target 进程组中的实例，逐个等待就绪。
async fn reload_rolling<W>(
    mgr: &ManagerHandle,
    target: &str,
    writer: &mut W,
) -> Result<(), OwlError>
where
    W: AsyncWrite + Unpin,
{
    use std::time::Duration;

    // 取实例 id 列表（按 instance_index 排序）。
    let instances = mgr
        .instance_ids(target.to_string())
        .await
        .map_err(|e| OwlError::Other(e.to_string()))?;
    if instances.is_empty() {
        write_frame(
            writer,
            &Response::Error(format!("未找到进程组: {target}")),
        )
        .await?;
        return Ok(());
    }

    write_frame(
        writer,
        &Response::Progress(format!("rolling reload {target}…")),
    )
    .await?;

    for (id, _port) in instances {
        // 记录当前信息用于 wait_ready 中的 id/name。
        let info = mgr
            .info(id.to_string())
            .await
            .map_err(|e| OwlError::Other(e.to_string()))?;

        write_frame(
            writer,
            &Response::Progress(format!(
                "\n- reload [{}] {} (instance #{})",
                info.id, info.name, info.instance_index
            )),
        )
        .await?;

        // 触发重启。
        mgr.restart(id.to_string())
            .await
            .map_err(|e| OwlError::Other(e.to_string()))?;

        wait_instance_online(mgr, info.id, 60, writer).await?;
    }

    write_frame(
        writer,
        &Response::Ok(format!("reload {target} 完成")),
    )
    .await?;
    // 小睡一会儿让输出 flush。
    tokio::time::sleep(Duration::from_millis(10)).await;
    Ok(())
}

/// reload 专用等待：容忍 Stopping/Stopped/Launching 过渡，仅在超时或 Errored 失败。
async fn wait_instance_online<W>(
    mgr: &ManagerHandle,
    id: u32,
    timeout_secs: u64,
    writer: &mut W,
) -> Result<(), OwlError>
where
    W: AsyncWrite + Unpin,
{
    use crate::process::entry::ProcessStatus;
    use std::time::{Duration, Instant};

    let deadline = Instant::now() + Duration::from_secs(timeout_secs.max(1));
    loop {
        if Instant::now() >= deadline {
            write_frame(
                writer,
                &Response::Error(format!("reload 等待实例 {id} 就绪超时（{timeout_secs}s）")),
            )
            .await?;
            return Ok(());
        }
        match mgr.info(id.to_string()).await {
            Ok(info) => match info.status {
                ProcessStatus::Online => {
                    write_frame(
                        writer,
                        &Response::Progress(format!(" -> ok [{}]", id)),
                    )
                    .await?;
                    return Ok(());
                }
                ProcessStatus::Errored => {
                    write_frame(
                        writer,
                        &Response::Error(format!("实例 {id} 在 reload 期间进入 errored")),
                    )
                    .await?;
                    return Ok(());
                }
                _ => {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                }
            },
            Err(e) => {
                write_frame(writer, &Response::Error(e.to_string())).await?;
                return Ok(());
            }
        }
    }
}


