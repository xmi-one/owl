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
            let resp = match mgr.start(*opts).await {
                Ok(info) => Response::ProcessDetail(info),
                Err(e) => Response::Error(e.to_string()),
            };
            write_frame(writer, &resp).await?;
            Ok(false)
        }
        Request::Stop { target } => reply_result(writer, mgr.stop(target).await).await,
        Request::Restart { target } => reply_result(writer, mgr.restart(target).await).await,
        Request::Delete { target } => reply_result(writer, mgr.delete(target).await).await,
        Request::Reset { target } => reply_result(writer, mgr.reset(target).await).await,
        Request::Flush { target } => reply_result(writer, mgr.flush(target).await).await,
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

    // follow：轮询文件增长，增量推送新行，直到客户端断开（写失败）。
    let mut offset = process_log::file_len(&path).await;
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        let len = process_log::file_len(&path).await;
        if len < offset {
            offset = 0; // 文件被截断/轮转，重置
        }
        if len > offset {
            if let Ok(content) = tokio::fs::read_to_string(&path).await {
                let fresh = tail_since(&content, offset);
                if !fresh.is_empty()
                    && write_frame(writer, &Response::LogChunk(fresh)).await.is_err()
                {
                    break;
                }
            }
            offset = len;
        }
    }
    Ok(())
}

/// 返回文件中字节偏移 `offset` 之后的完整行。
fn tail_since(content: &str, offset: u64) -> Vec<String> {
    let bytes = content.as_bytes();
    let start = (offset as usize).min(bytes.len());
    let slice = &content[start..];
    slice.lines().map(|s| s.to_string()).collect()
}
