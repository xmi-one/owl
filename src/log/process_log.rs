//! 子进程 stdout/stderr 异步管道采集与文件写入，及日志尾部读取。

use std::path::{Path, PathBuf};

use tokio::fs::OpenOptions;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader};

/// stderr 行前缀标记，便于 `owl logs` 区分/着色。
const ERR_TAG: &str = "[err] ";

/// 启动一个采集任务：逐行读取 `reader`，以 O_APPEND 追加到合并日志文件。
///
/// `is_err` 为真时给每行加 `[err]` 前缀。使用 O_APPEND + 整行写入，
/// 让 stdout/stderr 两个采集任务可安全写同一文件而不交错（POSIX append 原子性）。
pub fn spawn_collector<R>(reader: R, path: PathBuf, is_err: bool)
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut file = match OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
        {
            Ok(f) => f,
            Err(e) => {
                owl_logger::error!("无法打开日志文件 {}: {e}", path.display());
                return;
            }
        };

        let mut lines = BufReader::new(reader).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    let mut buf = String::with_capacity(line.len() + ERR_TAG.len() + 1);
                    if is_err {
                        buf.push_str(ERR_TAG);
                    }
                    buf.push_str(&line);
                    buf.push('\n');
                    // 磁盘满等写错误时降级：记一次告警并停写，绝不让 daemon 崩。
                    if let Err(e) = file.write_all(buf.as_bytes()).await {
                        owl_logger::warn!("写日志失败({}): {e}", path.display());
                        break;
                    }
                }
                Ok(None) => break, // EOF：管道关闭
                Err(e) => {
                    owl_logger::warn!("读取子进程输出失败({}): {e}", path.display());
                    break;
                }
            }
        }
        let _ = file.flush().await;
    });
}

/// 读取日志文件末尾 `n` 行（MVP：整文件读取，后续按 seek 优化）。
pub async fn read_last_lines(path: &Path, n: usize) -> Vec<String> {
    let content = match tokio::fs::read_to_string(path).await {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };
    let all: Vec<&str> = content.lines().collect();
    let start = all.len().saturating_sub(n);
    all[start..].iter().map(|s| s.to_string()).collect()
}

/// 当前文件字节长度（用于 follow 增量读取）。
pub async fn file_len(path: &Path) -> u64 {
    tokio::fs::metadata(path)
        .await
        .map(|m| m.len())
        .unwrap_or(0)
}
