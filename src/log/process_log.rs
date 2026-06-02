//! 子进程 stdout/stderr 异步管道采集与文件写入，及日志尾部读取。

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use chrono::Local;
use flate2::Compression;
use flate2::write::GzEncoder;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader};

/// stderr 行前缀标记，便于 `owl logs` 区分/着色。
const ERR_TAG: &str = "[err] ";
const MAX_LOG_BYTES: u64 = 10 * 1024 * 1024;
const MAX_BACKUPS: usize = 5;

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
        let mut current_day = day_stamp();

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
                    rotate_if_needed(&path, &mut file, &mut current_day).await;
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

async fn rotate_if_needed(path: &Path, file: &mut tokio::fs::File, current_day: &mut String) {
    // 1) 按日期切分：跨天时把昨日日志归档为 .YYYY-MM-DD.log.gz
    let today = day_stamp();
    if &today != current_day {
        rotate_by_date(path, file, current_day.clone()).await;
        *current_day = today;
        return;
    }

    // 2) 按大小滚动：保留 .1 ~ .N
    let len = match file.metadata().await {
        Ok(m) => m.len(),
        Err(_) => return,
    };
    if len < MAX_LOG_BYTES {
        return;
    }
    let _ = file.flush().await;
    // backup rollover: .4 -> .5, ... .1 -> .2, current -> .1
    for i in (1..=MAX_BACKUPS).rev() {
        let src = if i == 1 {
            path.to_path_buf()
        } else {
            PathBuf::from(format!("{}.{}", path.display(), i - 1))
        };
        let dst = PathBuf::from(format!("{}.{}", path.display(), i));
        if src.exists() {
            let _ = tokio::fs::rename(&src, &dst).await;
        }
    }
    if let Ok(newf) = OpenOptions::new().create(true).append(true).open(path).await {
        let old = std::mem::replace(file, newf);
        drop(old);
    }
}

fn day_stamp() -> String {
    Local::now().format("%Y-%m-%d").to_string()
}

async fn rotate_by_date(path: &Path, file: &mut tokio::fs::File, day: String) {
    let _ = file.flush().await;
    let archived = PathBuf::from(format!("{}.{}.log", path.display(), day));
    if tokio::fs::rename(path, &archived).await.is_err() {
        return;
    }
    if let Ok(newf) = OpenOptions::new().create(true).append(true).open(path).await {
        let old = std::mem::replace(file, newf);
        drop(old);
    }
    let _ = gzip_file(archived).await;
}

async fn gzip_file(path: PathBuf) -> Result<(), String> {
    tokio::task::spawn_blocking(move || {
        let mut input =
            std::fs::File::open(&path).map_err(|e| format!("打开归档失败({}): {e}", path.display()))?;
        let gz_path = PathBuf::from(format!("{}.gz", path.display()));
        let output = std::fs::File::create(&gz_path)
            .map_err(|e| format!("创建 gzip 失败({}): {e}", gz_path.display()))?;
        let mut encoder = GzEncoder::new(output, Compression::default());
        let mut buf = [0u8; 64 * 1024];
        loop {
            let n = input
                .read(&mut buf)
                .map_err(|e| format!("读取归档失败({}): {e}", path.display()))?;
            if n == 0 {
                break;
            }
            encoder
                .write_all(&buf[..n])
                .map_err(|e| format!("写入 gzip 失败({}): {e}", gz_path.display()))?;
        }
        encoder
            .finish()
            .map_err(|e| format!("完成 gzip 失败({}): {e}", gz_path.display()))?;
        std::fs::remove_file(&path)
            .map_err(|e| format!("删除原归档失败({}): {e}", path.display()))?;
        Ok::<(), String>(())
    })
    .await
    .map_err(|e| format!("gzip 任务失败: {e}"))?
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
