//! 子进程 stdout/stderr 异步管道采集与文件写入，及日志尾部读取。

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use chrono::Local;
use flate2::write::GzEncoder;
use flate2::Compression;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;

/// stderr 行前缀标记，便于 `owl logs` 区分/着色。
const ERR_TAG: &str = "[err] ";
const MAX_LOG_BYTES: u64 = 10 * 1024 * 1024;
const MAX_BACKUPS: usize = 5;

/// 启动单个应用的日志 writer。stdout/stderr collector 都通过该 channel 写入，
/// 因而一个日志文件始终只有一个轮转所有者。
pub fn spawn_writer(path: PathBuf) -> mpsc::Sender<Vec<u8>> {
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(1024);
    tokio::spawn(async move {
        let mut file = match open_log_file(&path).await {
            Ok(f) => f,
            Err(e) => {
                owl_logger::error!("无法打开日志文件 {}: {e}", path.display());
                return;
            }
        };
        let mut current_day = day_stamp();

        while let Some(buf) = rx.recv().await {
            // 磁盘满等写错误时停止 writer；sender 会随 channel 关闭而退出，避免
            // 在内存中无限积压日志。
            if let Err(e) = file.write_all(&buf).await {
                owl_logger::warn!("写日志失败({}): {e}", path.display());
                break;
            }
            rotate_if_needed(&path, &mut file, &mut current_day).await;
        }
        let _ = file.flush().await;
    });
    tx
}

/// 启动一个采集任务：逐行读取 `reader`，发送给同一应用唯一的日志 writer。
///
/// `is_err` 为真时给每行加 `[err]` 前缀。有界 channel 让写入慢时向子进程的
/// pipe 施加自然背压，而不是无界占用 daemon 内存。
pub fn spawn_collector<R>(reader: R, tx: mpsc::Sender<Vec<u8>>, is_err: bool)
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
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
                    if tx.send(buf.into_bytes()).await.is_err() {
                        break;
                    }
                }
                Ok(None) => break, // EOF：管道关闭
                Err(e) => {
                    owl_logger::warn!("读取子进程输出失败: {e}");
                    break;
                }
            }
        }
    });
}

async fn open_log_file(path: &Path) -> std::io::Result<tokio::fs::File> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).await;
    }
    Ok(file)
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
    if let Ok(newf) = open_log_file(path).await {
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
    if let Ok(newf) = open_log_file(path).await {
        let old = std::mem::replace(file, newf);
        drop(old);
    }
    let _ = gzip_file(archived).await;
}

async fn gzip_file(path: PathBuf) -> Result<(), String> {
    tokio::task::spawn_blocking(move || {
        let mut input = std::fs::File::open(&path)
            .map_err(|e| format!("打开归档失败({}): {e}", path.display()))?;
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
    if n == 0 {
        return Vec::new();
    }
    let base = path.to_path_buf();
    let all = tokio::task::spawn_blocking(move || read_last_lines_sync(&base, n))
        .await
        .unwrap_or_default();
    all
}

fn read_last_lines_plain_backwards(path: &Path, limit: usize) -> std::io::Result<Vec<String>> {
    let mut file = std::fs::File::open(path)?;
    let file_len = file.seek(SeekFrom::End(0))?;
    if file_len == 0 {
        return Ok(Vec::new());
    }

    let chunk_size = 4096;
    let mut pos = file_len;
    let mut newlines_found = 0;
    let mut buffer = vec![0u8; chunk_size];
    let mut ignore_last_newline = true;

    while pos > 0 && newlines_found <= limit {
        let read_size = std::cmp::min(pos, chunk_size as u64) as usize;
        pos -= read_size as u64;
        file.seek(SeekFrom::Start(pos))?;
        file.read_exact(&mut buffer[..read_size])?;

        for i in (0..read_size).rev() {
            let byte = buffer[i];
            if byte == b'\n' {
                if ignore_last_newline && pos + i as u64 == file_len - 1 {
                    ignore_last_newline = false;
                    continue;
                }
                newlines_found += 1;
                if newlines_found > limit {
                    pos = pos + i as u64 + 1;
                    break;
                }
            }
        }
    }

    if newlines_found <= limit {
        pos = 0;
    }

    file.seek(SeekFrom::Start(pos))?;
    let mut content = Vec::new();
    file.read_to_end(&mut content)?;

    let text = String::from_utf8_lossy(&content);
    let mut lines: Vec<String> = text.lines().map(|s| s.to_string()).collect();
    if lines.len() > limit {
        let start = lines.len().saturating_sub(limit);
        lines = lines[start..].to_vec();
    }
    Ok(lines)
}

fn read_last_lines_from_single_file(path: &Path, limit: usize) -> std::io::Result<Vec<String>> {
    let is_gz = path
        .extension()
        .and_then(|s| s.to_str())
        .map(|s| s.eq_ignore_ascii_case("gz"))
        .unwrap_or(false);
    if is_gz {
        let content = read_log_content(path).map_err(std::io::Error::other)?;
        let lines: Vec<String> = content.lines().map(|s| s.to_string()).collect();
        let start = lines.len().saturating_sub(limit);
        return Ok(lines[start..].to_vec());
    }
    read_last_lines_plain_backwards(path, limit)
}

fn read_last_lines_sync(path: &Path, n: usize) -> Vec<String> {
    let files = collect_related_logs(path);
    if files.is_empty() {
        return Vec::new();
    }
    let mut collected_lines = Vec::new();
    let mut needed = n;
    for p in files.iter().rev() {
        if needed == 0 {
            break;
        }
        let lines_from_file = match read_last_lines_from_single_file(p, needed) {
            Ok(ls) => ls,
            Err(e) => {
                owl_logger::warn!("无法读取日志文件末尾 {}: {e}", p.display());
                continue;
            }
        };
        needed = needed.saturating_sub(lines_from_file.len());
        collected_lines = [lines_from_file, collected_lines].concat();
    }
    collected_lines
}

/// 收集与当前日志相关的文件：当前日志 + 轮转文件 + 日期归档(.gz)，按修改时间升序。
fn collect_related_logs(path: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let parent = match path.parent() {
        Some(p) => p,
        None => return files,
    };
    let base = match path.file_name().and_then(|s| s.to_str()) {
        Some(b) => b.to_string(),
        None => return files,
    };
    let mut candidates: Vec<(std::time::SystemTime, PathBuf)> = Vec::new();
    if path.exists() {
        let ts = path
            .metadata()
            .and_then(|m| m.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        candidates.push((ts, path.to_path_buf()));
    }
    if let Ok(rd) = std::fs::read_dir(parent) {
        for ent in rd.flatten() {
            let p = ent.path();
            if p == path {
                continue;
            }
            let name = match p.file_name().and_then(|s| s.to_str()) {
                Some(n) => n,
                None => continue,
            };
            if !name.starts_with(&base) {
                continue;
            }
            // 相关后缀示例：.1 / .2 / .YYYY-MM-DD.log / .YYYY-MM-DD.log.gz
            let ts = p
                .metadata()
                .and_then(|m| m.modified())
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
            candidates.push((ts, p));
        }
    }
    candidates.sort_by_key(|(ts, _)| *ts);
    files.extend(candidates.into_iter().map(|(_, p)| p));
    files
}

fn read_log_content(path: &Path) -> Result<String, String> {
    let is_gz = path
        .extension()
        .and_then(|s| s.to_str())
        .map(|s| s.eq_ignore_ascii_case("gz"))
        .unwrap_or(false);
    if is_gz {
        let f = std::fs::File::open(path)
            .map_err(|e| format!("打开 gzip 日志失败({}): {e}", path.display()))?;
        let mut d = flate2::read::GzDecoder::new(f);
        let mut out = String::new();
        d.read_to_string(&mut out)
            .map_err(|e| format!("解压 gzip 日志失败({}): {e}", path.display()))?;
        Ok(out)
    } else {
        std::fs::read_to_string(path).map_err(|e| format!("读取日志失败({}): {e}", path.display()))
    }
}
