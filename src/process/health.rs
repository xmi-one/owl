//! 健康检查探针：脚本 / 裸 HTTP(GET) / TCP 端口。
//!
//! 不引入 `reqwest`（见 9.1）：HTTP 探针用裸 `TcpStream` 手写 `GET` 并读状态行。
//! 仅支持明文 `http://`；`https://` 需 TLS 栈，暂不支持（可归 Phase 3）。

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::process::entry::HealthCheckConfig;

/// 按配置执行一次探针。优先级：script > url(http) > 无（交由调用方回退到 TCP/uptime）。
/// 返回 `None` 表示该配置没有可执行的探针项。
pub async fn probe(cfg: &HealthCheckConfig, port: Option<u16>) -> Option<bool> {
    let timeout = Duration::from_secs(cfg.timeout_secs.max(1));
    if let Some(script) = &cfg.script {
        return Some(script_probe(script, timeout).await);
    }
    if let Some(url) = &cfg.url {
        let rendered = render_url(url, port);
        return Some(http_probe(&rendered, timeout).await);
    }
    None
}

/// TCP 端口可连接即视为就绪（用于无 health_check、仅有 port 的场景）。
pub async fn tcp_probe(host: &str, port: u16, timeout: Duration) -> bool {
    matches!(
        tokio::time::timeout(timeout, TcpStream::connect((host, port))).await,
        Ok(Ok(_))
    )
}

/// 把 `{port}` 占位符渲染为实际端口。
fn render_url(url: &str, port: Option<u16>) -> String {
    match port {
        Some(p) => url.replace("{port}", &p.to_string()),
        None => url.to_string(),
    }
}

async fn http_probe(url: &str, timeout: Duration) -> bool {
    let parsed = match parse_http_url(url) {
        Some(v) => v,
        None => return false,
    };
    let (host, port, path) = parsed;
    let fut = async {
        let mut stream = TcpStream::connect((host.as_str(), port)).await.ok()?;
        let host_header = if port == 80 {
            host.clone()
        } else {
            format!("{host}:{port}")
        };
        let req = format!(
            "GET {path} HTTP/1.0\r\nHost: {host_header}\r\nUser-Agent: owl\r\nConnection: close\r\n\r\n"
        );
        stream.write_all(req.as_bytes()).await.ok()?;
        let mut buf = Vec::with_capacity(256);
        let mut tmp = [0u8; 256];
        // 读到首个换行即可解析状态行。
        loop {
            let n = stream.read(&mut tmp).await.ok()?;
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n]);
            if buf.contains(&b'\n') || buf.len() > 1024 {
                break;
            }
        }
        Some(status_ok(&buf))
    };
    matches!(tokio::time::timeout(timeout, fut).await, Ok(Some(true)))
}

/// 解析状态行 `HTTP/1.x CODE ...`，2xx/3xx 为 true。
fn status_ok(buf: &[u8]) -> bool {
    let text = String::from_utf8_lossy(buf);
    let line = text.lines().next().unwrap_or("");
    let code = line.split_whitespace().nth(1).and_then(|c| c.parse::<u16>().ok());
    matches!(code, Some(c) if (200..400).contains(&c))
}

/// 解析 `http://host[:port][/path]` → (host, port, path)。
fn parse_http_url(url: &str) -> Option<(String, u16, String)> {
    let rest = url.strip_prefix("http://")?;
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let (mut host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse::<u16>().ok()?),
        None => (authority.to_string(), 80),
    };
    if host.is_empty() {
        return None;
    }
    if host.starts_with('[') && host.ends_with(']') {
        host = host[1..host.len() - 1].to_string();
    }
    Some((host, port, path.to_string()))
}

async fn script_probe(script: &str, timeout: Duration) -> bool {
    let mut cmd = tokio::process::Command::new("sh");
    cmd.arg("-c").arg(script);
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::null());
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(_) => return false,
    };
    match tokio::time::timeout(timeout, child.wait()).await {
        Ok(Ok(status)) => status.success(),
        _ => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_script_probe_success() {
        assert!(script_probe("exit 0", Duration::from_secs(2)).await);
    }

    #[tokio::test]
    async fn test_script_probe_failure() {
        assert!(!script_probe("exit 1", Duration::from_secs(2)).await);
    }

    #[tokio::test]
    async fn test_script_probe_timeout() {
        let start = std::time::Instant::now();
        let healthy = script_probe("sleep 5", Duration::from_secs(1)).await;
        assert!(!healthy);
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_secs(1));
        assert!(elapsed < Duration::from_secs(3));
    }
}

