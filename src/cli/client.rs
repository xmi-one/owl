//! UDS 客户端：连接 Daemon、握手、发送请求、接收（可能多帧）响应。

use tokio::io::BufReader;
use tokio::net::UnixStream;

use crate::common::errors::{OwlError, Result};
use crate::common::paths;
use crate::ipc::message::{Handshake, Request, Response};
use crate::ipc::protocol::{read_frame, write_frame};

pub struct Client {
    reader: BufReader<tokio::net::unix::OwnedReadHalf>,
    writer: tokio::net::unix::OwnedWriteHalf,
}

impl Client {
    /// 连接并完成握手。
    pub async fn connect() -> Result<Self> {
        let stream = UnixStream::connect(paths::socket_path())
            .await
            .map_err(|_| OwlError::Other("无法连接 Daemon（未运行？）".into()))?;
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        write_frame(&mut write_half, &Handshake::current()).await?;
        let read_fut = read_frame::<_, Response>(&mut reader);
        let resp = match tokio::time::timeout(std::time::Duration::from_secs(3), read_fut).await {
            Ok(res) => res?,
            Err(_) => return Err(OwlError::Other("等待 Daemon 握手响应超时".into())),
        };
        match resp {
            Some(Response::Ok(_)) => {}
            Some(Response::VersionMismatch { daemon_version }) => {
                return Err(OwlError::Protocol(format!(
                    "协议版本不匹配（Daemon={daemon_version}）。请执行 `owl kill` 后重试以重启 Daemon。"
                )));
            }
            Some(other) => {
                return Err(OwlError::Protocol(format!("握手异常响应: {other:?}")));
            }
            None => return Err(OwlError::Protocol("握手时连接被关闭".into())),
        }
        Ok(Client {
            reader,
            writer: write_half,
        })
    }

    pub async fn send(&mut self, req: &Request) -> Result<()> {
        write_frame(&mut self.writer, req).await
    }

    pub async fn recv(&mut self) -> Result<Option<Response>> {
        read_frame(&mut self.reader).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ipc::message::{Handshake, Response};
    use crate::ipc::protocol::{read_frame, write_frame};
    use tokio::net::UnixListener;

    static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn get_test_owl_home() -> std::path::PathBuf {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!("test_owl_home_{}", now));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[tokio::test]
    async fn test_client_handshake_success() {
        let _guard = TEST_LOCK.lock().await;

        let test_dir = get_test_owl_home();
        std::env::set_var("OWL_HOME", &test_dir);
        let socket_path = paths::socket_path();

        let listener = UnixListener::bind(&socket_path).unwrap();
        let server_handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read_half, mut write_half) = stream.into_split();
            let mut reader = BufReader::new(read_half);

            let hs = read_frame::<_, Handshake>(&mut reader)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(hs.protocol_version, crate::ipc::message::PROTOCOL_VERSION);

            write_frame(&mut write_half, &Response::Ok("mock-server".into()))
                .await
                .unwrap();
        });

        let client = Client::connect().await;
        assert!(client.is_ok());

        server_handle.await.unwrap();
        let _ = std::fs::remove_dir_all(&test_dir);
    }

    #[tokio::test]
    async fn test_client_handshake_timeout() {
        let _guard = TEST_LOCK.lock().await;

        let test_dir = get_test_owl_home();
        std::env::set_var("OWL_HOME", &test_dir);
        let socket_path = paths::socket_path();

        let listener = UnixListener::bind(&socket_path).unwrap();
        let server_handle = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        });

        let start = std::time::Instant::now();
        let client = Client::connect().await;
        let elapsed = start.elapsed();

        assert!(client.is_err());
        let err_msg = match client {
            Err(e) => e.to_string(),
            _ => unreachable!(),
        };
        assert!(err_msg.contains("等待 Daemon 握手响应超时"));
        assert!(elapsed >= std::time::Duration::from_secs(3));
        assert!(elapsed < std::time::Duration::from_secs(5));

        server_handle.abort();
        let _ = std::fs::remove_dir_all(&test_dir);
    }
}
