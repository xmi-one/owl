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
        match read_frame::<_, Response>(&mut reader).await? {
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
