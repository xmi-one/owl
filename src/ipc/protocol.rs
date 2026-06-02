//! 长度前缀帧编解码：4 字节大端序长度 + JSON 负载 (UTF-8)。

use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::common::errors::OwlError;

/// 单帧最大长度（防御性上限，16 MiB）。
const MAX_FRAME_LEN: u32 = 16 * 1024 * 1024;

/// 写入一帧：先写 4 字节大端长度，再写 JSON 负载。
pub async fn write_frame<W, T>(writer: &mut W, value: &T) -> Result<(), OwlError>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let payload = serde_json::to_vec(value)?;
    if payload.len() as u64 > MAX_FRAME_LEN as u64 {
        return Err(OwlError::Protocol(format!(
            "帧过大: {} bytes",
            payload.len()
        )));
    }
    writer.write_u32(payload.len() as u32).await?;
    writer.write_all(&payload).await?;
    writer.flush().await?;
    Ok(())
}

/// 读取一帧并反序列化。EOF 在读取长度阶段返回 `Ok(None)`。
pub async fn read_frame<R, T>(reader: &mut R) -> Result<Option<T>, OwlError>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let len = match reader.read_u32().await {
        Ok(len) => len,
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    if len > MAX_FRAME_LEN {
        return Err(OwlError::Protocol(format!("声明的帧长度过大: {len}")));
    }
    let mut buf = vec![0u8; len as usize];
    reader.read_exact(&mut buf).await?;
    let value = serde_json::from_slice(&buf)?;
    Ok(Some(value))
}
