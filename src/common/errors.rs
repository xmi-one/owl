//! 统一错误类型。

use thiserror::Error;

#[derive(Error, Debug)]
pub enum OwlError {
    #[error("I/O 错误: {0}")]
    Io(#[from] std::io::Error),

    #[error("序列化错误: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("协议错误: {0}")]
    Protocol(String),

    #[error("未找到进程: {0}")]
    NotFound(String),

    #[error("进程已存在: {0}")]
    AlreadyExists(String),

    #[error("无效参数: {0}")]
    Invalid(String),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, OwlError>;
