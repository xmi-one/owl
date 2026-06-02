//! Daemon 自身日志：基于 owl-logger（轮转/压缩/清理/panic 捕获/动态调级）。

use crate::common::paths;

/// 初始化 Daemon 日志，返回需保活的 guard（drop 时自动 flush）。
///
/// 返回 `Box<dyn Any>` 以避免在调用处命名 owl-logger 的 guard 具体类型；
/// daemon 主流程持有它直到退出。
pub fn init() -> Box<dyn std::any::Any> {
    let _ = paths::ensure_dirs();
    let log_dir = paths::logs_dir().to_string_lossy().to_string();

    let guard = owl_logger::builder()
        .file_name("owl-daemon")
        .log_dir(log_dir)
        .error_file(owl_logger::LogLevel::Error)
        .init();

    Box::new(guard)
}

/// 运行时动态调日志级别（对接 `owl log-level`）。
pub fn set_level(level: &str) -> Result<(), String> {
    let lvl = match level.to_ascii_lowercase().as_str() {
        "trace" => owl_logger::LogLevel::Trace,
        "debug" => owl_logger::LogLevel::Debug,
        "info" => owl_logger::LogLevel::Info,
        "warn" => owl_logger::LogLevel::Warn,
        "error" => owl_logger::LogLevel::Error,
        other => return Err(format!("未知日志级别: {other}")),
    };
    owl_logger::set_level(lvl).map_err(|e| e.to_string())
}
