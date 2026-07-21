//! `~/.owl/` 路径管理。支持 `OWL_HOME` 环境变量覆盖（便于测试隔离）。

use std::path::PathBuf;

/// Owl 主目录：`$OWL_HOME` 或 `~/.owl`。
pub fn owl_home() -> PathBuf {
    if let Ok(dir) = std::env::var("OWL_HOME") {
        if !dir.is_empty() {
            return PathBuf::from(dir);
        }
    }
    let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
    home.join(".owl")
}

pub fn socket_path() -> PathBuf {
    owl_home().join("owl.sock")
}

pub fn pid_path() -> PathBuf {
    owl_home().join("daemon.pid")
}

pub fn lock_path() -> PathBuf {
    owl_home().join("owl.lock")
}

pub fn state_path() -> PathBuf {
    owl_home().join("state.json")
}

pub fn logs_dir() -> PathBuf {
    owl_home().join("logs")
}

/// 子进程合并日志文件（Phase 1：stdout/stderr 合并写入，stderr 行带标记）。
pub fn proc_log(name: &str, id: u32) -> PathBuf {
    logs_dir().join(format!("{name}-{id}.log"))
}

/// 确保所有必要目录存在。
pub fn ensure_dirs() -> std::io::Result<()> {
    std::fs::create_dir_all(owl_home())?;
    std::fs::create_dir_all(logs_dir())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(owl_home(), std::fs::Permissions::from_mode(0o700))?;
        std::fs::set_permissions(logs_dir(), std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}
