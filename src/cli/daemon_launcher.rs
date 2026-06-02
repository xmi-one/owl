//! 检测并按需拉起后台 Daemon。单例由 flock 保证，并处理陈旧 socket。

use std::os::unix::io::AsRawFd;
use std::time::{Duration, Instant};

use crate::common::errors::{OwlError, Result};
use crate::common::paths;

const LAUNCH_TIMEOUT: Duration = Duration::from_secs(3);
const POLL: Duration = Duration::from_millis(100);

/// 确保 Daemon 在运行；若未运行则拉起并等待其就绪。
pub async fn ensure_daemon() -> Result<()> {
    if can_connect() {
        return Ok(());
    }
    paths::ensure_dirs()?;

    // flock 抢占单例锁。
    let lock_file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(paths::lock_path())?;
    let _guard = FlockGuard::acquire(&lock_file)?;

    // 双重检查：拿到锁期间可能已有人拉起。
    if can_connect() {
        return Ok(());
    }

    spawn_daemon()?;

    let start = Instant::now();
    while start.elapsed() < LAUNCH_TIMEOUT {
        tokio::time::sleep(POLL).await;
        if can_connect() {
            return Ok(());
        }
    }
    Err(OwlError::Other("Daemon 启动超时".into()))
}

fn can_connect() -> bool {
    std::os::unix::net::UnixStream::connect(paths::socket_path()).is_ok()
}

/// 以新会话（setsid）派生自身的 `daemon` 子命令，重定向标准流到 /dev/null。
fn spawn_daemon() -> Result<()> {
    let exe = std::env::current_exe()
        .map_err(|e| OwlError::Other(format!("无法获取当前可执行文件: {e}")))?;
    let mut cmd = std::process::Command::new(exe);
    cmd.arg("daemon");
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::null());

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        unsafe {
            cmd.pre_exec(|| {
                nix::unistd::setsid()
                    .map(|_| ())
                    .map_err(|e| std::io::Error::from_raw_os_error(e as i32))
            });
        }
    }

    cmd.spawn()
        .map_err(|e| OwlError::Other(format!("拉起 Daemon 失败: {e}")))?;
    Ok(())
}

/// 持有期间保持文件的独占 flock，drop 时释放。
struct FlockGuard<'a> {
    file: &'a std::fs::File,
}

impl<'a> FlockGuard<'a> {
    fn acquire(file: &'a std::fs::File) -> Result<Self> {
        let fd = file.as_raw_fd();
        let rc = unsafe { libc::flock(fd, libc::LOCK_EX) };
        if rc != 0 {
            return Err(OwlError::Io(std::io::Error::last_os_error()));
        }
        Ok(FlockGuard { file })
    }
}

impl Drop for FlockGuard<'_> {
    fn drop(&mut self) {
        let fd = self.file.as_raw_fd();
        unsafe {
            libc::flock(fd, libc::LOCK_UN);
        }
    }
}
