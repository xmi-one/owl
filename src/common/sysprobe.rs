//! 系统探测与信号发送：PID 存活、启动时间校验、安全 kill。
//!
//! 安全红线（方案 B）：发任何信号前都必须用 `(pid, start_time)` 校验，
//! 防止 PID 复用后误杀无关进程。

use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

use std::cell::RefCell;

thread_local! {
    static SYSTEM: RefCell<System> = RefCell::new(System::new());
}

/// 返回指定 PID 的启动时间（Unix epoch 秒），进程不存在则 `None`。
pub fn pid_start_time(pid: u32) -> Option<u64> {
    SYSTEM.with(|sys| {
        let mut sys = sys.borrow_mut();
        let p = Pid::from_u32(pid);
        sys.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[p]),
            true,
            ProcessRefreshKind::nothing(),
        );
        sys.process(p).map(|proc_| proc_.start_time())
    })
}

/// PID 是否存活。
#[allow(dead_code)]
pub fn pid_alive(pid: u32) -> bool {
    pid_start_time(pid).is_some()
}

/// 校验 (pid, start_time) 是否仍指向同一个进程。
pub fn validate(pid: u32, expected_start: Option<u64>) -> bool {
    match pid_start_time(pid) {
        None => false,
        Some(actual) => match expected_start {
            None => true, // 无历史启动时间时退化为仅存活校验
            Some(exp) => actual == exp,
        },
    }
}

#[cfg(unix)]
mod imp {
    use super::validate;
    use nix::sys::signal::{kill, Signal};
    use nix::unistd::Pid as NixPid;

    /// 把信号名解析为 `Signal`，未知则回退 `SIGTERM`。
    pub fn parse_signal(name: Option<&str>) -> Signal {
        match name.map(|s| s.to_ascii_uppercase()) {
            None => Signal::SIGTERM,
            Some(s) => match s.trim_start_matches("SIG") {
                "TERM" => Signal::SIGTERM,
                "KILL" => Signal::SIGKILL,
                "INT" => Signal::SIGINT,
                "QUIT" => Signal::SIGQUIT,
                "HUP" => Signal::SIGHUP,
                "USR1" => Signal::SIGUSR1,
                "USR2" => Signal::SIGUSR2,
                _ => Signal::SIGTERM,
            },
        }
    }

    /// 安全发送信号：先校验 `(pid, start_time)`，不匹配则跳过（返回 false）。
    pub fn send_signal_validated(pid: u32, start_time: Option<u64>, sig: Signal) -> bool {
        if !validate(pid, start_time) {
            return false;
        }
        kill(NixPid::from_raw(pid as i32), sig).is_ok()
    }
}

#[cfg(unix)]
pub use imp::{parse_signal, send_signal_validated};

#[cfg(unix)]
pub use nix::sys::signal::Signal;
