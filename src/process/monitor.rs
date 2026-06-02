//! 资源指标采集（CPU% / 内存）。
//!
//! 策略（见方案 9.2/9.3）：
//! - **仅**对受管 PID 做定向刷新（`refresh_processes_specifics` + cpu/memory），
//!   绝不 `refresh_all`（后者扫全进程/磁盘/网络，分配密集）。
//! - CPU% 由两次采样差值得出（多核累加，与 `top` 一致）。
//! - 懒监控：后台慢 tick 仅为配了 `max_memory` 的进程做阈值检查；
//!   `owl list/info` 触发一次即时采样满足展示。

use std::collections::HashMap;

use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

/// 单次采样得到的指标。
#[derive(Debug, Clone, Copy, Default)]
pub struct Sample {
    pub cpu_percent: f32,
    pub memory_bytes: u64,
}

/// 持有一个 `System` 实例以保留上次 CPU 时间片，支撑差值计算。
pub struct Monitor {
    sys: System,
}

impl Monitor {
    pub fn new() -> Self {
        Monitor {
            sys: System::new(),
        }
    }

    /// 对给定 PID 集合做一次定向刷新并返回指标。空集合直接返回空。
    pub fn sample(&mut self, pids: &[u32]) -> HashMap<u32, Sample> {
        let mut out = HashMap::new();
        if pids.is_empty() {
            return out;
        }
        let want: Vec<Pid> = pids.iter().map(|p| Pid::from_u32(*p)).collect();
        self.sys.refresh_processes_specifics(
            ProcessesToUpdate::Some(&want),
            true,
            ProcessRefreshKind::nothing().with_cpu().with_memory(),
        );
        for pid in pids {
            if let Some(proc_) = self.sys.process(Pid::from_u32(*pid)) {
                out.insert(
                    *pid,
                    Sample {
                        cpu_percent: proc_.cpu_usage(),
                        memory_bytes: proc_.memory(),
                    },
                );
            }
        }
        out
    }
}

impl Default for Monitor {
    fn default() -> Self {
        Self::new()
    }
}
