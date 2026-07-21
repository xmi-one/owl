//! `state.json` 持久化：带 schema 版本，原子且尽量持久化写入。

use std::io::Write;

use serde::{Deserialize, Serialize};

use crate::common::errors::{OwlError, Result};
use crate::common::paths;
use crate::process::entry::PersistedApp;

pub const SCHEMA_VERSION: u32 = 2;

/// `state.json` 顶层结构。
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StateFile {
    pub schema_version: u32,
    pub next_id: u32,
    pub apps: Vec<PersistedApp>,
}

impl Default for StateFile {
    fn default() -> Self {
        StateFile {
            schema_version: SCHEMA_VERSION,
            next_id: 0,
            apps: Vec::new(),
        }
    }
}

impl StateFile {
    /// 从磁盘加载。文件不存在时返回默认值；不可读、损坏或版本过新时拒绝启动，
    /// 防止之后的持久化把仍可恢复的 state.json 覆盖为空状态。
    pub fn load() -> Result<Self> {
        let path = paths::state_path();
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(StateFile::default()),
            Err(e) => return Err(e.into()),
        };
        match serde_json::from_slice::<StateFile>(&bytes) {
            Ok(mut sf) => {
                if sf.schema_version > SCHEMA_VERSION {
                    return Err(OwlError::Other(format!(
                        "state.json schema_version={} 高于当前支持的 {}；请升级 Owl 或恢复兼容备份",
                        sf.schema_version, SCHEMA_VERSION
                    )));
                }
                if sf.schema_version < SCHEMA_VERSION {
                    for app in &mut sf.apps {
                        if app.port_base.is_some() && app.port_max.is_none() {
                            // v1 没有范围上界；保持其“基准端口可向上分配”的语义。
                            app.port_max = Some(u16::MAX);
                        }
                    }
                    sf.schema_version = SCHEMA_VERSION;
                }
                Ok(sf)
            }
            Err(e) => Err(OwlError::Other(format!(
                "无法解析 {}: {e}。为避免覆盖现有状态，Daemon 未启动；请修复或从备份恢复该文件",
                path.display()
            ))),
        }
    }

    /// 原子写入：专属临时文件写入并 fsync，再 rename 替换与同步目录。
    pub fn save(&self) -> Result<()> {
        paths::ensure_dirs()?;
        let path = paths::state_path();
        let parent = path
            .parent()
            .ok_or_else(|| OwlError::Other("state.json 没有父目录".into()))?;
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| OwlError::Other(format!("系统时间异常: {e}")))?
            .as_nanos();
        let tmp = parent.join(format!(".state.{}.{}.tmp", std::process::id(), nonce));
        let bytes = serde_json::to_vec_pretty(self)?;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&tmp, &path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
            std::fs::File::open(parent)?.sync_all()?;
        }
        Ok(())
    }
}
