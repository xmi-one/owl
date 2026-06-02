//! `state.json` 持久化：带 schema 版本，原子写入（temp + rename）。

use serde::{Deserialize, Serialize};

use crate::common::errors::Result;
use crate::common::paths;
use crate::process::entry::PersistedApp;

pub const SCHEMA_VERSION: u32 = 1;

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
    /// 从磁盘加载；文件不存在或损坏时返回默认值（并尽量不丢失数据）。
    pub fn load() -> Self {
        let path = paths::state_path();
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(_) => return StateFile::default(),
        };
        match serde_json::from_slice::<StateFile>(&bytes) {
            Ok(mut sf) => {
                if sf.schema_version > SCHEMA_VERSION {
                    // 高于当前实现：保守起见仍加载，但调用方可据此提示升级。
                }
                if sf.schema_version < SCHEMA_VERSION {
                    sf.schema_version = SCHEMA_VERSION;
                }
                sf
            }
            Err(_) => StateFile::default(),
        }
    }

    /// 原子写入：写临时文件后 rename 替换。
    pub fn save(&self) -> Result<()> {
        paths::ensure_dirs()?;
        let path = paths::state_path();
        let tmp = path.with_extension("json.tmp");
        let bytes = serde_json::to_vec_pretty(self)?;
        std::fs::write(&tmp, &bytes)?;
        std::fs::rename(&tmp, &path)?;
        Ok(())
    }
}
