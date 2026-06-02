//! 声明式配置：`owl.toml` 解析为一组 `StartOptions`。

pub mod owl_config;

pub use owl_config::{load_file, parse_size};
