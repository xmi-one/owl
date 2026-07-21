//! 声明式配置：`owl.toml` 解析为一组 `StartOptions`。

pub mod ecosystem;
pub mod owl_config;

pub use owl_config::parse_size;

pub fn load_file_auto(
    path: &str,
) -> crate::common::errors::Result<Vec<crate::ipc::message::StartOptions>> {
    if path.ends_with(".json") {
        ecosystem::load_file(path)
    } else {
        owl_config::load_file(path)
    }
}
