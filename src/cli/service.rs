//! 生成 systemd / launchd 服务模板。

use std::path::Path;

use crate::common::paths;

pub fn render_systemd(name: &str) -> Result<String, String> {
    let exe = std::env::current_exe()
        .map_err(|e| format!("无法获取 owl 可执行路径: {e}"))?
        .display()
        .to_string();
    let owl_home = paths::owl_home().display().to_string();
    Ok(format!(
        "[Unit]
Description=Owl Process Manager Daemon
After=network.target

[Service]
Type=simple
ExecStart={exe} daemon
Restart=always
RestartSec=2
Environment=OWL_HOME={owl_home}
WorkingDirectory={owl_home}

[Install]
WantedBy=multi-user.target
# 保存建议: /etc/systemd/system/{name}.service
"
    ))
}

pub fn render_launchd(name: &str) -> Result<String, String> {
    let exe = std::env::current_exe()
        .map_err(|e| format!("无法获取 owl 可执行路径: {e}"))?
        .display()
        .to_string();
    let owl_home = paths::owl_home().display().to_string();
    Ok(format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.{name}.daemon</string>

  <key>ProgramArguments</key>
  <array>
    <string>{exe}</string>
    <string>daemon</string>
  </array>

  <key>EnvironmentVariables</key>
  <dict>
    <key>OWL_HOME</key>
    <string>{owl_home}</string>
  </dict>

  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>

  <key>WorkingDirectory</key>
  <string>{owl_home}</string>

  <!-- 保存建议: ~/Library/LaunchAgents/com.{name}.daemon.plist -->
</dict>
</plist>
"#
    ))
}

pub fn write_or_print(content: &str, output: Option<&str>) -> Result<String, String> {
    match output {
        None => {
            println!("{content}");
            Ok("已输出到 stdout".into())
        }
        Some(path) => {
            let p = Path::new(path);
            if let Some(parent) = p.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| format!("创建目录失败({}): {e}", parent.display()))?;
                }
            }
            std::fs::write(p, content).map_err(|e| format!("写文件失败({path}): {e}"))?;
            Ok(format!("已写入: {path}"))
        }
    }
}
