# Changelog

All notable changes to this project are documented in this file.

## [Unreleased]

### Added

- `owl monit` 升级为交互式 TUI 面板（`ratatui + crossterm`），保留 `--json/--count` 脚本模式。
- `owl save` / `owl resurrect` 快照保存与恢复（默认 `$OWL_HOME/saved.json`）。
- `owl service generate systemd|launchd` 服务单元模板生成。
- 内置 HTTP API + WebSocket（默认 `127.0.0.1:8757`，支持 `OWL_API_TOKEN` 鉴权）。
- PM2 `ecosystem.config.json` 兼容解析并接入 `owl apply` 自动识别。

### Changed

- `owl logs -n` 支持跨当前日志、轮转文件与 `.gz` 日期归档聚合读取尾部。
- 子进程日志补齐按日期切分与 gzip 归档能力。

### Docs

- README 补充多实例/`scale`/`reload`、`apply`、日志归档尾读、服务生成、HTTP API 等说明。

## Milestone commits

- `7cab90c` feat: 实现 Owl 进程管理器 Phase 1 (MVP)
- `b66e795` feat(phase2): 资源监控 + 健康检查 + --wait-ready
- `77f9d03` feat(phase2): owl.toml 声明式配置 + owl apply 收敛
- `eac436f` feat(phase2): 完成多实例编排、日志轮转与 shell 补全
- `7049512` feat(phase3): 增加内置 HTTP API 与 WebSocket 推送
- `c7ceed7` feat(phase3): 增加 systemd/launchd 服务模板生成
- `a5d74eb` feat(phase3): 增加 save/resurrect 快照恢复能力
- `732d2dc` feat(phase3): 增加 monit 实时监控命令
- `1184e85` feat(phase2): 支持 PM2 ecosystem.config.json 兼容解析
- `60428ef` feat(phase2): 补齐日志按日期切分与 gzip 归档
- `2106dcb` feat(logs): 支持跨轮转与 gzip 归档读取尾部日志
- `d561550` feat(monit): 升级为交互式 TUI 面板
