# Release Preparation (建议 v0.3.0)

## 1) 版本建议

- 建议版本：`v0.3.0`
- 理由：包含 Phase 2 全量能力 + Phase 3 核心能力（API / service 生成 / save-resurrect / monit TUI），属于里程碑升级。

## 2) 发布前检查清单

- [ ] `cargo build --release`
- [ ] `cargo clippy`
- [ ] 基本生命周期：`start/list/stop/restart/delete/logs/kill`
- [ ] 声明式收敛：`owl apply owl.toml --dry-run` / `--prune`
- [ ] PM2 配置兼容：`owl apply ecosystem.config.json`
- [ ] HTTP API：`/api/health`、`/api/processes`、`/api/ws`
- [ ] 服务模板：`owl service generate systemd|launchd`
- [ ] 快照恢复：`owl save` / `owl resurrect`
- [ ] 监控面板：`owl monit`（TUI）+ `--json --count`
- [ ] 日志归档尾读：`owl logs -n` 跨 `.gz` 归档

## 3) GitHub Release 标题建议

`v0.3.0 - Phase 2 Complete + Phase 3 Core`

## 4) Release Notes 草案

### Highlights

- 完成 Phase 2：监控、健康检查、声明式收敛、多实例、日志轮转。
- 完成 Phase 3 核心：HTTP API + WebSocket、systemd/launchd 生成、save/resurrect、monit TUI。
- 支持 PM2 `ecosystem.config.json` 迁移路径。

### Breaking / Important Notes

- 平台策略仍为 Unix-first（Windows 命名管道支持未包含在本次版本中）。
- `reload` 在固定端口应用场景以稳定策略为主，不强推端口冲突风险方案。

### Quick Start

```bash
owl service generate systemd --output /etc/systemd/system/owl.service
owl apply owl.toml
owl monit
```
