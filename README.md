# Owl

高性能、语言无关的进程管理器（Rust 版 PM2）。Client-Daemon 架构，通过 Unix Domain Socket 通信，子进程在 Daemon 崩溃后仍可存活并被新 Daemon 重接管。

> 当前已覆盖 **Phase 1 + Phase 2 核心能力**：核心生命周期、多实例、资源监控、健康探针、`owl.toml`+`apply`、`scale`、`reload`、`--wait-ready`。

## 构建

```bash
cargo build --release
# 产物：target/release/owl（约 1.7MB，已 strip）
```

## 快速开始

```bash
# 启动进程（直接 exec，不经 shell；命令写在 -- 之后）
owl start --name web -- node server.js
owl start --name api --restart-strategy always -- ./api-server --port 8080
# 拉起已存在（已停止）进程（兼容 PM2 使用习惯）
owl start web
owl start 4

# 查看 / 详情
owl list                 # 别名：ls / ps
owl info web

# 日志（合并 stdout/stderr，stderr 行标 [err]）
owl logs web -n 50
owl logs web --follow

# 生命周期（target 可为 id / name / all）
owl restart web
owl stop all
owl delete web           # 别名：rm
owl reset web            # 清零重启计数
owl flush web            # 清空该进程日志
owl scale web 4          # 调整实例数
owl reload web           # 无停机滚动重启（逐实例）

# 终止 Daemon（在线子进程会脱离存活）
owl kill
```

首次执行任意命令会自动在后台拉起 Daemon（flock 保证单例）。

## 常用启动选项

| 选项 | 说明 |
| --- | --- |
| `--name <NAME>` | 进程名（缺省由命令派生） |
| `--cwd <DIR>` | 工作目录（默认：执行 `owl start` 时的当前目录） |
| `--env KEY=VALUE` | 注入环境变量（可重复） |
| `--restart-strategy <always\|on-failure\|never>` | 重启策略（默认 `on-failure`） |
| `--max-restarts <N>` | 窗口内最大连续崩溃次数，超过则 `errored` |
| `--restart-delay <MS>` | 固定重启间隔（设置后禁用指数退避） |
| `--kill-signal <SIG>` | 停止信号（默认 `SIGTERM`） |
| `--instances <N>` | 多实例启动（同名进程组） |
| `--port <P\|auto:START-END>` | 端口基准；实例 i 使用 `START+i` |
| `--health-url <URL>` | 健康探针 URL（支持 `{port}`） |
| `--health-script <CMD>` | 脚本健康探针（exit 0=健康） |
| `--wait-ready` | 启动后阻塞直到就绪 |
| `--ready-timeout <SEC>` | 就绪等待超时 |

全局：`--json`（机器可读输出）、`--no-color`。

## 多实例 / scale / reload

```bash
# 启动 3 实例，自动分配端口 5000/5001/5002
owl start --name web --instances 3 --port auto:5000-5100 -- node server.js

# 扩到 5 实例（新增实例按 instance_index 顺延）
owl scale web 5

# 缩到 2 实例（优先移除最大 instance_index）
owl scale web 2

# 无停机滚动重启：逐实例 restart -> wait-ready -> 下一实例
owl reload web
```

## 声明式配置与 apply

```bash
# 预览变更
owl apply owl.toml --dry-run

# 执行收敛（默认保留配置外进程）
owl apply owl.toml

# 同时删除配置外进程
owl apply owl.toml --prune

# PM2 ecosystem.config.json 兼容（常见字段子集）
owl apply ecosystem.config.json
```

## 日志说明

- 默认每进程写入 `~/.owl/logs/<name>-<id>.log`（合并 stdout/stderr，stderr 带 `[err]` 前缀）。
- 支持按大小轮转：超阈值后滚动为 `.1` 到 `.5` 备份文件。
- `owl logs -n` 会跨当前日志、轮转文件和 `.gz` 日期归档聚合读取尾部内容。

## Shell 补全

```bash
owl completions bash > /etc/bash_completion.d/owl
owl completions zsh > ~/.zfunc/_owl
owl completions fish > ~/.config/fish/completions/owl.fish
```

## 生成服务单元（Phase 3-2）

```bash
# systemd（输出到 stdout）
owl service generate systemd

# systemd（直接写文件）
owl service generate systemd --name owl \
  --output /etc/systemd/system/owl.service

# launchd（直接写文件）
owl service generate launchd --name owl \
  --output ~/Library/LaunchAgents/com.owl.daemon.plist
```

## 内置 HTTP API（Phase 3-1）

- 默认监听：`127.0.0.1:8757`
- 地址覆盖：环境变量 `OWL_API_ADDR`（例如 `127.0.0.1:9000`）
- 鉴权（可选）：环境变量 `OWL_API_TOKEN`，启用后需携带
  `Authorization: Bearer <token>`

示例：

```bash
# 健康检查
curl http://127.0.0.1:8757/api/health

# 列表 / 详情
curl http://127.0.0.1:8757/api/processes
curl http://127.0.0.1:8757/api/processes/web

# 启动
curl -X POST http://127.0.0.1:8757/api/start \
  -H 'content-type: application/json' \
  -d '{"name":"web","command":"sleep","args":["30"],"cwd":null,"env":{},"instances":1,"port":null,"max_memory":null,"max_restarts":null,"restart_delay_ms":null,"restart_strategy":"OnFailure","kill_signal":null,"health_check":null,"wait_ready":false,"ready_timeout_secs":null}'

# 操作
curl -X POST http://127.0.0.1:8757/api/processes/web/restart
curl -X POST http://127.0.0.1:8757/api/processes/web/stop
curl -X POST http://127.0.0.1:8757/api/processes/web/delete
```

WebSocket：

- 地址：`ws://127.0.0.1:8757/api/ws`
- 每秒推送一次 `process_list` JSON。

## 设计要点

- **子进程独立存活（方案 B）**：子进程经 `setsid` 脱离会话；Daemon 崩溃后子进程被 init 收养，新 Daemon 启动时按 `(pid, start_time)` 校验后重接管，避免 PID 复用误杀。
- **Actor 并发模型**：`ProcessManager` 状态由单 task 独占，IPC / supervisor / 定时器经 channel 通信，无锁跨 `await`。
- **崩溃退避**：默认指数退避（1s→16s 封顶）；`max_restarts` 指窗口内连续崩溃次数，长稳运行后自动清零。
- **原子持久化**：`state.json` 采用临时文件 + rename，带 `schema_version`。
- **低占用**：单线程 `current_thread` 运行时 + 精简 tokio features；release 采用 `opt-level=z + lto + strip`。

## 目录布局（`$OWL_HOME`，默认 `~/.owl`）

```
~/.owl/
├── owl.sock        # UDS（0600）
├── owl.lock        # 单例 flock
├── daemon.pid      # Daemon PID
├── state.json      # 持久化配置
└── logs/
    ├── owl-daemon-*.log    # Daemon 自身日志（owl-logger）
    └── <name>-<id>.log     # 各进程合并日志
```

设置 `OWL_HOME` 可隔离运行环境（便于测试 / 多用户）。
