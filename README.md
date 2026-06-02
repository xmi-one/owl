# Owl

高性能、语言无关的进程管理器（Rust 版 PM2）。Client-Daemon 架构，通过 Unix Domain Socket 通信，子进程在 Daemon 崩溃后仍可存活并被新 Daemon 重接管。

> 当前为 **Phase 1 (MVP)**：核心生命周期已可用，多实例 / 资源监控 / 健康探针 / `owl.toml` / HTTP API 等为后续阶段（见 `implementation_plan.md`）。

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

# 终止 Daemon（在线子进程会脱离存活）
owl kill
```

首次执行任意命令会自动在后台拉起 Daemon（flock 保证单例）。

## 常用启动选项

| 选项 | 说明 |
| --- | --- |
| `--name <NAME>` | 进程名（缺省由命令派生） |
| `--cwd <DIR>` | 工作目录 |
| `--env KEY=VALUE` | 注入环境变量（可重复） |
| `--restart-strategy <always\|on-failure\|never>` | 重启策略（默认 `on-failure`） |
| `--max-restarts <N>` | 窗口内最大连续崩溃次数，超过则 `errored` |
| `--restart-delay <MS>` | 固定重启间隔（设置后禁用指数退避） |
| `--kill-signal <SIG>` | 停止信号（默认 `SIGTERM`） |

全局：`--json`（机器可读输出）、`--no-color`。

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
