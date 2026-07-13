# DAGForge

<div align="center">

**基于现代 C++23 构建的高性能、分片式工作流运行时**

[![C++23](https://img.shields.io/badge/C%2B%2B-23-blue.svg?style=flat-square&logo=c%2B%2B)](https://en.cppreference.com/w/cpp/23)
[![License](https://img.shields.io/badge/license-Apache--2.0-white?labelColor=black&style=flat-square)](LICENSE)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/CHAK-MING/DAGForge)
[![Release](https://img.shields.io/github/v/release/CHAK-MING/dagforge?include_prereleases&style=flat-square)](https://github.com/CHAK-MING/dagforge/releases)

[English](README.md) | [简体中文](README_CN.md)

</div>

---

## ⚡ 什么是 DAGForge？

**DAGForge** 是一个通用 DAG 工作流运行时。上层应用提交 Workflow Plan，
DAGForge 负责校验、编译、调度、执行、暂停、恢复、取消和观测，并提供明确、
可重复的运行时语义。

运行时采用 Shard 所有权模型、C++23 协程和有界并发，减少多核环境中的共享
状态竞争。自然语言理解、计划生成和 Agent Loop 属于上层应用，不进入运行时
核心。

---

## ✨ 核心特性

- **🚀 分片运行时：** 每个 Workflow Run 归属固定 Owner Shard，运行状态采用单写者模型。
- **🧱 不可变执行计划：** 严格的 JSON 或 TOML Workflow Plan 编译为不可变 Execution Plan。
- **✅ 编译期准入校验：** 执行前校验节点、依赖、环、端口、条件边、策略、重试设置和资源预算。
- **🔗 显式强类型数据流：** 节点只通过声明的输入绑定和输出端口传递值，不依赖隐藏共享状态。
- **🛡️ 强制命令沙箱：** Command 是唯一启动外部进程的执行器，不提供非沙箱降级路径。
- **🔄 Run / Task / Attempt 状态机：** 暂停、恢复、延迟重试、超时、Fail-fast、取消和进程回收都有明确状态。
- **📦 Artifact：** 大型值可外置为 Artifact 引用，避免在节点之间复制大对象。
- **🧾 Evidence 与 Checkpoint：** 记录关键运行事件，并在指定任务边界生成检查点。
- **🔁 幂等触发：** 相同幂等键复用已有 Run，避免重复执行。
- **📡 可选 HTTP 控制面：** 提供 Plan、Run、输出、Evidence、生命周期控制、健康检查和 Prometheus 指标。

---

## 🏗️ 运行架构

```text
上层应用 / 工作流作者
          |
          v
   Workflow Plan v1
          |
          v
     Plan Compiler
          |
          v
  Immutable Execution Plan
          |
          v
    Workflow Runtime
   /                  \
CommandExecutor      ComputePool
   |
   v
SandboxBackend
   |
   v
Minijail Sandbox
```

Workflow Plan 描述执行意图；运行时负责确定性的校验、状态转换、调度、输出传播
和执行清理。

---

## 🧩 执行模型

`CommandExecutor` 是唯一会启动外部进程的执行器。Command 必须使用绝对程序
路径和显式参数数组，运行时不会隐式插入 Shell。

Workflow Plan 中的每个节点都是沙箱命令任务。上游值只有通过显式输入绑定和
`input_env` 映射，才会进入命令环境。HTTP 调用、模型推理、MCP Tool、评估和
其他领域逻辑都由上层选择普通程序实现，C++ 运行时不再把这些协议编码成节点
类型。

`ComputePool` 仍是运行时内部设施，不是 Workflow Plan 算子。Owner Shard
需要卸载 CPU 工作时，由运行时实现自动选择它。

---

## 🛡️ Command 沙箱

Command 通过固定版本的 Google Minijail Helper 启动。每个 Command 都会获得：

- 独立的 user、PID、mount、network、IPC、UTS 和 cgroup namespace；
- Landlock 文件系统限制；
- seccomp denylist 和 `no_new_privs`；
- 独立且限制大小的 `/tmp`；
- 内存、文件、进程、文件描述符、CPU 和运行时限；
- 独立可写 Workspace。

沙箱二进制、策略文件或必要内核能力缺失时，任务直接失败。DAGForge 不会退回
宿主机直接执行。

`CommandExecutor` 只依赖 `ISandboxBackend` 接口。当前发布的实现是
Minijail；工作流调度和命令执行不再依赖 Minijail 参数或进程管理细节。

---

## 🚀 快速开始

### 1) 环境准备

- 支持 user namespace、seccomp 和 Landlock 的 Linux x86-64 或 ARM64
- GCC 15+
- build2 0.17+
- Boost 1.88+
- OpenSSL 和 libcap 开发包
- 用于构建固定 Minijail 的 Git、Make 和 Python 3

### 2) 源码构建

```bash
./scripts/setup-build2.sh
./scripts/install-minijail.sh
./scripts/build.sh
```

构建脚本会输出使用的 build2 配置和可执行文件路径。

### 3) 校验 Workflow Plan

```bash
dagforge validate --file dags/hello_world.toml
```

### 4) 本地运行

```bash
dagforge run \
  --config system_config.toml \
  --file dags/hello_world.toml \
  --wait
```

可通过 `--payload` 传入 JSON 或文本触发数据：

```bash
dagforge run \
  --config system_config.toml \
  --file dags/hello_world.toml \
  --payload '{"request":"hello"}' \
  --wait
```

### 5) 启动 HTTP 控制面

```bash
dagforge serve --config system_config.toml
```

### 6) Docker Compose

```bash
docker compose up --build
```

---

## 📝 Workflow Plan

DAGForge 接受严格的 JSON 或 TOML Workflow Plan，未知字段会被拒绝。

最小 TOML 示例：

```toml
workflow_id = "hello-world"
schema_version = 1

[[nodes]]
id = "start"
outputs = ["stdout", "stderr", "exit_code", "result"]
timeout_sec = 30

[nodes.config]
program = "/bin/echo"
arguments = ["hello from DAGForge"]
```

沙箱 Command 示例：

```toml
[[nodes]]
id = "render"
outputs = ["stdout", "stderr", "exit_code", "result"]
timeout_sec = 30

[nodes.config]
program = "/usr/bin/python3"
arguments = ["-c", "print('hello from the sandbox')"]
env = [{ key = "MODE", value = "test" }]
```

上游输出必须显式映射到环境变量：

```toml
inputs = [{ input = "payload", source_node = "prepare", source_port = "result" }]

[nodes.config]
program = "/usr/bin/python3"
arguments = ["/workspace/consume.py"]
input_env = [{ input = "payload", environment = "DAGFORGE_INPUT" }]
```

完整约定见 [`dags/hello_world.toml`](dags/hello_world.toml) 和
[`docs/USER_GUIDE.md`](docs/USER_GUIDE.md)。

---

## 🔄 状态模型

Plan 中的 Node 在运行时投影为 Task，每次真实执行都会创建独立 Attempt 记录。

| 层级 | 状态 |
| --- | --- |
| Run | `running`、`pausing`、`paused`、`stopping`、`succeeded`、`failed`、`cancelled` |
| Task | `pending`、`ready`、`running`、`retry_waiting`、`succeeded`、`failed`、`skipped`、`cancelled` |
| Attempt | `starting`、`running`、`terminating`、`succeeded`、`failed`、`timed_out`、`cancelled` |

暂停只停止新任务派发，已运行的 Attempt 会正常结束。取消和 Fail-fast 会保持
`stopping`，直到所有活动 Attempt 终止并完成进程回收。

---

## ⚙️ 系统配置

配置文件包含七个顶层区段：

| 区段 | 用途 |
| --- | --- |
| `[runtime]` | Shard 数量和 CPU 亲和性 |
| `[compute]` | 有界计算线程池和线程亲和性 |
| `[sandbox]` | Minijail 路径、Workspace 根目录和资源限制 |
| `[workflow]` | Workflow 运行时开关 |
| `[admission]` | 服务端拥有的程序、环境变量和预算限制 |
| `[storage]` | 可选的持久化 Run、Evidence 和 Artifact 目录 |
| `[api]` | 可选 HTTP 地址、端口和 TLS 配置 |

Workflow Plan 不能自行授权。计划在编译和注册前必须通过服务端
`AdmissionPolicy`。

完整配置见 [`system_config.toml`](system_config.toml)。

---

## 📡 HTTP 控制面

当 `[api].enabled = true` 时，HTTP 服务提供：

- Plan 注册和列表；
- Workflow Run 创建和状态查询；
- Task 输出和 Evidence 查询；
- 暂停、恢复和取消；
- 健康检查、运行状态和 Prometheus 指标。

API 禁用时不会分配 HTTP Server。当前控制面没有内置认证中间件，开发环境外
应绑定回环地址，或部署在可信网关之后。

接口说明见 [`docs/API.md`](docs/API.md)。

---

## 💾 存储边界

默认使用内存存储。设置 `[storage].enabled = true` 后，运行时会在
`[storage].directory` 下保存原子 Run Checkpoint、追加式 Evidence 和文件型
Artifact。

已完成 Run 及其输出会在重启后恢复。新进程无法安全接管上一个运行时实例创建
的沙箱进程，因此恢复时发现的非终态 Run 会明确结束为 `failed`，活动 Attempt
记录为基础设施失败，不会继续显示成虚假的 `running`。

---

## 🧪 测试与基准

运行全部单元测试和集成测试：

```bash
~/.local/share/build2-configs/dagforge-gcc/dagforge/bin/all-unit-tests
```

运行 Runtime 和内存基准：

```bash
~/.local/share/build2-configs/dagforge-gcc/dagforge/bin/bench-core
```

---

## 📚 文档指南

- **[用户指南](docs/USER_GUIDE.md)** — Workflow Plan、配置和运行语义。
- **[API 参考](docs/API.md)** — HTTP 控制面接口。
- **[状态机 ADR](docs/adr/0001-run-task-attempt-state-machine.md)** — Run、Task 和 Attempt 设计。
- **[Clangd 配置](docs/CLANGD_SETUP.md)** — Modules 和编辑器索引。
- **[基准说明](docs/BENCH_REPORT.md)** — 当前基准目标和报告规则。

---

## 🤝 贡献代码

1. Fork 本仓库。
2. 创建功能分支。
3. 提交代码和测试。
4. 推送分支并创建 Pull Request。

---

## 📄 开源协议

基于 **Apache License 2.0** 协议发布。详情请参阅 [`LICENSE`](LICENSE)。
