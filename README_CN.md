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
- **🧱 不可变执行计划：** 严格的 JSON Workflow Plan 编译为不可变 Execution Plan。
- **✅ 编译期准入校验：** 执行前校验节点、依赖、环、端口、条件边、策略、重试设置和资源预算。
- **🔗 显式强类型数据流：** 节点只通过声明的输入绑定和输出端口传递值，不依赖隐藏共享状态。
- **🔌 执行器无关调度：** Workflow Runtime 只按执行器名称路由 Task，不解释执行器专属配置。
- **🛡️ 强制命令沙箱：** Command 执行器不允许降级为宿主机直接执行，生产默认只接受精确白名单中的已知二进制。
- **🌐 受治理的 HTTP 执行：** 可选 HTTP 执行器使用异步 DNS/TCP/TLS/HTTP I/O，并同时受精确 Origin 与解析后地址策略约束。
- **🔄 Run / Task / Attempt 状态机：** 暂停、恢复、延迟重试、超时、Fail-fast、取消和进程回收都有明确状态。
- **📦 Artifact：** 大型值可外置为 Artifact 引用，避免在节点之间复制大对象。
- **🧾 Evidence 与 Checkpoint：** 记录关键运行事件，并在指定任务边界生成检查点。
- **🔁 幂等触发：** 相同幂等键复用已有 Run，避免重复执行。
- **📡 可选 HTTP 控制面：** 提供 Plan、Run、输出、Evidence、生命周期控制、健康检查和 Prometheus 指标。

---

## 🏗️ 运行架构

```text
上层 AI / 应用
          |
          v
 JSON Workflow Plan v1
          |
          v
     Plan Compiler
          |
          v
  Immutable Execution Plan
          |
          v
 Workflow Runtime / Scheduler
          |
          v
      Executor Registry
       /              \
      v                v
Command Executor   HTTP Executor
      |             异步 TCP/TLS
      v
Minijail Sandbox
```

JSON Plan 描述 Task、依赖、输入输出绑定、执行策略、执行器名称和不透明的执行器
配置。Compiler 校验图结构，并把配置校验委托给对应执行器。Workflow Runtime
负责 Run / Task / Attempt 状态、调度、重试、输出传播、暂停、取消和结束判断，
不理解 Task 的业务含义。

---

## 🧩 执行模型

每个 Task 都包含执行器名称和 JSON `config`。通用 Compiler 与 Runtime 把
`config` 当作不透明数据；`ExecutorRegistry` 负责解析名称、委托配置编译、启动
任务和路由取消。

`command` 执行器负责解释 `program`、`arguments`、`env` 和 `input_env`，
并启动沙箱外部进程。可选 `http` 执行器负责方法、URL、请求头、请求体输入绑定、
可接受状态码、取消和响应输出。第二种异步执行模型接入后，Workflow Runtime
仍不需要增加 HTTP 分支或新的 Task 类型。

Owner Shard 协程只负责调度、定时器、执行器回调、取消和状态转换；具体工作以
何种方式执行，由执行器实现自行决定。

---

## 🛡️ Command 沙箱

Command 通过固定版本的 Google Minijail Helper 启动。每个 Command 都会获得：

- 独立的 user、PID、mount、network、IPC、UTS 和 cgroup namespace；
- Landlock 文件系统限制；
- seccomp denylist 和 `no_new_privs`；
- 独立且限制大小的 `/tmp`；
- 内存、文件、进程、文件描述符、CPU 和运行时限；
- 独立可写 Workspace。

该边界用于约束由管理员安装并精确白名单化的已知程序处理不可信输入，不用于
安全运行恶意原生二进制、Workflow 上传的可执行文件或攻击者可写的共享库。
沙箱二进制/BPF 可被组或其他用户写入、Workspace 不安全、Landlock 不可用或
资源限制非法时，应用在启动阶段直接失败。DAGForge 不会退回宿主机直接执行。

stdout、stderr 和未终止的流式单行分别有硬上限；超限会杀死整个进程组，而不是
静默截断。应用关闭时先拒绝新任务、杀死并等待活动沙箱进程回收，再停止 Runtime。

`MinijailCommandExecutor` 直接实现 Command 执行接口。Workflow 调度只依赖
`ICommandExecutor`；Minijail 参数、进程监管和沙箱状态都封装在底层执行器
实现内部。

---

## 🌐 HTTP 执行器

可选 `http` 执行器默认关闭。出站网络权限由系统配置控制，Workflow JSON 无权
自行扩大访问范围：

```toml
[http_executor]
enabled = true
allow_plaintext = false
deny_private_networks = true
allowed_origins = ["https://api.example.com"]
allowed_ip_cidrs = []
max_request_headers = 64
max_request_header_bytes = 65536
max_request_body_bytes = 1048576
max_response_headers = 128
max_response_header_bytes = 65536
max_response_body_bytes = 10485760
max_concurrent_requests_per_shard = 32
max_concurrent_requests = 256
tls_min_version = "1.2"
tls_ca_file = ""
tls_client_cert_file = ""
tls_client_key_file = ""
```

Origin 按 scheme、host 和有效端口精确匹配。每个 DNS 结果都会在连接前检查；
默认拒绝 loopback、link-local、私网、组播、文档网段和其他特殊用途地址，只有
显式 CIDR 才能放行。HTTPS 校验 SNI 与主机名，可配置私有 CA 和 mTLS，并限制
最低 TLS 1.2/1.3。分片级和进程级并发上限都在打开 socket 前生效。

控制面配置证书后，监听端口是 TLS-only，不会在同一端口自动探测并降级到明文。
Server 同时限制活动连接、空闲时间、解析大小和每连接请求数，并在关闭时主动关闭
活动连接。v1 不支持重定向、代理、Cookie 和动态 URL。取消会中断 DNS、TCP、
TLS、写入和读取。示例见 [`dags/http_pipeline.json`](dags/http_pipeline.json)。

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
dagforge validate --file dags/hello_world.json
```

执行器权限由系统配置决定。HTTP Workflow 应使用运行时相同的配置进行校验：

```bash
dagforge validate \
  --config system_config.toml \
  --file dags/http_pipeline.json
```

### 4) 本地运行

```bash
dagforge run \
  --config system_config.toml \
  --file dags/hello_world.json \
  --wait
```

可通过 `--payload` 传入 JSON 或文本触发数据：

```bash
dagforge run \
  --config system_config.toml \
  --file dags/hello_world.json \
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

DAGForge 只接受严格的 JSON Workflow Plan，未知字段会被拒绝。

```json
{
  "workflow_id": "hello-world",
  "schema_version": 1,
  "nodes": [
    {
      "id": "start",
      "executor": "command",
      "outputs": ["stdout", "stderr", "exit_code", "result"],
      "timeout_sec": 30,
      "config": {
        "program": "/bin/echo",
        "arguments": ["hello from DAGForge"],
        "env": [],
        "input_env": []
      }
    }
  ]
}
```

通用输入绑定位于执行器配置之外。执行器按照自己的契约映射输入，例如 Command
执行器可通过 `config.input_env` 将输入映射为环境变量。

完整约定见 [`dags/hello_world.json`](dags/hello_world.json)、
[`dags/http_pipeline.json`](dags/http_pipeline.json) 和
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

配置文件包含六个顶层区段：

| 区段 | 用途 |
| --- | --- |
| `[runtime]` | Shard 数量和 CPU 亲和性 |
| `[sandbox]` | Command 沙箱路径、资源限制和命令白名单 |
| `[workflow]` | Workflow 运行时开关 |
| `[admission]` | 服务端拥有的执行器白名单和 Plan 预算限制 |
| `[storage]` | 可选的持久化 Run、Evidence 和 Artifact 目录 |
| `[api]` | 可选 HTTP 地址、端口和 TLS 配置 |

Workflow Plan 不能自行授权。计划在编译和注册前必须通过服务端
`AdmissionPolicy`。

HTTP 控制面支持从环境变量加载 Bearer Token，并限制请求体大小和并发请求数。
只有 `api.bearer_token_env` 为空时才关闭认证。

完整配置见 [`system_config.toml`](system_config.toml)。

---

## 📡 HTTP 控制面

当 `[api].enabled = true` 时，HTTP 服务提供：

- Plan 注册和列表；
- Workflow Run 创建和状态查询；
- Task 输出和 Evidence 查询；
- 暂停、恢复和取消；
- 健康检查、运行状态和 Prometheus 指标。

API 禁用时不会分配 HTTP Server。配置 `api.bearer_token_env` 后，所有路由
都要求对应的 Bearer Token。

接口说明见 [`docs/API.md`](docs/API.md)。

---

## 💾 存储边界

默认使用内存存储。设置 `[storage].enabled = true` 后，运行时会在
`[storage].directory` 下保存原子 Run Checkpoint、追加式 Evidence 和文件型
Artifact。

已完成 Run 及其输出会在重启后恢复。新进程无法安全接管上一个运行时实例创建
的沙箱进程，因此恢复时发现的非终态 Run 会明确结束为 `failed`，活动 Attempt
记录为基础设施失败，不会继续显示成虚假的 `running`。

`storage.max_completed_runs` 和 `storage.max_evidence_records` 限制历史保留
数量。API 集合接口通过 `offset` 和 `limit` 分页。

---

## 🧪 测试与基准

运行全部单元测试和集成测试：

```bash
~/.local/share/build2-configs/dagforge-gcc/dagforge/bin/all-unit-tests
```

通过真实 HTTP 服务、Command executor 和 Minijail 沙箱运行 Workflow JSON
端到端集合：

```bash
python3 scripts/test-real-workflows.py \
  --binary "$HOME/.local/share/build2-configs/dagforge-gcc/dagforge/bin/dagforge"
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
