# DAGForge

**使用 C++23 构建的通用、高性能 DAG 运行时。**

[English](README.md) | [简体中文](README_CN.md)

DAGForge 0.4 定位为可编程工作流的底层执行层。上层应用——例如 Python
实现的 AI Planner——生成版本化 Workflow Plan；DAGForge 负责严格校验、
编译、调度、执行、观测和取消，并提供确定的运行语义。

自然语言理解、意图拆解、Agent Loop 和失败后的重新规划不属于底层运行时，
应由上层 AI 应用负责。

## 架构

```text
AI 应用 / 工作流作者
        |
        v
  WorkflowPlan v1
        |
        v
   PlanCompiler
        |
        v
不可变 ExecutionPlan
        |
        v
  WorkflowRuntime
 /       |        \
执行器  计算池    适配器
```

当前运行时提供：

- 基于 Boost.Asio 和 C++23 协程的 owner-shard 工作流执行；
- 不可变执行计划，以及环、端口、策略和资源校验；
- 显式、强类型的节点输出和输入绑定；
- 有界并发、运行时限、节点超时、重试和取消；
- Shell、Docker、Lua、HTTP、Compute、Model、Tool、Evaluator、Approval、
  Noop 节点；
- 大值 Artifact 外置；
- Checkpoint、Evidence、幂等触发和人工审批 Gate；
- REST 控制面和 Prometheus 指标。

当前 Checkpoint、Evidence、Artifact、Plan 和已完成 Run 使用内存适配器。
持久化恢复是后续 0.4 里程碑，当前版本不宣称进程重启后可恢复。

## 环境要求

- Linux x86-64 或 ARM64
- GCC 15+
- build2 0.17+
- Boost 1.88+
- OpenSSL 开发库

0.4 Runtime Core 不依赖 MySQL 或 Node.js。

## 构建

```bash
./scripts/setup-build2.sh
./scripts/build.sh
```

构建脚本会输出实际 build2 配置目录和产物路径。构建后可执行：

```bash
~/.local/share/build2-configs/dagforge-gcc/dagforge/bin/all-unit-tests
```

## 配置

当前支持四个顶层配置区段：

- `[runtime]`：Shard 数量和 CPU 亲和性；
- `[compute]`：有界 CPU 计算池；
- `[workflow]`：工作流适配器和 Provider 配置；
- `[api]`：HTTP 控制面。

完整示例见 [`system_config.toml`](system_config.toml)。

## CLI

校验工作流：

```bash
dagforge validate --file dags/hello_world.toml
```

本地运行并等待结束：

```bash
dagforge run \
  --config system_config.toml \
  --file dags/hello_world.toml \
  --wait
```

启动 REST 服务：

```bash
dagforge serve --config system_config.toml
```

## Workflow Plan

最小 TOML Plan：

```toml
workflow_id = "hello-world"
schema_version = 1

[[nodes]]
id = "start"
type = "noop"
outputs = ["result"]
timeout_sec = 30

[nodes.config]
```

Plan 支持严格 JSON 或 TOML；未知字段会被拒绝。

## HTTP 控制面

服务提供 Plan 注册、工作流启动、Run 状态、输出、Evidence、Approval、取消、
健康检查、状态和指标接口。详见 [`docs/API.md`](docs/API.md)。

## Benchmark

0.4 只保留直接衡量当前 Runtime、ComputePool 和 Lua Executor 的基准：

```bash
~/.local/share/build2-configs/dagforge-gcc/dagforge/bin/bench-core
```

旧 Airflow 风格基准测试的是已经退役的 0.3 Scheduler/Storage 栈，因此已删除。

## 文档

- [`docs/USER_GUIDE.md`](docs/USER_GUIDE.md)
- [`docs/API.md`](docs/API.md)
- [`docs/CLANGD_SETUP.md`](docs/CLANGD_SETUP.md)
- [`docs/BENCH_REPORT.md`](docs/BENCH_REPORT.md)

## License

Apache License 2.0，详见 [`LICENSE`](LICENSE)。
