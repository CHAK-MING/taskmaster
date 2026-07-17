# 架构与所有权

## 产品边界

DAGForge 是执行严格 JSON Workflow Plan 的单机工作流运行时。它负责图校验、调度、重试、取消、恢复、修复运行、持久化和控制面，不负责业务规划、模型推理策略或通用分布式调度。产品兼容面是 CLI/JSON 与 HTTP API/JSON。

## 依赖方向

- `core/` 拥有错误域、Result 基础、契约、内存策略、Runtime、Shard、协程与 metrics 基元，不依赖业务模块。
- `util/` 拥有文本、解析、枚举、ID、JSON、时间、哈希和日志等通用能力；通用能力不得认识 Workflow、HTTP route 或存储 envelope。
- `io/` 拥有 IoContext、Asio 错误归一化、取消、deadline 和 timing wheel；它可以依赖基础层，但不依赖 Workflow。
- `config/` 只拥有服务端持久配置、环境覆盖和 executor policy DTO，不拥有 CLI parser state、单次 HTTP request state 或运行时解析结果。
- `http/` 拥有通用 DNS/TCP/TLS/HTTP client、server、router 和 parser，不认识 Workflow Node、Run 状态或 Artifact 语义。
- `sandbox/` 只暴露 `CommandSpec` 与 `ICommandRunner` seam；Minijail policy、进程监督和平台细节留在 `src/`，不得依赖 Workflow。
- `workflow/` 拥有 Workflow value、Plan、Compiler、Run/Task/Attempt 状态机、Runtime、Store、`ITaskExecutor` 和 Registry，不依赖具体 executor、HTTP transport 或 sandbox 实现。
- `executors/<kind>/` 各自拥有节点协议、编译后配置和 Workflow 到底层能力的适配；不同 executor 的协议 invariant 不得塞进通用工具层。
- `app/` 是 composition root，负责配置加载、组件构造、具体 executor 注册、CLI、HTTP 控制面和 shutdown 编排，不承载 executor 内部规则。

## 文件放置

- 只被一个 `.cpp` 使用的声明放在该文件匿名命名空间。
- 多个实现文件共享但不公开的声明放 `src/dagforge/<subsystem>/detail/*.hpp`。
- 只有稳定的仓库内接口放 `include/dagforge/**`；公共模板需要的内联实现可放 `include/dagforge/**/detail/*.inc`。
- module interface 只放 `src/modules/*.cppm`，负责重新导出已有头，不复制实现和领域规则。
- 新文件必须能用一句不含“以及”的话描述职责，并明确谁拥有它、谁允许依赖它、删除后哪些调用方会变复杂。

## 基础层原则

- 基础层接口必须有明确成功值、失败值、所有权、线程语义、生命周期和复杂度。
- 外部整数、字符串、errno、Boost error code 和 JSON 都是非可信边界，必须安全处理未知值和非法输入。
- table hash、运行时路由 hash 与稳定 digest 是不同概念，不得因为返回类型相同而复用语义。
- wall clock 用于时间戳，steady clock 用于 deadline 和耗时；不得用系统时间驱动超时。
- 线程局部 memory resource override 只能覆盖同步动态作用域，不得跨 `co_await`、线程迁移或长期保存。

## 决策记录

已接受且跨模块生效的决策放 `docs/adr/`。实现与 ADR 冲突时必须先明确指出冲突并更新决策，不得静默绕过；一次性调查、任务记录和中间评审只放本地 `.scratch/`，完成后删除。
