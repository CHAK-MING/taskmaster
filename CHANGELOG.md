# 更新日志

DAGForge 的重要变化记录在本文件中。未发布内容只描述当前主分支相对最近正式版本的用户可见行为、兼容影响和工程基础变化，不记录一次性调查、内部过程稿或生成物。

## [未发布]

### 破坏性变更

- 产品从 0.3 的 DAG 调度器、MySQL 持久化、Web UI、WebSocket、cron、sensor、XCom 和多种宿主执行器收敛为单机 JSON Workflow Runtime；相关旧配置、API、CLI、模块、示例和兼容类型已删除。
- Workflow Plan 与 System Configuration 只接受严格 JSON，未知字段和不支持的 schema version 会失败，不再静默忽略或回退旧格式。
- Workflow Runtime 只认识 executor-neutral Task、编译后配置、typed inputs/outputs 和 `ExecutionFailure`；Command 与 HTTP 协议由各自 executor 拥有。
- Run、Task、Attempt 使用独立状态机；取消、deadline 和 fail-fast 先进入 stopping 并等待活动 Attempt 完整终止，终态字段和 API 语义与 0.3 不兼容。
- CLI 改为 `validate`、`run`、`serve` 和语义化 `api` 操作，删除旧 DAG 管理、`--wait`、trigger-only payload 和 retired management 命令。

### 新增

- 新增严格 Workflow Plan v1、Plan Compiler、immutable ExecutionPlan、ExecutorRegistry、WorkflowRuntime、typed workflow values、条件边、fan-out/fan-in、published outputs、重试、暂停、恢复、取消、idempotency 和 Repair Run。
- 新增 Command executor，使用 server-owned program registry、最小环境、Minijail、namespace、Landlock、seccomp、resource limits、私有 Attempt workdir 和完整 process-group kill/reap。
- 新增 HTTP executor，支持 origin/CIDR egress policy、TLS verification、custom CA、可选 mTLS、分阶段 timeout、取消、容量限制、bounded keep-alive、响应大小限制和稳定输出端口。
- 新增可选文件持久化，包括 versioned Plan、Checkpoint、Evidence 和 Artifact envelope、原子替换、父目录同步、存储目录独占锁、恢复、reconciliation、retention，以及随 release archive 交付的备份恢复 runbook。
- 新增 HTTP 控制面和 CLI 客户端，覆盖 Plan、Run、output、Evidence、failure report、repair、Artifact、pause、resume、cancel、health、status 和 metrics。
- 新增 release archive 重现性、依赖清单、Minijail revision、coverage、sanitizer、fuzz、module graph、foundation header、benchmark 和真实 Workflow 验证链。

### 基础库

- 基础层固定为 C++23，新增公共头严格独立编译、module smoke、依赖方向和禁止类型门禁。
- 新增 table-driven static error domain，未知整数错误值安全返回稳定消息，不再进入未定义行为。
- 新增详细整数与 JSON 解析结果、项目自有 enum metadata、typed ID validation、显式 trusted construction 和 serde 边界校验。
- JSON 序列化失败现在必须传播，删除会把失败伪装成合法 `null` 的 `dump_json()` 路径。
- Asio、Boost 和 errno 错误统一归一化为项目 `Result`，sleep、post、timer、HTTP 和进程边界不再把预期失败泄漏为异常。
- metrics 支持 bucket 校验、chrono observation、弱一致 snapshot 文档和明确 overflow 语义；time API 明确区分 UTC、named zone、wall clock 与 steady deadline。
- Logger 拆分为结构化 `LogRecord`、可注入 Sink、显式 overflow policy、drop counter、source location、flush 和 reconfigure 结果。
- typed ID、domain tag 与 UUID 生成职责分离；线程局部 memory resource override 具有非空、LIFO 和创建线程契约，并明确禁止跨协程挂起点。

### 修复

- 修复 shutdown 顺序、跨 executor completion、外部线程回调、重复完成、socket 关闭亲和性和活动进程回收中的竞态与泄漏。
- 修复 checkpoint 过度写入、Evidence 每次 append 全量重写、Plan/Checkpoint stale cache、Artifact 删除结果不真实和目录 durability 失败被吞掉的问题。
- 修复 restart recovery、idempotency、published output、failure artifact、Plan digest、存储大小上限、损坏记录和 crash-tail repair 的边界行为。
- 修复 HTTP DNS rebinding、防私网绕过、timeout 分类、response overflow、keep-alive shutdown、TLS policy 和 Command 环境传递问题。

## [0.3.0] - 2026-03-30

### 变更

- 核心迁移到 C++ modules 和 build2，更新 Linux release 打包流程。
- 扩展 Runtime、Scheduler、API、日志、WebSocket 和 benchmark 可观测性，并刷新 Web UI。

## [0.2.0] - 2026-03-18

### 变更

- 重构配置、MySQL 持久化、Scheduler 和 executor 生命周期，补充 timeout、invalid command、non-zero exit 和日志流式输出测试。

## [0.1.0-beta]

### 新增

- 首个公开 beta，包含 sharded runtime、DAG 调度、Shell/Docker/Sensor executor、MySQL、CLI、REST API、WebSocket 和 React Web UI。
