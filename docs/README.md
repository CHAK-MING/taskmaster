# DAGForge 文档

## 用户文档

- [用户指南](USER_GUIDE.md)：构建、配置、Workflow Plan v1、CLI、运行语义、持久化和运维边界。
- [HTTP API](API.md)：当前控制面 route、请求和响应结构。
- [备份与恢复](BACKUP_RESTORE.md)：持久化状态目录的停机备份、校验、恢复、回滚和演练流程。
- [基准规范](BENCH_REPORT.md)：当前 benchmark target、执行方法和报告规则。
- [clangd 配置](CLANGD_SETUP.md)：build2、C++ modules 和 VS Code/clangd 环境。

## 工程文档

- [AI 开发入口](../AGENTS.md)：所有 AI 会话必须读取的硬规则和工作流程。
- [架构与所有权](agents/architecture.md)：模块职责、依赖方向和文件放置。
- [C++ 编码规范](agents/coding-style.md)：C++23、`.clang-format`、命名、头文件、错误、JSON 和协程规则。
- [领域文档规则](agents/domain.md)：`CONTEXT.md`、领域词汇和 ADR 的使用方式。
- [本地任务与 Spec](agents/issue-tracker.md)：`.scratch/` 的本地任务结构和清理规则。
- [验证与交付](agents/verification.md)：格式化、构建、测试、审计和提交要求。

## 架构决策

- [ADR 0001](adr/0001-run-task-attempt-state-machine.md)：Run、Task 和 Attempt 使用独立状态机。
- [ADR 0002](adr/0002-cxx23-foundation-contracts.md)：C++23-first 基础层契约、依赖边界和验证门禁。
