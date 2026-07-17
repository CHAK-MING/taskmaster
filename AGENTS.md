# DAGForge AI 开发指南

本文件是所有 AI 编码会话的入口。开始工作前先读取本文件，再按任务读取 `docs/agents/` 中的专项规范、`CONTEXT.md` 和相关 ADR；不得依赖未提交的本地提示词、旧审计稿或网络文章推断项目约定。

## 必读文档

- 架构所有权与依赖方向：`docs/agents/architecture.md`
- C++23、命名、头文件、module、错误和 serde 规范：`docs/agents/coding-style.md`
- 领域词汇与 ADR 使用方式：`docs/agents/domain.md`
- 本地 spec 与任务拆分：`docs/agents/issue-tracker.md`
- 构建、格式化、测试和提交要求：`docs/agents/verification.md`

## 不可违反的规则

- DAGForge 的产品兼容面是 CLI/JSON 和 HTTP API/JSON；`include/dagforge/**` 是仓库内部稳定接口，不构成外部 ABI 或通用 C++ SDK 承诺。
- 项目最低标准是 C++23。优先使用标准库能力，第三方实现细节必须封装在项目拥有的 seam 后面，不得把 Abseil、Boost、Glaze 或实验性标准库类型泄漏到无关公共接口。
- 普通失败使用 `Result<T>`、`ok(...)` 和 `fail(...)`；异步接口通常返回 `task<Result<T>>`。不得用异常表达预期失败，不得吞掉错误或把失败伪装成有效值。
- JSON 统一经过 `dagforge/util/json.hpp`；稳定 wire model 使用 typed serde。解析负责输入形状，领域 invariant 由拥有该概念的 Validator、Compiler、Store 或 Runtime 检查。
- 新 Asio 操作使用 `dagforge::use_nothrow` 与 `co_as_result(...)`。协程不得跨挂起点借用调用方栈、临时对象、线程局部覆盖或 teardown-only `this`。
- owner-shard 状态只能在 owner shard 修改。跨 shard 使用 Runtime 的 post/spawn；不得用 mutex 掩盖执行器所有权错误。
- shutdown 必须先停止接收新工作并 quiesce，再等待外部操作、持久化和回调收敛，最后 teardown Runtime 与底层资源。
- 新公共头必须自包含、带 `#pragma once` 和 module-interface include guard，并通过严格独立编译与 module smoke。
- 日志使用 `dagforge::log`，不得直接向 stdout/stderr 写第一方运行日志；敏感值、凭据、完整请求体和未筛选外部错误不得进入日志。
- 持久化代码必须明确逻辑提交、物理清理和目录 durability 的区别；磁盘格式变化必须有版本、兼容策略和恢复测试。
- 不新增 `common`、`misc`、`helpers` 等杂物目录。只有能够集中真实知识、减少调用方复杂度并通过 deletion test 的 seam 才值得建立。
- Markdown 自然段和列表项不按列宽手动换行。C++ 只由仓库根目录 `.clang-format` 格式化，不手工制造与格式器冲突的布局。

## 工作流程

1. 先运行 `git status --short` 并确认当前分支、基线和已有本地改动；共享检出区不干净时使用独立 worktree，禁止覆盖或顺手提交他人的改动。
2. 搜索现有实现、测试、调用方、module 与 build2 注册，再决定修改位置；大型任务在本地 `.scratch/<effort>/` 建立 spec 和小切片，但 `.scratch/` 永不提交。
3. 每个切片只解决一个可验证问题，删除被替代的代码和文档，不留下 sibling variant、调试输出、生成物或无调用方抽象。
4. 对修改过的第一方 C++ 运行 `bash scripts/format.sh`，随后执行与风险匹配的 focused tests；完成前至少运行 `bash scripts/test.sh quick`、规范检查和 `git diff --check`。
5. 提交使用 Conventional Commits，正文说明动机和兼容影响；不得 push、force-update、重写共享历史或删除远端分支，除非用户明确要求。
