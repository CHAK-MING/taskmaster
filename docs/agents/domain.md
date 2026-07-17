# 领域文档使用规则

开始修改 Workflow、Runtime、Executor、Artifact、Evidence、Plan 或恢复语义前，先读取根目录 `CONTEXT.md` 和相关 `docs/adr/*.md`。代码、测试、提交和文档必须使用其中已经定义的 Run、Task、Attempt、Plan、Artifact、Evidence、Repair Run、owner shard 等词汇，不随意创造同义词。

当实现需要改变已接受决策时，先在变更说明中指出受影响 ADR，再修改或新增 ADR；不得通过新增旁路、兼容字段或第二套状态悄悄规避原决策。只有稳定且跨多次实现仍有效的领域词汇进入 `CONTEXT.md`，只有具有长期约束力的架构取舍进入 ADR，一次性分析和未定方案留在本地 `.scratch/`。
