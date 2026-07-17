# 本地任务与 Spec

复杂任务可以在 `.scratch/<effort>/` 保存本地 spec、调查记录和切片清单，用于跨会话保持上下文；该目录被 Git 忽略，任何内容都不得进入提交、release archive 或长期架构文档。

## 推荐结构

- `.scratch/<effort>/spec.md`：目标、非目标、约束、兼容影响和完成条件。
- `.scratch/<effort>/map.md`：当前结论、依赖顺序、风险和下一切片。
- `.scratch/<effort>/issues/NN-<slug>.md`：一个文件只描述一个可验证切片，记录状态、阻塞项和验证命令。
- `.scratch/<effort>/reviews/`：临时 Standards/Spec review；已解决的信息应进入代码、测试、`CONTEXT.md` 或 ADR，过程稿随后删除。

任务完成后删除对应 `.scratch/<effort>/`。需要长期保留的用户说明进入 `docs/USER_GUIDE.md` 或 `docs/API.md`，长期工程规则进入 `AGENTS.md` 与 `docs/agents/`，跨模块决策进入 `docs/adr/`，版本变化进入 `CHANGELOG.md`。
