# 贡献指南

## 开发环境

DAGForge 使用 C++23、build2、Boost、OpenSSL、GoogleTest 和固定 revision 的 Minijail。使用 `bash scripts/setup-build2.sh` 建立构建环境，使用 `bash scripts/install-minijail.sh` 安装 sandbox helper，并通过仓库脚本构建和测试，不自行拼接另一套编译命令。

## 开始工作

先阅读 `AGENTS.md`、与任务相关的 `docs/agents/*.md`、`CONTEXT.md` 和 ADR。运行 `git status --short` 确认本地状态；检出区已有未提交改动时使用独立 worktree，不覆盖、不 reset、不顺手提交无关文件。

## 格式与检查

第一方 C++ 使用根目录 `.clang-format`。运行 `bash scripts/format.sh` 格式化相对 `HEAD` 新增或修改的文件，运行 `bash scripts/format.sh --check` 做检查。不要格式化 `third_party/`、生成物或与当前任务无关的旧文件。

功能修改至少运行 `bash scripts/test.sh quick`、`python3 scripts/check-foundation-contracts.py --compiler "${CXX:-g++}"`、`bash scripts/check-agent-conventions.sh`、`bash scripts/check-module-graph.sh`、`python3 scripts/check-test-layout.py` 和 `git diff --check`。Runtime、并发、存储、HTTP、TLS、sandbox 或 release 变化还应运行 `bash scripts/test.sh all`、`bash scripts/test-runtime-audit.sh`、`bash scripts/test-coverage.sh`、fuzz 或 release verification 中与风险对应的部分。

## 变更纪律

保持 executor-neutral Workflow contract、owner-shard 所有权、显式 `Result` 失败、严格 JSON、版本化持久化和 quiesce-before-teardown。每个 corrected failure mode 添加回归测试；删除被替代的代码、文档和生成物；不要把 `.scratch/`、本地审计稿、benchmark 输出或 IDE 配置提交到 Git。

## 提交与评审

使用 Conventional Commits。提交说明应包含用户可见 contract、失败模型、兼容影响和验证证据，并明确配置、持久化格式、API response、sandbox policy、release 内容或第三方 revision 的变化。Markdown 自然段和列表项保持一个物理行，不按列宽手动换行。
