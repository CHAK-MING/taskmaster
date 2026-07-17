# 验证与交付

## 开始前

- 运行 `git status --short`、`git branch --show-current` 和必要的 `git log`，确认基线、分支关系和本地改动。
- 共享检出区有未提交改动时新建 worktree；不得 reset、clean、checkout 或覆盖不属于当前任务的文件。
- 阅读相关测试、build2 注册、module interface 和 release 脚本，先确定证明完成的命令。

## 常用命令

- 格式化当前新增或修改的第一方 C++：`bash scripts/format.sh`
- 检查当前新增或修改的第一方 C++：`bash scripts/format.sh --check`
- Foundation 公共头与 C++23 能力门禁：`python3 scripts/check-foundation-contracts.py --compiler "${CXX:-g++}"`
- Agent 与代码约定：`bash scripts/check-agent-conventions.sh`
- Module graph：`bash scripts/check-module-graph.sh`
- 测试布局：`python3 scripts/check-test-layout.py`
- 快速验证：`bash scripts/test.sh quick`
- 完整功能验证：`bash scripts/test.sh all`
- Sanitizer 审计：`bash scripts/test-runtime-audit.sh`
- Coverage：`bash scripts/test-coverage.sh`
- Vendored dependency 完整性：`bash scripts/verify-vendored-deps.sh`

## 风险匹配

- 纯函数或 parser 修改必须有 focused unit test。
- 公共头、module、Result、JSON、ID、时间和错误域修改必须运行 foundation gate、module smoke 和相关 unit tests。
- Runtime、shard、协程、取消和 shutdown 修改必须运行 component tests，并根据风险运行 ASAN、TSAN 或 UBSAN。
- 持久化格式、恢复、Artifact、Evidence 或 Plan 修改必须覆盖损坏输入、版本、大小上限、原子提交和重启恢复。
- HTTP、TLS、sandbox 和进程管理修改必须运行对应 integration/e2e 场景；缺少系统依赖时明确记录未运行项，不得用 unit test 代替后宣称完成。

## 提交前

确认没有调试输出、生成物、未引用文件、`.scratch/` 内容、失效链接或与任务无关的格式化；运行 `git diff --check`，审阅 `git diff --stat` 和完整 diff，再以 Conventional Commits 提交。合并前要求工作树干净、目标分支是当前基线祖先、所有声明的验证真实通过。
