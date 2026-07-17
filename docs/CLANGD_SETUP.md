# clangd 与 VS Code 配置

DAGForge 使用 build2、C++23 和 C++ modules。正式构建可以使用 GCC，但 clangd 必须读取由同一 Clang major version 生成的 PCM 和 `compile_commands.json`，不能加载 GCC `.gcm`。

## 依赖

安装 Clang、clangd 和 Bear，Clang 与 clangd major version 必须一致，推荐 20 或更高版本。还需要 build2/bdep、项目系统依赖和 Python 3。

## 生成 IDE 构建

```bash
bash scripts/setup-clangd.sh
```

需要指定版本时使用：

```bash
bash scripts/setup-clangd.sh --compiler /usr/bin/clang++-21 --clangd /usr/bin/clangd-21
```

脚本会重建独立 `@clangd` build2 configuration、生成匹配的 PCM、捕获真实编译命令并更新 `.clangd-tools/clangd`。`compile_commands.json`、`.clangd-tools/` 和 clangd build directory 都是本地生成物，不提交到 Git。

## 何时刷新

module declaration/import、buildfile、compiler flag、include path、Clang major version 或 C/C++ source list 变化后重新运行脚本；普通函数实现修改不需要重建 compilation database。

## 常见问题

- 缺少 `compile_commands.json`：运行 `bash scripts/setup-clangd.sh`，普通 GCC build 不会生成 Clang compilation database。
- Clang 与 clangd 不匹配：显式传入同一 major version 的 executable，PCM 与 compiler version 不兼容。
- VS Code 仍使用全局 clangd：检查扩展输出，路径应指向 `${workspaceFolder}/.clangd-tools/clangd`，必要时重启 language server。
- 重复 module diagnostics：不要启用额外 experimental module scanner；build2 command 已包含精确 `-fmodule-file` 参数。
- third-party diagnostics：`.clangd` 会限制 `third_party/` 的 background indexing，但从第一方模板实例化暴露的真实错误仍会显示。
