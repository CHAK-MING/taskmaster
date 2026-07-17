# C++ 编码规范

## 格式化

仓库根目录 `.clang-format` 是唯一格式权威。修改第一方 C++ 后运行 `bash scripts/format.sh`，检查模式使用 `bash scripts/format.sh --check`；默认只处理相对 `HEAD` 新增或修改的文件，`--all` 仅用于有意的全仓格式化任务。禁止格式化 `third_party/`、生成文件或与任务无关的旧代码。

## 命名

- 类型、concept、枚举和值类型使用 `PascalCase`；长期多态 seam 优先使用项目已有的 `I` 前缀约定，已有稳定名称不得为统一外观而机械改名。
- 函数、方法、局部变量、参数和命名空间使用 `snake_case`。
- 成员变量使用尾随下划线，文件或类级常量使用 `kPascalCase`，枚举值使用 `PascalCase`。
- 文件名使用 `snake_case`；名称优先表达领域含义，不使用模糊的 `data`、`manager`、`helper`、`common` 或自创缩写。

## 头文件与 include

- 每个公共头必须自包含、使用 `#pragma once`，并把 textual includes 放在 `DAGFORGE_BUILDING_MODULE_INTERFACE` guard 内。
- `.cpp` 的 include 顺序是对应头、其他 DAGForge 头、第三方头、标准库头，各组之间空一行；`.clang-format` 不自动重排 include。
- 头文件禁止文件作用域 `using namespace`，禁止依赖传递 include，禁止把只为一个实现服务的声明放入公共接口。
- public 成员在前，protected 次之，private 最后；单参数构造函数默认 `explicit`，可失败初始化使用返回 `Result<T>` 的工厂。

## C++23

- 项目最低标准是 C++23，不提供旧标准兼容层。
- 优先使用 `std::expected`、`std::move_only_function`、`std::source_location`、ranges、concepts、`<=>` 和 C++23 chrono；只有当前标准库缺失实现时才通过项目自有 seam 提供兼容。
- 不能因为“使用新语法”而机械重写清晰代码；新特性必须减少重复、保护 invariant 或明确所有权。
- 资源使用 RAII，禁止裸 `new/delete`；非拥有视图如 `string_view`、`span` 和引用不得越过不受控生命周期或协程挂起点。

## 错误、解析与 JSON

- 预期失败返回 `Result<T>`；成功用 `ok(...)`，失败用 `fail(...)`。异常只处理无法在当前接口中合理表达的第三方异常边界，并必须转换为稳定错误。
- 解析函数返回能区分空输入、非法字符、越界和尾随内容的结果，不用默认值表示失败。
- JSON 读写只经过 `dagforge/util/json.hpp`；写失败必须传播，禁止返回 `null`、空对象或其他合法 JSON 伪装失败。
- Typed ID 的非可信输入必须经过 `parse()`；`from_trusted()` 仅用于调用方已经建立并能说明 invariant 的内部路径。
- enum 的字符串映射由项目 `EnumTraits` 拥有，Glaze 只消费同一份 metadata；未知 token 返回失败，不静默回退默认枚举。

## 协程与并发

- 异步接口通常返回 `task<Result<T>>`，fire-and-forget 只允许在生命周期拥有者能追踪、取消并等待的后台任务中使用。
- 新 Asio 操作使用 `use_nothrow` 和 `co_as_result(...)`，统一 Boost/errno 错误、取消和 timeout 语义。
- owner-shard 可变状态不加跨 shard 直接访问；消息发送后捕获值必须拥有，不能捕获会先销毁的栈引用。

## 注释与文档

注释解释约束、权衡和外部行为，不复述代码，不记录编辑过程，不保留注释掉的旧实现。Markdown 自然段和列表项保持单个物理行，代码块按真实命令和语法排版。
