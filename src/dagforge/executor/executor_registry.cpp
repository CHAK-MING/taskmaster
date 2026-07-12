#include "dagforge/executor/executor.hpp"

#include <utility>

namespace dagforge {

auto ExecutorRegistry::instance() -> ExecutorRegistry & {
  static ExecutorRegistry registry = [] {
    ExecutorRegistry value;
    value.register_type(
        ExecutorType::Shell,
        [](Runtime &runtime) { return create_shell_executor(runtime); });
    value.register_type(
        ExecutorType::Docker,
        [](Runtime &runtime) { return create_docker_executor(runtime); });
    value.register_type(
        ExecutorType::Lua,
        [](Runtime &runtime) { return create_lua_executor(runtime); });
    value.register_type(
        ExecutorType::Noop,
        [](Runtime &runtime) { return create_noop_executor(runtime); });
    return value;
  }();
  return registry;
}

auto ExecutorRegistry::register_type(ExecutorType type, Creator creator)
    -> void {
  entries_[type] = Entry{.creator = std::move(creator)};
}

auto ExecutorRegistry::create(ExecutorType type, Runtime &runtime) const
    -> std::unique_ptr<IExecutor> {
  const auto it = entries_.find(type);
  if (it == entries_.end()) {
    return nullptr;
  }
  return it->second.creator(runtime);
}

auto ExecutorRegistry::registered_types() const -> std::vector<ExecutorType> {
  return entries_ | std::views::keys | std::ranges::to<std::vector>();
}

} // namespace dagforge
