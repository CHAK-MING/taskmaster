#pragma once

#include <chrono>
#include <coroutine>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>

namespace taskmaster {

enum class ExecutorType : std::uint8_t {
  Shell,
};

class ExecutorTypeRegistry {
public:
  static auto instance() -> ExecutorTypeRegistry& {
    static ExecutorTypeRegistry registry;
    return registry;
  }

  auto register_type(ExecutorType type, std::string_view name) -> void {
    type_to_name_[type] = name;
    name_to_type_.emplace(std::string(name), type);
  }

  [[nodiscard]] auto to_string(ExecutorType type) const noexcept
      -> std::string_view {
    auto it = type_to_name_.find(type);
    return it != type_to_name_.end() ? it->second : "unknown";
  }

  [[nodiscard]] auto from_string(std::string_view name) const noexcept
      -> ExecutorType {
    auto it = name_to_type_.find(std::string(name));
    return it != name_to_type_.end() ? it->second : ExecutorType::Shell;
  }

private:
  ExecutorTypeRegistry() {
    register_type(ExecutorType::Shell, "shell");
  }

  std::unordered_map<ExecutorType, std::string_view> type_to_name_;
  std::unordered_map<std::string, ExecutorType> name_to_type_;
};

[[nodiscard]] inline auto executor_type_to_string(ExecutorType type) noexcept
    -> std::string_view {
  return ExecutorTypeRegistry::instance().to_string(type);
}

[[nodiscard]] inline auto string_to_executor_type(std::string_view str) noexcept
    -> ExecutorType {
  return ExecutorTypeRegistry::instance().from_string(str);
}

struct ShellExecutorConfig {
  std::string command;
  std::string working_dir;
  std::chrono::seconds timeout{std::chrono::seconds(300)};
};

using ExecutorConfig = std::variant<ShellExecutorConfig>;

struct ExecutorResult {
  int exit_code{0};
  std::string stdout_output;
  std::string stderr_output;
  std::string error;
  bool timed_out{false};
};

using ExecutorCallback = std::move_only_function<void(
    const std::string& instance_id, ExecutorResult result)>;

class IExecutor {
public:
  virtual ~IExecutor() = default;

  virtual auto execute(const std::string& instance_id,
                       const ExecutorConfig& config, ExecutorCallback callback)
      -> void = 0;

  virtual auto cancel(std::string_view instance_id) -> void = 0;
};

class Runtime;  // Forward declaration

[[nodiscard]] auto create_shell_executor(Runtime& rt)
    -> std::unique_ptr<IExecutor>;

class ExecutorAwaiter {
public:
  ExecutorAwaiter(IExecutor& executor, std::string instance_id,
                  ExecutorConfig config)
      : executor_{executor},
        instance_id_{std::move(instance_id)},
        config_{std::move(config)} {
  }

  // Non-copyable and non-movable because lambda captures 'this' pointer
  ExecutorAwaiter(const ExecutorAwaiter&) = delete;
  ExecutorAwaiter& operator=(const ExecutorAwaiter&) = delete;
  ExecutorAwaiter(ExecutorAwaiter&&) = delete;
  ExecutorAwaiter& operator=(ExecutorAwaiter&&) = delete;

  [[nodiscard]] auto await_ready() const noexcept -> bool {
    return false;
  }

  auto await_suspend(std::coroutine_handle<> handle) -> void {
    executor_.execute(instance_id_, config_,
                      [this, handle](const std::string&, ExecutorResult res) {
                        result_ = std::move(res);
                        handle.resume();
                      });
  }

  [[nodiscard]] auto await_resume() noexcept -> ExecutorResult {
    return std::move(result_);
  }

private:
  IExecutor& executor_;
  std::string instance_id_;
  ExecutorConfig config_;
  ExecutorResult result_;
};

inline auto execute_async(IExecutor& executor, std::string instance_id,
                          ExecutorConfig config) -> ExecutorAwaiter {
  return ExecutorAwaiter{executor, std::move(instance_id), std::move(config)};
}

}  // namespace taskmaster
