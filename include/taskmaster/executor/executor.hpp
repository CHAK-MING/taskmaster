#pragma once

#include "taskmaster/core/runtime.hpp"
#include "taskmaster/util/id.hpp"
#include <chrono>
#include <cstdint>
#include <flat_map>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
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

  std::flat_map<ExecutorType, std::string_view> type_to_name_;
  std::flat_map<std::string, ExecutorType, std::less<>> name_to_type_;
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
  std::flat_map<std::string, std::string> env;
};

using ExecutorConfig = std::variant<ShellExecutorConfig>;

struct ExecutorResult {
  int exit_code{0};
  std::string stdout_output;
  std::string stderr_output;
  std::string error;
  bool timed_out{false};
};

struct ExecutorRequest {
  InstanceId instance_id;
  ExecutorConfig config;
};

struct ExecutionSink {
  std::move_only_function<void(const InstanceId& instance_id,
                               std::string_view message)>
      on_state;
  std::move_only_function<void(const InstanceId& instance_id,
                               std::string_view data)>
      on_stdout;
  std::move_only_function<void(const InstanceId& instance_id,
                               std::string_view data)>
      on_stderr;
  std::move_only_function<void(const InstanceId& instance_id,
                               ExecutorResult result)>
      on_complete;
};

class Runtime;
struct ExecutorContext {
  Runtime& runtime;
};

class IExecutor {
public:
  virtual ~IExecutor() = default;

  virtual auto start(ExecutorContext ctx, ExecutorRequest req,
                     ExecutionSink sink) -> void = 0;

  virtual auto cancel(const InstanceId& instance_id) -> void = 0;
};

class Runtime;

[[nodiscard]] auto create_shell_executor(Runtime& rt)
    -> std::unique_ptr<IExecutor>;

[[nodiscard]] auto create_noop_executor(Runtime& rt)
    -> std::unique_ptr<IExecutor>;

class ExecutorAwaiter {
public:
  ExecutorAwaiter(IExecutor& executor, InstanceId instance_id,
                  ExecutorConfig config, Runtime& runtime)
      : executor_{executor},
        instance_id_{std::move(instance_id)},
        config_{std::move(config)},
        runtime_{&runtime} {
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
    ExecutorContext ctx{.runtime = *runtime_};

    ExecutorRequest req{.instance_id = std::move(instance_id_),
                        .config = std::move(config_)};

    ExecutionSink sink;
    sink.on_complete =
        [this, handle](const InstanceId&, ExecutorResult res) mutable {
          result_ = std::move(res);
          runtime_->schedule(handle);
        };

    executor_.start(ctx, std::move(req), std::move(sink));
  }

  [[nodiscard]] auto await_resume() noexcept -> ExecutorResult {
    return std::move(result_);
  }

private:
  IExecutor& executor_;
  InstanceId instance_id_;
  ExecutorConfig config_;
  ExecutorResult result_;
  Runtime* runtime_;
};

inline auto execute_async(Runtime& runtime, IExecutor& executor,
                          InstanceId instance_id, ExecutorConfig config)
    -> ExecutorAwaiter {
  return ExecutorAwaiter{executor, std::move(instance_id), std::move(config),
                         runtime};
}

}  // namespace taskmaster
