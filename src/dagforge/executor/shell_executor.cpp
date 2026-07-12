#include "dagforge/executor/executor.hpp"
#include "dagforge/executor/executor_state.hpp"
#include "dagforge/executor/executor_utils.hpp"
#include "dagforge/executor/process_launch.hpp"
#include "dagforge/executor/process_management.hpp"
#include "dagforge/util/log.hpp"

#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/readable_pipe.hpp>
#include <boost/process/v2/process.hpp>
#include <boost/process/v2/stdio.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <cstdlib>
#include <experimental/scope>
#include <format>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace dagforge {

namespace {

namespace bp = boost::process::v2;

inline constexpr std::size_t kMaxOutputSize = 10UZ * 1024 * 1024;
inline constexpr std::size_t kReadBufferSize = 4096;
inline constexpr std::size_t kInitialOutputReserve = 8192;
inline constexpr int kTimeoutExitCode = kExitCodeTimeout;
inline constexpr auto kHeartbeatInterval = std::chrono::seconds(1);

// is_valid_env_key is in executor_utils.hpp
using ::dagforge::is_valid_env_key;

using ShellShardState = ExecutorShardState<ActiveProcess>;

struct ExecutionContext {
  ShellShardState *state{};

  auto register_process(const InstanceId &id, pid_t pid) const noexcept
      -> void {
    state->register_active(id, ActiveProcess{.pid = pid});
  }

  auto unregister_process(const InstanceId &id) const noexcept -> void {
    state->unregister_active(id);
  }

  [[nodiscard]] auto find_process(const InstanceId &id) const noexcept
      -> std::optional<ActiveProcess> {
    return state->find_active(id);
  }
};

auto emit_stream_line(ExecutionSink &sink, const InstanceId &instance_id,
                      std::string_view stream, std::string_view line,
                      bool &streamed_any) -> void {
  if (stream == "stdout") {
    if (sink.on_stdout) {
      streamed_any = true;
      sink.on_stdout(instance_id, line);
    }
    return;
  }
  if (sink.on_stderr) {
    streamed_any = true;
    sink.on_stderr(instance_id, line);
  }
}

auto emit_heartbeat(
    const std::shared_ptr<ExecutorHeartbeatCallback> &heartbeat_callback,
    const InstanceId &instance_id) -> void {
  if (heartbeat_callback && *heartbeat_callback) {
    (*heartbeat_callback)(instance_id);
  }
}

auto run_executor_heartbeat(
    std::shared_ptr<ExecutorHeartbeatCallback> heartbeat_callback,
    std::shared_ptr<std::atomic_bool> stop, InstanceId instance_id)
    -> spawn_task {
  if (!heartbeat_callback || !*heartbeat_callback) {
    co_return;
  }

  while (!stop->load(std::memory_order_acquire)) {
    try {
      co_await async_sleep_on_timing_wheel(kHeartbeatInterval);
    } catch (const std::exception &) {
      co_return;
    }
    if (stop->load(std::memory_order_acquire)) {
      co_return;
    }
    (*heartbeat_callback)(instance_id);
  }
}

[[nodiscard]] auto read_pipe_all(boost::asio::readable_pipe &pipe,
                                 pmr::string &out,
                                 boost::asio::cancellation_signal &cancel_sig,
                                 const InstanceId &instance_id,
                                 ExecutionSink &sink, std::string stream,
                                 bool &streamed_any)
    -> task<void> {
  std::array<char, kReadBufferSize> buffer{};
  std::string pending_line;
  pending_line.reserve(kReadBufferSize);

  auto flush_complete_lines = [&]() {
    std::size_t start = 0;
    while (true) {
      const auto newline = pending_line.find('\n', start);
      if (newline == std::string::npos) {
        break;
      }
      auto line = std::string_view(pending_line).substr(start, newline - start);
      emit_stream_line(sink, instance_id, stream, line, streamed_any);
      start = newline + 1;
    }
    if (start > 0) {
      pending_line.erase(0, start);
    }
  };

  while (true) {
    auto read_res = co_await co_as_result(pipe.async_read_some(
        boost::asio::buffer(buffer.data(), buffer.size()),
        boost::asio::bind_cancellation_slot(cancel_sig.slot(), use_nothrow)));
    if (!read_res) {
      if (!pending_line.empty()) {
        emit_stream_line(sink, instance_id, stream, pending_line, streamed_any);
      }
      co_return;
    }
    const auto bytes = *read_res;
    if (bytes > 0 && out.size() < kMaxOutputSize) {
      const auto remaining = kMaxOutputSize - out.size();
      const auto to_append = std::min<std::size_t>(remaining, bytes);
      out.append(buffer.data(), to_append);
    }
    if (bytes > 0) {
      pending_line.append(buffer.data(), bytes);
      flush_complete_lines();
    }
  }
}

[[nodiscard]] auto
wait_process_with_timeout(bp::process &proc, std::chrono::seconds timeout,
                          boost::asio::cancellation_signal &cancel_sig)
    -> task<Result<ProcessWaitResult>> {
  using namespace boost::asio::experimental::awaitable_operators;

  auto outcome = co_await (reap_process(proc) ||
                           async_sleep_on_timing_wheel(timeout));
  if (outcome.index() == 0) {
    co_return std::move(std::get<0>(outcome));
  }

  cancel_sig.emit(boost::asio::cancellation_type::total);
  auto result_res = co_await terminate_and_reap_process(proc, true);
  if (!result_res) {
    co_return fail(result_res.error());
  }
  auto result = std::move(*result_res);
  result.exit_code = kTimeoutExitCode;
  co_return ok(std::move(result));
}

auto execute_command(std::string cmd, std::string working_dir,
                     std::optional<bp::process_environment> env,
                     std::chrono::seconds timeout, InstanceId instance_id,
                     ExecutionSink sink,
                     std::shared_ptr<ExecutorHeartbeatCallback>
                         heartbeat_callback,
                     ExecutionContext ctx,
                     std::shared_ptr<pmr::memory_resource> resource_owner,
                     Runtime &runtime)
    -> spawn_task {
  auto *resource = resource_owner != nullptr ? resource_owner.get()
                                             : current_memory_resource_or_default();
  auto &io = current_io_context();
  boost::asio::readable_pipe stdout_pipe(io);
  boost::asio::readable_pipe stderr_pipe(io);
  ExecutorResult result = make_executor_result(resource);
  result.stdout_output.reserve(kInitialOutputReserve);
  result.stderr_output.reserve(kInitialOutputReserve);
  auto heartbeat_stop = std::make_shared<std::atomic_bool>(false);
  const auto stop_heartbeat = std::experimental::scope_exit(
      [heartbeat_stop] { heartbeat_stop->store(true, std::memory_order_release); });
  emit_heartbeat(heartbeat_callback, instance_id);
  if (heartbeat_callback && *heartbeat_callback) {
    runtime.spawn(run_executor_heartbeat(heartbeat_callback, heartbeat_stop,
                                         instance_id.clone()));
  }

  std::optional<bp::process> proc;
  try {
    ProcessLaunchSpec spec{
        .args = {"-c", std::move(cmd)},
        .stdio = bp::process_stdio{
            .in = nullptr, .out = stdout_pipe, .err = stderr_pipe},
        .env = std::move(env),
        .working_dir = std::move(working_dir)};
    proc.emplace(launch_shell_process(io, std::move(spec)));
  } catch (const std::exception &ex) {
    result.exit_code = -1;
    result.error.assign(ex.what());
    if (sink.on_complete) {
      sink.on_complete(instance_id, std::move(result));
    }
    co_return;
  }

  const auto pid = proc->id();
  ctx.register_process(instance_id, pid);
  log::debug("shell process started pid={} instance_id={}", pid, instance_id);

  boost::asio::cancellation_signal cancel_sig;
  bool stdout_streamed = false;
  bool stderr_streamed = false;
  using namespace boost::asio::experimental::awaitable_operators;
  auto wait_result =
      co_await (read_pipe_all(stdout_pipe, result.stdout_output, cancel_sig,
                              instance_id, sink, "stdout",
                              stdout_streamed) &&
                read_pipe_all(stderr_pipe, result.stderr_output, cancel_sig,
                              instance_id, sink, "stderr",
                              stderr_streamed) &&
                wait_process_with_timeout(*proc, timeout, cancel_sig));

  result.stdout_streamed = stdout_streamed;
  result.stderr_streamed = stderr_streamed;
  if (!wait_result) {
    result.exit_code = -1;
    result.error = pmr::string(
        std::format("Failed to wait for process: {}",
                    wait_result.error().message()),
        resource);
  } else {
    auto &process_result = *wait_result;
    result.timed_out = process_result.timed_out;
    result.exit_code = process_result.exit_code;
    if (result.timed_out) {
      result.error = pmr::string("Execution timeout", resource);
      if (process_result.error) {
        result.error = pmr::string(
            std::format("Execution timeout: {}",
                        process_result.error.message()),
            resource);
      }
    } else if (process_result.error) {
      result.error = pmr::string(
          std::format("Failed to wait for process: {}",
                      process_result.error.message()),
          resource);
    }
  }

  ctx.unregister_process(instance_id);
  log::debug("shell finish: instance_id={} exit_code={} timed_out={} err='{}'",
             instance_id, result.exit_code, result.timed_out, result.error);
  if (sink.on_complete) {
    sink.on_complete(instance_id, std::move(result));
  }
  co_return;
}

} // namespace

class ShellExecutor final : public IExecutor {
public:
  explicit ShellExecutor(Runtime &rt)
      : runtime_{&rt}, shard_states_(rt.shard_count()) {}

  ShellExecutor(ShellExecutor &&) noexcept = delete;
  ShellExecutor &operator=(ShellExecutor &&) = delete;
  ShellExecutor(const ShellExecutor &) = delete;
  ShellExecutor &operator=(const ShellExecutor &) = delete;

  auto start(ExecutorRequest req, ExecutionSink sink) -> Result<void> override {
    const auto *shell = req.config.as<ShellExecutorConfig>();
    if (!shell) {
      return fail(Error::InvalidArgument);
    }
    auto resource_owner = req.memory_resource;

    log::debug("ShellExecutor start: instance_id={} timeout={}s cmd='{}'",
               req.instance_id, req.execution_timeout.count(),
               cmd_preview(req.command));

    std::string cmd = req.command;
    std::optional<bp::process_environment> env;
    if (!shell->env.empty()) {
      for (const auto &[key, value] : shell->env) {
        if (!is_valid_env_key(key)) {
          log::error("Invalid environment variable key: {}", key);
          return fail(Error::InvalidArgument);
        }
      }
      env = build_process_env(shell->env);
    }

    auto sid = runtime_->is_current_shard() ? runtime_->current_shard() : 0;
    std::shared_ptr<ExecutorHeartbeatCallback> heartbeat_callback;
    if (sink.on_heartbeat) {
      heartbeat_callback = std::make_shared<ExecutorHeartbeatCallback>(
          std::move(sink.on_heartbeat));
    }
    auto t = execute_command(std::move(cmd), req.working_dir, std::move(env),
                             req.execution_timeout, req.instance_id,
                             std::move(sink), std::move(heartbeat_callback),
                             ExecutionContext{.state = &shard_states_[sid]},
                             resource_owner, *runtime_);
    runtime_->spawn(std::move(t));
    return ok();
  }

  auto cancel(const InstanceId &instance_id) -> void override {
    cancel_on_all_shards(*runtime_, shard_states_, instance_id,
                         [](ShellShardState &state, const InstanceId &id) {
                           auto it = state.find_active_mut(id);
                           if (it == state.active_end() || it->second.pid <= 0) {
                             return;
                           }
                           kill_process_group_or_process(it->second.pid);
                           log::debug("Cancelled process for instance {}", id);
                         });
  }

private:
  Runtime *runtime_;
  std::vector<ShellShardState> shard_states_;
};

auto create_shell_executor(Runtime &rt) -> std::unique_ptr<IExecutor> {
  return std::make_unique<ShellExecutor>(rt);
}

} // namespace dagforge
