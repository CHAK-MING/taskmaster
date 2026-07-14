#include "dagforge/executor/command_executor.hpp"

#include "detail/command_validation.hpp"
#include "detail/shard_state.hpp"
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
#include <cerrno>
#include <chrono>
#include <cctype>
#include <cstdlib>
#include <experimental/scope>
#include <filesystem>
#include <format>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#if defined(__linux__)
#include <linux/landlock.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

namespace dagforge {
namespace {

namespace bp = boost::process::v2;
namespace fs = std::filesystem;

inline constexpr std::size_t kMaxOutputSize = 10UZ * 1024 * 1024;
inline constexpr std::size_t kReadBufferSize = 4096;
inline constexpr std::size_t kInitialOutputReserve = 8192;
inline constexpr int kTimeoutExitCode = kExitCodeTimeout;
inline constexpr auto kHeartbeatInterval = std::chrono::seconds(1);
inline constexpr std::string_view kSandboxPath =
    "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";
inline constexpr std::array<std::string_view, 3> kReservedEnvironment{
    "HOME", "PATH", "TMPDIR"};

using CommandShardState =
    executor_detail::ShardExecutionState<ActiveProcess>;

struct ExecutionContext {
  CommandShardState *state{};

  auto register_process(const InstanceId &id, pid_t pid) const noexcept
      -> void {
    state->register_active(id, ActiveProcess{.pid = pid});
  }

  auto unregister_process(const InstanceId &id) const noexcept -> void {
    state->unregister_active(id);
  }
};

[[nodiscard]] auto expand_user_path(std::string_view value) -> fs::path {
  if (!value.starts_with("~/")) {
    return fs::path(value);
  }
  const char *home = std::getenv("HOME");
  if (home == nullptr || *home == '\0') {
    return fs::path(value);
  }
  return fs::path(home) / value.substr(2);
}

[[nodiscard]] auto path_is_within(const fs::path &path,
                                  const fs::path &root) -> bool {
  const auto relative = path.lexically_relative(root);
  return !relative.empty() && !relative.is_absolute() &&
         *relative.begin() != "..";
}

[[nodiscard]] auto safe_instance_name(std::string_view value) -> bool {
  return !value.empty() && std::ranges::all_of(value, [](unsigned char ch) {
           return std::isalnum(ch) != 0 || ch == '-' || ch == '_' || ch == '.';
         });
}

[[nodiscard]] auto landlock_available() noexcept -> bool {
#if defined(__linux__) && defined(SYS_landlock_create_ruleset)
  errno = 0;
  const auto abi = ::syscall(SYS_landlock_create_ruleset, nullptr, 0,
                             LANDLOCK_CREATE_RULESET_VERSION);
  return abi > 0;
#else
  return false;
#endif
}

[[nodiscard]] auto resolve_regular_file(std::string_view configured,
                                        bool require_executable)
    -> Result<fs::path> {
  std::error_code error;
  auto path = fs::absolute(expand_user_path(configured), error);
  if (error) {
    return fail(error);
  }
  path = fs::weakly_canonical(path, error);
  if (error || !fs::is_regular_file(path, error)) {
    return fail(Error::NotFound);
  }
#if defined(__linux__)
  if (require_executable && ::access(path.c_str(), X_OK) != 0) {
    return fail(Error::Unauthorized);
  }
#else
  (void)require_executable;
#endif
  return ok(std::move(path));
}

[[nodiscard]] auto resolve_workspace(const SandboxConfig &sandbox,
                                     const InstanceId &instance_id)
    -> Result<fs::path> {
  if (!safe_instance_name(instance_id.value())) {
    return fail(Error::InvalidArgument);
  }

  std::error_code error;
  auto root = fs::absolute(expand_user_path(sandbox.workspace_root), error);
  if (error) {
    return fail(error);
  }
  fs::create_directories(root, error);
  if (error) {
    return fail(error);
  }
  root = fs::canonical(root, error);
  if (error) {
    return fail(error);
  }
  auto temporary_root = fs::canonical(fs::temp_directory_path(error), error);
  if (error || path_is_within(root, temporary_root)) {
    return fail(Error::InvalidArgument);
  }

  auto workspace = root / instance_id.str();
  fs::create_directories(workspace, error);
  if (error) {
    return fail(error);
  }
  workspace = fs::canonical(workspace, error);
  if (error || !path_is_within(workspace, root)) {
    return fail(Error::Unauthorized);
  }

  return ok(std::move(workspace));
}

[[nodiscard]] auto resolve_executable_program(const fs::path &program)
    -> Result<fs::path> {
  std::error_code error;
  auto resolved = fs::canonical(program, error);
  if (error || !fs::is_regular_file(resolved, error) ||
      ::access(resolved.c_str(), X_OK) != 0) {
    return fail(Error::Unauthorized);
  }
  return ok(std::move(resolved));
}

auto add_existing_path(std::vector<std::string> &args,
                       std::string_view option, const fs::path &path) -> void {
  std::error_code error;
  if (fs::exists(path, error)) {
    args.emplace_back(option);
    args.push_back(path.string());
  }
}

[[nodiscard]] auto make_rlimit(std::string_view name, std::uint64_t value)
    -> std::string {
  return std::format("{},{},{}", name, value, value);
}

[[nodiscard]] auto build_sandbox_environment(
    const fs::path &workspace, const CommandSpec &command)
    -> bp::process_environment {
  std::vector<std::pair<std::string, std::string>> environment;
  environment.reserve(command.environment.size() + 3);
  environment.emplace_back("PATH", kSandboxPath);
  environment.emplace_back("HOME", workspace.string());
  environment.emplace_back("TMPDIR", "/tmp");
  for (const auto &[key, value] : command.environment) {
    environment.emplace_back(key, value);
  }
  return bp::process_environment(std::move(environment));
}

[[nodiscard]] auto build_sandbox_arguments(
    const SandboxConfig &sandbox, const fs::path &seccomp_bpf,
    const fs::path &workspace, const CommandSpec &command,
    std::chrono::seconds timeout) -> Result<std::vector<std::string>> {
  if (command.program.empty() || command.program.contains('\0') ||
      !fs::path(command.program).is_absolute()) {
    return fail(Error::Unauthorized);
  }
  auto program = resolve_executable_program(command.program);
  if (!program) {
    return fail(program.error());
  }
  if (std::ranges::any_of(command.arguments, [](const auto &argument) {
        return argument.contains('\0');
      })) {
    return fail(Error::InvalidArgument);
  }
  for (const auto &[key, value] : command.environment) {
    if (!executor_detail::is_valid_environment_key(key) ||
        value.contains('\0') ||
        std::ranges::find(kReservedEnvironment, key) !=
            kReservedEnvironment.end()) {
      return fail(Error::InvalidArgument);
    }
  }

  std::vector<std::string> args{
      "--logging=syslog", "-T", "static", "-U", "-m", "-M", "-e",
      "-l", "--uts=dagforge", "-N", "-v", "-n",
      std::format("-t{}", sandbox.tmp_bytes), "--fs-default-paths"};

  add_existing_path(args, "--fs-path-rx", "/usr/local/bin");
  add_existing_path(args, "--fs-path-rx", "/usr/local/lib");
  add_existing_path(args, "--fs-path-rx", "/usr/local/lib64");
  add_existing_path(args, "--fs-path-ro", "/etc/ld.so.cache");
  add_existing_path(args, "--fs-path-ro", "/etc/passwd");
  add_existing_path(args, "--fs-path-ro", "/etc/group");
  add_existing_path(args, "--fs-path-ro", "/etc/nsswitch.conf");
  add_existing_path(args, "--fs-path-ro", "/etc/localtime");
  add_existing_path(args, "--fs-path-ro", "/proc");
  add_existing_path(args, "--fs-path-rw", "/dev/null");
  add_existing_path(args, "--fs-path-ro", "/dev/urandom");
  add_existing_path(args, "--fs-path-ro", "/dev/random");
  args.emplace_back("--fs-path-advanced-rw");
  args.emplace_back("/tmp");
  args.emplace_back("--fs-path-advanced-rw");
  args.push_back(workspace.string());

  args.emplace_back("--seccomp-bpf-binary");
  args.push_back(seccomp_bpf.string());
  args.emplace_back("-R");
  args.push_back(make_rlimit("RLIMIT_AS", sandbox.max_memory_bytes));
  args.emplace_back("-R");
  args.push_back(make_rlimit("RLIMIT_FSIZE", sandbox.max_file_bytes));
  args.emplace_back("-R");
  args.push_back(make_rlimit("RLIMIT_NPROC", sandbox.max_processes));
  args.emplace_back("-R");
  args.push_back(make_rlimit("RLIMIT_NOFILE", sandbox.max_open_files));
  args.emplace_back("-R");
  args.emplace_back("RLIMIT_CORE,0,0");
  args.emplace_back("-R");
  args.push_back(make_rlimit(
      "RLIMIT_CPU", static_cast<std::uint64_t>(std::max<std::int64_t>(
                        1, timeout.count()))));

  args.emplace_back("--");
  args.push_back(program->string());
  args.insert(args.end(), command.arguments.begin(), command.arguments.end());
  return ok(std::move(args));
}

auto emit_stream_line(CommandExecutionSink &sink,
                      const InstanceId &instance_id,
                      std::string_view stream, std::string_view line,
                      bool &streamed_any) -> void {
  auto &callback = stream == "stdout" ? sink.on_stdout : sink.on_stderr;
  if (callback) {
    streamed_any = true;
    callback(instance_id, line);
  }
}

auto emit_heartbeat(
    const std::shared_ptr<CommandHeartbeatCallback> &heartbeat_callback,
    const InstanceId &instance_id) -> void {
  if (heartbeat_callback && *heartbeat_callback) {
    (*heartbeat_callback)(instance_id);
  }
}

auto run_executor_heartbeat(
    std::shared_ptr<CommandHeartbeatCallback> heartbeat_callback,
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
    if (!stop->load(std::memory_order_acquire)) {
      (*heartbeat_callback)(instance_id);
    }
  }
}

[[nodiscard]] auto read_pipe_all(boost::asio::readable_pipe &pipe,
                                 pmr::string &out,
                                 boost::asio::cancellation_signal &cancel_sig,
                                 const InstanceId &instance_id,
                                 CommandExecutionSink &sink,
                                 std::string stream,
                                 bool &streamed_any) -> task<void> {
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
      emit_stream_line(sink, instance_id, stream,
                       std::string_view(pending_line).substr(start,
                                                             newline - start),
                       streamed_any);
      start = newline + 1;
    }
    if (start > 0) {
      pending_line.erase(0, start);
    }
  };

  while (true) {
    auto read_result = co_await co_as_result(pipe.async_read_some(
        boost::asio::buffer(buffer),
        boost::asio::bind_cancellation_slot(cancel_sig.slot(), use_nothrow)));
    if (!read_result) {
      if (!pending_line.empty()) {
        emit_stream_line(sink, instance_id, stream, pending_line, streamed_any);
      }
      co_return;
    }
    const auto bytes = *read_result;
    if (bytes > 0 && out.size() < kMaxOutputSize) {
      const auto remaining = kMaxOutputSize - out.size();
      out.append(buffer.data(), std::min<std::size_t>(remaining, bytes));
    }
    if (bytes > 0) {
      pending_line.append(buffer.data(), bytes);
      flush_complete_lines();
    }
  }
}

[[nodiscard]] auto
wait_process_with_timeout(bp::process &process, std::chrono::seconds timeout,
                          boost::asio::cancellation_signal &cancel_signal)
    -> task<Result<ProcessWaitResult>> {
  using namespace boost::asio::experimental::awaitable_operators;
  auto outcome =
      co_await (reap_process(process) || async_sleep_on_timing_wheel(timeout));
  if (outcome.index() == 0) {
    co_return std::move(std::get<0>(outcome));
  }

  cancel_signal.emit(boost::asio::cancellation_type::total);
  auto result = co_await terminate_and_reap_process(process, true);
  if (!result) {
    co_return fail(result.error());
  }
  result->exit_code = kTimeoutExitCode;
  co_return result;
}

auto execute_command(fs::path minijail, std::vector<std::string> arguments,
                     bp::process_environment environment, fs::path workspace,
                     std::chrono::seconds timeout, InstanceId instance_id,
                     CommandExecutionSink sink,
                     std::shared_ptr<CommandHeartbeatCallback> heartbeat,
                     ExecutionContext context,
                     std::shared_ptr<pmr::memory_resource> resource_owner,
                     Runtime &runtime) -> spawn_task {
  auto *resource = resource_owner != nullptr
                       ? resource_owner.get()
                       : current_memory_resource_or_default();
  auto &io = current_io_context();
  boost::asio::readable_pipe stdout_pipe(io);
  boost::asio::readable_pipe stderr_pipe(io);
  auto result = make_command_execution_result(resource);
  result.stdout_output.reserve(kInitialOutputReserve);
  result.stderr_output.reserve(kInitialOutputReserve);

  auto heartbeat_stop = std::make_shared<std::atomic_bool>(false);
  const auto stop_heartbeat = std::experimental::scope_exit([heartbeat_stop] {
    heartbeat_stop->store(true, std::memory_order_release);
  });
  emit_heartbeat(heartbeat, instance_id);
  if (heartbeat && *heartbeat) {
    runtime.spawn(run_executor_heartbeat(heartbeat, heartbeat_stop,
                                         instance_id.clone()));
  }

  std::optional<bp::process> process;
  try {
    process.emplace(launch_process(
        io, ProcessLaunchSpec{
                .program = minijail.string(),
                .args = std::move(arguments),
                .stdio = bp::process_stdio{.in = nullptr,
                                           .out = stdout_pipe,
                                           .err = stderr_pipe},
                .env = std::move(environment),
                .working_dir = workspace.string()}));
  } catch (const std::exception &error) {
    result.exit_code = -1;
    result.error.assign(error.what());
    if (sink.on_complete) {
      sink.on_complete(instance_id, std::move(result));
    }
    co_return;
  }

  context.register_process(instance_id, process->id());
  if (sink.on_state) {
    sink.on_state(instance_id, "running");
  }
  log::debug("sandboxed command started pid={} instance_id={}", process->id(),
             instance_id);

  boost::asio::cancellation_signal cancel_signal;
  bool stdout_streamed = false;
  bool stderr_streamed = false;
  using namespace boost::asio::experimental::awaitable_operators;
  auto wait_result =
      co_await (read_pipe_all(stdout_pipe, result.stdout_output, cancel_signal,
                              instance_id, sink, "stdout", stdout_streamed) &&
                read_pipe_all(stderr_pipe, result.stderr_output, cancel_signal,
                              instance_id, sink, "stderr", stderr_streamed) &&
                wait_process_with_timeout(*process, timeout, cancel_signal));

  result.stdout_streamed = stdout_streamed;
  result.stderr_streamed = stderr_streamed;
  if (!wait_result) {
    result.exit_code = -1;
    result.error = pmr::string(
        std::format("Failed to wait for sandbox: {}",
                    wait_result.error().message()),
        resource);
  } else {
    result.timed_out = wait_result->timed_out;
    result.exit_code = wait_result->exit_code;
    if (result.timed_out) {
      result.error = pmr::string("Execution timeout", resource);
    } else if (wait_result->error) {
      result.error = pmr::string(
          std::format("Sandbox process failed: {}",
                      wait_result->error.message()),
          resource);
    }
  }

  context.unregister_process(instance_id);
  log::debug(
      "sandboxed command finished instance_id={} exit_code={} timed_out={}",
      instance_id, result.exit_code, result.timed_out);
  if (sink.on_complete) {
    sink.on_complete(instance_id, std::move(result));
  }
}

class MinijailCommandExecutor final : public ICommandExecutor {
public:
  MinijailCommandExecutor(Runtime &runtime, SandboxConfig sandbox)
      : runtime_(&runtime), sandbox_(std::move(sandbox)),
        shard_states_(runtime.shard_count()) {}

  auto start(CommandExecutionRequest request, CommandExecutionSink sink)
      -> Result<void> override {
    if (!landlock_available()) {
      return fail(Error::Unsupported);
    }
    auto minijail = resolve_regular_file(sandbox_.minijail_path, true);
    if (!minijail) {
      return fail(minijail.error());
    }
    auto seccomp = resolve_regular_file(sandbox_.seccomp_bpf_path, false);
    if (!seccomp) {
      return fail(seccomp.error());
    }
    auto workspace = resolve_workspace(sandbox_, request.instance_id);
    if (!workspace) {
      return fail(workspace.error());
    }
    auto arguments = build_sandbox_arguments(
        sandbox_, *seccomp, *workspace, request.command,
        request.execution_timeout);
    if (!arguments) {
      return fail(arguments.error());
    }
    auto environment =
        build_sandbox_environment(*workspace, request.command);

    log::debug("CommandExecutor start instance_id={} program='{}'",
               request.instance_id, request.command.program);
    auto shard = runtime_->is_current_shard() ? runtime_->current_shard() : 0;
    std::shared_ptr<CommandHeartbeatCallback> heartbeat;
    if (sink.on_heartbeat) {
      heartbeat = std::make_shared<CommandHeartbeatCallback>(
          std::move(sink.on_heartbeat));
    }
    runtime_->spawn(execute_command(
        std::move(*minijail), std::move(*arguments), std::move(environment),
        std::move(*workspace), request.execution_timeout, request.instance_id,
        CommandExecutionSink{.on_state = std::move(sink.on_state),
                             .on_stdout = std::move(sink.on_stdout),
                             .on_stderr = std::move(sink.on_stderr),
                             .on_complete = std::move(sink.on_complete)},
        std::move(heartbeat), ExecutionContext{.state = &shard_states_[shard]},
        request.memory_resource, *runtime_));
    return ok();
  }

  auto cancel(const InstanceId &instance_id) -> void override {
    executor_detail::cancel_on_all_shards(
        *runtime_, shard_states_, instance_id,
        [](CommandShardState &state, const InstanceId &id) {
          auto active = state.find_active_mut(id);
          if (active == state.active_end() || active->second.pid <= 0) {
            return;
          }
          kill_process_group_or_process(active->second.pid);
          log::debug("Cancelled sandbox for instance {}", id);
        });
  }

private:
  Runtime *runtime_;
  SandboxConfig sandbox_;
  std::vector<CommandShardState> shard_states_;
};

} // namespace

auto create_command_executor(Runtime &runtime, SandboxConfig sandbox)
    -> std::unique_ptr<ICommandExecutor> {
  return std::make_unique<MinijailCommandExecutor>(runtime,
                                                   std::move(sandbox));
}

} // namespace dagforge
