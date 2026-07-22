#include "dagforge/sandbox/command_runner.hpp"

#include "../config/detail/executor_config_validation.hpp"
#include "detail/command_policy.hpp"
#include "detail/command_validation.hpp"
#include "detail/policy_command_runner.hpp"
#include "detail/minijail_command_runner.hpp"
#include "detail/process_management.hpp"
#include "dagforge/core/scope_exit.hpp"
#include "dagforge/io/context.hpp"
#include "dagforge/util/log.hpp"

#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/readable_pipe.hpp>
#include <boost/process/v2/environment.hpp>
#include <boost/process/v2/process.hpp>
#include <boost/process/v2/start_dir.hpp>
#include <boost/process/v2/stdio.hpp>
#include <boost/system/error_code.hpp>
#if defined(BOOST_PROCESS_V2_POSIX)
#include <boost/process/v2/posix/vfork_launcher.hpp>
#endif

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cctype>
#include <cstdlib>
#include <filesystem>
#include <format>
#include <memory>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#if defined(__linux__)
#include <linux/landlock.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

namespace dagforge::sandbox {
namespace {

namespace bp = boost::process::v2;
namespace fs = std::filesystem;

inline constexpr std::size_t kReadBufferSize = 4096;
inline constexpr std::size_t kInitialOutputReserve = 8192;
inline constexpr int kTimeoutExitCode = kExitCodeTimeout;
inline constexpr auto kHeartbeatInterval = std::chrono::seconds(1);
inline constexpr std::string_view kSandboxPath =
    "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

#if defined(BOOST_PROCESS_V2_POSIX)
struct NewProcessGroupInit {
  template <typename Launcher, typename PathLike>
  auto on_exec_setup(Launcher &, const PathLike &, const char *const *&)
      -> boost::system::error_code {
    if (::setpgid(0, 0) != 0) {
      return boost::system::error_code(errno,
                                       boost::system::generic_category());
    }
    return {};
  }
};
#endif

struct ProcessLaunchSpec {
  std::string program;
  std::vector<std::string> args;
  bp::process_stdio stdio;
  bp::process_environment env;
  std::string working_dir;
};

template <typename Launcher, typename... Inits>
auto invoke_process(Launcher &&launch, io::IoContext &io,
                    ProcessLaunchSpec spec, Inits &&... extra)
    -> bp::process {
  auto program = std::move(spec.program);
  return launch(io.native_handle(), program, std::move(spec.args),
                std::move(spec.stdio),
                bp::process_start_dir{std::move(spec.working_dir)},
                std::move(spec.env), std::forward<Inits>(extra)...);
}

template <typename... Inits>
auto launch_process(io::IoContext &io, ProcessLaunchSpec spec,
                    Inits &&... extra) -> bp::process {
#if defined(BOOST_PROCESS_V2_POSIX)
  bp::posix::vfork_launcher launcher;
  return invoke_process(launcher, io, std::move(spec), NewProcessGroupInit{},
                        std::forward<Inits>(extra)...);
#else
  auto launcher = [](auto &ctx, const char *command, auto args,
                     auto &&... init) {
    return bp::process(ctx, command, std::move(args),
                       std::forward<decltype(init)>(init)...);
  };
  return invoke_process(launcher, io, std::move(spec),
                        std::forward<Inits>(extra)...);
#endif
}

struct SandboxProcessRegistry {
  std::atomic_bool shutting_down{false};
  std::mutex mutex;
  std::condition_variable changed;
  std::unordered_map<std::string, pid_t> active;

  [[nodiscard]] auto register_process(const InstanceId &id, pid_t pid) -> bool {
    std::lock_guard lock(mutex);
    if (shutting_down.load(std::memory_order_acquire)) {
      return false;
    }
    return active.emplace(id.str(), pid).second;
  }

  auto unregister_process(const InstanceId &id) -> void {
    {
      std::lock_guard lock(mutex);
      active.erase(id.str());
    }
    changed.notify_all();
  }

  [[nodiscard]] auto find_process(const InstanceId &id) -> pid_t {
    std::lock_guard lock(mutex);
    const auto it = active.find(id.str());
    return it == active.end() ? -1 : it->second;
  }

  auto quiesce(std::chrono::milliseconds timeout) -> Result<void> {
    shutting_down.store(true, std::memory_order_release);
    std::vector<pid_t> processes;
    {
      std::lock_guard lock(mutex);
      processes.reserve(active.size());
      for (const auto &[_, pid] : active) {
        processes.push_back(pid);
      }
    }
    for (const auto pid : processes) {
      kill_process_group_or_process(pid);
    }
    std::unique_lock lock(mutex);
    if (!changed.wait_for(lock, timeout,
                          [this] { return active.empty(); })) {
      for (const auto &[_, pid] : active) {
        kill_process_group_or_process(pid);
      }
      log::error("Timed out while reaping {} sandbox process groups",
                 active.size());
      return fail(Error::Timeout);
    }
    return ok();
  }
};

struct ExecutionContext {
  std::shared_ptr<SandboxProcessRegistry> registry;

  [[nodiscard]] auto register_process(const InstanceId &id, pid_t pid) const
      -> bool {
    return registry->register_process(id, pid);
  }

  auto unregister_process(const InstanceId &id) const -> void {
    registry->unregister_process(id);
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
                                        bool require_executable,
                                        bool require_trusted_permissions)
    -> Result<fs::path> {
  std::error_code error;
  auto path = fs::absolute(expand_user_path(configured), error);
  if (error) {
    return fail(error);
  }
  return detail::trusted_regular_file(
      path, require_executable, require_trusted_permissions);
}

[[nodiscard]] auto prepare_execution_root(const config::MinijailConfig &sandbox)
    -> Result<fs::path> {
  std::error_code error;
  auto root = fs::absolute(expand_user_path(sandbox.execution_root), error);
  if (error) {
    return fail(error);
  }
  if (fs::exists(root, error) && fs::is_symlink(fs::symlink_status(root, error))) {
    return fail(Error::Unauthorized);
  }
  fs::create_directories(root, error);
  if (error) {
    return fail(error);
  }
  fs::permissions(root, fs::perms::owner_all, fs::perm_options::replace, error);
  if (error) {
    return fail(error);
  }
  root = fs::canonical(root, error);
  if (error || !fs::is_directory(root, error)) {
    return fail(error ? error : make_error_code(Error::InvalidArgument));
  }
  auto temporary_root = fs::canonical(fs::temp_directory_path(error), error);
  if (error || root == temporary_root || path_is_within(root, temporary_root)) {
    return fail(Error::InvalidArgument);
  }
  return ok(std::move(root));
}

[[nodiscard]] auto resolve_workdir(const fs::path &root,
                                   const InstanceId &instance_id)
    -> Result<fs::path> {
  if (!safe_instance_name(instance_id.value())) {
    return fail(Error::InvalidArgument);
  }

  std::error_code error;
  auto workdir = root / instance_id.str();
  if (fs::exists(workdir, error)) {
    if (error) {
      return fail(error);
    }
    return fail(Error::AlreadyExists);
  }
  if (!fs::create_directory(workdir, error)) {
    return fail(error ? error : make_error_code(Error::AlreadyExists));
  }
  if (error) {
    return fail(error);
  }
  fs::permissions(workdir, fs::perms::owner_all, fs::perm_options::replace,
                  error);
  if (error) {
    return fail(error);
  }
  workdir = fs::canonical(workdir, error);
  if (error || !path_is_within(workdir, root)) {
    return fail(Error::Unauthorized);
  }

  return ok(std::move(workdir));
}

[[nodiscard]] auto resolve_executable_program(const fs::path &program)
    -> Result<fs::path> {
  return detail::trusted_regular_file(program, true, false);
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
    const fs::path &workdir, const CommandSpec &command,
    const std::unordered_map<std::string, std::string> &inherited)
    -> bp::process_environment {
  std::map<std::string, std::string> configured(inherited.begin(),
                                                inherited.end());
  for (const auto &[key, value] : command.environment) {
    configured[key] = value;
  }

  std::vector<std::pair<std::string, std::string>> environment;
  environment.reserve(configured.size() + 3);
  environment.emplace_back("PATH", kSandboxPath);
  environment.emplace_back("HOME", workdir.string());
  environment.emplace_back("TMPDIR", "/tmp");
  for (const auto &[key, value] : configured) {
    environment.emplace_back(key, value);
  }
  return bp::process_environment(std::move(environment));
}

[[nodiscard]] auto build_sandbox_arguments(
    const config::MinijailConfig &sandbox, const fs::path &seccomp_bpf,
    const fs::path &workdir, const CommandSpec &command,
    std::chrono::seconds timeout) -> Result<std::vector<std::string>> {
  if (command.program.empty() || command.program.contains('\0') ||
      !fs::path(command.program).is_absolute()) {
    return fail(Error::Unauthorized);
  }
  auto program = resolve_executable_program(command.program);
  if (!program) {
    return fail(program.error());
  }
  if (!detail::command_arguments_are_safe(command)) {
    return fail(Error::InvalidArgument);
  }
  for (const auto &[key, value] : command.environment) {
    if (!detail::environment_entry_is_safe(key, value)) {
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
  args.push_back(workdir.string());

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

auto emit_stream_line(CommandRunSink &sink,
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
    auto sleep = co_await async_sleep_on_timing_wheel(kHeartbeatInterval);
    if (!sleep) {
      co_return;
    }
    if (!stop->load(std::memory_order_acquire)) {
      (*heartbeat_callback)(instance_id);
    }
  }
}

struct OutputLimitState {
  bool exceeded{false};
  std::string stream;
};

auto mark_output_limit_exceeded(
    OutputLimitState &limit_state, std::string_view stream, pid_t pid,
    boost::asio::cancellation_signal &cancel_signal) -> void {
  if (limit_state.exceeded) {
    return;
  }
  limit_state.exceeded = true;
  limit_state.stream.assign(stream);
  kill_process_group_or_process(pid);
  cancel_signal.emit(boost::asio::cancellation_type::total);
}

[[nodiscard]] auto read_pipe_all(boost::asio::readable_pipe &pipe,
                                 pmr::string &out,
                                 boost::asio::cancellation_signal &cancel_sig,
                                 const InstanceId &instance_id,
                                 CommandRunSink &sink,
                                 std::string stream,
                                 bool &streamed_any, pid_t pid,
                                 std::uint64_t max_output_bytes,
                                 std::uint64_t max_stream_line_bytes,
                                 OutputLimitState &limit_state) -> task<void> {
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
    if (bytes > max_output_bytes -
                    std::min<std::uint64_t>(out.size(), max_output_bytes)) {
      mark_output_limit_exceeded(limit_state, stream, pid, cancel_sig);
      co_return;
    }
    if (bytes > 0) {
      out.append(buffer.data(), bytes);
      pending_line.append(buffer.data(), bytes);
      flush_complete_lines();
      if (pending_line.size() > max_stream_line_bytes) {
        mark_output_limit_exceeded(limit_state, stream, pid, cancel_sig);
        co_return;
      }
    }
  }
}

[[nodiscard]] auto wait_process_with_timeout(
    bp::process &process, std::chrono::seconds timeout,
    std::shared_ptr<boost::asio::cancellation_signal> cancel_signal,
    Runtime &runtime) -> task<Result<ProcessWaitResult>> {
  struct DeadlineState {
    std::atomic_bool completed{false};
    std::atomic_bool timed_out{false};
  };

  auto state = std::make_shared<DeadlineState>();
  const auto shard = runtime.current_shard();
  const auto pid = process.id();
  const auto deadline = runtime.schedule_after_on(
      shard, timeout, [state, cancel_signal = std::move(cancel_signal), pid] {
        bool expected = false;
        if (!state->completed.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel,
                std::memory_order_acquire)) {
          return;
        }
        state->timed_out.store(true, std::memory_order_release);
        kill_process_group_or_process(pid);
        cancel_signal->emit(boost::asio::cancellation_type::total);
      });
  const auto cancel_deadline =
      dagforge::scope_exit([&runtime, shard, deadline] {
        runtime.cancel_after_on(shard, deadline);
      });

  auto result = co_await reap_process(process);
  state->completed.store(true, std::memory_order_release);
  if (!result) {
    co_return fail(result.error());
  }
  if (state->timed_out.load(std::memory_order_acquire)) {
    result->timed_out = true;
    result->exit_code = kTimeoutExitCode;
  }
  co_return result;
}

auto execute_command(fs::path minijail, std::vector<std::string> arguments,
                     bp::process_environment environment, fs::path workdir,
                     std::chrono::seconds timeout, InstanceId instance_id,
                     CommandRunSink sink,
                     std::shared_ptr<CommandHeartbeatCallback> heartbeat,
                     ExecutionContext context,
                     std::shared_ptr<pmr::memory_resource> resource_owner,
                     config::MinijailConfig sandbox, Runtime &runtime)
    -> spawn_task {
  auto *resource = resource_owner != nullptr
                       ? resource_owner.get()
                       : current_memory_resource_or_default();
  auto &io = current_io_context();
  boost::asio::readable_pipe stdout_pipe(io.native_handle());
  boost::asio::readable_pipe stderr_pipe(io.native_handle());
  auto result = make_command_run_result(resource);
  result.stdout_output.reserve(kInitialOutputReserve);
  result.stderr_output.reserve(kInitialOutputReserve);
  bool cleanup_pending = !sandbox.retain_workdirs;
  const auto cleanup_now = [&] {
    if (!std::exchange(cleanup_pending, false)) {
      return;
    }
    std::error_code ignored;
    fs::remove_all(workdir, ignored);
  };
  const auto cleanup_workdir =
      dagforge::scope_exit([&cleanup_now] { cleanup_now(); });

  auto heartbeat_stop = std::make_shared<std::atomic_bool>(false);
  const auto stop_heartbeat = dagforge::scope_exit([heartbeat_stop] {
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
                .working_dir = workdir.string()}));
  } catch (const std::exception &error) {
    result.exit_code = -1;
    result.error.assign(error.what());
    cleanup_now();
    if (sink.on_complete) {
      sink.on_complete(instance_id, std::move(result));
    }
    co_return;
  }

  if (!context.register_process(instance_id, process->id())) {
    auto stopped = co_await terminate_and_reap_process(*process);
    result.exit_code = stopped ? stopped->exit_code : -1;
    result.error.assign("Sandbox executor is shutting down");
    cleanup_now();
    if (sink.on_complete) {
      sink.on_complete(instance_id, std::move(result));
    }
    co_return;
  }
  if (sink.on_state) {
    sink.on_state(instance_id, "running");
  }
  log::debug("sandboxed command started pid={} instance_id={}", process->id(),
             instance_id);

  auto cancel_signal = std::make_shared<boost::asio::cancellation_signal>();
  bool stdout_streamed = false;
  bool stderr_streamed = false;
  OutputLimitState output_limit;
  using namespace boost::asio::experimental::awaitable_operators;
  auto wait_result = co_await (
      read_pipe_all(stdout_pipe, result.stdout_output, *cancel_signal,
                    instance_id, sink, "stdout", stdout_streamed, process->id(),
                    sandbox.max_stdout_bytes, sandbox.max_stream_line_bytes,
                    output_limit) &&
      read_pipe_all(stderr_pipe, result.stderr_output, *cancel_signal,
                    instance_id, sink, "stderr", stderr_streamed, process->id(),
                    sandbox.max_stderr_bytes, sandbox.max_stream_line_bytes,
                    output_limit) &&
      wait_process_with_timeout(*process, timeout, cancel_signal, runtime));

  result.stdout_streamed = stdout_streamed;
  result.stderr_streamed = stderr_streamed;
  if (output_limit.exceeded) {
    result.resource_exhausted = true;
    result.exit_code = wait_result ? wait_result->exit_code : -1;
    result.error = pmr::string(
        std::format("{} output exceeded configured limit", output_limit.stream),
        resource);
  } else if (!wait_result) {
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
  cleanup_now();
  if (sink.on_complete) {
    sink.on_complete(instance_id, std::move(result));
  }
}

class MinijailCommandRunner final : public ICommandRunner {
public:
  MinijailCommandRunner(Runtime &runtime, config::MinijailConfig config,
                        fs::path minijail, fs::path seccomp_bpf,
                        fs::path execution_root)
      : runtime_(&runtime), config_(std::move(config)),
        minijail_(std::move(minijail)), seccomp_bpf_(std::move(seccomp_bpf)),
        execution_root_(std::move(execution_root)),
        registry_(std::make_shared<SandboxProcessRegistry>()) {}

  ~MinijailCommandRunner() override {
    (void)quiesce(std::chrono::seconds(5));
  }

  auto prepare(CommandPreparationRequest request) const
      -> Result<CommandSpec> override {
    return request.deferred_environment_keys.empty()
               ? ok(std::move(request.command))
               : fail(Error::InvalidState);
  }

  auto start(CommandRunRequest request, CommandRunSink sink)
      -> Result<void> override {
    if (registry_->shutting_down.load(std::memory_order_acquire)) {
      return fail(Error::InvalidState);
    }
    auto workdir = resolve_workdir(execution_root_, request.instance_id);
    if (!workdir) {
      return fail(workdir.error());
    }
    bool cleanup_on_failure = !config_.retain_workdirs;
    const auto cleanup_workdir = dagforge::scope_exit([&] {
      if (cleanup_on_failure) {
        std::error_code ignored;
        fs::remove_all(*workdir, ignored);
      }
    });
    auto arguments = build_sandbox_arguments(
        config_, seccomp_bpf_, *workdir, request.command,
        request.execution_timeout);
    if (!arguments) {
      return fail(arguments.error());
    }
    auto environment = build_sandbox_environment(*workdir, request.command, {});

    log::debug("CommandExecutor start instance_id={} program='{}'",
               request.instance_id, request.command.program);
    std::shared_ptr<CommandHeartbeatCallback> heartbeat;
    if (sink.on_heartbeat) {
      heartbeat = std::make_shared<CommandHeartbeatCallback>(
          std::move(sink.on_heartbeat));
    }
    runtime_->spawn(execute_command(
        minijail_, std::move(*arguments), std::move(environment),
        std::move(*workdir), request.execution_timeout, request.instance_id,
        CommandRunSink{.on_state = std::move(sink.on_state),
                             .on_stdout = std::move(sink.on_stdout),
                             .on_stderr = std::move(sink.on_stderr),
                             .on_complete = std::move(sink.on_complete)},
        std::move(heartbeat), ExecutionContext{.registry = registry_},
        request.memory_resource, config_, *runtime_));
    cleanup_on_failure = false;
    return ok();
  }

  auto cancel(const InstanceId &instance_id) -> void override {
    const auto pid = registry_->find_process(instance_id);
    if (pid > 0) {
      kill_process_group_or_process(pid);
      log::debug("Cancelled sandbox for instance {}", instance_id);
    }
  }

  auto quiesce(std::chrono::milliseconds timeout) -> Result<void> override {
    return registry_->quiesce(timeout);
  }

private:
  Runtime *runtime_;
  config::MinijailConfig config_;
  fs::path minijail_;
  fs::path seccomp_bpf_;
  fs::path execution_root_;
  std::shared_ptr<SandboxProcessRegistry> registry_;
};

} // namespace

namespace detail {
auto create_minijail_command_runner(
    Runtime &runtime, config::MinijailConfig sandbox,
    config::CommandPolicyConfig policy)
    -> Result<std::unique_ptr<ICommandRunner>> {
  if (!landlock_available()) {
    return fail(Error::Unsupported);
  }
  if (!config::detail::minijail_resource_limits_valid(sandbox)) {
    return fail(Error::InvalidArgument);
  }
  auto minijail = resolve_regular_file(sandbox.executable, true,
                                       sandbox.require_trusted_files);
  if (!minijail) {
    return fail(minijail.error());
  }
  auto seccomp = resolve_regular_file(sandbox.seccomp_bpf_path, false,
                                      sandbox.require_trusted_files);
  if (!seccomp) {
    return fail(seccomp.error());
  }
  auto execution_root = prepare_execution_root(sandbox);
  if (!execution_root) {
    return fail(execution_root.error());
  }
  auto runner = std::unique_ptr<ICommandRunner>{
      std::make_unique<MinijailCommandRunner>(
          runtime, std::move(sandbox), std::move(*minijail),
          std::move(*seccomp), std::move(*execution_root))};
  return create_policy_command_runner(std::move(runner), std::move(policy));
}

} // namespace detail

} // namespace dagforge::sandbox
