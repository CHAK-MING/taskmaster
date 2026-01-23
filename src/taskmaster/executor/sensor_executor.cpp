#include "taskmaster/core/coroutine.hpp"
#include "taskmaster/core/error.hpp"
#include "taskmaster/core/runtime.hpp"
#include "taskmaster/app/http/http_client.hpp"
#include "taskmaster/app/http/http_types.hpp"
#include "taskmaster/executor/executor.hpp"
#include "taskmaster/util/log.hpp"

#include <charconv>
#include <chrono>
#include <filesystem>
#include <optional>
#include <mutex>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

#include <signal.h>
#include <sys/wait.h>

#include <unistd.h>

namespace taskmaster {

namespace {

struct SensorContext {
  std::mutex* mutex;
  std::unordered_set<std::string>* cancelled;
};

auto check_file_sensor(const std::string& path) -> bool {
  std::error_code ec;
  return std::filesystem::exists(path, ec) && !ec;
}

struct ParsedHttpUrl {
  std::string host;
  uint16_t port{80};
  std::string path{"/"};
};

[[nodiscard]] auto parse_http_url(std::string_view url)
    -> std::optional<ParsedHttpUrl> {
  constexpr std::string_view kHttpPrefix = "http://";
  if (url.starts_with("https://")) {
    return std::nullopt;
  }
  if (url.starts_with(kHttpPrefix)) {
    url.remove_prefix(kHttpPrefix.size());
  }

  auto slash = url.find('/');
  std::string_view hostport = (slash == std::string_view::npos) ? url
                                                                : url.substr(0, slash);
  std::string_view path = (slash == std::string_view::npos) ? std::string_view{"/"}
                                                            : url.substr(slash);
  if (hostport.empty()) {
    return std::nullopt;
  }

  ParsedHttpUrl out;
  out.path = std::string(path);

  auto colon = hostport.rfind(':');
  if (colon != std::string_view::npos && colon + 1 < hostport.size()) {
    out.host = std::string(hostport.substr(0, colon));
    std::string_view port_sv = hostport.substr(colon + 1);
    unsigned port = 0;
    auto [ptr, ec] = std::from_chars(port_sv.data(), port_sv.data() + port_sv.size(), port);
    if (ec != std::errc{} || ptr != port_sv.data() + port_sv.size() || port == 0 || port > 65535) {
      return std::nullopt;
    }
    out.port = static_cast<uint16_t>(port);
  } else {
    out.host = std::string(hostport);
  }

  if (out.host.empty()) {
    return std::nullopt;
  }
  return out;
}

[[nodiscard]] auto parse_http_method(std::string_view method) noexcept
    -> http::HttpMethod {
  if (method == "POST" || method == "post") return http::HttpMethod::POST;
  if (method == "PUT" || method == "put") return http::HttpMethod::PUT;
  if (method == "DELETE" || method == "delete") return http::HttpMethod::DELETE;
  if (method == "PATCH" || method == "patch") return http::HttpMethod::PATCH;
  if (method == "OPTIONS" || method == "options") return http::HttpMethod::OPTIONS;
  if (method == "HEAD" || method == "head") return http::HttpMethod::HEAD;
  return http::HttpMethod::GET;
}

auto is_cancelled(const InstanceId& instance_id, SensorContext* ctx) -> bool {
  std::scoped_lock lock(*ctx->mutex);
  if (ctx->cancelled->contains(std::string(instance_id.value()))) {
    ctx->cancelled->erase(std::string(instance_id.value()));
    return true;
  }
  return false;
}

auto complete_cancelled(const InstanceId& instance_id, ExecutionSink& sink) -> void {
  if (sink.on_complete) {
    sink.on_complete(instance_id,
                     ExecutorResult{.exit_code = 1, .error = "Sensor cancelled"});
  }
}

auto run_file_sensor(SensorExecutorConfig config, InstanceId instance_id,
                     ExecutionSink sink, SensorContext* ctx) -> spawn_task {
  auto start_time = std::chrono::steady_clock::now();
  auto deadline = start_time + config.timeout;

  while (std::chrono::steady_clock::now() < deadline) {
    if (is_cancelled(instance_id, ctx)) {
      complete_cancelled(instance_id, sink);
      co_return;
    }

    if (check_file_sensor(config.target)) {
      if (sink.on_complete) {
        sink.on_complete(instance_id,
                         ExecutorResult{.exit_code = 0,
                                        .stdout_output = "File exists: " + config.target});
      }
      co_return;
    }

    if (sink.on_state) {
      sink.on_state(instance_id, "Waiting for file: " + config.target);
    }

    co_await async_sleep(config.poke_interval);
  }

  if (sink.on_complete) {
    if (config.soft_fail) {
      sink.on_complete(instance_id,
                       ExecutorResult{.exit_code = 100,
                                      .stdout_output = "Sensor timeout (soft_fail)",
                                      .timed_out = true});
    } else {
      sink.on_complete(instance_id,
                       ExecutorResult{.exit_code = 1,
                                      .error = "Sensor timeout: file not found",
                                      .timed_out = true});
    }
  }
}

[[nodiscard]] auto get_exit_code(int status) -> int {
  if (WIFEXITED(status)) {
    return WEXITSTATUS(status);
  }
  if (WIFSIGNALED(status)) {
    return 128 + WTERMSIG(status);
  }
  return -1;
}

auto spawn_shell_command(std::string_view cmd) -> pid_t {
  pid_t pid = vfork();
  if (pid < 0) {
    return -1;
  }
  if (pid == 0) {
    setpgid(0, 0);
    execl("/bin/sh", "sh", "-c", std::string(cmd).c_str(), nullptr);
    _exit(127);
  }
  setpgid(pid, pid);
  return pid;
}

auto run_command_sensor(SensorExecutorConfig config, InstanceId instance_id,
                        ExecutionSink sink, SensorContext* ctx) -> spawn_task {
  auto start_time = std::chrono::steady_clock::now();
  auto deadline = start_time + config.timeout;

  while (std::chrono::steady_clock::now() < deadline) {
    if (is_cancelled(instance_id, ctx)) {
      complete_cancelled(instance_id, sink);
      co_return;
    }

    if (sink.on_state) {
      sink.on_state(instance_id, "Running command sensor");
    }

    pid_t pid = spawn_shell_command(config.target);
    if (pid < 0) {
      co_await async_sleep(config.poke_interval);
      continue;
    }

    int status = 0;
    while (true) {
      if (is_cancelled(instance_id, ctx)) {
        kill(-pid, SIGKILL);
        (void)waitpid(pid, &status, 0);
        complete_cancelled(instance_id, sink);
        co_return;
      }

      pid_t r = waitpid(pid, &status, WNOHANG);
      if (r == pid) {
        break;
      }
      if (r == 0) {
        co_await async_sleep(std::chrono::milliseconds(10));
        continue;
      }
      if (errno == EINTR) {
        continue;
      }
      status = 0;
      break;
    }

    if (get_exit_code(status) == 0) {
      if (sink.on_complete) {
        sink.on_complete(instance_id,
                         ExecutorResult{.exit_code = 0,
                                        .stdout_output = "Command succeeded"});
      }
      co_return;
    }

    co_await async_sleep(config.poke_interval);
  }

  if (sink.on_complete) {
    if (config.soft_fail) {
      sink.on_complete(instance_id,
                       ExecutorResult{.exit_code = 100,
                                      .stdout_output = "Sensor timeout (soft_fail)",
                                      .timed_out = true});
    } else {
      sink.on_complete(instance_id,
                       ExecutorResult{.exit_code = 1,
                                      .error = "Sensor timeout: command did not succeed",
                                      .timed_out = true});
    }
  }
}

auto run_http_sensor(SensorExecutorConfig config, InstanceId instance_id,
                     ExecutionSink sink, SensorContext* ctx) -> spawn_task {
  auto parsed = parse_http_url(config.target);
  if (!parsed) {
    if (sink.on_complete) {
      sink.on_complete(instance_id,
                       ExecutorResult{.exit_code = 1,
                                      .error = "Invalid http sensor url"});
    }
    co_return;
  }

  auto start_time = std::chrono::steady_clock::now();
  auto deadline = start_time + config.timeout;
  const auto method = parse_http_method(config.http_method);
  const auto expected = static_cast<uint16_t>(config.expected_status);

  while (std::chrono::steady_clock::now() < deadline) {
    if (is_cancelled(instance_id, ctx)) {
      complete_cancelled(instance_id, sink);
      co_return;
    }

    if (sink.on_state) {
      sink.on_state(instance_id,
                    std::format("Waiting for http {} {} (expect {})",
                                method, parsed->path, expected));
    }

    auto client = co_await http::HttpClient::connect_tcp(current_io_context(),
                                                         parsed->host, parsed->port);
    if (client) {
      http::HttpRequest req;
      req.method = method;
      req.path = parsed->path;
      auto resp = co_await client->request(std::move(req));
      if (static_cast<uint16_t>(resp.status) == expected) {
        if (sink.on_complete) {
          sink.on_complete(instance_id,
                           ExecutorResult{.exit_code = 0,
                                          .stdout_output = std::format(
                                              "HTTP {} {} returned {}",
                                              method, parsed->path,
                                              static_cast<uint16_t>(resp.status))});
        }
        co_return;
      }
    }

    co_await async_sleep(config.poke_interval);
  }

  if (sink.on_complete) {
    if (config.soft_fail) {
      sink.on_complete(instance_id,
                       ExecutorResult{.exit_code = 100,
                                      .stdout_output = "Sensor timeout (soft_fail)",
                                      .timed_out = true});
    } else {
      sink.on_complete(instance_id,
                       ExecutorResult{.exit_code = 1,
                                      .error = "Sensor timeout: http not ready",
                                      .timed_out = true});
    }
  }
}

}  // namespace

class SensorExecutor : public IExecutor {
public:
  explicit SensorExecutor(Runtime& runtime)
      : runtime_(&runtime), ctx_{&mutex_, &cancelled_} {}

  auto start(ExecutorContext exec_ctx, ExecutorRequest req, ExecutionSink sink)
      -> void override {
    (void)exec_ctx;
    auto* config = std::get_if<SensorExecutorConfig>(&req.config);
    if (!config) {
      if (sink.on_complete) {
        sink.on_complete(req.instance_id,
                         ExecutorResult{.exit_code = 1,
                                        .error = "Invalid sensor config"});
      }
      return;
    }

    log::info("SensorExecutor start: instance_id={} type={} target={}",
              req.instance_id,
              config->type == SensorType::File ? "file" :
              config->type == SensorType::Http ? "http" : "command",
              config->target);

    switch (config->type) {
      case SensorType::File: {
        auto t = run_file_sensor(*config, req.instance_id, std::move(sink), &ctx_);
        runtime_->schedule_external(t.take());
        break;
      }
      case SensorType::Http: {
        auto t = run_http_sensor(*config, req.instance_id, std::move(sink), &ctx_);
        runtime_->schedule_external(t.take());
        break;
      }
      case SensorType::Command: {
        auto t = run_command_sensor(*config, req.instance_id, std::move(sink), &ctx_);
        runtime_->schedule_external(t.take());
        break;
      }
    }
  }

  auto cancel(const InstanceId& instance_id) -> void override {
    std::scoped_lock lock(mutex_);
    cancelled_.insert(std::string(instance_id.value()));
    log::info("SensorExecutor cancel: instance_id={}", instance_id);
  }

private:
  Runtime* runtime_;
  std::mutex mutex_;
  std::unordered_set<std::string> cancelled_;
  SensorContext ctx_;
};

auto create_sensor_executor(Runtime& rt) -> std::unique_ptr<IExecutor> {
  return std::make_unique<SensorExecutor>(rt);
}

}  // namespace taskmaster
