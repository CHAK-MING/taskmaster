#include "taskmaster/core/coroutine.hpp"
#include "taskmaster/core/error.hpp"
#include "taskmaster/core/runtime.hpp"
#include "taskmaster/executor/executor.hpp"
#include "taskmaster/util/log.hpp"

#include <chrono>
#include <filesystem>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

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

auto run_file_sensor(SensorExecutorConfig config, InstanceId instance_id,
                     ExecutionSink sink, SensorContext* ctx) -> spawn_task {
  auto start_time = std::chrono::steady_clock::now();
  auto deadline = start_time + config.timeout;

  while (std::chrono::steady_clock::now() < deadline) {
    {
      std::lock_guard lock(*ctx->mutex);
      if (ctx->cancelled->contains(std::string(instance_id.value()))) {
        ctx->cancelled->erase(std::string(instance_id.value()));
        if (sink.on_complete) {
          sink.on_complete(instance_id,
                           ExecutorResult{.exit_code = 1,
                                          .error = "Sensor cancelled"});
        }
        co_return;
      }
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

  if (config.soft_fail) {
    if (sink.on_complete) {
      sink.on_complete(instance_id,
                       ExecutorResult{.exit_code = 0,
                                      .stdout_output = "Sensor timeout (soft_fail)",
                                      .timed_out = true});
    }
  } else {
    if (sink.on_complete) {
      sink.on_complete(instance_id,
                       ExecutorResult{.exit_code = 1,
                                      .error = "Sensor timeout: file not found",
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
      case SensorType::Http:
      case SensorType::Command: {
        if (sink.on_complete) {
          sink.on_complete(req.instance_id,
                           ExecutorResult{.exit_code = 1,
                                          .error = "Sensor type not yet implemented"});
        }
        break;
      }
    }
  }

  auto cancel(const InstanceId& instance_id) -> void override {
    std::lock_guard lock(mutex_);
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
