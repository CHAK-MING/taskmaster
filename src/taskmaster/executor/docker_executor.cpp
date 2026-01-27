#include "taskmaster/executor/docker_executor.hpp"

#include "taskmaster/client/docker/docker_client.hpp"
#include "taskmaster/core/coroutine.hpp"
#include "taskmaster/core/runtime.hpp"
#include "taskmaster/io/context.hpp"
#include "taskmaster/util/log.hpp"

#include <experimental/scope>
#include <format>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <unordered_map>
#include <unordered_set>

namespace taskmaster {

namespace {

auto generate_container_name(const InstanceId& instance_id) -> std::string {
  thread_local std::mt19937 gen(std::random_device{}());
  thread_local std::uniform_int_distribution<> dis(0, 0xFFFF);
  return std::format("taskmaster-{}-{:04x}", instance_id, dis(gen));
}

auto finish(ExecutionSink& sink, const InstanceId& instance_id,
            ExecutorResult result) -> void {
  if (sink.on_complete) {
    sink.on_complete(instance_id, std::move(result));
  }
}

struct ActiveContainer {
  std::string container_id;
  std::string socket_path;
};

class DockerExecutionContext {
public:
  auto register_container(const InstanceId& id, std::string container_id,
                          std::string socket_path) -> void {
    std::scoped_lock lock(mutex_);
    active_containers_[id] = ActiveContainer{std::move(container_id),
                                             std::move(socket_path)};
  }

  auto unregister_container(const InstanceId& id) -> void {
    std::scoped_lock lock(mutex_);
    active_containers_.erase(id);
    cancelled_instances_.erase(id);
  }

  auto get_container(const InstanceId& id) -> std::optional<ActiveContainer> {
    std::scoped_lock lock(mutex_);
    auto it = active_containers_.find(id);
    if (it != active_containers_.end()) {
      return it->second;
    }
    return std::nullopt;
  }

  auto mark_cancelled(const InstanceId& id) -> void {
    std::scoped_lock lock(mutex_);
    cancelled_instances_.insert(id);
  }

  auto is_cancelled(const InstanceId& id) -> bool {
    std::scoped_lock lock(mutex_);
    return cancelled_instances_.contains(id);
  }

private:
  std::mutex mutex_;
  std::unordered_map<InstanceId, ActiveContainer> active_containers_;
  std::unordered_set<InstanceId> cancelled_instances_;
};

auto execute_docker_task(DockerExecutorConfig config, InstanceId instance_id,
                         ExecutionSink sink,
                         std::shared_ptr<DockerExecutionContext> ctx)
    -> spawn_task {
  ExecutorResult result;
  auto& io_ctx = current_io_context();

  auto client_result = co_await docker::DockerClient::connect(
      io_ctx, config.docker_socket);

  if (!client_result) {
    result.error = "Failed to connect to Docker daemon";
    result.exit_code = -1;
    finish(sink, instance_id, std::move(result));
    co_return;
  }

  auto client = std::move(*client_result);
  auto container_name = generate_container_name(instance_id);

  docker::ContainerConfig container_config{
      .image = config.image,
      .command = config.command,
      .working_dir = config.working_dir,
      .env = config.env,
  };

  if (config.pull_policy == ImagePullPolicy::Always) {
    auto pull_result = co_await client->pull_image(config.image);
    if (!pull_result) {
      result.error = std::format("Failed to pull image: {}",
                                 to_string_view(pull_result.error()));
      result.exit_code = -1;
      finish(sink, instance_id, std::move(result));
      co_return;
    }
  }

  auto create_result =
      co_await client->create_container(container_config, container_name);

  if (!create_result &&
      create_result.error() == docker::DockerError::ImageNotFound &&
      config.pull_policy == ImagePullPolicy::IfNotPresent) {
    log::info("DockerExecutor: image not found, pulling {}", config.image);
    auto pull_result = co_await client->pull_image(config.image);
    if (!pull_result) {
      result.error = std::format("Failed to pull image: {}",
                                 to_string_view(pull_result.error()));
      result.exit_code = -1;
      finish(sink, instance_id, std::move(result));
      co_return;
    }
    create_result =
        co_await client->create_container(container_config, container_name);
  }

  if (!create_result) {
    result.error = std::format("Failed to create container: {}",
                               static_cast<int>(create_result.error()));
    result.exit_code = -1;
    finish(sink, instance_id, std::move(result));
    co_return;
  }

  auto container_id = create_result->id;
  log::info("DockerExecutor: created container {} for instance {}",
            container_id, instance_id);

  ctx->register_container(instance_id, container_id, config.docker_socket);
  std::experimental::scope_exit unregister{
      [ctx, instance_id] { ctx->unregister_container(instance_id); }};

  auto start_result = co_await client->start_container(container_id);
  if (!start_result) {
    result.error = "Failed to start container";
    result.exit_code = -1;
    (void)co_await client->remove_container(container_id, true);
    finish(sink, instance_id, std::move(result));
    co_return;
  }

  auto wait_result = co_await client->wait_container(container_id);
  
  if (ctx->is_cancelled(instance_id)) {
    result.error = "Task was cancelled";
    result.exit_code = -1;
    finish(sink, instance_id, std::move(result));
    co_return;
  }

  if (!wait_result) {
    result.error = "Failed to wait for container";
    result.exit_code = -1;
    (void)co_await client->remove_container(container_id, true);
    finish(sink, instance_id, std::move(result));
    co_return;
  }

  result.exit_code = wait_result->status_code;
  if (!wait_result->error.empty()) {
    result.error = wait_result->error;
  }

  auto logs_result = co_await client->get_logs(container_id);
  if (logs_result) {
    result.stdout_output = std::move(logs_result->stdout_output);
    result.stderr_output = std::move(logs_result->stderr_output);
  }

  (void)co_await client->remove_container(container_id, true);
  log::info("DockerExecutor: finished instance {} exit_code={}", instance_id,
            result.exit_code);

  finish(sink, instance_id, std::move(result));
}

}  // namespace

struct DockerExecutor::Impl {
  Runtime* runtime;
  std::shared_ptr<DockerExecutionContext> ctx;

  explicit Impl(Runtime& rt)
      : runtime(&rt), ctx(std::make_shared<DockerExecutionContext>()) {}
};

DockerExecutor::DockerExecutor(Runtime& rt)
    : impl_(std::make_unique<Impl>(rt)) {}

DockerExecutor::~DockerExecutor() = default;

DockerExecutor::DockerExecutor(DockerExecutor&&) noexcept = default;
auto DockerExecutor::operator=(DockerExecutor&&) noexcept
    -> DockerExecutor& = default;

auto DockerExecutor::start(ExecutorContext exec_ctx, ExecutorRequest req,
                           ExecutionSink sink) -> void {
  (void)exec_ctx;

  const auto* docker_config = std::get_if<DockerExecutorConfig>(&req.config);
  if (!docker_config) {
    ExecutorResult result;
    result.exit_code = -1;
    result.error = "Invalid executor config";
    if (sink.on_complete) {
      sink.on_complete(req.instance_id, std::move(result));
    }
    return;
  }

  auto cmd_preview = docker_config->command.size() > 80
                         ? docker_config->command.substr(0, 80) + "..."
                         : docker_config->command;
  log::info("DockerExecutor start: instance_id={} image={} cmd='{}'",
            req.instance_id, docker_config->image, cmd_preview);

  auto t = execute_docker_task(*docker_config, req.instance_id,
                               std::move(sink), impl_->ctx);
  impl_->runtime->schedule_external(t.take());
}

auto DockerExecutor::cancel(const InstanceId& instance_id) -> void {
  impl_->ctx->mark_cancelled(instance_id);
  
  auto container_opt = impl_->ctx->get_container(instance_id);
  if (!container_opt) {
    log::warn("DockerExecutor: no active container for instance {}",
              instance_id);
    return;
  }

  log::info("DockerExecutor: cancelling container {} for instance {}",
            container_opt->container_id, instance_id);

  auto& io_ctx = impl_->runtime->current_context();

  auto cancel_task = [](io::IoContext& ctx, std::string container_id,
                        std::string socket_path) -> spawn_task {
    auto client_result = co_await docker::DockerClient::connect(ctx, socket_path);
    if (client_result) {
      (void)co_await (*client_result)->stop_container(container_id, std::chrono::seconds{5});
      (void)co_await (*client_result)->remove_container(container_id, true);
    }
  };

  auto t = cancel_task(io_ctx, container_opt->container_id,
                       container_opt->socket_path);
  impl_->runtime->schedule_external(t.take());
}

auto create_docker_executor(Runtime& rt) -> std::unique_ptr<IExecutor> {
  return std::make_unique<DockerExecutor>(rt);
}

}  // namespace taskmaster
