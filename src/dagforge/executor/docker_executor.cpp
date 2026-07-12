#include "dagforge/client/docker/docker_client.hpp"
#include "dagforge/executor/executor.hpp"
#include "dagforge/executor/executor_state.hpp"
#include "dagforge/executor/executor_utils.hpp"
#include "dagforge/util/log.hpp"

#include <boost/asio/experimental/awaitable_operators.hpp>

#include <atomic>
#include <experimental/scope>
#include <format>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace dagforge {

struct ContainerHandle {
  std::string container_id;
  std::string socket_path;
};

using DockerShardState = ExecutorShardState<ContainerHandle>;

struct DockerExecutionContext {
  DockerShardState *state{};

  auto register_container(const InstanceId &id, std::string container_id,
                          std::string socket_path) -> void {
    state->register_active(
        id, ContainerHandle{.container_id = std::move(container_id),
                            .socket_path = std::move(socket_path)});
  }

  auto unregister_container(const InstanceId &id) -> void {
    state->unregister_active(id);
  }

  [[nodiscard]] auto get_container(const InstanceId &id) const -> Result<ContainerHandle> {
    if (auto handle = state->find_active(id)) {
      return ok(std::move(*handle));
    }
    return fail(Error::NotFound);
  }

  auto mark_cancelled(const InstanceId &id) -> void { state->mark_cancelled(id); }

  [[nodiscard]] auto is_cancelled(const InstanceId &id) const -> bool {
    return state->is_cancelled(id);
  }
};

namespace {

using RealDockerClient = docker::DockerClient<http::HttpClient>;
using namespace boost::asio::experimental::awaitable_operators;
inline constexpr auto kHeartbeatInterval = std::chrono::seconds(1);

[[nodiscard]] auto docker_error_message(const std::error_code &error)
    -> std::string {
  return error.message();
}

auto generate_container_name(const InstanceId &id) -> std::string {
  std::string name = "dagforge_" + std::string(id.value());
  std::replace(name.begin(), name.end(), ':', '_');
  std::replace(name.begin(), name.end(), '-', '_');
  return name;
}

auto finish(ExecutionSink &sink, const InstanceId &id, ExecutorResult res)
    -> void {
  if (sink.on_complete) {
    sink.on_complete(id, std::move(res));
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

struct DockerWaitResult {
  Result<docker::WaitContainerResponse> wait_result;
  bool timed_out{false};
};

auto wait_container_with_timeout(RealDockerClient &client,
                                 std::string container_id,
                                 std::chrono::seconds timeout)
    -> task<DockerWaitResult> {
  auto outcome = co_await (client.wait_container(container_id) ||
                           async_sleep_on_timing_wheel(timeout));
  if (outcome.index() == 0) {
    co_return DockerWaitResult{
        .wait_result = std::move(std::get<0>(outcome)),
        .timed_out = false,
    };
  }
  co_return DockerWaitResult{
      .wait_result = fail(make_error_code(std::errc::timed_out)),
      .timed_out = true,
  };
}

auto fetch_container_logs(RealDockerClient &client, std::string container_id,
                          ExecutorResult &result) -> task<void> {
  auto logs_result = co_await client.get_logs(container_id);
  if (!logs_result) {
    log::error("DockerExecutor: failed to get logs for container {}: {}",
               container_id, docker_error_message(logs_result.error()));
    co_return;
  }
  result.stdout_output = std::move(logs_result->stdout_output);
  result.stderr_output = std::move(logs_result->stderr_output);
}

auto cleanup_container(RealDockerClient &client, std::string container_id,
                       bool stop_first, std::chrono::seconds stop_timeout)
    -> task<void> {
  if (stop_first) {
    auto stop_result = co_await client.stop_container(container_id, stop_timeout);
    if (!stop_result) {
      log::error("DockerExecutor: failed to stop container {}: {}",
                 container_id, docker_error_message(stop_result.error()));
    }
  }
  auto remove_result = co_await client.remove_container(container_id, true);
  if (!remove_result) {
    log::error("DockerExecutor: failed to remove container {}: {}",
               container_id, docker_error_message(remove_result.error()));
  }
}

auto execute_docker_task(Runtime &runtime, DockerExecutorConfig config,
                         std::string command, std::string working_dir,
                         std::chrono::seconds execution_timeout,
                         InstanceId inst_id, ExecutionSink sink,
                         std::shared_ptr<ExecutorHeartbeatCallback>
                             heartbeat_callback,
                         std::shared_ptr<pmr::memory_resource> resource_owner,
                         DockerExecutionContext ctx) -> spawn_task {
  auto *resource = resource_owner != nullptr ? resource_owner.get()
                                             : current_memory_resource_or_default();
  ExecutorResult result = make_executor_result(resource);
  auto heartbeat_stop = std::make_shared<std::atomic_bool>(false);
  std::experimental::scope_exit stop_heartbeat{
      [heartbeat_stop] { heartbeat_stop->store(true, std::memory_order_release); }};
  emit_heartbeat(heartbeat_callback, inst_id);
  if (heartbeat_callback && *heartbeat_callback) {
    runtime.spawn(run_executor_heartbeat(heartbeat_callback, heartbeat_stop,
                                         inst_id.clone()));
  }

  auto client_result = co_await RealDockerClient::connect(
      runtime.current_context(), config.docker_socket);

  if (!client_result) {
    result.error = "Failed to connect to Docker daemon";
    result.exit_code = -1;
    finish(sink, inst_id, std::move(result));
    co_return;
  }

  auto client = std::move(*client_result);
  auto container_name = generate_container_name(inst_id);

  docker::ContainerConfig container_config{
      .image = config.image,
      .command = std::move(command),
      .working_dir = std::move(working_dir),
      .env = config.env,
  };

  if (config.pull_policy == ImagePullPolicy::Always) {
    auto pull_result = co_await client->pull_image(config.image);
    if (!pull_result) {
      result.error = std::format("Failed to pull image: {}",
                                 docker_error_message(pull_result.error()));
      result.exit_code = -1;
      finish(sink, inst_id, std::move(result));
      co_return;
    }
  }

  auto create_result =
      co_await client->create_container(container_config, container_name);

  if (!create_result &&
      create_result.error() == make_error_code(docker::DockerError::ImageNotFound) &&
      config.pull_policy == ImagePullPolicy::IfNotPresent) {
    log::debug("DockerExecutor: image not found, pulling {}", config.image);
    auto pull_result = co_await client->pull_image(config.image);
    if (!pull_result) {
      result.error = std::format("Failed to pull image: {}",
                                 docker_error_message(pull_result.error()));
      result.exit_code = -1;
      finish(sink, inst_id, std::move(result));
      co_return;
    }
    create_result =
        co_await client->create_container(container_config, container_name);
  }

  if (!create_result.has_value()) {
    result.error = std::format(
        "Failed to create container: {}",
        docker_error_message(create_result.error()));
    result.exit_code = -1;
    finish(sink, inst_id, std::move(result));
    co_return;
  }

  auto container_id = create_result->id;
  log::debug("DockerExecutor: created container {} for instance {}",
             container_id, inst_id);

  ctx.register_container(inst_id, container_id, config.docker_socket);
  std::experimental::scope_exit unregister{
      [state = ctx.state, inst_id] { state->unregister_active(inst_id); }};

  auto start_result = co_await client->start_container(container_id);
  if (!start_result) {
    result.error = "Failed to start container";
    result.exit_code = -1;
    co_await cleanup_container(*client, container_id, false,
                               std::chrono::seconds{0});
    finish(sink, inst_id, std::move(result));
    co_return;
  }

  auto wait_result =
      co_await wait_container_with_timeout(*client, container_id,
                                           execution_timeout);

  if (ctx.is_cancelled(inst_id)) {
    result.error = "Task was cancelled";
    result.exit_code = -1;
    finish(sink, inst_id, std::move(result));
    co_return;
  }

  if (wait_result.timed_out) {
    result.timed_out = true;
    result.exit_code = kExitCodeTimeout;
    result.error = "Execution timeout";
    auto stop_result =
        co_await client->stop_container(container_id, std::chrono::seconds{1});
    if (!stop_result) {
      log::error("DockerExecutor: failed to stop timed-out container {}: {}",
                 container_id, docker_error_message(stop_result.error()));
    }
    co_await fetch_container_logs(*client, container_id, result);
    co_await cleanup_container(*client, container_id, false,
                               std::chrono::seconds{0});
    finish(sink, inst_id, std::move(result));
    co_return;
  }

  if (!wait_result.wait_result) {
    result.error = std::format("Failed to wait for container: {}",
                               docker_error_message(
                                   wait_result.wait_result.error()));
    result.exit_code = -1;
    co_await cleanup_container(*client, container_id, false,
                               std::chrono::seconds{0});
    finish(sink, inst_id, std::move(result));
    co_return;
  }

  result.exit_code = wait_result.wait_result->status_code;
  if (!wait_result.wait_result->error.empty()) {
    result.error = wait_result.wait_result->error;
  }

  co_await fetch_container_logs(*client, container_id, result);

  co_await cleanup_container(*client, container_id, false,
                             std::chrono::seconds{0});
  log::debug("DockerExecutor: finished instance {} exit_code={} timed_out={}",
             inst_id, result.exit_code, result.timed_out);

  finish(sink, inst_id, std::move(result));
}

} // namespace

class DockerExecutorImpl final : public IExecutor {
public:
  explicit DockerExecutorImpl(Runtime &rt)
      : runtime_(&rt), shard_states_(rt.shard_count()) {}

  DockerExecutorImpl(DockerExecutorImpl &&) noexcept = default;
  DockerExecutorImpl &operator=(DockerExecutorImpl &&) noexcept = default;
  DockerExecutorImpl(const DockerExecutorImpl &) = delete;
  DockerExecutorImpl &operator=(const DockerExecutorImpl &) = delete;

  auto start(ExecutorRequest req, ExecutionSink sink) -> Result<void> override {
    const auto *docker_config = req.config.as<DockerExecutorConfig>();
    if (!docker_config) {
      if (sink.on_complete) {
        ExecutorResult result = make_executor_result(req.resource());
        result.exit_code = -1;
        result.error =
            pmr::string("Invalid configuration for Docker executor",
                             req.resource());
        sink.on_complete(req.instance_id, std::move(result));
      }
      return fail(Error::InvalidArgument);
    }

    log::debug("DockerExecutor start: instance_id={} image={} cmd='{}'",
               req.instance_id, docker_config->image,
               cmd_preview(req.command));
    auto resource_owner = req.memory_resource;
    std::shared_ptr<ExecutorHeartbeatCallback> heartbeat_callback;
    if (sink.on_heartbeat) {
      heartbeat_callback = std::make_shared<ExecutorHeartbeatCallback>(
          std::move(sink.on_heartbeat));
    }

    auto sid = runtime_->is_current_shard() ? runtime_->current_shard() : 0;
    auto t = execute_docker_task(
        *runtime_, *docker_config, req.command, req.working_dir,
        req.execution_timeout, req.instance_id, std::move(sink),
        std::move(heartbeat_callback), resource_owner,
        DockerExecutionContext{.state = &shard_states_[sid]});
    runtime_->spawn(std::move(t));
    return ok();
  }

  auto cancel(const InstanceId &instance_id) -> void override {
    cancel_on_all_shards(*runtime_, shard_states_, instance_id,
                         [this, instance_id](DockerShardState &state,
                                             const InstanceId &id) {
                           DockerExecutionContext ctx{.state = &state};
                           ctx.mark_cancelled(id);
                           auto container_res = ctx.get_container(id);
                           if (!container_res) {
                             if (container_res.error() !=
                                 make_error_code(Error::NotFound)) {
                               log::error(
                                   "DockerExecutor: error retrieving container for instance {}: {}",
                                   instance_id, container_res.error().message());
                             }
                             return;
                           }

                           log::debug(
                               "DockerExecutor: cancelling container {} for instance {}",
                               container_res->container_id, id);

                           auto cancel_task = [](Runtime &runtime,
                                                 std::string container_id,
                                                 std::string socket_path)
                               -> spawn_task {
                             auto client_result = co_await RealDockerClient::connect(
                                 runtime.current_context(), socket_path);
                             if (client_result) {
                               auto stop_result =
                                   co_await (*client_result)
                                       ->stop_container(container_id,
                                                        std::chrono::seconds{5});
                               if (!stop_result) {
                                 log::error("Failed to stop container {}: {}",
                                            container_id,
                                            docker_error_message(
                                                stop_result.error()));
                               }

                               auto remove_result = co_await (*client_result)
                                                        ->remove_container(
                                                            container_id, true);
                               if (!remove_result) {
                                 log::error(
                                     "Failed to remove container {}: {}",
                                     container_id,
                                     docker_error_message(
                                         remove_result.error()));
                               }
                             }
                             co_return;
                           };

                           auto sid = runtime_->is_current_shard()
                                          ? runtime_->current_shard()
                                          : 0;
                           runtime_->spawn_on(
                               sid, cancel_task(*runtime_,
                                                container_res->container_id,
                                                container_res->socket_path));
                         });
  }

private:
  Runtime *runtime_;
  std::vector<DockerShardState> shard_states_;
};

auto create_docker_executor(Runtime &rt) -> std::unique_ptr<IExecutor> {
  return std::make_unique<DockerExecutorImpl>(rt);
}

} // namespace dagforge
