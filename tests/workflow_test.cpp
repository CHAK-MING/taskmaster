#include "dagforge/core/runtime.hpp"
#include "dagforge/core/sync_wait.hpp"
#include "dagforge/executors/command/executor.hpp"
#include "dagforge/sandbox/command_runner.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/workflow/executor_registry.hpp"
#include "dagforge/workflow/plan_compiler.hpp"
#include "dagforge/workflow/run_value_store.hpp"
#include "dagforge/workflow/workflow_control_plane.hpp"
#include "dagforge/workflow/workflow_plan_loader.hpp"
#include "dagforge/workflow/workflow_runtime.hpp"

#include "../src/dagforge/executors/command/detail/testing.hpp"

#include "gtest/gtest.h"

#include <atomic>
#include <array>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <deque>
#include <filesystem>
#include <fstream>
#include <format>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <utility>

#include <unistd.h>

using namespace dagforge;
using namespace dagforge::config;
using namespace dagforge::executors;
using namespace dagforge::sandbox;
using namespace dagforge::workflow;

namespace {

[[nodiscard]] auto temporary_test_directory(std::string_view name)
    -> std::filesystem::path {
  return std::filesystem::temp_directory_path() /
         std::format("dagforge-{}-{}", name, ::getpid());
}

class ManualTaskExecutor final : public ITaskExecutor {
public:
  explicit ManualTaskExecutor(Runtime *runtime = nullptr) : runtime_(runtime) {}

  [[nodiscard]] auto type() const noexcept -> std::string_view override {
    return "test";
  }

  [[nodiscard]] auto compile(JsonValue config,
                             ExecutorCompileContext) const
      -> Result<JsonValue> override {
    return ok(std::move(config));
  }

  auto start(TaskExecutionRequest request, TaskExecutionSink sink)
      -> Result<void> override {
    {
      std::lock_guard lock(mutex_);
      if (quiescing_) {
        return fail(Error::InvalidState);
      }
      if (start_error_) {
        return fail(*start_error_);
      }
    }
    {
      std::lock_guard lock(mutex_);
      if (!defer_running_signal_ && sink.on_state) {
        sink.on_state(request.instance_id, "running");
      }
    }
    std::lock_guard lock(mutex_);
    pending_.push_back(Pending{
        .instance_id = std::move(request.instance_id),
        .owner = runtime_ != nullptr ? runtime_->current_shard() : kInvalidShard,
        .config = std::move(request.config),
        .inputs = std::move(request.inputs),
        .outputs = std::move(request.outputs),
        .sink = std::move(sink),
    });
    changed_.notify_all();
    return ok();
  }

  auto cancel(const InstanceId &instance_id) -> void override {
    bool synchronous = false;
    {
      std::lock_guard lock(mutex_);
      if (defer_cancel_) {
        return;
      }
      synchronous = synchronous_cancel_;
    }
    if (synchronous) {
      complete_inline(instance_id, -1, {});
    } else {
      complete(instance_id, -1, {});
    }
  }

  auto quiesce(std::chrono::milliseconds timeout) -> Result<void> override {
    std::unique_lock lock(mutex_);
    quiescing_ = true;
    ++quiesce_count_;
    if (!changed_.wait_for(lock, timeout,
                           [this] { return pending_.empty(); })) {
      return fail(Error::Timeout);
    }
    return ok();
  }

  [[nodiscard]] auto quiesce_count() const -> std::size_t {
    std::lock_guard lock(mutex_);
    return quiesce_count_;
  }

  auto defer_cancel_completion(bool value = true) -> void {
    std::lock_guard lock(mutex_);
    defer_cancel_ = value;
  }

  auto defer_running_signal(bool value = true) -> void {
    std::lock_guard lock(mutex_);
    defer_running_signal_ = value;
  }

  auto synchronous_cancel_completion(bool value = true) -> void {
    std::lock_guard lock(mutex_);
    synchronous_cancel_ = value;
  }

  auto fail_start(Error error) -> void {
    std::lock_guard lock(mutex_);
    start_error_ = error;
  }

  auto signal_running_next() -> bool {
    std::lock_guard lock(mutex_);
    if (pending_.empty() || !pending_.front().sink.on_state) {
      return false;
    }
    pending_.front().sink.on_state(pending_.front().instance_id, "running");
    return true;
  }

  [[nodiscard]] auto wait_for_pending(
      std::size_t count,
      std::chrono::milliseconds timeout = std::chrono::seconds(2)) -> bool {
    std::unique_lock lock(mutex_);
    return changed_.wait_for(lock, timeout,
                             [&] { return pending_.size() >= count; });
  }

  [[nodiscard]] auto pending_count() const -> std::size_t {
    std::lock_guard lock(mutex_);
    return pending_.size();
  }

  [[nodiscard]] auto next_inputs() const -> ExecutorInputs {
    std::lock_guard lock(mutex_);
    if (pending_.empty()) {
      return {};
    }
    return pending_.front().inputs;
  }

  auto complete_next(int exit_code = 0, std::string output = {}) -> bool {
    std::optional<InstanceId> instance_id;
    {
      std::lock_guard lock(mutex_);
      if (pending_.empty()) {
        return false;
      }
      instance_id = pending_.front().instance_id.clone();
    }
    complete(*instance_id, exit_code, std::move(output));
    return true;
  }

  auto complete_next_with_outputs(ExecutorOutputs outputs) -> bool {
    std::optional<Pending> pending;
    {
      std::lock_guard lock(mutex_);
      if (pending_.empty()) {
        return false;
      }
      pending.emplace(std::move(pending_.front()));
      pending_.pop_front();
      changed_.notify_all();
    }
    if (runtime_ == nullptr) {
      return false;
    }
    runtime_->post_to(
        pending->owner,
        [pending = std::move(*pending),
         result = task_succeeded(std::move(outputs))]() mutable {
          if (pending.sink.on_complete) {
            pending.sink.on_complete(pending.instance_id, std::move(result));
          }
        });
    return true;
  }

  auto complete_next_inline_twice(std::string first_output,
                                  std::string second_output) -> bool {
    std::optional<InstanceId> instance_id;
    {
      std::lock_guard lock(mutex_);
      if (pending_.empty()) {
        return false;
      }
      instance_id = pending_.front().instance_id.clone();
    }
    auto pending = take_pending(*instance_id);
    if (!pending || !pending->sink.on_complete) {
      return false;
    }

    auto first = make_outputs(*pending, 0, first_output);
    auto second = make_outputs(*pending, 0, second_output);
    pending->sink.on_complete(pending->instance_id, std::move(first));
    pending->sink.on_complete(pending->instance_id, std::move(second));
    return true;
  }

private:
  struct Pending {
    InstanceId instance_id;
    shard_id owner{kInvalidShard};
    JsonValue config{JsonValue::object_t{}};
    ExecutorInputs inputs;
    std::vector<WorkflowPortId> outputs;
    TaskExecutionSink sink;
  };

  [[nodiscard]] static auto make_outputs(const Pending &pending,
                                         int exit_code,
                                         const std::string &output)
      -> TaskExecutionResult {
    if (exit_code != 0) {
      JsonValue details = JsonValue::object_t{};
      details["exit_code"] = static_cast<std::int64_t>(exit_code);
      return task_failed(make_execution_failure(
          Error::Unknown, "test_exit_nonzero",
          std::format("Test executor exited with status {}", exit_code),
          std::move(details)));
    }
    ExecutorOutputs outputs;
    outputs.reserve(pending.outputs.size());
    for (const auto &port : pending.outputs) {
      if (port == "exit_code") {
        outputs.emplace_back(port.clone(),
                             static_cast<std::int64_t>(exit_code));
      } else if (port == "stderr") {
        outputs.emplace_back(port.clone(), std::string{});
      } else {
        outputs.emplace_back(port.clone(), output);
      }
    }
    return task_succeeded(std::move(outputs));
  }

  [[nodiscard]] auto take_pending(const InstanceId &instance_id)
      -> std::optional<Pending> {
    std::lock_guard lock(mutex_);
    const auto it =
        std::ranges::find(pending_, instance_id, &Pending::instance_id);
    if (it == pending_.end()) {
      return std::nullopt;
    }
    std::optional<Pending> pending;
    pending.emplace(std::move(*it));
    pending_.erase(it);
    changed_.notify_all();
    return pending;
  }

  auto complete(const InstanceId &instance_id, int exit_code,
                std::string output) -> void {
    auto pending = take_pending(instance_id);
    if (!pending) {
      return;
    }
    auto result = make_outputs(*pending, exit_code, output);
    if (runtime_ == nullptr) {
      return;
    }
    runtime_->post_to(
        pending->owner,
        [pending = std::move(*pending), result = std::move(result)]() mutable {
          if (pending.sink.on_complete) {
            pending.sink.on_complete(pending.instance_id, std::move(result));
          }
        });
  }

  auto complete_inline(const InstanceId &instance_id, int exit_code,
                       std::string output) -> void {
    auto pending = take_pending(instance_id);
    if (!pending) {
      return;
    }
    auto result = make_outputs(*pending, exit_code, output);
    if (pending->sink.on_complete) {
      pending->sink.on_complete(pending->instance_id, std::move(result));
    }
  }

  Runtime *runtime_;
  mutable std::mutex mutex_;
  std::condition_variable changed_;
  std::deque<Pending> pending_;
  bool defer_cancel_{false};
  bool defer_running_signal_{false};
  bool synchronous_cancel_{false};
  bool quiescing_{false};
  std::size_t quiesce_count_{0};
  std::optional<Error> start_error_;
};

struct TestExecutorEnvironment {
  explicit TestExecutorEnvironment(Runtime *runtime = nullptr)
      : executor(std::make_shared<ManualTaskExecutor>(runtime)) {
    auto registered = registry.register_executor(executor);
    if (!registered) {
      throw std::runtime_error(registered.error().message());
    }
  }

  explicit TestExecutorEnvironment(Runtime &runtime)
      : TestExecutorEnvironment(&runtime) {}

  std::shared_ptr<ManualTaskExecutor> executor;
  ExecutorRegistry registry;
};

class RecordingCommandRunner final : public ICommandRunner {
public:
  auto start(CommandRunRequest request, CommandRunSink sink)
      -> Result<void> override {
    std::lock_guard lock(mutex_);
    instance_id_ = request.instance_id.clone();
    command_ = std::move(request.command);
    sink_ = std::move(sink);
    return ok();
  }

  auto cancel(const InstanceId &instance_id) -> void override {
    std::lock_guard lock(mutex_);
    cancelled_ = instance_id.clone();
  }

  auto quiesce(std::chrono::milliseconds) -> Result<void> override {
    return ok();
  }

  [[nodiscard]] auto command() const -> std::optional<CommandSpec> {
    std::lock_guard lock(mutex_);
    return command_;
  }

  [[nodiscard]] auto cancelled() const -> std::optional<InstanceId> {
    std::lock_guard lock(mutex_);
    return cancelled_;
  }

  auto complete(int exit_code, std::string stdout_output = {}) -> void {
    auto result = make_command_run_result();
    result.exit_code = exit_code;
    result.stdout_output.assign(stdout_output.begin(), stdout_output.end());
    complete_result(std::move(result));
  }

  auto complete_result(CommandRunResult result) -> void {
    std::optional<InstanceId> instance_id;
    std::optional<CommandRunSink> sink;
    {
      std::lock_guard lock(mutex_);
      if (!instance_id_ || !sink_) {
        return;
      }
      instance_id = std::move(instance_id_);
      sink = std::move(sink_);
    }
    if (sink->on_complete) {
      sink->on_complete(*instance_id, std::move(result));
    }
  }

private:
  mutable std::mutex mutex_;
  std::optional<InstanceId> instance_id_;
  std::optional<CommandSpec> command_;
  std::optional<CommandRunSink> sink_;
  std::optional<InstanceId> cancelled_;
};

struct CommandTaskExecutorEnvironment {
  explicit CommandTaskExecutorEnvironment(CommandPolicyConfig policy = {})
      : policy(std::move(policy)) {
    if (this->policy.programs.empty() &&
        this->policy.allowed_programs.empty()) {
      this->policy.allow_unlisted_programs = true;
    }
    if (this->policy.allowed_environment.empty()) {
      this->policy.allow_unlisted_environment = true;
    }
    auto owned_runner = std::make_unique<RecordingCommandRunner>();
    runner = owned_runner.get();
    auto executor = command::detail::create_task_executor(
        std::move(owned_runner), this->policy);
    if (!executor) {
      throw std::runtime_error(executor.error().message());
    }
    auto registered = registry.register_executor(std::move(*executor));
    if (!registered) {
      throw std::runtime_error(registered.error().message());
    }
  }

  RecordingCommandRunner *runner{};
  CommandPolicyConfig policy;
  ExecutorRegistry registry;
};

[[nodiscard]] auto command_config(
    std::string program, std::vector<std::string> arguments = {},
    std::vector<std::pair<std::string, std::string>> environment = {},
    std::vector<std::pair<std::string, std::string>> input_environment = {})
    -> JsonValue {
  JsonValue config = JsonValue::object_t{};
  config["program"] = std::move(program);

  JsonValue args = JsonValue::array_t{};
  for (auto &argument : arguments) {
    args.get_array().push_back(std::move(argument));
  }
  config["arguments"] = std::move(args);

  JsonValue env = JsonValue::array_t{};
  for (auto &[key, value] : environment) {
    JsonValue entry = JsonValue::object_t{};
    entry["key"] = std::move(key);
    entry["value"] = std::move(value);
    env.get_array().push_back(std::move(entry));
  }
  config["env"] = std::move(env);

  JsonValue input_env = JsonValue::array_t{};
  for (auto &[input, variable] : input_environment) {
    JsonValue entry = JsonValue::object_t{};
    entry["input"] = std::move(input);
    entry["environment"] = std::move(variable);
    input_env.get_array().push_back(std::move(entry));
  }
  config["input_env"] = std::move(input_env);
  return config;
}

[[nodiscard]] auto wait_for_state(WorkflowRuntime &runtime, Runtime &core,
                                  const WorkflowRunId &run_id, RunState state,
                                  std::chrono::milliseconds timeout =
                                      std::chrono::seconds(5))
    -> Result<std::shared_ptr<const RunSnapshot>> {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    auto snapshot = sync_wait_on_runtime(core, runtime.snapshot(run_id));
    if (snapshot && (*snapshot)->state == state) {
      return snapshot;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  return fail(Error::Timeout);
}

[[nodiscard]] auto wait_for_task_state(
    WorkflowRuntime &runtime, Runtime &core, const WorkflowRunId &run_id,
    std::size_t task_index, TaskState state,
    std::chrono::milliseconds timeout = std::chrono::seconds(5))
    -> Result<std::shared_ptr<const RunSnapshot>> {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    auto snapshot = sync_wait_on_runtime(core, runtime.snapshot(run_id));
    if (snapshot && task_index < (*snapshot)->tasks.size() &&
        (*snapshot)->tasks[task_index].state == state) {
      return snapshot;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  return fail(Error::Timeout);
}

[[nodiscard]] auto wait_for_attempt_state(
    WorkflowRuntime &runtime, Runtime &core, const WorkflowRunId &run_id,
    std::size_t task_index, std::size_t attempt_index, AttemptState state,
    std::chrono::milliseconds timeout = std::chrono::seconds(5))
    -> Result<std::shared_ptr<const RunSnapshot>> {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    auto snapshot = sync_wait_on_runtime(core, runtime.snapshot(run_id));
    if (snapshot && task_index < (*snapshot)->tasks.size() &&
        attempt_index < (*snapshot)->tasks[task_index].attempts.size() &&
        (*snapshot)->tasks[task_index].attempts[attempt_index].state == state) {
      return snapshot;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  return fail(Error::Timeout);
}

[[nodiscard]] auto base_plan(std::string_view id) -> WorkflowPlan {
  WorkflowPlan plan;
  plan.workflow_id = WorkflowId{id};
  return plan;
}

struct ExecutorContinuationObservation {
  shard_id before{kInvalidShard};
  shard_id after{kInvalidShard};
  ExecutorOutputs outputs;
};

class FailingArtifactStore final : public IArtifactStore {
public:
  [[nodiscard]] auto put(std::span<const std::byte>, std::string)
      -> Result<ArtifactRef> override {
    return fail(Error::ResourceExhausted);
  }

  [[nodiscard]] auto get(const ArtifactId &) const
      -> Result<ArtifactBlob> override {
    return fail(Error::NotFound);
  }

  auto erase(const ArtifactId &) -> Result<void> override {
    return fail(Error::NotFound);
  }
};

[[nodiscard]] auto observe_executor_completion(
    Runtime &runtime, ExecutorRegistry &registry)
    -> task<Result<ExecutorContinuationObservation>> {
  if (!runtime.is_current_shard()) {
    co_return fail(Error::InvalidState);
  }
  const auto before = runtime.current_shard();
  auto outputs = co_await execute_task_async(
      runtime, before, registry, "test",
      TaskExecutionRequest{
          .instance_id = InstanceId{"executor-continuation"},
          .outputs = {WorkflowPortId{"result"}},
      });
  if (!outputs) {
    co_return fail(make_error_code(outputs.error().kind));
  }
  const auto after = runtime.is_current_shard() ? runtime.current_shard()
                                                : kInvalidShard;
  co_return ok(ExecutorContinuationObservation{
      .before = before,
      .after = after,
      .outputs = std::move(*outputs),
  });
}

} // namespace

TEST(WorkflowPlanLoaderTest, ParsesJsonPlanWithOpaqueExecutorConfig) {
  constexpr std::string_view json_text = R"({
    "workflow_id":"loader-json",
    "nodes":[{
      "id":"custom",
      "executor":"test",
      "outputs":["result"],
      "timeout_sec":30,
      "config":{"operation":"analyze","options":{"batch":8}}
    }]
  })";
  auto json_plan = WorkflowPlanLoader::from_json(json_text);
  ASSERT_TRUE(json_plan.has_value()) << json_plan.error().message();
  EXPECT_EQ(json_plan->workflow_id, WorkflowId{"loader-json"});
  ASSERT_EQ(json_plan->nodes.size(), 1U);
  EXPECT_EQ(json_plan->nodes.front().executor, "test");
  ASSERT_TRUE(json_plan->nodes.front().config.is_object());
  EXPECT_EQ(json_plan->nodes.front()
                .config.get_object()
                .at("operation")
                .as<std::string>(),
            "analyze");
  ASSERT_EQ(json_plan->nodes.front().outputs.size(), 1U);
  EXPECT_EQ(json_plan->nodes.front().outputs.front(),
            WorkflowPortId{"result"});
  TestExecutorEnvironment environment;
  EXPECT_TRUE(PlanCompiler{environment.registry}
                  .compile(*json_plan)
                  .has_value());

  constexpr std::string_view invalid_policy_json = R"({
    "workflow_id":"loader-invalid-policy",
    "nodes":[{"id":"task","executor":"test"}],
    "policy":{"failure_policy":"unknown"}
  })";
  auto invalid_policy = WorkflowPlanLoader::from_json(invalid_policy_json);
  ASSERT_FALSE(invalid_policy.has_value());
  EXPECT_EQ(invalid_policy.error(), make_error_code(Error::InvalidArgument));
}

TEST(WorkflowPlanLoaderTest, RoundTripsInputsConditionsAndPublishedOutputs) {
  auto plan = base_plan("loader-round-trip");
  plan.schema_version = 7;
  plan.policy.failure_policy = FailurePolicy::FailFast;
  plan.policy.budget.max_nodes = 8;
  plan.policy.budget.max_parallel_nodes = 2;
  plan.policy.budget.max_total_output_bytes = 4096;
  plan.policy.budget.max_run_duration = std::chrono::seconds(45);
  plan.nodes = {
      NodePlan{
          .node_id = WorkflowNodeId{"source"},
          .name = "Source",
          .executor = "test",
          .outputs = {WorkflowPortId{"result"}},
      },
      NodePlan{
          .node_id = WorkflowNodeId{"target"},
          .name = "Target",
          .executor = "test",
          .inputs = {InputBinding{
              .input = WorkflowPortId{"value"},
              .source = OutputRef{.node_id = WorkflowNodeId{"source"},
                                  .port = WorkflowPortId{"result"}},
          }},
          .outputs = {WorkflowPortId{"result"}},
          .max_retries = 2,
          .retry_initial_delay = std::chrono::milliseconds(25),
          .retry_max_delay = std::chrono::milliseconds(100),
          .timeout = std::chrono::seconds(12),
          .checkpoint = true,
      },
  };
  plan.edges.push_back(ConditionalEdge{
      .source = OutputRef{.node_id = WorkflowNodeId{"source"},
                          .port = WorkflowPortId{"result"}},
      .target = WorkflowNodeId{"target"},
      .condition = ConditionExpr{.kind = ConditionKind::BoolEquals,
                                 .expected_bool = true},
  });
  plan.outputs.push_back(OutputRef{.node_id = WorkflowNodeId{"target"},
                                   .port = WorkflowPortId{"result"}});

  auto encoded = WorkflowPlanLoader::to_json(plan);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().message();
  auto decoded = WorkflowPlanLoader::from_json(*encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().message();
  ASSERT_EQ(decoded->nodes.size(), 2U);
  ASSERT_EQ(decoded->nodes[1].inputs.size(), 1U);
  EXPECT_EQ(decoded->nodes[1].inputs.front().input, WorkflowPortId{"value"});
  EXPECT_EQ(decoded->nodes[1].inputs.front().source.node_id,
            WorkflowNodeId{"source"});
  ASSERT_EQ(decoded->edges.size(), 1U);
  EXPECT_EQ(decoded->edges.front().condition.kind, ConditionKind::BoolEquals);
  EXPECT_TRUE(decoded->edges.front().condition.expected_bool);
  ASSERT_EQ(decoded->outputs.size(), 1U);
  EXPECT_EQ(decoded->outputs.front().node_id, WorkflowNodeId{"target"});
  EXPECT_EQ(decoded->policy.failure_policy, FailurePolicy::FailFast);

  constexpr std::string_view invalid_condition = R"({
    "workflow_id":"invalid-condition",
    "nodes":[{"id":"source","executor":"test"}],
    "edges":[{
      "source_node":"source",
      "target":"source",
      "condition":{"kind":"unsupported"}
    }]
  })";
  auto rejected = WorkflowPlanLoader::from_json(invalid_condition);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error(), make_error_code(Error::InvalidArgument));

  constexpr std::string_view invalid_node = R"({
    "workflow_id":"invalid-node",
    "nodes":[{
      "id":"task",
      "executor":"test",
      "retry_initial_delay_ms":100,
      "retry_max_delay_ms":10
    }]
  })";
  auto invalid_node_result = WorkflowPlanLoader::from_json(invalid_node);
  ASSERT_FALSE(invalid_node_result.has_value());
  EXPECT_EQ(invalid_node_result.error(),
            make_error_code(Error::InvalidArgument));
}

TEST(WorkflowStateModelTest, RejectsIllegalTerminalTransitions) {
  EXPECT_TRUE(can_transition(RunState::Running, RunState::Pausing));
  EXPECT_TRUE(can_transition(RunState::Pausing, RunState::Paused));
  EXPECT_TRUE(can_transition(RunState::Paused, RunState::Running));
  EXPECT_TRUE(can_transition(RunState::Running, RunState::Stopping));
  EXPECT_TRUE(can_transition(RunState::Stopping, RunState::Cancelled));
  EXPECT_FALSE(can_transition(RunState::Cancelled, RunState::Running));

  EXPECT_TRUE(can_transition(TaskState::Running, TaskState::RetryWaiting));
  EXPECT_TRUE(can_transition(TaskState::RetryWaiting, TaskState::Ready));
  EXPECT_FALSE(can_transition(TaskState::Succeeded, TaskState::Running));

  EXPECT_TRUE(
      can_transition(AttemptState::Running, AttemptState::Terminating));
  EXPECT_TRUE(
      can_transition(AttemptState::Terminating, AttemptState::TimedOut));
  EXPECT_FALSE(
      can_transition(AttemptState::Succeeded, AttemptState::Running));
}

TEST(WorkflowControlPlaneTest, DeduplicatesPlansByDigest) {
  TestExecutorEnvironment environment;
  AdmissionConfig admission;
  admission.allowed_executors = {"test"};
  WorkflowControlPlane control{environment.registry, AdmissionPolicy{admission}};
  auto plan = base_plan("dedupe");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}}});
  auto first = control.register_plan(plan);
  auto second = control.register_plan(std::move(plan));
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ((*first)->plan_id, (*second)->plan_id);
  EXPECT_EQ(control.list_plans().size(), 1U);

  auto fail_fast = base_plan("dedupe");
  fail_fast.policy.failure_policy = FailurePolicy::FailFast;
  fail_fast.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  auto changed = control.register_plan(std::move(fail_fast));
  ASSERT_TRUE(changed.has_value());
  EXPECT_NE((*first)->plan_id, (*changed)->plan_id);
  EXPECT_NE((*first)->digest, (*changed)->digest);
  EXPECT_EQ(control.list_plans().size(), 2U);
}

TEST(WorkflowControlPlaneTest, DigestIgnoresExecutorConfigObjectKeyOrder) {
  TestExecutorEnvironment environment;
  AdmissionConfig admission;
  admission.allowed_executors = {"test"};
  WorkflowControlPlane control{environment.registry, AdmissionPolicy{admission}};

  auto first_plan = base_plan("config-order");
  JsonValue first_config = JsonValue::object_t{};
  first_config["alpha"] = 1;
  first_config["nested"] = JsonValue::object_t{};
  first_config["nested"]["left"] = true;
  first_config["nested"]["right"] = "value";
  first_plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"task"},
      .executor = "test",
      .config = std::move(first_config),
  });

  auto second_plan = base_plan("config-order");
  JsonValue second_config = JsonValue::object_t{};
  second_config["nested"] = JsonValue::object_t{};
  second_config["nested"]["right"] = "value";
  second_config["nested"]["left"] = true;
  second_config["alpha"] = 1;
  second_plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"task"},
      .executor = "test",
      .config = std::move(second_config),
  });

  auto first = control.register_plan(std::move(first_plan));
  auto second = control.register_plan(std::move(second_plan));
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ((*first)->digest, (*second)->digest);
  EXPECT_EQ((*first)->plan_id, (*second)->plan_id);
  EXPECT_EQ(control.list_plans().size(), 1U);
}

TEST(WorkflowControlPlaneTest, DigestIncludesPublishedOutputs) {
  TestExecutorEnvironment environment;
  AdmissionConfig admission;
  admission.allowed_executors = {"test"};
  WorkflowControlPlane control{environment.registry, AdmissionPolicy{admission}};

  auto internal_only = base_plan("published-output-digest");
  internal_only.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"task"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  auto published = internal_only;
  published.outputs.push_back(OutputRef{
      .node_id = WorkflowNodeId{"task"},
      .port = WorkflowPortId{"result"},
  });

  auto first = control.register_plan(std::move(internal_only));
  auto second = control.register_plan(std::move(published));
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  EXPECT_NE((*first)->digest, (*second)->digest);
  EXPECT_NE((*first)->plan_id, (*second)->plan_id);
  EXPECT_EQ(control.list_plans().size(), 2U);
}

TEST(WorkflowControlPlaneTest, EnforcesServerAdmissionPolicy) {
  TestExecutorEnvironment environment;
  AdmissionConfig config;
  config.allow_unlisted_executors = false;
  config.allowed_executors = {"test"};
  config.max_parallel_nodes = 32;
  WorkflowControlPlane control{environment.registry, AdmissionPolicy{config}};

  auto allowed = base_plan("admission-allowed");
  allowed.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  EXPECT_TRUE(control.register_plan(std::move(allowed)).has_value());

  auto blocked_executor = base_plan("admission-executor");
  blocked_executor.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .executor = "blocked",
      .outputs = {WorkflowPortId{"result"}},
  });
  auto executor_result = control.register_plan(std::move(blocked_executor));
  ASSERT_FALSE(executor_result.has_value());
  EXPECT_EQ(executor_result.error(), make_error_code(Error::Unauthorized));

  auto excessive_budget = base_plan("admission-budget");
  excessive_budget.policy.budget.max_parallel_nodes = 33;
  excessive_budget.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  auto budget_result = control.register_plan(std::move(excessive_budget));
  ASSERT_FALSE(budget_result.has_value());
  EXPECT_EQ(budget_result.error(), make_error_code(Error::ResourceExhausted));
}

TEST(WorkflowPlanCompilerTest, RejectsCyclesAndInvalidExecutorConfig) {
  TestExecutorEnvironment environment;
  PlanCompiler compiler{environment.registry};

  auto cycle = base_plan("cycle");
  cycle.nodes = {
      NodePlan{.node_id = WorkflowNodeId{"a"},
               .executor = "test",
               .inputs = {InputBinding{.input = WorkflowPortId{"value"},
                                      .source = OutputRef{
                                          .node_id = WorkflowNodeId{"b"},
                                          .port = WorkflowPortId{"result"}}}},
               .outputs = {WorkflowPortId{"result"}}},
      NodePlan{.node_id = WorkflowNodeId{"b"},
               .executor = "test",
               .inputs = {InputBinding{.input = WorkflowPortId{"value"},
                                      .source = OutputRef{
                                          .node_id = WorkflowNodeId{"a"},
                                          .port = WorkflowPortId{"result"}}}},
               .outputs = {WorkflowPortId{"result"}}},
  };
  auto cycle_result = compiler.compile(std::move(cycle));
  ASSERT_FALSE(cycle_result.has_value());
  EXPECT_EQ(cycle_result.error(), make_error_code(Error::CycleDetected));

  CommandTaskExecutorEnvironment command_environment;
  auto relative_command = base_plan("relative-command");
  relative_command.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .executor = "command",
      .config = command_config("./true"),
      .outputs = {WorkflowPortId{"result"}},
  });
  auto relative_result =
      PlanCompiler{command_environment.registry}.compile(
          std::move(relative_command));
  ASSERT_FALSE(relative_result.has_value());
  EXPECT_EQ(relative_result.error(), make_error_code(Error::InvalidArgument));

  auto unknown_executor = base_plan("unknown-executor");
  unknown_executor.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"task"},
      .executor = "unknown",
  });
  auto unknown_result = compiler.compile(std::move(unknown_executor));
  ASSERT_FALSE(unknown_result.has_value());
  EXPECT_EQ(unknown_result.error(), make_error_code(Error::Unsupported));
}

TEST(CommandTaskExecutorTest, OwnsCommandPolicyAndInputMapping) {
  CommandPolicyConfig policy;
  policy.allow_unlisted_programs = false;
  policy.programs = {{.name = "echo", .path = "/bin/echo"}};
  policy.allow_unlisted_environment = false;
  policy.allowed_environment = {"UPSTREAM"};
  CommandTaskExecutorEnvironment environment{policy};

  std::vector<InputBinding> inputs{
      InputBinding{.input = WorkflowPortId{"value"},
                   .source = OutputRef{
                       .node_id = WorkflowNodeId{"source"},
                       .port = WorkflowPortId{"result"}}}};
  std::vector<WorkflowPortId> outputs{WorkflowPortId{"result"}};
  const ExecutorCompileContext context{.inputs = inputs, .outputs = outputs};

  auto compiled = environment.registry.compile(
      "command",
      command_config("echo", {}, {}, {{"value", "UPSTREAM"}}),
      context);
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  auto blocked_program = environment.registry.compile(
      "command", command_config("/bin/cat"), context);
  ASSERT_FALSE(blocked_program.has_value());
  EXPECT_EQ(blocked_program.error(), make_error_code(Error::Unauthorized));

  auto blocked_environment = environment.registry.compile(
      "command", command_config("echo", {}, {{"SECRET", "value"}}),
      context);
  ASSERT_FALSE(blocked_environment.has_value());
  EXPECT_EQ(blocked_environment.error(),
            make_error_code(Error::Unauthorized));

  auto sandbox_override = command_config("echo");
  sandbox_override["network"] = true;
  auto blocked_override = environment.registry.compile(
      "command", std::move(sandbox_override), context);
  ASSERT_FALSE(blocked_override.has_value());
  EXPECT_EQ(blocked_override.error(), make_error_code(Error::ParseError));

  std::optional<TaskExecutionResult> completion;
  TaskExecutionSink sink;
  sink.on_complete = [&completion](const InstanceId &,
                                   TaskExecutionResult result) {
    completion.emplace(std::move(result));
  };
  ExecutorInputs values;
  values.emplace(
      "value",
      std::make_shared<const WorkflowValue>(std::string{"hello"}));
  const InstanceId instance_id{"command-adapter"};
  auto started = environment.registry.start(
      "command",
      TaskExecutionRequest{
          .instance_id = instance_id.clone(),
          .config = std::move(*compiled),
          .inputs = std::move(values),
          .outputs = outputs,
          .timeout = std::chrono::seconds(1),
      },
      std::move(sink));
  ASSERT_TRUE(started.has_value()) << started.error().message();

  auto command = environment.runner->command();
  ASSERT_TRUE(command.has_value());
  EXPECT_EQ(command->program,
            std::filesystem::canonical("/bin/echo").string());
  ASSERT_TRUE(command->environment.contains("UPSTREAM"));
  EXPECT_EQ(command->environment.at("UPSTREAM"), "hello");

  environment.runner->complete(0, "done");
  ASSERT_TRUE(completion.has_value());
  ASSERT_TRUE(completion->has_value()) << completion->error().message;
  ASSERT_EQ((*completion)->size(), 1U);
  EXPECT_EQ((*completion)->front().first, WorkflowPortId{"result"});
  EXPECT_EQ(std::get<std::string>((*completion)->front().second), "done");

  environment.registry.cancel("command", instance_id);
  ASSERT_TRUE(environment.runner->cancelled().has_value());
  EXPECT_EQ(*environment.runner->cancelled(), instance_id);
}

TEST(CommandTaskExecutorTest, DefaultPolicyRejectsUnlistedPrograms) {
  auto runner = std::make_unique<RecordingCommandRunner>();
  CommandPolicyConfig policy;
  auto executor =
      command::detail::create_task_executor(std::move(runner), policy);
  ASSERT_TRUE(executor.has_value()) << executor.error().message();

  ExecutorRegistry registry;
  ASSERT_TRUE(registry.register_executor(std::move(*executor)).has_value());
  std::vector<InputBinding> inputs;
  std::vector<WorkflowPortId> outputs{WorkflowPortId{"result"}};
  auto compiled = registry.compile(
      "command", command_config("/bin/true"),
      ExecutorCompileContext{.inputs = inputs, .outputs = outputs});
  ASSERT_FALSE(compiled.has_value());
  EXPECT_EQ(compiled.error(), make_error_code(Error::Unauthorized));
}

TEST(CommandTaskExecutorTest, MapsAllInputTypesAndCompletionFailures) {
  CommandPolicyConfig policy;
  policy.allow_unlisted_programs = false;
  policy.programs = {{.name = "echo", .path = "/bin/echo"}};
  policy.allow_unlisted_environment = false;
  policy.allowed_environment = {"NULL_VALUE", "BOOL_VALUE", "INT_VALUE",
                                "DOUBLE_VALUE", "STRING_VALUE", "JSON_VALUE",
                                "ARTIFACT_VALUE"};
  CommandTaskExecutorEnvironment environment{policy};

  std::vector<InputBinding> bindings;
  std::vector<std::pair<std::string, std::string>> input_environment;
  for (const auto &[input, variable] :
       std::vector<std::pair<std::string, std::string>>{
           {"null", "NULL_VALUE"},       {"boolean", "BOOL_VALUE"},
           {"integer", "INT_VALUE"},     {"real", "DOUBLE_VALUE"},
           {"text", "STRING_VALUE"},     {"json", "JSON_VALUE"},
           {"artifact", "ARTIFACT_VALUE"}}) {
    bindings.push_back(InputBinding{
        .input = WorkflowPortId{input},
        .source = OutputRef{.node_id = WorkflowNodeId{"source"},
                            .port = WorkflowPortId{input}},
    });
    input_environment.emplace_back(input, variable);
  }
  const std::vector outputs{WorkflowPortId{"result"}};
  auto compiled = environment.registry.compile(
      "command", command_config("echo", {}, {}, input_environment),
      ExecutorCompileContext{.inputs = bindings, .outputs = outputs});
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  JsonValue object = JsonValue::object_t{};
  object["key"] = "value";
  ExecutorInputs values{
      {"null", std::make_shared<const WorkflowValue>(std::monostate{})},
      {"boolean", std::make_shared<const WorkflowValue>(true)},
      {"integer", std::make_shared<const WorkflowValue>(std::int64_t{42})},
      {"real", std::make_shared<const WorkflowValue>(3.5)},
      {"text", std::make_shared<const WorkflowValue>(std::string{"hello"})},
      {"json", std::make_shared<const WorkflowValue>(std::move(object))},
      {"artifact", std::make_shared<const WorkflowValue>(ArtifactRef{
                       .artifact_id = ArtifactId{"artifact-input"}})},
  };
  std::optional<TaskExecutionResult> completion;
  TaskExecutionSink sink{
      .on_complete = [&completion](const InstanceId &,
                                   TaskExecutionResult result) {
        completion.emplace(std::move(result));
      },
  };
  ASSERT_TRUE(environment.registry
                  .start("command",
                         TaskExecutionRequest{
                             .instance_id = InstanceId{"typed-command"},
                             .config = *compiled,
                             .inputs = std::move(values),
                             .outputs = outputs,
                             .timeout = std::chrono::seconds(1),
                         },
                         std::move(sink))
                  .has_value());
  auto command = environment.runner->command();
  ASSERT_TRUE(command.has_value());
  EXPECT_EQ(command->environment.at("NULL_VALUE"), "");
  EXPECT_EQ(command->environment.at("BOOL_VALUE"), "true");
  EXPECT_EQ(command->environment.at("INT_VALUE"), "42");
  EXPECT_EQ(command->environment.at("DOUBLE_VALUE"), "3.5");
  EXPECT_EQ(command->environment.at("STRING_VALUE"), "hello");
  EXPECT_EQ(command->environment.at("JSON_VALUE"), R"({"key":"value"})");
  EXPECT_EQ(command->environment.at("ARTIFACT_VALUE"), "artifact-input");
  environment.runner->complete(0, "done");
  ASSERT_TRUE(completion.has_value());
  ASSERT_TRUE(completion->has_value());

  const auto execute_failure = [&](std::string instance,
                                   CommandRunResult result) {
    completion.reset();
    TaskExecutionSink failure_sink{
        .on_complete = [&completion](const InstanceId &,
                                     TaskExecutionResult value) {
          completion.emplace(std::move(value));
        },
    };
    auto started = environment.registry.start(
        "command",
        TaskExecutionRequest{
            .instance_id = InstanceId{std::move(instance)},
            .config = *compiled,
            .inputs = {{"null", std::make_shared<const WorkflowValue>()},
                       {"boolean", std::make_shared<const WorkflowValue>(true)},
                       {"integer", std::make_shared<const WorkflowValue>(
                                       std::int64_t{1})},
                       {"real", std::make_shared<const WorkflowValue>(1.0)},
                       {"text", std::make_shared<const WorkflowValue>(
                                    std::string{"x"})},
                       {"json", std::make_shared<const WorkflowValue>(
                                    JsonValue::object_t{})},
                       {"artifact", std::make_shared<const WorkflowValue>(
                                        ArtifactRef{.artifact_id =
                                                        ArtifactId{"a"}})}},
            .outputs = outputs,
            .timeout = std::chrono::seconds(1),
        },
        std::move(failure_sink));
    EXPECT_TRUE(started.has_value()) << started.error().message();
    environment.runner->complete_result(std::move(result));
    EXPECT_TRUE(completion.has_value());
    if (!completion || completion->has_value()) {
      return make_execution_failure(
          Error::Unknown, "test_completion_missing",
          "Expected Command completion failure was not observed");
    }
    return completion->error();
  };
  auto timed_out = make_command_run_result();
  timed_out.timed_out = true;
  auto timeout_failure =
      execute_failure("command-timeout", std::move(timed_out));
  EXPECT_EQ(timeout_failure.kind, Error::Timeout);
  EXPECT_EQ(timeout_failure.code, "command_timed_out");
  auto exhausted = make_command_run_result();
  exhausted.resource_exhausted = true;
  exhausted.error = "stderr exceeded configured limit";
  auto exhausted_failure =
      execute_failure("command-exhausted", std::move(exhausted));
  EXPECT_EQ(exhausted_failure.kind, Error::ResourceExhausted);
  EXPECT_EQ(exhausted_failure.code, "command_resource_exhausted");
  auto failed = make_command_run_result();
  failed.exit_code = 7;
  failed.stdout_output = "partial output";
  failed.stderr_output = "invalid configuration";
  auto command_failure =
      execute_failure("command-failed", std::move(failed));
  EXPECT_EQ(command_failure.kind, Error::Unknown);
  EXPECT_EQ(command_failure.code, "command_exit_nonzero");
  EXPECT_EQ(command_failure.details["exit_code"].as<std::int64_t>(), 7);
  EXPECT_EQ(command_failure.details["stdout"].as<std::string>(),
            "partial output");
  EXPECT_EQ(command_failure.details["stderr"].as<std::string>(),
            "invalid configuration");
  auto runner_failed = make_command_run_result();
  runner_failed.error = "runner failed before process completion";
  auto runner_failure =
      execute_failure("command-runner-failed", std::move(runner_failed));
  EXPECT_EQ(runner_failure.kind, Error::Unknown);
  EXPECT_EQ(runner_failure.code, "command_runner_failed");
  EXPECT_EQ(runner_failure.message,
            "runner failed before process completion");

  TaskExecutionSink no_completion;
  ASSERT_TRUE(environment.registry
                  .start("command",
                         TaskExecutionRequest{
                             .instance_id = InstanceId{"no-completion"},
                             .config = *compiled,
                             .inputs = {{"null", std::make_shared<const WorkflowValue>()},
                                        {"boolean", std::make_shared<const WorkflowValue>(true)},
                                        {"integer", std::make_shared<const WorkflowValue>(std::int64_t{1})},
                                        {"real", std::make_shared<const WorkflowValue>(1.0)},
                                        {"text", std::make_shared<const WorkflowValue>(std::string{"x"})},
                                        {"json", std::make_shared<const WorkflowValue>(JsonValue::object_t{})},
                                        {"artifact", std::make_shared<const WorkflowValue>(ArtifactRef{.artifact_id = ArtifactId{"a"}})}},
                             .outputs = outputs,
                         },
                         std::move(no_completion))
                  .has_value());
  environment.runner->complete(0);
}

TEST(CommandTaskExecutorTest, RejectsDuplicateAndMissingInputEnvironment) {
  CommandPolicyConfig policy;
  policy.allow_unlisted_programs = true;
  policy.allow_unlisted_environment = true;
  CommandTaskExecutorEnvironment environment{policy};
  const std::vector inputs{InputBinding{
      .input = WorkflowPortId{"value"},
      .source = OutputRef{.node_id = WorkflowNodeId{"source"},
                          .port = WorkflowPortId{"result"}}}};
  const std::vector outputs{WorkflowPortId{"result"}};
  const ExecutorCompileContext context{.inputs = inputs, .outputs = outputs};

  auto duplicate_static = command_config(
      "/bin/echo", {}, {{"VALUE", "one"}, {"VALUE", "two"}});
  EXPECT_EQ(environment.registry
                .compile("command", std::move(duplicate_static), context)
                .error(),
            make_error_code(Error::InvalidArgument));
  auto duplicate_dynamic = command_config(
      "/bin/echo", {}, {{"VALUE", "one"}}, {{"value", "VALUE"}});
  EXPECT_EQ(environment.registry
                .compile("command", std::move(duplicate_dynamic), context)
                .error(),
            make_error_code(Error::InvalidArgument));
  auto missing_binding =
      command_config("/bin/echo", {}, {}, {{"missing", "MISSING"}});
  EXPECT_EQ(environment.registry
                .compile("command", std::move(missing_binding), context)
                .error(),
            make_error_code(Error::InvalidArgument));

  auto compiled = environment.registry.compile(
      "command", command_config("/bin/echo", {}, {}, {{"value", "VALUE"}}),
      context);
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();
  EXPECT_EQ(environment.registry
                .start("command",
                       TaskExecutionRequest{.instance_id = InstanceId{"missing"},
                                            .config = std::move(*compiled),
                                            .outputs = outputs},
                       {})
                .error(),
            make_error_code(Error::InvalidArgument));
  EXPECT_EQ(command::detail::create_task_executor(nullptr, policy).error(),
            make_error_code(Error::InvalidArgument));
}

TEST(ExecutorRegistryTest, MarshalsAndDeduplicatesCompletion) {
  Runtime core(1, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);

  std::atomic_bool completion_ok{false};
  std::jthread completer([executor = environment.executor, &completion_ok] {
    if (!executor->wait_for_pending(1)) {
      return;
    }
    completion_ok.store(
        executor->complete_next_inline_twice("first", "second"),
        std::memory_order_release);
  });

  auto observed = sync_wait_on_runtime(
      core, observe_executor_completion(core, environment.registry));
  completer.join();

  ASSERT_TRUE(completion_ok.load(std::memory_order_acquire));
  ASSERT_TRUE(observed.has_value()) << observed.error().message();
  EXPECT_NE(observed->before, kInvalidShard);
  EXPECT_EQ(observed->after, observed->before);
  ASSERT_EQ(observed->outputs.size(), 1U);
  EXPECT_EQ(observed->outputs.front().first, WorkflowPortId{"result"});
  EXPECT_EQ(std::get<std::string>(observed->outputs.front().second), "first");

  core.stop();
}

TEST(ExecutionFailureTest, NormalizesSystemErrorsAndProjectsStableJson) {
  EXPECT_EQ(normalize_execution_error(
                std::make_error_code(std::errc::operation_canceled)),
            Error::Cancelled);
  EXPECT_EQ(normalize_execution_error(
                std::make_error_code(std::errc::timed_out)),
            Error::Timeout);
  EXPECT_EQ(normalize_execution_error(
                std::make_error_code(std::errc::permission_denied)),
            Error::Unauthorized);
  EXPECT_EQ(normalize_execution_error(
                std::make_error_code(std::errc::no_such_file_or_directory)),
            Error::NotFound);
  EXPECT_EQ(normalize_execution_error(
                std::make_error_code(std::errc::not_enough_memory)),
            Error::ResourceExhausted);
  EXPECT_EQ(normalize_execution_error(
                std::make_error_code(std::errc::address_family_not_supported)),
            Error::Unknown);
  EXPECT_EQ(normalize_execution_error(make_error_code(Error::Success)),
            Error::Unknown);

  auto normalized = make_execution_failure(Error::Success, {}, {},
                                           JsonValue::array_t{});
  EXPECT_EQ(normalized.kind, Error::Unknown);
  EXPECT_EQ(normalized.code, "unknown");
  EXPECT_EQ(normalized.message, make_error_code(Error::Unknown).message());
  EXPECT_TRUE(normalized.details.is_object());

  const auto cause = std::make_error_code(std::errc::permission_denied);
  auto non_object_details = parse_json(R"("discarded")");
  ASSERT_TRUE(non_object_details.has_value());
  ASSERT_TRUE(non_object_details->is_string());
  auto failure = make_execution_failure(
      cause, "permission_denied", {}, std::move(*non_object_details));
  EXPECT_EQ(failure.kind, Error::Unauthorized);
  EXPECT_EQ(failure.code, "permission_denied");
  EXPECT_EQ(failure.message, cause.message());
  ASSERT_TRUE(failure.details.is_object());
  const auto &cause_json = failure.details["cause"];
  ASSERT_TRUE(cause_json.is_object());
  EXPECT_EQ(cause_json["category"].as<std::string>(),
            cause.category().name());
  EXPECT_EQ(cause_json["value"].as<std::int64_t>(), cause.value());
  EXPECT_EQ(cause_json["message"].as<std::string>(), cause.message());

  const auto projected = execution_failure_json(failure);
  EXPECT_EQ(projected["kind"].as<std::string>(), "unauthorized");
  EXPECT_EQ(projected["code"].as<std::string>(), "permission_denied");
  EXPECT_EQ(projected["message"].as<std::string>(), cause.message());
  EXPECT_TRUE(projected["details"].is_object());
}

TEST(ExecutorRegistryTest, RejectsInvalidAndDuplicateRegistrations) {
  ExecutorRegistry registry;
  EXPECT_EQ(registry.register_executor(nullptr).error(),
            make_error_code(Error::InvalidArgument));
  ASSERT_TRUE(
      registry.register_executor(std::make_shared<ManualTaskExecutor>())
          .has_value());
  EXPECT_EQ(registry
                .register_executor(std::make_shared<ManualTaskExecutor>())
                .error(),
            make_error_code(Error::AlreadyExists));
  EXPECT_EQ(registry
                .start("missing", TaskExecutionRequest{}, TaskExecutionSink{})
                .error(),
            make_error_code(Error::Unsupported));
}

TEST(ExecutorRegistryTest, QuiesceStopsRegisteredExecutorsAndRejectsNewWork) {
  TestExecutorEnvironment environment;

  auto quiesced = environment.registry.quiesce(std::chrono::seconds(1));
  ASSERT_TRUE(quiesced.has_value()) << quiesced.error().message();
  EXPECT_EQ(environment.executor->quiesce_count(), 1U);

  TaskExecutionSink sink;
  auto started = environment.registry.start(
      "test",
      TaskExecutionRequest{.instance_id = InstanceId{"after-quiesce"}},
      std::move(sink));
  ASSERT_FALSE(started.has_value());
  EXPECT_EQ(started.error(), make_error_code(Error::InvalidState));

  auto registered = environment.registry.register_executor(
      std::make_shared<ManualTaskExecutor>());
  ASSERT_FALSE(registered.has_value());
  EXPECT_EQ(registered.error(), make_error_code(Error::InvalidState));
}

TEST(WorkflowPlanCompilerTest, RejectsUnknownFailurePolicy) {
  TestExecutorEnvironment environment;
  auto plan = base_plan("invalid-failure-policy");
  plan.policy.failure_policy = static_cast<FailurePolicy>(255);
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  auto compiled = PlanCompiler{environment.registry}.compile(std::move(plan));
  ASSERT_FALSE(compiled.has_value());
  EXPECT_EQ(compiled.error(), make_error_code(Error::InvalidArgument));
}

TEST(WorkflowPlanCompilerTest, RejectsUnknownSchemaVersion) {
  TestExecutorEnvironment environment;
  auto plan = base_plan("old-schema");
  plan.schema_version = 2;
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  auto compiled = PlanCompiler{environment.registry}.compile(std::move(plan));
  ASSERT_FALSE(compiled.has_value());
  EXPECT_EQ(compiled.error(), make_error_code(Error::InvalidArgument));
}

TEST(WorkflowPlanCompilerTest, RejectsUnknownPublishedOutput) {
  TestExecutorEnvironment environment;
  auto plan = base_plan("unknown-published-output");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"task"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  plan.outputs.push_back(OutputRef{
      .node_id = WorkflowNodeId{"task"},
      .port = WorkflowPortId{"missing"},
  });

  auto compiled =
      PlanCompiler{environment.registry}.compile(std::move(plan));
  ASSERT_FALSE(compiled.has_value());
  EXPECT_EQ(compiled.error(), make_error_code(Error::NotFound));
}

TEST(WorkflowRuntimeTest, PauseDrainsActiveAttemptBeforeResume) {
  Runtime core(2, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);
  WorkflowRuntime runtime(core, environment.registry);

  auto plan = base_plan("pause-flow");
  plan.nodes = {
      NodePlan{
          .node_id = WorkflowNodeId{"first"},
          .executor = "test",
          .outputs = {WorkflowPortId{"result"}},
      },
      NodePlan{
          .node_id = WorkflowNodeId{"second"},
          .executor = "test",
          .inputs = {InputBinding{
              .input = WorkflowPortId{"value"},
              .source = OutputRef{.node_id = WorkflowNodeId{"first"},
                                  .port = WorkflowPortId{"result"}}}},
          .outputs = {WorkflowPortId{"result"}},
      },
  };
  auto compiled = PlanCompiler{environment.registry}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  auto started = runtime.start(
      *compiled, TriggerEnvelope{.workflow_id = WorkflowId{"pause-flow"},
                                 .source = "test",
                                 .event_type = "request"});
  ASSERT_TRUE(started.has_value()) << started.error().message();
  ASSERT_TRUE(environment.executor->wait_for_pending(1));

  auto pause = sync_wait_on_runtime(core, runtime.pause(*started));
  ASSERT_TRUE(pause.has_value()) << pause.error().message();
  ASSERT_TRUE(wait_for_state(runtime, core, *started, RunState::Pausing));
  ASSERT_TRUE(environment.executor->complete_next(0, "first"));

  auto paused = wait_for_state(runtime, core, *started, RunState::Paused);
  ASSERT_TRUE(paused.has_value()) << paused.error().message();
  ASSERT_EQ((*paused)->tasks.size(), 2U);
  EXPECT_EQ((*paused)->tasks[0].state, TaskState::Succeeded);
  EXPECT_EQ((*paused)->tasks[1].state, TaskState::Ready);
  ASSERT_EQ((*paused)->tasks[0].attempts.size(), 1U);
  EXPECT_EQ((*paused)->tasks[0].attempts[0].state,
            AttemptState::Succeeded);
  EXPECT_EQ(environment.executor->pending_count(), 0U);

  auto resume = sync_wait_on_runtime(core, runtime.resume(*started));
  ASSERT_TRUE(resume.has_value()) << resume.error().message();
  ASSERT_TRUE(environment.executor->wait_for_pending(1));
  ASSERT_TRUE(environment.executor->complete_next(0, "second"));

  auto completed =
      wait_for_state(runtime, core, *started, RunState::Succeeded);
  ASSERT_TRUE(completed.has_value()) << completed.error().message();
  EXPECT_EQ((*completed)->tasks[1].state, TaskState::Succeeded);

  auto output = sync_wait_on_runtime(
      core, runtime.output(*started,
                           OutputRef{.node_id = WorkflowNodeId{"second"},
                                     .port = WorkflowPortId{"result"}}));
  ASSERT_TRUE(output.has_value()) << output.error().message();
  EXPECT_EQ(std::get<std::string>(**output), "second");
  EXPECT_FALSE(runtime.evidence(*started).empty());

  core.stop();
}

TEST(WorkflowRuntimeTest, AttemptStartsBeforeExecutorReportsRunning) {
  Runtime core(1, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);
  environment.executor->defer_running_signal();
  WorkflowRuntime runtime(core, environment.registry);

  auto plan = base_plan("attempt-starting");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  auto compiled = PlanCompiler{environment.registry}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value());

  auto started = runtime.start(
      *compiled,
      TriggerEnvelope{.workflow_id = WorkflowId{"attempt-starting"},
                      .source = "test",
                      .event_type = "request"});
  ASSERT_TRUE(started.has_value());
  ASSERT_TRUE(environment.executor->wait_for_pending(1));

  auto starting = wait_for_attempt_state(runtime, core, *started, 0, 0,
                                         AttemptState::Starting);
  ASSERT_TRUE(starting.has_value()) << starting.error().message();
  EXPECT_NE((*starting)->tasks[0].attempts[0].created_at,
            std::chrono::system_clock::time_point{});
  EXPECT_EQ((*starting)->tasks[0].attempts[0].started_at,
            std::chrono::system_clock::time_point{});

  ASSERT_TRUE(environment.executor->signal_running_next());
  auto running = wait_for_attempt_state(runtime, core, *started, 0, 0,
                                        AttemptState::Running);
  ASSERT_TRUE(running.has_value()) << running.error().message();
  EXPECT_NE((*running)->tasks[0].attempts[0].started_at,
            std::chrono::system_clock::time_point{});

  ASSERT_TRUE(environment.executor->complete_next(0, "ok"));
  ASSERT_TRUE(
      wait_for_state(runtime, core, *started, RunState::Succeeded).has_value());
  core.stop();
}

TEST(WorkflowRuntimeTest, RunDeadlineStopsAndReapsActiveAttempt) {
  Runtime core(1, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);
  WorkflowRuntime runtime(core, environment.registry);

  auto plan = base_plan("deadline");
  plan.policy.budget.max_run_duration = std::chrono::milliseconds(25);
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  auto compiled = PlanCompiler{environment.registry}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value());

  auto started = runtime.start(
      *compiled, TriggerEnvelope{.workflow_id = WorkflowId{"deadline"},
                                 .source = "test",
                                 .event_type = "request"});
  ASSERT_TRUE(started.has_value());
  ASSERT_TRUE(environment.executor->wait_for_pending(1));
  auto failed = wait_for_state(runtime, core, *started, RunState::Failed);
  ASSERT_TRUE(failed.has_value()) << failed.error().message();
  ASSERT_TRUE((*failed)->failure.has_value());
  EXPECT_EQ((*failed)->failure->kind, Error::Timeout);
  EXPECT_EQ((*failed)->failure->code, "run_deadline_exceeded");
  ASSERT_TRUE((*failed)->stop_intent.has_value());
  EXPECT_EQ(*(*failed)->stop_intent, StopIntent::Fail);
  ASSERT_EQ((*failed)->tasks.size(), 1U);
  EXPECT_EQ((*failed)->tasks[0].state, TaskState::Cancelled);
  ASSERT_EQ((*failed)->tasks[0].attempts.size(), 1U);
  EXPECT_EQ((*failed)->tasks[0].attempts[0].state,
            AttemptState::Cancelled);
  EXPECT_EQ((*failed)->tasks[0].attempts[0].termination_reason,
            TerminationReason::RunFailed);
  core.stop();
}

TEST(WorkflowRuntimeTest, CancelStaysStoppingUntilAttemptIsReaped) {
  Runtime core(1, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);
  environment.executor->defer_cancel_completion();
  WorkflowRuntime runtime(core, environment.registry);

  auto plan = base_plan("cancel-drain");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  auto compiled = PlanCompiler{environment.registry}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value());

  auto started = runtime.start(
      *compiled, TriggerEnvelope{.workflow_id = WorkflowId{"cancel-drain"},
                                 .source = "test",
                                 .event_type = "request"});
  ASSERT_TRUE(started.has_value());
  ASSERT_TRUE(environment.executor->wait_for_pending(1));

  auto cancelled = sync_wait_on_runtime(core, runtime.cancel(*started));
  ASSERT_TRUE(cancelled.has_value()) << cancelled.error().message();
  auto stopping = wait_for_state(runtime, core, *started, RunState::Stopping);
  ASSERT_TRUE(stopping.has_value()) << stopping.error().message();
  ASSERT_EQ((*stopping)->tasks.size(), 1U);
  EXPECT_EQ((*stopping)->tasks[0].state, TaskState::Running);
  ASSERT_EQ((*stopping)->tasks[0].attempts.size(), 1U);
  EXPECT_EQ((*stopping)->tasks[0].attempts[0].state,
            AttemptState::Terminating);
  EXPECT_EQ((*stopping)->tasks[0].attempts[0].termination_reason,
            TerminationReason::RunCancelled);

  ASSERT_TRUE(environment.executor->complete_next(-1));
  auto completed =
      wait_for_state(runtime, core, *started, RunState::Cancelled);
  ASSERT_TRUE(completed.has_value()) << completed.error().message();
  EXPECT_EQ((*completed)->tasks[0].state, TaskState::Cancelled);
  EXPECT_EQ((*completed)->tasks[0].attempts[0].state,
            AttemptState::Cancelled);
  ASSERT_TRUE((*completed)->stop_intent.has_value());
  EXPECT_EQ(*(*completed)->stop_intent, StopIntent::Cancel);
  core.stop();
}

TEST(WorkflowRuntimeTest, QuiesceCancelsActiveRunsAndRejectsNewStarts) {
  Runtime core(1, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);
  WorkflowRuntime runtime(core, environment.registry);

  auto plan = base_plan("quiesce");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  auto compiled = PlanCompiler{environment.registry}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value());

  auto started = runtime.start(
      *compiled, TriggerEnvelope{.workflow_id = WorkflowId{"quiesce"},
                                 .source = "test",
                                 .event_type = "request"});
  ASSERT_TRUE(started.has_value());
  ASSERT_TRUE(environment.executor->wait_for_pending(1));

  auto quiesced = runtime.quiesce(std::chrono::seconds(2));
  ASSERT_TRUE(quiesced.has_value()) << quiesced.error().message();
  EXPECT_EQ(runtime.active_run_count(), 0U);

  auto completed = sync_wait_on_runtime(core, runtime.snapshot(*started));
  ASSERT_TRUE(completed.has_value()) << completed.error().message();
  EXPECT_EQ((*completed)->state, RunState::Cancelled);
  ASSERT_EQ((*completed)->tasks.size(), 1U);
  EXPECT_EQ((*completed)->tasks[0].state, TaskState::Cancelled);

  auto rejected = runtime.start(
      *compiled, TriggerEnvelope{.workflow_id = WorkflowId{"quiesce"},
                                 .source = "test",
                                 .event_type = "request"});
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error(), make_error_code(Error::SystemNotRunning));
  core.stop();
}

TEST(WorkflowRuntimeTest, SynchronousCancelCompletionIsReentrantSafe) {
  Runtime core(1, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);
  environment.executor->synchronous_cancel_completion();
  WorkflowRuntime runtime(core, environment.registry);

  auto plan = base_plan("sync-cancel");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  auto compiled = PlanCompiler{environment.registry}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value());

  auto started = runtime.start(
      *compiled, TriggerEnvelope{.workflow_id = WorkflowId{"sync-cancel"},
                                 .source = "test",
                                 .event_type = "request"});
  ASSERT_TRUE(started.has_value());
  ASSERT_TRUE(environment.executor->wait_for_pending(1));

  auto cancelled = sync_wait_on_runtime(core, runtime.cancel(*started));
  ASSERT_TRUE(cancelled.has_value()) << cancelled.error().message();
  auto completed =
      wait_for_state(runtime, core, *started, RunState::Cancelled);
  ASSERT_TRUE(completed.has_value()) << completed.error().message();
  EXPECT_EQ((*completed)->tasks[0].state, TaskState::Cancelled);
  EXPECT_EQ((*completed)->tasks[0].attempts[0].state,
            AttemptState::Cancelled);
  core.stop();
}

TEST(WorkflowRuntimeTest, RetryWaitingCreatesDistinctAttempts) {
  Runtime core(1, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);
  WorkflowRuntime runtime(core, environment.registry);

  auto plan = base_plan("retry-attempts");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .executor = "test",
      .outputs = {WorkflowPortId{"stdout"}, WorkflowPortId{"exit_code"},
                  WorkflowPortId{"result"}},
      .max_retries = 1,
      .retry_initial_delay = std::chrono::milliseconds(20),
      .retry_max_delay = std::chrono::milliseconds(20),
  });
  auto compiled = PlanCompiler{environment.registry}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value());

  auto started = runtime.start(
      *compiled, TriggerEnvelope{.workflow_id = WorkflowId{"retry-attempts"},
                                 .source = "test",
                                 .event_type = "request"});
  ASSERT_TRUE(started.has_value());
  ASSERT_TRUE(environment.executor->wait_for_pending(1));
  ASSERT_TRUE(environment.executor->complete_next(1));

  auto waiting = wait_for_task_state(runtime, core, *started, 0,
                                     TaskState::RetryWaiting);
  ASSERT_TRUE(waiting.has_value()) << waiting.error().message();
  ASSERT_EQ((*waiting)->tasks[0].attempts.size(), 1U);
  EXPECT_EQ((*waiting)->tasks[0].attempts[0].state, AttemptState::Failed);
  EXPECT_EQ((*waiting)->tasks[0].attempts[0].failure_class,
            FailureClass::Retryable);
  EXPECT_TRUE((*waiting)->tasks[0].next_attempt_at.has_value());

  ASSERT_TRUE(environment.executor->wait_for_pending(1));
  ASSERT_TRUE(environment.executor->complete_next(0, "ok"));
  auto completed =
      wait_for_state(runtime, core, *started, RunState::Succeeded);
  ASSERT_TRUE(completed.has_value()) << completed.error().message();
  ASSERT_EQ((*completed)->tasks[0].attempt_count, 2U);
  ASSERT_EQ((*completed)->tasks[0].attempts.size(), 2U);
  EXPECT_NE((*completed)->tasks[0].attempts[0].attempt_id,
            (*completed)->tasks[0].attempts[1].attempt_id);
  EXPECT_EQ((*completed)->tasks[0].attempts[0].state,
            AttemptState::Failed);
  EXPECT_EQ((*completed)->tasks[0].attempts[1].state,
            AttemptState::Succeeded);
  EXPECT_EQ((*completed)->tasks[0].attempts[1].exit_code, 0);
  core.stop();
}

TEST(WorkflowRuntimeTest, PermanentFailureDoesNotRetry) {
  Runtime core(1, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);
  environment.executor->fail_start(Error::Unsupported);
  WorkflowRuntime runtime(core, environment.registry);

  auto plan = base_plan("permanent-failure");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
      .max_retries = 3,
      .retry_initial_delay = std::chrono::milliseconds(1),
      .retry_max_delay = std::chrono::milliseconds(1),
  });
  auto compiled = PlanCompiler{environment.registry}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value());

  auto started = runtime.start(
      *compiled,
      TriggerEnvelope{.workflow_id = WorkflowId{"permanent-failure"},
                      .source = "test",
                      .event_type = "request"});
  ASSERT_TRUE(started.has_value());
  auto failed = wait_for_state(runtime, core, *started, RunState::Failed);
  ASSERT_TRUE(failed.has_value()) << failed.error().message();
  ASSERT_TRUE((*failed)->failure.has_value());
  EXPECT_EQ((*failed)->failure->kind, Error::Unsupported);
  EXPECT_EQ((*failed)->failure->code, "executor_start_failed");
  EXPECT_EQ((*failed)->tasks[0].attempt_count, 1U);
  ASSERT_EQ((*failed)->tasks[0].attempts.size(), 1U);
  EXPECT_EQ((*failed)->tasks[0].attempts[0].state, AttemptState::Failed);
  EXPECT_EQ((*failed)->tasks[0].attempts[0].failure_class,
            FailureClass::Permanent);
  core.stop();
}

TEST(WorkflowRuntimeTest, OutputBudgetFailureDoesNotRetry) {
  Runtime core(1, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);
  WorkflowRuntime runtime(core, environment.registry);

  auto plan = base_plan("output-budget-permanent");
  plan.policy.budget.max_total_output_bytes = 4;
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .executor = "test",
      .outputs = {WorkflowPortId{"stdout"}},
      .max_retries = 3,
      .retry_initial_delay = std::chrono::milliseconds(1),
      .retry_max_delay = std::chrono::milliseconds(1),
  });
  auto compiled = PlanCompiler{environment.registry}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value());

  auto started = runtime.start(
      *compiled,
      TriggerEnvelope{.workflow_id = WorkflowId{"output-budget-permanent"},
                      .source = "test",
                      .event_type = "request"});
  ASSERT_TRUE(started.has_value());
  ASSERT_TRUE(environment.executor->wait_for_pending(1));
  ASSERT_TRUE(environment.executor->complete_next(0, "too-large"));
  auto failed = wait_for_state(runtime, core, *started, RunState::Failed);
  ASSERT_TRUE(failed.has_value()) << failed.error().message();
  EXPECT_EQ((*failed)->tasks[0].attempt_count, 1U);
  EXPECT_EQ((*failed)->tasks[0].attempts[0].state, AttemptState::Failed);
  EXPECT_EQ((*failed)->tasks[0].attempts[0].failure_class,
            FailureClass::Permanent);
  core.stop();
}

TEST(WorkflowRuntimeTest, RejectsUndeclaredExecutorOutput) {
  Runtime core(1, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);
  WorkflowRuntime runtime(core, environment.registry);

  auto plan = base_plan("executor-output-contract");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"task"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  auto compiled =
      PlanCompiler{environment.registry}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value());

  auto started = runtime.start(
      *compiled,
      TriggerEnvelope{.workflow_id = WorkflowId{"executor-output-contract"},
                      .source = "test",
                      .event_type = "request"});
  ASSERT_TRUE(started.has_value());
  ASSERT_TRUE(environment.executor->wait_for_pending(1));
  ASSERT_TRUE(environment.executor->complete_next_with_outputs(
      {{WorkflowPortId{"undeclared"}, std::string{"value"}}}));

  auto failed = wait_for_state(runtime, core, *started, RunState::Failed);
  ASSERT_TRUE(failed.has_value()) << failed.error().message();
  ASSERT_EQ((*failed)->tasks.size(), 1U);
  EXPECT_EQ((*failed)->tasks[0].state, TaskState::Failed);
  ASSERT_EQ((*failed)->tasks[0].attempts.size(), 1U);
  EXPECT_EQ((*failed)->tasks[0].attempts[0].failure_class,
            FailureClass::Permanent);
  ASSERT_TRUE((*failed)->tasks[0].attempts[0].failure.has_value());
  EXPECT_EQ((*failed)->tasks[0].attempts[0].failure->kind,
            Error::ProtocolError);
  EXPECT_EQ((*failed)->tasks[0].attempts[0].failure->code,
            "output_contract_violation");
  core.stop();
}

TEST(WorkflowRuntimeTest, FailFastStopsIndependentAttempts) {
  Runtime core(1, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);
  environment.executor->defer_cancel_completion();
  WorkflowRuntime runtime(core, environment.registry);

  auto plan = base_plan("fail-fast");
  plan.policy.failure_policy = FailurePolicy::FailFast;
  plan.policy.budget.max_parallel_nodes = 2;
  plan.nodes = {
      NodePlan{
          .node_id = WorkflowNodeId{"first"},
          .executor = "test",
          .outputs = {WorkflowPortId{"result"}},
      },
      NodePlan{
          .node_id = WorkflowNodeId{"second"},
          .executor = "test",
          .outputs = {WorkflowPortId{"result"}},
      },
  };
  auto compiled = PlanCompiler{environment.registry}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value());

  auto started = runtime.start(
      *compiled, TriggerEnvelope{.workflow_id = WorkflowId{"fail-fast"},
                                 .source = "test",
                                 .event_type = "request"});
  ASSERT_TRUE(started.has_value());
  ASSERT_TRUE(environment.executor->wait_for_pending(2));
  ASSERT_TRUE(environment.executor->complete_next(1));

  auto stopping = wait_for_state(runtime, core, *started, RunState::Stopping);
  ASSERT_TRUE(stopping.has_value()) << stopping.error().message();
  EXPECT_EQ((*stopping)->stop_intent, StopIntent::Fail);
  EXPECT_EQ((*stopping)->tasks[0].state, TaskState::Failed);
  EXPECT_EQ((*stopping)->tasks[1].state, TaskState::Running);
  EXPECT_EQ((*stopping)->tasks[1].attempts[0].state,
            AttemptState::Terminating);

  ASSERT_TRUE(environment.executor->complete_next(-1));
  auto failed = wait_for_state(runtime, core, *started, RunState::Failed);
  ASSERT_TRUE(failed.has_value()) << failed.error().message();
  EXPECT_EQ((*failed)->tasks[0].state, TaskState::Failed);
  EXPECT_EQ((*failed)->tasks[1].state, TaskState::Cancelled);
  core.stop();
}

TEST(WorkflowRuntimeTest, CommandNodeOwnsRunIdAcrossSuspension) {
  namespace fs = std::filesystem;

  const char *home = std::getenv("HOME");
  if (home == nullptr || *home == '\0') {
    GTEST_SKIP() << "HOME is unavailable";
  }

  CommandExecutorConfig executor_config;
  const auto helper = fs::path(home) /
                      ".local/libexec/dagforge/minijail/minijail0";
  const auto policy = fs::path(home) /
                      ".local/libexec/dagforge/minijail/dagforge_command.bpf";
  if (!fs::is_regular_file(helper) || !fs::is_regular_file(policy)) {
    GTEST_SKIP() << "Minijail helper is not installed";
  }
  executor_config.minijail.execution_root =
      (fs::path(home) / ".cache" / "dagforge" / "tests" /
       std::format("workflow-command-{}", ::getpid()))
          .string();
  executor_config.policy.allowed_programs = {"/bin/sh"};
  executor_config.minijail.retain_workdirs = true;
  std::error_code cleanup_error;
  fs::remove_all(executor_config.minijail.execution_root, cleanup_error);

  Runtime core(2, false, 0);
  ASSERT_TRUE(core.start().has_value());
  auto executor = command::create_task_executor(core, executor_config);
  ASSERT_TRUE(executor.has_value()) << executor.error().message();
  ExecutorRegistry executors;
  ASSERT_TRUE(executors.register_executor(std::move(*executor)).has_value());
  WorkflowRuntime runtime(core, executors);

  auto plan = base_plan("command-suspension");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .executor = "command",
      .config = command_config(
          "/bin/sh", {"-c", "sleep 0.05; printf workflow-ok"}),
      .outputs = {WorkflowPortId{"stdout"}, WorkflowPortId{"stderr"},
                  WorkflowPortId{"exit_code"}, WorkflowPortId{"result"}},
  });
  auto compiled = PlanCompiler{executors}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  auto started = runtime.start(
      *compiled,
      TriggerEnvelope{.workflow_id = WorkflowId{"command-suspension"},
                      .source = "test",
                      .event_type = "request"});
  ASSERT_TRUE(started.has_value()) << started.error().message();
  auto completed = wait_for_state(runtime, core, *started, RunState::Succeeded,
                                  std::chrono::seconds(3));
  ASSERT_TRUE(completed.has_value()) << completed.error().message();

  auto output = sync_wait_on_runtime(
      core, runtime.output(*started,
                           OutputRef{.node_id = WorkflowNodeId{"command"},
                                     .port = WorkflowPortId{"stdout"}}));
  ASSERT_TRUE(output.has_value()) << output.error().message();
  const auto *text = std::get_if<std::string>(output->get());
  ASSERT_NE(text, nullptr);
  EXPECT_EQ(*text, "workflow-ok");

  EXPECT_TRUE(executors.quiesce(std::chrono::seconds(5)).has_value());
  core.stop();
  fs::remove_all(executor_config.minijail.execution_root, cleanup_error);
}

TEST(WorkflowRuntimeTest, IdempotentTriggerReturnsExistingRun) {
  Runtime core(1, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);
  WorkflowRuntime runtime(core, environment.registry);

  auto plan = base_plan("idempotent");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}}});
  auto compiled = PlanCompiler{environment.registry}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value());

  TriggerEnvelope trigger{
      .workflow_id = WorkflowId{"idempotent"},
      .source = "api",
      .event_type = "request",
      .idempotency_key = "same-request",
  };
  auto first = runtime.start(*compiled, trigger);
  auto second = runtime.start(*compiled, std::move(trigger));
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(*first, *second);

  ASSERT_TRUE(environment.executor->wait_for_pending(1));
  ASSERT_TRUE(environment.executor->complete_next(0, "done"));
  auto completed = wait_for_state(runtime, core, *first, RunState::Succeeded);
  EXPECT_TRUE(completed.has_value());
  core.stop();
}

TEST(WorkflowRuntimeTest, PropagatesUpstreamOutputToGenericExecutor) {
  Runtime core(2, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);
  WorkflowRuntime runtime(core, environment.registry);

  auto plan = base_plan("generic-dataflow");
  plan.nodes = {
      NodePlan{
          .node_id = WorkflowNodeId{"produce"},
          .executor = "test",
          .outputs = {WorkflowPortId{"result"}},
      },
      NodePlan{
          .node_id = WorkflowNodeId{"consume"},
          .executor = "test",
          .inputs = {InputBinding{
              .input = WorkflowPortId{"value"},
              .source = OutputRef{.node_id = WorkflowNodeId{"produce"},
                                  .port = WorkflowPortId{"result"}}}},
          .outputs = {WorkflowPortId{"result"}},
      },
  };
  auto compiled = PlanCompiler{environment.registry}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  auto started = runtime.start(
      *compiled,
      TriggerEnvelope{.workflow_id = WorkflowId{"generic-dataflow"},
                      .source = "test",
                      .event_type = "request"});
  ASSERT_TRUE(started.has_value());
  ASSERT_TRUE(environment.executor->wait_for_pending(1));
  ASSERT_TRUE(environment.executor->complete_next(0, "hello"));
  ASSERT_TRUE(environment.executor->wait_for_pending(1));
  auto inputs = environment.executor->next_inputs();
  ASSERT_TRUE(inputs.contains("value"));
  const auto *upstream = std::get_if<std::string>(inputs.at("value").get());
  ASSERT_NE(upstream, nullptr);
  EXPECT_EQ(*upstream, "hello");
  ASSERT_TRUE(environment.executor->complete_next(0, "consumed"));
  auto completed = wait_for_state(runtime, core, *started, RunState::Succeeded);
  ASSERT_TRUE(completed.has_value()) << completed.error().message();
  core.stop();
}

TEST(WorkflowRuntimeTest, EvaluatesConditionsForEveryWorkflowValueType) {
  Runtime core(2, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);
  WorkflowRuntime runtime(core, environment.registry);

  std::size_t case_number = 0;
  const auto run_case = [&](WorkflowValue value, ConditionExpr condition,
                            bool selected) {
    const auto workflow_name = std::format("condition-value-{}", case_number++);
    auto plan = base_plan(workflow_name);
    plan.nodes = {
        NodePlan{
            .node_id = WorkflowNodeId{"produce"},
            .executor = "test",
            .outputs = {WorkflowPortId{"result"}},
        },
        NodePlan{
            .node_id = WorkflowNodeId{"consume"},
            .executor = "test",
            .outputs = {WorkflowPortId{"result"}},
        },
    };
    plan.edges.push_back(ConditionalEdge{
        .source = OutputRef{.node_id = WorkflowNodeId{"produce"},
                            .port = WorkflowPortId{"result"}},
        .target = WorkflowNodeId{"consume"},
        .condition = std::move(condition),
    });

    auto compiled = PlanCompiler{environment.registry}.compile(std::move(plan));
    ASSERT_TRUE(compiled.has_value()) << compiled.error().message();
    auto started = runtime.start(
        *compiled,
        TriggerEnvelope{.workflow_id = WorkflowId{workflow_name},
                        .source = "test",
                        .event_type = "condition"});
    ASSERT_TRUE(started.has_value()) << started.error().message();
    ASSERT_TRUE(environment.executor->wait_for_pending(1));
    ASSERT_TRUE(environment.executor->complete_next_with_outputs(
        {{WorkflowPortId{"result"}, std::move(value)}}));

    if (selected) {
      ASSERT_TRUE(environment.executor->wait_for_pending(1));
      ASSERT_TRUE(environment.executor->complete_next(0, "selected"));
    }
    auto completed =
        wait_for_state(runtime, core, *started, RunState::Succeeded);
    ASSERT_TRUE(completed.has_value()) << completed.error().message();
    ASSERT_EQ((*completed)->tasks.size(), 2U);
    EXPECT_EQ((*completed)->tasks[1].state,
              selected ? TaskState::Succeeded : TaskState::Skipped);
    if (!selected) {
      EXPECT_EQ((*completed)->tasks[1].skip_reason,
                SkipReason::ConditionFalse);
    }
  };

  run_case(std::monostate{},
           ConditionExpr{.kind = ConditionKind::BoolEquals,
                         .expected_bool = false},
           true);
  run_case(false,
           ConditionExpr{.kind = ConditionKind::BoolEquals,
                         .expected_bool = false},
           true);
  run_case(true,
           ConditionExpr{.kind = ConditionKind::BoolEquals,
                         .expected_bool = true},
           true);
  run_case(std::int64_t{0},
           ConditionExpr{.kind = ConditionKind::BoolEquals,
                         .expected_bool = false},
           true);
  run_case(std::int64_t{42},
           ConditionExpr{.kind = ConditionKind::BoolEquals,
                         .expected_bool = true},
           true);
  run_case(0.0,
           ConditionExpr{.kind = ConditionKind::BoolEquals,
                         .expected_bool = false},
           true);
  run_case(3.5,
           ConditionExpr{.kind = ConditionKind::BoolEquals,
                         .expected_bool = true},
           true);
  run_case(std::string{},
           ConditionExpr{.kind = ConditionKind::BoolEquals,
                         .expected_bool = false},
           true);
  run_case(std::string{"ready"},
           ConditionExpr{.kind = ConditionKind::BoolEquals,
                         .expected_bool = true},
           true);
  run_case(JsonValue{},
           ConditionExpr{.kind = ConditionKind::BoolEquals,
                         .expected_bool = false},
           true);
  JsonValue object = JsonValue::object_t{};
  object["ready"] = true;
  run_case(std::move(object),
           ConditionExpr{.kind = ConditionKind::BoolEquals,
                         .expected_bool = true},
           true);
  run_case(ArtifactRef{},
           ConditionExpr{.kind = ConditionKind::BoolEquals,
                         .expected_bool = false},
           true);
  run_case(ArtifactRef{.artifact_id = ArtifactId{"artifact-ready"}},
           ConditionExpr{.kind = ConditionKind::BoolEquals,
                         .expected_bool = true},
           true);

  run_case(std::monostate{},
           ConditionExpr{.kind = ConditionKind::StringEquals,
                         .expected_string = ""},
           true);
  run_case(true,
           ConditionExpr{.kind = ConditionKind::StringEquals,
                         .expected_string = "true"},
           true);
  run_case(std::int64_t{42},
           ConditionExpr{.kind = ConditionKind::StringEquals,
                         .expected_string = "42"},
           true);
  run_case(3.5,
           ConditionExpr{.kind = ConditionKind::StringEquals,
                         .expected_string = "3.5"},
           true);
  run_case(std::string{"ready"},
           ConditionExpr{.kind = ConditionKind::StringEquals,
                         .expected_string = "ready"},
           true);
  JsonValue json_text{"ready"};
  run_case(json_text,
           ConditionExpr{.kind = ConditionKind::StringEquals,
                         .expected_string = dump_json(json_text)},
           true);
  run_case(ArtifactRef{.artifact_id = ArtifactId{"artifact-string"}},
           ConditionExpr{.kind = ConditionKind::StringEquals,
                         .expected_string = "artifact-string"},
           true);
  run_case(true,
           ConditionExpr{.kind = ConditionKind::BoolEquals,
                         .expected_bool = false},
           false);

  core.stop();
}

TEST(WorkflowRuntimeTest, ClassifiesExecutorStartFailuresByOperationalMeaning) {
  Runtime core(2, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);
  WorkflowRuntime runtime(core, environment.registry);

  struct FailureCase {
    Error error;
    FailureClass failure_class;
    RunState run_state{RunState::Failed};
  };
  const std::array cases{
      FailureCase{Error::Cancelled, FailureClass::Cancelled,
                  RunState::Cancelled},
      FailureCase{Error::Timeout, FailureClass::Timeout},
      FailureCase{Error::InvalidArgument, FailureClass::Permanent},
      FailureCase{Error::ParseError, FailureClass::Permanent},
      FailureCase{Error::FileNotFound, FailureClass::Permanent},
      FailureCase{Error::NotFound, FailureClass::Permanent},
      FailureCase{Error::AlreadyExists, FailureClass::Permanent},
      FailureCase{Error::InvalidUrl, FailureClass::Permanent},
      FailureCase{Error::ProtocolError, FailureClass::Permanent},
      FailureCase{Error::Unauthorized, FailureClass::Permanent},
      FailureCase{Error::Unsupported, FailureClass::Permanent},
      FailureCase{Error::InvalidState, FailureClass::Permanent},
      FailureCase{Error::ResourceExhausted, FailureClass::Permanent},
      FailureCase{Error::SystemNotRunning, FailureClass::Infrastructure},
      FailureCase{Error::QueueFull, FailureClass::Infrastructure},
      FailureCase{Error::ProcessForkFailed, FailureClass::Infrastructure},
      FailureCase{Error::Unknown, FailureClass::Retryable},
  };

  std::size_t index = 0;
  for (const auto &failure : cases) {
    environment.executor->fail_start(failure.error);
    const auto workflow_name = std::format("failure-class-{}", index++);
    auto plan = base_plan(workflow_name);
    plan.nodes.push_back(NodePlan{
        .node_id = WorkflowNodeId{"task"},
        .executor = "test",
        .outputs = {WorkflowPortId{"result"}},
    });
    auto compiled = PlanCompiler{environment.registry}.compile(std::move(plan));
    ASSERT_TRUE(compiled.has_value()) << compiled.error().message();
    auto started = runtime.start(
        *compiled,
        TriggerEnvelope{.workflow_id = WorkflowId{workflow_name},
                        .source = "test",
                        .event_type = "failure"});
    ASSERT_TRUE(started.has_value()) << started.error().message();
    auto failed =
        wait_for_state(runtime, core, *started, failure.run_state);
    ASSERT_TRUE(failed.has_value())
        << failed.error().message() << " for error="
        << static_cast<int>(failure.error);
    ASSERT_EQ((*failed)->tasks.size(), 1U);
    ASSERT_EQ((*failed)->tasks.front().attempts.size(), 1U);
    EXPECT_EQ((*failed)->tasks.front().attempts.front().failure_class,
              failure.failure_class)
        << static_cast<int>(failure.error);
  }
  core.stop();
}

TEST(WorkflowRuntimeTest, PublishesRunTaskAndCompletionCallbacks) {
  Runtime core(2, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);
  WorkflowRuntime runtime(core, environment.registry);

  auto plan = base_plan("callback-lifecycle");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"task"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  plan.outputs.push_back(OutputRef{.node_id = WorkflowNodeId{"task"},
                                   .port = WorkflowPortId{"result"}});
  auto compiled = PlanCompiler{environment.registry}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  struct CallbackState {
    std::mutex mutex;
    std::vector<RunState> run_states;
    std::vector<TaskState> task_states;
    std::promise<std::shared_ptr<const RunSnapshot>> completed;
  };
  auto callbacks = std::make_shared<CallbackState>();
  auto completed = callbacks->completed.get_future();
  auto started = runtime.start(
      *compiled,
      TriggerEnvelope{.workflow_id = WorkflowId{"callback-lifecycle"},
                      .source = "test",
                      .event_type = "callback"},
      WorkflowCallbacks{
          .on_run_state = [callbacks](const RunSnapshot &snapshot) {
            std::lock_guard lock(callbacks->mutex);
            callbacks->run_states.push_back(snapshot.state);
          },
          .on_task_state =
              [callbacks](const WorkflowRunId &, const TaskSnapshot &snapshot) {
                std::lock_guard lock(callbacks->mutex);
                callbacks->task_states.push_back(snapshot.state);
              },
          .on_complete =
              [callbacks](const WorkflowRunId &,
                          std::shared_ptr<const RunSnapshot> snapshot) {
                callbacks->completed.set_value(std::move(snapshot));
              },
      });
  ASSERT_TRUE(started.has_value()) << started.error().message();
  ASSERT_TRUE(environment.executor->wait_for_pending(1));
  ASSERT_TRUE(environment.executor->complete_next(0, "published"));
  ASSERT_EQ(completed.wait_for(std::chrono::seconds(5)),
            std::future_status::ready);
  auto snapshot = completed.get();
  ASSERT_NE(snapshot, nullptr);
  EXPECT_EQ(snapshot->state, RunState::Succeeded);
  {
    std::lock_guard lock(callbacks->mutex);
    EXPECT_NE(std::ranges::find(callbacks->run_states, RunState::Running),
              callbacks->run_states.end());
    EXPECT_NE(std::ranges::find(callbacks->run_states, RunState::Succeeded),
              callbacks->run_states.end());
    EXPECT_NE(std::ranges::find(callbacks->task_states, TaskState::Running),
              callbacks->task_states.end());
    EXPECT_NE(std::ranges::find(callbacks->task_states, TaskState::Succeeded),
              callbacks->task_states.end());
  }
  core.stop();
}

TEST(WorkflowRuntimeTest, ValidatesStoppedQuiescedAndRestoreLifecycles) {
  Runtime core(1, false, 0);
  TestExecutorEnvironment environment;
  WorkflowRuntime runtime(core, environment.registry, nullptr, nullptr, nullptr,
                          1);

  auto plan = base_plan("lifecycle-validation");
  plan.nodes.push_back(NodePlan{.node_id = WorkflowNodeId{"task"},
                                .executor = "test",
                                .outputs = {WorkflowPortId{"result"}}});
  auto compiled = PlanCompiler{environment.registry}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  EXPECT_EQ(runtime.start(nullptr, TriggerEnvelope{}).error(),
            make_error_code(Error::InvalidArgument));
  EXPECT_EQ(runtime
                .start(*compiled,
                       TriggerEnvelope{
                           .workflow_id = WorkflowId{"different-workflow"}})
                .error(),
            make_error_code(Error::InvalidArgument));
  EXPECT_EQ(runtime
                .start(*compiled,
                       TriggerEnvelope{
                           .workflow_id = WorkflowId{"lifecycle-validation"}})
                .error(),
            make_error_code(Error::SystemNotRunning));
  EXPECT_EQ(runtime.restore(*compiled, WorkflowCheckpoint{}).error(),
            make_error_code(Error::InvalidState));
  EXPECT_TRUE(runtime.quiesce(std::chrono::milliseconds(10)).has_value());

  ASSERT_TRUE(core.start().has_value());
  WorkflowCheckpoint checkpoint;
  checkpoint.snapshot.run_id = WorkflowRunId{"restore-running"};
  checkpoint.snapshot.workflow_id = compiled.value()->workflow_id.clone();
  checkpoint.snapshot.plan_id = compiled.value()->plan_id.clone();
  checkpoint.snapshot.state = RunState::Succeeded;
  EXPECT_EQ(runtime.restore(*compiled, std::move(checkpoint)).error(),
            make_error_code(Error::InvalidState));

  std::promise<Result<void>> shard_quiesce;
  auto shard_result = shard_quiesce.get_future();
  core.post_to(0, [&] {
    shard_quiesce.set_value(runtime.quiesce(std::chrono::milliseconds(10)));
  });
  ASSERT_EQ(shard_result.wait_for(std::chrono::seconds(2)),
            std::future_status::ready);
  EXPECT_EQ(shard_result.get().error(), make_error_code(Error::InvalidState));

  EXPECT_TRUE(runtime.quiesce(std::chrono::seconds(2)).has_value());
  EXPECT_EQ(runtime
                .start(*compiled,
                       TriggerEnvelope{
                           .workflow_id = WorkflowId{"lifecycle-validation"}})
                .error(),
            make_error_code(Error::SystemNotRunning));
  core.stop();
}

TEST(WorkflowRuntimeTest, RestoreRetentionEvictsSnapshotsAndIdempotency) {
  Runtime core(1, false, 0);
  TestExecutorEnvironment environment(core);
  WorkflowRuntime runtime(core, environment.registry, nullptr, nullptr, nullptr,
                          1);

  const auto compile_plan = [&](std::string_view workflow) {
    auto plan = base_plan(workflow);
    plan.nodes.push_back(NodePlan{.node_id = WorkflowNodeId{"task"},
                                  .executor = "test",
                                  .outputs = {WorkflowPortId{"result"}}});
    return PlanCompiler{environment.registry}.compile(std::move(plan));
  };
  auto first_plan = compile_plan("restore-first");
  auto second_plan = compile_plan("restore-second");
  ASSERT_TRUE(first_plan.has_value()) << first_plan.error().message();
  ASSERT_TRUE(second_plan.has_value()) << second_plan.error().message();

  const auto restore_terminal = [&](const auto &plan, std::string run_id,
                                    std::string idempotency_key) {
    WorkflowCheckpoint checkpoint;
    checkpoint.snapshot.run_id = WorkflowRunId{std::move(run_id)};
    checkpoint.snapshot.workflow_id = plan->workflow_id.clone();
    checkpoint.snapshot.plan_id = plan->plan_id.clone();
    checkpoint.snapshot.state = RunState::Succeeded;
    checkpoint.trigger.workflow_id = plan->workflow_id.clone();
    checkpoint.trigger.idempotency_key = std::move(idempotency_key);
    return runtime.restore(plan, std::move(checkpoint));
  };
  ASSERT_TRUE(restore_terminal(*first_plan, "restored-first", "restore-key")
                  .has_value());
  ASSERT_TRUE(restore_terminal(*second_plan, "restored-second", "second-key")
                  .has_value());

  ASSERT_TRUE(core.start().has_value());
  auto first_snapshot = sync_wait_on_runtime(
      core, runtime.snapshot(WorkflowRunId{"restored-first"}));
  EXPECT_EQ(first_snapshot.error(), make_error_code(Error::NotFound));
  auto second_snapshot = sync_wait_on_runtime(
      core, runtime.snapshot(WorkflowRunId{"restored-second"}));
  ASSERT_TRUE(second_snapshot.has_value()) << second_snapshot.error().message();
  EXPECT_EQ((*second_snapshot)->state, RunState::Succeeded);

  auto restarted = runtime.start(
      *first_plan,
      TriggerEnvelope{.workflow_id = WorkflowId{"restore-first"},
                      .idempotency_key = "restore-key"});
  ASSERT_TRUE(restarted.has_value()) << restarted.error().message();
  EXPECT_NE(*restarted, WorkflowRunId{"restored-first"});
  ASSERT_TRUE(environment.executor->wait_for_pending(1));
  ASSERT_TRUE(environment.executor->complete_next());
  ASSERT_TRUE(wait_for_state(runtime, core, *restarted, RunState::Succeeded));
  core.stop();
}

TEST(WorkflowRuntimeTest, SkipsFalseBranchesAndFailedDependencies) {
  const auto run_case = [](bool fail_upstream) {
    Runtime core(1, false, 0);
    ASSERT_TRUE(core.start().has_value());
    TestExecutorEnvironment environment(core);
    WorkflowRuntime runtime(core, environment.registry);

    auto plan = base_plan(fail_upstream ? "skip-failed" : "skip-condition");
    plan.nodes.push_back(NodePlan{
        .node_id = WorkflowNodeId{"source"},
        .executor = "test",
        .outputs = {WorkflowPortId{"result"}},
    });
    plan.nodes.push_back(NodePlan{
        .node_id = WorkflowNodeId{"dependent"},
        .executor = "test",
        .outputs = {WorkflowPortId{"result"}},
    });
    plan.edges.push_back(ConditionalEdge{
        .source = OutputRef{.node_id = WorkflowNodeId{"source"},
                            .port = WorkflowPortId{"result"}},
        .target = WorkflowNodeId{"dependent"},
        .condition = fail_upstream
                         ? ConditionExpr{.kind = ConditionKind::Always}
                         : ConditionExpr{
                               .kind = ConditionKind::StringEquals,
                               .expected_string = "selected"},
    });
    auto compiled = PlanCompiler{environment.registry}.compile(std::move(plan));
    ASSERT_TRUE(compiled.has_value()) << compiled.error().message();
    auto started = runtime.start(
        *compiled,
        TriggerEnvelope{
            .workflow_id = compiled.value()->workflow_id.clone()});
    ASSERT_TRUE(started.has_value()) << started.error().message();
    ASSERT_TRUE(environment.executor->wait_for_pending(1));
    ASSERT_TRUE(environment.executor->complete_next(fail_upstream ? 1 : 0,
                                                    "not-selected"));
    auto terminal = wait_for_state(runtime, core, *started,
                                   fail_upstream ? RunState::Failed
                                                 : RunState::Succeeded);
    ASSERT_TRUE(terminal.has_value()) << terminal.error().message();
    ASSERT_EQ((*terminal)->tasks.size(), 2U);
    EXPECT_EQ((*terminal)->tasks[1].state, TaskState::Skipped);
    EXPECT_EQ((*terminal)->tasks[1].skip_reason,
              fail_upstream ? SkipReason::UpstreamFailed
                            : SkipReason::ConditionFalse);
    EXPECT_EQ(environment.executor->pending_count(), 0U);
    core.stop();
  };

  run_case(false);
  run_case(true);
}

TEST(WorkflowRuntimeTest, MissingRunControlOperationsReturnNotFound) {
  Runtime core(2, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);
  WorkflowRuntime runtime(core, environment.registry);
  const WorkflowRunId missing{"missing-run"};

  auto snapshot = sync_wait_on_runtime(core, runtime.snapshot(missing));
  EXPECT_EQ(snapshot.error(), make_error_code(Error::NotFound));
  auto output = sync_wait_on_runtime(
      core, runtime.output(missing,
                           OutputRef{.node_id = WorkflowNodeId{"node"},
                                     .port = WorkflowPortId{"result"}}));
  EXPECT_EQ(output.error(), make_error_code(Error::NotFound));
  EXPECT_EQ(sync_wait_on_runtime(core, runtime.pause(missing)).error(),
            make_error_code(Error::NotFound));
  EXPECT_EQ(sync_wait_on_runtime(core, runtime.resume(missing)).error(),
            make_error_code(Error::NotFound));
  EXPECT_EQ(sync_wait_on_runtime(core, runtime.cancel(missing)).error(),
            make_error_code(Error::NotFound));
  EXPECT_EQ(runtime.quiesce(std::chrono::milliseconds(0)).error(),
            make_error_code(Error::InvalidArgument));
  core.stop();
}

TEST(WorkflowRuntimeTest, RestoredIdempotencyKeyReturnsAuthoritativeRun) {
  Runtime core(1, false, 0);
  TestExecutorEnvironment environment(core);
  WorkflowRuntime runtime(core, environment.registry);

  auto plan = base_plan("restored-idempotency");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"task"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  const WorkflowPlanId plan_id{"restored-plan"};
  auto compiled = PlanCompiler{environment.registry}.compile(plan, plan_id);
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  WorkflowCheckpoint checkpoint{
      .plan = plan,
      .trigger = TriggerEnvelope{
          .trigger_id = WorkflowTriggerId{"trigger"},
          .workflow_id = WorkflowId{"restored-idempotency"},
          .source = "api",
          .event_type = "request",
          .idempotency_key = "same-key",
      },
      .snapshot = RunSnapshot{
          .run_id = WorkflowRunId{"restored-idempotency__run"},
          .workflow_id = WorkflowId{"restored-idempotency"},
          .plan_id = plan_id.clone(),
          .state = RunState::Succeeded,
          .tasks = {TaskSnapshot{.node_id = WorkflowNodeId{"task"},
                                 .state = TaskState::Succeeded}},
      },
  };
  ASSERT_TRUE(runtime.restore(*compiled, checkpoint).has_value());
  ASSERT_TRUE(core.start().has_value());

  auto duplicate = runtime.start(
      *compiled,
      TriggerEnvelope{.workflow_id = WorkflowId{"restored-idempotency"},
                      .source = "api",
                      .event_type = "request",
                      .idempotency_key = "same-key"});
  ASSERT_TRUE(duplicate.has_value()) << duplicate.error().message();
  EXPECT_EQ(*duplicate, checkpoint.snapshot.run_id);
  EXPECT_EQ(environment.executor->pending_count(), 0U);

  auto snapshot = sync_wait_on_runtime(core, runtime.snapshot(*duplicate));
  ASSERT_TRUE(snapshot.has_value()) << snapshot.error().message();
  EXPECT_EQ((*snapshot)->state, RunState::Succeeded);
  core.stop();
}

TEST(WorkflowRuntimeTest, LargeCommandOutputIsExternalizedAsArtifact) {
  Runtime core(1, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);
  WorkflowRuntime runtime(core, environment.registry);

  auto plan = base_plan("artifact-flow");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .executor = "test",
      .outputs = {WorkflowPortId{"stdout"}},
  });
  auto compiled = PlanCompiler{environment.registry}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value());

  auto started = runtime.start(
      *compiled,
      TriggerEnvelope{.workflow_id = WorkflowId{"artifact-flow"},
                      .source = "test",
                      .event_type = "request"});
  ASSERT_TRUE(started.has_value());
  ASSERT_TRUE(environment.executor->wait_for_pending(1));
  ASSERT_TRUE(
      environment.executor->complete_next(0, std::string(300'000, 'x')));
  ASSERT_TRUE(wait_for_state(runtime, core, *started, RunState::Succeeded));

  auto output = sync_wait_on_runtime(
      core, runtime.output(*started,
                           OutputRef{.node_id = WorkflowNodeId{"command"},
                                     .port = WorkflowPortId{"stdout"}}));
  ASSERT_TRUE(output.has_value());
  const auto *artifact = std::get_if<ArtifactRef>(output->get());
  ASSERT_NE(artifact, nullptr);
  EXPECT_EQ(artifact->size_bytes, 300'000U);
  auto blob = runtime.artifact_store().get(artifact->artifact_id);
  ASSERT_TRUE(blob.has_value());
  EXPECT_EQ(blob->data.size(), 300'000U);

  core.stop();
}

TEST(WorkflowStorageTest, FileArtifactStorePersistsAndVerifiesContent) {
  const auto directory = temporary_test_directory("artifact-store");
  std::error_code error;
  std::filesystem::remove_all(directory, error);

  const std::array<std::byte, 4> data{
      std::byte{'D'}, std::byte{'A'}, std::byte{'G'}, std::byte{'F'}};
  FileArtifactStore writer(directory);
  auto stored = writer.put(data, "application/octet-stream");
  ASSERT_TRUE(stored.has_value()) << stored.error().message();

  FileArtifactStore reader(directory);
  auto loaded = reader.get(stored->artifact_id);
  ASSERT_TRUE(loaded.has_value()) << loaded.error().message();
  EXPECT_EQ(loaded->ref.digest, stored->digest);
  EXPECT_EQ(loaded->ref.media_type, "application/octet-stream");
  ASSERT_EQ(loaded->data.size(), data.size());
  EXPECT_TRUE(std::ranges::equal(loaded->data, data));
  EXPECT_TRUE(reader.erase(stored->artifact_id).has_value());

  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowStorageTest, CheckpointAndEvidenceStoresExposeFailureContracts) {
  CheckpointStore memory;
  WorkflowCheckpoint invalid;
  EXPECT_EQ(memory.save(invalid).error(),
            make_error_code(Error::InvalidArgument));
  EXPECT_EQ(memory.load(WorkflowRunId{"missing"}).error(),
            make_error_code(Error::NotFound));
  EXPECT_EQ(memory.erase(WorkflowRunId{"missing"}).error(),
            make_error_code(Error::NotFound));
  EXPECT_TRUE(memory.list()->empty());

  const auto directory = temporary_test_directory("checkpoint-errors");
  std::error_code error;
  std::filesystem::remove_all(directory, error);
  std::filesystem::create_directories(directory, error);
  {
    std::ofstream output(directory / "corrupt.json");
    output << "not-json";
  }
  CheckpointStore corrupt_store(directory);
  auto listed = corrupt_store.list();
  ASSERT_FALSE(listed.has_value());
  EXPECT_EQ(listed.error(), make_error_code(Error::ParseError));

  const auto evidence_path = directory / "evidence.jsonl";
  {
    std::ofstream output(evidence_path);
    output << "not-json\n\n";
  }
  EvidenceLedger ledger(evidence_path, 1);
  EXPECT_EQ(ledger.size(), 0U);
  EvidenceRecord invalid_record;
  EXPECT_EQ(ledger.append(std::move(invalid_record)).error(),
            make_error_code(Error::InvalidArgument));
  const WorkflowRunId run_id{"run"};
  for (std::string_view node : {"first", "second"}) {
    EvidenceRecord record;
    record.run_id = run_id.clone();
    record.node_id = WorkflowNodeId{node};
    record.type = EvidenceType::TaskCompleted;
    EXPECT_TRUE(ledger.append(std::move(record)).has_value());
  }
  EXPECT_EQ(ledger.size(), 1U);
  auto records = ledger.records(run_id);
  ASSERT_EQ(records.size(), 1U);
  EXPECT_EQ(records.front().node_id, WorkflowNodeId{"second"});
  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowStorageTest, InMemoryArtifactStoreSupportsLifecycleAndNotFound) {
  InMemoryArtifactStore store;
  const std::array<std::byte, 3> data{
      std::byte{'o'}, std::byte{'n'}, std::byte{'e'}};

  auto stored = store.put(data, "text/plain");
  ASSERT_TRUE(stored.has_value()) << stored.error().message();
  EXPECT_EQ(store.size(), 1U);

  auto loaded = store.get(stored->artifact_id);
  ASSERT_TRUE(loaded.has_value()) << loaded.error().message();
  EXPECT_EQ(loaded->ref.media_type, "text/plain");
  EXPECT_TRUE(std::ranges::equal(loaded->data, data));

  EXPECT_TRUE(store.erase(stored->artifact_id).has_value());
  EXPECT_EQ(store.size(), 0U);
  EXPECT_EQ(store.get(stored->artifact_id).error(),
            make_error_code(Error::NotFound));
  EXPECT_EQ(store.erase(stored->artifact_id).error(),
            make_error_code(Error::NotFound));
}

TEST(WorkflowStorageTest, FileArtifactStoreRejectsMissingAndCorruptContent) {
  const auto directory = temporary_test_directory("artifact-corruption");
  std::error_code error;
  std::filesystem::remove_all(directory, error);

  FileArtifactStore store(directory);
  EXPECT_EQ(store.get(ArtifactId{"missing"}).error(),
            make_error_code(Error::NotFound));
  EXPECT_EQ(store.erase(ArtifactId{"missing"}).error(),
            make_error_code(Error::NotFound));

  const std::array<std::byte, 4> data{
      std::byte{'D'}, std::byte{'A'}, std::byte{'T'}, std::byte{'A'}};
  auto first = store.put(data, "application/octet-stream");
  ASSERT_TRUE(first.has_value()) << first.error().message();
  {
    std::ofstream output(directory / (first->artifact_id.str() + ".bin"),
                         std::ios::binary | std::ios::trunc);
    output << "tampered";
  }
  EXPECT_EQ(store.get(first->artifact_id).error(),
            make_error_code(Error::ProtocolError));
  EXPECT_TRUE(store.erase(first->artifact_id).has_value());

  auto second = store.put(data, "application/octet-stream");
  ASSERT_TRUE(second.has_value()) << second.error().message();
  {
    std::ofstream output(directory / (second->artifact_id.str() + ".json"),
                         std::ios::binary | std::ios::trunc);
    output << "not-json";
  }
  EXPECT_EQ(store.get(second->artifact_id).error(),
            make_error_code(Error::ParseError));
  EXPECT_TRUE(store.erase(second->artifact_id).has_value());

  const auto blocked = directory / "blocked";
  {
    std::ofstream output(blocked);
    output << "not a directory";
  }
  FileArtifactStore invalid(blocked);
  auto failed = invalid.put(data, "application/octet-stream");
  EXPECT_FALSE(failed.has_value());

  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowStorageTest, RunValueStoreCoversTypedBudgetAndArtifactScenarios) {
  Runtime core(1, false, 0);
  InMemoryArtifactStore artifacts;
  RunValueStore off_shard(core, 0, artifacts, 1024, 4);
  const OutputRef outside_ref{.node_id = WorkflowNodeId{"outside"},
                              .port = WorkflowPortId{"value"}};
  EXPECT_EQ(off_shard.put(outside_ref, true).error(),
            make_error_code(Error::InvalidState));
  EXPECT_EQ(off_shard.get(outside_ref).error(),
            make_error_code(Error::InvalidState));
  EXPECT_FALSE(off_shard.contains(outside_ref));
  EXPECT_EQ(off_shard.snapshot().error(), make_error_code(Error::InvalidState));
  EXPECT_EQ(off_shard.erase_node(WorkflowNodeId{"outside"}).error(),
            make_error_code(Error::InvalidState));

  ASSERT_TRUE(core.start().has_value());
  struct Observation {
    std::error_code invalid_output;
    std::error_code missing_output;
    std::error_code budget_error;
    std::error_code artifact_error;
    std::size_t snapshot_size{0};
    std::size_t artifact_count{0};
    std::uint64_t total_before_erase{0};
    std::uint64_t total_after_erase{0};
    bool contains_text{false};
    bool text_externalized{false};
    bool json_externalized{false};
  };
  std::promise<Observation> promise;
  auto future = promise.get_future();
  core.post_to(0, [&] {
    Observation observed;
    RunValueStore store(core, 0, artifacts, 4096, 4);
    observed.invalid_output =
        store.put(OutputRef{}, std::string{"bad"}).error();

    const OutputRef flag{.node_id = WorkflowNodeId{"typed"},
                         .port = WorkflowPortId{"flag"}};
    const OutputRef integer{.node_id = WorkflowNodeId{"typed"},
                            .port = WorkflowPortId{"integer"}};
    const OutputRef real{.node_id = WorkflowNodeId{"typed"},
                         .port = WorkflowPortId{"real"}};
    const OutputRef none{.node_id = WorkflowNodeId{"typed"},
                         .port = WorkflowPortId{"none"}};
    const OutputRef artifact{.node_id = WorkflowNodeId{"typed"},
                             .port = WorkflowPortId{"artifact"}};
    const OutputRef text{.node_id = WorkflowNodeId{"typed"},
                         .port = WorkflowPortId{"text"}};
    const OutputRef json{.node_id = WorkflowNodeId{"typed"},
                         .port = WorkflowPortId{"json"}};
    (void)store.put(flag, true);
    (void)store.put(integer, std::int64_t{42});
    (void)store.put(real, 3.5);
    (void)store.put(none, std::monostate{});
    (void)store.put(
        artifact,
        ArtifactRef{.artifact_id = ArtifactId{"existing"},
                    .media_type = "text/plain",
                    .size_bytes = 1,
                    .digest = "digest"});
    (void)store.put(text, std::string{"externalized text"});
    JsonValue object = JsonValue::object_t{};
    object["message"] = "externalized json";
    (void)store.put(json, std::move(object));

    observed.contains_text = store.contains(text);
    observed.missing_output =
        store.get(OutputRef{.node_id = WorkflowNodeId{"missing"},
                            .port = WorkflowPortId{"value"}})
            .error();
    auto text_value = store.get(text);
    auto json_value = store.get(json);
    observed.text_externalized =
        text_value && std::holds_alternative<ArtifactRef>(**text_value);
    observed.json_externalized =
        json_value && std::holds_alternative<ArtifactRef>(**json_value);
    auto snapshot = store.snapshot();
    observed.snapshot_size = snapshot ? snapshot->size() : 0;
    observed.total_before_erase = store.total_output_bytes();
    (void)store.put(integer, std::int64_t{7});
    (void)store.erase_node(WorkflowNodeId{"typed"});
    observed.total_after_erase = store.total_output_bytes();

    RunValueStore budget_store(core, 0, artifacts, 3, 1024);
    observed.budget_error =
        budget_store
            .put(OutputRef{.node_id = WorkflowNodeId{"budget"},
                           .port = WorkflowPortId{"value"}},
                 std::string{"four"})
            .error();

    FailingArtifactStore failing_artifacts;
    RunValueStore failing_store(core, 0, failing_artifacts, 1024, 1);
    observed.artifact_error =
        failing_store
            .put(OutputRef{.node_id = WorkflowNodeId{"artifact"},
                           .port = WorkflowPortId{"value"}},
                 std::string{"cannot-store"})
            .error();
    observed.artifact_count = artifacts.size();
    promise.set_value(std::move(observed));
  });

  ASSERT_EQ(future.wait_for(std::chrono::seconds(2)),
            std::future_status::ready);
  const auto observed = future.get();
  EXPECT_EQ(observed.invalid_output, make_error_code(Error::InvalidArgument));
  EXPECT_EQ(observed.missing_output, make_error_code(Error::NotFound));
  EXPECT_EQ(observed.budget_error, make_error_code(Error::ResourceExhausted));
  EXPECT_EQ(observed.artifact_error,
            make_error_code(Error::ResourceExhausted));
  EXPECT_EQ(observed.snapshot_size, 7U);
  EXPECT_GE(observed.artifact_count, 2U);
  EXPECT_GT(observed.total_before_erase, 0U);
  EXPECT_EQ(observed.total_after_erase, 0U);
  EXPECT_TRUE(observed.contains_text);
  EXPECT_TRUE(observed.text_externalized);
  EXPECT_TRUE(observed.json_externalized);
  core.stop();
}

TEST(WorkflowStorageTest, EvidenceLedgerReloadsJsonLines) {
  const auto directory = temporary_test_directory("evidence-ledger");
  const auto file = directory / "evidence.jsonl";
  std::error_code error;
  std::filesystem::remove_all(directory, error);

  const WorkflowRunId run_id{"evidence-run"};
  {
    EvidenceLedger writer(file);
    EvidenceRecord record;
    record.run_id = run_id.clone();
    record.node_id = WorkflowNodeId{"command"};
    record.type = EvidenceType::TaskCompleted;
    record.actor.subject = "tester";
    record.metadata = JsonValue::object_t{};
    record.metadata["result"] = "ok";
    ASSERT_TRUE(writer.append(std::move(record)).has_value());
  }

  EvidenceLedger reader(file);
  auto records = reader.records(run_id);
  ASSERT_EQ(records.size(), 1U);
  EXPECT_EQ(records.front().node_id, WorkflowNodeId{"command"});
  EXPECT_EQ(records.front().type, EvidenceType::TaskCompleted);
  EXPECT_EQ(records.front().actor.subject, "tester");

  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowStorageTest, EvidenceLedgerRetainsNewestRecords) {
  const auto directory = temporary_test_directory("evidence-retention");
  const auto file = directory / "evidence.jsonl";
  std::error_code error;
  std::filesystem::remove_all(directory, error);
  const WorkflowRunId run_id{"retained-run"};

  {
    EvidenceLedger writer(file, 2);
    for (std::string_view node : {"first", "second", "third"}) {
      EvidenceRecord record;
      record.run_id = run_id.clone();
      record.node_id = WorkflowNodeId{node};
      record.type = EvidenceType::TaskCompleted;
      ASSERT_TRUE(writer.append(std::move(record)).has_value());
    }
    EXPECT_EQ(writer.size(), 2U);
  }

  EvidenceLedger reader(file, 2);
  auto records = reader.records(run_id);
  ASSERT_EQ(records.size(), 2U);
  EXPECT_EQ(records[0].node_id, WorkflowNodeId{"second"});
  EXPECT_EQ(records[1].node_id, WorkflowNodeId{"third"});
  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowStorageTest, CheckpointStoreRoundTripsPlanStateAndValues) {
  const auto directory = temporary_test_directory("checkpoint-store");
  std::error_code error;
  std::filesystem::remove_all(directory, error);

  TestExecutorEnvironment environment;
  auto plan = base_plan("persisted-plan");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .executor = "test",
      .config = JsonValue{{"message", "hello"}},
      .outputs = {WorkflowPortId{"stdout"}},
  });
  auto compiled = PlanCompiler{environment.registry}.compile(plan);
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  WorkflowCheckpoint checkpoint{
      .plan = plan,
      .trigger = TriggerEnvelope{
          .trigger_id = WorkflowTriggerId{"trigger"},
          .workflow_id = WorkflowId{"persisted-plan"},
          .source = "test",
          .event_type = "request",
          .payload = std::string{"payload"},
      },
      .snapshot = RunSnapshot{
          .run_id = WorkflowRunId{"persisted-plan__run"},
          .workflow_id = WorkflowId{"persisted-plan"},
          .plan_id = (*compiled)->plan_id.clone(),
          .state = RunState::Succeeded,
          .tasks = {TaskSnapshot{.node_id = WorkflowNodeId{"command"},
                                 .state = TaskState::Succeeded}},
      },
      .values = {{OutputRef{.node_id = WorkflowNodeId{"command"},
                            .port = WorkflowPortId{"stdout"}},
                  std::string{"hello"}}},
  };

  CheckpointStore writer(directory);
  ASSERT_TRUE(writer.save(checkpoint).has_value());

  CheckpointStore reader(directory);
  auto loaded = reader.load(checkpoint.snapshot.run_id);
  ASSERT_TRUE(loaded.has_value()) << loaded.error().message();
  EXPECT_EQ(loaded->plan.workflow_id, WorkflowId{"persisted-plan"});
  EXPECT_EQ(loaded->snapshot.state, RunState::Succeeded);
  ASSERT_EQ(loaded->values.size(), 1U);
  EXPECT_EQ(std::get<std::string>(loaded->values.front().second), "hello");
  auto listed = reader.list();
  ASSERT_TRUE(listed.has_value());
  EXPECT_EQ(listed->size(), 1U);

  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowRuntimeTest, RestartConvertsActiveAttemptToInfrastructureFailure) {
  Runtime core(1, false, 0);
  TestExecutorEnvironment environment(core);
  environment.executor->fail_start(Error::Unsupported);
  auto checkpoint_store = std::make_shared<CheckpointStore>();
  WorkflowRuntime runtime(core, environment.registry, {}, {}, checkpoint_store);

  auto plan = base_plan("restart-active");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  const WorkflowPlanId plan_id{"restored-plan"};
  auto compiled = PlanCompiler{environment.registry}.compile(plan, plan_id);
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  WorkflowCheckpoint checkpoint{
      .plan = plan,
      .trigger = TriggerEnvelope{
          .trigger_id = WorkflowTriggerId{"trigger"},
          .workflow_id = WorkflowId{"restart-active"},
          .source = "test",
          .event_type = "request",
      },
      .snapshot = RunSnapshot{
          .run_id = WorkflowRunId{"restart-active__run"},
          .workflow_id = WorkflowId{"restart-active"},
          .plan_id = plan_id.clone(),
          .state = RunState::Running,
          .tasks = {TaskSnapshot{
              .node_id = WorkflowNodeId{"command"},
              .state = TaskState::Running,
              .attempt_count = 1,
              .active_attempt_id = AttemptId{"attempt"},
              .attempts = {AttemptSnapshot{
                  .attempt_id = AttemptId{"attempt"},
                  .number = 1,
                  .state = AttemptState::Running,
              }},
          }},
      },
  };
  ASSERT_TRUE(runtime.restore(*compiled, checkpoint).has_value());
  ASSERT_TRUE(core.start().has_value());

  auto restored = sync_wait_on_runtime(
      core, runtime.snapshot(checkpoint.snapshot.run_id));
  ASSERT_TRUE(restored.has_value()) << restored.error().message();
  EXPECT_EQ((*restored)->state, RunState::Failed);
  EXPECT_EQ((*restored)->tasks.front().state, TaskState::Failed);
  ASSERT_EQ((*restored)->tasks.front().attempts.size(), 1U);
  EXPECT_EQ((*restored)->tasks.front().attempts.front().failure_class,
            FailureClass::Infrastructure);
  EXPECT_EQ((*restored)->tasks.front().attempts.front().state,
            AttemptState::Failed);
  core.stop();
}

TEST(WorkflowRuntimeTest, PersistsAuthoritativeRunTransitions) {
  const auto directory = temporary_test_directory("runtime-persistence");
  std::error_code error;
  std::filesystem::remove_all(directory, error);

  Runtime core(1, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);
  auto checkpoint_store = std::make_shared<CheckpointStore>(directory);
  WorkflowRuntime runtime(core, environment.registry, {}, {}, checkpoint_store);

  auto plan = base_plan("persisted-runtime");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .executor = "test",
      .outputs = {WorkflowPortId{"stdout"}},
  });
  auto compiled = PlanCompiler{environment.registry}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value());
  auto started = runtime.start(
      *compiled,
      TriggerEnvelope{.workflow_id = WorkflowId{"persisted-runtime"},
                      .source = "test",
                      .event_type = "request"});
  ASSERT_TRUE(started.has_value());
  ASSERT_TRUE(environment.executor->wait_for_pending(1));
  ASSERT_TRUE(environment.executor->complete_next(0, "persisted"));
  ASSERT_TRUE(
      wait_for_state(runtime, core, *started, RunState::Succeeded).has_value());

  CheckpointStore reader(directory);
  auto persisted = reader.load(*started);
  ASSERT_TRUE(persisted.has_value()) << persisted.error().message();
  EXPECT_EQ(persisted->snapshot.state, RunState::Succeeded);
  ASSERT_EQ(persisted->values.size(), 1U);
  EXPECT_EQ(std::get<std::string>(persisted->values.front().second),
            "persisted");

  core.stop();
  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowRuntimeTest, PersistsOnlyExplicitNodeAndTerminalCheckpoints) {
  const auto run_case = [](bool checkpoint_first) {
    Runtime core(1, false, 0);
    ASSERT_TRUE(core.start().has_value());
    TestExecutorEnvironment environment(core);
    auto checkpoint_store = std::make_shared<CheckpointStore>();
    WorkflowRuntime runtime(core, environment.registry, {}, {},
                            checkpoint_store);

    const auto workflow_id =
        checkpoint_first ? "explicit-checkpoint" : "terminal-only-checkpoint";
    auto plan = base_plan(workflow_id);
    plan.nodes = {
        NodePlan{
            .node_id = WorkflowNodeId{"first"},
            .executor = "test",
            .outputs = {WorkflowPortId{"result"}},
            .checkpoint = checkpoint_first,
        },
        NodePlan{
            .node_id = WorkflowNodeId{"second"},
            .executor = "test",
            .inputs = {InputBinding{
                .input = WorkflowPortId{"value"},
                .source = OutputRef{.node_id = WorkflowNodeId{"first"},
                                    .port = WorkflowPortId{"result"}},
            }},
            .outputs = {WorkflowPortId{"result"}},
        },
    };
    auto compiled = PlanCompiler{environment.registry}.compile(std::move(plan));
    ASSERT_TRUE(compiled.has_value()) << compiled.error().message();
    auto started = runtime.start(
        *compiled, TriggerEnvelope{.workflow_id = WorkflowId{workflow_id},
                                   .source = "test",
                                   .event_type = "checkpoint-policy"});
    ASSERT_TRUE(started.has_value()) << started.error().message();

    ASSERT_TRUE(environment.executor->wait_for_pending(1));
    ASSERT_TRUE(environment.executor->complete_next(0, "first"));
    ASSERT_TRUE(environment.executor->wait_for_pending(1));

    auto intermediate = checkpoint_store->load(*started);
    if (checkpoint_first) {
      ASSERT_TRUE(intermediate.has_value()) << intermediate.error().message();
      EXPECT_EQ(intermediate->snapshot.state, RunState::Running);
      ASSERT_EQ(intermediate->snapshot.tasks.size(), 2U);
      EXPECT_EQ(intermediate->snapshot.tasks[0].state, TaskState::Succeeded);
      EXPECT_EQ(intermediate->snapshot.tasks[1].state, TaskState::Pending);
      ASSERT_EQ(intermediate->values.size(), 1U);
    } else {
      ASSERT_FALSE(intermediate.has_value());
      EXPECT_EQ(intermediate.error(), make_error_code(Error::NotFound));
    }

    ASSERT_TRUE(environment.executor->complete_next(0, "second"));
    ASSERT_TRUE(wait_for_state(runtime, core, *started, RunState::Succeeded)
                    .has_value());
    auto terminal = checkpoint_store->load(*started);
    ASSERT_TRUE(terminal.has_value()) << terminal.error().message();
    EXPECT_EQ(terminal->snapshot.state, RunState::Succeeded);
    ASSERT_EQ(terminal->values.size(), 2U);
    core.stop();
  };

  run_case(false);
  run_case(true);
}

TEST(WorkflowRuntimeTest, CompletedRunRetentionEvictsOldestRun) {
  Runtime core(1, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);
  WorkflowRuntime runtime(
      core, environment.registry, std::make_shared<InMemoryArtifactStore>(),
      std::make_shared<EvidenceLedger>(),
      std::make_shared<CheckpointStore>(), 1);

  auto plan = base_plan("retention");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  auto compiled = PlanCompiler{environment.registry}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value());

  auto first = runtime.start(
      *compiled, TriggerEnvelope{.workflow_id = WorkflowId{"retention"},
                                 .source = "test",
                                 .event_type = "first"});
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(environment.executor->wait_for_pending(1));
  ASSERT_TRUE(environment.executor->complete_next(0, "first"));
  ASSERT_TRUE(
      wait_for_state(runtime, core, *first, RunState::Succeeded).has_value());

  auto second = runtime.start(
      *compiled, TriggerEnvelope{.workflow_id = WorkflowId{"retention"},
                                 .source = "test",
                                 .event_type = "second"});
  ASSERT_TRUE(second.has_value());
  ASSERT_TRUE(environment.executor->wait_for_pending(1));
  ASSERT_TRUE(environment.executor->complete_next(0, "second"));
  ASSERT_TRUE(
      wait_for_state(runtime, core, *second, RunState::Succeeded).has_value());

  auto expired = sync_wait_on_runtime(core, runtime.snapshot(*first));
  ASSERT_FALSE(expired.has_value());
  EXPECT_EQ(expired.error(), make_error_code(Error::NotFound));
  EXPECT_TRUE(sync_wait_on_runtime(core, runtime.snapshot(*second)).has_value());
  core.stop();
}

TEST(WorkflowStorageTest, EvidenceLedgerHandlesInvalidAndMalformedRecords) {
  EvidenceLedger memory_ledger;
  EXPECT_EQ(memory_ledger.append(EvidenceRecord{}).error(),
            make_error_code(Error::InvalidArgument));

  EvidenceRecord memory_record;
  memory_record.run_id = WorkflowRunId{"memory-run"};
  auto generated_id = memory_ledger.append(std::move(memory_record));
  ASSERT_TRUE(generated_id.has_value()) << generated_id.error().message();
  EXPECT_FALSE(generated_id->empty());
  EXPECT_EQ(memory_ledger.size(), 1U);
  EXPECT_TRUE(memory_ledger.records(WorkflowRunId{"other-run"}).empty());

  const auto directory = temporary_test_directory("evidence-malformed");
  const auto file = directory / "evidence.jsonl";
  std::error_code error;
  std::filesystem::remove_all(directory, error);
  std::filesystem::create_directories(directory, error);
  ASSERT_FALSE(error);
  {
    std::ofstream output(file, std::ios::binary | std::ios::trunc);
    output << '\n' << "not-json" << '\n';
  }

  EvidenceLedger loaded(file, 1);
  EXPECT_EQ(loaded.size(), 0U);
  EvidenceRecord record;
  record.run_id = WorkflowRunId{"disk-run"};
  ASSERT_TRUE(loaded.append(std::move(record)).has_value());
  EXPECT_EQ(loaded.size(), 1U);

  EvidenceLedger zero_retention({}, 0);
  EvidenceRecord discarded;
  discarded.run_id = WorkflowRunId{"discarded"};
  EXPECT_TRUE(zero_retention.append(std::move(discarded)).has_value());
  EXPECT_EQ(zero_retention.size(), 0U);

  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowStorageTest, CheckpointStoreSupportsMemoryLifecycleAndCorruption) {
  CheckpointStore memory_store;
  WorkflowCheckpoint invalid;
  EXPECT_EQ(memory_store.save(invalid).error(),
            make_error_code(Error::InvalidArgument));
  EXPECT_EQ(memory_store.load(WorkflowRunId{"missing"}).error(),
            make_error_code(Error::NotFound));
  EXPECT_EQ(memory_store.erase(WorkflowRunId{"missing"}).error(),
            make_error_code(Error::NotFound));

  WorkflowCheckpoint checkpoint;
  checkpoint.plan.workflow_id = WorkflowId{"memory-checkpoint"};
  checkpoint.snapshot.run_id = WorkflowRunId{"memory-run"};
  checkpoint.snapshot.workflow_id = WorkflowId{"memory-checkpoint"};
  checkpoint.snapshot.plan_id = WorkflowPlanId{"memory-plan"};
  checkpoint.snapshot.state = RunState::Succeeded;
  ASSERT_TRUE(memory_store.save(checkpoint).has_value());
  ASSERT_TRUE(memory_store.load(checkpoint.snapshot.run_id).has_value());
  auto listed_memory = memory_store.list();
  ASSERT_TRUE(listed_memory.has_value());
  ASSERT_EQ(listed_memory->size(), 1U);
  EXPECT_TRUE(memory_store.erase(checkpoint.snapshot.run_id).has_value());
  auto empty_memory = memory_store.list();
  ASSERT_TRUE(empty_memory.has_value());
  EXPECT_TRUE(empty_memory->empty());

  const auto directory = temporary_test_directory("checkpoint-corruption");
  std::error_code error;
  std::filesystem::remove_all(directory, error);
  std::filesystem::create_directories(directory, error);
  ASSERT_FALSE(error);
  {
    std::ofstream output(directory / "broken.json",
                         std::ios::binary | std::ios::trunc);
    output << "not-json";
    std::ofstream ignored(directory / "ignored.txt",
                          std::ios::binary | std::ios::trunc);
    ignored << "ignored";
  }

  CheckpointStore disk_store(directory);
  EXPECT_EQ(disk_store.load(WorkflowRunId{"broken"}).error(),
            make_error_code(Error::ParseError));
  EXPECT_EQ(disk_store.list().error(), make_error_code(Error::ParseError));

  std::filesystem::remove(directory / "broken.json", error);
  ASSERT_FALSE(error);
  checkpoint.snapshot.run_id = WorkflowRunId{"disk-run"};
  checkpoint.snapshot.plan_id = WorkflowPlanId{"disk-plan"};
  checkpoint.created_at = std::chrono::system_clock::now();
  ASSERT_TRUE(disk_store.save(checkpoint).has_value());
  CheckpointStore reloaded(directory);
  auto listed_disk = reloaded.list();
  ASSERT_TRUE(listed_disk.has_value()) << listed_disk.error().message();
  ASSERT_EQ(listed_disk->size(), 1U);
  EXPECT_EQ(listed_disk->front().snapshot.run_id,
            WorkflowRunId{"disk-run"});
  EXPECT_TRUE(reloaded.erase(WorkflowRunId{"disk-run"}).has_value());
  EXPECT_EQ(reloaded.erase(WorkflowRunId{"disk-run"}).error(),
            make_error_code(Error::NotFound));
  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowStorageTest, PersistentCodecRoundTripsRichRuntimeStateAndValues) {
  const auto directory = temporary_test_directory("checkpoint-rich-codec");
  std::error_code error;
  std::filesystem::remove_all(directory, error);

  WorkflowCheckpoint checkpoint;
  checkpoint.plan = base_plan("rich-codec");
  checkpoint.plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"task"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  checkpoint.trigger.trigger_id = WorkflowTriggerId{"trigger-rich"};
  checkpoint.trigger.workflow_id = WorkflowId{"rich-codec"};
  checkpoint.trigger.source = "codec-test";
  checkpoint.trigger.event_type = "roundtrip";
  JsonValue trigger_payload = JsonValue::object_t{};
  trigger_payload["nested"] = "value";
  checkpoint.trigger.payload = std::move(trigger_payload);
  checkpoint.trigger.idempotency_key = "rich-key";
  checkpoint.trigger.principal.subject = "tester";
  checkpoint.trigger.principal.roles = {"admin", "operator"};
  checkpoint.trigger.trace.trace_id = "trace-id";
  checkpoint.trigger.trace.parent_span_id = "parent-span";
  checkpoint.trigger.occurred_at =
      std::chrono::system_clock::time_point{std::chrono::milliseconds{111}};

  checkpoint.snapshot.run_id = WorkflowRunId{"rich-run"};
  checkpoint.snapshot.workflow_id = WorkflowId{"rich-codec"};
  checkpoint.snapshot.plan_id = WorkflowPlanId{"rich-plan"};
  checkpoint.snapshot.state = RunState::Failed;
  checkpoint.snapshot.stop_intent = StopIntent::Fail;
  checkpoint.snapshot.stop_reason = "terminal failure";
  JsonValue run_failure_details = JsonValue::object_t{};
  run_failure_details["source"] = "codec";
  checkpoint.snapshot.failure = make_execution_failure(
      Error::Unknown, "workflow_failed", "Workflow failed",
      std::move(run_failure_details));
  checkpoint.snapshot.created_at =
      std::chrono::system_clock::time_point{std::chrono::milliseconds{100}};
  checkpoint.snapshot.started_at =
      std::chrono::system_clock::time_point{std::chrono::milliseconds{200}};
  checkpoint.snapshot.finished_at =
      std::chrono::system_clock::time_point{std::chrono::milliseconds{300}};
  TaskSnapshot task;
  task.node_id = WorkflowNodeId{"task"};
  task.state = TaskState::RetryWaiting;
  task.attempt_count = 2;
  task.active_attempt_id = AttemptId{"attempt-active"};
  task.next_attempt_at =
      std::chrono::system_clock::time_point{std::chrono::milliseconds{450}};
  task.skip_reason = SkipReason::BranchNotSelected;
  task.failure = make_execution_failure(
      Error::Timeout, "retry_scheduled", "Retry scheduled");
  task.started_at =
      std::chrono::system_clock::time_point{std::chrono::milliseconds{210}};
  task.finished_at =
      std::chrono::system_clock::time_point{std::chrono::milliseconds{290}};
  task.attempts.push_back(AttemptSnapshot{
      .attempt_id = AttemptId{"attempt-1"},
      .number = 1,
      .state = AttemptState::TimedOut,
      .termination_reason = TerminationReason::AttemptTimeout,
      .failure_class = FailureClass::Timeout,
      .exit_code = 124,
      .failure = make_execution_failure(
          Error::Timeout, "deadline_exceeded", "Deadline exceeded"),
      .created_at = std::chrono::system_clock::time_point{
          std::chrono::milliseconds{220}},
      .started_at = std::chrono::system_clock::time_point{
          std::chrono::milliseconds{230}},
      .finished_at = std::chrono::system_clock::time_point{
          std::chrono::milliseconds{240}},
  });
  checkpoint.snapshot.tasks.push_back(std::move(task));
  checkpoint.created_at =
      std::chrono::system_clock::time_point{std::chrono::milliseconds{500}};

  JsonValue json_value = JsonValue::object_t{};
  json_value["number"] = std::int64_t{7};
  const ArtifactRef artifact{
      .artifact_id = ArtifactId{"artifact-rich"},
      .media_type = "application/octet-stream",
      .size_bytes = 99,
      .digest = "digest-rich",
  };
  const auto add_value = [&](std::string port, WorkflowValue value) {
    checkpoint.values.emplace_back(
        OutputRef{.node_id = WorkflowNodeId{"task"},
                  .port = WorkflowPortId{std::move(port)}},
        std::move(value));
  };
  add_value("null", std::monostate{});
  add_value("bool", true);
  add_value("int", std::int64_t{42});
  add_value("double", 3.25);
  add_value("string", std::string{"text"});
  add_value("json", std::move(json_value));
  add_value("artifact", artifact);

  CheckpointStore store(directory / "runs");
  ASSERT_TRUE(store.save(checkpoint).has_value());
  auto loaded = store.load(WorkflowRunId{"rich-run"});
  ASSERT_TRUE(loaded.has_value()) << loaded.error().message();
  EXPECT_EQ(loaded->trigger.principal.roles,
            (std::vector<std::string>{"admin", "operator"}));
  EXPECT_EQ(loaded->trigger.trace.parent_span_id, "parent-span");
  EXPECT_EQ(loaded->snapshot.stop_intent, StopIntent::Fail);
  ASSERT_TRUE(loaded->snapshot.failure.has_value());
  EXPECT_EQ(loaded->snapshot.failure->code, "workflow_failed");
  EXPECT_EQ(loaded->snapshot.failure->details["source"].as<std::string>(),
            "codec");
  ASSERT_EQ(loaded->snapshot.tasks.size(), 1U);
  ASSERT_EQ(loaded->snapshot.tasks.front().attempts.size(), 1U);
  EXPECT_EQ(loaded->snapshot.tasks.front().active_attempt_id,
            AttemptId{"attempt-active"});
  EXPECT_EQ(loaded->snapshot.tasks.front().skip_reason,
            SkipReason::BranchNotSelected);
  ASSERT_TRUE(loaded->snapshot.tasks.front().failure.has_value());
  EXPECT_EQ(loaded->snapshot.tasks.front().failure->code,
            "retry_scheduled");
  EXPECT_EQ(loaded->snapshot.tasks.front().attempts.front().failure_class,
            FailureClass::Timeout);
  EXPECT_EQ(loaded->snapshot.tasks.front()
                .attempts.front()
                .termination_reason,
            TerminationReason::AttemptTimeout);
  ASSERT_TRUE(
      loaded->snapshot.tasks.front().attempts.front().failure.has_value());
  EXPECT_EQ(loaded->snapshot.tasks.front().attempts.front().failure->code,
            "deadline_exceeded");
  ASSERT_EQ(loaded->values.size(), 7U);
  EXPECT_TRUE(std::holds_alternative<std::monostate>(loaded->values[0].second));
  EXPECT_EQ(std::get<bool>(loaded->values[1].second), true);
  EXPECT_EQ(std::get<std::int64_t>(loaded->values[2].second), 42);
  EXPECT_DOUBLE_EQ(std::get<double>(loaded->values[3].second), 3.25);
  EXPECT_EQ(std::get<std::string>(loaded->values[4].second), "text");
  EXPECT_TRUE(std::holds_alternative<JsonValue>(loaded->values[5].second));
  const auto &loaded_artifact =
      std::get<ArtifactRef>(loaded->values[6].second);
  EXPECT_EQ(loaded_artifact.artifact_id, artifact.artifact_id);
  EXPECT_EQ(loaded_artifact.media_type, artifact.media_type);
  EXPECT_EQ(loaded_artifact.size_bytes, artifact.size_bytes);
  EXPECT_EQ(loaded_artifact.digest, artifact.digest);

  const auto evidence_file = directory / "evidence.jsonl";
  EvidenceLedger ledger(evidence_file, 10);
  JsonValue metadata = JsonValue::object_t{};
  metadata["attempt"] = std::int64_t{1};
  EvidenceRecord record{
      .evidence_id = EvidenceId{"evidence-rich"},
      .run_id = WorkflowRunId{"rich-run"},
      .node_id = WorkflowNodeId{"task"},
      .type = EvidenceType::AttemptCompleted,
      .timestamp = std::chrono::system_clock::time_point{
          std::chrono::milliseconds{600}},
      .actor = Principal{.subject = "tester", .roles = {"operator"}},
      .metadata = std::move(metadata),
      .artifact = artifact,
      .content_digest = "evidence-digest",
  };
  ASSERT_TRUE(ledger.append(std::move(record)).has_value());
  EvidenceLedger reloaded_ledger(evidence_file, 10);
  const auto records = reloaded_ledger.records(WorkflowRunId{"rich-run"});
  ASSERT_EQ(records.size(), 1U);
  EXPECT_EQ(records.front().actor.subject, "tester");
  ASSERT_TRUE(records.front().artifact.has_value());
  EXPECT_EQ(records.front().artifact->artifact_id, artifact.artifact_id);
  EXPECT_EQ(records.front().artifact->media_type, artifact.media_type);
  EXPECT_EQ(records.front().artifact->size_bytes, artifact.size_bytes);
  EXPECT_EQ(records.front().artifact->digest, artifact.digest);
  EXPECT_EQ(records.front().content_digest, "evidence-digest");

  auto checkpoint_json = parse_json([&] {
    std::ifstream input(directory / "runs" / "rich-run.json",
                        std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(input), {});
  }());
  ASSERT_TRUE(checkpoint_json.has_value());
  checkpoint_json->get_object()["schema_version"] = std::int64_t{2};
  {
    std::ofstream output(directory / "runs" / "unsupported.json",
                         std::ios::binary | std::ios::trunc);
    output << dump_json(*checkpoint_json);
  }
  CheckpointStore unsupported_store(directory / "runs");
  EXPECT_EQ(unsupported_store.load(WorkflowRunId{"unsupported"}).error(),
            make_error_code(Error::Unsupported));

  checkpoint_json->get_object()["schema_version"] = std::int64_t{1};
  auto &snapshot_json =
      checkpoint_json->get_object()["snapshot"].get_object();
  snapshot_json["failure"].get_object()["kind"] = std::int64_t{255};
  {
    std::ofstream output(directory / "runs" / "invalid-failure.json",
                         std::ios::binary | std::ios::trunc);
    output << dump_json(*checkpoint_json);
  }
  CheckpointStore invalid_failure_store(directory / "runs");
  EXPECT_EQ(
      invalid_failure_store.load(WorkflowRunId{"invalid-failure"}).error(),
      make_error_code(Error::ParseError));

  auto invalid_checkpoint = checkpoint;
  invalid_checkpoint.snapshot.failure = ExecutionFailure{
      .kind = Error::Success,
      .code = {},
      .message = {},
      .details = JsonValue::array_t{},
  };
  EXPECT_EQ(store.save(invalid_checkpoint).error(),
            make_error_code(Error::InvalidArgument));

  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowControlPlaneTest, RestoresLooksUpAndSortsRegisteredPlans) {
  TestExecutorEnvironment environment;
  AdmissionConfig admission;
  admission.allowed_executors = {"test"};
  WorkflowControlPlane control{environment.registry, AdmissionPolicy{admission}};

  EXPECT_EQ(control.get_latest(WorkflowId{"missing"}).error(),
            make_error_code(Error::NotFound));
  EXPECT_EQ(control.get_plan(WorkflowPlanId{"missing"}).error(),
            make_error_code(Error::NotFound));

  auto zeta = base_plan("zeta");
  zeta.nodes.push_back(NodePlan{.node_id = WorkflowNodeId{"task"},
                                .executor = "test"});
  auto alpha = base_plan("alpha");
  alpha.nodes.push_back(NodePlan{.node_id = WorkflowNodeId{"task"},
                                 .executor = "test"});

  auto restored =
      control.restore_plan(std::move(zeta), WorkflowPlanId{"restored-zeta"});
  ASSERT_TRUE(restored.has_value()) << restored.error().message();
  auto registered = control.register_plan(std::move(alpha));
  ASSERT_TRUE(registered.has_value()) << registered.error().message();

  auto latest = control.get_latest(WorkflowId{"zeta"});
  ASSERT_TRUE(latest.has_value()) << latest.error().message();
  EXPECT_EQ((*latest)->plan_id, WorkflowPlanId{"restored-zeta"});
  auto by_id = control.get_plan(WorkflowPlanId{"restored-zeta"});
  ASSERT_TRUE(by_id.has_value()) << by_id.error().message();
  EXPECT_EQ((*by_id)->workflow_id, WorkflowId{"zeta"});

  const auto plans = control.list_plans();
  ASSERT_EQ(plans.size(), 2U);
  EXPECT_EQ(plans[0]->workflow_id, WorkflowId{"alpha"});
  EXPECT_EQ(plans[1]->workflow_id, WorkflowId{"zeta"});

  auto denied = base_plan("denied");
  denied.nodes.push_back(NodePlan{.node_id = WorkflowNodeId{"task"},
                                  .executor = "unknown"});
  EXPECT_EQ(control.restore_plan(std::move(denied), WorkflowPlanId{"denied"})
                .error(),
            make_error_code(Error::Unauthorized));
}
