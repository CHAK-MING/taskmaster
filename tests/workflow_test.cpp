#include "dagforge/core/runtime.hpp"
#include "dagforge/core/sync_wait.hpp"
#include "dagforge/executor/command_executor.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/workflow/executor_registry.hpp"
#include "dagforge/workflow/executors/command_adapter.hpp"
#include "dagforge/workflow/plan_compiler.hpp"
#include "dagforge/workflow/workflow_control_plane.hpp"
#include "dagforge/workflow/workflow_plan_loader.hpp"
#include "dagforge/workflow/workflow_runtime.hpp"

#include "gtest/gtest.h"

#include <atomic>
#include <array>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <deque>
#include <filesystem>
#include <format>
#include <memory>
#include <mutex>
#include <string>
#include <stdexcept>
#include <thread>
#include <utility>

#include <unistd.h>

using namespace dagforge;
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
         result = ok(std::move(outputs))]() mutable {
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
      -> Result<ExecutorOutputs> {
    if (exit_code != 0) {
      return fail(Error::Unknown);
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
    return ok(std::move(outputs));
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

class RecordingCommandExecutor final : public ICommandExecutor {
public:
  auto start(CommandExecutionRequest request, CommandExecutionSink sink)
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

  [[nodiscard]] auto command() const -> std::optional<CommandSpec> {
    std::lock_guard lock(mutex_);
    return command_;
  }

  [[nodiscard]] auto cancelled() const -> std::optional<InstanceId> {
    std::lock_guard lock(mutex_);
    return cancelled_;
  }

  auto complete(int exit_code, std::string stdout_output = {}) -> void {
    std::optional<InstanceId> instance_id;
    std::optional<CommandExecutionSink> sink;
    {
      std::lock_guard lock(mutex_);
      if (!instance_id_ || !sink_) {
        return;
      }
      instance_id = std::move(instance_id_);
      sink = std::move(sink_);
    }
    auto result = make_command_execution_result();
    result.exit_code = exit_code;
    result.stdout_output.assign(stdout_output.begin(), stdout_output.end());
    if (sink->on_complete) {
      sink->on_complete(*instance_id, std::move(result));
    }
  }

private:
  mutable std::mutex mutex_;
  std::optional<InstanceId> instance_id_;
  std::optional<CommandSpec> command_;
  std::optional<CommandExecutionSink> sink_;
  std::optional<InstanceId> cancelled_;
};

struct CommandExecutorEnvironment {
  explicit CommandExecutorEnvironment(SandboxConfig sandbox = {})
      : sandbox(std::move(sandbox)) {
    auto registered = registry.register_executor(
        create_command_executor_adapter(executor, this->sandbox));
    if (!registered) {
      throw std::runtime_error(registered.error().message());
    }
  }

  RecordingCommandExecutor executor;
  SandboxConfig sandbox;
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
                                      std::chrono::seconds(2))
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
    std::chrono::milliseconds timeout = std::chrono::seconds(2))
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
    std::chrono::milliseconds timeout = std::chrono::seconds(2))
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
    co_return fail(outputs.error());
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
  WorkflowControlPlane control{environment.registry};
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
  WorkflowControlPlane control{environment.registry};

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
  WorkflowControlPlane control{environment.registry};

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

  CommandExecutorEnvironment command_environment;
  auto relative_command = base_plan("relative-command");
  relative_command.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .executor = "command",
      .config = command_config("true"),
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

TEST(CommandExecutorAdapterTest, OwnsCommandPolicyAndInputMapping) {
  SandboxConfig sandbox;
  sandbox.allow_unlisted_programs = false;
  sandbox.allowed_programs = {"/bin/echo"};
  sandbox.allow_unlisted_environment = false;
  sandbox.allowed_environment = {"UPSTREAM"};
  CommandExecutorEnvironment environment{sandbox};

  std::vector<InputBinding> inputs{
      InputBinding{.input = WorkflowPortId{"value"},
                   .source = OutputRef{
                       .node_id = WorkflowNodeId{"source"},
                       .port = WorkflowPortId{"result"}}}};
  std::vector<WorkflowPortId> outputs{WorkflowPortId{"result"}};
  const ExecutorCompileContext context{.inputs = inputs, .outputs = outputs};

  auto compiled = environment.registry.compile(
      "command",
      command_config("/bin/echo", {}, {}, {{"value", "UPSTREAM"}}),
      context);
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  auto blocked_program = environment.registry.compile(
      "command", command_config("/bin/cat"), context);
  ASSERT_FALSE(blocked_program.has_value());
  EXPECT_EQ(blocked_program.error(), make_error_code(Error::Unauthorized));

  auto blocked_environment = environment.registry.compile(
      "command", command_config("/bin/echo", {}, {{"SECRET", "value"}}),
      context);
  ASSERT_FALSE(blocked_environment.has_value());
  EXPECT_EQ(blocked_environment.error(),
            make_error_code(Error::Unauthorized));

  std::optional<Result<ExecutorOutputs>> completion;
  TaskExecutionSink sink;
  sink.on_complete = [&completion](const InstanceId &,
                                   Result<ExecutorOutputs> result) {
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

  auto command = environment.executor.command();
  ASSERT_TRUE(command.has_value());
  ASSERT_TRUE(command->environment.contains("UPSTREAM"));
  EXPECT_EQ(command->environment.at("UPSTREAM"), "hello");

  environment.executor.complete(0, "done");
  ASSERT_TRUE(completion.has_value());
  ASSERT_TRUE(completion->has_value()) << completion->error().message();
  ASSERT_EQ((*completion)->size(), 1U);
  EXPECT_EQ((*completion)->front().first, WorkflowPortId{"result"});
  EXPECT_EQ(std::get<std::string>((*completion)->front().second), "done");

  environment.registry.cancel("command", instance_id);
  ASSERT_TRUE(environment.executor.cancelled().has_value());
  EXPECT_EQ(*environment.executor.cancelled(), instance_id);
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
  EXPECT_EQ((*failed)->error, "workflow run deadline exceeded");
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
  EXPECT_EQ((*failed)->error, "unsupported operation");
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
  EXPECT_EQ((*failed)->tasks[0].attempts[0].error,
            make_error_code(Error::ProtocolError).message());
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

  SandboxConfig sandbox;
  const auto helper = fs::path(home) /
                      ".local/libexec/dagforge/minijail/minijail0";
  const auto policy = fs::path(home) /
                      ".local/libexec/dagforge/minijail/dagforge_command.bpf";
  if (!fs::is_regular_file(helper) || !fs::is_regular_file(policy)) {
    GTEST_SKIP() << "Minijail helper is not installed";
  }
  sandbox.workspace_root =
      (fs::path(home) / ".cache" / "dagforge" / "tests" /
       std::format("workflow-command-{}", ::getpid()))
          .string();
  std::error_code cleanup_error;
  fs::remove_all(sandbox.workspace_root, cleanup_error);

  Runtime core(2, false, 0);
  ASSERT_TRUE(core.start().has_value());
  auto executor = create_command_executor(core, sandbox);
  ASSERT_NE(executor, nullptr);
  ExecutorRegistry executors;
  ASSERT_TRUE(executors
                  .register_executor(
                      create_command_executor_adapter(*executor, sandbox))
                  .has_value());
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

  core.stop();
  fs::remove_all(sandbox.workspace_root, cleanup_error);
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
