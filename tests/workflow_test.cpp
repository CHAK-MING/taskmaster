#include "dagforge/core/runtime.hpp"
#include "dagforge/core/sync_wait.hpp"
#include "dagforge/executor/executor.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/workflow/plan_compiler.hpp"
#include "dagforge/workflow/workflow_control_plane.hpp"
#include "dagforge/workflow/workflow_runtime.hpp"

#include "gtest/gtest.h"

#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <deque>
#include <filesystem>
#include <format>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

#include <unistd.h>

using namespace dagforge;
using namespace dagforge::workflow;

namespace {

class NullExecutor final : public IExecutor {
public:
  auto start(ExecutorRequest, ExecutionSink) -> Result<void> override {
    return fail(Error::Unsupported);
  }
  auto cancel(const InstanceId &) -> void override {}
};

class ManualExecutor final : public IExecutor {
public:
  explicit ManualExecutor(Runtime &runtime) : runtime_(&runtime) {}

  auto start(ExecutorRequest request, ExecutionSink sink)
      -> Result<void> override {
    {
      std::lock_guard lock(mutex_);
      if (!defer_running_signal_ && sink.on_state) {
        sink.on_state(request.instance_id, "running");
      }
    }
    std::lock_guard lock(mutex_);
    pending_.push_back(Pending{
        .instance_id = std::move(request.instance_id),
        .owner = runtime_->current_shard(),
        .command = std::move(request.command),
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

  [[nodiscard]] auto next_command() const
      -> std::optional<CommandExecutorConfig> {
    std::lock_guard lock(mutex_);
    if (pending_.empty()) {
      return std::nullopt;
    }
    return pending_.front().command;
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

private:
  struct Pending {
    InstanceId instance_id;
    shard_id owner{kInvalidShard};
    CommandExecutorConfig command;
    ExecutionSink sink;
  };

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
    auto result = make_executor_result();
    result.exit_code = exit_code;
    result.stdout_output.assign(output.begin(), output.end());
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
    auto result = make_executor_result();
    result.exit_code = exit_code;
    result.stdout_output.assign(output.begin(), output.end());
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
};

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

} // namespace

TEST(WorkflowPlanLoaderTest, ParsesJsonAndTomlPlans) {
  constexpr std::string_view json_text = R"({
    "workflow_id":"loader-json",
    "nodes":[{
      "id":"command",
      "outputs":["result"],
      "timeout_sec":30,
      "config":{"program":"/bin/true"}
    }]
  })";
  auto json_plan = WorkflowPlanLoader::from_json(json_text);
  ASSERT_TRUE(json_plan.has_value()) << json_plan.error().message();
  EXPECT_EQ(json_plan->workflow_id, WorkflowId{"loader-json"});
  ASSERT_EQ(json_plan->nodes.size(), 1U);
  EXPECT_EQ(json_plan->nodes.front().command.program, "/bin/true");
  ASSERT_EQ(json_plan->nodes.front().outputs.size(), 1U);
  EXPECT_EQ(json_plan->nodes.front().outputs.front(),
            WorkflowPortId{"result"});
  EXPECT_TRUE(PlanCompiler{}.compile(*json_plan).has_value());

  constexpr std::string_view toml_text = R"(
workflow_id = "loader-toml"
schema_version = 1

[[nodes]]
id = "command"
outputs = ["result"]
timeout_sec = 30
checkpoint = true

[nodes.config]
program = "/bin/true"
)";
  auto toml_plan = WorkflowPlanLoader::from_toml(toml_text);
  ASSERT_TRUE(toml_plan.has_value()) << toml_plan.error().message();
  EXPECT_EQ(toml_plan->workflow_id, WorkflowId{"loader-toml"});
  ASSERT_EQ(toml_plan->nodes.size(), 1U);
  EXPECT_TRUE(toml_plan->nodes.front().checkpoint);
  ASSERT_EQ(toml_plan->nodes.front().outputs.size(), 1U);
  EXPECT_EQ(toml_plan->nodes.front().outputs.front(),
            WorkflowPortId{"result"});
  EXPECT_TRUE(PlanCompiler{}.compile(*toml_plan).has_value());

  constexpr std::string_view command_toml = R"(
workflow_id = "loader-command"
schema_version = 1

[[nodes]]
id = "command"
outputs = ["stdout", "stderr", "exit_code", "result"]
max_retries = 2
retry_initial_delay_ms = 25
retry_max_delay_ms = 100
timeout_sec = 30

[nodes.config]
program = "/bin/sh"
arguments = ["-c", "printf loader-ok"]
env = [{ key = "MODE", value = "test" }]

[policy]
failure_policy = "fail_fast"
)";
  auto command_plan = WorkflowPlanLoader::from_toml(command_toml);
  ASSERT_TRUE(command_plan.has_value()) << command_plan.error().message();
  ASSERT_EQ(command_plan->nodes.size(), 1U);
  EXPECT_EQ(command_plan->nodes.front().command.program, "/bin/sh");
  ASSERT_EQ(command_plan->nodes.front().command.arguments.size(), 2U);
  EXPECT_EQ(command_plan->nodes.front().command.arguments.back(),
            "printf loader-ok");
  ASSERT_EQ(command_plan->nodes.front().command.env.size(), 1U);
  EXPECT_EQ(command_plan->nodes.front().command.env.front().key, "MODE");
  EXPECT_EQ(command_plan->nodes.front().max_retries, 2);
  EXPECT_EQ(command_plan->nodes.front().retry_initial_delay,
            std::chrono::milliseconds(25));
  EXPECT_EQ(command_plan->nodes.front().retry_max_delay,
            std::chrono::milliseconds(100));
  EXPECT_EQ(command_plan->policy.failure_policy, FailurePolicy::FailFast);
  EXPECT_TRUE(PlanCompiler{}.compile(*command_plan).has_value());

  constexpr std::string_view invalid_command_toml = R"(
workflow_id = "loader-invalid-command"

[[nodes]]
id = "command"

[nodes.config]
program = "/bin/true"
unknown = true

)";
  auto invalid_command =
      WorkflowPlanLoader::from_toml(invalid_command_toml);
  ASSERT_FALSE(invalid_command.has_value());
  EXPECT_EQ(invalid_command.error(), make_error_code(Error::ParseError));

  constexpr std::string_view invalid_policy_toml = R"(
workflow_id = "loader-invalid-policy"

[[nodes]]
id = "command"
outputs = ["result"]

[nodes.config]
program = "/bin/true"

[policy]
failure_policy = "unknown"
)";
  auto invalid_policy = WorkflowPlanLoader::from_toml(invalid_policy_toml);
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
  WorkflowControlPlane control;
  auto plan = base_plan("dedupe");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .command = CommandNodeConfig{.program = "/bin/true"},
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
      .command = CommandNodeConfig{.program = "/bin/true"},
      .outputs = {WorkflowPortId{"result"}},
  });
  auto changed = control.register_plan(std::move(fail_fast));
  ASSERT_TRUE(changed.has_value());
  EXPECT_NE((*first)->plan_id, (*changed)->plan_id);
  EXPECT_NE((*first)->digest, (*changed)->digest);
  EXPECT_EQ(control.list_plans().size(), 2U);
}

TEST(WorkflowControlPlaneTest, EnforcesServerAdmissionPolicy) {
  AdmissionConfig config;
  config.allow_unlisted_programs = false;
  config.allow_unlisted_environment = false;
  config.allowed_programs = {"/bin/echo"};
  config.allowed_environment = {"DAGFORGE_INPUT"};
  config.max_parallel_nodes = 32;
  WorkflowControlPlane control{PlanCompiler{}, AdmissionPolicy{config}};

  auto allowed = base_plan("admission-allowed");
  allowed.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .command = CommandNodeConfig{
          .program = "/bin/echo",
          .env = {{.key = "DAGFORGE_INPUT", .value = "hello"}},
      },
      .outputs = {WorkflowPortId{"result"}},
  });
  EXPECT_TRUE(control.register_plan(std::move(allowed)).has_value());

  auto blocked_program = base_plan("admission-program");
  blocked_program.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .command = CommandNodeConfig{.program = "/bin/cat"},
      .outputs = {WorkflowPortId{"result"}},
  });
  auto program_result = control.register_plan(std::move(blocked_program));
  ASSERT_FALSE(program_result.has_value());
  EXPECT_EQ(program_result.error(), make_error_code(Error::Unauthorized));

  auto blocked_environment = base_plan("admission-environment");
  blocked_environment.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .command = CommandNodeConfig{
          .program = "/bin/echo",
          .env = {{.key = "SECRET", .value = "value"}},
      },
      .outputs = {WorkflowPortId{"result"}},
  });
  auto environment_result =
      control.register_plan(std::move(blocked_environment));
  ASSERT_FALSE(environment_result.has_value());
  EXPECT_EQ(environment_result.error(), make_error_code(Error::Unauthorized));

  auto excessive_budget = base_plan("admission-budget");
  excessive_budget.policy.budget.max_parallel_nodes = 33;
  excessive_budget.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .command = CommandNodeConfig{.program = "/bin/echo"},
      .outputs = {WorkflowPortId{"result"}},
  });
  auto budget_result = control.register_plan(std::move(excessive_budget));
  ASSERT_FALSE(budget_result.has_value());
  EXPECT_EQ(budget_result.error(), make_error_code(Error::ResourceExhausted));
}

TEST(WorkflowPlanCompilerTest, RejectsCyclesAndUnsafeCommandPlans) {
  PlanCompiler compiler;

  auto cycle = base_plan("cycle");
  cycle.nodes = {
      NodePlan{.node_id = WorkflowNodeId{"a"},
               .command = CommandNodeConfig{.program = "/bin/true"},
               .inputs = {InputBinding{.input = WorkflowPortId{"value"},
                                      .source = OutputRef{
                                          .node_id = WorkflowNodeId{"b"},
                                          .port = WorkflowPortId{"result"}}}},
               .outputs = {WorkflowPortId{"result"}}},
      NodePlan{.node_id = WorkflowNodeId{"b"},
               .command = CommandNodeConfig{.program = "/bin/true"},
               .inputs = {InputBinding{.input = WorkflowPortId{"value"},
                                      .source = OutputRef{
                                          .node_id = WorkflowNodeId{"a"},
                                          .port = WorkflowPortId{"result"}}}},
               .outputs = {WorkflowPortId{"result"}}},
  };
  auto cycle_result = compiler.compile(std::move(cycle));
  ASSERT_FALSE(cycle_result.has_value());
  EXPECT_EQ(cycle_result.error(), make_error_code(Error::CycleDetected));

  auto relative_command = base_plan("relative-command");
  relative_command.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .command = CommandNodeConfig{.program = "true"},
      .outputs = {WorkflowPortId{"result"}},
  });
  auto relative_result = compiler.compile(std::move(relative_command));
  ASSERT_FALSE(relative_result.has_value());
  EXPECT_EQ(relative_result.error(), make_error_code(Error::InvalidArgument));

}

TEST(WorkflowPlanCompilerTest, RejectsUnknownFailurePolicy) {
  auto plan = base_plan("invalid-failure-policy");
  plan.policy.failure_policy = static_cast<FailurePolicy>(255);
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .command = CommandNodeConfig{.program = "/bin/true"},
      .outputs = {WorkflowPortId{"result"}},
  });
  auto compiled = PlanCompiler{}.compile(std::move(plan));
  ASSERT_FALSE(compiled.has_value());
  EXPECT_EQ(compiled.error(), make_error_code(Error::InvalidArgument));
}

TEST(WorkflowPlanCompilerTest, RejectsUnknownSchemaVersion) {
  auto plan = base_plan("old-schema");
  plan.schema_version = 2;
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .command = CommandNodeConfig{.program = "/bin/true"},
      .outputs = {WorkflowPortId{"result"}},
  });
  auto compiled = PlanCompiler{}.compile(std::move(plan));
  ASSERT_FALSE(compiled.has_value());
  EXPECT_EQ(compiled.error(), make_error_code(Error::InvalidArgument));
}

TEST(WorkflowRuntimeTest, PauseDrainsActiveAttemptBeforeResume) {
  Runtime core(2, false, 0,
               ComputePoolConfig{.thread_count = 1, .queue_capacity = 16});
  ASSERT_TRUE(core.start().has_value());
  ManualExecutor executor(core);
  WorkflowRuntime runtime(core, executor);

  auto plan = base_plan("pause-flow");
  plan.nodes = {
      NodePlan{
          .node_id = WorkflowNodeId{"first"},
          .command = CommandNodeConfig{.program = "/bin/true"},
          .outputs = {WorkflowPortId{"result"}},
      },
      NodePlan{
          .node_id = WorkflowNodeId{"second"},
          .command = CommandNodeConfig{.program = "/bin/true"},
          .inputs = {InputBinding{
              .input = WorkflowPortId{"value"},
              .source = OutputRef{.node_id = WorkflowNodeId{"first"},
                                  .port = WorkflowPortId{"result"}}}},
          .outputs = {WorkflowPortId{"result"}},
      },
  };
  auto compiled = PlanCompiler{}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  auto started = runtime.start(
      *compiled, TriggerEnvelope{.workflow_id = WorkflowId{"pause-flow"},
                                 .source = "test",
                                 .event_type = "request"});
  ASSERT_TRUE(started.has_value()) << started.error().message();
  ASSERT_TRUE(executor.wait_for_pending(1));

  auto pause = sync_wait_on_runtime(core, runtime.pause(*started));
  ASSERT_TRUE(pause.has_value()) << pause.error().message();
  ASSERT_TRUE(wait_for_state(runtime, core, *started, RunState::Pausing));
  ASSERT_TRUE(executor.complete_next(0, "first"));

  auto paused = wait_for_state(runtime, core, *started, RunState::Paused);
  ASSERT_TRUE(paused.has_value()) << paused.error().message();
  ASSERT_EQ((*paused)->tasks.size(), 2U);
  EXPECT_EQ((*paused)->tasks[0].state, TaskState::Succeeded);
  EXPECT_EQ((*paused)->tasks[1].state, TaskState::Ready);
  ASSERT_EQ((*paused)->tasks[0].attempts.size(), 1U);
  EXPECT_EQ((*paused)->tasks[0].attempts[0].state,
            AttemptState::Succeeded);
  EXPECT_EQ(executor.pending_count(), 0U);

  auto resume = sync_wait_on_runtime(core, runtime.resume(*started));
  ASSERT_TRUE(resume.has_value()) << resume.error().message();
  ASSERT_TRUE(executor.wait_for_pending(1));
  ASSERT_TRUE(executor.complete_next(0, "second"));

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
  Runtime core(1, false, 0,
               ComputePoolConfig{.thread_count = 1, .queue_capacity = 8});
  ASSERT_TRUE(core.start().has_value());
  ManualExecutor executor(core);
  executor.defer_running_signal();
  WorkflowRuntime runtime(core, executor);

  auto plan = base_plan("attempt-starting");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .command = CommandNodeConfig{.program = "/bin/true"},
      .outputs = {WorkflowPortId{"result"}},
  });
  auto compiled = PlanCompiler{}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value());

  auto started = runtime.start(
      *compiled,
      TriggerEnvelope{.workflow_id = WorkflowId{"attempt-starting"},
                      .source = "test",
                      .event_type = "request"});
  ASSERT_TRUE(started.has_value());
  ASSERT_TRUE(executor.wait_for_pending(1));

  auto starting = wait_for_attempt_state(runtime, core, *started, 0, 0,
                                         AttemptState::Starting);
  ASSERT_TRUE(starting.has_value()) << starting.error().message();
  EXPECT_NE((*starting)->tasks[0].attempts[0].created_at,
            std::chrono::system_clock::time_point{});
  EXPECT_EQ((*starting)->tasks[0].attempts[0].started_at,
            std::chrono::system_clock::time_point{});

  ASSERT_TRUE(executor.signal_running_next());
  auto running = wait_for_attempt_state(runtime, core, *started, 0, 0,
                                        AttemptState::Running);
  ASSERT_TRUE(running.has_value()) << running.error().message();
  EXPECT_NE((*running)->tasks[0].attempts[0].started_at,
            std::chrono::system_clock::time_point{});

  ASSERT_TRUE(executor.complete_next(0, "ok"));
  ASSERT_TRUE(
      wait_for_state(runtime, core, *started, RunState::Succeeded).has_value());
  core.stop();
}

TEST(WorkflowRuntimeTest, RunDeadlineStopsAndReapsActiveAttempt) {
  Runtime core(1, false, 0,
               ComputePoolConfig{.thread_count = 1, .queue_capacity = 8});
  ASSERT_TRUE(core.start().has_value());
  ManualExecutor executor(core);
  WorkflowRuntime runtime(core, executor);

  auto plan = base_plan("deadline");
  plan.policy.budget.max_run_duration = std::chrono::milliseconds(25);
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .command = CommandNodeConfig{.program = "/bin/true"},
      .outputs = {WorkflowPortId{"result"}},
  });
  auto compiled = PlanCompiler{}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value());

  auto started = runtime.start(
      *compiled, TriggerEnvelope{.workflow_id = WorkflowId{"deadline"},
                                 .source = "test",
                                 .event_type = "request"});
  ASSERT_TRUE(started.has_value());
  ASSERT_TRUE(executor.wait_for_pending(1));
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
  Runtime core(1, false, 0,
               ComputePoolConfig{.thread_count = 1, .queue_capacity = 8});
  ASSERT_TRUE(core.start().has_value());
  ManualExecutor executor(core);
  executor.defer_cancel_completion();
  WorkflowRuntime runtime(core, executor);

  auto plan = base_plan("cancel-drain");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .command = CommandNodeConfig{.program = "/bin/true"},
      .outputs = {WorkflowPortId{"result"}},
  });
  auto compiled = PlanCompiler{}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value());

  auto started = runtime.start(
      *compiled, TriggerEnvelope{.workflow_id = WorkflowId{"cancel-drain"},
                                 .source = "test",
                                 .event_type = "request"});
  ASSERT_TRUE(started.has_value());
  ASSERT_TRUE(executor.wait_for_pending(1));

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

  ASSERT_TRUE(executor.complete_next(-1));
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
  Runtime core(1, false, 0,
               ComputePoolConfig{.thread_count = 1, .queue_capacity = 8});
  ASSERT_TRUE(core.start().has_value());
  ManualExecutor executor(core);
  executor.synchronous_cancel_completion();
  WorkflowRuntime runtime(core, executor);

  auto plan = base_plan("sync-cancel");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .command = CommandNodeConfig{.program = "/bin/true"},
      .outputs = {WorkflowPortId{"result"}},
  });
  auto compiled = PlanCompiler{}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value());

  auto started = runtime.start(
      *compiled, TriggerEnvelope{.workflow_id = WorkflowId{"sync-cancel"},
                                 .source = "test",
                                 .event_type = "request"});
  ASSERT_TRUE(started.has_value());
  ASSERT_TRUE(executor.wait_for_pending(1));

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
  Runtime core(1, false, 0,
               ComputePoolConfig{.thread_count = 1, .queue_capacity = 8});
  ASSERT_TRUE(core.start().has_value());
  ManualExecutor executor(core);
  WorkflowRuntime runtime(core, executor);

  auto plan = base_plan("retry-attempts");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .command = CommandNodeConfig{.program = "/bin/true"},
      .outputs = {WorkflowPortId{"stdout"}, WorkflowPortId{"exit_code"},
                  WorkflowPortId{"result"}},
      .max_retries = 1,
      .retry_initial_delay = std::chrono::milliseconds(20),
      .retry_max_delay = std::chrono::milliseconds(20),
  });
  auto compiled = PlanCompiler{}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value());

  auto started = runtime.start(
      *compiled, TriggerEnvelope{.workflow_id = WorkflowId{"retry-attempts"},
                                 .source = "test",
                                 .event_type = "request"});
  ASSERT_TRUE(started.has_value());
  ASSERT_TRUE(executor.wait_for_pending(1));
  ASSERT_TRUE(executor.complete_next(1));

  auto waiting = wait_for_task_state(runtime, core, *started, 0,
                                     TaskState::RetryWaiting);
  ASSERT_TRUE(waiting.has_value()) << waiting.error().message();
  ASSERT_EQ((*waiting)->tasks[0].attempts.size(), 1U);
  EXPECT_EQ((*waiting)->tasks[0].attempts[0].state, AttemptState::Failed);
  EXPECT_EQ((*waiting)->tasks[0].attempts[0].failure_class,
            FailureClass::Retryable);
  EXPECT_TRUE((*waiting)->tasks[0].next_attempt_at.has_value());

  ASSERT_TRUE(executor.wait_for_pending(1));
  ASSERT_TRUE(executor.complete_next(0, "ok"));
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
  Runtime core(1, false, 0,
               ComputePoolConfig{.thread_count = 1, .queue_capacity = 8});
  ASSERT_TRUE(core.start().has_value());
  NullExecutor executor;
  WorkflowRuntime runtime(core, executor);

  auto plan = base_plan("permanent-failure");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .command = CommandNodeConfig{.program = "/bin/true"},
      .outputs = {WorkflowPortId{"result"}},
      .max_retries = 3,
      .retry_initial_delay = std::chrono::milliseconds(1),
      .retry_max_delay = std::chrono::milliseconds(1),
  });
  auto compiled = PlanCompiler{}.compile(std::move(plan));
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
  Runtime core(1, false, 0,
               ComputePoolConfig{.thread_count = 1, .queue_capacity = 8});
  ASSERT_TRUE(core.start().has_value());
  ManualExecutor executor(core);
  WorkflowRuntime runtime(core, executor);

  auto plan = base_plan("output-budget-permanent");
  plan.policy.budget.max_total_output_bytes = 4;
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .command = CommandNodeConfig{.program = "/bin/true"},
      .outputs = {WorkflowPortId{"stdout"}},
      .max_retries = 3,
      .retry_initial_delay = std::chrono::milliseconds(1),
      .retry_max_delay = std::chrono::milliseconds(1),
  });
  auto compiled = PlanCompiler{}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value());

  auto started = runtime.start(
      *compiled,
      TriggerEnvelope{.workflow_id = WorkflowId{"output-budget-permanent"},
                      .source = "test",
                      .event_type = "request"});
  ASSERT_TRUE(started.has_value());
  ASSERT_TRUE(executor.wait_for_pending(1));
  ASSERT_TRUE(executor.complete_next(0, "too-large"));
  auto failed = wait_for_state(runtime, core, *started, RunState::Failed);
  ASSERT_TRUE(failed.has_value()) << failed.error().message();
  EXPECT_EQ((*failed)->tasks[0].attempt_count, 1U);
  EXPECT_EQ((*failed)->tasks[0].attempts[0].state, AttemptState::Failed);
  EXPECT_EQ((*failed)->tasks[0].attempts[0].failure_class,
            FailureClass::Permanent);
  core.stop();
}

TEST(WorkflowRuntimeTest, FailFastStopsIndependentAttempts) {
  Runtime core(1, false, 0,
               ComputePoolConfig{.thread_count = 1, .queue_capacity = 8});
  ASSERT_TRUE(core.start().has_value());
  ManualExecutor executor(core);
  executor.defer_cancel_completion();
  WorkflowRuntime runtime(core, executor);

  auto plan = base_plan("fail-fast");
  plan.policy.failure_policy = FailurePolicy::FailFast;
  plan.policy.budget.max_parallel_nodes = 2;
  plan.nodes = {
      NodePlan{
          .node_id = WorkflowNodeId{"first"},
          .command = CommandNodeConfig{.program = "/bin/true"},
          .outputs = {WorkflowPortId{"result"}},
      },
      NodePlan{
          .node_id = WorkflowNodeId{"second"},
          .command = CommandNodeConfig{.program = "/bin/true"},
          .outputs = {WorkflowPortId{"result"}},
      },
  };
  auto compiled = PlanCompiler{}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value());

  auto started = runtime.start(
      *compiled, TriggerEnvelope{.workflow_id = WorkflowId{"fail-fast"},
                                 .source = "test",
                                 .event_type = "request"});
  ASSERT_TRUE(started.has_value());
  ASSERT_TRUE(executor.wait_for_pending(2));
  ASSERT_TRUE(executor.complete_next(1));

  auto stopping = wait_for_state(runtime, core, *started, RunState::Stopping);
  ASSERT_TRUE(stopping.has_value()) << stopping.error().message();
  EXPECT_EQ((*stopping)->stop_intent, StopIntent::Fail);
  EXPECT_EQ((*stopping)->tasks[0].state, TaskState::Failed);
  EXPECT_EQ((*stopping)->tasks[1].state, TaskState::Running);
  EXPECT_EQ((*stopping)->tasks[1].attempts[0].state,
            AttemptState::Terminating);

  ASSERT_TRUE(executor.complete_next(-1));
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

  Runtime core(2, false, 0,
               ComputePoolConfig{.thread_count = 1, .queue_capacity = 8});
  ASSERT_TRUE(core.start().has_value());
  auto executor = create_command_executor(core, sandbox);
  ASSERT_NE(executor, nullptr);
  WorkflowRuntime runtime(core, *executor);

  auto plan = base_plan("command-suspension");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .command = CommandNodeConfig{
          .program = "/bin/sh",
          .arguments = {"-c", "sleep 0.05; printf workflow-ok"},
      },
      .outputs = {WorkflowPortId{"stdout"}, WorkflowPortId{"stderr"},
                  WorkflowPortId{"exit_code"}, WorkflowPortId{"result"}},
  });
  auto compiled = PlanCompiler{}.compile(std::move(plan));
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
  Runtime core(1, false, 0,
               ComputePoolConfig{.thread_count = 1, .queue_capacity = 8});
  ASSERT_TRUE(core.start().has_value());
  ManualExecutor executor(core);
  WorkflowRuntime runtime(core, executor);

  auto plan = base_plan("idempotent");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .command = CommandNodeConfig{.program = "/bin/true"},
      .outputs = {WorkflowPortId{"result"}}});
  auto compiled = PlanCompiler{}.compile(std::move(plan));
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

  ASSERT_TRUE(executor.wait_for_pending(1));
  ASSERT_TRUE(executor.complete_next(0, "done"));
  auto completed = wait_for_state(runtime, core, *first, RunState::Succeeded);
  EXPECT_TRUE(completed.has_value());
  core.stop();
}

TEST(WorkflowRuntimeTest, InjectsUpstreamOutputIntoCommandEnvironment) {
  Runtime core(2, false, 0,
               ComputePoolConfig{.thread_count = 1, .queue_capacity = 16});
  ASSERT_TRUE(core.start().has_value());
  ManualExecutor executor(core);
  WorkflowRuntime runtime(core, executor);

  auto plan = base_plan("command-dataflow");
  plan.nodes = {
      NodePlan{
          .node_id = WorkflowNodeId{"produce"},
          .command = CommandNodeConfig{.program = "/bin/true"},
          .outputs = {WorkflowPortId{"result"}},
      },
      NodePlan{
          .node_id = WorkflowNodeId{"consume"},
          .command = CommandNodeConfig{
              .program = "/bin/true",
              .input_env = {{.input = "value",
                             .environment = "UPSTREAM_VALUE"}},
          },
          .inputs = {InputBinding{
              .input = WorkflowPortId{"value"},
              .source = OutputRef{.node_id = WorkflowNodeId{"produce"},
                                  .port = WorkflowPortId{"result"}}}},
          .outputs = {WorkflowPortId{"result"}},
      },
  };
  auto compiled = PlanCompiler{}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  auto started = runtime.start(
      *compiled,
      TriggerEnvelope{.workflow_id = WorkflowId{"command-dataflow"},
                      .source = "test",
                      .event_type = "request"});
  ASSERT_TRUE(started.has_value());
  ASSERT_TRUE(executor.wait_for_pending(1));
  ASSERT_TRUE(executor.complete_next(0, "hello"));
  ASSERT_TRUE(executor.wait_for_pending(1));
  auto command = executor.next_command();
  ASSERT_TRUE(command.has_value());
  ASSERT_TRUE(command->env.contains("UPSTREAM_VALUE"));
  EXPECT_EQ(command->env.at("UPSTREAM_VALUE"), "hello");
  ASSERT_TRUE(executor.complete_next(0, "consumed"));
  auto completed = wait_for_state(runtime, core, *started, RunState::Succeeded);
  ASSERT_TRUE(completed.has_value()) << completed.error().message();
  core.stop();
}

TEST(WorkflowRuntimeTest, LargeCommandOutputIsExternalizedAsArtifact) {
  Runtime core(1, false, 0,
               ComputePoolConfig{.thread_count = 1, .queue_capacity = 8});
  ASSERT_TRUE(core.start().has_value());
  ManualExecutor executor(core);
  WorkflowRuntime runtime(core, executor);

  auto plan = base_plan("artifact-flow");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .command = CommandNodeConfig{.program = "/bin/true"},
      .outputs = {WorkflowPortId{"stdout"}},
  });
  auto compiled = PlanCompiler{}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value());

  auto started = runtime.start(
      *compiled,
      TriggerEnvelope{.workflow_id = WorkflowId{"artifact-flow"},
                      .source = "test",
                      .event_type = "request"});
  ASSERT_TRUE(started.has_value());
  ASSERT_TRUE(executor.wait_for_pending(1));
  ASSERT_TRUE(executor.complete_next(0, std::string(300'000, 'x')));
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
