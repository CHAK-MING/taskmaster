#include "dagforge/core/runtime.hpp"
#include "dagforge/core/sync_wait.hpp"
#include "dagforge/executor/executor.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/workflow/plan_compiler.hpp"
#include "dagforge/workflow/workflow_control_plane.hpp"
#include "dagforge/workflow/workflow_runtime.hpp"

#include "gtest/gtest.h"

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <utility>

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

template <typename T> auto config_value(const T &config) -> JsonValue {
  auto encoded = serialize_json(config);
  if (!encoded) {
    throw std::runtime_error("failed to encode workflow node config");
  }
  auto parsed = parse_json(*encoded);
  if (!parsed) {
    throw std::runtime_error("failed to parse workflow node config");
  }
  return std::move(*parsed);
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

[[nodiscard]] auto base_plan(std::string_view id) -> WorkflowPlan {
  WorkflowPlan plan;
  plan.workflow_id = WorkflowId{id};
  plan.policy.allow_shell = false;
  plan.policy.require_approval_for_shell = true;
  return plan;
}

} // namespace

TEST(WorkflowPlanLoaderTest, ParsesJsonAndTomlPlans) {
  constexpr std::string_view json_text = R"({
    "workflow_id":"loader-json",
    "nodes":[{
      "id":"noop",
      "type":"noop",
      "outputs":["result"],
      "timeout_sec":30,
      "config":{}
    }]
  })";
  auto json_plan = WorkflowPlanLoader::from_json(json_text);
  ASSERT_TRUE(json_plan.has_value()) << json_plan.error().message();
  EXPECT_EQ(json_plan->workflow_id, WorkflowId{"loader-json"});
  ASSERT_EQ(json_plan->nodes.size(), 1U);
  EXPECT_EQ(json_plan->nodes.front().type, NodeType::Noop);
  ASSERT_EQ(json_plan->nodes.front().outputs.size(), 1U);
  EXPECT_EQ(json_plan->nodes.front().outputs.front(),
            WorkflowPortId{"result"});
  EXPECT_TRUE(PlanCompiler{}.compile(*json_plan).has_value());

  constexpr std::string_view toml_text = R"(
workflow_id = "loader-toml"
schema_version = 1

[[nodes]]
id = "noop"
type = "noop"
outputs = ["result"]
timeout_sec = 30
checkpoint = true

[nodes.config]
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
}

TEST(WorkflowControlPlaneTest, DeduplicatesPlansByDigest) {
  WorkflowControlPlane control;
  auto plan = base_plan("dedupe");
  plan.nodes.push_back(NodePlan{.node_id = WorkflowNodeId{"noop"},
                                .type = NodeType::Noop,
                                .outputs = {WorkflowPortId{"result"}}});
  auto first = control.register_plan(plan);
  auto second = control.register_plan(std::move(plan));
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ((*first)->plan_id, (*second)->plan_id);
  EXPECT_EQ(control.list_plans().size(), 1U);
}

TEST(WorkflowPlanCompilerTest, EnforcesProviderToolAndHostAllowlists) {
  PlanCompiler compiler;

  auto plan = base_plan("allowlist");
  plan.policy.allowed_http_hosts = {"allowed.example"};
  plan.policy.allowed_model_providers = {"openai"};
  plan.policy.allowed_tools = {"internal/echo"};
  plan.nodes = {
      NodePlan{.node_id = WorkflowNodeId{"http"},
               .type = NodeType::Http,
               .config = config_value(
                   HttpNodeConfig{.url = "https://blocked.example/data"}),
               .outputs = {WorkflowPortId{"result"}}},
  };
  auto blocked = compiler.compile(std::move(plan));
  ASSERT_FALSE(blocked.has_value());
  EXPECT_EQ(blocked.error(), make_error_code(Error::Unauthorized));
}

TEST(WorkflowPlanCompilerTest, RejectsCyclesAndMissingApprovalForShell) {
  PlanCompiler compiler;

  auto cycle = base_plan("cycle");
  cycle.nodes = {
      NodePlan{.node_id = WorkflowNodeId{"a"},
               .type = NodeType::Noop,
               .inputs = {InputBinding{.input = WorkflowPortId{"value"},
                                      .source = OutputRef{
                                          .node_id = WorkflowNodeId{"b"},
                                          .port = WorkflowPortId{"result"}}}},
               .outputs = {WorkflowPortId{"result"}}},
      NodePlan{.node_id = WorkflowNodeId{"b"},
               .type = NodeType::Noop,
               .inputs = {InputBinding{.input = WorkflowPortId{"value"},
                                      .source = OutputRef{
                                          .node_id = WorkflowNodeId{"a"},
                                          .port = WorkflowPortId{"result"}}}},
               .outputs = {WorkflowPortId{"result"}}},
  };
  auto cycle_result = compiler.compile(std::move(cycle));
  ASSERT_FALSE(cycle_result.has_value());
  EXPECT_EQ(cycle_result.error(), make_error_code(Error::CycleDetected));

  auto shell = base_plan("unsafe-shell");
  shell.policy.allow_shell = true;
  shell.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"shell"},
      .type = NodeType::Shell,
      .config = config_value(ShellNodeConfig{.command = "true"}),
      .outputs = {WorkflowPortId{"result"}},
  });
  auto shell_result = compiler.compile(std::move(shell));
  ASSERT_FALSE(shell_result.has_value());
  EXPECT_EQ(shell_result.error(), make_error_code(Error::Unauthorized));
}

TEST(WorkflowRuntimeTest, ApprovalResumesOwnerShardExecution) {
  Runtime core(2, false, 0,
               ComputePoolConfig{.thread_count = 1, .queue_capacity = 16});
  ASSERT_TRUE(core.start().has_value());
  NullExecutor executor;
  WorkflowRuntime runtime(core, executor);

  auto plan = base_plan("approval-flow");
  plan.nodes = {
      NodePlan{
          .node_id = WorkflowNodeId{"approval"},
          .type = NodeType::Approval,
          .config = config_value(
              ApprovalNodeConfig{.summary = "Approve execution",
                                 .expires_after_sec = 60}),
          .outputs = {WorkflowPortId{"result"}},
          .checkpoint = true,
      },
      NodePlan{
          .node_id = WorkflowNodeId{"compute"},
          .type = NodeType::Compute,
          .config = config_value(ComputeNodeConfig{.operation = "identity"}),
          .inputs = {InputBinding{
              .input = WorkflowPortId{"approved"},
              .source = OutputRef{.node_id = WorkflowNodeId{"approval"},
                                  .port = WorkflowPortId{"result"}}}},
          .outputs = {WorkflowPortId{"result"}},
          .checkpoint = true,
      },
  };
  plan.edges.push_back(ConditionalEdge{
      .source = OutputRef{.node_id = WorkflowNodeId{"approval"},
                          .port = WorkflowPortId{"result"}},
      .target = WorkflowNodeId{"compute"},
      .condition = ConditionExpr{.kind = ConditionKind::BoolEquals,
                                 .expected_bool = true},
  });
  auto compiled = PlanCompiler{}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  TriggerEnvelope trigger{
      .workflow_id = WorkflowId{"approval-flow"},
      .source = "test",
      .event_type = "request",
      .payload = std::string{"payload"},
      .idempotency_key = "approval-1",
      .principal = Principal{.subject = "tester", .roles = {"operator"}},
  };
  auto started = runtime.start(*compiled, std::move(trigger));
  ASSERT_TRUE(started.has_value()) << started.error().message();

  auto waiting = wait_for_state(runtime, core, *started,
                                RunState::AwaitingApproval);
  ASSERT_TRUE(waiting.has_value()) << waiting.error().message();

  auto approvals =
      sync_wait_on_runtime(core, runtime.pending_approvals(*started));
  ASSERT_TRUE(approvals.has_value());
  ASSERT_EQ(approvals->size(), 1U);

  auto approved = sync_wait_on_runtime(
      core, runtime.approve(*started, approvals->front().approval_id, true,
                            Principal{.subject = "reviewer"}, "approved"));
  ASSERT_TRUE(approved.has_value()) << approved.error().message();

  auto completed = wait_for_state(runtime, core, *started, RunState::Success);
  ASSERT_TRUE(completed.has_value()) << completed.error().message();

  auto output = sync_wait_on_runtime(
      core, runtime.output(*started,
                           OutputRef{.node_id = WorkflowNodeId{"compute"},
                                     .port = WorkflowPortId{"result"}}));
  ASSERT_TRUE(output.has_value()) << output.error().message();
  ASSERT_NE(std::get_if<bool>(output->get()), nullptr);
  EXPECT_TRUE(std::get<bool>(**output));
  EXPECT_FALSE(runtime.evidence(*started).empty());
  EXPECT_TRUE(runtime.checkpoint_store().load(*started).has_value());

  core.stop();
}

TEST(WorkflowRuntimeTest, RunDeadlineFailsSuspendedWorkflow) {
  Runtime core(1, false, 0,
               ComputePoolConfig{.thread_count = 1, .queue_capacity = 8});
  ASSERT_TRUE(core.start().has_value());
  NullExecutor executor;
  WorkflowRuntime runtime(core, executor);

  auto plan = base_plan("deadline");
  plan.policy.budget.max_run_duration = std::chrono::milliseconds(25);
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"approval"},
      .type = NodeType::Approval,
      .config = config_value(
          ApprovalNodeConfig{.summary = "wait", .expires_after_sec = 60}),
      .outputs = {WorkflowPortId{"result"}},
  });
  auto compiled = PlanCompiler{}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value());

  auto started = runtime.start(
      *compiled, TriggerEnvelope{.workflow_id = WorkflowId{"deadline"},
                                 .source = "test",
                                 .event_type = "request"});
  ASSERT_TRUE(started.has_value());
  auto failed = wait_for_state(runtime, core, *started, RunState::Failed);
  ASSERT_TRUE(failed.has_value()) << failed.error().message();
  EXPECT_EQ((*failed)->error, "workflow run deadline exceeded");
  core.stop();
}

TEST(WorkflowRuntimeTest, IdempotentTriggerReturnsExistingRun) {
  Runtime core(1, false, 0,
               ComputePoolConfig{.thread_count = 1, .queue_capacity = 8});
  ASSERT_TRUE(core.start().has_value());
  NullExecutor executor;
  WorkflowRuntime runtime(core, executor);

  auto plan = base_plan("idempotent");
  plan.nodes.push_back(NodePlan{.node_id = WorkflowNodeId{"noop"},
                                .type = NodeType::Noop,
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

  auto completed = wait_for_state(runtime, core, *first, RunState::Success);
  EXPECT_TRUE(completed.has_value());
  core.stop();
}

TEST(WorkflowRuntimeTest, ModelToolEvaluatorPipelineUsesTypedValues) {
  Runtime core(2, false, 0,
               ComputePoolConfig{.thread_count = 1, .queue_capacity = 16});
  ASSERT_TRUE(core.start().has_value());
  NullExecutor executor;

  WorkflowAdapters adapters;
  adapters.invoke_model = [](ModelCall call) -> task<Result<ModelResponse>> {
    auto arguments = parse_json(R"({"query":"hello"})");
    if (!arguments) {
      co_return fail(arguments.error());
    }
    ModelResponse response;
    response.message = Message{.role = "assistant", .content = "hello"};
    response.structured_output = std::move(*arguments);
    response.usage = ModelUsage{.input_tokens = 10, .output_tokens = 2};
    response.provider_request_id = std::format("req-{}", call.node_id);
    co_return ok(std::move(response));
  };
  adapters.invoke_tool = [](ToolInvocation invocation)
      -> task<Result<ToolResult>> {
    co_return ok(ToolResult{.name = std::move(invocation.tool),
                            .success = true,
                            .output = std::move(invocation.arguments)});
  };
  WorkflowRuntime runtime(core, executor, {}, {}, {}, std::move(adapters));

  auto plan = base_plan("ai-pipeline");
  plan.nodes = {
      NodePlan{
          .node_id = WorkflowNodeId{"model"},
          .type = NodeType::Model,
          .config = config_value(ModelNodeConfig{
              .provider = "test",
              .model = "test-model",
              .prompt = "Respond to: ",
              .prompt_input = "$trigger",
          }),
          .outputs = {WorkflowPortId{"result"},
                      WorkflowPortId{"structured_output"}},
      },
      NodePlan{
          .node_id = WorkflowNodeId{"tool"},
          .type = NodeType::Tool,
          .config = config_value(ToolNodeConfig{
              .tool = "echo",
              .arguments_input = "arguments",
          }),
          .inputs = {InputBinding{
              .input = WorkflowPortId{"arguments"},
              .source = OutputRef{.node_id = WorkflowNodeId{"model"},
                                  .port = WorkflowPortId{
                                      "structured_output"}}}},
          .outputs = {WorkflowPortId{"result"}},
      },
      NodePlan{
          .node_id = WorkflowNodeId{"evaluate"},
          .type = NodeType::Evaluator,
          .config = config_value(EvaluatorNodeConfig{.operation = "truthy"}),
          .inputs = {InputBinding{
              .input = WorkflowPortId{"tool_result"},
              .source = OutputRef{.node_id = WorkflowNodeId{"tool"},
                                  .port = WorkflowPortId{"result"}}}},
          .outputs = {WorkflowPortId{"result"},
                      WorkflowPortId{"passed"}},
      },
  };
  auto compiled = PlanCompiler{}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  auto started = runtime.start(
      *compiled,
      TriggerEnvelope{.workflow_id = WorkflowId{"ai-pipeline"},
                      .source = "event",
                      .event_type = "message",
                      .payload = std::string{"hello"},
                      .idempotency_key = "ai-1"});
  ASSERT_TRUE(started.has_value());
  auto completed = wait_for_state(runtime, core, *started, RunState::Success);
  ASSERT_TRUE(completed.has_value()) << completed.error().message();

  auto passed = sync_wait_on_runtime(
      core, runtime.output(*started,
                           OutputRef{.node_id = WorkflowNodeId{"evaluate"},
                                     .port = WorkflowPortId{"passed"}}));
  ASSERT_TRUE(passed.has_value());
  EXPECT_TRUE(std::get<bool>(**passed));
  core.stop();
}

TEST(WorkflowRuntimeTest, LargeModelTextIsExternalizedAsArtifact) {
  Runtime core(1, false, 0,
               ComputePoolConfig{.thread_count = 1, .queue_capacity = 8});
  ASSERT_TRUE(core.start().has_value());
  NullExecutor executor;

  WorkflowAdapters adapters;
  adapters.invoke_model = [](ModelCall) -> task<Result<ModelResponse>> {
    ModelResponse response;
    response.message =
        Message{.role = "assistant", .content = std::string(300'000, 'x')};
    co_return ok(std::move(response));
  };
  WorkflowRuntime runtime(core, executor, {}, {}, {}, std::move(adapters));

  auto plan = base_plan("artifact-flow");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"model"},
      .type = NodeType::Model,
      .config = config_value(
          ModelNodeConfig{.provider = "test", .model = "large"}),
      .outputs = {WorkflowPortId{"text"}},
  });
  auto compiled = PlanCompiler{}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value());

  auto started = runtime.start(
      *compiled,
      TriggerEnvelope{.workflow_id = WorkflowId{"artifact-flow"},
                      .source = "test",
                      .event_type = "request"});
  ASSERT_TRUE(started.has_value());
  ASSERT_TRUE(wait_for_state(runtime, core, *started, RunState::Success));

  auto output = sync_wait_on_runtime(
      core, runtime.output(*started,
                           OutputRef{.node_id = WorkflowNodeId{"model"},
                                     .port = WorkflowPortId{"text"}}));
  ASSERT_TRUE(output.has_value());
  const auto *artifact = std::get_if<ArtifactRef>(output->get());
  ASSERT_NE(artifact, nullptr);
  EXPECT_EQ(artifact->size_bytes, 300'000U);
  auto blob = runtime.artifact_store().get(artifact->artifact_id);
  ASSERT_TRUE(blob.has_value());
  EXPECT_EQ(blob->data.size(), 300'000U);

  core.stop();
}
