#include "dagforge/workflow/workflow_runtime.hpp"

#include "dagforge/client/http/http_client.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/util/log.hpp"
#include "dagforge/util/url.hpp"

#include <boost/asio/async_result.hpp>

#include <openssl/evp.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <format>
#include <memory>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

namespace dagforge::workflow {
namespace {

[[nodiscard]] auto is_terminal(NodeState state) noexcept -> bool {
  return state == NodeState::Success || state == NodeState::Failed ||
         state == NodeState::Skipped || state == NodeState::Cancelled;
}

[[nodiscard]] auto is_success(NodeState state) noexcept -> bool {
  return state == NodeState::Success;
}

[[nodiscard]] auto has_output(const NodePlan &node,
                              std::string_view port) -> bool {
  return std::ranges::any_of(node.outputs, [&](const auto &output) {
    return output == port;
  });
}

auto add_output(NodeOutputs &outputs, const NodePlan &node,
                std::string_view preferred_port, WorkflowValue value) -> void {
  if (has_output(node, preferred_port)) {
    outputs.emplace_back(WorkflowPortId{preferred_port}, std::move(value));
    return;
  }
  if (preferred_port == "result" && node.outputs.size() == 1 &&
      outputs.empty()) {
    outputs.emplace_back(node.outputs.front().clone(), std::move(value));
  }
}

template <typename T>
[[nodiscard]] auto parse_node_config(const JsonValue &config) -> Result<T> {
  return parse_json_as_allow_unknown<T>(dump_json(config));
}

[[nodiscard]] auto value_to_string(const WorkflowValue &value) -> std::string {
  return std::visit(
      [](const auto &typed) -> std::string {
        using T = std::decay_t<decltype(typed)>;
        if constexpr (std::same_as<T, std::monostate>) {
          return {};
        } else if constexpr (std::same_as<T, bool>) {
          return typed ? "true" : "false";
        } else if constexpr (std::same_as<T, std::int64_t> ||
                             std::same_as<T, double>) {
          return std::format("{}", typed);
        } else if constexpr (std::same_as<T, std::string>) {
          return typed;
        } else if constexpr (std::same_as<T, JsonValue>) {
          return dump_json(typed);
        } else if constexpr (std::same_as<T, MessageList>) {
          std::string out;
          for (const auto &message : typed) {
            if (!out.empty()) {
              out.push_back('\n');
            }
            out.append(message.role);
            out.append(": ");
            out.append(message.content);
          }
          return out;
        } else if constexpr (std::same_as<T, ToolResult>) {
          return typed.success ? dump_json(typed.output) : typed.error;
        } else if constexpr (std::same_as<T, ArtifactRef>) {
          return typed.artifact_id.str();
        } else if constexpr (std::same_as<T, ModelResponse>) {
          return typed.message.content;
        } else if constexpr (std::same_as<T, EvaluationResult>) {
          return typed.reason;
        }
        return {};
      },
      value);
}

[[nodiscard]] auto value_truthy(const WorkflowValue &value) -> bool {
  return std::visit(
      [](const auto &typed) -> bool {
        using T = std::decay_t<decltype(typed)>;
        if constexpr (std::same_as<T, std::monostate>) {
          return false;
        } else if constexpr (std::same_as<T, bool>) {
          return typed;
        } else if constexpr (std::same_as<T, std::int64_t> ||
                             std::same_as<T, double>) {
          return typed != 0;
        } else if constexpr (std::same_as<T, std::string>) {
          return !typed.empty();
        } else if constexpr (std::same_as<T, JsonValue>) {
          return dump_json(typed) != "null";
        } else if constexpr (std::same_as<T, MessageList>) {
          return !typed.empty();
        } else if constexpr (std::same_as<T, ToolResult>) {
          return typed.success;
        } else if constexpr (std::same_as<T, ArtifactRef>) {
          return !typed.artifact_id.empty();
        } else if constexpr (std::same_as<T, ModelResponse>) {
          return !typed.message.content.empty() || !typed.tool_calls.empty() ||
                 typed.structured_output.has_value();
        } else if constexpr (std::same_as<T, EvaluationResult>) {
          return typed.passed;
        }
        return false;
      },
      value);
}

using RuntimeInputMap = std::unordered_map<
    std::string, std::shared_ptr<const WorkflowValue>>;

[[nodiscard]] auto first_input(const RuntimeInputMap &inputs)
    -> Result<std::shared_ptr<const WorkflowValue>> {
  if (inputs.empty()) {
    return fail(Error::NotFound);
  }
  return ok(inputs.begin()->second);
}

[[nodiscard]] auto ordered_input_names(
    const RuntimeInputMap &inputs,
    const std::vector<std::string> &requested) -> Result<std::vector<std::string>> {
  if (!requested.empty()) {
    for (const auto &name : requested) {
      if (!inputs.contains(name)) {
        return fail(Error::NotFound);
      }
    }
    return ok(requested);
  }

  std::vector<std::string> names;
  names.reserve(inputs.size());
  for (const auto &[name, _] : inputs) {
    names.push_back(name);
  }
  std::ranges::sort(names);
  return ok(std::move(names));
}

[[nodiscard]] auto sha256_text(std::string_view input) -> Result<std::string> {
  auto context = std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)>{
      EVP_MD_CTX_new(), &EVP_MD_CTX_free};
  if (!context || EVP_DigestInit_ex(context.get(), EVP_sha256(), nullptr) != 1 ||
      EVP_DigestUpdate(context.get(), input.data(), input.size()) != 1) {
    return fail(Error::Unknown);
  }

  std::array<unsigned char, EVP_MAX_MD_SIZE> bytes{};
  unsigned int size = 0;
  if (EVP_DigestFinal_ex(context.get(), bytes.data(), &size) != 1) {
    return fail(Error::Unknown);
  }

  static constexpr char kHex[] = "0123456789abcdef";
  std::string out(static_cast<std::size_t>(size) * 2, '\0');
  for (unsigned int i = 0; i < size; ++i) {
    out[static_cast<std::size_t>(i) * 2] = kHex[bytes[i] >> 4U];
    out[static_cast<std::size_t>(i) * 2 + 1] = kHex[bytes[i] & 0x0fU];
  }
  return ok(std::move(out));
}

[[nodiscard]] auto make_metadata(
    std::initializer_list<std::pair<std::string, JsonValue>> fields)
    -> JsonValue {
  JsonValue value = JsonValue::object_t{};
  for (auto &[key, field] : fields) {
    value[key] = std::move(field);
  }
  return value;
}

[[nodiscard]] auto instance_id_for(const WorkflowRunId &run_id,
                                   const WorkflowNodeId &node_id)
    -> InstanceId {
  return InstanceId{std::format("{}_{}", run_id, node_id)};
}

[[nodiscard]] auto to_boost_error(std::error_code error)
    -> boost::system::error_code {
  return boost::system::error_code{error};
}

} // namespace

WorkflowRuntime::WorkflowRuntime(
    Runtime &runtime, IExecutor &executor,
    std::shared_ptr<IArtifactStore> artifact_store,
    std::shared_ptr<EvidenceLedger> evidence_ledger,
    std::shared_ptr<CheckpointStore> checkpoint_store,
    WorkflowAdapters adapters)
    : runtime_(runtime), executor_(executor),
      artifact_store_(std::move(artifact_store)),
      evidence_ledger_(std::move(evidence_ledger)),
      checkpoint_store_(std::move(checkpoint_store)),
      adapters_(std::move(adapters)), shard_states_(runtime.shard_count()) {
  if (!artifact_store_) {
    artifact_store_ = std::make_shared<InMemoryArtifactStore>();
  }
  if (!evidence_ledger_) {
    evidence_ledger_ = std::make_shared<EvidenceLedger>();
  }
  if (!checkpoint_store_) {
    checkpoint_store_ = std::make_shared<CheckpointStore>();
  }
}

WorkflowRuntime::~WorkflowRuntime() { lifetime_token_.reset(); }

auto WorkflowRuntime::owner_shard(const WorkflowRunId &run_id) const noexcept
    -> shard_id {
  return static_cast<shard_id>(
      std::hash<WorkflowRunId>{}(run_id) % std::max(1U, runtime_.shard_count()));
}

auto WorkflowRuntime::start(std::shared_ptr<const ExecutionPlan> plan,
                            TriggerEnvelope trigger,
                            WorkflowCallbacks callbacks)
    -> Result<WorkflowRunId> {
  if (!runtime_.is_running()) {
    return fail(Error::SystemNotRunning);
  }
  if (!plan || plan->workflow_id.empty() || trigger.workflow_id.empty() ||
      plan->workflow_id != trigger.workflow_id) {
    return fail(Error::InvalidArgument);
  }
  if (trigger.trigger_id.empty()) {
    trigger.trigger_id = generate_workflow_trigger_id();
  }

  if (!trigger.idempotency_key.empty()) {
    std::lock_guard lock(idempotency_mutex_);
    if (const auto it = idempotency_runs_.find(trigger.idempotency_key);
        it != idempotency_runs_.end()) {
      return ok(it->second.clone());
    }
  }

  auto run_id = generate_workflow_run_id(plan->workflow_id);
  if (!trigger.idempotency_key.empty()) {
    std::lock_guard lock(idempotency_mutex_);
    idempotency_runs_.emplace(trigger.idempotency_key, run_id.clone());
  }

  const auto target = owner_shard(run_id);
  runtime_.post_to(
      target,
      [this, weak_lifetime = std::weak_ptr<int>(lifetime_token_),
       run_id = run_id.clone(), plan = std::move(plan),
       trigger = std::move(trigger), callbacks = std::move(callbacks)]() mutable {
        if (weak_lifetime.expired()) {
          return;
        }
        initialize_run(std::move(run_id), std::move(plan), std::move(trigger),
                       std::move(callbacks));
      });
  return ok(std::move(run_id));
}

auto WorkflowRuntime::initialize_run(
    WorkflowRunId run_id, std::shared_ptr<const ExecutionPlan> plan,
    TriggerEnvelope trigger, WorkflowCallbacks callbacks) -> void {
  const auto owner = runtime_.current_shard();
  auto &state = shard_states_[owner];

  ActiveRun run;
  run.plan = std::move(plan);
  run.trigger = std::move(trigger);
  run.snapshot.run_id = run_id.clone();
  run.snapshot.workflow_id = run.plan->workflow_id.clone();
  run.snapshot.plan_id = run.plan->plan_id.clone();
  run.snapshot.state = RunState::Running;
  run.snapshot.created_at = std::chrono::system_clock::now();
  run.snapshot.started_at = run.snapshot.created_at;
  run.callbacks = std::move(callbacks);
  run.values = std::make_unique<RunValueStore>(
      runtime_, owner, *artifact_store_,
      run.plan->policy.budget.max_total_output_bytes);

  run.nodes.reserve(run.plan->nodes.size());
  run.snapshot.nodes.reserve(run.plan->nodes.size());
  for (const auto &compiled : run.plan->nodes) {
    NodeRuntimeState node;
    node.snapshot.node_id = compiled.plan.node_id.clone();
    if (compiled.dependencies.empty()) {
      node.snapshot.state = NodeState::Ready;
      run.ready.push_back(compiled.index);
    }
    run.snapshot.nodes.push_back(node.snapshot);
    run.nodes.push_back(std::move(node));
  }

  auto [it, inserted] = state.active_runs.emplace(run_id.str(), std::move(run));
  if (!inserted) {
    return;
  }
  it->second.deadline_handle = runtime_.schedule_after_on(
      owner, it->second.plan->policy.budget.max_run_duration,
      [this, weak_lifetime = std::weak_ptr<int>(lifetime_token_),
       run_id = run_id.clone()] {
        if (weak_lifetime.expired()) {
          return;
        }
        auto &owner_state = shard_states_[owner_shard(run_id)];
        const auto active = owner_state.active_runs.find(run_id.str());
        if (active == owner_state.active_runs.end()) {
          return;
        }
        fail_run(active->second, "workflow run deadline exceeded");
        (void)complete_run_if_terminal(run_id);
      });
  active_run_count_.fetch_add(1, std::memory_order_release);
  append_evidence(it->second, it->second.nodes.size(),
                  EvidenceType::TriggerReceived,
                  make_metadata({{"source", it->second.trigger.source},
                                 {"event_type", it->second.trigger.event_type},
                                 {"plan_digest", it->second.plan->digest}}));
  append_evidence(it->second, it->second.nodes.size(),
                  EvidenceType::PlanCompiled,
                  make_metadata({{"plan_id", it->second.plan->plan_id.str()},
                                 {"digest", it->second.plan->digest}}));
  emit_run_state(it->second);
  dispatch(run_id);
}

auto WorkflowRuntime::dispatch(const WorkflowRunId &run_id) -> void {
  const auto owner = owner_shard(run_id);
  if (!runtime_.is_current_shard() || runtime_.current_shard() != owner) {
    runtime_.post_to(owner, [this, run_id = run_id.clone()] {
      dispatch(run_id);
    });
    return;
  }

  auto &state = shard_states_[owner];
  auto it = state.active_runs.find(run_id.str());
  if (it == state.active_runs.end() || it->second.dispatching) {
    return;
  }
  it->second.dispatching = true;

  while (true) {
    it = state.active_runs.find(run_id.str());
    if (it == state.active_runs.end()) {
      return;
    }
    auto &run = it->second;
    if (run.snapshot.state == RunState::Cancelled ||
        run.snapshot.state == RunState::Failed ||
        run.snapshot.state == RunState::Success) {
      break;
    }

    const auto limit = std::max<std::size_t>(
        1, run.plan->policy.budget.max_parallel_nodes);
    if (run.ready.empty() || run.active_nodes >= limit) {
      break;
    }

    const auto node_index = run.ready.front();
    run.ready.pop_front();
    if (node_index >= run.nodes.size() ||
        run.nodes[node_index].snapshot.state != NodeState::Ready) {
      continue;
    }

    auto passes = conditions_pass(run, node_index);
    if (!passes) {
      fail_run(run, passes.error().message());
      break;
    }
    if (!*passes) {
      run.nodes[node_index].snapshot.state = NodeState::Skipped;
      run.nodes[node_index].snapshot.finished_at =
          std::chrono::system_clock::now();
      run.snapshot.nodes[node_index] = run.nodes[node_index].snapshot;
      emit_node_state(run, node_index);
      update_dependents(run_id, node_index);
      continue;
    }

    start_node(run_id, node_index);
  }

  it = state.active_runs.find(run_id.str());
  if (it == state.active_runs.end()) {
    return;
  }
  it->second.dispatching = false;
  (void)complete_run_if_terminal(run_id);
}

auto WorkflowRuntime::start_node(const WorkflowRunId &run_id,
                                 std::size_t node_index) -> void {
  auto &run = shard_states_[owner_shard(run_id)].active_runs.at(run_id.str());
  auto &node_state = run.nodes[node_index];
  node_state.snapshot.state = NodeState::Running;
  node_state.snapshot.attempt += 1;
  node_state.snapshot.started_at = std::chrono::system_clock::now();
  node_state.snapshot.finished_at = {};
  node_state.snapshot.error.clear();
  run.snapshot.nodes[node_index] = node_state.snapshot;
  run.active_nodes += 1;
  emit_node_state(run, node_index);
  append_evidence(run, node_index, EvidenceType::NodeStarted,
                  make_metadata({{"attempt", node_state.snapshot.attempt},
                                 {"type", std::string{to_string_view(
                                              run.plan->nodes[node_index]
                                                  .plan.type)}}}));

  switch (run.plan->nodes[node_index].plan.type) {
  case NodeType::Approval:
    start_approval_node(run_id, node_index);
    return;
  case NodeType::Compute:
  case NodeType::Evaluator:
    start_compute_node(run_id, node_index);
    return;
  case NodeType::Noop: {
    auto inputs = input_values(run, node_index);
    if (!inputs) {
      complete_node(run_id, node_index, fail(inputs.error()));
      return;
    }
    complete_node(run_id, node_index,
                  execute_inline_node(run.plan->nodes[node_index].plan,
                                      *inputs));
    return;
  }
  case NodeType::Command:
  case NodeType::Http:
  case NodeType::Model:
  case NodeType::Tool:
    runtime_.spawn_on(owner_shard(run_id),
                      start_async_node(run_id.clone(), node_index));
    return;
  }
}

auto WorkflowRuntime::start_async_node(WorkflowRunId run_id,
                                       std::size_t node_index) -> spawn_task {
  const auto owner = owner_shard(run_id);
  auto &state = shard_states_[owner];
  const auto run_it = state.active_runs.find(run_id.str());
  if (run_it == state.active_runs.end()) {
    co_return;
  }

  auto node = run_it->second.plan->nodes[node_index].plan;
  if (node.type == NodeType::Command) {
    run_it->second.nodes[node_index].instance_id =
        instance_id_for(run_id, node.node_id);
  }
  auto inputs = input_values(run_it->second, node_index);
  if (!inputs) {
    complete_node(run_id, node_index, fail(inputs.error()));
    co_return;
  }
  auto trigger = run_it->second.trigger;

  Result<NodeOutputs> result = fail(Error::Unsupported);
  switch (node.type) {
  case NodeType::Command:
    result = co_await execute_command_node(run_id.clone(), std::move(node),
                                           std::move(*inputs));
    break;
  case NodeType::Http:
    result = co_await execute_http_node(run_id.clone(), std::move(node),
                                        std::move(*inputs));
    break;
  case NodeType::Model:
    append_evidence(run_it->second, node_index, EvidenceType::ModelRequest);
    result = co_await execute_model_node(run_id.clone(), std::move(node),
                                         std::move(*inputs),
                                         std::move(trigger));
    break;
  case NodeType::Tool:
    append_evidence(run_it->second, node_index, EvidenceType::ToolRequest);
    result = co_await execute_tool_node(run_id.clone(), std::move(node),
                                        std::move(*inputs));
    break;
  default:
    break;
  }

  if (!runtime_.is_current_shard() || runtime_.current_shard() != owner) {
    runtime_.post_to(owner,
                     [this, weak_lifetime = std::weak_ptr<int>(lifetime_token_),
                      run_id = run_id.clone(), node_index,
                      result = std::move(result)]() mutable {
                       if (!weak_lifetime.expired()) {
                         complete_node(run_id, node_index, std::move(result));
                       }
                     });
    co_return;
  }
  complete_node(run_id, node_index, std::move(result));
}

auto WorkflowRuntime::start_compute_node(const WorkflowRunId &run_id,
                                         std::size_t node_index) -> void {
  auto &run = shard_states_[owner_shard(run_id)].active_runs.at(run_id.str());
  auto inputs = input_values(run, node_index);
  if (!inputs) {
    complete_node(run_id, node_index, fail(inputs.error()));
    return;
  }

  auto node = run.plan->nodes[node_index].plan;
  ComputeOptions options;
  options.priority = node.type == NodeType::Evaluator
                         ? ComputePriority::High
                         : ComputePriority::Normal;
  options.start_deadline = std::chrono::steady_clock::now() + node.timeout;

  auto submitted = runtime_.submit_compute(
      owner_shard(run_id), options,
      [this, node = std::move(node), inputs = std::move(*inputs)](
          std::stop_token stop_token) mutable -> Result<NodeOutputs> {
        return execute_compute_work(std::move(node), std::move(inputs),
                                    stop_token);
      },
      [this, weak_lifetime = std::weak_ptr<int>(lifetime_token_),
       run_id = run_id.clone(), node_index](Result<NodeOutputs> result) mutable {
        if (!weak_lifetime.expired()) {
          complete_node(run_id, node_index, std::move(result));
        }
      });
  if (!submitted) {
    complete_node(run_id, node_index, fail(submitted.error()));
    return;
  }
  run.nodes[node_index].compute_handle = std::move(*submitted);
}

auto WorkflowRuntime::start_approval_node(const WorkflowRunId &run_id,
                                          std::size_t node_index) -> void {
  auto &run = shard_states_[owner_shard(run_id)].active_runs.at(run_id.str());
  auto config = parse_node_config<ApprovalNodeConfig>(
      run.plan->nodes[node_index].plan.config);
  if (!config) {
    complete_node(run_id, node_index, fail(config.error()));
    return;
  }
  if (config->expires_after_sec <= 0) {
    complete_node(run_id, node_index, fail(Error::InvalidArgument));
    return;
  }

  auto inputs = input_values(run, node_index);
  if (!inputs) {
    complete_node(run_id, node_index, fail(inputs.error()));
    return;
  }

  JsonValue context = JsonValue::object_t{};
  for (const auto &[name, value] : *inputs) {
    context[name] = value_to_string(*value);
  }

  ApprovalRequest request{
      .approval_id = generate_approval_id(),
      .run_id = run_id.clone(),
      .node_id = run.plan->nodes[node_index].plan.node_id.clone(),
      .requested_by = run.trigger.principal,
      .summary = config->summary,
      .context = std::move(context),
      .expires_at = std::chrono::system_clock::now() +
                    std::chrono::seconds(config->expires_after_sec),
  };

  auto &node = run.nodes[node_index];
  node.snapshot.state = NodeState::AwaitingApproval;
  node.approval = request;
  run.snapshot.nodes[node_index] = node.snapshot;
  run.snapshot.pending_approvals.push_back(request.approval_id.clone());
  if (run.active_nodes > 0) {
    run.active_nodes -= 1;
  }
  run.snapshot.state = RunState::AwaitingApproval;
  emit_node_state(run, node_index);
  emit_run_state(run);
  append_evidence(run, node_index, EvidenceType::ApprovalRequested,
                  make_metadata({{"approval_id", request.approval_id.str()},
                                 {"summary", request.summary}}));
  if (run.callbacks.on_approval_requested) {
    run.callbacks.on_approval_requested(request);
  }
}

auto WorkflowRuntime::complete_node(const WorkflowRunId &run_id,
                                    std::size_t node_index,
                                    Result<NodeOutputs> result) -> void {
  const auto owner = owner_shard(run_id);
  if (!runtime_.is_current_shard() || runtime_.current_shard() != owner) {
    runtime_.post_to(owner,
                     [this, weak_lifetime = std::weak_ptr<int>(lifetime_token_),
                      run_id = run_id.clone(), node_index,
                      result = std::move(result)]() mutable {
                       if (!weak_lifetime.expired()) {
                         complete_node(run_id, node_index, std::move(result));
                       }
                     });
    return;
  }

  auto &state = shard_states_[owner];
  const auto run_it = state.active_runs.find(run_id.str());
  if (run_it == state.active_runs.end() || node_index >= run_it->second.nodes.size()) {
    return;
  }
  auto &run = run_it->second;
  auto &node = run.nodes[node_index];
  if (is_terminal(node.snapshot.state)) {
    return;
  }

  node.compute_handle.reset();
  node.instance_id.reset();
  if (node.snapshot.state == NodeState::Running && run.active_nodes > 0) {
    run.active_nodes -= 1;
  }

  if (!result) {
    if (node.snapshot.attempt <= run.plan->nodes[node_index].plan.max_retries &&
        run.snapshot.state != RunState::Cancelled) {
      node.snapshot.state = NodeState::Ready;
      node.snapshot.error = result.error().message();
      run.snapshot.nodes[node_index] = node.snapshot;
      run.ready.push_back(node_index);
      emit_node_state(run, node_index);
      dispatch(run_id);
      return;
    }

    node.snapshot.state = result.error() == make_error_code(Error::Cancelled)
                              ? NodeState::Cancelled
                              : NodeState::Failed;
    node.snapshot.error = result.error().message();
    node.snapshot.finished_at = std::chrono::system_clock::now();
    run.snapshot.nodes[node_index] = node.snapshot;
    append_evidence(run, node_index, EvidenceType::NodeFailed,
                    make_metadata({{"error", node.snapshot.error}}));
    emit_node_state(run, node_index);
    update_dependents(run_id, node_index);
    dispatch(run_id);
    return;
  }

  for (auto &[port, value] : *result) {
    if (auto *model = std::get_if<ModelResponse>(&value)) {
      run.model_tokens_used +=
          model->usage.input_tokens + model->usage.output_tokens;
      if (run.model_tokens_used > run.plan->policy.budget.max_model_tokens) {
        node.snapshot.state = NodeState::Failed;
        node.snapshot.error = "model token budget exceeded";
        node.snapshot.finished_at = std::chrono::system_clock::now();
        run.snapshot.nodes[node_index] = node.snapshot;
        append_evidence(run, node_index, EvidenceType::NodeFailed,
                        make_metadata({{"error", node.snapshot.error}}));
        emit_node_state(run, node_index);
        update_dependents(run_id, node_index);
        dispatch(run_id);
        return;
      }
    }

    auto stored = run.values->put(
        OutputRef{.node_id = run.plan->nodes[node_index].plan.node_id.clone(),
                  .port = port.clone()},
        std::move(value));
    if (!stored) {
      node.snapshot.state = NodeState::Failed;
      node.snapshot.error = stored.error().message();
      node.snapshot.finished_at = std::chrono::system_clock::now();
      run.snapshot.nodes[node_index] = node.snapshot;
      append_evidence(run, node_index, EvidenceType::NodeFailed,
                      make_metadata({{"error", node.snapshot.error}}));
      emit_node_state(run, node_index);
      update_dependents(run_id, node_index);
      dispatch(run_id);
      return;
    }
  }

  node.snapshot.state = NodeState::Success;
  node.snapshot.finished_at = std::chrono::system_clock::now();
  node.snapshot.error.clear();
  run.snapshot.nodes[node_index] = node.snapshot;
  append_evidence(run, node_index, EvidenceType::NodeCompleted);
  if (run.plan->nodes[node_index].plan.type == NodeType::Model) {
    append_evidence(run, node_index, EvidenceType::ModelResponse);
  } else if (run.plan->nodes[node_index].plan.type == NodeType::Tool) {
    append_evidence(run, node_index, EvidenceType::ToolResponse);
  } else if (run.plan->nodes[node_index].plan.type == NodeType::Evaluator) {
    append_evidence(run, node_index, EvidenceType::Evaluation);
  }
  emit_node_state(run, node_index);

  if (run.plan->nodes[node_index].plan.checkpoint) {
    checkpoint(run);
  }
  update_dependents(run_id, node_index);
  dispatch(run_id);
}

auto WorkflowRuntime::update_dependents(const WorkflowRunId &run_id,
                                        std::size_t completed_index) -> void {
  auto &run = shard_states_[owner_shard(run_id)].active_runs.at(run_id.str());
  for (const auto dependent : run.plan->nodes[completed_index].dependents) {
    if (dependent >= run.nodes.size() ||
        run.nodes[dependent].snapshot.state != NodeState::Pending) {
      continue;
    }

    bool all_terminal = true;
    bool all_success = true;
    for (const auto dependency : run.plan->nodes[dependent].dependencies) {
      const auto state = run.nodes[dependency].snapshot.state;
      all_terminal = all_terminal && is_terminal(state);
      all_success = all_success && is_success(state);
    }
    if (!all_terminal) {
      continue;
    }

    if (!all_success) {
      run.nodes[dependent].snapshot.state = NodeState::Skipped;
      run.nodes[dependent].snapshot.finished_at =
          std::chrono::system_clock::now();
      run.snapshot.nodes[dependent] = run.nodes[dependent].snapshot;
      emit_node_state(run, dependent);
      update_dependents(run_id, dependent);
      continue;
    }

    auto passes = conditions_pass(run, dependent);
    if (!passes) {
      fail_run(run, passes.error().message());
      return;
    }
    if (!*passes) {
      run.nodes[dependent].snapshot.state = NodeState::Skipped;
      run.nodes[dependent].snapshot.finished_at =
          std::chrono::system_clock::now();
      run.snapshot.nodes[dependent] = run.nodes[dependent].snapshot;
      emit_node_state(run, dependent);
      update_dependents(run_id, dependent);
      continue;
    }

    run.nodes[dependent].snapshot.state = NodeState::Ready;
    run.snapshot.nodes[dependent] = run.nodes[dependent].snapshot;
    run.ready.push_back(dependent);
    emit_node_state(run, dependent);
  }
}

auto WorkflowRuntime::conditions_pass(const ActiveRun &run,
                                      std::size_t node_index) const
    -> Result<bool> {
  const auto &node_id = run.plan->nodes[node_index].plan.node_id;
  for (const auto &edge : run.plan->edges) {
    if (edge.target != node_id || edge.condition.kind == ConditionKind::Always) {
      continue;
    }
    auto value = run.values->get(edge.source);
    if (!value) {
      return fail(value.error());
    }

    bool passed = false;
    switch (edge.condition.kind) {
    case ConditionKind::Always:
      passed = true;
      break;
    case ConditionKind::BoolEquals:
      passed = value_truthy(**value) == edge.condition.expected_bool;
      break;
    case ConditionKind::StringEquals:
      passed = value_to_string(**value) == edge.condition.expected_string;
      break;
    case ConditionKind::EvaluationPassed:
      if (const auto *evaluation =
              std::get_if<EvaluationResult>(value->get())) {
        passed = evaluation->passed == edge.condition.expected_bool;
      }
      break;
    }
    if (!passed) {
      return ok(false);
    }
  }
  return ok(true);
}

auto WorkflowRuntime::input_values(const ActiveRun &run,
                                   std::size_t node_index) const
    -> Result<InputMap> {
  InputMap inputs;
  for (const auto &binding : run.plan->nodes[node_index].plan.inputs) {
    auto value = run.values->get(binding.source);
    if (!value) {
      return fail(value.error());
    }
    inputs.emplace(binding.input.str(), std::move(*value));
  }
  return ok(std::move(inputs));
}

auto WorkflowRuntime::complete_run_if_terminal(const WorkflowRunId &run_id)
    -> bool {
  const auto owner = owner_shard(run_id);
  auto &state = shard_states_[owner];
  const auto it = state.active_runs.find(run_id.str());
  if (it == state.active_runs.end()) {
    return true;
  }
  auto &run = it->second;

  bool all_terminal = true;
  bool any_failed = false;
  bool any_cancelled = false;
  bool any_waiting = false;
  for (const auto &node : run.nodes) {
    all_terminal = all_terminal && is_terminal(node.snapshot.state);
    any_failed = any_failed || node.snapshot.state == NodeState::Failed;
    any_cancelled = any_cancelled || node.snapshot.state == NodeState::Cancelled;
    any_waiting = any_waiting ||
                  node.snapshot.state == NodeState::AwaitingApproval;
  }

  if (!all_terminal) {
    const auto next_state = any_waiting && run.active_nodes == 0 && run.ready.empty()
                                ? RunState::AwaitingApproval
                                : RunState::Running;
    if (run.snapshot.state != next_state) {
      run.snapshot.state = next_state;
      emit_run_state(run);
    }
    return false;
  }

  const auto requested_terminal_state = run.snapshot.state;
  if (requested_terminal_state == RunState::Failed) {
    run.snapshot.state = RunState::Failed;
  } else if (requested_terminal_state == RunState::Cancelled) {
    run.snapshot.state = RunState::Cancelled;
  } else {
    run.snapshot.state = any_cancelled ? RunState::Cancelled
                                       : (any_failed ? RunState::Failed
                                                     : RunState::Success);
  }
  run.snapshot.finished_at = std::chrono::system_clock::now();
  run.snapshot.pending_approvals.clear();
  emit_run_state(run);
  append_evidence(run, run.nodes.size(),
                  run.snapshot.state == RunState::Success
                      ? EvidenceType::RunCompleted
                      : EvidenceType::RunFailed,
                  make_metadata({{"state", std::string{
                                              to_string_view(run.snapshot.state)}}}));
  checkpoint(run);

  if (run.deadline_handle.valid()) {
    runtime_.cancel_after_on(owner, run.deadline_handle);
    run.deadline_handle = {};
  }

  auto snapshot = make_snapshot(run);
  auto values = run.values->snapshot();
  if (values) {
    state.completed_values[run_id.str()] = std::move(*values);
  }
  state.completed_runs[run_id.str()] = snapshot;
  if (run.callbacks.on_complete) {
    run.callbacks.on_complete(run_id, snapshot);
  }
  state.active_runs.erase(it);
  active_run_count_.fetch_sub(1, std::memory_order_release);
  return true;
}

auto WorkflowRuntime::make_snapshot(const ActiveRun &run) const
    -> std::shared_ptr<const RunSnapshot> {
  auto snapshot = run.snapshot;
  snapshot.nodes.clear();
  snapshot.nodes.reserve(run.nodes.size());
  for (const auto &node : run.nodes) {
    snapshot.nodes.push_back(node.snapshot);
  }
  return std::make_shared<const RunSnapshot>(std::move(snapshot));
}

auto WorkflowRuntime::emit_run_state(ActiveRun &run) -> void {
  if (run.callbacks.on_run_state) {
    run.callbacks.on_run_state(run.snapshot);
  }
}

auto WorkflowRuntime::emit_node_state(ActiveRun &run,
                                      std::size_t node_index) -> void {
  run.snapshot.nodes[node_index] = run.nodes[node_index].snapshot;
  if (run.callbacks.on_node_state) {
    run.callbacks.on_node_state(run.snapshot.run_id,
                                run.nodes[node_index].snapshot);
  }
}

auto WorkflowRuntime::append_evidence(const ActiveRun &run,
                                      std::size_t node_index,
                                      EvidenceType type,
                                      JsonValue metadata) -> void {
  EvidenceRecord record;
  record.run_id = run.snapshot.run_id.clone();
  if (node_index < run.plan->nodes.size()) {
    record.node_id = run.plan->nodes[node_index].plan.node_id.clone();
  }
  record.type = type;
  record.actor = run.trigger.principal;
  record.metadata = std::move(metadata);
  (void)evidence_ledger_->append(std::move(record));
}

auto WorkflowRuntime::checkpoint(ActiveRun &run) -> void {
  auto values = run.values->snapshot();
  if (!values) {
    return;
  }
  WorkflowCheckpoint checkpoint{
      .run_id = run.snapshot.run_id.clone(),
      .plan_id = run.snapshot.plan_id.clone(),
      .state = run.snapshot.state,
      .nodes = run.snapshot.nodes,
      .values = std::move(*values),
      .created_at = std::chrono::system_clock::now(),
  };
  if (checkpoint_store_->save(std::move(checkpoint))) {
    append_evidence(run, run.nodes.size(), EvidenceType::Checkpoint);
  }
}

auto WorkflowRuntime::fail_run(ActiveRun &run, std::string error) -> void {
  run.snapshot.state = RunState::Failed;
  run.snapshot.error = std::move(error);
  run.snapshot.finished_at = std::chrono::system_clock::now();
  for (auto &node : run.nodes) {
    if (!is_terminal(node.snapshot.state)) {
      if (node.compute_handle) {
        (void)node.compute_handle->request_stop();
      }
      if (node.instance_id) {
        executor_.cancel(*node.instance_id);
      }
      node.snapshot.state = NodeState::Cancelled;
      node.snapshot.finished_at = run.snapshot.finished_at;
    }
  }
}

auto WorkflowRuntime::execute_command_node(WorkflowRunId run_id,
                                           NodePlan node, InputMap)
    -> task<Result<NodeOutputs>> {
  if (node.type != NodeType::Command) {
    co_return fail(Error::Unsupported);
  }

  auto config = parse_node_config<CommandNodeConfig>(node.config);
  if (!config || config->program.empty()) {
    co_return fail(config ? Error::InvalidArgument : config.error());
  }

  CommandExecutorConfig command{
      .program = std::move(config->program),
      .arguments = std::move(config->arguments),
  };
  for (auto &entry : config->env) {
    if (!command.env.emplace(std::move(entry.key), std::move(entry.value))
             .second) {
      co_return fail(Error::InvalidArgument);
    }
  }

  const auto instance_id = instance_id_for(run_id, node.node_id);

  auto result = co_await execute_async(
      runtime_, executor_, instance_id, std::move(command), {}, {}, {}, {},
      node.timeout);
  if (!result) {
    co_return fail(result.error());
  }

  NodeOutputs outputs;
  add_output(outputs, node, "stdout", std::string{result->stdout_output});
  add_output(outputs, node, "stderr", std::string{result->stderr_output});
  add_output(outputs, node, "exit_code",
             static_cast<std::int64_t>(result->exit_code));
  add_output(outputs, node, "result", std::string{result->stdout_output});
  if (result->timed_out) {
    co_return fail(Error::Timeout);
  }
  if (result->exit_code != 0) {
    co_return fail(Error::Unknown);
  }
  co_return ok(std::move(outputs));
}

auto WorkflowRuntime::execute_http_node(WorkflowRunId, NodePlan node,
                                        InputMap inputs)
    -> task<Result<NodeOutputs>> {
  auto config = parse_node_config<HttpNodeConfig>(node.config);
  if (!config) {
    co_return fail(config.error());
  }
  if (config->url.empty()) {
    co_return fail(Error::InvalidArgument);
  }

  auto parsed = util::parse_http_url(config->url);
  if (!parsed) {
    co_return fail(parsed.error());
  }

  const auto client_config = http::HttpClientConfig{
      .connect_timeout = std::chrono::duration_cast<std::chrono::milliseconds>(
          node.timeout),
      .read_timeout = std::chrono::duration_cast<std::chrono::milliseconds>(
          node.timeout)};
  auto client = parsed->tls
                    ? co_await http::HttpClient::connect_tls(
                          current_io_context(), parsed->host, parsed->port,
                          client_config)
                    : co_await http::HttpClient::connect_tcp(
                          current_io_context(), parsed->host, parsed->port,
                          client_config);
  if (!client) {
    co_return fail(client.error());
  }

  http::HttpRequest request;
  const auto method = config->method;
  if (method == "POST") {
    request.method = http::HttpMethod::POST;
  } else if (method == "PUT") {
    request.method = http::HttpMethod::PUT;
  } else if (method == "DELETE") {
    request.method = http::HttpMethod::DELETE;
  } else if (method == "PATCH") {
    request.method = http::HttpMethod::PATCH;
  } else {
    request.method = http::HttpMethod::GET;
  }
  request.path = parsed->path;
  for (const auto &header : config->headers) {
    request.headers.add(header.key, header.value);
  }

  std::string body = config->body;
  if (!config->body_input.empty()) {
    const auto input = inputs.find(config->body_input);
    if (input == inputs.end()) {
      co_return fail(Error::NotFound);
    }
    body = value_to_string(*input->second);
  }
  request.body.assign(body.begin(), body.end());

  auto response = co_await (*client)->request(std::move(request));
  (*client)->close();
  if (!response) {
    co_return fail(response.error());
  }
  const auto status = static_cast<int>(response->status);
  if (config->expected_status > 0 && status != config->expected_status) {
    co_return fail(Error::ProtocolError);
  }

  std::string response_body(response->body.begin(), response->body.end());
  NodeOutputs outputs;
  add_output(outputs, node, "status", static_cast<std::int64_t>(status));
  add_output(outputs, node, "body", response_body);
  add_output(outputs, node, "result", std::move(response_body));
  co_return ok(std::move(outputs));
}

auto WorkflowRuntime::execute_model_node(WorkflowRunId run_id, NodePlan node,
                                         InputMap inputs,
                                         TriggerEnvelope trigger)
    -> task<Result<NodeOutputs>> {
  if (!adapters_.invoke_model) {
    co_return fail(Error::Unsupported);
  }
  auto config = parse_node_config<ModelNodeConfig>(node.config);
  if (!config) {
    co_return fail(config.error());
  }
  if (config->model.empty()) {
    co_return fail(Error::InvalidArgument);
  }

  MessageList messages;
  if (!config->system_prompt.empty()) {
    messages.push_back(Message{.role = "system",
                               .content = config->system_prompt});
  }
  std::string prompt = config->prompt;
  if (config->prompt_input == "$trigger") {
    prompt.append(value_to_string(trigger.payload));
  } else if (!config->prompt_input.empty()) {
    const auto input = inputs.find(config->prompt_input);
    if (input == inputs.end()) {
      co_return fail(Error::NotFound);
    }
    prompt.append(value_to_string(*input->second));
  }
  messages.push_back(Message{.role = "user", .content = std::move(prompt)});

  auto response = co_await adapters_.invoke_model(ModelCall{
      .run_id = run_id.clone(),
      .node_id = node.node_id.clone(),
      .provider = std::move(config->provider),
      .model = std::move(config->model),
      .messages = std::move(messages),
      .response_schema = std::move(config->response_schema),
      .max_output_tokens = config->max_output_tokens,
      .temperature = config->temperature,
      .credential = std::move(config->credential),
      .deadline = std::chrono::steady_clock::now() + node.timeout,
  });
  if (!response) {
    co_return fail(response.error());
  }

  NodeOutputs outputs;
  if (response->structured_output) {
    add_output(outputs, node, "structured_output",
               *response->structured_output);
  }
  add_output(outputs, node, "text", response->message.content);
  add_output(outputs, node, "result", std::move(*response));
  co_return ok(std::move(outputs));
}

auto WorkflowRuntime::execute_tool_node(WorkflowRunId run_id, NodePlan node,
                                        InputMap inputs)
    -> task<Result<NodeOutputs>> {
  if (!adapters_.invoke_tool) {
    co_return fail(Error::Unsupported);
  }
  auto config = parse_node_config<ToolNodeConfig>(node.config);
  if (!config) {
    co_return fail(config.error());
  }
  if (config->tool.empty()) {
    co_return fail(Error::InvalidArgument);
  }

  auto arguments = std::move(config->arguments);
  if (!config->arguments_input.empty()) {
    const auto input = inputs.find(config->arguments_input);
    if (input == inputs.end()) {
      co_return fail(Error::NotFound);
    }
    if (const auto *json = std::get_if<JsonValue>(input->second.get())) {
      arguments = *json;
    } else {
      auto parsed = parse_json(value_to_string(*input->second));
      if (!parsed) {
        co_return fail(parsed.error());
      }
      arguments = std::move(*parsed);
    }
  }

  auto result = co_await adapters_.invoke_tool(ToolInvocation{
      .run_id = run_id.clone(),
      .node_id = node.node_id.clone(),
      .tool = std::move(config->tool),
      .arguments = std::move(arguments),
      .credential = std::move(config->credential),
      .deadline = std::chrono::steady_clock::now() + node.timeout,
  });
  if (!result) {
    co_return fail(result.error());
  }
  if (!result->success) {
    co_return fail(Error::Unknown);
  }

  NodeOutputs outputs;
  add_output(outputs, node, "output", result->output);
  add_output(outputs, node, "result", std::move(*result));
  co_return ok(std::move(outputs));
}

auto WorkflowRuntime::execute_inline_node(const NodePlan &node,
                                          const InputMap &inputs) const
    -> Result<NodeOutputs> {
  if (node.type != NodeType::Noop) {
    return fail(Error::Unsupported);
  }
  NodeOutputs outputs;
  if (inputs.empty()) {
    add_output(outputs, node, "result", true);
  } else {
    add_output(outputs, node, "result", *inputs.begin()->second);
  }
  return ok(std::move(outputs));
}

auto WorkflowRuntime::execute_compute_work(NodePlan node, InputMap inputs,
                                           std::stop_token stop_token)
    -> Result<NodeOutputs> {
  if (stop_token.stop_requested()) {
    return fail(Error::Cancelled);
  }

  if (node.type == NodeType::Compute) {
    auto config = parse_node_config<ComputeNodeConfig>(node.config);
    if (!config) {
      return fail(config.error());
    }
    auto names = ordered_input_names(inputs, config->input_order);
    if (!names) {
      return fail(names.error());
    }

    WorkflowValue result;
    if (config->operation == "identity") {
      auto input = first_input(inputs);
      if (!input) {
        return fail(input.error());
      }
      result = **input;
    } else if (config->operation == "concat") {
      std::string text;
      for (const auto &name : *names) {
        if (stop_token.stop_requested()) {
          return fail(Error::Cancelled);
        }
        if (!text.empty()) {
          text.append(config->separator);
        }
        text.append(value_to_string(*inputs.at(name)));
      }
      result = std::move(text);
    } else if (config->operation == "sha256") {
      auto input = first_input(inputs);
      if (!input) {
        return fail(input.error());
      }
      auto digest = sha256_text(value_to_string(**input));
      if (!digest) {
        return fail(digest.error());
      }
      result = std::move(*digest);
    } else if (config->operation == "json_parse") {
      auto input = first_input(inputs);
      if (!input) {
        return fail(input.error());
      }
      auto parsed = parse_json(value_to_string(**input));
      if (!parsed) {
        return fail(parsed.error());
      }
      result = std::move(*parsed);
    } else if (config->operation == "json_stringify") {
      auto input = first_input(inputs);
      if (!input) {
        return fail(input.error());
      }
      result = value_to_string(**input);
    } else {
      return fail(Error::Unsupported);
    }

    NodeOutputs outputs;
    add_output(outputs, node, "result", std::move(result));
    return ok(std::move(outputs));
  }

  if (node.type == NodeType::Evaluator) {
    auto config = parse_node_config<EvaluatorNodeConfig>(node.config);
    if (!config) {
      return fail(config.error());
    }
    std::shared_ptr<const WorkflowValue> input;
    if (!config->input.empty()) {
      const auto it = inputs.find(config->input);
      if (it == inputs.end()) {
        return fail(Error::NotFound);
      }
      input = it->second;
    } else {
      auto first = first_input(inputs);
      if (!first) {
        return fail(first.error());
      }
      input = std::move(*first);
    }

    EvaluationResult evaluation;
    if (config->operation == "truthy") {
      evaluation.passed = value_truthy(*input);
      evaluation.score = evaluation.passed ? 1.0 : 0.0;
      evaluation.reason = evaluation.passed ? "value is truthy" :
                                              "value is not truthy";
    } else if (config->operation == "equals") {
      evaluation.passed = value_to_string(*input) == config->expected;
      evaluation.score = evaluation.passed ? 1.0 : 0.0;
      evaluation.reason = evaluation.passed ? "values match" :
                                              "values do not match";
    } else if (config->operation == "contains") {
      evaluation.passed =
          value_to_string(*input).find(config->expected) != std::string::npos;
      evaluation.score = evaluation.passed ? 1.0 : 0.0;
      evaluation.reason = evaluation.passed ? "substring found" :
                                              "substring not found";
    } else if (config->operation == "score_at_least") {
      const auto *prior = std::get_if<EvaluationResult>(input.get());
      if (!prior) {
        return fail(Error::InvalidArgument);
      }
      evaluation.passed = prior->score >= config->minimum_score;
      evaluation.score = prior->score;
      evaluation.reason = evaluation.passed ? "score threshold met" :
                                              "score threshold not met";
    } else {
      return fail(Error::Unsupported);
    }
    evaluation.evidence = make_metadata(
        {{"input", value_to_string(*input)},
         {"expected", config->expected},
         {"operation", config->operation}});

    NodeOutputs outputs;
    add_output(outputs, node, "passed", evaluation.passed);
    add_output(outputs, node, "score", evaluation.score);
    add_output(outputs, node, "result", std::move(evaluation));
    return ok(std::move(outputs));
  }

  return fail(Error::Unsupported);
}

auto WorkflowRuntime::snapshot(const WorkflowRunId &run_id) const
    -> task<Result<std::shared_ptr<const RunSnapshot>>> {
  const auto target = owner_shard(run_id);
  auto copy_snapshot = [this, &run_id, target]()
      -> Result<std::shared_ptr<const RunSnapshot>> {
    const auto &state = shard_states_[target];
    if (const auto active = state.active_runs.find(run_id.str());
        active != state.active_runs.end()) {
      return ok(make_snapshot(active->second));
    }
    if (const auto completed = state.completed_runs.find(run_id.str());
        completed != state.completed_runs.end()) {
      return ok(completed->second);
    }
    return fail(Error::NotFound);
  };

  if (runtime_.is_current_shard() && runtime_.current_shard() == target) {
    co_return copy_snapshot();
  }

  auto [ec, value] = co_await boost::asio::async_initiate<
      const decltype(dagforge::use_nothrow),
      void(boost::system::error_code, std::shared_ptr<const RunSnapshot>)>(
      [this, weak_lifetime = std::weak_ptr<int>(lifetime_token_),
       run_id = run_id.clone(), target](auto handler) mutable {
        runtime_.post_to(
            target, [this, weak_lifetime, run_id = std::move(run_id), target,
                     handler = std::move(handler)]() mutable {
              if (weak_lifetime.expired()) {
                handler(to_boost_error(make_error_code(Error::Cancelled)),
                        std::shared_ptr<const RunSnapshot>{});
                return;
              }
              const auto &state = shard_states_[target];
              if (const auto active = state.active_runs.find(run_id.str());
                  active != state.active_runs.end()) {
                handler(boost::system::error_code{},
                        make_snapshot(active->second));
                return;
              }
              if (const auto completed =
                      state.completed_runs.find(run_id.str());
                  completed != state.completed_runs.end()) {
                handler(boost::system::error_code{}, completed->second);
                return;
              }
              handler(to_boost_error(make_error_code(Error::NotFound)),
                      std::shared_ptr<const RunSnapshot>{});
            });
      },
      dagforge::use_nothrow);
  if (ec) {
    co_return fail(ec);
  }
  co_return ok(std::move(value));
}

auto WorkflowRuntime::output(const WorkflowRunId &run_id,
                             const OutputRef &output_ref) const
    -> task<Result<std::shared_ptr<const WorkflowValue>>> {
  const auto target = owner_shard(run_id);
  auto [ec, value] = co_await boost::asio::async_initiate<
      const decltype(dagforge::use_nothrow),
      void(boost::system::error_code, std::shared_ptr<const WorkflowValue>)>(
      [this, weak_lifetime = std::weak_ptr<int>(lifetime_token_),
       run_id = run_id.clone(), output_ref, target](auto handler) mutable {
        runtime_.post_to(
            target, [this, weak_lifetime, run_id = std::move(run_id),
                     output_ref = std::move(output_ref), target,
                     handler = std::move(handler)]() mutable {
              if (weak_lifetime.expired()) {
                handler(to_boost_error(make_error_code(Error::Cancelled)),
                        std::shared_ptr<const WorkflowValue>{});
                return;
              }
              auto &state = shard_states_[target];
              if (auto active = state.active_runs.find(run_id.str());
                  active != state.active_runs.end()) {
                auto result = active->second.values->get(output_ref);
                if (!result) {
                  handler(to_boost_error(result.error()),
                          std::shared_ptr<const WorkflowValue>{});
                  return;
                }
                handler(boost::system::error_code{}, std::move(*result));
                return;
              }
              if (const auto completed =
                      state.completed_values.find(run_id.str());
                  completed != state.completed_values.end()) {
                for (const auto &[ref, stored] : completed->second) {
                  if (ref == output_ref) {
                    handler(boost::system::error_code{},
                            std::make_shared<const WorkflowValue>(stored));
                    return;
                  }
                }
              }
              handler(to_boost_error(make_error_code(Error::NotFound)),
                      std::shared_ptr<const WorkflowValue>{});
            });
      },
      dagforge::use_nothrow);
  if (ec) {
    co_return fail(ec);
  }
  co_return ok(std::move(value));
}

auto WorkflowRuntime::pending_approvals(const WorkflowRunId &run_id) const
    -> task<Result<std::vector<ApprovalRequest>>> {
  const auto target = owner_shard(run_id);
  auto [ec, approvals] = co_await boost::asio::async_initiate<
      const decltype(dagforge::use_nothrow),
      void(boost::system::error_code, std::vector<ApprovalRequest>)>(
      [this, weak_lifetime = std::weak_ptr<int>(lifetime_token_),
       run_id = run_id.clone(), target](auto handler) mutable {
        runtime_.post_to(
            target, [this, weak_lifetime, run_id = std::move(run_id), target,
                     handler = std::move(handler)]() mutable {
              if (weak_lifetime.expired()) {
                handler(to_boost_error(make_error_code(Error::Cancelled)),
                        std::vector<ApprovalRequest>{});
                return;
              }
              const auto &state = shard_states_[target];
              const auto it = state.active_runs.find(run_id.str());
              if (it == state.active_runs.end()) {
                handler(to_boost_error(make_error_code(Error::NotFound)),
                        std::vector<ApprovalRequest>{});
                return;
              }
              std::vector<ApprovalRequest> approvals;
              for (const auto &node : it->second.nodes) {
                if (node.approval) {
                  approvals.push_back(*node.approval);
                }
              }
              handler(boost::system::error_code{}, std::move(approvals));
            });
      },
      dagforge::use_nothrow);
  if (ec) {
    co_return fail(ec);
  }
  co_return ok(std::move(approvals));
}

auto WorkflowRuntime::approve(const WorkflowRunId &run_id,
                              const ApprovalId &approval_id, bool approved,
                              Principal actor, std::string comment)
    -> task<Result<void>> {
  const auto target = owner_shard(run_id);
  auto [ec] = co_await boost::asio::async_initiate<
      const decltype(dagforge::use_nothrow), void(boost::system::error_code)>(
      [this, weak_lifetime = std::weak_ptr<int>(lifetime_token_),
       run_id = run_id.clone(), approval_id = approval_id.clone(), approved,
       actor = std::move(actor), comment = std::move(comment),
       target](auto handler) mutable {
        runtime_.post_to(
            target, [this, weak_lifetime, run_id = std::move(run_id),
                     approval_id = std::move(approval_id), approved,
                     actor = std::move(actor), comment = std::move(comment),
                     target, handler = std::move(handler)]() mutable {
              if (weak_lifetime.expired()) {
                handler(to_boost_error(make_error_code(Error::Cancelled)));
                return;
              }
              auto &state = shard_states_[target];
              const auto run_it = state.active_runs.find(run_id.str());
              if (run_it == state.active_runs.end()) {
                handler(to_boost_error(make_error_code(Error::NotFound)));
                return;
              }
              auto &run = run_it->second;
              for (std::size_t index = 0; index < run.nodes.size(); ++index) {
                auto &node = run.nodes[index];
                if (!node.approval ||
                    node.approval->approval_id != approval_id) {
                  continue;
                }
                if (node.approval->expires_at <
                    std::chrono::system_clock::now()) {
                  node.approval.reset();
                  complete_node(run_id, index, fail(Error::Timeout));
                  handler(to_boost_error(make_error_code(Error::Timeout)));
                  return;
                }

                append_evidence(
                    run, index, EvidenceType::ApprovalResolved,
                    make_metadata({{"approval_id", approval_id.str()},
                                   {"approved", approved},
                                   {"actor", actor.subject},
                                   {"comment", comment}}));
                node.approval.reset();
                std::erase(run.snapshot.pending_approvals, approval_id);
                node.snapshot.state = NodeState::Running;
                run.active_nodes += 1;
                run.snapshot.state = RunState::Running;
                NodeOutputs outputs;
                add_output(outputs, run.plan->nodes[index].plan, "result",
                           approved);
                complete_node(run_id, index, ok(std::move(outputs)));
                handler(boost::system::error_code{});
                return;
              }
              handler(to_boost_error(make_error_code(Error::NotFound)));
            });
      },
      dagforge::use_nothrow);
  if (ec) {
    co_return fail(ec);
  }
  co_return ok();
}

auto WorkflowRuntime::cancel(const WorkflowRunId &run_id)
    -> task<Result<void>> {
  const auto target = owner_shard(run_id);
  auto [ec] = co_await boost::asio::async_initiate<
      const decltype(dagforge::use_nothrow), void(boost::system::error_code)>(
      [this, weak_lifetime = std::weak_ptr<int>(lifetime_token_),
       run_id = run_id.clone(), target](auto handler) mutable {
        runtime_.post_to(
            target, [this, weak_lifetime, run_id = std::move(run_id), target,
                     handler = std::move(handler)]() mutable {
              if (weak_lifetime.expired()) {
                handler(to_boost_error(make_error_code(Error::Cancelled)));
                return;
              }
              auto &state = shard_states_[target];
              const auto it = state.active_runs.find(run_id.str());
              if (it == state.active_runs.end()) {
                handler(to_boost_error(make_error_code(Error::NotFound)));
                return;
              }
              auto &run = it->second;
              run.snapshot.state = RunState::Cancelled;
              run.snapshot.finished_at = std::chrono::system_clock::now();
              for (auto &node : run.nodes) {
                if (node.compute_handle) {
                  (void)node.compute_handle->request_stop();
                }
                if (node.instance_id) {
                  executor_.cancel(*node.instance_id);
                }
                if (!is_terminal(node.snapshot.state)) {
                  node.snapshot.state = NodeState::Cancelled;
                  node.snapshot.finished_at = run.snapshot.finished_at;
                }
              }
              run.active_nodes = 0;
              run.ready.clear();
              (void)complete_run_if_terminal(run_id);
              handler(boost::system::error_code{});
            });
      },
      dagforge::use_nothrow);
  if (ec) {
    co_return fail(ec);
  }
  co_return ok();
}

auto WorkflowRuntime::evidence(const WorkflowRunId &run_id) const
    -> std::vector<EvidenceRecord> {
  return evidence_ledger_->records(run_id);
}

} // namespace dagforge::workflow
