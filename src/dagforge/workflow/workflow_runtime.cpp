#include "dagforge/workflow/workflow_runtime.hpp"

#include "dagforge/client/http/http_client.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/util/log.hpp"
#include "dagforge/util/url.hpp"

#include <boost/asio/async_result.hpp>

#include <openssl/evp.h>

#include <algorithm>
#include <array>
#include <cassert>
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

[[nodiscard]] auto is_success(TaskState state) noexcept -> bool {
  return state == TaskState::Succeeded;
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
                                   const WorkflowNodeId &node_id,
                                   const AttemptId &attempt_id)
    -> InstanceId {
  return InstanceId{std::format("{}_{}_{}", run_id, node_id, attempt_id)};
}

[[nodiscard]] auto classify_failure(std::error_code error) noexcept
    -> FailureClass {
  if (error == make_error_code(Error::Cancelled)) {
    return FailureClass::Cancelled;
  }
  if (error == make_error_code(Error::Timeout)) {
    return FailureClass::Timeout;
  }
  if (error == make_error_code(Error::InvalidArgument) ||
      error == make_error_code(Error::ParseError) ||
      error == make_error_code(Error::FileNotFound) ||
      error == make_error_code(Error::NotFound) ||
      error == make_error_code(Error::AlreadyExists) ||
      error == make_error_code(Error::InvalidUrl) ||
      error == make_error_code(Error::ProtocolError) ||
      error == make_error_code(Error::Unauthorized) ||
      error == make_error_code(Error::Unsupported) ||
      error == make_error_code(Error::InvalidState) ||
      error == make_error_code(Error::ResourceExhausted)) {
    return FailureClass::Permanent;
  }
  if (error == make_error_code(Error::SystemNotRunning) ||
      error == make_error_code(Error::QueueFull) ||
      error == make_error_code(Error::ProcessForkFailed)) {
    return FailureClass::Infrastructure;
  }
  return FailureClass::Retryable;
}

[[nodiscard]] auto retryable(FailureClass failure_class) noexcept -> bool {
  return failure_class == FailureClass::Retryable ||
         failure_class == FailureClass::Timeout ||
         failure_class == FailureClass::Infrastructure;
}

[[nodiscard]] auto retry_delay(const NodePlan &node,
                               std::uint32_t attempt_number)
    -> std::chrono::milliseconds {
  auto delay = node.retry_initial_delay;
  for (std::uint32_t current = 1;
       current < attempt_number && delay < node.retry_max_delay; ++current) {
    delay = std::min(node.retry_max_delay, delay * 2);
  }
  return delay;
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

  run.tasks.reserve(run.plan->nodes.size());
  run.snapshot.tasks.reserve(run.plan->nodes.size());
  for (const auto &compiled : run.plan->nodes) {
    TaskRuntimeState task;
    task.snapshot.node_id = compiled.plan.node_id.clone();
    if (compiled.dependencies.empty()) {
      task.snapshot.state = TaskState::Ready;
      run.ready.push_back(compiled.index);
    }
    run.snapshot.tasks.push_back(task.snapshot);
    run.tasks.push_back(std::move(task));
  }
  assert(invariants_hold(run));

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
        (void)request_stop(run_id, StopIntent::Fail,
                           "workflow run deadline exceeded");
      });
  active_run_count_.fetch_add(1, std::memory_order_release);
  append_evidence(it->second, it->second.tasks.size(),
                  EvidenceType::TriggerReceived,
                  make_metadata({{"source", it->second.trigger.source},
                                 {"event_type", it->second.trigger.event_type},
                                 {"plan_digest", it->second.plan->digest}}));
  append_evidence(it->second, it->second.tasks.size(),
                  EvidenceType::PlanCompiled,
                  make_metadata({{"plan_id", it->second.plan->plan_id.str()},
                                 {"digest", it->second.plan->digest}}));
  emit_run_state(it->second);
  dispatch(run_id);
}

auto WorkflowRuntime::transition_run(ActiveRun &run, RunState state)
    -> Result<void> {
  if (!can_transition(run.snapshot.state, state)) {
    return fail(Error::InvalidState);
  }
  run.snapshot.state = state;
  if (is_terminal(state)) {
    run.snapshot.finished_at = std::chrono::system_clock::now();
  }
  emit_run_state(run);
  return ok();
}

auto WorkflowRuntime::transition_task(ActiveRun &run, std::size_t task_index,
                                      TaskState state) -> Result<void> {
  if (task_index >= run.tasks.size()) {
    return fail(Error::InvalidArgument);
  }
  auto &task = run.tasks[task_index].snapshot;
  if (!can_transition(task.state, state)) {
    return fail(Error::InvalidState);
  }
  task.state = state;
  if (state == TaskState::Running &&
      task.started_at == std::chrono::system_clock::time_point{}) {
    task.started_at = std::chrono::system_clock::now();
  }
  if (state == TaskState::Ready) {
    task.next_attempt_at.reset();
  }
  if (is_terminal(state)) {
    task.active_attempt_id.reset();
    task.next_attempt_at.reset();
    task.finished_at = std::chrono::system_clock::now();
  }
  emit_task_state(run, task_index);
  return ok();
}

auto WorkflowRuntime::transition_attempt(AttemptSnapshot &attempt,
                                         AttemptState state) -> Result<void> {
  if (!can_transition(attempt.state, state)) {
    return fail(Error::InvalidState);
  }
  attempt.state = state;
  if (state == AttemptState::Running &&
      attempt.started_at == std::chrono::system_clock::time_point{}) {
    attempt.started_at = std::chrono::system_clock::now();
  }
  if (is_terminal(state)) {
    attempt.finished_at = std::chrono::system_clock::now();
  }
  return ok();
}

auto WorkflowRuntime::active_attempt(TaskRuntimeState &task,
                                     const AttemptId &attempt_id)
    -> AttemptSnapshot * {
  if (!task.snapshot.active_attempt_id ||
      *task.snapshot.active_attempt_id != attempt_id ||
      task.snapshot.attempts.empty()) {
    return nullptr;
  }
  auto &attempt = task.snapshot.attempts.back();
  return attempt.attempt_id == attempt_id ? &attempt : nullptr;
}

auto WorkflowRuntime::invariants_hold(const ActiveRun &run) const noexcept
    -> bool {
  if (run.snapshot.tasks.size() != run.tasks.size()) {
    return false;
  }
  std::size_t active_attempts = 0;
  for (std::size_t index = 0; index < run.tasks.size(); ++index) {
    const auto &task = run.tasks[index];
    const auto &published = run.snapshot.tasks[index];
    if (published.state != task.snapshot.state ||
        published.attempt_count != task.snapshot.attempt_count ||
        published.active_attempt_id != task.snapshot.active_attempt_id) {
      return false;
    }
    if (task.snapshot.attempt_count != task.snapshot.attempts.size()) {
      return false;
    }

    const AttemptSnapshot *active = nullptr;
    if (task.snapshot.active_attempt_id) {
      if (task.snapshot.attempts.empty()) {
        return false;
      }
      const auto &candidate = task.snapshot.attempts.back();
      if (candidate.attempt_id != *task.snapshot.active_attempt_id ||
          is_terminal(candidate.state)) {
        return false;
      }
      active = std::addressof(candidate);
      ++active_attempts;
    }

    if ((task.snapshot.state == TaskState::Running) != (active != nullptr)) {
      return false;
    }
    if (task.snapshot.state == TaskState::RetryWaiting) {
      if (task.snapshot.attempts.empty() ||
          !is_terminal(task.snapshot.attempts.back().state) ||
          !task.snapshot.next_attempt_at) {
        return false;
      }
    }
    if (is_terminal(task.snapshot.state) &&
        task.snapshot.active_attempt_id) {
      return false;
    }
  }

  if (active_attempts != run.active_attempts) {
    return false;
  }
  if (is_terminal(run.snapshot.state)) {
    if (active_attempts != 0 ||
        !std::ranges::all_of(run.tasks, [](const auto &task) {
          return is_terminal(task.snapshot.state);
        })) {
      return false;
    }
  }
  return true;
}

auto WorkflowRuntime::begin_attempt(ActiveRun &run, std::size_t task_index)
    -> AttemptId {
  auto &task = run.tasks[task_index];
  auto attempt_id = generate_attempt_id();
  task.snapshot.attempt_count += 1;
  task.snapshot.active_attempt_id = attempt_id.clone();
  task.snapshot.next_attempt_at.reset();
  task.snapshot.skip_reason.reset();
  task.snapshot.attempts.push_back(AttemptSnapshot{
      .attempt_id = attempt_id.clone(),
      .number = task.snapshot.attempt_count,
      .state = AttemptState::Starting,
      .created_at = std::chrono::system_clock::now(),
  });
  run.active_attempts += 1;
  (void)transition_task(run, task_index, TaskState::Running);
  append_evidence(run, task_index, EvidenceType::AttemptStarted,
                  make_metadata({{"attempt_id", attempt_id.str()},
                                 {"number", task.snapshot.attempt_count}}));
  append_evidence(run, task_index, EvidenceType::TaskStarted,
                  make_metadata({{"attempt", task.snapshot.attempt_count},
                                 {"type", std::string{to_string_view(
                                              run.plan->nodes[task_index]
                                                  .plan.type)}}}));
  assert(invariants_hold(run));
  return attempt_id;
}

auto WorkflowRuntime::mark_attempt_running(ActiveRun &run,
                                           std::size_t task_index,
                                           const AttemptId &attempt_id)
    -> void {
  if (task_index >= run.tasks.size()) {
    return;
  }
  auto *attempt = active_attempt(run.tasks[task_index], attempt_id);
  if (attempt == nullptr || is_terminal(attempt->state)) {
    return;
  }
  if (transition_attempt(*attempt, AttemptState::Running)) {
    emit_task_state(run, task_index);
    assert(invariants_hold(run));
  }
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
  if (it->second.snapshot.state != RunState::Running) {
    settle_control_state(run_id);
    return;
  }
  it->second.dispatching = true;

  while (true) {
    it = state.active_runs.find(run_id.str());
    if (it == state.active_runs.end()) {
      return;
    }
    auto &run = it->second;
    if (run.snapshot.state != RunState::Running) {
      break;
    }

    const auto limit = std::max<std::size_t>(
        1, run.plan->policy.budget.max_parallel_nodes);
    if (run.ready.empty() || run.active_attempts >= limit) {
      break;
    }

    const auto node_index = run.ready.front();
    run.ready.pop_front();
    if (node_index >= run.tasks.size() ||
        run.tasks[node_index].snapshot.state != TaskState::Ready) {
      continue;
    }

    auto passes = conditions_pass(run, node_index);
    if (!passes) {
      (void)request_stop(run_id, StopIntent::Fail, passes.error().message());
      break;
    }
    if (!*passes) {
      run.tasks[node_index].snapshot.skip_reason = SkipReason::ConditionFalse;
      (void)transition_task(run, node_index, TaskState::Skipped);
      update_dependents(run_id, node_index);
      continue;
    }

    start_task(run_id, node_index);
  }

  it = state.active_runs.find(run_id.str());
  if (it == state.active_runs.end()) {
    return;
  }
  it->second.dispatching = false;
  settle_control_state(run_id);
}

auto WorkflowRuntime::start_task(const WorkflowRunId &run_id,
                                 std::size_t task_index) -> void {
  auto &run = shard_states_[owner_shard(run_id)].active_runs.at(run_id.str());
  const auto attempt_id = begin_attempt(run, task_index);

  switch (run.plan->nodes[task_index].plan.type) {
  case NodeType::Compute:
  case NodeType::Evaluator:
    start_compute_task(run_id, task_index, attempt_id.clone());
    return;
  case NodeType::Noop: {
    mark_attempt_running(run, task_index, attempt_id);
    auto inputs = input_values(run, task_index);
    if (!inputs) {
      complete_task(run_id, task_index, attempt_id, fail(inputs.error()));
      return;
    }
    complete_task(run_id, task_index, attempt_id,
                  execute_inline_node(run.plan->nodes[task_index].plan,
                                      *inputs));
    return;
  }
  case NodeType::Command:
  case NodeType::Http:
  case NodeType::Model:
  case NodeType::Tool:
    runtime_.spawn_on(owner_shard(run_id),
                      start_async_task(run_id.clone(), task_index,
                                       attempt_id.clone()));
    return;
  }
}

auto WorkflowRuntime::start_async_task(WorkflowRunId run_id,
                                       std::size_t task_index,
                                       AttemptId attempt_id) -> spawn_task {
  const auto owner = owner_shard(run_id);
  auto &state = shard_states_[owner];
  const auto run_it = state.active_runs.find(run_id.str());
  if (run_it == state.active_runs.end()) {
    co_return;
  }

  auto node = run_it->second.plan->nodes[task_index].plan;
  if (node.type != NodeType::Command) {
    mark_attempt_running(run_it->second, task_index, attempt_id);
  }
  if (node.type == NodeType::Command) {
    run_it->second.tasks[task_index].instance_id =
        instance_id_for(run_id, node.node_id, attempt_id);
  }
  auto inputs = input_values(run_it->second, task_index);
  if (!inputs) {
    complete_task(run_id, task_index, attempt_id, fail(inputs.error()));
    co_return;
  }
  auto trigger = run_it->second.trigger;

  Result<NodeOutputs> result = fail(Error::Unsupported);
  switch (node.type) {
  case NodeType::Command:
    result = co_await execute_command_node(
        run_id.clone(), task_index, attempt_id.clone(), std::move(node),
        std::move(*inputs));
    break;
  case NodeType::Http:
    result = co_await execute_http_node(run_id.clone(), std::move(node),
                                        std::move(*inputs));
    break;
  case NodeType::Model:
    append_evidence(run_it->second, task_index, EvidenceType::ModelRequest);
    result = co_await execute_model_node(run_id.clone(), std::move(node),
                                         std::move(*inputs),
                                         std::move(trigger));
    break;
  case NodeType::Tool:
    append_evidence(run_it->second, task_index, EvidenceType::ToolRequest);
    result = co_await execute_tool_node(run_id.clone(), std::move(node),
                                        std::move(*inputs));
    break;
  default:
    break;
  }

  if (!runtime_.is_current_shard() || runtime_.current_shard() != owner) {
    runtime_.post_to(owner,
                     [this, weak_lifetime = std::weak_ptr<int>(lifetime_token_),
                      run_id = run_id.clone(), task_index,
                      attempt_id = attempt_id.clone(),
                      result = std::move(result)]() mutable {
                       if (!weak_lifetime.expired()) {
                         complete_task(run_id, task_index, attempt_id,
                                       std::move(result));
                       }
                     });
    co_return;
  }
  complete_task(run_id, task_index, attempt_id, std::move(result));
}

auto WorkflowRuntime::start_compute_task(const WorkflowRunId &run_id,
                                         std::size_t task_index,
                                         AttemptId attempt_id) -> void {
  auto &run = shard_states_[owner_shard(run_id)].active_runs.at(run_id.str());
  mark_attempt_running(run, task_index, attempt_id);
  auto inputs = input_values(run, task_index);
  if (!inputs) {
    complete_task(run_id, task_index, attempt_id, fail(inputs.error()));
    return;
  }

  auto node = run.plan->nodes[task_index].plan;
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
       run_id = run_id.clone(), task_index,
       attempt_id = attempt_id.clone()](Result<NodeOutputs> result) mutable {
        if (!weak_lifetime.expired()) {
          complete_task(run_id, task_index, attempt_id, std::move(result));
        }
      });
  if (!submitted) {
    complete_task(run_id, task_index, attempt_id, fail(submitted.error()));
    return;
  }
  run.tasks[task_index].compute_handle = std::move(*submitted);
}

auto WorkflowRuntime::complete_task(const WorkflowRunId &run_id,
                                    std::size_t task_index,
                                    const AttemptId &attempt_id,
                                    Result<NodeOutputs> result) -> void {
  const auto owner = owner_shard(run_id);
  if (!runtime_.is_current_shard() || runtime_.current_shard() != owner) {
    runtime_.post_to(owner,
                     [this, weak_lifetime = std::weak_ptr<int>(lifetime_token_),
                      run_id = run_id.clone(), task_index,
                      attempt_id = attempt_id.clone(),
                      result = std::move(result)]() mutable {
                       if (!weak_lifetime.expired()) {
                         complete_task(run_id, task_index, attempt_id,
                                       std::move(result));
                       }
                     });
    return;
  }

  auto &state = shard_states_[owner];
  const auto run_it = state.active_runs.find(run_id.str());
  if (run_it == state.active_runs.end() ||
      task_index >= run_it->second.tasks.size()) {
    return;
  }
  auto &run = run_it->second;
  auto &task = run.tasks[task_index];
  auto *attempt = active_attempt(task, attempt_id);
  if (attempt == nullptr || is_terminal(attempt->state) ||
      is_terminal(task.snapshot.state)) {
    return;
  }

  task.compute_handle.reset();
  task.instance_id.reset();
  if (run.active_attempts > 0) {
    run.active_attempts -= 1;
  }

  auto finish_failure = [&](std::error_code error) {
    const auto failure_class = classify_failure(error);
    attempt->failure_class = failure_class;
    attempt->error = error.message();
    task.snapshot.last_error = attempt->error;
    if (failure_class == FailureClass::Timeout) {
      attempt->termination_reason = TerminationReason::AttemptTimeout;
      (void)transition_attempt(*attempt, AttemptState::TimedOut);
    } else if (failure_class == FailureClass::Cancelled) {
      (void)transition_attempt(*attempt, AttemptState::Cancelled);
    } else {
      (void)transition_attempt(*attempt, AttemptState::Failed);
    }
    task.snapshot.active_attempt_id.reset();
    append_evidence(run, task_index, EvidenceType::AttemptCompleted,
                    make_metadata({{"attempt_id", attempt_id.str()},
                                   {"state", std::string{
                                                 to_string_view(attempt->state)}},
                                   {"error", attempt->error}}));

    const auto &plan = run.plan->nodes[task_index].plan;
    if (retryable(failure_class) &&
        task.snapshot.attempt_count <=
            static_cast<std::uint32_t>(plan.max_retries) &&
        run.snapshot.state != RunState::Stopping) {
      const auto delay = retry_delay(plan, task.snapshot.attempt_count);
      task.snapshot.next_attempt_at =
          std::chrono::system_clock::now() + delay;
      (void)transition_task(run, task_index, TaskState::RetryWaiting);
      schedule_retry(run_id, task_index);
      assert(invariants_hold(run));
      return;
    }

    const auto final_state = failure_class == FailureClass::Cancelled
                                 ? TaskState::Cancelled
                                 : TaskState::Failed;
    (void)transition_task(run, task_index, final_state);
    append_evidence(run, task_index, EvidenceType::TaskFailed,
                    make_metadata({{"error", task.snapshot.last_error}}));
    assert(invariants_hold(run));
    if (final_state == TaskState::Failed &&
        run.plan->policy.failure_policy == FailurePolicy::FailFast) {
      (void)request_stop(run_id, StopIntent::Fail, task.snapshot.last_error);
      return;
    }
    update_dependents(run_id, task_index);
  };

  if (run.snapshot.state == RunState::Stopping) {
    if (attempt->state != AttemptState::Terminating) {
      (void)transition_attempt(*attempt, AttemptState::Terminating);
    }
    attempt->termination_reason =
        run.snapshot.stop_intent == StopIntent::Cancel
            ? TerminationReason::RunCancelled
            : TerminationReason::RunFailed;
    (void)transition_attempt(*attempt, AttemptState::Cancelled);
    task.snapshot.active_attempt_id.reset();
    task.snapshot.last_error = run.snapshot.stop_reason;
    (void)transition_task(run, task_index, TaskState::Cancelled);
    append_evidence(run, task_index, EvidenceType::AttemptCompleted,
                    make_metadata({{"attempt_id", attempt_id.str()},
                                   {"state", "cancelled"}}));
    assert(invariants_hold(run));
    settle_control_state(run_id);
    return;
  }

  if (!result) {
    finish_failure(result.error());
    settle_control_state(run_id);
    dispatch(run_id);
    return;
  }

  if (attempt->state == AttemptState::Starting) {
    (void)transition_attempt(*attempt, AttemptState::Running);
  }

  std::optional<std::error_code> output_error;
  for (auto &[port, value] : *result) {
    if (port == "exit_code") {
      if (const auto *exit_code = std::get_if<std::int64_t>(&value)) {
        attempt->exit_code = static_cast<int>(*exit_code);
      }
    }
    if (auto *model = std::get_if<ModelResponse>(&value)) {
      run.model_tokens_used +=
          model->usage.input_tokens + model->usage.output_tokens;
      if (run.model_tokens_used > run.plan->policy.budget.max_model_tokens) {
        output_error = make_error_code(Error::ResourceExhausted);
        break;
      }
    }

    auto stored = run.values->put(
        OutputRef{.node_id = run.plan->nodes[task_index].plan.node_id.clone(),
                  .port = port.clone()},
        std::move(value));
    if (!stored) {
      output_error = stored.error();
      break;
    }
  }

  if (output_error) {
    finish_failure(*output_error);
    settle_control_state(run_id);
    dispatch(run_id);
    return;
  }

  (void)transition_attempt(*attempt, AttemptState::Succeeded);
  task.snapshot.active_attempt_id.reset();
  task.snapshot.last_error.clear();
  (void)transition_task(run, task_index, TaskState::Succeeded);
  append_evidence(run, task_index, EvidenceType::AttemptCompleted,
                  make_metadata({{"attempt_id", attempt_id.str()},
                                 {"state", "succeeded"}}));
  append_evidence(run, task_index, EvidenceType::TaskCompleted);
  if (run.plan->nodes[task_index].plan.type == NodeType::Model) {
    append_evidence(run, task_index, EvidenceType::ModelResponse);
  } else if (run.plan->nodes[task_index].plan.type == NodeType::Tool) {
    append_evidence(run, task_index, EvidenceType::ToolResponse);
  } else if (run.plan->nodes[task_index].plan.type == NodeType::Evaluator) {
    append_evidence(run, task_index, EvidenceType::Evaluation);
  }

  if (run.plan->nodes[task_index].plan.checkpoint) {
    checkpoint(run);
  }
  assert(invariants_hold(run));
  update_dependents(run_id, task_index);
  settle_control_state(run_id);
  dispatch(run_id);
}

auto WorkflowRuntime::schedule_retry(const WorkflowRunId &run_id,
                                     std::size_t task_index) -> void {
  const auto owner = owner_shard(run_id);
  auto &state = shard_states_[owner];
  const auto run_it = state.active_runs.find(run_id.str());
  if (run_it == state.active_runs.end() ||
      task_index >= run_it->second.tasks.size()) {
    return;
  }
  auto &run = run_it->second;
  auto &task = run.tasks[task_index];
  const auto delay =
      retry_delay(run.plan->nodes[task_index].plan,
                  task.snapshot.attempt_count);
  task.retry_handle = runtime_.schedule_after_on(
      owner, delay,
      [this, weak_lifetime = std::weak_ptr<int>(lifetime_token_),
       run_id = run_id.clone(), task_index] {
        if (weak_lifetime.expired()) {
          return;
        }
        auto &owner_state = shard_states_[owner_shard(run_id)];
        const auto active = owner_state.active_runs.find(run_id.str());
        if (active == owner_state.active_runs.end() ||
            task_index >= active->second.tasks.size()) {
          return;
        }
        auto &run = active->second;
        auto &task = run.tasks[task_index];
        task.retry_handle = {};
        if (task.snapshot.state != TaskState::RetryWaiting ||
            run.snapshot.state == RunState::Stopping ||
            is_terminal(run.snapshot.state)) {
          return;
        }
        (void)transition_task(run, task_index, TaskState::Ready);
        run.ready.push_back(task_index);
        assert(invariants_hold(run));
        dispatch(run_id);
      });
}

auto WorkflowRuntime::update_dependents(const WorkflowRunId &run_id,
                                        std::size_t completed_index) -> void {
  auto &run = shard_states_[owner_shard(run_id)].active_runs.at(run_id.str());
  for (const auto dependent : run.plan->nodes[completed_index].dependents) {
    if (dependent >= run.tasks.size() ||
        run.tasks[dependent].snapshot.state != TaskState::Pending) {
      continue;
    }

    bool all_terminal = true;
    bool all_success = true;
    for (const auto dependency : run.plan->nodes[dependent].dependencies) {
      const auto state = run.tasks[dependency].snapshot.state;
      all_terminal = all_terminal && is_terminal(state);
      all_success = all_success && is_success(state);
    }
    if (!all_terminal) {
      continue;
    }

    if (!all_success) {
      auto reason = SkipReason::BranchNotSelected;
      for (const auto dependency : run.plan->nodes[dependent].dependencies) {
        const auto state = run.tasks[dependency].snapshot.state;
        if (state == TaskState::Failed) {
          reason = SkipReason::UpstreamFailed;
          break;
        }
        if (state == TaskState::Cancelled) {
          reason = SkipReason::UpstreamCancelled;
        }
      }
      run.tasks[dependent].snapshot.skip_reason = reason;
      (void)transition_task(run, dependent, TaskState::Skipped);
      update_dependents(run_id, dependent);
      continue;
    }

    auto passes = conditions_pass(run, dependent);
    if (!passes) {
      (void)request_stop(run_id, StopIntent::Fail, passes.error().message());
      return;
    }
    if (!*passes) {
      run.tasks[dependent].snapshot.skip_reason = SkipReason::ConditionFalse;
      (void)transition_task(run, dependent, TaskState::Skipped);
      update_dependents(run_id, dependent);
      continue;
    }

    (void)transition_task(run, dependent, TaskState::Ready);
    run.ready.push_back(dependent);
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

auto WorkflowRuntime::request_stop(const WorkflowRunId &run_id,
                                   StopIntent intent, std::string reason)
    -> Result<void> {
  const auto owner = owner_shard(run_id);
  if (!runtime_.is_current_shard() || runtime_.current_shard() != owner) {
    return fail(Error::InvalidState);
  }
  auto &state = shard_states_[owner];
  const auto it = state.active_runs.find(run_id.str());
  if (it == state.active_runs.end()) {
    return fail(Error::NotFound);
  }
  auto &run = it->second;
  if (is_terminal(run.snapshot.state)) {
    return fail(Error::InvalidState);
  }
  if (run.snapshot.state == RunState::Stopping) {
    return ok();
  }

  run.snapshot.stop_intent = intent;
  run.snapshot.stop_reason = std::move(reason);
  if (intent == StopIntent::Fail) {
    run.snapshot.error = run.snapshot.stop_reason;
  }
  auto transitioned = transition_run(run, RunState::Stopping);
  if (!transitioned) {
    return transitioned;
  }
  append_evidence(run, run.tasks.size(), EvidenceType::RunStopRequested,
                  make_metadata({{"intent", std::string{to_string_view(intent)}},
                                 {"reason", run.snapshot.stop_reason}}));
  run.ready.clear();
  std::vector<InstanceId> instances_to_cancel;

  for (std::size_t index = 0; index < run.tasks.size(); ++index) {
    auto &task = run.tasks[index];
    if (task.retry_handle.valid()) {
      runtime_.cancel_after_on(owner, task.retry_handle);
      task.retry_handle = {};
    }
    if (task.snapshot.state == TaskState::Pending ||
        task.snapshot.state == TaskState::Ready ||
        task.snapshot.state == TaskState::RetryWaiting) {
      task.snapshot.last_error = run.snapshot.stop_reason;
      (void)transition_task(run, index, TaskState::Cancelled);
      continue;
    }
    if (task.snapshot.state != TaskState::Running ||
        !task.snapshot.active_attempt_id) {
      continue;
    }
    auto *attempt = active_attempt(task, *task.snapshot.active_attempt_id);
    if (attempt != nullptr && !is_terminal(attempt->state)) {
      attempt->termination_reason =
          intent == StopIntent::Cancel ? TerminationReason::RunCancelled
                                       : TerminationReason::RunFailed;
      (void)transition_attempt(*attempt, AttemptState::Terminating);
      emit_task_state(run, index);
    }
    if (task.compute_handle) {
      (void)task.compute_handle->request_stop();
    }
    if (task.instance_id) {
      instances_to_cancel.push_back(task.instance_id->clone());
    }
  }
  assert(invariants_hold(run));

  // Executor callbacks may complete synchronously. Do not retain or access
  // ActiveRun references after crossing this external boundary.
  for (const auto &instance_id : instances_to_cancel) {
    executor_.cancel(instance_id);
  }
  (void)finalize_run_if_ready(run_id);
  return ok();
}

auto WorkflowRuntime::settle_control_state(const WorkflowRunId &run_id)
    -> void {
  if (finalize_run_if_ready(run_id)) {
    return;
  }
  const auto owner = owner_shard(run_id);
  auto &state = shard_states_[owner];
  const auto it = state.active_runs.find(run_id.str());
  if (it == state.active_runs.end()) {
    return;
  }
  auto &run = it->second;
  if (run.snapshot.state == RunState::Pausing && run.active_attempts == 0) {
    if (transition_run(run, RunState::Paused)) {
      append_evidence(run, run.tasks.size(), EvidenceType::RunPaused);
      assert(invariants_hold(run));
    }
  }
}

auto WorkflowRuntime::finalize_run_if_ready(const WorkflowRunId &run_id)
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
  for (const auto &task : run.tasks) {
    all_terminal = all_terminal && is_terminal(task.snapshot.state);
    any_failed = any_failed || task.snapshot.state == TaskState::Failed;
    any_cancelled =
        any_cancelled || task.snapshot.state == TaskState::Cancelled;
  }

  if (!all_terminal || run.active_attempts != 0) {
    return false;
  }

  RunState terminal_state = RunState::Succeeded;
  if (run.snapshot.state == RunState::Stopping) {
    switch (run.snapshot.stop_intent.value_or(StopIntent::Fail)) {
    case StopIntent::Succeed:
      terminal_state = RunState::Succeeded;
      break;
    case StopIntent::Fail:
      terminal_state = RunState::Failed;
      break;
    case StopIntent::Cancel:
      terminal_state = RunState::Cancelled;
      break;
    }
  } else {
    terminal_state = any_cancelled ? RunState::Cancelled
                                   : (any_failed ? RunState::Failed
                                                 : RunState::Succeeded);
  }
  if (run.snapshot.error.empty() && terminal_state != RunState::Succeeded) {
    const auto failed_task = std::ranges::find_if(
        run.tasks, [terminal_state](const auto &task) {
          return terminal_state == RunState::Failed
                     ? task.snapshot.state == TaskState::Failed
                     : task.snapshot.state == TaskState::Cancelled;
        });
    if (failed_task != run.tasks.end()) {
      run.snapshot.error = failed_task->snapshot.last_error;
    }
  }
  if (!transition_run(run, terminal_state)) {
    return false;
  }
  assert(invariants_hold(run));
  const auto evidence_type =
      run.snapshot.state == RunState::Succeeded
          ? EvidenceType::RunCompleted
          : (run.snapshot.state == RunState::Cancelled
                 ? EvidenceType::RunCancelled
                 : EvidenceType::RunFailed);
  append_evidence(run, run.tasks.size(), evidence_type,
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
  snapshot.tasks.clear();
  snapshot.tasks.reserve(run.tasks.size());
  for (const auto &task : run.tasks) {
    snapshot.tasks.push_back(task.snapshot);
  }
  return std::make_shared<const RunSnapshot>(std::move(snapshot));
}

auto WorkflowRuntime::emit_run_state(ActiveRun &run) -> void {
  if (run.callbacks.on_run_state) {
    run.callbacks.on_run_state(run.snapshot);
  }
}

auto WorkflowRuntime::emit_task_state(ActiveRun &run,
                                      std::size_t task_index) -> void {
  run.snapshot.tasks[task_index] = run.tasks[task_index].snapshot;
  if (run.callbacks.on_task_state) {
    run.callbacks.on_task_state(run.snapshot.run_id,
                                run.tasks[task_index].snapshot);
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
      .stop_intent = run.snapshot.stop_intent,
      .stop_reason = run.snapshot.stop_reason,
      .tasks = run.snapshot.tasks,
      .values = std::move(*values),
      .created_at = std::chrono::system_clock::now(),
  };
  if (checkpoint_store_->save(std::move(checkpoint))) {
    append_evidence(run, run.tasks.size(), EvidenceType::Checkpoint);
  }
}

auto WorkflowRuntime::execute_command_node(WorkflowRunId run_id,
                                           std::size_t task_index,
                                           AttemptId attempt_id,
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

  const auto instance_id = instance_id_for(run_id, node.node_id, attempt_id);

  auto result = co_await execute_async(
      runtime_, executor_, instance_id, std::move(command), {}, {}, {}, {},
      node.timeout,
      [this, weak_lifetime = std::weak_ptr<int>(lifetime_token_),
       run_id = run_id.clone(), task_index,
       attempt_id = attempt_id.clone()](std::string_view state) mutable {
        if (state != "running" || weak_lifetime.expired()) {
          return;
        }
        const auto owner = owner_shard(run_id);
        auto mark_running =
            [this, weak_lifetime, run_id = run_id.clone(), task_index,
             attempt_id = attempt_id.clone()]() mutable {
              if (weak_lifetime.expired()) {
                return;
              }
              auto &owner_state = shard_states_[owner_shard(run_id)];
              const auto active = owner_state.active_runs.find(run_id.str());
              if (active == owner_state.active_runs.end()) {
                return;
              }
              mark_attempt_running(active->second, task_index, attempt_id);
            };
        if (runtime_.is_current_shard() && runtime_.current_shard() == owner) {
          mark_running();
        } else {
          runtime_.post_to(owner, std::move(mark_running));
        }
      });
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

auto WorkflowRuntime::pause(const WorkflowRunId &run_id)
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
              if (run.snapshot.state == RunState::Pausing ||
                  run.snapshot.state == RunState::Paused) {
                handler(boost::system::error_code{});
                return;
              }
              auto transitioned = transition_run(run, RunState::Pausing);
              if (!transitioned) {
                handler(to_boost_error(transitioned.error()));
                return;
              }
              append_evidence(run, run.tasks.size(),
                              EvidenceType::RunPauseRequested);
              settle_control_state(run_id);
              handler(boost::system::error_code{});
            });
      },
      dagforge::use_nothrow);
  if (ec) {
    co_return fail(ec);
  }
  co_return ok();
}

auto WorkflowRuntime::resume(const WorkflowRunId &run_id)
    -> task<Result<void>> {
  const auto target = owner_shard(run_id);
  auto [ec] = co_await boost::asio::async_initiate<
      const decltype(dagforge::use_nothrow), void(boost::system::error_code)>(
      [this, weak_lifetime = std::weak_ptr<int>(lifetime_token_),
       run_id = run_id.clone(), target](auto handler) mutable {
        runtime_.post_to(
            target, [this, weak_lifetime, run_id = std::move(run_id),
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
              if (run.snapshot.state == RunState::Running) {
                handler(boost::system::error_code{});
                return;
              }
              auto transitioned = transition_run(run, RunState::Running);
              if (!transitioned) {
                handler(to_boost_error(transitioned.error()));
                return;
              }
              append_evidence(run, run.tasks.size(), EvidenceType::RunResumed);
              dispatch(run_id);
              handler(boost::system::error_code{});
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
              auto stopped =
                  request_stop(run_id, StopIntent::Cancel, "cancel requested");
              if (!stopped) {
                handler(to_boost_error(stopped.error()));
                return;
              }
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
