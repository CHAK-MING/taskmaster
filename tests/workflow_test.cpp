#include "dagforge/core/runtime.hpp"
#include "dagforge/core/sync_wait.hpp"
#include "dagforge/config/storage_config.hpp"
#include "dagforge/executors/command/executor.hpp"
#include "dagforge/sandbox/command_runner.hpp"
#include "dagforge/util/ascii.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/workflow/executor_registry.hpp"
#include "dagforge/workflow/plan_compiler.hpp"
#include "dagforge/workflow/plan_store.hpp"
#include "dagforge/workflow/run_value_store.hpp"
#include "dagforge/workflow/workflow_control_plane.hpp"
#include "dagforge/workflow/workflow_plan_loader.hpp"
#include "dagforge/workflow/workflow_runtime.hpp"

#include "../src/dagforge/executors/command/detail/testing.hpp"
#include "../src/dagforge/workflow/storage/detail/durable_file.hpp"
#include "../src/dagforge/workflow/storage/detail/durable_file_testing.hpp"
#include "../src/dagforge/workflow/storage/detail/json_file_catalog.hpp"
#include "../src/dagforge/workflow/detail/repair_planner.hpp"
#include "../src/dagforge/workflow/detail/retry_policy.hpp"
#include "../src/dagforge/workflow/detail/sha256.hpp"
#include "../src/dagforge/workflow/detail/state_machine.hpp"
#include "../src/dagforge/workflow/storage/detail/storage_codec.hpp"

#include "gtest/gtest.h"
#include "json_test_utils.hpp"
#include "test_utils.hpp"

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
#include <unordered_set>
#include <utility>

#include <unistd.h>
#include <sys/stat.h>

using namespace dagforge;
using namespace dagforge::config;
using namespace dagforge::executors;
using namespace dagforge::sandbox;
using namespace dagforge::workflow;
using dagforge::test::make_payload;
using dagforge::test::materialize;
using dagforge::test::parse_payload;

namespace {

const StorageConfig kStorageDefaults{};

[[nodiscard]] auto open_test_evidence(
    std::filesystem::path file,
    std::size_t max_records = StorageConfig{}.max_evidence_records)
    -> std::shared_ptr<EvidenceLedger> {
  auto opened = EvidenceLedger::open(
      std::move(file), max_records,
      kStorageDefaults.max_evidence_file_bytes,
      kStorageDefaults.max_evidence_record_bytes);
  if (!opened) {
    throw std::runtime_error(opened.error().message());
  }
  return std::move(*opened);
}

[[nodiscard]] auto temporary_test_directory(std::string_view name)
    -> std::filesystem::path {
  return std::filesystem::temp_directory_path() /
         std::format("dagforge-{}-{}", name, ::getpid());
}

[[nodiscard]] auto storage_fixture(std::string_view name) -> std::string {
  const auto path = std::filesystem::path{__FILE__}.parent_path() / "fixtures" /
                    "storage" / name;
  std::ifstream input(path, std::ios::binary);
  if (!input) {
    throw std::runtime_error(
        std::format("failed to open fixture {}", path.string()));
  }
  std::string contents(std::istreambuf_iterator<char>(input), {});
  while (contents.ends_with('\n') || contents.ends_with('\r')) {
    contents.pop_back();
  }
  return contents;
}

[[nodiscard]] auto storage_payload(std::string_view envelope)
    -> Result<std::string> {
  auto parsed = parse_json(envelope);
  if (!parsed || !parsed->is_object()) {
    return fail(Error::ParseError);
  }
  const auto payload = parsed->get_object().find("payload");
  if (payload == parsed->get_object().end()) {
    return fail(Error::ParseError);
  }
  return serialize_json(payload->second);
}

class ManualTaskExecutor final : public ITaskExecutor {
public:
  explicit ManualTaskExecutor(Runtime *runtime = nullptr) : runtime_(runtime) {}

  [[nodiscard]] auto type() const noexcept -> std::string_view override {
    return "test";
  }

  [[nodiscard]] auto compile(JsonPayload config,
                             ExecutorCompileContext) const
      -> Result<CompiledExecutorConfig> override {
    return ok(CompiledExecutorConfig::from_encoded(std::move(config)));
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
        .principal = std::move(request.principal),
        .trace = std::move(request.trace),
        .config = request.config.encoded(),
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

  [[nodiscard]] auto next_context() const
      -> std::optional<std::pair<Principal, TraceContext>> {
    std::lock_guard lock(mutex_);
    if (pending_.empty()) {
      return std::nullopt;
    }
    return std::pair{pending_.front().principal, pending_.front().trace};
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

  auto complete_next_with_failure(ExecutionFailure failure) -> bool {
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
         result = task_failed(std::move(failure))]() mutable {
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
    Principal principal;
    TraceContext trace;
    JsonPayload config;
    ExecutorInputs inputs;
    std::vector<WorkflowPortId> outputs;
    TaskExecutionSink sink;
  };

  [[nodiscard]] static auto make_outputs(const Pending &pending,
                                         int exit_code,
                                         const std::string &output)
      -> TaskExecutionResult {
    if (exit_code != 0) {
      return task_failed(make_execution_failure(
          Error::Unknown, "test_exit_nonzero",
          std::format("Test executor exited with status {}", exit_code),
          make_payload(glz::obj{"exit_code", exit_code})));
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
  auto prepare(CommandPreparationRequest request) const
      -> Result<CommandSpec> override {
    return ok(std::move(request.command));
  }

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
    -> JsonPayload {
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
  return make_payload(config);
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
      -> Result<ArtifactPutResult> override {
    return fail(Error::ResourceExhausted);
  }

  [[nodiscard]] auto get(const ArtifactId &) const
      -> Result<ArtifactBlob> override {
    return fail(Error::NotFound);
  }

  auto erase(const ArtifactId &) -> Result<ArtifactEraseResult> override {
    return fail(Error::NotFound);
  }
};

class ScriptedArtifactStore final : public IArtifactStore {
public:
  [[nodiscard]] auto put(std::span<const std::byte> data,
                         std::string media_type)
      -> Result<ArtifactPutResult> override {
    ArtifactRef ref{
        .artifact_id = ArtifactId{std::format("scripted-{}", next_id_++)},
        .media_type = std::move(media_type),
        .size_bytes = data.size(),
        .digest = std::format("digest-{}", next_id_),
    };
    artifacts_.push_back(
        ArtifactBlob{.ref = ref, .data = {data.begin(), data.end()}});
    return ok(ArtifactPutResult{std::move(ref)});
  }

  [[nodiscard]] auto get(const ArtifactId &artifact_id) const
      -> Result<ArtifactBlob> override {
    const auto artifact = std::ranges::find_if(
        artifacts_, [&](const ArtifactBlob &candidate) {
          return candidate.ref.artifact_id == artifact_id;
        });
    return artifact == artifacts_.end() ? fail(Error::NotFound)
                                        : ok(*artifact);
  }

  auto erase(const ArtifactId &artifact_id)
      -> Result<ArtifactEraseResult> override {
    if (erase_failure_ && *erase_failure_ == artifact_id) {
      return fail(Error::PersistenceError);
    }
    const auto artifact = std::ranges::find_if(
        artifacts_, [&](const ArtifactBlob &candidate) {
          return candidate.ref.artifact_id == artifact_id;
        });
    if (artifact == artifacts_.end()) {
      return fail(Error::NotFound);
    }
    artifacts_.erase(artifact);
    return ok(ArtifactEraseResult{.logical_deleted = true});
  }

  auto fail_erase_for(ArtifactId artifact_id) -> void {
    erase_failure_ = std::move(artifact_id);
  }

  auto clear_erase_failure() -> void { erase_failure_.reset(); }

  [[nodiscard]] auto size() const noexcept -> std::size_t {
    return artifacts_.size();
  }

  [[nodiscard]] auto contains(const ArtifactId &artifact_id) const -> bool {
    return std::ranges::any_of(artifacts_, [&](const ArtifactBlob &candidate) {
      return candidate.ref.artifact_id == artifact_id;
    });
  }

private:
  std::size_t next_id_{1};
  std::optional<ArtifactId> erase_failure_;
  std::vector<ArtifactBlob> artifacts_;
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
  const auto executor_config =
      materialize(json_plan->nodes.front().config);
  EXPECT_EQ(executor_config["operation"].as<std::string>(), "analyze");
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
  EXPECT_EQ(invalid_policy.error(), make_error_code(Error::ParseError));
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
  EXPECT_EQ(decoded->policy.budget.max_run_duration,
            std::chrono::seconds(45));
  EXPECT_EQ(decoded->nodes[1].retry_initial_delay,
            std::chrono::milliseconds(25));
  EXPECT_EQ(decoded->nodes[1].retry_max_delay,
            std::chrono::milliseconds(100));
  EXPECT_EQ(decoded->nodes[1].timeout, std::chrono::seconds(12));

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
  EXPECT_EQ(rejected.error(), make_error_code(Error::ParseError));

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
  ASSERT_TRUE(invalid_node_result.has_value())
      << invalid_node_result.error().message();
  TestExecutorEnvironment environment;
  auto invalid_node_compiled =
      PlanCompiler{environment.registry}.compile(std::move(*invalid_node_result));
  ASSERT_FALSE(invalid_node_compiled.has_value());
  EXPECT_EQ(invalid_node_compiled.error(),
            make_error_code(Error::InvalidArgument));

  constexpr std::string_view missing_port = R"({
    "workflow_id":"missing-port",
    "nodes":[
      {"id":"source","executor":"test","outputs":["result"]},
      {"id":"target","executor":"test",
       "inputs":[{"input":"value","node":"source"}]}
    ]
  })";
  auto missing_port_plan = WorkflowPlanLoader::from_json(missing_port);
  ASSERT_TRUE(missing_port_plan.has_value())
      << missing_port_plan.error().message();
  auto missing_port_compiled =
      PlanCompiler{environment.registry}.compile(std::move(*missing_port_plan));
  ASSERT_FALSE(missing_port_compiled.has_value());
  EXPECT_EQ(missing_port_compiled.error(),
            make_error_code(Error::InvalidArgument));
}

TEST(WorkflowSerdeTest, UsesTypedDomainMetadataDirectly) {
  auto node_id_json = serialize_json(WorkflowNodeId{"node-a"});
  ASSERT_TRUE(node_id_json.has_value()) << node_id_json.error().message();
  EXPECT_EQ(*node_id_json, R"("node-a")");

  auto node_id = parse_json_as<WorkflowNodeId>(*node_id_json);
  ASSERT_TRUE(node_id.has_value()) << node_id.error().message();
  EXPECT_EQ(*node_id, WorkflowNodeId{"node-a"});

  auto delay_json = serialize_json(std::chrono::milliseconds{25});
  ASSERT_TRUE(delay_json.has_value()) << delay_json.error().message();
  EXPECT_EQ(*delay_json, "25");

  EXPECT_EQ(to_string_view(FailurePolicy::FailFast), "fail_fast");
  EXPECT_EQ(to_string_view(ConditionKind::StringEquals), "string_equals");
  EXPECT_EQ(to_string_view(RunState::Paused), "paused");
  EXPECT_EQ(to_string_view(StopIntent::Cancel), "cancel");
  EXPECT_EQ(to_string_view(TaskState::RetryWaiting), "retry_waiting");
  EXPECT_EQ(to_string_view(AttemptState::TimedOut), "timed_out");
  EXPECT_EQ(to_string_view(SkipReason::BranchNotSelected),
            "branch_not_selected");
  EXPECT_EQ(to_string_view(TerminationReason::RunFailed), "run_failed");
  EXPECT_EQ(to_string_view(EvidenceType::RepairRunStarted),
            "repair_run_started");

  auto task_state_json = serialize_json(TaskState::RetryWaiting);
  ASSERT_TRUE(task_state_json.has_value())
      << task_state_json.error().message();
  EXPECT_EQ(*task_state_json, R"("retry_waiting")");

  auto task_state = parse_json_as<TaskState>(*task_state_json);
  ASSERT_TRUE(task_state.has_value()) << task_state.error().message();
  EXPECT_EQ(*task_state, TaskState::RetryWaiting);

  auto error_json = serialize_json(Error::InvalidArgument);
  ASSERT_TRUE(error_json.has_value()) << error_json.error().message();
  EXPECT_EQ(*error_json, R"("invalid_argument")");
  auto parsed_error = parse_json_as<Error>(*error_json);
  ASSERT_TRUE(parsed_error.has_value()) << parsed_error.error().message();
  EXPECT_EQ(*parsed_error, Error::InvalidArgument);
  EXPECT_FALSE(parse_json_as<Error>(R"("not_an_error")").has_value());
}

TEST(WorkflowSerdeTest, RoundTripsTaggedWorkflowValues) {
  const std::array values{
      WorkflowValue{std::monostate{}},
      WorkflowValue{true},
      WorkflowValue{std::int64_t{42}},
      WorkflowValue{3.25},
      WorkflowValue{std::string{"text"}},
      WorkflowValue{parse_payload(R"({"nested":[1,true]})")},
      WorkflowValue{ArtifactRef{
          .artifact_id = ArtifactId{"artifact-a"},
          .media_type = "application/json",
          .size_bytes = 17,
          .digest = "digest-a",
      }},
  };
  constexpr std::array<std::string_view, 7> tags{
      "null", "bool", "integer", "number", "string", "json", "artifact"};
  constexpr std::array<std::string_view, 7> text{
      "", "true", "42", "3.25", "text", R"({"nested":[1,true]})",
      "artifact-a"};

  for (std::size_t index = 0; index < values.size(); ++index) {
    EXPECT_EQ(workflow_value_text(values[index]), text[index]);
    auto encoded = serialize_json(values[index]);
    ASSERT_TRUE(encoded.has_value()) << encoded.error().message();
    auto object = parse_json(*encoded);
    ASSERT_TRUE(object.has_value()) << object.error().message();
    EXPECT_EQ((*object)["type"].as<std::string>(), tags[index]);

    auto decoded = parse_json_as<WorkflowValue>(*encoded);
    ASSERT_TRUE(decoded.has_value()) << decoded.error().message();
    EXPECT_EQ(decoded->index(), index);
  }

  EXPECT_FALSE(parse_json_as<WorkflowValue>(
                   R"({"type":"unsupported","value":1})")
                   .has_value());
}

TEST(WorkflowSerdeTest, UsesDirectCheckpointAndEvidenceSchemas) {
  WorkflowCheckpoint checkpoint;
  checkpoint.plan.workflow_id = WorkflowId{"direct-storage"};
  checkpoint.trigger.trigger_id = WorkflowTriggerId{"trigger-a"};
  checkpoint.trigger.workflow_id = WorkflowId{"direct-storage"};
  checkpoint.snapshot.run_id = WorkflowRunId{"run-a"};
  checkpoint.snapshot.workflow_id = WorkflowId{"direct-storage"};
  checkpoint.snapshot.plan_id = WorkflowPlanId{"plan-a"};
  checkpoint.values.emplace_back(
      OutputRef{.node_id = WorkflowNodeId{"node-a"},
                .port = WorkflowPortId{"result"}},
      std::string{"value"});

  auto encoded_checkpoint = serialize_json(checkpoint);
  ASSERT_TRUE(encoded_checkpoint.has_value())
      << encoded_checkpoint.error().message();
  auto checkpoint_json = parse_json(*encoded_checkpoint);
  ASSERT_TRUE(checkpoint_json.has_value())
      << checkpoint_json.error().message();
  EXPECT_TRUE(checkpoint_json->contains("plan"));
  EXPECT_TRUE(checkpoint_json->contains("trigger"));
  EXPECT_FALSE(checkpoint_json->contains("plan_json"));
  EXPECT_FALSE(checkpoint_json->contains("schema_version"));
  const auto &trigger = (*checkpoint_json)["trigger"];
  EXPECT_TRUE(trigger.contains("principal"));
  EXPECT_FALSE(trigger.contains("principal_subject"));
  const auto &stored_value =
      (*checkpoint_json)["values"].get_array().front();
  EXPECT_TRUE(stored_value.contains("output"));
  EXPECT_EQ(stored_value["value"]["type"].as<std::string>(), "string");

  EvidenceRecord evidence{
      .evidence_id = EvidenceId{"evidence-a"},
      .run_id = WorkflowRunId{"run-a"},
      .type = EvidenceType::TaskCompleted,
      .actor = Principal{.subject = "tester", .roles = {"operator"}},
  };
  auto encoded_evidence = serialize_json(evidence);
  ASSERT_TRUE(encoded_evidence.has_value())
      << encoded_evidence.error().message();
  auto evidence_json = parse_json(*encoded_evidence);
  ASSERT_TRUE(evidence_json.has_value()) << evidence_json.error().message();
  EXPECT_TRUE(evidence_json->contains("actor"));
  EXPECT_FALSE(evidence_json->contains("actor_subject"));
  EXPECT_EQ((*evidence_json)["type"].as<std::string>(), "task_completed");
}

TEST(WorkflowSerdeTest, SerializesRuntimeSnapshotsWithApiWireSemantics) {
  RunSnapshot snapshot{
      .run_id = WorkflowRunId{"run-a"},
      .workflow_id = WorkflowId{"workflow-a"},
      .plan_id = WorkflowPlanId{"plan-a"},
      .state = RunState::Failed,
      .stop_intent = StopIntent::Fail,
      .stop_reason = "failed",
      .repair_revision = 0,
      .tasks = {TaskSnapshot{
          .node_id = WorkflowNodeId{"node-a"},
          .state = TaskState::Failed,
          .attempt_count = 1,
          .attempts = {AttemptSnapshot{
              .attempt_id = AttemptId{"attempt-a"},
              .number = 1,
              .state = AttemptState::Failed,
              .created_at = std::chrono::system_clock::time_point{
                  std::chrono::milliseconds{25}},
          }},
      }},
      .created_at = std::chrono::system_clock::time_point{
          std::chrono::milliseconds{10}},
  };

  auto encoded = serialize_json(snapshot);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().message();
  auto json = parse_json(*encoded);
  ASSERT_TRUE(json.has_value()) << json.error().message();
  const auto &object = json->get_object();
  EXPECT_EQ(object.at("state").as<std::string>(), "failed");
  EXPECT_EQ(object.at("stop_intent").as<std::string>(), "fail");
  EXPECT_EQ(object.at("created_at_ms").as<std::int64_t>(), 10);
  EXPECT_FALSE(object.contains("started_at_ms"));
  EXPECT_FALSE(object.contains("finished_at_ms"));
  EXPECT_FALSE(object.contains("repair_reason"));
  EXPECT_FALSE(object.contains("parent_run_id"));

  const auto &task = object.at("tasks").get_array().front().get_object();
  EXPECT_EQ(task.at("state").as<std::string>(), "failed");
  EXPECT_FALSE(task.contains("next_attempt_at_ms"));
  EXPECT_FALSE(task.contains("active_attempt_id"));
  const auto &attempt =
      task.at("attempts").get_array().front().get_object();
  EXPECT_EQ(attempt.at("state").as<std::string>(), "failed");
  EXPECT_EQ(attempt.at("created_at_ms").as<std::int64_t>(), 25);
  EXPECT_FALSE(attempt.contains("started_at_ms"));
}

TEST(WorkflowSerdeTest, SerializesFailuresFromOneMetadataDefinition) {
  ExecutionFailure failure{
      .kind = Error::InvalidArgument,
      .code = "invalid_input",
      .message = "bad input",
      .artifacts = {FailureArtifact{
          .name = "details",
          .artifact = ArtifactRef{
              .artifact_id = ArtifactId{"artifact-b"},
              .media_type = "application/json",
              .size_bytes = 9,
              .digest = "digest-b",
          },
      }},
  };

  auto encoded = serialize_json(failure);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().message();
  auto json = parse_json(*encoded);
  ASSERT_TRUE(json.has_value()) << json.error().message();
  const auto &object = json->get_object();
  EXPECT_EQ(object.at("kind").as<std::string>(), "invalid_argument");
  EXPECT_EQ(object.at("code").as<std::string>(), "invalid_input");
  const auto &artifact =
      object.at("artifacts").get_array().front().get_object();
  EXPECT_EQ(artifact.at("name").as<std::string>(), "details");
  EXPECT_EQ(artifact.at("artifact_id").as<std::string>(), "artifact-b");
  EXPECT_FALSE(artifact.contains("artifact"));
}

TEST(WorkflowStateModelTest, RejectsIllegalTerminalTransitions) {
  EXPECT_TRUE(workflow::detail::can_transition(RunState::Running,
                                               RunState::Pausing));
  EXPECT_TRUE(workflow::detail::can_transition(RunState::Pausing,
                                               RunState::Paused));
  EXPECT_TRUE(workflow::detail::can_transition(RunState::Paused,
                                               RunState::Running));
  EXPECT_TRUE(workflow::detail::can_transition(RunState::Running,
                                               RunState::Stopping));
  EXPECT_TRUE(workflow::detail::can_transition(RunState::Stopping,
                                               RunState::Cancelled));
  EXPECT_FALSE(workflow::detail::can_transition(RunState::Cancelled,
                                                RunState::Running));

  EXPECT_TRUE(workflow::detail::can_transition(TaskState::Running,
                                               TaskState::RetryWaiting));
  EXPECT_TRUE(workflow::detail::can_transition(TaskState::RetryWaiting,
                                               TaskState::Ready));
  EXPECT_FALSE(workflow::detail::can_transition(TaskState::Succeeded,
                                                TaskState::Running));

  EXPECT_TRUE(
      workflow::detail::can_transition(AttemptState::Running,
                                       AttemptState::Terminating));
  EXPECT_TRUE(
      workflow::detail::can_transition(AttemptState::Starting,
                                       AttemptState::TimedOut));
  EXPECT_TRUE(
      workflow::detail::can_transition(AttemptState::Terminating,
                                       AttemptState::TimedOut));
  EXPECT_FALSE(
      workflow::detail::can_transition(AttemptState::Succeeded,
                                       AttemptState::Running));
}

TEST(WorkflowStateModelTest, AppliesTransitionsAndValidatesSnapshots) {
  const auto now = std::chrono::system_clock::now();
  const auto later = now + std::chrono::seconds(1);

  RunSnapshot transitioned_run{
      .run_id = WorkflowRunId{"state-run"},
      .workflow_id = WorkflowId{"state-workflow"},
      .plan_id = WorkflowPlanId{"state-plan"},
      .state = RunState::Running,
  };
  EXPECT_EQ(workflow::detail::transition(transitioned_run, RunState::Cancelled,
                                         now)
                .error(),
            make_error_code(Error::InvalidState));
  ASSERT_TRUE(workflow::detail::transition(transitioned_run,
                                           RunState::Succeeded, later)
                  .has_value());
  EXPECT_EQ(transitioned_run.finished_at, later);

  TaskSnapshot transitioned_task{
      .node_id = WorkflowNodeId{"task"},
      .state = TaskState::Pending,
      .next_attempt_at = later,
  };
  ASSERT_TRUE(workflow::detail::transition(transitioned_task, TaskState::Ready,
                                           now)
                  .has_value());
  EXPECT_FALSE(transitioned_task.next_attempt_at.has_value());
  ASSERT_TRUE(workflow::detail::transition(transitioned_task,
                                           TaskState::Running, now)
                  .has_value());
  EXPECT_EQ(transitioned_task.started_at, now);
  transitioned_task.active_attempt_id = AttemptId{"attempt"};
  transitioned_task.next_attempt_at = later;
  ASSERT_TRUE(workflow::detail::transition(transitioned_task,
                                           TaskState::Succeeded, later)
                  .has_value());
  EXPECT_FALSE(transitioned_task.active_attempt_id.has_value());
  EXPECT_FALSE(transitioned_task.next_attempt_at.has_value());
  EXPECT_EQ(transitioned_task.finished_at, later);

  AttemptSnapshot transitioned_attempt{
      .attempt_id = AttemptId{"attempt"},
      .number = 1,
      .state = AttemptState::Starting,
  };
  ASSERT_TRUE(workflow::detail::transition(transitioned_attempt,
                                           AttemptState::Running, now)
                  .has_value());
  EXPECT_EQ(transitioned_attempt.started_at, now);
  ASSERT_TRUE(workflow::detail::transition(transitioned_attempt,
                                           AttemptState::Succeeded, later)
                  .has_value());
  EXPECT_EQ(transitioned_attempt.finished_at, later);
  EXPECT_EQ(workflow::detail::transition(transitioned_attempt,
                                         AttemptState::Running, later)
                .error(),
            make_error_code(Error::InvalidState));

  const auto failure = make_execution_failure(
      Error::Unknown, "state_failure", "State model failure");
  RunSnapshot persisted_failure{
      .run_id = WorkflowRunId{"persisted-run"},
      .workflow_id = WorkflowId{"persisted-workflow"},
      .plan_id = WorkflowPlanId{"persisted-plan"},
      .state = RunState::Succeeded,
  };
  workflow::detail::apply_persistence_failure(persisted_failure, failure);
  EXPECT_EQ(persisted_failure.state, RunState::Failed);
  EXPECT_EQ(persisted_failure.stop_intent, StopIntent::Fail);
  EXPECT_EQ(persisted_failure.stop_reason, failure.message);
  ASSERT_TRUE(persisted_failure.failure.has_value());
  EXPECT_EQ(persisted_failure.failure->code, failure.code);

  AttemptSnapshot active_attempt{
      .attempt_id = AttemptId{"active-attempt"},
      .number = 1,
      .state = AttemptState::Running,
  };
  EXPECT_TRUE(workflow::detail::attempt_snapshot_is_valid(active_attempt));
  auto invalid_attempt = active_attempt;
  invalid_attempt.attempt_id = {};
  EXPECT_FALSE(workflow::detail::attempt_snapshot_is_valid(invalid_attempt));
  invalid_attempt = active_attempt;
  invalid_attempt.number = 0;
  EXPECT_FALSE(workflow::detail::attempt_snapshot_is_valid(invalid_attempt));
  invalid_attempt = active_attempt;
  invalid_attempt.failure = failure;
  EXPECT_FALSE(workflow::detail::attempt_snapshot_is_valid(invalid_attempt));

  TaskSnapshot active_task{
      .node_id = WorkflowNodeId{"active-task"},
      .state = TaskState::Running,
      .attempt_count = 1,
      .active_attempt_id = active_attempt.attempt_id,
      .attempts = {active_attempt},
  };
  EXPECT_TRUE(workflow::detail::task_snapshot_is_valid(active_task));

  auto invalid_task = active_task;
  invalid_task.node_id = {};
  EXPECT_FALSE(workflow::detail::task_snapshot_is_valid(invalid_task));
  invalid_task = active_task;
  invalid_task.attempt_count = 2;
  EXPECT_FALSE(workflow::detail::task_snapshot_is_valid(invalid_task));
  invalid_task = active_task;
  invalid_task.active_attempt_id = AttemptId{"different"};
  EXPECT_FALSE(workflow::detail::task_snapshot_is_valid(invalid_task));
  invalid_task = active_task;
  invalid_task.state = TaskState::Ready;
  EXPECT_FALSE(workflow::detail::task_snapshot_is_valid(invalid_task));

  AttemptSnapshot completed_attempt{
      .attempt_id = AttemptId{"completed-attempt"},
      .number = 1,
      .state = AttemptState::Succeeded,
  };
  TaskSnapshot completed_task{
      .node_id = WorkflowNodeId{"completed-task"},
      .state = TaskState::Succeeded,
      .attempt_count = 1,
      .attempts = {completed_attempt},
  };
  EXPECT_TRUE(workflow::detail::task_snapshot_is_valid(completed_task));
  invalid_task = completed_task;
  invalid_task.active_attempt_id = completed_attempt.attempt_id;
  EXPECT_FALSE(workflow::detail::task_snapshot_is_valid(invalid_task));
  invalid_task = completed_task;
  invalid_task.failure = failure;
  EXPECT_FALSE(workflow::detail::task_snapshot_is_valid(invalid_task));

  TaskSnapshot reused_task{
      .node_id = WorkflowNodeId{"reused-task"},
      .state = TaskState::Succeeded,
      .reused_from_run_id = WorkflowRunId{"parent-run"},
  };
  EXPECT_TRUE(workflow::detail::task_snapshot_is_valid(reused_task));
  invalid_task = reused_task;
  invalid_task.state = TaskState::Failed;
  invalid_task.failure = failure;
  EXPECT_FALSE(workflow::detail::task_snapshot_is_valid(invalid_task));

  TaskSnapshot failed_task{
      .node_id = WorkflowNodeId{"failed-task"},
      .state = TaskState::Failed,
      .failure = failure,
  };
  EXPECT_TRUE(workflow::detail::task_snapshot_is_valid(failed_task));
  invalid_task = failed_task;
  invalid_task.failure.reset();
  EXPECT_FALSE(workflow::detail::task_snapshot_is_valid(invalid_task));

  TaskSnapshot retry_task{
      .node_id = WorkflowNodeId{"retry-task"},
      .state = TaskState::RetryWaiting,
      .attempt_count = 1,
      .next_attempt_at = later,
      .attempts = {AttemptSnapshot{
          .attempt_id = AttemptId{"retry-attempt"},
          .number = 1,
          .state = AttemptState::Failed,
          .failure = failure,
      }},
  };
  EXPECT_TRUE(workflow::detail::task_snapshot_is_valid(retry_task));
  invalid_task = retry_task;
  invalid_task.next_attempt_at.reset();
  EXPECT_FALSE(workflow::detail::task_snapshot_is_valid(invalid_task));
  invalid_task = completed_task;
  invalid_task.next_attempt_at = later;
  EXPECT_FALSE(workflow::detail::task_snapshot_is_valid(invalid_task));

  auto duplicate_attempts = completed_task;
  duplicate_attempts.attempt_count = 2;
  auto duplicate = completed_attempt;
  duplicate.number = 2;
  duplicate_attempts.attempts.push_back(std::move(duplicate));
  EXPECT_FALSE(workflow::detail::task_snapshot_is_valid(duplicate_attempts));

  RunSnapshot active_run{
      .run_id = WorkflowRunId{"aggregate-run"},
      .workflow_id = WorkflowId{"aggregate-workflow"},
      .plan_id = WorkflowPlanId{"aggregate-plan"},
      .state = RunState::Running,
      .tasks = {active_task},
  };
  EXPECT_TRUE(workflow::detail::run_snapshot_is_valid(active_run));
  EXPECT_TRUE(workflow::detail::runtime_projection_is_valid(
      active_run, std::span<const TaskSnapshot>{active_run.tasks}, 1));

  auto invalid_run = active_run;
  invalid_run.run_id = {};
  EXPECT_FALSE(workflow::detail::run_snapshot_is_valid(invalid_run));
  invalid_run = active_run;
  invalid_run.parent_run_id = WorkflowRunId{"parent"};
  EXPECT_FALSE(workflow::detail::run_snapshot_is_valid(invalid_run));
  invalid_run.parent_plan_id = WorkflowPlanId{"parent-plan"};
  invalid_run.repair_revision = 1;
  EXPECT_TRUE(workflow::detail::run_snapshot_is_valid(invalid_run));
  invalid_run.repair_revision = 0;
  EXPECT_FALSE(workflow::detail::run_snapshot_is_valid(invalid_run));
  invalid_run = active_run;
  invalid_run.tasks.push_back(active_task);
  EXPECT_FALSE(workflow::detail::run_snapshot_is_valid(invalid_run));
  invalid_run = active_run;
  invalid_run.state = RunState::Succeeded;
  EXPECT_FALSE(workflow::detail::run_snapshot_is_valid(invalid_run));
  invalid_run.tasks = {completed_task};
  EXPECT_TRUE(workflow::detail::run_snapshot_is_valid(invalid_run));
  invalid_run.failure = failure;
  EXPECT_FALSE(workflow::detail::run_snapshot_is_valid(invalid_run));
  invalid_run.state = RunState::Failed;
  invalid_run.tasks = {failed_task};
  EXPECT_TRUE(workflow::detail::run_snapshot_is_valid(invalid_run));
  invalid_run.failure.reset();
  EXPECT_FALSE(workflow::detail::run_snapshot_is_valid(invalid_run));

  auto projection = active_run;
  projection.tasks.clear();
  EXPECT_FALSE(workflow::detail::runtime_projection_is_valid(
      projection, std::span<const TaskSnapshot>{active_run.tasks}, 1));
  projection = active_run;
  auto mismatched_tasks = active_run.tasks;
  mismatched_tasks.front().state = TaskState::Ready;
  EXPECT_FALSE(workflow::detail::runtime_projection_is_valid(
      projection, std::span<const TaskSnapshot>{mismatched_tasks}, 1));
  EXPECT_FALSE(workflow::detail::runtime_projection_is_valid(
      active_run, std::span<const TaskSnapshot>{active_run.tasks}, 0));
}

TEST(WorkflowStateModelTest, RehydratesInterruptedRetryAndStoppingRuns) {
  const auto now = std::chrono::system_clock::now();
  const auto restart_failure = make_execution_failure(
      Error::Unknown, "previous_failure", "Previous run failure");

  RunSnapshot resumed{
      .run_id = WorkflowRunId{"resumed-run"},
      .workflow_id = WorkflowId{"resumed-workflow"},
      .plan_id = WorkflowPlanId{"resumed-plan"},
      .state = RunState::Pausing,
      .tasks = {
          TaskSnapshot{
              .node_id = WorkflowNodeId{"interrupted"},
              .state = TaskState::Running,
              .attempt_count = 1,
              .active_attempt_id = AttemptId{"interrupted-attempt"},
              .attempts = {AttemptSnapshot{
                  .attempt_id = AttemptId{"interrupted-attempt"},
                  .number = 1,
                  .state = AttemptState::Running,
              }},
          },
          TaskSnapshot{
              .node_id = WorkflowNodeId{"expired-retry"},
              .state = TaskState::RetryWaiting,
              .attempt_count = 1,
              .next_attempt_at = now,
              .attempts = {AttemptSnapshot{
                  .attempt_id = AttemptId{"expired-attempt"},
                  .number = 1,
                  .state = AttemptState::Failed,
                  .failure = restart_failure,
              }},
          },
          TaskSnapshot{
              .node_id = WorkflowNodeId{"future-retry"},
              .state = TaskState::RetryWaiting,
              .attempt_count = 1,
              .next_attempt_at = now + std::chrono::minutes(1),
              .attempts = {AttemptSnapshot{
                  .attempt_id = AttemptId{"future-attempt"},
                  .number = 1,
                  .state = AttemptState::Failed,
                  .failure = restart_failure,
              }},
          },
      },
  };

  const auto resumed_preparation =
      workflow::detail::rehydrate_for_restart(resumed, now);
  EXPECT_EQ(resumed.state, RunState::Paused);
  ASSERT_EQ(resumed_preparation.finalized_attempts.size(), 1U);
  EXPECT_EQ(resumed_preparation.finalized_attempts.front(), 0U);
  EXPECT_TRUE(resumed_preparation.failed_tasks.empty());
  EXPECT_EQ(resumed.tasks[0].state, TaskState::Ready);
  EXPECT_FALSE(resumed.tasks[0].active_attempt_id.has_value());
  ASSERT_TRUE(resumed.tasks[0].attempts[0].failure.has_value());
  EXPECT_EQ(resumed.tasks[0].attempts[0].failure->code,
            "runtime_restarted");
  EXPECT_EQ(resumed.tasks[1].state, TaskState::Ready);
  EXPECT_FALSE(resumed.tasks[1].next_attempt_at.has_value());
  EXPECT_EQ(resumed.tasks[2].state, TaskState::RetryWaiting);
  EXPECT_TRUE(resumed.tasks[2].next_attempt_at.has_value());

  RunSnapshot cancelled{
      .run_id = WorkflowRunId{"cancelled-run"},
      .workflow_id = WorkflowId{"cancelled-workflow"},
      .plan_id = WorkflowPlanId{"cancelled-plan"},
      .state = RunState::Stopping,
      .stop_intent = StopIntent::Cancel,
      .stop_reason = "operator cancelled",
      .tasks = {
          TaskSnapshot{
              .node_id = WorkflowNodeId{"active"},
              .state = TaskState::Running,
              .attempt_count = 1,
              .active_attempt_id = AttemptId{"active-attempt"},
              .attempts = {AttemptSnapshot{
                  .attempt_id = AttemptId{"active-attempt"},
                  .number = 1,
                  .state = AttemptState::Terminating,
                  .termination_reason = TerminationReason::RunCancelled,
              }},
          },
          TaskSnapshot{
              .node_id = WorkflowNodeId{"pending"},
              .state = TaskState::Pending,
          },
      },
  };
  const auto cancelled_preparation =
      workflow::detail::rehydrate_for_restart(cancelled, now);
  ASSERT_EQ(cancelled_preparation.finalized_attempts.size(), 1U);
  EXPECT_TRUE(cancelled_preparation.failed_tasks.empty());
  for (const auto &task : cancelled.tasks) {
    EXPECT_EQ(task.state, TaskState::Cancelled);
    ASSERT_TRUE(task.failure.has_value());
    EXPECT_EQ(task.failure->kind, Error::Cancelled);
  }
  EXPECT_EQ(cancelled.tasks[0].attempts[0].state,
            AttemptState::Cancelled);
  EXPECT_EQ(cancelled.tasks[0].attempts[0].termination_reason,
            TerminationReason::RunCancelled);

  RunSnapshot failed{
      .run_id = WorkflowRunId{"failed-run"},
      .workflow_id = WorkflowId{"failed-workflow"},
      .plan_id = WorkflowPlanId{"failed-plan"},
      .state = RunState::Stopping,
      .stop_intent = StopIntent::Fail,
      .tasks = {
          TaskSnapshot{
              .node_id = WorkflowNodeId{"running"},
              .state = TaskState::Running,
              .attempt_count = 1,
              .active_attempt_id = AttemptId{"running-attempt"},
              .attempts = {AttemptSnapshot{
                  .attempt_id = AttemptId{"running-attempt"},
                  .number = 1,
                  .state = AttemptState::Running,
              }},
          },
          TaskSnapshot{
              .node_id = WorkflowNodeId{"ready"},
              .state = TaskState::Ready,
          },
      },
      .failure = restart_failure,
  };
  const auto failed_preparation =
      workflow::detail::rehydrate_for_restart(failed, now);
  ASSERT_EQ(failed_preparation.finalized_attempts.size(), 1U);
  ASSERT_EQ(failed_preparation.failed_tasks.size(), 2U);
  for (const auto &task : failed.tasks) {
    EXPECT_EQ(task.state, TaskState::Failed);
    ASSERT_TRUE(task.failure.has_value());
    EXPECT_EQ(task.failure->code, restart_failure.code);
  }
  EXPECT_EQ(failed.tasks[0].attempts[0].state, AttemptState::Failed);
  EXPECT_EQ(failed.tasks[0].attempts[0].termination_reason,
            TerminationReason::RunFailed);
}

TEST(WorkflowPrimitiveTest, NormalizesAsciiWithoutLocaleDependence) {
  EXPECT_EQ(util::ascii_lowercase("HeAdEr-123"), "header-123");
  EXPECT_EQ(util::ascii_uppercase("token_name"), "TOKEN_NAME");
  EXPECT_TRUE(util::ascii_is_alnum('Z'));
  EXPECT_TRUE(util::ascii_is_alnum('7'));
  EXPECT_FALSE(util::ascii_is_alnum('-'));
}

TEST(WorkflowPrimitiveTest, ComputesKnownSha256Digest) {
  auto digest = workflow::detail::sha256_hex("abc");
  ASSERT_TRUE(digest.has_value()) << digest.error().message();
  EXPECT_EQ(*digest,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
}

TEST(WorkflowRetryPolicyTest, StopsPermanentFailuresAndExhaustedBudgets) {
  NodePlan node{
      .node_id = WorkflowNodeId{"task"},
      .max_retries = 3,
      .retry_initial_delay = std::chrono::milliseconds(100),
      .retry_max_delay = std::chrono::milliseconds(800),
  };
  const WorkflowRunId run_id{"retry-policy__run"};

  constexpr std::array permanent_failures{
      Error::Success,          Error::FileNotFound,    Error::ParseError,
      Error::InvalidArgument,  Error::NotFound,        Error::AlreadyExists,
      Error::Cancelled,        Error::CycleDetected,   Error::ReadOnly,
      Error::HasDependents,    Error::HasActiveRuns,   Error::InvalidUrl,
      Error::ResourceExhausted, Error::InvalidState,   Error::Incomplete,
      Error::ProtocolError,    Error::Unauthorized,    Error::Unsupported,
  };
  for (const auto failure : permanent_failures) {
    EXPECT_FALSE(workflow::detail::next_retry_delay(
        node, failure, 1, run_id, node.node_id));
  }
  EXPECT_FALSE(workflow::detail::next_retry_delay(
      node, Error::Unknown, 0, run_id, node.node_id));
  EXPECT_FALSE(workflow::detail::next_retry_delay(
      node, Error::Unknown, 4, run_id, node.node_id));

  constexpr std::array retryable_failures{
      Error::FileOpenFailed,      Error::DatabaseError,
      Error::DatabaseOpenFailed,  Error::DatabaseQueryFailed,
      Error::Timeout,             Error::SystemNotRunning,
      Error::QueueFull,           Error::ProcessForkFailed,
      Error::RateLimited,         Error::PersistenceError,
      Error::Unknown,
  };
  for (const auto failure : retryable_failures) {
    EXPECT_TRUE(workflow::detail::next_retry_delay(
                    node, failure, 1, run_id, node.node_id)
                    .has_value());
  }
}

TEST(WorkflowRetryPolicyTest, AppliesDeterministicFullJitterWithinSaturatedCap) {
  NodePlan node{
      .node_id = WorkflowNodeId{"task"},
      .max_retries = 100,
      .retry_initial_delay = std::chrono::milliseconds(100),
      .retry_max_delay = std::chrono::milliseconds(800),
  };

  const WorkflowRunId stable_run{"retry-policy__stable"};
  const auto first = workflow::detail::next_retry_delay(
      node, Error::Timeout, 1, stable_run, node.node_id);
  const auto repeated = workflow::detail::next_retry_delay(
      node, Error::Timeout, 1, stable_run, node.node_id);
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(first, repeated);
  EXPECT_GE(*first, std::chrono::milliseconds::zero());
  EXPECT_LE(*first, std::chrono::milliseconds(100));

  std::unordered_set<std::int64_t> delays;
  for (std::uint32_t index = 0; index < 64; ++index) {
    const auto delay = workflow::detail::next_retry_delay(
        node, Error::RateLimited, 1,
        WorkflowRunId{std::format("retry-policy__{}", index)}, node.node_id);
    ASSERT_TRUE(delay.has_value());
    EXPECT_GE(*delay, std::chrono::milliseconds::zero());
    EXPECT_LE(*delay, std::chrono::milliseconds(100));
    delays.emplace(delay->count());
  }
  EXPECT_GT(delays.size(), 1U);

  const auto saturated = workflow::detail::next_retry_delay(
      node, Error::PersistenceError, 100, stable_run, node.node_id);
  ASSERT_TRUE(saturated.has_value());
  EXPECT_GE(*saturated, std::chrono::milliseconds::zero());
  EXPECT_LE(*saturated, node.retry_max_delay);
}

TEST(WorkflowControlPlaneTest, DeduplicatesPlansByDigest) {
  TestExecutorEnvironment environment;
  AdmissionConfig admission;
  admission.allowed_executors = {"test"};
  WorkflowControlPlane control{environment.registry, PlanValidator{admission}};
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
  WorkflowControlPlane control{environment.registry, PlanValidator{admission}};

  auto first_plan = base_plan("config-order");
  JsonValue first_config = JsonValue::object_t{};
  first_config["alpha"] = 1;
  first_config["nested"] = JsonValue::object_t{};
  first_config["nested"]["left"] = true;
  first_config["nested"]["right"] = "value";
  first_plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"task"},
      .executor = "test",
      .config = make_payload(first_config),
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
      .config = make_payload(second_config),
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
  WorkflowControlPlane control{environment.registry, PlanValidator{admission}};

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

TEST(WorkflowControlPlaneTest, CanonicalDigestIgnoresCollectionOrder) {
  WorkflowPlan plan;
  plan.workflow_id = WorkflowId{"canonical-order"};
  plan.nodes = {
      NodePlan{
          .node_id = WorkflowNodeId{"second"},
          .executor = "test",
          .inputs = {
              InputBinding{
                  .input = WorkflowPortId{"beta"},
                  .source = OutputRef{.node_id = WorkflowNodeId{"first"},
                                      .port = WorkflowPortId{"out2"}},
              },
              InputBinding{
                  .input = WorkflowPortId{"alpha"},
                  .source = OutputRef{.node_id = WorkflowNodeId{"first"},
                                      .port = WorkflowPortId{"out1"}},
              },
          },
          .outputs = {WorkflowPortId{"z"}, WorkflowPortId{"a"}},
      },
      NodePlan{
          .node_id = WorkflowNodeId{"first"},
          .executor = "test",
          .outputs = {WorkflowPortId{"out2"}, WorkflowPortId{"out1"}},
      },
  };
  plan.edges = {
      ConditionalEdge{
          .source = OutputRef{.node_id = WorkflowNodeId{"first"},
                              .port = WorkflowPortId{"out2"}},
          .target = WorkflowNodeId{"second"},
          .condition = ConditionExpr{
              .kind = ConditionKind::StringEquals,
              .expected_string = "go",
          },
      },
      ConditionalEdge{
          .source = OutputRef{.node_id = WorkflowNodeId{"first"},
                              .port = WorkflowPortId{"out1"}},
          .target = WorkflowNodeId{"second"},
          .condition = ConditionExpr{.kind = ConditionKind::Always},
      },
  };
  plan.outputs = {
      OutputRef{.node_id = WorkflowNodeId{"second"},
                .port = WorkflowPortId{"z"}},
      OutputRef{.node_id = WorkflowNodeId{"second"},
                .port = WorkflowPortId{"a"}},
  };

  auto reordered = plan;
  std::ranges::reverse(reordered.nodes);
  std::ranges::reverse(reordered.edges);
  std::ranges::reverse(reordered.outputs);
  for (auto &node : reordered.nodes) {
    std::ranges::reverse(node.inputs);
    std::ranges::reverse(node.outputs);
  }

  auto first = PlanCompiler::digest(plan);
  auto second = PlanCompiler::digest(reordered);
  ASSERT_TRUE(first.has_value()) << first.error().message();
  ASSERT_TRUE(second.has_value()) << second.error().message();
  EXPECT_EQ(*first, *second);
}

TEST(WorkflowControlPlaneTest, EnforcesServerPlanValidation) {
  TestExecutorEnvironment environment;
  AdmissionConfig config;
  config.allow_unlisted_executors = false;
  config.allowed_executors = {"test"};
  config.max_parallel_nodes = 32;
  WorkflowControlPlane control{environment.registry, PlanValidator{config}};

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

  auto uncompiled_start = environment.registry.start(
      "command",
      TaskExecutionRequest{
          .instance_id = InstanceId{"uncompiled-command"},
          .config = CompiledExecutorConfig::from_encoded(
              command_config("echo")),
          .outputs = outputs,
      },
      {});
  ASSERT_FALSE(uncompiled_start.has_value());
  EXPECT_EQ(uncompiled_start.error(), make_error_code(Error::InvalidState));

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

  const std::vector unsupported_outputs{WorkflowPortId{"unknown"}};
  auto unsupported_output = environment.registry.compile(
      "command", command_config("echo"),
      ExecutorCompileContext{.inputs = inputs,
                             .outputs = unsupported_outputs});
  ASSERT_FALSE(unsupported_output.has_value());
  EXPECT_EQ(unsupported_output.error(),
            make_error_code(Error::InvalidArgument));

  auto sandbox_override = command_config("echo");
  auto sandbox_override_json = materialize(sandbox_override);
  sandbox_override_json["network"] = true;
  auto blocked_override = environment.registry.compile(
      "command", make_payload(sandbox_override_json), context);
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
      {"json", std::make_shared<const WorkflowValue>(make_payload(object))},
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
                                    JsonPayload{})},
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
  const auto command_details = materialize(command_failure.details);
  EXPECT_EQ(command_details["exit_code"].as<std::int64_t>(), 7);
  EXPECT_EQ(command_details["stdout"].as<std::string>(),
            "partial output");
  EXPECT_EQ(command_details["stderr"].as<std::string>(),
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
                                        {"json", std::make_shared<const WorkflowValue>(JsonPayload{})},
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

  auto normalized = make_execution_failure(
      Error::Success, {}, {}, parse_payload("[]"));
  EXPECT_EQ(normalized.kind, Error::Unknown);
  EXPECT_EQ(normalized.code, "unknown");
  EXPECT_EQ(normalized.message, make_error_code(Error::Unknown).message());
  EXPECT_TRUE(normalized.details.valid());

  const auto cause = std::make_error_code(std::errc::permission_denied);
  auto failure = make_execution_failure(cause, "permission_denied", {});
  EXPECT_EQ(failure.kind, Error::Unauthorized);
  EXPECT_EQ(failure.code, "permission_denied");
  EXPECT_EQ(failure.message, cause.message());
  const auto failure_details = materialize(failure.details);
  const auto &cause_json = failure_details["cause"];
  ASSERT_TRUE(cause_json.is_object());
  EXPECT_EQ(cause_json["category"].as<std::string>(),
            cause.category().name());
  EXPECT_EQ(cause_json["value"].as<std::int64_t>(), cause.value());
  EXPECT_EQ(cause_json["message"].as<std::string>(), cause.message());

  auto projected = serialize_json(failure);
  ASSERT_TRUE(projected.has_value()) << projected.error().message();
  auto projected_json = parse_json(*projected);
  ASSERT_TRUE(projected_json.has_value()) << projected_json.error().message();
  EXPECT_EQ((*projected_json)["kind"].as<std::string>(), "unauthorized");
  EXPECT_EQ((*projected_json)["code"].as<std::string>(),
            "permission_denied");
  EXPECT_EQ((*projected_json)["message"].as<std::string>(), cause.message());
  EXPECT_TRUE((*projected_json)["details"].is_object());
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
  auto checkpoints = std::make_shared<CheckpointStore>();
  WorkflowRuntime runtime(core, environment.registry, {}, {}, checkpoints);

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
  auto durable_paused = checkpoints->load(*started);
  ASSERT_TRUE(durable_paused.has_value())
      << durable_paused.error().message();
  EXPECT_EQ(durable_paused->snapshot.state, RunState::Paused);
  EXPECT_EQ(durable_paused->snapshot.tasks[0].state, TaskState::Succeeded);
  EXPECT_EQ(durable_paused->snapshot.tasks[1].state, TaskState::Ready);

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

TEST(WorkflowRuntimeTest, PersistsOnlyRecoveryBoundaries) {
  Runtime core(1, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);
  auto checkpoints = std::make_shared<CheckpointStore>();
  WorkflowRuntime runtime(core, environment.registry, {}, {}, checkpoints);

  auto plan = base_plan("sparse-checkpoint");
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
      *compiled,
      TriggerEnvelope{.workflow_id = WorkflowId{"sparse-checkpoint"}});
  ASSERT_TRUE(started.has_value()) << started.error().message();
  ASSERT_TRUE(environment.executor->wait_for_pending(1));
  ASSERT_TRUE(environment.executor->complete_next(0, "first"));
  ASSERT_TRUE(environment.executor->wait_for_pending(1));

  auto initial = checkpoints->load(*started);
  ASSERT_TRUE(initial.has_value()) << initial.error().message();
  ASSERT_EQ(initial->snapshot.tasks.size(), 2U);
  EXPECT_EQ(initial->snapshot.tasks[0].state, TaskState::Pending);
  EXPECT_EQ(initial->snapshot.tasks[1].state, TaskState::Pending);
  EXPECT_TRUE(initial->values.empty());

  ASSERT_TRUE(environment.executor->complete_next(0, "second"));
  ASSERT_TRUE(wait_for_state(runtime, core, *started, RunState::Succeeded)
                  .has_value());
  auto terminal = checkpoints->load(*started);
  ASSERT_TRUE(terminal.has_value()) << terminal.error().message();
  EXPECT_EQ(terminal->snapshot.state, RunState::Succeeded);
  ASSERT_EQ(terminal->values.size(), 2U);

  core.stop();
}

TEST(WorkflowRuntimeTest, ExplicitNodeCheckpointPersistsCompletedPrefix) {
  Runtime core(1, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);
  auto checkpoints = std::make_shared<CheckpointStore>();
  WorkflowRuntime runtime(core, environment.registry, {}, {}, checkpoints);

  auto plan = base_plan("explicit-checkpoint");
  plan.nodes = {
      NodePlan{
          .node_id = WorkflowNodeId{"first"},
          .executor = "test",
          .outputs = {WorkflowPortId{"result"}},
          .checkpoint = true,
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
      *compiled,
      TriggerEnvelope{.workflow_id = WorkflowId{"explicit-checkpoint"}});
  ASSERT_TRUE(started.has_value()) << started.error().message();
  ASSERT_TRUE(environment.executor->wait_for_pending(1));
  ASSERT_TRUE(environment.executor->complete_next(0, "first"));
  ASSERT_TRUE(environment.executor->wait_for_pending(1));

  auto durable = checkpoints->load(*started);
  ASSERT_TRUE(durable.has_value()) << durable.error().message();
  ASSERT_EQ(durable->snapshot.tasks.size(), 2U);
  EXPECT_EQ(durable->snapshot.tasks[0].state, TaskState::Succeeded);
  EXPECT_EQ(durable->snapshot.tasks[1].state, TaskState::Pending);
  ASSERT_EQ(durable->values.size(), 1U);
  EXPECT_EQ(durable->values.front().output.node_id, WorkflowNodeId{"first"});

  ASSERT_TRUE(environment.executor->complete_next(0, "second"));
  ASSERT_TRUE(wait_for_state(runtime, core, *started, RunState::Succeeded)
                  .has_value());
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
  auto checkpoints = std::make_shared<CheckpointStore>();
  WorkflowRuntime runtime(core, environment.registry, {}, {}, checkpoints);

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
  auto durable_stopping = checkpoints->load(*started);
  ASSERT_TRUE(durable_stopping.has_value())
      << durable_stopping.error().message();
  EXPECT_EQ(durable_stopping->snapshot.state, RunState::Stopping);
  EXPECT_EQ(durable_stopping->snapshot.stop_intent, StopIntent::Cancel);
  EXPECT_EQ(durable_stopping->snapshot.tasks[0].attempts[0].state,
            AttemptState::Terminating);

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

TEST(WorkflowRuntimeTest, DestructionWaitsForQueuedRunActivation) {
  Runtime core(1, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);

  auto plan = base_plan("queued-activation-destruction");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  auto compiled = PlanCompiler{environment.registry}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value());

  auto blocker_entered = std::make_shared<std::promise<void>>();
  auto blocker_entered_future = blocker_entered->get_future();
  auto release_blocker = std::make_shared<std::promise<void>>();
  auto release_signal = release_blocker->get_future().share();
  core.post_to(0, [blocker_entered, release_signal] {
    blocker_entered->set_value();
    release_signal.wait();
  });
  if (blocker_entered_future.wait_for(std::chrono::seconds(2)) !=
      std::future_status::ready) {
    release_blocker->set_value();
    core.stop();
    FAIL() << "owner shard blocker did not start";
  }

  auto runtime =
      std::make_unique<WorkflowRuntime>(core, environment.registry);
  auto started = runtime->start(
      *compiled,
      TriggerEnvelope{
          .workflow_id = WorkflowId{"queued-activation-destruction"},
          .source = "test",
          .event_type = "request",
      });
  if (!started) {
    release_blocker->set_value();
    runtime.reset();
    core.stop();
    FAIL() << started.error().message();
  }

  auto destroyed = std::make_shared<std::promise<void>>();
  auto destroyed_future = destroyed->get_future();
  std::jthread destroyer([&runtime, destroyed] {
    runtime.reset();
    destroyed->set_value();
  });
  EXPECT_EQ(destroyed_future.wait_for(std::chrono::milliseconds(25)),
            std::future_status::timeout);

  release_blocker->set_value();
  EXPECT_EQ(destroyed_future.wait_for(std::chrono::seconds(2)),
            std::future_status::ready);
  destroyer.join();
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
  auto checkpoints = std::make_shared<CheckpointStore>();
  WorkflowRuntime runtime(core, environment.registry, {}, {}, checkpoints);

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

  ASSERT_TRUE(dagforge::test::wait_until(
      [&] {
        auto persisted = checkpoints->load(*started);
        return persisted && persisted->snapshot.tasks[0].state ==
                                TaskState::RetryWaiting;
      },
      std::chrono::seconds(2)));
  auto durable_retry = checkpoints->load(*started);
  ASSERT_TRUE(durable_retry.has_value()) << durable_retry.error().message();
  EXPECT_EQ(durable_retry->snapshot.tasks[0].state,
            TaskState::RetryWaiting);
  EXPECT_TRUE(durable_retry->snapshot.tasks[0].next_attempt_at.has_value());

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
  ASSERT_TRUE((*completed)->tasks[0].attempts[0].failure.has_value());
  EXPECT_EQ((*completed)->tasks[0].attempts[0].failure->kind, Error::Unknown);
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
  ASSERT_TRUE((*failed)->tasks[0].attempts[0].failure.has_value());
  EXPECT_EQ((*failed)->tasks[0].attempts[0].failure->kind,
            Error::Unsupported);
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
      .outputs = {WorkflowPortId{"partial"}, WorkflowPortId{"stdout"}},
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
  ASSERT_TRUE(environment.executor->complete_next_with_outputs(
      {{WorkflowPortId{"partial"}, std::string{"ok"}},
       {WorkflowPortId{"stdout"}, std::string{"too-large"}}}));
  auto failed = wait_for_state(runtime, core, *started, RunState::Failed);
  ASSERT_TRUE(failed.has_value()) << failed.error().message();
  EXPECT_EQ((*failed)->tasks[0].attempt_count, 1U);
  EXPECT_EQ((*failed)->tasks[0].attempts[0].state, AttemptState::Failed);
  ASSERT_TRUE((*failed)->tasks[0].attempts[0].failure.has_value());
  EXPECT_EQ((*failed)->tasks[0].attempts[0].failure->kind,
            Error::ResourceExhausted);
  auto partial = sync_wait_on_runtime(
      core, runtime.output(
                *started,
                OutputRef{.node_id = WorkflowNodeId{"command"},
                          .port = WorkflowPortId{"partial"}}));
  EXPECT_EQ(partial.error(), make_error_code(Error::NotFound));
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

  auto revised_plan = base_plan("idempotent");
  revised_plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .executor = "test",
      .config = make_payload(JsonValue{{"revision", 2}}),
      .outputs = {WorkflowPortId{"result"}}});
  auto revised =
      PlanCompiler{environment.registry}.compile(std::move(revised_plan));
  ASSERT_TRUE(revised.has_value()) << revised.error().message();
  EXPECT_EQ(runtime
                .start(*revised,
                       TriggerEnvelope{
                           .workflow_id = WorkflowId{"idempotent"},
                           .idempotency_key = "same-request"})
                .error(),
            make_error_code(Error::AlreadyExists));

  auto other_plan = base_plan("other-workflow");
  other_plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"command"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}}});
  auto other =
      PlanCompiler{environment.registry}.compile(std::move(other_plan));
  ASSERT_TRUE(other.has_value()) << other.error().message();
  EXPECT_EQ(runtime
                .start(*other,
                       TriggerEnvelope{
                           .workflow_id = WorkflowId{"other-workflow"},
                           .idempotency_key = "same-request"})
                .error(),
            make_error_code(Error::AlreadyExists));

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

TEST(WorkflowRuntimeTest, PropagatesPrincipalAndTraceToExecutor) {
  Runtime core(2, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);
  WorkflowRuntime runtime(core, environment.registry);

  auto plan = base_plan("executor-context");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"task"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  auto compiled = PlanCompiler{environment.registry}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  auto started = runtime.start(
      *compiled,
      TriggerEnvelope{
          .workflow_id = WorkflowId{"executor-context"},
          .source = "api",
          .event_type = "request",
          .principal = Principal{.subject = "user-42",
                                 .roles = {"planner", "operator"}},
          .trace = TraceContext{.trace_id = "trace-123",
                                .parent_span_id = "span-456"},
      });
  ASSERT_TRUE(started.has_value()) << started.error().message();
  ASSERT_TRUE(environment.executor->wait_for_pending(1));
  const auto context = environment.executor->next_context();
  ASSERT_TRUE(context.has_value());
  EXPECT_EQ(context->first.subject, "user-42");
  EXPECT_EQ(context->first.roles,
            (std::vector<std::string>{"planner", "operator"}));
  EXPECT_EQ(context->second.trace_id, "trace-123");
  EXPECT_EQ(context->second.parent_span_id, "span-456");
  ASSERT_TRUE(environment.executor->complete_next(0, "done"));
  ASSERT_TRUE(wait_for_state(runtime, core, *started, RunState::Succeeded)
                  .has_value());
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
  run_case(parse_payload("null"),
           ConditionExpr{.kind = ConditionKind::BoolEquals,
                         .expected_bool = false},
           true);
  JsonValue object = JsonValue::object_t{};
  object["ready"] = true;
  run_case(make_payload(object),
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
  auto json_text = parse_payload(R"("ready")");
  run_case(json_text,
           ConditionExpr{.kind = ConditionKind::StringEquals,
                         .expected_string = std::string{json_text.encoded()}},
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

TEST(WorkflowRuntimeTest, MapsExecutorStartFailuresToAttemptOutcome) {
  Runtime core(2, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);
  WorkflowRuntime runtime(core, environment.registry);

  struct FailureCase {
    Error error;
    AttemptState attempt_state{AttemptState::Failed};
    RunState run_state{RunState::Failed};
  };
  const std::array cases{
      FailureCase{Error::Cancelled, AttemptState::Cancelled,
                  RunState::Cancelled},
      FailureCase{Error::Timeout, AttemptState::TimedOut},
      FailureCase{Error::InvalidArgument},
      FailureCase{Error::ParseError},
      FailureCase{Error::FileNotFound},
      FailureCase{Error::NotFound},
      FailureCase{Error::AlreadyExists},
      FailureCase{Error::InvalidUrl},
      FailureCase{Error::ProtocolError},
      FailureCase{Error::Unauthorized},
      FailureCase{Error::Unsupported},
      FailureCase{Error::InvalidState},
      FailureCase{Error::ResourceExhausted},
      FailureCase{Error::SystemNotRunning},
      FailureCase{Error::QueueFull},
      FailureCase{Error::ProcessForkFailed},
      FailureCase{Error::Unknown},
  };

  std::size_t index = 0;
  for (const auto &failure : cases) {
    environment.executor->fail_start(failure.error);
    const auto workflow_name = std::format("attempt-outcome-{}", index++);
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
    const auto &attempt = (*failed)->tasks.front().attempts.front();
    EXPECT_EQ(attempt.state, failure.attempt_state)
        << static_cast<int>(failure.error);
    ASSERT_TRUE(attempt.failure.has_value());
    EXPECT_EQ(attempt.failure->kind, failure.error);
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
    checkpoint.plan.workflow_id = plan->workflow_id.clone();
    checkpoint.plan.edges = plan->edges;
    checkpoint.plan.outputs = plan->outputs;
    checkpoint.plan.policy = plan->policy;
    for (const auto &node : plan->nodes) {
      checkpoint.plan.nodes.push_back(node.plan);
      checkpoint.snapshot.tasks.push_back(TaskSnapshot{
          .node_id = node.plan.node_id.clone(),
          .state = TaskState::Succeeded,
      });
    }
    checkpoint.trigger.trigger_id = generate_workflow_trigger_id();
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

TEST(WorkflowRuntimeTest, RestoreRejectsDuplicateRunsAndConflictingKeys) {
  Runtime core(1, false, 0);
  TestExecutorEnvironment environment(core);
  WorkflowRuntime runtime(core, environment.registry);

  auto plan = base_plan("restore-idempotency-conflict");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"task"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  auto compiled = PlanCompiler{environment.registry}.compile(plan);
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  const auto checkpoint_for = [&](std::string run_id) {
    return WorkflowCheckpoint{
        .plan = plan,
        .trigger = TriggerEnvelope{
            .trigger_id = WorkflowTriggerId{run_id + "-trigger"},
            .workflow_id = WorkflowId{"restore-idempotency-conflict"},
            .source = "test",
            .event_type = "restore",
            .idempotency_key = "restored-request",
        },
        .snapshot = RunSnapshot{
            .run_id = WorkflowRunId{std::move(run_id)},
            .workflow_id = WorkflowId{"restore-idempotency-conflict"},
            .plan_id = (*compiled)->plan_id.clone(),
            .state = RunState::Succeeded,
            .tasks = {TaskSnapshot{.node_id = WorkflowNodeId{"task"},
                                   .state = TaskState::Succeeded}},
        },
    };
  };

  auto first = checkpoint_for("restore-first-record");
  ASSERT_TRUE(runtime.restore(*compiled, first).has_value());
  EXPECT_EQ(runtime.restore(*compiled, first).error(),
            make_error_code(Error::AlreadyExists));

  auto conflicting = checkpoint_for("restore-second-record");
  EXPECT_EQ(runtime.restore(*compiled, std::move(conflicting)).error(),
            make_error_code(Error::AlreadyExists));
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
  FileArtifactStore writer(directory,
                           kStorageDefaults.max_artifact_metadata_bytes,
                           kStorageDefaults.max_artifact_bytes);
  auto stored = writer.put(data, "application/octet-stream");
  ASSERT_TRUE(stored.has_value()) << stored.error().message();

  FileArtifactStore reader(directory,
                           kStorageDefaults.max_artifact_metadata_bytes,
                           kStorageDefaults.max_artifact_bytes);
  auto loaded = reader.get(stored->artifact_id);
  ASSERT_TRUE(loaded.has_value()) << loaded.error().message();
  EXPECT_EQ(loaded->ref.digest, stored->digest);
  EXPECT_EQ(loaded->ref.media_type, "application/octet-stream");
  ASSERT_EQ(loaded->data.size(), data.size());
  EXPECT_TRUE(std::ranges::equal(loaded->data, data));
  EXPECT_TRUE(reader.erase(stored->artifact_id).has_value());

  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowStorageTest, DurableFileReplacesAppendsAndRemoves) {
  const auto directory = temporary_test_directory("durable-file");
  const auto target = directory / "nested" / "state.json";
  std::error_code error;
  std::filesystem::remove_all(directory, error);

  ASSERT_TRUE(
      workflow::storage_detail::store_text_file_atomic(target, "first")
          .has_value());
  auto first = workflow::storage_detail::load_text_file(target, 1024);
  ASSERT_TRUE(first.has_value()) << first.error().message();
  EXPECT_EQ(*first, "first");

  ASSERT_TRUE(
      workflow::storage_detail::store_text_file_atomic(target, "second")
          .has_value());
  ASSERT_TRUE(workflow::storage_detail::append_text_file_durable(
                  target, "\nthird", 1024)
                  .has_value());
  auto replaced = workflow::storage_detail::load_text_file(target, 1024);
  ASSERT_TRUE(replaced.has_value()) << replaced.error().message();
  EXPECT_EQ(*replaced, "second\nthird");

  for (const auto &entry :
       std::filesystem::directory_iterator(target.parent_path())) {
    EXPECT_FALSE(entry.path().filename().string().starts_with(
        "state.json.tmp."));
  }

  auto removed = workflow::storage_detail::remove_file_durable(target);
  ASSERT_TRUE(removed.has_value()) << removed.error().message();
  EXPECT_TRUE(removed->removed);
  EXPECT_TRUE(removed->durability_confirmed());
  auto missing = workflow::storage_detail::remove_file_durable(target);
  ASSERT_TRUE(missing.has_value()) << missing.error().message();
  EXPECT_FALSE(missing->removed);
  EXPECT_TRUE(missing->durability_confirmed());

  std::filesystem::create_directory(target, error);
  ASSERT_FALSE(error);
  EXPECT_FALSE(
      workflow::storage_detail::store_text_file_atomic(target, "blocked")
          .has_value());
  EXPECT_TRUE(std::filesystem::is_directory(target));
  for (const auto &entry :
       std::filesystem::directory_iterator(target.parent_path())) {
    EXPECT_FALSE(entry.path().filename().string().starts_with(
        "state.json.tmp."));
  }

  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowStorageTest, DurableRemoveReportsPostUnlinkSyncFailure) {
  const auto directory = temporary_test_directory("durable-remove-sync");
  const auto target = directory / "state.json";
  std::error_code error;
  std::filesystem::remove_all(directory, error);
  ASSERT_TRUE(
      workflow::storage_detail::store_text_file_atomic(target, "state")
          .has_value());

  workflow::storage_detail::testing::fail_next_directory_sync();
  auto removed = workflow::storage_detail::remove_file_durable(target);
  ASSERT_TRUE(removed.has_value()) << removed.error().message();
  EXPECT_TRUE(removed->removed);
  EXPECT_FALSE(removed->durability_confirmed());
  EXPECT_EQ(removed->durability_error,
            make_error_code(Error::PersistenceError));
  EXPECT_FALSE(std::filesystem::exists(target));

  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowStorageTest, DurableWriteReportsPostRenameSyncFailure) {
  const auto directory = temporary_test_directory("durable-write-sync");
  const auto target = directory / "state.json";
  std::error_code error;
  std::filesystem::remove_all(directory, error);
  std::filesystem::create_directories(directory, error);
  ASSERT_FALSE(error);

  workflow::storage_detail::testing::fail_next_directory_sync();
  auto written =
      workflow::storage_detail::store_text_file_atomic(target, "committed");
  ASSERT_TRUE(written.has_value()) << written.error().message();
  EXPECT_TRUE(written->committed);
  EXPECT_FALSE(written->durability_confirmed());
  EXPECT_EQ(written->durability_error,
            make_error_code(Error::PersistenceError));
  auto visible = workflow::storage_detail::load_text_file(target, 1024);
  ASSERT_TRUE(visible.has_value()) << visible.error().message();
  EXPECT_EQ(*visible, "committed");

  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowStorageTest, DurableFileRejectsUnsafeAndBlockedPaths) {
  const auto directory = temporary_test_directory("durable-file-errors");
  std::error_code error;
  std::filesystem::remove_all(directory, error);
  std::filesystem::create_directories(directory, error);
  ASSERT_FALSE(error);

  EXPECT_EQ(workflow::storage_detail::load_file(directory, 1024).error(),
            make_error_code(Error::InvalidState));
  EXPECT_EQ(
      workflow::storage_detail::append_text_file_durable(directory, "x", 1024)
          .error(),
      make_error_code(Error::InvalidState));

  const auto parent_file = directory / "parent-file";
  {
    std::ofstream output(parent_file);
    output << "not-a-directory";
  }
  EXPECT_EQ(workflow::storage_detail::store_text_file_atomic(
                parent_file / "child", "x")
                .error(),
            make_error_code(Error::InvalidState));
  EXPECT_FALSE(
      workflow::storage_detail::remove_file_durable(parent_file / "child")
          .has_value());

  const auto read_only = directory / "read-only";
  std::filesystem::create_directory(read_only, error);
  ASSERT_FALSE(error);
  std::filesystem::permissions(
      read_only,
      std::filesystem::perms::owner_read |
          std::filesystem::perms::owner_exec,
      std::filesystem::perm_options::replace, error);
  ASSERT_FALSE(error);
  EXPECT_FALSE(workflow::storage_detail::store_text_file_atomic(
                   read_only / "state.json", "blocked")
                   .has_value());
  std::filesystem::permissions(read_only, std::filesystem::perms::owner_all,
                               std::filesystem::perm_options::replace, error);
  ASSERT_FALSE(error);

  const auto target = directory / "target";
  ASSERT_TRUE(
      workflow::storage_detail::store_text_file_atomic(target, "original")
          .has_value());
  const auto link = directory / "target-link";
  std::filesystem::create_symlink(target, link, error);
  ASSERT_FALSE(error);
  EXPECT_FALSE(
      workflow::storage_detail::load_text_file(link, 1024).has_value());
  EXPECT_EQ(
      workflow::storage_detail::append_text_file_durable(link, "blocked", 1024)
          .error(),
      make_error_code(Error::InvalidState));

  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowStorageTest, DurableFileEnforcesByteLimitsBeforeAllocation) {
  const auto directory = temporary_test_directory("durable-file-limits");
  const auto target = directory / "state.bin";
  std::error_code error;
  std::filesystem::remove_all(directory, error);
  ASSERT_TRUE(workflow::storage_detail::store_text_file_atomic(
                  target, "12345678")
                  .has_value());

  EXPECT_EQ(workflow::storage_detail::load_text_file(target, 4).error(),
            make_error_code(Error::ResourceExhausted));
  EXPECT_EQ(workflow::storage_detail::load_file(target, 4).error(),
            make_error_code(Error::ResourceExhausted));
  EXPECT_EQ(workflow::storage_detail::append_text_file_durable(
                target, "9", 8)
                .error(),
            make_error_code(Error::ResourceExhausted));
  auto unchanged = workflow::storage_detail::load_text_file(target, 8);
  ASSERT_TRUE(unchanged.has_value()) << unchanged.error().message();
  EXPECT_EQ(*unchanged, "12345678");
  EXPECT_EQ(workflow::storage_detail::load_text_file(target, 0).error(),
            make_error_code(Error::InvalidArgument));

  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowStorageTest, JsonCatalogRejectsManagedPathCorruption) {
  const auto directory = temporary_test_directory("json-catalog-errors");
  std::error_code error;
  std::filesystem::remove_all(directory, error);

  auto missing = workflow::storage_detail::load_json_catalog(directory, 1024);
  ASSERT_TRUE(missing.has_value()) << missing.error().message();
  EXPECT_TRUE(missing->empty());

  {
    std::ofstream root_file(directory);
    root_file << "not-a-directory";
  }
  EXPECT_EQ(workflow::storage_detail::load_json_catalog(directory, 1024)
                .error(),
            make_error_code(Error::InvalidState));
  std::filesystem::remove(directory, error);
  ASSERT_FALSE(error);

  std::filesystem::create_directories(directory, error);
  ASSERT_FALSE(error);
  {
    std::ofstream ignored(directory / "ignored.txt");
    ignored << "ignored";
  }
  std::filesystem::create_directory(directory / "managed.json", error);
  ASSERT_FALSE(error);
  EXPECT_EQ(workflow::storage_detail::load_json_catalog(directory, 1024)
                .error(),
            make_error_code(Error::InvalidState));
  std::filesystem::remove_all(directory / "managed.json", error);
  ASSERT_FALSE(error);
  {
    std::ofstream invalid_key(directory / "..json");
    invalid_key << "{}";
  }
  EXPECT_EQ(workflow::storage_detail::load_json_catalog(directory, 1024)
                .error(),
            make_error_code(Error::ParseError));

  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowStorageTest, StorageCodecRejectsInvalidModels) {
  EXPECT_EQ(workflow::storage_detail::encode_artifact_metadata(ArtifactRef{})
                .error(),
            make_error_code(Error::InvalidArgument));
  EXPECT_EQ(workflow::storage_detail::decode_artifact_metadata("{}")
                .error(),
            make_error_code(Error::ParseError));
  EXPECT_EQ(workflow::storage_detail::encode_evidence(EvidenceRecord{})
                .error(),
            make_error_code(Error::InvalidArgument));
  EXPECT_EQ(workflow::storage_detail::decode_evidence("{}")
                .error(),
            make_error_code(Error::ParseError));

  WorkflowCheckpoint checkpoint;
  EXPECT_EQ(workflow::storage_detail::encode_checkpoint(checkpoint).error(),
            make_error_code(Error::InvalidArgument));
  EXPECT_EQ(workflow::storage_detail::decode_checkpoint("{}")
                .error(),
            make_error_code(Error::ParseError));
}

TEST(WorkflowStorageTest, StorageEnvelopeGoldenFilesRequireCurrentVersion) {
  const auto artifact_v1 =
      storage_fixture("artifact-metadata-envelope-v1.json");
  auto artifact =
      workflow::storage_detail::decode_artifact_metadata(artifact_v1);
  ASSERT_TRUE(artifact.has_value()) << artifact.error().message();
  EXPECT_EQ(artifact->artifact_id, ArtifactId{"golden-artifact"});
  auto encoded_artifact =
      workflow::storage_detail::encode_artifact_metadata(*artifact);
  ASSERT_TRUE(encoded_artifact.has_value())
      << encoded_artifact.error().message();
  EXPECT_EQ(*encoded_artifact, artifact_v1);
  auto artifact_payload = storage_payload(artifact_v1);
  ASSERT_TRUE(artifact_payload.has_value())
      << artifact_payload.error().message();
  auto unversioned_artifact =
      workflow::storage_detail::decode_artifact_metadata(*artifact_payload);
  ASSERT_FALSE(unversioned_artifact.has_value());
  EXPECT_EQ(unversioned_artifact.error(), make_error_code(Error::ParseError));

  const auto evidence_v1 = storage_fixture("evidence-envelope-v1.json");
  auto evidence = workflow::storage_detail::decode_evidence(evidence_v1);
  ASSERT_TRUE(evidence.has_value()) << evidence.error().message();
  EXPECT_EQ(evidence->evidence_id, EvidenceId{"evidence-rich"});
  auto encoded_evidence = workflow::storage_detail::encode_evidence(*evidence);
  ASSERT_TRUE(encoded_evidence.has_value())
      << encoded_evidence.error().message();
  EXPECT_EQ(*encoded_evidence, evidence_v1);
  auto evidence_payload = storage_payload(evidence_v1);
  ASSERT_TRUE(evidence_payload.has_value())
      << evidence_payload.error().message();
  auto unversioned_evidence =
      workflow::storage_detail::decode_evidence(*evidence_payload);
  ASSERT_FALSE(unversioned_evidence.has_value());
  EXPECT_EQ(unversioned_evidence.error(), make_error_code(Error::ParseError));

  const auto checkpoint_v1 = storage_fixture("checkpoint-envelope-v1.json");
  auto checkpoint = workflow::storage_detail::decode_checkpoint(checkpoint_v1);
  ASSERT_TRUE(checkpoint.has_value()) << checkpoint.error().message();
  EXPECT_EQ(checkpoint->snapshot.run_id, WorkflowRunId{"rich-run"});
  auto encoded_checkpoint =
      workflow::storage_detail::encode_checkpoint(*checkpoint);
  ASSERT_TRUE(encoded_checkpoint.has_value())
      << encoded_checkpoint.error().message();
  EXPECT_EQ(*encoded_checkpoint, checkpoint_v1);
  auto checkpoint_payload = storage_payload(checkpoint_v1);
  ASSERT_TRUE(checkpoint_payload.has_value())
      << checkpoint_payload.error().message();
  auto unversioned_checkpoint =
      workflow::storage_detail::decode_checkpoint(*checkpoint_payload);
  ASSERT_FALSE(unversioned_checkpoint.has_value());
  EXPECT_EQ(unversioned_checkpoint.error(), make_error_code(Error::ParseError));

  const auto plan_v1 = storage_fixture("stored-plan-envelope-v1.json");
  auto stored = workflow::storage_detail::decode_stored_plan(plan_v1);
  ASSERT_TRUE(stored.has_value()) << stored.error().message();
  EXPECT_EQ(stored->plan_id, WorkflowPlanId{"digest-drift-plan"});
  auto encoded_plan = workflow::storage_detail::encode_stored_plan(*stored);
  ASSERT_TRUE(encoded_plan.has_value()) << encoded_plan.error().message();
  EXPECT_EQ(*encoded_plan, plan_v1);
  auto plan_payload = storage_payload(plan_v1);
  ASSERT_TRUE(plan_payload.has_value()) << plan_payload.error().message();
  auto unversioned_plan =
      workflow::storage_detail::decode_stored_plan(*plan_payload);
  ASSERT_FALSE(unversioned_plan.has_value());
  EXPECT_EQ(unversioned_plan.error(), make_error_code(Error::ParseError));

  auto artifact_future = parse_json(artifact_v1);
  ASSERT_TRUE(artifact_future.has_value()) << artifact_future.error().message();
  artifact_future->get_object()["version"] = std::int64_t{2};
  EXPECT_EQ(workflow::storage_detail::decode_artifact_metadata(
                serialize_json(*artifact_future).value())
                .error(),
            make_error_code(Error::Unsupported));

  auto evidence_future = parse_json(evidence_v1);
  ASSERT_TRUE(evidence_future.has_value()) << evidence_future.error().message();
  evidence_future->get_object()["version"] = std::int64_t{2};
  EXPECT_EQ(
      workflow::storage_detail::decode_evidence(
          serialize_json(*evidence_future).value())
          .error(),
      make_error_code(Error::Unsupported));

  auto checkpoint_future = parse_json(checkpoint_v1);
  ASSERT_TRUE(checkpoint_future.has_value())
      << checkpoint_future.error().message();
  checkpoint_future->get_object()["version"] = std::int64_t{2};
  EXPECT_EQ(
      workflow::storage_detail::decode_checkpoint(
          serialize_json(*checkpoint_future).value())
          .error(),
      make_error_code(Error::Unsupported));

  auto plan_future = parse_json(plan_v1);
  ASSERT_TRUE(plan_future.has_value()) << plan_future.error().message();
  plan_future->get_object()["version"] = std::int64_t{2};
  EXPECT_EQ(
      workflow::storage_detail::decode_stored_plan(
          serialize_json(*plan_future).value())
          .error(),
      make_error_code(Error::Unsupported));

  auto wrong_format = parse_json(evidence_v1);
  ASSERT_TRUE(wrong_format.has_value()) << wrong_format.error().message();
  wrong_format->get_object()["format"] = "dagforge.checkpoint";
  EXPECT_EQ(workflow::storage_detail::decode_evidence(
                serialize_json(*wrong_format).value())
                .error(),
            make_error_code(Error::ParseError));

  auto invalid_old_version = parse_json(plan_v1);
  ASSERT_TRUE(invalid_old_version.has_value())
      << invalid_old_version.error().message();
  invalid_old_version->get_object()["version"] = std::int64_t{0};
  EXPECT_EQ(workflow::storage_detail::decode_stored_plan(
                serialize_json(*invalid_old_version).value())
                .error(),
            make_error_code(Error::ParseError));
}

TEST(WorkflowStorageTest, StoresRejectDataOutsideConfiguredLimits) {
  const auto directory = temporary_test_directory("storage-limits");
  std::error_code error;
  std::filesystem::remove_all(directory, error);
  std::filesystem::create_directories(directory, error);
  ASSERT_FALSE(error);

  const std::array<std::byte, 4> data{
      std::byte{'D'}, std::byte{'A'}, std::byte{'T'}, std::byte{'A'}};
  FileArtifactStore tiny_artifacts(directory / "artifacts", 1024, 3);
  EXPECT_EQ(tiny_artifacts.put(data, "application/octet-stream").error(),
            make_error_code(Error::ResourceExhausted));

  auto evidence = EvidenceLedger::open(directory / "evidence.jsonl", 10,
                                       1024, 16);
  ASSERT_TRUE(evidence.has_value()) << evidence.error().message();
  EvidenceRecord record;
  record.run_id = WorkflowRunId{"run"};
  record.node_id = WorkflowNodeId{"node"};
  EXPECT_EQ((*evidence)->append(std::move(record)).error(),
            make_error_code(Error::ResourceExhausted));

  {
    std::ofstream oversized(directory / "oversized-evidence.jsonl",
                            std::ios::binary | std::ios::trunc);
    oversized << std::string(65, 'x');
  }
  auto oversized_evidence = EvidenceLedger::open(
      directory / "oversized-evidence.jsonl", 10, 64, 64);
  ASSERT_FALSE(oversized_evidence.has_value());
  EXPECT_EQ(oversized_evidence.error(),
            make_error_code(Error::ResourceExhausted));

  TestExecutorEnvironment environment;
  auto plan = base_plan("limited-plan");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"task"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  auto compiled = PlanCompiler{environment.registry}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();
  PlanStore tiny_plans(directory / "plans", 32);
  EXPECT_EQ(tiny_plans.save(**compiled).error(),
            make_error_code(Error::ResourceExhausted));

  WorkflowCheckpoint checkpoint;
  checkpoint.plan = source_plan(**compiled);
  checkpoint.trigger.trigger_id = WorkflowTriggerId{"limited-trigger"};
  checkpoint.trigger.workflow_id = (*compiled)->workflow_id.clone();
  checkpoint.snapshot.run_id = WorkflowRunId{"limited-run"};
  checkpoint.snapshot.workflow_id = (*compiled)->workflow_id.clone();
  checkpoint.snapshot.plan_id = (*compiled)->plan_id.clone();
  checkpoint.snapshot.state = RunState::Running;
  checkpoint.snapshot.tasks.push_back(TaskSnapshot{
      .node_id = WorkflowNodeId{"task"},
      .state = TaskState::Pending,
  });
  CheckpointStore tiny_checkpoints(directory / "runs", 32);
  EXPECT_EQ(tiny_checkpoints.save(std::move(checkpoint)).error(),
            make_error_code(Error::ResourceExhausted));

  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowStorageTest, StoresExposeCommittedButDeferredWrites) {
  const auto directory = temporary_test_directory("storage-deferred-writes");
  std::error_code error;
  std::filesystem::remove_all(directory, error);
  std::filesystem::create_directories(directory / "plans", error);
  ASSERT_FALSE(error);
  std::filesystem::create_directories(directory / "runs", error);
  ASSERT_FALSE(error);

  TestExecutorEnvironment environment;
  auto plan = base_plan("deferred-plan");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"task"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  WorkflowControlPlane control(
      environment.registry, PlanValidator{},
      std::make_shared<PlanStore>(directory / "plans",
                                  kStorageDefaults.max_plan_bytes));
  workflow::storage_detail::testing::fail_next_directory_sync();
  auto registered = control.register_plan(plan);
  ASSERT_TRUE(registered.has_value()) << registered.error().message();
  EXPECT_TRUE(registered->durability_deferred);
  EXPECT_EQ((*registered)->workflow_id, WorkflowId{"deferred-plan"});
  EXPECT_TRUE(control.get_plan((*registered)->plan_id).has_value());
  auto duplicate = control.register_plan(plan);
  ASSERT_TRUE(duplicate.has_value()) << duplicate.error().message();
  EXPECT_TRUE(duplicate->durability_deferred);
  EXPECT_EQ((*duplicate)->plan_id, (*registered)->plan_id);

  WorkflowCheckpoint checkpoint{
      .plan = std::move(plan),
      .trigger = TriggerEnvelope{
          .trigger_id = WorkflowTriggerId{"deferred-trigger"},
          .workflow_id = WorkflowId{"deferred-plan"},
      },
      .snapshot = RunSnapshot{
          .run_id = WorkflowRunId{"deferred-run"},
          .workflow_id = WorkflowId{"deferred-plan"},
          .plan_id = (*registered)->plan_id.clone(),
          .state = RunState::Succeeded,
          .tasks = {TaskSnapshot{
              .node_id = WorkflowNodeId{"task"},
              .state = TaskState::Succeeded,
          }},
      },
  };
  CheckpointStore checkpoints(directory / "runs",
                              kStorageDefaults.max_checkpoint_bytes);
  workflow::storage_detail::testing::fail_next_directory_sync();
  auto saved = checkpoints.save(checkpoint);
  ASSERT_TRUE(saved.has_value()) << saved.error().message();
  EXPECT_TRUE(saved->durability_deferred);
  EXPECT_TRUE(checkpoints.load(checkpoint.snapshot.run_id).has_value());

  auto ledger = EvidenceLedger::open(
      directory / "evidence.jsonl", 10,
      kStorageDefaults.max_evidence_file_bytes,
      kStorageDefaults.max_evidence_record_bytes);
  ASSERT_TRUE(ledger.has_value()) << ledger.error().message();
  EvidenceRecord evidence;
  evidence.run_id = checkpoint.snapshot.run_id.clone();
  evidence.node_id = WorkflowNodeId{"task"};
  workflow::storage_detail::testing::fail_next_directory_sync();
  auto appended = (*ledger)->append(std::move(evidence));
  ASSERT_TRUE(appended.has_value()) << appended.error().message();
  EXPECT_TRUE(appended->durability_deferred);
  EXPECT_EQ((*ledger)->size(), 1U);

  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowStorageTest,
     ArtifactPutExposesDeferredDurabilityAndWorkflowRejectsIt) {
  const auto directory = temporary_test_directory("artifact-put-deferred");
  std::error_code error;
  std::filesystem::remove_all(directory, error);
  std::filesystem::create_directories(directory, error);
  ASSERT_FALSE(error);
  FileArtifactStore artifacts(directory,
                              kStorageDefaults.max_artifact_metadata_bytes,
                              kStorageDefaults.max_artifact_bytes);
  const std::array<std::byte, 4> data{
      std::byte{'D'}, std::byte{'A'}, std::byte{'T'}, std::byte{'A'}};

  workflow::storage_detail::testing::fail_directory_sync_after(1);
  auto stored = artifacts.put(data, "application/octet-stream");
  ASSERT_TRUE(stored.has_value()) << stored.error().message();
  EXPECT_TRUE(stored->durability_deferred);
  EXPECT_TRUE(artifacts.get(stored->artifact_id).has_value());
  EXPECT_TRUE(artifacts.erase(stored->artifact_id).has_value());

  Runtime core(1, false, 0);
  ASSERT_TRUE(core.start().has_value());
  RunValueStore values(core, shard_id{0}, artifacts, 1024, 1);
  std::promise<Result<void>> publication;
  auto publication_result = publication.get_future();
  core.post_to(shard_id{0}, [&values, &publication] {
    workflow::storage_detail::testing::fail_directory_sync_after(1);
    publication.set_value(values.put(
        OutputRef{.node_id = WorkflowNodeId{"task"},
                  .port = WorkflowPortId{"result"}},
        std::string{"large-value"}));
  });
  ASSERT_EQ(publication_result.wait_for(std::chrono::seconds(2)),
            std::future_status::ready);
  auto published = publication_result.get();
  ASSERT_FALSE(published.has_value());
  EXPECT_EQ(published.error(), make_error_code(Error::PersistenceError));
  auto report = artifacts.reconcile();
  ASSERT_TRUE(report.has_value()) << report.error().message();
  EXPECT_TRUE(report->clean());
  core.stop();

  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowStorageTest, CheckpointStoreSurfacesDurableDeleteFailureAndDiskState) {
  const auto directory = temporary_test_directory("checkpoint-delete-failure");
  std::error_code error;
  std::filesystem::remove_all(directory, error);

  WorkflowCheckpoint checkpoint{
      .plan = base_plan("checkpoint-delete-failure"),
      .trigger = TriggerEnvelope{
          .trigger_id = WorkflowTriggerId{"delete-trigger"},
          .workflow_id = WorkflowId{"checkpoint-delete-failure"},
      },
      .snapshot = RunSnapshot{
          .run_id = WorkflowRunId{"delete-run"},
          .workflow_id = WorkflowId{"checkpoint-delete-failure"},
          .plan_id = WorkflowPlanId{"delete-plan"},
          .state = RunState::Succeeded,
      },
  };
  CheckpointStore store(directory, kStorageDefaults.max_checkpoint_bytes);
  ASSERT_TRUE(store.save(checkpoint).has_value());

  const auto checkpoint_path = directory / "delete-run.json";
  std::filesystem::remove(checkpoint_path, error);
  ASSERT_FALSE(error);
  std::filesystem::create_directory(checkpoint_path, error);
  ASSERT_FALSE(error);

  EXPECT_FALSE(store.erase(checkpoint.snapshot.run_id).has_value());
  EXPECT_EQ(store.load(checkpoint.snapshot.run_id).error(),
            make_error_code(Error::InvalidState));
  EXPECT_EQ(store.list().error(), make_error_code(Error::InvalidState));

  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowStorageTest,
     CheckpointDeleteReportsPostUnlinkDurabilityWithoutHidingCommit) {
  const auto directory = temporary_test_directory("checkpoint-delete-sync");
  std::error_code error;
  std::filesystem::remove_all(directory, error);

  WorkflowCheckpoint checkpoint{
      .plan = base_plan("checkpoint-delete-sync"),
      .trigger = TriggerEnvelope{
          .trigger_id = WorkflowTriggerId{"delete-sync-trigger"},
          .workflow_id = WorkflowId{"checkpoint-delete-sync"},
      },
      .snapshot = RunSnapshot{
          .run_id = WorkflowRunId{"delete-sync-run"},
          .workflow_id = WorkflowId{"checkpoint-delete-sync"},
          .plan_id = WorkflowPlanId{"delete-sync-plan"},
          .state = RunState::Succeeded,
      },
  };
  CheckpointStore store(directory, kStorageDefaults.max_checkpoint_bytes);
  ASSERT_TRUE(store.save(checkpoint).has_value());

  workflow::storage_detail::testing::fail_next_directory_sync();
  auto erased = store.erase(checkpoint.snapshot.run_id);
  ASSERT_TRUE(erased.has_value()) << erased.error().message();
  EXPECT_TRUE(erased->removed);
  EXPECT_TRUE(erased->durability_deferred);
  EXPECT_EQ(store.load(checkpoint.snapshot.run_id).error(),
            make_error_code(Error::NotFound));
  auto listed = store.list();
  ASSERT_TRUE(listed.has_value()) << listed.error().message();
  EXPECT_TRUE(listed->empty());

  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowStorageTest, RejectsInconsistentAttemptOutcomes) {
  auto plan = base_plan("attempt-outcome-codec");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"task"},
      .executor = "test",
  });
  const auto timeout = make_execution_failure(
      Error::Timeout, "attempt_timed_out", "Attempt timed out");
  WorkflowCheckpoint checkpoint{
      .plan = std::move(plan),
      .trigger = TriggerEnvelope{
          .trigger_id = WorkflowTriggerId{"outcome-trigger"},
          .workflow_id = WorkflowId{"attempt-outcome-codec"},
      },
      .snapshot = RunSnapshot{
          .run_id = WorkflowRunId{"outcome-run"},
          .workflow_id = WorkflowId{"attempt-outcome-codec"},
          .plan_id = WorkflowPlanId{"outcome-plan"},
          .state = RunState::Failed,
          .tasks = {TaskSnapshot{
              .node_id = WorkflowNodeId{"task"},
              .state = TaskState::Failed,
              .attempt_count = 1,
              .failure = timeout,
              .attempts = {AttemptSnapshot{
                  .attempt_id = AttemptId{"outcome-attempt"},
                  .number = 1,
                  .state = AttemptState::TimedOut,
                  .failure = timeout,
              }},
          }},
          .failure = timeout,
      },
  };
  ASSERT_TRUE(
      workflow::storage_detail::validate_checkpoint(checkpoint).has_value());

  auto encoded = workflow::storage_detail::encode_checkpoint(checkpoint);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().message();
  auto envelope = parse_json(*encoded);
  ASSERT_TRUE(envelope.has_value()) << envelope.error().message();
  envelope->get_object()
      .at("payload")
      .get_object()
      .at("snapshot")
      .get_object()
      .at("tasks")
      .get_array()
      .front()
      .get_object()
      .at("attempts")
      .get_array()
      .front()["failure_class"] = std::int64_t{0};
  EXPECT_EQ(workflow::storage_detail::decode_checkpoint(
                serialize_json(*envelope).value())
                .error(),
            make_error_code(Error::ParseError));

  auto failed_with_timeout = checkpoint;
  failed_with_timeout.snapshot.tasks[0].attempts[0].state =
      AttemptState::Failed;
  EXPECT_EQ(workflow::storage_detail::validate_checkpoint(failed_with_timeout)
                .error(),
            make_error_code(Error::InvalidArgument));

  auto timed_out_with_unknown = checkpoint;
  timed_out_with_unknown.snapshot.tasks[0].attempts[0].failure =
      make_execution_failure(Error::Unknown, "unknown", "Unknown failure");
  EXPECT_EQ(
      workflow::storage_detail::validate_checkpoint(timed_out_with_unknown)
          .error(),
      make_error_code(Error::InvalidArgument));

  auto terminating_without_reason = checkpoint;
  auto &snapshot = terminating_without_reason.snapshot;
  snapshot.state = RunState::Running;
  snapshot.failure.reset();
  auto &task = snapshot.tasks[0];
  task.state = TaskState::Running;
  task.active_attempt_id = task.attempts[0].attempt_id.clone();
  task.failure.reset();
  auto &attempt = task.attempts[0];
  attempt.state = AttemptState::Terminating;
  attempt.failure.reset();
  attempt.termination_reason.reset();
  EXPECT_EQ(
      workflow::storage_detail::validate_checkpoint(terminating_without_reason)
          .error(),
      make_error_code(Error::InvalidArgument));
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
  CheckpointStore corrupt_store(directory,
                                kStorageDefaults.max_checkpoint_bytes);
  auto listed = corrupt_store.list();
  ASSERT_FALSE(listed.has_value());
  EXPECT_EQ(listed.error(), make_error_code(Error::ParseError));

  const auto evidence_path = directory / "evidence.jsonl";
  {
    std::ofstream output(evidence_path);
    output << "not-json\n\n";
  }
  auto corrupt_evidence = EvidenceLedger::open(
      evidence_path, 1, kStorageDefaults.max_evidence_file_bytes,
      kStorageDefaults.max_evidence_record_bytes);
  ASSERT_FALSE(corrupt_evidence.has_value());
  EXPECT_EQ(corrupt_evidence.error(), make_error_code(Error::ParseError));
  std::filesystem::remove(evidence_path, error);
  ASSERT_FALSE(error);
  auto ledger = open_test_evidence(evidence_path, 1);
  EvidenceRecord invalid_record;
  EXPECT_EQ(ledger->append(std::move(invalid_record)).error(),
            make_error_code(Error::InvalidArgument));
  const WorkflowRunId run_id{"run"};
  for (std::string_view node : {"first", "second"}) {
    EvidenceRecord record;
    record.run_id = run_id.clone();
    record.node_id = WorkflowNodeId{node};
    record.type = EvidenceType::TaskCompleted;
    EXPECT_TRUE(ledger->append(std::move(record)).has_value());
  }
  EXPECT_EQ(ledger->size(), 1U);
  auto records = ledger->records(run_id);
  ASSERT_EQ(records.size(), 1U);
  EXPECT_EQ(records.front().node_id, WorkflowNodeId{"second"});

  const auto blocked_path = directory / "blocked-evidence.jsonl";
  std::filesystem::create_directory(blocked_path, error);
  ASSERT_FALSE(error);
  auto blocked = EvidenceLedger::open(
      blocked_path, kStorageDefaults.max_evidence_records,
      kStorageDefaults.max_evidence_file_bytes,
      kStorageDefaults.max_evidence_record_bytes);
  ASSERT_FALSE(blocked.has_value());
  EXPECT_EQ(blocked.error(), make_error_code(Error::InvalidState));

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
  EXPECT_EQ(store.get(ArtifactId{"../outside"}).error(),
            make_error_code(Error::InvalidArgument));
  EXPECT_EQ(store.erase(ArtifactId{".."}).error(),
            make_error_code(Error::InvalidArgument));
}

TEST(WorkflowStorageTest, FileArtifactStoreRejectsMissingAndCorruptContent) {
  const auto directory = temporary_test_directory("artifact-corruption");
  std::error_code error;
  std::filesystem::remove_all(directory, error);

  FileArtifactStore store(directory,
                          kStorageDefaults.max_artifact_metadata_bytes,
                          kStorageDefaults.max_artifact_bytes);
  EXPECT_EQ(store.get(ArtifactId{"missing"}).error(),
            make_error_code(Error::NotFound));
  EXPECT_EQ(store.erase(ArtifactId{"missing"}).error(),
            make_error_code(Error::NotFound));
  EXPECT_EQ(store.get(ArtifactId{"../outside"}).error(),
            make_error_code(Error::InvalidArgument));
  EXPECT_EQ(store.erase(ArtifactId{".."}).error(),
            make_error_code(Error::InvalidArgument));

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

  auto symlinked = store.put(data, "application/octet-stream");
  ASSERT_TRUE(symlinked.has_value()) << symlinked.error().message();
  const auto symlinked_data =
      directory / (symlinked->artifact_id.str() + ".bin");
  std::filesystem::remove(symlinked_data, error);
  ASSERT_FALSE(error);
  const auto outside = directory / "outside-data";
  {
    std::ofstream output(outside, std::ios::binary | std::ios::trunc);
    output << "outside";
  }
  std::filesystem::create_symlink(outside, symlinked_data, error);
  ASSERT_FALSE(error);
  EXPECT_FALSE(store.get(symlinked->artifact_id).has_value());
  EXPECT_TRUE(store.erase(symlinked->artifact_id).has_value());

  const auto blocked = directory / "blocked";
  {
    std::ofstream output(blocked);
    output << "not a directory";
  }
  FileArtifactStore invalid(blocked,
                            kStorageDefaults.max_artifact_metadata_bytes,
                            kStorageDefaults.max_artifact_bytes);
  auto failed = invalid.put(data, "application/octet-stream");
  EXPECT_FALSE(failed.has_value());

  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowStorageTest, ArtifactDeleteReportsDeferredCleanupTruthfully) {
  const auto directory = temporary_test_directory("artifact-delete-deferred");
  std::error_code error;
  std::filesystem::remove_all(directory, error);
  FileArtifactStore store(directory,
                          kStorageDefaults.max_artifact_metadata_bytes,
                          kStorageDefaults.max_artifact_bytes);
  const std::array<std::byte, 4> data{
      std::byte{'D'}, std::byte{'A'}, std::byte{'T'}, std::byte{'A'}};
  auto stored = store.put(data, "application/octet-stream");
  ASSERT_TRUE(stored.has_value()) << stored.error().message();

  const auto data_path =
      directory / (stored->artifact_id.str() + ".bin");
  std::filesystem::remove(data_path, error);
  ASSERT_FALSE(error);
  std::filesystem::create_directory(data_path, error);
  ASSERT_FALSE(error);

  auto erased = store.erase(stored->artifact_id);
  ASSERT_TRUE(erased.has_value()) << erased.error().message();
  EXPECT_TRUE(erased->logical_deleted);
  EXPECT_TRUE(erased->cleanup_deferred);
  EXPECT_EQ(store.get(stored->artifact_id).error(),
            make_error_code(Error::NotFound));

  auto report = store.reconcile();
  ASSERT_TRUE(report.has_value()) << report.error().message();
  EXPECT_FALSE(report->clean());
  EXPECT_EQ(report->count(ArtifactReconciliationState::OrphanData), 1U);
  ASSERT_EQ(report->entries.size(), 1U);
  EXPECT_EQ(report->entries.front().storage_key, stored->artifact_id.str());
  EXPECT_EQ(report->entries.front().state,
            ArtifactReconciliationState::OrphanData);

  auto retried = store.erase(stored->artifact_id);
  ASSERT_FALSE(retried.has_value());
  EXPECT_EQ(retried.error(), make_error_code(Error::PersistenceError));
  EXPECT_TRUE(std::filesystem::is_directory(data_path));

  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowStorageTest,
     ArtifactDeleteReportsDeferredMetadataDurabilityWithoutLaterSync) {
  const auto directory = temporary_test_directory("artifact-delete-durability");
  std::error_code error;
  std::filesystem::remove_all(directory, error);
  FileArtifactStore store(directory,
                          kStorageDefaults.max_artifact_metadata_bytes,
                          kStorageDefaults.max_artifact_bytes);
  const std::array<std::byte, 4> data{
      std::byte{'D'}, std::byte{'A'}, std::byte{'T'}, std::byte{'A'}};
  auto stored = store.put(data, "application/octet-stream");
  ASSERT_TRUE(stored.has_value()) << stored.error().message();

  const auto data_path =
      directory / (stored->artifact_id.str() + ".bin");
  std::filesystem::remove(data_path, error);
  ASSERT_FALSE(error);

  workflow::storage_detail::testing::fail_next_directory_sync();
  auto erased = store.erase(stored->artifact_id);
  ASSERT_TRUE(erased.has_value()) << erased.error().message();
  EXPECT_TRUE(erased->logical_deleted);
  EXPECT_FALSE(erased->cleanup_deferred);
  EXPECT_TRUE(erased->durability_deferred);
  EXPECT_EQ(store.get(stored->artifact_id).error(),
            make_error_code(Error::NotFound));
  EXPECT_FALSE(std::filesystem::exists(data_path));

  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowStorageTest,
     ArtifactDeleteUsesLaterDirectorySyncToConfirmMetadataDeletion) {
  const auto directory = temporary_test_directory("artifact-delete-resync");
  std::error_code error;
  std::filesystem::remove_all(directory, error);
  FileArtifactStore store(directory,
                          kStorageDefaults.max_artifact_metadata_bytes,
                          kStorageDefaults.max_artifact_bytes);
  const std::array<std::byte, 4> data{
      std::byte{'D'}, std::byte{'A'}, std::byte{'T'}, std::byte{'A'}};
  auto stored = store.put(data, "application/octet-stream");
  ASSERT_TRUE(stored.has_value()) << stored.error().message();

  workflow::storage_detail::testing::fail_next_directory_sync();
  auto erased = store.erase(stored->artifact_id);
  ASSERT_TRUE(erased.has_value()) << erased.error().message();
  EXPECT_TRUE(erased->logical_deleted);
  EXPECT_FALSE(erased->cleanup_deferred);
  EXPECT_FALSE(erased->durability_deferred);
  EXPECT_EQ(store.get(stored->artifact_id).error(),
            make_error_code(Error::NotFound));

  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowStorageTest, ArtifactStoreSurfacesMetadataCommitFailure) {
  const auto directory = temporary_test_directory("artifact-metadata-delete");
  std::error_code error;
  std::filesystem::remove_all(directory, error);
  FileArtifactStore store(directory,
                          kStorageDefaults.max_artifact_metadata_bytes,
                          kStorageDefaults.max_artifact_bytes);
  const std::array<std::byte, 4> data{
      std::byte{'D'}, std::byte{'A'}, std::byte{'T'}, std::byte{'A'}};
  auto stored = store.put(data, "application/octet-stream");
  ASSERT_TRUE(stored.has_value()) << stored.error().message();

  const auto metadata_path =
      directory / (stored->artifact_id.str() + ".json");
  std::filesystem::remove(metadata_path, error);
  ASSERT_FALSE(error);
  std::filesystem::create_directory(metadata_path, error);
  ASSERT_FALSE(error);

  EXPECT_EQ(store.erase(stored->artifact_id).error(),
            make_error_code(Error::PersistenceError));
  EXPECT_EQ(store.get(stored->artifact_id).error(),
            make_error_code(Error::InvalidState));
  auto report = store.reconcile();
  ASSERT_TRUE(report.has_value()) << report.error().message();
  EXPECT_EQ(report->count(ArtifactReconciliationState::MalformedMetadata), 1U);
  EXPECT_TRUE(std::filesystem::exists(
      directory / (stored->artifact_id.str() + ".bin")));

  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowStorageTest, ArtifactReconciliationHandlesRootAndSizeErrors) {
  const auto directory = temporary_test_directory("artifact-reconcile-errors");
  std::error_code error;
  std::filesystem::remove_all(directory, error);

  FileArtifactStore missing(directory, 16, 16);
  auto missing_report = missing.reconcile();
  ASSERT_TRUE(missing_report.has_value()) << missing_report.error().message();
  EXPECT_TRUE(missing_report->clean());

  {
    std::ofstream root_file(directory);
    root_file << "not-a-directory";
  }
  EXPECT_EQ(missing.reconcile().error(), make_error_code(Error::InvalidState));
  std::filesystem::remove(directory, error);
  ASSERT_FALSE(error);
  std::filesystem::create_directories(directory, error);
  ASSERT_FALSE(error);

  {
    std::ofstream metadata(directory / "large-metadata.json");
    metadata << std::string(32, 'x');
    std::ofstream data(directory / "large-metadata.bin",
                       std::ios::binary);
    data << "x";
  }
  {
    const ArtifactRef ref{
        .artifact_id = ArtifactId{"large-data"},
        .media_type = "application/octet-stream",
        .size_bytes = 32,
        .digest = "digest",
    };
    auto encoded = workflow::storage_detail::encode_artifact_metadata(ref);
    ASSERT_TRUE(encoded.has_value()) << encoded.error().message();
    std::ofstream metadata(directory / "large-data.json");
    metadata << *encoded;
    std::ofstream data(directory / "large-data.bin", std::ios::binary);
    data << std::string(32, 'x');
  }
  FileArtifactStore limited(directory, 256, 8);
  auto report = limited.reconcile();
  ASSERT_TRUE(report.has_value()) << report.error().message();
  EXPECT_EQ(report->count(ArtifactReconciliationState::MalformedMetadata), 1U);
  EXPECT_EQ(report->count(ArtifactReconciliationState::ContentMismatch), 1U);

  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowStorageTest, ArtifactReconciliationClassifiesWithoutMutation) {
  const auto directory = temporary_test_directory("artifact-reconciliation");
  std::error_code error;
  std::filesystem::remove_all(directory, error);
  FileArtifactStore store(directory,
                          kStorageDefaults.max_artifact_metadata_bytes,
                          kStorageDefaults.max_artifact_bytes);
  const std::array<std::byte, 4> data{
      std::byte{'D'}, std::byte{'A'}, std::byte{'T'}, std::byte{'A'}};

  auto complete = store.put(data, "application/octet-stream");
  ASSERT_TRUE(complete.has_value()) << complete.error().message();
  auto orphan_metadata = store.put(data, "application/octet-stream");
  ASSERT_TRUE(orphan_metadata.has_value())
      << orphan_metadata.error().message();
  auto mismatch = store.put(data, "application/octet-stream");
  ASSERT_TRUE(mismatch.has_value()) << mismatch.error().message();

  std::filesystem::remove(
      directory / (orphan_metadata->artifact_id.str() + ".bin"), error);
  ASSERT_FALSE(error);
  {
    std::ofstream output(
        directory / (mismatch->artifact_id.str() + ".bin"),
        std::ios::binary | std::ios::trunc);
    output << "tampered";
  }
  {
    std::ofstream output(directory / "orphan-data.bin",
                         std::ios::binary | std::ios::trunc);
    output << "orphan";
  }
  {
    std::ofstream output(directory / "malformed.json",
                         std::ios::binary | std::ios::trunc);
    output << "not-json";
  }
  {
    std::ofstream output(directory / "..json",
                         std::ios::binary | std::ios::trunc);
    output << "invalid-key";
  }

  std::vector<std::string> before;
  for (const auto &entry : std::filesystem::directory_iterator(directory)) {
    before.push_back(entry.path().filename().string());
  }
  std::ranges::sort(before);

  auto report = store.reconcile();
  ASSERT_TRUE(report.has_value()) << report.error().message();
  EXPECT_FALSE(report->clean());
  EXPECT_EQ(report->count(ArtifactReconciliationState::Complete), 1U);
  EXPECT_EQ(report->count(ArtifactReconciliationState::OrphanData), 1U);
  EXPECT_EQ(report->count(ArtifactReconciliationState::OrphanMetadata), 1U);
  EXPECT_EQ(report->count(ArtifactReconciliationState::MalformedMetadata), 1U);
  EXPECT_EQ(report->count(ArtifactReconciliationState::ContentMismatch), 1U);
  EXPECT_EQ(report->count(ArtifactReconciliationState::InvalidEntry), 1U);
  ASSERT_EQ(report->entries.size(), 6U);
  EXPECT_TRUE(std::ranges::is_sorted(
      report->entries, {}, &ArtifactReconciliationEntry::storage_key));

  const auto has_entry = [&](std::string_view key,
                             ArtifactReconciliationState state) {
    return std::ranges::any_of(report->entries, [&](const auto &entry) {
      return entry.storage_key == key && entry.state == state;
    });
  };
  EXPECT_TRUE(has_entry(complete->artifact_id.str(),
                        ArtifactReconciliationState::Complete));
  EXPECT_TRUE(has_entry(orphan_metadata->artifact_id.str(),
                        ArtifactReconciliationState::OrphanMetadata));
  EXPECT_TRUE(has_entry(mismatch->artifact_id.str(),
                        ArtifactReconciliationState::ContentMismatch));
  EXPECT_TRUE(has_entry("orphan-data",
                        ArtifactReconciliationState::OrphanData));
  EXPECT_TRUE(has_entry("malformed",
                        ArtifactReconciliationState::MalformedMetadata));
  EXPECT_TRUE(has_entry("..json",
                        ArtifactReconciliationState::InvalidEntry));

  std::vector<std::string> after;
  for (const auto &entry : std::filesystem::directory_iterator(directory)) {
    after.push_back(entry.path().filename().string());
  }
  std::ranges::sort(after);
  EXPECT_EQ(after, before);

  FileArtifactStore missing(
      directory / "missing", kStorageDefaults.max_artifact_metadata_bytes,
      kStorageDefaults.max_artifact_bytes);
  auto empty = missing.reconcile();
  ASSERT_TRUE(empty.has_value()) << empty.error().message();
  EXPECT_TRUE(empty->clean());
  EXPECT_TRUE(empty->entries.empty());

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
    (void)store.put(json, make_payload(object));

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
  EXPECT_EQ(observed.artifact_count, 0U);
  EXPECT_GT(observed.total_before_erase, 0U);
  EXPECT_EQ(observed.total_after_erase, 0U);
  EXPECT_TRUE(observed.contains_text);
  EXPECT_TRUE(observed.text_externalized);
  EXPECT_TRUE(observed.json_externalized);
  core.stop();
}

TEST(WorkflowStorageTest, RunValueStoreRollsBackArtifactReplacementAndCleanup) {
  Runtime core(1, false, 0);
  ASSERT_TRUE(core.start().has_value());
  ScriptedArtifactStore artifacts;

  struct Observation {
    std::error_code replacement_error;
    std::error_code cleanup_error;
    std::error_code json_error;
    bool previous_value_retained{false};
    bool replacement_artifact_removed{false};
    bool cleaned_output_removed{false};
    std::size_t artifacts_after_replacement{0};
    std::uint64_t total_after_cleanup{0};
  };
  std::promise<Observation> promise;
  auto future = promise.get_future();

  core.post_to(0, [&] {
    Observation observed;
    RunValueStore store(core, 0, artifacts, 4096, 1);
    const OutputRef replaced{
        .node_id = WorkflowNodeId{"replace"},
        .port = WorkflowPortId{"value"},
    };
    ASSERT_TRUE(store.put(replaced, std::string{"first"}).has_value());
    auto first = store.get(replaced);
    ASSERT_TRUE(first.has_value());
    const auto first_ref = std::get<ArtifactRef>(**first);

    artifacts.fail_erase_for(first_ref.artifact_id.clone());
    auto replacement = store.put(replaced, std::string{"second"});
    observed.replacement_error = replacement.error();
    auto retained = store.get(replaced);
    observed.previous_value_retained =
        retained && std::get<ArtifactRef>(**retained).artifact_id ==
                        first_ref.artifact_id;
    observed.artifacts_after_replacement = artifacts.size();
    observed.replacement_artifact_removed = artifacts.size() == 1U;

    artifacts.clear_erase_failure();
    const OutputRef cleaned{
        .node_id = WorkflowNodeId{"cleanup"},
        .port = WorkflowPortId{"value"},
    };
    ASSERT_TRUE(store.put(cleaned, std::string{"cleanup"}).has_value());
    auto cleanup_value = store.get(cleaned);
    ASSERT_TRUE(cleanup_value.has_value());
    const auto cleanup_ref = std::get<ArtifactRef>(**cleanup_value);
    artifacts.fail_erase_for(cleanup_ref.artifact_id.clone());
    auto cleanup = store.erase_node(cleaned.node_id);
    observed.cleanup_error = cleanup.error();
    observed.cleaned_output_removed = !store.contains(cleaned);
    observed.total_after_cleanup = store.total_output_bytes();

    FailingArtifactStore failing;
    RunValueStore failing_json(core, 0, failing, 4096, 1);
    JsonValue object = JsonValue::object_t{};
    object["value"] = "cannot-store";
    observed.json_error =
        failing_json
            .put(OutputRef{.node_id = WorkflowNodeId{"json"},
                           .port = WorkflowPortId{"value"}},
                 make_payload(object))
            .error();
    promise.set_value(std::move(observed));
  });

  ASSERT_EQ(future.wait_for(std::chrono::seconds(2)),
            std::future_status::ready);
  const auto observed = future.get();
  EXPECT_EQ(observed.replacement_error,
            make_error_code(Error::PersistenceError));
  EXPECT_EQ(observed.cleanup_error, make_error_code(Error::PersistenceError));
  EXPECT_EQ(observed.json_error, make_error_code(Error::ResourceExhausted));
  EXPECT_TRUE(observed.previous_value_retained);
  EXPECT_TRUE(observed.replacement_artifact_removed);
  EXPECT_TRUE(observed.cleaned_output_removed);
  EXPECT_EQ(observed.artifacts_after_replacement, 1U);
  EXPECT_GT(observed.total_after_cleanup, 0U);
  core.stop();
}

TEST(WorkflowStorageTest, EvidenceLedgerReloadsJsonLines) {
  const auto directory = temporary_test_directory("evidence-ledger");
  const auto file = directory / "evidence.jsonl";
  std::error_code error;
  std::filesystem::remove_all(directory, error);

  const WorkflowRunId run_id{"evidence-run"};
  {
    auto writer = open_test_evidence(file);
    EvidenceRecord record;
    record.run_id = run_id.clone();
    record.node_id = WorkflowNodeId{"command"};
    record.type = EvidenceType::TaskCompleted;
    record.actor.subject = "tester";
    auto metadata = JsonPayload::from(glz::obj{"result", "ok"});
    ASSERT_TRUE(metadata.has_value()) << metadata.error().message();
    record.metadata = std::move(*metadata);
    ASSERT_TRUE(writer->append(std::move(record)).has_value());
  }

  auto reader = open_test_evidence(file);
  auto records = reader->records(run_id);
  ASSERT_EQ(records.size(), 1U);
  EXPECT_EQ(records.front().node_id, WorkflowNodeId{"command"});
  EXPECT_EQ(records.front().type, EvidenceType::TaskCompleted);
  EXPECT_EQ(records.front().actor.subject, "tester");
  EXPECT_EQ(materialize(records.front().metadata)["result"].as<std::string>(),
            "ok");

  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowStorageTest, EvidenceLedgerValidatesOpenAndCanonicalizesFinalRecord) {
  EXPECT_EQ(EvidenceLedger::open({}, 1, 1, 1).error(),
            make_error_code(Error::InvalidArgument));
  EXPECT_EQ(EvidenceLedger::open("evidence.jsonl", 0, 1, 1).error(),
            make_error_code(Error::InvalidArgument));
  EXPECT_EQ(EvidenceLedger::open("evidence.jsonl", 1, 8, 9).error(),
            make_error_code(Error::InvalidArgument));

  const auto directory = temporary_test_directory("evidence-final-record");
  const auto file = directory / "evidence.jsonl";
  std::error_code error;
  std::filesystem::remove_all(directory, error);
  std::filesystem::create_directories(directory, error);
  ASSERT_FALSE(error);

  EvidenceRecord record;
  record.evidence_id = EvidenceId{"canonical-record"};
  record.run_id = WorkflowRunId{"canonical-run"};
  record.node_id = WorkflowNodeId{"node"};
  auto encoded = workflow::storage_detail::encode_evidence(record);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().message();
  {
    std::ofstream output(file, std::ios::binary | std::ios::trunc);
    output << *encoded;
  }
  auto opened = EvidenceLedger::open(
      file, 10, kStorageDefaults.max_evidence_file_bytes,
      kStorageDefaults.max_evidence_record_bytes);
  ASSERT_TRUE(opened.has_value()) << opened.error().message();
  EXPECT_EQ((*opened)->size(), 1U);
  auto canonical = workflow::storage_detail::load_text_file(
      file, kStorageDefaults.max_evidence_file_bytes);
  ASSERT_TRUE(canonical.has_value()) << canonical.error().message();
  ASSERT_FALSE(canonical->empty());
  EXPECT_EQ(canonical->back(), '\n');

  {
    std::ofstream output(file, std::ios::binary | std::ios::trunc);
    output << std::string(33, 'x') << '\n';
  }
  EXPECT_EQ(EvidenceLedger::open(file, 10, 64, 32).error(),
            make_error_code(Error::ResourceExhausted));

  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowStorageTest, EvidenceLedgerRetainsNewestRecords) {
  const auto directory = temporary_test_directory("evidence-retention");
  const auto file = directory / "evidence.jsonl";
  std::error_code error;
  std::filesystem::remove_all(directory, error);
  const WorkflowRunId run_id{"retained-run"};

  {
    auto writer = open_test_evidence(file, 2);
    for (std::string_view node : {"first", "second", "third"}) {
      EvidenceRecord record;
      record.run_id = run_id.clone();
      record.node_id = WorkflowNodeId{node};
      record.type = EvidenceType::TaskCompleted;
      ASSERT_TRUE(writer->append(std::move(record)).has_value());
    }
    EXPECT_EQ(writer->size(), 2U);
  }

  auto reader = open_test_evidence(file, 2);
  auto records = reader->records(run_id);
  ASSERT_EQ(records.size(), 2U);
  EXPECT_EQ(records[0].node_id, WorkflowNodeId{"second"});
  EXPECT_EQ(records[1].node_id, WorkflowNodeId{"third"});
  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowStorageTest, EvidenceLedgerAmortizesRetentionCompaction) {
  const auto directory = temporary_test_directory("evidence-compaction");
  const auto file = directory / "evidence.jsonl";
  std::error_code error;
  std::filesystem::remove_all(directory, error);
  const WorkflowRunId run_id{"compaction-run"};

  auto writer = open_test_evidence(file, 2);
  const auto append = [&](std::string_view node) {
    EvidenceRecord record;
    record.run_id = run_id.clone();
    record.node_id = WorkflowNodeId{node};
    record.type = EvidenceType::TaskCompleted;
    ASSERT_TRUE(writer->append(std::move(record)).has_value());
  };
  append("node-00");
  append("node-01");

  struct stat initial_metadata {};
  ASSERT_EQ(::stat(file.c_str(), &initial_metadata), 0);
  for (std::size_t index = 2; index < 12; ++index) {
    append(std::format("node-{:02}", index));
    struct stat current_metadata {};
    ASSERT_EQ(::stat(file.c_str(), &current_metadata), 0);
    EXPECT_EQ(current_metadata.st_ino, initial_metadata.st_ino);
  }
  EXPECT_EQ(writer->size(), 2U);
  writer.reset();

  auto reopened = open_test_evidence(file, 2);
  const auto records = reopened->records(run_id);
  ASSERT_EQ(records.size(), 2U);
  EXPECT_EQ(records[0].node_id, WorkflowNodeId{"node-10"});
  EXPECT_EQ(records[1].node_id, WorkflowNodeId{"node-11"});
  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowStorageTest, EvidenceLedgerCompactsBeforeFileLimit) {
  const auto directory = temporary_test_directory("evidence-file-limit");
  const auto file = directory / "evidence.jsonl";
  std::error_code error;
  std::filesystem::remove_all(directory, error);
  const WorkflowRunId run_id{"file-limit-run"};

  {
    auto writer = open_test_evidence(file, 2);
    for (std::string_view node : {"aa", "bb"}) {
      EvidenceRecord record;
      record.run_id = run_id.clone();
      record.node_id = WorkflowNodeId{node};
      record.type = EvidenceType::TaskCompleted;
      ASSERT_TRUE(writer->append(std::move(record)).has_value());
    }
  }
  const auto existing_size = std::filesystem::file_size(file, error);
  ASSERT_FALSE(error);

  auto opened = EvidenceLedger::open(
      file, 2, existing_size, existing_size);
  ASSERT_TRUE(opened.has_value()) << opened.error().message();
  EvidenceRecord record;
  record.run_id = run_id.clone();
  record.node_id = WorkflowNodeId{"cc"};
  record.type = EvidenceType::TaskCompleted;
  ASSERT_TRUE((*opened)->append(std::move(record)).has_value());
  EXPECT_LE(std::filesystem::file_size(file, error), existing_size);
  ASSERT_FALSE(error);
  const auto records = (*opened)->records(run_id);
  ASSERT_EQ(records.size(), 2U);
  EXPECT_EQ(records[0].node_id, WorkflowNodeId{"bb"});
  EXPECT_EQ(records[1].node_id, WorkflowNodeId{"cc"});
  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowStorageTest, EvidenceLedgerTrimsOversizedFileOnLoad) {
  const auto directory = temporary_test_directory("evidence-load-retention");
  const auto file = directory / "evidence.jsonl";
  std::error_code error;
  std::filesystem::remove_all(directory, error);
  const WorkflowRunId run_id{"load-retained-run"};

  {
    auto writer = open_test_evidence(file, 10);
    for (std::string_view node : {"first", "second", "third"}) {
      EvidenceRecord record;
      record.run_id = run_id.clone();
      record.node_id = WorkflowNodeId{node};
      record.type = EvidenceType::TaskCompleted;
      ASSERT_TRUE(writer->append(std::move(record)).has_value());
    }
  }

  auto reader = open_test_evidence(file, 1);
  auto records = reader->records(run_id);
  ASSERT_EQ(records.size(), 1U);
  EXPECT_EQ(records.front().node_id, WorkflowNodeId{"third"});

  auto reopened = open_test_evidence(file, 10);
  auto persisted = reopened->records(run_id);
  ASSERT_EQ(persisted.size(), 1U);
  EXPECT_EQ(persisted.front().node_id, WorkflowNodeId{"third"});
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
      .config = make_payload(JsonValue{{"message", "hello"}}),
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

  CheckpointStore writer(directory, kStorageDefaults.max_checkpoint_bytes);
  ASSERT_TRUE(writer.save(checkpoint).has_value());

  CheckpointStore reader(directory, kStorageDefaults.max_checkpoint_bytes);
  auto loaded = reader.load(checkpoint.snapshot.run_id);
  ASSERT_TRUE(loaded.has_value()) << loaded.error().message();
  EXPECT_EQ(loaded->plan.workflow_id, WorkflowId{"persisted-plan"});
  EXPECT_EQ(loaded->snapshot.state, RunState::Succeeded);
  ASSERT_EQ(loaded->values.size(), 1U);
  EXPECT_EQ(std::get<std::string>(loaded->values.front().value), "hello");
  auto listed = reader.list();
  ASSERT_TRUE(listed.has_value());
  EXPECT_EQ(listed->size(), 1U);

  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowControlPlaneTest, PersistsPlanCatalogWithoutRunCheckpoints) {
  const auto directory = temporary_test_directory("plan-store");
  std::error_code error;
  std::filesystem::remove_all(directory, error);

  TestExecutorEnvironment environment;
  AdmissionConfig admission;
  admission.allowed_executors = {"test"};
  WorkflowPlanId stored_plan_id;
  std::string stored_digest;
  {
    auto store = std::make_shared<PlanStore>(
        directory, kStorageDefaults.max_plan_bytes);
    WorkflowControlPlane control(environment.registry,
                                 PlanValidator{admission}, store);
    auto plan = base_plan("catalog-only");
    plan.nodes.push_back(NodePlan{
        .node_id = WorkflowNodeId{"task"},
        .executor = "test",
        .outputs = {WorkflowPortId{"result"}},
    });
    auto registered = control.register_plan(std::move(plan));
    ASSERT_TRUE(registered.has_value()) << registered.error().message();
    stored_plan_id = (*registered)->plan_id.clone();
    stored_digest = (*registered)->digest;

    auto second = base_plan("catalog-second");
    second.nodes.push_back(NodePlan{
        .node_id = WorkflowNodeId{"task"},
        .executor = "test",
        .outputs = {WorkflowPortId{"result"}},
    });
    ASSERT_TRUE(control.register_plan(std::move(second)).has_value());

    auto loaded = store->load(stored_plan_id);
    ASSERT_TRUE(loaded.has_value()) << loaded.error().message();
    EXPECT_EQ(loaded->digest, stored_digest);
    EXPECT_EQ(store->load(WorkflowPlanId{"missing"}).error(),
              make_error_code(Error::NotFound));
    EXPECT_EQ(store->load(WorkflowPlanId{"../outside"}).error(),
              make_error_code(Error::InvalidArgument));

    ExecutionPlan invalid;
    EXPECT_EQ(store->save(invalid).error(),
              make_error_code(Error::InvalidArgument));

    EXPECT_TRUE(store->save(**registered).has_value());
    auto conflicting_plan = base_plan("catalog-only");
    conflicting_plan.nodes.push_back(NodePlan{
        .node_id = WorkflowNodeId{"task"},
        .executor = "test",
        .config = make_payload(JsonValue{{"revision", 2}}),
        .outputs = {WorkflowPortId{"result"}},
    });
    auto conflicting = PlanCompiler{environment.registry}.compile(
        std::move(conflicting_plan), stored_plan_id);
    ASSERT_TRUE(conflicting.has_value()) << conflicting.error().message();
    EXPECT_EQ(store->save(**conflicting).error(),
              make_error_code(Error::AlreadyExists));
    invalid.plan_id = WorkflowPlanId{"../outside"};
    invalid.workflow_id = WorkflowId{"unsafe"};
    EXPECT_EQ(store->save(invalid).error(),
              make_error_code(Error::InvalidArgument));
  }

  auto reopened_store = std::make_shared<PlanStore>(
      directory, kStorageDefaults.max_plan_bytes);
  auto stored = reopened_store->list();
  ASSERT_TRUE(stored.has_value()) << stored.error().message();
  ASSERT_EQ(stored->size(), 2U);
  const auto persisted = std::ranges::find(
      *stored, stored_plan_id, &StoredPlan::plan_id);
  ASSERT_NE(persisted, stored->end());
  EXPECT_EQ(persisted->digest, stored_digest);
  EXPECT_EQ(persisted->plan.workflow_id, WorkflowId{"catalog-only"});

  auto loaded_from_file = reopened_store->load(stored_plan_id);
  ASSERT_TRUE(loaded_from_file.has_value())
      << loaded_from_file.error().message();
  EXPECT_EQ(loaded_from_file->digest, stored_digest);

  auto same_persisted = PlanCompiler{environment.registry}.compile(
      persisted->plan, persisted->plan_id);
  ASSERT_TRUE(same_persisted.has_value())
      << same_persisted.error().message();
  EXPECT_TRUE((PlanStore{directory, kStorageDefaults.max_plan_bytes}
                   .save(**same_persisted)
                   .has_value()));

  auto conflicting_persisted_plan = persisted->plan;
  conflicting_persisted_plan.nodes.front().config =
      make_payload(JsonValue{{"revision", 3}});
  auto conflicting_persisted = PlanCompiler{environment.registry}.compile(
      std::move(conflicting_persisted_plan), persisted->plan_id);
  ASSERT_TRUE(conflicting_persisted.has_value())
      << conflicting_persisted.error().message();
  EXPECT_EQ((PlanStore{directory, kStorageDefaults.max_plan_bytes}
                 .save(**conflicting_persisted)
                 .error()),
            make_error_code(Error::AlreadyExists));

  WorkflowControlPlane restored(environment.registry,
                                PlanValidator{admission}, reopened_store);
  auto tampered = persisted->plan;
  EXPECT_EQ(restored.restore_plan(std::move(tampered), stored_plan_id,
                                  "wrong-digest")
                .error(),
            make_error_code(Error::ParseError));
  for (auto &entry : *stored) {
    auto loaded = restored.restore_plan(std::move(entry.plan), entry.plan_id,
                                        entry.digest);
    ASSERT_TRUE(loaded.has_value()) << loaded.error().message();
  }
  auto by_id = restored.get_plan(stored_plan_id);
  ASSERT_TRUE(by_id.has_value()) << by_id.error().message();
  EXPECT_EQ((*by_id)->workflow_id, WorkflowId{"catalog-only"});
  EXPECT_EQ(reopened_store->load(WorkflowPlanId{"../outside"}).error(),
            make_error_code(Error::InvalidArgument));

  PlanStore memory_store;
  auto memory_first_plan = base_plan("memory-first");
  memory_first_plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"task"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  auto memory_first = PlanCompiler{environment.registry}.compile(
      std::move(memory_first_plan), WorkflowPlanId{"memory-first-plan"});
  ASSERT_TRUE(memory_first.has_value()) << memory_first.error().message();
  ASSERT_TRUE(memory_store.save(**memory_first).has_value());
  std::this_thread::sleep_for(std::chrono::milliseconds(1));
  auto memory_second_plan = base_plan("memory-second");
  memory_second_plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"task"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  auto memory_second = PlanCompiler{environment.registry}.compile(
      std::move(memory_second_plan), WorkflowPlanId{"memory-second-plan"});
  ASSERT_TRUE(memory_second.has_value()) << memory_second.error().message();
  ASSERT_TRUE(memory_store.save(**memory_second).has_value());
  auto memory_plans = memory_store.list();
  ASSERT_TRUE(memory_plans.has_value()) << memory_plans.error().message();
  ASSERT_EQ(memory_plans->size(), 2U);
  EXPECT_EQ(memory_plans->front().plan_id,
            WorkflowPlanId{"memory-first-plan"});
  EXPECT_TRUE(memory_store.load(WorkflowPlanId{"memory-first-plan"})
                  .has_value());
  EXPECT_EQ(memory_store.load(WorkflowPlanId{"missing"}).error(),
            make_error_code(Error::NotFound));

  const auto malformed_directory = directory / "malformed-save";
  std::filesystem::create_directories(malformed_directory, error);
  ASSERT_FALSE(error);
  {
    std::ofstream malformed(malformed_directory / "malformed-plan.json");
    malformed << "not-json";
  }
  auto malformed_plan = base_plan("malformed-save");
  malformed_plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"task"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  auto malformed_compiled = PlanCompiler{environment.registry}.compile(
      std::move(malformed_plan), WorkflowPlanId{"malformed-plan"});
  ASSERT_TRUE(malformed_compiled.has_value())
      << malformed_compiled.error().message();
  EXPECT_EQ((PlanStore{malformed_directory,
                       kStorageDefaults.max_plan_bytes}
                 .save(**malformed_compiled)
                 .error()),
            make_error_code(Error::ParseError));

  const auto blocked_directory = directory / "blocked-store";
  {
    std::ofstream blocker(blocked_directory);
    blocker << "not-a-directory";
  }
  EXPECT_FALSE((PlanStore{blocked_directory,
                          kStorageDefaults.max_plan_bytes}
                    .save(**memory_first)
                    .has_value()));

  std::filesystem::copy_file(
      directory / (stored_plan_id.str() + ".json"),
      directory / "aliased-plan.json",
      std::filesystem::copy_options::overwrite_existing, error);
  ASSERT_FALSE(error);
  EXPECT_EQ((PlanStore{directory, kStorageDefaults.max_plan_bytes}
                 .load(WorkflowPlanId{"aliased-plan"})
                 .error()),
            make_error_code(Error::ParseError));
  std::filesystem::remove(directory / "aliased-plan.json", error);
  ASSERT_FALSE(error);

  {
    std::ofstream ignored(directory / "ignored.txt");
    ignored << "ignored";
  }
  ASSERT_TRUE(reopened_store->list().has_value());

  std::filesystem::copy_file(
      directory / (stored_plan_id.str() + ".json"),
      directory / "wrong.json",
      std::filesystem::copy_options::overwrite_existing, error);
  ASSERT_FALSE(error);
  EXPECT_EQ((PlanStore{directory, kStorageDefaults.max_plan_bytes}
                 .list()
                 .error()),
            make_error_code(Error::ParseError));
  std::filesystem::remove(directory / "wrong.json", error);
  ASSERT_FALSE(error);

  {
    std::ofstream broken(directory / "broken.json");
    broken << "not-json";
  }
  EXPECT_EQ((PlanStore{directory, kStorageDefaults.max_plan_bytes}
                 .list()
                 .error()),
            make_error_code(Error::ParseError));

  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowControlPlaneTest, PlanStoreRejectsStructuredCorruptionAndDigestDrift) {
  const auto directory = temporary_test_directory("plan-store-corruption");
  std::error_code error;
  std::filesystem::remove_all(directory, error);
  std::filesystem::create_directories(directory, error);
  ASSERT_FALSE(error);

  {
    std::ofstream invalid(directory / "invalid.json");
    invalid << R"({"format":"dagforge.stored-plan","version":1,"payload":{"plan_id":"invalid","digest":"digest","created_at_ms":0,"plan":{"workflow_id":"invalid","nodes":[{"id":"","executor":"test"}]}}})";
  }
  EXPECT_EQ((PlanStore{directory, kStorageDefaults.max_plan_bytes}
                 .load(WorkflowPlanId{"invalid"})
                 .error()),
            make_error_code(Error::InvalidArgument));
  EXPECT_EQ((PlanStore{directory, kStorageDefaults.max_plan_bytes}
                 .list()
                 .error()),
            make_error_code(Error::InvalidArgument));
  std::filesystem::remove(directory / "invalid.json", error);
  ASSERT_FALSE(error);

  {
    std::ofstream missing_fields(directory / "missing-fields.json");
    missing_fields << R"({"format":"dagforge.stored-plan","version":1,"payload":{"plan_id":"","digest":"","created_at_ms":"bad","plan":[]}})";
  }
  EXPECT_EQ((PlanStore{directory, kStorageDefaults.max_plan_bytes}
                 .list()
                 .error()),
            make_error_code(Error::ParseError));
  std::filesystem::remove(directory / "missing-fields.json", error);
  ASSERT_FALSE(error);

  TestExecutorEnvironment environment;
  auto plan = base_plan("digest-drift");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"task"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  auto compiled = PlanCompiler{environment.registry}.compile(
      std::move(plan), WorkflowPlanId{"digest-drift-plan"});
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  PlanStore store(directory, kStorageDefaults.max_plan_bytes);
  ASSERT_TRUE(store.save(**compiled).has_value());
  const auto path = directory / "digest-drift-plan.json";
  auto encoded = workflow::storage_detail::load_text_file(
      path, kStorageDefaults.max_plan_bytes);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().message();
  ASSERT_FALSE(
      glz::write_at<"/payload/digest">(R"("tampered-digest")", *encoded));
  ASSERT_TRUE(workflow::storage_detail::store_text_file_atomic(
                  path, std::move(*encoded))
                  .has_value());
  EXPECT_EQ(store.load(WorkflowPlanId{"digest-drift-plan"}).error(),
            make_error_code(Error::ParseError));
  EXPECT_EQ(store.list().error(), make_error_code(Error::ParseError));
  EXPECT_EQ((PlanStore{directory, kStorageDefaults.max_plan_bytes}
                 .load(WorkflowPlanId{"digest-drift-plan"})
                 .error()),
            make_error_code(Error::ParseError));
  EXPECT_EQ((PlanStore{directory, kStorageDefaults.max_plan_bytes}
                 .list()
                 .error()),
            make_error_code(Error::ParseError));

  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowRecoveryTest, ExplainsConservativeRepairInvalidation) {
  TestExecutorEnvironment environment;
  InMemoryArtifactStore artifacts;

  auto parent_plan = base_plan("repair-reasons");
  parent_plan.nodes = {
      NodePlan{.node_id = WorkflowNodeId{"root"},
               .executor = "test",
               .outputs = {WorkflowPortId{"result"}}},
      NodePlan{.node_id = WorkflowNodeId{"changed"},
               .executor = "test",
               .config = make_payload(
                   JsonValue{{"version", std::int64_t{1}}}),
               .outputs = {WorkflowPortId{"result"}}},
      NodePlan{
          .node_id = WorkflowNodeId{"child"},
          .executor = "test",
          .inputs = {InputBinding{
              .input = WorkflowPortId{"source"},
              .source = OutputRef{.node_id = WorkflowNodeId{"changed"},
                                  .port = WorkflowPortId{"result"}},
          }},
          .outputs = {WorkflowPortId{"result"}},
      },
      NodePlan{.node_id = WorkflowNodeId{"conditioned"},
               .executor = "test",
               .outputs = {WorkflowPortId{"result"}}},
      NodePlan{.node_id = WorkflowNodeId{"missing"},
               .executor = "test",
               .outputs = {WorkflowPortId{"result"}}},
      NodePlan{.node_id = WorkflowNodeId{"artifact"},
               .executor = "test",
               .outputs = {WorkflowPortId{"result"}}},
      NodePlan{.node_id = WorkflowNodeId{"failed"},
               .executor = "test",
               .outputs = {WorkflowPortId{"result"}}},
  };
  parent_plan.edges.push_back(ConditionalEdge{
      .source = OutputRef{.node_id = WorkflowNodeId{"root"},
                          .port = WorkflowPortId{"result"}},
      .target = WorkflowNodeId{"conditioned"},
      .condition = ConditionExpr{.kind = ConditionKind::StringEquals,
                                 .expected_string = "go"},
  });

  WorkflowCheckpoint parent{
      .plan = parent_plan,
      .trigger = TriggerEnvelope{
          .trigger_id = WorkflowTriggerId{"repair-reasons-trigger"},
          .workflow_id = WorkflowId{"repair-reasons"},
      },
      .snapshot = RunSnapshot{
          .run_id = WorkflowRunId{"repair-reasons__parent"},
          .workflow_id = WorkflowId{"repair-reasons"},
          .plan_id = WorkflowPlanId{"repair-reasons-plan"},
          .state = RunState::Failed,
          .tasks = {
              TaskSnapshot{.node_id = WorkflowNodeId{"root"},
                           .state = TaskState::Succeeded},
              TaskSnapshot{.node_id = WorkflowNodeId{"changed"},
                           .state = TaskState::Succeeded},
              TaskSnapshot{.node_id = WorkflowNodeId{"child"},
                           .state = TaskState::Succeeded},
              TaskSnapshot{.node_id = WorkflowNodeId{"conditioned"},
                           .state = TaskState::Succeeded},
              TaskSnapshot{.node_id = WorkflowNodeId{"missing"},
                           .state = TaskState::Succeeded},
              TaskSnapshot{.node_id = WorkflowNodeId{"artifact"},
                           .state = TaskState::Succeeded},
              TaskSnapshot{.node_id = WorkflowNodeId{"failed"},
                           .state = TaskState::Failed},
          },
      },
      .values = {
          {OutputRef{.node_id = WorkflowNodeId{"root"},
                     .port = WorkflowPortId{"result"}},
           std::string{"go"}},
          {OutputRef{.node_id = WorkflowNodeId{"changed"},
                     .port = WorkflowPortId{"result"}},
           std::string{"old"}},
          {OutputRef{.node_id = WorkflowNodeId{"child"},
                     .port = WorkflowPortId{"result"}},
           std::string{"child"}},
          {OutputRef{.node_id = WorkflowNodeId{"conditioned"},
                     .port = WorkflowPortId{"result"}},
           std::string{"conditioned"}},
          {OutputRef{.node_id = WorkflowNodeId{"artifact"},
                     .port = WorkflowPortId{"result"}},
           ArtifactRef{.artifact_id = ArtifactId{"missing-artifact"},
                       .media_type = "application/json",
                       .size_bytes = 10,
                       .digest = "missing-digest"}},
      },
  };

  auto revised_plan = parent_plan;
  revised_plan.nodes[1].config =
      make_payload(JsonValue{{"version", std::int64_t{2}}});
  revised_plan.edges.front().condition.expected_string = "continue";
  revised_plan.nodes.push_back(
      NodePlan{.node_id = WorkflowNodeId{"added"},
               .executor = "test",
               .outputs = {WorkflowPortId{"result"}}});
  auto revised = PlanCompiler{environment.registry}.compile(
      std::move(revised_plan), WorkflowPlanId{"repair-reasons-revised"});
  ASSERT_TRUE(revised.has_value()) << revised.error().message();

  auto planned = workflow::detail::plan_repair(**revised, parent, artifacts);
  ASSERT_TRUE(planned.has_value()) << planned.error().message();
  const auto reason_for = [&](std::string_view node_id) -> std::string {
    const auto decision = std::ranges::find_if(
        planned->decisions, [&](const RepairNodeDecision &candidate) {
          return candidate.node_id == WorkflowNodeId{node_id};
        });
    return decision == planned->decisions.end() ? std::string{}
                                                 : decision->reason;
  };
  EXPECT_EQ(reason_for("root"), "reused");
  EXPECT_EQ(reason_for("changed"), "execution_contract_changed");
  EXPECT_EQ(reason_for("child"), "dependency_invalidated");
  EXPECT_EQ(reason_for("conditioned"), "incoming_condition_changed");
  EXPECT_EQ(reason_for("missing"), "required_output_missing");
  EXPECT_EQ(reason_for("artifact"), "required_output_missing");
  EXPECT_EQ(reason_for("failed"), "source_not_succeeded");
  EXPECT_EQ(reason_for("added"), "node_added");

  auto wrong_workflow = parent;
  wrong_workflow.snapshot.workflow_id = WorkflowId{"other"};
  EXPECT_EQ(workflow::detail::plan_repair(**revised, wrong_workflow, artifacts)
                .error(),
            make_error_code(Error::InvalidArgument));
}

TEST(WorkflowRecoveryTest, NormalizesPausingAndExpiredRetryState) {
  const auto now = std::chrono::system_clock::now();
  const auto prior_failure = make_execution_failure(
      Error::Unknown, "retryable_failure", "Retryable failure");
  RunSnapshot snapshot{
      .run_id = WorkflowRunId{"recovery-normalization"},
      .workflow_id = WorkflowId{"recovery-normalization"},
      .plan_id = WorkflowPlanId{"recovery-normalization-plan"},
      .state = RunState::Pausing,
      .tasks = {
          TaskSnapshot{
              .node_id = WorkflowNodeId{"expired"},
              .state = TaskState::RetryWaiting,
              .attempt_count = 1,
              .next_attempt_at = now - std::chrono::milliseconds(1),
              .failure = prior_failure,
              .attempts = {AttemptSnapshot{
                  .attempt_id = AttemptId{"expired-attempt"},
                  .number = 1,
                  .state = AttemptState::Failed,
                  .failure = prior_failure,
              }},
          },
          TaskSnapshot{
              .node_id = WorkflowNodeId{"future"},
              .state = TaskState::RetryWaiting,
              .attempt_count = 1,
              .next_attempt_at = now + std::chrono::minutes(1),
              .failure = prior_failure,
              .attempts = {AttemptSnapshot{
                  .attempt_id = AttemptId{"future-attempt"},
                  .number = 1,
                  .state = AttemptState::Failed,
                  .failure = prior_failure,
              }},
          },
      },
  };

  (void)workflow::detail::rehydrate_for_restart(snapshot, now);
  EXPECT_EQ(snapshot.state, RunState::Paused);
  EXPECT_EQ(snapshot.tasks[0].state, TaskState::Ready);
  EXPECT_FALSE(snapshot.tasks[0].next_attempt_at.has_value());
  EXPECT_EQ(snapshot.tasks[1].state, TaskState::RetryWaiting);
  EXPECT_TRUE(snapshot.tasks[1].next_attempt_at.has_value());
}

TEST(WorkflowRuntimeTest, RestartResumesInterruptedAttemptWithoutRerunningSuccess) {
  Runtime core(1, false, 0);
  TestExecutorEnvironment environment(core);
  auto checkpoint_store = std::make_shared<CheckpointStore>();
  WorkflowRuntime runtime(core, environment.registry, {}, {}, checkpoint_store);

  auto plan = base_plan("restart-active");
  plan.nodes = {
      NodePlan{
          .node_id = WorkflowNodeId{"completed"},
          .executor = "test",
          .outputs = {WorkflowPortId{"result"}},
      },
      NodePlan{
          .node_id = WorkflowNodeId{"interrupted"},
          .executor = "test",
          .outputs = {WorkflowPortId{"result"}},
      },
  };
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
          .tasks = {
              TaskSnapshot{
                  .node_id = WorkflowNodeId{"completed"},
                  .state = TaskState::Succeeded,
                  .attempt_count = 1,
                  .attempts = {AttemptSnapshot{
                      .attempt_id = AttemptId{"completed-attempt"},
                      .number = 1,
                      .state = AttemptState::Succeeded,
                  }},
              },
              TaskSnapshot{
                  .node_id = WorkflowNodeId{"interrupted"},
                  .state = TaskState::Running,
                  .attempt_count = 1,
                  .active_attempt_id = AttemptId{"interrupted-attempt"},
                  .attempts = {AttemptSnapshot{
                      .attempt_id = AttemptId{"interrupted-attempt"},
                      .number = 1,
                      .state = AttemptState::Running,
                  }},
              },
          },
      },
      .values = {{OutputRef{.node_id = WorkflowNodeId{"completed"},
                            .port = WorkflowPortId{"result"}},
                  std::string{"retained"}}},
  };
  ASSERT_TRUE(runtime.restore(*compiled, checkpoint).has_value());
  ASSERT_TRUE(core.start().has_value());
  ASSERT_TRUE(runtime.activate_restored().has_value());
  ASSERT_TRUE(environment.executor->wait_for_pending(1));

  auto restored = sync_wait_on_runtime(
      core, runtime.snapshot(checkpoint.snapshot.run_id));
  ASSERT_TRUE(restored.has_value()) << restored.error().message();
  EXPECT_EQ((*restored)->state, RunState::Running);
  EXPECT_EQ((*restored)->tasks[0].state, TaskState::Succeeded);
  EXPECT_EQ((*restored)->tasks[0].attempt_count, 1U);
  EXPECT_EQ((*restored)->tasks[1].state, TaskState::Running);
  ASSERT_EQ((*restored)->tasks[1].attempts.size(), 2U);
  EXPECT_EQ((*restored)->tasks[1].attempts.front().state,
            AttemptState::Failed);
  ASSERT_TRUE((*restored)->tasks[1].attempts.front().failure.has_value());
  EXPECT_EQ((*restored)->tasks[1].attempts.front().failure->code,
            "runtime_restarted");

  auto retained = sync_wait_on_runtime(
      core, runtime.output(
                checkpoint.snapshot.run_id,
                OutputRef{.node_id = WorkflowNodeId{"completed"},
                          .port = WorkflowPortId{"result"}}));
  ASSERT_TRUE(retained.has_value()) << retained.error().message();
  EXPECT_EQ(std::get<std::string>(**retained), "retained");

  ASSERT_TRUE(environment.executor->complete_next(0, "resumed"));
  auto completed = wait_for_state(runtime, core, checkpoint.snapshot.run_id,
                                  RunState::Succeeded);
  ASSERT_TRUE(completed.has_value()) << completed.error().message();
  EXPECT_EQ((*completed)->tasks[0].attempt_count, 1U);
  EXPECT_EQ((*completed)->tasks[1].attempt_count, 2U);

  auto report = sync_wait_on_runtime(
      core, runtime.failure_report(checkpoint.snapshot.run_id));
  ASSERT_TRUE(report.has_value()) << report.error().message();
  ASSERT_EQ(report->tasks.size(), 1U);
  ASSERT_EQ(report->tasks.front().attempts.size(), 1U);
  EXPECT_EQ(report->tasks.front().attempts.front().failure.code,
            "runtime_restarted");
  const auto recovery_evidence = runtime.evidence(checkpoint.snapshot.run_id);
  EXPECT_NE(std::ranges::find_if(
                recovery_evidence, [](const EvidenceRecord &record) {
                  return record.type == EvidenceType::RunRecoveryResumed;
                }),
            recovery_evidence.end());
  const auto recovered_attempt = std::ranges::find_if(
      recovery_evidence, [](const EvidenceRecord &record) {
        if (record.type != EvidenceType::AttemptCompleted) {
          return false;
        }
        auto metadata = record.metadata.materialize();
        if (!metadata || !metadata->is_object()) {
          return false;
        }
        const auto failure = metadata->get_object().find("failure");
        return failure != metadata->get_object().end() &&
               failure->second.is_object() &&
               failure->second["code"].as<std::string>() ==
                   "runtime_restarted";
      });
  EXPECT_NE(recovered_attempt, recovery_evidence.end());
  core.stop();
}

TEST(WorkflowRuntimeTest, RestoreRejectsCheckpointFromDifferentPlanDigest) {
  Runtime core(1, false, 0);
  TestExecutorEnvironment environment(core);
  WorkflowRuntime runtime(core, environment.registry);

  auto checkpoint_plan = base_plan("restore-digest");
  JsonValue original_config = JsonValue::object_t{};
  original_config["revision"] = std::int64_t{1};
  checkpoint_plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"task"},
      .executor = "test",
      .config = make_payload(original_config),
      .outputs = {WorkflowPortId{"result"}},
  });
  const WorkflowPlanId plan_id{"restore-digest-plan"};

  auto different_plan = checkpoint_plan;
  auto different_config =
      materialize(different_plan.nodes.front().config);
  different_config["revision"] = std::int64_t{2};
  different_plan.nodes.front().config = make_payload(different_config);
  auto compiled =
      PlanCompiler{environment.registry}.compile(std::move(different_plan),
                                                  plan_id);
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  WorkflowCheckpoint checkpoint{
      .plan = std::move(checkpoint_plan),
      .trigger = TriggerEnvelope{
          .trigger_id = WorkflowTriggerId{"restore-digest-trigger"},
          .workflow_id = WorkflowId{"restore-digest"},
          .source = "test",
          .event_type = "restore",
      },
      .snapshot = RunSnapshot{
          .run_id = WorkflowRunId{"restore-digest__run"},
          .workflow_id = WorkflowId{"restore-digest"},
          .plan_id = plan_id.clone(),
          .state = RunState::Running,
          .tasks = {TaskSnapshot{.node_id = WorkflowNodeId{"task"},
                                 .state = TaskState::Ready}},
      },
  };

  EXPECT_EQ(runtime.restore(*compiled, std::move(checkpoint)).error(),
            make_error_code(Error::InvalidState));
}

TEST(WorkflowRuntimeTest, RestoredPausedRunWaitsForExplicitResume) {
  Runtime core(1, false, 0);
  TestExecutorEnvironment environment(core);
  WorkflowRuntime runtime(core, environment.registry);

  auto plan = base_plan("restore-paused");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"task"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  auto compiled = PlanCompiler{environment.registry}.compile(
      plan, WorkflowPlanId{"restore-paused-plan"});
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  WorkflowCheckpoint checkpoint{
      .plan = std::move(plan),
      .trigger = TriggerEnvelope{
          .trigger_id = WorkflowTriggerId{"restore-paused-trigger"},
          .workflow_id = WorkflowId{"restore-paused"},
          .source = "test",
          .event_type = "paused",
      },
      .snapshot = RunSnapshot{
          .run_id = WorkflowRunId{"restore-paused__run"},
          .workflow_id = WorkflowId{"restore-paused"},
          .plan_id = WorkflowPlanId{"restore-paused-plan"},
          .state = RunState::Paused,
          .tasks = {TaskSnapshot{.node_id = WorkflowNodeId{"task"},
                                 .state = TaskState::Ready}},
      },
  };
  ASSERT_TRUE(runtime.restore(*compiled, checkpoint).has_value());
  ASSERT_TRUE(core.start().has_value());
  ASSERT_TRUE(runtime.activate_restored().has_value());
  EXPECT_EQ(environment.executor->pending_count(), 0U);

  auto paused = sync_wait_on_runtime(
      core, runtime.snapshot(checkpoint.snapshot.run_id));
  ASSERT_TRUE(paused.has_value()) << paused.error().message();
  EXPECT_EQ((*paused)->state, RunState::Paused);

  auto resumed =
      sync_wait_on_runtime(core, runtime.resume(checkpoint.snapshot.run_id));
  ASSERT_TRUE(resumed.has_value()) << resumed.error().message();
  ASSERT_TRUE(environment.executor->wait_for_pending(1));
  ASSERT_TRUE(environment.executor->complete_next(0, "resumed"));
  ASSERT_TRUE(wait_for_state(runtime, core, checkpoint.snapshot.run_id,
                             RunState::Succeeded)
                  .has_value());
  core.stop();
}

TEST(WorkflowRuntimeTest, RestoredRetryWaitHonorsPersistedDeadline) {
  Runtime core(1, false, 0);
  TestExecutorEnvironment environment(core);
  WorkflowRuntime runtime(core, environment.registry);

  auto plan = base_plan("restore-retry");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"task"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
      .max_retries = 2,
      .retry_initial_delay = std::chrono::milliseconds(10),
      .retry_max_delay = std::chrono::milliseconds(100),
  });
  auto compiled = PlanCompiler{environment.registry}.compile(
      plan, WorkflowPlanId{"restore-retry-plan"});
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  const auto retry_at =
      std::chrono::system_clock::now() + std::chrono::milliseconds(150);
  const auto prior_failure = make_execution_failure(
      Error::Unknown, "transient_failure", "Transient failure");
  WorkflowCheckpoint checkpoint{
      .plan = std::move(plan),
      .trigger = TriggerEnvelope{
          .trigger_id = WorkflowTriggerId{"restore-retry-trigger"},
          .workflow_id = WorkflowId{"restore-retry"},
          .source = "test",
          .event_type = "retry",
      },
      .snapshot = RunSnapshot{
          .run_id = WorkflowRunId{"restore-retry__run"},
          .workflow_id = WorkflowId{"restore-retry"},
          .plan_id = WorkflowPlanId{"restore-retry-plan"},
          .state = RunState::Running,
          .tasks = {TaskSnapshot{
              .node_id = WorkflowNodeId{"task"},
              .state = TaskState::RetryWaiting,
              .attempt_count = 1,
              .next_attempt_at = retry_at,
              .failure = prior_failure,
              .attempts = {AttemptSnapshot{
                  .attempt_id = AttemptId{"prior-attempt"},
                  .number = 1,
                  .state = AttemptState::Failed,
                  .failure = prior_failure,
              }},
          }},
      },
  };
  ASSERT_TRUE(runtime.restore(*compiled, checkpoint).has_value());
  ASSERT_TRUE(core.start().has_value());
  ASSERT_TRUE(runtime.activate_restored().has_value());
  EXPECT_FALSE(environment.executor->wait_for_pending(
      1, std::chrono::milliseconds(30)));
  ASSERT_TRUE(environment.executor->wait_for_pending(
      1, std::chrono::seconds(1)));
  ASSERT_TRUE(environment.executor->complete_next(0, "retried"));
  auto completed = wait_for_state(runtime, core, checkpoint.snapshot.run_id,
                                  RunState::Succeeded);
  ASSERT_TRUE(completed.has_value()) << completed.error().message();
  EXPECT_EQ((*completed)->tasks.front().attempt_count, 2U);
  core.stop();
}

TEST(WorkflowRuntimeTest, RestoredStoppingRunFinishesCancellation) {
  Runtime core(1, false, 0);
  TestExecutorEnvironment environment(core);
  WorkflowRuntime runtime(core, environment.registry);

  auto plan = base_plan("restore-stopping");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"task"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  auto compiled = PlanCompiler{environment.registry}.compile(
      plan, WorkflowPlanId{"restore-stopping-plan"});
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  WorkflowCheckpoint checkpoint{
      .plan = std::move(plan),
      .trigger = TriggerEnvelope{
          .trigger_id = WorkflowTriggerId{"restore-stopping-trigger"},
          .workflow_id = WorkflowId{"restore-stopping"},
          .source = "test",
          .event_type = "cancel",
      },
      .snapshot = RunSnapshot{
          .run_id = WorkflowRunId{"restore-stopping__run"},
          .workflow_id = WorkflowId{"restore-stopping"},
          .plan_id = WorkflowPlanId{"restore-stopping-plan"},
          .state = RunState::Stopping,
          .stop_intent = StopIntent::Cancel,
          .stop_reason = "operator cancelled",
          .tasks = {TaskSnapshot{
              .node_id = WorkflowNodeId{"task"},
              .state = TaskState::Running,
              .attempt_count = 1,
              .active_attempt_id = AttemptId{"active-attempt"},
              .attempts = {AttemptSnapshot{
                  .attempt_id = AttemptId{"active-attempt"},
                  .number = 1,
                  .state = AttemptState::Terminating,
                  .termination_reason = TerminationReason::RunCancelled,
              }},
          }},
      },
  };
  ASSERT_TRUE(runtime.restore(*compiled, checkpoint).has_value());
  ASSERT_TRUE(core.start().has_value());
  ASSERT_TRUE(runtime.activate_restored().has_value());

  auto cancelled = sync_wait_on_runtime(
      core, runtime.snapshot(checkpoint.snapshot.run_id));
  ASSERT_TRUE(cancelled.has_value()) << cancelled.error().message();
  EXPECT_EQ((*cancelled)->state, RunState::Cancelled);
  EXPECT_EQ((*cancelled)->tasks.front().state, TaskState::Cancelled);
  EXPECT_EQ((*cancelled)->tasks.front().attempts.front().state,
            AttemptState::Cancelled);
  EXPECT_EQ(environment.executor->pending_count(), 0U);
  core.stop();
}

TEST(WorkflowRuntimeTest, RestoredStoppingFailureRecordsTaskEvidence) {
  Runtime core(1, false, 0);
  TestExecutorEnvironment environment(core);
  auto evidence = std::make_shared<EvidenceLedger>();
  WorkflowRuntime runtime(core, environment.registry, {}, evidence);

  auto plan = base_plan("restore-stopping-failure");
  plan.nodes = {
      NodePlan{
          .node_id = WorkflowNodeId{"active"},
          .executor = "test",
          .outputs = {WorkflowPortId{"result"}},
      },
      NodePlan{
          .node_id = WorkflowNodeId{"pending"},
          .executor = "test",
          .outputs = {WorkflowPortId{"result"}},
      },
  };
  const WorkflowPlanId plan_id{"restore-stopping-failure-plan"};
  auto compiled = PlanCompiler{environment.registry}.compile(plan, plan_id);
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();
  const auto run_failure = make_execution_failure(
      Error::PersistenceError, "checkpoint_persist_failed",
      "Checkpoint persistence failed");

  WorkflowCheckpoint checkpoint{
      .plan = std::move(plan),
      .trigger = TriggerEnvelope{
          .trigger_id = WorkflowTriggerId{"restore-stopping-failure-trigger"},
          .workflow_id = WorkflowId{"restore-stopping-failure"},
          .source = "test",
          .event_type = "failure",
      },
      .snapshot = RunSnapshot{
          .run_id = WorkflowRunId{"restore-stopping-failure__run"},
          .workflow_id = WorkflowId{"restore-stopping-failure"},
          .plan_id = plan_id.clone(),
          .state = RunState::Stopping,
          .stop_intent = StopIntent::Fail,
          .stop_reason = run_failure.message,
          .tasks = {
              TaskSnapshot{
                  .node_id = WorkflowNodeId{"active"},
                  .state = TaskState::Running,
                  .attempt_count = 1,
                  .active_attempt_id = AttemptId{"active-attempt"},
                  .attempts = {AttemptSnapshot{
                      .attempt_id = AttemptId{"active-attempt"},
                      .number = 1,
                      .state = AttemptState::Terminating,
                      .termination_reason = TerminationReason::RunFailed,
                  }},
              },
              TaskSnapshot{
                  .node_id = WorkflowNodeId{"pending"},
                  .state = TaskState::Pending,
              },
          },
          .failure = run_failure,
      },
  };
  const auto run_id = checkpoint.snapshot.run_id.clone();
  ASSERT_TRUE(runtime.restore(*compiled, std::move(checkpoint)).has_value());
  ASSERT_TRUE(core.start().has_value());
  ASSERT_TRUE(runtime.activate_restored().has_value());

  auto failed = sync_wait_on_runtime(core, runtime.snapshot(run_id));
  ASSERT_TRUE(failed.has_value()) << failed.error().message();
  EXPECT_EQ((*failed)->state, RunState::Failed);
  ASSERT_EQ((*failed)->tasks.size(), 2U);
  EXPECT_EQ((*failed)->tasks[0].state, TaskState::Failed);
  EXPECT_EQ((*failed)->tasks[1].state, TaskState::Failed);
  ASSERT_EQ((*failed)->tasks[0].attempts.size(), 1U);
  EXPECT_EQ((*failed)->tasks[0].attempts[0].state, AttemptState::Failed);
  ASSERT_TRUE((*failed)->tasks[0].attempts[0].failure.has_value());
  EXPECT_EQ((*failed)->tasks[0].attempts[0].failure->kind,
            Error::PersistenceError);

  const auto records = evidence->records(run_id);
  EXPECT_EQ(std::ranges::count(records, EvidenceType::TaskFailed,
                               &EvidenceRecord::type),
            2);
  EXPECT_NE(std::ranges::find(records, EvidenceType::RunRecoveryResumed,
                              &EvidenceRecord::type),
            records.end());
  core.stop();
}

TEST(WorkflowRuntimeTest, RecoveryFailsWhenConditionValueIsMissing) {
  Runtime core(1, false, 0);
  TestExecutorEnvironment environment(core);
  WorkflowRuntime runtime(core, environment.registry);

  auto plan = base_plan("restore-missing-condition-value");
  plan.nodes = {
      NodePlan{
          .node_id = WorkflowNodeId{"source"},
          .executor = "test",
          .outputs = {WorkflowPortId{"result"}},
      },
      NodePlan{
          .node_id = WorkflowNodeId{"dependent"},
          .executor = "test",
          .outputs = {WorkflowPortId{"result"}},
      },
  };
  plan.edges.push_back(ConditionalEdge{
      .source = OutputRef{.node_id = WorkflowNodeId{"source"},
                          .port = WorkflowPortId{"result"}},
      .target = WorkflowNodeId{"dependent"},
      .condition = ConditionExpr{.kind = ConditionKind::StringEquals,
                                 .expected_string = "continue"},
  });
  const WorkflowPlanId plan_id{"restore-missing-condition-value-plan"};
  auto compiled = PlanCompiler{environment.registry}.compile(plan, plan_id);
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  WorkflowCheckpoint checkpoint{
      .plan = std::move(plan),
      .trigger = TriggerEnvelope{
          .trigger_id = WorkflowTriggerId{"missing-condition-trigger"},
          .workflow_id = WorkflowId{"restore-missing-condition-value"},
          .source = "test",
          .event_type = "recovery",
      },
      .snapshot = RunSnapshot{
          .run_id = WorkflowRunId{"restore-missing-condition-value__run"},
          .workflow_id = WorkflowId{"restore-missing-condition-value"},
          .plan_id = plan_id.clone(),
          .state = RunState::Running,
          .tasks = {
              TaskSnapshot{.node_id = WorkflowNodeId{"source"},
                           .state = TaskState::Succeeded},
              TaskSnapshot{.node_id = WorkflowNodeId{"dependent"},
                           .state = TaskState::Pending},
          },
      },
  };
  const auto run_id = checkpoint.snapshot.run_id.clone();
  ASSERT_TRUE(runtime.restore(*compiled, std::move(checkpoint)).has_value());
  ASSERT_TRUE(core.start().has_value());
  ASSERT_TRUE(runtime.activate_restored().has_value());

  auto failed = sync_wait_on_runtime(core, runtime.snapshot(run_id));
  ASSERT_TRUE(failed.has_value()) << failed.error().message();
  EXPECT_EQ((*failed)->state, RunState::Failed);
  ASSERT_TRUE((*failed)->failure.has_value());
  EXPECT_EQ((*failed)->failure->code, "recovery_prime_failed");
  EXPECT_EQ((*failed)->tasks[0].state, TaskState::Succeeded);
  EXPECT_EQ((*failed)->tasks[1].state, TaskState::Cancelled);
  EXPECT_EQ(environment.executor->pending_count(), 0U);
  core.stop();
}

TEST(WorkflowRuntimeTest, RecoveryValueFailureBecomesTerminalRunFailure) {
  Runtime core(1, false, 0);
  TestExecutorEnvironment environment(core);
  auto checkpoints = std::make_shared<CheckpointStore>();
  auto artifacts = std::make_shared<FailingArtifactStore>();
  WorkflowRuntime runtime(core, environment.registry, artifacts, {},
                          checkpoints);

  auto plan = base_plan("restore-value-failure");
  plan.policy.budget.max_total_output_bytes = 512 * 1024;
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"task"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  const WorkflowPlanId plan_id{"restore-value-failure-plan"};
  auto compiled = PlanCompiler{environment.registry}.compile(plan, plan_id);
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  WorkflowCheckpoint checkpoint{
      .plan = std::move(plan),
      .trigger = TriggerEnvelope{
          .trigger_id = WorkflowTriggerId{"restore-value-failure-trigger"},
          .workflow_id = WorkflowId{"restore-value-failure"},
          .source = "test",
          .event_type = "recovery",
      },
      .snapshot = RunSnapshot{
          .run_id = WorkflowRunId{"restore-value-failure__run"},
          .workflow_id = WorkflowId{"restore-value-failure"},
          .plan_id = plan_id.clone(),
          .state = RunState::Running,
          .tasks = {TaskSnapshot{.node_id = WorkflowNodeId{"task"},
                                 .state = TaskState::Succeeded}},
      },
      .values = {{OutputRef{.node_id = WorkflowNodeId{"task"},
                            .port = WorkflowPortId{"result"}},
                  std::string(300 * 1024, 'x')}},
  };
  ASSERT_TRUE(runtime.restore(*compiled, std::move(checkpoint)).has_value());
  ASSERT_TRUE(core.start().has_value());
  ASSERT_TRUE(runtime.activate_restored().has_value());

  auto restored = sync_wait_on_runtime(
      core,
      runtime.snapshot(WorkflowRunId{"restore-value-failure__run"}));
  ASSERT_TRUE(restored.has_value()) << restored.error().message();
  EXPECT_EQ((*restored)->state, RunState::Failed);
  ASSERT_TRUE((*restored)->failure.has_value());
  EXPECT_EQ((*restored)->failure->code, "recovery_value_restore_failed");
  EXPECT_EQ(environment.executor->pending_count(), 0U);

  auto persisted =
      checkpoints->load(WorkflowRunId{"restore-value-failure__run"});
  ASSERT_TRUE(persisted.has_value()) << persisted.error().message();
  EXPECT_EQ(persisted->snapshot.state, RunState::Failed);
  core.stop();
}

TEST(WorkflowRuntimeTest, RestoreValidatesArtifactReferences) {
  Runtime core(1, false, 0);
  TestExecutorEnvironment environment(core);
  auto artifacts = std::make_shared<InMemoryArtifactStore>();
  WorkflowRuntime runtime(core, environment.registry, artifacts);

  auto plan = base_plan("restore-artifact-validation");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"task"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  auto compiled = PlanCompiler{environment.registry}.compile(plan);
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  const auto checkpoint_for = [&](ArtifactRef reference, std::string run_id) {
    return WorkflowCheckpoint{
        .plan = plan,
        .trigger = TriggerEnvelope{
            .trigger_id = WorkflowTriggerId{run_id + "-trigger"},
            .workflow_id = WorkflowId{"restore-artifact-validation"},
            .source = "test",
            .event_type = "restore",
        },
        .snapshot = RunSnapshot{
            .run_id = WorkflowRunId{std::move(run_id)},
            .workflow_id = WorkflowId{"restore-artifact-validation"},
            .plan_id = (*compiled)->plan_id.clone(),
            .state = RunState::Succeeded,
            .tasks = {TaskSnapshot{.node_id = WorkflowNodeId{"task"},
                                   .state = TaskState::Succeeded}},
        },
        .values = {{OutputRef{.node_id = WorkflowNodeId{"task"},
                              .port = WorkflowPortId{"result"}},
                    std::move(reference)}},
    };
  };

  auto missing = checkpoint_for(
      ArtifactRef{.artifact_id = ArtifactId{"missing"},
                  .media_type = "application/json",
                  .size_bytes = 7,
                  .digest = "missing-digest"},
      "restore-missing-artifact");
  EXPECT_EQ(runtime.restore(*compiled, std::move(missing)).error(),
            make_error_code(Error::NotFound));

  const std::string payload{"payload"};
  auto stored = artifacts->put(
      std::as_bytes(std::span{payload.data(), payload.size()}),
      "text/plain");
  ASSERT_TRUE(stored.has_value()) << stored.error().message();
  auto mismatched = checkpoint_for(*stored, "restore-mismatched-artifact");
  std::get<ArtifactRef>(mismatched.values.front().value).digest =
      "wrong-digest";
  EXPECT_EQ(runtime.restore(*compiled, std::move(mismatched)).error(),
            make_error_code(Error::ParseError));

  auto valid = checkpoint_for(*stored, "restore-valid-artifact");
  EXPECT_TRUE(runtime.restore(*compiled, std::move(valid)).has_value());
}

TEST(WorkflowRuntimeTest, PersistsAuthoritativeRunTransitions) {
  const auto directory = temporary_test_directory("runtime-persistence");
  std::error_code error;
  std::filesystem::remove_all(directory, error);

  Runtime core(1, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);
  auto checkpoint_store = std::make_shared<CheckpointStore>(
      directory, kStorageDefaults.max_checkpoint_bytes);
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

  CheckpointStore reader(directory, kStorageDefaults.max_checkpoint_bytes);
  auto persisted = reader.load(*started);
  ASSERT_TRUE(persisted.has_value()) << persisted.error().message();
  EXPECT_EQ(persisted->snapshot.state, RunState::Succeeded);
  ASSERT_EQ(persisted->values.size(), 1U);
  EXPECT_EQ(std::get<std::string>(persisted->values.front().value),
            "persisted");

  core.stop();
  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowRuntimeTest, PersistsInitialExplicitAndTerminalBoundaries) {
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

    auto initial = checkpoint_store->load(*started);
    ASSERT_TRUE(initial.has_value()) << initial.error().message();
    EXPECT_EQ(initial->snapshot.state, RunState::Running);
    ASSERT_EQ(initial->snapshot.tasks.size(), 2U);

    ASSERT_TRUE(environment.executor->wait_for_pending(1));
    ASSERT_TRUE(environment.executor->complete_next(0, "first"));
    ASSERT_TRUE(environment.executor->wait_for_pending(1));

    auto intermediate = checkpoint_store->load(*started);
    ASSERT_TRUE(intermediate.has_value()) << intermediate.error().message();
    EXPECT_EQ(intermediate->snapshot.state, RunState::Running);
    ASSERT_EQ(intermediate->snapshot.tasks.size(), 2U);
    if (checkpoint_first) {
      EXPECT_EQ(intermediate->snapshot.tasks[0].state, TaskState::Succeeded);
      EXPECT_EQ(intermediate->snapshot.tasks[1].state, TaskState::Pending);
      ASSERT_EQ(intermediate->values.size(), 1U);
    } else {
      EXPECT_EQ(intermediate->snapshot.tasks[0].state, TaskState::Pending);
      EXPECT_EQ(intermediate->snapshot.tasks[1].state, TaskState::Pending);
      EXPECT_TRUE(intermediate->values.empty());
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

TEST(WorkflowRuntimeTest, InitialPersistenceFailureRejectsRunBeforeDispatch) {
  const auto blocker = temporary_test_directory("initial-checkpoint-failure");
  std::error_code filesystem_error;
  std::filesystem::remove_all(blocker, filesystem_error);
  {
    std::ofstream output(blocker, std::ios::binary | std::ios::trunc);
    output << "not-a-directory";
  }

  Runtime core(1, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);
  auto checkpoints = std::make_shared<CheckpointStore>(
      blocker, kStorageDefaults.max_checkpoint_bytes);
  WorkflowRuntime runtime(core, environment.registry, {}, {}, checkpoints);

  auto plan = base_plan("initial-persistence-failure");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"task"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  auto compiled = PlanCompiler{environment.registry}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  auto started = runtime.start(
      *compiled,
      TriggerEnvelope{
          .workflow_id = WorkflowId{"initial-persistence-failure"},
          .source = "test",
          .event_type = "write-failure",
          .idempotency_key = "must-not-publish",
      });
  ASSERT_FALSE(started.has_value());
  EXPECT_EQ(environment.executor->pending_count(), 0U);

  auto retried = runtime.start(
      *compiled,
      TriggerEnvelope{
          .workflow_id = WorkflowId{"initial-persistence-failure"},
          .source = "test",
          .event_type = "write-failure",
          .idempotency_key = "must-not-publish",
      });
  ASSERT_FALSE(retried.has_value());
  EXPECT_EQ(environment.executor->pending_count(), 0U);

  core.stop();
  std::filesystem::remove(blocker, filesystem_error);
}

TEST(WorkflowRuntimeTest, RepairReusesIndependentSuccessfulBranches) {
  Runtime core(1, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);
  auto checkpoint_store = std::make_shared<CheckpointStore>();
  WorkflowRuntime runtime(core, environment.registry, {}, {}, checkpoint_store);

  auto parent_plan = base_plan("repair-fanout");
  parent_plan.nodes = {
      NodePlan{
          .node_id = WorkflowNodeId{"branch_a"},
          .executor = "test",
          .config = make_payload(JsonValue{{"source", "a"}}),
          .outputs = {WorkflowPortId{"result"}},
      },
      NodePlan{
          .node_id = WorkflowNodeId{"branch_b"},
          .executor = "test",
          .config = make_payload(JsonValue{{"source", "broken"}}),
          .outputs = {WorkflowPortId{"result"}},
      },
      NodePlan{
          .node_id = WorkflowNodeId{"branch_c"},
          .executor = "test",
          .config = make_payload(JsonValue{{"source", "c"}}),
          .outputs = {WorkflowPortId{"result"}},
      },
      NodePlan{
          .node_id = WorkflowNodeId{"aggregate"},
          .executor = "test",
          .inputs = {
              InputBinding{
                  .input = WorkflowPortId{"a"},
                  .source = OutputRef{.node_id = WorkflowNodeId{"branch_a"},
                                      .port = WorkflowPortId{"result"}},
              },
              InputBinding{
                  .input = WorkflowPortId{"b"},
                  .source = OutputRef{.node_id = WorkflowNodeId{"branch_b"},
                                      .port = WorkflowPortId{"result"}},
              },
              InputBinding{
                  .input = WorkflowPortId{"c"},
                  .source = OutputRef{.node_id = WorkflowNodeId{"branch_c"},
                                      .port = WorkflowPortId{"result"}},
              },
          },
          .outputs = {WorkflowPortId{"result"}},
      },
  };
  auto parent_compiled =
      PlanCompiler{environment.registry}.compile(parent_plan,
                                                  WorkflowPlanId{"parent-plan"});
  ASSERT_TRUE(parent_compiled.has_value())
      << parent_compiled.error().message();

  const auto parent_failure = make_execution_failure(
      Error::ProtocolError, "branch_b_invalid_response",
      "Branch B returned invalid data");
  WorkflowCheckpoint parent{
      .plan = parent_plan,
      .trigger = TriggerEnvelope{
          .trigger_id = WorkflowTriggerId{"parent-trigger"},
          .workflow_id = WorkflowId{"repair-fanout"},
          .source = "test",
          .event_type = "fanout",
      },
      .snapshot = RunSnapshot{
          .run_id = WorkflowRunId{"repair-fanout__parent"},
          .workflow_id = WorkflowId{"repair-fanout"},
          .plan_id = WorkflowPlanId{"parent-plan"},
          .state = RunState::Failed,
          .stop_intent = StopIntent::Fail,
          .stop_reason = parent_failure.message,
          .tasks = {
              TaskSnapshot{.node_id = WorkflowNodeId{"branch_a"},
                           .state = TaskState::Succeeded},
              TaskSnapshot{.node_id = WorkflowNodeId{"branch_b"},
                           .state = TaskState::Failed,
                           .failure = parent_failure},
              TaskSnapshot{.node_id = WorkflowNodeId{"branch_c"},
                           .state = TaskState::Succeeded},
              TaskSnapshot{.node_id = WorkflowNodeId{"aggregate"},
                           .state = TaskState::Skipped,
                           .skip_reason = SkipReason::UpstreamFailed},
          },
          .failure = parent_failure,
      },
      .values = {
          {OutputRef{.node_id = WorkflowNodeId{"branch_a"},
                     .port = WorkflowPortId{"result"}},
           std::string{"a-value"}},
          {OutputRef{.node_id = WorkflowNodeId{"branch_c"},
                     .port = WorkflowPortId{"result"}},
           std::string{"c-value"}},
      },
  };
  ASSERT_TRUE(checkpoint_store->save(parent).has_value());

  auto revised_plan = parent_plan;
  revised_plan.nodes[1].config =
      make_payload(JsonValue{{"source", "fixed"}});
  auto revised = PlanCompiler{environment.registry}.compile(
      std::move(revised_plan), WorkflowPlanId{"revised-plan"});
  ASSERT_TRUE(revised.has_value()) << revised.error().message();

  auto repaired = runtime.repair(
      *revised, parent.snapshot.run_id,
      RepairRequest{.reason = "fix branch B response schema",
                    .idempotency_key = "repair-once"});
  ASSERT_TRUE(repaired.has_value()) << repaired.error().message();
  ASSERT_EQ(repaired->nodes.size(), 4U);
  EXPECT_TRUE(repaired->nodes[0].reused);
  EXPECT_FALSE(repaired->nodes[1].reused);
  EXPECT_TRUE(repaired->nodes[2].reused);
  EXPECT_FALSE(repaired->nodes[3].reused);

  ASSERT_TRUE(environment.executor->wait_for_pending(1));
  EXPECT_EQ(environment.executor->pending_count(), 1U);
  ASSERT_TRUE(environment.executor->complete_next(0, "b-value"));
  ASSERT_TRUE(environment.executor->wait_for_pending(1));
  const auto aggregate_inputs = environment.executor->next_inputs();
  ASSERT_EQ(aggregate_inputs.size(), 3U);
  EXPECT_EQ(std::get<std::string>(*aggregate_inputs.at("a")), "a-value");
  EXPECT_EQ(std::get<std::string>(*aggregate_inputs.at("b")), "b-value");
  EXPECT_EQ(std::get<std::string>(*aggregate_inputs.at("c")), "c-value");
  ASSERT_TRUE(environment.executor->complete_next(0, "combined"));

  auto completed = wait_for_state(runtime, core, repaired->run_id,
                                  RunState::Succeeded);
  ASSERT_TRUE(completed.has_value()) << completed.error().message();
  ASSERT_EQ((*completed)->tasks.size(), 4U);
  EXPECT_EQ((*completed)->parent_run_id, parent.snapshot.run_id);
  EXPECT_EQ((*completed)->parent_plan_id, parent.snapshot.plan_id);
  EXPECT_EQ((*completed)->repair_revision, 1U);
  EXPECT_EQ((*completed)->tasks[0].attempt_count, 0U);
  EXPECT_EQ((*completed)->tasks[0].reused_from_run_id,
            parent.snapshot.run_id);
  EXPECT_EQ((*completed)->tasks[1].attempt_count, 1U);
  EXPECT_EQ((*completed)->tasks[2].attempt_count, 0U);
  EXPECT_EQ((*completed)->tasks[3].attempt_count, 1U);

  auto duplicate = runtime.repair(
      *revised, parent.snapshot.run_id,
      RepairRequest{.reason = "duplicate",
                    .idempotency_key = "repair-once"});
  ASSERT_TRUE(duplicate.has_value()) << duplicate.error().message();
  EXPECT_EQ(duplicate->run_id, repaired->run_id);

  auto unchanged_parent = checkpoint_store->load(parent.snapshot.run_id);
  ASSERT_TRUE(unchanged_parent.has_value())
      << unchanged_parent.error().message();
  EXPECT_EQ(unchanged_parent->snapshot.state, RunState::Failed);
  EXPECT_EQ(unchanged_parent->snapshot.failure->code,
            "branch_b_invalid_response");

  core.stop();
}

TEST(WorkflowRuntimeTest, RepairInvalidatesMissingArtifactAndDependents) {
  Runtime core(1, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);
  auto artifacts = std::make_shared<InMemoryArtifactStore>();
  auto checkpoints = std::make_shared<CheckpointStore>();
  WorkflowRuntime runtime(core, environment.registry, artifacts, {},
                          checkpoints);

  auto plan = base_plan("repair-missing-artifact");
  plan.nodes = {
      NodePlan{
          .node_id = WorkflowNodeId{"source"},
          .executor = "test",
          .outputs = {WorkflowPortId{"result"}},
      },
      NodePlan{
          .node_id = WorkflowNodeId{"consumer"},
          .executor = "test",
          .inputs = {InputBinding{
              .input = WorkflowPortId{"input"},
              .source = OutputRef{.node_id = WorkflowNodeId{"source"},
                                  .port = WorkflowPortId{"result"}},
          }},
          .outputs = {WorkflowPortId{"result"}},
      },
  };
  const WorkflowPlanId parent_plan_id{"missing-artifact-parent-plan"};
  auto parent_compiled =
      PlanCompiler{environment.registry}.compile(plan, parent_plan_id);
  ASSERT_TRUE(parent_compiled.has_value())
      << parent_compiled.error().message();

  const auto parent_failure = make_execution_failure(
      Error::Incomplete, "required_output_missing",
      "Published consumer output was not retained");
  WorkflowCheckpoint parent{
      .plan = plan,
      .trigger = TriggerEnvelope{
          .trigger_id = WorkflowTriggerId{"missing-artifact-trigger"},
          .workflow_id = WorkflowId{"repair-missing-artifact"},
          .source = "test",
          .event_type = "repair",
      },
      .snapshot = RunSnapshot{
          .run_id = WorkflowRunId{"repair-missing-artifact__parent"},
          .workflow_id = WorkflowId{"repair-missing-artifact"},
          .plan_id = parent_plan_id.clone(),
          .state = RunState::Failed,
          .stop_intent = StopIntent::Fail,
          .stop_reason = parent_failure.message,
          .tasks = {
              TaskSnapshot{.node_id = WorkflowNodeId{"source"},
                           .state = TaskState::Succeeded},
              TaskSnapshot{.node_id = WorkflowNodeId{"consumer"},
                           .state = TaskState::Succeeded},
          },
          .failure = parent_failure,
      },
      .values = {{OutputRef{.node_id = WorkflowNodeId{"source"},
                            .port = WorkflowPortId{"result"}},
                  ArtifactRef{
                      .artifact_id = ArtifactId{"missing-artifact"},
                      .media_type = "application/json",
                      .size_bytes = 128,
                      .digest = "missing-digest",
                  }}},
  };
  ASSERT_TRUE(checkpoints->save(parent).has_value());

  auto revised = PlanCompiler{environment.registry}.compile(
      std::move(plan), WorkflowPlanId{"missing-artifact-revised-plan"});
  ASSERT_TRUE(revised.has_value()) << revised.error().message();
  auto repaired = runtime.repair(
      *revised, parent.snapshot.run_id,
      RepairRequest{.reason = "retry after artifact retention loss"});
  ASSERT_TRUE(repaired.has_value()) << repaired.error().message();
  ASSERT_EQ(repaired->nodes.size(), 2U);
  EXPECT_FALSE(repaired->nodes[0].reused);
  EXPECT_EQ(repaired->nodes[0].reason, "required_output_missing");
  EXPECT_FALSE(repaired->nodes[1].reused);
  EXPECT_EQ(repaired->nodes[1].reason, "dependency_invalidated");

  ASSERT_TRUE(environment.executor->wait_for_pending(1));
  ASSERT_TRUE(environment.executor->complete_next(0, "restored-source"));
  ASSERT_TRUE(environment.executor->wait_for_pending(1));
  const auto inputs = environment.executor->next_inputs();
  ASSERT_EQ(inputs.size(), 1U);
  EXPECT_EQ(std::get<std::string>(*inputs.at("input")),
            "restored-source");
  ASSERT_TRUE(environment.executor->complete_next(0, "consumed"));
  ASSERT_TRUE(wait_for_state(runtime, core, repaired->run_id,
                             RunState::Succeeded)
                  .has_value());

  core.stop();
}

TEST(WorkflowRuntimeTest, RepairRejectsInvalidParentsAndConflictingKeys) {
  const auto directory = temporary_test_directory("repair-validation");
  std::error_code filesystem_error;
  std::filesystem::remove_all(directory, filesystem_error);

  Runtime core(1, false, 0);
  TestExecutorEnvironment environment(core);
  auto checkpoints = std::make_shared<CheckpointStore>(
      directory, kStorageDefaults.max_checkpoint_bytes);
  WorkflowRuntime runtime(core, environment.registry, {}, {}, checkpoints);

  auto plan = base_plan("repair-validation");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"task"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  const WorkflowPlanId plan_id{"repair-validation-plan"};
  auto compiled = PlanCompiler{environment.registry}.compile(plan, plan_id);
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  EXPECT_EQ(runtime
                .repair(*compiled, WorkflowRunId{"missing"},
                        RepairRequest{.reason = "before startup"})
                .error(),
            make_error_code(Error::InvalidState));
  ASSERT_TRUE(core.start().has_value());
  EXPECT_EQ(runtime
                .repair(*compiled, WorkflowRunId{"missing"},
                        RepairRequest{.reason = "missing parent"})
                .error(),
            make_error_code(Error::NotFound));
  EXPECT_EQ(runtime
                .repair(*compiled, WorkflowRunId{"missing"}, RepairRequest{})
                .error(),
            make_error_code(Error::InvalidState));

  const auto checkpoint_for = [&](std::string run_id, RunState state,
                                  std::string workflow = "repair-validation") {
    auto source = plan;
    source.workflow_id = WorkflowId{workflow};
    auto checkpoint = WorkflowCheckpoint{
        .plan = std::move(source),
        .trigger = TriggerEnvelope{
            .trigger_id = WorkflowTriggerId{run_id + "-trigger"},
            .workflow_id = WorkflowId{workflow},
            .source = "test",
            .event_type = "repair-parent",
        },
        .snapshot = RunSnapshot{
            .run_id = WorkflowRunId{std::move(run_id)},
            .workflow_id = WorkflowId{workflow},
            .plan_id = plan_id.clone(),
            .state = state,
            .tasks = {TaskSnapshot{.node_id = WorkflowNodeId{"task"},
                                   .state = state == RunState::Running
                                                ? TaskState::Ready
                                                : TaskState::Failed}},
        },
    };
    if (state == RunState::Failed) {
      const auto failure = make_execution_failure(
          Error::Unknown, "repair_parent_failed", "Repair parent failed");
      checkpoint.snapshot.failure = failure;
      checkpoint.snapshot.tasks.front().failure = failure;
    }
    return checkpoint;
  };

  auto active_parent = checkpoint_for("repair-active-parent", RunState::Running);
  ASSERT_TRUE(checkpoints->save(active_parent).has_value());
  EXPECT_EQ(runtime
                .repair(*compiled, active_parent.snapshot.run_id,
                        RepairRequest{.reason = "parent is active"})
                .error(),
            make_error_code(Error::InvalidState));

  auto wrong_parent = checkpoint_for("repair-wrong-parent", RunState::Failed,
                                     "other-workflow");
  ASSERT_TRUE(checkpoints->save(wrong_parent).has_value());
  EXPECT_EQ(runtime
                .repair(*compiled, wrong_parent.snapshot.run_id,
                        RepairRequest{.reason = "wrong workflow"})
                .error(),
            make_error_code(Error::InvalidState));

  auto parent = checkpoint_for("repair-valid-parent", RunState::Failed);
  ASSERT_TRUE(checkpoints->save(parent).has_value());
  auto ordinary = runtime.start(
      *compiled,
      TriggerEnvelope{.workflow_id = WorkflowId{"repair-validation"},
                      .source = "test",
                      .event_type = "ordinary",
                      .idempotency_key = "shared-key"});
  ASSERT_TRUE(ordinary.has_value()) << ordinary.error().message();
  EXPECT_EQ(runtime
                .repair(*compiled, parent.snapshot.run_id,
                        RepairRequest{.reason = "conflicting key",
                                      .idempotency_key = "shared-key"})
                .error(),
            make_error_code(Error::AlreadyExists));
  ASSERT_TRUE(environment.executor->wait_for_pending(1));
  ASSERT_TRUE(environment.executor->complete_next(0, "done"));
  ASSERT_TRUE(wait_for_state(runtime, core, *ordinary, RunState::Succeeded)
                  .has_value());

  auto repair_started = runtime.repair(
      *compiled, parent.snapshot.run_id,
      RepairRequest{.reason = "first repair",
                    .idempotency_key = "repair-request"});
  ASSERT_TRUE(repair_started.has_value())
      << repair_started.error().message();
  ASSERT_TRUE(environment.executor->wait_for_pending(1));

  auto revised_plan = plan;
  revised_plan.nodes.front().config =
      make_payload(JsonValue{{"revision", 2}});
  auto revised = PlanCompiler{environment.registry}.compile(
      std::move(revised_plan), WorkflowPlanId{"repair-validation-plan-v2"});
  ASSERT_TRUE(revised.has_value()) << revised.error().message();
  EXPECT_EQ(runtime
                .repair(*revised, parent.snapshot.run_id,
                        RepairRequest{.reason = "different repair",
                                      .idempotency_key = "repair-request"})
                .error(),
            make_error_code(Error::AlreadyExists));
  ASSERT_TRUE(environment.executor->complete_next(0, "repaired"));
  ASSERT_TRUE(wait_for_state(runtime, core, repair_started->run_id,
                             RunState::Succeeded)
                  .has_value());

  std::filesystem::remove_all(directory, filesystem_error);
  ASSERT_FALSE(filesystem_error);
  {
    std::ofstream blocker(directory, std::ios::binary | std::ios::trunc);
    blocker << "not-a-directory";
  }
  auto persistence_failure = runtime.repair(
      *compiled, parent.snapshot.run_id,
      RepairRequest{.reason = "cannot persist child"});
  ASSERT_FALSE(persistence_failure.has_value());

  core.stop();
  std::filesystem::remove(directory, filesystem_error);
}

TEST(WorkflowRuntimeTest, ExternalizesLargeFailureDetailsForRepairClients) {
  Runtime core(1, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);
  auto artifacts = std::make_shared<InMemoryArtifactStore>();
  WorkflowRuntime runtime(core, environment.registry, artifacts);

  auto plan = base_plan("large-failure");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"task"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  auto compiled = PlanCompiler{environment.registry}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();
  auto started = runtime.start(
      *compiled, TriggerEnvelope{.workflow_id = WorkflowId{"large-failure"},
                                 .source = "test",
                                 .event_type = "diagnostic"});
  ASSERT_TRUE(started.has_value()) << started.error().message();
  ASSERT_TRUE(environment.executor->wait_for_pending(1));

  JsonValue details = JsonValue::object_t{};
  details["payload"] = std::string(70 * 1024, 'x');
  ASSERT_TRUE(environment.executor->complete_next_with_failure(
      make_execution_failure(Error::ProtocolError, "large_diagnostic",
                             "Large diagnostic payload",
                             make_payload(details))));
  ASSERT_TRUE(wait_for_state(runtime, core, *started, RunState::Failed)
                  .has_value());

  auto report =
      sync_wait_on_runtime(core, runtime.failure_report(*started));
  ASSERT_TRUE(report.has_value()) << report.error().message();
  ASSERT_TRUE(report->failure.has_value());
  ASSERT_EQ(report->failure->artifacts.size(), 1U);
  EXPECT_EQ(report->failure->artifacts.front().name, "details");
  const auto retained_details = materialize(report->failure->details);
  ASSERT_TRUE(retained_details["externalized"].is_boolean());
  EXPECT_TRUE(retained_details["externalized"].get<bool>());

  auto blob = artifacts->get(
      report->failure->artifacts.front().artifact.artifact_id);
  ASSERT_TRUE(blob.has_value()) << blob.error().message();
  const std::string encoded{
      reinterpret_cast<const char *>(blob->data.data()), blob->data.size()};
  auto decoded = glz::get_as_json<std::string, "/payload">(encoded);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->size(), 70U * 1024U);

  core.stop();
}

TEST(WorkflowRuntimeTest, BoundsFailureWhenArtifactRetentionFails) {
  Runtime core(1, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);
  auto artifacts = std::make_shared<FailingArtifactStore>();
  WorkflowRuntime runtime(core, environment.registry, artifacts);

  auto plan = base_plan("failure-retention-failure");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"task"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  auto compiled = PlanCompiler{environment.registry}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();
  auto started = runtime.start(
      *compiled,
      TriggerEnvelope{.workflow_id = WorkflowId{"failure-retention-failure"},
                      .source = "test",
                      .event_type = "diagnostic"});
  ASSERT_TRUE(started.has_value()) << started.error().message();
  ASSERT_TRUE(environment.executor->wait_for_pending(1));

  JsonValue details = JsonValue::object_t{};
  details["payload"] = std::string(70 * 1024, 'x');
  ASSERT_TRUE(environment.executor->complete_next_with_failure(
      make_execution_failure(Error::ProtocolError, "large_diagnostic",
                             "Large diagnostic payload",
                             make_payload(details))));
  ASSERT_TRUE(wait_for_state(runtime, core, *started, RunState::Failed)
                  .has_value());

  auto report = sync_wait_on_runtime(core, runtime.failure_report(*started));
  ASSERT_TRUE(report.has_value()) << report.error().message();
  ASSERT_TRUE(report->failure.has_value());
  EXPECT_TRUE(report->failure->artifacts.empty());
  const auto bounded_details = materialize(report->failure->details);
  ASSERT_TRUE(bounded_details["externalization_failed"].is_boolean());
  EXPECT_TRUE(bounded_details["externalization_failed"].get<bool>());
  EXPECT_LT(report->failure->details.size(), 1024U);

  core.stop();
}

TEST(WorkflowRuntimeTest, PersistenceFailureStopsRunWithStructuredError) {
  const auto directory = temporary_test_directory("checkpoint-write-failure");
  std::error_code filesystem_error;
  std::filesystem::remove_all(directory, filesystem_error);

  Runtime core(1, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);
  auto checkpoints = std::make_shared<CheckpointStore>(
      directory, kStorageDefaults.max_checkpoint_bytes);
  WorkflowRuntime runtime(core, environment.registry, {}, {}, checkpoints);

  auto plan = base_plan("persistence-failure");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"task"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  auto compiled = PlanCompiler{environment.registry}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();
  auto started = runtime.start(
      *compiled,
      TriggerEnvelope{.workflow_id = WorkflowId{"persistence-failure"},
                      .source = "test",
                      .event_type = "write-failure"});
  ASSERT_TRUE(started.has_value()) << started.error().message();
  ASSERT_TRUE(environment.executor->wait_for_pending(1));
  ASSERT_TRUE(wait_for_attempt_state(runtime, core, *started, 0, 0,
                                     AttemptState::Running)
                  .has_value());

  std::filesystem::remove_all(directory, filesystem_error);
  ASSERT_FALSE(filesystem_error);
  {
    std::ofstream blocker(directory, std::ios::binary | std::ios::trunc);
    blocker << "not-a-directory";
  }

  ASSERT_TRUE(environment.executor->complete_next(0, "completed"));
  auto failed = wait_for_state(runtime, core, *started, RunState::Failed);
  ASSERT_TRUE(failed.has_value()) << failed.error().message();
  ASSERT_TRUE((*failed)->failure.has_value());
  EXPECT_EQ((*failed)->failure->kind, Error::PersistenceError);
  EXPECT_EQ((*failed)->failure->code, "checkpoint_persist_failed");

  auto report = sync_wait_on_runtime(core, runtime.failure_report(*started));
  ASSERT_TRUE(report.has_value()) << report.error().message();
  ASSERT_TRUE(report->failure.has_value());
  EXPECT_EQ(report->failure->code, "checkpoint_persist_failed");

  core.stop();
  std::filesystem::remove(directory, filesystem_error);
}

TEST(WorkflowRuntimeTest, EvidencePersistenceFailureStopsRun) {
  Runtime core(1, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);
  auto opened_evidence = EvidenceLedger::open(
      std::filesystem::path{"/proc/dagforge-impossible/evidence.jsonl"},
      kStorageDefaults.max_evidence_records,
      kStorageDefaults.max_evidence_file_bytes,
      kStorageDefaults.max_evidence_record_bytes);
  ASSERT_TRUE(opened_evidence.has_value())
      << opened_evidence.error().message();
  auto evidence = std::move(*opened_evidence);
  WorkflowRuntime runtime(
      core, environment.registry, std::make_shared<InMemoryArtifactStore>(),
      evidence, std::make_shared<CheckpointStore>());

  auto plan = base_plan("evidence-persistence-failure");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"task"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  auto compiled = PlanCompiler{environment.registry}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();
  auto started = runtime.start(
      *compiled,
      TriggerEnvelope{
          .workflow_id = WorkflowId{"evidence-persistence-failure"},
          .source = "test",
          .event_type = "write-failure",
      });
  ASSERT_TRUE(started.has_value()) << started.error().message();

  auto failed = wait_for_state(runtime, core, *started, RunState::Failed);
  ASSERT_TRUE(failed.has_value()) << failed.error().message();
  ASSERT_TRUE((*failed)->failure.has_value());
  EXPECT_EQ((*failed)->failure->kind, Error::PersistenceError);
  EXPECT_EQ((*failed)->failure->code, "evidence_persist_failed");
  EXPECT_EQ(evidence->size(), 0U);

  auto report = sync_wait_on_runtime(core, runtime.failure_report(*started));
  ASSERT_TRUE(report.has_value()) << report.error().message();
  ASSERT_TRUE(report->failure.has_value());
  EXPECT_EQ(report->failure->code, "evidence_persist_failed");

  core.stop();
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

TEST(WorkflowRuntimeTest, RetentionKeepsRunWhenCheckpointDeletionFails) {
  const auto directory = temporary_test_directory("retention-delete-failure");
  std::error_code filesystem_error;
  std::filesystem::remove_all(directory, filesystem_error);

  Runtime core(1, false, 0);
  ASSERT_TRUE(core.start().has_value());
  TestExecutorEnvironment environment(core);
  auto checkpoints = std::make_shared<CheckpointStore>(
      directory, kStorageDefaults.max_checkpoint_bytes);
  WorkflowRuntime runtime(
      core, environment.registry, std::make_shared<InMemoryArtifactStore>(),
      std::make_shared<EvidenceLedger>(), checkpoints, 1);

  auto plan = base_plan("retention-delete-failure");
  plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"task"},
      .executor = "test",
      .outputs = {WorkflowPortId{"result"}},
  });
  auto compiled = PlanCompiler{environment.registry}.compile(std::move(plan));
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  auto first = runtime.start(
      *compiled,
      TriggerEnvelope{
          .workflow_id = WorkflowId{"retention-delete-failure"},
          .idempotency_key = "retained-key",
      });
  ASSERT_TRUE(first.has_value()) << first.error().message();
  ASSERT_TRUE(environment.executor->wait_for_pending(1));
  ASSERT_TRUE(environment.executor->complete_next(0, "first"));
  ASSERT_TRUE(wait_for_state(runtime, core, *first, RunState::Succeeded)
                  .has_value());

  auto second = runtime.start(
      *compiled,
      TriggerEnvelope{
          .workflow_id = WorkflowId{"retention-delete-failure"},
          .idempotency_key = "second-key",
      });
  ASSERT_TRUE(second.has_value()) << second.error().message();
  ASSERT_TRUE(environment.executor->wait_for_pending(1));

  std::filesystem::remove_all(directory, filesystem_error);
  ASSERT_FALSE(filesystem_error);
  {
    std::ofstream blocker(directory, std::ios::binary | std::ios::trunc);
    blocker << "not-a-directory";
  }

  ASSERT_TRUE(environment.executor->complete_next(0, "second"));
  ASSERT_TRUE(wait_for_state(runtime, core, *second, RunState::Failed)
                  .has_value());
  EXPECT_TRUE(sync_wait_on_runtime(core, runtime.snapshot(*first)).has_value());
  EXPECT_TRUE(sync_wait_on_runtime(core, runtime.snapshot(*second)).has_value());

  auto duplicate = runtime.start(
      *compiled,
      TriggerEnvelope{
          .workflow_id = WorkflowId{"retention-delete-failure"},
          .idempotency_key = "retained-key",
      });
  ASSERT_TRUE(duplicate.has_value()) << duplicate.error().message();
  EXPECT_EQ(*duplicate, *first);

  core.stop();
  std::filesystem::remove(directory, filesystem_error);
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

  auto malformed = EvidenceLedger::open(
      file, 1, kStorageDefaults.max_evidence_file_bytes,
      kStorageDefaults.max_evidence_record_bytes);
  ASSERT_FALSE(malformed.has_value());
  EXPECT_EQ(malformed.error(), make_error_code(Error::ParseError));
  std::filesystem::remove(file, error);
  ASSERT_FALSE(error);

  auto loaded = open_test_evidence(file, 1);
  EvidenceRecord record;
  record.run_id = WorkflowRunId{"disk-run"};
  ASSERT_TRUE(loaded->append(std::move(record)).has_value());
  EXPECT_EQ(loaded->size(), 1U);
  {
    std::ofstream output(file, std::ios::binary | std::ios::app);
    output << R"({"evidence_id":"truncated)";
  }
  auto repaired = EvidenceLedger::open(
      file, 1, kStorageDefaults.max_evidence_file_bytes,
      kStorageDefaults.max_evidence_record_bytes);
  ASSERT_TRUE(repaired.has_value()) << repaired.error().message();
  EXPECT_EQ((*repaired)->size(), 1U);
  auto repaired_text = workflow::storage_detail::load_text_file(
      file, kStorageDefaults.max_evidence_file_bytes);
  ASSERT_TRUE(repaired_text.has_value()) << repaired_text.error().message();
  EXPECT_FALSE(repaired_text->contains("truncated"));
  ASSERT_FALSE(repaired_text->empty());
  EXPECT_EQ(repaired_text->back(), '\n');

  const auto committed = *repaired_text;
  ASSERT_TRUE(workflow::storage_detail::store_text_file_atomic(
                  file, committed + "not-json\n" + committed)
                  .has_value());
  auto interior_corruption = EvidenceLedger::open(
      file, 10, kStorageDefaults.max_evidence_file_bytes,
      kStorageDefaults.max_evidence_record_bytes);
  ASSERT_FALSE(interior_corruption.has_value());
  EXPECT_EQ(interior_corruption.error(), make_error_code(Error::ParseError));

  ASSERT_TRUE(workflow::storage_detail::store_text_file_atomic(
                  file, committed + "not-json")
                  .has_value());
  auto invalid_final_record = EvidenceLedger::open(
      file, 10, kStorageDefaults.max_evidence_file_bytes,
      kStorageDefaults.max_evidence_record_bytes);
  ASSERT_FALSE(invalid_final_record.has_value());
  EXPECT_EQ(invalid_final_record.error(), make_error_code(Error::ParseError));

  EvidenceLedger zero_retention(0);
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
  EXPECT_EQ(memory_store.load(WorkflowRunId{"../outside"}).error(),
            make_error_code(Error::InvalidArgument));
  EXPECT_EQ(memory_store.erase(WorkflowRunId{"missing"}).error(),
            make_error_code(Error::NotFound));

  WorkflowCheckpoint checkpoint;
  checkpoint.plan.workflow_id = WorkflowId{"memory-checkpoint"};
  checkpoint.trigger.trigger_id = WorkflowTriggerId{"memory-trigger"};
  checkpoint.trigger.workflow_id = WorkflowId{"memory-checkpoint"};
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

  CheckpointStore disk_store(directory,
                             kStorageDefaults.max_checkpoint_bytes);
  EXPECT_EQ(disk_store.load(WorkflowRunId{"broken"}).error(),
            make_error_code(Error::ParseError));
  EXPECT_EQ(disk_store.list().error(), make_error_code(Error::ParseError));

  std::filesystem::remove(directory / "broken.json", error);
  ASSERT_FALSE(error);
  auto directory_target = checkpoint;
  directory_target.snapshot.run_id = WorkflowRunId{"directory-target"};
  directory_target.snapshot.plan_id = WorkflowPlanId{"directory-target-plan"};
  std::filesystem::create_directories(directory / "directory-target.json",
                                      error);
  ASSERT_FALSE(error);
  EXPECT_FALSE(disk_store.save(std::move(directory_target)).has_value());
  EXPECT_TRUE(
      std::filesystem::is_directory(directory / "directory-target.json"));
  std::filesystem::remove_all(directory / "directory-target.json", error);
  ASSERT_FALSE(error);

  checkpoint.snapshot.run_id = WorkflowRunId{"disk-run"};
  checkpoint.snapshot.plan_id = WorkflowPlanId{"disk-plan"};
  checkpoint.created_at = std::chrono::system_clock::now();
  auto disk_saved = disk_store.save(checkpoint);
  ASSERT_TRUE(disk_saved.has_value()) << disk_saved.error().message();
  std::filesystem::copy_file(
      directory / "disk-run.json", directory / "aliased.json",
      std::filesystem::copy_options::overwrite_existing, error);
  ASSERT_FALSE(error);
  EXPECT_EQ(disk_store.list().error(), make_error_code(Error::ParseError));
  std::filesystem::remove(directory / "aliased.json", error);
  ASSERT_FALSE(error);
  CheckpointStore reloaded(directory,
                           kStorageDefaults.max_checkpoint_bytes);
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

TEST(WorkflowStorageTest, CheckpointCatalogOrderingIsDeterministic) {
  const auto make_checkpoint = [](std::string run_id) {
    WorkflowCheckpoint checkpoint;
    checkpoint.plan.workflow_id = WorkflowId{"ordered-checkpoints"};
    checkpoint.trigger.trigger_id = WorkflowTriggerId{run_id + "-trigger"};
    checkpoint.trigger.workflow_id = WorkflowId{"ordered-checkpoints"};
    checkpoint.snapshot.run_id = WorkflowRunId{std::move(run_id)};
    checkpoint.snapshot.workflow_id = WorkflowId{"ordered-checkpoints"};
    checkpoint.snapshot.plan_id = WorkflowPlanId{"ordered-plan"};
    checkpoint.snapshot.state = RunState::Succeeded;
    checkpoint.created_at = std::chrono::system_clock::time_point{
        std::chrono::milliseconds{100}};
    return checkpoint;
  };
  const auto assert_order = [](const auto &listed) {
    ASSERT_TRUE(listed.has_value()) << listed.error().message();
    ASSERT_EQ(listed->size(), 2U);
    EXPECT_EQ((*listed)[0].snapshot.run_id, WorkflowRunId{"alpha"});
    EXPECT_EQ((*listed)[1].snapshot.run_id, WorkflowRunId{"zeta"});
  };

  CheckpointStore memory;
  ASSERT_TRUE(memory.save(make_checkpoint("zeta")).has_value());
  ASSERT_TRUE(memory.save(make_checkpoint("alpha")).has_value());
  assert_order(memory.list());

  const auto directory = temporary_test_directory("checkpoint-ordering");
  std::error_code error;
  std::filesystem::remove_all(directory, error);
  CheckpointStore disk(directory, kStorageDefaults.max_checkpoint_bytes);
  ASSERT_TRUE(disk.save(make_checkpoint("zeta")).has_value());
  ASSERT_TRUE(disk.save(make_checkpoint("alpha")).has_value());
  assert_order(disk.list());
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
      .outputs = {WorkflowPortId{"null"}, WorkflowPortId{"bool"},
                  WorkflowPortId{"int"}, WorkflowPortId{"double"},
                  WorkflowPortId{"string"}, WorkflowPortId{"json"},
                  WorkflowPortId{"artifact"}},
  });
  checkpoint.plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"reused"},
      .executor = "test",
  });
  checkpoint.plan.nodes.push_back(NodePlan{
      .node_id = WorkflowNodeId{"values"},
      .executor = "test",
      .outputs = {WorkflowPortId{"null"}, WorkflowPortId{"bool"},
                  WorkflowPortId{"int"}, WorkflowPortId{"double"},
                  WorkflowPortId{"string"}, WorkflowPortId{"json"},
                  WorkflowPortId{"artifact"}},
  });
  checkpoint.trigger.trigger_id = WorkflowTriggerId{"trigger-rich"};
  checkpoint.trigger.workflow_id = WorkflowId{"rich-codec"};
  checkpoint.trigger.source = "codec-test";
  checkpoint.trigger.event_type = "roundtrip";
  JsonValue trigger_payload = JsonValue::object_t{};
  trigger_payload["nested"] = "value";
  checkpoint.trigger.payload = make_payload(trigger_payload);
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
  checkpoint.snapshot.parent_run_id = WorkflowRunId{"parent-run"};
  checkpoint.snapshot.parent_plan_id = WorkflowPlanId{"parent-plan"};
  checkpoint.snapshot.repair_revision = 2;
  checkpoint.snapshot.repair_reason = "repair schema";
  JsonValue run_failure_details = JsonValue::object_t{};
  run_failure_details["source"] = "codec";
  checkpoint.snapshot.failure = make_execution_failure(
      Error::Unknown, "workflow_failed", "Workflow failed",
      make_payload(run_failure_details));
  checkpoint.snapshot.failure->artifacts.push_back(FailureArtifact{
      .name = "details",
      .artifact = ArtifactRef{
          .artifact_id = ArtifactId{"failure-artifact"},
          .media_type = "application/json",
          .size_bytes = 77,
          .digest = "failure-digest",
      },
  });
  checkpoint.snapshot.created_at =
      std::chrono::system_clock::time_point{std::chrono::milliseconds{100}};
  checkpoint.snapshot.started_at =
      std::chrono::system_clock::time_point{std::chrono::milliseconds{200}};
  checkpoint.snapshot.finished_at =
      std::chrono::system_clock::time_point{std::chrono::milliseconds{300}};
  TaskSnapshot task;
  task.node_id = WorkflowNodeId{"task"};
  task.state = TaskState::Failed;
  task.attempt_count = 2;
  task.failure = make_execution_failure(
      Error::Unknown, "retries_exhausted", "Retries exhausted");
  task.started_at =
      std::chrono::system_clock::time_point{std::chrono::milliseconds{210}};
  task.finished_at =
      std::chrono::system_clock::time_point{std::chrono::milliseconds{290}};
  task.attempts.push_back(AttemptSnapshot{
      .attempt_id = AttemptId{"attempt-1"},
      .number = 1,
      .state = AttemptState::TimedOut,
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
  task.attempts.push_back(AttemptSnapshot{
      .attempt_id = AttemptId{"attempt-2"},
      .number = 2,
      .state = AttemptState::Failed,
      .failure = make_execution_failure(
          Error::Unknown, "second_attempt_failed", "Second attempt failed"),
      .created_at = std::chrono::system_clock::time_point{
          std::chrono::milliseconds{250}},
      .started_at = std::chrono::system_clock::time_point{
          std::chrono::milliseconds{260}},
      .finished_at = std::chrono::system_clock::time_point{
          std::chrono::milliseconds{270}},
  });
  checkpoint.snapshot.tasks.push_back(std::move(task));
  checkpoint.snapshot.tasks.push_back(TaskSnapshot{
      .node_id = WorkflowNodeId{"reused"},
      .state = TaskState::Succeeded,
      .reused_from_run_id = WorkflowRunId{"parent-run"},
  });
  checkpoint.snapshot.tasks.push_back(TaskSnapshot{
      .node_id = WorkflowNodeId{"values"},
      .state = TaskState::Succeeded,
  });
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
        OutputRef{.node_id = WorkflowNodeId{"values"},
                  .port = WorkflowPortId{std::move(port)}},
        std::move(value));
  };
  add_value("null", std::monostate{});
  add_value("bool", true);
  add_value("int", std::int64_t{42});
  add_value("double", 3.25);
  add_value("string", std::string{"text"});
  add_value("json", make_payload(json_value));
  add_value("artifact", artifact);

  CheckpointStore store(directory / "runs",
                        kStorageDefaults.max_checkpoint_bytes);
  auto saved = store.save(checkpoint);
  ASSERT_TRUE(saved.has_value()) << saved.error().message();
  CheckpointStore reader(directory / "runs",
                         kStorageDefaults.max_checkpoint_bytes);
  auto loaded = reader.load(WorkflowRunId{"rich-run"});
  ASSERT_TRUE(loaded.has_value()) << loaded.error().message();
  EXPECT_EQ(loaded->trigger.principal.roles,
            (std::vector<std::string>{"admin", "operator"}));
  EXPECT_EQ(loaded->trigger.trace.parent_span_id, "parent-span");
  EXPECT_EQ(loaded->snapshot.stop_intent, StopIntent::Fail);
  EXPECT_EQ(loaded->snapshot.parent_run_id, WorkflowRunId{"parent-run"});
  EXPECT_EQ(loaded->snapshot.parent_plan_id, WorkflowPlanId{"parent-plan"});
  EXPECT_EQ(loaded->snapshot.repair_revision, 2U);
  EXPECT_EQ(loaded->snapshot.repair_reason, "repair schema");
  ASSERT_TRUE(loaded->snapshot.failure.has_value());
  EXPECT_EQ(loaded->snapshot.failure->code, "workflow_failed");
  EXPECT_EQ(materialize(loaded->snapshot.failure->details)["source"]
                .as<std::string>(),
            "codec");
  ASSERT_EQ(loaded->snapshot.failure->artifacts.size(), 1U);
  EXPECT_EQ(loaded->snapshot.failure->artifacts.front().name, "details");
  EXPECT_EQ(loaded->snapshot.failure->artifacts.front().artifact.artifact_id,
            ArtifactId{"failure-artifact"});
  ASSERT_EQ(loaded->snapshot.tasks.size(), 3U);
  ASSERT_EQ(loaded->snapshot.tasks.front().attempts.size(), 2U);
  EXPECT_FALSE(loaded->snapshot.tasks.front().active_attempt_id.has_value());
  EXPECT_EQ(loaded->snapshot.tasks[1].reused_from_run_id,
            WorkflowRunId{"parent-run"});
  ASSERT_TRUE(loaded->snapshot.tasks.front().failure.has_value());
  EXPECT_EQ(loaded->snapshot.tasks.front().failure->code,
            "retries_exhausted");
  EXPECT_EQ(loaded->snapshot.tasks.front().attempts.front().state,
            AttemptState::TimedOut);
  EXPECT_FALSE(loaded->snapshot.tasks.front()
                   .attempts.front()
                   .termination_reason.has_value());
  ASSERT_TRUE(
      loaded->snapshot.tasks.front().attempts.front().failure.has_value());
  EXPECT_EQ(loaded->snapshot.tasks.front().attempts.front().failure->code,
            "deadline_exceeded");
  ASSERT_EQ(loaded->values.size(), 7U);
  EXPECT_TRUE(std::holds_alternative<std::monostate>(loaded->values[0].value));
  EXPECT_EQ(std::get<bool>(loaded->values[1].value), true);
  EXPECT_EQ(std::get<std::int64_t>(loaded->values[2].value), 42);
  EXPECT_DOUBLE_EQ(std::get<double>(loaded->values[3].value), 3.25);
  EXPECT_EQ(std::get<std::string>(loaded->values[4].value), "text");
  EXPECT_TRUE(std::holds_alternative<JsonPayload>(loaded->values[5].value));
  const auto &loaded_artifact =
      std::get<ArtifactRef>(loaded->values[6].value);
  EXPECT_EQ(loaded_artifact.artifact_id, artifact.artifact_id);
  EXPECT_EQ(loaded_artifact.media_type, artifact.media_type);
  EXPECT_EQ(loaded_artifact.size_bytes, artifact.size_bytes);
  EXPECT_EQ(loaded_artifact.digest, artifact.digest);

  const auto evidence_file = directory / "evidence.jsonl";
  auto ledger = open_test_evidence(evidence_file, 10);
  auto metadata = JsonPayload::from(
      glz::obj{"attempt", std::int64_t{1}});
  ASSERT_TRUE(metadata.has_value()) << metadata.error().message();
  EvidenceRecord record{
      .evidence_id = EvidenceId{"evidence-rich"},
      .run_id = WorkflowRunId{"rich-run"},
      .node_id = WorkflowNodeId{"task"},
      .type = EvidenceType::AttemptCompleted,
      .timestamp = std::chrono::system_clock::time_point{
          std::chrono::milliseconds{600}},
      .actor = Principal{.subject = "tester", .roles = {"operator"}},
      .metadata = std::move(*metadata),
      .artifact = artifact,
      .content_digest = "evidence-digest",
  };
  ASSERT_TRUE(ledger->append(std::move(record)).has_value());
  auto reloaded_ledger = open_test_evidence(evidence_file, 10);
  const auto records =
      reloaded_ledger->records(WorkflowRunId{"rich-run"});
  ASSERT_EQ(records.size(), 1U);
  EXPECT_EQ(records.front().actor.subject, "tester");
  ASSERT_TRUE(records.front().artifact.has_value());
  EXPECT_EQ(records.front().artifact->artifact_id, artifact.artifact_id);
  EXPECT_EQ(records.front().artifact->media_type, artifact.media_type);
  EXPECT_EQ(records.front().artifact->size_bytes, artifact.size_bytes);
  EXPECT_EQ(records.front().artifact->digest, artifact.digest);
  EXPECT_EQ(records.front().content_digest, "evidence-digest");

  std::ifstream input(directory / "runs" / "rich-run.json",
                      std::ios::binary);
  std::string checkpoint_json(std::istreambuf_iterator<char>(input), {});
  ASSERT_FALSE(glz::write_at<"/payload/snapshot/run_id">(
      R"("invalid-failure")", checkpoint_json));
  ASSERT_FALSE(glz::write_at<"/payload/snapshot/failure/kind">(
      R"("not_an_error")", checkpoint_json));
  {
    std::ofstream output(directory / "runs" / "invalid-failure.json",
                         std::ios::binary | std::ios::trunc);
    output << checkpoint_json;
  }
  CheckpointStore invalid_failure_store(
      directory / "runs", kStorageDefaults.max_checkpoint_bytes);
  EXPECT_EQ(
      invalid_failure_store.load(WorkflowRunId{"invalid-failure"}).error(),
      make_error_code(Error::ParseError));

  auto invalid_checkpoint = checkpoint;
  invalid_checkpoint.snapshot.failure = ExecutionFailure{
      .kind = Error::Success,
      .code = {},
      .message = {},
  };
  EXPECT_EQ(store.save(invalid_checkpoint).error(),
            make_error_code(Error::InvalidArgument));

  auto over_budget_checkpoint = checkpoint;
  over_budget_checkpoint.plan.policy.budget.max_total_output_bytes = 1;
  EXPECT_EQ(store.save(std::move(over_budget_checkpoint)).error(),
            make_error_code(Error::ResourceExhausted));

  auto failed_output_checkpoint = checkpoint;
  failed_output_checkpoint.snapshot.tasks[2].state = TaskState::Failed;
  failed_output_checkpoint.snapshot.tasks[2].failure = make_execution_failure(
      Error::Unknown, "value_task_failed", "Value task failed");
  EXPECT_EQ(store.save(std::move(failed_output_checkpoint)).error(),
            make_error_code(Error::InvalidArgument));

  auto missing_published_output = checkpoint;
  missing_published_output.snapshot.state = RunState::Succeeded;
  missing_published_output.snapshot.stop_intent.reset();
  missing_published_output.snapshot.stop_reason.clear();
  missing_published_output.snapshot.failure.reset();
  missing_published_output.snapshot.tasks[0].state = TaskState::Skipped;
  missing_published_output.snapshot.tasks[0].failure.reset();
  missing_published_output.plan.outputs = {
      OutputRef{.node_id = WorkflowNodeId{"values"},
                .port = WorkflowPortId{"string"}}};
  std::erase_if(missing_published_output.values, [](const auto &entry) {
    return entry.output.node_id == WorkflowNodeId{"values"} &&
           entry.output.port == WorkflowPortId{"string"};
  });
  EXPECT_EQ(store.save(std::move(missing_published_output)).error(),
            make_error_code(Error::InvalidArgument));

  std::filesystem::remove_all(directory, error);
}

TEST(WorkflowControlPlaneTest, RestoresLooksUpAndSortsRegisteredPlans) {
  TestExecutorEnvironment environment;
  AdmissionConfig admission;
  admission.allowed_executors = {"test"};
  WorkflowControlPlane control{environment.registry, PlanValidator{admission}};

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
