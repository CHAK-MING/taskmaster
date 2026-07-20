#include "dagforge/core/runtime.hpp"
#include "dagforge/executors/transform/executor.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/workflow/task_executor.hpp"

#include "json_test_utils.hpp"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <format>
#include <future>
#include <memory>
#include <ranges>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace dagforge::executors::transform::test {
namespace {

using dagforge::test::materialize;
using dagforge::test::parse_payload;

[[nodiscard]] auto
compile_context(std::span<const workflow::InputBinding> inputs,
                std::span<const WorkflowPortId> outputs)
    -> workflow::ExecutorCompileContext {
  return {.inputs = inputs, .outputs = outputs};
}

[[nodiscard]] auto
compile_expression(const std::shared_ptr<workflow::ITaskExecutor> &executor,
                   std::string_view expression,
                   std::span<const workflow::InputBinding> inputs,
                   std::span<const WorkflowPortId> outputs)
    -> workflow::ExecutorCompileResult<workflow::CompiledExecutorConfig> {
  auto config =
      JsonPayload::from(glz::obj{"expression", std::string{expression}});
  if (!config) {
    return workflow::executor_compile_fail(
        workflow::make_executor_compile_failure(
            config.error(), "transform_config_encode_failed",
            "Transform configuration could not be encoded"));
  }
  return executor->compile(std::move(*config),
                           compile_context(inputs, outputs));
}

template <typename T>
[[nodiscard]] auto shared_value(T value)
    -> std::shared_ptr<const workflow::WorkflowValue> {
  return std::make_shared<const workflow::WorkflowValue>(std::move(value));
}

[[nodiscard]] auto
execute(const std::shared_ptr<workflow::ITaskExecutor> &executor,
        workflow::CompiledExecutorConfig config,
        workflow::ExecutorInputs inputs, std::vector<WorkflowPortId> outputs,
        std::chrono::seconds timeout = std::chrono::seconds(2),
        std::string instance = "transform-test")
    -> workflow::TaskExecutionResult {
  auto completion =
      std::make_shared<std::promise<workflow::TaskExecutionResult>>();
  auto future = completion->get_future();
  workflow::TaskExecutionSink sink{
      .on_complete =
          [completion](const InstanceId &,
                       workflow::TaskExecutionResult result) mutable {
            completion->set_value(std::move(result));
          },
  };
  auto started = executor->start(
      workflow::TaskExecutionRequest{
          .instance_id = InstanceId{std::move(instance)},
          .config = std::move(config),
          .inputs = std::move(inputs),
          .outputs = std::move(outputs),
          .timeout = timeout,
      },
      std::move(sink));
  if (!started) {
    return workflow::task_failed(workflow::make_execution_failure(
        started.error(), "executor_start_failed",
        "Transform executor rejected the start request"));
  }
  if (future.wait_for(std::chrono::seconds(5)) != std::future_status::ready) {
    return workflow::task_failed(workflow::make_execution_failure(
        Error::Timeout, "test_wait_timed_out",
        "Test timed out waiting for Transform executor completion"));
  }
  return future.get();
}

[[nodiscard]] auto output_value(const workflow::ExecutorOutputs &outputs,
                                std::string_view port)
    -> const workflow::WorkflowValue * {
  const auto found = std::ranges::find_if(
      outputs, [&](const auto &entry) { return entry.first == port; });
  return found == outputs.end() ? nullptr : &found->second;
}

} // namespace

TEST(TransformTaskExecutorTest, CompilesStrictConfigAndExpression) {
  Runtime runtime(2);
  auto executor = create_task_executor(runtime);
  ASSERT_TRUE(executor.has_value()) << executor.error().message();
  EXPECT_EQ((*executor)->type(), "transform");

  const std::array inputs{workflow::InputBinding{
      .input = WorkflowPortId{"value"},
      .source = workflow::OutputRef{.node_id = WorkflowNodeId{"upstream"},
                                    .port = WorkflowPortId{"result"}},
  }};
  const std::array outputs{WorkflowPortId{"result"}};

  auto compiled = compile_expression(*executor, "$value + 1", inputs, outputs);
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();
  EXPECT_EQ(materialize(compiled->encoded())["expression"].as<std::string>(),
            "$value + 1");

  auto invalid_expression =
      compile_expression(*executor, "value +", inputs, outputs);
  ASSERT_FALSE(invalid_expression.has_value());
  EXPECT_EQ(invalid_expression.error().kind, Error::InvalidArgument);
  EXPECT_EQ(invalid_expression.error().code, "transform_expression_invalid");
  EXPECT_EQ(invalid_expression.error().path, "/expression");
  const auto invalid_details = materialize(invalid_expression.error().details);
  EXPECT_EQ(invalid_details["jsonata_code"].as<std::string>(), "S0207");

  auto unknown_key = (*executor)->compile(
      parse_payload(R"({"expression":"value","unknown":true})"),
      compile_context(inputs, outputs));
  ASSERT_FALSE(unknown_key.has_value());
  EXPECT_EQ(unknown_key.error().kind, Error::ParseError);
  EXPECT_EQ(unknown_key.error().code, "transform_config_invalid");
  EXPECT_EQ(unknown_key.error().path, "");

  const std::array<WorkflowPortId, 0> no_outputs{};
  auto missing_output =
      compile_expression(*executor, "value", inputs, no_outputs);
  ASSERT_FALSE(missing_output.has_value());
  EXPECT_EQ(missing_output.error().kind, Error::InvalidArgument);
  EXPECT_EQ(missing_output.error().code, "transform_outputs_required");
}

TEST(TransformTaskExecutorTest, RollsBackAcceptedStateWhenStartCallbackThrows) {
  Runtime runtime(1);
  auto executor = create_task_executor(runtime);
  ASSERT_TRUE(executor.has_value()) << executor.error().message();
  const std::array<workflow::InputBinding, 0> inputs{};
  const std::array outputs{WorkflowPortId{"result"}};
  auto compiled = compile_expression(*executor, "1", inputs, outputs);
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  auto started = (*executor)->start(
      workflow::TaskExecutionRequest{
          .instance_id = InstanceId{"transform-state-throw"},
          .config = std::move(*compiled),
          .outputs = {WorkflowPortId{"result"}},
      },
      workflow::TaskExecutionSink{
          .on_state =
              [](const InstanceId &, std::string_view) {
                throw std::runtime_error{"state callback failed"};
              },
      });
  ASSERT_FALSE(started.has_value());
  EXPECT_EQ(started.error(), make_error_code(Error::Unknown));
  EXPECT_TRUE((*executor)->quiesce(std::chrono::milliseconds(100)).has_value());
}

TEST(TransformTaskExecutorTest, CommitsRunningStateBeforeWorkerExecution) {
  Runtime runtime(1);
  auto executor = create_task_executor(runtime);
  ASSERT_TRUE(executor.has_value()) << executor.error().message();
  const std::array<workflow::InputBinding, 0> inputs{};
  const std::array outputs{WorkflowPortId{"result"}};
  auto compiled = compile_expression(*executor, "1", inputs, outputs);
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  std::promise<void> running_entered;
  auto running_entered_future = running_entered.get_future();
  std::promise<void> release_running;
  auto release_running_future = release_running.get_future().share();
  auto completion =
      std::make_shared<std::promise<workflow::TaskExecutionResult>>();
  auto completion_future = completion->get_future();
  auto started = std::async(std::launch::async, [&] {
    return (*executor)->start(
        workflow::TaskExecutionRequest{
            .instance_id = InstanceId{"transform-running-commit"},
            .config = std::move(*compiled),
            .outputs = {WorkflowPortId{"result"}},
        },
        workflow::TaskExecutionSink{
            .on_state =
                [&running_entered, release_running_future](
                    const InstanceId &, std::string_view) mutable {
                  running_entered.set_value();
                  release_running_future.wait();
                },
            .on_complete =
                [completion](const InstanceId &,
                             workflow::TaskExecutionResult result) mutable {
                  completion->set_value(std::move(result));
                },
        });
  });
  ASSERT_EQ(running_entered_future.wait_for(std::chrono::seconds(1)),
            std::future_status::ready);
  EXPECT_EQ(completion_future.wait_for(std::chrono::milliseconds(50)),
            std::future_status::timeout);
  auto quiesced = std::async(std::launch::async, [&] {
    return (*executor)->quiesce(std::chrono::seconds(2));
  });
  EXPECT_EQ(quiesced.wait_for(std::chrono::milliseconds(50)),
            std::future_status::timeout);
  release_running.set_value();
  ASSERT_EQ(started.wait_for(std::chrono::seconds(1)),
            std::future_status::ready);
  EXPECT_TRUE(started.get().has_value());
  ASSERT_EQ(completion_future.wait_for(std::chrono::seconds(1)),
            std::future_status::ready);
  auto completed = completion_future.get();
  ASSERT_FALSE(completed.has_value());
  EXPECT_EQ(completed.error().kind, Error::Cancelled);
  ASSERT_EQ(quiesced.wait_for(std::chrono::seconds(1)),
            std::future_status::ready);
  EXPECT_TRUE(quiesced.get().has_value());
}

TEST(TransformTaskExecutorTest, EvaluatesNamedInputDocument) {
  Runtime runtime(2);
  auto executor = create_task_executor(runtime);
  ASSERT_TRUE(executor.has_value()) << executor.error().message();

  const std::array inputs{
      workflow::InputBinding{
          .input = WorkflowPortId{"greeting"},
          .source = workflow::OutputRef{.node_id = WorkflowNodeId{"upstream"},
                                        .port = WorkflowPortId{"greeting"}},
      },
      workflow::InputBinding{
          .input = WorkflowPortId{"count"},
          .source = workflow::OutputRef{.node_id = WorkflowNodeId{"upstream"},
                                        .port = WorkflowPortId{"count"}},
      },
      workflow::InputBinding{
          .input = WorkflowPortId{"payload"},
          .source = workflow::OutputRef{.node_id = WorkflowNodeId{"upstream"},
                                        .port = WorkflowPortId{"payload"}},
      },
  };
  const std::array outputs{WorkflowPortId{"result"}};
  auto compiled = compile_expression(
      *executor, R"($greeting & ":" & $string($count) & ":" & $payload.name)",
      inputs, outputs);
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  workflow::ExecutorInputs values;
  values.emplace("greeting", shared_value(std::string{"hello"}));
  values.emplace("count", shared_value(std::int64_t{2}));
  values.emplace("payload", shared_value(parse_payload(R"({"name":"Ada"})")));
  auto result = execute(*executor, std::move(*compiled), std::move(values),
                        {WorkflowPortId{"result"}});
  ASSERT_TRUE(result.has_value())
      << (result ? std::string{} : result.error().message);
  const auto *value = output_value(*result, "result");
  ASSERT_NE(value, nullptr);
  ASSERT_TRUE(std::holds_alternative<std::string>(*value));
  EXPECT_EQ(std::get<std::string>(*value), "hello:2:Ada");
}

TEST(TransformTaskExecutorTest, KeepsEveryInputNameAccessibleFromRoot) {
  Runtime runtime(1);
  auto executor = create_task_executor(runtime);
  ASSERT_TRUE(executor.has_value()) << executor.error().message();
  const std::array inputs{workflow::InputBinding{
      .input = WorkflowPortId{"hyphen-name"},
      .source = workflow::OutputRef{.node_id = WorkflowNodeId{"upstream"},
                                    .port = WorkflowPortId{"result"}},
  }};
  const std::array outputs{WorkflowPortId{"result"}};
  auto compiled = compile_expression(*executor, "$lookup($, 'hyphen-name')",
                                     inputs, outputs);
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();
  workflow::ExecutorInputs values;
  values.emplace("hyphen-name", shared_value(std::string{"available"}));
  auto result = execute(*executor, std::move(*compiled), std::move(values),
                        {WorkflowPortId{"result"}});
  ASSERT_TRUE(result.has_value())
      << (result ? std::string{} : result.error().message);
  const auto *value = output_value(*result, "result");
  ASSERT_NE(value, nullptr);
  EXPECT_EQ(std::get<std::string>(*value), "available");
}

TEST(TransformTaskExecutorTest, ConvertsNullBooleanAndNumericResults) {
  Runtime runtime(1);
  auto executor = create_task_executor(runtime);
  ASSERT_TRUE(executor.has_value()) << executor.error().message();
  const std::array<workflow::InputBinding, 0> inputs{};

  const auto evaluate_result =
      [&](std::string_view expression,
          std::string instance) -> workflow::TaskExecutionResult {
    const std::array outputs{WorkflowPortId{"result"}};
    auto compiled = compile_expression(*executor, expression, inputs, outputs);
    EXPECT_TRUE(compiled.has_value())
        << (compiled ? std::string{} : compiled.error().message());
    return compiled ? execute(*executor, std::move(*compiled), {},
                              {WorkflowPortId{"result"}},
                              std::chrono::seconds(2), std::move(instance))
                    : workflow::task_failed(workflow::make_execution_failure(
                          Error::InvalidArgument, "test_compile_failed",
                          "Transform test expression did not compile"));
  };

  auto null_result = evaluate_result("null", "transform-null");
  ASSERT_TRUE(null_result.has_value());
  const auto *null_value = output_value(*null_result, "result");
  ASSERT_NE(null_value, nullptr);
  EXPECT_TRUE(std::holds_alternative<std::monostate>(*null_value));

  auto bool_result = evaluate_result("true", "transform-bool");
  ASSERT_TRUE(bool_result.has_value());
  const auto *bool_value = output_value(*bool_result, "result");
  ASSERT_NE(bool_value, nullptr);
  EXPECT_TRUE(std::get<bool>(*bool_value));

  auto number_result = evaluate_result("2 + 3", "transform-number");
  ASSERT_TRUE(number_result.has_value());
  const auto *number_value = output_value(*number_result, "result");
  ASSERT_NE(number_value, nullptr);
  EXPECT_EQ(std::get<double>(*number_value), 5.0);
}

TEST(TransformTaskExecutorTest, ProjectsExactMultiOutputObject) {
  Runtime runtime(2);
  auto executor = create_task_executor(runtime);
  ASSERT_TRUE(executor.has_value()) << executor.error().message();

  const std::array inputs{
      workflow::InputBinding{
          .input = WorkflowPortId{"left"},
          .source = workflow::OutputRef{.node_id = WorkflowNodeId{"upstream"},
                                        .port = WorkflowPortId{"left"}},
      },
      workflow::InputBinding{
          .input = WorkflowPortId{"right"},
          .source = workflow::OutputRef{.node_id = WorkflowNodeId{"upstream"},
                                        .port = WorkflowPortId{"right"}},
      },
  };
  const std::array outputs{WorkflowPortId{"sum"}, WorkflowPortId{"label"}};
  auto compiled = compile_expression(
      *executor, R"({"sum": $left + $right, "label": "total"})", inputs,
      outputs);
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();
  workflow::ExecutorInputs values;
  values.emplace("left", shared_value(std::int64_t{2}));
  values.emplace("right", shared_value(std::int64_t{3}));
  auto result = execute(*executor, std::move(*compiled), std::move(values),
                        {WorkflowPortId{"sum"}, WorkflowPortId{"label"}});
  ASSERT_TRUE(result.has_value())
      << (result ? std::string{} : result.error().message);
  ASSERT_EQ(result->size(), 2U);
  const auto *sum = output_value(*result, "sum");
  const auto *label = output_value(*result, "label");
  ASSERT_NE(sum, nullptr);
  ASSERT_NE(label, nullptr);
  ASSERT_TRUE(std::holds_alternative<double>(*sum));
  EXPECT_EQ(std::get<double>(*sum), 5.0);
  EXPECT_EQ(std::get<std::string>(*label), "total");

  auto invalid =
      compile_expression(*executor, R"({"sum": 5})", inputs, outputs);
  ASSERT_TRUE(invalid.has_value()) << invalid.error().message();
  workflow::ExecutorInputs invalid_values;
  invalid_values.emplace("left", shared_value(std::int64_t{2}));
  invalid_values.emplace("right", shared_value(std::int64_t{3}));
  auto invalid_result =
      execute(*executor, std::move(*invalid), std::move(invalid_values),
              {WorkflowPortId{"sum"}, WorkflowPortId{"label"}},
              std::chrono::seconds(2), "transform-shape");
  ASSERT_FALSE(invalid_result.has_value());
  EXPECT_EQ(invalid_result.error().code, "transform_output_shape_invalid");

  auto extra = compile_expression(
      *executor, R"({"sum": $left + $right, "label": "total", "extra": true})",
      inputs, outputs);
  ASSERT_TRUE(extra.has_value()) << extra.error().message();
  workflow::ExecutorInputs extra_values;
  extra_values.emplace("left", shared_value(std::int64_t{2}));
  extra_values.emplace("right", shared_value(std::int64_t{3}));
  auto extra_result =
      execute(*executor, std::move(*extra), std::move(extra_values),
              {WorkflowPortId{"sum"}, WorkflowPortId{"label"}},
              std::chrono::seconds(2), "transform-extra-shape");
  ASSERT_FALSE(extra_result.has_value());
  EXPECT_EQ(extra_result.error().code, "transform_output_shape_invalid");
}

TEST(TransformTaskExecutorTest, PreservesStructuredValuesAndArtifactMetadata) {
  Runtime runtime(2);
  auto executor = create_task_executor(runtime);
  ASSERT_TRUE(executor.has_value()) << executor.error().message();

  const std::array inputs{workflow::InputBinding{
      .input = WorkflowPortId{"artifact"},
      .source = workflow::OutputRef{.node_id = WorkflowNodeId{"upstream"},
                                    .port = WorkflowPortId{"artifact"}},
  }};
  const std::array outputs{WorkflowPortId{"result"}};
  auto compiled = compile_expression(
      *executor,
      R"({"id": $artifact.artifact_id, "bytes": $artifact.size_bytes, "kind": $artifact.type})",
      inputs, outputs);
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();
  workflow::ExecutorInputs values;
  values.emplace("artifact", shared_value(workflow::ArtifactRef{
                                 .artifact_id = ArtifactId{"artifact-a"},
                                 .media_type = "application/json",
                                 .size_bytes = 7,
                                 .digest = "sha256:test",
                             }));
  auto result = execute(*executor, std::move(*compiled), std::move(values),
                        {WorkflowPortId{"result"}});
  ASSERT_TRUE(result.has_value())
      << (result ? std::string{} : result.error().message);
  const auto *value = output_value(*result, "result");
  ASSERT_NE(value, nullptr);
  ASSERT_TRUE(std::holds_alternative<JsonPayload>(*value));
  const auto json = materialize(std::get<JsonPayload>(*value));
  EXPECT_EQ(json["id"].as<std::string>(), "artifact-a");
  EXPECT_EQ(json["bytes"].as<std::int64_t>(), 7);
  EXPECT_EQ(json["kind"].as<std::string>(), "artifact");

  auto passthrough =
      compile_expression(*executor, "$artifact", inputs, outputs);
  ASSERT_TRUE(passthrough.has_value()) << passthrough.error().message();
  workflow::ExecutorInputs passthrough_values;
  passthrough_values.emplace("artifact",
                             shared_value(workflow::ArtifactRef{
                                 .artifact_id = ArtifactId{"artifact-b"},
                                 .media_type = "application/octet-stream",
                                 .size_bytes = 9,
                                 .digest = "sha256:other",
                             }));
  auto passthrough_result =
      execute(*executor, std::move(*passthrough), std::move(passthrough_values),
              {WorkflowPortId{"result"}}, std::chrono::seconds(2),
              "transform-artifact-passthrough");
  ASSERT_TRUE(passthrough_result.has_value());
  const auto *passthrough_value = output_value(*passthrough_result, "result");
  ASSERT_NE(passthrough_value, nullptr);
  EXPECT_TRUE(std::holds_alternative<JsonPayload>(*passthrough_value));
  EXPECT_FALSE(
      std::holds_alternative<workflow::ArtifactRef>(*passthrough_value));
}

TEST(TransformTaskExecutorTest, MapsLanguageAndResultFailures) {
  Runtime runtime(2);
  auto executor = create_task_executor(runtime);
  ASSERT_TRUE(executor.has_value()) << executor.error().message();
  const std::array<workflow::InputBinding, 0> inputs{};
  const std::array outputs{WorkflowPortId{"result"}};

  auto dynamic = compile_expression(*executor, "1 + 'x'", inputs, outputs);
  ASSERT_TRUE(dynamic.has_value()) << dynamic.error().message();
  auto dynamic_result =
      execute(*executor, std::move(*dynamic), {}, {WorkflowPortId{"result"}},
              std::chrono::seconds(2), "transform-dynamic");
  ASSERT_FALSE(dynamic_result.has_value());
  EXPECT_EQ(dynamic_result.error().code, "transform_evaluation_failed");
  const auto details = materialize(dynamic_result.error().details);
  EXPECT_EQ(details["jsonata_code"].as<std::string>(), "T2002");

  auto undefined = compile_expression(*executor, "missing", inputs, outputs);
  ASSERT_TRUE(undefined.has_value()) << undefined.error().message();
  auto undefined_result =
      execute(*executor, std::move(*undefined), {}, {WorkflowPortId{"result"}},
              std::chrono::seconds(2), "transform-undefined");
  ASSERT_FALSE(undefined_result.has_value());
  EXPECT_EQ(undefined_result.error().code, "transform_result_undefined");

  auto function =
      compile_expression(*executor, "function($x){$x}", inputs, outputs);
  ASSERT_TRUE(function.has_value()) << function.error().message();
  auto function_result =
      execute(*executor, std::move(*function), {}, {WorkflowPortId{"result"}},
              std::chrono::seconds(2), "transform-function");
  ASSERT_FALSE(function_result.has_value());
  EXPECT_EQ(function_result.error().code, "transform_result_not_json");

  auto recursive = compile_expression(
      *executor,
      "($sum := function($n){$n = 0 ? 0 : $n + $sum($n - 1)}; "
      "$sum(2000))",
      inputs, outputs);
  ASSERT_TRUE(recursive.has_value()) << recursive.error().message();
  auto recursive_result =
      execute(*executor, std::move(*recursive), {}, {WorkflowPortId{"result"}},
              std::chrono::seconds(2), "transform-resource");
  ASSERT_FALSE(recursive_result.has_value());
  EXPECT_EQ(recursive_result.error().kind, Error::ResourceExhausted);
  EXPECT_EQ(recursive_result.error().code, "transform_resource_exhausted");
}

TEST(TransformTaskExecutorTest, ReusesOneCompiledProgramAcrossWorkers) {
  Runtime runtime(4);
  auto executor = create_task_executor(runtime);
  ASSERT_TRUE(executor.has_value()) << executor.error().message();
  const std::array inputs{workflow::InputBinding{
      .input = WorkflowPortId{"value"},
      .source = workflow::OutputRef{.node_id = WorkflowNodeId{"upstream"},
                                    .port = WorkflowPortId{"result"}},
  }};
  const std::array outputs{WorkflowPortId{"result"}};
  auto compiled = compile_expression(*executor, "$value * 2", inputs, outputs);
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  constexpr std::size_t kTasks = 16;
  std::vector<std::future<workflow::TaskExecutionResult>> futures;
  futures.reserve(kTasks);
  for (std::size_t index = 0; index < kTasks; ++index) {
    auto completion =
        std::make_shared<std::promise<workflow::TaskExecutionResult>>();
    futures.push_back(completion->get_future());
    workflow::ExecutorInputs values;
    values.emplace("value", shared_value(static_cast<std::int64_t>(index)));
    auto started = (*executor)->start(
        workflow::TaskExecutionRequest{
            .instance_id =
                InstanceId{std::format("transform-parallel-{}", index)},
            .config = *compiled,
            .inputs = std::move(values),
            .outputs = {WorkflowPortId{"result"}},
            .timeout = std::chrono::seconds(2),
        },
        workflow::TaskExecutionSink{
            .on_complete =
                [completion](const InstanceId &,
                             workflow::TaskExecutionResult result) mutable {
                  completion->set_value(std::move(result));
                },
        });
    ASSERT_TRUE(started.has_value()) << started.error().message();
  }
  for (std::size_t index = 0; index < futures.size(); ++index) {
    auto result = futures[index].get();
    ASSERT_TRUE(result.has_value())
        << (result ? std::string{} : result.error().message);
    const auto *value = output_value(*result, "result");
    ASSERT_NE(value, nullptr);
    EXPECT_EQ(std::get<double>(*value), static_cast<double>(index * 2));
  }
  EXPECT_TRUE((*executor)->quiesce(std::chrono::seconds(1)).has_value());
}

TEST(TransformTaskExecutorTest, CancelsTimesOutAndQuiesces) {
  Runtime runtime(1);
  auto executor = create_task_executor(runtime);
  ASSERT_TRUE(executor.has_value()) << executor.error().message();
  const std::array<workflow::InputBinding, 0> inputs{};
  const std::array outputs{WorkflowPortId{"result"}};
  constexpr std::string_view kLoop =
      "($loop := function($n){$loop($n + 1)}; $loop(0))";

  auto cancellable = compile_expression(*executor, kLoop, inputs, outputs);
  ASSERT_TRUE(cancellable.has_value()) << cancellable.error().message();
  auto completion =
      std::make_shared<std::promise<workflow::TaskExecutionResult>>();
  auto future = completion->get_future();
  std::promise<void> running;
  auto running_future = running.get_future();
  std::atomic_bool running_signalled{false};
  workflow::TaskExecutionSink sink{
      .on_state =
          [&](const InstanceId &instance_id, std::string_view state) {
            if (state == "running" && !running_signalled.exchange(true)) {
              running.set_value();
              (*executor)->cancel(instance_id);
            }
          },
      .on_complete =
          [completion](const InstanceId &,
                       workflow::TaskExecutionResult result) mutable {
            completion->set_value(std::move(result));
          },
  };
  auto started = (*executor)->start(
      workflow::TaskExecutionRequest{
          .instance_id = InstanceId{"transform-cancel"},
          .config = std::move(*cancellable),
          .outputs = {WorkflowPortId{"result"}},
          .timeout = std::chrono::seconds(5),
      },
      std::move(sink));
  ASSERT_TRUE(started.has_value()) << started.error().message();
  ASSERT_EQ(running_future.wait_for(std::chrono::seconds(1)),
            std::future_status::ready);
  ASSERT_EQ(future.wait_for(std::chrono::seconds(2)),
            std::future_status::ready);
  auto cancelled = future.get();
  ASSERT_FALSE(cancelled.has_value());
  EXPECT_EQ(cancelled.error().kind, Error::Cancelled);
  EXPECT_EQ(cancelled.error().code, "transform_cancelled");

  auto timed = compile_expression(*executor, kLoop, inputs, outputs);
  ASSERT_TRUE(timed.has_value()) << timed.error().message();
  auto timed_result =
      execute(*executor, std::move(*timed), {}, {WorkflowPortId{"result"}},
              std::chrono::seconds::zero(), "transform-timeout");
  ASSERT_FALSE(timed_result.has_value());
  EXPECT_EQ(timed_result.error().kind, Error::Timeout);
  EXPECT_EQ(timed_result.error().code, "transform_timed_out");

  auto quiesced_program = compile_expression(*executor, kLoop, inputs, outputs);
  ASSERT_TRUE(quiesced_program.has_value())
      << quiesced_program.error().message();
  auto quiesced_completion =
      std::make_shared<std::promise<workflow::TaskExecutionResult>>();
  auto quiesced_future = quiesced_completion->get_future();
  std::promise<void> quiesced_running;
  auto quiesced_running_future = quiesced_running.get_future();
  auto quiesced_started = (*executor)->start(
      workflow::TaskExecutionRequest{
          .instance_id = InstanceId{"transform-quiesce"},
          .config = std::move(*quiesced_program),
          .outputs = {WorkflowPortId{"result"}},
          .timeout = std::chrono::seconds(5),
      },
      workflow::TaskExecutionSink{
          .on_state =
              [&quiesced_running](const InstanceId &, std::string_view state) {
                if (state == "running") {
                  quiesced_running.set_value();
                }
              },
          .on_complete =
              [quiesced_completion](
                  const InstanceId &,
                  workflow::TaskExecutionResult result) mutable {
                quiesced_completion->set_value(std::move(result));
              },
      });
  ASSERT_TRUE(quiesced_started.has_value())
      << quiesced_started.error().message();
  ASSERT_EQ(quiesced_running_future.wait_for(std::chrono::seconds(1)),
            std::future_status::ready);
  ASSERT_TRUE((*executor)->quiesce(std::chrono::seconds(2)).has_value());
  ASSERT_EQ(quiesced_future.wait_for(std::chrono::seconds(1)),
            std::future_status::ready);
  auto quiesced = quiesced_future.get();
  ASSERT_FALSE(quiesced.has_value());
  EXPECT_EQ(quiesced.error().kind, Error::Cancelled);

  auto after_quiesce = (*executor)->start(
      workflow::TaskExecutionRequest{
          .instance_id = InstanceId{"transform-after-quiesce"},
          .config = workflow::CompiledExecutorConfig::from_encoded(
              parse_payload(R"({"expression":"1"})")),
          .outputs = {WorkflowPortId{"result"}},
      },
      {});
  ASSERT_FALSE(after_quiesce.has_value());
  EXPECT_EQ(after_quiesce.error(), make_error_code(Error::InvalidState));
}

TEST(TransformTaskExecutorTest, QuiesceWaitsForCompletionCallbacks) {
  Runtime runtime(1);
  auto executor = create_task_executor(runtime);
  ASSERT_TRUE(executor.has_value()) << executor.error().message();
  const std::array<workflow::InputBinding, 0> inputs{};
  const std::array outputs{WorkflowPortId{"result"}};
  auto compiled = compile_expression(*executor, "1", inputs, outputs);
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  std::promise<void> callback_entered;
  auto callback_entered_future = callback_entered.get_future();
  std::promise<void> release_callback;
  auto release_callback_future = release_callback.get_future().share();
  auto started = (*executor)->start(
      workflow::TaskExecutionRequest{
          .instance_id = InstanceId{"transform-callback"},
          .config = std::move(*compiled),
          .outputs = {WorkflowPortId{"result"}},
          .timeout = std::chrono::seconds(2),
      },
      workflow::TaskExecutionSink{
          .on_complete =
              [&callback_entered, release_callback_future](
                  const InstanceId &, workflow::TaskExecutionResult) mutable {
                callback_entered.set_value();
                release_callback_future.wait();
              },
      });
  ASSERT_TRUE(started.has_value()) << started.error().message();
  ASSERT_EQ(callback_entered_future.wait_for(std::chrono::seconds(1)),
            std::future_status::ready);

  auto quiesced = std::async(std::launch::async, [&] {
    return (*executor)->quiesce(std::chrono::seconds(2));
  });
  EXPECT_EQ(quiesced.wait_for(std::chrono::milliseconds(50)),
            std::future_status::timeout);
  release_callback.set_value();
  ASSERT_EQ(quiesced.wait_for(std::chrono::seconds(1)),
            std::future_status::ready);
  EXPECT_TRUE(quiesced.get().has_value());
}

TEST(TransformTaskExecutorTest, QuiesceTimeoutRemainsTerminal) {
  Runtime runtime(1);
  auto executor = create_task_executor(runtime);
  ASSERT_TRUE(executor.has_value()) << executor.error().message();
  const std::array<workflow::InputBinding, 0> inputs{};
  const std::array outputs{WorkflowPortId{"result"}};
  auto compiled = compile_expression(*executor, "1", inputs, outputs);
  auto rejected_config = compile_expression(*executor, "2", inputs, outputs);
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();
  ASSERT_TRUE(rejected_config.has_value()) << rejected_config.error().message();

  std::promise<void> callback_entered;
  auto callback_entered_future = callback_entered.get_future();
  std::promise<void> release_callback;
  auto release_callback_future = release_callback.get_future().share();
  auto started = (*executor)->start(
      workflow::TaskExecutionRequest{
          .instance_id = InstanceId{"transform-quiesce-timeout"},
          .config = std::move(*compiled),
          .outputs = {WorkflowPortId{"result"}},
          .timeout = std::chrono::seconds(2),
      },
      workflow::TaskExecutionSink{
          .on_complete =
              [&callback_entered, release_callback_future](
                  const InstanceId &, workflow::TaskExecutionResult) mutable {
                callback_entered.set_value();
                release_callback_future.wait();
              },
      });
  ASSERT_TRUE(started.has_value()) << started.error().message();
  ASSERT_EQ(callback_entered_future.wait_for(std::chrono::seconds(1)),
            std::future_status::ready);

  auto timed_out = (*executor)->quiesce(std::chrono::milliseconds::zero());
  ASSERT_FALSE(timed_out.has_value());
  EXPECT_EQ(timed_out.error(), make_error_code(Error::Timeout));

  auto rejected = (*executor)->start(
      workflow::TaskExecutionRequest{
          .instance_id = InstanceId{"transform-after-quiesce-timeout"},
          .config = std::move(*rejected_config),
          .outputs = {WorkflowPortId{"result"}},
      },
      {});
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error(), make_error_code(Error::InvalidState));

  release_callback.set_value();
  EXPECT_TRUE((*executor)->quiesce(std::chrono::seconds(1)).has_value());
}

TEST(TransformTaskExecutorTest, IncludesQueueWaitInNodeTimeout) {
  Runtime runtime(1);
  auto executor = create_task_executor(runtime);
  ASSERT_TRUE(executor.has_value()) << executor.error().message();
  const std::array<workflow::InputBinding, 0> inputs{};
  const std::array outputs{WorkflowPortId{"result"}};
  auto blocker = compile_expression(*executor, "1", inputs, outputs);
  auto queued = compile_expression(*executor, "2", inputs, outputs);
  ASSERT_TRUE(blocker.has_value()) << blocker.error().message();
  ASSERT_TRUE(queued.has_value()) << queued.error().message();

  std::promise<void> blocker_entered;
  auto blocker_entered_future = blocker_entered.get_future();
  std::promise<void> release_blocker;
  auto release_blocker_future = release_blocker.get_future().share();
  auto blocker_started = (*executor)->start(
      workflow::TaskExecutionRequest{
          .instance_id = InstanceId{"transform-queue-blocker"},
          .config = std::move(*blocker),
          .outputs = {WorkflowPortId{"result"}},
          .timeout = std::chrono::seconds(5),
      },
      workflow::TaskExecutionSink{
          .on_complete =
              [&blocker_entered, release_blocker_future](
                  const InstanceId &, workflow::TaskExecutionResult) mutable {
                blocker_entered.set_value();
                release_blocker_future.wait();
              },
      });
  ASSERT_TRUE(blocker_started.has_value()) << blocker_started.error().message();
  ASSERT_EQ(blocker_entered_future.wait_for(std::chrono::seconds(1)),
            std::future_status::ready);

  auto queued_completion =
      std::make_shared<std::promise<workflow::TaskExecutionResult>>();
  auto queued_future = queued_completion->get_future();
  auto queued_started = (*executor)->start(
      workflow::TaskExecutionRequest{
          .instance_id = InstanceId{"transform-queue-timeout"},
          .config = std::move(*queued),
          .outputs = {WorkflowPortId{"result"}},
          .timeout = std::chrono::seconds(1),
      },
      workflow::TaskExecutionSink{
          .on_complete =
              [queued_completion](
                  const InstanceId &,
                  workflow::TaskExecutionResult result) mutable {
                queued_completion->set_value(std::move(result));
              },
      });
  ASSERT_TRUE(queued_started.has_value()) << queued_started.error().message();
  std::this_thread::sleep_for(std::chrono::milliseconds(1100));
  release_blocker.set_value();

  ASSERT_EQ(queued_future.wait_for(std::chrono::seconds(1)),
            std::future_status::ready);
  auto timed_out = queued_future.get();
  ASSERT_FALSE(timed_out.has_value());
  EXPECT_EQ(timed_out.error().kind, Error::Timeout);
  EXPECT_EQ(timed_out.error().code, "transform_timed_out");
  EXPECT_TRUE((*executor)->quiesce(std::chrono::seconds(1)).has_value());
}

} // namespace dagforge::executors::transform::test
