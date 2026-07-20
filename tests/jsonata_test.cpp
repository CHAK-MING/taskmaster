#include "dagforge/jsonata/program.hpp"
#include "dagforge/util/json.hpp"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

namespace dagforge::jsonata::test {

namespace {

auto compile(std::string source) -> Program {
  auto program = Program::compile(CompileRequest{.source = std::move(source)});
  EXPECT_TRUE(program.has_value())
      << (program ? std::string{}
                  : program.error().code + ": " + program.error().message);
  return program ? std::move(*program) : Program{};
}

auto evaluate(const Program &program, std::string_view input)
    -> DiagnosticResult<EvaluationSuccess> {
  auto parsed = parse_json(input);
  EXPECT_TRUE(parsed.has_value()) << parsed.error().message();
  if (!parsed) {
    return std::unexpected(Failure{.kind = FailureKind::Host,
                                   .code = "H9000",
                                   .message = "test input parse failed"});
  }
  return program.evaluate(EvaluationRequest{.input = std::cref(*parsed)});
}

auto encoded(const EvaluationSuccess &result) -> std::string {
  if (!result.value) {
    return "undefined";
  }
  auto out = serialize_json(*result.value);
  EXPECT_TRUE(out.has_value()) << out.error().message();
  return out ? std::move(*out) : std::string{};
}

} // namespace

TEST(JsonataProgramTest, EvaluatesLiteralsArithmeticAndBinary64Numbers) {
  const auto program =
      compile(R"({"value": 1 + 2 * 3, "large": 9007199254740993})");
  const auto result = evaluate(program, "null");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(encoded(*result), R"({"value":7,"large":9007199254740992})");
}

TEST(JsonataProgramTest, EvaluatesPathsPredicatesObjectsAndConditionals) {
  const auto program = compile(
      R"({"names": people[age >= 18].name, "count": $count(people[age >= 18]), "status": $count(people) > 2 ? "many" : "few"})");
  const auto result = evaluate(
      program,
      R"({"people":[{"name":"Ada","age":36},{"name":"Lin","age":16},{"name":"Grace","age":28}]})");
  ASSERT_TRUE(result.has_value())
      << (result ? std::string{} : result.error().message);
  EXPECT_EQ(encoded(*result),
            R"({"names":["Ada","Grace"],"count":2,"status":"many"})");
}

TEST(JsonataProgramTest,
     SupportsBlocksVariablesLambdasAndHigherOrderFunctions) {
  const auto program =
      compile(R"(($double := function($v){$v * 2}; $map([1,2,3], $double)))");
  const auto result = evaluate(program, "null");
  ASSERT_TRUE(result.has_value())
      << (result ? std::string{} : result.error().message);
  EXPECT_EQ(encoded(*result), "[2,4,6]");
}

TEST(JsonataProgramTest, KeepsEscapedClosuresAliveUntilEvaluationCompletes) {
  const auto program =
      compile("($factory := function($value){function(){ $value }}; "
              "$closure := $factory(7); $closure())");
  const auto result = evaluate(program, "null");
  ASSERT_TRUE(result.has_value())
      << (result ? std::string{}
                 : result.error().code + ": " + result.error().message);
  EXPECT_EQ(encoded(*result), "7");
}

TEST(JsonataProgramTest, ReleasesClosureCyclesAfterJsonProjectionFailure) {
  const auto result =
      evaluate(compile("($function := function(){1}; [$function])"), "null");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code, "T1006");
}

TEST(JsonataProgramTest, ReleasesClosureCyclesAfterDynamicFailure) {
  const auto result = evaluate(
      compile("($recursive := function(){ $recursive() }; $error('stop'))"),
      "null");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code, "D3137");
}

TEST(JsonataProgramTest, PreservesUndefinedSeparatelyFromJsonNull) {
  const auto missing = evaluate(compile("missing.path"), R"({"x":1})");
  ASSERT_TRUE(missing.has_value());
  EXPECT_EQ(missing->kind, EvaluationValueKind::Undefined);
  EXPECT_FALSE(missing->value.has_value());

  const auto json_null = evaluate(compile("null"), R"({"x":1})");
  ASSERT_TRUE(json_null.has_value());
  EXPECT_EQ(json_null->kind, EvaluationValueKind::Json);
  ASSERT_TRUE(json_null->value.has_value());
  EXPECT_TRUE(json_null->value->is_null());
}

TEST(JsonataProgramTest, RepresentsTopLevelFunctionAsAValidLanguageValue) {
  const auto result = evaluate(compile("function($v){$v}"), "null");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->kind, EvaluationValueKind::Function);
  EXPECT_FALSE(result->value.has_value());
}

TEST(JsonataProgramTest, ReportsStableJsonataSyntaxCodesAndPositions) {
  const auto result = Program::compile(CompileRequest{.source = "1 +"});
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().kind, FailureKind::Syntax);
  EXPECT_EQ(result.error().code, "S0207");
  EXPECT_GT(result.error().position, 0U);
}

TEST(JsonataProgramTest, ReusesOneImmutableProgramAcrossThreads) {
  const auto program = compile("$$.value + $offset");
  const auto input = parse_json(R"({"value":40})");
  const auto offset = parse_json("2");
  ASSERT_TRUE(input.has_value());
  ASSERT_TRUE(offset.has_value());
  const std::array bindings{
      Binding{.name = "offset", .value = std::cref(*offset)}};

  std::atomic<int> successes{0};
  std::vector<std::jthread> workers;
  for (int index = 0; index < 8; ++index) {
    workers.emplace_back([&] {
      auto result = program.evaluate(
          EvaluationRequest{.input = std::cref(*input), .bindings = bindings});
      if (result && encoded(*result) == "42") {
        successes.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }
  workers.clear();
  EXPECT_EQ(successes.load(std::memory_order_relaxed), 8);
}

TEST(JsonataProgramTest, SharedSequenceFlatteningDoesNotCorruptBindings) {
  const auto program =
      compile("library.books#$pos.$[$substring(title,0,3) = 'The'].$pos");
  const auto result = evaluate(
      program,
      R"({"library":{"books":[{"title":"Structure and Interpretation of Computer Programs"},{"title":"The C Programming Language"},{"title":"The Art of Computer Programming"}]}})");
  ASSERT_TRUE(result.has_value())
      << (result ? std::string{} : result.error().message);
  ASSERT_TRUE(result->value.has_value());
  EXPECT_EQ(encoded(*result), "[1,2]");
}

TEST(JsonataProgramTest, PreservesNonFiniteNumbersUntilAConsumerRejectsThem) {
  const auto direct = evaluate(compile("1 / 0"), "null");
  ASSERT_FALSE(direct.has_value());
  EXPECT_EQ(direct.error().code, "D1001");

  const auto stringified = evaluate(compile("$string(1 / 0)"), "null");
  ASSERT_FALSE(stringified.has_value());
  EXPECT_EQ(stringified.error().code, "D3001");
}

TEST(JsonataProgramTest, FormatsCalendarEraAndRejectsMalformedDatePictures) {
  const auto formatted =
      evaluate(compile("$fromMillis(1521801216617, '[FNn], the [Dwo] of [MNn] "
                       "[Y] [E] [C]')"),
               "null");
  ASSERT_TRUE(formatted.has_value())
      << (formatted ? std::string{} : formatted.error().message);
  EXPECT_EQ(encoded(*formatted),
            R"("Friday, the twenty-third of March 2018 ISO ISO")");

  const auto malformed =
      evaluate(compile("$fromMillis(1419940800000, '[YN]-[M')"), "null");
  ASSERT_FALSE(malformed.has_value());
  EXPECT_EQ(malformed.error().code, "D3135");
}

TEST(JsonataProgramTest, RejectsCollidingObjectKeyExpressions) {
  const auto result = evaluate(
      compile("$.{ type: $average(value), kind: $sum(value) }"),
      R"([{"type":"a","kind":"a","value":0},{"type":"a","kind":"b","value":1},{"type":"b","kind":"a","value":2},{"type":"b","kind":"b","value":3}])");
  ASSERT_FALSE(result.has_value())
      << (result ? encoded(*result) : result.error().message);
  EXPECT_EQ(result.error().code, "D1009");
}

TEST(JsonataProgramTest, AppliesTransformUpdateAndDeleteContracts) {
  const auto unchanged =
      evaluate(compile("$ ~> |items|missing, 'description'|"),
               R"({"items":{"description":"old","value":1}})");
  ASSERT_TRUE(unchanged.has_value())
      << (unchanged ? std::string{} : unchanged.error().message);
  EXPECT_EQ(encoded(*unchanged), R"({"items":{"value":1}})");

  const auto invalid_update =
      evaluate(compile("$ ~> |items|5|"), R"({"items":{}})");
  ASSERT_FALSE(invalid_update.has_value());
  EXPECT_EQ(invalid_update.error().code, "T2011");

  const auto invalid_delete =
      evaluate(compile("$ ~> |items|{}, 5|"), R"({"items":{}})");
  ASSERT_FALSE(invalid_delete.has_value());
  EXPECT_EQ(invalid_delete.error().code, "T2012");
}

TEST(JsonataProgramTest, TrampolinesMutualTailCallsWithoutHidingRealRecursion) {
  const auto input = parse_json("null");
  ASSERT_TRUE(input.has_value());

  const auto mutual =
      compile("($even := function($n){$n = 0 ? true : $odd($n - 1)}; "
              "$odd := function($n){$n = 0 ? false : $even($n - 1)}; "
              "$even(1000))");
  EvaluationRequest mutual_request{.input = std::cref(*input)};
  mutual_request.limits.max_call_depth = 32;
  mutual_request.limits.max_steps = 1'000'000;
  mutual_request.limits.timeout = std::chrono::seconds{2};
  const auto mutual_result = mutual.evaluate(mutual_request);
  ASSERT_TRUE(mutual_result.has_value())
      << (mutual_result ? std::string{}
                        : mutual_result.error().code + ": " +
                              mutual_result.error().message);
  EXPECT_EQ(encoded(*mutual_result), "true");

  const auto recursive =
      compile("($sum := function($n){$n = 0 ? 0 : $n + $sum($n - 1)}; "
              "$sum(1000))");
  EvaluationRequest recursive_request{.input = std::cref(*input)};
  recursive_request.limits.max_call_depth = 32;
  recursive_request.limits.max_steps = 1'000'000;
  recursive_request.limits.timeout = std::chrono::seconds{2};
  const auto recursive_result = recursive.evaluate(recursive_request);
  ASSERT_FALSE(recursive_result.has_value());
  EXPECT_EQ(recursive_result.error().code, "D1011");
}

TEST(JsonataProgramTest, SharesStepBudgetAcrossNestedEval) {
  const auto input = parse_json("null");
  ASSERT_TRUE(input.has_value());
  const auto program = compile("$eval('1')");

  EvaluationRequest baseline_request{.input = std::cref(*input)};
  baseline_request.limits.max_steps = 1'000;
  baseline_request.limits.timeout = std::chrono::steady_clock::duration::zero();
  const auto baseline = program.evaluate(baseline_request);
  ASSERT_TRUE(baseline.has_value())
      << (baseline ? std::string{} : baseline.error().message);
  ASSERT_GT(baseline->statistics.steps, 1U);

  EvaluationRequest limited_request{.input = std::cref(*input)};
  limited_request.limits.max_steps = baseline->statistics.steps - 1U;
  limited_request.limits.timeout = std::chrono::steady_clock::duration::zero();
  const auto limited = program.evaluate(limited_request);
  ASSERT_FALSE(limited.has_value());
  EXPECT_EQ(limited.error().code, "H2001");
  EXPECT_NE(limited.error().message.find("step limit"), std::string::npos);
}

TEST(JsonataProgramTest, InterruptsNestedEvalCompilation) {
  std::stop_source cancelled;
  cancelled.request_stop();
  EvaluationRequest cancelled_request{.stop_token = cancelled.get_token()};
  const auto cancelled_result =
      compile(R"($eval("1 + 2"))").evaluate(cancelled_request);
  ASSERT_FALSE(cancelled_result.has_value());
  EXPECT_EQ(cancelled_result.error().code, "H1001");

  EvaluationRequest timeout_request;
  timeout_request.limits.timeout = std::chrono::nanoseconds{1};
  const auto timeout_result =
      compile(R"($eval("1 + 2"))").evaluate(timeout_request);
  ASSERT_FALSE(timeout_result.has_value());
  EXPECT_EQ(timeout_result.error().code, "D1012");
}

TEST(JsonataProgramTest, KeepsFunctionsReturnedFromEvalBoundToTheirProgram) {
  const auto result =
      evaluate(compile("($f := $eval('function($x){$x + 1}'); $f(2))"), "null");
  ASSERT_TRUE(result.has_value())
      << (result ? std::string{}
                 : result.error().code + ": " + result.error().message);
  EXPECT_EQ(encoded(*result), "3");
}

TEST(JsonataProgramTest, EnforcesOwnedStringAndProgramCompileBudgets) {
  auto string_limited = Program::compile(CompileRequest{
      .source = R"("abcd")",
      .limits = CompileLimits{.max_string_bytes = 3},
  });
  ASSERT_FALSE(string_limited.has_value());
  EXPECT_EQ(string_limited.error().code, "H1105");

  auto program_limited = Program::compile(CompileRequest{
      .source = "1",
      .limits = CompileLimits{.max_program_bytes = 1},
  });
  ASSERT_FALSE(program_limited.has_value());
  EXPECT_EQ(program_limited.error().code, "H1106");

  std::string invalid_utf8{"\xC0\xAF", 2};
  auto invalid_source =
      Program::compile(CompileRequest{.source = std::move(invalid_utf8)});
  ASSERT_FALSE(invalid_source.has_value());
  EXPECT_EQ(invalid_source.error().code, "H1107");
}

TEST(JsonataProgramTest, EnforcesWildcardAndDescendantCardinalityBudgets) {
  const auto input = parse_json(R"({"a":[1,2,3]})");
  ASSERT_TRUE(input.has_value());

  for (const auto expression : {"*", "**"}) {
    const auto program = compile(expression);
    EvaluationRequest request{.input = std::cref(*input)};
    request.limits.max_sequence_items = 2;
    request.limits.timeout = std::chrono::steady_clock::duration::zero();
    const auto result = program.evaluate(request);
    ASSERT_FALSE(result.has_value()) << expression;
    EXPECT_EQ(result.error().code, "D2015") << expression;
  }
}

TEST(JsonataProgramTest, ChargesDeepWalksAgainstStepBudget) {
  JsonValue input;
  input = JsonValue::object_t{};
  input["needle"] = 7;
  for (std::size_t depth = 0; depth < 32; ++depth) {
    JsonValue wrapper;
    wrapper = JsonValue::array_t{};
    wrapper.get_array().push_back(std::move(input));
    input = std::move(wrapper);
  }

  for (const auto expression : {"needle", "*", "**"}) {
    EvaluationRequest request{.input = std::cref(input)};
    request.limits.max_steps = 5;
    request.limits.timeout = std::chrono::steady_clock::duration::zero();
    const auto result = compile(expression).evaluate(request);
    ASSERT_FALSE(result.has_value()) << expression;
    EXPECT_EQ(result.error().code, "H2001") << expression;
  }
}

TEST(JsonataProgramTest, EnforcesValueGraphAndStringBudgetsWithPeakStatistics) {
  const auto input = parse_json("null");
  ASSERT_TRUE(input.has_value());

  EvaluationRequest value_request{.input = std::cref(*input)};
  value_request.limits.max_value_nodes = 3;
  value_request.limits.timeout = std::chrono::steady_clock::duration::zero();
  const auto value_result = compile("[1,2,3]").evaluate(value_request);
  ASSERT_FALSE(value_result.has_value());
  EXPECT_EQ(value_result.error().code, "H2100");

  EvaluationRequest string_request{.input = std::cref(*input)};
  string_request.limits.max_string_bytes = 3;
  string_request.limits.timeout = std::chrono::steady_clock::duration::zero();
  const auto string_result = compile(R"("abcd")").evaluate(string_request);
  ASSERT_FALSE(string_result.has_value());
  EXPECT_EQ(string_result.error().code, "H2101");

  EvaluationRequest observed_request{.input = std::cref(*input)};
  observed_request.limits.timeout = std::chrono::steady_clock::duration::zero();
  const auto observed =
      compile(R"(($a := ["ab","c"]; 1..3))").evaluate(observed_request);
  ASSERT_TRUE(observed.has_value());
  EXPECT_GE(observed->statistics.peak_value_nodes, 3U);
  EXPECT_GE(observed->statistics.peak_string_bytes, 3U);
  EXPECT_GE(observed->statistics.peak_sequence_items, 3U);
}

TEST(JsonataProgramTest, RejectsOversizedInputsBeforeRuntimeAllocation) {
  const auto wide_input = parse_json("[1,2,3,4]");
  ASSERT_TRUE(wide_input.has_value());
  EvaluationRequest node_request{.input = std::cref(*wide_input)};
  node_request.limits.max_value_nodes = 4;
  node_request.limits.timeout = std::chrono::steady_clock::duration::zero();
  const auto node_result = compile("$").evaluate(node_request);
  ASSERT_FALSE(node_result.has_value());
  EXPECT_EQ(node_result.error().code, "H2100");

  const auto string_input = parse_json(R"({"long-key":"abcd"})");
  ASSERT_TRUE(string_input.has_value());
  EvaluationRequest string_request{.input = std::cref(*string_input)};
  string_request.limits.max_string_bytes = 7;
  string_request.limits.timeout = std::chrono::steady_clock::duration::zero();
  const auto string_result = compile("$").evaluate(string_request);
  ASSERT_FALSE(string_result.has_value());
  EXPECT_EQ(string_result.error().code, "H2101");

  std::stop_source cancelled;
  cancelled.request_stop();
  EvaluationRequest cancelled_request{.input = std::cref(*wide_input),
                                      .stop_token = cancelled.get_token()};
  const auto cancelled_result = compile("$").evaluate(cancelled_request);
  ASSERT_FALSE(cancelled_result.has_value());
  EXPECT_EQ(cancelled_result.error().code, "H1001");
}

TEST(JsonataProgramTest, EnforcesDynamicEnvironmentAndDateRegexBudgets) {
  const auto input = parse_json("null");
  ASSERT_TRUE(input.has_value());

  EvaluationRequest binding_request{.input = std::cref(*input)};
  binding_request.limits.max_environment_bindings_created = 1;
  binding_request.limits.timeout = std::chrono::steady_clock::duration::zero();
  const auto binding_result =
      compile("($a := 1; $b := 2; $a + $b)").evaluate(binding_request);
  ASSERT_FALSE(binding_result.has_value());
  EXPECT_EQ(binding_result.error().code, "H2002");

  EvaluationRequest regex_request{.input = std::cref(*input)};
  regex_request.limits.max_regex_matches = 0;
  regex_request.limits.timeout = std::chrono::steady_clock::duration::zero();
  const auto regex_result =
      compile(R"($toMillis("2018-02-01T09:42:00Z"))").evaluate(regex_request);
  ASSERT_FALSE(regex_result.has_value());
  EXPECT_EQ(regex_result.error().code, "H2102");

  std::string adversarial(1024, 'a');
  adversarial.push_back('X');
  const auto regex_resource_program =
      compile(std::format(R"($contains("{}", /(a+)+$/))", adversarial));
  EvaluationRequest regex_resource_request{.input = std::cref(*input)};
  regex_resource_request.limits.max_steps = 64;
  regex_resource_request.limits.timeout = std::chrono::seconds(1);
  const auto regex_resource_result =
      regex_resource_program.evaluate(regex_resource_request);
  ASSERT_FALSE(regex_resource_result.has_value());
  EXPECT_EQ(regex_resource_result.error().code, "H2103");
}

TEST(JsonataProgramTest, ReturnsDiagnosticsInsteadOfThrowingForRuntimeTypes) {
  const auto invalid_round = evaluate(compile(R"($round(1, "x"))"), "null");
  ASSERT_FALSE(invalid_round.has_value());
  EXPECT_EQ(invalid_round.error().code, "T0410");

  const auto regex_equality = evaluate(compile("/a/ = /a/"), "null");
  ASSERT_TRUE(regex_equality.has_value());
  EXPECT_EQ(encoded(*regex_equality), "false");
}

TEST(JsonataProgramTest, TraversesDeepNarrowInputWithoutNativeRecursion) {
  JsonValue input;
  input = 1;
  for (std::size_t depth = 0; depth < 2048; ++depth) {
    JsonValue wrapper;
    wrapper = JsonValue::array_t{};
    wrapper.get_array().push_back(std::move(input));
    input = std::move(wrapper);
  }

  const auto program = compile("$");
  EvaluationRequest request{.input = std::cref(input)};
  request.limits.timeout = std::chrono::seconds{2};
  const auto result = program.evaluate(request);
  ASSERT_TRUE(result.has_value())
      << (result ? std::string{} : result.error().message);
  EXPECT_EQ(result->kind, EvaluationValueKind::Json);
  EXPECT_TRUE(result->value.has_value());
}

TEST(JsonataProgramTest, ProjectsNamesThroughDeepArraysWithoutNativeRecursion) {
  JsonValue input = JsonValue::object_t{};
  input["needle"] = 7;
  for (std::size_t depth = 0; depth < 4096; ++depth) {
    JsonValue wrapper;
    wrapper = JsonValue::array_t{};
    wrapper.get_array().push_back(std::move(input));
    input = std::move(wrapper);
  }

  const auto program = compile("needle");
  EvaluationRequest request{.input = std::cref(input)};
  request.limits.timeout = std::chrono::seconds{3};
  const auto result = program.evaluate(request);
  ASSERT_TRUE(result.has_value())
      << (result ? std::string{} : result.error().message);
  EXPECT_EQ(encoded(*result), "7");
}

} // namespace dagforge::jsonata::test
