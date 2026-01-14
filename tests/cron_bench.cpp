#include "taskmaster/scheduler/cron.hpp"

#include <benchmark/benchmark.h>

using namespace taskmaster;

static void BM_CronParseEveryMinute(benchmark::State& state) {
  for (auto _ : state) {
    auto result = CronExpr::parse("* * * * *");
    benchmark::DoNotOptimize(result);
  }
}

static void BM_CronParseHourly(benchmark::State& state) {
  for (auto _ : state) {
    auto result = CronExpr::parse("@hourly");
    benchmark::DoNotOptimize(result);
  }
}

static void BM_CronParseComplex(benchmark::State& state) {
  for (auto _ : state) {
    auto result = CronExpr::parse("0,15,30,45 9-17 * * 1-5");
    benchmark::DoNotOptimize(result);
  }
}

static void BM_CronNextAfterEveryMinute(benchmark::State& state) {
  auto cron = CronExpr::parse("* * * * *");
  if (!cron.has_value()) {
    state.SkipWithError("Failed to parse cron expression");
    return;
  }

  for (auto _ : state) {
    auto now = std::chrono::system_clock::now();
    auto next = cron.value().next_after(now);
    benchmark::DoNotOptimize(next);
  }
}

static void BM_CronNextAfterHourly(benchmark::State& state) {
  auto cron = CronExpr::parse("@hourly");
  if (!cron.has_value()) {
    state.SkipWithError("Failed to parse cron expression");
    return;
  }

  for (auto _ : state) {
    auto now = std::chrono::system_clock::now();
    auto next = cron.value().next_after(now);
    benchmark::DoNotOptimize(next);
  }
}

static void BM_CronNextAfterComplex(benchmark::State& state) {
  auto cron = CronExpr::parse("0,15,30,45 9-17 * * 1-5");
  if (!cron.has_value()) {
    state.SkipWithError("Failed to parse cron expression");
    return;
  }

  for (auto _ : state) {
    auto now = std::chrono::system_clock::now();
    auto next = cron.value().next_after(now);
    benchmark::DoNotOptimize(next);
  }
}

static void BM_CronParseAndNext(benchmark::State& state) {
  const std::string expr = "0,15,30,45 9-17 * * 1-5";

  for (auto _ : state) {
    auto cron = CronExpr::parse(expr);
    if (cron.has_value()) {
      auto now = std::chrono::system_clock::now();
      auto next = cron.value().next_after(now);
      benchmark::DoNotOptimize(next);
    }
  }
}

static void BM_CronMultipleExpressions(benchmark::State& state) {
  std::vector<std::string> expressions = {
      "* * * * *",          "@hourly",     "@daily",         "0 2 * * *",
      "0,15,30,45 * * * *", "*/5 * * * *", "0 9-17 * * 1-5",
  };

  for (auto _ : state) {
    for (const auto& expr : expressions) {
      auto cron = CronExpr::parse(expr);
      benchmark::DoNotOptimize(cron);
    }
  }

  state.SetItemsProcessed(expressions.size() * state.iterations());
}

BENCHMARK(BM_CronParseEveryMinute);
BENCHMARK(BM_CronParseHourly);
BENCHMARK(BM_CronParseComplex);
BENCHMARK(BM_CronNextAfterEveryMinute);
BENCHMARK(BM_CronNextAfterHourly);
BENCHMARK(BM_CronNextAfterComplex);
BENCHMARK(BM_CronParseAndNext);
BENCHMARK(BM_CronMultipleExpressions);
