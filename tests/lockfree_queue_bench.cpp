#include "taskmaster/core/lockfree_queue.hpp"

#include <benchmark/benchmark.h>

using namespace taskmaster;

static void BM_SPSCQueuePushPop(benchmark::State& state) {
  const int capacity = state.range(0);
  const int iterations = state.range(1);
  SPSCQueue<int> queue(capacity);

  for (auto _ : state) {
    for (int i = 0; i < iterations; ++i) {
      queue.push(i);
      queue.try_pop();
    }
  }

  state.SetItemsProcessed(iterations * state.iterations());
}

static void BM_SPSCQueuePushPopConcurrent(benchmark::State& state) {
  const int capacity = state.range(0);
  const int iterations = state.range(1);
  SPSCQueue<int> queue(capacity);
  std::atomic<bool> done{false};
  std::atomic<int> sum{0};

  for (auto _ : state) {
    done.store(false);

    std::thread producer([&]() {
      for (int i = 0; i < iterations; ++i) {
        while (!queue.push(i)) {
          benchmark::DoNotOptimize(queue.empty());
        }
      }
      done.store(true);
    });

    std::thread consumer([&]() {
      int local_sum = 0;
      while (!done.load() || !queue.empty()) {
        auto val = queue.try_pop();
        if (val.has_value()) {
          local_sum += val.value();
        }
      }
      sum.store(local_sum);
    });

    producer.join();
    consumer.join();
  }

  state.SetItemsProcessed(iterations * state.iterations());
  state.SetLabel("sum=" + std::to_string(sum.load()));
}

static void BM_BoundedMPSCQueuePushPop(benchmark::State& state) {
  const int capacity = state.range(0);
  const int iterations = state.range(1);
  BoundedMPSCQueue<int> queue(capacity);

  for (auto _ : state) {
    for (int i = 0; i < iterations; ++i) {
      queue.push(i);
      queue.try_pop();
    }
  }

  state.SetItemsProcessed(iterations * state.iterations());
}

static void BM_BoundedMPSCQueuePushPopMultiProducer(benchmark::State& state) {
  const int capacity = state.range(0);
  const int iterations = state.range(1);
  const int num_producers = state.range(2);
  BoundedMPSCQueue<int> queue(capacity);
  std::atomic<bool> done{false};
  std::atomic<int> sum{0};

  for (auto _ : state) {
    done.store(false);

    std::vector<std::thread> producers;
    for (int p = 0; p < num_producers; ++p) {
      producers.emplace_back([&]() {
        for (int i = 0; i < iterations / num_producers; ++i) {
          while (!queue.push(i)) {
            benchmark::DoNotOptimize(queue.empty());
          }
        }
      });
    }

    std::thread consumer([&]() {
      int local_sum = 0;
      while (!done.load() || !queue.empty()) {
        auto val = queue.try_pop();
        if (val.has_value()) {
          local_sum += val.value();
        }
      }
      sum.store(local_sum);
    });

    for (auto& p : producers) {
      p.join();
    }
    done.store(true);
    consumer.join();
  }

  state.SetItemsProcessed(iterations * state.iterations());
}

static void BM_SPSCQueueSize(benchmark::State& state) {
  const int capacity = state.range(0);
  SPSCQueue<int> queue(capacity);

  for (auto _ : state) {
    benchmark::DoNotOptimize(queue.size());
  }
}

static void BM_SPSCQueueEmpty(benchmark::State& state) {
  const int capacity = state.range(0);
  SPSCQueue<int> queue(capacity);

  for (auto _ : state) {
    benchmark::DoNotOptimize(queue.empty());
  }
}

BENCHMARK(BM_SPSCQueuePushPop)
    ->Args({64, 100})
    ->Args({64, 1000})
    ->Args({256, 1000})
    ->Args({1024, 1000});

BENCHMARK(BM_SPSCQueuePushPopConcurrent)
    ->Args({64, 1000})
    ->Args({256, 10000})
    ->Args({1024, 10000});

BENCHMARK(BM_BoundedMPSCQueuePushPop)
    ->Args({64, 100})
    ->Args({64, 1000})
    ->Args({256, 1000})
    ->Args({1024, 1000});

BENCHMARK(BM_BoundedMPSCQueuePushPopMultiProducer)
    ->Args({64, 1000, 2})
    ->Args({256, 10000, 2})
    ->Args({1024, 10000, 4});

BENCHMARK(BM_SPSCQueueSize)->Arg(64)->Arg(256)->Arg(1024);

BENCHMARK(BM_SPSCQueueEmpty)->Arg(64)->Arg(256)->Arg(1024);

BENCHMARK_MAIN();
