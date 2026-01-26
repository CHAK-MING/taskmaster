#include <benchmark/benchmark.h>

#include <barrier>
#include <latch>
#include <limits>
#include <atomic>
#include <string>
#include <thread>
#include <vector>
#include <algorithm>
#include <memory>
#include <mutex>
#include <chrono>
#include <cstdio>
#include <span>

#include "taskmaster/core/runtime.hpp"
#include "taskmaster/core/coroutine.hpp"
#include "taskmaster/executor/executor.hpp"
#include "taskmaster/util/id.hpp"

namespace {

using namespace taskmaster;

// PaddedCounter to avoid false sharing between shards
struct PaddedCounter {
  alignas(64) std::atomic<int> value{0};
};

[[nodiscard]] auto to_positive_int(long v, int fallback) -> int {
  if (v <= 0) return fallback;
  if (v > std::numeric_limits<int>::max()) return fallback;
  return static_cast<int>(v);
}

// Atomic wait/notify primitive for chunk completion
struct ChunkCompletion {
    std::atomic<int> completed{0};
    int target = 0;

    void reset(int t) {
        target = t;
        completed.store(0, std::memory_order_relaxed);
    }

    void notify() {
        // fetch_add returns the PREVIOUS value.
        // If previous value was (target - 1), then now it is target.
        if (completed.fetch_add(1, std::memory_order_relaxed) + 1 == target) {
            completed.notify_one();
        }
    }

    void wait() {
        int v = completed.load(std::memory_order_relaxed);
        while (v < target) {
            completed.wait(v);
            v = completed.load(std::memory_order_relaxed);
        }
    }
};

// FireAndForgetTask for self-destroying coroutines (avoids leaks in batch benchmarks)
struct FireAndForgetTask {
    struct promise_type {
        FireAndForgetTask get_return_object() {
            return FireAndForgetTask{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_always initial_suspend() noexcept { return {}; }
        
        std::coroutine_handle<> continuation() const noexcept { return {}; }

        taskmaster::final_awaiter final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        void unhandled_exception() { std::terminate(); }
    };

    std::coroutine_handle<promise_type> handle_;

    explicit FireAndForgetTask(std::coroutine_handle<promise_type> h) : handle_(h) {}
    
    FireAndForgetTask(FireAndForgetTask&& other) noexcept : handle_(other.handle_) {
        other.handle_ = nullptr;
    }
    
    FireAndForgetTask(const FireAndForgetTask&) = delete;
    ~FireAndForgetTask() = default;

    std::coroutine_handle<> get_handle() const { return handle_; }
};

// -----------------------------------------------------------------------------
// BENCHMARK 1: Single-Producer Task Scheduling (Sequential)
// -----------------------------------------------------------------------------

void BM_E2E_TaskScheduling(benchmark::State& state) {
  const int num_tasks = to_positive_int(state.range(0), 10000);
  const int shards = to_positive_int(state.range(1), 16);

  const InstanceId instance_id{"0"};
  const ExecutorConfig config =
      ShellExecutorConfig{.command = "true", .working_dir = ""};

  Runtime runtime(static_cast<unsigned>(shards));
  runtime.start();
  auto executor = create_noop_executor(runtime);

  for (auto _ : state) {
    auto completed = std::make_unique<PaddedCounter[]>(static_cast<std::size_t>(shards));
    
    ChunkCompletion done;
    done.reset(num_tasks);

    for (int i = 0; i < num_tasks; ++i) {
      ExecutorRequest req;
      req.instance_id = instance_id;
      req.config = config;

      ExecutionSink sink;
      sink.on_complete = [&](const InstanceId &, ExecutorResult) {
        const auto sid = runtime.get_shard_id();
        if (sid != kInvalidShard && sid < static_cast<unsigned>(shards)) {
            completed[static_cast<std::size_t>(sid)].value.fetch_add(
                1, std::memory_order_relaxed);
        }
        done.notify();
      };

      executor->start({.runtime = runtime}, std::move(req), std::move(sink));
    }

    done.wait();
  }

  runtime.stop();

  state.SetItemsProcessed(state.iterations() * num_tasks);
  state.counters["tasks"] = num_tasks;
  state.counters["shards"] = shards;
}

BENCHMARK(BM_E2E_TaskScheduling)
    ->ArgsProduct({{10000, 100000}, {1, 2, 4, 8, 16, 32}})
    ->ArgNames({"tasks", "shards"})
    ->Iterations(3)
    ->Unit(benchmark::kMillisecond);

// -----------------------------------------------------------------------------
// BENCHMARK 2: Task Latency
// -----------------------------------------------------------------------------

void BM_E2E_TaskLatency(benchmark::State& state) {
  const ExecutorConfig config =
      ShellExecutorConfig{.command = "true", .working_dir = ""};

  const int shards = to_positive_int(state.range(0), 16);

  Runtime runtime(static_cast<unsigned>(shards));
  runtime.start();
  auto executor = create_noop_executor(runtime);

  for (auto _ : state) {
    std::latch done(1);

    ExecutorRequest req;
    req.instance_id = InstanceId{"latency-test"};
    req.config = config;

    ExecutionSink sink;
    sink.on_complete = [&](const InstanceId &, ExecutorResult) {
      done.count_down();
    };

    executor->start({.runtime = runtime}, std::move(req), std::move(sink));
    done.wait();
  }

  runtime.stop();

  state.SetItemsProcessed(state.iterations());
  state.counters["shards"] = shards;
}

BENCHMARK(BM_E2E_TaskLatency)
    ->Arg(1)->Arg(2)->Arg(4)->Arg(8)->Arg(16)->Arg(32)
    ->ArgNames({"shards"})
    ->Iterations(1000)
    ->Unit(benchmark::kNanosecond);

// -----------------------------------------------------------------------------
// BENCHMARK 3: Batch Scheduling (Chunked & Safe)
// -----------------------------------------------------------------------------

// Helper to create a task with raw pointer captures for zero-overhead
auto make_batch_task(PaddedCounter* counters, 
                     std::atomic<int>* fallback,
                     ChunkCompletion* chunk_done,
                     unsigned shard_count) -> FireAndForgetTask {
    co_await async_yield();
    bool counted = false;
    if (detail::current_runtime) {
        auto sid = detail::current_shard_id;
        if (sid != kInvalidShard && sid < shard_count) {
             counters[sid].value.fetch_add(1, std::memory_order_relaxed);
             counted = true;
        }
    }
    if (!counted) {
        fallback->fetch_add(1, std::memory_order_relaxed);
    }
    if (chunk_done) {
        chunk_done->notify();
    }
}

void BM_E2E_TaskScheduling_Batch(benchmark::State& state) {
  const int num_tasks = to_positive_int(state.range(0), 100000);
  const int shards = to_positive_int(state.range(1), 16);
  constexpr int kChunkSize = 1024;

  Runtime runtime(static_cast<unsigned>(shards));
  runtime.start();
  const unsigned shard_count = runtime.shard_count();

  // Pre-allocate resources to isolate scheduling cost
  auto shard_counters = std::make_unique<PaddedCounter[]>(static_cast<std::size_t>(shards));
  std::atomic<int> fallback_counter{0};
  
  ChunkCompletion chunk_logic;
  std::vector<FireAndForgetTask> chunk_tasks;
  chunk_tasks.reserve(kChunkSize);
  std::vector<std::coroutine_handle<>> handles;
  handles.reserve(kChunkSize);

  for (auto _ : state) {
    int tasks_remaining = num_tasks;

    while (tasks_remaining > 0) {
        const int current_chunk = std::min(kChunkSize, tasks_remaining);
        chunk_logic.reset(current_chunk);
        
        chunk_tasks.clear();
        handles.clear();
        
        for (int i = 0; i < current_chunk; ++i) {
            auto task = make_batch_task(shard_counters.get(), &fallback_counter, &chunk_logic, shard_count);
            handles.push_back(task.get_handle());
            chunk_tasks.push_back(std::move(task));
        }

        runtime.schedule_external_batch(handles);
        
        tasks_remaining -= current_chunk;

        // Wait for THIS CHUNK
        chunk_logic.wait();

        // End of scope: chunk_tasks destroyed.
        // The tasks self-destroy after running.
    }
  }

  runtime.stop();

  state.SetItemsProcessed(state.iterations() * num_tasks);
  state.counters["tasks"] = num_tasks;
  state.counters["shards"] = shards;
}

BENCHMARK(BM_E2E_TaskScheduling_Batch)
    ->ArgsProduct({{100000}, {1, 2, 4, 8, 16}})
    ->ArgNames({"tasks", "shards"})
    ->UseRealTime()
    ->Iterations(1)
    ->Unit(benchmark::kMillisecond);

void BM_BatchHeavy_TaskScheduling(benchmark::State& state) {
    BM_E2E_TaskScheduling_Batch(state);
}

BENCHMARK(BM_BatchHeavy_TaskScheduling)
    ->ArgsProduct({{1000000}, {1, 2, 4, 8, 16, 32}})
    ->ArgNames({"tasks", "shards"})
    ->UseRealTime()
    ->Iterations(1)
    ->Unit(benchmark::kMillisecond);

// -----------------------------------------------------------------------------
// BENCHMARK 4: Multi-Producer E2E (Threaded)
// -----------------------------------------------------------------------------

struct MultiProducerState {
  std::unique_ptr<Runtime> runtime;
  std::unique_ptr<std::barrier<>> barrier;
  std::unique_ptr<PaddedCounter[]> global_counters;
  std::atomic<int> active_count{0};
};

void BM_MP_TaskScheduling(benchmark::State& state) {
  static std::atomic<MultiProducerState*> g_state{nullptr};

  const int total_tasks_target = to_positive_int(state.range(0), 100000);
  const int shards = to_positive_int(state.range(1), 16);
  const int num_producers = state.threads();
  const int producer_id = state.thread_index();
  
  const int tasks_per_thread = total_tasks_target / num_producers;

  // Initialize Shared State (Thread 0 only)
  if (producer_id == 0) {
    auto* s = new MultiProducerState();
    s->runtime = std::make_unique<Runtime>(static_cast<unsigned>(shards));
    s->runtime->start();
    s->barrier = std::make_unique<std::barrier<>>(num_producers);
    s->global_counters = std::make_unique<PaddedCounter[]>(
        static_cast<std::size_t>(shards * num_producers));
    s->active_count.store(num_producers, std::memory_order_relaxed);
    
    // Store global
    g_state.store(s, std::memory_order_release);
  }

  // Wait for initialization
  MultiProducerState* shared = nullptr;
  while ((shared = g_state.load(std::memory_order_acquire)) == nullptr) {
    std::this_thread::yield();
  }

  // Sync start
  shared->barrier->arrive_and_wait();

  // Create local executor
  auto executor = create_noop_executor(*shared->runtime);
  const ExecutorConfig config = ShellExecutorConfig{.command = "true"};
  const InstanceId instance_id{"0"};
  
  ChunkCompletion chunk_logic;

  for (auto _ : state) {
    int remaining = tasks_per_thread;
    constexpr int kLocalChunk = 512;
    
    while (remaining > 0) {
        int chunk = std::min(remaining, kLocalChunk);
        chunk_logic.reset(chunk);
        
        for (int i = 0; i < chunk; ++i) {
             ExecutorRequest req;
             req.instance_id = instance_id;
             req.config = config;

             ExecutionSink sink;
             sink.on_complete = [producer_id, num_producers, shared, shards, &chunk_logic](const InstanceId &, ExecutorResult) {
                if (auto* rt = detail::current_runtime) {
                   auto sid = rt->get_shard_id();
                   if (sid != kInvalidShard && sid < static_cast<unsigned>(shards)) {
                       shared->global_counters[sid * num_producers + producer_id]
                             .value.fetch_add(1, std::memory_order_relaxed);
                   }
                }
                chunk_logic.notify();
             };
             executor->start({.runtime = *shared->runtime}, std::move(req), std::move(sink));
        }
        
        remaining -= chunk;
        
        chunk_logic.wait();
    }
  }

  // Sync end
  shared->barrier->arrive_and_wait();

  // Cleanup (Thread 0)
  if (producer_id == 0) {
    g_state.store(nullptr, std::memory_order_release);
    shared->runtime->stop();
  }

  // Last thread out deletes the state
  if (shared->active_count.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    delete shared;
  }
}

BENCHMARK(BM_MP_TaskScheduling)
    ->ArgsProduct({{100000}, {1, 2, 4, 8, 16, 32}})
    ->ArgNames({"tasks", "shards"})
    ->Threads(1)->Threads(2)->Threads(4)->Threads(8)->Threads(16)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);

void BM_MP_BatchScheduling(benchmark::State& state) {
  static std::atomic<MultiProducerState*> g_state{nullptr};

  const int total_tasks_target = to_positive_int(state.range(0), 100000);
  const int shards = to_positive_int(state.range(1), 16);
  const int num_producers = state.threads();
  const int producer_id = state.thread_index();
  
  const int tasks_per_thread = total_tasks_target / num_producers;
  constexpr int kChunkSize = 128;

  // Initialize Shared State (Thread 0 only)
  if (producer_id == 0) {
    auto* s = new MultiProducerState();
    s->runtime = std::make_unique<Runtime>(static_cast<unsigned>(shards));
    s->runtime->start();
    s->barrier = std::make_unique<std::barrier<>>(num_producers);
    s->global_counters = nullptr;
    s->active_count.store(num_producers, std::memory_order_relaxed);
    
    g_state.store(s, std::memory_order_release);
  }

  // Wait for initialization
  MultiProducerState* shared = nullptr;
  while ((shared = g_state.load(std::memory_order_acquire)) == nullptr) {
    std::this_thread::yield();
  }

  // Sync start
  shared->barrier->arrive_and_wait();

  // Local resources per thread
  auto local_counters = std::make_unique<PaddedCounter[]>(static_cast<std::size_t>(shards));
  std::atomic<int> local_fallback{0};
  
  ChunkCompletion chunk_logic;
  std::vector<FireAndForgetTask> chunk_tasks;
  chunk_tasks.reserve(kChunkSize);
  std::vector<std::coroutine_handle<>> handles;
  handles.reserve(kChunkSize);

  const unsigned shard_count = shared->runtime->shard_count();

  for (auto _ : state) {
    int remaining = tasks_per_thread;
    
    while (remaining > 0) {
        int chunk = std::min(remaining, kChunkSize);
        chunk_logic.reset(chunk);
        
        chunk_tasks.clear();
        handles.clear();
        
        for (int i = 0; i < chunk; ++i) {
            auto task = make_batch_task(local_counters.get(), &local_fallback, &chunk_logic, shard_count);
            handles.push_back(task.get_handle());
            chunk_tasks.push_back(std::move(task));
        }

        shared->runtime->schedule_external_batch(handles);
        
        remaining -= chunk;
        
        chunk_logic.wait();
    }
  }

  // Sync end
  shared->barrier->arrive_and_wait();

  // Cleanup (Thread 0)
  if (producer_id == 0) {
    g_state.store(nullptr, std::memory_order_release);
    shared->runtime->stop();
  }

  // Last thread out deletes the state
  if (shared->active_count.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    delete shared;
  }
}

BENCHMARK(BM_MP_BatchScheduling)
    ->ArgsProduct({{100000}, {1, 2, 4, 8, 16, 32}})
    ->ArgNames({"tasks", "shards"})
    ->Threads(1)->Threads(2)->Threads(4)->Threads(8)->Threads(16)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);

} // namespace
