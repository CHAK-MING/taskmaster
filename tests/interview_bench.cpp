// Interview-level Benchmarks for TaskMaster
// Covers common systems programming interview topics:
// - Lock-free data structures
// - Memory allocators (PMR vs standard)
// - Coroutine overhead
// - Cache effects and false sharing
// - Synchronization primitives

#include "taskmaster/core/coroutine.hpp"
#include "taskmaster/core/lockfree_queue.hpp"
#include "taskmaster/core/runtime.hpp"
#include "taskmaster/io/stream.hpp"

#include <benchmark/benchmark.h>

#include <algorithm>
#include <atomic>
#include <fcntl.h>
#include <chrono>
#include <latch>
#include <memory_resource>
#include <mutex>
#include <numeric>
#include <queue>
#include <random>
#include <thread>
#include <vector>

using namespace taskmaster;

// ============================================================================
// Section 1: Lock-free Queue vs Mutex Queue Comparison
// Interview Topic: Why use lock-free? Trade-offs?
// ============================================================================

// Mutex-based queue for comparison
template <typename T>
class MutexQueue {
public:
  explicit MutexQueue(std::size_t /*capacity*/) {}

  auto push(T value) -> bool {
    std::lock_guard lock(mutex_);
    queue_.push(std::move(value));
    return true;
  }

  auto try_pop() -> std::optional<T> {
    std::lock_guard lock(mutex_);
    if (queue_.empty()) return std::nullopt;
    T value = std::move(queue_.front());
    queue_.pop();
    return value;
  }

  auto empty() const -> bool {
    std::lock_guard lock(mutex_);
    return queue_.empty();
  }

private:
  mutable std::mutex mutex_;
  std::queue<T> queue_;
};

// Single-threaded baseline: Lock-free SPSC
static void BM_LockFree_SPSC_SingleThread(benchmark::State& state) {
  SPSCQueue<int> queue(1024);
  int sum = 0;

  for (auto _ : state) {
    for (int i = 0; i < 1000; ++i) {
      (void)queue.push(i);
      auto val = queue.try_pop();
      sum += val.value_or(0);
    }
  }

  benchmark::DoNotOptimize(sum);
  state.SetItemsProcessed(state.iterations() * 1000);
}

// Single-threaded baseline: Mutex queue
static void BM_Mutex_Queue_SingleThread(benchmark::State& state) {
  MutexQueue<int> queue(1024);
  int sum = 0;

  for (auto _ : state) {
    for (int i = 0; i < 1000; ++i) {
      (void)queue.push(i);
      auto val = queue.try_pop();
      sum += val.value_or(0);
    }
  }

  benchmark::DoNotOptimize(sum);
  state.SetItemsProcessed(state.iterations() * 1000);
}

// Multi-threaded: Lock-free SPSC with dedicated producer/consumer
static void BM_LockFree_SPSC_TwoThreads(benchmark::State& state) {
  SPSCQueue<int> queue(4096);
  const int iterations = 100000;

  for (auto _ : state) {
    std::atomic<bool> done{false};
    std::atomic<long long> consumer_sum{0};

    std::thread producer([&]() {
      for (int i = 0; i < iterations; ++i) {
        while (!queue.push(i)) {
          std::this_thread::yield();
        }
      }
      done.store(true, std::memory_order_release);
    });

    std::thread consumer([&]() {
      long long sum = 0;
      while (!done.load(std::memory_order_acquire) || !queue.empty()) {
        if (auto val = queue.try_pop()) {
          sum += *val;
        }
      }
      consumer_sum.store(sum, std::memory_order_relaxed);
    });

    producer.join();
    consumer.join();
    benchmark::DoNotOptimize(consumer_sum.load());
  }

  state.SetItemsProcessed(state.iterations() * iterations);
}

// Multi-threaded: Mutex queue with dedicated producer/consumer
static void BM_Mutex_Queue_TwoThreads(benchmark::State& state) {
  MutexQueue<int> queue(4096);
  const int iterations = 100000;

  for (auto _ : state) {
    std::atomic<bool> done{false};
    std::atomic<long long> consumer_sum{0};

    std::thread producer([&]() {
      for (int i = 0; i < iterations; ++i) {
        queue.push(i);
      }
      done.store(true, std::memory_order_release);
    });

    std::thread consumer([&]() {
      long long sum = 0;
      while (!done.load(std::memory_order_acquire) || !queue.empty()) {
        if (auto val = queue.try_pop()) {
          sum += *val;
        }
      }
      consumer_sum.store(sum, std::memory_order_relaxed);
    });

    producer.join();
    consumer.join();
    benchmark::DoNotOptimize(consumer_sum.load());
  }

  state.SetItemsProcessed(state.iterations() * iterations);
}

// Multi-producer: BoundedMPSC vs Mutex
static void BM_LockFree_MPSC_MultiProducer(benchmark::State& state) {
  const int num_producers = static_cast<int>(state.range(0));
  const int items_per_producer = 10000;
  BoundedMPSCQueue<int> queue(8192);

  for (auto _ : state) {
    std::atomic<int> producers_done{0};
    std::atomic<long long> consumer_sum{0};

    std::vector<std::thread> producers;
    producers.reserve(num_producers);

    for (int p = 0; p < num_producers; ++p) {
      producers.emplace_back([&, p]() {
        for (int i = 0; i < items_per_producer; ++i) {
          while (!queue.push(p * items_per_producer + i)) {
            std::this_thread::yield();
          }
        }
        producers_done.fetch_add(1, std::memory_order_release);
      });
    }

    std::thread consumer([&]() {
      long long sum = 0;
      while (producers_done.load(std::memory_order_acquire) < num_producers ||
             !queue.empty()) {
        if (auto val = queue.try_pop()) {
          sum += *val;
        }
      }
      consumer_sum.store(sum, std::memory_order_relaxed);
    });

    for (auto& t : producers) t.join();
    consumer.join();
    benchmark::DoNotOptimize(consumer_sum.load());
  }

  state.SetItemsProcessed(state.iterations() * num_producers * items_per_producer);
  state.SetLabel(std::to_string(num_producers) + " producers");
}

// ============================================================================
// Section 2: False Sharing Demonstration
// Interview Topic: What is false sharing? How to avoid it?
// ============================================================================

struct alignas(64) PaddedCounter {
  std::atomic<long long> value{0};
};

struct UnpaddedCounter {
  std::atomic<long long> value{0};
};

static void BM_FalseSharing_Padded(benchmark::State& state) {
  const int num_threads = static_cast<int>(state.range(0));
  std::vector<PaddedCounter> counters(num_threads);
  const int iterations = 100000;

  for (auto _ : state) {
    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int t = 0; t < num_threads; ++t) {
      threads.emplace_back([&, t]() {
        for (int i = 0; i < iterations; ++i) {
          counters[t].value.fetch_add(1, std::memory_order_relaxed);
        }
      });
    }

    for (auto& t : threads) t.join();
  }

  long long total = 0;
  for (const auto& c : counters) {
    total += c.value.load(std::memory_order_relaxed);
  }
  benchmark::DoNotOptimize(total);
  state.SetItemsProcessed(state.iterations() * num_threads * iterations);
  state.SetLabel("cache-line aligned");
}

static void BM_FalseSharing_Unpadded(benchmark::State& state) {
  const int num_threads = static_cast<int>(state.range(0));
  std::vector<UnpaddedCounter> counters(num_threads);
  const int iterations = 100000;

  for (auto _ : state) {
    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int t = 0; t < num_threads; ++t) {
      threads.emplace_back([&, t]() {
        for (int i = 0; i < iterations; ++i) {
          counters[t].value.fetch_add(1, std::memory_order_relaxed);
        }
      });
    }

    for (auto& t : threads) t.join();
  }

  long long total = 0;
  for (const auto& c : counters) {
    total += c.value.load(std::memory_order_relaxed);
  }
  benchmark::DoNotOptimize(total);
  state.SetItemsProcessed(state.iterations() * num_threads * iterations);
  state.SetLabel("contiguous (false sharing)");
}

// ============================================================================
// Section 3: Memory Allocator Comparison
// Interview Topic: PMR, pool allocators, allocation strategies
// ============================================================================

static void BM_StdAllocator_SmallObjects(benchmark::State& state) {
  const int num_allocs = static_cast<int>(state.range(0));
  std::vector<std::unique_ptr<int>> ptrs;
  ptrs.reserve(num_allocs);

  for (auto _ : state) {
    ptrs.clear();
    for (int i = 0; i < num_allocs; ++i) {
      ptrs.push_back(std::make_unique<int>(i));
    }
    benchmark::DoNotOptimize(ptrs.data());
  }

  state.SetItemsProcessed(state.iterations() * num_allocs);
  state.SetLabel("std::allocator");
}

static void BM_PMR_MonotonicBuffer_SmallObjects(benchmark::State& state) {
  const int num_allocs = static_cast<int>(state.range(0));
  std::vector<char> buffer(num_allocs * 32);  // Pre-allocated buffer

  for (auto _ : state) {
    std::pmr::monotonic_buffer_resource pool{buffer.data(), buffer.size()};
    std::pmr::polymorphic_allocator<int> alloc{&pool};

    std::pmr::vector<int*> ptrs{&pool};
    ptrs.reserve(num_allocs);

    for (int i = 0; i < num_allocs; ++i) {
      int* p = alloc.allocate(1);
      std::construct_at(p, i);
      ptrs.push_back(p);
    }
    benchmark::DoNotOptimize(ptrs.data());
    // monotonic_buffer_resource deallocates everything at destruction
  }

  state.SetItemsProcessed(state.iterations() * num_allocs);
  state.SetLabel("pmr::monotonic_buffer");
}

static void BM_PMR_PoolResource_SmallObjects(benchmark::State& state) {
  const int num_allocs = static_cast<int>(state.range(0));

  for (auto _ : state) {
    std::pmr::unsynchronized_pool_resource pool;
    std::pmr::polymorphic_allocator<int> alloc{&pool};

    std::pmr::vector<int*> ptrs{&pool};
    ptrs.reserve(num_allocs);

    for (int i = 0; i < num_allocs; ++i) {
      int* p = alloc.allocate(1);
      std::construct_at(p, i);
      ptrs.push_back(p);
    }

    // Deallocate
    for (int* p : ptrs) {
      std::destroy_at(p);
      alloc.deallocate(p, 1);
    }
    benchmark::DoNotOptimize(ptrs.data());
  }

  state.SetItemsProcessed(state.iterations() * num_allocs);
  state.SetLabel("pmr::unsynchronized_pool");
}

// ============================================================================
// Section 4: Coroutine Overhead
// Interview Topic: Coroutine frame allocation, suspension cost
// ============================================================================

static task<int> simple_coroutine(int value) {
  co_return value + 1;
}

static task<int> nested_coroutine(int depth) {
  if (depth <= 0) {
    co_return 0;
  }
  int result = co_await nested_coroutine(depth - 1);
  co_return result + 1;
}

static void BM_Coroutine_Creation(benchmark::State& state) {
  for (auto _ : state) {
    auto t = simple_coroutine(42);
    benchmark::DoNotOptimize(t.handle());
  }

  state.SetItemsProcessed(state.iterations());
  state.SetLabel("task<int> creation");
}

static void BM_Coroutine_CreationAndResume(benchmark::State& state) {
  for (auto _ : state) {
    auto t = simple_coroutine(42);
    t.handle().resume();  // Run to completion
    benchmark::DoNotOptimize(t.done());
  }

  state.SetItemsProcessed(state.iterations());
  state.SetLabel("create + resume");
}

static void BM_Coroutine_NestedCalls(benchmark::State& state) {
  const int depth = static_cast<int>(state.range(0));

  for (auto _ : state) {
    auto t = nested_coroutine(depth);

    // Drive the coroutine chain to completion
    std::vector<std::coroutine_handle<>> handles;
    handles.push_back(t.handle());

    while (!handles.empty()) {
      auto h = handles.back();
      if (h.done()) {
        handles.pop_back();
        continue;
      }
      h.resume();
    }

    benchmark::DoNotOptimize(t.done());
  }

  state.SetItemsProcessed(state.iterations());
  state.SetLabel("depth=" + std::to_string(depth));
}

// ============================================================================
// Section 5: Atomic Operations Comparison
// Interview Topic: Memory ordering, CAS vs fetch_add
// ============================================================================

static void BM_Atomic_FetchAdd_Relaxed(benchmark::State& state) {
  std::atomic<long long> counter{0};

  for (auto _ : state) {
    counter.fetch_add(1, std::memory_order_relaxed);
  }

  benchmark::DoNotOptimize(counter.load());
  state.SetItemsProcessed(state.iterations());
}

static void BM_Atomic_FetchAdd_SeqCst(benchmark::State& state) {
  std::atomic<long long> counter{0};

  for (auto _ : state) {
    counter.fetch_add(1, std::memory_order_seq_cst);
  }

  benchmark::DoNotOptimize(counter.load());
  state.SetItemsProcessed(state.iterations());
}

static void BM_Atomic_CAS_Relaxed(benchmark::State& state) {
  std::atomic<long long> counter{0};

  for (auto _ : state) {
    long long expected = counter.load(std::memory_order_relaxed);
    while (!counter.compare_exchange_weak(expected, expected + 1,
                                          std::memory_order_relaxed)) {
    }
  }

  benchmark::DoNotOptimize(counter.load());
  state.SetItemsProcessed(state.iterations());
}

static void BM_Atomic_CAS_SeqCst(benchmark::State& state) {
  std::atomic<long long> counter{0};

  for (auto _ : state) {
    long long expected = counter.load(std::memory_order_seq_cst);
    while (!counter.compare_exchange_weak(expected, expected + 1,
                                          std::memory_order_seq_cst)) {
    }
  }

  benchmark::DoNotOptimize(counter.load());
  state.SetItemsProcessed(state.iterations());
}

// Multi-threaded atomic contention
static void BM_Atomic_Contention(benchmark::State& state) {
  const int num_threads = static_cast<int>(state.range(0));
  std::atomic<long long> counter{0};
  const int iterations = 100000;

  for (auto _ : state) {
    counter.store(0, std::memory_order_relaxed);
    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int t = 0; t < num_threads; ++t) {
      threads.emplace_back([&]() {
        for (int i = 0; i < iterations; ++i) {
          counter.fetch_add(1, std::memory_order_relaxed);
        }
      });
    }

    for (auto& t : threads) t.join();
  }

  benchmark::DoNotOptimize(counter.load());
  state.SetItemsProcessed(state.iterations() * num_threads * iterations);
}

// ============================================================================
// Section 6: EventFd Performance
// Interview Topic: Inter-thread signaling mechanisms
// ============================================================================

static void BM_EventFd_SignalConsume(benchmark::State& state) {
  auto result = io::EventFd::create(0, EFD_NONBLOCK);
  if (!result) {
    state.SkipWithError("Failed to create eventfd");
    return;
  }
  auto eventfd = std::move(*result);

  for (auto _ : state) {
    eventfd.signal(1);
    auto val = eventfd.consume();
    benchmark::DoNotOptimize(val);
  }

  state.SetItemsProcessed(state.iterations());
}

static void BM_EventFd_CrossThread(benchmark::State& state) {
  auto result = io::EventFd::create(0, EFD_NONBLOCK);
  if (!result) {
    state.SkipWithError("Failed to create eventfd");
    return;
  }
  auto eventfd = std::move(*result);
  const int iterations = 10000;

  for (auto _ : state) {
    std::atomic<bool> done{false};
    std::atomic<int> signals_received{0};

    std::thread signaler([&]() {
      for (int i = 0; i < iterations; ++i) {
        eventfd.signal(1);
      }
      done.store(true, std::memory_order_release);
    });

    std::thread consumer([&]() {
      int received = 0;
      while (!done.load(std::memory_order_acquire) || eventfd.consume() > 0) {
        auto val = eventfd.consume();
        received += static_cast<int>(val);
      }
      signals_received.store(received, std::memory_order_relaxed);
    });

    signaler.join();
    consumer.join();
    benchmark::DoNotOptimize(signals_received.load());
  }

  state.SetItemsProcessed(state.iterations() * iterations);
}

// ============================================================================
// Section 7: Latency Percentiles (Lock-free Queue)
// Interview Topic: Tail latency, p99/p999 analysis
// ============================================================================

static void BM_SPSC_LatencyPercentiles(benchmark::State& state) {
  SPSCQueue<std::chrono::steady_clock::time_point> queue(1024);
  std::vector<long long> latencies;
  latencies.reserve(10000);

  for (auto _ : state) {
    state.PauseTiming();
    latencies.clear();
    state.ResumeTiming();

    for (int i = 0; i < 10000; ++i) {
      auto start = std::chrono::steady_clock::now();
      (void)queue.push(start);
      auto val = queue.try_pop();
      auto end = std::chrono::steady_clock::now();

      if (val) {
        auto latency =
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - *val)
                .count();
        latencies.push_back(latency);
      }
    }

    state.PauseTiming();
    std::sort(latencies.begin(), latencies.end());

    auto p50 = latencies[latencies.size() * 50 / 100];
    auto p99 = latencies[latencies.size() * 99 / 100];
    auto p999 = latencies[latencies.size() * 999 / 1000];

    state.counters["p50_ns"] = static_cast<double>(p50);
    state.counters["p99_ns"] = static_cast<double>(p99);
    state.counters["p999_ns"] = static_cast<double>(p999);
    state.ResumeTiming();
  }

  state.SetItemsProcessed(state.iterations() * 10000);
}

// ============================================================================
// Register Benchmarks
// ============================================================================

// Lock-free vs Mutex
BENCHMARK(BM_LockFree_SPSC_SingleThread);
BENCHMARK(BM_Mutex_Queue_SingleThread);
BENCHMARK(BM_LockFree_SPSC_TwoThreads);
BENCHMARK(BM_Mutex_Queue_TwoThreads);
BENCHMARK(BM_LockFree_MPSC_MultiProducer)->Arg(2)->Arg(4)->Arg(8);

// False Sharing
BENCHMARK(BM_FalseSharing_Padded)->Arg(2)->Arg(4)->Arg(8);
BENCHMARK(BM_FalseSharing_Unpadded)->Arg(2)->Arg(4)->Arg(8);

// Memory Allocators
BENCHMARK(BM_StdAllocator_SmallObjects)->Arg(100)->Arg(1000)->Arg(10000);
BENCHMARK(BM_PMR_MonotonicBuffer_SmallObjects)->Arg(100)->Arg(1000)->Arg(10000);
BENCHMARK(BM_PMR_PoolResource_SmallObjects)->Arg(100)->Arg(1000)->Arg(10000);

// Coroutines
BENCHMARK(BM_Coroutine_Creation);
BENCHMARK(BM_Coroutine_CreationAndResume);
BENCHMARK(BM_Coroutine_NestedCalls)->Arg(1)->Arg(5)->Arg(10)->Arg(20);

// Atomics
BENCHMARK(BM_Atomic_FetchAdd_Relaxed);
BENCHMARK(BM_Atomic_FetchAdd_SeqCst);
BENCHMARK(BM_Atomic_CAS_Relaxed);
BENCHMARK(BM_Atomic_CAS_SeqCst);
BENCHMARK(BM_Atomic_Contention)->Arg(2)->Arg(4)->Arg(8);

// EventFd
BENCHMARK(BM_EventFd_SignalConsume);
BENCHMARK(BM_EventFd_CrossThread);

BENCHMARK(BM_SPSC_LatencyPercentiles);
