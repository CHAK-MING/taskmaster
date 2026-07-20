#include "dagforge/executors/transform/executor.hpp"

#include "dagforge/core/contract.hpp"
#include "dagforge/core/scope_exit.hpp"

#include "detail/evaluation.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <exception>
#include <memory>
#include <mutex>
#include <new>
#include <ranges>
#include <stop_token>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace dagforge::executors::transform {
namespace {

struct TransformTaskState {
  InstanceId instance_id;
  workflow::TaskExecutionSink sink;
  std::stop_source cancellation;
  bool committed{false};
  std::atomic_bool completed{false};
};

struct TransformWorkItem {
  std::shared_ptr<TransformTaskState> state;
  workflow::TaskExecutionRequest request;
  std::chrono::steady_clock::time_point accepted_at;
};

class TransformTaskExecutor final : public workflow::ITaskExecutor {
public:
  TransformTaskExecutor(std::size_t worker_count,
                        workflow::ExecutorDescription description)
      : description_(std::move(description)) {
    workers_.reserve(worker_count);
    for (std::size_t index = 0; index < worker_count; ++index) {
      workers_.emplace_back(
          [this](std::stop_token token) { worker_loop(token); });
    }
  }

  ~TransformTaskExecutor() override { shutdown(); }

  [[nodiscard]] auto type() const noexcept -> std::string_view override {
    return "transform";
  }

  [[nodiscard]] auto describe() const
      -> Result<workflow::ExecutorDescription> override {
    return ok(description_);
  }

  [[nodiscard]] auto compile(JsonPayload config,
                             workflow::ExecutorCompileContext context) const
      -> workflow::ExecutorCompileResult<
          workflow::CompiledExecutorConfig> override {
    return detail::compile_transform(std::move(config), context);
  }

  auto start(workflow::TaskExecutionRequest request,
             workflow::TaskExecutionSink sink) -> Result<void> override {
    auto validated = detail::validate_transform_request(request);
    if (!validated) {
      return fail(validated.error());
    }

    auto state = std::make_shared<TransformTaskState>();
    state->instance_id = request.instance_id.clone();
    state->sink = std::move(sink);
    const auto key = state->instance_id.str();
    const auto accepted_at = std::chrono::steady_clock::now();
    {
      std::lock_guard lock(mutex_);
      if (quiescing_ || stopping_) {
        return fail(Error::InvalidState);
      }
      if (!active_.emplace(key, state).second) {
        return fail(Error::AlreadyExists);
      }
    }
    bool enqueued = false;
    auto rollback_active = dagforge::scope_exit([&] {
      std::lock_guard lock(mutex_);
      if (enqueued) {
        const auto queued = std::ranges::find_if(
            queue_, [&](const auto &item) { return item.state == state; });
        if (queued != queue_.end()) {
          queue_.erase(queued);
        }
      }
      active_.erase(key);
      lifecycle_changed_.notify_all();
      work_available_.notify_all();
    });
    try {
      {
        std::lock_guard lock(mutex_);
        queue_.push_back(TransformWorkItem{
            .state = state,
            .request = std::move(request),
            .accepted_at = accepted_at,
        });
        enqueued = true;
      }
      if (state->sink.on_state) {
        state->sink.on_state(state->instance_id, "running");
      }
    } catch (const std::bad_alloc &) {
      return fail(Error::ResourceExhausted);
    } catch (...) {
      return fail(Error::Unknown);
    }
    {
      std::lock_guard lock(mutex_);
      state->committed = true;
      rollback_active.release();
    }
    work_available_.notify_one();
    return ok();
  }

  auto cancel(const InstanceId &instance_id) -> void override {
    std::shared_ptr<TransformTaskState> state;
    {
      std::lock_guard lock(mutex_);
      const auto found = active_.find(instance_id.str());
      if (found != active_.end()) {
        state = found->second;
      }
    }
    if (state) {
      state->cancellation.request_stop();
    }
  }

  auto quiesce(std::chrono::milliseconds timeout) -> Result<void> override {
    {
      std::lock_guard lock(mutex_);
      quiescing_ = true;
      for (const auto &[_, state] : active_) {
        state->cancellation.request_stop();
      }
    }
    work_available_.notify_all();

    std::unique_lock lock(mutex_);
    if (!lifecycle_changed_.wait_for(lock, timeout,
                                     [this] { return active_.empty(); })) {
      return fail(Error::Timeout);
    }
    stopping_ = true;
    lock.unlock();
    work_available_.notify_all();
    workers_.clear();
    return ok();
  }

private:
  auto complete(const std::shared_ptr<TransformTaskState> &state,
                workflow::TaskExecutionResult result) -> void {
    if (state->completed.exchange(true, std::memory_order_acq_rel)) {
      return;
    }
    const auto release_active = dagforge::scope_exit([&] {
      std::lock_guard lock(mutex_);
      active_.erase(state->instance_id.str());
      lifecycle_changed_.notify_all();
    });
    if (state->sink.on_complete) {
      state->sink.on_complete(state->instance_id, std::move(result));
    }
  }

  auto worker_loop(std::stop_token token) -> void {
    const std::stop_callback notify_stop(
        token, [this] { work_available_.notify_all(); });
    for (;;) {
      TransformWorkItem item;
      {
        std::unique_lock lock(mutex_);
        work_available_.wait(lock, [this, token] {
          return (!queue_.empty() && queue_.front().state->committed) ||
                 ((stopping_ || token.stop_requested()) && queue_.empty());
        });
        if ((stopping_ || token.stop_requested()) && queue_.empty()) {
          return;
        }
        item = std::move(queue_.front());
        queue_.pop_front();
      }

      workflow::TaskExecutionResult result = [&] {
        try {
          return detail::evaluate_transform(
              item.request, item.state->cancellation.get_token(),
              item.accepted_at);
        } catch (const std::bad_alloc &) {
          return workflow::task_failed(workflow::make_execution_failure(
              Error::ResourceExhausted, "transform_resource_exhausted",
              "Transform evaluation could not allocate memory"));
        } catch (const std::exception &) {
          return workflow::task_failed(workflow::make_execution_failure(
              Error::Unknown, "transform_internal_error",
              "Transform evaluation failed unexpectedly"));
        } catch (...) {
          return workflow::task_failed(workflow::make_execution_failure(
              Error::Unknown, "transform_internal_error",
              "Transform evaluation failed unexpectedly"));
        }
      }();
      try {
        complete(item.state, std::move(result));
      } catch (const std::bad_alloc &) {
        contract_violation("Transform completion callback exhausted memory");
      } catch (...) {
        contract_violation("Transform completion callback threw");
      }
    }
  }

  auto shutdown() noexcept -> void {
    {
      std::lock_guard lock(mutex_);
      quiescing_ = true;
      stopping_ = true;
      for (const auto &[_, state] : active_) {
        state->cancellation.request_stop();
      }
    }
    work_available_.notify_all();
    workers_.clear();
  }

  std::mutex mutex_;
  std::condition_variable work_available_;
  std::condition_variable lifecycle_changed_;
  std::deque<TransformWorkItem> queue_;
  std::unordered_map<std::string, std::shared_ptr<TransformTaskState>> active_;
  std::vector<std::jthread> workers_;
  workflow::ExecutorDescription description_;
  bool quiescing_{false};
  bool stopping_{false};
};

} // namespace

auto create_task_executor(Runtime &runtime)
    -> Result<std::shared_ptr<workflow::ITaskExecutor>> {
  const auto worker_count = std::max<std::size_t>(1, runtime.shard_count());
  auto description = detail::describe_transform();
  if (!description) {
    return fail(description.error());
  }
  try {
    return ok(std::shared_ptr<workflow::ITaskExecutor>{
        std::make_shared<TransformTaskExecutor>(worker_count,
                                                std::move(*description))});
  } catch (const std::bad_alloc &) {
    return fail(Error::ResourceExhausted);
  } catch (const std::system_error &) {
    return fail(Error::ResourceExhausted);
  } catch (...) {
    return fail(Error::Unknown);
  }
}

} // namespace dagforge::executors::transform
