#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/app/api/api_server.hpp"
#include "dagforge/app/services/execution_service.hpp"
#include "dagforge/app/services/persistence_service.hpp"
#include "dagforge/app/services/scheduler_service.hpp"
#include "dagforge/core/coroutine.hpp"
#include "dagforge/core/error.hpp"
#endif

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>


namespace dagforge {

class Runtime;

class ExecutionEventBridge {
public:
  struct Dependencies {
    Runtime *runtime{nullptr};
    ExecutionService *execution{nullptr};
    SchedulerService *scheduler{nullptr};
    PersistenceService *persistence{nullptr};
    std::move_only_function<ApiServer *()> api_server;
    std::move_only_function<std::optional<DAGId>(const DAGRunId &dag_run_id)>
        resolve_dag_id;
    std::move_only_function<void(const DAGRunId &dag_run_id, DAGRunState status)>
        on_run_status;
    std::move_only_function<void(const DAGRunId &dag_run_id, const DAGRun &run)>
        on_run_completed;
    std::move_only_function<int(const DAGRunId &dag_run_id, NodeIndex idx)>
        get_max_retries;
    std::move_only_function<std::chrono::seconds(const DAGRunId &dag_run_id,
                                                 NodeIndex idx)>
        get_retry_interval;
    std::move_only_function<void(const DAGId &dag_id,
                                 std::chrono::system_clock::time_point execution_date)>
        on_scheduler_trigger;
    std::atomic<std::uint64_t> *dropped_persistence_events{nullptr};
    std::atomic<std::uint64_t> *mysql_batch_write_ops{nullptr};
  };

  explicit ExecutionEventBridge(Dependencies deps);
  ExecutionEventBridge(const ExecutionEventBridge &) = delete;
  auto operator=(const ExecutionEventBridge &) -> ExecutionEventBridge & = delete;
  ExecutionEventBridge(ExecutionEventBridge &&) = default;
  auto operator=(ExecutionEventBridge &&) -> ExecutionEventBridge & = default;
  ~ExecutionEventBridge() = default;

  auto wire() -> void;

private:
  template <typename OpFactory>
  auto run_persistence_factory(std::shared_ptr<OpFactory> factory,
                               bool count_mysql_write) -> spawn_task {
    auto res = co_await (*factory)();
    record_persistence_result(res, count_mysql_write);
    co_return;
  }

  template <typename OpFactory>
  auto enqueue_persistence(OpFactory &&factory,
                           bool count_mysql_write = true) -> void {
    auto shared_factory =
        std::make_shared<std::decay_t<OpFactory>>(std::forward<OpFactory>(factory));
    spawn_persistence_task(run_persistence_factory(std::move(shared_factory),
                                                  count_mysql_write));
  }

  template <typename T>
  auto record_persistence_result(const Result<T> &result,
                                 bool count_mysql_write = true) -> void {
    if (!result) {
      deps_.dropped_persistence_events->fetch_add(1, std::memory_order_relaxed);
      return;
    }
    if (count_mysql_write) {
      deps_.mysql_batch_write_ops->fetch_add(1, std::memory_order_relaxed);
    }
  }

  auto persist_task_log(DAGRunId run_id, TaskId task_id, int attempt,
                        std::string stream, std::string message)
      -> task<Result<void>>;
  auto persist_run(std::shared_ptr<const DAGRun> run) -> task<Result<int64_t>>;
  auto persist_xcom(DAGRunId run_id, TaskId task_id, std::string key,
                    std::string value_json) -> task<Result<void>>;
  auto spawn_persistence_task(spawn_task task) -> void;

  Dependencies deps_;
};

} // namespace dagforge
