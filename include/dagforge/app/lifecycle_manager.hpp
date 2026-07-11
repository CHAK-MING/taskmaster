#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/app/services/dag_catalog_service.hpp"
#include "dagforge/app/services/execution_service.hpp"
#include "dagforge/app/services/persistence_service.hpp"
#include "dagforge/app/services/scheduler_service.hpp"
#include "dagforge/config/system_config.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/core/runtime.hpp"
#endif

#include <ankerl/unordered_dense.h>

#include <atomic>
#include <chrono>
#include <functional>

namespace dagforge {

class DAGManager;

class LifecycleManager {
public:
  using DagStateIndex = ankerl::unordered_dense::map<DAGId, DagStateRecord>;

  struct Dependencies {
    // All callbacks may capture Application-owned state. The owner must keep
    // those objects alive for the entire LifecycleManager lifetime.
    std::atomic<bool> &running;
    SystemConfig &config;
    Runtime &runtime;
    PersistenceService &persistence;
    SchedulerService &scheduler;
    ExecutionService &execution;
    DagCatalogService &dag_catalog;
    DAGManager &dag_manager;
    std::move_only_function<Result<void>()> ensure_api_initialized;
    std::move_only_function<Result<void>()> start_api;
    std::move_only_function<void()> stop_api;
    std::move_only_function<void()> start_config_watcher;
    std::move_only_function<Result<void>(std::chrono::steady_clock::time_point)>
        stop_config_watcher;
    std::move_only_function<Result<void>(std::chrono::steady_clock::time_point)>
        stop_scheduler;
    std::move_only_function<Result<void>(std::chrono::steady_clock::time_point)>
        wait_for_execution_quiesced;
    std::move_only_function<Result<void>(std::chrono::steady_clock::time_point)>
        close_persistence;
  };

  explicit LifecycleManager(Dependencies deps);

  [[nodiscard]] auto init() -> Result<void>;
  [[nodiscard]] auto init_db_only() -> Result<void>;
  [[nodiscard]] auto start() -> Result<void>;
  auto stop() noexcept -> void;

private:
  auto rollback_partial_start(bool log_started) noexcept -> void;

  Dependencies deps_;
};

} // namespace dagforge
