#include "dagforge/app/services/dag_catalog_service.hpp"
#include "dagforge/app/services/persistence_service.hpp"
#include "dagforge/app/services/scheduler_service.hpp"
#include "dagforge/config/dag_file_loader.hpp"
#include "dagforge/dag/dag_validator.hpp"
#include "dagforge/util/log.hpp"

#include <ranges>

namespace dagforge {

DagCatalogService::DagCatalogService(DAGManager &dag_manager,
                                     PersistenceService *persistence,
                                     SchedulerService *scheduler)
    : dag_manager_(dag_manager), persistence_(persistence),
      scheduler_(scheduler) {}

auto DagCatalogService::set_persistence_service(PersistenceService *persistence)
    -> void {
  persistence_ = persistence;
}

auto DagCatalogService::set_scheduler_service(SchedulerService *scheduler)
    -> void {
  scheduler_ = scheduler;
}

auto DagCatalogService::load_directory(std::string_view dags_dir)
    -> Result<bool> {
  return load_directory(dags_dir, nullptr);
}

auto DagCatalogService::load_directory(std::string_view dags_dir,
                                       const DagStateIndex *state_index)
    -> Result<bool> {
  return stage_dags_from_directory(dags_dir, state_index)
      .and_then([&](std::vector<DAGInfo> &&staged_dags) -> Result<bool> {
        if (staged_dags.empty()) {
          return ok(false);
        }

        if (persistence_ && persistence_->is_open()) {
          for (auto &dag : staged_dags) {
            auto persisted =
                persistence_->sync_wait(persistence_->upsert_dag_info(
                    dag.dag_id, dag, dag_manager_.has_dag(dag.dag_id)));
            if (!persisted) {
              log::error("Failed to persist DAG {} from {}: {}", dag.dag_id,
                         dags_dir, persisted.error().message());
              return fail(persisted.error());
            }
            dag = std::move(*persisted);
          }
        }

        const auto previous_dags = dag_manager_.list_dags();
        if (auto replace_res = dag_manager_.replace_all(std::move(staged_dags));
            !replace_res) {
          return fail(replace_res.error());
        }

        refresh_scheduler_registrations(previous_dags);
        return ok(true);
      });
}

auto DagCatalogService::handle_file_change(
    std::string_view dags_dir, const std::filesystem::path &filename)
    -> Result<void> {
  if (!std::filesystem::exists(filename)) {
    DAGId dag_id{filename.stem().string()};

    log::debug("DAG file removed: {}", filename.string());
    if (auto result = dag_manager_.delete_dag(dag_id);
        !result && result.error() != make_error_code(Error::NotFound)) {
      log::warn("Failed to delete DAG {}: {}", dag_id,
                result.error().message());
      return fail(result.error());
    }
    if (scheduler_) {
      scheduler_->unregister_dag(dag_id);
    }
    return ok();
  }

  log::debug("DAG file changed: {}", filename.string());

  DAGFileLoader loader(dags_dir);
  return loader.load_file(filename)
      .and_then([&](const auto &file) {
        return reload_single_dag(file.dag_id, file.info);
      })
      .transform([&]() {
        log::debug("Successfully reloaded DAG from {}", filename.string());
        return;
      })
      .or_else([&](std::error_code ec) -> Result<void> {
        log::error("Failed to reload DAG from {}: {}", filename.string(),
                   ec.message());
        return fail(ec);
      });
}

auto DagCatalogService::ensure_materialized(const DAGId &dag_id)
    -> task<Result<DAGInfo>> {
  auto dag_res = dag_manager_.get_dag(dag_id);
  if (!dag_res) {
    co_return fail(dag_res.error());
  }

  auto snapshot_res =
      co_await materialize_snapshot(dag_id, std::move(*dag_res), false);
  if (!snapshot_res) {
    co_return fail(snapshot_res.error());
  }
  co_return ok(std::move(*snapshot_res));
}

auto DagCatalogService::materialize_snapshot(const DAGId &dag_id, DAGInfo info,
                                             bool force_refresh)
    -> task<Result<DAGInfo>> {
  const auto has_rowids = info.dag_rowid > 0 &&
                          std::ranges::all_of(info.tasks, [](const auto &task) {
                            return task.task_rowid > 0;
                          });
  const auto has_runtime_artifacts =
      static_cast<bool>(info.compiled_graph) &&
      static_cast<bool>(info.compiled_executor_configs) &&
      static_cast<bool>(info.compiled_indexed_task_configs);

  if (!force_refresh && has_rowids && has_runtime_artifacts) {
    co_return ok(std::move(info));
  }

  if (!force_refresh && has_rowids && !has_runtime_artifacts) {
    if (auto prepared = info.prepare_runtime_artifacts(); !prepared) {
      co_return fail(prepared.error());
    }
    co_return ok(std::move(info));
  }

  if (!persistence_ || !persistence_->is_open()) {
    co_return fail(Error::DatabaseError);
  }

  auto persisted = co_await persistence_->upsert_dag_info(
      dag_id, info, info.dag_rowid > 0);
  if (!persisted) {
    co_return fail(persisted.error());
  }

  dag_manager_.patch_dag_state_local(dag_id,
                                     state_from_snapshot_info(*persisted));
  co_return ok(std::move(*persisted));
}

auto DagCatalogService::validate_dag_info(const DAGInfo &info) const
    -> Result<void> {
  return DAGValidator::validate(info);
}

auto DagCatalogService::stage_dags_from_directory(
    std::string_view dags_dir, const DagStateIndex *state_index) const
    -> Result<std::vector<DAGInfo>> {
  DAGFileLoader loader(dags_dir);
  return loader.load_all_strict().and_then(
      [&](std::vector<DAGFile> &&dags) -> Result<std::vector<DAGInfo>> {
        std::vector<DAGInfo> staged_dags;
        staged_dags.reserve(dags.size());
        ankerl::unordered_dense::map<DAGId, std::size_t> staged_ids;
        staged_ids.reserve(dags.size());

        for (auto &dag_file : dags) {
          DAGInfo info = std::move(dag_file.info);
          info.dag_id = dag_file.dag_id.clone();

          if (const auto *index = state_index) {
            if (auto state_it = index->find(info.dag_id);
                state_it != index->end()) {
              apply_dag_state_to_dag_info(info, state_it->second);
            }
          }

          auto [_, inserted] =
              staged_ids.emplace(info.dag_id.clone(), staged_dags.size());
          if (!inserted) {
            log::error("Duplicate DAG id {} while loading {}", info.dag_id,
                       dags_dir);
            return fail(Error::AlreadyExists);
          }

          if (auto validation = validate_dag_info(info); !validation) {
            log::error("Failed to validate DAG {}: {}", info.dag_id,
                       validation.error().message());
            return fail(validation.error());
          }

          if (auto prepare = info.prepare_runtime_artifacts(); !prepare) {
            log::error("Failed to prepare DAG {} runtime artifacts: {}",
                       info.dag_id, prepare.error().message());
            return fail(prepare.error());
          }

          log::debug("Staged DAG {} with {} tasks from {}", info.dag_id,
                     info.tasks.size(), dags_dir);
          staged_dags.emplace_back(std::move(info));
        }

        return ok(std::move(staged_dags));
      });
}

auto DagCatalogService::reload_single_dag(const DAGId &dag_id,
                                          const DAGInfo &info) -> Result<void> {
  return validate_dag_info(info)
      .and_then([&]() { return dag_manager_.upsert_dag(dag_id, info); })
      .transform([&]() {
        if (!scheduler_ || !scheduler_->is_running()) {
          return;
        }

        scheduler_->unregister_dag(dag_id);
        if (!info.cron.empty()) {
          scheduler_->register_dag(dag_id, info);
        }
      });
}

auto DagCatalogService::refresh_scheduler_registrations(
    const std::vector<DAGInfo> &previous_dags) -> void {
  if (!scheduler_ || !scheduler_->is_running()) {
    return;
  }

  for (const auto &dag : previous_dags) {
    scheduler_->unregister_dag(dag.dag_id);
  }

  for (const auto &dag : dag_manager_.list_dags()) {
    scheduler_->register_dag(dag.dag_id, dag);
  }
}

} // namespace dagforge
