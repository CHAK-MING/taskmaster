#pragma once

#include "taskmaster/core/error.hpp"
#include "taskmaster/dag/dag_run.hpp"
#include "taskmaster/storage/persistence.hpp"
#include "taskmaster/util/id.hpp"

#include <memory>
#include <nlohmann/json.hpp>
#include <string_view>

namespace taskmaster {

class PersistenceService {
public:
  explicit PersistenceService(std::string_view db_path);
  ~PersistenceService();

  PersistenceService(const PersistenceService&) = delete;
  auto operator=(const PersistenceService&) -> PersistenceService& = delete;

  [[nodiscard]] auto open() -> Result<void>;
  auto close() -> void;
  [[nodiscard]] auto is_open() const -> bool;

  [[nodiscard]] auto persistence() -> Persistence*;

  auto save_run(const DAGRun& run) -> void;
  auto save_task(DAGRunId dag_run_id, const TaskInstanceInfo& info) -> void;
  auto save_log(DAGRunId dag_run_id, TaskId task, int attempt,
                std::string_view level, std::string_view msg) -> void;
  auto save_xcom(DAGRunId dag_run_id, TaskId task,
                 std::string_view key, const nlohmann::json& value) -> void;

private:
  std::unique_ptr<Persistence> db_;
};

}  // namespace taskmaster
