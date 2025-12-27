#pragma once

#include "taskmaster/core/error.hpp"
#include "taskmaster/dag/dag_run.hpp"
#include "taskmaster/storage/persistence.hpp"

#include <memory>
#include <string_view>

namespace taskmaster {

// Wraps Persistence with fire-and-forget semantics for non-critical operations
class PersistenceService {
public:
  explicit PersistenceService(std::string_view db_path);
  ~PersistenceService();

  PersistenceService(const PersistenceService&) = delete;
  auto operator=(const PersistenceService&) -> PersistenceService& = delete;

  [[nodiscard]] auto open() -> Result<void>;
  auto close() -> void;
  [[nodiscard]] auto is_open() const -> bool;

  // Direct access for complex operations
  [[nodiscard]] auto persistence() -> Persistence*;

  // Fire-and-forget operations (log errors but don't fail)
  auto save_run(const DAGRun& run) -> void;
  auto save_task(std::string_view run_id, const TaskInstanceInfo& info) -> void;
  auto save_log(std::string_view run_id, std::string_view task, int attempt,
                std::string_view level, std::string_view msg) -> void;

private:
  std::unique_ptr<Persistence> db_;
};

}  // namespace taskmaster
