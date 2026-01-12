#pragma once

#include "taskmaster/config/dag_definition.hpp"
#include "taskmaster/core/error.hpp"

#include <filesystem>
#include <functional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace taskmaster {

struct DAGFile {
  DAGId dag_id;
  DAGDefinition definition;
};

using DAGLoadCallback = std::function<void(DAGId, const DAGDefinition&)>;
using DAGRemoveCallback = std::function<void(DAGId dag_id)>;

class DAGFileLoader {
public:
  explicit DAGFileLoader(std::string_view directory);

  [[nodiscard]] auto load_all() -> Result<std::vector<DAGFile>>;
  [[nodiscard]] auto load_file(const std::filesystem::path& path)
      -> Result<DAGFile>;
  [[nodiscard]] auto scan_for_changes()
      -> std::pair<std::vector<DAGFile>, std::vector<DAGId>>;

  auto set_on_dag_loaded(DAGLoadCallback cb) -> void;
  auto set_on_dag_removed(DAGRemoveCallback cb) -> void;

  [[nodiscard]] auto directory() const -> const std::filesystem::path&;

private:
  std::filesystem::path directory_;
  std::unordered_map<std::string, std::filesystem::file_time_type>
      file_timestamps_;
  DAGLoadCallback on_dag_loaded_;
  DAGRemoveCallback on_dag_removed_;
};

}  // namespace taskmaster
