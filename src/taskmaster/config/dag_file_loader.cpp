#include "taskmaster/config/dag_file_loader.hpp"
#include "taskmaster/util/log.hpp"
#include "taskmaster/util/util.hpp"


namespace taskmaster {

DAGFileLoader::DAGFileLoader(std::string_view directory)
    : directory_(directory) {}

auto DAGFileLoader::load_all() -> Result<std::vector<DAGFile>> {
  if (!std::filesystem::exists(directory_)) {
    log::warn("DAG directory does not exist: {}", directory_.string());
    return ok(std::vector<DAGFile>{});
  }

  std::vector<DAGFile> dags;
  file_timestamps_.clear();

  for (const auto& entry : std::filesystem::directory_iterator(directory_)) {
    if (!entry.is_regular_file()) {
      continue;
    }

    auto ext = entry.path().extension().string();
    if (ext != ".yaml" && ext != ".yml") {
      continue;
    }

    auto result = load_file(entry.path());
    if (result) {
      file_timestamps_[entry.path().string()] = entry.last_write_time();
      dags.push_back(std::move(*result));
      log::info("Loaded DAG '{}' from file: {}", dags.back().dag_id, entry.path().string());
    } else {
      log::warn("Failed to load DAG from {}: {}", entry.path().string(),
                result.error().message());
    }
  }

  log::info("Loaded {} DAG(s) from {}", dags.size(), directory_.string());
  return ok(std::move(dags));
}

auto DAGFileLoader::load_file(const std::filesystem::path& path)
    -> Result<DAGFile> {
  auto def_result = DAGDefinitionLoader::load_from_file(path.string());
  if (!def_result) {
    return fail(def_result.error());
  }

  auto dag_id = path.stem().string();

  return ok(DAGFile{std::move(DAGId{dag_id}), std::move(*def_result)});
}


auto DAGFileLoader::set_on_dag_loaded(DAGLoadCallback cb) -> void {
  on_dag_loaded_ = std::move(cb);
}

auto DAGFileLoader::set_on_dag_removed(DAGRemoveCallback cb) -> void {
  on_dag_removed_ = std::move(cb);
}

auto DAGFileLoader::directory() const -> const std::filesystem::path& {
  return directory_;
}

auto DAGFileLoader::scan_for_changes()
    -> std::pair<std::vector<DAGFile>, std::vector<DAGId>> {
  std::vector<DAGFile> updated;
  std::vector<DAGId> removed;

  if (!std::filesystem::exists(directory_)) {
    return {updated, removed};
  }

  std::unordered_map<std::string, std::filesystem::file_time_type, StringHash, StringEqual> current_files;

  for (const auto& entry : std::filesystem::directory_iterator(directory_)) {
    if (!entry.is_regular_file()) {
      continue;
    }

    auto ext = entry.path().extension().string();
    if (ext != ".yaml" && ext != ".yml") {
      continue;
    }

    auto path_str = entry.path().string();
    auto last_write = entry.last_write_time();
    current_files[path_str] = last_write;

    auto it = file_timestamps_.find(path_str);
    if (it == file_timestamps_.end() || it->second != last_write) {
      auto result = load_file(entry.path());
      if (result) {
        updated.push_back(std::move(*result));
        file_timestamps_[path_str] = last_write;
      }
    }
  }

  for (const auto& [path, _] : file_timestamps_) {
    if (!current_files.contains(path)) {
      std::filesystem::path p(path);
      removed.push_back(DAGId{p.stem().string()});
    }
  }

  for (const auto& dag_id : removed) {
    std::erase_if(file_timestamps_, [&dag_id](const auto& entry) {
      return std::filesystem::path(entry.first).stem().string() == dag_id.value();
    });
  }

  return {updated, removed};
}

}  // namespace taskmaster
