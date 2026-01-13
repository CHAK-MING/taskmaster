#pragma once

#include "taskmaster/util/id.hpp"

#include <nlohmann/json.hpp>

#include <chrono>
#include <optional>
#include <string>
#include <unordered_map>

namespace taskmaster {

struct XComValue {
  std::string key;
  nlohmann::json value;
  std::size_t byte_size{0};
  std::chrono::system_clock::time_point created_at;

  [[nodiscard]] auto as_string() const -> std::string {
    if (value.is_string()) {
      return value.get<std::string>();
    }
    return value.dump();
  }

  [[nodiscard]] auto as_int() const -> std::optional<int64_t> {
    if (value.is_number_integer()) {
      return value.get<int64_t>();
    }
    return std::nullopt;
  }

  [[nodiscard]] auto as_double() const -> std::optional<double> {
    if (value.is_number()) {
      return value.get<double>();
    }
    return std::nullopt;
  }
};

struct XComRef {
  TaskId task_id;
  std::string key;

  auto operator==(const XComRef& other) const -> bool = default;
};

struct XComRefHash {
  auto operator()(const XComRef& ref) const -> std::size_t {
    return std::hash<std::string>{}(std::string(ref.task_id.value())) ^
           (std::hash<std::string>{}(ref.key) << 1);
  }
};

using XComMap = std::unordered_map<std::string, XComValue>;

class XComCache {
public:
  void set(const DAGRunId& run_id, const TaskId& task_id,
           const std::string& key, const nlohmann::json& value) {
    auto cache_key = make_key(run_id, task_id, key);
    cache_[cache_key] = value;
  }

  [[nodiscard]] auto get(const DAGRunId& run_id, const TaskId& task_id,
                         const std::string& key) const
      -> std::optional<nlohmann::json> {
    auto cache_key = make_key(run_id, task_id, key);
    auto it = cache_.find(cache_key);
    if (it != cache_.end()) {
      return std::make_optional(it->second);
    }
    return std::nullopt;
  }

  void clear_run(const DAGRunId& run_id) {
    std::string prefix = std::string(run_id.value()) + ":";
    std::erase_if(cache_, [&prefix](const auto& pair) {
      return pair.first.starts_with(prefix);
    });
  }

  void clear() { cache_.clear(); }

private:
  [[nodiscard]] static auto make_key(const DAGRunId& run_id,
                                      const TaskId& task_id,
                                      const std::string& key) -> std::string {
    return std::string(run_id.value()) + ":" + 
           std::string(task_id.value()) + ":" + key;
  }

  std::unordered_map<std::string, nlohmann::json> cache_;
};

}  // namespace taskmaster
