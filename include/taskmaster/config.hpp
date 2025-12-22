#pragma once

#include <chrono>
#include <string>
#include <unordered_map>
#include <vector>

#include "taskmaster/error.hpp"

namespace taskmaster {

struct TaskConfig {
  std::string id;
  std::string name;
  std::string cron;
  std::string command;
  std::string working_dir;
  std::vector<std::string> deps;
  std::chrono::seconds timeout{std::chrono::seconds(300)};
  int max_retries{3};
  bool enabled{true};
};

struct SchedulerConfig {
  std::string log_level{"info"};
  std::string log_file;
  std::string pid_file;
  int tick_interval_ms{1000};
};

struct ApiConfig {
  bool enabled{false};
  uint16_t port{8080};
  std::string host{"127.0.0.1"};
};

struct Config {
  SchedulerConfig scheduler;
  ApiConfig api;
  std::vector<TaskConfig> tasks;
  std::unordered_map<std::string, std::size_t, StringHash, std::equal_to<>>
      task_index;

  [[nodiscard]] auto find_task(std::string_view task_id) const
      -> const TaskConfig * {
    auto it = task_index.find(task_id);
    return it != task_index.end() ? &tasks[it->second] : nullptr;
  }

  auto rebuild_index() -> void {
    task_index.clear();
    for (std::size_t i = 0; i < tasks.size(); ++i) {
      task_index[tasks[i].id] = i;
    }
  }
};

class ConfigLoader {
public:
  [[nodiscard]] static auto load_from_file(std::string_view path)
      -> Result<Config>;
  [[nodiscard]] static auto load_from_string(std::string_view yaml_str)
      -> Result<Config>;
  [[nodiscard]] static auto save_to_file(const Config &config,
                                         std::string_view path) -> Result<void>;
  [[nodiscard]] static auto to_string(const Config &config) -> std::string;
};

} // namespace taskmaster
