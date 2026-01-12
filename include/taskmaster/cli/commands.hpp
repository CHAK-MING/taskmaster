#pragma once

#include <cstdint>
#include <string>

namespace taskmaster::cli {

struct ServeOptions {
  std::string config_file;
  bool no_api{false};
  bool daemon{false};
};

struct RunOptions {
  std::string db_file;
  std::string dag_id;
};

struct ListOptions {
  std::string db_file;
};

struct ValidateOptions {
  std::string config_file;
};

struct StatusOptions {
  std::string db_file;
  std::string dag_id;
  std::string dag_run_id;
};

[[nodiscard]] auto cmd_serve(const ServeOptions& opts) -> int;
[[nodiscard]] auto cmd_run(const RunOptions& opts) -> int;
[[nodiscard]] auto cmd_list(const ListOptions& opts) -> int;
[[nodiscard]] auto cmd_validate(const ValidateOptions& opts) -> int;
[[nodiscard]] auto cmd_status(const StatusOptions& opts) -> int;

}  // namespace taskmaster::cli
