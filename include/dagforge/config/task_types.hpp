#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"
#include "dagforge/util/enum.hpp"
#include "dagforge/util/hash.hpp"
#include "dagforge/util/id.hpp"
#endif

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <boost/describe/enum.hpp>

#include <memory>
#include <ranges>
#include <regex>
#include <string>
#include <vector>
#endif

namespace dagforge {

enum class XComSource : std::uint8_t { Stdout, Stderr, ExitCode, Json };
BOOST_DESCRIBE_ENUM(XComSource, Stdout, Stderr, ExitCode, Json)

[[nodiscard]] constexpr auto to_string_view(XComSource value) noexcept
    -> std::string_view {
  return ::dagforge::util::enum_to_snake_case_view(value);
}

template <>
[[nodiscard]] inline auto parse<XComSource>(std::string_view s) noexcept
    -> XComSource {
  return ::dagforge::util::parse_enum(s, XComSource::Stdout);
}

struct XComPushConfig {
  std::string key;
  XComSource source{XComSource::Stdout};
  std::string json_path;
  std::string regex_pattern;
  int regex_group{0};
  std::shared_ptr<const std::regex> compiled_regex{};

  [[nodiscard]] auto compile_regex() -> Result<void> {
    if (regex_pattern.empty()) {
      compiled_regex.reset();
      return ok();
    }
    try {
      compiled_regex = std::make_shared<const std::regex>(regex_pattern);
      return ok();
    } catch (const std::regex_error &) {
      return fail(Error::InvalidArgument);
    }
  }
};

struct TaskDependency {
  TaskId task_id;
  std::string label;

  bool operator==(const TaskId &other) const { return task_id == other; }
  bool operator==(const TaskDependency &other) const = default;
};

inline auto get_dep_task_ids(const std::vector<TaskDependency> &deps) {
  return deps |
         std::views::transform([](const TaskDependency &d) -> const TaskId & {
           return d.task_id;
         });
}

struct XComRef {
  TaskId task_id;
  std::string key;

  auto operator==(const XComRef &other) const -> bool = default;
};

struct XComPullConfig {
  XComRef ref;
  std::string env_var;
  bool required{false};
  std::string default_value_json;
  std::string default_value_rendered;
  bool has_default_value{false};

  [[nodiscard]] auto source_task() const noexcept -> const TaskId & {
    return ref.task_id;
  }

  [[nodiscard]] auto key() const noexcept -> const std::string & {
    return ref.key;
  }
};

struct XComRefHash {
  auto operator()(const XComRef &ref) const -> std::size_t {
    return util::combine(ref.task_id, ref.key);
  }
};

} // namespace dagforge
