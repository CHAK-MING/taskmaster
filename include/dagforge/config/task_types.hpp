#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"
#include "dagforge/util/enum.hpp"
#include "dagforge/util/hash.hpp"
#include "dagforge/util/id.hpp"
#endif

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <memory>
#include <ranges>
#include <regex>
#include <string>
#include <utility>
#include <vector>
#endif

namespace dagforge {

enum class XComSource : std::uint8_t { Stdout, Stderr, ExitCode, Json };

} // namespace dagforge

namespace glz {
template <> struct meta<dagforge::XComSource> {
  static constexpr auto value =
      glz::enumerate("stdout", dagforge::XComSource::Stdout, "stderr",
                dagforge::XComSource::Stderr, "exit_code",
                dagforge::XComSource::ExitCode, "json",
                dagforge::XComSource::Json);
};
} // namespace glz

namespace dagforge {

[[nodiscard]] constexpr auto to_string_view(XComSource value) noexcept
    -> std::string_view {
  return ::dagforge::util::enum_to_string_view(value);
}

template <>
[[nodiscard]] inline auto parse<XComSource>(std::string_view s) noexcept
    -> XComSource {
  return ::dagforge::util::parse_enum(s, XComSource::Stdout);
}

struct XComPushConfig {
  std::string key;
  XComSource source{XComSource::Stdout};
  std::string json_pointer;
  std::string regex_pattern;
  int regex_group{0};
  std::shared_ptr<const std::regex> compiled_regex{};

  [[nodiscard]] auto prepare() -> Result<void> {
    if (!json_pointer.empty()) {
      if (json_pointer.front() != '/') {
        return fail(Error::InvalidArgument);
      }
      for (std::size_t i = 0; i < json_pointer.size(); ++i) {
        if (json_pointer[i] != '~') {
          continue;
        }
        if (++i >= json_pointer.size() ||
            (json_pointer[i] != '0' && json_pointer[i] != '1')) {
          return fail(Error::InvalidArgument);
        }
      }
    }

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
  auto operator()(const XComRef &ref) const noexcept -> std::size_t {
    using HashKey = std::pair<std::string_view, std::string_view>;
    return static_cast<std::size_t>(ankerl::unordered_dense::hash<HashKey>{}(
        HashKey{ref.task_id.value(), ref.key}));
  }
};

} // namespace dagforge
