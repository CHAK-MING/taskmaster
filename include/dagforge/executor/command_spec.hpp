#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <flat_map>
#include <string>
#include <vector>
#endif

namespace dagforge {

struct CommandSpec {
  std::string program;
  std::vector<std::string> arguments;
  std::flat_map<std::string, std::string, std::less<>> environment;

  auto operator==(const CommandSpec &) const -> bool = default;
};

inline constexpr int kExitCodeTimeout = 124;

} // namespace dagforge
