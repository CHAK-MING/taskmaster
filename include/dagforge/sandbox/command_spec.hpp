#pragma once

#include <flat_map>
#include <string>
#include <vector>

namespace dagforge::sandbox {

struct CommandSpec {
  std::string program;
  std::vector<std::string> arguments;
  std::flat_map<std::string, std::string, std::less<>> environment;

  auto operator==(const CommandSpec &) const -> bool = default;
};

inline constexpr int kExitCodeTimeout = 124;

} // namespace dagforge::sandbox
