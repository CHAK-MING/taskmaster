#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#endif

namespace dagforge::config {

struct WorkflowConfig {
  bool enabled{true};

  auto operator==(const WorkflowConfig &) const -> bool = default;
};

} // namespace dagforge::config
