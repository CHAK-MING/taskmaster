#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <cstddef>
#include <string>
#endif

namespace dagforge::config {

struct StorageConfig {
  bool enabled{false};
  std::string directory{"./state"};
  std::size_t max_completed_runs{10'000};
  std::size_t max_evidence_records{100'000};

  auto operator==(const StorageConfig &) const -> bool = default;
};

} // namespace dagforge::config
