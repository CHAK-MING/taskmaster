#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>
#endif

namespace dagforge::config {

struct AdmissionConfig {
  bool allow_unlisted_executors{false};
  std::vector<std::string> allowed_executors;
  std::size_t max_nodes{256};
  std::size_t max_parallel_nodes{32};
  std::uint64_t max_total_output_bytes{64ULL * 1024ULL * 1024ULL};
  int max_run_duration_sec{3600};

  auto operator==(const AdmissionConfig &) const -> bool = default;
};

} // namespace dagforge::config
