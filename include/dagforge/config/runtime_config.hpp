#pragma once

namespace dagforge::config {

struct RuntimeConfig {
  int shards{0};
  bool pin_shards_to_cores{false};
  int cpu_affinity_offset{0};

  auto operator==(const RuntimeConfig &) const -> bool = default;
};

} // namespace dagforge::config
