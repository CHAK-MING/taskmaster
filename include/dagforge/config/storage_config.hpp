#pragma once

#include <cstddef>
#include <string>

namespace dagforge::config {

struct StorageConfig {
  bool enabled{false};
  std::string directory{"./state"};
  std::size_t max_completed_runs{10'000};
  std::size_t max_evidence_records{100'000};
  std::size_t max_plan_bytes{8 * 1024 * 1024};
  std::size_t max_checkpoint_bytes{64 * 1024 * 1024};
  std::size_t max_evidence_file_bytes{256 * 1024 * 1024};
  std::size_t max_evidence_record_bytes{1024 * 1024};
  std::size_t max_artifact_metadata_bytes{1024 * 1024};
  std::size_t max_artifact_bytes{256 * 1024 * 1024};

  auto operator==(const StorageConfig &) const -> bool = default;
};

} // namespace dagforge::config
