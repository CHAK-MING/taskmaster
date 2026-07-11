#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"
#include <string>
#endif

namespace dagforge {

struct SampleRecord {
  std::string name;
  int value{0};
};

[[nodiscard]] auto validate_sample_record(const SampleRecord &record)
    -> Result<void>;

} // namespace dagforge

