#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/coroutine.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/core/runtime.hpp"

#include <string>
#endif

namespace dagforge {

class SampleService {
public:
  explicit SampleService(Runtime &runtime);

  [[nodiscard]] auto fetch(std::string key) -> task<Result<std::string>>;
  [[nodiscard]] auto store(std::string key, std::string value)
      -> task<Result<void>>;

private:
  Runtime &runtime_;
};

} // namespace dagforge
