#pragma once

#include "model.hpp"

namespace dagforge::jsonata::detail {

[[nodiscard]] auto lower_path_ast(ProgramData &program,
                                  const CompileLimits &limits) -> Result<void>;

} // namespace dagforge::jsonata::detail
