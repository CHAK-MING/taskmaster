#pragma once

#include "dagforge/util/json.hpp"

#include <string>
#include <vector>

namespace dagforge::workflow {

struct ExecutorDescription {
  std::string type;
  std::string summary;
  JsonPayload config_schema;
  std::vector<JsonPayload> examples;
  JsonPayload constraints;
};

} // namespace dagforge::workflow
