#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/util/json.hpp"

#include <string>
#include <vector>
#endif

namespace dagforge::workflow {

struct ExecutorDescription {
  std::string type;
  std::string summary;
  JsonPayload config_schema;
  std::vector<JsonPayload> examples;
  JsonPayload constraints;
};

} // namespace dagforge::workflow
