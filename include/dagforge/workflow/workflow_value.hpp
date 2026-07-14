#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/util/id.hpp"
#include "dagforge/util/json.hpp"
#include <cstdint>
#include <string>
#include <variant>
#include <vector>
#endif

namespace dagforge::workflow {

struct Principal {
  std::string subject;
  std::vector<std::string> roles;
};

struct TraceContext {
  std::string trace_id;
  std::string parent_span_id;
};

struct ArtifactRef {
  ArtifactId artifact_id;
  std::string media_type{"application/octet-stream"};
  std::uint64_t size_bytes{0};
  std::string digest;
};

using WorkflowValue =
    std::variant<std::monostate, bool, std::int64_t, double, std::string,
                 JsonValue, ArtifactRef>;

struct OutputRef {
  WorkflowNodeId node_id;
  WorkflowPortId port;

  auto operator==(const OutputRef &) const -> bool = default;
};

struct InputBinding {
  WorkflowPortId input;
  OutputRef source;
};

} // namespace dagforge::workflow
