module;

#include <cstdint>

export module dagforge.workflow;

export import dagforge.base;
export import dagforge.domain;

export namespace dagforge::workflow {
inline constexpr std::uint32_t kWorkflowSchemaVersion = 1;
}
