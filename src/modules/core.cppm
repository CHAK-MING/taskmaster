module;

#include "dagforge/core/constants.hpp"
#include "dagforge/core/memory.hpp"
#include "dagforge/core/metrics.hpp"

export module dagforge.core;

export import dagforge.base;

export namespace dagforge {
namespace pmr = std::pmr;

using ::dagforge::Allocator;
using ::dagforge::ThreadMemoryResourceOverride;
} // namespace dagforge

export namespace dagforge::io {
using ::dagforge::io::kEventBufferSize;
using ::dagforge::io::kInitialOutputReserve;
using ::dagforge::io::kReadBufferSize;
} // namespace dagforge::io

export namespace dagforge::metrics {
using ::dagforge::metrics::Counter;
using ::dagforge::metrics::Histogram;
} // namespace dagforge::metrics

export namespace dagforge::timing {
using ::dagforge::timing::kConfigWatchInterval;
using ::dagforge::timing::kDaemonPollInterval;
using ::dagforge::timing::kRuntimeYieldInterval;
using ::dagforge::timing::kShutdownPollInterval;
} // namespace dagforge::timing
