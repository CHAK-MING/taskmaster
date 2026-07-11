#pragma once

#if defined(__GNUC__)
#pragma GCC system_header
#endif

// build2 extracts dependencies with -fdirectives-only. benchmark.h probes
// __COUNTER__ in a preprocessor condition, which GCC rejects in that mode.
// For DAGForge benchmarks we force benchmark.h onto its __LINE__ fallback.
#ifdef __COUNTER__
#undef __COUNTER__
#endif

#include <benchmark/benchmark.h>
