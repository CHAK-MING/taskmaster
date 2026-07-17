#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#if __has_include(<scope>)
#include <scope>
#else
#include <experimental/scope>
#endif
#endif

namespace dagforge {
#if __has_include(<scope>)
using std::scope_exit;
#else
using std::experimental::scope_exit;
#endif
} // namespace dagforge
