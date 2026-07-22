#pragma once

#if __has_include(<scope>)
#include <scope>
#else
#include <experimental/scope>
#endif

namespace dagforge {
#if __has_include(<scope>)
using std::scope_exit;
#else
using std::experimental::scope_exit;
#endif
} // namespace dagforge
