module;

#include <cerrno>

#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <source_location>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>

#if __has_include(<scope>)
#include <scope>
#else
#include <experimental/scope>
#endif

export module dagforge.base;

#define DAGFORGE_BUILDING_MODULE_INTERFACE 1
export {
#include "dagforge/core/contract.hpp"
// error.hpp expects the error-domain declarations to already be in the module
// purview because module-interface builds suppress textual dependency includes.
// clang-format off
#include "dagforge/core/error_domain.hpp"
#include "dagforge/core/error.hpp"
// clang-format on
#include "dagforge/core/scope_exit.hpp"
}
#undef DAGFORGE_BUILDING_MODULE_INTERFACE
