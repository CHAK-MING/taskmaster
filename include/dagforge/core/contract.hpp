#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <source_location>
#include <string_view>
#endif

namespace dagforge {

[[noreturn]] auto contract_violation(
    std::string_view message,
    std::source_location origin = std::source_location::current()) noexcept
    -> void;

} // namespace dagforge
