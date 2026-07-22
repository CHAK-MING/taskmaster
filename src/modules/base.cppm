module;

#include "dagforge/core/contract.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/core/error_domain.hpp"
#include "dagforge/core/scope_exit.hpp"

export module dagforge.base;

export namespace dagforge {
using ::dagforge::contract_violation;
using ::dagforge::Error;
using ::dagforge::error_category;
using ::dagforge::ErrorCategory;
using ::dagforge::ErrorDomainEntry;
using ::dagforge::fail;
using ::dagforge::make_error_code;
using ::dagforge::ok;
using ::dagforge::Result;
using ::dagforge::ResultValue;
using ::dagforge::scope_exit;
using ::dagforge::StaticErrorCategory;
using ::dagforge::sys_check;
using ::dagforge::to_string_view;
} // namespace dagforge
