module;

#include <string>
#include <string_view>

export module dagforge.sample;

export import dagforge.core;

#define DAGFORGE_BUILDING_MODULE_INTERFACE 1
export {
#include "dagforge/sample/sample_service.hpp"
#include "dagforge/sample/sample_public_header.hpp"
}
#undef DAGFORGE_BUILDING_MODULE_INTERFACE
