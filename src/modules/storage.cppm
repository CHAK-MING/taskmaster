module;

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>

export module dagforge.storage.schema;

export import dagforge.dag;
export import dagforge.domain;

#define DAGFORGE_BUILDING_MODULE_INTERFACE 1
export {
#include "dagforge/storage/mysql_schema.hpp"
#include "dagforge/storage/orm_models.hpp"
}
#undef DAGFORGE_BUILDING_MODULE_INTERFACE
