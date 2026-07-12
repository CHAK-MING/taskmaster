module;

#include <boost/uuid/time_generator_v7.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <algorithm>
#include <cctype>
#include <compare>
#include <concepts>
#include <cstddef>
#include <format>
#include <functional>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>

export module dagforge.domain;

#define DAGFORGE_BUILDING_MODULE_INTERFACE 1
export {
#include "dagforge/util/id.hpp"
}
#undef DAGFORGE_BUILDING_MODULE_INTERFACE
