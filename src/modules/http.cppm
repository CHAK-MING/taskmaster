module;

#include <boost/algorithm/string/predicate.hpp>
#include <boost/beast/http/status.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <format>
#include <initializer_list>
#include <iterator>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

export module dagforge.http;

export import dagforge.base;
export import dagforge.util;

#define DAGFORGE_BUILDING_MODULE_INTERFACE 1
export {
#include "dagforge/http/http_types.hpp"
}
#undef DAGFORGE_BUILDING_MODULE_INTERFACE
