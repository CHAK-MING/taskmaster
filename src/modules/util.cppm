module;

#include <ankerl/unordered_dense.h>
#include <glaze/core/common.hpp>
#include <time.h>

#include <bit>
#include <cctype>
#include <charconv>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <format>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>

export module dagforge.util;

export import dagforge.base;

#define DAGFORGE_BUILDING_MODULE_INTERFACE 1
export {
#include "dagforge/util/enum.hpp"
#include "dagforge/util/hash.hpp"
#include "dagforge/util/conv.hpp"
#include "dagforge/util/time.hpp"
#include "dagforge/util/string_hash.hpp"
}
#undef DAGFORGE_BUILDING_MODULE_INTERFACE
