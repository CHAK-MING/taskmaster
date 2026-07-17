module;

#include <ankerl/unordered_dense.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cctype>
#include <charconv>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <expected>
#include <format>
#include <functional>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>

export module dagforge.util;

export import dagforge.base;

#define DAGFORGE_BUILDING_MODULE_INTERFACE 1
export {
#include "dagforge/util/ascii.hpp"
#include "dagforge/util/enum.hpp"
#include "dagforge/util/hash.hpp"
// conv.hpp projects the detailed parse contract into the legacy Result API.
// clang-format off
#include "dagforge/util/parse.hpp"
#include "dagforge/util/conv.hpp"
// clang-format on
#include "dagforge/util/string_hash.hpp"
#include "dagforge/util/time.hpp"
}
#undef DAGFORGE_BUILDING_MODULE_INTERFACE
