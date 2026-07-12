#pragma once


#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/util/conv.hpp"
#include "dagforge/util/enum.hpp"
#include "dagforge/util/hash.hpp"
#include "dagforge/util/time.hpp"
#include "dagforge/util/string_hash.hpp"
#include "dagforge/util/url.hpp"
#endif

namespace dagforge {

template <class... Ts> struct overloaded : Ts... {
  using Ts::operator()...;
};
template <class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

} // namespace dagforge
