#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/util/enum.hpp"
#if __has_include(<boost/mysql/format_sql.hpp>)
#include <boost/mysql/format_sql.hpp>
#endif

#include <boost/describe/enum.hpp>
#endif

#if __has_include(<boost/mysql/format_sql.hpp>)
namespace boost::mysql {

template <typename T>
  requires(std::is_enum_v<T> &&
           boost::describe::has_describe_enumerators<T>::value)
struct formatter<T> {
  auto parse(const char *begin, const char *) -> const char * { return begin; }

  auto format(T value, format_context_base &ctx) const -> void {
    boost::mysql::format_sql_to(ctx, "{}",
                                dagforge::util::enum_to_snake_case_view(value));
  }
};

} // namespace boost::mysql
#endif
