#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <boost/beast/core/detail/base64.hpp>

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#endif

namespace dagforge::util {

[[nodiscard]] inline auto base64_encode(std::span<const std::byte> data)
    -> std::string {
  std::string result(boost::beast::detail::base64::encoded_size(data.size()),
                     '\0');
  auto written = boost::beast::detail::base64::encode(result.data(),
                                                      data.data(), data.size());
  result.resize(written);
  return result;
}

} // namespace dagforge::util
