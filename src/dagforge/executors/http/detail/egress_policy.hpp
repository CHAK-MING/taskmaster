#pragma once

#include "dagforge/config/http_executor_config.hpp"
#include "dagforge/core/error.hpp"

#include <boost/asio/ip/address.hpp>

#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

namespace dagforge::executors::http::detail {

struct ParsedHttpTarget {
  bool tls{false};
  std::string host;
  std::uint16_t port{80};
  std::string target{"/"};
  std::string origin;
  std::string host_header;
};

class HttpEgressPolicy {
public:
  struct IpCidr {
    boost::asio::ip::address network;
    unsigned prefix_length{0};
  };

  [[nodiscard]] static auto create(config::HttpEgressConfig config)
      -> Result<HttpEgressPolicy>;

  [[nodiscard]] auto authorize(std::string_view url) const
      -> Result<ParsedHttpTarget>;
  [[nodiscard]] auto
  address_allowed(const boost::asio::ip::address &address) const -> bool;
  [[nodiscard]] auto config() const noexcept
      -> const config::HttpEgressConfig & {
    return config_;
  }

private:
  config::HttpEgressConfig config_;
  std::unordered_set<std::string> allowed_origins_;
  std::vector<IpCidr> allowed_cidrs_;
};

} // namespace dagforge::executors::http::detail
