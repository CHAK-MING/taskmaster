#include "detail/egress_policy.hpp"

#include <boost/system/system_error.hpp>
#include <boost/url/url.hpp>

#include <algorithm>
#include <array>
#include <cctype>
#include <charconv>
#include <format>
#include <ranges>
#include <span>
#include <string>
#include <utility>

namespace dagforge::executors::http::detail {
namespace {

[[nodiscard]] auto lowercase_ascii(std::string value) -> std::string {
  std::ranges::transform(value, value.begin(), [](unsigned char ch) {
    return static_cast<char>(std::tolower(ch));
  });
  return value;
}

[[nodiscard]] auto prefix_matches(std::span<const unsigned char> address,
                                  std::span<const unsigned char> network,
                                  unsigned prefix_length) -> bool {
  const auto whole_bytes = prefix_length / 8;
  const auto remaining_bits = prefix_length % 8;
  if (!std::equal(address.begin(), address.begin() + whole_bytes,
                  network.begin())) {
    return false;
  }
  if (remaining_bits == 0) {
    return true;
  }
  const auto mask = static_cast<unsigned char>(0xffU << (8U - remaining_bits));
  return (address[whole_bytes] & mask) == (network[whole_bytes] & mask);
}

[[nodiscard]] auto parse_target(std::string_view url)
    -> Result<ParsedHttpTarget> {
  if (url.find("://") == std::string_view::npos) {
    return fail(Error::InvalidUrl);
  }
  boost::urls::url uri;
  try {
    uri = boost::urls::url{url};
  } catch (const boost::system::system_error &) {
    return fail(Error::InvalidUrl);
  }
  const auto scheme = lowercase_ascii(std::string{uri.scheme()});
  if (scheme != "http" && scheme != "https") {
    return fail(Error::InvalidUrl);
  }
  if (!uri.userinfo().empty() || !uri.fragment().empty()) {
    return fail(Error::InvalidUrl);
  }

  auto host = lowercase_ascii(std::string{uri.host()});
  if (host.empty()) {
    return fail(Error::InvalidUrl);
  }
  const bool tls = scheme == "https";
  std::uint16_t port = tls ? 443 : 80;
  if (uri.has_port()) {
    const auto parsed_port = uri.port_number();
    if (parsed_port == 0 || parsed_port > 65535) {
      return fail(Error::InvalidUrl);
    }
    port = static_cast<std::uint16_t>(parsed_port);
  }

  auto authority_host = host;
  if (host.contains(':') && !host.starts_with('[')) {
    authority_host = std::format("[{}]", host);
  }
  auto target = std::string{uri.encoded_path()};
  if (target.empty()) {
    target = "/";
  }
  if (const auto query = uri.encoded_query(); !query.empty()) {
    target.push_back('?');
    target.append(query.data(), query.size());
  }

  const auto default_port = tls ? 443 : 80;
  auto host_header = authority_host;
  if (port != default_port) {
    host_header.append(std::format(":{}", port));
  }
  return ok(ParsedHttpTarget{
      .tls = tls,
      .host = std::move(host),
      .port = port,
      .target = std::move(target),
      .origin = std::format("{}://{}:{}", scheme, authority_host, port),
      .host_header = std::move(host_header),
  });
}

[[nodiscard]] auto parse_cidr(std::string_view value)
    -> Result<HttpEgressPolicy::IpCidr> {
  const auto separator = value.rfind('/');
  if (separator == std::string_view::npos || separator == 0 ||
      separator + 1 >= value.size()) {
    return fail(Error::InvalidArgument);
  }
  boost::system::error_code address_error;
  auto address = boost::asio::ip::make_address(
      std::string{value.substr(0, separator)}, address_error);
  if (address_error) {
    return fail(Error::InvalidArgument);
  }
  unsigned prefix = 0;
  const auto token = value.substr(separator + 1);
  const auto [end, error] =
      std::from_chars(token.data(), token.data() + token.size(), prefix);
  const auto maximum = address.is_v4() ? 32U : 128U;
  if (error != std::errc{} || end != token.data() + token.size() ||
      prefix > maximum) {
    return fail(Error::InvalidArgument);
  }
  return ok(HttpEgressPolicy::IpCidr{.network = std::move(address),
                                     .prefix_length = prefix});
}

[[nodiscard]] auto cidr_contains(const HttpEgressPolicy::IpCidr &cidr,
                                 const boost::asio::ip::address &address)
    -> bool {
  if (cidr.network.is_v4() != address.is_v4()) {
    return false;
  }
  if (address.is_v4()) {
    const auto candidate = address.to_v4().to_bytes();
    const auto network = cidr.network.to_v4().to_bytes();
    return prefix_matches(candidate, network, cidr.prefix_length);
  }
  const auto candidate = address.to_v6().to_bytes();
  const auto network = cidr.network.to_v6().to_bytes();
  return prefix_matches(candidate, network, cidr.prefix_length);
}

[[nodiscard]] auto special_use_address(const boost::asio::ip::address &address)
    -> bool {
  if (address.is_unspecified() || address.is_loopback() ||
      address.is_multicast()) {
    return true;
  }
  if (address.is_v4()) {
    const auto value = address.to_v4().to_uint();
    const auto in_range = [value](std::uint32_t network, unsigned prefix) {
      const auto mask = prefix == 0 ? 0U : 0xffffffffU << (32U - prefix);
      return (value & mask) == (network & mask);
    };
    return in_range(0x00000000U, 8) || in_range(0x0a000000U, 8) ||
           in_range(0x64400000U, 10) || in_range(0x7f000000U, 8) ||
           in_range(0xa9fe0000U, 16) || in_range(0xac100000U, 12) ||
           in_range(0xc0000000U, 24) || in_range(0xc0000200U, 24) ||
           in_range(0xc0a80000U, 16) || in_range(0xc6120000U, 15) ||
           in_range(0xc6336400U, 24) || in_range(0xcb007100U, 24) ||
           in_range(0xe0000000U, 4) || in_range(0xf0000000U, 4);
  }
  const auto v6 = address.to_v6();
  if (v6.is_link_local() || v6.is_site_local()) {
    return true;
  }
  if (v6.is_v4_mapped()) {
    const auto bytes = v6.to_bytes();
    return special_use_address(boost::asio::ip::address_v4{
        {bytes[12], bytes[13], bytes[14], bytes[15]}});
  }
  const auto bytes = v6.to_bytes();
  const std::array<unsigned char, 16> ula{0xfc};
  const std::array<unsigned char, 16> documentation{0x20, 0x01, 0x0d, 0xb8};
  return prefix_matches(bytes, ula, 7) ||
         prefix_matches(bytes, documentation, 32);
}

} // namespace

auto HttpEgressPolicy::create(config::HttpEgressConfig config)
    -> Result<HttpEgressPolicy> {
  if (config.max_request_headers == 0 || config.max_request_header_bytes == 0 ||
      config.max_request_body_bytes == 0 || config.max_response_headers == 0 ||
      config.max_response_header_bytes == 0 ||
      config.max_response_body_bytes == 0 ||
      config.max_concurrent_requests_per_shard == 0 ||
      config.max_concurrent_requests == 0 ||
      (config.tls_min_version != "1.2" && config.tls_min_version != "1.3") ||
      (config.tls_client_cert_file.empty() !=
       config.tls_client_key_file.empty())) {
    return fail(Error::InvalidArgument);
  }

  HttpEgressPolicy policy;
  policy.config_ = std::move(config);
  for (const auto &configured : policy.config_.allowed_origins) {
    auto target = parse_target(configured);
    if (!target) {
      return fail(target.error());
    }
    if (target->target != "/") {
      return fail(Error::InvalidArgument);
    }
    if (!target->tls && !policy.config_.allow_plaintext) {
      return fail(Error::Unauthorized);
    }
    if (!policy.allowed_origins_.emplace(std::move(target->origin)).second) {
      return fail(Error::InvalidArgument);
    }
  }
  policy.allowed_cidrs_.reserve(policy.config_.allowed_ip_cidrs.size());
  for (const auto &configured : policy.config_.allowed_ip_cidrs) {
    auto cidr = parse_cidr(configured);
    if (!cidr) {
      return fail(cidr.error());
    }
    policy.allowed_cidrs_.push_back(std::move(*cidr));
  }
  return ok(std::move(policy));
}

auto HttpEgressPolicy::authorize(std::string_view url) const
    -> Result<ParsedHttpTarget> {
  auto target = parse_target(url);
  if (!target) {
    return fail(target.error());
  }
  if (!target->tls && !config_.allow_plaintext) {
    return fail(Error::Unauthorized);
  }
  if (!allowed_origins_.contains(target->origin)) {
    return fail(Error::Unauthorized);
  }
  return target;
}

auto HttpEgressPolicy::address_allowed(
    const boost::asio::ip::address &address) const -> bool {
  if (std::ranges::any_of(allowed_cidrs_, [&](const IpCidr &cidr) {
        return cidr_contains(cidr, address);
      })) {
    return true;
  }
  return !config_.deny_private_networks || !special_use_address(address);
}

} // namespace dagforge::executors::http::detail
