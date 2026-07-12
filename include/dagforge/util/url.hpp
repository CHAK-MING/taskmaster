#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"
#include <boost/url/encode.hpp>
#include <boost/url/parse.hpp>
#include <boost/url/rfc/unreserved_chars.hpp>

#include <cstdint>
#include <string>
#include <string_view>
#endif


namespace dagforge::util {

struct ParsedHttpUrl {
  std::string host;
  std::uint16_t port{80};
  std::string path{"/"};
  bool tls{false};
};

[[nodiscard]] inline auto url_encode(std::string_view input) -> std::string {
  return boost::urls::encode(input, boost::urls::unreserved_chars);
}

[[nodiscard]] inline auto parse_http_url(std::string_view url)
    -> Result<ParsedHttpUrl> {
  std::string normalized;
  if (url.find("://") == std::string_view::npos) {
    normalized = "http://";
    normalized.append(url);
    url = normalized;
  }

  auto parsed = boost::urls::parse_uri(url);
  if (!parsed) {
    return fail(Error::InvalidUrl);
  }
  const auto uri = *parsed;

  if (uri.scheme() != "http" && uri.scheme() != "https") {
    return fail(Error::InvalidUrl);
  }

  ParsedHttpUrl out;
  out.tls = uri.scheme() == "https";
  out.port = out.tls ? 443 : 80;
  out.host = std::string(uri.host());
  if (out.host.empty()) {
    return fail(Error::InvalidUrl);
  }

  if (uri.has_port()) {
    auto port = uri.port_number();
    if (port == 0) {
      return fail(Error::InvalidUrl);
    }
    out.port = static_cast<std::uint16_t>(port);
  }

  auto path = std::string(uri.encoded_path());
  if (path.empty()) {
    path = "/";
  }
  if (auto query = uri.encoded_query(); !query.empty()) {
    path.push_back('?');
    path.append(query.data(), query.size());
  }
  out.path = std::move(path);

  return out;
}

} // namespace dagforge::util
