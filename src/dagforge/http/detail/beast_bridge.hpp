#pragma once

#include "dagforge/core/error.hpp"
#include "dagforge/http/http_types.hpp"

#include <boost/beast/http.hpp>

#include <cstdint>
#include <string_view>

namespace dagforge::http::detail {

namespace beast_http = boost::beast::http;

using BeastRequest =
    beast_http::request<beast_http::vector_body<std::uint8_t>>;
using BeastResponse =
    beast_http::response<beast_http::vector_body<std::uint8_t>>;

[[nodiscard]] auto from_beast_method(beast_http::verb method) noexcept
    -> Result<HttpMethod>;
[[nodiscard]] auto to_beast_method(HttpMethod method) noexcept
    -> beast_http::verb;

[[nodiscard]] auto from_beast_request(const BeastRequest &request)
    -> Result<HttpRequest>;
[[nodiscard]] auto from_beast_response(const BeastResponse &response)
    -> HttpResponse;

[[nodiscard]] auto to_beast_request(HttpRequest request, std::string_view host,
                                    bool keep_alive)
    -> Result<BeastRequest>;
[[nodiscard]] auto to_beast_response(const HttpResponse &response,
                                     unsigned version, bool keep_alive)
    -> BeastResponse;

} // namespace dagforge::http::detail
