#pragma once

#include "taskmaster/client/http/http_types.hpp"

#include <llhttp.h>

#include <memory>
#include <optional>
#include <span>

namespace taskmaster::http {

class HttpRequestParser {
public:
  HttpRequestParser();
  ~HttpRequestParser();

  HttpRequestParser(const HttpRequestParser&) = delete;
  auto operator=(const HttpRequestParser&) -> HttpRequestParser& = delete;

  auto parse(std::span<const uint8_t> data) -> std::optional<HttpRequest>;
  auto reset() -> void;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};


class HttpResponseParser {
public:
  HttpResponseParser();
  ~HttpResponseParser();

  HttpResponseParser(const HttpResponseParser&) = delete;
  auto operator=(const HttpResponseParser&) -> HttpResponseParser& = delete;

  auto parse(std::span<const uint8_t> data) -> std::optional<HttpResponse>;
  auto reset() -> void;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace taskmaster::http
