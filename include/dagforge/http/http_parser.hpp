#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/http/http_types.hpp"
#include "dagforge/core/error.hpp"
#endif

#include <memory>
#include <span>


namespace dagforge::http {

class HttpRequestParser {
public:
  HttpRequestParser();
  ~HttpRequestParser();

  HttpRequestParser(const HttpRequestParser &) = delete;
  auto operator=(const HttpRequestParser &) -> HttpRequestParser & = delete;

  [[nodiscard]] auto parse(std::span<const uint8_t> data)
      -> Result<HttpRequest>;
  auto reset() -> void;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

class HttpResponseParser {
public:
  HttpResponseParser();
  ~HttpResponseParser();

  HttpResponseParser(const HttpResponseParser &) = delete;
  auto operator=(const HttpResponseParser &) -> HttpResponseParser & = delete;

  [[nodiscard]] auto parse(std::span<const uint8_t> data)
      -> Result<HttpResponse>;
  auto reset() -> void;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace dagforge::http
