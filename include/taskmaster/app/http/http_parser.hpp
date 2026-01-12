#pragma once

#include "taskmaster/app/http/http_types.hpp"

#include <llhttp.h>

#include <memory>
#include <optional>
#include <span>

namespace taskmaster::http {

class HttpParser {
public:
  HttpParser();
  ~HttpParser();

  HttpParser(const HttpParser&) = delete;
  auto operator=(const HttpParser&) -> HttpParser& = delete;

  auto parse(std::span<const uint8_t> data) -> std::optional<HttpRequest>;
  auto reset() -> void;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace taskmaster::http
