#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/coroutine.hpp"
#include "dagforge/http/http_types.hpp"

#include <functional>
#endif

namespace dagforge::http {

using RouteHandler =
    std::move_only_function<dagforge::task<HttpResponse>(HttpRequest)>;

class Router {
public:
  Router();
  ~Router();

  Router(const Router &) = delete;
  auto operator=(const Router &) -> Router & = delete;

  auto add_route(HttpMethod method, std::string path, RouteHandler handler)
      -> void;

  auto get(std::string path, RouteHandler handler) -> void;
  auto post(std::string path, RouteHandler handler) -> void;
  auto put(std::string path, RouteHandler handler) -> void;
  auto del(std::string path, RouteHandler handler) -> void;

  [[nodiscard]] auto route(HttpRequest req) -> dagforge::task<HttpResponse>;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace dagforge::http
