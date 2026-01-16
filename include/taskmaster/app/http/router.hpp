#pragma once

#include "taskmaster/app/http/http_types.hpp"
#include "taskmaster/core/coroutine.hpp"
#include "taskmaster/core/runtime.hpp"

#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>

namespace taskmaster::http {

using RouteHandler = std::move_only_function<taskmaster::task<HttpResponse>(const HttpRequest&)>;

class Router {
public:
  Router();
  ~Router();

  Router(const Router&) = delete;
  auto operator=(const Router&) -> Router& = delete;

  auto add_route(HttpMethod method, std::string path, RouteHandler handler)
      -> void;
  
  auto get(std::string path, RouteHandler handler) -> void;
  auto post(std::string path, RouteHandler handler) -> void;
  auto put(std::string path, RouteHandler handler) -> void;
  auto del(std::string path, RouteHandler handler) -> void;

  [[nodiscard]] auto route(const HttpRequest& req)
      -> taskmaster::task<HttpResponse>;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace taskmaster::http
