#include "taskmaster/app/http/router.hpp"

#include <algorithm>
#include <format>
#include <ranges>
#include <sstream>

namespace taskmaster::http {

struct Router::Impl {
  struct RoutePattern {
    std::vector<std::string> segments;
    std::vector<bool> is_param;
    std::vector<std::string> param_names;
  };

  struct Route {
    HttpMethod method;
    std::string pattern;
    RoutePattern parsed;
    RouteHandler handler;
  };

  std::vector<Route> routes;

  static auto split_path(std::string_view path) -> std::vector<std::string> {
    std::vector<std::string> segments;
    for (auto part : path | std::views::split('/')) {
      if (auto sv = std::string_view(part); !sv.empty()) {
        segments.emplace_back(sv);
      }
    }
    return segments;
  }

  static auto parse_segment(std::string_view seg) 
      -> std::tuple<std::string, bool> {
    if (seg.starts_with('{') && seg.ends_with('}')) {
      return {std::string(seg.substr(1, seg.size() - 2)), true};
    }
    return {std::string(seg), false};
  }

  static auto parse_pattern(std::string_view pattern) -> RoutePattern {
    RoutePattern result;
    for (const auto& seg : split_path(pattern)) {
      auto [name, is_param] = parse_segment(seg);
      result.segments.push_back(name);
      result.is_param.push_back(is_param);
      if (is_param) {
        result.param_names.push_back(name);
      }
    }
    return result;
  }

  static auto match_route(const RoutePattern& pattern,
                          std::string_view path)
      -> std::optional<std::unordered_map<std::string, std::string>> {
    auto path_segments = split_path(path);
    
    if (path_segments.size() != pattern.segments.size()) {
      return std::nullopt;
    }
    
    std::unordered_map<std::string, std::string> params;
    
    for (size_t i = 0; i < pattern.segments.size(); ++i) {
      if (pattern.is_param[i]) {
        params[pattern.segments[i]] = path_segments[i];
      } else {
        if (pattern.segments[i] != path_segments[i]) {
          return std::nullopt;
        }
      }
    }
    
    return params;
  }
};

Router::Router() : impl_(std::make_unique<Impl>()) {
}

Router::~Router() = default;

auto Router::add_route(HttpMethod method, std::string path,
                       RouteHandler handler) -> void {
  auto parsed = Impl::parse_pattern(path);
  impl_->routes.push_back(
      Impl::Route{method, std::move(path), std::move(parsed), std::move(handler)});
}

auto Router::get(std::string path, RouteHandler handler) -> void {
  add_route(HttpMethod::GET, std::move(path), std::move(handler));
}

auto Router::post(std::string path, RouteHandler handler) -> void {
  add_route(HttpMethod::POST, std::move(path), std::move(handler));
}

auto Router::put(std::string path, RouteHandler handler) -> void {
  add_route(HttpMethod::PUT, std::move(path), std::move(handler));
}

auto Router::del(std::string path, RouteHandler handler) -> void {
  add_route(HttpMethod::DELETE, std::move(path), std::move(handler));
}

auto Router::route(const HttpRequest& req) const -> taskmaster::task<HttpResponse> {
  for (const auto& route : impl_->routes) {
    if (route.method != req.method) {
      continue;
    }
    
    auto params = Impl::match_route(route.parsed, req.path);
    if (params) {
      HttpRequest modified_req = req;
      modified_req.path_params = std::move(*params);
      co_return co_await route.handler(modified_req);
    }
  }
  
  co_return HttpResponse::not_found();
}

}  // namespace taskmaster::http
