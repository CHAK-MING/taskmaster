#include "dagforge/app/http/router.hpp"

#include <algorithm>
#include <ankerl/unordered_dense.h>
#include <array>
#include <optional>
#include <ranges>


namespace dagforge::http {

using dagforge::StringEqual;
using dagforge::StringHash;

struct Router::Impl {
  struct RoutePattern {
    std::vector<std::string> segments;
    std::vector<bool> is_param;
    std::vector<std::string> param_names;
  };

  struct Route {
    std::string pattern;
    RoutePattern parsed;
    RouteHandler handler;
  };

  struct MethodRoutes {
    ankerl::unordered_dense::map<std::string, std::size_t, StringHash,
                                 StringEqual>
        static_lookup;
    std::vector<Route> static_routes;
    ankerl::unordered_dense::map<std::size_t, std::vector<Route>>
        dynamic_by_segments;
  };

  std::array<MethodRoutes, 7> methods;

  static auto method_index(HttpMethod method) -> std::size_t {
    switch (method) {
    case HttpMethod::GET:
      return 0;
    case HttpMethod::POST:
      return 1;
    case HttpMethod::PUT:
      return 2;
    case HttpMethod::DELETE:
      return 3;
    case HttpMethod::PATCH:
      return 4;
    case HttpMethod::OPTIONS:
      return 5;
    case HttpMethod::HEAD:
      return 6;
    }
    return 0;
  }

  static auto split_path(std::string_view path) -> std::vector<std::string> {
    std::vector<std::string> segments;
    for (auto part : path | std::views::split('/')) {
      segments.emplace_back(std::string_view(part));
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
    for (const auto &seg : split_path(pattern)) {
      auto [name, is_param] = parse_segment(seg);
      result.segments.emplace_back(name);
      result.is_param.emplace_back(is_param);
      if (is_param) {
        result.param_names.emplace_back(name);
      }
    }
    return result;
  }

  static auto split_path_views(std::string_view path)
      -> std::vector<std::string_view> {
    std::vector<std::string_view> segments;
    for (auto part : path | std::views::split('/')) {
      segments.emplace_back(std::string_view(part));
    }
    return segments;
  }

  static auto match_route(const RoutePattern &pattern,
                          const std::vector<std::string_view> &path_segments,
                          HttpRequest &req) -> bool {
    if (path_segments.size() != pattern.segments.size()) {
      return false;
    }

    req.path_params.clear();
    req.path_params.reserve(pattern.param_names.size());

    for (auto &&[pat_seg, is_param, path_seg] :
         std::views::zip(pattern.segments, pattern.is_param, path_segments)) {
      if (is_param) {
        req.path_params.emplace(pat_seg, path_seg);
      } else if (pat_seg != path_seg) {
        req.path_params.clear();
        return false;
      }
    }

    return true;
  }

  auto route_exists_for_other_method(std::string_view route_path,
                                     HttpMethod current_method) const -> bool {
    const auto path_segments = split_path_views(route_path);
    for (std::size_t index = 0; index < methods.size(); ++index) {
      if (index == method_index(current_method)) {
        continue;
      }

      const auto &method_routes = methods[index];
      if (method_routes.static_lookup.contains(std::string(route_path))) {
        return true;
      }

      auto dyn_it = method_routes.dynamic_by_segments.find(path_segments.size());
      if (dyn_it == method_routes.dynamic_by_segments.end()) {
        continue;
      }

      HttpRequest probe;
      for (const auto &route : dyn_it->second) {
        if (match_route(route.parsed, path_segments, probe)) {
          return true;
        }
      }
    }

    return false;
  }
};

Router::Router() : impl_(std::make_unique<Impl>()) {}

Router::~Router() = default;

auto Router::add_route(HttpMethod method, std::string path,
                       RouteHandler handler) -> void {
  auto parsed = Impl::parse_pattern(path);
  const bool has_param = std::any_of(
      parsed.is_param.begin(), parsed.is_param.end(), [](bool v) { return v; });

  auto &method_routes = impl_->methods[Impl::method_index(method)];

  if (!has_param) {
    const auto index = method_routes.static_routes.size();
    method_routes.static_lookup.emplace(path, index);
    method_routes.static_routes.emplace_back(
        Impl::Route{.pattern = std::move(path),
                    .parsed = std::move(parsed),
                    .handler = std::move(handler)});
    return;
  }

  auto &bucket = method_routes.dynamic_by_segments[parsed.segments.size()];
  bucket.emplace_back(Impl::Route{.pattern = std::move(path),
                                  .parsed = std::move(parsed),
                                  .handler = std::move(handler)});
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

auto Router::route(HttpRequest req) -> dagforge::task<HttpResponse> {
  const auto query_pos = req.path.find('?');
  const std::string_view route_path =
      query_pos == std::string::npos
          ? std::string_view(req.path)
          : std::string_view(req.path).substr(0, query_pos);
  auto path_segments = Impl::split_path_views(route_path);

  auto &method_routes = impl_->methods[Impl::method_index(req.method)];

  if (auto it = method_routes.static_lookup.find(std::string(route_path));
      it != method_routes.static_lookup.end()) {
    auto response =
        co_await method_routes.static_routes[it->second].handler(std::move(req));
    co_return response;
  }

  auto dyn_it = method_routes.dynamic_by_segments.find(path_segments.size());
  if (dyn_it != method_routes.dynamic_by_segments.end()) {
    for (auto &route : dyn_it->second) {
      if (Impl::match_route(route.parsed, path_segments, req)) {
        auto response = co_await route.handler(std::move(req));
        co_return response;
      }
    }
  }

  if (impl_->route_exists_for_other_method(route_path, req.method)) {
    co_return HttpResponse{.status = HttpStatus::MethodNotAllowed};
  }

  co_return HttpResponse::not_found();
}

} // namespace dagforge::http
