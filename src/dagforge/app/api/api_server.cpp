#include "dagforge/app/api/api_server.hpp"
#include "detail/api_context.hpp"
#include "detail/dto_mapper.hpp"
#include "detail/routes/system.hpp"
#include "detail/routes/workflows.hpp"
#include "dagforge/app/metrics_exporter.hpp"
#include "dagforge/http/http_server.hpp"
#include "dagforge/util/log.hpp"

#include <chrono>
#include <cstdlib>
#include <tuple>

namespace dagforge {

struct ApiServer::Impl : std::enable_shared_from_this<Impl> {
  Application &app_;
  std::shared_ptr<http::HttpServer> server_;
  std::atomic<std::uint64_t> http_active_requests_{0};
  api_detail::HttpMetricsRegistry http_metrics_;
  api_detail::ApiContext ctx_;
  bool auth_configuration_valid_{true};

  explicit Impl(Application &app)
      : app_(app),
        server_(std::make_shared<http::HttpServer>(app_.runtime())),
        ctx_{.app = app_,
             .server = *server_,
             .http_active_requests = http_active_requests_,
             .http_metrics = http_metrics_,
             .max_request_body_bytes =
                 app_.config().api.max_request_body_bytes,
             .max_concurrent_requests =
                 app_.config().api.max_concurrent_requests} {
    const auto &token_env = app_.config().api.bearer_token_env;
    if (!token_env.empty()) {
      if (const char *token = std::getenv(token_env.c_str());
          token != nullptr && *token != '\0') {
        ctx_.bearer_token = token;
      } else {
        auth_configuration_valid_ = false;
      }
    }
    if (!server_) {
      log::error("ApiServer: HttpServer allocation failed (server_ is null)");
    } else {
      log::debug("ApiServer constructed. Impl: {}, server_: {}",
                 static_cast<void *>(this), static_cast<void *>(server_.get()));
    }
  }

  void init() {
    api_detail::register_system_routes(ctx_);
    api_detail::register_workflow_routes(ctx_);
  }

  auto http_active_requests() const -> std::uint64_t {
    return http_active_requests_.load(std::memory_order_relaxed);
  }

  auto http_request_counts() const -> std::vector<
      std::tuple<std::string, std::string, std::string, std::uint64_t>> {
    return http_metrics_.request_counts();
  }

  auto http_request_duration_snapshots() const
      -> std::vector<std::pair<std::string, metrics::Histogram::Snapshot>> {
    return http_metrics_.request_duration_snapshots();
  }

  auto start() -> Result<void> {
    const auto &api_cfg = app_.config().api;
    if (!auth_configuration_valid_) {
      log::error("API bearer token environment variable '{}' is unset",
                 api_cfg.bearer_token_env);
      return fail(Error::InvalidArgument);
    }
    auto configured = server_->configure(http::HttpServerConfig{
        .max_request_header_bytes = api_cfg.max_request_header_bytes,
        .max_request_body_bytes = api_cfg.max_request_body_bytes,
        .connection_idle_timeout =
            std::chrono::milliseconds(api_cfg.connection_idle_timeout_ms),
        .max_connections = api_cfg.max_connections,
        .max_requests_per_connection = api_cfg.max_requests_per_connection,
    });
    if (!configured) {
      return fail(configured.error());
    }
    if (api_cfg.tls_enabled) {
      auto tls_res = server_->set_tls_credentials(api_cfg.tls_cert_file,
                                                  api_cfg.tls_key_file,
                                                  api_cfg.tls_min_version);
      if (!tls_res) {
        log::error("API TLS setup failed: {}", tls_res.error().message());
        return fail(tls_res.error());
      }
    }
    return server_->start(api_cfg.host, api_cfg.port, api_cfg.reuse_port);
  }

  void stop() {
    if (server_) {
      server_->stop();
    }
  }

  [[nodiscard]] auto is_running() const -> bool {
    return server_ && server_->is_running();
  }

};

ApiServer::ApiServer(Application &app) : impl_(std::make_shared<Impl>(app)) {
  impl_->init();
}
ApiServer::~ApiServer() = default;

auto ApiServer::start() -> Result<void> {
  const auto impl = impl_;
  return impl->start();
}

void ApiServer::stop() {
  const auto impl = impl_;
  impl->stop();
}

auto ApiServer::is_running() const -> bool {
  auto impl = impl_;
  return impl->is_running();
}

auto ApiServer::http_active_requests() const -> std::uint64_t {
  auto impl = impl_;
  return impl->http_active_requests();
}

auto ApiServer::http_request_counts() const -> std::vector<
    std::tuple<std::string, std::string, std::string, std::uint64_t>> {
  auto impl = impl_;
  return impl->http_request_counts();
}

auto ApiServer::http_request_duration_snapshots() const
    -> std::vector<std::pair<std::string, metrics::Histogram::Snapshot>> {
  auto impl = impl_;
  return impl->http_request_duration_snapshots();
}

} // namespace dagforge
