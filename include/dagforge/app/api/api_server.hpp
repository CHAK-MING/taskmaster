#pragma once

#include "dagforge/core/error.hpp"
#include "dagforge/core/metrics.hpp"

#include <memory>
#include <string>
#include <tuple>
#include <vector>

namespace dagforge {

class Application;

namespace http {
class HttpServer;
} // namespace http

class ApiServer {
public:
  explicit ApiServer(Application &app);
  ~ApiServer();

  [[nodiscard]] auto start() -> Result<void>;
  void stop();
  [[nodiscard]] bool is_running() const;

  [[nodiscard]] auto http_active_requests() const -> std::uint64_t;
  [[nodiscard]] auto http_request_counts() const -> std::vector<
      std::tuple<std::string, std::string, std::string, std::uint64_t>>;
  [[nodiscard]] auto http_request_duration_snapshots() const
      -> std::vector<std::pair<std::string, metrics::Histogram::Snapshot>>;

private:
  struct Impl;
  std::shared_ptr<Impl> impl_;
};

} // namespace dagforge
