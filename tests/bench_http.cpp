#include "dagforge/core/sync_wait.hpp"
#include "dagforge/http/http_client.hpp"
#include "dagforge/http/http_server.hpp"
#include "dagforge/http/router.hpp"

#include "bench_utils.hpp"
#include "test_utils.hpp"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>

namespace dagforge::http {
namespace {

[[nodiscard]] auto connect_client(std::uint16_t port, bool keep_alive)
    -> task<Result<std::unique_ptr<HttpClient>>> {
  co_return co_await HttpClient::connect_tcp(
      current_io_context(), "127.0.0.1", port,
      HttpClientConfig{.keep_alive = keep_alive});
}

struct LocalHttpScenario {
  bench::RuntimeGuard runtime{2};
  HttpServer server{runtime.runtime};
  std::uint16_t port{test::pick_unused_tcp_port_or_zero()};

  explicit LocalHttpScenario(std::size_t response_bytes) {
    if (port == 0) {
      throw std::runtime_error("failed to reserve loopback port");
    }
    auto configured = server.configure(HttpServerConfig{
        .max_requests_per_connection = std::numeric_limits<std::size_t>::max(),
    });
    if (!configured) {
      throw std::runtime_error(configured.error().message());
    }
    auto body = std::make_shared<const std::string>(response_bytes, 'x');
    server.router().get("/payload", [body](HttpRequest) -> task<HttpResponse> {
      HttpResponse response = HttpResponse::ok();
      response.set_body(*body);
      co_return response;
    });
    auto started = server.start("127.0.0.1", port, false);
    if (!started) {
      throw std::runtime_error(started.error().message());
    }
  }

  ~LocalHttpScenario() { server.stop(); }
};

void BM_HttpKeepAliveRoundTrip(benchmark::State &state) {
  const auto response_bytes = static_cast<std::size_t>(state.range(0));
  LocalHttpScenario scenario(response_bytes);
  auto connected = sync_wait_on_runtime(scenario.runtime.runtime,
                                        connect_client(scenario.port, true));
  if (!connected) {
    state.SkipWithError(connected.error().message().c_str());
    return;
  }

  for (auto _ : state) {
    auto response = sync_wait_on_runtime(scenario.runtime.runtime,
                                         (*connected)->get("/payload"));
    if (!response || response->status != HttpStatus::Ok ||
        response->body.size() != response_bytes) {
      state.SkipWithError(response ? "unexpected HTTP response"
                                   : response.error().message().c_str());
      return;
    }
    benchmark::DoNotOptimize(response->body.data());
  }
  state.SetItemsProcessed(state.iterations());
  state.SetBytesProcessed(static_cast<std::int64_t>(response_bytes) *
                          state.iterations());
}

void BM_HttpReconnectRoundTrip(benchmark::State &state) {
  const auto response_bytes = static_cast<std::size_t>(state.range(0));
  LocalHttpScenario scenario(response_bytes);

  for (auto _ : state) {
    auto connected = sync_wait_on_runtime(scenario.runtime.runtime,
                                          connect_client(scenario.port, false));
    if (!connected) {
      state.SkipWithError(connected.error().message().c_str());
      return;
    }
    auto response = sync_wait_on_runtime(scenario.runtime.runtime,
                                         (*connected)->get("/payload"));
    if (!response || response->status != HttpStatus::Ok ||
        response->body.size() != response_bytes) {
      state.SkipWithError(response ? "unexpected HTTP response"
                                   : response.error().message().c_str());
      return;
    }
    benchmark::DoNotOptimize(response->body.data());
  }
  state.SetItemsProcessed(state.iterations());
  state.SetBytesProcessed(static_cast<std::int64_t>(response_bytes) *
                          state.iterations());
}

BENCHMARK(BM_HttpKeepAliveRoundTrip)
    ->Arg(64)
    ->Arg(16 * 1024)
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime();

BENCHMARK(BM_HttpReconnectRoundTrip)
    ->Arg(64)
    ->Arg(16 * 1024)
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime();

} // namespace
} // namespace dagforge::http
