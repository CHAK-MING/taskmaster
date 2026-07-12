#pragma once

#include "dagforge/core/coroutine.hpp"
#include "dagforge/config/system_config.hpp"
#include "dagforge/app/services/persistence_service.hpp"
#include "dagforge/dag/dag_manager.hpp"
#include "dagforge/io/context.hpp"
#include "dagforge/util/conv.hpp"
#include "dagforge/util/id.hpp"

#include <arpa/inet.h>
#include <atomic>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <format>
#include <functional>
#include <mutex>
#include <netinet/in.h>
#include <optional>
#include <queue>
#include <stdexcept>
#include <stdlib.h>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace dagforge::test {

[[nodiscard]] inline auto env_or_default(const char *key, std::string fallback)
    -> std::string {
  if (const char *v = std::getenv(key); v && *v != '\0') {
    return v;
  }
  return fallback;
}

[[nodiscard]] inline auto env_port_or_default(const char *key,
                                              std::uint16_t fallback)
    -> std::uint16_t {
  const char *raw = std::getenv(key);
  if (!raw || *raw == '\0') {
    return fallback;
  }

  auto port = util::parse_int<unsigned int>(raw);
  if (!port || *port == 0 || *port > 65535) {
    return fallback;
  }
  return static_cast<std::uint16_t>(*port);
}

[[nodiscard]] inline auto unique_token(std::string_view base) -> std::string {
  static std::atomic<std::uint64_t> seq{0};
  const auto n = seq.fetch_add(1, std::memory_order_relaxed);
  return std::format("{}_{}", base, n);
}

[[nodiscard]] inline auto load_test_db_config() -> DatabaseConfig {
  DatabaseConfig cfg;
  cfg.host = env_or_default("DAGFORGE_TEST_MYSQL_HOST", cfg.host);
  cfg.port = env_port_or_default("DAGFORGE_TEST_MYSQL_PORT", cfg.port);
  cfg.username = env_or_default("DAGFORGE_TEST_MYSQL_USER", cfg.username);
  cfg.password = env_or_default("DAGFORGE_TEST_MYSQL_PASSWORD", cfg.password);
  cfg.database = env_or_default(
      "DAGFORGE_TEST_MYSQL_DB",
      std::format("dagforge_test_{}", static_cast<long long>(::getpid())));
  cfg.connect_timeout = 2;
  return cfg;
}

[[nodiscard]] inline auto make_test_config(std::uint16_t api_port = 0)
    -> SystemConfig {
  SystemConfig cfg{};
  cfg.api.enabled = true;
  cfg.api.host = "127.0.0.1";
  cfg.api.port = api_port;
  cfg.scheduler.shards = 2;
  cfg.scheduler.max_concurrency = 16;
  cfg.database = load_test_db_config();
  return cfg;
}

// Run a coroutine synchronously on a fresh IoContext and return its result.
// Aborts (throws) if the coroutine does not complete within `timeout`.
template <typename T>
[[nodiscard]] inline auto
run_coro(task<T> coro,
         std::chrono::milliseconds timeout = std::chrono::seconds(10)) -> T {
  io::IoContext io;
  std::exception_ptr eptr;
  std::optional<T> result;
  boost::asio::co_spawn(
      io,
      [&]() -> task<void> {
        result = co_await std::move(coro);
        co_return;
      },
      [&](std::exception_ptr e) { eptr = e; });
  (void)io.run_for(timeout);
  if (!result && !eptr)
    throw std::runtime_error("run_coro timed out");
  if (eptr)
    std::rethrow_exception(eptr);
  return std::move(*result);
}

// Specialisation for task<void>
inline auto
run_coro(task<void> coro,
         std::chrono::milliseconds timeout = std::chrono::seconds(10)) -> void {
  io::IoContext io;
  std::exception_ptr eptr;
  bool done = false;
  boost::asio::co_spawn(
      io,
      [&]() -> task<void> {
        co_await std::move(coro);
        done = true;
        co_return;
      },
      [&](std::exception_ptr e) { eptr = e; });
  (void)io.run_for(timeout);
  if (!done && !eptr)
    throw std::runtime_error("run_coro timed out");
  if (eptr)
    std::rethrow_exception(eptr);
}

inline auto reset_mysql_test_database(const DatabaseConfig &db_cfg) -> void {
  Runtime runtime(1);
  auto runtime_res = runtime.start();
  if (!runtime_res) {
    return;
  }

  {
    PersistenceService service(runtime, db_cfg);
    auto open_res = run_coro(service.open());
    if (open_res) {
      auto clear_res = run_coro(service.clear_all_dag_data());
      (void)clear_res;
    }
    auto close_res = run_coro(service.close());
    if (!close_res) {
      throw std::runtime_error(std::format(
          "reset_mysql_test_database: close failed: {}",
          close_res.error().message()));
    }
  }
  runtime.stop();
}

template <typename Predicate>
[[nodiscard]] inline auto
poll_until(Predicate &&predicate, std::chrono::milliseconds timeout,
           std::chrono::milliseconds interval = std::chrono::milliseconds(10))
    -> bool {
  auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (std::invoke(std::forward<Predicate>(predicate))) {
      return true;
    }
    std::this_thread::sleep_for(interval);
  }
  return std::invoke(std::forward<Predicate>(predicate));
}

[[nodiscard]] inline auto create_simple_dag_info(std::string_view name)
    -> DAGInfo {
  DAGInfo info;
  info.name = std::string(name);
  return info;
}

[[nodiscard]] inline auto
create_task_config(TaskId task_id, std::string_view command = "echo hello")
    -> TaskConfig {
  auto result = TaskConfig::builder()
                    .id(std::string(task_id.value()))
                    .name(std::string(task_id.value()))
                    .command(std::string(command))
                    .executor(ExecutorType::Shell)
                    .timeout(std::chrono::seconds(30))
                    .retry(0, std::chrono::seconds(60))
                    .build();
  if (!result) {
    throw std::runtime_error("Failed to build TaskConfig");
  }
  return std::move(*result);
}

[[nodiscard]] inline auto create_task_config(TaskId task_id,
                                             std::string_view command,
                                             std::vector<TaskId> dependencies)
    -> TaskConfig {
  auto builder = TaskConfig::builder()
                     .id(std::string(task_id.value()))
                     .name(std::string(task_id.value()))
                     .command(std::string(command))
                     .executor(ExecutorType::Shell)
                     .timeout(std::chrono::seconds(30))
                     .retry(0, std::chrono::seconds(60));

  for (auto &dep : dependencies) {
    builder.depends_on(std::string(dep.value()));
  }
  return std::move(builder).build().value();
}

[[nodiscard]] inline auto
create_task_config(TaskId task_id, std::string_view name,
                   std::string_view command, std::vector<TaskId> dependencies)
    -> TaskConfig {
  auto builder = TaskConfig::builder()
                     .id(std::string(task_id.value()))
                     .name(std::string(name))
                     .command(std::string(command))
                     .executor(ExecutorType::Shell)
                     .timeout(std::chrono::seconds(30))
                     .retry(0, std::chrono::seconds(60));

  for (auto &dep : dependencies) {
    builder.depends_on(std::string(dep.value()));
  }
  auto result = std::move(builder).build();
  if (!result) {
    throw std::runtime_error("Failed to build TaskConfig");
  }
  return std::move(*result);
}

[[nodiscard]] inline auto create_dag_info_with_task(std::string_view name,
                                                    TaskId task_id) -> DAGInfo {
  DAGInfo info;
  info.name = std::string(name);
  info.tasks.push_back(create_task_config(std::move(task_id)));
  info.rebuild_task_index();
  return info;
}

[[nodiscard]] inline auto pick_unused_tcp_port()
    -> std::optional<std::uint16_t> {
  int sock = ::socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    return std::nullopt;
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = 0;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

  if (::bind(sock, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
    ::close(sock);
    return std::nullopt;
  }

  socklen_t len = sizeof(addr);
  if (::getsockname(sock, reinterpret_cast<sockaddr *>(&addr), &len) < 0) {
    ::close(sock);
    return std::nullopt;
  }

  std::uint16_t port = ntohs(addr.sin_port);
  ::close(sock);
  if (port == 0) {
    return std::nullopt;
  }
  return port;
}

[[nodiscard]] inline auto pick_unused_tcp_port_or_zero() -> std::uint16_t {
  auto port = pick_unused_tcp_port();
  return port.has_value() ? *port : 0;
}

[[nodiscard]] inline auto
make_temp_path(std::string_view prefix = "dagforge_test_") -> std::string {
  std::string templ = std::string("/tmp/") + std::string(prefix) + "XXXXXX";
  int fd = ::mkstemp(templ.data());
  if (fd < 0) {
    return "";
  }
  ::close(fd);
  return templ;
}

[[nodiscard]] inline auto
make_temp_dir(std::string_view prefix = "dagforge_dags_") -> std::string {
  std::string templ = std::string("/tmp/") + std::string(prefix) + "XXXXXX";
  char *path = ::mkdtemp(templ.data());
  return path ? std::string(path) : "";
}

[[nodiscard]] inline auto dag_id(const char *s) -> DAGId {
  return DAGId{std::string{s}};
}

[[nodiscard]] inline auto dag_id(std::string_view s) -> DAGId {
  return DAGId{std::string{s}};
}

[[nodiscard]] inline auto dag_id(std::string s) -> DAGId {
  return DAGId{std::move(s)};
}

[[nodiscard]] inline auto task_id(const char *s) -> TaskId {
  return TaskId{std::string{s}};
}

[[nodiscard]] inline auto task_id(std::string_view s) -> TaskId {
  return TaskId{std::string{s}};
}

[[nodiscard]] inline auto task_id(std::string s) -> TaskId {
  return TaskId{std::move(s)};
}

[[nodiscard]] inline auto dag_run_id(const char *s) -> DAGRunId {
  return DAGRunId{std::string{s}};
}

[[nodiscard]] inline auto dag_run_id(std::string_view s) -> DAGRunId {
  return DAGRunId{std::string{s}};
}

[[nodiscard]] inline auto dag_run_id(std::string s) -> DAGRunId {
  return DAGRunId{std::move(s)};
}

inline void busy_wait_for(std::chrono::milliseconds duration) {
  auto start = std::chrono::steady_clock::now();
  while (std::chrono::steady_clock::now() - start < duration) {
    std::this_thread::yield();
  }
}

inline void sleep_ms(std::chrono::milliseconds ms) {
  std::this_thread::sleep_for(ms);
}

[[nodiscard]] inline auto parse_http_status(std::string_view response) -> int {
  const auto space_pos = response.find(' ');
  if (space_pos == std::string_view::npos || space_pos + 4 > response.size()) {
    return 0;
  }

  auto parsed = util::parse_int<int>(response.substr(space_pos + 1, 3));
  return parsed.value_or(0);
}

[[nodiscard]] inline auto
http_get(std::uint16_t port, std::string_view path,
         std::chrono::seconds timeout = std::chrono::seconds(5))
    -> std::pair<int, std::string> {
  int sock = ::socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    return {-1, ""};
  }

  timeval tv{};
  tv.tv_sec = static_cast<decltype(tv.tv_sec)>(timeout.count());
  tv.tv_usec = 0;
  ::setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  ::setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

  if (::connect(sock, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
    ::close(sock);
    return {-1, ""};
  }

  std::string request = std::format(
      "GET {} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n", path);
  (void)::send(sock, request.data(), request.size(), 0);

  std::string response;
  char buffer[4096];
  ssize_t n;
  while ((n = ::recv(sock, buffer, sizeof(buffer), 0)) > 0) {
    response.append(buffer, static_cast<std::size_t>(n));
  }
  ::close(sock);

  const auto status_code = parse_http_status(response);

  auto body_start = response.find("\r\n\r\n");
  std::string body =
      (body_start != std::string::npos) ? response.substr(body_start + 4) : "";

  return {status_code, std::move(body)};
}

[[nodiscard]] inline auto
http_post(std::uint16_t port, std::string_view path, std::string_view json_body,
          std::chrono::seconds timeout = std::chrono::seconds(5))
    -> std::pair<int, std::string> {
  int sock = ::socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    return {-1, ""};
  }

  timeval tv{};
  tv.tv_sec = static_cast<decltype(tv.tv_sec)>(timeout.count());
  tv.tv_usec = 0;
  ::setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  ::setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

  if (::connect(sock, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
    ::close(sock);
    return {-1, ""};
  }

  std::string request = std::format("POST {} HTTP/1.1\r\n"
                                    "Host: localhost\r\n"
                                    "Content-Type: application/json\r\n"
                                    "Content-Length: {}\r\n"
                                    "Connection: close\r\n\r\n{}",
                                    path, json_body.size(), json_body);
  (void)::send(sock, request.data(), request.size(), 0);

  std::string response;
  char buffer[4096];
  ssize_t n;
  while ((n = ::recv(sock, buffer, sizeof(buffer), 0)) > 0) {
    response.append(buffer, static_cast<std::size_t>(n));
  }
  ::close(sock);

  const auto status_code = parse_http_status(response);

  auto body_start = response.find("\r\n\r\n");
  std::string body =
      (body_start != std::string::npos) ? response.substr(body_start + 4) : "";

  return {status_code, std::move(body)};
}

class SimpleBarrier {
public:
  explicit SimpleBarrier(std::size_t count)
      : count_(count), waiting_(0), generation_(0) {}

  void arrive_and_wait() {
    std::size_t my_generation = generation_.load(std::memory_order_acquire);

    std::size_t arrived = waiting_.fetch_add(1, std::memory_order_acq_rel) + 1;

    if (arrived == count_) {
      waiting_.store(0, std::memory_order_release);
      generation_.fetch_add(1, std::memory_order_acq_rel);
      cv_.notify_all();
      return;
    }

    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this, my_generation] {
      return generation_.load(std::memory_order_acquire) != my_generation;
    });
  }

private:
  std::size_t count_;
  std::atomic<std::size_t> waiting_;
  std::atomic<std::size_t> generation_;
  std::mutex mutex_;
  std::condition_variable cv_;
};

using Barrier = SimpleBarrier;

} // namespace dagforge::test
