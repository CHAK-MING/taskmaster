#pragma once

#include "dagforge/core/coroutine.hpp"
#include "dagforge/io/context.hpp"

#include <arpa/inet.h>
#include <chrono>
#include <exception>
#include <functional>
#include <netinet/in.h>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

#include <boost/asio/co_spawn.hpp>

namespace dagforge::test {

template <typename T>
[[nodiscard]] inline auto
run_coro(task<T> coro,
         std::chrono::milliseconds timeout = std::chrono::seconds(10)) -> T {
  io::IoContext io;
  std::exception_ptr exception;
  std::optional<T> result;
  boost::asio::co_spawn(
      io,
      [&]() -> task<void> {
        result = co_await std::move(coro);
        co_return;
      },
      [&](std::exception_ptr current) { exception = current; });
  (void)io.run_for(timeout);
  if (!result && !exception) {
    throw std::runtime_error("run_coro timed out");
  }
  if (exception) {
    std::rethrow_exception(exception);
  }
  return std::move(*result);
}

inline auto
run_coro(task<void> coro,
         std::chrono::milliseconds timeout = std::chrono::seconds(10)) -> void {
  io::IoContext io;
  std::exception_ptr exception;
  bool completed = false;
  boost::asio::co_spawn(
      io,
      [&]() -> task<void> {
        co_await std::move(coro);
        completed = true;
        co_return;
      },
      [&](std::exception_ptr current) { exception = current; });
  (void)io.run_for(timeout);
  if (!completed && !exception) {
    throw std::runtime_error("run_coro timed out");
  }
  if (exception) {
    std::rethrow_exception(exception);
  }
}

[[nodiscard]] inline auto pick_unused_tcp_port()
    -> std::optional<std::uint16_t> {
  const int socket = ::socket(AF_INET, SOCK_STREAM, 0);
  if (socket < 0) {
    return std::nullopt;
  }

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = 0;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (::bind(socket, reinterpret_cast<sockaddr *>(&address), sizeof(address)) <
      0) {
    ::close(socket);
    return std::nullopt;
  }

  socklen_t length = sizeof(address);
  if (::getsockname(socket, reinterpret_cast<sockaddr *>(&address), &length) <
      0) {
    ::close(socket);
    return std::nullopt;
  }

  const auto port = ntohs(address.sin_port);
  ::close(socket);
  return port == 0 ? std::nullopt
                   : std::optional<std::uint16_t>{port};
}

[[nodiscard]] inline auto pick_unused_tcp_port_or_zero() -> std::uint16_t {
  return pick_unused_tcp_port().value_or(0);
}

[[nodiscard]] inline auto
make_temp_path(std::string_view prefix = "dagforge_test_") -> std::string {
  std::string path = "/tmp/";
  path.append(prefix);
  path.append("XXXXXX");
  const int file = ::mkstemp(path.data());
  if (file < 0) {
    return {};
  }
  ::close(file);
  return path;
}

inline auto busy_wait_for(std::chrono::milliseconds duration) -> void {
  const auto start = std::chrono::steady_clock::now();
  while (std::chrono::steady_clock::now() - start < duration) {
    std::this_thread::yield();
  }
}

template <typename Predicate>
[[nodiscard]] inline auto wait_until(
    Predicate &&predicate, std::chrono::milliseconds timeout,
    std::chrono::milliseconds poll_interval = std::chrono::milliseconds(1))
    -> bool {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (true) {
    if (std::invoke(predicate)) {
      return true;
    }
    if (std::chrono::steady_clock::now() >= deadline) {
      return false;
    }
    std::this_thread::sleep_for(poll_interval);
  }
}

} // namespace dagforge::test
