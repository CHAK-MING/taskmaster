#include "dagforge/app/http/websocket.hpp"
#include "dagforge/core/asio_awaitable.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/util/log.hpp"


#include <boost/asio/as_tuple.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/generic/stream_protocol.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <boost/system/system_error.hpp>

#include <atomic>
#include <array>
#include <cstddef>
#include <deque>
#include <exception>
#include <format>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <utility>
#include <variant>
#include <vector>

namespace dagforge::http {

namespace {

namespace beast = boost::beast;
namespace beast_http = beast::http;
namespace beast_ws = beast::websocket;

using WsStream =
    beast_ws::stream<boost::asio::generic::stream_protocol::socket>;
using WsTlsStream =
    beast_ws::stream<boost::asio::ssl::stream<boost::asio::ip::tcp::socket>>;
using UpgradeRequest = beast_http::request<beast_http::empty_body>;

constexpr std::array<std::uint64_t, 17> kWebSocketMessageSizeBucketsBytes{
    64ULL,     128ULL,    256ULL,    512ULL,    1'024ULL,  2'048ULL,
    4'096ULL,  8'192ULL,  16'384ULL, 32'768ULL, 65'536ULL, 131'072ULL,
    262'144ULL, 524'288ULL, 1'048'576ULL, 4'194'304ULL, 8'388'608ULL};

struct WebSocketMetricsState {
  std::atomic<std::uint64_t> messages_sent_total{0};
  std::atomic<std::uint64_t> messages_received_total{0};
  metrics::Histogram message_size_sent{std::span<const std::uint64_t>(
      kWebSocketMessageSizeBucketsBytes)};
  metrics::Histogram message_size_received{std::span<const std::uint64_t>(
      kWebSocketMessageSizeBucketsBytes)};
};

auto websocket_metrics() -> WebSocketMetricsState & {
  static WebSocketMetricsState state;
  return state;
}

auto record_websocket_sent(std::size_t size_bytes) -> void {
  auto &metrics_state = websocket_metrics();
  metrics_state.messages_sent_total.fetch_add(1, std::memory_order_relaxed);
  metrics_state.message_size_sent.observe_ns(
      static_cast<std::uint64_t>(size_bytes));
}

auto record_websocket_received(std::size_t size_bytes) -> void {
  auto &metrics_state = websocket_metrics();
  metrics_state.messages_received_total.fetch_add(1,
                                                 std::memory_order_relaxed);
  metrics_state.message_size_received.observe_ns(
      static_cast<std::uint64_t>(size_bytes));
}

// Write request discriminated union — all fields needed by write_loop.
struct WriteText {
  std::string payload;
};
struct WriteBinary {
  std::vector<std::byte> payload;
};
struct WritePong {
  std::string payload;
};
struct WriteClose {};
using WriteRequest =
    std::variant<WriteText, WriteBinary, WritePong, WriteClose>;

auto to_beast_verb(HttpMethod method) -> beast_http::verb {
  switch (method) {
  case HttpMethod::GET:
    return beast_http::verb::get;
  case HttpMethod::POST:
    return beast_http::verb::post;
  case HttpMethod::PUT:
    return beast_http::verb::put;
  case HttpMethod::DELETE:
    return beast_http::verb::delete_;
  case HttpMethod::PATCH:
    return beast_http::verb::patch;
  case HttpMethod::OPTIONS:
    return beast_http::verb::options;
  case HttpMethod::HEAD:
    return beast_http::verb::head;
  }
  return beast_http::verb::get;
}

auto to_upgrade_request(HttpRequest request) -> UpgradeRequest {
  UpgradeRequest out;
  out.method(to_beast_verb(request.method));
  out.version(request.version_major * 10 + request.version_minor);
  if (request.query_string.empty()) {
    out.target(request.path);
  } else {
    out.target(std::format("{}?{}", request.path, request.query_string));
  }

  for (const auto &[name, value] : request.headers) {
    out.set(name, value);
  }
  return out;
}

struct WsOpResult {
  Result<std::size_t> bytes;
};

template <typename WsStreamT>
auto ws_async_accept(WsStreamT &ws, UpgradeRequest req) -> task<Result<void>> {
  auto accept_res = co_await co_as_result(ws.async_accept(std::move(req), use_nothrow));
  co_return accept_res;
}

template <typename WsStreamT>
auto ws_async_accept_no_req(WsStreamT &ws) -> task<Result<void>> {
  auto accept_res = co_await co_as_result(ws.async_accept(use_nothrow));
  co_return accept_res;
}

template <typename WsStreamT>
auto ws_async_read(WsStreamT &ws, beast::flat_buffer &buffer)
    -> task<WsOpResult> {
  co_return WsOpResult{
      .bytes = co_await co_as_result(ws.async_read(buffer, use_nothrow))};
}

template <typename WsStreamT>
auto ws_async_write(WsStreamT &ws, boost::asio::const_buffer buffer)
    -> task<WsOpResult> {
  co_return WsOpResult{
      .bytes = co_await co_as_result(ws.async_write(buffer, use_nothrow))};
}

template <typename WsStreamT>
auto ws_async_close(WsStreamT &ws) -> task<Result<void>> {
  auto close_res = co_await co_as_result(
      ws.async_close(beast_ws::close_code::normal, use_nothrow));
  co_return close_res;
}

template <typename WsStreamT>
auto ws_async_pong(WsStreamT &ws, std::string data) -> task<Result<void>> {
  auto pong_res = co_await co_as_result(
      ws.async_pong(beast_ws::ping_data{std::move(data)}, use_nothrow));
  co_return pong_res;
}

template <typename ImplT> auto drain_writes(ImplT impl) -> spawn_task {
  while (!impl->closed.load(std::memory_order_acquire)) {
    if (impl->pending_writes.empty()) {
      impl->write_in_flight = false;
      co_return;
    }
    auto req = std::move(impl->pending_writes.front());
    impl->pending_writes.pop_front();

    if (std::holds_alternative<WriteClose>(req)) {
      const auto close_res = co_await ws_async_close(impl->ws);
      if (!close_res &&
          close_res.error() != make_error_code(beast_ws::error::closed)) {
        log::debug("WebSocket close failed: {}", close_res.error().message());
      }
      impl->closed.store(true, std::memory_order_release);
      impl->notify_waiters();
      impl->write_in_flight = false;
      co_return;
    }
    if (std::holds_alternative<WritePong>(req)) {
      auto pong_res = co_await ws_async_pong(
          impl->ws, std::move(std::get<WritePong>(req).payload));
      if (!pong_res) {
        log::debug("WebSocket pong failed: {}", pong_res.error().message());
        impl->closed.store(true, std::memory_order_release);
        impl->notify_waiters();
        impl->write_in_flight = false;
        co_return;
      }
      continue;
    }
    if (std::holds_alternative<WriteText>(req)) {
      impl->ws.text(true);
      auto &payload = std::get<WriteText>(req).payload;
      auto write_res =
          co_await ws_async_write(impl->ws, boost::asio::buffer(payload));
      if (!write_res.bytes) {
        log::debug("WebSocket text write failed: {}",
                   write_res.bytes.error().message());
        impl->closed.store(true, std::memory_order_release);
        impl->notify_waiters();
        impl->write_in_flight = false;
        co_return;
      }
      continue;
    }
    impl->ws.binary(true);
    auto &payload = std::get<WriteBinary>(req).payload;
    auto write_res = co_await ws_async_write(
        impl->ws, boost::asio::buffer(payload.data(), payload.size()));
    if (!write_res.bytes) {
      log::debug("WebSocket binary write failed: {}",
                 write_res.bytes.error().message());
      impl->closed.store(true, std::memory_order_release);
      impl->notify_waiters();
      impl->write_in_flight = false;
      co_return;
    }
  }
  impl->write_in_flight = false;
}

template <typename ImplT>
auto enqueue_write(ImplT impl, WriteRequest req,
                   boost::asio::any_io_executor ex) -> void {
  boost::asio::post(
      ex, [impl = std::move(impl), req = std::move(req), ex]() mutable {
        if (impl->closed.load(std::memory_order_acquire)) {
          return;
        }
        impl->pending_writes.emplace_back(std::move(req));
        if (impl->write_in_flight) {
          return;
        }
        impl->write_in_flight = true;
        boost::asio::co_spawn(ex, drain_writes(std::move(impl)),
                              boost::asio::detached);
      });
}

} // namespace

struct WebSocketConnection::Impl {
  WsStream ws;
  beast::flat_buffer read_buffer;
  std::optional<UpgradeRequest> upgrade_request;
  std::atomic<bool> accepted{false};
  std::atomic<bool> closed{false};
  int fd_num{-1};
  std::vector<std::move_only_function<void()>> accept_waiters;
  std::deque<WriteRequest> pending_writes;
  bool write_in_flight{false};

  Impl(boost::asio::generic::stream_protocol::socket socket,
       std::optional<UpgradeRequest> req)
      : ws(std::move(socket)), upgrade_request(std::move(req)), accepted(false),
        fd_num(ws.next_layer().native_handle()) {
    beast_ws::permessage_deflate pmd;
    pmd.client_enable = true;
    pmd.server_enable = true;
    pmd.compLevel = 3;
    ws.set_option(pmd);
    ws.auto_fragment(false);
    ws.read_message_max(8 * 1024 * 1024);
    ws.set_option(
        beast_ws::stream_base::timeout::suggested(beast::role_type::server));
    ws.set_option(
        beast_ws::stream_base::decorator([](beast_ws::response_type &res) {
          res.set(beast_http::field::server, "DAGForge");
        }));
  }

  auto wait_for_accept() -> spawn_task {
    auto ws_ex = ws.get_executor();
    co_await boost::asio::async_initiate<const boost::asio::use_awaitable_t<>,
                                         void()>(
        [this, ws_ex](auto handler) mutable {
          auto caller_ex = boost::asio::get_associated_executor(handler);
          boost::asio::post(
              ws_ex, [this, caller_ex, handler = std::move(handler)]() mutable {
                if (accepted.load(std::memory_order_acquire) ||
                    closed.load(std::memory_order_acquire)) {
                  boost::asio::post(caller_ex, std::move(handler));
                  return;
                }
                accept_waiters.emplace_back(
                    [h = std::move(handler), caller_ex]() mutable {
                      boost::asio::post(caller_ex, std::move(h));
                    });
              });
        },
        boost::asio::use_awaitable);
  }

  auto notify_waiters() -> void {
    auto waiters = std::move(accept_waiters);
    for (auto &fn : waiters) {
      fn();
    }
  }
};

WebSocketConnection::WebSocketConnection(
    boost::asio::generic::stream_protocol::socket socket,
    HttpRequest upgrade_request)
    : impl_(std::make_shared<Impl>(
          std::move(socket), to_upgrade_request(std::move(upgrade_request)))) {}

WebSocketConnection::WebSocketConnection(
    boost::asio::generic::stream_protocol::socket socket)
    : impl_(std::make_shared<Impl>(std::move(socket), std::nullopt)) {}

WebSocketConnection::~WebSocketConnection() = default;

auto WebSocketConnection::send_text(std::string text) -> spawn_task {
  auto impl = impl_;
  if (!impl->accepted.load(std::memory_order_acquire)) {
    co_await impl->wait_for_accept();
  }
  if (impl->closed.load(std::memory_order_acquire)) {
    co_return;
  }
  record_websocket_sent(text.size());
  auto ex = impl->ws.get_executor();
  enqueue_write(std::move(impl), WriteText{std::move(text)}, ex);
}

auto WebSocketConnection::send_binary(std::vector<std::byte> data)
    -> spawn_task {
  auto impl = impl_;
  if (!impl->accepted.load(std::memory_order_acquire)) {
    co_await impl->wait_for_accept();
  }
  if (impl->closed.load(std::memory_order_acquire)) {
    co_return;
  }
  record_websocket_sent(data.size());
  auto ex = impl->ws.get_executor();
  enqueue_write(std::move(impl), WriteBinary{std::move(data)}, ex);
}

auto WebSocketConnection::send_close() -> spawn_task {
  auto impl = impl_;
  if (impl->closed.load(std::memory_order_acquire)) {
    co_return;
  }
  record_websocket_sent(0);
  auto ex = impl->ws.get_executor();
  enqueue_write(std::move(impl), WriteClose{}, ex);
}

auto WebSocketConnection::send_pong(std::vector<std::byte> data) -> spawn_task {
  auto impl = impl_;
  if (impl->closed.load(std::memory_order_acquire)) {
    co_return;
  }
  std::string payload;
  payload.resize(data.size());
  for (std::size_t i = 0; i < data.size(); ++i) {
    payload[i] = static_cast<char>(std::to_integer<unsigned char>(data[i]));
  }
  record_websocket_sent(payload.size());
  auto ex = impl->ws.get_executor();
  enqueue_write(std::move(impl), WritePong{std::move(payload)}, ex);
}

auto WebSocketConnection::handle_frames(
    std::move_only_function<void(WebSocketOpCode, std::span<const std::byte>)>
        on_message) -> spawn_task {
  // Keep object alive for the full coroutine lifetime even if hub ownership
  // changes.
  auto self = shared_from_this();
  (void)self;
  auto impl = impl_;
  if (!impl->accepted.load(std::memory_order_acquire)) {
    if (impl->upgrade_request.has_value()) {
      auto accept_res =
          co_await ws_async_accept(impl->ws, std::move(*impl->upgrade_request));
      impl->upgrade_request.reset();
      if (!accept_res) {
        log::debug("WebSocket accept failed: {}", accept_res.error().message());
        impl->closed.store(true, std::memory_order_release);
        impl->notify_waiters();
        co_return;
      }
    } else {
      // No HTTP request - accept directly (for socketpair or pre-established
      // connections)
      auto accept_res = co_await ws_async_accept_no_req(impl->ws);
      if (!accept_res) {
        log::debug("WebSocket accept failed: {}", accept_res.error().message());
        impl->closed.store(true, std::memory_order_release);
        impl->notify_waiters();
        co_return;
      }
    }
    impl->accepted.store(true, std::memory_order_release);
    impl->notify_waiters();
  }

  while (!impl->closed.load(std::memory_order_acquire)) {
    const auto read_res = co_await ws_async_read(impl->ws, impl->read_buffer);
    if (!read_res.bytes) {
      if (read_res.bytes.error() == make_error_code(beast_ws::error::closed)) {
        on_message(WebSocketOpCode::Close, {});
      } else {
        log::debug("WebSocket read failed: {}",
                   read_res.bytes.error().message());
      }
      impl->closed.store(true, std::memory_order_release);
      break;
    }

    const auto data_size = boost::asio::buffer_size(impl->read_buffer.data());
    std::vector<std::byte> payload(data_size);
    boost::asio::buffer_copy(
        boost::asio::buffer(payload.data(), payload.size()),
        impl->read_buffer.data());
    record_websocket_received(data_size);

    const auto opcode =
        impl->ws.got_text() ? WebSocketOpCode::Text : WebSocketOpCode::Binary;
    impl->read_buffer.consume(data_size);
    try {
      on_message(opcode, payload);
    } catch (const std::exception &e) {
      log::error("WebSocket on_message threw exception fd={}: {}", impl->fd_num,
                 e.what());
      impl->closed.store(true, std::memory_order_release);
      break;
    } catch (...) {
      log::error("WebSocket on_message threw non-standard exception fd={}",
                 impl->fd_num);
      impl->closed.store(true, std::memory_order_release);
      break;
    }
  }
}

auto WebSocketConnection::is_closed() const -> bool {
  return !impl_ || impl_->closed.load(std::memory_order_acquire);
}

auto WebSocketConnection::fd() const -> int {
  return impl_ ? impl_->fd_num : -1;
}

auto WebSocketConnection::force_close() -> void {
  auto impl = impl_;
  if (!impl)
    return;
  if (impl->closed.exchange(true, std::memory_order_acq_rel))
    return;
  auto ex = impl->ws.get_executor();
  boost::asio::dispatch(ex, [impl = std::move(impl)]() mutable {
    boost::system::error_code ec;
    impl->ws.next_layer().cancel(ec);
    impl->ws.next_layer().close(ec);
    impl->notify_waiters();
  });
}

struct TlsWebSocketConnection::Impl {
  WsTlsStream ws;
  beast::flat_buffer read_buffer;
  std::optional<UpgradeRequest> upgrade_request;
  std::atomic<bool> accepted{false};
  std::atomic<bool> closed{false};
  int fd_num{-1};
  std::vector<std::move_only_function<void()>> accept_waiters;
  std::deque<WriteRequest> pending_writes;
  bool write_in_flight{false};

  explicit Impl(boost::asio::ssl::stream<boost::asio::ip::tcp::socket> stream,
                std::optional<UpgradeRequest> req)
      : ws(std::move(stream)), upgrade_request(std::move(req)),
        fd_num(ws.next_layer().next_layer().native_handle()) {
    beast_ws::permessage_deflate pmd;
    pmd.client_enable = true;
    pmd.server_enable = true;
    pmd.compLevel = 3;
    ws.set_option(pmd);
    ws.auto_fragment(false);
    ws.read_message_max(8 * 1024 * 1024);
    ws.set_option(
        beast_ws::stream_base::timeout::suggested(beast::role_type::server));
    ws.set_option(
        beast_ws::stream_base::decorator([](beast_ws::response_type &res) {
          res.set(beast_http::field::server, "DAGForge");
        }));
  }

  auto wait_for_accept() -> spawn_task {
    auto ws_ex = ws.get_executor();
    co_await boost::asio::async_initiate<const boost::asio::use_awaitable_t<>,
                                         void()>(
        [this, ws_ex](auto handler) mutable {
          auto caller_ex = boost::asio::get_associated_executor(handler);
          boost::asio::post(
              ws_ex, [this, caller_ex, handler = std::move(handler)]() mutable {
                if (accepted.load(std::memory_order_acquire) ||
                    closed.load(std::memory_order_acquire)) {
                  boost::asio::post(caller_ex, std::move(handler));
                  return;
                }
                accept_waiters.emplace_back(
                    [h = std::move(handler), caller_ex]() mutable {
                      boost::asio::post(caller_ex, std::move(h));
                    });
              });
        },
        boost::asio::use_awaitable);
  }

  auto notify_waiters() -> void {
    auto waiters = std::move(accept_waiters);
    for (auto &fn : waiters)
      fn();
  }
};

TlsWebSocketConnection::TlsWebSocketConnection(
    boost::asio::ssl::stream<boost::asio::ip::tcp::socket> stream,
    HttpRequest upgrade_request)
    : impl_(std::make_shared<Impl>(
          std::move(stream), to_upgrade_request(std::move(upgrade_request)))) {}

TlsWebSocketConnection::~TlsWebSocketConnection() = default;

auto TlsWebSocketConnection::send_text(std::string text) -> spawn_task {
  auto impl = impl_;
  if (!impl->accepted.load(std::memory_order_acquire)) {
    co_await impl->wait_for_accept();
  }
  if (impl->closed.load(std::memory_order_acquire)) {
    co_return;
  }
  record_websocket_sent(text.size());
  auto ex = impl->ws.get_executor();
  enqueue_write(std::move(impl), WriteText{std::move(text)}, ex);
}

auto TlsWebSocketConnection::send_binary(std::vector<std::byte> data)
    -> spawn_task {
  auto impl = impl_;
  if (!impl->accepted.load(std::memory_order_acquire)) {
    co_await impl->wait_for_accept();
  }
  if (impl->closed.load(std::memory_order_acquire)) {
    co_return;
  }
  record_websocket_sent(data.size());
  auto ex = impl->ws.get_executor();
  enqueue_write(std::move(impl), WriteBinary{std::move(data)}, ex);
}

auto TlsWebSocketConnection::send_close() -> spawn_task {
  auto impl = impl_;
  if (impl->closed.load(std::memory_order_acquire)) {
    co_return;
  }
  record_websocket_sent(0);
  auto ex = impl->ws.get_executor();
  enqueue_write(std::move(impl), WriteClose{}, ex);
}

auto TlsWebSocketConnection::send_pong(std::vector<std::byte> data)
    -> spawn_task {
  auto impl = impl_;
  if (impl->closed.load(std::memory_order_acquire)) {
    co_return;
  }
  std::string payload(data.size(), '\0');
  for (std::size_t i = 0; i < data.size(); ++i) {
    payload[i] = static_cast<char>(std::to_integer<unsigned char>(data[i]));
  }
  record_websocket_sent(payload.size());
  auto ex = impl->ws.get_executor();
  enqueue_write(std::move(impl), WritePong{std::move(payload)}, ex);
}

auto TlsWebSocketConnection::handle_frames(
    std::move_only_function<void(WebSocketOpCode, std::span<const std::byte>)>
        on_message) -> spawn_task {
  auto self = shared_from_this();
  (void)self;
  auto impl = impl_;
  if (!impl->accepted.load(std::memory_order_acquire)) {
    if (impl->upgrade_request.has_value()) {
      auto accept_res =
          co_await ws_async_accept(impl->ws, std::move(*impl->upgrade_request));
      impl->upgrade_request.reset();
      if (!accept_res) {
        log::debug("TLS WebSocket accept failed: {}",
                   accept_res.error().message());
        impl->closed.store(true, std::memory_order_release);
        impl->notify_waiters();
        co_return;
      }
    } else {
      auto accept_res = co_await ws_async_accept_no_req(impl->ws);
      if (!accept_res) {
        log::debug("TLS WebSocket accept failed: {}",
                   accept_res.error().message());
        impl->closed.store(true, std::memory_order_release);
        impl->notify_waiters();
        co_return;
      }
    }
    impl->accepted.store(true, std::memory_order_release);
    impl->notify_waiters();
  }

  while (!impl->closed.load(std::memory_order_acquire)) {
    const auto read_res = co_await ws_async_read(impl->ws, impl->read_buffer);
    if (!read_res.bytes) {
      if (read_res.bytes.error() == make_error_code(beast_ws::error::closed)) {
        on_message(WebSocketOpCode::Close, {});
      } else {
        log::debug("TLS WebSocket read failed: {}",
                   read_res.bytes.error().message());
      }
      impl->closed.store(true, std::memory_order_release);
      break;
    }

    const auto data_size = boost::asio::buffer_size(impl->read_buffer.data());
    std::vector<std::byte> payload(data_size);
    boost::asio::buffer_copy(
        boost::asio::buffer(payload.data(), payload.size()),
        impl->read_buffer.data());
    record_websocket_received(data_size);

    const auto opcode =
        impl->ws.got_text() ? WebSocketOpCode::Text : WebSocketOpCode::Binary;
    impl->read_buffer.consume(data_size);
    try {
      on_message(opcode, payload);
    } catch (const std::exception &e) {
      log::error("TLS WebSocket on_message threw exception fd={}: {}",
                 impl->fd_num, e.what());
      impl->closed.store(true, std::memory_order_release);
      break;
    } catch (...) {
      log::error("TLS WebSocket on_message threw non-standard exception fd={}",
                 impl->fd_num);
      impl->closed.store(true, std::memory_order_release);
      break;
    }
  }
}

auto TlsWebSocketConnection::is_closed() const -> bool {
  return !impl_ || impl_->closed.load(std::memory_order_acquire);
}

auto TlsWebSocketConnection::fd() const -> int {
  return impl_ ? impl_->fd_num : -1;
}

auto TlsWebSocketConnection::force_close() -> void {
  auto impl = impl_;
  if (!impl)
    return;
  if (impl->closed.exchange(true, std::memory_order_acq_rel))
    return;
  auto ex = impl->ws.get_executor();
  boost::asio::dispatch(ex, [impl = std::move(impl)]() mutable {
    boost::system::error_code ec;
    impl->ws.next_layer().next_layer().cancel(ec);
    impl->ws.next_layer().next_layer().close(ec);
    impl->notify_waiters();
  });
}

struct WebSocketHub::Impl : std::enable_shared_from_this<WebSocketHub::Impl> {
  static constexpr std::size_t kMaxPendingMessages = 256;

  struct Connection {
    std::shared_ptr<IWebSocketConnection> conn;
    std::optional<std::string> run_filter;
    std::deque<std::string> pending_messages;
    std::size_t dropped_messages{0};
    bool sending{false};
  };

  // Per-shard local state — only accessed by the shard that owns it,
  // so no mutex is required.
  struct ShardLocalState {
    std::vector<std::shared_ptr<Connection>> connections;
  };

  Runtime &runtime;
  std::vector<ShardLocalState> shard_states_; // indexed by shard_id
  std::atomic<std::size_t> total_connections_{0};

  explicit Impl(Runtime &rt) : runtime(rt), shard_states_(rt.shard_count()) {}

  // drain_connection runs entirely on the shard that owns the connection,
  // so no mutex is required to access the connection's pending_messages.
  static auto drain_connection(std::shared_ptr<Connection> connection)
      -> spawn_task {
    while (!connection->conn->is_closed()) {
      if (connection->pending_messages.empty()) {
        connection->sending = false;
        co_return;
      }
      std::string msg = std::move(connection->pending_messages.front());
      connection->pending_messages.pop_front();
      co_await connection->conn->send_text(std::move(msg));
    }
    connection->sending = false;
  }

  // Called on shard `shard_id`'s executor to enqueue and drain a single shard.
  auto broadcast_on_shard(shard_id sid, const std::string &json_str,
                          const std::optional<std::string> &dag_run_id)
      -> void {
    auto &local = shard_states_[sid];
    const auto before = local.connections.size();
    std::erase_if(local.connections,
                  [](const std::shared_ptr<Connection> &entry) {
                    return !entry || !entry->conn || entry->conn->is_closed();
                  });
    const auto removed = before - local.connections.size();
    if (removed > 0) {
      total_connections_.fetch_sub(removed, std::memory_order_relaxed);
    }

    for (auto &entry : local.connections) {
      if (entry->run_filter && dag_run_id &&
          *entry->run_filter != *dag_run_id) {
        continue;
      }

      if (entry->pending_messages.size() >= kMaxPendingMessages) {
        entry->pending_messages.pop_front();
        ++entry->dropped_messages;
        if ((entry->dropped_messages & (entry->dropped_messages - 1)) == 0) {
          log::warn("WebSocket slow consumer fd={} dropped={} pending={}",
                    entry->conn->fd(), entry->dropped_messages,
                    entry->pending_messages.size());
        }
      }
      entry->pending_messages.emplace_back(json_str);
      if (!entry->sending) {
        entry->sending = true;
        runtime.spawn_on(sid, drain_connection(entry));
      }
    }
  }

  auto broadcast_json(const std::string &json_str,
                      const std::optional<std::string> &dag_run_id) -> void {
    if (total_connections_.load(std::memory_order_relaxed) == 0) {
      return;
    }

    const auto n = runtime.shard_count();
    for (unsigned i = 0; i < n; ++i) {
      // Post to each shard's executor so that broadcast_on_shard runs
      // exclusively on that shard — no cross-shard data access, no locks.
      boost::asio::post(runtime.executor_for(i),
                        [self = shared_from_this(), i, json_str, dag_run_id]() {
                          self->broadcast_on_shard(i, json_str, dag_run_id);
                        });
    }
  }
};

WebSocketHub::WebSocketHub(Runtime &runtime)
    : impl_(std::make_shared<Impl>(runtime)) {}

WebSocketHub::~WebSocketHub() = default;

auto WebSocketHub::add_connection(std::shared_ptr<IWebSocketConnection> conn,
                                  std::optional<std::string> run_filter)
    -> void {
  // Must be called from within a shard coroutine so that current_shard()
  // identifies the executor that owns this TCP socket.
  const shard_id sid = impl_->runtime.current_shard();
  impl_->shard_states_[sid].connections.emplace_back(
      std::make_shared<Impl::Connection>(
          Impl::Connection{.conn = std::move(conn),
                           .run_filter = std::move(run_filter),
                           .pending_messages = {},
                           .dropped_messages = 0,
                           .sending = false}));
  impl_->total_connections_.fetch_add(1, std::memory_order_relaxed);
}

auto WebSocketHub::remove_connection(int fd) -> void {
  // Must be called from the same shard that called add_connection.
  const shard_id sid = impl_->runtime.current_shard();
  auto &local = impl_->shard_states_[sid].connections;
  const auto before = local.size();
  std::erase_if(local, [fd](const std::shared_ptr<Impl::Connection> &entry) {
    return entry->conn->fd() == fd;
  });
  const auto removed = before - local.size();
  if (removed > 0) {
    impl_->total_connections_.fetch_sub(removed, std::memory_order_relaxed);
  }
}

auto WebSocketHub::broadcast_log(const LogMessage &msg) -> void {
  if (impl_->total_connections_.load(std::memory_order_relaxed) == 0) {
    return;
  }

  JsonValue j = {{"type", "log"},
                 {"timestamp", msg.timestamp},
                 {"dag_run_id", msg.dag_run_id},
                 {"task_id", msg.task_id},
                 {"stream", msg.stream},
                 {"content", msg.content}};
  impl_->broadcast_json(dump_json(j), msg.dag_run_id);
}

auto WebSocketHub::broadcast_event(const EventMessage &event) -> void {
  if (impl_->total_connections_.load(std::memory_order_relaxed) == 0) {
    return;
  }

  JsonValue j = {{"type", "event"},          {"timestamp", event.timestamp},
                 {"event", event.event},     {"dag_run_id", event.dag_run_id},
                 {"task_id", event.task_id}, {"data", event.data}};
  impl_->broadcast_json(dump_json(j), event.dag_run_id);
}

auto WebSocketHub::connection_count() const -> size_t {
  return impl_->total_connections_.load(std::memory_order_relaxed);
}

auto WebSocketHub::messages_sent_total() const -> std::uint64_t {
  return websocket_metrics().messages_sent_total.load(
      std::memory_order_relaxed);
}

auto WebSocketHub::messages_received_total() const -> std::uint64_t {
  return websocket_metrics().messages_received_total.load(
      std::memory_order_relaxed);
}

auto WebSocketHub::message_size_snapshots() const
    -> std::vector<std::pair<std::string, metrics::Histogram::Snapshot>> {
  std::vector<std::pair<std::string, metrics::Histogram::Snapshot>> out;
  out.emplace_back("sent", websocket_metrics().message_size_sent.snapshot());
  out.emplace_back("received",
                   websocket_metrics().message_size_received.snapshot());
  return out;
}

auto WebSocketHub::close_all() -> void {
  const auto n = impl_->runtime.shard_count();
  for (unsigned i = 0; i < n; ++i) {
    boost::asio::post(impl_->runtime.executor_for(i), [self = impl_, i]() {
      auto connections = std::move(self->shard_states_[i].connections);
      self->shard_states_[i].connections.clear();
      if (!connections.empty()) {
        self->total_connections_.fetch_sub(connections.size(),
                                           std::memory_order_relaxed);
      }
      for (auto &entry : connections) {
        if (!entry || !entry->conn) {
          continue;
        }
        entry->conn->force_close();
      }
    });
  }
}

} // namespace dagforge::http
