#include "dagforge/executors/http/executor.hpp"

#include "dagforge/core/scope_exit.hpp"
#include "dagforge/http/http_client.hpp"
#include "dagforge/io/context.hpp"
#include "dagforge/util/ascii.hpp"
#include "dagforge/util/json.hpp"

#include "detail/egress_policy.hpp"
#include "../detail/task_executor_utils.hpp"

#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/url/parse.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <compare>
#include <condition_variable>
#include <cstdint>
#include <format>
#include <memory>
#include <mutex>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace dagforge::executors::http::detail {

struct HeaderEntry {
  std::string name;
  std::string value;
};

struct InputHeaderBinding {
  std::string input;
  std::string header;
};

struct NodeConfig {
  std::string method;
  std::string url;
  std::vector<HeaderEntry> headers;
  std::vector<InputHeaderBinding> input_headers;
  std::optional<std::string> body;
  std::optional<std::string> body_input;
  std::vector<std::uint16_t> accepted_statuses;
};

struct DiagnosticHeader {
  std::string name;
  std::string value;
  bool redacted{false};
};

} // namespace dagforge::executors::http::detail

namespace glz {

template <> struct meta<dagforge::executors::http::detail::HeaderEntry> {
  using T = dagforge::executors::http::detail::HeaderEntry;
  static constexpr auto value = object("name", &T::name, "value", &T::value);
};

template <> struct meta<dagforge::executors::http::detail::InputHeaderBinding> {
  using T = dagforge::executors::http::detail::InputHeaderBinding;
  static constexpr auto value =
      object("input", &T::input, "header", &T::header);
};

template <> struct meta<dagforge::executors::http::detail::NodeConfig> {
  using T = dagforge::executors::http::detail::NodeConfig;
  static constexpr auto value =
      object("method", &T::method, "url", &T::url, "headers", &T::headers,
             "input_headers", &T::input_headers, "body", &T::body, "body_input",
             &T::body_input, "accepted_statuses", &T::accepted_statuses);
};

} // namespace glz

namespace dagforge::executors::http {
namespace {

namespace transport = ::dagforge::http;

using executors::detail::add_output;
using executors::detail::input_exists;
using executors::detail::outputs_supported;

inline constexpr std::array<std::string_view, 4> kSupportedOutputs{
    "status", "body", "headers", "result"};

[[nodiscard]] auto parse_node_config(const JsonPayload &config)
    -> Result<detail::NodeConfig> {
  return parse_json_as<detail::NodeConfig>(config.encoded());
}

[[nodiscard]] auto encode_node_config(const detail::NodeConfig &config)
    -> Result<JsonPayload> {
  return JsonPayload::from(config);
}

[[nodiscard]] auto http_description(const config::HttpEgressConfig &config)
    -> Result<workflow::ExecutorDescription> {
  auto schema = json_schema_payload<detail::NodeConfig>();
  if (!schema) {
    return fail(schema.error());
  }
  const auto example_url = config.allowed_origins.empty()
                               ? std::string{"https://allowed.example/resource"}
                               : config.allowed_origins.front() + "/resource";
  auto example =
      JsonPayload::from(glz::obj{"method", "GET", "url", example_url});
  if (!example) {
    return fail(example.error());
  }
  const std::vector<std::string_view> supported_outputs(
      kSupportedOutputs.begin(), kSupportedOutputs.end());
  auto constraints = JsonPayload::from(
      glz::obj{"allowed_origins",           config.allowed_origins,
               "allow_plaintext",           config.allow_plaintext,
               "deny_private_networks",     config.deny_private_networks,
               "ip_exception_count",        config.allowed_ip_cidrs.size(),
               "max_request_headers",       config.max_request_headers,
               "max_request_header_bytes",  config.max_request_header_bytes,
               "max_request_body_bytes",    config.max_request_body_bytes,
               "max_response_headers",      config.max_response_headers,
               "max_response_header_bytes", config.max_response_header_bytes,
               "max_response_body_bytes",   config.max_response_body_bytes,
               "max_concurrent_requests",   config.max_concurrent_requests,
               "tls_min_version",           config.tls_min_version,
               "supported_outputs",         supported_outputs});
  if (!constraints) {
    return fail(constraints.error());
  }
  return ok(workflow::ExecutorDescription{
      .type = "http",
      .summary = "Call an HTTP origin authorized by server egress policy",
      .config_schema = std::move(*schema),
      .examples = {std::move(*example)},
      .constraints = std::move(*constraints),
  });
}

[[nodiscard]] auto parse_method(std::string_view method)
    -> Result<::dagforge::http::HttpMethod> {
  using Method = ::dagforge::http::HttpMethod;
  if (method == "GET")
    return ok(Method::GET);
  if (method == "POST")
    return ok(Method::POST);
  if (method == "PUT")
    return ok(Method::PUT);
  if (method == "PATCH")
    return ok(Method::PATCH);
  if (method == "DELETE")
    return ok(Method::DELETE);
  if (method == "OPTIONS")
    return ok(Method::OPTIONS);
  if (method == "HEAD")
    return ok(Method::HEAD);
  return fail(Error::InvalidArgument);
}

[[nodiscard]] auto valid_header_name(std::string_view name) -> bool {
  if (name.empty()) {
    return false;
  }
  return std::ranges::all_of(name, [](unsigned char ch) {
    return std::isalnum(ch) != 0 ||
           std::string_view{"!#$%&'*+-.^_`|~"}.contains(ch);
  });
}

[[nodiscard]] auto safe_header_value(std::string_view value) -> bool {
  return !value.contains('\r') && !value.contains('\n') &&
         !value.contains('\0');
}

[[nodiscard]] auto executor_owned_header(std::string_view name) -> bool {
  static constexpr std::array<std::string_view, 10> kForbidden{
      "host",       "content-length",   "transfer-encoding",
      "connection", "proxy-connection", "keep-alive",
      "te",         "trailer",          "upgrade",
      "expect",
  };
  const auto normalized = util::ascii_lowercase(name);
  return std::ranges::find(kForbidden, normalized) != kForbidden.end();
}

[[nodiscard]] auto header_wire_bytes(std::string_view name,
                                     std::string_view value) -> std::uint64_t {
  return static_cast<std::uint64_t>(name.size() + value.size() + 4);
}

struct HttpRequestState {
  InstanceId instance_id;
  workflow::TaskExecutionSink sink;
  boost::asio::cancellation_signal cancellation;
  std::unique_ptr<transport::HttpClient> client;
  io::TimingWheel::Handle timeout_handle;
  bool cancel_requested{false};
  bool timed_out{false};
  bool completed{false};
  bool global_slot_acquired{false};
};

struct PooledHttpClient {
  std::unique_ptr<transport::HttpClient> client;
  std::chrono::steady_clock::time_point expires_at;
};

struct HttpShardState {
  std::unordered_map<InstanceId, std::shared_ptr<HttpRequestState>> active;
  std::unordered_map<std::string, std::vector<PooledHttpClient>> idle_clients;
  std::size_t idle_client_count{0};

  auto register_active(const InstanceId &id,
                       std::shared_ptr<HttpRequestState> state) -> void {
    active[id] = std::move(state);
  }

  auto unregister_active(const InstanceId &id) -> void { active.erase(id); }

  [[nodiscard]] auto find_active_mut(const InstanceId &id) {
    return active.find(id);
  }

  [[nodiscard]] auto active_end() { return active.end(); }
};

struct HttpExecutorCore {
  Runtime *runtime{};
  detail::HttpEgressPolicy policy;
  std::vector<HttpShardState> shard_states;
  std::atomic<std::size_t> active_requests{0};
  std::atomic_bool quiescing{false};
  std::mutex lifecycle_mutex;
  std::condition_variable lifecycle_changed;

  HttpExecutorCore(Runtime &runtime_in, detail::HttpEgressPolicy policy_in)
      : runtime(&runtime_in), policy(std::move(policy_in)),
        shard_states(runtime_in.shard_count()) {}
};

[[nodiscard]] auto try_acquire_global_slot(HttpExecutorCore &core) -> bool {
  auto current = core.active_requests.load(std::memory_order_relaxed);
  while (current < core.policy.config().max_concurrent_requests) {
    if (core.active_requests.compare_exchange_weak(current, current + 1,
                                                   std::memory_order_acq_rel,
                                                   std::memory_order_relaxed)) {
      return true;
    }
  }
  return false;
}

auto prune_idle_origin(HttpShardState &state, std::string_view origin,
                       std::chrono::steady_clock::time_point now) -> void {
  const auto found = state.idle_clients.find(std::string{origin});
  if (found == state.idle_clients.end()) {
    return;
  }
  auto &clients = found->second;
  const auto old_size = clients.size();
  std::erase_if(clients, [now](PooledHttpClient &entry) {
    if (entry.client && entry.client->is_reusable() && entry.expires_at > now) {
      return false;
    }
    if (entry.client) {
      entry.client->close();
    }
    return true;
  });
  state.idle_client_count -= old_size - clients.size();
  if (clients.empty()) {
    state.idle_clients.erase(found);
  }
}

auto prune_idle_clients(HttpShardState &state,
                        std::chrono::steady_clock::time_point now) -> void {
  for (auto current = state.idle_clients.begin();
       current != state.idle_clients.end();) {
    auto &clients = current->second;
    const auto old_size = clients.size();
    std::erase_if(clients, [now](PooledHttpClient &entry) {
      if (entry.client && entry.client->is_reusable() &&
          entry.expires_at > now) {
        return false;
      }
      if (entry.client) {
        entry.client->close();
      }
      return true;
    });
    state.idle_client_count -= old_size - clients.size();
    if (clients.empty()) {
      current = state.idle_clients.erase(current);
    } else {
      ++current;
    }
  }
}

[[nodiscard]] auto acquire_idle_client(HttpExecutorCore &core, shard_id shard,
                                       std::string_view origin)
    -> std::unique_ptr<transport::HttpClient> {
  auto &state = core.shard_states[shard];
  prune_idle_origin(state, origin, std::chrono::steady_clock::now());
  const auto found = state.idle_clients.find(std::string{origin});
  if (found == state.idle_clients.end()) {
    return {};
  }

  auto &clients = found->second;
  if (clients.empty()) {
    state.idle_clients.erase(found);
    return {};
  }
  auto client = std::move(clients.back().client);
  clients.pop_back();
  state.idle_client_count -= 1;
  if (clients.empty()) {
    state.idle_clients.erase(found);
  }
  return client;
}

auto release_idle_client(HttpExecutorCore &core, shard_id shard,
                         std::string origin,
                         std::unique_ptr<transport::HttpClient> client)
    -> void {
  if (!client) {
    return;
  }
  const auto &config = core.policy.config();
  if (core.quiescing.load(std::memory_order_acquire) ||
      !client->is_reusable()) {
    client->close();
    return;
  }

  auto &state = core.shard_states[shard];
  const auto now = std::chrono::steady_clock::now();
  prune_idle_clients(state, now);
  const auto found = state.idle_clients.find(origin);
  const auto origin_size =
      found == state.idle_clients.end() ? 0U : found->second.size();
  if (state.idle_client_count >= config.max_idle_connections_per_shard ||
      origin_size >= config.max_idle_connections_per_origin) {
    client->close();
    return;
  }

  auto &clients = state.idle_clients[std::move(origin)];
  clients.push_back(PooledHttpClient{
      .client = std::move(client),
      .expires_at =
          now + std::chrono::milliseconds(config.idle_connection_timeout_ms),
  });
  state.idle_client_count += 1;
}

auto close_idle_clients(HttpShardState &state) -> void {
  for (auto &[_, clients] : state.idle_clients) {
    for (auto &entry : clients) {
      if (entry.client) {
        entry.client->close();
      }
    }
  }
  state.idle_clients.clear();
  state.idle_client_count = 0;
}

[[nodiscard]] auto request_header_bytes(const transport::HttpHeaders &headers)
    -> std::uint64_t {
  std::uint64_t total = 0;
  for (const auto &header : headers) {
    total += header_wire_bytes(header.name, header.value);
  }
  return total;
}

[[nodiscard]] auto value_to_text(const workflow::WorkflowValue &value)
    -> Result<std::pair<std::string, bool>> {
  if (std::holds_alternative<workflow::ArtifactRef>(value)) {
    return fail(Error::Unsupported);
  }
  return ok(std::pair{workflow::workflow_value_text(value),
                      std::holds_alternative<JsonPayload>(value)});
}

[[nodiscard]] auto valid_utf8(std::span<const std::uint8_t> bytes) -> bool {
  return glz::validate_utf8(reinterpret_cast<const char *>(bytes.data()),
                            bytes.size());
}

[[nodiscard]] auto valid_utf8(std::string_view value) -> bool {
  return valid_utf8(std::span{
      reinterpret_cast<const std::uint8_t *>(value.data()), value.size()});
}

[[nodiscard]] auto valid_response_headers(const transport::HttpHeaders &headers)
    -> bool {
  return std::ranges::all_of(headers, [](const auto &header) {
    return valid_utf8(header.name) && valid_utf8(header.value);
  });
}

[[nodiscard]] auto accepted_status(std::uint16_t status,
                                   std::span<const std::uint16_t> accepted)
    -> bool {
  if (accepted.empty()) {
    return status >= 200 && status <= 299;
  }
  return std::ranges::find(accepted, status) != accepted.end();
}

[[nodiscard]] auto error_for_status(std::uint16_t status) -> Error {
  if (status == 401 || status == 403)
    return Error::Unauthorized;
  if (status == 404)
    return Error::NotFound;
  if (status == 408)
    return Error::Timeout;
  if (status == 429)
    return Error::RateLimited;
  if (status >= 500 && status <= 599)
    return Error::Unknown;
  return Error::ProtocolError;
}

[[nodiscard]] auto response_headers(const transport::HttpHeaders &headers)
    -> Result<JsonPayload> {
  std::vector<detail::HeaderEntry> encoded;
  encoded.reserve(headers.size());
  for (const auto &field : headers) {
    encoded.push_back(
        detail::HeaderEntry{.name = field.name, .value = field.value});
  }
  return JsonPayload::from(encoded);
}

[[nodiscard]] auto sensitive_response_header(std::string_view name) -> bool {
  static constexpr std::array<std::string_view, 6> kSensitiveHeaders{
      "authorization", "proxy-authorization", "cookie",
      "set-cookie",    "www-authenticate",    "proxy-authenticate",
  };
  const auto normalized = util::ascii_lowercase(name);
  return std::ranges::find(kSensitiveHeaders, normalized) !=
             kSensitiveHeaders.end() ||
         normalized.ends_with("-api-key") || normalized.ends_with("-secret") ||
         normalized.ends_with("-token");
}

[[nodiscard]] auto
diagnostic_response_headers(const transport::HttpHeaders &headers)
    -> std::vector<detail::DiagnosticHeader> {
  std::vector<detail::DiagnosticHeader> encoded;
  encoded.reserve(headers.size());
  for (const auto &field : headers) {
    const auto redacted = sensitive_response_header(field.name);
    encoded.push_back(detail::DiagnosticHeader{
        .name = field.name,
        .value = redacted ? "[redacted]" : field.value,
        .redacted = redacted,
    });
  }
  return encoded;
}

[[nodiscard]] auto
http_operation_failure(const std::shared_ptr<HttpRequestState> &state,
                       std::error_code operation_error)
    -> workflow::ExecutionFailure {
  auto details = JsonPayload::from(
      glz::obj{"cause", workflow::FailureCause{
                            .category = operation_error.category().name(),
                            .value = operation_error.value(),
                            .message = operation_error.message(),
                        }});
  if (!details) {
    return workflow::make_execution_failure(
        Error::ProtocolError, "http_failure_details_encode_failed",
        "HTTP failure diagnostics could not be encoded");
  }
  if (state->timed_out) {
    return workflow::make_execution_failure(Error::Timeout, "http_timed_out",
                                            "HTTP request timed out",
                                            std::move(*details));
  }
  if (state->cancel_requested) {
    return workflow::make_execution_failure(Error::Cancelled, "http_cancelled",
                                            "HTTP request was cancelled",
                                            std::move(*details));
  }
  return workflow::make_execution_failure(
      operation_error, "http_transport_failed", "HTTP transport failed");
}

[[nodiscard]] auto http_response_details(std::uint16_t status,
                                         const transport::HttpHeaders &headers,
                                         std::span<const std::uint8_t> body)
    -> Result<JsonPayload> {
  const auto headers_valid = valid_response_headers(headers);
  const auto body_valid = valid_utf8(body);
  const auto diagnostic_headers =
      headers_valid ? std::optional{diagnostic_response_headers(headers)}
                    : std::nullopt;
  const auto diagnostic_body =
      body_valid
          ? std::optional{std::string{
                reinterpret_cast<const char *>(body.data()), body.size()}}
          : std::nullopt;
  return JsonPayload::from(glz::obj{
      "status", status, "body_size_bytes", body.size(), "headers_valid_utf8",
      headers_valid, "body_valid_utf8", body_valid, "headers",
      diagnostic_headers, "body", diagnostic_body});
}

[[nodiscard]] auto rejected_status_failure(
    std::uint16_t status, const transport::HttpHeaders &headers,
    std::span<const std::uint8_t> body) -> workflow::ExecutionFailure {
  auto details = http_response_details(status, headers, body);
  if (!details) {
    return workflow::make_execution_failure(
        Error::ProtocolError, "http_failure_details_encode_failed",
        "HTTP response diagnostics could not be encoded");
  }
  return workflow::make_execution_failure(
      error_for_status(status), "http_status_rejected",
      std::format("HTTP response status {} was not accepted", status),
      std::move(*details));
}

[[nodiscard]] auto invalid_response_failure(
    std::uint16_t status, const transport::HttpHeaders &headers,
    std::span<const std::uint8_t> body) -> workflow::ExecutionFailure {
  auto details = http_response_details(status, headers, body);
  if (!details) {
    return workflow::make_execution_failure(
        Error::ProtocolError, "http_failure_details_encode_failed",
        "HTTP response diagnostics could not be encoded");
  }
  return workflow::make_execution_failure(
      Error::ProtocolError, "http_invalid_response",
      "HTTP response contains invalid UTF-8 data", std::move(*details));
}

auto cancel_state(const std::shared_ptr<HttpRequestState> &state,
                  bool timed_out) -> void {
  if (state->completed) {
    return;
  }
  state->timed_out = state->timed_out || timed_out;
  state->cancel_requested = state->cancel_requested || !timed_out;
  state->cancellation.emit(boost::asio::cancellation_type::total);
  if (state->client) {
    state->client->close();
  }
}

auto complete_request(const std::shared_ptr<HttpExecutorCore> &core,
                      shard_id shard,
                      const std::shared_ptr<HttpRequestState> &state,
                      workflow::TaskExecutionResult result) -> void {
  if (state->completed) {
    return;
  }
  state->completed = true;
  if (state->timeout_handle.valid()) {
    core->runtime->cancel_after_on(shard, state->timeout_handle);
  }
  core->shard_states[shard].unregister_active(state->instance_id);
  if (state->global_slot_acquired) {
    state->global_slot_acquired = false;
    core->active_requests.fetch_sub(1, std::memory_order_acq_rel);
    core->lifecycle_changed.notify_all();
  }
  auto completion = std::move(state->sink.on_complete);
  if (completion) {
    completion(state->instance_id, std::move(result));
  }
}

[[nodiscard]] auto
interrupted_error(const std::shared_ptr<HttpRequestState> &state)
    -> std::optional<std::error_code> {
  if (state->timed_out) {
    return make_error_code(Error::Timeout);
  }
  if (state->cancel_requested) {
    return make_error_code(Error::Cancelled);
  }
  return std::nullopt;
}

auto run_http_request(std::shared_ptr<HttpExecutorCore> core, shard_id shard,
                      std::shared_ptr<HttpRequestState> state,
                      detail::ParsedHttpTarget target,
                      transport::HttpRequest request, detail::NodeConfig config,
                      std::vector<WorkflowPortId> requested_outputs)
    -> spawn_task {
  if (auto interrupted = interrupted_error(state)) {
    complete_request(
        core, shard, state,
        workflow::task_failed(http_operation_failure(state, *interrupted)));
    co_return;
  }
  const auto &egress = core->policy.config();
  transport::HttpClientConfig client_config{
      .dns_timeout = std::chrono::milliseconds(egress.dns_timeout_ms),
      .connect_timeout = std::chrono::milliseconds(egress.connect_timeout_ms),
      .tls_handshake_timeout =
          std::chrono::milliseconds(egress.tls_handshake_timeout_ms),
      .write_timeout = std::chrono::milliseconds(egress.write_timeout_ms),
      .first_byte_timeout =
          std::chrono::milliseconds(egress.first_byte_timeout_ms),
      .read_timeout = std::chrono::milliseconds(egress.read_timeout_ms),
      .max_response_headers = egress.max_response_headers,
      .max_response_header_size =
          static_cast<std::size_t>(egress.max_response_header_bytes),
      .max_response_size =
          static_cast<std::size_t>(egress.max_response_body_bytes),
      .keep_alive = true,
      .tls_min_version = egress.tls_min_version,
      .tls_ca_file = egress.tls_ca_file,
      .tls_client_cert_file = egress.tls_client_cert_file,
      .tls_client_key_file = egress.tls_client_key_file,
      .endpoint_allowed =
          [core](const boost::asio::ip::address &address) {
            return core->policy.address_allowed(address);
          },
  };

  state->client = acquire_idle_client(*core, shard, target.origin);
  if (!state->client) {
    auto connected = target.tls
                         ? co_await transport::HttpClient::connect_tls(
                               current_io_context(), target.host, target.port,
                               client_config, state->cancellation.slot())
                         : co_await transport::HttpClient::connect_tcp(
                               current_io_context(), target.host, target.port,
                               client_config, state->cancellation.slot());
    if (!connected) {
      complete_request(core, shard, state,
                       workflow::task_failed(
                           http_operation_failure(state, connected.error())));
      co_return;
    }
    state->client = std::move(*connected);
  }
  if (auto interrupted = interrupted_error(state)) {
    complete_request(
        core, shard, state,
        workflow::task_failed(http_operation_failure(state, *interrupted)));
    co_return;
  }

  auto response = co_await state->client->request(std::move(request),
                                                  state->cancellation.slot());
  if (!response) {
    complete_request(
        core, shard, state,
        workflow::task_failed(http_operation_failure(state, response.error())));
    co_return;
  }
  if (auto interrupted = interrupted_error(state)) {
    complete_request(
        core, shard, state,
        workflow::task_failed(http_operation_failure(state, *interrupted)));
    co_return;
  }
  if (state->client->is_reusable()) {
    release_idle_client(*core, shard, target.origin, std::move(state->client));
  } else {
    state->client.reset();
  }

  const auto status = static_cast<std::uint16_t>(response->status);
  if (!accepted_status(status, config.accepted_statuses)) {
    complete_request(core, shard, state,
                     workflow::task_failed(rejected_status_failure(
                         status, response->headers, response->body)));
    co_return;
  }
  if (!valid_utf8(response->body) ||
      !valid_response_headers(response->headers)) {
    complete_request(core, shard, state,
                     workflow::task_failed(invalid_response_failure(
                         status, response->headers, response->body)));
    co_return;
  }

  const std::string body{response->body_as_string()};
  auto headers = response_headers(response->headers);
  if (!headers) {
    complete_request(core, shard, state,
                     workflow::task_failed(workflow::make_execution_failure(
                         headers.error(), "http_response_headers_encode_failed",
                         "HTTP response headers could not be encoded")));
    co_return;
  }
  workflow::ExecutorOutputs outputs;
  outputs.reserve(requested_outputs.size());
  add_output(outputs, requested_outputs, "status",
             static_cast<std::int64_t>(status));
  add_output(outputs, requested_outputs, "body", body);
  add_output(outputs, requested_outputs, "headers", std::move(*headers));
  add_output(outputs, requested_outputs, "result", body);
  complete_request(core, shard, state,
                   workflow::task_succeeded(std::move(outputs)));
}

class HttpTaskExecutor final : public workflow::ITaskExecutor {
public:
  HttpTaskExecutor(std::shared_ptr<HttpExecutorCore> core,
                   workflow::ExecutorDescription description)
      : core_(std::move(core)), description_(std::move(description)) {}

  [[nodiscard]] auto type() const noexcept -> std::string_view override {
    return "http";
  }

  [[nodiscard]] auto describe() const
      -> Result<workflow::ExecutorDescription> override {
    return ok(description_);
  }

  [[nodiscard]] auto compile(JsonPayload config,
                             workflow::ExecutorCompileContext context) const
      -> workflow::ExecutorCompileResult<
          workflow::CompiledExecutorConfig> override {
    auto parsed = parse_node_config(config);
    if (!parsed) {
      return workflow::executor_compile_fail(
          workflow::make_executor_compile_failure(
              parsed.error(), "http_config_invalid",
              "HTTP configuration does not match the expected schema"));
    }
    auto method = parse_method(parsed->method);
    auto target = core_->policy.authorize(parsed->url);
    if (!method) {
      return workflow::executor_compile_fail(
          workflow::make_executor_compile_failure(
              method.error(), "http_method_invalid",
              "HTTP method is not supported", "/method"));
    }
    if (!target) {
      return workflow::executor_compile_fail(
          workflow::make_executor_compile_failure(
              target.error(), "http_target_not_allowed",
              "HTTP target is not allowed by server egress policy", "/url"));
    }
    if (parsed->body && parsed->body_input) {
      return workflow::executor_compile_fail(
          workflow::make_executor_compile_failure(
              Error::InvalidArgument, "http_body_source_conflict",
              "HTTP configuration must use exactly one of body or body_input",
              "/body"));
    }
    if ((*method == transport::HttpMethod::GET ||
         *method == transport::HttpMethod::HEAD) &&
        (parsed->body || parsed->body_input)) {
      return workflow::executor_compile_fail(
          workflow::make_executor_compile_failure(
              Error::InvalidArgument, "http_method_body_forbidden",
              "GET and HEAD HTTP requests cannot carry a body", "/body"));
    }
    if (parsed->body &&
        parsed->body->size() > core_->policy.config().max_request_body_bytes) {
      return workflow::executor_compile_fail(
          workflow::make_executor_compile_failure(
              Error::ResourceExhausted, "http_body_too_large",
              "HTTP static request body exceeds the configured byte limit",
              "/body"));
    }
    if (parsed->body_input && (parsed->body_input->empty() ||
                               !input_exists(context, *parsed->body_input))) {
      return workflow::executor_compile_fail(
          workflow::make_executor_compile_failure(
              Error::InvalidArgument, "http_body_input_invalid",
              "HTTP body_input must reference a declared input",
              "/body_input"));
    }

    if (parsed->headers.size() + parsed->input_headers.size() >
        core_->policy.config().max_request_headers) {
      return workflow::executor_compile_fail(
          workflow::make_executor_compile_failure(
              Error::ResourceExhausted, "http_header_count_exceeded",
              "HTTP request header count exceeds the configured limit",
              "/headers"));
    }
    std::unordered_set<std::string> header_names;
    std::uint64_t static_header_bytes = 0;
    for (const auto &header : parsed->headers) {
      auto normalized = util::ascii_lowercase(header.name);
      if (!valid_header_name(header.name) || !safe_header_value(header.value) ||
          executor_owned_header(header.name) ||
          !header_names.emplace(normalized).second) {
        return workflow::executor_compile_fail(
            workflow::make_executor_compile_failure(
                Error::InvalidArgument, "http_header_invalid",
                "HTTP static headers must be safe, unique, and caller-owned",
                "/headers"));
      }
      static_header_bytes += header_wire_bytes(header.name, header.value);
    }
    for (const auto &binding : parsed->input_headers) {
      auto normalized = util::ascii_lowercase(binding.header);
      if (binding.input.empty() || !input_exists(context, binding.input) ||
          !valid_header_name(binding.header) ||
          executor_owned_header(binding.header) ||
          !header_names.emplace(normalized).second) {
        return workflow::executor_compile_fail(
            workflow::make_executor_compile_failure(
                Error::InvalidArgument, "http_input_header_invalid",
                "HTTP input headers must reference declared inputs and safe "
                "unique headers",
                "/input_headers"));
      }
      static_header_bytes += header_wire_bytes(binding.header, {});
    }
    if (static_header_bytes > core_->policy.config().max_request_header_bytes) {
      return workflow::executor_compile_fail(
          workflow::make_executor_compile_failure(
              Error::ResourceExhausted, "http_header_bytes_exceeded",
              "HTTP request headers exceed the configured byte limit",
              "/headers"));
    }

    std::unordered_set<std::uint16_t> accepted;
    for (const auto status : parsed->accepted_statuses) {
      if (status < 100 || status > 599 || !accepted.emplace(status).second) {
        return workflow::executor_compile_fail(
            workflow::make_executor_compile_failure(
                Error::InvalidArgument, "http_accepted_status_invalid",
                "HTTP accepted_statuses must contain unique status codes from "
                "100 to 599",
                "/accepted_statuses"));
      }
    }
    std::ranges::sort(parsed->accepted_statuses);

    if (!outputs_supported(context.outputs, kSupportedOutputs)) {
      return workflow::executor_compile_fail(
          workflow::make_executor_compile_failure(
              Error::InvalidArgument, "http_outputs_unsupported",
              "HTTP node declares an unsupported output"));
    }

    auto encoded = encode_node_config(*parsed);
    if (!encoded) {
      return workflow::executor_compile_fail(
          workflow::make_executor_compile_failure(
              encoded.error(), "http_config_encode_failed",
              "HTTP compiled configuration could not be encoded"));
    }
    return workflow::executor_compile_ok(
        workflow::CompiledExecutorConfig::from_encoded(std::move(*encoded)));
  }

  auto start(workflow::TaskExecutionRequest request,
             workflow::TaskExecutionSink sink) -> Result<void> override {
    if (core_->quiescing.load(std::memory_order_acquire)) {
      return fail(Error::InvalidState);
    }
    if (!core_->runtime->is_current_shard()) {
      return fail(Error::InvalidState);
    }
    const auto shard = core_->runtime->current_shard();
    auto &shard_state = core_->shard_states[shard];
    if (shard_state.active.size() >=
        core_->policy.config().max_concurrent_requests_per_shard) {
      return fail(Error::QueueFull);
    }
    if (shard_state.active.contains(request.instance_id)) {
      return fail(Error::AlreadyExists);
    }

    auto parsed = parse_node_config(request.config.encoded());
    if (!parsed) {
      return fail(parsed.error());
    }
    auto method = parse_method(parsed->method);
    auto target = core_->policy.authorize(parsed->url);
    if (!method) {
      return fail(method.error());
    }
    if (!target) {
      return fail(target.error());
    }

    transport::HttpRequest http_request;
    http_request.method = *method;
    http_request.path = target->target;
    http_request.headers.set("Host", target->host_header);
    for (const auto &header : parsed->headers) {
      http_request.headers.add(header.name, header.value);
    }
    for (const auto &binding : parsed->input_headers) {
      const auto input = request.inputs.find(binding.input);
      if (input == request.inputs.end()) {
        return fail(Error::InvalidArgument);
      }
      auto value = value_to_text(*input->second);
      if (!value || !safe_header_value(value->first)) {
        return fail(value ? Error::InvalidArgument : value.error());
      }
      http_request.headers.add(binding.header, std::move(value->first));
    }

    std::string body;
    bool json_body = false;
    if (parsed->body) {
      body = *parsed->body;
    } else if (parsed->body_input) {
      const auto input = request.inputs.find(*parsed->body_input);
      if (input == request.inputs.end()) {
        return fail(Error::InvalidArgument);
      }
      auto value = value_to_text(*input->second);
      if (!value) {
        return fail(value.error());
      }
      body = std::move(value->first);
      json_body = value->second;
    }
    if (body.size() > core_->policy.config().max_request_body_bytes) {
      return fail(Error::ResourceExhausted);
    }
    if (json_body && !http_request.headers.contains("Content-Type")) {
      http_request.headers.set("Content-Type", "application/json");
    }
    if (request_header_bytes(http_request.headers) >
        core_->policy.config().max_request_header_bytes) {
      return fail(Error::ResourceExhausted);
    }
    http_request.body.assign(body.begin(), body.end());

    if (!try_acquire_global_slot(*core_)) {
      return fail(Error::ResourceExhausted);
    }
    bool release_global_slot = true;
    const auto release_on_failure = dagforge::scope_exit([&] {
      if (release_global_slot) {
        core_->active_requests.fetch_sub(1, std::memory_order_acq_rel);
      }
    });

    auto state = std::make_shared<HttpRequestState>();
    state->instance_id = request.instance_id.clone();
    state->sink = std::move(sink);
    state->global_slot_acquired = true;
    shard_state.register_active(state->instance_id, state);
    if (core_->quiescing.load(std::memory_order_acquire)) {
      cancel_state(state, false);
    }
    state->timeout_handle = core_->runtime->schedule_after_on(
        shard, request.timeout,
        [weak = std::weak_ptr<HttpRequestState>{state}] {
          if (auto locked = weak.lock()) {
            cancel_state(locked, true);
          }
        });

    if (state->sink.on_state) {
      state->sink.on_state(state->instance_id, "running");
    }
    core_->runtime->spawn(run_http_request(
        core_, shard, state, std::move(*target), std::move(http_request),
        std::move(*parsed), std::move(request.outputs)));
    release_global_slot = false;
    return ok();
  }

  auto cancel(const InstanceId &instance_id) -> void override {
    for (shard_id shard = 0; shard < core_->shard_states.size(); ++shard) {
      core_->runtime->post_to(shard, [core = core_, shard,
                                      instance_id = instance_id.clone()] {
        auto active = core->shard_states[shard].find_active_mut(instance_id);
        if (active == core->shard_states[shard].active_end()) {
          return;
        }
        cancel_state(active->second, false);
      });
    }
  }

  auto quiesce(std::chrono::milliseconds timeout) -> Result<void> override {
    core_->quiescing.store(true, std::memory_order_release);
    auto closed_shards = std::make_shared<std::atomic<std::size_t>>(0);
    for (shard_id shard = 0; shard < core_->shard_states.size(); ++shard) {
      core_->runtime->post_to(shard, [core = core_, shard, closed_shards] {
        close_idle_clients(core->shard_states[shard]);
        for (auto &[_, state] : core->shard_states[shard].active) {
          cancel_state(state, false);
        }
        closed_shards->fetch_add(1, std::memory_order_acq_rel);
        core->lifecycle_changed.notify_all();
      });
    }

    std::unique_lock lock(core_->lifecycle_mutex);
    if (!core_->lifecycle_changed.wait_for(
            lock, timeout, [core = core_, closed_shards] {
              return core->active_requests.load(std::memory_order_acquire) ==
                         0 &&
                     closed_shards->load(std::memory_order_acquire) ==
                         core->shard_states.size();
            })) {
      return fail(Error::Timeout);
    }
    return ok();
  }

private:
  std::shared_ptr<HttpExecutorCore> core_;
  workflow::ExecutorDescription description_;
};

} // namespace

auto create_task_executor(Runtime &runtime,
                          const config::HttpEgressConfig &config)
    -> Result<std::shared_ptr<workflow::ITaskExecutor>> {
  auto policy = detail::HttpEgressPolicy::create(config);
  if (!policy) {
    return fail(policy.error());
  }
  auto description = http_description(config);
  if (!description) {
    return fail(description.error());
  }
  auto core = std::make_shared<HttpExecutorCore>(runtime, std::move(*policy));
  return ok(std::shared_ptr<workflow::ITaskExecutor>{
      std::make_shared<HttpTaskExecutor>(std::move(core),
                                         std::move(*description))});
}

} // namespace dagforge::executors::http
