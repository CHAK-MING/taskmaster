#include "dagforge/workflow/executors/http_adapter.hpp"

#include "dagforge/http/http_client.hpp"
#include "../../executor/detail/shard_state.hpp"
#include "dagforge/io/context.hpp"
#include "dagforge/util/json.hpp"

#include "detail/adapter_utils.hpp"

#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/url/parse.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <charconv>
#include <chrono>
#include <compare>
#include <cstdint>
#include <experimental/scope>
#include <format>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>

namespace dagforge::detail {

struct HttpHeaderEntry {
  std::string name;
  std::string value;
};

struct HttpInputHeaderBinding {
  std::string input;
  std::string header;
};

struct HttpNodeConfig {
  std::string method;
  std::string url;
  std::vector<HttpHeaderEntry> headers;
  std::vector<HttpInputHeaderBinding> input_headers;
  std::optional<std::string> body;
  std::optional<std::string> body_input;
  std::vector<std::uint16_t> accepted_statuses;
};

} // namespace dagforge::detail

namespace glz {

template <> struct meta<dagforge::detail::HttpHeaderEntry> {
  using T = dagforge::detail::HttpHeaderEntry;
  static constexpr auto value = object("name", &T::name, "value", &T::value);
};

template <> struct meta<dagforge::detail::HttpInputHeaderBinding> {
  using T = dagforge::detail::HttpInputHeaderBinding;
  static constexpr auto value = object("input", &T::input, "header",
                                       &T::header);
};

template <> struct meta<dagforge::detail::HttpNodeConfig> {
  using T = dagforge::detail::HttpNodeConfig;
  static constexpr auto value = object(
      "method", &T::method, "url", &T::url, "headers", &T::headers,
      "input_headers", &T::input_headers, "body", &T::body, "body_input",
      &T::body_input, "accepted_statuses", &T::accepted_statuses);
};

} // namespace glz

namespace dagforge {
namespace {

using workflow::executor_detail::add_output;
using workflow::executor_detail::input_exists;

struct ParsedHttpTarget {
  bool tls{false};
  std::string host;
  std::uint16_t port{80};
  std::string target{"/"};
  std::string origin;
  std::string host_header;
};

struct IpCidr {
  boost::asio::ip::address network;
  unsigned prefix_length{0};
};

struct HttpExecutorPolicy {
  HttpExecutorConfig config;
  std::unordered_set<std::string> allowed_origins;
  std::vector<IpCidr> allowed_cidrs;
};

struct HttpRequestState {
  InstanceId instance_id;
  workflow::TaskExecutionSink sink;
  boost::asio::cancellation_signal cancellation;
  std::unique_ptr<http::HttpClient> client;
  io::TimingWheel::Handle timeout_handle;
  bool cancel_requested{false};
  bool timed_out{false};
  bool completed{false};
  bool global_slot_acquired{false};
};

using HttpShardState =
    executor_detail::ShardExecutionState<std::shared_ptr<HttpRequestState>>;

struct HttpExecutorCore {
  Runtime *runtime{};
  HttpExecutorPolicy policy;
  std::vector<HttpShardState> shard_states;
  std::atomic<std::size_t> active_requests{0};

  HttpExecutorCore(Runtime &runtime_in, HttpExecutorPolicy policy_in)
      : runtime(&runtime_in), policy(std::move(policy_in)),
        shard_states(runtime_in.shard_count()) {}
};

[[nodiscard]] auto prefix_matches(std::span<const unsigned char> address,
                                  std::span<const unsigned char> network,
                                  unsigned prefix_length) -> bool {
  const auto whole_bytes = prefix_length / 8;
  const auto remaining_bits = prefix_length % 8;
  if (!std::equal(address.begin(), address.begin() + whole_bytes,
                  network.begin())) {
    return false;
  }
  if (remaining_bits == 0) {
    return true;
  }
  const auto mask = static_cast<unsigned char>(0xffU << (8U - remaining_bits));
  return (address[whole_bytes] & mask) == (network[whole_bytes] & mask);
}

[[nodiscard]] auto cidr_contains(const IpCidr &cidr,
                                 const boost::asio::ip::address &address)
    -> bool {
  if (cidr.network.is_v4() != address.is_v4()) {
    return false;
  }
  if (address.is_v4()) {
    const auto candidate = address.to_v4().to_bytes();
    const auto network = cidr.network.to_v4().to_bytes();
    return prefix_matches(candidate, network, cidr.prefix_length);
  }
  const auto candidate = address.to_v6().to_bytes();
  const auto network = cidr.network.to_v6().to_bytes();
  return prefix_matches(candidate, network, cidr.prefix_length);
}

[[nodiscard]] auto parse_cidr(std::string_view value) -> Result<IpCidr> {
  const auto separator = value.rfind('/');
  if (separator == std::string_view::npos || separator == 0 ||
      separator + 1 >= value.size()) {
    return fail(Error::InvalidArgument);
  }
  boost::system::error_code address_error;
  auto address = boost::asio::ip::make_address(
      std::string{value.substr(0, separator)}, address_error);
  if (address_error) {
    return fail(Error::InvalidArgument);
  }
  unsigned prefix = 0;
  const auto token = value.substr(separator + 1);
  const auto [end, error] =
      std::from_chars(token.data(), token.data() + token.size(), prefix);
  const auto maximum = address.is_v4() ? 32U : 128U;
  if (error != std::errc{} || end != token.data() + token.size() ||
      prefix > maximum) {
    return fail(Error::InvalidArgument);
  }
  return ok(IpCidr{.network = std::move(address), .prefix_length = prefix});
}

[[nodiscard]] auto special_use_address(
    const boost::asio::ip::address &address) -> bool {
  if (address.is_unspecified() || address.is_loopback() ||
      address.is_multicast()) {
    return true;
  }
  if (address.is_v4()) {
    const auto value = address.to_v4().to_uint();
    const auto in_range = [value](std::uint32_t network, unsigned prefix) {
      const auto mask = prefix == 0 ? 0U : 0xffffffffU << (32U - prefix);
      return (value & mask) == (network & mask);
    };
    return in_range(0x00000000U, 8) || in_range(0x0a000000U, 8) ||
           in_range(0x64400000U, 10) || in_range(0x7f000000U, 8) ||
           in_range(0xa9fe0000U, 16) || in_range(0xac100000U, 12) ||
           in_range(0xc0000000U, 24) || in_range(0xc0000200U, 24) ||
           in_range(0xc0a80000U, 16) || in_range(0xc6120000U, 15) ||
           in_range(0xc6336400U, 24) || in_range(0xcb007100U, 24) ||
           in_range(0xe0000000U, 4) || in_range(0xf0000000U, 4);
  }
  const auto v6 = address.to_v6();
  if (v6.is_link_local() || v6.is_site_local()) {
    return true;
  }
  if (v6.is_v4_mapped()) {
    return special_use_address(boost::asio::ip::address_v4{
        {v6.to_bytes()[12], v6.to_bytes()[13], v6.to_bytes()[14],
         v6.to_bytes()[15]}});
  }
  const auto bytes = v6.to_bytes();
  const std::array<unsigned char, 16> ula{0xfc};
  const std::array<unsigned char, 16> documentation{0x20, 0x01, 0x0d, 0xb8};
  return prefix_matches(bytes, ula, 7) ||
         prefix_matches(bytes, documentation, 32);
}

[[nodiscard]] auto address_allowed(
    const HttpExecutorPolicy &policy,
    const boost::asio::ip::address &address) -> bool {
  if (std::ranges::any_of(policy.allowed_cidrs,
                          [&](const IpCidr &cidr) {
                            return cidr_contains(cidr, address);
                          })) {
    return true;
  }
  return !policy.config.deny_private_networks ||
         !special_use_address(address);
}

[[nodiscard]] auto try_acquire_global_slot(HttpExecutorCore &core) -> bool {
  auto current = core.active_requests.load(std::memory_order_relaxed);
  while (current < core.policy.config.max_concurrent_requests) {
    if (core.active_requests.compare_exchange_weak(
            current, current + 1, std::memory_order_acq_rel,
            std::memory_order_relaxed)) {
      return true;
    }
  }
  return false;
}

[[nodiscard]] auto lowercase_ascii(std::string value) -> std::string {
  std::ranges::transform(value, value.begin(), [](unsigned char ch) {
    return static_cast<char>(std::tolower(ch));
  });
  return value;
}

[[nodiscard]] auto parse_http_target(std::string_view url)
    -> Result<ParsedHttpTarget> {
  if (url.find("://") == std::string_view::npos) {
    return fail(Error::InvalidUrl);
  }
  auto parsed = boost::urls::parse_uri(url);
  if (!parsed) {
    return fail(Error::InvalidUrl);
  }
  const auto uri = *parsed;
  const auto scheme = lowercase_ascii(std::string{uri.scheme()});
  if (scheme != "http" && scheme != "https") {
    return fail(Error::InvalidUrl);
  }
  if (!uri.userinfo().empty() || !uri.fragment().empty()) {
    return fail(Error::InvalidUrl);
  }

  auto host = lowercase_ascii(std::string{uri.host()});
  if (host.empty()) {
    return fail(Error::InvalidUrl);
  }
  const bool tls = scheme == "https";
  std::uint16_t port = tls ? 443 : 80;
  if (uri.has_port()) {
    const auto parsed_port = uri.port_number();
    if (parsed_port == 0 || parsed_port > 65535) {
      return fail(Error::InvalidUrl);
    }
    port = static_cast<std::uint16_t>(parsed_port);
  }

  auto authority_host = host;
  if (host.contains(':') && !host.starts_with('[')) {
    authority_host = std::format("[{}]", host);
  }
  auto target = std::string{uri.encoded_path()};
  if (target.empty()) {
    target = "/";
  }
  if (const auto query = uri.encoded_query(); !query.empty()) {
    target.push_back('?');
    target.append(query.data(), query.size());
  }

  const auto default_port = tls ? 443 : 80;
  auto host_header = authority_host;
  if (port != default_port) {
    host_header.append(std::format(":{}", port));
  }
  return ok(ParsedHttpTarget{
      .tls = tls,
      .host = std::move(host),
      .port = port,
      .target = std::move(target),
      .origin = std::format("{}://{}:{}", scheme, authority_host, port),
      .host_header = std::move(host_header),
  });
}

[[nodiscard]] auto valid_origin(std::string_view value,
                                bool allow_plaintext)
    -> Result<std::string> {
  auto parsed = parse_http_target(value);
  if (!parsed) {
    return fail(parsed.error());
  }
  if (parsed->target != "/") {
    return fail(Error::InvalidArgument);
  }
  if (!parsed->tls && !allow_plaintext) {
    return fail(Error::Unauthorized);
  }
  return ok(std::move(parsed->origin));
}

[[nodiscard]] auto parse_method(std::string_view method)
    -> Result<http::HttpMethod> {
  if (method == "GET")
    return ok(http::HttpMethod::GET);
  if (method == "POST")
    return ok(http::HttpMethod::POST);
  if (method == "PUT")
    return ok(http::HttpMethod::PUT);
  if (method == "PATCH")
    return ok(http::HttpMethod::PATCH);
  if (method == "DELETE")
    return ok(http::HttpMethod::DELETE);
  if (method == "OPTIONS")
    return ok(http::HttpMethod::OPTIONS);
  if (method == "HEAD")
    return ok(http::HttpMethod::HEAD);
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
      "host",          "content-length", "transfer-encoding",
      "connection",    "proxy-connection", "keep-alive",
      "te",            "trailer",        "upgrade",
      "expect",
  };
  const auto normalized = lowercase_ascii(std::string{name});
  return std::ranges::find(kForbidden, normalized) != kForbidden.end();
}

[[nodiscard]] auto header_wire_bytes(std::string_view name,
                                     std::string_view value)
    -> std::uint64_t {
  return static_cast<std::uint64_t>(name.size() + value.size() + 4);
}

[[nodiscard]] auto request_header_bytes(const http::HttpHeaders &headers)
    -> std::uint64_t {
  std::uint64_t total = 0;
  for (const auto &header : headers) {
    total += header_wire_bytes(header.name, header.value);
  }
  return total;
}

[[nodiscard]] auto output_supported(std::string_view output) -> bool {
  return output == "status" || output == "body" || output == "headers" ||
         output == "result";
}

[[nodiscard]] auto value_to_text(const workflow::WorkflowValue &value)
    -> Result<std::pair<std::string, bool>> {
  return std::visit(
      [](const auto &typed) -> Result<std::pair<std::string, bool>> {
        using T = std::decay_t<decltype(typed)>;
        if constexpr (std::same_as<T, std::monostate>) {
          return ok(std::pair{std::string{}, false});
        } else if constexpr (std::same_as<T, bool> ||
                             std::same_as<T, std::int64_t> ||
                             std::same_as<T, double>) {
          return ok(std::pair{std::format("{}", typed), false});
        } else if constexpr (std::same_as<T, std::string>) {
          return ok(std::pair{typed, false});
        } else if constexpr (std::same_as<T, JsonValue>) {
          return ok(std::pair{dump_json(typed), true});
        } else if constexpr (std::same_as<T, workflow::ArtifactRef>) {
          return fail(Error::Unsupported);
        }
        return fail(Error::Unsupported);
      },
      value);
}

[[nodiscard]] auto valid_utf8(std::span<const std::uint8_t> bytes) -> bool {
  std::size_t index = 0;
  const auto continuation = [](std::uint8_t value) {
    return value >= 0x80 && value <= 0xbf;
  };
  while (index < bytes.size()) {
    const auto first = bytes[index++];
    if (first <= 0x7f) {
      continue;
    }
    if (first >= 0xc2 && first <= 0xdf) {
      if (index >= bytes.size() || !continuation(bytes[index++]))
        return false;
      continue;
    }
    if (first >= 0xe0 && first <= 0xef) {
      if (index + 1 >= bytes.size())
        return false;
      const auto second = bytes[index++];
      const auto third = bytes[index++];
      if (!continuation(third) ||
          (first == 0xe0 ? second < 0xa0 || second > 0xbf
                         : first == 0xed ? second < 0x80 || second > 0x9f
                                         : !continuation(second))) {
        return false;
      }
      continue;
    }
    if (first >= 0xf0 && first <= 0xf4) {
      if (index + 2 >= bytes.size())
        return false;
      const auto second = bytes[index++];
      const auto third = bytes[index++];
      const auto fourth = bytes[index++];
      if (!continuation(third) || !continuation(fourth) ||
          (first == 0xf0 ? second < 0x90 || second > 0xbf
                         : first == 0xf4 ? second < 0x80 || second > 0x8f
                                         : !continuation(second))) {
        return false;
      }
      continue;
    }
    return false;
  }
  return true;
}

[[nodiscard]] auto valid_utf8(std::string_view value) -> bool {
  return valid_utf8(std::span{
      reinterpret_cast<const std::uint8_t *>(value.data()), value.size()});
}

[[nodiscard]] auto valid_response_headers(
    const http::HttpHeaders &headers) -> bool {
  return std::ranges::all_of(headers, [](const auto &header) {
    return valid_utf8(header.name) && valid_utf8(header.value);
  });
}

[[nodiscard]] auto accepted_status(
    std::uint16_t status, std::span<const std::uint16_t> accepted) -> bool {
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

[[nodiscard]] auto response_headers_json(const http::HttpHeaders &headers)
    -> JsonValue {
  JsonValue encoded = JsonValue::array_t{};
  for (const auto &field : headers) {
    JsonValue item = JsonValue::object_t{};
    item["name"] = field.name;
    item["value"] = field.value;
    encoded.get_array().push_back(std::move(item));
  }
  return encoded;
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
                      Result<workflow::ExecutorOutputs> result) -> void {
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
  }
  auto completion = std::move(state->sink.on_complete);
  if (completion) {
    completion(state->instance_id, std::move(result));
  }
}

[[nodiscard]] auto lifecycle_error(
    const std::shared_ptr<HttpRequestState> &state,
    std::error_code operation_error) -> std::error_code {
  if (state->timed_out) {
    return make_error_code(Error::Timeout);
  }
  if (state->cancel_requested) {
    return make_error_code(Error::Cancelled);
  }
  return operation_error;
}

[[nodiscard]] auto interrupted_error(
    const std::shared_ptr<HttpRequestState> &state)
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
                      ParsedHttpTarget target, http::HttpRequest request,
                      detail::HttpNodeConfig config,
                      std::vector<WorkflowPortId> requested_outputs,
                      std::chrono::seconds timeout) -> spawn_task {
  if (auto interrupted = interrupted_error(state)) {
    complete_request(core, shard, state, fail(*interrupted));
    co_return;
  }
  const auto timeout_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(timeout);
  const auto transport_timeout = timeout_ms + std::chrono::seconds(5);
  http::HttpClientConfig client_config{
      .connect_timeout = transport_timeout,
      .read_timeout = transport_timeout,
      .max_response_headers = core->policy.config.max_response_headers,
      .max_response_header_size = static_cast<std::size_t>(
          core->policy.config.max_response_header_bytes),
      .max_response_size = static_cast<std::size_t>(
          core->policy.config.max_response_body_bytes),
      .keep_alive = false,
      .tls_min_version = core->policy.config.tls_min_version,
      .tls_ca_file = core->policy.config.tls_ca_file,
      .tls_client_cert_file = core->policy.config.tls_client_cert_file,
      .tls_client_key_file = core->policy.config.tls_client_key_file,
      .endpoint_allowed = [core](const boost::asio::ip::address &address) {
        return address_allowed(core->policy, address);
      },
  };

  auto connected = target.tls
                       ? co_await http::HttpClient::connect_tls(
                             current_io_context(), target.host, target.port,
                             client_config, state->cancellation.slot())
                       : co_await http::HttpClient::connect_tcp(
                             current_io_context(), target.host, target.port,
                             client_config, state->cancellation.slot());
  if (!connected) {
    complete_request(core, shard, state,
                     fail(lifecycle_error(state, connected.error())));
    co_return;
  }
  if (auto interrupted = interrupted_error(state)) {
    complete_request(core, shard, state, fail(*interrupted));
    co_return;
  }
  state->client = std::move(*connected);

  auto response = co_await state->client->request(
      std::move(request), state->cancellation.slot());
  if (!response) {
    complete_request(core, shard, state,
                     fail(lifecycle_error(state, response.error())));
    co_return;
  }
  if (auto interrupted = interrupted_error(state)) {
    complete_request(core, shard, state, fail(*interrupted));
    co_return;
  }

  const auto status = static_cast<std::uint16_t>(response->status);
  if (!accepted_status(status, config.accepted_statuses)) {
    complete_request(core, shard, state, fail(error_for_status(status)));
    co_return;
  }
  if (!valid_utf8(response->body) ||
      !valid_response_headers(response->headers)) {
    complete_request(core, shard, state, fail(Error::ProtocolError));
    co_return;
  }

  const std::string body{reinterpret_cast<const char *>(response->body.data()),
                         response->body.size()};
  workflow::ExecutorOutputs outputs;
  outputs.reserve(requested_outputs.size());
  add_output(outputs, requested_outputs, "status",
             static_cast<std::int64_t>(status));
  add_output(outputs, requested_outputs, "body", body);
  add_output(outputs, requested_outputs, "headers",
             response_headers_json(response->headers));
  add_output(outputs, requested_outputs, "result", body);
  complete_request(core, shard, state, ok(std::move(outputs)));
}

class HttpWorkflowAdapter final : public workflow::ITaskExecutor {
public:
  explicit HttpWorkflowAdapter(std::shared_ptr<HttpExecutorCore> core)
      : core_(std::move(core)) {}

  [[nodiscard]] auto type() const noexcept -> std::string_view override {
    return "http";
  }

  [[nodiscard]] auto compile(
      JsonValue config, workflow::ExecutorCompileContext context) const
      -> Result<JsonValue> override {
    auto parsed = parse_json_as<detail::HttpNodeConfig>(dump_json(config));
    if (!parsed) {
      return fail(parsed.error());
    }
    auto method = parse_method(parsed->method);
    auto target = parse_http_target(parsed->url);
    if (!method || !target) {
      return fail(!method ? method.error() : target.error());
    }
    if (!target->tls && !core_->policy.config.allow_plaintext) {
      return fail(Error::Unauthorized);
    }
    if (!core_->policy.allowed_origins.contains(target->origin)) {
      return fail(Error::Unauthorized);
    }
    if (parsed->body && parsed->body_input) {
      return fail(Error::InvalidArgument);
    }
    if ((*method == http::HttpMethod::GET ||
         *method == http::HttpMethod::HEAD) &&
        (parsed->body || parsed->body_input)) {
      return fail(Error::InvalidArgument);
    }
    if (parsed->body &&
        parsed->body->size() > core_->policy.config.max_request_body_bytes) {
      return fail(Error::ResourceExhausted);
    }
    if (parsed->body_input &&
        (parsed->body_input->empty() ||
         !input_exists(context, *parsed->body_input))) {
      return fail(Error::InvalidArgument);
    }

    if (parsed->headers.size() + parsed->input_headers.size() >
        core_->policy.config.max_request_headers) {
      return fail(Error::ResourceExhausted);
    }
    std::unordered_set<std::string> header_names;
    std::uint64_t static_header_bytes = 0;
    for (const auto &header : parsed->headers) {
      const auto normalized = lowercase_ascii(header.name);
      if (!valid_header_name(header.name) ||
          !safe_header_value(header.value) ||
          executor_owned_header(header.name) ||
          !header_names.emplace(normalized).second) {
        return fail(Error::InvalidArgument);
      }
      static_header_bytes += header_wire_bytes(header.name, header.value);
    }
    for (const auto &binding : parsed->input_headers) {
      const auto normalized = lowercase_ascii(binding.header);
      if (binding.input.empty() || !input_exists(context, binding.input) ||
          !valid_header_name(binding.header) ||
          executor_owned_header(binding.header) ||
          !header_names.emplace(normalized).second) {
        return fail(Error::InvalidArgument);
      }
      static_header_bytes += header_wire_bytes(binding.header, {});
    }
    if (static_header_bytes >
        core_->policy.config.max_request_header_bytes) {
      return fail(Error::ResourceExhausted);
    }

    std::unordered_set<std::uint16_t> accepted;
    for (const auto status : parsed->accepted_statuses) {
      if (status < 100 || status > 599 || !accepted.emplace(status).second) {
        return fail(Error::InvalidArgument);
      }
    }
    std::ranges::sort(parsed->accepted_statuses);

    for (const auto &output : context.outputs) {
      if (!output_supported(output.str())) {
        return fail(Error::InvalidArgument);
      }
    }

    auto encoded = serialize_json(*parsed);
    if (!encoded) {
      return fail(encoded.error());
    }
    return parse_json(*encoded);
  }

  auto start(workflow::TaskExecutionRequest request,
             workflow::TaskExecutionSink sink) -> Result<void> override {
    if (!core_->runtime->is_current_shard()) {
      return fail(Error::InvalidState);
    }
    const auto shard = core_->runtime->current_shard();
    auto &shard_state = core_->shard_states[shard];
    if (shard_state.active.size() >=
        core_->policy.config.max_concurrent_requests_per_shard) {
      return fail(Error::QueueFull);
    }
    if (shard_state.active.contains(request.instance_id)) {
      return fail(Error::AlreadyExists);
    }

    auto parsed =
        parse_json_as<detail::HttpNodeConfig>(dump_json(request.config));
    if (!parsed) {
      return fail(parsed.error());
    }
    auto method = parse_method(parsed->method);
    auto target = parse_http_target(parsed->url);
    if (!method) {
      return fail(method.error());
    }
    if (!target) {
      return fail(target.error());
    }
    if (!target->tls && !core_->policy.config.allow_plaintext) {
      return fail(Error::Unauthorized);
    }
    if (!core_->policy.allowed_origins.contains(target->origin)) {
      return fail(Error::Unauthorized);
    }

    http::HttpRequest http_request;
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
    if (body.size() > core_->policy.config.max_request_body_bytes) {
      return fail(Error::ResourceExhausted);
    }
    if (json_body && !http_request.headers.contains("Content-Type")) {
      http_request.headers.set("Content-Type", "application/json");
    }
    if (request_header_bytes(http_request.headers) >
        core_->policy.config.max_request_header_bytes) {
      return fail(Error::ResourceExhausted);
    }
    http_request.body.assign(body.begin(), body.end());

    if (!try_acquire_global_slot(*core_)) {
      return fail(Error::ResourceExhausted);
    }
    bool release_global_slot = true;
    const auto release_on_failure = std::experimental::scope_exit([&] {
      if (release_global_slot) {
        core_->active_requests.fetch_sub(1, std::memory_order_acq_rel);
      }
    });

    auto state = std::make_shared<HttpRequestState>();
    state->instance_id = request.instance_id.clone();
    state->sink = std::move(sink);
    state->global_slot_acquired = true;
    shard_state.register_active(state->instance_id, state);
    state->timeout_handle = core_->runtime->schedule_after_on(
        shard, request.timeout, [weak = std::weak_ptr<HttpRequestState>{state}] {
          if (auto locked = weak.lock()) {
            cancel_state(locked, true);
          }
        });

    if (state->sink.on_state) {
      state->sink.on_state(state->instance_id, "running");
    }
    core_->runtime->spawn(run_http_request(
        core_, shard, state, std::move(*target), std::move(http_request),
        std::move(*parsed), std::move(request.outputs), request.timeout));
    release_global_slot = false;
    return ok();
  }

  auto cancel(const InstanceId &instance_id) -> void override {
    for (shard_id shard = 0; shard < core_->shard_states.size(); ++shard) {
      core_->runtime->post_to(
          shard, [core = core_, shard, instance_id = instance_id.clone()] {
            auto active = core->shard_states[shard].find_active_mut(instance_id);
            if (active == core->shard_states[shard].active_end()) {
              return;
            }
            cancel_state(active->second, false);
          });
    }
  }

private:
  std::shared_ptr<HttpExecutorCore> core_;
};

} // namespace

namespace workflow {

auto create_http_executor_adapter(Runtime &runtime, HttpExecutorConfig config)
    -> Result<std::shared_ptr<ITaskExecutor>> {
  if (config.max_request_headers == 0 ||
      config.max_request_header_bytes == 0 ||
      config.max_request_body_bytes == 0 ||
      config.max_response_headers == 0 ||
      config.max_response_header_bytes == 0 ||
      config.max_response_body_bytes == 0 ||
      config.max_concurrent_requests_per_shard == 0 ||
      config.max_concurrent_requests == 0 ||
      (config.tls_min_version != "1.2" &&
       config.tls_min_version != "1.3") ||
      (config.tls_client_cert_file.empty() !=
       config.tls_client_key_file.empty())) {
    return fail(Error::InvalidArgument);
  }
  HttpExecutorPolicy policy{.config = std::move(config)};
  for (const auto &configured : policy.config.allowed_origins) {
    auto origin = valid_origin(configured, policy.config.allow_plaintext);
    if (!origin) {
      return fail(origin.error());
    }
    if (!policy.allowed_origins.emplace(std::move(*origin)).second) {
      return fail(Error::InvalidArgument);
    }
  }
  policy.allowed_cidrs.reserve(policy.config.allowed_ip_cidrs.size());
  for (const auto &configured : policy.config.allowed_ip_cidrs) {
    auto cidr = parse_cidr(configured);
    if (!cidr) {
      return fail(cidr.error());
    }
    policy.allowed_cidrs.push_back(std::move(*cidr));
  }
  auto core =
      std::make_shared<HttpExecutorCore>(runtime, std::move(policy));
  return ok(std::shared_ptr<ITaskExecutor>{
      std::make_shared<HttpWorkflowAdapter>(std::move(core))});
}

} // namespace workflow

} // namespace dagforge
