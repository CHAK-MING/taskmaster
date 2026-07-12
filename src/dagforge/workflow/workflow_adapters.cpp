#include "dagforge/workflow/workflow_adapters.hpp"

#include "dagforge/client/http/http_client.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/util/url.hpp"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <format>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace dagforge::workflow {
namespace {

struct ProviderCatalog {
  WorkflowConfig config;
};

[[nodiscard]] auto find_provider(const WorkflowConfig &config,
                                 std::string_view name)
    -> Result<const ModelProviderConfig *> {
  const auto requested = name.empty() ? std::string_view{"openai"} : name;
  const auto it = std::ranges::find(config.model_providers, requested,
                                    &ModelProviderConfig::name);
  if (it == config.model_providers.end()) {
    return fail(Error::NotFound);
  }
  return ok(std::addressof(*it));
}

struct ResolvedTool {
  const McpServerConfig *server{nullptr};
  std::string name;
};

[[nodiscard]] auto resolve_tool(const WorkflowConfig &config,
                                std::string_view tool) -> Result<ResolvedTool> {
  if (tool.empty()) {
    return fail(Error::InvalidArgument);
  }

  const auto separator = tool.find('/');
  if (separator != std::string_view::npos) {
    const auto server_name = tool.substr(0, separator);
    const auto tool_name = tool.substr(separator + 1);
    const auto it = std::ranges::find(config.mcp_servers, server_name,
                                      &McpServerConfig::name);
    if (it == config.mcp_servers.end() || tool_name.empty()) {
      return fail(Error::NotFound);
    }
    return ok(ResolvedTool{.server = std::addressof(*it),
                           .name = std::string{tool_name}});
  }

  if (config.mcp_servers.size() != 1) {
    return fail(Error::InvalidArgument);
  }
  return ok(ResolvedTool{.server = std::addressof(config.mcp_servers.front()),
                         .name = std::string{tool}});
}

[[nodiscard]] auto resolve_secret(const CredentialRef &credential,
                                  std::string_view fallback_env)
    -> Result<std::string> {
  const auto env_name = credential.name.empty()
                            ? fallback_env
                            : std::string_view{credential.name};
  if (env_name.empty()) {
    return ok(std::string{});
  }
  const char *value = std::getenv(std::string{env_name}.c_str());
  if (value == nullptr || *value == '\0') {
    return fail(Error::Unauthorized);
  }
  return ok(std::string{value});
}

[[nodiscard]] auto timeout_for(
    std::chrono::steady_clock::time_point deadline, int configured_seconds)
    -> Result<std::chrono::milliseconds> {
  const auto configured = std::chrono::seconds(configured_seconds);
  if (deadline == std::chrono::steady_clock::time_point{}) {
    return ok(std::chrono::duration_cast<std::chrono::milliseconds>(configured));
  }
  const auto remaining = deadline - std::chrono::steady_clock::now();
  if (remaining <= std::chrono::steady_clock::duration::zero()) {
    return fail(Error::Timeout);
  }
  return ok(std::chrono::duration_cast<std::chrono::milliseconds>(
      std::min(remaining,
               std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                   configured))));
}

[[nodiscard]] auto member(const JsonValue &value, std::string_view key)
    -> const JsonValue * {
  if (!value.is_object()) {
    return nullptr;
  }
  const auto &object = value.get_object();
  const auto it = object.find(std::string{key});
  return it == object.end() ? nullptr : std::addressof(it->second);
}

[[nodiscard]] auto string_member(const JsonValue &value, std::string_view key)
    -> std::string {
  const auto *item = member(value, key);
  return item && item->is_string() ? item->as<std::string>() : std::string{};
}

[[nodiscard]] auto uint_member(const JsonValue &value, std::string_view key)
    -> std::uint64_t {
  const auto *item = member(value, key);
  if (!item || !item->is_number()) {
    return 0;
  }
  return static_cast<std::uint64_t>(item->as<double>());
}

[[nodiscard]] auto bool_member(const JsonValue &value, std::string_view key,
                               bool fallback = false) -> bool {
  const auto *item = member(value, key);
  return item && item->is_boolean() ? item->as<bool>() : fallback;
}

[[nodiscard]] auto join_path(std::string_view base_path,
                             std::string_view suffix) -> std::string {
  std::string out;
  if (base_path.empty() || base_path == "/") {
    out = suffix.empty() ? "/" : std::string{suffix};
  } else {
    out = base_path;
    if (out.ends_with('/') && suffix.starts_with('/')) {
      out.pop_back();
    } else if (!out.ends_with('/') && !suffix.starts_with('/')) {
      out.push_back('/');
    }
    out.append(suffix);
  }
  if (out.empty() || out.front() != '/') {
    out.insert(out.begin(), '/');
  }
  return out;
}

[[nodiscard]] auto json_from_http_body(const http::HttpResponse &response)
    -> Result<JsonValue> {
  std::string body(response.body.begin(), response.body.end());
  const auto content_type = response.headers.get("Content-Type");
  if (content_type && content_type->find("text/event-stream") !=
                          std::string::npos) {
    std::string selected;
    std::size_t offset = 0;
    while (offset < body.size()) {
      const auto end = body.find('\n', offset);
      auto line = std::string_view{body}.substr(
          offset, end == std::string::npos ? body.size() - offset
                                           : end - offset);
      if (line.starts_with("data:")) {
        line.remove_prefix(5);
        while (!line.empty() && line.front() == ' ') {
          line.remove_prefix(1);
        }
        if (line != "[DONE]") {
          selected.assign(line);
        }
      }
      if (end == std::string::npos) {
        break;
      }
      offset = end + 1;
    }
    if (selected.empty()) {
      return fail(Error::ProtocolError);
    }
    return parse_json(selected);
  }
  return parse_json(body);
}

[[nodiscard]] auto connect_url(const util::ParsedHttpUrl &url,
                               http::HttpClientConfig config)
    -> task<Result<std::unique_ptr<http::HttpClient>>> {
  if (url.tls) {
    co_return co_await http::HttpClient::connect_tls(
        current_io_context(), url.host, url.port, config);
  }
  co_return co_await http::HttpClient::connect_tcp(
      current_io_context(), url.host, url.port, config);
}

[[nodiscard]] auto post_json(http::HttpClient &client, std::string path,
                             std::string body, http::HttpHeaders headers)
    -> task<Result<http::HttpResponse>> {
  headers.set("Content-Type", "application/json");
  co_return co_await client.post_json(path, body, headers);
}

[[nodiscard]] auto parse_openai_response(const JsonValue &json,
                                         bool expect_structured)
    -> Result<ModelResponse> {
  if (!json.is_object()) {
    return fail(Error::ProtocolError);
  }

  if (const auto *error = member(json, "error"); error && !error->is_null()) {
    return fail(Error::Unknown);
  }

  ModelResponse response;
  response.provider_request_id = string_member(json, "id");
  std::string text = string_member(json, "output_text");

  const auto *output = member(json, "output");
  if (output && output->is_array()) {
    for (const auto &item : output->get_array()) {
      const auto type = string_member(item, "type");
      if (type == "message") {
        const auto *content = member(item, "content");
        if (content && content->is_array()) {
          for (const auto &part : content->get_array()) {
            const auto part_type = string_member(part, "type");
            if (part_type == "output_text" || part_type == "text") {
              text.append(string_member(part, "text"));
            }
          }
        }
      } else if (type == "function_call") {
        ToolCall call;
        call.name = string_member(item, "name");
        const auto arguments = string_member(item, "arguments");
        auto parsed_arguments = parse_json(arguments);
        call.arguments = parsed_arguments ? std::move(*parsed_arguments)
                                          : JsonValue{arguments};
        response.tool_calls.push_back(std::move(call));
      }
    }
  }

  response.message = Message{.role = "assistant", .content = text};
  if (expect_structured && !text.empty()) {
    auto structured = parse_json(text);
    if (!structured) {
      return fail(Error::ParseError);
    }
    response.structured_output = std::move(*structured);
  }

  if (const auto *usage = member(json, "usage")) {
    response.usage.input_tokens = uint_member(*usage, "input_tokens");
    response.usage.output_tokens = uint_member(*usage, "output_tokens");
  }
  return ok(std::move(response));
}

[[nodiscard]] auto invoke_openai(std::shared_ptr<const ProviderCatalog> catalog,
                                 ModelCall call)
    -> task<Result<ModelResponse>> {
  auto provider = find_provider(catalog->config, call.provider);
  if (!provider) {
    co_return fail(provider.error());
  }
  auto base_url = util::parse_http_url((*provider)->base_url);
  if (!base_url) {
    co_return fail(base_url.error());
  }
  auto timeout = timeout_for(call.deadline, (*provider)->timeout_sec);
  if (!timeout) {
    co_return fail(timeout.error());
  }
  auto secret = resolve_secret(call.credential, (*provider)->api_key_env);
  if (!secret) {
    co_return fail(secret.error());
  }

  http::HttpClientConfig client_config{
      .connect_timeout = *timeout,
      .read_timeout = *timeout,
      .max_response_size = (*provider)->max_response_bytes,
      .keep_alive = false,
  };
  auto client = co_await connect_url(*base_url, client_config);
  if (!client) {
    co_return fail(client.error());
  }

  JsonValue request = JsonValue::object_t{};
  request["model"] = call.model;
  request["max_output_tokens"] = call.max_output_tokens;
  request["temperature"] = call.temperature;
  JsonValue input = JsonValue::array_t{};
  for (const auto &message : call.messages) {
    JsonValue item = JsonValue::object_t{};
    item["role"] = message.role;
    item["content"] = message.content;
    input.get_array().push_back(std::move(item));
  }
  request["input"] = std::move(input);

  http::HttpHeaders headers;
  headers.set("Authorization", std::format("Bearer {}", *secret));
  auto response = co_await post_json(
      **client, join_path(base_url->path, (*provider)->responses_path),
      dump_json(request), std::move(headers));
  (*client)->close();
  if (!response) {
    co_return fail(response.error());
  }
  const auto status = static_cast<int>(response->status);
  if (status < 200 || status >= 300) {
    co_return fail(status == 401 || status == 403 ? Error::Unauthorized
                                                  : Error::ProtocolError);
  }
  auto json = json_from_http_body(*response);
  if (!json) {
    co_return fail(json.error());
  }
  co_return parse_openai_response(*json, call.response_schema.has_value());
}

[[nodiscard]] auto mcp_request(http::HttpClient &client, std::string path,
                               JsonValue request,
                               const McpServerConfig &server,
                               const std::optional<std::string> &session_id,
                               const std::string &secret)
    -> task<Result<std::pair<JsonValue, std::optional<std::string>>>> {
  http::HttpHeaders headers;
  headers.set("Accept", "application/json, text/event-stream");
  headers.set("MCP-Protocol-Version", server.protocol_version);
  if (session_id && !session_id->empty()) {
    headers.set("Mcp-Session-Id", *session_id);
  }
  if (!secret.empty()) {
    headers.set("Authorization", std::format("Bearer {}", secret));
  }

  auto response =
      co_await post_json(client, std::move(path), dump_json(request), headers);
  if (!response) {
    co_return fail(response.error());
  }
  const auto status = static_cast<int>(response->status);
  if (status < 200 || status >= 300) {
    co_return fail(status == 401 || status == 403 ? Error::Unauthorized
                                                  : Error::ProtocolError);
  }
  auto body = json_from_http_body(*response);
  if (!body) {
    co_return fail(body.error());
  }
  std::optional<std::string> returned_session;
  if (auto session = response->headers.get("Mcp-Session-Id"); session) {
    returned_session = std::move(*session);
  }
  co_return ok(std::pair{std::move(*body), std::move(returned_session)});
}

[[nodiscard]] auto invoke_mcp(std::shared_ptr<const ProviderCatalog> catalog,
                              ToolInvocation invocation)
    -> task<Result<ToolResult>> {
  auto resolved = resolve_tool(catalog->config, invocation.tool);
  if (!resolved) {
    co_return fail(resolved.error());
  }
  const auto &server = *resolved->server;
  auto url = util::parse_http_url(server.url);
  if (!url) {
    co_return fail(url.error());
  }
  auto timeout = timeout_for(invocation.deadline, server.timeout_sec);
  if (!timeout) {
    co_return fail(timeout.error());
  }
  auto secret = resolve_secret(invocation.credential, server.bearer_token_env);
  if (!secret) {
    co_return fail(secret.error());
  }

  auto client = co_await connect_url(
      *url, http::HttpClientConfig{.connect_timeout = *timeout,
                                   .read_timeout = *timeout,
                                   .max_response_size =
                                       server.max_response_bytes,
                                   .keep_alive = true});
  if (!client) {
    co_return fail(client.error());
  }

  JsonValue initialize = JsonValue::object_t{};
  initialize["jsonrpc"] = "2.0";
  initialize["id"] = 1;
  initialize["method"] = "initialize";
  JsonValue initialize_params = JsonValue::object_t{};
  initialize_params["protocolVersion"] = server.protocol_version;
  initialize_params["capabilities"] = JsonValue::object_t{};
  JsonValue client_info = JsonValue::object_t{};
  client_info["name"] = "dagforge";
  client_info["version"] = "0.4.0";
  initialize_params["clientInfo"] = std::move(client_info);
  initialize["params"] = std::move(initialize_params);

  auto initialized = co_await mcp_request(**client, url->path,
                                          std::move(initialize), server,
                                          std::nullopt, *secret);
  if (!initialized) {
    (*client)->close();
    co_return fail(initialized.error());
  }
  if (member(initialized->first, "error") != nullptr) {
    (*client)->close();
    co_return fail(Error::ProtocolError);
  }

  JsonValue notification = JsonValue::object_t{};
  notification["jsonrpc"] = "2.0";
  notification["method"] = "notifications/initialized";
  notification["params"] = JsonValue::object_t{};
  auto notification_result = co_await mcp_request(
      **client, url->path, std::move(notification), server,
      initialized->second, *secret);
  if (!notification_result &&
      notification_result.error() != make_error_code(Error::ParseError)) {
    (*client)->close();
    co_return fail(notification_result.error());
  }

  JsonValue request = JsonValue::object_t{};
  request["jsonrpc"] = "2.0";
  request["id"] = 2;
  request["method"] = "tools/call";
  JsonValue params = JsonValue::object_t{};
  params["name"] = resolved->name;
  params["arguments"] = std::move(invocation.arguments);
  request["params"] = std::move(params);

  auto called = co_await mcp_request(**client, url->path, std::move(request),
                                     server, initialized->second, *secret);
  (*client)->close();
  if (!called) {
    co_return fail(called.error());
  }
  if (const auto *error = member(called->first, "error")) {
    co_return ok(ToolResult{.name = std::move(resolved->name),
                            .success = false,
                            .error = string_member(*error, "message")});
  }

  const auto *result = member(called->first, "result");
  if (!result) {
    co_return fail(Error::ProtocolError);
  }
  ToolResult tool_result;
  tool_result.name = std::move(resolved->name);
  tool_result.success = !bool_member(*result, "isError", false);
  if (const auto *structured = member(*result, "structuredContent")) {
    tool_result.output = *structured;
  } else {
    tool_result.output = *result;
  }
  if (!tool_result.success) {
    const auto *content = member(*result, "content");
    if (content && content->is_array()) {
      for (const auto &part : content->get_array()) {
        if (string_member(part, "type") == "text") {
          if (!tool_result.error.empty()) {
            tool_result.error.push_back('\n');
          }
          tool_result.error.append(string_member(part, "text"));
        }
      }
    }
  }
  co_return ok(std::move(tool_result));
}

} // namespace

auto make_default_workflow_adapters(WorkflowConfig config)
    -> WorkflowAdapters {
  auto catalog =
      std::make_shared<const ProviderCatalog>(ProviderCatalog{std::move(config)});
  WorkflowAdapters adapters;
  adapters.invoke_model = [catalog](ModelCall call) {
    return invoke_openai(catalog, std::move(call));
  };
  adapters.invoke_tool = [catalog](ToolInvocation invocation) {
    return invoke_mcp(catalog, std::move(invocation));
  };
  return adapters;
}

} // namespace dagforge::workflow
