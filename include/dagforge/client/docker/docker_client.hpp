#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/client/http/http_client.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/core/coroutine.hpp"
#include "dagforge/executor/executor_utils.hpp"
#include "dagforge/io/context.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/util/log.hpp"
#include "dagforge/util/url.hpp"

#include <bit>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <expected>
#include <flat_map>
#include <format>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>
#endif

namespace dagforge::docker {

using json = JsonValue;

struct DockerClientConfig {
  std::chrono::milliseconds connect_timeout{30000};
  std::chrono::milliseconds read_timeout{300000};
  std::string api_version{"v1.43"};
};

struct ContainerConfig {
  std::string image;
  std::string command;
  std::string working_dir;
  std::flat_map<std::string, std::string> env;
};

struct CreateContainerResponse {
  std::string id;
  std::vector<std::string> warnings;
};

struct WaitContainerResponse {
  int status_code{0};
  std::string error;
};

struct ContainerLogs {
  std::string stdout_output;
  std::string stderr_output;
};

enum class DockerError : std::uint8_t {
  ConnectionFailed,
  ApiError,
  ContainerNotFound,
  ImageNotFound,
  Conflict,
  Timeout,
  ParseError,
  InvalidInput,
};

[[nodiscard]] constexpr auto to_string_view(DockerError error) noexcept
    -> std::string_view {
  switch (error) {
  case DockerError::ConnectionFailed:
    return "connection failed";
  case DockerError::ApiError:
    return "API error";
  case DockerError::ContainerNotFound:
    return "container not found";
  case DockerError::ImageNotFound:
    return "image not found";
  case DockerError::Conflict:
    return "conflict";
  case DockerError::Timeout:
    return "timeout";
  case DockerError::ParseError:
    return "parse error";
  case DockerError::InvalidInput:
    return "invalid input";
  }
  return "unknown error";
}

class DockerErrorCategory final : public std::error_category {
public:
  [[nodiscard]] auto name() const noexcept -> const char * override {
    return "dagforge.docker";
  }

  [[nodiscard]] auto message(int ev) const -> std::string override {
    return std::string{
        to_string_view(static_cast<DockerError>(static_cast<std::uint8_t>(ev)))};
  }

  using std::error_category::equivalent;

  [[nodiscard]] auto equivalent(int code,
                                const std::error_condition &condition) const noexcept
      -> bool override {
    if (condition.category() != std::generic_category()) {
      return false;
    }

    switch (static_cast<DockerError>(static_cast<std::uint8_t>(code))) {
    case DockerError::ConnectionFailed:
      return condition == std::errc::connection_refused ||
             condition == std::errc::not_connected;
    case DockerError::Timeout:
      return condition == std::errc::timed_out;
    case DockerError::InvalidInput:
      return condition == std::errc::invalid_argument;
    default:
      return false;
    }
  }
};

[[nodiscard]] inline auto docker_error_category() noexcept
    -> const DockerErrorCategory & {
  static const DockerErrorCategory instance;
  return instance;
}

[[nodiscard]] inline auto make_error_code(DockerError error) noexcept
    -> std::error_code {
  return {std::to_underlying(error), docker_error_category()};
}

namespace detail {
inline auto is_valid_container_id(std::string_view id) -> bool {
  if (id.empty() || id.size() > 64)
    return false;
  return std::ranges::all_of(
      id, [](char c) { return std::isalnum(c) || c == '_' || c == '-'; });
}

inline auto parse_json_body(const std::vector<std::uint8_t> &body)
    -> Result<json> {
  const auto *ptr = reinterpret_cast<const char *>(body.data());
  return parse_json(std::string_view(ptr, body.size()));
}

struct CreateContainerResponseJson {
  std::string Id;
  std::vector<std::string> Warnings;
};

struct WaitContainerErrorJson {
  std::string Message;
};

struct WaitContainerResponseJson {
  std::int64_t StatusCode{0};
  std::optional<WaitContainerErrorJson> Error;
};

template <typename T>
inline auto read_json_body(const std::vector<std::uint8_t> &body) -> Result<T> {
  const auto *ptr = reinterpret_cast<const char *>(body.data());
  return parse_json_as<T>(std::string_view{ptr, body.size()});
}
} // namespace detail

} // namespace dagforge::docker

template <>
struct std::is_error_code_enum<dagforge::docker::DockerError> : std::true_type {};

namespace glz {

template <> struct meta<dagforge::docker::detail::CreateContainerResponseJson> {
  using T = dagforge::docker::detail::CreateContainerResponseJson;
  static constexpr auto value = object("Id", &T::Id, "Warnings", &T::Warnings);
};

template <> struct meta<dagforge::docker::detail::WaitContainerErrorJson> {
  using T = dagforge::docker::detail::WaitContainerErrorJson;
  static constexpr auto value = object("Message", &T::Message);
};

template <> struct meta<dagforge::docker::detail::WaitContainerResponseJson> {
  using T = dagforge::docker::detail::WaitContainerResponseJson;
  static constexpr auto value =
      object("StatusCode", &T::StatusCode, "Error", &T::Error);
};

} // namespace glz

namespace dagforge::docker {

template <dagforge::http::HttpConnector Connector = dagforge::http::HttpClient>
class DockerClient {
public:
  DockerClient(io::IoContext &ctx, std::unique_ptr<Connector> client,
               DockerClientConfig config = {},
               std::string socket_path = "/var/run/docker.sock")
      : ctx_(&ctx), connector_(std::move(client)), config_(std::move(config)),
        socket_path_(std::move(socket_path)) {}

  ~DockerClient() = default;

  DockerClient(const DockerClient &) = delete;
  auto operator=(const DockerClient &) -> DockerClient & = delete;
  DockerClient(DockerClient &&) noexcept = default;
  auto operator=(DockerClient &&) noexcept -> DockerClient & = default;

  static auto connect(io::IoContext &ctx,
                      std::string socket_path = "/var/run/docker.sock",
                      DockerClientConfig config = {})
      -> task<Result<std::unique_ptr<DockerClient<Connector>>>> {
    http::HttpClientConfig http_config{
        .connect_timeout = config.connect_timeout,
        .read_timeout = config.read_timeout,
        .max_response_size = 100ULL * 1024ULL * 1024ULL,
        .keep_alive = true,
    };

    auto http_client_res =
        co_await Connector::connect_unix(ctx, socket_path, http_config);

    if (!http_client_res) {
      log::error("Failed to connect to Docker socket: {}", socket_path);
      co_return fail(make_error_code(DockerError::ConnectionFailed));
    }

    co_return std::make_unique<DockerClient<Connector>>(
        ctx, std::move(*http_client_res), std::move(config),
        std::move(socket_path));
  }

  auto create_container(ContainerConfig config, std::string name = {})
      -> task<Result<CreateContainerResponse>> {
    if (auto ready = co_await ensure_connected(); !ready) {
      co_return fail(ready.error());
    }
    json body;
    body["Image"] = config.image;
    body["AttachStdout"] = true;
    body["AttachStderr"] = true;
    body["Tty"] = false;

    if (!config.command.empty()) {
      body["Cmd"] = json::array_t{json("sh"), json("-c"), json(config.command)};
    }

    if (!config.working_dir.empty()) {
      body["WorkingDir"] = config.working_dir;
    }

    if (!config.env.empty()) {
      json env_array = json::array_t{};
      for (const auto &[key, value] : config.env) {
        if (!::dagforge::is_valid_env_key(key)) {
          log::error("Invalid environment variable key: {}", key);
          co_return fail(make_error_code(DockerError::InvalidInput));
        }
        env_array.get_array().push_back(std::format("{}={}", key, value));
      }
      body["Env"] = env_array;
    }

    std::string path =
        std::format("/{}/containers/create", config_.api_version);
    if (!name.empty()) {
      std::format_to(std::back_inserter(path), "?name={}",
                     dagforge::util::url_encode(name));
    }

    auto response_res = co_await connector_->post_json(path, dump_json(body));
    if (!response_res) {
      log::error("Failed to create container request: {}",
                 response_res.error().message());
      co_return fail(make_error_code(DockerError::ConnectionFailed));
    }
    auto &response = *response_res;

    if (response.status == http::HttpStatus::NotFound) {
      log::error("Docker image not found: {}", config.image);
      co_return fail(make_error_code(DockerError::ImageNotFound));
    }

    if (response.status == http::HttpStatus::Conflict) {
      log::error("Container name conflict: {}", name);
      co_return fail(make_error_code(DockerError::Conflict));
    }

    if (response.status != http::HttpStatus::Created) {
      log::error("Failed to create container: status={}",
                 static_cast<int>(response.status));
      co_return fail(make_error_code(DockerError::ApiError));
    }

    auto parsed = detail::read_json_body<detail::CreateContainerResponseJson>(
        response.body);
    if (!parsed) {
      log::error("Failed to parse create container response");
      co_return fail(make_error_code(DockerError::ParseError));
    }
    co_return CreateContainerResponse{
        .id = std::move(parsed->Id),
        .warnings = std::move(parsed->Warnings),
    };
  }

  auto pull_image(std::string image) -> task<Result<void>> {
    if (auto ready = co_await ensure_connected(); !ready) {
      co_return fail(ready.error());
    }
    if (image.empty()) {
      log::error("Empty image name");
      co_return fail(make_error_code(DockerError::InvalidInput));
    }

    std::string image_name{image};
    std::string tag = "latest";

    if (auto pos = image.rfind(':'); pos != std::string_view::npos) {
      if (image.find('/', pos) == std::string_view::npos) {
        image_name = std::string(image.substr(0, pos));
        tag = std::string(image.substr(pos + 1));
      }
    }

    std::string path =
        std::format("/{}/images/create?fromImage={}&tag={}",
                    config_.api_version, dagforge::util::url_encode(image_name),
                    dagforge::util::url_encode(tag));

    log::info("DockerClient: pulling image {}:{}", image_name, tag);

    auto response_res = co_await connector_->post(path, {});
    if (!response_res) {
      log::error("Failed to pull image {}:{}: {}", image_name, tag,
                 response_res.error().message());
      co_return fail(make_error_code(DockerError::ConnectionFailed));
    }
    auto &response = *response_res;

    if (response.status == http::HttpStatus::NotFound) {
      log::error("Image not found: {}:{}", image_name, tag);
      co_return fail(make_error_code(DockerError::ImageNotFound));
    }

    if (response.status != http::HttpStatus::Ok) {
      log::error("Failed to pull image {}:{}: status={}", image_name, tag,
                 static_cast<int>(response.status));
      co_return fail(make_error_code(DockerError::ApiError));
    }

    log::info("DockerClient: successfully pulled image {}:{}", image_name, tag);
    co_return ok();
  }

  auto start_container(std::string container_id)
      -> task<Result<void>> {
    if (auto ready = co_await ensure_connected(); !ready) {
      co_return fail(ready.error());
    }
    if (!detail::is_valid_container_id(container_id)) {
      log::error("Invalid container ID: {}", container_id);
      co_return fail(make_error_code(DockerError::InvalidInput));
    }

    std::string path = std::format("/{}/containers/{}/start",
                                   config_.api_version, container_id);
    auto response_res = co_await connector_->post(path, {});
    if (!response_res) {
      log::error("Failed to start container {}: {}", container_id,
                 response_res.error().message());
      co_return fail(make_error_code(DockerError::ConnectionFailed));
    }
    auto &response = *response_res;

    if (response.status == http::HttpStatus::NotFound) {
      log::error("Container not found: {}", container_id);
      co_return fail(make_error_code(DockerError::ContainerNotFound));
    }

    if (response.status != http::HttpStatus::NoContent &&
        response.status != http::HttpStatus::NotModified) {
      log::error("Failed to start container {}: status={}", container_id,
                 static_cast<int>(response.status));
      co_return fail(make_error_code(DockerError::ApiError));
    }

    co_return ok();
  }

  auto wait_container(std::string container_id)
      -> task<Result<WaitContainerResponse>> {
    if (auto ready = co_await ensure_connected(); !ready) {
      co_return fail(ready.error());
    }
    if (!detail::is_valid_container_id(container_id)) {
      log::error("Invalid container ID: {}", container_id);
      co_return fail(make_error_code(DockerError::InvalidInput));
    }

    std::string path = std::format("/{}/containers/{}/wait",
                                   config_.api_version, container_id);
    auto response_res = co_await connector_->post(path, {});
    if (!response_res) {
      log::error("Failed to wait on container {}: {}", container_id,
                 response_res.error().message());
      co_return fail(make_error_code(DockerError::ConnectionFailed));
    }
    auto &response = *response_res;

    if (response.status == http::HttpStatus::NotFound) {
      log::error("Container not found: {}", container_id);
      co_return fail(make_error_code(DockerError::ContainerNotFound));
    }

    if (response.status != http::HttpStatus::Ok) {
      log::error("Failed to wait for container {}: status={}", container_id,
                 static_cast<int>(response.status));
      co_return fail(make_error_code(DockerError::ApiError));
    }

    auto parsed = detail::read_json_body<detail::WaitContainerResponseJson>(
        response.body);
    if (!parsed) {
      log::error("Failed to parse wait container response");
      co_return fail(make_error_code(DockerError::ParseError));
    }
    co_return WaitContainerResponse{
        .status_code = static_cast<int>(parsed->StatusCode),
        .error =
            parsed->Error ? std::move(parsed->Error->Message) : std::string{},
    };
  }

  auto get_logs(std::string container_id)
      -> task<Result<ContainerLogs>> {
    if (auto ready = co_await ensure_connected(); !ready) {
      co_return fail(ready.error());
    }
    if (!detail::is_valid_container_id(container_id)) {
      log::error("Invalid container ID: {}", container_id);
      co_return fail(make_error_code(DockerError::InvalidInput));
    }

    std::string path =
        std::format("/{}/containers/{}/logs?stdout=true&stderr=true",
                    config_.api_version, container_id);
    auto response_res = co_await connector_->get(path);
    if (!response_res) {
      log::error("Failed to get logs for container {}: {}", container_id,
                 response_res.error().message());
      co_return fail(make_error_code(DockerError::ConnectionFailed));
    }
    auto &response = *response_res;

    if (response.status == http::HttpStatus::NotFound) {
      log::error("Container not found: {}", container_id);
      co_return fail(make_error_code(DockerError::ContainerNotFound));
    }

    if (response.status != http::HttpStatus::Ok) {
      log::error("Failed to get logs for container {}: status={}", container_id,
                 static_cast<int>(response.status));
      co_return fail(make_error_code(DockerError::ApiError));
    }

    std::string_view raw_logs(
        reinterpret_cast<const char *>(response.body.data()),
        response.body.size());
    co_return parse_log_stream(raw_logs);
  }

  auto stop_container(std::string container_id,
                      std::chrono::seconds timeout = std::chrono::seconds{10})
      -> task<Result<void>> {
    if (auto ready = co_await ensure_connected(); !ready) {
      co_return fail(ready.error());
    }
    if (!detail::is_valid_container_id(container_id)) {
      log::error("Invalid container ID: {}", container_id);
      co_return fail(make_error_code(DockerError::InvalidInput));
    }

    std::string path =
        std::format("/{}/containers/{}/stop?t={}", config_.api_version,
                    container_id, timeout.count());
    auto response_res = co_await connector_->post(path, {});
    if (!response_res) {
      log::error("Failed to stop container {}: {}", container_id,
                 response_res.error().message());
      co_return fail(make_error_code(DockerError::ConnectionFailed));
    }
    auto &response = *response_res;

    if (response.status == http::HttpStatus::NotFound) {
      log::error("Container not found: {}", container_id);
      co_return fail(make_error_code(DockerError::ContainerNotFound));
    }

    if (response.status != http::HttpStatus::NoContent &&
        response.status != http::HttpStatus::NotModified) {
      log::error("Failed to stop container {}: status={}", container_id,
                 static_cast<int>(response.status));
      co_return fail(make_error_code(DockerError::ApiError));
    }

    co_return ok();
  }

  auto remove_container(std::string container_id, bool force = false)
      -> task<Result<void>> {
    if (auto ready = co_await ensure_connected(); !ready) {
      co_return fail(ready.error());
    }
    if (!detail::is_valid_container_id(container_id)) {
      log::error("Invalid container ID: {}", container_id);
      co_return fail(make_error_code(DockerError::InvalidInput));
    }

    std::string path =
        std::format("/{}/containers/{}?force={}", config_.api_version,
                    container_id, force ? "true" : "false");
    auto response_res = co_await connector_->delete_(path);
    if (!response_res) {
      log::error("Failed to remove container {}: {}", container_id,
                 response_res.error().message());
      co_return fail(make_error_code(DockerError::ConnectionFailed));
    }
    auto &response = *response_res;

    if (response.status == http::HttpStatus::NotFound) {
      co_return ok();
    }

    if (response.status != http::HttpStatus::NoContent) {
      log::error("Failed to remove container {}: status={}", container_id,
                 static_cast<int>(response.status));
      co_return fail(make_error_code(DockerError::ApiError));
    }

    co_return ok();
  }

  [[nodiscard]] auto is_connected() const noexcept -> bool {
    return connector_ && connector_->is_connected();
  }

private:
  auto ensure_connected() -> task<Result<void>> {
    if (connector_ && connector_->is_connected()) {
      co_return ok();
    }

    http::HttpClientConfig http_config{
        .connect_timeout = config_.connect_timeout,
        .read_timeout = config_.read_timeout,
        .max_response_size = 100ULL * 1024ULL * 1024ULL,
        .keep_alive = true,
    };
    auto connector_res =
        co_await Connector::connect_unix(*ctx_, socket_path_, http_config);
    if (!connector_res) {
      log::error("Failed to reconnect to Docker socket: {}", socket_path_);
      co_return fail(make_error_code(DockerError::ConnectionFailed));
    }

    connector_ = std::move(*connector_res);
    co_return ok();
  }

  auto parse_log_stream(std::string_view raw_logs) -> ContainerLogs {
    ContainerLogs logs;
    std::size_t pos = 0;
    constexpr std::size_t kDockerLogHeaderSize = 8;
    while (pos + kDockerLogHeaderSize <= raw_logs.size()) {
      std::uint8_t stream_type = static_cast<std::uint8_t>(raw_logs[pos]);
      std::uint32_t size = 0;
      std::memcpy(&size, raw_logs.data() + pos + 4, sizeof(size));
      size = std::byteswap(size);
      pos += kDockerLogHeaderSize;
      if (pos + size > raw_logs.size())
        break;
      std::string_view payload = raw_logs.substr(pos, size);
      if (stream_type == 1)
        logs.stdout_output.append(payload);
      else if (stream_type == 2)
        logs.stderr_output.append(payload);
      pos += size;
    }
    return logs;
  }

  io::IoContext *ctx_;
  std::unique_ptr<Connector> connector_;
  DockerClientConfig config_;
  std::string socket_path_;
};

} // namespace dagforge::docker
