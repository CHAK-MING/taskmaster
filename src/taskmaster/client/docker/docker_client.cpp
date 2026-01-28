#include "taskmaster/client/docker/docker_client.hpp"
#include "taskmaster/client/http/http_client.hpp"

#include "taskmaster/util/log.hpp"

#include <nlohmann/json.hpp>

#include <bit>
#include <cctype>
#include <cstdint>
#include <format>

namespace taskmaster::docker {

using json = nlohmann::json;

namespace {

constexpr std::size_t kDockerLogHeaderSize = 8;
constexpr std::uint8_t kStdoutStream = 1;
constexpr std::uint8_t kStderrStream = 2;

auto url_encode(std::string_view input) -> std::string {
  std::string result;
  result.reserve(input.size() * 3);  // Worst case: each char becomes %XX
  for (char c : input) {
    if (std::isalnum(static_cast<unsigned char>(c)) || c == '-' || c == '_' ||
        c == '.' || c == '~') {
      result += c;
    } else {
      std::format_to(std::back_inserter(result), "%{:02X}", 
                     static_cast<unsigned char>(c));
    }
  }
  return result;
}

auto is_valid_container_id(std::string_view id) -> bool {
  if (id.empty() || id.size() > 64) {
    return false;
  }
  for (char c : id) {
    if (!std::isalnum(static_cast<unsigned char>(c))) {
      return false;
    }
  }
  return true;
}

auto is_valid_env_key(std::string_view key) -> bool {
  if (key.empty()) {
    return false;
  }
  for (char c : key) {
    if (!std::isalnum(static_cast<unsigned char>(c)) && c != '_') {
      return false;
    }
  }
  return true;
}

}  // namespace

struct DockerClient::Impl {
  io::IoContext* ctx{nullptr};
  std::unique_ptr<http::HttpClient> http_client;
  DockerClientConfig config;

  Impl(io::IoContext& context, std::unique_ptr<http::HttpClient> client,
       DockerClientConfig cfg)
      : ctx(&context),
        http_client(std::move(client)),
        config(std::move(cfg)) {}
};

DockerClient::DockerClient(io::IoContext& ctx,
                           std::unique_ptr<http::HttpClient> client,
                           DockerClientConfig config)
    : impl_(std::make_unique<Impl>(ctx, std::move(client), std::move(config))) {
}

DockerClient::~DockerClient() = default;

DockerClient::DockerClient(DockerClient&&) noexcept = default;
auto DockerClient::operator=(DockerClient&&) noexcept
    -> DockerClient& = default;

auto DockerClient::connect(io::IoContext& ctx, std::string_view socket_path,
                           DockerClientConfig config)
    -> task<DockerResult<std::unique_ptr<DockerClient>>> {
  http::HttpClientConfig http_config{
      .connect_timeout = config.connect_timeout,
      .read_timeout = config.read_timeout,
      .max_response_size = 100 * 1024 * 1024,
      .keep_alive = true,
  };

  auto http_client =
      co_await http::HttpClient::connect_unix(ctx, socket_path, http_config);

  if (!http_client) {
    log::error("Failed to connect to Docker socket: {}", socket_path);
    co_return std::unexpected(DockerError::ConnectionFailed);
  }

  co_return std::make_unique<DockerClient>(ctx, std::move(http_client),
                                           std::move(config));
}

auto DockerClient::create_container(const ContainerConfig& config,
                                    std::string_view name)
    -> task<DockerResult<CreateContainerResponse>> {
  json body;
  body["Image"] = config.image;
  body["AttachStdout"] = true;
  body["AttachStderr"] = true;
  body["Tty"] = false;

  if (!config.command.empty()) {
    body["Cmd"] = json::array({"sh", "-c", config.command});
  }

  if (!config.working_dir.empty()) {
    body["WorkingDir"] = config.working_dir;
  }

  if (!config.env.empty()) {
    json env_array = json::array();
    for (const auto& [key, value] : config.env) {
      if (!is_valid_env_key(key)) {
        log::error("Invalid environment variable key: {}", key);
        co_return std::unexpected(DockerError::InvalidInput);
      }
      env_array.push_back(std::format("{}={}", key, value));
    }
    body["Env"] = env_array;
  }

  std::string path =
      std::format("/{}/containers/create", impl_->config.api_version);
  if (!name.empty()) {
    std::format_to(std::back_inserter(path), "?name={}", url_encode(name));
  }

  auto response =
      co_await impl_->http_client->post_json(path, body.dump());

  if (response.status == http::HttpStatus::NotFound) {
    log::error("Docker image not found: {}", config.image);
    co_return std::unexpected(DockerError::ImageNotFound);
  }

  if (response.status == http::HttpStatus::Conflict) {
    log::error("Container name conflict: {}", name);
    co_return std::unexpected(DockerError::Conflict);
  }

  if (response.status != http::HttpStatus::Created) {
    log::error("Failed to create container: status={}",
               static_cast<int>(response.status));
    co_return std::unexpected(DockerError::ApiError);
  }

  try {
    auto json_body =
        json::parse(response.body.begin(), response.body.end());
    CreateContainerResponse result;
    result.id = json_body.value("Id", "");
    if (json_body.contains("Warnings") && json_body["Warnings"].is_array()) {
      for (const auto& w : json_body["Warnings"]) {
        result.warnings.push_back(w.get<std::string>());
      }
    }
    co_return result;
  } catch (const json::exception& e) {
    log::error("Failed to parse create container response: {}", e.what());
    co_return std::unexpected(DockerError::ParseError);
  }
}

auto DockerClient::pull_image(std::string_view image)
    -> task<DockerResult<void>> {
  if (image.empty()) {
    log::error("Empty image name");
    co_return std::unexpected(DockerError::InvalidInput);
  }

  std::string image_name{image};
  std::string tag = "latest";

  if (auto pos = image.rfind(':'); pos != std::string_view::npos) {
    if (image.find('/', pos) == std::string_view::npos) {
      image_name = std::string(image.substr(0, pos));
      tag = std::string(image.substr(pos + 1));
    }
  }

  std::string path = std::format("/{}/images/create?fromImage={}&tag={}",
                                 impl_->config.api_version,
                                 url_encode(image_name), url_encode(tag));

  log::info("DockerClient: pulling image {}:{}", image_name, tag);

  auto response = co_await impl_->http_client->post(path, {});

  if (response.status == http::HttpStatus::NotFound) {
    log::error("Image not found: {}:{}", image_name, tag);
    co_return std::unexpected(DockerError::ImageNotFound);
  }

  if (response.status != http::HttpStatus::Ok) {
    log::error("Failed to pull image {}:{}: status={}", image_name, tag,
               static_cast<int>(response.status));
    co_return std::unexpected(DockerError::ApiError);
  }

  log::info("DockerClient: successfully pulled image {}:{}", image_name, tag);
  co_return DockerResult<void>{};
}

auto DockerClient::start_container(std::string_view container_id)
    -> task<DockerResult<void>> {
  if (!is_valid_container_id(container_id)) {
    log::error("Invalid container ID: {}", container_id);
    co_return std::unexpected(DockerError::InvalidInput);
  }

  std::string path = std::format("/{}/containers/{}/start",
                                 impl_->config.api_version, container_id);

  auto response =
      co_await impl_->http_client->post(path, {});

  if (response.status == http::HttpStatus::NotFound) {
    log::error("Container not found: {}", container_id);
    co_return std::unexpected(DockerError::ContainerNotFound);
  }

  if (response.status != http::HttpStatus::NoContent &&
      response.status != http::HttpStatus::NotModified) {
    log::error("Failed to start container {}: status={}", container_id,
               static_cast<int>(response.status));
    co_return std::unexpected(DockerError::ApiError);
  }

  co_return DockerResult<void>{};
}

auto DockerClient::wait_container(std::string_view container_id)
    -> task<DockerResult<WaitContainerResponse>> {
  if (!is_valid_container_id(container_id)) {
    log::error("Invalid container ID: {}", container_id);
    co_return std::unexpected(DockerError::InvalidInput);
  }

  std::string path = std::format("/{}/containers/{}/wait",
                                 impl_->config.api_version, container_id);

  auto response =
      co_await impl_->http_client->post(path, {});

  if (response.status == http::HttpStatus::NotFound) {
    log::error("Container not found: {}", container_id);
    co_return std::unexpected(DockerError::ContainerNotFound);
  }

  if (response.status != http::HttpStatus::Ok) {
    log::error("Failed to wait for container {}: status={}", container_id,
               static_cast<int>(response.status));
    co_return std::unexpected(DockerError::ApiError);
  }

  try {
    auto json_body =
        json::parse(response.body.begin(), response.body.end());
    WaitContainerResponse result;
    result.status_code = json_body.value("StatusCode", 0);
    if (json_body.contains("Error") && !json_body["Error"].is_null()) {
      result.error = json_body["Error"].value("Message", "");
    }
    co_return result;
  } catch (const json::exception& e) {
    log::error("Failed to parse wait container response: {}", e.what());
    co_return std::unexpected(DockerError::ParseError);
  }
}

auto DockerClient::get_logs(std::string_view container_id)
    -> task<DockerResult<ContainerLogs>> {
  if (!is_valid_container_id(container_id)) {
    log::error("Invalid container ID: {}", container_id);
    co_return std::unexpected(DockerError::InvalidInput);
  }

  std::string path =
      std::format("/{}/containers/{}/logs?stdout=true&stderr=true",
                  impl_->config.api_version, container_id);

  auto response = co_await impl_->http_client->get(path);

  if (response.status == http::HttpStatus::NotFound) {
    log::error("Container not found: {}", container_id);
    co_return std::unexpected(DockerError::ContainerNotFound);
  }

  if (response.status != http::HttpStatus::Ok) {
    log::error("Failed to get logs for container {}: status={}", container_id,
               static_cast<int>(response.status));
    co_return std::unexpected(DockerError::ApiError);
  }

  std::string_view raw_logs(
      reinterpret_cast<const char*>(response.body.data()), response.body.size());
  co_return parse_log_stream(raw_logs);
}

auto DockerClient::stop_container(std::string_view container_id,
                                  std::chrono::seconds timeout)
    -> task<DockerResult<void>> {
  if (!is_valid_container_id(container_id)) {
    log::error("Invalid container ID: {}", container_id);
    co_return std::unexpected(DockerError::InvalidInput);
  }

  std::string path =
      std::format("/{}/containers/{}/stop?t={}", impl_->config.api_version,
                  container_id, timeout.count());

  auto response =
      co_await impl_->http_client->post(path, {});

  if (response.status == http::HttpStatus::NotFound) {
    log::error("Container not found: {}", container_id);
    co_return std::unexpected(DockerError::ContainerNotFound);
  }

  if (response.status != http::HttpStatus::NoContent &&
      response.status != http::HttpStatus::NotModified) {
    log::error("Failed to stop container {}: status={}", container_id,
               static_cast<int>(response.status));
    co_return std::unexpected(DockerError::ApiError);
  }

  co_return DockerResult<void>{};
}

auto DockerClient::remove_container(std::string_view container_id, bool force)
    -> task<DockerResult<void>> {
  if (!is_valid_container_id(container_id)) {
    log::error("Invalid container ID: {}", container_id);
    co_return std::unexpected(DockerError::InvalidInput);
  }

  std::string path = std::format("/{}/containers/{}?force={}",
                                 impl_->config.api_version, container_id,
                                 force ? "true" : "false");

  auto response = co_await impl_->http_client->delete_(path);

  if (response.status == http::HttpStatus::NotFound) {
    co_return DockerResult<void>{};
  }

  if (response.status != http::HttpStatus::NoContent) {
    log::error("Failed to remove container {}: status={}", container_id,
               static_cast<int>(response.status));
    co_return std::unexpected(DockerError::ApiError);
  }

  co_return DockerResult<void>{};
}

auto DockerClient::is_connected() const noexcept -> bool {
  return impl_ && impl_->http_client && impl_->http_client->is_connected();
}

auto DockerClient::parse_log_stream(std::string_view raw_logs)
    -> ContainerLogs {
  ContainerLogs logs;
  std::size_t pos = 0;

  while (pos + kDockerLogHeaderSize <= raw_logs.size()) {
    std::uint8_t stream_type = static_cast<std::uint8_t>(raw_logs[pos]);

    std::uint32_t size = 0;
    std::memcpy(&size, raw_logs.data() + pos + 4, sizeof(size));
    size = std::byteswap(size);

    pos += kDockerLogHeaderSize;

    if (pos + size > raw_logs.size()) {
      break;
    }

    std::string_view payload = raw_logs.substr(pos, size);

    if (stream_type == kStdoutStream) {
      logs.stdout_output.append(payload);
    } else if (stream_type == kStderrStream) {
      logs.stderr_output.append(payload);
    }

    pos += size;
  }

  return logs;
}

}  // namespace taskmaster::docker
