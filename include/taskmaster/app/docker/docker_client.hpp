#pragma once

#include "taskmaster/app/http/http_client.hpp"
#include "taskmaster/core/coroutine.hpp"
#include "taskmaster/io/context.hpp"

#include <chrono>
#include <cstdint>
#include <expected>
#include <flat_map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace taskmaster::docker {

struct DockerClientConfig {
  std::chrono::milliseconds connect_timeout{30000};
  std::chrono::milliseconds read_timeout{300000};  // 5 min for long running
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

enum class DockerError {
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

template <typename T>
using DockerResult = std::expected<T, DockerError>;

class DockerClient {
public:
  DockerClient(io::IoContext& ctx, std::unique_ptr<http::HttpClient> client,
               DockerClientConfig config = {});
  ~DockerClient();

  DockerClient(const DockerClient&) = delete;
  auto operator=(const DockerClient&) -> DockerClient& = delete;
  DockerClient(DockerClient&&) noexcept;
  auto operator=(DockerClient&&) noexcept -> DockerClient&;

  static auto connect(io::IoContext& ctx,
                      std::string_view socket_path = "/var/run/docker.sock",
                      DockerClientConfig config = {})
      -> task<DockerResult<std::unique_ptr<DockerClient>>>;

  auto create_container(const ContainerConfig& config,
                        std::string_view name = "")
      -> task<DockerResult<CreateContainerResponse>>;

  auto pull_image(std::string_view image) -> task<DockerResult<void>>;

  auto start_container(std::string_view container_id)
      -> task<DockerResult<void>>;

  auto wait_container(std::string_view container_id)
      -> task<DockerResult<WaitContainerResponse>>;

  auto get_logs(std::string_view container_id) -> task<DockerResult<ContainerLogs>>;

  auto stop_container(std::string_view container_id,
                      std::chrono::seconds timeout = std::chrono::seconds{10})
      -> task<DockerResult<void>>;

  auto remove_container(std::string_view container_id, bool force = false)
      -> task<DockerResult<void>>;

  [[nodiscard]] auto is_connected() const noexcept -> bool;

private:
  auto parse_log_stream(std::string_view raw_logs) -> ContainerLogs;

  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace taskmaster::docker
