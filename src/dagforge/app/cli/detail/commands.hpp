#pragma once

#include <string>
#include <vector>

namespace dagforge::cli {

struct ServeOptions {
  std::string config_path{"system_config.json"};
};

struct ValidateOptions {
  std::string plan_path;
  std::string config_path;
};

struct RunOptions {
  std::string plan_path;
  std::string config_path{"system_config.json"};
};

struct ApiOptions {
  std::string method;
  std::string endpoint{"http://127.0.0.1:8888"};
  std::string path;
  std::string body;
  std::string content_type;
  std::vector<std::string> headers;
  std::string bearer_token;
  std::string tls_min_version{"1.2"};
  std::string tls_ca_file;
  std::string tls_client_cert_file;
  std::string tls_client_key_file;
  std::string output_path;
  bool include_headers{false};
};

[[nodiscard]] auto execute(const ServeOptions &options) -> int;
[[nodiscard]] auto execute(const ValidateOptions &options) -> int;
[[nodiscard]] auto execute(const RunOptions &options) -> int;
[[nodiscard]] auto execute(const ApiOptions &options) -> int;

} // namespace dagforge::cli
