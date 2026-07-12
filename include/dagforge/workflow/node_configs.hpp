#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/workflow/workflow_types.hpp"

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>
#endif

namespace dagforge::workflow {

struct KeyValue {
  std::string key;
  std::string value;
};

struct ShellNodeConfig {
  std::string command;
  std::string working_dir;
  std::vector<KeyValue> env;
};

struct DockerNodeConfig {
  std::string image;
  std::string command;
  std::string working_dir;
  std::vector<KeyValue> env;
  std::string docker_socket{"/var/run/docker.sock"};
};

struct LuaNodeConfig {
  std::string script;
  std::string script_file;
  std::uint64_t max_instructions{100'000};
  std::uint64_t max_memory_bytes{8ULL * 1024ULL * 1024ULL};
};

struct HttpNodeConfig {
  std::string url;
  std::string method{"GET"};
  std::vector<KeyValue> headers;
  std::string body;
  std::string body_input;
  int expected_status{0};
};

struct ModelNodeConfig {
  std::string provider;
  std::string model;
  std::string system_prompt;
  std::string prompt;
  std::string prompt_input;
  CredentialRef credential;
  std::optional<JsonValue> response_schema;
  std::uint64_t max_output_tokens{4096};
  double temperature{0.0};
};

struct ToolNodeConfig {
  std::string tool;
  JsonValue arguments;
  std::string arguments_input;
  CredentialRef credential;
};

struct ComputeNodeConfig {
  std::string operation{"identity"};
  std::vector<std::string> input_order;
  std::string separator;
};

struct EvaluatorNodeConfig {
  std::string operation{"truthy"};
  std::string input;
  std::string expected;
  double minimum_score{1.0};
};

struct ApprovalNodeConfig {
  std::string summary;
  int expires_after_sec{24 * 60 * 60};
};

struct ModelCall {
  WorkflowRunId run_id;
  WorkflowNodeId node_id;
  std::string provider;
  std::string model;
  MessageList messages;
  std::optional<JsonValue> response_schema;
  std::uint64_t max_output_tokens{4096};
  double temperature{0.0};
  CredentialRef credential;
  std::chrono::steady_clock::time_point deadline{};
};

struct ToolInvocation {
  WorkflowRunId run_id;
  WorkflowNodeId node_id;
  std::string tool;
  JsonValue arguments;
  CredentialRef credential;
  std::chrono::steady_clock::time_point deadline{};
};

} // namespace dagforge::workflow

namespace glz {
template <> struct meta<dagforge::workflow::KeyValue> {
  using T = dagforge::workflow::KeyValue;
  static constexpr auto value = object("key", &T::key, "value", &T::value);
};

template <> struct meta<dagforge::workflow::ShellNodeConfig> {
  using T = dagforge::workflow::ShellNodeConfig;
  static constexpr auto value = object("command", &T::command, "working_dir",
                                       &T::working_dir, "env", &T::env);
};

template <> struct meta<dagforge::workflow::DockerNodeConfig> {
  using T = dagforge::workflow::DockerNodeConfig;
  static constexpr auto value = object(
      "image", &T::image, "command", &T::command, "working_dir",
      &T::working_dir, "env", &T::env, "docker_socket", &T::docker_socket);
};

template <> struct meta<dagforge::workflow::LuaNodeConfig> {
  using T = dagforge::workflow::LuaNodeConfig;
  static constexpr auto value = object(
      "script", &T::script, "script_file", &T::script_file,
      "max_instructions", &T::max_instructions, "max_memory_bytes",
      &T::max_memory_bytes);
};

template <> struct meta<dagforge::workflow::HttpNodeConfig> {
  using T = dagforge::workflow::HttpNodeConfig;
  static constexpr auto value = object(
      "url", &T::url, "method", &T::method, "headers", &T::headers, "body",
      &T::body, "body_input", &T::body_input, "expected_status",
      &T::expected_status);
};

template <> struct meta<dagforge::workflow::ModelNodeConfig> {
  using T = dagforge::workflow::ModelNodeConfig;
  static constexpr auto value = object(
      "provider", &T::provider, "model", &T::model, "system_prompt",
      &T::system_prompt, "prompt", &T::prompt, "prompt_input",
      &T::prompt_input, "credential", &T::credential, "response_schema",
      &T::response_schema, "max_output_tokens", &T::max_output_tokens,
      "temperature", &T::temperature);
};

template <> struct meta<dagforge::workflow::ToolNodeConfig> {
  using T = dagforge::workflow::ToolNodeConfig;
  static constexpr auto value = object(
      "tool", &T::tool, "arguments", &T::arguments, "arguments_input",
      &T::arguments_input, "credential", &T::credential);
};

template <> struct meta<dagforge::workflow::ComputeNodeConfig> {
  using T = dagforge::workflow::ComputeNodeConfig;
  static constexpr auto value = object(
      "operation", &T::operation, "input_order", &T::input_order,
      "separator", &T::separator);
};

template <> struct meta<dagforge::workflow::EvaluatorNodeConfig> {
  using T = dagforge::workflow::EvaluatorNodeConfig;
  static constexpr auto value = object(
      "operation", &T::operation, "input", &T::input, "expected",
      &T::expected, "minimum_score", &T::minimum_score);
};

template <> struct meta<dagforge::workflow::ApprovalNodeConfig> {
  using T = dagforge::workflow::ApprovalNodeConfig;
  static constexpr auto value = object("summary", &T::summary,
                                       "expires_after_sec",
                                       &T::expires_after_sec);
};

template <> struct meta<dagforge::workflow::CredentialRef> {
  using T = dagforge::workflow::CredentialRef;
  static constexpr auto value = object("name", &T::name);
};
} // namespace glz
