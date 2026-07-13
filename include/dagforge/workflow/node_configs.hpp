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

struct CommandNodeConfig {
  std::string program;
  std::vector<std::string> arguments;
  std::vector<KeyValue> env;
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

template <> struct meta<dagforge::workflow::CommandNodeConfig> {
  using T = dagforge::workflow::CommandNodeConfig;
  static constexpr auto value = object(
      "program", &T::program, "arguments", &T::arguments, "env", &T::env);
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

template <> struct meta<dagforge::workflow::CredentialRef> {
  using T = dagforge::workflow::CredentialRef;
  static constexpr auto value = object("name", &T::name);
};
} // namespace glz
