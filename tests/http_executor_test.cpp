#include "../src/dagforge/executors/http/detail/egress_policy.hpp"

#include "dagforge/core/runtime.hpp"
#include "dagforge/executors/http/executor.hpp"
#include "dagforge/http/http_server.hpp"
#include "dagforge/http/router.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/workflow/task_executor.hpp"

#include "test_utils.hpp"

#include <gtest/gtest.h>

#include <boost/asio/ip/address.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <string_view>
#include <thread>

namespace dagforge::executors::http::test {
namespace {

auto base_config() -> config::HttpEgressConfig {
  config::HttpEgressConfig config;
  config.allowed_origins = {"https://example.com"};
  return config;
}

auto make_address(std::string_view text) -> boost::asio::ip::address {
  return boost::asio::ip::make_address(std::string{text});
}

auto executor_config() -> config::HttpEgressConfig {
  auto config = base_config();
  config.allow_plaintext = true;
  config.deny_private_networks = false;
  config.allowed_origins = {"http://127.0.0.1:8080"};
  return config;
}

auto json(std::string_view text) -> JsonValue {
  auto parsed = parse_json(text);
  EXPECT_TRUE(parsed.has_value()) << parsed.error().message();
  return parsed ? std::move(*parsed) : JsonValue{};
}

auto compile_context(std::span<const workflow::InputBinding> inputs,
                     std::span<const WorkflowPortId> outputs)
    -> workflow::ExecutorCompileContext {
  return {.inputs = inputs, .outputs = outputs};
}

[[nodiscard]] auto execute_on_shard(
    Runtime &runtime, const std::shared_ptr<workflow::ITaskExecutor> &executor,
    workflow::TaskExecutionRequest request,
    std::chrono::milliseconds timeout = std::chrono::seconds(3))
    -> Result<workflow::ExecutorOutputs> {
  auto completion =
      std::make_shared<std::promise<Result<workflow::ExecutorOutputs>>>();
  auto future = completion->get_future();
  runtime.post_to(
      0, [executor, request = std::move(request), completion]() mutable {
        workflow::TaskExecutionSink sink{
            .on_complete =
                [completion](const InstanceId &,
                             Result<workflow::ExecutorOutputs> result) mutable {
                  completion->set_value(std::move(result));
                },
        };
        auto started = executor->start(std::move(request), std::move(sink));
        if (!started) {
          completion->set_value(fail(started.error()));
        }
      });
  if (future.wait_for(timeout) != std::future_status::ready) {
    return fail(Error::Timeout);
  }
  return future.get();
}

[[nodiscard]] auto output_value(const workflow::ExecutorOutputs &outputs,
                                std::string_view port)
    -> const workflow::WorkflowValue * {
  const auto found = std::ranges::find_if(outputs, [&](const auto &entry) {
    return entry.first == port;
  });
  return found == outputs.end() ? nullptr : &found->second;
}

} // namespace

TEST(HttpEgressPolicyTest, CanonicalizesAndAuthorizesExactOrigins) {
  auto config = base_config();
  config.allowed_origins = {"HTTPS://Example.COM", "https://[2001:db8::1]:8443"};
  auto policy = detail::HttpEgressPolicy::create(std::move(config));
  ASSERT_TRUE(policy.has_value()) << policy.error().message();

  auto target = policy->authorize(
      "https://EXAMPLE.com/path/to/resource?mode=fast&limit=2");
  ASSERT_TRUE(target.has_value()) << target.error().message();
  EXPECT_TRUE(target->tls);
  EXPECT_EQ(target->host, "example.com");
  EXPECT_EQ(target->port, 443);
  EXPECT_EQ(target->origin, "https://example.com:443");
  EXPECT_EQ(target->host_header, "example.com");
  EXPECT_EQ(target->target, "/path/to/resource?mode=fast&limit=2");

  auto ipv6 = policy->authorize("https://[2001:db8::1]:8443/");
  ASSERT_TRUE(ipv6.has_value()) << ipv6.error().message();
  EXPECT_EQ(ipv6->host_header, "[2001:db8::1]:8443");
  EXPECT_EQ(ipv6->origin, "https://[2001:db8::1]:8443");

  EXPECT_EQ(policy->authorize("https://example.com:444/").error(),
            make_error_code(Error::Unauthorized));
}

TEST(HttpEgressPolicyTest, RejectsUnsafeOrAmbiguousOrigins) {
  for (std::string_view origin : {
           "example.com",
           "ftp://example.com",
           "https://user@example.com",
           "https://example.com/#fragment",
           "https://example.com/path",
           "https://example.com:0",
       }) {
    auto config = base_config();
    config.allowed_origins = {std::string{origin}};
    EXPECT_FALSE(detail::HttpEgressPolicy::create(std::move(config)).has_value())
        << origin;
  }

  auto duplicate = base_config();
  duplicate.allowed_origins = {"https://example.com",
                               "HTTPS://EXAMPLE.COM:443"};
  auto duplicate_policy =
      detail::HttpEgressPolicy::create(std::move(duplicate));
  ASSERT_FALSE(duplicate_policy.has_value());
  EXPECT_EQ(duplicate_policy.error(), make_error_code(Error::InvalidArgument));

  auto plaintext = base_config();
  plaintext.allowed_origins = {"http://example.com"};
  auto plaintext_policy =
      detail::HttpEgressPolicy::create(std::move(plaintext));
  ASSERT_FALSE(plaintext_policy.has_value());
  EXPECT_EQ(plaintext_policy.error(), make_error_code(Error::Unauthorized));
}

TEST(HttpEgressPolicyTest, PlaintextRequiresBothServerOptInAndExactOrigin) {
  auto config = base_config();
  config.allow_plaintext = true;
  config.allowed_origins = {"http://example.com:8080"};
  auto policy = detail::HttpEgressPolicy::create(std::move(config));
  ASSERT_TRUE(policy.has_value()) << policy.error().message();

  auto allowed = policy->authorize("http://example.com:8080/v1");
  ASSERT_TRUE(allowed.has_value()) << allowed.error().message();
  EXPECT_FALSE(allowed->tls);
  EXPECT_EQ(allowed->port, 8080);
  EXPECT_EQ(allowed->host_header, "example.com:8080");

  EXPECT_EQ(policy->authorize("http://example.com/v1").error(),
            make_error_code(Error::Unauthorized));
  EXPECT_EQ(policy->authorize("https://example.com:8080/v1").error(),
            make_error_code(Error::Unauthorized));
}

TEST(HttpEgressPolicyTest, ValidatesCidrSyntaxAndPrefixLength) {
  for (std::string_view cidr : {
           "127.0.0.1",
           "/8",
           "127.0.0.1/",
           "not-an-address/24",
           "127.0.0.1/x",
           "127.0.0.1/33",
           "2001:db8::/129",
       }) {
    auto config = base_config();
    config.allowed_ip_cidrs = {std::string{cidr}};
    auto policy = detail::HttpEgressPolicy::create(std::move(config));
    ASSERT_FALSE(policy.has_value()) << cidr;
    EXPECT_EQ(policy.error(), make_error_code(Error::InvalidArgument)) << cidr;
  }
}

TEST(HttpEgressPolicyTest, AppliesSpecialAddressDenialAndExplicitExceptions) {
  auto config = base_config();
  config.allowed_ip_cidrs = {"127.0.0.0/8", "fd00::/8"};
  auto policy = detail::HttpEgressPolicy::create(std::move(config));
  ASSERT_TRUE(policy.has_value()) << policy.error().message();

  EXPECT_TRUE(policy->address_allowed(make_address("127.0.0.42")));
  EXPECT_TRUE(policy->address_allowed(make_address("fd12::1")));
  EXPECT_TRUE(policy->address_allowed(make_address("8.8.8.8")));
  EXPECT_TRUE(policy->address_allowed(make_address("2606:4700:4700::1111")));

  for (std::string_view address : {
           "0.0.0.0",       "10.0.0.1",      "100.64.0.1",
           "169.254.1.1",   "172.16.0.1",    "192.0.2.1",
           "192.168.1.1",   "198.18.0.1",    "198.51.100.1",
           "203.0.113.1",   "224.0.0.1",     "240.0.0.1",
           "::",            "::1",           "fe80::1",
           "fec0::1",       "2001:db8::1",   "ff02::1",
           "::ffff:10.0.0.1",
       }) {
    EXPECT_FALSE(policy->address_allowed(make_address(address))) << address;
  }
}

TEST(HttpEgressPolicyTest, CanPermitPrivateNetworksGlobally) {
  auto config = base_config();
  config.deny_private_networks = false;
  auto policy = detail::HttpEgressPolicy::create(std::move(config));
  ASSERT_TRUE(policy.has_value()) << policy.error().message();
  EXPECT_TRUE(policy->address_allowed(make_address("10.0.0.1")));
  EXPECT_TRUE(policy->address_allowed(make_address("::1")));
}

TEST(HttpTaskExecutorTest, CompilesSupportedMethodsAndCanonicalizesStatusList) {
  Runtime runtime(1);
  auto executor = create_task_executor(runtime, executor_config());
  ASSERT_TRUE(executor.has_value()) << executor.error().message();
  EXPECT_EQ((*executor)->type(), "http");

  const std::array inputs{
      workflow::InputBinding{
          .input = WorkflowPortId{"payload"},
          .source = workflow::OutputRef{
              .node_id = WorkflowNodeId{"upstream"},
              .port = WorkflowPortId{"result"},
          },
      },
      workflow::InputBinding{
          .input = WorkflowPortId{"trace"},
          .source = workflow::OutputRef{
              .node_id = WorkflowNodeId{"upstream"},
              .port = WorkflowPortId{"trace"},
          },
      },
  };
  const std::array outputs{
      WorkflowPortId{"status"}, WorkflowPortId{"body"},
      WorkflowPortId{"headers"}, WorkflowPortId{"result"}};

  for (std::string_view method : {"GET", "POST", "PUT", "PATCH", "DELETE",
                                  "OPTIONS", "HEAD"}) {
    auto config = json(std::format(
        R"({{"method":"{}","url":"http://127.0.0.1:8080/path?x=1","headers":[{{"name":"X-Static","value":"value"}}],"input_headers":[{{"input":"trace","header":"X-Trace"}}],"accepted_statuses":[204,200]}})",
        method));
    if (method != "GET" && method != "HEAD") {
      config["body_input"] = "payload";
    }
    auto compiled = (*executor)->compile(
        std::move(config), compile_context(inputs, outputs));
    ASSERT_TRUE(compiled.has_value())
        << method << ": " << compiled.error().message();
    const auto &statuses = (*compiled)["accepted_statuses"].get_array();
    ASSERT_EQ(statuses.size(), 2U);
    EXPECT_EQ(statuses[0].as<std::int64_t>(), 200);
    EXPECT_EQ(statuses[1].as<std::int64_t>(), 204);
  }
}

TEST(HttpTaskExecutorTest, RejectsInvalidNodeContractsAndResourceOverruns) {
  Runtime runtime(1);
  auto limits = executor_config();
  limits.max_request_headers = 2;
  limits.max_request_header_bytes = 24;
  limits.max_request_body_bytes = 8;
  auto executor = create_task_executor(runtime, limits);
  ASSERT_TRUE(executor.has_value()) << executor.error().message();

  const std::array inputs{
      workflow::InputBinding{
          .input = WorkflowPortId{"payload"},
          .source = workflow::OutputRef{
              .node_id = WorkflowNodeId{"upstream"},
              .port = WorkflowPortId{"result"},
          },
      },
  };
  const std::array valid_outputs{WorkflowPortId{"result"}};
  const auto context = compile_context(inputs, valid_outputs);

  const auto expect_error = [&](std::string_view text, Error expected,
                                workflow::ExecutorCompileContext ctx) {
    auto compiled = (*executor)->compile(json(text), ctx);
    ASSERT_FALSE(compiled.has_value()) << text;
    EXPECT_EQ(compiled.error(), make_error_code(expected)) << text;
  };
  const auto expect_default_error = [&](std::string_view text, Error expected) {
    expect_error(text, expected, context);
  };

  expect_default_error(
      R"({"method":"TRACE","url":"http://127.0.0.1:8080/","headers":[],"input_headers":[],"accepted_statuses":[]})",
      Error::InvalidArgument);
  expect_default_error(
      R"({"method":"POST","url":"http://127.0.0.1:8080/","headers":[],"input_headers":[],"body":"x","body_input":"payload","accepted_statuses":[]})",
      Error::InvalidArgument);
  expect_default_error(
      R"({"method":"GET","url":"http://127.0.0.1:8080/","headers":[],"input_headers":[],"body":"x","accepted_statuses":[]})",
      Error::InvalidArgument);
  expect_default_error(
      R"({"method":"POST","url":"http://127.0.0.1:8080/","headers":[],"input_headers":[],"body":"123456789","accepted_statuses":[]})",
      Error::ResourceExhausted);
  expect_default_error(
      R"({"method":"POST","url":"http://127.0.0.1:8080/","headers":[],"input_headers":[],"body_input":"missing","accepted_statuses":[]})",
      Error::InvalidArgument);
  expect_default_error(
      R"({"method":"POST","url":"http://127.0.0.1:8080/","headers":[{"name":"X-A","value":"1"},{"name":"X-B","value":"2"},{"name":"X-C","value":"3"}],"input_headers":[],"accepted_statuses":[]})",
      Error::ResourceExhausted);

  for (std::string_view headers : {
           R"([{"name":"Bad Header","value":"x"}])",
           R"([{"name":"X-Test","value":"bad\r\nvalue"}])",
           R"([{"name":"Host","value":"example.com"}])",
           R"([{"name":"X-Test","value":"a"},{"name":"x-test","value":"b"}])",
           R"([{"name":"Long-Header","value":"long-value"}])",
       }) {
    expect_default_error(
        std::format(
            R"({{"method":"POST","url":"http://127.0.0.1:8080/","headers":{},"input_headers":[],"accepted_statuses":[]}})",
            headers),
        headers.contains("Long-Header") ? Error::ResourceExhausted
                                         : Error::InvalidArgument);
  }

  for (std::string_view bindings : {
           R"([{"input":"","header":"X-Test"}])",
           R"([{"input":"missing","header":"X-Test"}])",
           R"([{"input":"payload","header":"Host"}])",
           R"([{"input":"payload","header":"X-Test"},{"input":"payload","header":"x-test"}])",
       }) {
    expect_default_error(
        std::format(
            R"({{"method":"POST","url":"http://127.0.0.1:8080/","headers":[],"input_headers":{},"accepted_statuses":[]}})",
            bindings),
        Error::InvalidArgument);
  }

  for (std::string_view statuses : {"[99]", "[600]", "[200,200]"}) {
    expect_default_error(
        std::format(
            R"({{"method":"POST","url":"http://127.0.0.1:8080/","headers":[],"input_headers":[],"accepted_statuses":{}}})",
            statuses),
        Error::InvalidArgument);
  }

  const std::array invalid_outputs{WorkflowPortId{"unknown"}};
  expect_error(
      R"({"method":"POST","url":"http://127.0.0.1:8080/","headers":[],"input_headers":[],"accepted_statuses":[]})",
      Error::InvalidArgument, compile_context(inputs, invalid_outputs));

  expect_default_error(
      R"({"method":"POST","url":"https://other.example/","headers":[],"input_headers":[],"accepted_statuses":[]})",
      Error::Unauthorized);
}

TEST(HttpTaskExecutorTest, EnforcesStartAndQuiesceLifecycleBoundaries) {
  Runtime runtime(2);
  ASSERT_TRUE(runtime.start().has_value());
  auto executor = create_task_executor(runtime, executor_config());
  ASSERT_TRUE(executor.has_value()) << executor.error().message();

  workflow::TaskExecutionRequest request{
      .instance_id = InstanceId{"outside-shard"},
      .config = json(
          R"({"method":"GET","url":"http://127.0.0.1:8080/","headers":[],"input_headers":[],"accepted_statuses":[]})"),
      .outputs = {WorkflowPortId{"result"}},
  };
  auto outside = (*executor)->start(std::move(request), {});
  ASSERT_FALSE(outside.has_value());
  EXPECT_EQ(outside.error(), make_error_code(Error::InvalidState));

  EXPECT_TRUE((*executor)->quiesce(std::chrono::seconds(2)).has_value());

  std::promise<Result<void>> result;
  auto future = result.get_future();
  runtime.post_to(0, [executor = *executor, &result]() mutable {
    workflow::TaskExecutionRequest request{
        .instance_id = InstanceId{"after-quiesce"},
        .config = json(
            R"({"method":"GET","url":"http://127.0.0.1:8080/","headers":[],"input_headers":[],"accepted_statuses":[]})"),
        .outputs = {WorkflowPortId{"result"}},
    };
    result.set_value(executor->start(std::move(request), {}));
  });
  ASSERT_EQ(future.wait_for(std::chrono::seconds(2)),
            std::future_status::ready);
  auto after_quiesce = future.get();
  ASSERT_FALSE(after_quiesce.has_value());
  EXPECT_EQ(after_quiesce.error(), make_error_code(Error::InvalidState));
  runtime.stop();
}

TEST(HttpTaskExecutorTest, ExecutesLocalRequestsMapsOutputsAndReusesClients) {
  const auto port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);

  Runtime runtime(2);
  ASSERT_TRUE(runtime.start().has_value());
  ::dagforge::http::HttpServer server(runtime);
  std::atomic<unsigned> request_count{0};
  std::mutex observation_mutex;
  std::string observed_trace;
  std::string first_content_type;
  server.router().post(
      "/echo", [&](::dagforge::http::HttpRequest request)
                    -> task<::dagforge::http::HttpResponse> {
        const auto request_index =
            request_count.fetch_add(1, std::memory_order_relaxed);
        {
          std::lock_guard lock(observation_mutex);
          observed_trace = request.header("X-Trace").value_or("");
          if (request_index == 0) {
            first_content_type =
                request.header("Content-Type").value_or("");
          }
        }
        ::dagforge::http::HttpResponse response;
        response.status = ::dagforge::http::HttpStatus::Created;
        response.headers.add("X-Reply", "accepted");
        response.body = request.body;
        co_return response;
      });
  ASSERT_TRUE(server.start("127.0.0.1", port, false).has_value());

  auto limits = executor_config();
  limits.allowed_origins = {std::format("http://127.0.0.1:{}", port)};
  limits.max_idle_connections_per_shard = 4;
  limits.max_idle_connections_per_origin = 2;
  auto executor = create_task_executor(runtime, limits);
  ASSERT_TRUE(executor.has_value()) << executor.error().message();

  const std::array inputs{
      workflow::InputBinding{.input = WorkflowPortId{"payload"}},
      workflow::InputBinding{.input = WorkflowPortId{"trace"}},
  };
  const std::array outputs{
      WorkflowPortId{"status"}, WorkflowPortId{"body"},
      WorkflowPortId{"headers"}, WorkflowPortId{"result"}};
  auto compiled = (*executor)->compile(
      json(std::format(
          R"({{"method":"POST","url":"http://127.0.0.1:{}/echo","headers":[],"input_headers":[{{"input":"trace","header":"X-Trace"}}],"body_input":"payload","accepted_statuses":[201]}})",
          port)),
      compile_context(inputs, outputs));
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  JsonValue object = JsonValue::object_t{};
  object["message"] = "hello";
  workflow::TaskExecutionRequest first{
      .instance_id = InstanceId{"http-success-1"},
      .config = *compiled,
      .inputs = {{"payload", std::make_shared<const workflow::WorkflowValue>(
                                 std::move(object))},
                 {"trace", std::make_shared<const workflow::WorkflowValue>(
                               std::int64_t{42})}},
      .outputs =
          std::vector<WorkflowPortId>{outputs.begin(), outputs.end()},
      .timeout = std::chrono::seconds(2),
  };
  auto first_result = execute_on_shard(runtime, *executor, std::move(first));
  ASSERT_TRUE(first_result.has_value()) << first_result.error().message();
  const auto *status = output_value(*first_result, "status");
  const auto *body = output_value(*first_result, "body");
  const auto *headers = output_value(*first_result, "headers");
  const auto *result = output_value(*first_result, "result");
  ASSERT_NE(status, nullptr);
  ASSERT_NE(body, nullptr);
  ASSERT_NE(headers, nullptr);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(std::get<std::int64_t>(*status), 201);
  EXPECT_EQ(std::get<std::string>(*body), R"({"message":"hello"})");
  EXPECT_EQ(std::get<std::string>(*result), R"({"message":"hello"})");
  EXPECT_TRUE(std::holds_alternative<JsonValue>(*headers));

  workflow::TaskExecutionRequest second{
      .instance_id = InstanceId{"http-success-2"},
      .config = *compiled,
      .inputs = {{"payload", std::make_shared<const workflow::WorkflowValue>(
                                 std::string{"plain"})},
                 {"trace", std::make_shared<const workflow::WorkflowValue>(
                               true)}},
      .outputs =
          std::vector<WorkflowPortId>{outputs.begin(), outputs.end()},
      .timeout = std::chrono::seconds(2),
  };
  auto second_result = execute_on_shard(runtime, *executor, std::move(second));
  ASSERT_TRUE(second_result.has_value()) << second_result.error().message();
  EXPECT_EQ(std::get<std::string>(*output_value(*second_result, "body")),
            "plain");
  EXPECT_EQ(request_count.load(std::memory_order_relaxed), 2U);
  {
    std::lock_guard lock(observation_mutex);
    EXPECT_EQ(observed_trace, "true");
    EXPECT_EQ(first_content_type, "application/json");
  }

  EXPECT_TRUE((*executor)->quiesce(std::chrono::seconds(2)).has_value());
  server.stop();
  runtime.stop();
}

TEST(HttpTaskExecutorTest, MapsHttpStatusAndProtocolFailures) {
  const auto port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);

  Runtime runtime(2);
  ASSERT_TRUE(runtime.start().has_value());
  ::dagforge::http::HttpServer server(runtime);
  const auto add_status_route = [&](std::string path,
                                    ::dagforge::http::HttpStatus status) {
    server.router().get(
        std::move(path), [status](::dagforge::http::HttpRequest)
                             -> task<::dagforge::http::HttpResponse> {
          ::dagforge::http::HttpResponse response;
          response.status = status;
          response.set_body("status");
          co_return response;
        });
  };
  add_status_route("/unauthorized", ::dagforge::http::HttpStatus::Unauthorized);
  add_status_route("/forbidden", ::dagforge::http::HttpStatus::Forbidden);
  add_status_route("/missing", ::dagforge::http::HttpStatus::NotFound);
  add_status_route("/rate", ::dagforge::http::HttpStatus::TooManyRequests);
  add_status_route("/server", ::dagforge::http::HttpStatus::ServiceUnavailable);
  add_status_route("/bad", ::dagforge::http::HttpStatus::BadRequest);
  server.router().get(
      "/invalid-utf8", [](::dagforge::http::HttpRequest)
                           -> task<::dagforge::http::HttpResponse> {
        ::dagforge::http::HttpResponse response;
        response.body = {0xff, 0xfe};
        co_return response;
      });
  ASSERT_TRUE(server.start("127.0.0.1", port, false).has_value());

  auto limits = executor_config();
  limits.allowed_origins = {std::format("http://127.0.0.1:{}", port)};
  auto executor = create_task_executor(runtime, limits);
  ASSERT_TRUE(executor.has_value()) << executor.error().message();
  const std::array<workflow::InputBinding, 0> inputs{};
  const std::array outputs{WorkflowPortId{"result"}};

  const auto execute_path = [&](std::string_view path,
                                std::vector<std::uint16_t> accepted = {}) {
    JsonValue statuses = JsonValue::array_t{};
    for (const auto status : accepted) {
      statuses.get_array().push_back(static_cast<std::int64_t>(status));
    }
    JsonValue config = JsonValue::object_t{};
    config["method"] = "GET";
    config["url"] =
        std::format("http://127.0.0.1:{}{}", port, path);
    config["headers"] = JsonValue::array_t{};
    config["input_headers"] = JsonValue::array_t{};
    config["accepted_statuses"] = std::move(statuses);
    auto compiled = (*executor)->compile(std::move(config),
                                         compile_context(inputs, outputs));
    EXPECT_TRUE(compiled.has_value())
        << (compiled ? "" : compiled.error().message());
    if (!compiled) {
      return Result<workflow::ExecutorOutputs>{fail(compiled.error())};
    }
    return execute_on_shard(
        runtime, *executor,
        workflow::TaskExecutionRequest{
            .instance_id = InstanceId{std::string{"status"} +
                                      std::string{path}},
            .config = std::move(*compiled),
            .outputs = {WorkflowPortId{"result"}},
            .timeout = std::chrono::seconds(2),
        });
  };

  EXPECT_EQ(execute_path("/unauthorized").error(),
            make_error_code(Error::Unauthorized));
  EXPECT_EQ(execute_path("/forbidden").error(),
            make_error_code(Error::Unauthorized));
  EXPECT_EQ(execute_path("/missing").error(),
            make_error_code(Error::NotFound));
  EXPECT_EQ(execute_path("/rate").error(),
            make_error_code(Error::RateLimited));
  EXPECT_EQ(execute_path("/server").error(),
            make_error_code(Error::Unknown));
  EXPECT_EQ(execute_path("/bad").error(),
            make_error_code(Error::ProtocolError));
  EXPECT_EQ(execute_path("/invalid-utf8").error(),
            make_error_code(Error::ProtocolError));
  auto accepted_missing = execute_path("/missing", {404});
  ASSERT_TRUE(accepted_missing.has_value())
      << accepted_missing.error().message();
  EXPECT_EQ(std::get<std::string>(*output_value(*accepted_missing, "result")),
            "status");

  EXPECT_TRUE((*executor)->quiesce(std::chrono::seconds(2)).has_value());
  server.stop();
  runtime.stop();
}

TEST(HttpTaskExecutorTest, EnforcesActiveLimitsCancellationAndInputSafety) {
  const auto port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);

  Runtime runtime(2);
  ASSERT_TRUE(runtime.start().has_value());
  ::dagforge::http::HttpServer server(runtime);
  server.router().get(
      "/slow", [](::dagforge::http::HttpRequest)
                    -> task<::dagforge::http::HttpResponse> {
        co_await async_sleep(std::chrono::milliseconds(500));
        co_return ::dagforge::http::HttpResponse::ok().set_body("slow");
      });
  server.router().post(
      "/body", [](::dagforge::http::HttpRequest request)
                    -> task<::dagforge::http::HttpResponse> {
        co_return ::dagforge::http::HttpResponse::ok().set_body(
            std::string{request.body_as_string()});
      });
  ASSERT_TRUE(server.start("127.0.0.1", port, false).has_value());

  auto limits = executor_config();
  limits.allowed_origins = {std::format("http://127.0.0.1:{}", port)};
  limits.max_concurrent_requests = 1;
  limits.max_concurrent_requests_per_shard = 2;
  limits.max_request_body_bytes = 8;
  auto executor = create_task_executor(runtime, limits);
  ASSERT_TRUE(executor.has_value()) << executor.error().message();

  const std::array<workflow::InputBinding, 0> no_inputs{};
  const std::array result_output{WorkflowPortId{"result"}};
  auto slow_config = (*executor)->compile(
      json(std::format(
          R"({{"method":"GET","url":"http://127.0.0.1:{}/slow","headers":[],"input_headers":[],"accepted_statuses":[]}})",
          port)),
      compile_context(no_inputs, result_output));
  ASSERT_TRUE(slow_config.has_value()) << slow_config.error().message();

  auto completion =
      std::make_shared<std::promise<Result<workflow::ExecutorOutputs>>>();
  auto completion_future = completion->get_future();
  struct Starts {
    Result<void> first;
    Result<void> duplicate;
    Result<void> over_global_limit;
  };
  auto starts = std::make_shared<std::promise<Starts>>();
  auto starts_future = starts->get_future();
  runtime.post_to(0, [executor = *executor, config = *slow_config, completion,
                      starts]() mutable {
    workflow::TaskExecutionSink sink{
        .on_complete =
            [completion](const InstanceId &,
                         Result<workflow::ExecutorOutputs> result) mutable {
              completion->set_value(std::move(result));
            },
    };
    auto first = executor->start(
        workflow::TaskExecutionRequest{
            .instance_id = InstanceId{"active"},
            .config = config,
            .outputs = {WorkflowPortId{"result"}},
            .timeout = std::chrono::seconds(2),
        },
        std::move(sink));
    auto duplicate = executor->start(
        workflow::TaskExecutionRequest{
            .instance_id = InstanceId{"active"},
            .config = config,
            .outputs = {WorkflowPortId{"result"}},
            .timeout = std::chrono::seconds(2),
        },
        {});
    auto over_global_limit = executor->start(
        workflow::TaskExecutionRequest{
            .instance_id = InstanceId{"second"},
            .config = std::move(config),
            .outputs = {WorkflowPortId{"result"}},
            .timeout = std::chrono::seconds(2),
        },
        {});
    starts->set_value(Starts{.first = std::move(first),
                             .duplicate = std::move(duplicate),
                             .over_global_limit =
                                 std::move(over_global_limit)});
  });
  ASSERT_EQ(starts_future.wait_for(std::chrono::seconds(2)),
            std::future_status::ready);
  auto observed_starts = starts_future.get();
  EXPECT_TRUE(observed_starts.first.has_value());
  EXPECT_EQ(observed_starts.duplicate.error(),
            make_error_code(Error::AlreadyExists));
  EXPECT_EQ(observed_starts.over_global_limit.error(),
            make_error_code(Error::ResourceExhausted));

  (*executor)->cancel(InstanceId{"unknown"});
  (*executor)->cancel(InstanceId{"active"});
  ASSERT_EQ(completion_future.wait_for(std::chrono::seconds(2)),
            std::future_status::ready);
  EXPECT_EQ(completion_future.get().error(), make_error_code(Error::Cancelled));

  const std::array body_inputs{
      workflow::InputBinding{.input = WorkflowPortId{"payload"}},
      workflow::InputBinding{.input = WorkflowPortId{"trace"}},
  };
  auto body_config = (*executor)->compile(
      json(std::format(
          R"({{"method":"POST","url":"http://127.0.0.1:{}/body","headers":[],"input_headers":[{{"input":"trace","header":"X-Trace"}}],"body_input":"payload","accepted_statuses":[]}})",
          port)),
      compile_context(body_inputs, result_output));
  ASSERT_TRUE(body_config.has_value()) << body_config.error().message();

  const auto execute_inputs = [&](workflow::ExecutorInputs inputs,
                                  std::string instance) {
    return execute_on_shard(
        runtime, *executor,
        workflow::TaskExecutionRequest{
            .instance_id = InstanceId{std::move(instance)},
            .config = *body_config,
            .inputs = std::move(inputs),
            .outputs = {WorkflowPortId{"result"}},
            .timeout = std::chrono::seconds(2),
        });
  };
  EXPECT_EQ(execute_inputs({}, "missing-inputs").error(),
            make_error_code(Error::InvalidArgument));
  EXPECT_EQ(
      execute_inputs(
          {{"payload", std::make_shared<const workflow::WorkflowValue>(
                           workflow::ArtifactRef{
                               .artifact_id = ArtifactId{"artifact"}})},
           {"trace", std::make_shared<const workflow::WorkflowValue>(
                         std::string{"safe"})}},
          "artifact-body")
          .error(),
      make_error_code(Error::Unsupported));
  EXPECT_EQ(
      execute_inputs(
          {{"payload", std::make_shared<const workflow::WorkflowValue>(
                           std::string{"body"})},
           {"trace", std::make_shared<const workflow::WorkflowValue>(
                         std::string{"bad\r\nheader"})}},
          "unsafe-header")
          .error(),
      make_error_code(Error::InvalidArgument));
  EXPECT_EQ(
      execute_inputs(
          {{"payload", std::make_shared<const workflow::WorkflowValue>(
                           std::string{"too-large"})},
           {"trace", std::make_shared<const workflow::WorkflowValue>(
                         1.5)}},
          "oversized-body")
          .error(),
      make_error_code(Error::ResourceExhausted));

  auto timeout_result = execute_on_shard(
      runtime, *executor,
      workflow::TaskExecutionRequest{
          .instance_id = InstanceId{"immediate-timeout"},
          .config = *slow_config,
          .outputs = {WorkflowPortId{"result"}},
          .timeout = std::chrono::seconds(0),
      });
  EXPECT_EQ(timeout_result.error(), make_error_code(Error::Timeout));

  auto quiesce_completion =
      std::make_shared<std::promise<Result<workflow::ExecutorOutputs>>>();
  auto quiesce_future = quiesce_completion->get_future();
  auto started_for_quiesce = std::make_shared<std::promise<Result<void>>>();
  auto started_future = started_for_quiesce->get_future();
  runtime.post_to(
      0, [executor = *executor, config = *slow_config, quiesce_completion,
          started_for_quiesce]() mutable {
        workflow::TaskExecutionSink sink{
            .on_complete =
                [quiesce_completion](
                    const InstanceId &,
                    Result<workflow::ExecutorOutputs> result) mutable {
                  quiesce_completion->set_value(std::move(result));
                },
        };
        started_for_quiesce->set_value(executor->start(
            workflow::TaskExecutionRequest{
                .instance_id = InstanceId{"quiesce-active"},
                .config = std::move(config),
                .outputs = {WorkflowPortId{"result"}},
                .timeout = std::chrono::seconds(2),
            },
            std::move(sink)));
      });
  ASSERT_EQ(started_future.wait_for(std::chrono::seconds(2)),
            std::future_status::ready);
  ASSERT_TRUE(started_future.get().has_value());
  EXPECT_EQ((*executor)->quiesce(std::chrono::milliseconds(0)).error(),
            make_error_code(Error::Timeout));
  EXPECT_TRUE((*executor)->quiesce(std::chrono::seconds(2)).has_value());
  ASSERT_EQ(quiesce_future.wait_for(std::chrono::seconds(2)),
            std::future_status::ready);
  EXPECT_EQ(quiesce_future.get().error(), make_error_code(Error::Cancelled));

  server.stop();
  runtime.stop();
}

TEST(HttpTaskExecutorTest, MapsAllSupportedInputValueTypesAtStart) {
  Runtime runtime(1);
  ASSERT_TRUE(runtime.start().has_value());
  auto executor = create_task_executor(runtime, executor_config());
  ASSERT_TRUE(executor.has_value()) << executor.error().message();

  const std::array inputs{
      workflow::InputBinding{
          .input = WorkflowPortId{"payload"},
          .source = workflow::OutputRef{
              .node_id = WorkflowNodeId{"upstream"},
              .port = WorkflowPortId{"result"},
          },
      },
  };
  const std::array outputs{WorkflowPortId{"result"}};
  auto compiled = (*executor)->compile(
      json(R"({"method":"POST","url":"http://127.0.0.1:8080/","headers":[],"input_headers":[],"body_input":"payload","accepted_statuses":[]})"),
      compile_context(inputs, outputs));
  ASSERT_TRUE(compiled.has_value()) << compiled.error().message();

  const auto start_value = [&](std::string_view id, workflow::WorkflowValue value) {
    auto result = std::make_shared<std::promise<Result<void>>>();
    auto future = result->get_future();
    runtime.post_to(
        0, [executor = *executor, config = *compiled,
            id = std::string{id}, value = std::move(value), result]() mutable {
          workflow::TaskExecutionRequest request{
              .instance_id = InstanceId{id},
              .config = std::move(config),
              .inputs = {{"payload", std::make_shared<const workflow::WorkflowValue>(
                                         std::move(value))}},
              .outputs = {WorkflowPortId{"result"}},
              .timeout = std::chrono::seconds(1),
          };
          result->set_value(executor->start(std::move(request), {}));
        });
    EXPECT_EQ(future.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    return future.get();
  };

  EXPECT_TRUE(start_value("none", std::monostate{}).has_value());
  EXPECT_TRUE(start_value("boolean", true).has_value());
  EXPECT_TRUE(start_value("integer", std::int64_t{42}).has_value());
  EXPECT_TRUE(start_value("real", 3.5).has_value());
  EXPECT_TRUE(start_value("text", std::string{"payload"}).has_value());
  JsonValue object = JsonValue::object_t{};
  object["value"] = 42;
  EXPECT_TRUE(start_value("json", std::move(object)).has_value());

  auto artifact = start_value(
      "artifact",
      workflow::ArtifactRef{.artifact_id = ArtifactId{"artifact"}});
  ASSERT_FALSE(artifact.has_value());
  EXPECT_EQ(artifact.error(), make_error_code(Error::Unsupported));

  EXPECT_TRUE((*executor)->quiesce(std::chrono::seconds(3)).has_value());
  runtime.stop();
}

} // namespace dagforge::executors::http::test
