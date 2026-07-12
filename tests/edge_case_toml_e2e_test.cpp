#include "e2e_test_support.hpp"
#include "test_utils.hpp"

#include "gtest/gtest.h"

#include <filesystem>
#include <format>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>



#include "dagforge/app/application.hpp"
#include "dagforge/app/services/execution_service.hpp"
#include "dagforge/app/services/persistence_service.hpp"
#include "dagforge/app/services/scheduler_service.hpp"
#include "dagforge/util/json.hpp"


using namespace dagforge;

namespace {

auto repo_root() -> std::filesystem::path {
  const auto here = std::filesystem::path(__FILE__);
  return here.parent_path().parent_path();
}

auto fixture_root() -> std::filesystem::path {
  return repo_root() / "tests" / "fixtures" / "edge_cases";
}

auto fixture_path(std::string_view name) -> std::filesystem::path {
  return fixture_root() / std::string(name);
}

auto make_test_config(std::uint16_t api_port) -> SystemConfig {
  return dagforge::test::make_mysql_test_config(api_port);
}

class EdgeCaseTomlE2ETest : public dagforge::test::MySqlIsolatedTest {};

auto wait_api_ready(std::uint16_t port) -> bool {
  return dagforge::test::wait_api_ready(port);
}

auto load_fixture_to_temp_dir(std::string_view name,
                              std::string_view dag_id_override = {})
    -> std::filesystem::path {
  const auto src = fixture_path(name);
  auto tmp = dagforge::test::make_temp_dir("dagforge_edge_case_");
  if (tmp.empty()) {
    throw std::runtime_error("failed to create temp directory");
  }
  const auto dst_name = dag_id_override.empty()
                            ? src.filename()
                            : std::filesystem::path(
                                  std::format("{}.toml", dag_id_override));
  const auto dst = std::filesystem::path(tmp) / dst_name;
  std::filesystem::copy_file(src, dst,
                             std::filesystem::copy_options::overwrite_existing);
  return std::filesystem::path(tmp);
}

auto trigger_dag(std::uint16_t port, std::string_view dag_id) -> std::string {
  return dagforge::test::trigger_dag(port, dag_id);
}

auto fetch_run_state(std::uint16_t port, std::string_view run_id)
    -> std::optional<std::string> {
  return dagforge::test::fetch_run_state(port, run_id);
}

using TaskSnapshot = dagforge::test::TaskSnapshot;

auto fetch_run_tasks(std::uint16_t port, std::string_view run_id)
    -> std::unordered_map<std::string, TaskSnapshot> {
  return dagforge::test::fetch_run_tasks(port, run_id);
}

auto wait_for_all_tasks_terminal(std::uint16_t port, std::string_view run_id,
                                 std::chrono::seconds timeout =
                                     std::chrono::seconds(30))
    -> bool {
  auto is_terminal = [](std::string_view state) {
    return state == "success" || state == "failed" ||
           state == "upstream_failed" || state == "skipped";
  };

  return dagforge::test::poll_until(
      [&]() {
        auto tasks = fetch_run_tasks(port, run_id);
        if (tasks.empty()) {
          return false;
        }
        for (const auto &[task_id, snapshot] : tasks) {
          (void)task_id;
          if (!is_terminal(snapshot.state)) {
            return false;
          }
        }
        return true;
      },
      timeout, std::chrono::milliseconds(100));
}

} // namespace

TEST_F(EdgeCaseTomlE2ETest, TriggerRuleBoundaryScenario) {
  const auto port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);

  auto validation = DAGInfoLoader::load_from_file(
      fixture_path("scene08_trigger_rules_boundary.toml").string());
  ASSERT_TRUE(validation.has_value()) << validation.error().message();

  const auto dag_id = std::format("scene08_trigger_rules_boundary_{}", port);
  const auto dag_dir = load_fixture_to_temp_dir(
      "scene08_trigger_rules_boundary.toml", dag_id);
  auto cfg = make_test_config(port);
  cfg.dag_source.directory = dag_dir.string();
  Application app(cfg);
  auto init_res = app.init();
  ASSERT_TRUE(init_res) << init_res.error().message();

  auto start_res = app.start();
  if (!start_res) {
    GTEST_SKIP() << "MySQL unavailable for trigger-rule boundary E2E: "
                 << start_res.error().message();
  }

  ASSERT_TRUE(wait_api_ready(port));

  const auto run_id = trigger_dag(port, dag_id);
  ASSERT_TRUE(
      dagforge::test::poll_until([&]() { return fetch_run_state(port, run_id) == "failed"; },
                                 std::chrono::seconds(30), std::chrono::milliseconds(100)));
  ASSERT_TRUE(wait_for_all_tasks_terminal(port, run_id));
  const auto tasks = fetch_run_tasks(port, run_id);
  ASSERT_EQ(tasks.size(), 7U);
  EXPECT_EQ(tasks.at("seed_ok").state, "success");
  EXPECT_EQ(tasks.at("seed_fail").state, "failed");
  EXPECT_EQ(tasks.at("all_success_tail").state, "upstream_failed");
  EXPECT_EQ(tasks.at("one_failed_tail").state, "success");
  EXPECT_EQ(tasks.at("one_done_tail").state, "success");
  EXPECT_EQ(tasks.at("all_done_tail").state, "success");
  EXPECT_EQ(tasks.at("none_failed_tail").state, "upstream_failed");

  app.stop();
  std::filesystem::remove_all(dag_dir);
}

TEST_F(EdgeCaseTomlE2ETest, RetryTimeoutScenario) {
  const auto port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);

  auto validation = DAGInfoLoader::load_from_file(
      fixture_path("scene10_retry_timeout.toml").string());
  ASSERT_TRUE(validation.has_value()) << validation.error().message();

  const auto dag_id = std::format("scene10_retry_timeout_{}", port);
  const auto dag_dir = load_fixture_to_temp_dir("scene10_retry_timeout.toml",
                                                dag_id);
  auto cfg = make_test_config(port);
  cfg.dag_source.directory = dag_dir.string();
  Application app(cfg);
  auto init_res = app.init();
  ASSERT_TRUE(init_res) << init_res.error().message();

  auto start_res = app.start();
  if (!start_res) {
    GTEST_SKIP() << "MySQL unavailable for retry timeout E2E: "
                 << start_res.error().message();
  }

  ASSERT_TRUE(wait_api_ready(port));

  const auto run_id = trigger_dag(port, dag_id);
  ASSERT_TRUE(
      dagforge::test::poll_until([&]() { return fetch_run_state(port, run_id) == "success"; },
                                 std::chrono::seconds(40), std::chrono::milliseconds(100)));

  const auto tasks = fetch_run_tasks(port, run_id);
  ASSERT_EQ(tasks.size(), 2U);
  EXPECT_EQ(tasks.at("flaky").state, "success");
  EXPECT_GE(tasks.at("flaky").attempt, 2);
  EXPECT_EQ(tasks.at("report").state, "success");

  const auto marker = std::filesystem::path("/tmp") /
                      std::format("scene10_retry_timeout_{}_retry_marker", run_id);
  std::error_code ec;
  std::filesystem::remove(marker, ec);

  app.stop();
  std::filesystem::remove_all(dag_dir);
}

TEST_F(EdgeCaseTomlE2ETest, RetryTimeoutFinalFailureScenario) {
  const auto port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);

  auto validation = DAGInfoLoader::load_from_file(
      fixture_path("scene11_retry_timeout_terminal.toml").string());
  ASSERT_TRUE(validation.has_value()) << validation.error().message();

  const auto dag_id = std::format("scene11_retry_timeout_terminal_{}", port);
  const auto dag_dir = load_fixture_to_temp_dir(
      "scene11_retry_timeout_terminal.toml", dag_id);
  auto cfg = make_test_config(port);
  cfg.dag_source.directory = dag_dir.string();
  Application app(cfg);
  auto init_res = app.init();
  ASSERT_TRUE(init_res) << init_res.error().message();

  auto start_res = app.start();
  if (!start_res) {
    GTEST_SKIP() << "MySQL unavailable for retry timeout failure E2E: "
                 << start_res.error().message();
  }

  ASSERT_TRUE(wait_api_ready(port));

  const auto run_id = trigger_dag(port, dag_id);
  ASSERT_TRUE(dagforge::test::poll_until(
      [&]() { return fetch_run_state(port, run_id) == "failed"; },
      std::chrono::seconds(40), std::chrono::milliseconds(100)));
  ASSERT_TRUE(wait_for_all_tasks_terminal(port, run_id,
                                          std::chrono::seconds(40)));

  const auto tasks = fetch_run_tasks(port, run_id);
  ASSERT_EQ(tasks.size(), 3U);
  EXPECT_EQ(tasks.at("flaky_timeout").state, "failed");
  EXPECT_EQ(tasks.at("flaky_timeout").attempt, 2);
  EXPECT_EQ(tasks.at("all_done_tail").state, "success");
  EXPECT_EQ(tasks.at("all_success_tail").state, "upstream_failed");

  const auto marker =
      std::filesystem::path("/tmp") /
      std::format("scene11_retry_timeout_{}_marker", run_id);
  std::error_code ec;
  std::filesystem::remove(marker, ec);

  app.stop();
  std::filesystem::remove_all(dag_dir);
}

TEST_F(EdgeCaseTomlE2ETest, DockerTimeoutScenario) {
  if (!std::filesystem::exists("/var/run/docker.sock")) {
    GTEST_SKIP() << "Docker socket unavailable for docker-timeout E2E";
  }

  const auto port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);

  auto validation = DAGInfoLoader::load_from_file(
      fixture_path("scene12_docker_timeout.toml").string());
  ASSERT_TRUE(validation.has_value()) << validation.error().message();

  const auto dag_id = std::format("scene12_docker_timeout_{}", port);
  const auto dag_dir =
      load_fixture_to_temp_dir("scene12_docker_timeout.toml", dag_id);
  auto cfg = make_test_config(port);
  cfg.dag_source.directory = dag_dir.string();
  Application app(cfg);
  auto init_res = app.init();
  ASSERT_TRUE(init_res) << init_res.error().message();

  auto start_res = app.start();
  if (!start_res) {
    GTEST_SKIP() << "MySQL unavailable for docker-timeout E2E: "
                 << start_res.error().message();
  }

  ASSERT_TRUE(wait_api_ready(port));

  const auto run_id = trigger_dag(port, dag_id);
  ASSERT_TRUE(wait_for_all_tasks_terminal(port, run_id,
                                          std::chrono::seconds(90)));

  const auto tasks = fetch_run_tasks(port, run_id);
  ASSERT_EQ(tasks.size(), 3U);
  const auto &docker_task = tasks.at("docker_sleep");
  const auto &all_done_tail = tasks.at("all_done_tail");
  const auto &all_success_tail = tasks.at("all_success_tail");

  EXPECT_EQ(docker_task.state, "failed");
  EXPECT_EQ(all_done_tail.state, "success");
  EXPECT_EQ(all_success_tail.state, "upstream_failed");

  app.stop();
  std::filesystem::remove_all(dag_dir);
}

TEST_F(EdgeCaseTomlE2ETest, SensorSoftFailScenario) {
  const auto port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);

  auto validation = DAGInfoLoader::load_from_file(
      fixture_path("scene14_sensor_soft_fail.toml").string());
  ASSERT_TRUE(validation.has_value()) << validation.error().message();

  const auto dag_id = std::format("scene14_sensor_soft_fail_{}", port);
  const auto dag_dir =
      load_fixture_to_temp_dir("scene14_sensor_soft_fail.toml", dag_id);
  auto cfg = make_test_config(port);
  cfg.dag_source.directory = dag_dir.string();
  Application app(cfg);
  auto init_res = app.init();
  ASSERT_TRUE(init_res) << init_res.error().message();

  auto start_res = app.start();
  if (!start_res) {
    GTEST_SKIP() << "MySQL unavailable for sensor soft-fail E2E: "
                 << start_res.error().message();
  }

  ASSERT_TRUE(wait_api_ready(port));

  const auto run_id = trigger_dag(port, dag_id);
  ASSERT_TRUE(dagforge::test::poll_until(
      [&]() { return fetch_run_state(port, run_id) == "success"; },
      std::chrono::seconds(30), std::chrono::milliseconds(100)));
  ASSERT_TRUE(wait_for_all_tasks_terminal(port, run_id));

  const auto tasks = fetch_run_tasks(port, run_id);
  ASSERT_EQ(tasks.size(), 4U);
  EXPECT_EQ(tasks.at("wait_optional").state, "skipped");
  EXPECT_EQ(tasks.at("core").state, "success");
  EXPECT_EQ(tasks.at("none_failed_tail").state, "success");
  EXPECT_EQ(tasks.at("all_success_tail").state, "skipped");

  app.stop();
  std::filesystem::remove_all(dag_dir);
}
