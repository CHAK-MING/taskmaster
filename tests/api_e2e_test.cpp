#include "taskmaster/app/api/api_server.hpp"
#include "taskmaster/app/application.hpp"
#include "taskmaster/config/config.hpp"
#include "taskmaster/storage/persistence.hpp"
#include "taskmaster/util/id.hpp"

#include <arpa/inet.h>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <vector>

#include <unistd.h>

#include "gtest/gtest.h"
#include "nlohmann/json.hpp"

using namespace taskmaster;
using namespace std::chrono_literals;
namespace fs = std::filesystem;
using json = nlohmann::json;

namespace {

constexpr auto kServerStartupTimeout = std::chrono::milliseconds(2000);
constexpr auto kServerShutdownDelay = std::chrono::milliseconds(200);
constexpr auto kPollInterval = std::chrono::milliseconds(50);

auto pick_unused_tcp_port() -> uint16_t {
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) return 0;

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = 0;
  inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

  if (bind(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    close(sock);
    return 0;
  }

  socklen_t len = sizeof(addr);
  if (getsockname(sock, reinterpret_cast<sockaddr*>(&addr), &len) < 0) {
    close(sock);
    return 0;
  }

  uint16_t port = ntohs(addr.sin_port);
  close(sock);
  return port;
}

auto make_temp_path(const std::string& prefix = "taskmaster_test_") -> std::string {
  std::string templ = "/tmp/" + prefix + "XXXXXX";
  int fd = ::mkstemp(templ.data());
  EXPECT_GE(fd, 0) << "Failed to create temp file";
  if (fd >= 0) {
    ::close(fd);
  }
  return templ;
}

auto make_temp_dir(const std::string& prefix = "taskmaster_dags_") -> std::string {
  std::string templ = "/tmp/" + prefix + "XXXXXX";
  char* path = ::mkdtemp(templ.data());
  EXPECT_TRUE(path != nullptr) << "Failed to create temp dir";
  return path ? std::string(path) : "";
}

auto http_get(uint16_t port, std::string_view path)
    -> std::pair<int, std::string> {
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0)
    return {-1, ""};

  struct timeval tv;
  tv.tv_sec = 2;
  tv.tv_usec = 0;
  setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

  if (connect(sock, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
    close(sock);
    return {-1, ""};
  }

  std::string request = std::format(
      "GET {} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n", path);
  send(sock, request.data(), request.size(), 0);

  std::string response;
  char buffer[4096];
  ssize_t n;
  while ((n = recv(sock, buffer, sizeof(buffer), 0)) > 0) {
    response.append(buffer, n);
  }
  close(sock);

  int status_code = 0;
  if (response.size() > 12) {
    auto space_pos = response.find(' ');
    if (space_pos != std::string::npos) {
      status_code = std::stoi(response.substr(space_pos + 1, 3));
    }
  }

  auto body_start = response.find("\r\n\r\n");
  std::string body =
      (body_start != std::string::npos) ? response.substr(body_start + 4) : "";

  return {status_code, body};
}

auto http_post(uint16_t port, std::string_view path, std::string_view json_body)
    -> std::pair<int, std::string> {
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0)
    return {-1, ""};

  struct timeval tv;
  tv.tv_sec = 2;
  tv.tv_usec = 0;
  setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

  if (connect(sock, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
    close(sock);
    return {-1, ""};
  }

  std::string request = std::format("POST {} HTTP/1.1\r\n"
                                    "Host: localhost\r\n"
                                    "Content-Type: application/json\r\n"
                                    "Content-Length: {}\r\n"
                                    "Connection: close\r\n\r\n{}",
                                    path, json_body.size(), json_body);
  send(sock, request.data(), request.size(), 0);

  std::string response;
  char buffer[4096];
  ssize_t n;
  while ((n = recv(sock, buffer, sizeof(buffer), 0)) > 0) {
    response.append(buffer, n);
  }
  close(sock);

  int status_code = 0;
  if (response.size() > 12) {
    auto space_pos = response.find(' ');
    if (space_pos != std::string::npos) {
      status_code = std::stoi(response.substr(space_pos + 1, 3));
    }
  }

  auto body_start = response.find("\r\n\r\n");
  std::string body =
      (body_start != std::string::npos) ? response.substr(body_start + 4) : "";

  return {status_code, body};
}

} // namespace

class ApiE2ETest : public ::testing::Test {
protected:
  void SetUp() override {
    temp_db_ = make_temp_path("taskmaster_db_");
    temp_dags_dir_ = make_temp_dir("taskmaster_dags_");

    for (int i = 0; i < 5; ++i) {
        app_ = std::make_unique<Application>(temp_db_);
        auto& cfg = app_->config();
        cfg.api.enabled = true;
        cfg.api.port = pick_unused_tcp_port();
        cfg.dag_source.mode = DAGSourceMode::File;
        cfg.dag_source.directory = temp_dags_dir_;
        cfg.dag_source.scan_interval_sec = 1;

        if (app_->start().has_value()) {
            break;
        }
        app_.reset();
        std::this_thread::sleep_for(100ms);
    }
    
    ASSERT_TRUE(app_ != nullptr) << "Failed to create app";
    ASSERT_TRUE(app_->is_running()) << "Failed to start app after retries";
    ASSERT_TRUE(wait_for_server_ready());
  }

  void TearDown() override {
    if (app_ && app_->is_running()) {
      app_->stop();
    }
    std::this_thread::sleep_for(kServerShutdownDelay);

    if (!temp_db_.empty()) {
      fs::remove(temp_db_);
    }
    if (!temp_dags_dir_.empty()) {
      fs::remove_all(temp_dags_dir_);
    }
  }

  [[nodiscard]] auto wait_for_server_ready() -> bool {
    auto deadline = std::chrono::steady_clock::now() + kServerStartupTimeout;
    while (std::chrono::steady_clock::now() < deadline) {
      if (app_->is_running() && app_->api_server() && app_->api_server()->is_running()) {
        auto [status, _] = http_get(app_->config().api.port, "/api/health");
        if (status == 200) return true;
      }
      std::this_thread::sleep_for(kPollInterval);
    }
    ADD_FAILURE() << "Server failed to start within timeout";
    return false;
  }


  void write_dag_file(const std::string& filename, const std::string& content) {
    std::string temp_file = fs::path(temp_dags_dir_) / (filename + ".tmp");
    std::string target_file = fs::path(temp_dags_dir_) / filename;
    {
        std::ofstream out(temp_file);
        out << content;
        out.close();
    }
    fs::rename(temp_file, target_file);
  }

  // Poll for DAG to appear in API
  bool wait_for_dag_registration(const std::string& dag_id) {
    auto deadline = std::chrono::steady_clock::now() + 5s;
    while (std::chrono::steady_clock::now() < deadline) {
      auto [status, body] = http_get(app_->config().api.port, "/api/dags/" + dag_id);
      if (status == 200) return true;
      std::this_thread::sleep_for(kPollInterval);
    }
    return false;
  }

  // Poll for run completion
  std::pair<std::string, json> wait_for_run_completion(const std::string& run_id, std::chrono::seconds timeout = 10s) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
      auto [status, body] = http_get(app_->config().api.port, "/api/history/" + run_id);
      if (status == 200) {
        auto j = json::parse(body);
        std::string state = j["state"];
        if (state != "running" && state != "queued") {
          return {state, j};
        }
      }
      std::this_thread::sleep_for(kPollInterval);
    }
    return {"timeout", {}};
  }
  
  // Get tasks for a run
  json get_run_tasks(const std::string& run_id) {
    auto [status, body] = http_get(app_->config().api.port, "/api/runs/" + run_id + "/tasks");
    if (status != 200) return json::array();
    return json::parse(body)["tasks"];
  }

  // Trigger DAG
  std::string trigger_dag(const std::string& dag_id) {
    auto [status, body] = http_post(app_->config().api.port, "/api/dags/" + dag_id + "/trigger", "{}");
    if (status != 200 && status != 201) return "";
    return json::parse(body)["dag_run_id"];
  }

  std::string temp_db_;
  std::string temp_dags_dir_;
  std::unique_ptr<Application> app_;
};

TEST_F(ApiE2ETest, SensorsHappyPath) {
  std::string file_path = make_temp_path("sensor_target_");
  fs::remove(file_path);

  std::string dag_yaml = std::format(R"(
name: sensors_happy_dag
description: Test sensors happy path
tasks:
  - id: file_wait
    executor: sensor
    sensor_type: file
    sensor_path: {}
    sensor_interval: 1
    timeout: 5
    command: "dummy_file_wait"
  - id: http_check
    executor: sensor
    sensor_type: http
    sensor_url: http://127.0.0.1:{}/api/health
    sensor_expected_status: 200
    dependencies: [file_wait]
    command: "dummy_http_check"
  - id: command_check
    executor: sensor
    sensor_type: command
    command: "exit 0"
    dependencies: [http_check]
)", file_path, app_->config().api.port);

  write_dag_file("sensors_happy_dag.yaml", dag_yaml);
  ASSERT_TRUE(wait_for_dag_registration("sensors_happy_dag"));

  auto run_id = trigger_dag("sensors_happy_dag");
  ASSERT_FALSE(run_id.empty());

  std::this_thread::sleep_for(500ms);

  {
    std::ofstream f(file_path);
    f << "done";
  }

  auto [state, _] = wait_for_run_completion(run_id);
  EXPECT_EQ(state, "success");

  auto tasks = get_run_tasks(run_id);
  ASSERT_EQ(tasks.size(), 3u);
  for (const auto& task : tasks) {
      EXPECT_EQ(task["state"], "success") << "Task " << task["task_id"] << " failed";
  }
  
  fs::remove(file_path);
}

TEST_F(ApiE2ETest, SensorsSoftFail) {
  std::string dag_yaml = R"(
name: sensors_soft_fail_dag
tasks:
  - id: missing_file
    executor: sensor
    sensor_type: file
    sensor_path: /nonexistent/file/path/xyz_12345
    sensor_interval: 1
    timeout: 1
    soft_fail: true
    command: "dummy_missing_file"
  - id: downstream
    command: "echo done"
    dependencies: [missing_file]
    trigger_rule: all_done
)";

  write_dag_file("sensors_soft_fail_dag.yaml", dag_yaml);
  ASSERT_TRUE(wait_for_dag_registration("sensors_soft_fail_dag"));

  auto run_id = trigger_dag("sensors_soft_fail_dag");
  ASSERT_FALSE(run_id.empty());

  auto [state, _] = wait_for_run_completion(run_id);
  EXPECT_TRUE(state == "success" || state == "failed") << "Unexpected state: " << state;

  auto tasks = get_run_tasks(run_id);
  bool found_skipped = false;
  bool found_success = false;
  
  for (const auto& task : tasks) {
    if (task["task_id"] == "missing_file") {
        EXPECT_EQ(task["state"], "skipped");
        found_skipped = true;
    }
    if (task["task_id"] == "downstream") {
        EXPECT_EQ(task["state"], "success");
        found_success = true;
    }
  }
  EXPECT_TRUE(found_skipped);
  EXPECT_TRUE(found_success);
}

TEST_F(ApiE2ETest, TemplateVariables) {
  std::string dag_yaml = R"(
name: templates_dag
tasks:
  - id: echo_vars
    command: "echo 'DAG: {{dag_id}}, TASK: {{task_id}}, RUN: {{run_id}}'"
)";

  write_dag_file("templates_dag.yaml", dag_yaml);
  ASSERT_TRUE(wait_for_dag_registration("templates_dag"));

  auto run_id = trigger_dag("templates_dag");
  ASSERT_FALSE(run_id.empty());

  auto [state, _] = wait_for_run_completion(run_id);
  EXPECT_EQ(state, "success");

  auto [status, body] = http_get(app_->config().api.port, 
      std::format("/api/runs/{}/tasks/echo_vars/logs", run_id));
  ASSERT_EQ(status, 200);
  
  auto logs = json::parse(body)["logs"];
  bool found_msg = false;
  for (const auto& log : logs) {
      std::string msg = log["message"];
      if (msg.find("DAG: templates_dag") != std::string::npos &&
          msg.find("TASK: echo_vars") != std::string::npos &&
          msg.find(run_id) != std::string::npos) {
          found_msg = true;
          break;
      }
  }
  EXPECT_TRUE(found_msg) << "Logs did not contain substituted variables: " << body;
}

TEST_F(ApiE2ETest, Branching) {
  std::string dag_yaml = R"(
name: branching_dag
tasks:
  - id: branch
    command: "echo '[\"task_a\"]'"
    is_branch: true
    xcom_push:
      - key: branch
        source: json
  - id: task_a
    command: "echo A"
    dependencies: [branch]
  - id: task_b
    command: "echo B"
    dependencies: [branch]
)";

  write_dag_file("branching_dag.yaml", dag_yaml);
  ASSERT_TRUE(wait_for_dag_registration("branching_dag"));

  auto run_id = trigger_dag("branching_dag");
  ASSERT_FALSE(run_id.empty());

  auto [state, _] = wait_for_run_completion(run_id);
  EXPECT_EQ(state, "success");

  auto tasks = get_run_tasks(run_id);
  for (const auto& task : tasks) {
    if (task["task_id"] == "task_a") EXPECT_EQ(task["state"], "success");
    if (task["task_id"] == "task_b") EXPECT_EQ(task["state"], "skipped");
  }
}

TEST_F(ApiE2ETest, TriggerRules) {
  std::string dag_yaml = R"(
name: trigger_rules_dag
tasks:
  - id: fail_task
    command: "exit 1"
    max_retries: 0
  - id: recover_task
    command: "echo recovered"
    dependencies: [fail_task]
    trigger_rule: one_failed
)";

  write_dag_file("trigger_rules_dag.yaml", dag_yaml);
  ASSERT_TRUE(wait_for_dag_registration("trigger_rules_dag"));

  auto run_id = trigger_dag("trigger_rules_dag");
  ASSERT_FALSE(run_id.empty());

  auto [state, _] = wait_for_run_completion(run_id);
  
  auto tasks = get_run_tasks(run_id);
  for (const auto& task : tasks) {
    if (task["task_id"] == "fail_task") EXPECT_EQ(task["state"], "failed");
    if (task["task_id"] == "recover_task") EXPECT_EQ(task["state"], "success");
  }
}

TEST_F(ApiE2ETest, SchedulingCatchup) {
  std::string start_date = "2025-01-01";

  std::string dag_catchup = std::format(R"(
name: schedule_catchup_dag
cron: "0 0 * * *"
start_date: "{}"
end_date: "2025-01-05"
catchup: true
tasks:
  - id: t1
    command: "echo hi"
)", start_date);

  write_dag_file("schedule_catchup_dag.yaml", dag_catchup);
  ASSERT_TRUE(wait_for_dag_registration("schedule_catchup_dag"));

  auto deadline = std::chrono::steady_clock::now() + 5s;
  int runs_count = 0;
  while (std::chrono::steady_clock::now() < deadline) {
      auto [status, body] = http_get(app_->config().api.port, "/api/dags/schedule_catchup_dag/history");
      if (status == 200) {
          auto runs = json::parse(body)["runs"];
          runs_count = runs.size();
          if (runs_count >= 4) break;
      } else {
         std::cout << "History poll failed: " << status << " body: " << body << std::endl;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
  EXPECT_GE(runs_count, 4) << "Should have backfilled runs since 5 days ago";
}

TEST_F(ApiE2ETest, SchedulingNoCatchup) {
   auto now = std::chrono::system_clock::now();
  auto five_days_ago = now - std::chrono::hours(24 * 5);
  auto time_val = std::chrono::system_clock::to_time_t(five_days_ago);
  std::tm tm{};
  localtime_r(&time_val, &tm);
  char buf[32];
  std::strftime(buf, sizeof(buf), "%Y-%m-%d", &tm);
  std::string start_date = buf;

  std::string dag_no_catchup = std::format(R"(
name: schedule_no_catchup_dag
cron: "0 0 * * *"
start_date: "{}"
catchup: false
tasks:
  - id: t1
    command: "echo hi"
)", start_date);

  write_dag_file("schedule_no_catchup_dag.yaml", dag_no_catchup);
  ASSERT_TRUE(wait_for_dag_registration("schedule_no_catchup_dag"));

  std::this_thread::sleep_for(3s);

  auto [status, body] = http_get(app_->config().api.port, "/api/dags/schedule_no_catchup_dag/history");
  int runs_count = 0;
  if (status == 200) {
      auto runs = json::parse(body)["runs"];
      runs_count = runs.size();
  }
  EXPECT_EQ(runs_count, 0) << "Should not backfill when catchup=false";
}

TEST_F(ApiE2ETest, CatchupDeduplication) {
  std::string start_date = "2025-01-01";
  std::string end_date = "2025-01-05"; 

  std::string dag_yaml = std::format(R"(
name: dedupe_dag
cron: "0 0 * * *"
start_date: "{}"
end_date: "{}"
catchup: true
tasks:
  - id: t1
    command: "echo hi"
)", start_date, end_date);

  write_dag_file("dedupe_dag.yaml", dag_yaml);
  ASSERT_TRUE(wait_for_dag_registration("dedupe_dag"));

  auto deadline = std::chrono::steady_clock::now() + 10s;
  int runs_count_1 = 0;
  while (std::chrono::steady_clock::now() < deadline) {
      auto [status, body] = http_get(app_->config().api.port, "/api/dags/dedupe_dag/history");
      if (status == 200) {
          auto runs = json::parse(body)["runs"];
          runs_count_1 = runs.size();
          if (runs_count_1 >= 5) break; 
      }
      std::this_thread::sleep_for(200ms);
  }
  EXPECT_GE(runs_count_1, 4);

  // Restart application
  app_->stop();
  app_.reset();

  // Create new app instance reusing same DB and DAGs
  for (int i = 0; i < 5; ++i) {
      app_ = std::make_unique<Application>(temp_db_);
      auto& cfg = app_->config();
      cfg.api.enabled = true;
      cfg.api.port = pick_unused_tcp_port();
      cfg.dag_source.mode = DAGSourceMode::File;
      cfg.dag_source.directory = temp_dags_dir_;
      cfg.dag_source.scan_interval_sec = 1;
      
      if (app_->init().has_value() && app_->start().has_value()) {
          break;
      }
      app_.reset();
      std::this_thread::sleep_for(100ms);
  }
  
  ASSERT_TRUE(app_ != nullptr);
  ASSERT_TRUE(app_->is_running());
  ASSERT_TRUE(wait_for_server_ready());

  ASSERT_TRUE(wait_for_dag_registration("dedupe_dag"));

  std::this_thread::sleep_for(3s);

  auto [status, body] = http_get(app_->config().api.port, "/api/dags/dedupe_dag/history");
  ASSERT_EQ(status, 200);
  auto runs = json::parse(body)["runs"];
  int runs_count_2 = runs.size();

  EXPECT_EQ(runs_count_2, runs_count_1) << "Duplicate runs created after restart!";
}

TEST_F(ApiE2ETest, MaxConcurrencyEnforced) {
  app_->stop();
  app_.reset();

  for (int i = 0; i < 5; ++i) {
      Config config;
      config.storage.db_file = temp_db_;
      config.scheduler.max_concurrency = 1;
      config.api.enabled = true;
      config.api.port = pick_unused_tcp_port();
      config.dag_source.mode = DAGSourceMode::File;
      config.dag_source.directory = temp_dags_dir_;
      config.dag_source.scan_interval_sec = 1;

      app_ = std::make_unique<Application>(std::move(config));

      if (app_->init().has_value() && app_->start().has_value()) {
          break;
      }
      app_.reset();
      std::this_thread::sleep_for(100ms);
  }

  ASSERT_TRUE(app_ != nullptr);
  ASSERT_TRUE(app_->is_running());
  ASSERT_TRUE(wait_for_server_ready());


  std::string dag_yaml = R"(
name: concurrency_dag
tasks:
  - id: t1
    command: "sleep 2"
  - id: t2
    command: "sleep 2"
  - id: t3
    command: "sleep 2"
)";

  write_dag_file("concurrency_dag.yaml", dag_yaml);
  ASSERT_TRUE(wait_for_dag_registration("concurrency_dag"));

  auto start = std::chrono::steady_clock::now();
  auto run_id = trigger_dag("concurrency_dag");
  ASSERT_FALSE(run_id.empty());

  auto [state, _] = wait_for_run_completion(run_id, 20s);
  auto end = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  EXPECT_EQ(state, "success");
  
  EXPECT_GT(duration.count(), 4000) << "Tasks ran in parallel despite max_concurrency=1";
}


