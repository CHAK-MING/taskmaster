#include <gtest/gtest.h>
#include "taskmaster/app/application.hpp"
#include "taskmaster/storage/recovery.hpp"
#include "taskmaster/config/task_config.hpp"
#include "taskmaster/util/id.hpp"
#include <filesystem>
#include <csignal>
#include <sys/wait.h>
#include <unistd.h>
#include <thread>
#include <chrono>
#include <cstring>

using namespace taskmaster;
using namespace std::chrono_literals;
namespace fs = std::filesystem;

namespace {

constexpr auto kAppStartupDelay = std::chrono::milliseconds(200);
constexpr auto kTaskExecutionDelay = std::chrono::milliseconds(500);
constexpr auto kRecoveryTimeout = std::chrono::seconds(5);
constexpr auto kPollInterval = std::chrono::milliseconds(50);

}

class CrashRecoveryTest : public ::testing::Test {
protected:
  void SetUp() override {
    temp_db_ = create_temp_db();
  }

  void TearDown() override {
    if (!temp_db_.empty() && fs::exists(temp_db_)) {
      fs::remove(temp_db_);
    }
  }

  auto create_temp_db() -> std::string {
    std::string templ = "/tmp/taskmaster_crash_test_XXXXXX";
    int fd = ::mkstemp(templ.data());
    EXPECT_GE(fd, 0) << "Failed to create temp file: " << std::strerror(errno);
    if (fd >= 0) {
      ::close(fd);
    }
    return templ;
  }

  auto create_dag_info(std::string name) -> DAGInfo {
    DAGInfo info;
    info.name = std::move(name);
    return info;
  }

  auto create_task(std::string id, std::string name, std::string command,
                   std::vector<TaskId> deps = {}) -> TaskConfig {
    TaskConfig task;
    task.task_id = TaskId(std::move(id));
    task.name = std::move(name);
    task.command = std::move(command);
    task.dependencies = std::move(deps);
    return task;
  }

  auto wait_for_run_state(Application& app, const DAGId& dag_id,
                          DAGRunState expected_state) -> bool {
    auto deadline = std::chrono::steady_clock::now() + kRecoveryTimeout;
    while (std::chrono::steady_clock::now() < deadline) {
      auto history = app.persistence()->list_run_history(dag_id, 1);
      if (history.has_value() && !history->empty() &&
          history->front().state == expected_state) {
        return true;
      }
      std::this_thread::sleep_for(kPollInterval);
    }
    return false;
  }

  std::string temp_db_;
  DAGId dag_id1_{""};
  DAGId dag_id2_{""};
  DAGId dag_id3_{""};
};

TEST_F(CrashRecoveryTest, RecoverSingleRunningTask) {
  {
    Application app(temp_db_);
    ASSERT_TRUE(app.start().has_value());
    std::this_thread::sleep_for(kAppStartupDelay);

    dag_id1_ = DAGId("crash_test_dag");
    auto dag_result = app.dag_manager().create_dag(dag_id1_, create_dag_info("Crash Test"));
    ASSERT_TRUE(dag_result.has_value());

    auto task = create_task("long_task", "Long Running Task", "sleep 1");
    ASSERT_TRUE(app.dag_manager().add_task(dag_id1_, task).has_value());

    auto run_id = app.trigger_dag_by_id(dag_id1_);
    ASSERT_TRUE(run_id.has_value());

    std::this_thread::sleep_for(kTaskExecutionDelay);

    auto state = app.persistence()->get_dag_run_state(*run_id);
    ASSERT_TRUE(state.has_value());
    EXPECT_EQ(*state, DAGRunState::Running);

    app.stop();
  }

  {
    Application app(temp_db_);
    ASSERT_TRUE(app.start().has_value());
    std::this_thread::sleep_for(kAppStartupDelay);

    auto recover_result = app.recover_from_crash();
    ASSERT_TRUE(recover_result.has_value());

    ASSERT_TRUE(wait_for_run_state(app, dag_id1_, DAGRunState::Success));

    auto history = app.persistence()->list_run_history(dag_id1_, 10);
    ASSERT_TRUE(history.has_value());
    ASSERT_FALSE(history->empty());

    auto& run = history->front();
    EXPECT_EQ(run.state, DAGRunState::Success);

    app.stop();
  }
}

TEST_F(CrashRecoveryTest, RecoverMultipleIncompleteDagRuns) {
  {
    Application app(temp_db_);
    ASSERT_TRUE(app.start().has_value());
    std::this_thread::sleep_for(kAppStartupDelay);

    dag_id1_ = DAGId("dag1");
    dag_id2_ = DAGId("dag2");

    auto dag1 = app.dag_manager().create_dag(dag_id1_, create_dag_info("DAG 1"));
    auto dag2 = app.dag_manager().create_dag(dag_id2_, create_dag_info("DAG 2"));
    ASSERT_TRUE(dag1.has_value() && dag2.has_value());

    auto task1 = create_task("task1", "Task 1", "sleep 1");
    auto task2 = create_task("task2", "Task 2", "sleep 1");

    ASSERT_TRUE(app.dag_manager().add_task(dag_id1_, task1).has_value());
    ASSERT_TRUE(app.dag_manager().add_task(dag_id2_, task2).has_value());

    auto run1 = app.trigger_dag_by_id(dag_id1_);
    auto run2 = app.trigger_dag_by_id(dag_id2_);
    ASSERT_TRUE(run1.has_value() && run2.has_value());

    std::this_thread::sleep_for(kTaskExecutionDelay);

    app.stop();
  }

  {
    Application app(temp_db_);
    ASSERT_TRUE(app.start().has_value());
    std::this_thread::sleep_for(kAppStartupDelay);

    auto recover_result = app.recover_from_crash();
    ASSERT_TRUE(recover_result.has_value());

    ASSERT_TRUE(wait_for_run_state(app, dag_id1_, DAGRunState::Success));
    ASSERT_TRUE(wait_for_run_state(app, dag_id2_, DAGRunState::Success));

    auto history1 = app.persistence()->list_run_history(dag_id1_, 10);
    auto history2 = app.persistence()->list_run_history(dag_id2_, 10);

    ASSERT_TRUE(history1.has_value() && history2.has_value());
    ASSERT_FALSE(history1->empty());
    ASSERT_FALSE(history2->empty());

    EXPECT_EQ(history1->front().state, DAGRunState::Success);
    EXPECT_EQ(history2->front().state, DAGRunState::Success);

    app.stop();
  }
}

TEST_F(CrashRecoveryTest, RecoverWithTaskDependencies) {
  {
    Application app(temp_db_);
    ASSERT_TRUE(app.start().has_value());
    std::this_thread::sleep_for(kAppStartupDelay);

    dag_id3_ = DAGId("dep_dag");
    auto dag_result = app.dag_manager().create_dag(dag_id3_, create_dag_info("Dependency DAG"));
    ASSERT_TRUE(dag_result.has_value());

    auto t1 = create_task("t1", "Task 1", "sleep 1");
    auto t2 = create_task("t2", "Task 2", "sleep 1", {TaskId("t1")});
    auto t3 = create_task("t3", "Task 3", "sleep 1", {TaskId("t2")});

    ASSERT_TRUE(app.dag_manager().add_task(dag_id3_, t1).has_value());
    ASSERT_TRUE(app.dag_manager().add_task(dag_id3_, t2).has_value());
    ASSERT_TRUE(app.dag_manager().add_task(dag_id3_, t3).has_value());

    auto run_id = app.trigger_dag_by_id(dag_id3_);
    ASSERT_TRUE(run_id.has_value());

    std::this_thread::sleep_for(kTaskExecutionDelay);

    app.stop();
  }

  {
    Application app(temp_db_);
    ASSERT_TRUE(app.start().has_value());
    std::this_thread::sleep_for(kAppStartupDelay);

    auto recover_result = app.recover_from_crash();
    ASSERT_TRUE(recover_result.has_value());

    ASSERT_TRUE(wait_for_run_state(app, dag_id3_, DAGRunState::Success));

    auto history = app.persistence()->list_run_history(dag_id3_, 10);
    ASSERT_TRUE(history.has_value());
    ASSERT_FALSE(history->empty());

    EXPECT_EQ(history->front().state, DAGRunState::Success);

    auto tasks = app.persistence()->get_task_instances(history->front().dag_run_id);
    ASSERT_TRUE(tasks.has_value());
    EXPECT_EQ(tasks->size(), 3u);

    for (const auto& task : *tasks) {
      EXPECT_EQ(task.state, TaskState::Success);
    }

    app.stop();
  }
}

TEST_F(CrashRecoveryTest, NoRecoveryNeededWhenNoCrash) {
  Application app(temp_db_);
  ASSERT_TRUE(app.start().has_value());
  std::this_thread::sleep_for(kAppStartupDelay);

  auto recover_result = app.recover_from_crash();
  ASSERT_TRUE(recover_result.has_value());

  auto history = app.persistence()->list_run_history(std::nullopt, 100);
  ASSERT_TRUE(history.has_value());
  EXPECT_TRUE(history->empty());

  app.stop();
}
