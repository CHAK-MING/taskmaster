#include "taskmaster/app/api/api_server.hpp"
#include "taskmaster/app/http/websocket.hpp"
#include "taskmaster/app/application.hpp"
#include "taskmaster/dag/dag_manager.hpp"

#include <chrono>
#include <thread>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#include "gtest/gtest.h"
#include "test_utils.hpp"

using namespace taskmaster;

namespace {

constexpr auto kServerStartupDelay = std::chrono::milliseconds(100);
constexpr auto kServerShutdownDelay = std::chrono::milliseconds(100);
constexpr uint16_t kBaseTestPort = 18080;

}  // namespace

class APITest : public ::testing::Test {
protected:
  void SetUp() override {
    app_ = std::make_unique<Application>();
    server_ = std::make_unique<ApiServer>(*app_);
  }

  void TearDown() override {
    if (server_ && server_->is_running()) {
      server_->stop();
    }
    if (app_ && app_->is_running()) {
      app_->stop();
    }
  }

  [[nodiscard]] static auto create_simple_dag_info(std::string_view name)
      -> DAGInfo {
    DAGInfo info;
    info.name = std::string(name);
    return info;
  }

  [[nodiscard]] static auto create_dag_info_with_task(std::string_view name,
                                                       TaskId task_id)
      -> DAGInfo {
    DAGInfo info;
    info.name = std::string(name);

    TaskConfig task;
    task.task_id = task_id;
    task.name = std::string(task_id.value());
    task.command = "echo hello";
    task.executor = ExecutorType::Shell;
    task.execution_timeout = std::chrono::seconds(30);
    task.max_retries = 0;

    info.tasks.push_back(task);
    info.rebuild_task_index();
    return info;
  }

  std::unique_ptr<Application> app_;
  std::unique_ptr<ApiServer> server_;
};

class APIServerTest : public APITest {
protected:
  void SetUp() override {
    APITest::SetUp();
    assign_unique_port();
    start_application();
    start_server();
  }

  void TearDown() override {
    stop_server();
    stop_application();
    APITest::TearDown();
  }

  void assign_unique_port() {
    app_->config().api.port = next_port_++;
  }

  void start_application() {
    auto result = app_->start();
    ASSERT_TRUE(result.has_value()) << "Failed to start application";
  }

  void stop_application() {
    if (app_ && app_->is_running()) {
      app_->stop();
    }
  }

  void start_server() {
    server_->start();
    std::this_thread::sleep_for(kServerStartupDelay);
  }

  void stop_server() {
    if (server_ && server_->is_running()) {
      server_->stop();
      std::this_thread::sleep_for(kServerShutdownDelay);
    }
  }

private:
  static inline std::atomic<uint16_t> next_port_{kBaseTestPort};
};

TEST_F(APITest, InitialServerState_IsNotRunning) {
  EXPECT_FALSE(server_->is_running());
}

TEST_F(APITest, WebSocketHub_InitialConnectionCount_IsZero) {
  auto& hub = server_->websocket_hub();
  EXPECT_EQ(hub.connection_count(), 0);
}

TEST_F(APITest, ApplicationAccess_InitialDAGCount_IsZero) {
  EXPECT_EQ(app_->dag_manager().dag_count(), 0);
}

TEST_F(APIServerTest, ServerLifecycle_StartAndStop) {
  // Arrange
  ASSERT_TRUE(server_->is_running());

  // Act
  stop_server();

  // Assert
  EXPECT_FALSE(server_->is_running());
}

TEST_F(APIServerTest, ServerLifecycle_MultipleStartCalls_IsIdempotent) {
  // Arrange
  ASSERT_TRUE(server_->is_running());

  // Act
  server_->start();

  // Assert
  EXPECT_TRUE(server_->is_running());
}

TEST_F(APIServerTest, ServerLifecycle_MultipleStopCalls_IsIdempotent) {
  // Arrange
  ASSERT_TRUE(server_->is_running());

  // Act
  stop_server();
  server_->stop();

  // Assert
  EXPECT_FALSE(server_->is_running());
}

TEST_F(APITest, CreateDAG_WithValidInfo_Succeeds) {
  // Arrange
  DAGId dag_id{"test_dag"};
  auto info = create_dag_info_with_task("Test DAG", TaskId{"task1"});
  info.description = "Test Description";

  // Act
  auto result = app_->dag_manager().create_dag(dag_id, info);

  // Assert
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(app_->dag_manager().dag_count(), 1);

  auto dag_info = app_->dag_manager().get_dag(dag_id);
  ASSERT_TRUE(dag_info.has_value());
  EXPECT_EQ(dag_info->dag_id, dag_id);
  EXPECT_EQ(dag_info->name, "Test DAG");
  EXPECT_EQ(dag_info->tasks.size(), 1);
}

TEST_F(APITest, ListDAGs_WithMultipleDAGs_ReturnsAll) {
  // Arrange
  ASSERT_TRUE(app_->dag_manager().create_dag(DAGId{"dag1"}, create_simple_dag_info("DAG 1")).has_value());
  ASSERT_TRUE(app_->dag_manager().create_dag(DAGId{"dag2"}, create_simple_dag_info("DAG 2")).has_value());

  // Act
  auto dags = app_->dag_manager().list_dags();

  // Assert
  EXPECT_EQ(dags.size(), 2);
}

TEST_F(APITest, DeleteDAG_ExistingDAG_RemovesIt) {
  // Arrange
  DAGId dag_id{"delete_test"};
  ASSERT_TRUE(app_->dag_manager().create_dag(dag_id, create_simple_dag_info("Delete Test")).has_value());
  ASSERT_EQ(app_->dag_manager().dag_count(), 1);

  // Act
  auto result = app_->dag_manager().delete_dag(dag_id);

  // Assert
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(app_->dag_manager().dag_count(), 0);
}

TEST_F(APITest, TriggerDAG_ValidDAG_ReturnsRunId) {
  // Arrange
  DAGId dag_id{"trigger_test"};
  auto info = create_dag_info_with_task("Trigger Test", TaskId{"task1"});
  ASSERT_TRUE(app_->dag_manager().create_dag(dag_id, info).has_value());
  ASSERT_TRUE(app_->start().has_value());
  std::this_thread::sleep_for(kServerStartupDelay);

  // Act
  auto dag_run_id = app_->trigger_dag_by_id(dag_id, TriggerType::Manual);

  // Assert
  ASSERT_TRUE(dag_run_id.has_value());
  EXPECT_FALSE(dag_run_id->empty());

  app_->stop();
}

TEST_F(APITest, CreateDAG_DuplicateId_Fails) {
  // Arrange
  DAGId dag_id{"duplicate_dag"};
  ASSERT_TRUE(app_->dag_manager().create_dag(dag_id, create_simple_dag_info("First")).has_value());

  // Act
  auto result = app_->dag_manager().create_dag(dag_id, create_simple_dag_info("Second"));

  // Assert
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(app_->dag_manager().dag_count(), 1);
}

TEST_F(APITest, DeleteDAG_NonExistentId_Fails) {
  // Arrange
  DAGId non_existent_id{"does_not_exist"};

  // Act
  auto result = app_->dag_manager().delete_dag(non_existent_id);

  // Assert
  EXPECT_FALSE(result.has_value());
}

TEST_F(APITest, GetDAG_NonExistentId_ReturnsEmpty) {
  // Arrange
  DAGId non_existent_id{"does_not_exist"};

  // Act
  auto result = app_->dag_manager().get_dag(non_existent_id);

  // Assert
  EXPECT_FALSE(result.has_value());
}

TEST_F(APITest, TriggerDAG_NonExistentId_Fails) {
  // Arrange
  DAGId non_existent_id{"does_not_exist"};
  ASSERT_TRUE(app_->start().has_value());
  std::this_thread::sleep_for(kServerStartupDelay);

  // Act
  auto result = app_->trigger_dag_by_id(non_existent_id, TriggerType::Manual);

  // Assert
  EXPECT_FALSE(result.has_value());

  app_->stop();
}
