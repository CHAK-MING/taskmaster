#include "taskmaster/dag/dag_manager.hpp"
#include "taskmaster/storage/persistence.hpp"
#include "taskmaster/storage/state_strings.hpp"
#include "taskmaster/util/id.hpp"

#include <cstring>
#include <filesystem>
#include <format>
#include <optional>

#include "gtest/gtest.h"
#include <sqlite3.h>

#include <unistd.h>

using namespace taskmaster;

class PersistenceTest : public ::testing::Test {
protected:
  void SetUp() override {
    std::string tmp_pattern = "/tmp/taskmaster_test_XXXXXX";
    int fd = ::mkstemp(tmp_pattern.data());
    ASSERT_GE(fd, 0) << "Failed to create temp file: " << std::strerror(errno);
    ::close(fd);
    test_db_path_ = tmp_pattern + ".db";
    std::filesystem::rename(tmp_pattern, test_db_path_);
    persistence_ = std::make_unique<Persistence>(test_db_path_);
  }

  void TearDown() override {
    persistence_.reset();
    if (!test_db_path_.empty()) {
      std::filesystem::remove(test_db_path_);
    }
  }

  std::string test_db_path_;
  std::unique_ptr<Persistence> persistence_;
};

class OpenPersistenceTest : public PersistenceTest {
protected:
  void SetUp() override {
    PersistenceTest::SetUp();
    ASSERT_TRUE(persistence_->open().has_value());
  }

  void TearDown() override {
    persistence_->close();
    PersistenceTest::TearDown();
  }
};

TEST_F(PersistenceTest, InitialState_IsNotOpen) {
  EXPECT_FALSE(persistence_->is_open());
}

TEST_F(PersistenceTest, Open_Succeeds) {
  auto result = persistence_->open();

  EXPECT_TRUE(result.has_value());
  EXPECT_TRUE(persistence_->is_open());
}

TEST_F(PersistenceTest, Close_AfterOpen_ClosesDatabase) {
  ASSERT_TRUE(persistence_->open().has_value());

  persistence_->close();

  EXPECT_FALSE(persistence_->is_open());
}

TEST_F(PersistenceTest, DoubleOpen_IsIdempotent) {
  ASSERT_TRUE(persistence_->open().has_value());

  auto result = persistence_->open();

  EXPECT_TRUE(result.has_value());
}

TEST_F(PersistenceTest, CloseWithoutOpen_IsNoOp) {
  persistence_->close();

  EXPECT_FALSE(persistence_->is_open());
}

TEST_F(OpenPersistenceTest, ListDags_OnEmptyDb_Succeeds) {
  auto result = persistence_->list_dags();

  EXPECT_TRUE(result.has_value());
}

TEST_F(OpenPersistenceTest, BeginTransaction_Succeeds) {
  auto result = persistence_->begin_transaction();

  EXPECT_TRUE(result.has_value());
}

TEST_F(OpenPersistenceTest, CommitTransaction_AfterBegin_Succeeds) {
  ASSERT_TRUE(persistence_->begin_transaction().has_value());

  auto result = persistence_->commit_transaction();

  EXPECT_TRUE(result.has_value());
}

TEST_F(OpenPersistenceTest, RollbackTransaction_AfterBegin_Succeeds) {
  ASSERT_TRUE(persistence_->begin_transaction().has_value());

  auto result = persistence_->rollback_transaction();

  EXPECT_TRUE(result.has_value());
}

TEST_F(OpenPersistenceTest, MultipleTransactions_AllSucceed) {
  constexpr int kTransactionCount = 5;

  for (int i = 0; i < kTransactionCount; ++i) {
    ASSERT_TRUE(persistence_->begin_transaction().has_value());
    ASSERT_TRUE(persistence_->commit_transaction().has_value());
  }
}

TEST_F(OpenPersistenceTest, GetDagRunState_NonexistentId_ReturnsEmpty) {
  auto result = persistence_->get_dag_run_state(DAGRunId("nonexistent"));

  EXPECT_FALSE(result.has_value());
}

TEST_F(OpenPersistenceTest, GetIncompleteDagRuns_OnEmptyDb_ReturnsEmptyList) {
  auto result = persistence_->get_incomplete_dag_runs();

  EXPECT_TRUE(result.has_value());
}

TEST_F(OpenPersistenceTest, ListRunHistory_NoFilters_Succeeds) {
  auto result = persistence_->list_run_history();

  EXPECT_TRUE(result.has_value());
}

TEST_F(OpenPersistenceTest, ListRunHistory_WithDagIdFilter_Succeeds) {
  auto result = persistence_->list_run_history(DAGId("test_dag"));

  EXPECT_TRUE(result.has_value());
}

TEST_F(OpenPersistenceTest, ListRunHistory_WithLimitFilter_Succeeds) {
  constexpr size_t kLimit = 10;

  auto result = persistence_->list_run_history(std::nullopt, kLimit);

  EXPECT_TRUE(result.has_value());
}

TEST_F(OpenPersistenceTest, GetRunHistory_NonexistentId_ReturnsEmpty) {
  auto result = persistence_->get_run_history(DAGRunId("nonexistent"));

  EXPECT_FALSE(result.has_value());
}

TEST_F(OpenPersistenceTest, GetTaskInstances_NonexistentRun_ReturnsEmptyList) {
  auto result = persistence_->get_task_instances(DAGRunId("nonexistent"));

  EXPECT_TRUE(result.has_value());
}

TEST_F(PersistenceTest, Open_ReadOnlyPath_Fails) {
  auto readonly_persistence = std::make_unique<Persistence>("/nonexistent_dir/test.db");

  auto result = readonly_persistence->open();

  EXPECT_FALSE(result.has_value());
}

TEST_F(OpenPersistenceTest, CommitTransaction_WithoutBegin_Fails) {
  auto result = persistence_->commit_transaction();

  EXPECT_FALSE(result.has_value());
}

TEST_F(OpenPersistenceTest, RollbackTransaction_WithoutBegin_Fails) {
  auto result = persistence_->rollback_transaction();

  EXPECT_FALSE(result.has_value());
}

class GetPreviousTaskStateTest : public OpenPersistenceTest {
protected:
  auto make_execution_date(int day) -> std::chrono::system_clock::time_point {
    std::tm tm{};
    tm.tm_year = 2026 - 1900;
    tm.tm_mon = 0;
    tm.tm_mday = day;
    tm.tm_hour = 0;
    tm.tm_min = 0;
    tm.tm_sec = 0;
    return std::chrono::system_clock::from_time_t(timegm(&tm));
  }

  auto create_dag_run_with_task(const std::string& dag_id, int day, TaskState state)
      -> void {
    auto exec_date = make_execution_date(day);
    auto dag_run_id_str = std::format("{}_{}", dag_id, day);
    auto dag_run_id = DAGRunId(dag_run_id_str);

    sqlite3* db = nullptr;
    sqlite3_open(test_db_path_.c_str(), &db);

    auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                  exec_date.time_since_epoch())
                  .count();

    std::string insert_run = std::format(
        "INSERT INTO dag_runs (dag_run_id, state, execution_date) "
        "VALUES ('{}', 'success', {})",
        dag_run_id_str, ts);
    sqlite3_exec(db, insert_run.c_str(), nullptr, nullptr, nullptr);

    std::string insert_task = std::format(
        "INSERT INTO task_instances (dag_run_id, task_id, state) "
        "VALUES ('{}', '0', '{}')",
        dag_run_id_str, task_state_name(state));
    sqlite3_exec(db, insert_task.c_str(), nullptr, nullptr, nullptr);

    sqlite3_close(db);
  }
};

TEST_F(GetPreviousTaskStateTest, NoPriorRuns_ReturnsNullopt) {
  auto result = persistence_->get_previous_task_state(
      DAGId("test_dag"), 0, make_execution_date(15), "test_dag_current_run");

  ASSERT_TRUE(result.has_value());
  EXPECT_FALSE(result->has_value());
}

TEST_F(GetPreviousTaskStateTest, PreviousRunSuccess_ReturnsSuccess) {
  create_dag_run_with_task("test_dag", 10, TaskState::Success);

  auto result = persistence_->get_previous_task_state(
      DAGId("test_dag"), 0, make_execution_date(15), "test_dag_current_run");

  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->has_value());
  EXPECT_EQ(**result, TaskState::Success);
}

TEST_F(GetPreviousTaskStateTest, PreviousRunFailed_ReturnsFailed) {
  create_dag_run_with_task("test_dag", 10, TaskState::Failed);

  auto result = persistence_->get_previous_task_state(
      DAGId("test_dag"), 0, make_execution_date(15), "test_dag_current_run");

  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->has_value());
  EXPECT_EQ(**result, TaskState::Failed);
}

TEST_F(GetPreviousTaskStateTest, MultipleRuns_ReturnsMostRecent) {
  create_dag_run_with_task("test_dag", 5, TaskState::Failed);
  create_dag_run_with_task("test_dag", 10, TaskState::Success);

  auto result = persistence_->get_previous_task_state(
      DAGId("test_dag"), 0, make_execution_date(15), "test_dag_current_run");

  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->has_value());
  EXPECT_EQ(**result, TaskState::Success);
}

TEST_F(GetPreviousTaskStateTest, IgnoresFutureRuns) {
  create_dag_run_with_task("test_dag", 10, TaskState::Failed);
  create_dag_run_with_task("test_dag", 20, TaskState::Success);

  auto result = persistence_->get_previous_task_state(
      DAGId("test_dag"), 0, make_execution_date(15), "test_dag_current_run");

  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->has_value());
  EXPECT_EQ(**result, TaskState::Failed);
}

TEST_F(GetPreviousTaskStateTest, IgnoresOtherDags) {
  create_dag_run_with_task("other_dag", 10, TaskState::Success);

  auto result = persistence_->get_previous_task_state(
      DAGId("test_dag"), 0, make_execution_date(15), "test_dag_current_run");

  ASSERT_TRUE(result.has_value());
  EXPECT_FALSE(result->has_value());
}
