#include "taskmaster/dag/dag_manager.hpp"
#include "taskmaster/storage/persistence.hpp"
#include "taskmaster/util/id.hpp"

#include <cstring>
#include <filesystem>
#include <optional>

#include "gtest/gtest.h"

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
