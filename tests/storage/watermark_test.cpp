#include "taskmaster/storage/persistence.hpp"
#include "taskmaster/util/id.hpp"

#include <filesystem>
#include <gtest/gtest.h>
#include <unistd.h>
#include <chrono>

using namespace taskmaster;

class PersistenceWatermarkTest : public ::testing::Test {
protected:
  void SetUp() override {
    std::string tmp_pattern = "/tmp/taskmaster_watermark_test_XXXXXX";
    int fd = ::mkstemp(tmp_pattern.data());
    ASSERT_GE(fd, 0) << "Failed to create temp file";
    ::close(fd);
    test_db_path_ = tmp_pattern + ".db";
    std::filesystem::rename(tmp_pattern, test_db_path_);
    persistence_ = std::make_unique<Persistence>(test_db_path_);
    ASSERT_TRUE(persistence_->open().has_value());
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

TEST_F(PersistenceWatermarkTest, SaveWatermark_Succeeds) {
  auto now = std::chrono::system_clock::now();
  auto dag_id = DAGId{"dag1"};
  
  auto result = persistence_->save_watermark(dag_id, now);
  EXPECT_TRUE(result.has_value());
}

TEST_F(PersistenceWatermarkTest, GetWatermark_WhenExists_ReturnsValue) {
  auto now = std::chrono::system_clock::now();
  // Round to milliseconds as sqlite stores as int64 ms
  auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
  auto expected_time = std::chrono::system_clock::time_point(now_ms);
  
  auto dag_id = DAGId{"dag1"};
  
  ASSERT_TRUE(persistence_->save_watermark(dag_id, expected_time).has_value());
  
  auto result = persistence_->get_watermark(dag_id);
  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->has_value());
  EXPECT_EQ(**result, expected_time);
}

TEST_F(PersistenceWatermarkTest, GetWatermark_WhenNotExists_ReturnsNullopt) {
  auto dag_id = DAGId{"dag_nonexistent"};
  
  auto result = persistence_->get_watermark(dag_id);
  ASSERT_TRUE(result.has_value());
  EXPECT_FALSE(result->has_value());
}

TEST_F(PersistenceWatermarkTest, SaveWatermark_OverwritesExisting) {
  auto now = std::chrono::system_clock::now();
  auto dag_id = DAGId{"dag1"};
  
  ASSERT_TRUE(persistence_->save_watermark(dag_id, now).has_value());
  
  auto later = now + std::chrono::hours(1);
  auto later_ms = std::chrono::duration_cast<std::chrono::milliseconds>(later.time_since_epoch());
  auto expected_time = std::chrono::system_clock::time_point(later_ms);
  
  ASSERT_TRUE(persistence_->save_watermark(dag_id, expected_time).has_value());
  
  auto result = persistence_->get_watermark(dag_id);
  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->has_value());
  EXPECT_EQ(**result, expected_time);
}
