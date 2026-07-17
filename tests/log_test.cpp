#include "dagforge/util/log.hpp"
#include "test_utils.hpp"

#include "gtest/gtest.h"

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <iterator>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace {

auto read_file(const std::string &path) -> std::string {
  std::ifstream input(path, std::ios::binary);
  return {std::istreambuf_iterator<char>{input},
          std::istreambuf_iterator<char>{}};
}

class RecordingSink final : public dagforge::log::Sink {
public:
  [[nodiscard]] auto supports_color() const noexcept -> bool override {
    return false;
  }

  [[nodiscard]] auto write(const dagforge::log::Record &record,
                           std::string_view rendered)
      -> dagforge::Result<void> override {
    std::lock_guard lock(mutex_);
    records_.push_back(record);
    rendered_.append(rendered);
    return dagforge::ok();
  }

  [[nodiscard]] auto flush() -> dagforge::Result<void> override {
    flushes_.fetch_add(1, std::memory_order_relaxed);
    return dagforge::ok();
  }

  [[nodiscard]] auto records() const -> std::vector<dagforge::log::Record> {
    std::lock_guard lock(mutex_);
    return records_;
  }

  [[nodiscard]] auto rendered() const -> std::string {
    std::lock_guard lock(mutex_);
    return rendered_;
  }

  [[nodiscard]] auto flushes() const noexcept -> std::uint64_t {
    return flushes_.load(std::memory_order_relaxed);
  }

private:
  mutable std::mutex mutex_;
  std::vector<dagforge::log::Record> records_;
  std::string rendered_;
  std::atomic<std::uint64_t> flushes_{0};
};

} // namespace

TEST(LoggerTest, RepeatedStartStopCyclesDoNotCrash) {
  const auto path = dagforge::test::make_temp_path("dagforge_log_cycle_");
  ASSERT_FALSE(path.empty());

  for (int i = 0; i < 5; ++i) {
    ASSERT_TRUE(dagforge::log::set_output_file(path).has_value());
    dagforge::log::start();
    dagforge::log::info("cycle {}", i);
    dagforge::log::stop();
  }

  dagforge::log::set_output_stderr();
  std::remove(path.c_str());
}

TEST(LoggerTest, StoppedLoggerWritesSynchronouslyAndFiltersByLevel) {
  const auto path = dagforge::test::make_temp_path("dagforge_log_sync_");
  ASSERT_FALSE(path.empty());

  {
    dagforge::log::Logger logger;
    logger.set_level(dagforge::log::Level::Warn);
    EXPECT_EQ(logger.level(), dagforge::log::Level::Warn);
    ASSERT_TRUE(logger.set_output_file(path).has_value());
    logger.log(dagforge::log::Level::Info, "hidden message");
    logger.log(dagforge::log::Level::Error, "visible {}", 42);
    logger.stop();
    ASSERT_TRUE(logger.set_output_file("").has_value());
  }

  const auto content = read_file(path);
  EXPECT_EQ(content.find("hidden message"), std::string::npos);
  EXPECT_NE(content.find("visible 42"), std::string::npos);
  std::remove(path.c_str());
}

TEST(LoggerTest, RejectsInvalidFileBeforeWriterStarts) {
  dagforge::log::Logger logger;
  EXPECT_FALSE(
      logger.set_output_file("/definitely/missing/dagforge/log/output.log")
          .has_value());
  logger.set_output_stderr();
}

TEST(LoggerTest, AsyncWriterFlushesBatchesAndSwitchesFilesInOrder) {
  const auto first = dagforge::test::make_temp_path("dagforge_log_first_");
  const auto second = dagforge::test::make_temp_path("dagforge_log_second_");
  ASSERT_FALSE(first.empty());
  ASSERT_FALSE(second.empty());

  dagforge::log::Logger logger;
  logger.set_level(dagforge::log::Level::Trace);
  logger.start();
  logger.start();
  ASSERT_TRUE(logger.set_output_file(first).has_value());
  for (int i = 0; i < 100; ++i) {
    logger.log(dagforge::log::Level::Debug, "first-batch {}", i);
  }
  ASSERT_TRUE(logger.flush().has_value());
  EXPECT_NE(read_file(first).find("first-batch 99"), std::string::npos);
  ASSERT_TRUE(logger.set_output_file(second).has_value());
  for (int i = 0; i < 100; ++i) {
    logger.log(dagforge::log::Level::Info, "second-batch {}", i);
  }
  logger.stop();
  logger.stop();

  const auto first_content = read_file(first);
  const auto second_content = read_file(second);
  EXPECT_NE(first_content.find("first-batch 0"), std::string::npos);
  EXPECT_NE(first_content.find("first-batch 99"), std::string::npos);
  EXPECT_NE(second_content.find("second-batch 0"), std::string::npos);
  EXPECT_NE(second_content.find("second-batch 99"), std::string::npos);

  std::remove(first.c_str());
  std::remove(second.c_str());
}

TEST(LoggerTest, FileSinkNeverReceivesAnsiSequences) {
  const auto path = dagforge::test::make_temp_path("dagforge_log_plain_");
  ASSERT_FALSE(path.empty());

  dagforge::log::Logger logger;
  logger.set_color_policy(dagforge::log::ColorPolicy::Always);
  ASSERT_EQ(logger.color_policy(), dagforge::log::ColorPolicy::Always);
  ASSERT_TRUE(logger.set_output_file(path).has_value());
  logger.log(dagforge::log::Level::Warn, "plain file output");
  ASSERT_TRUE(logger.flush().has_value());

  const auto content = read_file(path);
  EXPECT_NE(content.find("[warn]"), std::string::npos);
  EXPECT_EQ(content.find('\x1b'), std::string::npos);
  std::remove(path.c_str());
}

TEST(LoggerTest, InjectableSinkReceivesRecordAndCallSite) {
  auto sink = std::make_shared<RecordingSink>();
  dagforge::log::Logger logger;
  ASSERT_TRUE(logger.set_sink(sink).has_value());
  logger.log(dagforge::log::Level::Info, "record {}", 7);
  ASSERT_TRUE(logger.flush().has_value());

  const auto records = sink->records();
  ASSERT_EQ(records.size(), 1U);
  EXPECT_EQ(records.front().level, dagforge::log::Level::Info);
  EXPECT_EQ(records.front().message, "record 7");
  EXPECT_NE(
      std::string_view{records.front().origin.file_name()}.find("log_test.cpp"),
      std::string_view::npos);
  EXPECT_GT(records.front().origin.line(), 0U);
  EXPECT_NE(sink->rendered().find("[info]"), std::string::npos);
  EXPECT_GE(sink->flushes(), 1U);
}

TEST(LoggerTest, FlushReportsSinkFailures) {
  dagforge::log::Logger logger;
  auto configured = logger.set_output_file("/dev/full");
  ASSERT_TRUE(configured.has_value()) << configured.error().message();
  logger.log(dagforge::log::Level::Error, "cannot persist");
  const auto flushed = logger.flush();
  ASSERT_FALSE(flushed.has_value());
  EXPECT_NE(flushed.error().value(), 0);
  logger.set_output_stderr();
}

TEST(LoggerTest, ExposesExplicitOverflowPolicyAndDropCount) {
  dagforge::log::Logger logger({
      .queue_capacity = 2,
      .batch_size = 1,
      .color_policy = dagforge::log::ColorPolicy::Never,
      .overflow_policy = dagforge::log::OverflowPolicy::Block,
  });
  EXPECT_EQ(logger.color_policy(), dagforge::log::ColorPolicy::Never);
  EXPECT_EQ(logger.overflow_policy(), dagforge::log::OverflowPolicy::Block);
  EXPECT_EQ(logger.dropped_messages(), 0U);

  logger.set_overflow_policy(dagforge::log::OverflowPolicy::DropNewest);
  EXPECT_EQ(logger.overflow_policy(),
            dagforge::log::OverflowPolicy::DropNewest);
}

TEST(LoggerTest, RunningWriterProcessesStdStreamControlItemsInOrder) {
  dagforge::log::Logger logger;
  logger.start();
  logger.set_output_stderr();
  ASSERT_TRUE(logger.set_output_file("").has_value());
  logger.stop();
}

TEST(LoggerTest, BoundedQueueSurvivesConcurrentBurst) {
  const auto path = dagforge::test::make_temp_path("dagforge_log_burst_");
  ASSERT_FALSE(path.empty());

  dagforge::log::Logger logger;
  logger.set_level(dagforge::log::Level::Info);
  ASSERT_TRUE(logger.set_output_file(path).has_value());
  logger.start();

  std::vector<std::jthread> writers;
  for (int writer = 0; writer < 4; ++writer) {
    writers.emplace_back([&logger, writer] {
      for (int message = 0; message < 4'000; ++message) {
        logger.log(dagforge::log::Level::Info, "burst {} {}", writer, message);
      }
    });
  }
  writers.clear();
  logger.stop();

  EXPECT_FALSE(read_file(path).empty());
  std::remove(path.c_str());
}
