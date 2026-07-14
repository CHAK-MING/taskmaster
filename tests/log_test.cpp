#include "dagforge/util/log.hpp"
#include "test_utils.hpp"

#include "gtest/gtest.h"

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <iterator>
#include <string>
#include <thread>
#include <vector>

namespace {

auto read_file(const std::string &path) -> std::string {
  std::ifstream input(path, std::ios::binary);
  return {std::istreambuf_iterator<char>{input},
          std::istreambuf_iterator<char>{}};
}

} // namespace

TEST(LoggerTest, RepeatedStartStopCyclesDoNotCrash) {
  const auto path = dagforge::test::make_temp_path("dagforge_log_cycle_");
  ASSERT_FALSE(path.empty());

  for (int i = 0; i < 5; ++i) {
    ASSERT_TRUE(dagforge::log::set_output_file(path));
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
    ASSERT_TRUE(logger.set_output_file(path));
    logger.log(dagforge::log::Level::Info, "hidden message");
    logger.log(dagforge::log::Level::Error, "visible {}", 42);
    logger.stop();
    ASSERT_TRUE(logger.set_output_file(""));
  }

  const auto content = read_file(path);
  EXPECT_EQ(content.find("hidden message"), std::string::npos);
  EXPECT_NE(content.find("visible 42"), std::string::npos);
  std::remove(path.c_str());
}

TEST(LoggerTest, RejectsInvalidFileBeforeWriterStarts) {
  dagforge::log::Logger logger;
  EXPECT_FALSE(logger.set_output_file(
      "/definitely/missing/dagforge/log/output.log"));
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
  ASSERT_TRUE(logger.set_output_file(first));
  for (int i = 0; i < 100; ++i) {
    logger.log(dagforge::log::Level::Debug, "first-batch {}", i);
  }
  ASSERT_TRUE(logger.set_output_file(second));
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

TEST(LoggerTest, RunningWriterProcessesStdStreamControlItemsInOrder) {
  dagforge::log::Logger logger;
  logger.start();
  logger.set_output_stderr();
  ASSERT_TRUE(logger.set_output_file(""));
  logger.stop();
}

TEST(LoggerTest, BoundedQueueSurvivesConcurrentBurst) {
  const auto path = dagforge::test::make_temp_path("dagforge_log_burst_");
  ASSERT_FALSE(path.empty());

  dagforge::log::Logger logger;
  logger.set_level(dagforge::log::Level::Info);
  ASSERT_TRUE(logger.set_output_file(path));
  logger.start();

  std::vector<std::jthread> writers;
  for (int writer = 0; writer < 4; ++writer) {
    writers.emplace_back([&logger, writer] {
      for (int message = 0; message < 4'000; ++message) {
        logger.log(dagforge::log::Level::Info, "burst {} {}", writer,
                   message);
      }
    });
  }
  writers.clear();
  logger.stop();

  EXPECT_FALSE(read_file(path).empty());
  std::remove(path.c_str());
}
