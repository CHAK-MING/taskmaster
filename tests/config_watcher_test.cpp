#include "dagforge/config/config_watcher.hpp"
#include "dagforge/core/runtime.hpp"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <thread>
#include <unistd.h>

#include "gtest/gtest.h"

using namespace dagforge;
namespace fs = std::filesystem;

class ConfigWatcherTest : public ::testing::Test {
protected:
  void SetUp() override {
    char templ[] = "/tmp/dagforge_watcher_test_XXXXXX";
    char *path = ::mkdtemp(templ);
    ASSERT_TRUE(path != nullptr);
    test_dir_ = path;
    ASSERT_TRUE(runtime_.start().has_value());
  }

  void TearDown() override {
    runtime_.stop();
    fs::remove_all(test_dir_);
  }

  auto create_toml_file(const std::string &name,
                        const std::string &content = "name = \"test\"")
      -> fs::path {
    auto path = test_dir_ / name;
    std::ofstream(path) << content;
    return path;
  }

  auto
  wait_for(std::atomic<int> &counter, int expected,
           std::chrono::milliseconds timeout = std::chrono::milliseconds(5000))
      -> bool {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (counter.load() < expected &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return counter.load() >= expected;
  }

  fs::path test_dir_;
  Runtime runtime_{1};
};

TEST_F(ConfigWatcherTest, ConstructorSetsDirectory) {
  ConfigWatcher watcher(runtime_, test_dir_.string());
  EXPECT_EQ(watcher.directory(), test_dir_);
}

TEST_F(ConfigWatcherTest, InitiallyNotRunning) {
  ConfigWatcher watcher(runtime_, test_dir_.string());
  EXPECT_FALSE(watcher.is_running());
}

TEST_F(ConfigWatcherTest, StartAndStop) {
  ConfigWatcher watcher(runtime_, test_dir_.string());

  ASSERT_TRUE(watcher.start().has_value());
  EXPECT_TRUE(watcher.is_running());

  EXPECT_TRUE(watcher.stop().has_value());
  EXPECT_FALSE(watcher.is_running());
}

TEST_F(ConfigWatcherTest, DoubleStartIsNoop) {
  ConfigWatcher watcher(runtime_, test_dir_.string());

  ASSERT_TRUE(watcher.start().has_value());
  EXPECT_TRUE(watcher.is_running());

  EXPECT_TRUE(watcher.start().has_value());
  EXPECT_TRUE(watcher.is_running());

  EXPECT_TRUE(watcher.stop().has_value());
  EXPECT_FALSE(watcher.is_running());
}

TEST_F(ConfigWatcherTest, DoubleStopIsNoop) {
  ConfigWatcher watcher(runtime_, test_dir_.string());

  ASSERT_TRUE(watcher.start().has_value());
  EXPECT_TRUE(watcher.stop().has_value());
  EXPECT_FALSE(watcher.is_running());

  EXPECT_TRUE(watcher.stop().has_value());
  EXPECT_FALSE(watcher.is_running());
}

TEST_F(ConfigWatcherTest, DestructorStopsWatcher) {
  {
    ConfigWatcher watcher(runtime_, test_dir_.string());
    ASSERT_TRUE(watcher.start().has_value());
    EXPECT_TRUE(watcher.is_running());
  }
}

TEST_F(ConfigWatcherTest, DetectsNewTomlFile) {
  std::atomic<int> change_count{0};
  fs::path changed_file;
  std::mutex changed_file_mutex;

  ConfigWatcher watcher(runtime_, test_dir_.string());
  watcher.set_on_file_changed([&](const fs::path &path) {
    std::lock_guard lock(changed_file_mutex);
    changed_file = path;
    change_count.fetch_add(1);
  });

  ASSERT_TRUE(watcher.start().has_value());
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  auto file = create_toml_file("new_dag.toml");

  ASSERT_TRUE(wait_for(change_count, 1));
  EXPECT_TRUE(watcher.stop().has_value());

  std::lock_guard lock(changed_file_mutex);
  EXPECT_EQ(changed_file.filename(), "new_dag.toml");
}

TEST_F(ConfigWatcherTest, DetectsModifiedTomlFile) {
  auto file = create_toml_file("existing.toml", "name = \"initial\"");

  std::atomic<int> change_count{0};

  ConfigWatcher watcher(runtime_, test_dir_.string());
  watcher.set_on_file_changed(
      [&](const fs::path &) { change_count.fetch_add(1); });

  ASSERT_TRUE(watcher.start().has_value());
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  std::ofstream(file, std::ios::app) << "\ndescription = \"modified\"";

  ASSERT_TRUE(wait_for(change_count, 1));

  EXPECT_TRUE(watcher.stop().has_value());
}

TEST_F(ConfigWatcherTest, DetectsDeletedTomlFile) {
  auto file = create_toml_file("to_delete.toml");

  std::atomic<int> remove_count{0};
  fs::path removed_file;
  std::mutex removed_file_mutex;

  ConfigWatcher watcher(runtime_, test_dir_.string());
  watcher.set_on_file_removed([&](const fs::path &path) {
    std::lock_guard lock(removed_file_mutex);
    removed_file = path;
    remove_count.fetch_add(1);
  });

  ASSERT_TRUE(watcher.start().has_value());
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  fs::remove(file);

  ASSERT_TRUE(wait_for(remove_count, 1));
  EXPECT_TRUE(watcher.stop().has_value());

  std::lock_guard lock(removed_file_mutex);
  EXPECT_EQ(removed_file.filename(), "to_delete.toml");
}

TEST_F(ConfigWatcherTest, IgnoresNonTomlFiles) {
  std::atomic<int> change_count{0};

  ConfigWatcher watcher(runtime_, test_dir_.string());
  watcher.set_on_file_changed(
      [&](const fs::path &) { change_count.fetch_add(1); });

  ASSERT_TRUE(watcher.start().has_value());
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  std::ofstream(test_dir_ / "readme.txt") << "ignored";
  std::ofstream(test_dir_ / "config.json") << "{}";

  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_EQ(change_count.load(), 0);

  EXPECT_TRUE(watcher.stop().has_value());
}

TEST_F(ConfigWatcherTest, AcceptsTomlExtension) {
  std::atomic<int> change_count{0};

  ConfigWatcher watcher(runtime_, test_dir_.string());
  watcher.set_on_file_changed(
      [&](const fs::path &) { change_count.fetch_add(1); });

  ASSERT_TRUE(watcher.start().has_value());
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  create_toml_file("dag.toml");

  ASSERT_TRUE(wait_for(change_count, 1));

  EXPECT_TRUE(watcher.stop().has_value());
}

TEST_F(ConfigWatcherTest, NoCallbacksSetDoesNotCrash) {
  ConfigWatcher watcher(runtime_, test_dir_.string());
  ASSERT_TRUE(watcher.start().has_value());
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  create_toml_file("test.toml");
  fs::remove(test_dir_ / "test.toml");

  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  EXPECT_TRUE(watcher.stop().has_value());
}

TEST_F(ConfigWatcherTest, RapidStartStopStress) {
  ConfigWatcher watcher(runtime_, test_dir_.string());

  for (int i = 0; i < 25; ++i) {
    ASSERT_TRUE(watcher.start().has_value());
    create_toml_file("stress_" + std::to_string(i) + ".toml");
    EXPECT_TRUE(watcher.stop().has_value());
    EXPECT_FALSE(watcher.is_running());
  }
}

TEST_F(ConfigWatcherTest, InvalidDirectoryFailsGracefully) {
  ConfigWatcher watcher(runtime_, "/nonexistent/path/that/does/not/exist");
  EXPECT_FALSE(watcher.start().has_value());
  EXPECT_FALSE(watcher.is_running());
}
