#include "taskmaster/config/config_watcher.hpp"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <thread>
#include <unistd.h>

#include "gtest/gtest.h"

using namespace taskmaster;
namespace fs = std::filesystem;

class ConfigWatcherTest : public ::testing::Test {
protected:
  void SetUp() override {
    char templ[] = "/tmp/taskmaster_watcher_test_XXXXXX";
    char* path = ::mkdtemp(templ);
    ASSERT_TRUE(path != nullptr);
    test_dir_ = path;
  }

  void TearDown() override {
    fs::remove_all(test_dir_);
  }

  auto create_yaml_file(const std::string& name, const std::string& content = "test: value") -> fs::path {
    auto path = test_dir_ / name;
    std::ofstream(path) << content;
    return path;
  }

  auto wait_for(std::atomic<int>& counter, int expected, std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)) -> bool {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (counter.load() < expected && std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return counter.load() >= expected;
  }

  fs::path test_dir_;
};

TEST_F(ConfigWatcherTest, ConstructorSetsDirectory) {
  ConfigWatcher watcher(test_dir_.string());
  EXPECT_EQ(watcher.directory(), test_dir_);
}

TEST_F(ConfigWatcherTest, InitiallyNotRunning) {
  ConfigWatcher watcher(test_dir_.string());
  EXPECT_FALSE(watcher.is_running());
}

TEST_F(ConfigWatcherTest, StartAndStop) {
  ConfigWatcher watcher(test_dir_.string());

  watcher.start();
  EXPECT_TRUE(watcher.is_running());

  watcher.stop();
  EXPECT_FALSE(watcher.is_running());
}

TEST_F(ConfigWatcherTest, DoubleStartIsNoop) {
  ConfigWatcher watcher(test_dir_.string());

  watcher.start();
  EXPECT_TRUE(watcher.is_running());

  watcher.start();
  EXPECT_TRUE(watcher.is_running());

  watcher.stop();
  EXPECT_FALSE(watcher.is_running());
}

TEST_F(ConfigWatcherTest, DoubleStopIsNoop) {
  ConfigWatcher watcher(test_dir_.string());

  watcher.start();
  watcher.stop();
  EXPECT_FALSE(watcher.is_running());

  watcher.stop();
  EXPECT_FALSE(watcher.is_running());
}

TEST_F(ConfigWatcherTest, DestructorStopsWatcher) {
  {
    ConfigWatcher watcher(test_dir_.string());
    watcher.start();
    EXPECT_TRUE(watcher.is_running());
  }
}

TEST_F(ConfigWatcherTest, DetectsNewYamlFile) {
  std::atomic<int> change_count{0};
  fs::path changed_file;

  ConfigWatcher watcher(test_dir_.string());
  watcher.set_on_file_changed([&](const fs::path& path) {
    changed_file = path;
    change_count.fetch_add(1);
  });

  watcher.start();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  auto file = create_yaml_file("new_dag.yaml");

  ASSERT_TRUE(wait_for(change_count, 1));
  EXPECT_EQ(changed_file.filename(), "new_dag.yaml");

  watcher.stop();
}

TEST_F(ConfigWatcherTest, DetectsModifiedYamlFile) {
  auto file = create_yaml_file("existing.yaml", "initial: content");

  std::atomic<int> change_count{0};

  ConfigWatcher watcher(test_dir_.string());
  watcher.set_on_file_changed([&](const fs::path&) {
    change_count.fetch_add(1);
  });

  watcher.start();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  std::ofstream(file, std::ios::app) << "\nmodified: true";

  ASSERT_TRUE(wait_for(change_count, 1));

  watcher.stop();
}

TEST_F(ConfigWatcherTest, DetectsDeletedYamlFile) {
  auto file = create_yaml_file("to_delete.yaml");

  std::atomic<int> remove_count{0};
  fs::path removed_file;

  ConfigWatcher watcher(test_dir_.string());
  watcher.set_on_file_removed([&](const fs::path& path) {
    removed_file = path;
    remove_count.fetch_add(1);
  });

  watcher.start();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  fs::remove(file);

  ASSERT_TRUE(wait_for(remove_count, 1));
  EXPECT_EQ(removed_file.filename(), "to_delete.yaml");

  watcher.stop();
}

TEST_F(ConfigWatcherTest, IgnoresNonYamlFiles) {
  std::atomic<int> change_count{0};

  ConfigWatcher watcher(test_dir_.string());
  watcher.set_on_file_changed([&](const fs::path&) {
    change_count.fetch_add(1);
  });

  watcher.start();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  std::ofstream(test_dir_ / "readme.txt") << "ignored";
  std::ofstream(test_dir_ / "config.json") << "{}";

  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_EQ(change_count.load(), 0);

  watcher.stop();
}

TEST_F(ConfigWatcherTest, AcceptsYmlExtension) {
  std::atomic<int> change_count{0};

  ConfigWatcher watcher(test_dir_.string());
  watcher.set_on_file_changed([&](const fs::path&) {
    change_count.fetch_add(1);
  });

  watcher.start();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  create_yaml_file("dag.yml");

  ASSERT_TRUE(wait_for(change_count, 1));

  watcher.stop();
}

TEST_F(ConfigWatcherTest, NoCallbacksSetDoesNotCrash) {
  ConfigWatcher watcher(test_dir_.string());
  watcher.start();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  create_yaml_file("test.yaml");
  fs::remove(test_dir_ / "test.yaml");

  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  watcher.stop();
}

TEST_F(ConfigWatcherTest, InvalidDirectoryFailsGracefully) {
  ConfigWatcher watcher("/nonexistent/path/that/does/not/exist");
  watcher.start();
  EXPECT_FALSE(watcher.is_running());
}
