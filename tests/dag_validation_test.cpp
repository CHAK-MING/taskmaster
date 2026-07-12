#include "dagforge/config/dag_info_loader.hpp"
#include "dagforge/config/dag_file_loader.hpp"
#include "dagforge/executor/executor.hpp"
#include "dagforge/util/id.hpp"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace dagforge {
namespace {

template <typename T>
concept HasSourceFileBuilderMethod = requires(T builder) {
  std::move(builder).source_file("unused");
};

static_assert(!HasSourceFileBuilderMethod<DAGInfoBuilder>);

class DAGValidationTest : public ::testing::Test {
protected:
  auto create_temp_dir() -> std::filesystem::path {
    static std::atomic<std::uint64_t> seq{0};
    const auto base = std::filesystem::temp_directory_path();
    for (int i = 0; i < 32; ++i) {
      const auto now_ns =
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::chrono::steady_clock::now().time_since_epoch())
              .count();
      auto temp_dir =
          base / ("dagforge_test_" + std::to_string(now_ns) + "_" +
                  std::to_string(seq.fetch_add(1, std::memory_order_relaxed)));
      if (std::filesystem::create_directory(temp_dir)) {
        temp_dirs_.push_back(temp_dir);
        return temp_dir;
      }
    }
    throw std::runtime_error("Failed to create unique temp directory");
  }

  auto write_toml_file(const std::filesystem::path &dir,
                       const std::string &filename, const std::string &content)
      -> std::filesystem::path {
    auto path = dir / filename;
    std::ofstream file(path);
    file << content;
    file.close();
    return path;
  }

  void TearDown() override {
    for (const auto &dir : temp_dirs_) {
      std::filesystem::remove_all(dir);
    }
  }

private:
  std::vector<std::filesystem::path> temp_dirs_;
};

TEST_F(DAGValidationTest, LoadValidDAG) {
  std::string valid_toml = R"(
id = "test_dag"
name = "test_dag"
description = "Test DAG"

[[tasks]]
id = "task1"
command = "echo hello"
)";

  auto result = DAGInfoLoader::load_from_string(valid_toml);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->name, "test_dag");
  EXPECT_EQ(result->description, "Test DAG");
  EXPECT_EQ(result->tasks.size(), 1);
  EXPECT_EQ(result->tasks[0].task_id, TaskId("task1"));
}

TEST_F(DAGValidationTest, LoadValidDAG_GlazeTomlBackend) {
  std::string valid_toml = R"(
id = "test_dag_glaze"
name = "test_dag_glaze"
description = "Test DAG via glaze"

[[tasks]]
id = "task1"
command = "echo hello"
)";

  auto result = DAGInfoLoader::load_from_string(valid_toml);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->name, "test_dag_glaze");
  EXPECT_EQ(result->tasks.size(), 1);
  EXPECT_EQ(result->tasks[0].task_id, TaskId("task1"));
}

TEST_F(DAGValidationTest, EmptyNameFallsBackToDagId) {
  std::string toml = R"(
id = "invalid_dag"
name = ""
[[tasks]]
id = "task1"
command = "echo hello"
)";

  auto result = DAGInfoLoader::load_from_string(toml);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->dag_id, DAGId("invalid_dag"));
  EXPECT_EQ(result->name, "invalid_dag");
}

TEST_F(DAGValidationTest, ParseStartAndEndDate) {
  std::string toml = R"(
id = "dated_dag"
name = "dated_dag"
start_date = 2025-03-01
end_date = 2025-03-15

[[tasks]]
id = "task1"
command = "echo hello"
)";

  auto result = DAGInfoLoader::load_from_string(toml);
  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->start_date.has_value());
  ASSERT_TRUE(result->end_date.has_value());
  EXPECT_LT(*result->start_date, *result->end_date);
}

TEST_F(DAGValidationTest, RejectQuotedDateStrings) {
  std::string toml = R"(
id = "dated_dag"
start_date = "2025-03-01"

[[tasks]]
id = "task1"
command = "echo hello"
)";

  auto result = DAGInfoLoader::load_from_string(toml);
  EXPECT_FALSE(result.has_value());
}

TEST_F(DAGValidationTest, RejectInvalidTomlLocalDate) {
  std::string toml = R"(
id = "dated_dag"
start_date = 2025-02-30

[[tasks]]
id = "task1"
command = "echo hello"
)";

  auto result = DAGInfoLoader::load_from_string(toml);
  EXPECT_FALSE(result.has_value());
}

TEST_F(DAGValidationTest, RejectRemovedXComAndBranchFields) {
  constexpr std::array cases{
      std::pair{
          std::string_view{"xcom_push"},
          std::string_view{R"(id = "removed_xcom_push"
[[tasks]]
id = "producer"
command = "echo producer"
[[tasks.xcom_push]]
key = "payload"
source = "stdout"
)"}},
      std::pair{
          std::string_view{"xcom_pull"},
          std::string_view{R"(id = "removed_xcom_pull"
[[tasks]]
id = "consumer"
command = "echo consumer"
[[tasks.xcom_pull]]
from = "producer"
key = "payload"
)"}},
      std::pair{
          std::string_view{"is_branch"},
          std::string_view{R"(id = "removed_branch"
[[tasks]]
id = "router"
command = "echo router"
is_branch = true
)"}},
      std::pair{
          std::string_view{"branch_xcom_key"},
          std::string_view{R"(id = "removed_branch_key"
[[tasks]]
id = "router"
command = "echo router"
branch_xcom_key = "branch"
)"}},
  };

  for (const auto &[field, toml] : cases) {
    std::string diagnostic;
    auto result = DAGInfoLoader::load_from_string(toml, &diagnostic);
    EXPECT_FALSE(result.has_value()) << field;
    EXPECT_NE(diagnostic.find(field), std::string::npos) << diagnostic;
  }
}

TEST_F(DAGValidationTest, RemovedFieldNamesInsideLuaScriptAreAllowed) {
  constexpr std::string_view toml = R"(id = "lua_names"
[[tasks]]
id = "lua_task"
executor = "lua"
script = """
  local xcom_push = "ordinary variable"
  local xcom_pull = xcom_push
  local is_branch = false
  local branch_xcom_key = "ordinary variable"
  return xcom_pull
"""
)";

  std::string diagnostic;
  auto result = DAGInfoLoader::load_from_string(toml, &diagnostic);
  EXPECT_TRUE(result.has_value()) << diagnostic;
}

TEST_F(DAGValidationTest, RejectUnknownFields) {
  std::string toml = R"(
id = "strict_dag"
unknown_setting = true

[[tasks]]
id = "task1"
command = "echo hello"
)";

  auto result = DAGInfoLoader::load_from_string(toml);
  EXPECT_FALSE(result.has_value());
}

TEST_F(DAGValidationTest, RejectEmptyTasks) {
  std::string invalid_toml = R"(
id = "test_dag"
name = "test_dag"
tasks = []
)";

  auto result = DAGInfoLoader::load_from_string(invalid_toml);
  EXPECT_FALSE(result.has_value());
}

TEST_F(DAGValidationTest, RejectDuplicateTaskIDs) {
  std::string invalid_toml = R"(
id = "test_dag"
name = "test_dag"
[[tasks]]
id = "task1"
command = "echo hello"

[[tasks]]
id = "task1"
command = "echo world"
)";

  auto result = DAGInfoLoader::load_from_string(invalid_toml);
  EXPECT_FALSE(result.has_value());
}

TEST_F(DAGValidationTest, RejectEmptyCommand) {
  std::string invalid_toml = R"(
id = "test_dag"
name = "test_dag"
[[tasks]]
id = "task1"
command = ""
)";

  auto result = DAGInfoLoader::load_from_string(invalid_toml);
  EXPECT_FALSE(result.has_value());
}

TEST_F(DAGValidationTest, RejectSelfDependency) {
  std::string invalid_toml = R"(
id = "test_dag"
name = "test_dag"
[[tasks]]
id = "task1"
command = "echo hello"
dependencies = ["task1"]
)";

  auto result = DAGInfoLoader::load_from_string(invalid_toml);
  EXPECT_FALSE(result.has_value());
}

TEST_F(DAGValidationTest, RejectNonexistentDependency) {
  std::string invalid_toml = R"(
id = "test_dag"
name = "test_dag"
[[tasks]]
id = "task1"
command = "echo hello"
dependencies = ["nonexistent"]
)";

  auto result = DAGInfoLoader::load_from_string(invalid_toml);
  EXPECT_FALSE(result.has_value());
}

TEST_F(DAGValidationTest, AcceptValidDependencies) {
  std::string valid_toml = R"(
id = "test_dag"
name = "test_dag"
[[tasks]]
id = "task1"
command = "echo hello"

[[tasks]]
id = "task2"
command = "echo world"
dependencies = ["task1"]
)";

  auto result = DAGInfoLoader::load_from_string(valid_toml);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->tasks.size(), 2);
  EXPECT_EQ(result->tasks[1].dependencies.size(), 1);
  EXPECT_EQ(result->tasks[1].dependencies[0], TaskId("task1"));
}

class DAGFileLoaderTest : public DAGValidationTest {};

TEST_F(DAGFileLoaderTest, LoadAllTomlFiles) {
  auto temp_dir = create_temp_dir();

  write_toml_file(temp_dir, "dag1.toml", R"(
id = "dag1"
name = "dag1"
[[tasks]]
id = "task1"
command = "echo 1"
)");

  write_toml_file(temp_dir, "ignored.txt", "not a toml file");

  DAGFileLoader loader(temp_dir.string());
  auto result = loader.load_all();

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->size(), 1);

  bool found_dag1 = false;
  for (const auto &dag_file : *result) {
    if (dag_file.info.name == "dag1") {
      found_dag1 = true;
    }
  }
  EXPECT_TRUE(found_dag1);
}

TEST_F(DAGFileLoaderTest, LoadAllSeesNewFileAfterSecondLoad) {
  auto temp_dir = create_temp_dir();

  write_toml_file(temp_dir, "dag1.toml", R"(
id = "dag1"
name = "dag1"
[[tasks]]
id = "task1"
command = "echo 1"
)");

  DAGFileLoader loader(temp_dir.string());
  auto initial = loader.load_all();
  ASSERT_TRUE(initial.has_value());
  EXPECT_EQ(initial->size(), 1);

  write_toml_file(temp_dir, "dag2.toml", R"(
id = "dag2"
name = "dag2"
[[tasks]]
id = "task2"
command = "echo 2"
)");

  auto second = loader.load_all();
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(second->size(), 2);

  bool found_dag2 = false;
  for (const auto &dag_file : *second) {
    if (dag_file.info.name == "dag2") {
      found_dag2 = true;
      break;
    }
  }
  EXPECT_TRUE(found_dag2);
}

TEST_F(DAGFileLoaderTest, LoadFileReadsModifiedContent) {
  auto temp_dir = create_temp_dir();

  auto file_path = write_toml_file(temp_dir, "dag1.toml", R"(
id = "dag1"
name = "dag1"
description = "original"

[[tasks]]
id = "task1"
command = "echo 1"
)");

  DAGFileLoader loader(temp_dir.string());
  auto original = loader.load_file(file_path);
  ASSERT_TRUE(original.has_value());
  EXPECT_EQ(original->info.description, "original");

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  write_toml_file(temp_dir, "dag1.toml", R"(
id = "dag1"
name = "dag1"
description = "modified"

[[tasks]]
id = "task1"
command = "echo 1"
)");

  auto modified = loader.load_file(file_path);
  ASSERT_TRUE(modified.has_value());
  EXPECT_EQ(modified->info.description, "modified");
}

TEST_F(DAGFileLoaderTest, LoadAllReflectsRemovedFile) {
  auto temp_dir = create_temp_dir();

  auto file_path = write_toml_file(temp_dir, "dag1.toml", R"(
id = "dag1"
name = "dag1"
[[tasks]]
id = "task1"
command = "echo 1"
)");

  DAGFileLoader loader(temp_dir.string());
  auto initial = loader.load_all();
  ASSERT_TRUE(initial.has_value());
  EXPECT_EQ(initial->size(), 1);

  std::filesystem::remove(file_path);

  auto after_remove = loader.load_all();
  ASSERT_TRUE(after_remove.has_value());
  EXPECT_EQ(after_remove->size(), 0);
}

TEST_F(DAGFileLoaderTest, LoadAllIgnoresInvalidFile) {
  auto temp_dir = create_temp_dir();

  write_toml_file(temp_dir, "dag1.toml", R"(
id = "dag1"
name = "dag1"
[[tasks]]
id = "task1"
command = "echo 1"
)");

  write_toml_file(temp_dir, "invalid.toml", R"(
id = "invalid_dag"
name = ""
tasks = []
)");

  DAGFileLoader loader(temp_dir.string());
  auto loaded = loader.load_all();
  ASSERT_TRUE(loaded.has_value());
  EXPECT_EQ(loaded->size(), 1);
  EXPECT_EQ((*loaded)[0].info.name, "dag1");
}

TEST_F(DAGFileLoaderTest, LoadFileMissingPathReturnsFileNotFound) {
  auto temp_dir = create_temp_dir();
  DAGFileLoader loader(temp_dir.string());

  auto loaded = loader.load_file(temp_dir / "missing.toml");
  ASSERT_FALSE(loaded.has_value());
  EXPECT_EQ(loaded.error(), make_error_code(Error::FileNotFound));
}

TEST_F(DAGFileLoaderTest, LoadAllMissingDirectoryReturnsEmptySet) {
  auto temp_dir = create_temp_dir();
  std::filesystem::remove_all(temp_dir);

  DAGFileLoader loader(temp_dir.string());
  auto loaded = loader.load_all();

  ASSERT_TRUE(loaded.has_value());
  EXPECT_TRUE(loaded->empty());
}

TEST_F(DAGFileLoaderTest, LoadAllSkipsSymlinkOutsideDirectory) {
  auto temp_dir = create_temp_dir();
  auto outside_dir = create_temp_dir();
  auto outside_file = write_toml_file(outside_dir, "outside.toml", R"(
id = "outside"
name = "outside"
[[tasks]]
id = "task1"
command = "echo 1"
)");

  std::filesystem::create_symlink(outside_file, temp_dir / "linked.toml");

  DAGFileLoader loader(temp_dir.string());
  auto loaded = loader.load_all();

  ASSERT_TRUE(loaded.has_value());
  EXPECT_TRUE(loaded->empty());

  std::filesystem::remove_all(outside_dir);
}

class DefaultArgsTest : public ::testing::Test {};

TEST_F(DefaultArgsTest, LoadFromString_AppliesDefaultTimeout) {
  std::string toml = R"(
id = "test_dag"
name = "test_dag"
[default_args]
timeout = 60

[[tasks]]
id = "task1"
command = "echo hello"
)";

  auto result = DAGInfoLoader::load_from_string(toml);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->tasks[0].execution_timeout, std::chrono::seconds(60));
}

TEST_F(DefaultArgsTest, LoadFromString_TaskOverridesDefault) {
  std::string toml = R"(
id = "test_dag"
name = "test_dag"
[default_args]
timeout = 60

[[tasks]]
id = "task1"
command = "echo hello"
timeout = 120
)";

  auto result = DAGInfoLoader::load_from_string(toml);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->tasks[0].execution_timeout, std::chrono::seconds(120));
}

TEST_F(DefaultArgsTest, LoadFromString_AppliesDefaultRetryInterval) {
  std::string toml = R"(
id = "test_dag"
name = "test_dag"
[default_args]
retry_interval = 30

[[tasks]]
id = "task1"
command = "echo hello"
)";

  auto result = DAGInfoLoader::load_from_string(toml);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->tasks[0].retry_interval, std::chrono::seconds(30));
}

TEST_F(DefaultArgsTest, LoadFromString_AppliesDefaultMaxRetries) {
  std::string toml = R"(
id = "test_dag"
name = "test_dag"
[default_args]
max_retries = 5

[[tasks]]
id = "task1"
command = "echo hello"
)";

  auto result = DAGInfoLoader::load_from_string(toml);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->tasks[0].max_retries, 5);
}

TEST_F(DefaultArgsTest, LoadFromString_AppliesRetryFields) {
  std::string toml = R"(
id = "test_dag"
name = "test_dag"
[default_args]
retry_interval = 45
max_retries = 7

[[tasks]]
id = "task1"
command = "echo hello"
)";

  auto result = DAGInfoLoader::load_from_string(toml);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->tasks[0].retry_interval, std::chrono::seconds(45));
  EXPECT_EQ(result->tasks[0].max_retries, 7);
}

TEST_F(DefaultArgsTest, LoadFromString_AppliesOneFailedTriggerRule) {
  std::string toml = R"(
id = "test_dag"
name = "test_dag"
[default_args]
trigger_rule = "one_failed"

[[tasks]]
id = "task1"
command = "echo hello"
)";

  auto result = DAGInfoLoader::load_from_string(toml);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->tasks[0].trigger_rule, TriggerRule::OneFailed);
}

TEST_F(DefaultArgsTest, LoadFromString_AppliesDefaultTriggerRule) {
  std::string toml = R"(
id = "test_dag"
name = "test_dag"
[default_args]
trigger_rule = "all_done"

[[tasks]]
id = "task1"
command = "echo hello"
)";

  auto result = DAGInfoLoader::load_from_string(toml);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->tasks[0].trigger_rule, TriggerRule::AllDone);
}

TEST_F(DefaultArgsTest, LoadFromString_AppliesDependsOnPast) {
  std::string toml = R"(
id = "test_dag"
name = "test_dag"
[default_args]
depends_on_past = true

[[tasks]]
id = "task1"
command = "echo hello"
)";

  auto result = DAGInfoLoader::load_from_string(toml);
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->tasks[0].depends_on_past);
}

TEST_F(DefaultArgsTest, LoadFromString_MultipleTasksInheritDefaults) {
  std::string toml = R"(
id = "test_dag"
name = "test_dag"
[default_args]
timeout = 60
max_retries = 3

[[tasks]]
id = "task1"
command = "echo hello"

[[tasks]]
id = "task2"
command = "echo world"
timeout = 120
)";

  auto result = DAGInfoLoader::load_from_string(toml);
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result->tasks.size(), 2);
  EXPECT_EQ(result->tasks[0].execution_timeout, std::chrono::seconds(60));
  EXPECT_EQ(result->tasks[0].max_retries, 3);
  EXPECT_EQ(result->tasks[1].execution_timeout, std::chrono::seconds(120));
  EXPECT_EQ(result->tasks[1].max_retries, 3);
}

TEST_F(DefaultArgsTest, LoadFromString_AcceptsWorkingDirectoryAlias) {
  std::string toml = R"(
id = "test_dag"
name = "test_dag"

[[tasks]]
id = "task1"
command = "pwd"
working_directory = "/tmp"
)";

  auto result = DAGInfoLoader::load_from_string(toml);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->tasks[0].working_dir, "/tmp");
}

class DependencyLabelTest : public ::testing::Test {};

TEST_F(DependencyLabelTest, LoadFromString_ScalarFormParsesAsEmptyLabel) {
  std::string toml = R"(
id = "test_dag"
name = "test_dag"
[[tasks]]
id = "task1"
command = "echo hello"

[[tasks]]
id = "task2"
command = "echo world"
dependencies = ["task1"]
)";

  auto result = DAGInfoLoader::load_from_string(toml);
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result->tasks[1].dependencies.size(), 1);
  EXPECT_EQ(result->tasks[1].dependencies[0].task_id, TaskId("task1"));
  EXPECT_TRUE(result->tasks[1].dependencies[0].label.empty());
}

TEST_F(DependencyLabelTest, LoadFromString_MapFormParsesLabel) {
  std::string toml = R"(
id = "test_dag"
name = "test_dag"
[[tasks]]
id = "task1"
command = "echo hello"

[[tasks]]
id = "task2"
command = "echo world"

[[tasks.dependencies]]
task = "task1"
label = "success_branch"
)";

  auto result = DAGInfoLoader::load_from_string(toml);
  EXPECT_FALSE(result.has_value());
}

TEST_F(DAGValidationTest, ToStringRoundTripsTaskMetadata) {
  TaskConfig upstream_a;
  upstream_a.task_id = TaskId{"upstream_a"};
  upstream_a.name = "upstream_a";
  upstream_a.command = "echo upstream_a";

  TaskConfig upstream_b;
  upstream_b.task_id = TaskId{"upstream_b"};
  upstream_b.name = "upstream_b";
  upstream_b.command = "echo upstream_b";

  TaskConfig task;
  task.task_id = TaskId{"task1"};
  task.name = "task1";
  task.command.clear();
  task.executor = ExecutorType::Sensor;
  task.execution_timeout = std::chrono::seconds(90);
  task.retry_interval = std::chrono::seconds(15);
  task.max_retries = 2;
  task.trigger_rule = TriggerRule::AllDone;
  task.depends_on_past = true;
  task.dependencies = {
      TaskDependency{.task_id = TaskId{"upstream_a"}, .label = "success"},
      TaskDependency{.task_id = TaskId{"upstream_b"}, .label = ""},
  };
  task.executor_config = SensorExecutorConfig{
      .type = SensorType::Http,
      .target = "http://example.com/health",
      .poke_interval = std::chrono::seconds(45),
      .soft_fail = true,
      .expected_status = 204,
      .http_method = "HEAD",
  };

  DAGInfo dag;
  dag.dag_id = DAGId{"roundtrip_dag"};
  dag.name = "roundtrip_dag";
  dag.description = "roundtrip";
  dag.cron = "0 * * * *";
  dag.catchup = true;
  dag.max_concurrent_runs = 3;
  dag.start_date = std::chrono::system_clock::time_point{
      std::chrono::days{20148}}; // 2025-03-01 UTC
  dag.end_date = std::chrono::system_clock::time_point{
      std::chrono::days{20162}}; // 2025-03-15 UTC
  dag.tasks.push_back(upstream_a);
  dag.tasks.push_back(upstream_b);
  dag.tasks.push_back(task);
  dag.rebuild_task_index();

  auto serialized = DAGInfoLoader::to_string(dag);
  auto reparsed = DAGInfoLoader::load_from_string(serialized);

  ASSERT_TRUE(reparsed.has_value());
  ASSERT_EQ(reparsed->tasks.size(), 3);
  const auto &task_out = reparsed->tasks[2];
  EXPECT_EQ(reparsed->dag_id, dag.dag_id);
  EXPECT_EQ(reparsed->start_date, dag.start_date);
  EXPECT_EQ(reparsed->end_date, dag.end_date);
  EXPECT_TRUE(task_out.depends_on_past);
  ASSERT_EQ(task_out.dependencies.size(), 2);
  EXPECT_TRUE(task_out.dependencies[0].label.empty());
  EXPECT_TRUE(task_out.dependencies[1].label.empty());
  const auto *sensor = task_out.executor_config.as<SensorExecutorConfig>();
  ASSERT_NE(sensor, nullptr);
  EXPECT_EQ(sensor->type, SensorType::Http);
  EXPECT_EQ(sensor->target, "http://example.com/health");
  EXPECT_EQ(sensor->http_method, "HEAD");
  EXPECT_EQ(sensor->expected_status, 204);
}

TEST_F(DependencyLabelTest, LoadFromString_MixedFormsParse) {
  std::string toml = R"(
id = "test_dag"
name = "test_dag"
[[tasks]]
id = "task1"
command = "echo hello"

[[tasks]]
id = "task2"
command = "echo world"

[[tasks]]
id = "task3"
command = "echo final"

[[tasks.dependencies]]
task = "task1"
label = ""

[[tasks.dependencies]]
task = "task2"
label = "branch_a"
)";

  auto result = DAGInfoLoader::load_from_string(toml);
  EXPECT_FALSE(result.has_value());
}

} // namespace
} // namespace dagforge
