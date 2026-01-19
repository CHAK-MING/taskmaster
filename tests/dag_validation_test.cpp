#include "taskmaster/config/dag_definition.hpp"
#include "taskmaster/config/dag_file_loader.hpp"
#include "taskmaster/util/id.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

namespace taskmaster {
namespace {

class DAGValidationTest : public ::testing::Test {
protected:
  auto create_temp_dir() -> std::filesystem::path {
    auto temp_dir = std::filesystem::temp_directory_path() /
                    ("taskmaster_test_" + std::to_string(std::time(nullptr)));
    std::filesystem::create_directories(temp_dir);
    temp_dirs_.push_back(temp_dir);
    return temp_dir;
  }

  auto write_yaml_file(const std::filesystem::path& dir,
                       const std::string& filename,
                       const std::string& content) -> std::filesystem::path {
    auto path = dir / filename;
    std::ofstream file(path);
    file << content;
    file.close();
    return path;
  }

  void TearDown() override {
    for (const auto& dir : temp_dirs_) {
      std::filesystem::remove_all(dir);
    }
  }

private:
  std::vector<std::filesystem::path> temp_dirs_;
};

TEST_F(DAGValidationTest, LoadValidDAG) {
  std::string valid_yaml = R"(
name: test_dag
description: Test DAG
tasks:
  - id: task1
    command: echo hello
)";

  auto result = DAGDefinitionLoader::load_from_string(valid_yaml);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->name, "test_dag");
  EXPECT_EQ(result->description, "Test DAG");
  EXPECT_EQ(result->tasks.size(), 1);
  EXPECT_EQ(result->tasks[0].task_id, TaskId("task1"));
}

TEST_F(DAGValidationTest, RejectEmptyName) {
  std::string invalid_yaml = R"(
name: ""
tasks:
  - id: task1
    command: echo hello
)";

  auto result = DAGDefinitionLoader::load_from_string(invalid_yaml);
  EXPECT_FALSE(result.has_value());
}

TEST_F(DAGValidationTest, RejectEmptyTasks) {
  std::string invalid_yaml = R"(
name: test_dag
tasks: []
)";

  auto result = DAGDefinitionLoader::load_from_string(invalid_yaml);
  EXPECT_FALSE(result.has_value());
}

TEST_F(DAGValidationTest, RejectDuplicateTaskIDs) {
  std::string invalid_yaml = R"(
name: test_dag
tasks:
  - id: task1
    command: echo hello
  - id: task1
    command: echo world
)";

  auto result = DAGDefinitionLoader::load_from_string(invalid_yaml);
  EXPECT_FALSE(result.has_value());
}

TEST_F(DAGValidationTest, RejectEmptyCommand) {
  std::string invalid_yaml = R"(
name: test_dag
tasks:
  - id: task1
    command: ""
)";

  auto result = DAGDefinitionLoader::load_from_string(invalid_yaml);
  EXPECT_FALSE(result.has_value());
}

TEST_F(DAGValidationTest, RejectSelfDependency) {
  std::string invalid_yaml = R"(
name: test_dag
tasks:
  - id: task1
    command: echo hello
    dependencies: [task1]
)";

  auto result = DAGDefinitionLoader::load_from_string(invalid_yaml);
  EXPECT_FALSE(result.has_value());
}

TEST_F(DAGValidationTest, RejectNonexistentDependency) {
  std::string invalid_yaml = R"(
name: test_dag
tasks:
  - id: task1
    command: echo hello
    dependencies: [nonexistent]
)";

  auto result = DAGDefinitionLoader::load_from_string(invalid_yaml);
  EXPECT_FALSE(result.has_value());
}

TEST_F(DAGValidationTest, AcceptValidDependencies) {
  std::string valid_yaml = R"(
name: test_dag
tasks:
  - id: task1
    command: echo hello
  - id: task2
    command: echo world
    dependencies: [task1]
)";

  auto result = DAGDefinitionLoader::load_from_string(valid_yaml);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->tasks.size(), 2);
  EXPECT_EQ(result->tasks[1].dependencies.size(), 1);
  EXPECT_EQ(result->tasks[1].dependencies[0], TaskId("task1"));
}

class DAGFileLoaderTest : public DAGValidationTest {};

TEST_F(DAGFileLoaderTest, LoadAllYAMLFiles) {
  auto temp_dir = create_temp_dir();

  write_yaml_file(temp_dir, "dag1.yaml", R"(
name: dag1
tasks:
  - id: task1
    command: echo 1
)");

  write_yaml_file(temp_dir, "dag2.yml", R"(
name: dag2
tasks:
  - id: task2
    command: echo 2
)");

  write_yaml_file(temp_dir, "ignored.txt", "not a yaml file");

  DAGFileLoader loader(temp_dir.string());
  auto result = loader.load_all();

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->size(), 2);

  bool found_dag1 = false;
  bool found_dag2 = false;
  for (const auto& dag_file : *result) {
    if (dag_file.definition.name == "dag1") {
      found_dag1 = true;
    }
    if (dag_file.definition.name == "dag2") {
      found_dag2 = true;
    }
  }
  EXPECT_TRUE(found_dag1);
  EXPECT_TRUE(found_dag2);
}

TEST_F(DAGFileLoaderTest, ScanForChangesDetectsNewFile) {
  auto temp_dir = create_temp_dir();

  write_yaml_file(temp_dir, "dag1.yaml", R"(
name: dag1
tasks:
  - id: task1
    command: echo 1
)");

  DAGFileLoader loader(temp_dir.string());
  auto initial = loader.load_all();
  ASSERT_TRUE(initial.has_value());
  EXPECT_EQ(initial->size(), 1);

  write_yaml_file(temp_dir, "dag2.yaml", R"(
name: dag2
tasks:
  - id: task2
    command: echo 2
)");

  auto [updated, removed] = loader.scan_for_changes();
  EXPECT_EQ(updated.size(), 1);
  EXPECT_EQ(updated[0].definition.name, "dag2");
  EXPECT_EQ(removed.size(), 0);
}

TEST_F(DAGFileLoaderTest, ScanForChangesDetectsModifiedFile) {
  auto temp_dir = create_temp_dir();

  auto file_path = write_yaml_file(temp_dir, "dag1.yaml", R"(
name: dag1
description: original
tasks:
  - id: task1
    command: echo 1
)");

  DAGFileLoader loader(temp_dir.string());
  auto initial = loader.load_all();
  ASSERT_TRUE(initial.has_value());

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  write_yaml_file(temp_dir, "dag1.yaml", R"(
name: dag1
description: modified
tasks:
  - id: task1
    command: echo 1
)");

  auto [updated, removed] = loader.scan_for_changes();
  EXPECT_EQ(updated.size(), 1);
  EXPECT_EQ(updated[0].definition.description, "modified");
  EXPECT_EQ(removed.size(), 0);
}

TEST_F(DAGFileLoaderTest, ScanForChangesDetectsRemovedFile) {
  auto temp_dir = create_temp_dir();

  auto file_path = write_yaml_file(temp_dir, "dag1.yaml", R"(
name: dag1
tasks:
  - id: task1
    command: echo 1
)");

  DAGFileLoader loader(temp_dir.string());
  auto initial = loader.load_all();
  ASSERT_TRUE(initial.has_value());
  EXPECT_EQ(initial->size(), 1);

  std::filesystem::remove(file_path);

  auto [updated, removed] = loader.scan_for_changes();
  EXPECT_EQ(updated.size(), 0);
  EXPECT_EQ(removed.size(), 1);
}

TEST_F(DAGFileLoaderTest, IgnoreInvalidFileDuringScan) {
  auto temp_dir = create_temp_dir();

  write_yaml_file(temp_dir, "dag1.yaml", R"(
name: dag1
tasks:
  - id: task1
    command: echo 1
)");

  DAGFileLoader loader(temp_dir.string());
  auto initial = loader.load_all();
  ASSERT_TRUE(initial.has_value());

  write_yaml_file(temp_dir, "invalid.yaml", R"(
name: ""
tasks: []
)");

  auto [updated, removed] = loader.scan_for_changes();
  EXPECT_EQ(updated.size(), 0);
  EXPECT_EQ(removed.size(), 0);
}

class DefaultArgsTest : public ::testing::Test {};

TEST_F(DefaultArgsTest, LoadFromString_AppliesDefaultTimeout) {
  std::string yaml = R"(
name: test_dag
default_args:
  timeout: 60
tasks:
  - id: task1
    command: echo hello
)";

  auto result = DAGDefinitionLoader::load_from_string(yaml);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->tasks[0].execution_timeout, std::chrono::seconds(60));
}

TEST_F(DefaultArgsTest, LoadFromString_TaskOverridesDefault) {
  std::string yaml = R"(
name: test_dag
default_args:
  timeout: 60
tasks:
  - id: task1
    command: echo hello
    timeout: 120
)";

  auto result = DAGDefinitionLoader::load_from_string(yaml);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->tasks[0].execution_timeout, std::chrono::seconds(120));
}

TEST_F(DefaultArgsTest, LoadFromString_AppliesDefaultRetryInterval) {
  std::string yaml = R"(
name: test_dag
default_args:
  retry_interval: 30
tasks:
  - id: task1
    command: echo hello
)";

  auto result = DAGDefinitionLoader::load_from_string(yaml);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->tasks[0].retry_interval, std::chrono::seconds(30));
}

TEST_F(DefaultArgsTest, LoadFromString_AppliesDefaultMaxRetries) {
  std::string yaml = R"(
name: test_dag
default_args:
  max_retries: 5
tasks:
  - id: task1
    command: echo hello
)";

  auto result = DAGDefinitionLoader::load_from_string(yaml);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->tasks[0].max_retries, 5);
}

TEST_F(DefaultArgsTest, LoadFromString_AppliesDefaultTriggerRule) {
  std::string yaml = R"(
name: test_dag
default_args:
  trigger_rule: all_done
tasks:
  - id: task1
    command: echo hello
)";

  auto result = DAGDefinitionLoader::load_from_string(yaml);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->tasks[0].trigger_rule, TriggerRule::AllDone);
}

TEST_F(DefaultArgsTest, LoadFromString_AppliesDependsOnPast) {
  std::string yaml = R"(
name: test_dag
default_args:
  depends_on_past: true
tasks:
  - id: task1
    command: echo hello
)";

  auto result = DAGDefinitionLoader::load_from_string(yaml);
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->tasks[0].depends_on_past);
}

TEST_F(DefaultArgsTest, LoadFromString_MultipleTasksInheritDefaults) {
  std::string yaml = R"(
name: test_dag
default_args:
  timeout: 60
  max_retries: 3
tasks:
  - id: task1
    command: echo hello
  - id: task2
    command: echo world
    timeout: 120
)";

  auto result = DAGDefinitionLoader::load_from_string(yaml);
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result->tasks.size(), 2);
  EXPECT_EQ(result->tasks[0].execution_timeout, std::chrono::seconds(60));
  EXPECT_EQ(result->tasks[0].max_retries, 3);
  EXPECT_EQ(result->tasks[1].execution_timeout, std::chrono::seconds(120));
  EXPECT_EQ(result->tasks[1].max_retries, 3);
}

class DependencyLabelTest : public ::testing::Test {};

TEST_F(DependencyLabelTest, LoadFromString_ScalarFormParsesAsEmptyLabel) {
  std::string yaml = R"(
name: test_dag
tasks:
  - id: task1
    command: echo hello
  - id: task2
    command: echo world
    dependencies: [task1]
)";

  auto result = DAGDefinitionLoader::load_from_string(yaml);
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result->tasks[1].dependencies.size(), 1);
  EXPECT_EQ(result->tasks[1].dependencies[0].task_id, TaskId("task1"));
  EXPECT_TRUE(result->tasks[1].dependencies[0].label.empty());
}

TEST_F(DependencyLabelTest, LoadFromString_MapFormParsesLabel) {
  std::string yaml = R"(
name: test_dag
tasks:
  - id: task1
    command: echo hello
  - id: task2
    command: echo world
    dependencies:
      - task: task1
        label: success_branch
)";

  auto result = DAGDefinitionLoader::load_from_string(yaml);
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result->tasks[1].dependencies.size(), 1);
  EXPECT_EQ(result->tasks[1].dependencies[0].task_id, TaskId("task1"));
  EXPECT_EQ(result->tasks[1].dependencies[0].label, "success_branch");
}

TEST_F(DependencyLabelTest, LoadFromString_MixedFormsParse) {
  std::string yaml = R"(
name: test_dag
tasks:
  - id: task1
    command: echo hello
  - id: task2
    command: echo world
  - id: task3
    command: echo final
    dependencies:
      - task1
      - task: task2
        label: branch_a
)";

  auto result = DAGDefinitionLoader::load_from_string(yaml);
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result->tasks[2].dependencies.size(), 2);
  EXPECT_EQ(result->tasks[2].dependencies[0].task_id, TaskId("task1"));
  EXPECT_TRUE(result->tasks[2].dependencies[0].label.empty());
  EXPECT_EQ(result->tasks[2].dependencies[1].task_id, TaskId("task2"));
  EXPECT_EQ(result->tasks[2].dependencies[1].label, "branch_a");
}

}
}
