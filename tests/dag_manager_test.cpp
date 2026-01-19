#include "taskmaster/dag/dag_manager.hpp"
#include "taskmaster/util/id.hpp"

#include "gtest/gtest.h"

using namespace taskmaster;

class DAGManagerTest : public ::testing::Test {
protected:
  void SetUp() override {
    dag_manager_ = std::make_unique<DAGManager>();
  }

  void TearDown() override {
    dag_manager_.reset();
  }

  auto create_simple_dag_info(std::string name) -> DAGInfo {
    DAGInfo info;
    info.name = std::move(name);
    return info;
  }

  auto create_dag_info_with_task(std::string name, TaskId task_id) -> DAGInfo {
    DAGInfo info;
    info.name = std::move(name);
    TaskConfig task;
    task.task_id = task_id;
    task.name = task_id.value();
    task.command = "echo hello";
    info.tasks.push_back(std::move(task));
    return info;
  }

  std::unique_ptr<DAGManager> dag_manager_;
};

TEST_F(DAGManagerTest, BasicConstruction) {
  DAGManager manager;
  EXPECT_EQ(manager.dag_count(), 0);
  EXPECT_FALSE(manager.has_dag(DAGId{"test"}));
}

TEST_F(DAGManagerTest, CreateDag) {
  DAGId dag_id{"test_dag"};
  auto info = create_simple_dag_info("Test DAG");

  auto result = dag_manager_->create_dag(dag_id, info);
  EXPECT_TRUE(result.has_value());
  EXPECT_EQ(dag_manager_->dag_count(), 1);
  EXPECT_TRUE(dag_manager_->has_dag(dag_id));
}

TEST_F(DAGManagerTest, CreateDuplicateDagFails) {
  DAGId dag_id{"test_dag"};
  auto info = create_simple_dag_info("Test DAG");

  auto result1 = dag_manager_->create_dag(dag_id, info);
  EXPECT_TRUE(result1.has_value());

  auto result2 = dag_manager_->create_dag(dag_id, info);
  EXPECT_FALSE(result2.has_value());
}

TEST_F(DAGManagerTest, GetDag) {
  DAGId dag_id{"test_dag"};
  auto info = create_simple_dag_info("Test DAG");
  ASSERT_TRUE(dag_manager_->create_dag(dag_id, info).has_value());

  auto result = dag_manager_->get_dag(dag_id);
  EXPECT_TRUE(result.has_value());
  EXPECT_EQ(result->name, "Test DAG");
}

TEST_F(DAGManagerTest, GetNonExistentDag) {
  auto result = dag_manager_->get_dag(DAGId{"nonexistent"});
  EXPECT_FALSE(result.has_value());
}

TEST_F(DAGManagerTest, ListDags) {
  ASSERT_TRUE(dag_manager_->create_dag(DAGId{"dag1"}, create_simple_dag_info("DAG 1")).has_value());
  ASSERT_TRUE(dag_manager_->create_dag(DAGId{"dag2"}, create_simple_dag_info("DAG 2")).has_value());
  ASSERT_TRUE(dag_manager_->create_dag(DAGId{"dag3"}, create_simple_dag_info("DAG 3")).has_value());

  auto dags = dag_manager_->list_dags();
  EXPECT_EQ(dags.size(), 3);
}

TEST_F(DAGManagerTest, ListDagsEmpty) {
  auto dags = dag_manager_->list_dags();
  EXPECT_EQ(dags.size(), 0);
}

TEST_F(DAGManagerTest, DeleteDag) {
  DAGId dag_id{"test_dag"};
  ASSERT_TRUE(dag_manager_->create_dag(dag_id, create_simple_dag_info("Test")).has_value());

  auto result = dag_manager_->delete_dag(dag_id);
  EXPECT_TRUE(result.has_value());

  EXPECT_FALSE(dag_manager_->has_dag(dag_id));
  EXPECT_EQ(dag_manager_->dag_count(), 0);
}

TEST_F(DAGManagerTest, DeleteNonExistentDag) {
  auto result = dag_manager_->delete_dag(DAGId{"nonexistent"});
  EXPECT_FALSE(result.has_value());
}

TEST_F(DAGManagerTest, AddTaskToDag) {
  DAGId dag_id{"test_dag"};
  ASSERT_TRUE(dag_manager_->create_dag(dag_id, create_simple_dag_info("Test")).has_value());

  TaskConfig task;
  task.task_id = TaskId{"task1"};
  task.name = "Task 1";
  task.command = "echo hello";

  auto result = dag_manager_->add_task(dag_id, task);
  EXPECT_TRUE(result.has_value());

  auto dag = dag_manager_->get_dag(dag_id);
  EXPECT_TRUE(dag.has_value());
  EXPECT_EQ(dag->tasks.size(), 1);
}

TEST_F(DAGManagerTest, AddTaskToNonExistentDag) {
  TaskConfig task;
  task.task_id = TaskId{"task1"};

  auto result = dag_manager_->add_task(DAGId{"nonexistent"}, task);
  EXPECT_FALSE(result.has_value());
}

TEST_F(DAGManagerTest, GetTaskFromDag) {
  DAGId dag_id{"test_dag"};
  TaskId task_id{"task1"};
  ASSERT_TRUE(dag_manager_->create_dag(dag_id, create_dag_info_with_task("Test", task_id)).has_value());

  auto result = dag_manager_->get_task(dag_id, task_id);
  EXPECT_TRUE(result.has_value());
  EXPECT_EQ(result->task_id, task_id);
}

TEST_F(DAGManagerTest, GetNonExistentTask) {
  DAGId dag_id{"test_dag"};
  ASSERT_TRUE(dag_manager_->create_dag(dag_id, create_simple_dag_info("Test")).has_value());

  auto result = dag_manager_->get_task(dag_id, TaskId{"nonexistent"});
  EXPECT_FALSE(result.has_value());
}

TEST_F(DAGManagerTest, DeleteTaskFromDag) {
  DAGId dag_id{"test_dag"};
  TaskId task_id{"task1"};
  ASSERT_TRUE(dag_manager_->create_dag(dag_id, create_dag_info_with_task("Test", task_id)).has_value());

  auto result = dag_manager_->delete_task(dag_id, task_id);
  EXPECT_TRUE(result.has_value());

  auto dag = dag_manager_->get_dag(dag_id);
  EXPECT_TRUE(dag.has_value());
  EXPECT_EQ(dag->tasks.size(), 0);
}

TEST_F(DAGManagerTest, DeleteNonExistentTask) {
  DAGId dag_id{"test_dag"};
  ASSERT_TRUE(dag_manager_->create_dag(dag_id, create_simple_dag_info("Test")).has_value());

  auto result = dag_manager_->delete_task(dag_id, TaskId{"nonexistent"});
  EXPECT_FALSE(result.has_value());
}

TEST_F(DAGManagerTest, ValidateDag) {
  DAGId dag_id{"test_dag"};
  ASSERT_TRUE(dag_manager_->create_dag(dag_id, create_simple_dag_info("Test")).has_value());

  auto result = dag_manager_->validate_dag(dag_id);
  EXPECT_TRUE(result.has_value());
}

TEST_F(DAGManagerTest, ValidateNonExistentDag) {
  auto result = dag_manager_->validate_dag(DAGId{"nonexistent"});
  EXPECT_FALSE(result.has_value());
}

TEST_F(DAGManagerTest, WouldCreateCycleSimple) {
  DAGId dag_id{"test_dag"};
  DAGInfo info;
  info.name = "Test";

  TaskConfig task1;
  task1.task_id = TaskId{"task1"};
  task1.dependencies = {};

  TaskConfig task2;
  task2.task_id = TaskId{"task2"};
  task2.dependencies = {TaskDependency{TaskId{"task1"}, ""}};

  TaskConfig task3;
  task3.task_id = TaskId{"task3"};
  task3.dependencies = {TaskDependency{TaskId{"task2"}, ""}};

  info.tasks = {task1, task2, task3};
  ASSERT_TRUE(dag_manager_->create_dag(dag_id, info).has_value());

  auto result = dag_manager_->would_create_cycle(dag_id, TaskId{"task1"}, {TaskId{"task3"}});
  EXPECT_TRUE(result);
}

TEST_F(DAGManagerTest, BuildDagGraph) {
  DAGId dag_id{"test_dag"};
  DAGInfo info;
  info.name = "Test";

  TaskConfig task1;
  task1.task_id = TaskId{"task1"};

  TaskConfig task2;
  task2.task_id = TaskId{"task2"};
  task2.dependencies = {TaskDependency{TaskId{"task1"}, ""}};

  info.tasks = {task1, task2};
  ASSERT_TRUE(dag_manager_->create_dag(dag_id, info).has_value());

  auto result = dag_manager_->build_dag_graph(dag_id);
  EXPECT_TRUE(result.has_value());
  EXPECT_EQ(result->size(), 2);
}

TEST_F(DAGManagerTest, HasDag) {
  DAGId dag_id{"test_dag"};
  ASSERT_TRUE(dag_manager_->create_dag(dag_id, create_simple_dag_info("Test")).has_value());

  EXPECT_TRUE(dag_manager_->has_dag(dag_id));
  EXPECT_FALSE(dag_manager_->has_dag(DAGId{"nonexistent"}));
}

TEST_F(DAGManagerTest, DagCount) {
  EXPECT_EQ(dag_manager_->dag_count(), 0);

  ASSERT_TRUE(dag_manager_->create_dag(DAGId{"dag1"}, create_simple_dag_info("DAG 1")).has_value());
  EXPECT_EQ(dag_manager_->dag_count(), 1);

  ASSERT_TRUE(dag_manager_->create_dag(DAGId{"dag2"}, create_simple_dag_info("DAG 2")).has_value());
  EXPECT_EQ(dag_manager_->dag_count(), 2);
}

TEST_F(DAGManagerTest, UpdateTask) {
  DAGId dag_id{"test_dag"};
  TaskId task_id{"task1"};
  ASSERT_TRUE(dag_manager_->create_dag(dag_id, create_dag_info_with_task("Test", task_id)).has_value());

  TaskConfig updated_task;
  updated_task.task_id = task_id;
  updated_task.name = "Updated Task";
  updated_task.command = "echo updated";

  auto result = dag_manager_->update_task(dag_id, task_id, updated_task);
  EXPECT_TRUE(result.has_value());

  auto task = dag_manager_->get_task(dag_id, task_id);
  EXPECT_TRUE(task.has_value());
  EXPECT_EQ(task->name, "Updated Task");
  EXPECT_EQ(task->command, "echo updated");
}

TEST_F(DAGManagerTest, UpdateNonExistentTask) {
  DAGId dag_id{"test_dag"};
  ASSERT_TRUE(dag_manager_->create_dag(dag_id, create_simple_dag_info("Test")).has_value());

  TaskConfig task;
  task.task_id = TaskId{"nonexistent"};
  task.name = "New Task";

  auto result = dag_manager_->update_task(dag_id, TaskId{"nonexistent"}, task);
  EXPECT_FALSE(result.has_value());
}

TEST_F(DAGManagerTest, ClearAll) {
  ASSERT_TRUE(dag_manager_->create_dag(DAGId{"dag1"}, create_simple_dag_info("DAG 1")).has_value());
  ASSERT_TRUE(dag_manager_->create_dag(DAGId{"dag2"}, create_simple_dag_info("DAG 2")).has_value());
  EXPECT_EQ(dag_manager_->dag_count(), 2);

  dag_manager_->clear_all();
  EXPECT_EQ(dag_manager_->dag_count(), 0);
}

TEST_F(DAGManagerTest, AddDuplicateTaskFails) {
  DAGId dag_id{"test_dag"};
  TaskId task_id{"task1"};
  ASSERT_TRUE(dag_manager_->create_dag(dag_id, create_dag_info_with_task("Test", task_id)).has_value());

  TaskConfig task;
  task.task_id = task_id;
  task.name = "Duplicate Task";
  task.command = "echo dup";

  auto result = dag_manager_->add_task(dag_id, task);
  EXPECT_FALSE(result.has_value());
}

TEST_F(DAGManagerTest, AddTaskWithInvalidDependencyFails) {
  DAGId dag_id{"test_dag"};
  ASSERT_TRUE(dag_manager_->create_dag(dag_id, create_simple_dag_info("Test")).has_value());

  TaskConfig task;
  task.task_id = TaskId{"task1"};
  task.name = "Task 1";
  task.command = "echo hello";
  task.dependencies = {TaskDependency{TaskId{"nonexistent"}, ""}};

  auto result = dag_manager_->add_task(dag_id, task);
  EXPECT_FALSE(result.has_value());
}

TEST_F(DAGManagerTest, DeleteTaskWithDependentFails) {
  DAGId dag_id{"test_dag"};
  DAGInfo info;
  info.name = "Test";

  TaskConfig task1;
  task1.task_id = TaskId{"task1"};
  task1.name = "Task 1";
  task1.command = "echo 1";

  TaskConfig task2;
  task2.task_id = TaskId{"task2"};
  task2.name = "Task 2";
  task2.command = "echo 2";
  task2.dependencies = {TaskDependency{TaskId{"task1"}, ""}};

  info.tasks = {task1, task2};
  ASSERT_TRUE(dag_manager_->create_dag(dag_id, info).has_value());

  auto result = dag_manager_->delete_task(dag_id, TaskId{"task1"});
  EXPECT_FALSE(result.has_value());
}

TEST_F(DAGManagerTest, WouldCreateCycle_ThreeNodeCycle) {
  DAGId dag_id{"cycle_dag"};
  DAGInfo info;
  info.name = "Cycle Test";

  TaskConfig task1;
  task1.task_id = TaskId{"a"};

  TaskConfig task2;
  task2.task_id = TaskId{"b"};
  task2.dependencies = {TaskDependency{TaskId{"a"}, ""}};

  TaskConfig task3;
  task3.task_id = TaskId{"c"};
  task3.dependencies = {TaskDependency{TaskId{"b"}, ""}};

  info.tasks = {task1, task2, task3};
  ASSERT_TRUE(dag_manager_->create_dag(dag_id, info).has_value());

  EXPECT_TRUE(dag_manager_->would_create_cycle(dag_id, TaskId{"a"}, {TaskId{"c"}}));
  EXPECT_FALSE(dag_manager_->would_create_cycle(dag_id, TaskId{"c"}, {TaskId{"a"}}));
}

TEST_F(DAGManagerTest, WouldCreateCycle_DiamondDependency) {
  DAGId dag_id{"diamond_dag"};
  DAGInfo info;
  info.name = "Diamond Test";

  TaskConfig start;
  start.task_id = TaskId{"start"};

  TaskConfig left;
  left.task_id = TaskId{"left"};
  left.dependencies = {TaskDependency{TaskId{"start"}, ""}};

  TaskConfig right;
  right.task_id = TaskId{"right"};
  right.dependencies = {TaskDependency{TaskId{"start"}, ""}};

  TaskConfig end;
  end.task_id = TaskId{"end"};
  end.dependencies = {{TaskId{"left"}, ""}, {TaskId{"right"}, ""}};

  info.tasks = {start, left, right, end};
  ASSERT_TRUE(dag_manager_->create_dag(dag_id, info).has_value());

  EXPECT_TRUE(dag_manager_->would_create_cycle(dag_id, TaskId{"start"}, {TaskId{"end"}}));
  EXPECT_FALSE(dag_manager_->would_create_cycle(dag_id, TaskId{"left"}, {TaskId{"right"}}));
}
