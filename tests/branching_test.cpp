#include <gtest/gtest.h>

#include "taskmaster/dag/dag.hpp"
#include "taskmaster/dag/dag_run.hpp"

#include <array>
#include <vector>

namespace taskmaster {

class BranchingTest : public ::testing::Test {
protected:
  DAG dag_;

  auto is_ready(DAGRun& run, NodeIndex idx) -> bool {
    auto info = run.get_task_info(idx);
    return info && info->state == TaskState::Pending &&
           run.get_ready_tasks().end() !=
               std::find(run.get_ready_tasks().begin(),
                         run.get_ready_tasks().end(), idx);
  }

  auto is_skipped(DAGRun& run, NodeIndex idx) -> bool {
    auto info = run.get_task_info(idx);
    return info && info->state == TaskState::Skipped;
  }
};

TEST_F(BranchingTest, IsBranchTask_ReturnsTrueForBranchNodes) {
  auto idx = dag_.add_node(TaskId("branch_task"), TriggerRule::AllSuccess, true);
  EXPECT_TRUE(dag_.is_branch_task(idx));
}

TEST_F(BranchingTest, IsBranchTask_ReturnsFalseForNormalNodes) {
  auto idx = dag_.add_node(TaskId("normal_task"), TriggerRule::AllSuccess, false);
  EXPECT_FALSE(dag_.is_branch_task(idx));
}

TEST_F(BranchingTest, IsBranchTask_DefaultIsFalse) {
  auto idx = dag_.add_node(TaskId("default_task"));
  EXPECT_FALSE(dag_.is_branch_task(idx));
}

TEST_F(BranchingTest, MarkCompletedWithBranch_SkipsUnselectedDownstream) {
  auto branch = dag_.add_node(TaskId("branch"), TriggerRule::AllSuccess, true);
  auto branch_a = dag_.add_node(TaskId("branch_a"));
  auto branch_b = dag_.add_node(TaskId("branch_b"));
  auto branch_c = dag_.add_node(TaskId("branch_c"));

  ASSERT_TRUE(dag_.add_edge(branch, branch_a).has_value());
  ASSERT_TRUE(dag_.add_edge(branch, branch_b).has_value());
  ASSERT_TRUE(dag_.add_edge(branch, branch_c).has_value());

  auto run_result = DAGRun::create(DAGRunId("run1"), dag_);
  ASSERT_TRUE(run_result.has_value());
  auto& run = *run_result;

  run.mark_task_started(branch, InstanceId("inst1"));

  std::array<TaskId, 1> selected{TaskId("branch_a")};
  run.mark_task_completed_with_branch(branch, 0, selected);

  EXPECT_TRUE(is_ready(run, branch_a));
  EXPECT_TRUE(is_skipped(run, branch_b));
  EXPECT_TRUE(is_skipped(run, branch_c));
}

TEST_F(BranchingTest, MarkCompletedWithBranch_MultipleSelectedBranches) {
  auto branch = dag_.add_node(TaskId("branch"), TriggerRule::AllSuccess, true);
  auto branch_a = dag_.add_node(TaskId("branch_a"));
  auto branch_b = dag_.add_node(TaskId("branch_b"));
  auto branch_c = dag_.add_node(TaskId("branch_c"));

  ASSERT_TRUE(dag_.add_edge(branch, branch_a).has_value());
  ASSERT_TRUE(dag_.add_edge(branch, branch_b).has_value());
  ASSERT_TRUE(dag_.add_edge(branch, branch_c).has_value());

  auto run_result = DAGRun::create(DAGRunId("run1"), dag_);
  ASSERT_TRUE(run_result.has_value());
  auto& run = *run_result;

  run.mark_task_started(branch, InstanceId("inst1"));

  std::array<TaskId, 2> selected{TaskId("branch_a"), TaskId("branch_c")};
  run.mark_task_completed_with_branch(branch, 0, selected);

  EXPECT_TRUE(is_ready(run, branch_a));
  EXPECT_TRUE(is_skipped(run, branch_b));
  EXPECT_TRUE(is_ready(run, branch_c));
}

TEST_F(BranchingTest, MarkCompletedWithBranch_EmptySelectionDoesNotSkip) {
  auto branch = dag_.add_node(TaskId("branch"), TriggerRule::AllSuccess, true);
  auto branch_a = dag_.add_node(TaskId("branch_a"));

  ASSERT_TRUE(dag_.add_edge(branch, branch_a).has_value());

  auto run_result = DAGRun::create(DAGRunId("run1"), dag_);
  ASSERT_TRUE(run_result.has_value());
  auto& run = *run_result;

  run.mark_task_started(branch, InstanceId("inst1"));

  std::span<const TaskId> empty_selection;
  run.mark_task_completed_with_branch(branch, 0, empty_selection);

  EXPECT_TRUE(is_ready(run, branch_a));
  EXPECT_FALSE(is_skipped(run, branch_a));
}

TEST_F(BranchingTest, MarkCompletedWithBranch_NonBranchTaskIgnoresSelection) {
  auto normal = dag_.add_node(TaskId("normal"), TriggerRule::AllSuccess, false);
  auto downstream = dag_.add_node(TaskId("downstream"));

  ASSERT_TRUE(dag_.add_edge(normal, downstream).has_value());

  auto run_result = DAGRun::create(DAGRunId("run1"), dag_);
  ASSERT_TRUE(run_result.has_value());
  auto& run = *run_result;

  run.mark_task_started(normal, InstanceId("inst1"));

  std::array<TaskId, 1> selected{TaskId("other")};
  run.mark_task_completed_with_branch(normal, 0, selected);

  EXPECT_TRUE(is_ready(run, downstream));
  EXPECT_FALSE(is_skipped(run, downstream));
}

TEST_F(BranchingTest, SkippedBranchPropagatesSkipToDownstream) {
  auto branch = dag_.add_node(TaskId("branch"), TriggerRule::AllSuccess, true);
  auto branch_a = dag_.add_node(TaskId("branch_a"));
  auto after_a = dag_.add_node(TaskId("after_a"));

  ASSERT_TRUE(dag_.add_edge(branch, branch_a).has_value());
  ASSERT_TRUE(dag_.add_edge(branch_a, after_a).has_value());

  auto run_result = DAGRun::create(DAGRunId("run1"), dag_);
  ASSERT_TRUE(run_result.has_value());
  auto& run = *run_result;

  run.mark_task_started(branch, InstanceId("inst1"));

  std::array<TaskId, 1> selected{TaskId("branch_b")};
  run.mark_task_completed_with_branch(branch, 0, selected);

  EXPECT_TRUE(is_skipped(run, branch_a));
  EXPECT_TRUE(is_skipped(run, after_a));
}

}  // namespace taskmaster
