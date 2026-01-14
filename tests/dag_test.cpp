#include "taskmaster/dag/dag.hpp"
#include "taskmaster/util/id.hpp"

#include "gtest/gtest.h"

using namespace taskmaster;

class DAGTest : public ::testing::Test {
protected:
  DAG dag_;
};

TEST_F(DAGTest, EmptyDAG) {
  EXPECT_EQ(dag_.size(), 0);
  EXPECT_TRUE(dag_.empty());
  EXPECT_TRUE(dag_.is_valid().has_value());
}

TEST_F(DAGTest, AddSingleNode) {
  auto idx = dag_.add_node(TaskId("task1"));
  EXPECT_NE(idx, kInvalidNode);
  EXPECT_EQ(dag_.size(), 1);
  EXPECT_TRUE(dag_.has_node(TaskId("task1")));
  EXPECT_EQ(dag_.get_index(TaskId("task1")), idx);
  EXPECT_EQ(dag_.get_key(idx), TaskId("task1"));
}

TEST_F(DAGTest, AddDuplicateNodeReturnsSameIndex) {
  auto idx1 = dag_.add_node(TaskId("task1"));
  auto idx2 = dag_.add_node(TaskId("task1"));
  EXPECT_EQ(idx1, idx2);
  EXPECT_EQ(dag_.size(), 1);
}

TEST_F(DAGTest, AddMultipleNodes) {
  dag_.add_node(TaskId("task1"));
  dag_.add_node(TaskId("task2"));
  dag_.add_node(TaskId("task3"));

  EXPECT_EQ(dag_.size(), 3);
  EXPECT_TRUE(dag_.has_node(TaskId("task1")));
  EXPECT_TRUE(dag_.has_node(TaskId("task2")));
  EXPECT_TRUE(dag_.has_node(TaskId("task3")));

  auto all_nodes = dag_.all_nodes();
  EXPECT_EQ(all_nodes.size(), 3);
}

TEST_F(DAGTest, AddEdgeCreatesDependency) {
  auto from = dag_.add_node(TaskId("task1"));
  auto to = dag_.add_node(TaskId("task2"));

  auto result = dag_.add_edge(TaskId("task1"), TaskId("task2"));
  EXPECT_TRUE(result.has_value());

  auto deps = dag_.get_deps(to);
  EXPECT_EQ(deps.size(), 1);
  EXPECT_EQ(deps[0], from);

  auto dependents = dag_.get_dependents(from);
  EXPECT_EQ(dependents.size(), 1);
  EXPECT_EQ(dependents[0], to);
}

TEST_F(DAGTest, AddEdgeByIndex) {
  auto from = dag_.add_node(TaskId("task1"));
  auto to = dag_.add_node(TaskId("task2"));

  auto result = dag_.add_edge(from, to);
  EXPECT_TRUE(result.has_value());

  auto deps = dag_.get_deps(to);
  EXPECT_EQ(deps.size(), 1);
  EXPECT_EQ(deps[0], from);
}

TEST_F(DAGTest, GetDepsView) {
  dag_.add_node(TaskId("task1"));
  dag_.add_node(TaskId("task2"));
  dag_.add_node(TaskId("task3"));

  dag_.add_edge(TaskId("task1"), TaskId("task3"));
  dag_.add_edge(TaskId("task2"), TaskId("task3"));

  auto idx3 = dag_.get_index(TaskId("task3"));
  auto deps_view = dag_.get_deps_view(idx3);
  EXPECT_EQ(deps_view.size(), 2);
}

TEST_F(DAGTest, GetDependentsView) {
  dag_.add_node(TaskId("task1"));
  dag_.add_node(TaskId("task2"));
  dag_.add_node(TaskId("task3"));

  dag_.add_edge(TaskId("task1"), TaskId("task2"));
  dag_.add_edge(TaskId("task1"), TaskId("task3"));

  auto idx1 = dag_.get_index(TaskId("task1"));
  auto dependents_view = dag_.get_dependents_view(idx1);
  EXPECT_EQ(dependents_view.size(), 2);
}

TEST_F(DAGTest, TopologicalOrder) {
  dag_.add_node(TaskId("task1"));
  dag_.add_node(TaskId("task2"));
  dag_.add_node(TaskId("task3"));

  dag_.add_edge(TaskId("task1"), TaskId("task2"));
  dag_.add_edge(TaskId("task2"), TaskId("task3"));

  auto order = dag_.get_topological_order();
  EXPECT_EQ(order.size(), 3);
  EXPECT_EQ(order[0], TaskId("task1"));
  EXPECT_EQ(order[1], TaskId("task2"));
  EXPECT_EQ(order[2], TaskId("task3"));
}

TEST_F(DAGTest, CycleDetection) {
  dag_.add_node(TaskId("task1"));
  dag_.add_node(TaskId("task2"));
  dag_.add_node(TaskId("task3"));

  EXPECT_TRUE(dag_.add_edge(TaskId("task1"), TaskId("task2")).has_value());
  EXPECT_TRUE(dag_.add_edge(TaskId("task2"), TaskId("task3")).has_value());
  auto cycle_result = dag_.add_edge(TaskId("task3"), TaskId("task1"));
  EXPECT_FALSE(cycle_result.has_value());
}

TEST_F(DAGTest, IsValidWithCycle) {
  dag_.add_node(TaskId("task1"));
  dag_.add_node(TaskId("task2"));

  dag_.add_edge(TaskId("task1"), TaskId("task2"));

  EXPECT_TRUE(dag_.is_valid().has_value());
}

TEST_F(DAGTest, Clear) {
  dag_.add_node(TaskId("task1"));
  dag_.add_node(TaskId("task2"));
  dag_.add_edge(TaskId("task1"), TaskId("task2"));

  EXPECT_EQ(dag_.size(), 2);

  dag_.clear();

  EXPECT_EQ(dag_.size(), 0);
  EXPECT_TRUE(dag_.empty());
}

TEST_F(DAGTest, GetNonExistentNode) {
  auto idx = dag_.get_index(TaskId("nonexistent"));
  EXPECT_EQ(idx, kInvalidNode);
}

TEST_F(DAGTest, HasNodeNonExistent) {
  EXPECT_FALSE(dag_.has_node(TaskId("nonexistent")));
}

TEST_F(DAGTest, AllNodes) {
  dag_.add_node(TaskId("a"));
  dag_.add_node(TaskId("b"));
  dag_.add_node(TaskId("c"));

  auto nodes = dag_.all_nodes();
  EXPECT_EQ(nodes.size(), 3);
}

TEST_F(DAGTest, ComplexDAG) {
  dag_.add_node(TaskId("start"));
  dag_.add_node(TaskId("middle1"));
  dag_.add_node(TaskId("middle2"));
  dag_.add_node(TaskId("end"));

  EXPECT_TRUE(dag_.add_edge(TaskId("start"), TaskId("middle1")).has_value());
  EXPECT_TRUE(dag_.add_edge(TaskId("start"), TaskId("middle2")).has_value());
  EXPECT_TRUE(dag_.add_edge(TaskId("middle1"), TaskId("end")).has_value());
  EXPECT_TRUE(dag_.add_edge(TaskId("middle2"), TaskId("end")).has_value());

  EXPECT_TRUE(dag_.is_valid().has_value());

  auto order = dag_.get_topological_order();
  EXPECT_EQ(order.size(), 4);
  EXPECT_EQ(order[0], TaskId("start"));
  EXPECT_EQ(order[3], TaskId("end"));
}

TEST_F(DAGTest, AddEdge_SelfLoop_Fails) {
  dag_.add_node(TaskId("task1"));

  auto result = dag_.add_edge(TaskId("task1"), TaskId("task1"));

  EXPECT_FALSE(result.has_value());
}

TEST_F(DAGTest, AddEdge_TwoNodeCycle_Fails) {
  dag_.add_node(TaskId("task1"));
  dag_.add_node(TaskId("task2"));

  EXPECT_TRUE(dag_.add_edge(TaskId("task1"), TaskId("task2")).has_value());
  auto result = dag_.add_edge(TaskId("task2"), TaskId("task1"));

  EXPECT_FALSE(result.has_value());
}

TEST_F(DAGTest, AddEdge_NonExistentSource_Fails) {
  dag_.add_node(TaskId("task2"));

  auto result = dag_.add_edge(TaskId("nonexistent"), TaskId("task2"));

  EXPECT_FALSE(result.has_value());
}
