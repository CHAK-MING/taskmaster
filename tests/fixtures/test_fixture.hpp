#pragma once

#include "dagforge/app/services/persistence_service.hpp"

#include "../test_utils.hpp"
#include "gtest/gtest.h"

#include <memory>
#include <string_view>

namespace dagforge::test {

class TestBase : public ::testing::Test {
protected:
  [[nodiscard]] auto unique_token(std::string_view base) const -> std::string {
    return dagforge::test::unique_token(base);
  }
};

class DatabaseTest : public TestBase {
protected:
  struct PersistedDag {
    DAGId dag_id;
    TaskId task_id;
    int64_t dag_rowid{0};
    int64_t task_rowid{0};
  };

  void SetUp() override {
    ASSERT_TRUE(runtime_.start().has_value());
    service_ =
        std::make_unique<PersistenceService>(runtime_, load_test_db_config());
    auto open_res = run_coro(service_->open());
    if (!open_res) {
      GTEST_SKIP() << "MySQL unavailable for database tests: "
                   << open_res.error().message();
    }
    db_ready_ = true;
    auto clear_res = run_coro(service_->clear_all_dag_data());
    ASSERT_TRUE(clear_res.has_value()) << clear_res.error().message();
  }

  void TearDown() override {
    if (service_ && db_ready_) {
      auto close_res = run_coro(service_->close());
      EXPECT_TRUE(close_res.has_value()) << close_res.error().message();
    }
    service_.reset();
    runtime_.stop();
  }

  auto create_persisted_dag(const char *dag_name, const char *task_name)
      -> PersistedDag {
    PersistedDag out;
    out.dag_id = DAGId(unique_token(dag_name));
    out.task_id = TaskId(unique_token(task_name));

    DAGInfo dag{};
    dag.dag_id = out.dag_id;
    dag.name = out.dag_id.str();
    dag.created_at = std::chrono::system_clock::now();
    dag.updated_at = dag.created_at;

    auto dag_rowid = run_coro(service_->save_dag(dag));
    EXPECT_TRUE(dag_rowid.has_value());
    out.dag_rowid = dag_rowid.value_or(0);

    auto task = TaskConfig::builder()
                    .id(out.task_id.str())
                    .name(out.task_id.str())
                    .command("echo ok")
                    .build();
    EXPECT_TRUE(task.has_value());

    auto task_rowid = run_coro(service_->save_task(out.dag_id, *task));
    EXPECT_TRUE(task_rowid.has_value());
    out.task_rowid = task_rowid.value_or(0);
    return out;
  }

  Runtime runtime_{1};
  std::unique_ptr<PersistenceService> service_;
  bool db_ready_{false};
};

} // namespace dagforge::test
