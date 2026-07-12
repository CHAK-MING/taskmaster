#include "dagforge/app/services/persistence_service.hpp"
#include "dagforge/cli/management_client.hpp"
#include "dagforge/config/task_config.hpp"
#include "dagforge/core/runtime.hpp"
#include "fixtures/test_fixture.hpp"
#include "test_utils.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <thread>

using namespace dagforge;
using namespace dagforge::test;

class PersistenceTest : public dagforge::test::DatabaseTest {
protected:
  auto create_run(const DAGRunId &run_id, int64_t dag_rowid) -> Result<DAGRun> {
    auto graph = std::make_shared<DAG>();
    if (auto add = graph->add_node(TaskId{"task_1"}); !add) {
      return fail(add.error());
    }
    auto run = DAGRun::create(run_id, graph);
    if (!run) {
      return fail(run.error());
    }
    run->set_dag_rowid(dag_rowid);
    run->set_dag_version(1);
    run->set_trigger_type(TriggerType::Manual);
    const auto now = std::chrono::system_clock::now();
    run->set_scheduled_at(now);
    run->set_started_at(now);
    run->set_finished_at(now);
    run->set_execution_date(now);
    return run;
  }

};

class PersistenceSmallTaskUpdateQueueTest : public PersistenceTest {
protected:
  void SetUp() override {
    ASSERT_TRUE(runtime_.start().has_value());
    service_ = std::make_unique<PersistenceService>(
        runtime_, load_test_db_config(), 4096, 1);
    auto open_res = run_coro(service_->open());
    if (!open_res) {
      GTEST_SKIP() << "MySQL unavailable for persistence queue tests: "
                   << open_res.error().message();
    }
    db_ready_ = true;
    auto clear_res = run_coro(service_->clear_all_dag_data());
    ASSERT_TRUE(clear_res.has_value()) << clear_res.error().message();
  }
};

TEST(PersistenceServiceLifecycleTest,
     ResetMysqlTestDatabaseDoesNotAbortOnOpenFailure) {
  EXPECT_EXIT(
      {
        auto bad_cfg = load_test_db_config();
        bad_cfg.username = "__dagforge_invalid_user__";
        bad_cfg.password = "__dagforge_invalid_password__";
        bad_cfg.database = unique_token("dagforge_invalid_db");
        reset_mysql_test_database(bad_cfg);
        std::_Exit(0);
      },
      ::testing::ExitedWithCode(0), "");
}

TEST(PersistenceServiceLifecycleTest,
     FailedOpenDoesNotPoisonSubsequentFreshServiceOpen) {
  auto bad_cfg = load_test_db_config();
  bad_cfg.host = "127.0.0.1";
  bad_cfg.port = pick_unused_tcp_port_or_zero();
  ASSERT_NE(bad_cfg.port, 0);
  bad_cfg.connect_timeout = 1;

  Runtime bad_runtime(1);
  ASSERT_TRUE(bad_runtime.start().has_value());
  PersistenceService bad_service(bad_runtime, bad_cfg);

  auto bad_open = run_coro(bad_service.open(), std::chrono::seconds(5));
  ASSERT_FALSE(bad_open.has_value());
  EXPECT_FALSE(bad_service.is_open());

  bad_runtime.stop();

  auto good_cfg = load_test_db_config();
  Runtime good_runtime(1);
  ASSERT_TRUE(good_runtime.start().has_value());
  PersistenceService good_service(good_runtime, good_cfg);

  auto good_open = run_coro(good_service.open(), std::chrono::seconds(10));
  if (!good_open) {
    GTEST_SKIP() << "MySQL unavailable for recovery lifecycle test: "
                 << good_open.error().message();
  }

  EXPECT_TRUE(good_service.is_open());

  auto dags = run_coro(good_service.list_dags());
  EXPECT_TRUE(dags.has_value()) << dags.error().message();

  auto close_res = run_coro(good_service.close());
  ASSERT_TRUE(close_res.has_value()) << close_res.error().message();
  good_runtime.stop();
}

TEST_F(PersistenceTest, SaveDagAndListDags_RealServicePath) {
  auto persisted = create_persisted_dag("persist_dag", "persist_task");
  ASSERT_GT(persisted.dag_rowid, 0);
  ASSERT_GT(persisted.task_rowid, 0);

  auto dags = run_coro(service_->list_dags());
  ASSERT_TRUE(dags.has_value());
  ASSERT_FALSE(dags->empty());
  const auto it = std::ranges::find_if(
      *dags, [&](const DAGInfo &d) { return d.dag_id == persisted.dag_id; });
  EXPECT_NE(it, dags->end());
}

TEST_F(PersistenceTest, CloseAndReopenRestoresDatabaseOperations) {
  auto initial = create_persisted_dag("reopen_dag", "reopen_task");
  ASSERT_GT(initial.dag_rowid, 0);

  auto close_res = run_coro(service_->close());
  ASSERT_TRUE(close_res.has_value()) << close_res.error().message();
  EXPECT_FALSE(service_->is_open());

  auto reopen = run_coro(service_->open(), std::chrono::seconds(10));
  ASSERT_TRUE(reopen.has_value()) << reopen.error().message();
  EXPECT_TRUE(service_->is_open());

  auto dags = run_coro(service_->list_dags());
  ASSERT_TRUE(dags.has_value()) << dags.error().message();
  const auto it = std::ranges::find_if(
      *dags, [&](const DAGInfo &d) { return d.dag_id == initial.dag_id; });
  EXPECT_NE(it, dags->end());
}

TEST_F(PersistenceTest, RepeatedCloseAndReopenQuiescesBatchWriters) {
  for (int iteration = 0; iteration < 25; ++iteration) {
    SCOPED_TRACE(iteration);

    auto close_res = run_coro(service_->close());
    ASSERT_TRUE(close_res.has_value()) << close_res.error().message();
    EXPECT_FALSE(service_->is_open());

    auto reopen_res = run_coro(service_->open(), std::chrono::seconds(10));
    ASSERT_TRUE(reopen_res.has_value()) << reopen_res.error().message();
    EXPECT_TRUE(service_->is_open());
  }
}

TEST_F(PersistenceTest, GetDagRunState_NonexistentId_ReturnsNotFound) {
  auto result = run_coro(service_->get_dag_run_state(DAGRunId("nonexistent")));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), Error::NotFound);
}

TEST_F(PersistenceTest, SaveTaskInstance_NonexistentRun_ReturnsNotFound) {
  TaskInstanceInfo info{};
  info.task_rowid = 1;
  info.attempt = 1;
  info.state = TaskState::Running;
  info.started_at = std::chrono::system_clock::now();

  auto result =
      run_coro(service_->update_task_instance(DAGRunId("missing_run"), info));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), Error::NotFound);
}

TEST_F(PersistenceTest,
       SubmitTaskInstanceUpdate_EventuallyPersistsWithoutAwaitingReply) {
  auto persisted =
      create_persisted_dag("submit_update_dag", "submit_update_task");
  ASSERT_GT(persisted.dag_rowid, 0);
  ASSERT_GT(persisted.task_rowid, 0);

  const DAGRunId run_id{unique_token("submit_update_run")};
  auto run = create_run(run_id, persisted.dag_rowid);
  ASSERT_TRUE(run.has_value());

  TaskInstanceInfo initial{};
  initial.task_rowid = persisted.task_rowid;
  initial.attempt = 1;
  initial.state = TaskState::Ready;

  auto create_res = run_coro(service_->create_run_with_task_instances(
      std::move(*run), std::vector<TaskInstanceInfo>{initial}));
  ASSERT_TRUE(create_res.has_value());

  auto tasks = run_coro(service_->get_task_instances(run_id));
  ASSERT_TRUE(tasks.has_value());
  ASSERT_EQ(tasks->size(), 1U);

  auto updated = tasks->front();
  updated.state = TaskState::Success;
  updated.finished_at = std::chrono::system_clock::now();
  updated.exit_code = 0;

  service_->submit_task_instance_update(run_id, updated);

  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);
  bool persisted_success = false;
  while (std::chrono::steady_clock::now() < deadline) {
    auto current = run_coro(service_->get_task_instances(run_id));
    ASSERT_TRUE(current.has_value());
    ASSERT_EQ(current->size(), 1U);
    if (current->front().state == TaskState::Success) {
      persisted_success = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  ASSERT_TRUE(persisted_success);
}

TEST_F(PersistenceSmallTaskUpdateQueueTest,
       SubmitTaskInstanceUpdateBackpressureStaysOnBatchQueue) {
  auto persisted =
      create_persisted_dag("queued_update_dag", "queued_update_task");
  ASSERT_GT(persisted.dag_rowid, 0);
  ASSERT_GT(persisted.task_rowid, 0);

  const DAGRunId run_id{unique_token("queued_update_run")};
  auto run = create_run(run_id, persisted.dag_rowid);
  ASSERT_TRUE(run.has_value());

  TaskInstanceInfo initial{};
  initial.task_rowid = persisted.task_rowid;
  initial.attempt = 1;
  initial.state = TaskState::Ready;

  auto create_res = run_coro(service_->create_run_with_task_instances(
      std::move(*run), std::vector<TaskInstanceInfo>{initial}));
  ASSERT_TRUE(create_res.has_value());

  auto tasks = run_coro(service_->get_task_instances(run_id));
  ASSERT_TRUE(tasks.has_value());
  ASSERT_EQ(tasks->size(), 1U);

  auto updated = tasks->front();
  updated.state = TaskState::Success;
  updated.finished_at = std::chrono::system_clock::now();
  updated.exit_code = 0;

  for (int i = 0; i < 512; ++i) {
    service_->submit_task_instance_update(run_id, updated);
  }

  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while ((service_->task_update_batch_requests_total() +
          service_->task_update_batch_fallback_total()) < 512 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  EXPECT_GT(service_->task_update_batch_rejected_total(), 0U);
  EXPECT_EQ(service_->task_update_batch_fallback_total(), 0U);
}

TEST_F(PersistenceTest, MarkIncompleteRunsFailed_RealServicePath) {
  auto persisted = create_persisted_dag("recovery_dag", "recovery_task");
  ASSERT_GT(persisted.dag_rowid, 0);
  ASSERT_GT(persisted.task_rowid, 0);

  const DAGRunId run_id{unique_token("recovery_run")};
  auto run = create_run(run_id, persisted.dag_rowid);
  ASSERT_TRUE(run.has_value());

  TaskInstanceInfo ti{};
  ti.task_rowid = persisted.task_rowid;
  ti.attempt = 1;
  ti.state = TaskState::Running;
  ti.started_at = std::chrono::system_clock::now();

  auto create_res = run_coro(service_->create_run_with_task_instances(
      std::move(*run), std::vector<TaskInstanceInfo>{ti}));
  ASSERT_TRUE(create_res.has_value());
  EXPECT_GT(create_res.value(), 0);

  auto mark_res = run_coro(service_->mark_incomplete_runs_failed());
  ASSERT_TRUE(mark_res.has_value());
  EXPECT_GE(mark_res.value(), 1U);

  auto state = run_coro(service_->get_dag_run_state(run_id));
  ASSERT_TRUE(state.has_value());
  EXPECT_EQ(state.value(), DAGRunState::Failed);
}

TEST_F(PersistenceTest, ClaimTaskInstances_ClaimsReadyRowsAsRunning) {
  auto persisted = create_persisted_dag("claim_dag", "claim_task");
  ASSERT_GT(persisted.dag_rowid, 0);
  ASSERT_GT(persisted.task_rowid, 0);

  const DAGRunId run_id{unique_token("claim_run")};
  auto run = create_run(run_id, persisted.dag_rowid);
  ASSERT_TRUE(run.has_value());

  TaskInstanceInfo ti{};
  ti.task_rowid = persisted.task_rowid;
  ti.attempt = 1;
  ti.state = TaskState::Ready;

  auto create_res = run_coro(service_->create_run_with_task_instances(
      std::move(*run), std::vector<TaskInstanceInfo>{ti}));
  ASSERT_TRUE(create_res.has_value());

  auto claimed = run_coro(service_->claim_task_instances(8, "worker-a"));
  ASSERT_TRUE(claimed.has_value());
  ASSERT_FALSE(claimed->empty());

  const auto it =
      std::ranges::find_if(*claimed, [&](const ClaimedTaskInstance &c) {
        return c.dag_run_id == run_id && c.task_rowid == persisted.task_rowid;
      });
  ASSERT_NE(it, claimed->end());
  EXPECT_EQ(it->attempt, 1);

  auto tasks = run_coro(service_->get_task_instances(run_id));
  ASSERT_TRUE(tasks.has_value());
  ASSERT_FALSE(tasks->empty());
  EXPECT_EQ(tasks->front().state, TaskState::Running);

  auto hb = run_coro(service_->touch_task_heartbeat(TaskInstanceKey{
      .run_rowid = it->run_rowid,
      .task_rowid = persisted.task_rowid,
      .attempt = 1,
  }));
  EXPECT_TRUE(hb.has_value());

  auto hb_again = run_coro(service_->touch_task_heartbeat(TaskInstanceKey{
      .run_rowid = it->run_rowid,
      .task_rowid = persisted.task_rowid,
      .attempt = 1,
  }));
  EXPECT_TRUE(hb_again.has_value());
}

TEST_F(PersistenceTest,
       CreateRunWithTaskInstances_NormalizesInitialAttemptToOne) {
  auto persisted =
      create_persisted_dag("attempt_norm_dag", "attempt_norm_task");
  ASSERT_GT(persisted.dag_rowid, 0);
  ASSERT_GT(persisted.task_rowid, 0);

  const DAGRunId run_id{unique_token("attempt_norm_run")};
  auto run = create_run(run_id, persisted.dag_rowid);
  ASSERT_TRUE(run.has_value());

  TaskInstanceInfo ti{};
  ti.task_rowid = persisted.task_rowid;
  ti.attempt = 0;
  ti.state = TaskState::Ready;

  auto create_res = run_coro(service_->create_run_with_task_instances(
      std::move(*run), std::vector<TaskInstanceInfo>{ti}));
  ASSERT_TRUE(create_res.has_value());

  auto tasks = run_coro(service_->get_task_instances(run_id));
  ASSERT_TRUE(tasks.has_value());
  ASSERT_EQ(tasks->size(), 1U);
  EXPECT_EQ(tasks->front().attempt, 1);
}

TEST_F(PersistenceTest, ClearFailedTasksRebuildsReadyStateAndRunState) {
  auto persisted = create_persisted_dag("clear_dag", "clear_task");
  ASSERT_GT(persisted.dag_rowid, 0);
  ASSERT_GT(persisted.task_rowid, 0);

  const DAGRunId run_id{unique_token("clear_run")};
  auto run = create_run(run_id, persisted.dag_rowid);
  ASSERT_TRUE(run.has_value());

  TaskInstanceInfo ti{};
  ti.task_rowid = persisted.task_rowid;
  ti.attempt = 1;
  ti.state = TaskState::Failed;
  ti.finished_at = std::chrono::system_clock::now();
  ti.exit_code = 1;
  ti.error_message = "boom";
  ti.error_type = "executor_error";

  auto create_res = run_coro(service_->create_run_with_task_instances(
      std::move(*run), std::vector<TaskInstanceInfo>{ti}));
  ASSERT_TRUE(create_res.has_value());

  auto mark_failed = run_coro(service_->mark_incomplete_runs_failed());
  ASSERT_TRUE(mark_failed.has_value());
  EXPECT_GE(mark_failed.value(), 1U);

  auto failed_state = run_coro(service_->get_dag_run_state(run_id));
  ASSERT_TRUE(failed_state.has_value());
  EXPECT_EQ(failed_state.value(), DAGRunState::Failed);

  cli::ManagementClient client(load_test_db_config());
  auto client_open = client.open();
  ASSERT_TRUE(client_open.has_value());

  auto clear_res = client.clear_failed_tasks(run_id);
  ASSERT_TRUE(clear_res.has_value()) << clear_res.error().message();

  auto run_state = run_coro(service_->get_dag_run_state(run_id));
  ASSERT_TRUE(run_state.has_value());
  EXPECT_EQ(run_state.value(), DAGRunState::Running);

  auto tasks = run_coro(service_->get_task_instances(run_id));
  ASSERT_TRUE(tasks.has_value());
  ASSERT_EQ(tasks->size(), 1U);
  EXPECT_EQ(tasks->front().state, TaskState::Ready);
  EXPECT_EQ(tasks->front().exit_code, 0);
  EXPECT_TRUE(tasks->front().error_message.empty());
  EXPECT_TRUE(tasks->front().error_type.empty());
  EXPECT_EQ(tasks->front().started_at, std::chrono::system_clock::time_point{});
  EXPECT_EQ(tasks->front().finished_at,
            std::chrono::system_clock::time_point{});
}

TEST_F(PersistenceTest,
       ReapZombieTaskInstances_MarksExpiredRunningTasksFailed) {
  auto persisted = create_persisted_dag("zombie_dag", "zombie_task");
  ASSERT_GT(persisted.dag_rowid, 0);
  ASSERT_GT(persisted.task_rowid, 0);

  const DAGRunId run_id{unique_token("zombie_run")};
  auto run = create_run(run_id, persisted.dag_rowid);
  ASSERT_TRUE(run.has_value());

  TaskInstanceInfo ti{};
  ti.task_rowid = persisted.task_rowid;
  ti.attempt = 1;
  ti.state = TaskState::Running;
  ti.started_at = std::chrono::system_clock::now();

  auto create_res = run_coro(service_->create_run_with_task_instances(
      std::move(*run), std::vector<TaskInstanceInfo>{ti}));
  ASSERT_TRUE(create_res.has_value());

  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  auto reaped = run_coro(service_->reap_zombie_task_instances(1));
  ASSERT_TRUE(reaped.has_value());
  EXPECT_GE(*reaped, 1U);

  auto tasks = run_coro(service_->get_task_instances(run_id));
  ASSERT_TRUE(tasks.has_value());
  ASSERT_FALSE(tasks->empty());
  EXPECT_EQ(tasks->front().state, TaskState::Failed);
}

TEST_F(PersistenceTest, GetRunHistory_FindsOlderRunBeyondListWindow) {
  auto persisted = create_persisted_dag("history_dag", "history_task");
  ASSERT_GT(persisted.dag_rowid, 0);
  ASSERT_GT(persisted.task_rowid, 0);

  constexpr int kRuns = 1005;
  const auto base = std::chrono::system_clock::now() - std::chrono::hours(1);
  DAGRunId oldest_id{"uninitialized"};

  for (int i = 0; i < kRuns; ++i) {
    const DAGRunId run_id{unique_token("history_run")};
    if (i == 0) {
      oldest_id = run_id;
    }

    auto run = create_run(run_id, persisted.dag_rowid);
    ASSERT_TRUE(run.has_value());
    const auto ts = base + std::chrono::milliseconds(i);
    run->set_scheduled_at(ts);
    run->set_started_at(ts);
    run->set_finished_at(ts);
    run->set_execution_date(ts);

    TaskInstanceInfo ti{};
    ti.task_rowid = persisted.task_rowid;
    ti.attempt = 1;
    ti.state = TaskState::Success;
    ti.started_at = ts;
    ti.finished_at = ts;

    auto create_res = run_coro(service_->create_run_with_task_instances(
        std::move(*run), std::vector<TaskInstanceInfo>{ti}));
    ASSERT_TRUE(create_res.has_value());
  }

  auto history = run_coro(service_->get_run_history(oldest_id));
  ASSERT_TRUE(history.has_value());
  EXPECT_EQ(history->dag_run_id, oldest_id);
}

TEST_F(PersistenceTest, MarkIncompleteRunsFailed_DoesNotFailPendingTasks) {
  auto persisted =
      create_persisted_dag("recovery_mix_dag", "recovery_mix_task");
  ASSERT_GT(persisted.dag_rowid, 0);
  ASSERT_GT(persisted.task_rowid, 0);

  auto second_task = TaskConfig::builder()
                         .id(unique_token("recovery_mix_task2"))
                         .name("recovery_mix_task2")
                         .command("echo ok")
                         .build();
  ASSERT_TRUE(second_task.has_value());
  auto second_task_rowid =
      run_coro(service_->save_task(persisted.dag_id, *second_task));
  ASSERT_TRUE(second_task_rowid.has_value());

  const DAGRunId run_id{unique_token("recovery_mix_run")};
  auto run = create_run(run_id, persisted.dag_rowid);
  ASSERT_TRUE(run.has_value());

  const auto now = std::chrono::system_clock::now();
  TaskInstanceInfo running_ti{};
  running_ti.task_rowid = persisted.task_rowid;
  running_ti.attempt = 1;
  running_ti.state = TaskState::Running;
  running_ti.started_at = now;

  TaskInstanceInfo pending_ti{};
  pending_ti.task_rowid = *second_task_rowid;
  pending_ti.attempt = 0;
  pending_ti.state = TaskState::Pending;

  auto create_res = run_coro(service_->create_run_with_task_instances(
      std::move(*run), std::vector<TaskInstanceInfo>{running_ti, pending_ti}));
  ASSERT_TRUE(create_res.has_value());

  auto mark_res = run_coro(service_->mark_incomplete_runs_failed());
  ASSERT_TRUE(mark_res.has_value());

  auto tasks = run_coro(service_->get_task_instances(run_id));
  ASSERT_TRUE(tasks.has_value());
  ASSERT_EQ(tasks->size(), 2U);

  auto find_by_rowid = [&](int64_t rowid) {
    return std::ranges::find_if(*tasks, [&](const TaskInstanceInfo &ti) {
      return ti.task_rowid == rowid;
    });
  };

  auto running_it = find_by_rowid(persisted.task_rowid);
  ASSERT_NE(running_it, tasks->end());
  EXPECT_EQ(running_it->state, TaskState::Failed);

  auto pending_it = find_by_rowid(*second_task_rowid);
  ASSERT_NE(pending_it, tasks->end());
  EXPECT_EQ(pending_it->state, TaskState::Pending);
}

TEST_F(PersistenceTest,
       UpsertDagInfoPersistsMultipleTasksAndRebuildsDependencies) {
  DAGInfo dag{};
  dag.dag_id = DAGId{unique_token("bulk_upsert_dag")};
  dag.name = dag.dag_id.str();
  dag.created_at = std::chrono::system_clock::now();
  dag.updated_at = dag.created_at;

  auto producer = TaskConfig::builder()
                      .id("producer")
                      .name("producer")
                      .command("echo producer")
                      .build();
  ASSERT_TRUE(producer.has_value());

  auto consumer = TaskConfig::builder()
                      .id("consumer")
                      .name("consumer")
                      .command("echo consumer")
                      .depends_on("producer")
                      .build();
  ASSERT_TRUE(consumer.has_value());

  dag.tasks = {std::move(*producer), std::move(*consumer)};
  dag.rebuild_task_index();
  const auto dag_id = dag.dag_id.clone();

  auto upserted =
      run_coro(service_->upsert_dag_info(dag_id, std::move(dag), false));
  ASSERT_TRUE(upserted.has_value()) << upserted.error().message();
  ASSERT_EQ(upserted->tasks.size(), 2U);
  EXPECT_GT(upserted->dag_rowid, 0);
  EXPECT_GT(upserted->tasks[0].task_rowid, 0);
  EXPECT_GT(upserted->tasks[1].task_rowid, 0);

  auto loaded = run_coro(service_->get_dag(upserted->dag_id));
  ASSERT_TRUE(loaded.has_value()) << loaded.error().message();
  ASSERT_EQ(loaded->tasks.size(), 2U);
  const auto *loaded_producer = loaded->find_task(TaskId{"producer"});
  const auto *loaded_consumer = loaded->find_task(TaskId{"consumer"});
  ASSERT_NE(loaded_producer, nullptr);
  ASSERT_NE(loaded_consumer, nullptr);
  EXPECT_GT(loaded_producer->task_rowid, 0);
  EXPECT_GT(loaded_consumer->task_rowid, 0);
  ASSERT_EQ(loaded_consumer->dependencies.size(), 1U);
  EXPECT_EQ(loaded_consumer->dependencies.front().task_id, TaskId{"producer"});
}

TEST(PersistenceOpenTest, OpenWithInvalidEndpointFailsGracefully) {
  Runtime runtime(1);
  ASSERT_TRUE(runtime.start().has_value());

  DatabaseConfig cfg = load_test_db_config();
  cfg.host = "127.0.0.1";
  cfg.port = 1;
  cfg.connect_timeout = 1;

  PersistenceService service(runtime, cfg);
  auto open_res = run_coro(service.open());
  EXPECT_FALSE(open_res.has_value());

  runtime.stop();
}

TEST(PersistenceOpenTest, OpenFailureIncrementsDbErrorsTotal) {
  Runtime runtime(1);
  ASSERT_TRUE(runtime.start().has_value());

  DatabaseConfig cfg = load_test_db_config();
  cfg.host = "127.0.0.1";
  cfg.port = 1;
  cfg.connect_timeout = 1;

  PersistenceService service(runtime, cfg);
  auto open_res = run_coro(service.open());
  EXPECT_FALSE(open_res.has_value());
  EXPECT_GT(service.db_errors_total(), 0U);

  runtime.stop();
}

TEST(PersistenceOpenTest, SyncWaitOpenSucceedsWhenDatabaseIsReachable) {
  Runtime runtime(1);
  ASSERT_TRUE(runtime.start().has_value());

  PersistenceService service(runtime, load_test_db_config());
  auto open_res = service.sync_wait(service.open());
  if (!open_res) {
    runtime.stop();
    GTEST_SKIP() << "MySQL unavailable for sync_wait open test: "
                 << open_res.error().message();
  }

  EXPECT_TRUE(open_res.has_value());
  auto close_res = service.sync_wait(service.close());
  ASSERT_TRUE(close_res.has_value()) << close_res.error().message();
  runtime.stop();
}

TEST(PersistenceOpenTest, SuccessfulOpenAndCloseIncrementTransactionsTotal) {
  Runtime runtime(1);
  ASSERT_TRUE(runtime.start().has_value());

  PersistenceService service(runtime, load_test_db_config());
  auto open_res = service.sync_wait(service.open());
  if (!open_res) {
    runtime.stop();
    GTEST_SKIP() << "MySQL unavailable for transaction metric test: "
                 << open_res.error().message();
  }

  auto close_res = service.sync_wait(service.close());
  ASSERT_TRUE(close_res.has_value()) << close_res.error().message();
  EXPECT_GE(service.db_transactions_total(), 2U);
  runtime.stop();
}

TEST(PersistenceOpenTest, SyncWaitListDagsSucceedsAfterOpen) {
  Runtime runtime(1);
  ASSERT_TRUE(runtime.start().has_value());

  DatabaseConfig cfg;
  cfg.host = env_or_default("DAGFORGE_E2E_MYSQL_HOST", "127.0.0.1");
  cfg.username = env_or_default("DAGFORGE_E2E_MYSQL_USER", "dagforge_e2e8");
  cfg.password = env_or_default("DAGFORGE_E2E_MYSQL_PASSWORD", "dagforge_e2e8");
  cfg.database = env_or_default("DAGFORGE_E2E_MYSQL_DB", "dagforge_e2e8");
  cfg.connect_timeout = 2;

  PersistenceService service(runtime, cfg);
  auto open_res = service.sync_wait(service.open());
  if (!open_res) {
    runtime.stop();
    GTEST_SKIP() << "MySQL unavailable for sync_wait list_dags test: "
                 << open_res.error().message();
  }

  auto dags_res = service.sync_wait(service.list_dags());
  EXPECT_TRUE(dags_res.has_value());

  auto close_res = service.sync_wait(service.close());
  ASSERT_TRUE(close_res.has_value()) << close_res.error().message();
  runtime.stop();
}
