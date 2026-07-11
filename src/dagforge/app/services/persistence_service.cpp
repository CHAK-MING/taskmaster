#include "dagforge/app/services/persistence_service.hpp"
#include "dagforge/core/asio_awaitable.hpp"
#include "dagforge/util/log.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <span>
#include <unordered_map>
#include <unordered_set>

namespace dagforge {

namespace {

constexpr std::array<std::uint64_t, 15> kIoLatencyBucketsNs{
    100'000ULL,       250'000ULL,       500'000ULL,       1'000'000ULL,
    2'500'000ULL,     5'000'000ULL,     10'000'000ULL,    25'000'000ULL,
    50'000'000ULL,    100'000'000ULL,   250'000'000ULL,   500'000'000ULL,
    1'000'000'000ULL, 2'500'000'000ULL, 10'000'000'000ULL};

enum class BatchCollectFailureStage {
  Receive,
  LingerWait,
};

template <typename RequestPtr> struct PendingBatch {
  std::vector<RequestPtr> requests;
  std::chrono::steady_clock::time_point first_enqueued_at{};
};

struct BatchWriterCompletionGuard {
  std::atomic<std::size_t> &inflight;

  ~BatchWriterCompletionGuard() {
    inflight.fetch_sub(1, std::memory_order_acq_rel);
  }
};

auto track_batch_writer(spawn_task writer, std::atomic<std::size_t> &inflight)
    -> spawn_task {
  BatchWriterCompletionGuard completion{inflight};
  co_await std::move(writer);
}

struct TaskUpdateDedupKey {
  int64_t task_rowid{0};
  int attempt{0};

  [[nodiscard]] friend auto operator==(const TaskUpdateDedupKey &lhs,
                                       const TaskUpdateDedupKey &rhs) noexcept
      -> bool = default;
};

struct TaskUpdateDedupKeyHash {
  [[nodiscard]] auto operator()(const TaskUpdateDedupKey &key) const noexcept
      -> std::size_t {
    const auto rowid_hash = std::hash<int64_t>{}(key.task_rowid);
    const auto attempt_hash = std::hash<int>{}(key.attempt);
    return rowid_hash ^
           (attempt_hash + 0x9e3779b97f4a7c15ULL + (rowid_hash << 6) +
            (rowid_hash >> 2));
  }
};

template <typename Queue, typename RequestPtr>
auto drain_ready_batch(Queue &queue, std::atomic<std::size_t> &queue_depth,
                       std::vector<RequestPtr> &batch,
                       std::size_t max_batch_size) -> void {
  while (batch.size() < max_batch_size) {
    bool drained = false;
    queue.try_receive([&](boost::system::error_code ec, RequestPtr req) {
      if (ec || !req) {
        return;
      }
      queue_depth.fetch_sub(1, std::memory_order_relaxed);
      batch.push_back(std::move(req));
      drained = true;
    });
    if (!drained) {
      break;
    }
  }
}

template <typename Queue, typename RequestPtr>
auto collect_pending_batch(Queue &queue, std::atomic<std::size_t> &queue_depth,
                           std::atomic<bool> &running,
                           boost::asio::steady_timer &timer,
                           std::size_t max_batch_size,
                           std::chrono::milliseconds linger,
                           BatchCollectFailureStage &failure_stage)
    -> task<Result<PendingBatch<RequestPtr>>> {
  failure_stage = BatchCollectFailureStage::Receive;
  auto first_res = co_await co_as_result(queue.async_receive(use_nothrow));
  if (!first_res) {
    co_return fail(first_res.error());
  }

  queue_depth.fetch_sub(1, std::memory_order_relaxed);

  PendingBatch<RequestPtr> batch;
  batch.requests.reserve(max_batch_size);
  batch.requests.push_back(std::move(*first_res));
  batch.first_enqueued_at = std::chrono::steady_clock::now();

  drain_ready_batch(queue, queue_depth, batch.requests, max_batch_size);
  if (batch.requests.size() >= max_batch_size) {
    co_return ok(std::move(batch));
  }

  if (batch.requests.size() >= std::max<std::size_t>(1, max_batch_size / 2)) {
    co_return ok(std::move(batch));
  }

  if (queue_depth.load(std::memory_order_acquire) == 0) {
    co_return ok(std::move(batch));
  }

  failure_stage = BatchCollectFailureStage::LingerWait;
  timer.expires_after(linger);
  auto linger_res = co_await co_as_result(timer.async_wait(use_nothrow));
  if (!linger_res && running.load(std::memory_order_relaxed)) {
    co_return fail(linger_res.error());
  }

  drain_ready_batch(queue, queue_depth, batch.requests, max_batch_size);
  co_return ok(std::move(batch));
}

auto wait_batch_writer_backoff(boost::asio::steady_timer &timer,
                               std::atomic<bool> &running,
                               const char *writer_name) -> task<void> {
  timer.expires_after(std::chrono::milliseconds(10));
  const auto operation_aborted =
      std::error_code{boost::asio::error::make_error_code(
          boost::asio::error::operation_aborted)};
  auto backoff_res = co_await co_as_result(timer.async_wait(use_nothrow));
  if (!backoff_res && backoff_res.error() != operation_aborted &&
      running.load(std::memory_order_relaxed)) {
    log::error("{} backoff wait failed: {}", writer_name,
               backoff_res.error().message());
  }
}

template <typename T>
auto wait_for_counter_zero(std::atomic<T> &counter,
                           std::chrono::steady_clock::time_point deadline)
    -> task<Result<void>> {
  auto executor = co_await boost::asio::this_coro::executor;
  boost::asio::steady_timer timer(executor);
  while (counter.load(std::memory_order_acquire) > 0 &&
         std::chrono::steady_clock::now() < deadline) {
    timer.expires_after(std::chrono::milliseconds(5));
    auto wait_res = co_await co_as_result(timer.async_wait(use_nothrow));
    if (!wait_res) {
      co_return fail(wait_res.error());
    }
  }
  if (counter.load(std::memory_order_acquire) > 0) {
    co_return fail(Error::Timeout);
  }
  co_return ok();
}

template <typename Queue, typename RequestPtr, typename FlushFn>
auto run_batch_writer_loop(storage::MySQLDatabase &db, Queue &queue,
                           std::atomic<bool> &running,
                           std::atomic<std::size_t> &queue_depth,
                           std::atomic<std::uint64_t> &acquire_failures_total,
                           const char *writer_name,
                           std::size_t max_batch_size,
                           FlushFn flush_batch) -> spawn_task {
  constexpr auto kLinger = std::chrono::milliseconds(1);
  auto executor = co_await boost::asio::this_coro::executor;
  boost::asio::steady_timer timer(executor);
  std::optional<boost::mysql::pooled_connection> dedicated_conn;

  while (running.load(std::memory_order_relaxed)) {
    bool should_backoff = false;
    try {
      if (!dedicated_conn.has_value()) {
        auto conn_res = co_await db.acquire_batch_writer_connection();
        if (!conn_res) {
          if (!running.load(std::memory_order_relaxed)) {
            break;
          }
          log::error("{} failed to acquire connection: {}", writer_name,
                     conn_res.error().message());
          acquire_failures_total.fetch_add(1, std::memory_order_relaxed);
          should_backoff = true;
        } else {
          dedicated_conn.emplace(std::move(*conn_res));
        }
      }

      if (!should_backoff) {
        auto failure_stage = BatchCollectFailureStage::Receive;
        auto batch_res = co_await collect_pending_batch<Queue, RequestPtr>(
            queue, queue_depth, running, timer, max_batch_size, kLinger,
            failure_stage);
        if (!batch_res) {
          if (!running.load(std::memory_order_relaxed)) {
            break;
          }
          const char *stage_name =
              failure_stage == BatchCollectFailureStage::Receive
                  ? "receive"
                  : "linger wait";
          log::error("{} {} failed: {}", writer_name, stage_name,
                     batch_res.error().message());
          dedicated_conn.reset();
          should_backoff = true;
        } else {
          co_await flush_batch(dedicated_conn->get(),
                               std::move(batch_res->requests),
                               batch_res->first_enqueued_at);
        }
      }
    } catch (const std::exception &e) {
      if (!running.load(std::memory_order_relaxed)) {
        break;
      }
      log::error("{} loop failed: {}", writer_name, e.what());
      dedicated_conn.reset();
      should_backoff = true;
    } catch (...) {
      if (!running.load(std::memory_order_relaxed)) {
        break;
      }
      log::error("{} loop failed", writer_name);
      dedicated_conn.reset();
      should_backoff = true;
    }

    if (should_backoff) {
      co_await wait_batch_writer_backoff(timer, running, writer_name);
    }
  }
}

#define DAGFORGE_DB_RETURN(expr, count_transaction)                            \
  do {                                                                         \
    const auto started_at = std::chrono::steady_clock::now();                  \
    auto result = co_await (expr);                                             \
    const auto elapsed_ns =                                                    \
        std::chrono::duration_cast<std::chrono::nanoseconds>(                  \
            std::chrono::steady_clock::now() - started_at)                     \
            .count();                                                          \
    db_query_duration_.observe_ns(                                             \
        static_cast<std::uint64_t>(elapsed_ns > 0 ? elapsed_ns : 0));          \
    if (result) {                                                              \
      if (count_transaction) {                                                 \
        db_transactions_total_.fetch_add(1, std::memory_order_relaxed);        \
      }                                                                        \
    } else {                                                                   \
      db_errors_total_.fetch_add(1, std::memory_order_relaxed);                \
    }                                                                          \
    co_return result;                                                          \
  } while (false)

} // namespace

PersistenceService::PersistenceService(Runtime &runtime,
                                       const DatabaseConfig &cfg,
                                       std::size_t create_run_batch_capacity,
                                       std::size_t task_update_batch_capacity)
    : runtime_(runtime), db_pool_(std::max(1u, runtime.shard_count())),
      create_run_batch_queue_(db_pool_.get_executor(),
                              create_run_batch_capacity),
      task_update_batch_queue_(db_pool_.get_executor(),
                               task_update_batch_capacity),
      task_update_batch_flush_histogram_(
          std::span<const std::uint64_t>(kIoLatencyBucketsNs)),
      db_(db_pool_.get_executor(), cfg) {}

PersistenceService::~PersistenceService() {
  trigger_batch_writer_running_.store(false, std::memory_order_release);
  task_update_batch_writer_running_.store(false, std::memory_order_release);
  // close() rejects receives started after this point. cancel() only wakes
  // operations that were already pending and leaves a shutdown race window.
  create_run_batch_queue_.close();
  task_update_batch_queue_.close();

  while (task_update_async_inflight_.load(std::memory_order_acquire) > 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }

  db_.shutdown();
  while (batch_writer_inflight_.load(std::memory_order_acquire) > 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  db_.wait_for_shutdown();
  db_pool_.join();
}

// ── Lifecycle
// ──────────────────────────────────────────────────────────────────

auto PersistenceService::open() -> task<Result<void>> {
  const bool reset_trigger_queue = !create_run_batch_queue_.is_open();
  const bool reset_task_update_queue = !task_update_batch_queue_.is_open();
  if (reset_trigger_queue || reset_task_update_queue) {
    if (batch_writer_inflight_.load(std::memory_order_acquire) != 0 ||
        task_update_async_inflight_.load(std::memory_order_acquire) != 0) {
      co_return fail(Error::InvalidState);
    }
    if (reset_trigger_queue) {
      create_run_batch_queue_.reset();
    }
    if (reset_task_update_queue) {
      task_update_batch_queue_.reset();
    }
  }

  const auto started_at = std::chrono::steady_clock::now();
  auto open_res = co_await db_.open();
  const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                              std::chrono::steady_clock::now() - started_at)
                              .count();
  db_query_duration_.observe_ns(
      static_cast<std::uint64_t>(elapsed_ns > 0 ? elapsed_ns : 0));
  if (!open_res) {
    db_errors_total_.fetch_add(1, std::memory_order_relaxed);
    co_return open_res;
  }
  db_transactions_total_.fetch_add(1, std::memory_order_relaxed);

  auto spawn_writer = [this](spawn_task writer) {
    batch_writer_inflight_.fetch_add(1, std::memory_order_acq_rel);
    try {
      boost::asio::co_spawn(
          db_pool_,
          track_batch_writer(std::move(writer), batch_writer_inflight_),
          boost::asio::detached);
    } catch (...) {
      batch_writer_inflight_.fetch_sub(1, std::memory_order_acq_rel);
      throw;
    }
  };

  if (!trigger_batch_writer_running_.exchange(true,
                                              std::memory_order_acq_rel)) {
    spawn_writer(trigger_batch_writer_loop());
  }
  if (!task_update_batch_writer_running_.exchange(true,
                                                  std::memory_order_acq_rel)) {
    spawn_writer(task_update_batch_writer_loop());
  }
  co_return open_res;
}

auto PersistenceService::close() -> task<Result<void>> {
  auto close_res = co_await close(std::chrono::steady_clock::time_point::max());
  if (!close_res) {
    co_return fail(close_res.error());
  }
  co_return ok();
}

auto PersistenceService::close(std::chrono::steady_clock::time_point deadline)
    -> task<Result<void>> {
  const auto started_at = std::chrono::steady_clock::now();
  trigger_batch_writer_running_.store(false, std::memory_order_release);
  task_update_batch_writer_running_.store(false, std::memory_order_release);
  // A writer may have passed its loop condition but not started receive yet.
  // Closing the channel makes that later receive fail immediately.
  create_run_batch_queue_.close();
  task_update_batch_queue_.close();

  auto submitter_wait =
      co_await wait_for_counter_zero(task_update_async_inflight_, deadline);
  if (!submitter_wait) {
    co_return fail(submitter_wait.error());
  }

  auto db_close_res = co_await db_.close();
  if (!db_close_res) {
    co_return fail(db_close_res.error());
  }

  auto writer_wait =
      co_await wait_for_counter_zero(batch_writer_inflight_, deadline);
  if (!writer_wait) {
    co_return fail(writer_wait.error());
  }

  db_.wait_for_shutdown();
  create_run_batch_queue_.reset();
  task_update_batch_queue_.reset();

  const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                              std::chrono::steady_clock::now() - started_at)
                              .count();
  db_query_duration_.observe_ns(
      static_cast<std::uint64_t>(elapsed_ns > 0 ? elapsed_ns : 0));
  db_transactions_total_.fetch_add(1, std::memory_order_relaxed);
  co_return ok();
}

auto PersistenceService::is_open() const noexcept -> bool {
  return db_.is_open();
}

auto PersistenceService::db_query_duration_histogram() const
    -> metrics::Histogram::Snapshot {
  return db_query_duration_.snapshot();
}

auto PersistenceService::db_connection_acquire_histogram() const
    -> metrics::Histogram::Snapshot {
  return db_.connection_acquire_histogram();
}

auto PersistenceService::db_errors_total() const noexcept -> std::uint64_t {
  return db_errors_total_.load(std::memory_order_relaxed);
}

auto PersistenceService::db_connection_acquire_failures_total() const noexcept
    -> std::uint64_t {
  return db_.connection_acquire_failures_total();
}

auto PersistenceService::db_transactions_total() const noexcept
    -> std::uint64_t {
  return db_transactions_total_.load(std::memory_order_relaxed);
}

auto PersistenceService::task_update_batch_flush_histogram() const
    -> metrics::Histogram::Snapshot {
  return task_update_batch_flush_histogram_.snapshot();
}

// ── DAG management
// ─────────────────────────────────────────────────────────────

auto PersistenceService::save_dag(const DAGInfo &dag) -> task<Result<int64_t>> {
  DAGFORGE_DB_RETURN(db_.save_dag(dag), true);
}

auto PersistenceService::get_dag(const DAGId &dag_id) -> task<Result<DAGInfo>> {
  DAGFORGE_DB_RETURN(db_.get_dag(dag_id), false);
}

auto PersistenceService::list_dags() -> task<Result<std::vector<DAGInfo>>> {
  DAGFORGE_DB_RETURN(db_.list_dags(), false);
}

auto PersistenceService::list_dag_states()
    -> task<Result<std::vector<DagStateRecord>>> {
  DAGFORGE_DB_RETURN(db_.list_dag_states(), false);
}

auto PersistenceService::delete_dag(const DAGId &dag_id) -> task<Result<void>> {
  DAGFORGE_DB_RETURN(db_.delete_dag(dag_id), true);
}

auto PersistenceService::set_dag_paused(const DAGId &dag_id, bool paused)
    -> task<Result<void>> {
  DAGFORGE_DB_RETURN(db_.set_dag_paused(dag_id, paused), true);
}

auto PersistenceService::set_dag_active(const DAGId &dag_id, bool active)
    -> task<Result<void>> {
  DAGFORGE_DB_RETURN(db_.set_dag_active(dag_id, active), true);
}

// ── DAG info upsert
// ──────────────────────────────────────────────────────

auto PersistenceService::upsert_dag_info(const DAGId &dag_id, DAGInfo dag_info,
                                         bool existed)
    -> task<Result<DAGInfo>> {
  (void)existed;

  const auto observe_elapsed =
      [this](std::chrono::steady_clock::time_point started_at) {
        const auto elapsed_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - started_at)
                .count();
        db_query_duration_.observe_ns(
            static_cast<std::uint64_t>(elapsed_ns > 0 ? elapsed_ns : 0));
      };

  auto transaction_successful = false;
  auto finalize_transaction = [&]() {
    if (transaction_successful) {
      db_transactions_total_.fetch_add(1, std::memory_order_relaxed);
    } else {
      db_errors_total_.fetch_add(1, std::memory_order_relaxed);
    }
  };

  auto started_at = std::chrono::steady_clock::now();
  auto conn_res = co_await db_.acquire_batch_writer_connection();
  observe_elapsed(started_at);
  if (!conn_res) {
    finalize_transaction();
    co_return fail(conn_res.error());
  }
  auto &conn = conn_res->get();

  started_at = std::chrono::steady_clock::now();
  auto rowid_res = co_await db_.save_dag_on_connection(conn, dag_info);
  observe_elapsed(started_at);
  if (!rowid_res) {
    finalize_transaction();
    co_return fail(rowid_res.error());
  }
  dag_info.dag_rowid = *rowid_res;

  started_at = std::chrono::steady_clock::now();
  auto existing_tasks_res =
      co_await db_.get_tasks_on_connection(conn, dag_info.dag_rowid);
  observe_elapsed(started_at);
  if (!existing_tasks_res) {
    finalize_transaction();
    co_return fail(existing_tasks_res.error());
  }

  std::unordered_set<std::string> incoming_task_ids;
  incoming_task_ids.reserve(dag_info.tasks.size());
  for (const auto &task : dag_info.tasks) {
    incoming_task_ids.insert(task.task_id.str());
  }

  for (const auto &existing_task : *existing_tasks_res) {
    if (incoming_task_ids.contains(existing_task.task_id.str())) {
      continue;
    }
    started_at = std::chrono::steady_clock::now();
    auto del_task = co_await db_.delete_task_on_connection(
        conn, dag_info.dag_rowid, existing_task.task_id);
    observe_elapsed(started_at);
    if (!del_task && del_task.error() != make_error_code(Error::NotFound)) {
      finalize_transaction();
      co_return fail(del_task.error());
    }
  }

  started_at = std::chrono::steady_clock::now();
  auto save_tasks_res =
      co_await db_.save_tasks_on_connection(conn, dag_info.dag_rowid,
                                            dag_info.tasks);
  observe_elapsed(started_at);
  if (!save_tasks_res) {
    finalize_transaction();
    co_return fail(save_tasks_res.error());
  }

  started_at = std::chrono::steady_clock::now();
  auto deps_res = co_await db_.replace_task_dependencies_on_connection(
      conn, dag_info.dag_rowid, dag_info.tasks);
  observe_elapsed(started_at);
  if (!deps_res) {
    finalize_transaction();
    co_return fail(deps_res.error());
  }

  if (auto prepared = dag_info.prepare_runtime_artifacts(); !prepared) {
    finalize_transaction();
    co_return fail(prepared.error());
  }

  conn_res->return_without_reset();
  transaction_successful = true;
  finalize_transaction();
  co_return ok(std::move(dag_info));
}

// ── Run management
// ─────────────────────────────────────────────────────────────

auto PersistenceService::save_dag_run(const DAGRun &run)
    -> task<Result<int64_t>> {
  DAGFORGE_DB_RETURN(db_.save_dag_run(run), true);
}

auto PersistenceService::update_dag_run(const DAGRun &run)
    -> task<Result<void>> {
  const auto started_at = std::chrono::steady_clock::now();
  auto result = (co_await db_.save_dag_run(run)).transform([](int64_t) {});
  const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                              std::chrono::steady_clock::now() - started_at)
                              .count();
  db_query_duration_.observe_ns(
      static_cast<std::uint64_t>(elapsed_ns > 0 ? elapsed_ns : 0));
  if (result) {
    db_transactions_total_.fetch_add(1, std::memory_order_relaxed);
  } else {
    db_errors_total_.fetch_add(1, std::memory_order_relaxed);
  }
  co_return result;
}

auto PersistenceService::get_dag_run_state(const DAGRunId &id)
    -> task<Result<DAGRunState>> {
  DAGFORGE_DB_RETURN(db_.get_dag_run_state(id), false);
}

// ── Atomic: create run + initial task instances
// ────────────────────────────────

auto PersistenceService::create_run_with_task_instances(
    DAGRun run, std::vector<TaskInstanceInfo> instances)
    -> task<Result<int64_t>> {
  const auto missing_task_rowids =
      std::count_if(instances.begin(), instances.end(),
                    [](const auto &ti) { return ti.task_rowid <= 0; });
  if (missing_task_rowids != 0) {
    const auto &first = instances.front();
    log::error(
        "create_run_with_task_instances has unresolved task_rowid values: "
        "dag_run_id={} run_rowid={} batch_size={} missing_task_rowid={} "
        "first_task_rowid={} first_attempt={} first_state={}",
        run.id(), run.run_rowid(), instances.size(), missing_task_rowids,
        first.task_rowid, first.attempt, enum_to_string(first.state));
  }

  auto caller_executor = co_await boost::asio::this_coro::executor;
  auto reply = std::make_shared<CreateRunBatchReply>(caller_executor, 1);
  auto request = std::make_shared<CreateRunBatchRequest>(CreateRunBatchRequest{
      .bundle = {.run = std::move(run), .instances = std::move(instances)},
      .caller_executor = caller_executor,
      .reply = reply,
  });

  if (!create_run_batch_queue_.try_send(boost::system::error_code{}, request)) {
    trigger_batch_rejected_total_.fetch_add(1, std::memory_order_relaxed);
    trigger_batch_fallback_total_.fetch_add(1, std::memory_order_relaxed);
    const auto started_at = std::chrono::steady_clock::now();
    auto fallback_result =
        co_await db_.create_run_with_task_instances_transaction(
            request->bundle.run, request->bundle.instances);
    const auto elapsed_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - started_at)
            .count();
    db_query_duration_.observe_ns(
        static_cast<std::uint64_t>(elapsed_ns > 0 ? elapsed_ns : 0));
    if (fallback_result) {
      db_transactions_total_.fetch_add(1, std::memory_order_relaxed);
      co_return ok(*fallback_result);
    }
    db_errors_total_.fetch_add(1, std::memory_order_relaxed);
    co_return fail(fallback_result.error());
  }

  trigger_batch_requests_total_.fetch_add(1, std::memory_order_relaxed);
  trigger_batch_queue_depth_.fetch_add(1, std::memory_order_relaxed);

  auto reply_res = co_await co_as_result(reply->async_receive(use_nothrow));
  if (reply_res) {
    co_return std::move(*reply_res);
  }

  log::error("Trigger batch reply receive failed: {}",
             reply_res.error().message());
  db_errors_total_.fetch_add(1, std::memory_order_relaxed);
  co_return fail(Error::DatabaseQueryFailed);
}

auto PersistenceService::trigger_batch_queue_depth() const noexcept
    -> std::size_t {
  return trigger_batch_queue_depth_.load(std::memory_order_relaxed);
}

auto PersistenceService::trigger_batch_last_size() const noexcept
    -> std::size_t {
  return trigger_batch_last_size_.load(std::memory_order_relaxed);
}

auto PersistenceService::trigger_batch_last_linger_us() const noexcept
    -> std::uint64_t {
  return trigger_batch_last_linger_us_.load(std::memory_order_relaxed);
}

auto PersistenceService::trigger_batch_last_flush_ms() const noexcept
    -> std::uint64_t {
  return trigger_batch_last_flush_ms_.load(std::memory_order_relaxed);
}

auto PersistenceService::trigger_batch_requests_total() const noexcept
    -> std::uint64_t {
  return trigger_batch_requests_total_.load(std::memory_order_relaxed);
}

auto PersistenceService::trigger_batch_commits_total() const noexcept
    -> std::uint64_t {
  return trigger_batch_commits_total_.load(std::memory_order_relaxed);
}

auto PersistenceService::trigger_batch_fallback_total() const noexcept
    -> std::uint64_t {
  return trigger_batch_fallback_total_.load(std::memory_order_relaxed);
}

auto PersistenceService::trigger_batch_rejected_total() const noexcept
    -> std::uint64_t {
  return trigger_batch_rejected_total_.load(std::memory_order_relaxed);
}

auto PersistenceService::trigger_batch_wakeup_lag_us() const noexcept
    -> std::uint64_t {
  return trigger_batch_wakeup_lag_us_.load(std::memory_order_relaxed);
}

auto PersistenceService::trigger_batch_writer_acquire_failures_total()
    const noexcept -> std::uint64_t {
  return trigger_batch_writer_acquire_failures_total_.load(
      std::memory_order_relaxed);
}

auto PersistenceService::task_update_batch_queue_depth() const noexcept
    -> std::size_t {
  return task_update_batch_queue_depth_.load(std::memory_order_relaxed);
}

auto PersistenceService::task_update_batch_last_size() const noexcept
    -> std::size_t {
  return task_update_batch_last_size_.load(std::memory_order_relaxed);
}

auto PersistenceService::task_update_batch_last_linger_us() const noexcept
    -> std::uint64_t {
  return task_update_batch_last_linger_us_.load(std::memory_order_relaxed);
}

auto PersistenceService::task_update_batch_last_flush_ms() const noexcept
    -> std::uint64_t {
  return task_update_batch_last_flush_ms_.load(std::memory_order_relaxed);
}

auto PersistenceService::task_update_batch_requests_total() const noexcept
    -> std::uint64_t {
  return task_update_batch_requests_total_.load(std::memory_order_relaxed);
}

auto PersistenceService::task_update_batch_commits_total() const noexcept
    -> std::uint64_t {
  return task_update_batch_commits_total_.load(std::memory_order_relaxed);
}

auto PersistenceService::task_update_batch_fallback_total() const noexcept
    -> std::uint64_t {
  return task_update_batch_fallback_total_.load(std::memory_order_relaxed);
}

auto PersistenceService::task_update_batch_rejected_total() const noexcept
    -> std::uint64_t {
  return task_update_batch_rejected_total_.load(std::memory_order_relaxed);
}

auto PersistenceService::task_update_batch_wakeup_lag_us() const noexcept
    -> std::uint64_t {
  return task_update_batch_wakeup_lag_us_.load(std::memory_order_relaxed);
}

auto PersistenceService::task_update_batch_writer_acquire_failures_total()
    const noexcept -> std::uint64_t {
  return task_update_batch_writer_acquire_failures_total_.load(
      std::memory_order_relaxed);
}

auto PersistenceService::trigger_batch_writer_loop() -> spawn_task {
  // Trigger bursts in the benchmark commonly enqueue around 100 DAG runs at
  // once. Using a slightly larger cap cuts the number of sequential flushes in
  // half while the backlog-aware collector still returns early for low-load and
  // latency-sensitive cases.
  constexpr std::size_t kMaxBatchSize = 64;
  co_await run_batch_writer_loop<CreateRunBatchQueue, CreateRunBatchRequestPtr>(
      db_, create_run_batch_queue_, trigger_batch_writer_running_,
      trigger_batch_queue_depth_, trigger_batch_writer_acquire_failures_total_,
      "Trigger batch writer", kMaxBatchSize,
      [this](boost::mysql::any_connection &conn,
             std::vector<CreateRunBatchRequestPtr> batch,
             std::chrono::steady_clock::time_point first_enqueued_at)
          -> task<void> {
        co_await flush_trigger_batch(conn, std::move(batch), first_enqueued_at);
      });
}

auto PersistenceService::flush_trigger_batch(
    boost::mysql::any_connection &conn,
    std::vector<CreateRunBatchRequestPtr> batch,
    std::chrono::steady_clock::time_point first_enqueued_at) -> task<void> {
  if (batch.empty()) {
    co_return;
  }

  const auto flush_started_at = std::chrono::steady_clock::now();
  trigger_batch_last_size_.store(batch.size(), std::memory_order_relaxed);
  trigger_batch_last_linger_us_.store(
      std::chrono::duration_cast<std::chrono::microseconds>(flush_started_at -
                                                            first_enqueued_at)
          .count(),
      std::memory_order_relaxed);

  std::vector<RunInsertBundle> bundles;
  bundles.reserve(batch.size());
  for (const auto &request : batch) {
    bundles.push_back(request->bundle);
  }

  const auto db_started_at = std::chrono::steady_clock::now();
  auto batch_res =
      co_await db_.create_runs_with_task_instances_transaction(conn, bundles);
  const auto commit_done = std::chrono::steady_clock::now();
  const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                              commit_done - db_started_at)
                              .count();
  db_query_duration_.observe_ns(
      static_cast<std::uint64_t>(elapsed_ns > 0 ? elapsed_ns : 0));
  trigger_batch_last_flush_ms_.store(
      std::chrono::duration_cast<std::chrono::milliseconds>(commit_done -
                                                            flush_started_at)
          .count(),
      std::memory_order_relaxed);

  if (batch_res) {
    trigger_batch_commits_total_.fetch_add(1, std::memory_order_relaxed);
    db_transactions_total_.fetch_add(1, std::memory_order_relaxed);
    for (std::size_t i = 0; i < batch.size(); ++i) {
      publish_batch_result(batch[i], ok((*batch_res)[i]), commit_done);
    }
    co_return;
  }

  trigger_batch_fallback_total_.fetch_add(batch.size(),
                                          std::memory_order_relaxed);
  for (auto &request : batch) {
    const auto started_at = std::chrono::steady_clock::now();
    auto result = co_await db_.create_run_with_task_instances_transaction(
        request->bundle.run, request->bundle.instances);
    const auto fallback_elapsed_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - started_at)
            .count();
    db_query_duration_.observe_ns(static_cast<std::uint64_t>(
        fallback_elapsed_ns > 0 ? fallback_elapsed_ns : 0));
    if (result) {
      db_transactions_total_.fetch_add(1, std::memory_order_relaxed);
    } else {
      db_errors_total_.fetch_add(1, std::memory_order_relaxed);
    }
    publish_batch_result(request, std::move(result), commit_done);
  }
}

auto PersistenceService::publish_batch_result(
    const CreateRunBatchRequestPtr &request, Result<int64_t> result,
    std::chrono::steady_clock::time_point commit_done) -> void {
  boost::asio::post(request->caller_executor,
                    [this, reply = request->reply,
                     result = std::move(result), commit_done]() mutable {
                      trigger_batch_wakeup_lag_us_.store(
                          std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::steady_clock::now() - commit_done)
                              .count(),
                          std::memory_order_relaxed);
                      if (!reply->try_send(boost::system::error_code{},
                                           std::move(result))) {
                        log::warn("Trigger batch reply channel was full");
                      }
                    });
}

auto PersistenceService::task_update_batch_writer_loop() -> spawn_task {
  constexpr std::size_t kMaxBatchSize = 128;
  co_await run_batch_writer_loop<TaskUpdateQueue, TaskUpdateRequestPtr>(
      db_, task_update_batch_queue_, task_update_batch_writer_running_,
      task_update_batch_queue_depth_,
      task_update_batch_writer_acquire_failures_total_,
      "Task update batch writer", kMaxBatchSize,
      [this](boost::mysql::any_connection &conn,
             std::vector<TaskUpdateRequestPtr> batch,
             std::chrono::steady_clock::time_point first_enqueued_at)
          -> task<void> {
        co_await flush_task_update_batch(conn, std::move(batch),
                                         first_enqueued_at);
      });
}

auto PersistenceService::flush_task_update_batch(
    boost::mysql::any_connection &conn, std::vector<TaskUpdateRequestPtr> batch,
    std::chrono::steady_clock::time_point first_enqueued_at) -> task<void> {
  if (batch.empty()) {
    co_return;
  }

  const auto flush_started_at = std::chrono::steady_clock::now();
  task_update_batch_last_size_.store(batch.size(), std::memory_order_relaxed);
  task_update_batch_last_linger_us_.store(
      std::chrono::duration_cast<std::chrono::microseconds>(flush_started_at -
                                                            first_enqueued_at)
          .count(),
      std::memory_order_relaxed);

  std::unordered_map<std::string_view, std::vector<TaskUpdateRequestPtr>> by_run;
  by_run.reserve(batch.size());

  for (auto &request : batch) {
    if (!request) {
      continue;
    }
    by_run[request->run_id.value()].push_back(request);
  }

  std::unordered_map<TaskUpdateDedupKey, TaskInstanceInfo, TaskUpdateDedupKeyHash>
      latest_by_task;
  latest_by_task.reserve(batch.size());

  for (auto &request : batch) {
    if (!request) {
      continue;
    }
    latest_by_task.insert_or_assign(TaskUpdateDedupKey{request->info.task_rowid,
                                                       request->info.attempt},
                                    request->info);
  }

  const auto db_started_at = std::chrono::steady_clock::now();
  bool flush_ok = true;
  for (auto &[run_id_str, requests] : by_run) {
    std::vector<TaskInstanceInfo> infos;
    infos.reserve(requests.size());
    for (const auto &req : requests) {
      auto it = latest_by_task.find(
          TaskUpdateDedupKey{req->info.task_rowid, req->info.attempt});
      if (it != latest_by_task.end()) {
        infos.push_back(it->second);
      }
    }
    auto result = co_await db_.save_task_instances_batch_on_connection(
        conn, DAGRunId{run_id_str}, infos,
        infos.empty() ? -1 : infos.front().run_rowid, -1);
    if (!result) {
      flush_ok = false;
    }
  }

  const auto commit_done = std::chrono::steady_clock::now();
  const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                              commit_done - db_started_at)
                              .count();
  db_query_duration_.observe_ns(
      static_cast<std::uint64_t>(elapsed_ns > 0 ? elapsed_ns : 0));
  task_update_batch_flush_histogram_.observe_ns(
      static_cast<std::uint64_t>(elapsed_ns > 0 ? elapsed_ns : 0));
  task_update_batch_last_flush_ms_.store(
      std::chrono::duration_cast<std::chrono::milliseconds>(commit_done -
                                                            flush_started_at)
          .count(),
      std::memory_order_relaxed);

  if (flush_ok) {
    task_update_batch_commits_total_.fetch_add(1, std::memory_order_relaxed);
    for (auto &request : batch) {
      publish_task_update_result(request, ok(), commit_done);
    }
    co_return;
  }

  task_update_batch_fallback_total_.fetch_add(batch.size(),
                                              std::memory_order_relaxed);
  for (auto &request : batch) {
    const auto started_at = std::chrono::steady_clock::now();
    auto result =
        co_await db_.update_task_instance(request->run_id, request->info);
    const auto fallback_elapsed_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - started_at)
            .count();
    db_query_duration_.observe_ns(static_cast<std::uint64_t>(
        fallback_elapsed_ns > 0 ? fallback_elapsed_ns : 0));
    if (result) {
      db_transactions_total_.fetch_add(1, std::memory_order_relaxed);
    } else {
      db_errors_total_.fetch_add(1, std::memory_order_relaxed);
    }
    publish_task_update_result(request, std::move(result), commit_done);
  }
}

auto PersistenceService::publish_task_update_result(
    const TaskUpdateRequestPtr &request, Result<void> result,
    std::chrono::steady_clock::time_point commit_done) -> void {
  if (!request->reply) {
    if (!result) {
      log::error("Task update fire-and-forget persistence failed: {}",
                 result.error().message());
    }
    return;
  }

  boost::asio::post(request->caller_executor,
                    [this, reply = request->reply,
                     result = std::move(result), commit_done]() mutable {
                      task_update_batch_wakeup_lag_us_.store(
                          std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::steady_clock::now() - commit_done)
                              .count(),
                          std::memory_order_relaxed);
                      if (!reply->try_send(boost::system::error_code{},
                                           std::move(result))) {
                        log::warn("Task update batch reply channel was full");
                      }
                    });
}

// ── Task instance persistence
// ──────────────────────────────────────────────────

auto PersistenceService::enqueue_task_update_request(
    const TaskUpdateRequestPtr &request) -> bool {
  if (!task_update_batch_queue_.try_send(boost::system::error_code{},
                                         request)) {
    task_update_batch_rejected_total_.fetch_add(1, std::memory_order_relaxed);
    return false;
  }

  task_update_batch_requests_total_.fetch_add(1, std::memory_order_relaxed);
  task_update_batch_queue_depth_.fetch_add(1, std::memory_order_relaxed);
  return true;
}

auto PersistenceService::send_task_update_request(
    const TaskUpdateRequestPtr &request) -> task<Result<void>> {
  if (enqueue_task_update_request(request)) {
    co_return ok();
  }

  auto send_res = co_await co_as_result(
      task_update_batch_queue_.async_send(boost::system::error_code{}, request,
                                          use_nothrow));
  if (!send_res) {
    co_return fail(Error::DatabaseQueryFailed);
  }

  task_update_batch_requests_total_.fetch_add(1, std::memory_order_relaxed);
  task_update_batch_queue_depth_.fetch_add(1, std::memory_order_relaxed);
  co_return ok();
}

auto PersistenceService::submit_task_update_async(TaskUpdateRequestPtr request)
    -> spawn_task {
  const auto finish = [this]() {
    task_update_async_inflight_.fetch_sub(1, std::memory_order_acq_rel);
  };
  auto send_result = co_await send_task_update_request(request);
  if (!send_result) {
    db_errors_total_.fetch_add(1, std::memory_order_relaxed);
    log::error("Task update enqueue failed: {}", send_result.error().message());
  }
  finish();
}

auto PersistenceService::update_task_instance(const DAGRunId &run_id,
                                             const TaskInstanceInfo &ti)
    -> task<Result<void>> {
  auto caller_executor = co_await boost::asio::this_coro::executor;
  auto reply = std::make_shared<TaskUpdateReply>(caller_executor, 1);
  auto request = std::make_shared<TaskUpdateRequest>(TaskUpdateRequest{
      .run_id = run_id.clone(),
      .info = ti,
      .caller_executor = caller_executor,
      .reply = reply,
  });

  auto send_result = co_await send_task_update_request(request);
  if (!send_result) {
    co_return fail(send_result.error());
  }

  auto reply_res = co_await co_as_result(reply->async_receive(use_nothrow));
  if (reply_res) {
    co_return std::move(*reply_res);
  }

  log::error("Task update batch reply receive failed: {}",
             reply_res.error().message());
  db_errors_total_.fetch_add(1, std::memory_order_relaxed);
  co_return fail(Error::DatabaseQueryFailed);
}

auto PersistenceService::submit_task_instance_update(const DAGRunId &run_id,
                                                     TaskInstanceInfo ti)
    -> void {
  auto request = std::make_shared<TaskUpdateRequest>(TaskUpdateRequest{
      .run_id = run_id.clone(),
      .info = std::move(ti),
      .caller_executor = db_pool_.get_executor(),
      .reply = nullptr,
  });

  task_update_async_inflight_.fetch_add(1, std::memory_order_acq_rel);
  boost::asio::co_spawn(
      db_pool_, submit_task_update_async(std::move(request)),
      boost::asio::detached);
}

auto PersistenceService::get_task_instances(const DAGRunId &run_id)
    -> task<Result<std::vector<TaskInstanceInfo>>> {
  DAGFORGE_DB_RETURN(db_.get_task_instances(run_id), false);
}

auto PersistenceService::save_task_instances_batch(
    const DAGRunId &run_id, const std::vector<TaskInstanceInfo> &instances)
    -> task<Result<void>> {
  DAGFORGE_DB_RETURN(db_.save_task_instances_batch(run_id, instances), true);
}

auto PersistenceService::save_task(const DAGId &dag_id,
                                   const TaskConfig &task_cfg)
    -> task<Result<int64_t>> {
  DAGFORGE_DB_RETURN(db_.save_task(dag_id, task_cfg), true);
}

auto PersistenceService::delete_task(const DAGId &dag_id, const TaskId &task_id)
    -> task<Result<void>> {
  DAGFORGE_DB_RETURN(db_.delete_task(dag_id, task_id), true);
}

auto PersistenceService::claim_task_instances(std::size_t limit,
                                              std::string_view worker_id)
    -> task<Result<std::vector<ClaimedTaskInstance>>> {
  DAGFORGE_DB_RETURN(db_.claim_task_instances(limit, worker_id), false);
}

auto PersistenceService::touch_task_heartbeat(const TaskInstanceKey &key)
    -> task<Result<void>> {
  DAGFORGE_DB_RETURN(db_.touch_task_heartbeat(key), true);
}

auto PersistenceService::submit_task_heartbeat(TaskInstanceKey key) -> void {
  if (!key.valid()) {
    return;
  }
  boost::asio::co_spawn(
      db_pool_,
      [this, key]() -> spawn_task {
        const auto started_at = std::chrono::steady_clock::now();
        auto result = co_await db_.touch_task_heartbeat(key);
        const auto elapsed_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - started_at)
                .count();
        db_query_duration_.observe_ns(
            static_cast<std::uint64_t>(elapsed_ns > 0 ? elapsed_ns : 0));
        if (result) {
          db_transactions_total_.fetch_add(1, std::memory_order_relaxed);
        } else {
          db_errors_total_.fetch_add(1, std::memory_order_relaxed);
          log::error("Task heartbeat persistence failed: {}",
                     result.error().message());
        }
      }(),
      boost::asio::detached);
}

auto PersistenceService::reap_zombie_task_instances(
    std::int64_t heartbeat_timeout_ms) -> task<Result<std::size_t>> {
  DAGFORGE_DB_RETURN(db_.reap_zombie_task_instances(heartbeat_timeout_ms),
                     true);
}

// ── XCom
// ───────────────────────────────────────────────────────────────────────

auto PersistenceService::save_xcom(const DAGRunId &run_id,
                                   const TaskId &task_id, std::string key,
                                   std::string value_json)
    -> task<Result<void>> {
  DAGFORGE_DB_RETURN(
      db_.save_xcom(run_id, task_id, std::move(key), std::move(value_json)),
      true);
}

auto PersistenceService::get_xcom(const DAGRunId &run_id, const TaskId &task_id,
                                  std::string_view key)
    -> task<Result<XComEntry>> {
  DAGFORGE_DB_RETURN(db_.get_xcom(run_id, task_id, key), false);
}

auto PersistenceService::get_task_xcoms(const DAGRunId &run_id,
                                        const TaskId &task_id)
    -> task<Result<std::vector<XComEntry>>> {
  DAGFORGE_DB_RETURN(db_.get_task_xcoms(run_id, task_id), false);
}

auto PersistenceService::get_run_xcoms(const DAGRunId &run_id)
    -> task<Result<std::vector<XComTaskEntry>>> {
  DAGFORGE_DB_RETURN(db_.get_run_xcoms(run_id), false);
}

// ── History / scheduling queries
// ───────────────────────────────────────────────

auto PersistenceService::get_run_history(const DAGRunId &run_id)
    -> task<Result<RunHistoryEntry>> {
  DAGFORGE_DB_RETURN(db_.get_run_history(run_id), false);
}

auto PersistenceService::list_run_history(std::size_t limit)
    -> task<Result<std::vector<RunHistoryEntry>>> {
  DAGFORGE_DB_RETURN(db_.list_run_history(limit), false);
}

auto PersistenceService::list_dag_run_history(const DAGId &dag_id,
                                              std::size_t limit)
    -> task<Result<std::vector<RunHistoryEntry>>> {
  DAGFORGE_DB_RETURN(db_.list_dag_run_history(dag_id, limit), false);
}

auto PersistenceService::has_dag_run(const DAGId &dag_id,
                                     TimePoint execution_date)
    -> task<Result<bool>> {
  DAGFORGE_DB_RETURN(db_.has_dag_run(dag_id, execution_date), false);
}

auto PersistenceService::list_dag_run_execution_dates(const DAGId &dag_id,
                                                      TimePoint start,
                                                      TimePoint end)
    -> task<Result<std::vector<TimePoint>>> {
  DAGFORGE_DB_RETURN(db_.list_dag_run_execution_dates(dag_id, start, end),
                     false);
}

auto PersistenceService::get_last_execution_date(const DAGId &dag_id)
    -> task<Result<TimePoint>> {
  DAGFORGE_DB_RETURN(db_.get_last_execution_date(dag_id), false);
}

// ── Watermarks
// ─────────────────────────────────────────────────────────────────

auto PersistenceService::save_watermark(const DAGId &dag_id, TimePoint ts)
    -> task<Result<void>> {
  DAGFORGE_DB_RETURN(db_.save_watermark(dag_id, ts), true);
}

auto PersistenceService::get_watermark(const DAGId &dag_id)
    -> task<Result<TimePoint>> {
  DAGFORGE_DB_RETURN(db_.get_watermark(dag_id), false);
}

auto PersistenceService::update_watermark_success(const DAGId &dag_id,
                                                  TimePoint ts)
    -> task<Result<void>> {
  DAGFORGE_DB_RETURN(db_.update_watermark_success(dag_id, ts), true);
}

auto PersistenceService::update_watermark_failure(const DAGId &dag_id,
                                                  TimePoint ts)
    -> task<Result<void>> {
  DAGFORGE_DB_RETURN(db_.update_watermark_failure(dag_id, ts), true);
}

// ── Previous task state
// ────────────────────────────────────────────────────────

auto PersistenceService::get_previous_task_state(
    std::int64_t task_rowid, TimePoint current_execution_date,
    const DAGRunId &current_run_id) -> task<Result<TaskState>> {
  DAGFORGE_DB_RETURN(db_.get_previous_task_state(
                         task_rowid, current_execution_date, current_run_id),
                     false);
}

// ── Recovery / debug
// ───────────────────────────────────────────────────────────

auto PersistenceService::mark_incomplete_runs_failed()
    -> task<Result<std::size_t>> {
  DAGFORGE_DB_RETURN(db_.mark_incomplete_runs_failed(), true);
}

auto PersistenceService::clear_all_dag_data() -> task<Result<void>> {
  DAGFORGE_DB_RETURN(db_.clear_all_dag_data(), true);
}

// ── Task Logs
// ───────────────────────────────────────────────────────────────

auto PersistenceService::append_task_log(const DAGRunId &run_id,
                                         const TaskId &task_id, int attempt,
                                         std::string stream,
                                         std::string content)
    -> task<Result<void>> {
  DAGFORGE_DB_RETURN(db_.append_task_log(run_id, task_id, attempt,
                                         std::move(stream), std::move(content)),
                     true);
}

auto PersistenceService::get_task_logs(const DAGRunId &run_id,
                                       const TaskId &task_id, int attempt,
                                       std::size_t limit)
    -> task<Result<std::vector<orm::TaskLogEntry>>> {
  DAGFORGE_DB_RETURN(db_.get_task_logs(run_id, task_id, attempt, limit), false);
}

auto PersistenceService::get_run_logs(const DAGRunId &run_id, std::size_t limit)
    -> task<Result<std::vector<orm::TaskLogEntry>>> {
  DAGFORGE_DB_RETURN(db_.get_run_logs(run_id, limit), false);
}

} // namespace dagforge
