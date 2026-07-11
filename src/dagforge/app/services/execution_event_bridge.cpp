#include "dagforge/app/services/execution_event_bridge.hpp"
#include "dagforge/app/http/websocket.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/util/log.hpp"
#include "dagforge/util/time.hpp"

#include <chrono>
#include <string>
#include <string_view>
#include <utility>

namespace dagforge {

namespace {

[[nodiscard]] auto should_persist_task_log(std::string_view stream,
                                           std::string_view msg) -> bool {
  // `run_task()` emits synthetic lifecycle markers for every task start/end.
  // They are useful for live UI updates, but persisting them for 10k bursts
  // creates a large amount of redundant write pressure. Keep real stdout/stderr
  // output in MySQL, but skip the synthetic markers because task status already
  // captures the same lifecycle transitions.
  if (stream != "stdout" || !msg.starts_with("Task '")) {
    return true;
  }
  return !(msg.find(" starting") != std::string_view::npos ||
           msg.find(" completed successfully") != std::string_view::npos ||
           msg.find(" skipped (exit code 100)") != std::string_view::npos);
}

} // namespace

ExecutionEventBridge::ExecutionEventBridge(Dependencies deps)
    : deps_(std::move(deps)) {}

auto ExecutionEventBridge::wire() -> void {
  if (!deps_.runtime || !deps_.execution || !deps_.scheduler ||
      !deps_.dropped_persistence_events || !deps_.mysql_batch_write_ops ||
      !deps_.api_server || !deps_.resolve_dag_id || !deps_.on_run_status ||
      !deps_.on_run_completed || !deps_.get_max_retries ||
      !deps_.get_retry_interval || !deps_.on_scheduler_trigger) {
    log::error("ExecutionEventBridge wiring skipped: incomplete dependencies");
    return;
  }

  ExecutionCallbacks callbacks;
  callbacks.on_task_status = [this](const DAGRunId &dag_run_id,
                                    const TaskId &task_id, TaskState status) {
    auto *api = deps_.api_server();
    if (!api || api->websocket_hub().connection_count() == 0) {
      return;
    }
    auto dag_id = deps_.resolve_dag_id(dag_run_id);
    if (!dag_id) {
      log::warn("Skipping task_status_changed broadcast for {} because dag_id "
                "could not be resolved",
                dag_run_id);
      return;
    }
    auto now = util::to_unix_millis(std::chrono::system_clock::now());
    JsonValue j = {{"type", "task_status_changed"},
                   {"dag_id", dag_id->value()},
                   {"dag_run_id", dag_run_id.value()},
                   {"run_id", dag_run_id.value()},
                   {"task_id", task_id.value()},
                   {"status", enum_to_string(status)},
                   {"timestamp", now}};
    http::WebSocketHub::EventMessage ev;
    ev.timestamp = util::format_timestamp();
    ev.event = "task_status_changed";
    ev.dag_run_id = dag_run_id.str();
    ev.task_id = task_id.str();
    ev.data = dump_json(j);
    api->websocket_hub().broadcast_event(ev);
  };

  callbacks.on_run_status = [this](const DAGRunId &dag_run_id,
                                   DAGRunState status) {
    deps_.on_run_status(dag_run_id, status);
    auto *api = deps_.api_server();
    if (!api || api->websocket_hub().connection_count() == 0) {
      return;
    }
    auto dag_id = deps_.resolve_dag_id(dag_run_id);
    if (!dag_id) {
      log::warn("Skipping dag_run_completed broadcast for {} because dag_id "
                "could not be resolved",
                dag_run_id);
      return;
    }
    auto now = util::to_unix_millis(std::chrono::system_clock::now());
    JsonValue j = {
        {"type", "dag_run_completed"},      {"dag_id", dag_id->value()},
        {"dag_run_id", dag_run_id.value()}, {"run_id", dag_run_id.value()},
        {"status", enum_to_string(status)}, {"timestamp", now}};
    http::WebSocketHub::EventMessage ev;
    ev.timestamp = util::format_timestamp();
    ev.event = "dag_run_completed";
    ev.dag_run_id = dag_run_id.str();
    ev.data = dump_json(j);
    api->websocket_hub().broadcast_event(ev);
  };

  callbacks.on_run_completed = [this](const DAGRunId &dag_run_id,
                                      const DAGRun &run) {
    deps_.on_run_completed(dag_run_id, run);
  };

  callbacks.on_log = [this](const DAGRunId &dag_run_id, const TaskId &task_id,
                            int attempt, std::string_view stream,
                            std::string_view msg) {
    if (auto *api = deps_.api_server();
        api && api->websocket_hub().connection_count() > 0) {
      api->websocket_hub().broadcast_log(
          http::WebSocketHub::LogMessage{.timestamp = util::format_timestamp(),
                                         .dag_run_id = dag_run_id.str(),
                                         .task_id = task_id.str(),
                                         .stream = std::string(stream),
                                         .content = std::string(msg)});
    }
    if (!deps_.persistence || !should_persist_task_log(stream, msg)) {
      return;
    }
    enqueue_persistence(
        [this, run_id = dag_run_id.clone(), task_id = task_id.clone(), attempt,
         stream = std::string(stream), message = std::string(msg)]() mutable {
          return persist_task_log(std::move(run_id), std::move(task_id), attempt,
                                  std::move(stream), std::move(message));
        },
        false);
  };

  callbacks.on_persist_run = [this](std::shared_ptr<const DAGRun> run) {
    if (!deps_.persistence) {
      return;
    }
    enqueue_persistence(
        [this, run = std::move(run)]() mutable {
          return persist_run(std::move(run));
        });
  };

  callbacks.on_persist_task = [this](const DAGRunId &dag_run_id,
                                     const TaskInstanceInfo &info) {
    if (!deps_.persistence) {
      return;
    }
    deps_.persistence->submit_task_instance_update(dag_run_id, info);
    deps_.mysql_batch_write_ops->fetch_add(1, std::memory_order_relaxed);
  };

  callbacks.on_persist_xcom = [this](const DAGRunId &dag_run_id,
                                     const TaskId &task_id,
                                     std::string_view key,
                                     std::string_view value_json) {
    if (!deps_.persistence) {
      return;
    }
    enqueue_persistence(
        [this, run_id = dag_run_id.clone(), task_id = task_id.clone(),
         key = std::string(key), value = std::string(value_json)]() mutable {
          return persist_xcom(std::move(run_id), std::move(task_id),
                              std::move(key), std::move(value));
        });
  };

  callbacks.get_xcom = [this](const DAGRunId &dag_run_id, const TaskId &task_id,
                              std::string_view key) -> task<Result<XComEntry>> {
    if (!deps_.persistence) {
      co_return fail(Error::NotFound);
    }
    auto xcom_res = co_await deps_.persistence->get_xcom(dag_run_id, task_id, key);
    if (!xcom_res) {
      co_return fail(xcom_res.error());
    }
    co_return ok(std::move(*xcom_res));
  };

  callbacks.get_task_xcoms =
      [this](const DAGRunId &dag_run_id,
             const TaskId &task_id) -> task<Result<std::vector<XComEntry>>> {
    if (!deps_.persistence) {
      co_return fail(Error::NotFound);
    }
    auto xcoms_res =
        co_await deps_.persistence->get_task_xcoms(dag_run_id, task_id);
    if (!xcoms_res) {
      co_return fail(xcoms_res.error());
    }
    co_return ok(std::move(*xcoms_res));
  };

  callbacks.get_run_xcoms = [this](const DAGRunId &dag_run_id)
      -> task<Result<std::vector<XComTaskEntry>>> {
    if (!deps_.persistence) {
      co_return fail(Error::NotFound);
    }
    auto xcoms_res = co_await deps_.persistence->get_run_xcoms(dag_run_id);
    if (!xcoms_res) {
      co_return fail(xcoms_res.error());
    }
    co_return ok(std::move(*xcoms_res));
  };

  callbacks.get_dag_id_by_run =
      [this](const DAGRunId &dag_run_id) -> task<Result<DAGId>> {
    if (!deps_.persistence) {
      co_return fail(Error::NotFound);
    }
    auto entry = co_await deps_.persistence->get_run_history(dag_run_id);
    if (!entry) {
      co_return fail(entry.error());
    }
    co_return ok(entry->dag_id.clone());
  };

  callbacks.get_max_retries = [this](const DAGRunId &dag_run_id,
                                     NodeIndex idx) {
    return deps_.get_max_retries(dag_run_id, idx);
  };

  callbacks.get_retry_interval = [this](const DAGRunId &dag_run_id,
                                        NodeIndex idx) {
    return deps_.get_retry_interval(dag_run_id, idx);
  };

  callbacks.on_task_heartbeat = [this](const TaskInstanceKey &key) {
    if (!deps_.persistence) {
      return;
    }
    deps_.persistence->submit_task_heartbeat(key);
  };

  callbacks.check_previous_task_state =
      [this](std::int64_t task_rowid,
             std::chrono::system_clock::time_point execution_date,
             const DAGRunId &current_dag_run_id) -> task<Result<TaskState>> {
    if (!deps_.persistence) {
      co_return fail(Error::NotFound);
    }
    if (task_rowid <= 0) {
      co_return fail(Error::NotFound);
    }
    auto state_res = co_await deps_.persistence->get_previous_task_state(
        task_rowid, execution_date, current_dag_run_id);
    if (!state_res) {
      co_return fail(state_res.error());
    }
    co_return ok(*state_res);
  };

  deps_.execution->set_callbacks(std::move(callbacks));

  deps_.scheduler->set_run_exists_callback(
      [this](const DAGId &dag_id,
             std::chrono::system_clock::time_point execution_date)
          -> task<Result<bool>> {
        if (!deps_.persistence) {
          co_return fail(Error::NotFound);
        }
        auto exists_res =
            co_await deps_.persistence->has_dag_run(dag_id, execution_date);
        if (!exists_res) {
          co_return fail(exists_res.error());
        }
        co_return ok(*exists_res);
      });

  deps_.scheduler->set_list_run_execution_dates_callback(
      [this](const DAGId &dag_id, std::chrono::system_clock::time_point start,
             std::chrono::system_clock::time_point end)
          -> task<Result<std::vector<std::chrono::system_clock::time_point>>> {
        if (!deps_.persistence) {
          co_return fail(Error::NotFound);
        }
        auto dates_res = co_await deps_.persistence->list_dag_run_execution_dates(
            dag_id, start, end);
        if (!dates_res) {
          co_return fail(dates_res.error());
        }
        co_return ok(std::move(*dates_res));
      });

  deps_.scheduler->set_get_watermark_callback(
      [this](const DAGId &dag_id)
          -> task<
              Result<std::optional<std::chrono::system_clock::time_point>>> {
        if (!deps_.persistence) {
          co_return fail(Error::NotFound);
        }
        auto watermark = co_await deps_.persistence->get_watermark(dag_id);
        if (!watermark) {
          if (watermark.error() == make_error_code(Error::NotFound)) {
            co_return ok(
                std::optional<std::chrono::system_clock::time_point>{});
          }
          co_return fail(watermark.error());
        }
        co_return ok(
            std::optional<std::chrono::system_clock::time_point>{*watermark});
      });

  deps_.scheduler->set_save_watermark_callback(
      [this](const DAGId &dag_id,
             std::chrono::system_clock::time_point watermark)
          -> task<Result<void>> {
        if (!deps_.persistence) {
          co_return fail(Error::NotFound);
        }
        auto save_res = co_await deps_.persistence->save_watermark(dag_id, watermark);
        if (!save_res) {
          co_return fail(save_res.error());
        }
        co_return ok();
      });

  deps_.scheduler->set_zombie_reaper_callback(
      [this](std::int64_t heartbeat_timeout_ms) -> task<Result<std::size_t>> {
        if (!deps_.persistence) {
          co_return fail(Error::NotFound);
        }
        auto reap_res = co_await deps_.persistence->reap_zombie_task_instances(
            heartbeat_timeout_ms);
        if (!reap_res) {
          co_return fail(reap_res.error());
        }
        co_return ok(*reap_res);
      });

  deps_.scheduler->set_on_dag_trigger(
      [this](const DAGId &dag_id,
             std::chrono::system_clock::time_point execution_date) {
        deps_.on_scheduler_trigger(dag_id, execution_date);
      });
}

auto ExecutionEventBridge::persist_task_log(DAGRunId run_id, TaskId task_id,
                                            int attempt, std::string stream,
                                            std::string message)
    -> task<Result<void>> {
  if (!deps_.persistence) {
    co_return fail(Error::SystemNotRunning);
  }
  auto result = co_await deps_.persistence->append_task_log(
      run_id, task_id, attempt, std::move(stream), std::move(message));
  if (!result) {
    co_return fail(result.error());
  }
  co_return ok();
}

auto ExecutionEventBridge::persist_run(std::shared_ptr<const DAGRun> run)
    -> task<Result<int64_t>> {
  if (!deps_.persistence || !run) {
    co_return fail(Error::SystemNotRunning);
  }
  auto result = co_await deps_.persistence->save_dag_run(*run);
  if (!result) {
    co_return fail(result.error());
  }
  co_return ok(*result);
}

auto ExecutionEventBridge::persist_xcom(DAGRunId run_id, TaskId task_id,
                                        std::string key,
                                        std::string value_json)
    -> task<Result<void>> {
  if (!deps_.persistence) {
    co_return fail(Error::SystemNotRunning);
  }
  auto result = co_await deps_.persistence->save_xcom(
      run_id, task_id, std::move(key), std::move(value_json));
  if (!result) {
    co_return fail(result.error());
  }
  co_return ok();
}

auto ExecutionEventBridge::spawn_persistence_task(spawn_task task) -> void {
  if (deps_.runtime->is_current_shard()) {
    deps_.runtime->spawn(std::move(task));
  } else {
    deps_.runtime->spawn_external(std::move(task));
  }
}

} // namespace dagforge
