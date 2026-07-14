#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/runtime.hpp"
#include "dagforge/core/shard.hpp"
#include "dagforge/util/id.hpp"
#endif

#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace dagforge::executor_detail {

template <typename Handle> struct ShardExecutionState {
  std::unordered_map<InstanceId, Handle> active;
  std::unordered_set<InstanceId> cancelled;

  auto register_active(const InstanceId &id, Handle handle) -> void {
    active[id] = std::move(handle);
  }

  auto unregister_active(const InstanceId &id) -> void { active.erase(id); }

  [[nodiscard]] auto find_active(const InstanceId &id) const
      -> std::optional<Handle> {
    auto it = active.find(id);
    if (it == active.end()) {
      return std::nullopt;
    }
    return it->second;
  }

  [[nodiscard]] auto find_active_mut(const InstanceId &id)
      -> typename std::unordered_map<InstanceId, Handle>::iterator {
    return active.find(id);
  }

  [[nodiscard]] auto active_end()
      -> typename std::unordered_map<InstanceId, Handle>::iterator {
    return active.end();
  }

  auto mark_cancelled(const InstanceId &id) -> void { cancelled.insert(id); }

  [[nodiscard]] auto consume_cancelled(const InstanceId &id) -> bool {
    if (!cancelled.contains(id)) {
      return false;
    }
    cancelled.erase(id);
    return true;
  }

  [[nodiscard]] auto is_cancelled(const InstanceId &id) const -> bool {
    return cancelled.contains(id);
  }
};

template <typename StateVector, typename Fn>
auto cancel_on_all_shards(Runtime &runtime, StateVector &states,
                          const InstanceId &instance_id, Fn &&fn) -> void {
  for (shard_id sid = 0; sid < states.size(); ++sid) {
    runtime.post_to(sid, [&states, sid, instance_id,
                          fn = std::forward<Fn>(fn)]() mutable {
      fn(states[sid], instance_id);
    });
  }
}

} // namespace dagforge::executor_detail
