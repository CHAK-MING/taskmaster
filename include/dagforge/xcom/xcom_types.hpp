#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/config/task_types.hpp"
#include "dagforge/util/string_hash.hpp"
#endif

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/util/id.hpp"
#endif

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <chrono>
#include <functional>
#include <optional>
#include <string>
#include <tuple>
#include <unordered_map>
#endif


namespace dagforge {

struct XComEntry {
  std::string key;
  // Serialized JSON payload.
  std::string value;
  std::size_t byte_size{0};
  std::chrono::system_clock::time_point created_at{};
};

struct XComTaskEntry {
  TaskId task_id;
  std::string key;
  // Serialized JSON payload.
  std::string value;
  std::size_t byte_size{0};
  std::chrono::system_clock::time_point created_at{};
};

using XComMap =
    std::unordered_map<std::string, XComEntry, StringHash, StringEqual>;

class XComCache {
public:
  // Thread-safety:
  // This cache is intentionally unsynchronized. It is expected to live inside
  // shard-local / run-local state and be accessed only from the owning shard's
  // event loop thread. Values are stored in their rendered, template-ready
  // representation so xcom_pull resolution avoids repeated JSON parsing.
  // Callers that want cross-thread access must provide their own external
  // synchronization.
  struct CacheKey {
    DAGRunId run_id;
    TaskId task_id;
    std::string key;

    auto operator==(const CacheKey &other) const -> bool = default;
  };

  using CacheKeyView =
      std::tuple<const DAGRunId &, const TaskId &, std::string_view>;

  struct CacheKeyHash {
    using is_transparent = void;
    using HashKey =
        std::tuple<std::string_view, std::string_view, std::string_view>;

    auto operator()(const CacheKey &key) const noexcept -> std::size_t {
      return hash(HashKey{key.run_id.value(), key.task_id.value(), key.key});
    }

    auto operator()(const CacheKeyView &key) const noexcept -> std::size_t {
      return hash(HashKey{std::get<0>(key).value(), std::get<1>(key).value(),
                          std::get<2>(key)});
    }

  private:
    [[nodiscard]] static auto hash(const HashKey &key) noexcept -> std::size_t {
      return static_cast<std::size_t>(
          ankerl::unordered_dense::hash<HashKey>{}(key));
    }
  };

  struct CacheKeyEqual {
    using is_transparent = void;

    auto operator()(const CacheKey &a, const CacheKey &b) const -> bool {
      return a == b;
    }
    auto operator()(const CacheKey &a, const CacheKeyView &t) const -> bool {
      return a.run_id == std::get<0>(t) && a.task_id == std::get<1>(t) &&
             a.key == std::get<2>(t);
    }
    auto operator()(const CacheKeyView &t, const CacheKey &a) const -> bool {
      return a.run_id == std::get<0>(t) && a.task_id == std::get<1>(t) &&
             a.key == std::get<2>(t);
    }
  };

  void set(const DAGRunId &run_id, const TaskId &task_id, std::string_view key,
           std::string_view rendered_value) {
    cache_[CacheKey{run_id.clone(), task_id.clone(), std::string(key)}] =
        std::string(rendered_value);
  }

  [[nodiscard]] auto get(const DAGRunId &run_id, const TaskId &task_id,
                         std::string_view key) const
      -> Result<std::reference_wrapper<const std::string>> {
    auto it = cache_.find(CacheKeyView{run_id, task_id, key});
    if (it != cache_.end()) {
      return ok(std::cref(it->second));
    }
    return fail(Error::NotFound);
  }

  void reserve(std::size_t n) { cache_.reserve(n); }

  void clear_run(const DAGRunId &run_id) {
    std::erase_if(cache_, [&run_id](const auto &pair) {
      return pair.first.run_id == run_id;
    });
  }

  void clear() { cache_.clear(); }

private:
  std::unordered_map<CacheKey, std::string, CacheKeyHash, CacheKeyEqual> cache_;
};

} // namespace dagforge
