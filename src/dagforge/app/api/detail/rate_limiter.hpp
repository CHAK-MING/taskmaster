#pragma once

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <unordered_map>

namespace dagforge::api_detail {

struct RateLimiterConfig {
  bool enabled{true};
  std::uint32_t capacity{20};
  std::uint32_t refill_per_sec{5};
  std::uint32_t idle_ttl_sec{900};
  std::uint32_t cleanup_interval_sec{60};
  std::uint32_t max_entries{8192};
};

class RateLimiter {
public:
  struct Decision {
    bool allowed{true};
    std::chrono::seconds retry_after{0};
  };

  explicit RateLimiter(RateLimiterConfig config)
      : config_(config), hash_seed_(make_hash_seed()),
        shards_{Shard{derive_shard_seed(0)},  Shard{derive_shard_seed(1)},
                Shard{derive_shard_seed(2)},  Shard{derive_shard_seed(3)},
                Shard{derive_shard_seed(4)},  Shard{derive_shard_seed(5)},
                Shard{derive_shard_seed(6)},  Shard{derive_shard_seed(7)},
                Shard{derive_shard_seed(8)},  Shard{derive_shard_seed(9)},
                Shard{derive_shard_seed(10)}, Shard{derive_shard_seed(11)},
                Shard{derive_shard_seed(12)}, Shard{derive_shard_seed(13)},
                Shard{derive_shard_seed(14)}, Shard{derive_shard_seed(15)}} {}

  [[nodiscard]] auto check_and_consume(std::string_view principal) -> Decision {
    if (!config_.enabled || principal.empty()) {
      return {};
    }

    const auto now = std::chrono::steady_clock::now();
    auto &shard = shards_[shard_index(principal)];
    std::scoped_lock lock(shard.mutex);

    maybe_cleanup(shard, now);

    auto it = shard.entries.find(principal);
    if (it == shard.entries.end()) {
      if (shard.entries.size() >= config_.max_entries) {
        maybe_cleanup(shard, now, true);
      }
      if (shard.entries.size() >= config_.max_entries) {
        evict_oldest_entry(shard);
      }
      if (shard.entries.size() >= config_.max_entries) {
        return Decision{.allowed = false,
                        .retry_after = std::chrono::seconds(1)};
      }
      it = shard.entries
               .emplace(std::string(principal),
                        Entry{.tokens = static_cast<double>(config_.capacity),
                              .last_refill = now,
                              .last_seen = now})
               .first;
    }

    auto &entry = it->second;
    refill(entry, now);
    entry.last_seen = now;
    if (entry.tokens >= 1.0) {
      entry.tokens -= 1.0;
      return {};
    }

    const double deficit = std::max(0.0, 1.0 - entry.tokens);
    const auto seconds = static_cast<std::uint64_t>(
        std::max(1.0, std::ceil(deficit / static_cast<double>(config_.refill_per_sec))));
    return Decision{.allowed = false,
                    .retry_after = std::chrono::seconds(seconds)};
  }

private:
  struct Entry {
    double tokens{0.0};
    std::chrono::steady_clock::time_point last_refill{};
    std::chrono::steady_clock::time_point last_seen{};
  };

  struct Shard {
    std::mutex mutex;
    struct Hasher {
      using is_transparent = void;

      std::uint64_t seed{0};

      [[nodiscard]] auto operator()(std::string_view value) const noexcept
          -> std::size_t {
        return static_cast<std::size_t>(RateLimiter::seeded_hash(value, seed));
      }

      [[nodiscard]] auto operator()(const std::string &value) const noexcept
          -> std::size_t {
        return (*this)(std::string_view(value));
      }

      [[nodiscard]] auto operator()(const char *value) const noexcept
          -> std::size_t {
        return value == nullptr ? 0U : (*this)(std::string_view(value));
      }
    };

    struct Equal {
      using is_transparent = void;

      [[nodiscard]] auto operator()(std::string_view lhs,
                                    std::string_view rhs) const noexcept
          -> bool {
        return lhs == rhs;
      }

      [[nodiscard]] auto operator()(const std::string &lhs,
                                    const std::string &rhs) const noexcept
          -> bool {
        return lhs == rhs;
      }

      [[nodiscard]] auto operator()(const std::string &lhs,
                                    std::string_view rhs) const noexcept
          -> bool {
        return std::string_view(lhs) == rhs;
      }

      [[nodiscard]] auto operator()(std::string_view lhs,
                                    const std::string &rhs) const noexcept
          -> bool {
        return lhs == std::string_view(rhs);
      }
    };

    Shard() : entries(0, Hasher{0}, Equal{}) {}
    explicit Shard(std::uint64_t seed) : entries(0, Hasher{seed}, Equal{}) {}

    std::unordered_map<std::string, Entry, Hasher, Equal> entries;
    std::chrono::steady_clock::time_point next_cleanup{};
  };

  static constexpr std::size_t kShardCount = 16;

  RateLimiterConfig config_;
  std::uint64_t hash_seed_{0};
  std::array<Shard, kShardCount> shards_{};

  [[nodiscard]] auto shard_index(std::string_view principal) const -> std::size_t {
    return static_cast<std::size_t>(seeded_hash(principal, hash_seed_)) %
           kShardCount;
  }

  [[nodiscard]] static auto make_hash_seed() -> std::uint64_t {
    std::random_device rd;
    std::uint64_t seed = (static_cast<std::uint64_t>(rd()) << 32U) ^ rd();
    if (seed == 0) {
      seed = 0x9e3779b97f4a7c15ULL;
    }
    return seed;
  }

  [[nodiscard]] static auto seeded_hash(std::string_view value,
                                        std::uint64_t seed) noexcept
      -> std::uint64_t {
    std::uint64_t hash = 1469598103934665603ULL ^ seed;
    for (const unsigned char ch : value) {
      hash ^= ch;
      hash *= 1099511628211ULL;
    }
    hash ^= (seed >> 7U);
    hash *= 1099511628211ULL;
    return hash;
  }

  [[nodiscard]] auto derive_shard_seed(std::uint64_t index) const noexcept
      -> std::uint64_t {
    return hash_seed_ ^ (index * 0xbf58476d1ce4e5b9ULL);
  }

  auto refill(Entry &entry, std::chrono::steady_clock::time_point now) const
      -> void {
    const auto elapsed = std::chrono::duration_cast<std::chrono::duration<double>>(
        now - entry.last_refill);
    if (elapsed.count() <= 0.0) {
      return;
    }
    entry.tokens = std::min(
        static_cast<double>(config_.capacity),
        entry.tokens + elapsed.count() * static_cast<double>(config_.refill_per_sec));
    entry.last_refill = now;
  }

  auto maybe_cleanup(Shard &shard, std::chrono::steady_clock::time_point now,
                     bool force = false) const -> void {
    if (!force && shard.next_cleanup > now) {
      return;
    }
    const auto idle_ttl = std::chrono::seconds(config_.idle_ttl_sec);
    for (auto it = shard.entries.begin(); it != shard.entries.end();) {
      if (now - it->second.last_seen >= idle_ttl) {
        it = shard.entries.erase(it);
      } else {
        ++it;
      }
    }
    shard.next_cleanup =
        now + std::chrono::seconds(config_.cleanup_interval_sec);
  }

  static auto evict_oldest_entry(Shard &shard) -> void {
    if (shard.entries.empty()) {
      return;
    }
    auto oldest = shard.entries.begin();
    for (auto it = std::next(shard.entries.begin()); it != shard.entries.end();
         ++it) {
      if (it->second.last_seen < oldest->second.last_seen) {
        oldest = it;
      }
    }
    shard.entries.erase(oldest);
  }
};

} // namespace dagforge::api_detail
