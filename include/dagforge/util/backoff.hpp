#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <chrono>
#include <cstdint>
#include <thread>
#endif

namespace dagforge {

class AdaptiveBackoff {
public:
  struct Config {
    int spin_attempts{6};
    int yield_attempts{12};
    std::chrono::microseconds initial_sleep{2};
    std::chrono::microseconds max_sleep{256};
  };

  AdaptiveBackoff() = default;
  explicit AdaptiveBackoff(Config cfg) : cfg_(cfg) {}

  void operator()() {
    if (attempts_ < cfg_.spin_attempts) {
      const int spins = 1 << (attempts_ < 10 ? attempts_ : 10);
      for (int i = 0; i < spins; ++i) {
#if defined(__x86_64__) || defined(__i386__)
        __builtin_ia32_pause();
#elif defined(__aarch64__)
        __asm__ volatile("yield" ::: "memory");
#endif
      }
    } else if (attempts_ < cfg_.yield_attempts) {
      std::this_thread::yield();
    } else {
      auto shift = static_cast<unsigned>(attempts_ - cfg_.yield_attempts);
      if (shift > 16) {
        shift = 16;
      }
      auto sleep = cfg_.initial_sleep * (1U << shift);
      if (sleep > cfg_.max_sleep) {
        sleep = cfg_.max_sleep;
      }
      std::this_thread::sleep_for(sleep);
    }
    ++attempts_;
  }

  void reset() { attempts_ = 0; }

  [[nodiscard]] auto attempts() const noexcept -> int { return attempts_; }

private:
  Config cfg_;
  int attempts_ = 0;
};

} // namespace dagforge
