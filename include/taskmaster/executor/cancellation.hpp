#pragma once

#include <atomic>
#include <memory>

namespace taskmaster {

class CancellationToken;

class CancellationSource {
public:
  CancellationSource() : state_(std::make_shared<State>()) {
  }

  [[nodiscard]] auto token() const noexcept -> CancellationToken;
  auto cancel() noexcept -> void {
    state_->cancelled.store(true, std::memory_order_release);
  }
  [[nodiscard]] auto is_cancelled() const noexcept -> bool {
    return state_->cancelled.load(std::memory_order_acquire);
  }

private:
  struct State {
    std::atomic<bool> cancelled{false};
  };
  std::shared_ptr<State> state_;

  friend class CancellationToken;
};

class CancellationToken {
public:
  CancellationToken() = default;

  [[nodiscard]] auto is_cancelled() const noexcept -> bool {
    return state_ && state_->cancelled.load(std::memory_order_acquire);
  }

  [[nodiscard]] explicit operator bool() const noexcept {
    return !is_cancelled();
  }

  [[nodiscard]] static auto none() noexcept -> CancellationToken {
    return {};
  }

private:
  explicit CancellationToken(std::shared_ptr<CancellationSource::State> state)
      : state_(std::move(state)) {
  }

  std::shared_ptr<CancellationSource::State> state_;

  friend class CancellationSource;
};

inline auto CancellationSource::token() const noexcept -> CancellationToken {
  return CancellationToken{state_};
}

}  // namespace taskmaster
