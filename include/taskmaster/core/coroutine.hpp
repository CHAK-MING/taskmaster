#pragma once

#include <atomic>
#include <concepts>
#include <coroutine>
#include <exception>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

namespace taskmaster {

template <typename T = void>
  requires std::movable<T> || std::is_void_v<T>
class task;

template <typename T>
concept await_suspend_result = std::same_as<T, void> || std::same_as<T, bool> ||
                               std::same_as<T, std::coroutine_handle<>>;

template <typename T>
concept awaiter = requires(T t, std::coroutine_handle<> h) {
  { t.await_ready() } -> std::same_as<bool>;
  { t.await_suspend(h) } -> await_suspend_result;
  { t.await_resume() };
};

template <typename T>
concept awaitable = awaiter<T> || requires(T t) {
  { t.operator co_await() } -> awaiter;
} || requires(T t) {
  { operator co_await(t) } -> awaiter;
};

// Concept for deferred initialization types
template <typename T>
concept DeferredInitializable =
    std::destructible<T> && !std::is_reference_v<T> && !std::is_const_v<T>;

// Concept for promise types with continuation support
template <typename Promise>
concept HasContinuation = requires(Promise p) {
  { p.continuation() } -> std::same_as<std::coroutine_handle<>>;
};

template <DeferredInitializable T>
class deferred_init {
public:
  deferred_init() noexcept = default;
  ~deferred_init() noexcept {
    if (initialized_) {
      std::destroy_at(std::launder(reinterpret_cast<T*>(&storage_)));
    }
  }

  deferred_init(const deferred_init&) = delete;
  deferred_init& operator=(const deferred_init&) = delete;
  deferred_init(deferred_init&&) = delete;
  deferred_init& operator=(deferred_init&&) = delete;

  template <typename... Args>
    requires std::is_constructible_v<T, Args...>
  auto
  emplace(Args&&... args) noexcept(std::is_nothrow_constructible_v<T, Args...>)
      -> void {
    std::construct_at(reinterpret_cast<T*>(&storage_),
                      std::forward<Args>(args)...);
    initialized_ = true;
  }

  [[nodiscard]] auto get() & noexcept -> T& {
    return *std::launder(reinterpret_cast<T*>(&storage_));
  }

  [[nodiscard]] auto get() && noexcept -> T&& {
    return std::move(*std::launder(reinterpret_cast<T*>(&storage_)));
  }

private:
  alignas(T) std::byte storage_[sizeof(T)];
  bool initialized_ = false;
};

template <typename T>
class task_promise;

class final_awaiter {
public:
  [[nodiscard]] auto await_ready() const noexcept -> bool {
    return false;
  }

  template <HasContinuation Promise>
  auto await_suspend(std::coroutine_handle<Promise> h) const noexcept
      -> std::coroutine_handle<> {
    if (auto continuation = h.promise().continuation()) {
      return continuation;
    }
    return std::noop_coroutine();
  }

  auto await_resume() const noexcept -> void {
  }
};

template <typename T>
class task_promise {
public:
  [[nodiscard]] auto get_return_object() noexcept -> task<T>;
  [[nodiscard]] auto initial_suspend() const noexcept -> std::suspend_always {
    return {};
  }
  [[nodiscard]] auto final_suspend() const noexcept -> final_awaiter {
    return {};
  }

  template <typename U>
    requires std::convertible_to<U&&, T>
  auto return_value(U&& value) noexcept(std::is_nothrow_constructible_v<T, U&&>)
      -> void {
    result_.emplace(std::forward<U>(value));
  }

  [[noreturn]] auto unhandled_exception() const noexcept -> void {
    std::terminate();
  }

  [[nodiscard]] auto result() & noexcept -> T& {
    return result_.get();
  }
  [[nodiscard]] auto result() && noexcept -> T&& {
    return std::move(result_).get();
  }

  [[nodiscard]] auto continuation() const noexcept -> std::coroutine_handle<> {
    return continuation_;
  }

  auto set_continuation(std::coroutine_handle<> c) noexcept -> void {
    continuation_ = c;
  }

private:
  deferred_init<T> result_;
  std::coroutine_handle<> continuation_{};
};

template <>
class task_promise<void> {
public:
  [[nodiscard]] auto get_return_object() noexcept -> task<void>;
  [[nodiscard]] auto initial_suspend() const noexcept -> std::suspend_always {
    return {};
  }
  [[nodiscard]] auto final_suspend() const noexcept -> final_awaiter {
    return {};
  }

  auto return_void() const noexcept -> void {
  }

  [[noreturn]] auto unhandled_exception() const noexcept -> void {
    std::terminate();
  }

  [[nodiscard]] auto continuation() const noexcept -> std::coroutine_handle<> {
    return continuation_;
  }

  auto set_continuation(std::coroutine_handle<> c) noexcept -> void {
    continuation_ = c;
  }

private:
  std::coroutine_handle<> continuation_{};
};

template <typename T>
  requires std::movable<T> || std::is_void_v<T>
class [[nodiscard]] task {
public:
  using promise_type = task_promise<T>;
  using handle_type = std::coroutine_handle<promise_type>;

  task() noexcept = default;
  explicit task(handle_type h) noexcept : handle_(h) {
  }

  task(task&& other) noexcept : handle_(std::exchange(other.handle_, nullptr)) {
  }

  task& operator=(task&& other) noexcept {
    if (this != &other) {
      destroy();
      handle_ = std::exchange(other.handle_, nullptr);
    }
    return *this;
  }

  task(const task&) = delete;
  task& operator=(const task&) = delete;

  ~task() {
    destroy();
  }

  [[nodiscard]] auto done() const noexcept -> bool {
    return !handle_ || handle_.done();
  }

  [[nodiscard]] auto take() noexcept -> std::coroutine_handle<> {
    return std::exchange(handle_, nullptr);
  }

  [[nodiscard]] auto handle() const noexcept -> handle_type {
    return handle_;
  }

  class awaiter {
  public:
    explicit awaiter(handle_type h) noexcept : handle_(h) {
    }

    [[nodiscard]] auto await_ready() const noexcept -> bool {
      return !handle_ || handle_.done();
    }

    auto await_suspend(std::coroutine_handle<> continuation) noexcept
        -> std::coroutine_handle<> {
      handle_.promise().set_continuation(continuation);
      return handle_;
    }

    [[nodiscard]] auto await_resume() noexcept -> decltype(auto) {
      if constexpr (!std::same_as<T, void>) {
        return std::move(handle_.promise()).result();
      }
    }

  private:
    handle_type handle_;
  };

  [[nodiscard]] auto operator co_await() noexcept -> awaiter {
    return awaiter{std::exchange(handle_, nullptr)};
  }

private:
  auto destroy() noexcept -> void {
    if (handle_) {
      handle_.destroy();
      handle_ = nullptr;
    }
  }

  handle_type handle_;
};

template <typename T>
auto task_promise<T>::get_return_object() noexcept -> task<T> {
  return task<T>{std::coroutine_handle<task_promise<T>>::from_promise(*this)};
}

inline auto task_promise<void>::get_return_object() noexcept -> task<void> {
  return task<void>{
      std::coroutine_handle<task_promise<void>>::from_promise(*this)};
}

using spawn_task = task<void>;

// ============================================================================
// when_all: Concurrently await multiple tasks
// ============================================================================

namespace detail {

struct when_all_counter {
  std::atomic<std::size_t> count;
  std::coroutine_handle<> continuation{nullptr};

  explicit when_all_counter(std::size_t n) noexcept : count(n) {}

  auto notify_complete() noexcept -> void {
    if (count.fetch_sub(1, std::memory_order_acq_rel) == 1 && continuation) {
      continuation.resume();
    }
  }
};

template <typename T>
struct when_all_task_state {
  deferred_init<T> result;
  when_all_counter* counter{nullptr};

  auto store_and_notify(T&& value) noexcept -> void {
    result.emplace(std::move(value));
    counter->notify_complete();
  }
};

template <typename T>
class when_all_task {
public:
  struct promise_type;
  using handle_type = std::coroutine_handle<promise_type>;

  struct promise_type {
    when_all_task_state<T>* state{nullptr};

    auto get_return_object() noexcept -> when_all_task {
      return when_all_task{handle_type::from_promise(*this)};
    }
    auto initial_suspend() noexcept -> std::suspend_always { return {}; }
    auto final_suspend() noexcept {
      struct final_awaiter {
        auto await_ready() noexcept -> bool { return false; }
        auto await_suspend(handle_type h) noexcept -> void {
          h.destroy();
        }
        auto await_resume() noexcept -> void {}
      };
      return final_awaiter{};
    }
    auto return_value(T value) noexcept -> void {
      if (state) {
        state->store_and_notify(std::move(value));
      }
    }
    [[noreturn]] auto unhandled_exception() noexcept -> void { std::terminate(); }
  };

  explicit when_all_task(handle_type h) noexcept : handle_(h) {}
  when_all_task(when_all_task&& other) noexcept : handle_(std::exchange(other.handle_, nullptr)) {}
  ~when_all_task() = default;

  auto start(when_all_task_state<T>& state) noexcept -> void {
    handle_.promise().state = &state;
    handle_.resume();
  }

private:
  handle_type handle_;
};

template <typename T>
auto make_when_all_task(task<T> t) -> when_all_task<T> {
  co_return co_await std::move(t);
}

}  // namespace detail

template <typename T, typename U>
class when_all_awaiter_2 {
public:
  when_all_awaiter_2(task<T>&& t1, task<U>&& t2) noexcept
      : task1_(detail::make_when_all_task(std::move(t1))),
        task2_(detail::make_when_all_task(std::move(t2))),
        counter_(2) {
    state1_.counter = &counter_;
    state2_.counter = &counter_;
  }

  [[nodiscard]] auto await_ready() const noexcept -> bool { return false; }

  auto await_suspend(std::coroutine_handle<> continuation) noexcept -> void {
    counter_.continuation = continuation;
    task1_.start(state1_);
    task2_.start(state2_);
  }

  [[nodiscard]] auto await_resume() noexcept -> std::tuple<T, U> {
    return {std::move(state1_.result).get(), std::move(state2_.result).get()};
  }

private:
  detail::when_all_task<T> task1_;
  detail::when_all_task<U> task2_;
  detail::when_all_task_state<T> state1_;
  detail::when_all_task_state<U> state2_;
  detail::when_all_counter counter_;
};

template <typename T, typename U>
[[nodiscard]] auto when_all(task<T>&& t1, task<U>&& t2) -> when_all_awaiter_2<T, U> {
  return when_all_awaiter_2<T, U>(std::move(t1), std::move(t2));
}

}  // namespace taskmaster
