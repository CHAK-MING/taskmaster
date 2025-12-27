#pragma once

#include <concepts>
#include <coroutine>
#include <cstdio>
#include <exception>
#include <memory>
#include <new>
#include <utility>

namespace taskmaster {

template <typename T>
class task;

class scheduler {
public:
  virtual ~scheduler() = default;
  virtual auto schedule(std::coroutine_handle<> handle) noexcept -> void = 0;
  [[nodiscard]] virtual auto get_shard_id() const noexcept -> unsigned = 0;
  [[nodiscard]] virtual auto is_current_shard() const noexcept -> bool = 0;
};

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

template <typename T>
class deferred_init {
public:
  deferred_init() noexcept = default;
  ~deferred_init() noexcept(std::is_nothrow_destructible_v<T>) {
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

  template <typename Promise>
  auto await_suspend(std::coroutine_handle<Promise> h) const noexcept
      -> std::coroutine_handle<> {
    auto continuation = h.promise().continuation();
    if (continuation) {
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
  std::coroutine_handle<> continuation_;
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
  std::coroutine_handle<> continuation_;
};

template <typename T = void>
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

inline thread_local scheduler* current_scheduler_ptr = nullptr;

[[nodiscard]] inline auto current_scheduler() noexcept -> scheduler* {
  return current_scheduler_ptr;
}

using spawn_task = task<void>;

inline auto spawn(spawn_task&& t) -> void {
  auto* sched = current_scheduler();
  if (sched) {
    sched->schedule(t.take());
  } else {
    // Programming error: spawn called outside of shard context
    std::fprintf(
        stderr,
        "[WARN] spawn() called without scheduler context, task dropped\n");
  }
}

}  // namespace taskmaster
