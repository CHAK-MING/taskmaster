#pragma once

#include <concepts>
#include <coroutine>
#include <memory>
#include <utility>

namespace taskmaster {

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

template <typename T> class deferred_init {
public:
  deferred_init() noexcept = default;

  template <typename U>
    requires std::is_constructible_v<T, U>
  explicit deferred_init(U &&value) noexcept(
      std::is_nothrow_constructible_v<T, U>) {
    std::construct_at(std::addressof(storage_.value), std::forward<U>(value));
  }

  template <typename... Args>
    requires std::is_constructible_v<T, Args...>
  auto
  emplace(Args &&...args) noexcept(std::is_nothrow_constructible_v<T, Args...>)
      -> void {
    std::construct_at(std::addressof(storage_.value),
                      std::forward<Args>(args)...);
  }

  [[nodiscard]] auto operator*() & noexcept -> T & { return storage_.value; }

  [[nodiscard]] auto operator*() const & noexcept -> const T & {
    return storage_.value;
  }

  [[nodiscard]] auto operator*() && noexcept -> T && {
    return std::move(storage_.value);
  }

  [[nodiscard]] auto operator->() noexcept -> T * {
    return std::addressof(storage_.value);
  }

  [[nodiscard]] auto operator->() const noexcept -> const T * {
    return std::addressof(storage_.value);
  }

private:
  union storage {
    storage() noexcept {}

    ~storage() noexcept(std::is_nothrow_destructible_v<T>) {
      if constexpr (!std::is_trivially_destructible_v<T>) {
        value.~T();
      }
    }

    T value;
  } storage_;
};

template <typename T = void> class task;
template <typename T = void> class task_promise;

template <typename T> class task_promise_base {
public:
  class final_awaiter {
  public:
    [[nodiscard]] auto await_ready() const noexcept -> bool { return false; }

    auto await_resume() const noexcept -> void {}

    [[nodiscard]] auto await_suspend(
        std::coroutine_handle<task_promise<T>> coroutine) const noexcept
        -> std::coroutine_handle<> {
      if (coroutine.promise().is_detached()) {
        coroutine.destroy();
        return std::noop_coroutine();
      }
      return coroutine.promise().get_continuation();
    }
  };

  task_promise_base() noexcept
      : continuation_{std::noop_coroutine()}, detached_{false} {}

  [[nodiscard]] auto initial_suspend() const noexcept -> std::suspend_always {
    return {};
  }

  [[nodiscard]] auto final_suspend() const noexcept -> final_awaiter {
    return {};
  }

  [[noreturn]] auto unhandled_exception() const noexcept -> void {
    // Log before terminating to aid debugging
    // Note: Cannot use log::error here as it may allocate
    std::fputs("[FATAL] Unhandled exception in task coroutine\n", stderr);
    std::fflush(stderr);
    std::terminate();
  }

  [[nodiscard]] auto get_continuation() const noexcept
      -> std::coroutine_handle<> {
    return continuation_;
  }

  auto set_continuation(std::coroutine_handle<> continuation) noexcept -> void {
    continuation_ = continuation;
  }

  [[nodiscard]] auto is_detached() const noexcept -> bool { return detached_; }

  auto set_detached(bool detached) noexcept -> void { detached_ = detached; }

private:
  std::coroutine_handle<> continuation_;
  bool detached_;
};

template <> class task_promise<void> final : public task_promise_base<void> {
public:
  [[nodiscard]] auto get_return_object() noexcept -> task<void>;
  auto return_void() const noexcept -> void {}
};

template <typename T> class task_promise final : public task_promise_base<T> {
public:
  [[nodiscard]] auto get_return_object() noexcept -> task<T>;

  template <typename U>
    requires std::convertible_to<U &&, T>
  auto return_value(U &&value) noexcept(std::is_nothrow_convertible_v<U &&, T>)
      -> void {
    result_.emplace(std::forward<U>(value));
  }

  [[nodiscard]] auto get_result() & noexcept -> T & { return *result_; }

  [[nodiscard]] auto get_result() const & noexcept -> const T & {
    return *result_;
  }

  [[nodiscard]] auto get_result() && noexcept -> T && {
    return std::move(*result_);
  }

private:
  deferred_init<T> result_;
};

template <typename T> class [[nodiscard]] task {
public:
  using promise_type = task_promise<T>;

  task(task &&other) noexcept
      : coroutine_{std::exchange(other.coroutine_, nullptr)} {}

  task &operator=(task &&other) noexcept {
    if (this != &other) {
      if (coroutine_) {
        coroutine_.destroy();
      }
      coroutine_ = std::exchange(other.coroutine_, nullptr);
    }
    return *this;
  }

  task(const task &) = delete;
  task &operator=(const task &) = delete;

  ~task() {
    if (coroutine_) {
      if (coroutine_.done()) {
        coroutine_.destroy();
      } else {
        coroutine_.promise().set_detached(true);
      }
    }
  }

  class task_awaiter {
  public:
    explicit task_awaiter(
        std::coroutine_handle<task_promise<T>> coroutine) noexcept
        : coroutine_{coroutine} {}

    task_awaiter(task_awaiter &&other) noexcept
        : coroutine_{std::exchange(other.coroutine_, nullptr)} {}

    task_awaiter &operator=(task_awaiter &&other) noexcept {
      if (this != &other) {
        if (coroutine_)
          coroutine_.destroy();
        coroutine_ = std::exchange(other.coroutine_, nullptr);
      }
      return *this;
    }

    ~task_awaiter() {
      if (coroutine_)
        coroutine_.destroy();
    }

    [[nodiscard]] auto await_ready() const noexcept -> bool {
      return coroutine_.done();
    }

    auto await_suspend(std::coroutine_handle<> continuation) const noexcept
        -> void {
      coroutine_.promise().set_continuation(continuation);
      coroutine_.resume();
    }

    [[nodiscard]] auto await_resume() const noexcept -> decltype(auto) {
      if constexpr (!std::same_as<T, void>) {
        return coroutine_.promise().get_result();
      }
    }

  private:
    std::coroutine_handle<task_promise<T>> coroutine_;
  };

  [[nodiscard]] auto operator co_await() noexcept -> task_awaiter {
    return task_awaiter{std::exchange(coroutine_, nullptr)};
  }

  [[nodiscard]] auto done() const noexcept -> bool {
    return coroutine_ && coroutine_.done();
  }

  auto detach() noexcept -> void {
    if (coroutine_) {
      coroutine_.promise().set_detached(true);
    }
  }

  [[nodiscard]] auto get_handle() const noexcept -> std::coroutine_handle<> {
    return coroutine_;
  }

  [[nodiscard]] auto get_result() & noexcept -> decltype(auto)
    requires(!std::same_as<T, void>)
  {
    return coroutine_.promise().get_result();
  }

  [[nodiscard]] auto get_result() && noexcept -> decltype(auto)
    requires(!std::same_as<T, void>)
  {
    return std::move(coroutine_.promise()).get_result();
  }

private:
  friend promise_type;

  explicit task(std::coroutine_handle<task_promise<T>> coroutine) noexcept
      : coroutine_{coroutine} {}

  std::coroutine_handle<task_promise<T>> coroutine_;
};

template <typename T = void> class eager_task;
template <typename T = void> class eager_task_promise;

template <typename T>
class eager_task_promise final : public task_promise_base<T> {
public:
  [[nodiscard]] auto get_return_object() noexcept -> eager_task<T>;

  [[nodiscard]] auto initial_suspend() const noexcept -> std::suspend_never {
    return {};
  }

  template <typename U>
    requires std::convertible_to<U &&, T>
  auto return_value(U &&value) noexcept(std::is_nothrow_convertible_v<U &&, T>)
      -> void {
    result_.emplace(std::forward<U>(value));
  }

  [[nodiscard]] auto get_result() & noexcept -> T & { return *result_; }

  [[nodiscard]] auto get_result() const & noexcept -> const T & {
    return *result_;
  }

  [[nodiscard]] auto get_result() && noexcept -> T && {
    return std::move(*result_);
  }

private:
  deferred_init<T> result_;
};

template <>
class eager_task_promise<void> final : public task_promise_base<void> {
public:
  [[nodiscard]] auto get_return_object() noexcept -> eager_task<void>;

  [[nodiscard]] auto initial_suspend() const noexcept -> std::suspend_never {
    return {};
  }

  auto return_void() const noexcept -> void {}
};

template <typename T> class [[nodiscard]] eager_task {
public:
  using promise_type = eager_task_promise<T>;

  eager_task(eager_task &&other) noexcept
      : coroutine_{std::exchange(other.coroutine_, nullptr)} {}

  eager_task &operator=(eager_task &&other) noexcept {
    if (this != &other) {
      if (coroutine_) {
        coroutine_.destroy();
      }
      coroutine_ = std::exchange(other.coroutine_, nullptr);
    }
    return *this;
  }

  eager_task(const eager_task &) = delete;
  eager_task &operator=(const eager_task &) = delete;

  ~eager_task() {
    if (coroutine_) {
      if (coroutine_.done()) {
        coroutine_.destroy();
      } else {
        coroutine_.promise().set_detached(true);
      }
    }
  }

  class eager_task_awaiter {
  public:
    [[nodiscard]] auto await_ready() const noexcept -> bool {
      return coroutine_.done();
    }

    [[nodiscard]] auto
    await_suspend(std::coroutine_handle<> continuation) const noexcept
        -> std::coroutine_handle<> {
      coroutine_.promise().set_continuation(continuation);
      return coroutine_;
    }

    [[nodiscard]] auto await_resume() const noexcept -> decltype(auto) {
      if constexpr (!std::same_as<T, void>) {
        return coroutine_.promise().get_result();
      }
    }

  private:
    friend eager_task;

    explicit eager_task_awaiter(
        std::coroutine_handle<eager_task_promise<T>> coroutine) noexcept
        : coroutine_{coroutine} {}

    std::coroutine_handle<eager_task_promise<T>> coroutine_;
  };

  [[nodiscard]] auto operator co_await() noexcept -> eager_task_awaiter {
    return eager_task_awaiter{coroutine_};
  }

  auto resume() const noexcept -> void {
    if (coroutine_) {
      coroutine_.resume();
    }
  }

  [[nodiscard]] auto done() const noexcept -> bool {
    return coroutine_ && coroutine_.done();
  }

  auto detach() noexcept -> void {
    if (coroutine_) {
      coroutine_.promise().set_detached(true);
    }
  }

private:
  friend eager_task_promise<T>;

  explicit eager_task(
      std::coroutine_handle<eager_task_promise<T>> coroutine) noexcept
      : coroutine_{coroutine} {}

  std::coroutine_handle<eager_task_promise<T>> coroutine_;
};

inline auto task_promise<void>::get_return_object() noexcept -> task<void> {
  return task<void>{std::coroutine_handle<task_promise>::from_promise(*this)};
}

template <typename T>
auto task_promise<T>::get_return_object() noexcept -> task<T> {
  return task<T>{std::coroutine_handle<task_promise>::from_promise(*this)};
}

inline auto eager_task_promise<void>::get_return_object() noexcept
    -> eager_task<void> {
  return eager_task<void>{
      std::coroutine_handle<eager_task_promise>::from_promise(*this)};
}

template <typename T>
auto eager_task_promise<T>::get_return_object() noexcept -> eager_task<T> {
  return eager_task<T>{
      std::coroutine_handle<eager_task_promise>::from_promise(*this)};
}

class spawn_task_promise;

class spawn_task {
public:
  using promise_type = spawn_task_promise;

  spawn_task() = default;
  spawn_task(std::coroutine_handle<> coro) : coro_(coro) {}

  ~spawn_task() {
    // Only destroy if we own the handle (not scheduled on Runtime)
    if (coro_ && coro_.done()) {
      coro_.destroy();
    }
  }

  spawn_task(spawn_task &&other) noexcept : coro_(std::exchange(other.coro_, {})) {}

  spawn_task &operator=(spawn_task &&other) noexcept {
    if (this != &other) {
      // Destroy our handle if we own it
      if (coro_ && coro_.done()) {
        coro_.destroy();
      }
      coro_ = std::exchange(other.coro_, {});
    }
    return *this;
  }

  spawn_task(const spawn_task &) = delete;
  spawn_task &operator=(const spawn_task &) = delete;

  [[nodiscard]] auto handle() const noexcept -> std::coroutine_handle<> { return coro_; }

  // Release ownership - the Runtime will handle destruction
  [[nodiscard]] auto release() noexcept -> std::coroutine_handle<> {
    return std::exchange(coro_, {});
  }

private:
  std::coroutine_handle<> coro_;
};

class spawn_task_promise {
public:
  [[nodiscard]] auto get_return_object() noexcept -> spawn_task {
    return spawn_task{std::coroutine_handle<spawn_task_promise>::from_promise(*this)};
  }

  [[nodiscard]] auto initial_suspend() const noexcept -> std::suspend_always {
    return {};
  }

  [[nodiscard]] auto final_suspend() const noexcept -> std::suspend_never {
    return {};
  }

  [[noreturn]] auto unhandled_exception() const noexcept -> void {
    // Log before terminating to aid debugging
    std::fputs("[FATAL] Unhandled exception in spawn_task coroutine\n", stderr);
    std::fflush(stderr);
    std::terminate();
  }

  auto return_void() const noexcept -> void {}
};

template <typename Awaitable>
  requires awaitable<Awaitable>
auto spawn(Awaitable &&awaitable) -> void {
  [](Awaitable a) noexcept -> spawn_task {
    co_await std::forward<Awaitable>(a);
  }(std::forward<Awaitable>(awaitable));
}

class suspend_always_awaiter {
public:
  [[nodiscard]] constexpr auto await_ready() const noexcept -> bool {
    return false;
  }
  constexpr auto await_suspend(std::coroutine_handle<>) const noexcept -> void {
  }
  constexpr auto await_resume() const noexcept -> void {}
};

class suspend_never_awaiter {
public:
  [[nodiscard]] constexpr auto await_ready() const noexcept -> bool {
    return true;
  }
  constexpr auto await_suspend(std::coroutine_handle<>) const noexcept -> void {
  }
  constexpr auto await_resume() const noexcept -> void {}
};

} // namespace taskmaster
