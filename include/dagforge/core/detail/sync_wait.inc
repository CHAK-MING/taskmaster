namespace dagforge {

template <typename T>
[[nodiscard]] auto sync_wait_on_runtime(Runtime &runtime, task<Result<T>> op)
    -> Result<T> {
  static std::atomic<std::uint64_t> rr{0};
  const auto shard =
      static_cast<shard_id>(rr.fetch_add(1, std::memory_order_relaxed) %
                            std::max(1U, runtime.shard_count()));
  auto fut = boost::asio::co_spawn(
      runtime.shard(shard).ctx().native_handle(), std::move(op),
      boost::asio::use_future);
  try {
    return fut.get();
  } catch (const std::future_error &) {
    return fail(Error::Unknown);
  } catch (const std::exception &) {
    return fail(Error::Unknown);
  }
}

inline auto sync_wait_on_runtime(Runtime &runtime, task<void> op) -> void {
  static std::atomic<std::uint64_t> rr{0};
  const auto shard =
      static_cast<shard_id>(rr.fetch_add(1, std::memory_order_relaxed) %
                            std::max(1U, runtime.shard_count()));
  auto fut = boost::asio::co_spawn(
      runtime.shard(shard).ctx().native_handle(), std::move(op),
      boost::asio::use_future);
  try {
    fut.get();
  } catch (const std::future_error &) {
    return;
  } catch (const std::exception &) {
    return;
  }
}

template <typename T>
[[nodiscard]] auto sync_wait_on_runtime(const Runtime &runtime,
                                        task<Result<T>> op) -> Result<T> {
  return sync_wait_on_runtime(const_cast<Runtime &>(runtime), std::move(op));
}

} // namespace dagforge
