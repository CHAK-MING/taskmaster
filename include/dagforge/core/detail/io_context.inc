namespace dagforge::io {

/// Thin DAGForge-owned wrapper around Boost.Asio's io_context.
/// This keeps an escape hatch to the native context while giving DAGForge a
/// stable type to hang future module-facing APIs on.
class IoContext {
public:
  using native_type = boost::asio::io_context;
  using executor_type = native_type::executor_type;

  IoContext() = default;
  explicit IoContext(int concurrency_hint) : context_(concurrency_hint) {}

  IoContext(const IoContext &) = delete;
  auto operator=(const IoContext &) -> IoContext & = delete;
  IoContext(IoContext &&) = delete;
  auto operator=(IoContext &&) -> IoContext & = delete;

  [[nodiscard]] auto get_executor() noexcept -> executor_type {
    return context_.get_executor();
  }

  auto restart() -> void { context_.restart(); }
  auto stop() -> void { context_.stop(); }

  [[nodiscard]] auto run() -> std::size_t { return context_.run(); }
  [[nodiscard]] auto run_one() -> std::size_t { return context_.run_one(); }
  template <typename Rep, typename Period>
  [[nodiscard]] auto run_one_for(std::chrono::duration<Rep, Period> timeout)
      -> std::size_t {
    return context_.run_one_for(timeout);
  }
  [[nodiscard]] auto poll() -> std::size_t { return context_.poll(); }
  [[nodiscard]] auto poll_one() -> std::size_t { return context_.poll_one(); }
  template <typename Rep, typename Period>
  [[nodiscard]] auto run_for(std::chrono::duration<Rep, Period> timeout)
      -> std::size_t {
    return context_.run_for(timeout);
  }
  [[nodiscard]] auto stopped() const -> bool { return context_.stopped(); }

  [[nodiscard]] auto native_handle() noexcept -> native_type & {
    return context_;
  }
  [[nodiscard]] auto native_handle() const noexcept -> const native_type & {
    return context_;
  }

  // Compatibility bridge while the codebase still talks directly to Asio.
  [[nodiscard]] operator native_type &() noexcept { return context_; }
  [[nodiscard]] operator const native_type &() const noexcept {
    return context_;
  }

private:
  native_type context_;
};

template <typename Rep, typename Period>
[[nodiscard]] auto async_sleep(IoContext &ctx,
                               std::chrono::duration<Rep, Period> duration)
    -> spawn_task;

} // namespace dagforge::io
