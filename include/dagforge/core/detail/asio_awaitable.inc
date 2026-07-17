namespace dagforge {

inline constexpr auto use_nothrow =
    boost::asio::as_tuple(boost::asio::use_awaitable);
namespace awaitable_ops = boost::asio::experimental::awaitable_operators;

template <typename T>
[[nodiscard]] inline auto
as_result(std::tuple<boost::system::error_code, T> &&v) -> Result<T> {
  auto [ec, value] = std::move(v);
  if (ec) {
    return fail(io::normalize_error_code(ec));
  }
  return ok(std::move(value));
}

[[nodiscard]] inline auto as_result(std::tuple<boost::system::error_code> &&v)
    -> Result<void> {
  auto [ec] = std::move(v);
  if (ec) {
    return fail(io::normalize_error_code(ec));
  }
  return ok();
}

template <typename T>
[[nodiscard]] inline auto
as_result(std::tuple<std::exception_ptr, T> &&v) -> Result<T> {
  auto [ep, value] = std::move(v);
  if (ep) {
    try {
      std::rethrow_exception(ep);
    } catch (const boost::system::system_error &e) {
      return fail(io::normalize_error_code(e.code()));
    } catch (const std::system_error &e) {
      return fail(e.code());
    } catch (...) {
      return fail(Error::Unknown);
    }
  }
  return ok(std::move(value));
}

[[nodiscard]] inline auto as_result(std::tuple<std::exception_ptr> &&v)
    -> Result<void> {
  auto [ep] = std::move(v);
  if (ep) {
    try {
      std::rethrow_exception(ep);
    } catch (const boost::system::system_error &e) {
      return fail(io::normalize_error_code(e.code()));
    } catch (const std::system_error &e) {
      return fail(e.code());
    } catch (...) {
      return fail(Error::Unknown);
    }
  }
  return ok();
}

[[nodiscard]] inline auto as_result(std::tuple<> &&)
    -> Result<void> {
  return ok();
}

template <typename T, typename Executor>
[[nodiscard]] inline auto co_as_result(
    boost::asio::awaitable<std::tuple<boost::system::error_code, T>, Executor> op)
    -> boost::asio::awaitable<Result<T>, Executor> {
  co_return as_result(co_await std::move(op));
}

template <typename Executor>
[[nodiscard]] inline auto co_as_result(
    boost::asio::awaitable<std::tuple<>, Executor> op)
    -> boost::asio::awaitable<Result<void>, Executor> {
  co_return as_result(co_await std::move(op));
}

template <typename Executor>
[[nodiscard]] inline auto co_as_result(
    boost::asio::awaitable<std::tuple<boost::system::error_code>, Executor> op)
    -> boost::asio::awaitable<Result<void>, Executor> {
  co_return as_result(co_await std::move(op));
}

template <typename T, typename Executor>
[[nodiscard]] inline auto co_as_result(
    boost::asio::awaitable<std::tuple<std::exception_ptr, T>, Executor> op)
    -> boost::asio::awaitable<Result<T>, Executor> {
  co_return as_result(co_await std::move(op));
}

template <typename Executor>
[[nodiscard]] inline auto co_as_result(
    boost::asio::awaitable<std::tuple<std::exception_ptr>, Executor> op)
    -> boost::asio::awaitable<Result<void>, Executor> {
  co_return as_result(co_await std::move(op));
}

} // namespace dagforge
