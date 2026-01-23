#include "taskmaster/io/async_fd.hpp"

#include <fcntl.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <array>

namespace taskmaster::io {

// AsyncFd Implementation

auto AsyncFd::read_all(std::size_t max_size) -> task<IoExpected<std::string>> {
  std::string result;
  std::array<char, 4096> buf;

  while (result.size() < max_size) {
    auto res = co_await async_read(io::buffer(buf));
    if (!res) {
      if (res.is_eof()) break;
      co_return std::unexpected(res.error);
    }
    if (res.bytes_transferred == 0) break;
    result.append(buf.data(), res.bytes_transferred);
  }

  co_return result;
}

auto AsyncFd::write_all(ConstBuffer buffer) -> task<IoResult> {
  std::size_t total = 0;
  while (!buffer.empty()) {
    auto res = co_await async_write(buffer);
    if (!res) {
      co_return IoResult{total, res.error};
    }
    total += res.bytes_transferred;
    buffer += res.bytes_transferred;
  }
  co_return io_success(total);
}

// AsyncPipe Implementation

auto AsyncPipe::create(IoContext& ctx) -> IoExpected<AsyncPipe> {
  int fds[2];
  if (::pipe(fds) < 0) {
    return std::unexpected(from_errno(errno));
  }
  return AsyncPipe{
      AsyncFd::from_raw(ctx, fds[0], Ownership::Owned),
      AsyncFd::from_raw(ctx, fds[1], Ownership::Owned)};
}

auto AsyncPipe::create(IoContext& ctx, int flags) -> IoExpected<AsyncPipe> {
  int fds[2];
  if (::pipe2(fds, flags) < 0) {
    return std::unexpected(from_errno(errno));
  }
  return AsyncPipe{
      AsyncFd::from_raw(ctx, fds[0], Ownership::Owned),
      AsyncFd::from_raw(ctx, fds[1], Ownership::Owned)};
}

// AsyncEventFd Implementation

auto AsyncEventFd::create(IoContext& ctx, unsigned int initval, int flags)
    -> IoExpected<AsyncEventFd> {
  int fd = ::eventfd(initval, flags);
  if (fd < 0) {
    return std::unexpected(from_errno(errno));
  }
  return AsyncEventFd{AsyncFd::from_raw(ctx, fd, Ownership::Owned)};
}

auto AsyncEventFd::signal(std::uint64_t value) noexcept -> bool {
  while (true) {
    auto ret = ::write(fd_.fd(), &value, sizeof(value));
    if (ret == sizeof(value)) return true;
    if (ret < 0 && errno == EINTR) continue;
    if (ret < 0 && errno == EAGAIN) return true;  // Already signaled
    return false;
  }
}

auto AsyncEventFd::consume() noexcept -> std::uint64_t {
  std::uint64_t total = 0;
  std::uint64_t val;
  while (::read(fd_.fd(), &val, sizeof(val)) > 0) {
    total += val;
  }
  return total;
}

}  // namespace taskmaster::io
