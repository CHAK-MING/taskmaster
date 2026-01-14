#include <benchmark/benchmark.h>
#include <liburing.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <array>
#include <cstdint>

namespace {

// ---- io_uring NOP throughput ----

void BM_IoUring_NOP(benchmark::State& state) {
  io_uring ring{};
  if (io_uring_queue_init(256, &ring, 0) < 0) {
    state.SkipWithError("io_uring_queue_init failed");
    return;
  }

  for (auto _ : state) {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring);
    io_uring_prep_nop(sqe);
    io_uring_submit(&ring);

    io_uring_cqe* cqe = nullptr;
    io_uring_wait_cqe(&ring, &cqe);
    io_uring_cqe_seen(&ring, cqe);
  }

  io_uring_queue_exit(&ring);
  state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_IoUring_NOP);

void BM_IoUring_NOP_Batched(benchmark::State& state) {
  const int batch_size = static_cast<int>(state.range(0));
  io_uring ring{};
  if (io_uring_queue_init(static_cast<unsigned>(batch_size * 2), &ring, 0) < 0) {
    state.SkipWithError("io_uring_queue_init failed");
    return;
  }

  for (auto _ : state) {
    for (int i = 0; i < batch_size; ++i) {
      io_uring_sqe* sqe = io_uring_get_sqe(&ring);
      io_uring_prep_nop(sqe);
    }
    io_uring_submit(&ring);

    for (int i = 0; i < batch_size; ++i) {
      io_uring_cqe* cqe = nullptr;
      io_uring_wait_cqe(&ring, &cqe);
      io_uring_cqe_seen(&ring, cqe);
    }
  }

  io_uring_queue_exit(&ring);
  state.SetItemsProcessed(state.iterations() * batch_size);
}
BENCHMARK(BM_IoUring_NOP_Batched)->Arg(8)->Arg(32)->Arg(128);

// ---- io_uring vs epoll: eventfd notification ----

void BM_IoUring_EventFd_Read(benchmark::State& state) {
  io_uring ring{};
  if (io_uring_queue_init(32, &ring, 0) < 0) {
    state.SkipWithError("io_uring_queue_init failed");
    return;
  }

  int efd = eventfd(0, EFD_NONBLOCK);
  if (efd < 0) {
    io_uring_queue_exit(&ring);
    state.SkipWithError("eventfd failed");
    return;
  }

  uint64_t write_val = 1;
  uint64_t read_buf = 0;

  for (auto _ : state) {
    ::write(efd, &write_val, sizeof(write_val));

    io_uring_sqe* sqe = io_uring_get_sqe(&ring);
    io_uring_prep_read(sqe, efd, &read_buf, sizeof(read_buf), 0);
    io_uring_submit(&ring);

    io_uring_cqe* cqe = nullptr;
    io_uring_wait_cqe(&ring, &cqe);
    benchmark::DoNotOptimize(read_buf);
    io_uring_cqe_seen(&ring, cqe);
  }

  close(efd);
  io_uring_queue_exit(&ring);
  state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_IoUring_EventFd_Read);

void BM_Epoll_EventFd_Read(benchmark::State& state) {
  int epfd = epoll_create1(0);
  if (epfd < 0) {
    state.SkipWithError("epoll_create1 failed");
    return;
  }

  int efd = eventfd(0, EFD_NONBLOCK);
  if (efd < 0) {
    close(epfd);
    state.SkipWithError("eventfd failed");
    return;
  }

  epoll_event ev{};
  ev.events = EPOLLIN;
  ev.data.fd = efd;
  epoll_ctl(epfd, EPOLL_CTL_ADD, efd, &ev);

  uint64_t write_val = 1;
  uint64_t read_buf = 0;
  std::array<epoll_event, 1> events{};

  for (auto _ : state) {
    ::write(efd, &write_val, sizeof(write_val));
    epoll_wait(epfd, events.data(), 1, -1);
    ::read(efd, &read_buf, sizeof(read_buf));
    benchmark::DoNotOptimize(read_buf);
  }

  close(efd);
  close(epfd);
  state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Epoll_EventFd_Read);

// ---- io_uring vs epoll: pipe throughput ----

void BM_IoUring_Pipe_Throughput(benchmark::State& state) {
  io_uring ring{};
  if (io_uring_queue_init(32, &ring, 0) < 0) {
    state.SkipWithError("io_uring_queue_init failed");
    return;
  }

  int pipefd[2];
  if (pipe(pipefd) < 0) {
    io_uring_queue_exit(&ring);
    state.SkipWithError("pipe failed");
    return;
  }

  constexpr size_t kBufSize = 4096;
  std::array<char, kBufSize> write_buf{};
  std::array<char, kBufSize> read_buf{};
  std::fill(write_buf.begin(), write_buf.end(), 'x');

  int64_t bytes_transferred = 0;

  for (auto _ : state) {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring);
    io_uring_prep_write(sqe, pipefd[1], write_buf.data(), kBufSize, 0);
    io_uring_submit(&ring);

    io_uring_cqe* cqe = nullptr;
    io_uring_wait_cqe(&ring, &cqe);
    io_uring_cqe_seen(&ring, cqe);

    sqe = io_uring_get_sqe(&ring);
    io_uring_prep_read(sqe, pipefd[0], read_buf.data(), kBufSize, 0);
    io_uring_submit(&ring);

    io_uring_wait_cqe(&ring, &cqe);
    bytes_transferred += cqe->res;
    io_uring_cqe_seen(&ring, cqe);
  }

  close(pipefd[0]);
  close(pipefd[1]);
  io_uring_queue_exit(&ring);

  state.SetBytesProcessed(bytes_transferred);
  state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_IoUring_Pipe_Throughput);

void BM_Epoll_Pipe_Throughput(benchmark::State& state) {
  int epfd = epoll_create1(0);
  if (epfd < 0) {
    state.SkipWithError("epoll_create1 failed");
    return;
  }

  int pipefd[2];
  if (pipe(pipefd) < 0) {
    close(epfd);
    state.SkipWithError("pipe failed");
    return;
  }

  epoll_event ev{};
  ev.events = EPOLLIN;
  ev.data.fd = pipefd[0];
  epoll_ctl(epfd, EPOLL_CTL_ADD, pipefd[0], &ev);

  constexpr size_t kBufSize = 4096;
  std::array<char, kBufSize> write_buf{};
  std::array<char, kBufSize> read_buf{};
  std::fill(write_buf.begin(), write_buf.end(), 'x');
  std::array<epoll_event, 1> events{};

  int64_t bytes_transferred = 0;

  for (auto _ : state) {
    ::write(pipefd[1], write_buf.data(), kBufSize);
    epoll_wait(epfd, events.data(), 1, -1);
    auto n = ::read(pipefd[0], read_buf.data(), kBufSize);
    bytes_transferred += n;
  }

  close(pipefd[0]);
  close(pipefd[1]);
  close(epfd);

  state.SetBytesProcessed(bytes_transferred);
  state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Epoll_Pipe_Throughput);

// ---- io_uring linked operations (SQE chaining) ----

void BM_IoUring_Linked_WriteRead(benchmark::State& state) {
  io_uring ring{};
  if (io_uring_queue_init(32, &ring, 0) < 0) {
    state.SkipWithError("io_uring_queue_init failed");
    return;
  }

  int pipefd[2];
  if (pipe(pipefd) < 0) {
    io_uring_queue_exit(&ring);
    state.SkipWithError("pipe failed");
    return;
  }

  constexpr size_t kBufSize = 64;
  std::array<char, kBufSize> write_buf{};
  std::array<char, kBufSize> read_buf{};

  for (auto _ : state) {
    io_uring_sqe* sqe1 = io_uring_get_sqe(&ring);
    io_uring_prep_write(sqe1, pipefd[1], write_buf.data(), kBufSize, 0);
    sqe1->flags |= IOSQE_IO_LINK;

    io_uring_sqe* sqe2 = io_uring_get_sqe(&ring);
    io_uring_prep_read(sqe2, pipefd[0], read_buf.data(), kBufSize, 0);

    io_uring_submit(&ring);

    io_uring_cqe* cqe = nullptr;
    io_uring_wait_cqe(&ring, &cqe);
    io_uring_cqe_seen(&ring, cqe);
    io_uring_wait_cqe(&ring, &cqe);
    io_uring_cqe_seen(&ring, cqe);
  }

  close(pipefd[0]);
  close(pipefd[1]);
  io_uring_queue_exit(&ring);
  state.SetItemsProcessed(state.iterations() * 2);
}
BENCHMARK(BM_IoUring_Linked_WriteRead);

// ---- Baseline: raw syscall overhead ----

void BM_Syscall_Read_EventFd(benchmark::State& state) {
  int efd = eventfd(0, EFD_NONBLOCK | EFD_SEMAPHORE);
  if (efd < 0) {
    state.SkipWithError("eventfd failed");
    return;
  }

  uint64_t write_val = 1;
  uint64_t read_buf = 0;

  for (auto _ : state) {
    ::write(efd, &write_val, sizeof(write_val));
    ::read(efd, &read_buf, sizeof(read_buf));
    benchmark::DoNotOptimize(read_buf);
  }

  close(efd);
  state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Syscall_Read_EventFd);

}  // namespace
