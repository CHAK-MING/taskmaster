#include "dagforge/util/log.hpp"

#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace dagforge::log {

namespace {

struct FileCloser {
  auto operator()(FILE *file) const noexcept -> void {
    if (file != nullptr) {
      std::fclose(file);
    }
  }
};

using OwnedFile = std::unique_ptr<FILE, FileCloser>;

enum class QueueItemKind : std::uint8_t {
  Message,
  SetStdout,
  SetStderr,
  SetFile,
};

struct QueueItem {
  QueueItemKind kind{QueueItemKind::Message};
  std::string message;
  OwnedFile file;
};

} // namespace

struct Logger::Impl {
  static constexpr std::size_t kQueueCapacity = 8192;
  static constexpr std::size_t kBatchSize = 64;

  std::atomic<Level> level{Level::Info};
  std::atomic<std::uint64_t> dropped_messages{0};

  std::mutex queue_mutex;
  std::condition_variable queue_ready;
  std::condition_variable queue_space;
  std::deque<QueueItem> queue;
  bool running{false};
  bool accepting{false};
  std::jthread writer;

  std::mutex output_mutex;
  FILE *output{stdout};
  OwnedFile file;

  [[nodiscard]] auto should_drop_on_overflow() -> bool {
    std::lock_guard output_lock(output_mutex);
    if (output == nullptr) {
      return true;
    }
    const int fd = ::fileno(output);
    return fd < 0 || ::isatty(fd) == 0;
  }

  auto write_immediately(std::string_view message) -> void {
    std::lock_guard output_lock(output_mutex);
    FILE *destination = output != nullptr ? output : stdout;
    std::fwrite(message.data(), 1, message.size(), destination);
    std::fflush(destination);
  }

  auto switch_to_stdout() -> void {
    std::lock_guard output_lock(output_mutex);
    file.reset();
    output = stdout;
  }

  auto switch_to_stderr() -> void {
    std::lock_guard output_lock(output_mutex);
    file.reset();
    output = stderr;
  }

  auto switch_to_file(OwnedFile next_file) -> void {
    std::lock_guard output_lock(output_mutex);
    file = std::move(next_file);
    output = file.get();
  }

  [[nodiscard]] auto enqueue_control(QueueItem item) -> bool {
    std::unique_lock queue_lock(queue_mutex);
    queue_space.wait(queue_lock, [this] {
      return queue.size() < kQueueCapacity || !accepting;
    });
    if (!accepting) {
      return false;
    }
    queue.push_back(std::move(item));
    queue_lock.unlock();
    queue_ready.notify_one();
    return true;
  }

  auto process_batch(std::vector<QueueItem> &batch) -> void {
    std::lock_guard output_lock(output_mutex);
    for (auto &item : batch) {
      switch (item.kind) {
      case QueueItemKind::Message: {
        FILE *destination = output != nullptr ? output : stdout;
        std::fwrite(item.message.data(), 1, item.message.size(), destination);
        break;
      }
      case QueueItemKind::SetStdout:
        file.reset();
        output = stdout;
        break;
      case QueueItemKind::SetStderr:
        file.reset();
        output = stderr;
        break;
      case QueueItemKind::SetFile:
        file = std::move(item.file);
        output = file.get();
        break;
      }
    }
    FILE *destination = output != nullptr ? output : stdout;
    std::fflush(destination);
  }

  auto writer_loop() -> void {
    std::vector<QueueItem> batch;
    batch.reserve(kBatchSize);

    for (;;) {
      {
        std::unique_lock queue_lock(queue_mutex);
        queue_ready.wait(queue_lock,
                         [this] { return !queue.empty() || !running; });
        if (queue.empty() && !running) {
          break;
        }

        batch.clear();
        while (!queue.empty() && batch.size() < kBatchSize) {
          batch.push_back(std::move(queue.front()));
          queue.pop_front();
        }
      }
      queue_space.notify_all();
      process_batch(batch);
    }
  }
};

Logger::Logger() : impl_(std::make_unique<Impl>()) {}

Logger::~Logger() { stop(); }

auto Logger::start() -> void {
  std::lock_guard queue_lock(impl_->queue_mutex);
  if (impl_->running) {
    return;
  }
  impl_->running = true;
  impl_->accepting = true;
  impl_->writer = std::jthread([this] { impl_->writer_loop(); });
}

auto Logger::stop() -> void {
  {
    std::lock_guard queue_lock(impl_->queue_mutex);
    if (!impl_->running) {
      return;
    }
    impl_->accepting = false;
    impl_->running = false;
  }
  impl_->queue_ready.notify_all();
  impl_->queue_space.notify_all();
  if (impl_->writer.joinable()) {
    impl_->writer.join();
  }
}

auto Logger::set_level(Level level) noexcept -> void {
  impl_->level.store(level, std::memory_order_release);
}

auto Logger::set_output_stderr() -> void {
  {
    std::lock_guard queue_lock(impl_->queue_mutex);
    if (!impl_->accepting) {
      impl_->switch_to_stderr();
      return;
    }
  }
  if (!impl_->enqueue_control(
          QueueItem{.kind = QueueItemKind::SetStderr})) {
    impl_->switch_to_stderr();
  }
}

auto Logger::set_output_file(std::string_view path) -> bool {
  if (path.empty()) {
    {
      std::lock_guard queue_lock(impl_->queue_mutex);
      if (!impl_->accepting) {
        impl_->switch_to_stdout();
        return true;
      }
    }
    return impl_->enqueue_control(
        QueueItem{.kind = QueueItemKind::SetStdout});
  }

  OwnedFile next_file{std::fopen(std::string(path).c_str(), "a")};
  if (!next_file) {
    return false;
  }
  std::setvbuf(next_file.get(), nullptr, _IOLBF, 0);

  {
    std::lock_guard queue_lock(impl_->queue_mutex);
    if (!impl_->accepting) {
      impl_->switch_to_file(std::move(next_file));
      return true;
    }
  }
  return impl_->enqueue_control(QueueItem{
      .kind = QueueItemKind::SetFile,
      .file = std::move(next_file),
  });
}

auto Logger::level() const noexcept -> Level {
  return impl_->level.load(std::memory_order_acquire);
}

auto Logger::should_log(Level level) const noexcept -> bool {
  return static_cast<std::uint8_t>(level) >=
         static_cast<std::uint8_t>(impl_->level.load(std::memory_order_acquire));
}

auto Logger::enqueue(std::string message) -> void {
  std::unique_lock queue_lock(impl_->queue_mutex);
  if (!impl_->accepting) {
    queue_lock.unlock();
    impl_->write_immediately(message);
    return;
  }

  if (impl_->queue.size() >= Impl::kQueueCapacity) {
    if (impl_->should_drop_on_overflow()) {
      impl_->dropped_messages.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    impl_->queue_space.wait(queue_lock, [this] {
      return impl_->queue.size() < Impl::kQueueCapacity ||
             !impl_->accepting;
    });
    if (!impl_->accepting) {
      queue_lock.unlock();
      impl_->write_immediately(message);
      return;
    }
  }

  impl_->queue.push_back(QueueItem{
      .kind = QueueItemKind::Message,
      .message = std::move(message),
  });
  queue_lock.unlock();
  impl_->queue_ready.notify_one();
}

Logger &logger() {
  static Logger instance;
  return instance;
}

} // namespace dagforge::log
