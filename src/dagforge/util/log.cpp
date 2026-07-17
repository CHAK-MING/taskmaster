#include "dagforge/util/log.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <cstdio>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <system_error>
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
  SetSink,
  Flush,
};

struct QueueItem {
  QueueItemKind kind{QueueItemKind::Message};
  Record record;
  OwnedFile file;
  std::shared_ptr<Sink> sink;
  std::shared_ptr<std::promise<std::error_code>> completion;
};

[[nodiscard]] auto errno_code() noexcept -> std::error_code {
  return {errno != 0 ? errno : EIO, std::generic_category()};
}

} // namespace

struct Logger::Impl {
  explicit Impl(LoggerOptions configured)
      : queue_capacity(std::max<std::size_t>(configured.queue_capacity, 1)),
        batch_size(std::max<std::size_t>(configured.batch_size, 1)),
        color_policy(configured.color_policy),
        overflow_policy(configured.overflow_policy) {}

  const std::size_t queue_capacity;
  const std::size_t batch_size;
  std::atomic<Level> level{Level::Info};
  std::atomic<ColorPolicy> color_policy{ColorPolicy::Auto};
  std::atomic<OverflowPolicy> overflow_policy{OverflowPolicy::DropNewest};
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
  std::shared_ptr<Sink> sink;
  std::error_code sink_error;

  [[nodiscard]] auto should_color_locked(FILE *destination) const noexcept
      -> bool {
    if (file && destination == file.get()) {
      return false;
    }
    if (sink) {
      return color_policy.load(std::memory_order_acquire) !=
                 ColorPolicy::Never &&
             sink->supports_color();
    }
    switch (color_policy.load(std::memory_order_acquire)) {
    case ColorPolicy::Always:
      return true;
    case ColorPolicy::Never:
      return false;
    case ColorPolicy::Auto:
      break;
    }
    if (destination == nullptr) {
      return false;
    }
    const int descriptor = ::fileno(destination);
    return descriptor >= 0 && ::isatty(descriptor) != 0;
  }

  [[nodiscard]] auto format_record(const Record &record, bool color) const
      -> std::string {
    const auto level = color
                           ? std::format("{}{}{}", level_color(record.level),
                                         level_name(record.level), "\o{33}[0m")
                           : std::string{level_name(record.level)};
    return std::format("[{}] [{}] [{}] {}\n",
                       util::format_local_timestamp(record.timestamp), level,
                       record.thread_id, record.message);
  }

  auto record_write_failure() noexcept -> void {
    if (!sink_error) {
      sink_error = errno_code();
    }
  }

  auto write_record_locked(const Record &record) -> void {
    FILE *destination = output != nullptr ? output : stdout;
    const auto message =
        format_record(record, should_color_locked(destination));
    if (sink) {
      auto written = sink->write(record, message);
      if (!written && !sink_error) {
        sink_error = written.error();
      }
      return;
    }
    if (std::fwrite(message.data(), 1, message.size(), destination) !=
        message.size()) {
      record_write_failure();
    }
  }

  [[nodiscard]] auto flush_locked() noexcept -> std::error_code {
    if (sink) {
      auto flushed = sink->flush();
      if (!flushed && !sink_error) {
        sink_error = flushed.error();
      }
      return sink_error;
    }
    FILE *destination = output != nullptr ? output : stdout;
    if (std::fflush(destination) != 0) {
      record_write_failure();
    }
    return sink_error;
  }

  auto write_immediately(const Record &record) -> void {
    std::lock_guard output_lock(output_mutex);
    write_record_locked(record);
    (void)flush_locked();
  }

  [[nodiscard]] auto flush_immediately() -> std::error_code {
    std::lock_guard output_lock(output_mutex);
    return flush_locked();
  }

  auto switch_to_stdout() -> void {
    std::lock_guard output_lock(output_mutex);
    file.reset();
    sink.reset();
    output = stdout;
    sink_error.clear();
  }

  auto switch_to_stderr() -> void {
    std::lock_guard output_lock(output_mutex);
    file.reset();
    sink.reset();
    output = stderr;
    sink_error.clear();
  }

  auto switch_to_file(OwnedFile next_file) -> void {
    std::lock_guard output_lock(output_mutex);
    file = std::move(next_file);
    sink.reset();
    output = file.get();
    sink_error.clear();
  }

  auto switch_to_sink(std::shared_ptr<Sink> next_sink) -> void {
    std::lock_guard output_lock(output_mutex);
    file.reset();
    output = nullptr;
    sink = std::move(next_sink);
    sink_error.clear();
  }

  [[nodiscard]] auto enqueue_control(QueueItem &&item) -> bool {
    std::unique_lock queue_lock(queue_mutex);
    queue_space.wait(queue_lock, [this] {
      return queue.size() < queue_capacity || !accepting;
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
      case QueueItemKind::Message:
        write_record_locked(item.record);
        break;
      case QueueItemKind::SetStdout:
        file.reset();
        sink.reset();
        output = stdout;
        sink_error.clear();
        break;
      case QueueItemKind::SetStderr:
        file.reset();
        sink.reset();
        output = stderr;
        sink_error.clear();
        break;
      case QueueItemKind::SetFile:
        file = std::move(item.file);
        sink.reset();
        output = file.get();
        sink_error.clear();
        break;
      case QueueItemKind::SetSink:
        file.reset();
        output = nullptr;
        sink = std::move(item.sink);
        sink_error.clear();
        break;
      case QueueItemKind::Flush:
        item.completion->set_value(flush_locked());
        break;
      }
    }
    (void)flush_locked();
  }

  auto writer_loop() -> void {
    std::vector<QueueItem> batch;
    batch.reserve(batch_size);

    for (;;) {
      {
        std::unique_lock queue_lock(queue_mutex);
        queue_ready.wait(queue_lock,
                         [this] { return !queue.empty() || !running; });
        if (queue.empty() && !running) {
          break;
        }

        batch.clear();
        while (!queue.empty() && batch.size() < batch_size) {
          batch.push_back(std::move(queue.front()));
          queue.pop_front();
        }
      }
      queue_space.notify_all();
      process_batch(batch);
    }
  }
};

Logger::Logger(LoggerOptions options)
    : impl_(std::make_unique<Impl>(options)) {}

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

auto Logger::set_color_policy(ColorPolicy policy) noexcept -> void {
  impl_->color_policy.store(policy, std::memory_order_release);
}

auto Logger::set_overflow_policy(OverflowPolicy policy) noexcept -> void {
  impl_->overflow_policy.store(policy, std::memory_order_release);
}

auto Logger::set_output_stderr() -> void {
  {
    std::lock_guard queue_lock(impl_->queue_mutex);
    if (!impl_->accepting) {
      impl_->switch_to_stderr();
      return;
    }
  }
  if (!impl_->enqueue_control(QueueItem{.kind = QueueItemKind::SetStderr})) {
    impl_->switch_to_stderr();
  }
}

auto Logger::set_sink(std::shared_ptr<Sink> sink) -> Result<void> {
  if (!sink) {
    return fail(Error::InvalidArgument);
  }
  {
    std::lock_guard queue_lock(impl_->queue_mutex);
    if (!impl_->accepting) {
      impl_->switch_to_sink(std::move(sink));
      return ok();
    }
  }
  QueueItem control{
      .kind = QueueItemKind::SetSink,
      .sink = std::move(sink),
  };
  if (!impl_->enqueue_control(std::move(control))) {
    impl_->switch_to_sink(std::move(control.sink));
  }
  return ok();
}

auto Logger::set_output_file(std::string_view path) -> Result<void> {
  if (path.empty()) {
    {
      std::lock_guard queue_lock(impl_->queue_mutex);
      if (!impl_->accepting) {
        impl_->switch_to_stdout();
        return ok();
      }
    }
    if (!impl_->enqueue_control(QueueItem{.kind = QueueItemKind::SetStdout})) {
      impl_->switch_to_stdout();
    }
    return ok();
  }

  errno = 0;
  OwnedFile next_file{std::fopen(std::string(path).c_str(), "a")};
  if (!next_file) {
    return fail(errno_code());
  }
  if (std::setvbuf(next_file.get(), nullptr, _IOLBF, 0) != 0) {
    return fail(errno_code());
  }

  {
    std::lock_guard queue_lock(impl_->queue_mutex);
    if (!impl_->accepting) {
      impl_->switch_to_file(std::move(next_file));
      return ok();
    }
  }
  QueueItem control{
      .kind = QueueItemKind::SetFile,
      .file = std::move(next_file),
  };
  if (!impl_->enqueue_control(std::move(control))) {
    impl_->switch_to_file(std::move(control.file));
  }
  return ok();
}

auto Logger::flush() -> Result<void> {
  auto completion = std::make_shared<std::promise<std::error_code>>();
  auto result = completion->get_future();
  {
    std::lock_guard queue_lock(impl_->queue_mutex);
    if (!impl_->accepting) {
      const auto error = impl_->flush_immediately();
      return error ? fail(error) : ok();
    }
  }
  if (!impl_->enqueue_control(QueueItem{
          .kind = QueueItemKind::Flush,
          .completion = std::move(completion),
      })) {
    const auto error = impl_->flush_immediately();
    return error ? fail(error) : ok();
  }
  const auto error = result.get();
  return error ? fail(error) : ok();
}

auto Logger::level() const noexcept -> Level {
  return impl_->level.load(std::memory_order_acquire);
}

auto Logger::color_policy() const noexcept -> ColorPolicy {
  return impl_->color_policy.load(std::memory_order_acquire);
}

auto Logger::overflow_policy() const noexcept -> OverflowPolicy {
  return impl_->overflow_policy.load(std::memory_order_acquire);
}

auto Logger::dropped_messages() const noexcept -> std::uint64_t {
  return impl_->dropped_messages.load(std::memory_order_acquire);
}

auto Logger::should_log(Level level) const noexcept -> bool {
  return static_cast<std::uint8_t>(level) >=
         static_cast<std::uint8_t>(
             impl_->level.load(std::memory_order_acquire));
}

auto Logger::enqueue(Record record) -> void {
  std::unique_lock queue_lock(impl_->queue_mutex);
  if (!impl_->accepting) {
    queue_lock.unlock();
    impl_->write_immediately(record);
    return;
  }

  if (impl_->queue.size() >= impl_->queue_capacity) {
    if (impl_->overflow_policy.load(std::memory_order_acquire) ==
        OverflowPolicy::DropNewest) {
      impl_->dropped_messages.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    impl_->queue_space.wait(queue_lock, [this] {
      return impl_->queue.size() < impl_->queue_capacity || !impl_->accepting;
    });
    if (!impl_->accepting) {
      queue_lock.unlock();
      impl_->write_immediately(record);
      return;
    }
  }

  impl_->queue.push_back(QueueItem{
      .kind = QueueItemKind::Message,
      .record = std::move(record),
  });
  queue_lock.unlock();
  impl_->queue_ready.notify_one();
}

auto logger() -> Logger & {
  static Logger instance;
  return instance;
}

} // namespace dagforge::log
