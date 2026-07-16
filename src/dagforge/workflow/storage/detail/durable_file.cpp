#include "durable_file.hpp"
#include "durable_file_testing.hpp"

#include <atomic>
#include <cerrno>
#include <cstring>
#include <cstdlib>
#include <fcntl.h>
#include <array>
#include <new>
#include <stdexcept>
#include <string>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

#include <sys/stat.h>
#include <unistd.h>

namespace dagforge::workflow::storage_detail {
namespace {

std::atomic<std::ptrdiff_t> directory_sync_failure_countdown{-1};

class FileDescriptor {
public:
  explicit FileDescriptor(int value = -1) noexcept : value_(value) {}

  FileDescriptor(const FileDescriptor &) = delete;
  auto operator=(const FileDescriptor &) -> FileDescriptor & = delete;

  FileDescriptor(FileDescriptor &&other) noexcept
      : value_(std::exchange(other.value_, -1)) {}

  ~FileDescriptor() { close_unchecked(); }

  [[nodiscard]] auto get() const noexcept -> int { return value_; }

  auto close() -> Result<void> {
    const auto value = std::exchange(value_, -1);
    if (value < 0 || ::close(value) == 0) {
      return ok();
    }
    return fail(std::error_code(errno, std::generic_category()));
  }

private:
  auto close_unchecked() noexcept -> void {
    if (value_ >= 0) {
      (void)::close(value_);
      value_ = -1;
    }
  }

  int value_{-1};
};

class TemporaryFile {
public:
  explicit TemporaryFile(std::filesystem::path path)
      : path_(std::move(path)) {}

  TemporaryFile(const TemporaryFile &) = delete;
  auto operator=(const TemporaryFile &) -> TemporaryFile & = delete;

  ~TemporaryFile() {
    if (active_) {
      std::error_code ignored;
      std::filesystem::remove(path_, ignored);
    }
  }

  [[nodiscard]] auto path() const noexcept -> const std::filesystem::path & {
    return path_;
  }

  auto release() noexcept -> void { active_ = false; }

private:
  std::filesystem::path path_;
  bool active_{true};
};

[[nodiscard]] auto parent_directory(const std::filesystem::path &path)
    -> std::filesystem::path {
  return path.parent_path().empty() ? std::filesystem::path{"."}
                                    : path.parent_path();
}

[[nodiscard]] auto errno_error() -> std::error_code {
  return std::error_code(errno, std::generic_category());
}

[[nodiscard]] auto open_error() -> std::error_code {
  return errno == ENOENT ? make_error_code(Error::NotFound) : errno_error();
}

auto write_all(int descriptor, std::span<const std::byte> data)
    -> Result<void> {
  std::size_t written = 0;
  while (written < data.size()) {
    const auto result = ::write(descriptor, data.data() + written,
                                data.size() - written);
    if (result > 0) {
      written += static_cast<std::size_t>(result);
      continue;
    }
    if (result < 0 && errno == EINTR) {
      continue;
    }
    return result == 0 ? fail(Error::Incomplete) : fail(errno_error());
  }
  return ok();
}

auto sync_descriptor(int descriptor) -> Result<void> {
  while (::fsync(descriptor) != 0) {
    if (errno != EINTR) {
      return fail(errno_error());
    }
  }
  return ok();
}

auto sync_directory(const std::filesystem::path &directory) -> Result<void> {
  auto remaining =
      directory_sync_failure_countdown.load(std::memory_order_relaxed);
  while (remaining >= 0) {
    if (remaining == 0) {
      if (directory_sync_failure_countdown.compare_exchange_weak(
              remaining, -1, std::memory_order_relaxed)) {
        return fail(Error::PersistenceError);
      }
      continue;
    }
    if (directory_sync_failure_countdown.compare_exchange_weak(
            remaining, remaining - 1, std::memory_order_relaxed)) {
      break;
    }
  }
  FileDescriptor descriptor{
      ::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC)};
  if (descriptor.get() < 0) {
    return fail(errno_error());
  }
  auto synced = sync_descriptor(descriptor.get());
  if (!synced) {
    return synced;
  }
  (void)descriptor.close();
  return ok();
}

auto ensure_directory_durable_impl(const std::filesystem::path &directory)
    -> Result<void> {
  struct stat metadata {};
  if (::lstat(directory.c_str(), &metadata) == 0) {
    return S_ISDIR(metadata.st_mode) ? ok() : fail(Error::InvalidState);
  }
  if (errno != ENOENT) {
    return fail(errno_error());
  }

  const auto parent = parent_directory(directory);
  if (parent != directory) {
    auto parent_ready = ensure_directory_durable_impl(parent);
    if (!parent_ready) {
      return parent_ready;
    }
  }
  if (::mkdir(directory.c_str(), S_IRWXU) != 0) {
    if (errno != EEXIST) {
      return fail(errno_error());
    }
    if (::lstat(directory.c_str(), &metadata) != 0 ||
        !S_ISDIR(metadata.st_mode)) {
      return fail(Error::InvalidState);
    }
    return ok();
  }
  return sync_directory(parent);
}

auto regular_file_metadata(int descriptor) -> Result<struct stat> {
  struct stat metadata {};
  if (::fstat(descriptor, &metadata) != 0) {
    return fail(errno_error());
  }
  return S_ISREG(metadata.st_mode) ? ok(metadata) : fail(Error::InvalidState);
}

auto truncate_descriptor(int descriptor, off_t size) -> Result<void> {
  while (::ftruncate(descriptor, size) != 0) {
    if (errno != EINTR) {
      return fail(errno_error());
    }
  }
  return ok();
}

template <typename Contents>
auto read_all(int descriptor, std::size_t max_bytes,
              std::size_t known_size) -> Result<Contents> {
  if (max_bytes == 0 || known_size > max_bytes) {
    return fail(max_bytes == 0 ? Error::InvalidArgument
                               : Error::ResourceExhausted);
  }
  Contents contents;
  std::array<std::byte, 16 * 1024> buffer{};
  try {
    contents.reserve(known_size);
    while (true) {
      const auto result = ::read(descriptor, buffer.data(), buffer.size());
      if (result > 0) {
        const auto count = static_cast<std::size_t>(result);
        if (count > max_bytes - contents.size()) {
          return fail(Error::ResourceExhausted);
        }
        if constexpr (std::is_same_v<Contents, std::string>) {
          contents.append(reinterpret_cast<const char *>(buffer.data()), count);
        } else {
          contents.insert(contents.end(), buffer.begin(),
                          buffer.begin() + count);
        }
        continue;
      }
      if (result == 0) {
        return ok(std::move(contents));
      }
      if (errno != EINTR) {
        return fail(errno_error());
      }
    }
  } catch (const std::bad_alloc &) {
    return fail(Error::ResourceExhausted);
  } catch (const std::length_error &) {
    return fail(Error::ResourceExhausted);
  }
}

[[nodiscard]] auto create_temporary_file(const std::filesystem::path &path)
    -> Result<std::pair<FileDescriptor, std::filesystem::path>> {
  auto pattern = path.string() + ".tmp.XXXXXX";
  std::vector<char> writable(pattern.begin(), pattern.end());
  writable.push_back('\0');
  const auto descriptor = ::mkstemp(writable.data());
  if (descriptor < 0) {
    return fail(errno_error());
  }
  FileDescriptor file{descriptor};
  if (::fcntl(file.get(), F_SETFD, FD_CLOEXEC) != 0) {
    const auto error = std::error_code(errno, std::generic_category());
    const std::filesystem::path temporary{writable.data()};
    (void)file.close();
    std::error_code ignored;
    std::filesystem::remove(temporary, ignored);
    return fail(error);
  }
  return ok(std::pair{std::move(file),
                      std::filesystem::path{writable.data()}});
}

} // namespace

namespace testing {

auto fail_next_directory_sync() noexcept -> void {
  directory_sync_failure_countdown.store(0, std::memory_order_relaxed);
}

auto fail_directory_sync_after(std::size_t successful_syncs) noexcept -> void {
  directory_sync_failure_countdown.store(
      static_cast<std::ptrdiff_t>(successful_syncs),
      std::memory_order_relaxed);
}

} // namespace testing

auto ensure_directory_durable(const std::filesystem::path &directory)
    -> Result<void> {
  return ensure_directory_durable_impl(directory);
}

auto valid_storage_key(std::string_view value) noexcept -> bool {
  return !value.empty() && value != "." && value != ".." &&
         std::filesystem::path{value}.filename() == value;
}

auto load_file(const std::filesystem::path &path, std::size_t max_bytes)
    -> Result<std::vector<std::byte>> {
  if (max_bytes == 0) {
    return fail(Error::InvalidArgument);
  }
  FileDescriptor descriptor{
      ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW)};
  if (descriptor.get() < 0) {
    return fail(open_error());
  }
  auto metadata = regular_file_metadata(descriptor.get());
  if (!metadata) {
    return fail(metadata.error());
  }
  if (metadata->st_size < 0 ||
      static_cast<std::uintmax_t>(metadata->st_size) > max_bytes) {
    return fail(Error::ResourceExhausted);
  }
  return read_all<std::vector<std::byte>>(
      descriptor.get(), max_bytes, static_cast<std::size_t>(metadata->st_size));
}

auto load_text_file(const std::filesystem::path &path, std::size_t max_bytes)
    -> Result<std::string> {
  if (max_bytes == 0) {
    return fail(Error::InvalidArgument);
  }
  FileDescriptor descriptor{
      ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW)};
  if (descriptor.get() < 0) {
    return fail(open_error());
  }
  auto metadata = regular_file_metadata(descriptor.get());
  if (!metadata) {
    return fail(metadata.error());
  }
  if (metadata->st_size < 0 ||
      static_cast<std::uintmax_t>(metadata->st_size) > max_bytes) {
    return fail(Error::ResourceExhausted);
  }
  return read_all<std::string>(descriptor.get(), max_bytes,
                               static_cast<std::size_t>(metadata->st_size));
}

auto store_file_atomic(const std::filesystem::path &path,
                       std::span<const std::byte> data)
    -> Result<DurableWriteResult> {
  const auto directory = parent_directory(path);
  auto directory_ready = ensure_directory_durable(directory);
  if (!directory_ready) {
    return fail(directory_ready.error());
  }

  auto created = create_temporary_file(path);
  if (!created) {
    return fail(created.error());
  }
  auto [descriptor, temporary_path] = std::move(*created);
  TemporaryFile temporary{std::move(temporary_path)};

  auto written = write_all(descriptor.get(), data);
  if (!written) {
    return fail(written.error());
  }
  auto contents_synced = sync_descriptor(descriptor.get());
  if (!contents_synced) {
    return fail(contents_synced.error());
  }
  auto closed = descriptor.close();
  if (!closed) {
    return fail(closed.error());
  }

  if (::rename(temporary.path().c_str(), path.c_str()) != 0) {
    return fail(errno_error());
  }
  temporary.release();
  auto directory_synced = sync_directory(directory);
  if (!directory_synced) {
    return ok(DurableWriteResult{
        .committed = true,
        .durability_error = directory_synced.error(),
    });
  }
  return ok(DurableWriteResult{.committed = true});
}

auto store_text_file_atomic(const std::filesystem::path &path,
                            std::string_view text)
    -> Result<DurableWriteResult> {
  return store_file_atomic(
      path, std::as_bytes(std::span{text.data(), text.size()}));
}

auto append_text_file_durable(const std::filesystem::path &path,
                              std::string_view text,
                              std::size_t max_file_bytes)
    -> Result<DurableWriteResult> {
  if (max_file_bytes == 0 || text.size() > max_file_bytes) {
    return fail(max_file_bytes == 0 ? Error::InvalidArgument
                                    : Error::ResourceExhausted);
  }
  const auto directory = parent_directory(path);
  auto directory_ready = ensure_directory_durable(directory);
  if (!directory_ready) {
    return fail(directory_ready.error());
  }

  struct stat metadata {};
  const bool existed = ::lstat(path.c_str(), &metadata) == 0;
  if (!existed && errno != ENOENT) {
    return fail(errno_error());
  }
  if (!existed) {
    return store_text_file_atomic(path, text);
  }
  if (existed && !S_ISREG(metadata.st_mode)) {
    return fail(Error::InvalidState);
  }

  FileDescriptor descriptor{
      ::open(path.c_str(), O_WRONLY | O_APPEND | O_CLOEXEC | O_NOFOLLOW)};
  if (descriptor.get() < 0) {
    return fail(open_error());
  }
  auto opened_metadata = regular_file_metadata(descriptor.get());
  if (!opened_metadata) {
    return fail(opened_metadata.error());
  }
  const auto original_size = opened_metadata->st_size;
  if (original_size < 0 ||
      static_cast<std::uintmax_t>(original_size) > max_file_bytes ||
      text.size() > max_file_bytes - static_cast<std::size_t>(original_size)) {
    return fail(Error::ResourceExhausted);
  }
  auto written = write_all(
      descriptor.get(), std::as_bytes(std::span{text.data(), text.size()}));
  if (!written) {
    if (existed) {
      auto truncated = truncate_descriptor(descriptor.get(), original_size);
      if (!truncated) {
        return fail(truncated.error());
      }
      auto rollback_synced = sync_descriptor(descriptor.get());
      if (!rollback_synced) {
        return fail(rollback_synced.error());
      }
      return fail(written.error());
    }
    auto closed = descriptor.close();
    if (!closed) {
      return fail(closed.error());
    }
    auto removed = remove_file_durable(path);
    return removed ? fail(written.error()) : fail(removed.error());
  }
  auto synced = sync_descriptor(descriptor.get());
  if (!synced) {
    auto truncated = truncate_descriptor(descriptor.get(), original_size);
    if (!truncated) {
      return fail(truncated.error());
    }
    auto rollback_synced = sync_descriptor(descriptor.get());
    return rollback_synced ? fail(synced.error()) : fail(rollback_synced.error());
  }
  (void)descriptor.close();
  return ok(DurableWriteResult{.committed = true});
}

auto remove_file_durable(const std::filesystem::path &path)
    -> Result<DurableRemoveResult> {
  const auto directory = parent_directory(path);
  if (::unlink(path.c_str()) != 0) {
    if (errno == ENOENT) {
      struct stat metadata {};
      if (::lstat(directory.c_str(), &metadata) != 0) {
        return errno == ENOENT ? ok(DurableRemoveResult{})
                               : fail(errno_error());
      }
      if (!S_ISDIR(metadata.st_mode)) {
        return fail(Error::InvalidState);
      }
      return ok(DurableRemoveResult{});
    }
    return fail(errno_error());
  }
  auto synced = sync_directory(directory);
  if (!synced) {
    return ok(DurableRemoveResult{
        .removed = true,
        .durability_error = synced.error(),
    });
  }
  return ok(DurableRemoveResult{.removed = true});
}

} // namespace dagforge::workflow::storage_detail
