#include "storage_directory_lock.hpp"

#include "durable_file.hpp"

#include <cerrno>
#include <fcntl.h>
#include <format>
#include <string>
#include <system_error>

#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

namespace dagforge::workflow {
namespace {

[[nodiscard]] auto system_error() -> std::error_code {
  return std::error_code(errno, std::system_category());
}

[[nodiscard]] auto trusted_owner(mode_t mode, uid_t owner) noexcept -> bool {
  return (mode & (S_IWGRP | S_IWOTH)) == 0 &&
         (owner == 0 || owner == ::geteuid());
}

auto write_owner(int descriptor) -> Result<void> {
  if (::ftruncate(descriptor, 0) != 0 || ::lseek(descriptor, 0, SEEK_SET) < 0) {
    return fail(system_error());
  }
  const auto owner = std::format("{}\n", static_cast<long long>(::getpid()));
  std::size_t written = 0;
  while (written < owner.size()) {
    const auto count =
        ::write(descriptor, owner.data() + written, owner.size() - written);
    if (count > 0) {
      written += static_cast<std::size_t>(count);
      continue;
    }
    if (count < 0 && errno == EINTR) {
      continue;
    }
    return fail(count == 0 ? Error::Incomplete : system_error());
  }
  while (::fsync(descriptor) != 0) {
    if (errno != EINTR) {
      return fail(system_error());
    }
  }
  return ok();
}

} // namespace

StorageDirectoryLock::StorageDirectoryLock(int descriptor) noexcept
    : descriptor_(descriptor) {}

StorageDirectoryLock::~StorageDirectoryLock() {
  if (descriptor_ < 0) {
    return;
  }
  (void)::flock(descriptor_, LOCK_UN);
  (void)::close(descriptor_);
}

auto StorageDirectoryLock::acquire(const std::filesystem::path &directory)
    -> Result<std::unique_ptr<StorageDirectoryLock>> {
  if (directory.empty()) {
    return fail(Error::InvalidArgument);
  }
  auto ready = storage_detail::ensure_directory_durable(directory);
  if (!ready) {
    return fail(ready.error());
  }
  struct stat directory_metadata {};
  if (::lstat(directory.c_str(), &directory_metadata) != 0) {
    return fail(system_error());
  }
  if (!S_ISDIR(directory_metadata.st_mode)) {
    return fail(Error::InvalidState);
  }
  if (!trusted_owner(directory_metadata.st_mode, directory_metadata.st_uid)) {
    return fail(Error::Unauthorized);
  }
  const auto path = directory / ".dagforge.lock";
  const auto descriptor = ::open(path.c_str(),
                                 O_CREAT | O_RDWR | O_CLOEXEC | O_NOFOLLOW,
                                 S_IRUSR | S_IWUSR);
  if (descriptor < 0) {
    return fail(system_error());
  }
  struct stat metadata {};
  if (::fstat(descriptor, &metadata) != 0) {
    const auto error = system_error();
    (void)::close(descriptor);
    return fail(error);
  }
  if (!S_ISREG(metadata.st_mode)) {
    (void)::close(descriptor);
    return fail(Error::InvalidState);
  }
  if (!trusted_owner(metadata.st_mode, metadata.st_uid)) {
    (void)::close(descriptor);
    return fail(Error::Unauthorized);
  }
  if (::flock(descriptor, LOCK_EX | LOCK_NB) != 0) {
    const auto error = errno == EWOULDBLOCK || errno == EAGAIN
                           ? make_error_code(Error::AlreadyExists)
                           : system_error();
    (void)::close(descriptor);
    return fail(error);
  }
  auto written = write_owner(descriptor);
  if (!written) {
    (void)::flock(descriptor, LOCK_UN);
    (void)::close(descriptor);
    return fail(written.error());
  }
  return ok(std::unique_ptr<StorageDirectoryLock>(
      new StorageDirectoryLock(descriptor)));
}

} // namespace dagforge::workflow
