#pragma once

#include "dagforge/core/error.hpp"

#include <filesystem>
#include <memory>

namespace dagforge::workflow {

class StorageDirectoryLock final {
public:
  ~StorageDirectoryLock();

  StorageDirectoryLock(const StorageDirectoryLock &) = delete;
  auto operator=(const StorageDirectoryLock &)
      -> StorageDirectoryLock & = delete;

  [[nodiscard]] static auto acquire(const std::filesystem::path &directory)
      -> Result<std::unique_ptr<StorageDirectoryLock>>;

private:
  explicit StorageDirectoryLock(int descriptor) noexcept;

  int descriptor_{-1};
};

} // namespace dagforge::workflow
