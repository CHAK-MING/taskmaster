#pragma once

#include "dagforge/core/error.hpp"

#include <cstddef>
#include <filesystem>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

namespace dagforge::workflow::storage_detail {

struct DurableRemoveResult {
  bool removed{false};
  std::error_code durability_error;

  [[nodiscard]] auto durability_confirmed() const noexcept -> bool {
    return !durability_error;
  }
};

struct DurableWriteResult {
  bool committed{false};
  std::error_code durability_error;

  [[nodiscard]] auto durability_confirmed() const noexcept -> bool {
    return !durability_error;
  }
};

[[nodiscard]] auto valid_storage_key(std::string_view value) noexcept -> bool;

auto ensure_directory_durable(const std::filesystem::path &directory)
    -> Result<void>;

[[nodiscard]] auto load_text_file(const std::filesystem::path &path,
                                  std::size_t max_bytes)
    -> Result<std::string>;

[[nodiscard]] auto load_file(const std::filesystem::path &path,
                             std::size_t max_bytes)
    -> Result<std::vector<std::byte>>;

auto store_file_atomic(const std::filesystem::path &path,
                       std::span<const std::byte> data)
    -> Result<DurableWriteResult>;

auto store_text_file_atomic(const std::filesystem::path &path,
                            std::string_view text)
    -> Result<DurableWriteResult>;

auto append_text_file_durable(const std::filesystem::path &path,
                              std::string_view text,
                              std::size_t max_file_bytes)
    -> Result<DurableWriteResult>;

[[nodiscard]] auto remove_file_durable(const std::filesystem::path &path)
    -> Result<DurableRemoveResult>;

} // namespace dagforge::workflow::storage_detail
