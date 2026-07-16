#pragma once

#include "dagforge/core/error.hpp"

#include <filesystem>
#include <cstddef>
#include <string>
#include <vector>

namespace dagforge::workflow::storage_detail {

struct JsonCatalogFile {
  std::string key;
  std::string contents;
};

[[nodiscard]] auto load_json_catalog(const std::filesystem::path &directory,
                                     std::size_t max_file_bytes)
    -> Result<std::vector<JsonCatalogFile>>;

} // namespace dagforge::workflow::storage_detail
