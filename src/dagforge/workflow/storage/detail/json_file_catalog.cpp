#include "json_file_catalog.hpp"

#include "durable_file.hpp"

#include <filesystem>
#include <string>
#include <utility>
#include <vector>

namespace dagforge::workflow::storage_detail {

auto load_json_catalog(const std::filesystem::path &directory,
                       std::size_t max_file_bytes)
    -> Result<std::vector<JsonCatalogFile>> {
  std::vector<JsonCatalogFile> files;
  std::error_code error;
  const bool exists = std::filesystem::exists(directory, error);
  if (error) {
    return fail(error);
  }
  if (!exists) {
    return ok(std::move(files));
  }
  const bool is_directory = std::filesystem::is_directory(directory, error);
  if (error) {
    return fail(error);
  }
  if (!is_directory) {
    return fail(Error::InvalidState);
  }

  for (std::filesystem::directory_iterator it(directory, error), end;
       !error && it != end; it.increment(error)) {
    if (it->path().extension() != ".json") {
      continue;
    }
    std::error_code type_error;
    const bool regular = it->is_regular_file(type_error);
    if (type_error) {
      return fail(type_error);
    }
    if (!regular) {
      return fail(Error::InvalidState);
    }
    auto key = it->path().stem().string();
    if (!valid_storage_key(key)) {
      return fail(Error::ParseError);
    }
    auto contents = load_text_file(it->path(), max_file_bytes);
    if (!contents) {
      return fail(contents.error());
    }
    files.push_back(JsonCatalogFile{
        .key = std::move(key),
        .contents = std::move(*contents),
    });
  }
  if (error) {
    return fail(error);
  }
  return ok(std::move(files));
}

} // namespace dagforge::workflow::storage_detail
