#include "dagforge/workflow/artifact_store.hpp"

#include "detail/storage_codec.hpp"

#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string_view>
#include <utility>
#include <vector>

namespace dagforge::workflow {
namespace {

[[nodiscard]] auto valid_storage_key(std::string_view value) -> bool {
  return !value.empty() && value != "." && value != ".." &&
         std::filesystem::path{value}.filename() == value;
}

} // namespace

auto InMemoryArtifactStore::put(std::span<const std::byte> data,
                                std::string media_type)
    -> Result<ArtifactRef> {
  auto digest = storage_detail::compute_digest(data);
  if (!digest) {
    return fail(digest.error());
  }

  ArtifactBlob blob;
  blob.ref = ArtifactRef{
      .artifact_id = generate_artifact_id(),
      .media_type = std::move(media_type),
      .size_bytes = static_cast<std::uint64_t>(data.size()),
      .digest = std::move(*digest),
  };
  blob.data.assign(data.begin(), data.end());

  auto ref = blob.ref;
  std::lock_guard lock(mutex_);
  artifacts_.emplace(ref.artifact_id.str(), std::move(blob));
  return ok(std::move(ref));
}

auto InMemoryArtifactStore::get(const ArtifactId &artifact_id) const
    -> Result<ArtifactBlob> {
  if (!valid_storage_key(artifact_id.str())) {
    return fail(Error::InvalidArgument);
  }
  std::lock_guard lock(mutex_);
  const auto it = artifacts_.find(artifact_id.str());
  if (it == artifacts_.end()) {
    return fail(Error::NotFound);
  }
  return ok(it->second);
}

auto InMemoryArtifactStore::erase(const ArtifactId &artifact_id)
    -> Result<void> {
  if (!valid_storage_key(artifact_id.str())) {
    return fail(Error::InvalidArgument);
  }
  std::lock_guard lock(mutex_);
  if (artifacts_.erase(artifact_id.str()) == 0) {
    return fail(Error::NotFound);
  }
  return ok();
}

auto InMemoryArtifactStore::size() const -> std::size_t {
  std::lock_guard lock(mutex_);
  return artifacts_.size();
}

FileArtifactStore::FileArtifactStore(std::filesystem::path directory)
    : directory_(std::move(directory)) {
  std::error_code error;
  std::filesystem::create_directories(directory_, error);
}

auto FileArtifactStore::put(std::span<const std::byte> data,
                            std::string media_type)
    -> Result<ArtifactRef> {
  auto digest = storage_detail::compute_digest(data);
  if (!digest) {
    return fail(digest.error());
  }
  ArtifactRef ref{
      .artifact_id = generate_artifact_id(),
      .media_type = std::move(media_type),
      .size_bytes = static_cast<std::uint64_t>(data.size()),
      .digest = std::move(*digest),
  };
  auto encoded = storage_detail::encode_artifact_metadata(ref);
  if (!encoded) {
    return fail(encoded.error());
  }

  std::lock_guard lock(mutex_);
  std::error_code error;
  std::filesystem::create_directories(directory_, error);
  if (error) {
    return fail(error);
  }
  const auto base = directory_ / ref.artifact_id.str();
  const auto data_path = base.string() + ".bin";
  const auto temporary = base.string() + ".bin.tmp";
  {
    std::ofstream output(temporary, std::ios::binary | std::ios::trunc);
    if (!output) {
      return fail(Error::Unknown);
    }
    output.write(reinterpret_cast<const char *>(data.data()),
                 static_cast<std::streamsize>(data.size()));
    output.flush();
    if (!output) {
      return fail(Error::Unknown);
    }
  }
  std::filesystem::rename(temporary, data_path, error);
  if (error) {
    std::filesystem::remove(temporary, error);
    return fail(error);
  }
  auto metadata_result =
      storage_detail::store_text_file_atomic(base.string() + ".json", *encoded);
  if (!metadata_result) {
    std::filesystem::remove(data_path, error);
    return fail(metadata_result.error());
  }
  return ok(std::move(ref));
}

auto FileArtifactStore::get(const ArtifactId &artifact_id) const
    -> Result<ArtifactBlob> {
  if (!valid_storage_key(artifact_id.str())) {
    return fail(Error::InvalidArgument);
  }
  std::lock_guard lock(mutex_);
  const auto base = directory_ / artifact_id.str();
  auto metadata_text = storage_detail::load_text_file(base.string() + ".json");
  if (!metadata_text) {
    return fail(metadata_text.error());
  }
  auto metadata = storage_detail::decode_artifact_metadata(*metadata_text);
  if (!metadata || metadata->artifact_id != artifact_id) {
    return fail(metadata ? Error::ParseError : metadata.error());
  }
  std::ifstream input(base.string() + ".bin", std::ios::binary);
  if (!input) {
    return fail(Error::NotFound);
  }
  std::vector<char> bytes(std::istreambuf_iterator<char>(input), {});
  ArtifactBlob blob{
      .ref = ArtifactRef{
          .artifact_id = artifact_id.clone(),
          .media_type = std::move(metadata->media_type),
          .size_bytes = metadata->size_bytes,
          .digest = std::move(metadata->digest),
      },
  };
  blob.data.resize(bytes.size());
  std::memcpy(blob.data.data(), bytes.data(), bytes.size());
  auto digest = storage_detail::compute_digest(blob.data);
  if (!digest || *digest != blob.ref.digest ||
      blob.data.size() != blob.ref.size_bytes) {
    return fail(Error::ProtocolError);
  }
  return ok(std::move(blob));
}

auto FileArtifactStore::erase(const ArtifactId &artifact_id) -> Result<void> {
  if (!valid_storage_key(artifact_id.str())) {
    return fail(Error::InvalidArgument);
  }
  std::lock_guard lock(mutex_);
  const auto base = directory_ / artifact_id.str();
  std::error_code data_error;
  const auto removed_data =
      std::filesystem::remove(base.string() + ".bin", data_error);
  std::error_code metadata_error;
  const auto removed_metadata =
      std::filesystem::remove(base.string() + ".json", metadata_error);
  if (data_error)
    return fail(data_error);
  if (metadata_error)
    return fail(metadata_error);
  return removed_data || removed_metadata ? ok() : fail(Error::NotFound);
}

} // namespace dagforge::workflow
