#include "dagforge/workflow/artifact_store.hpp"

#include "dagforge/util/log.hpp"

#include "detail/durable_file.hpp"
#include "detail/storage_codec.hpp"

#include <algorithm>
#include <filesystem>
#include <map>
#include <ranges>
#include <string>
#include <utility>
#include <vector>

namespace dagforge::workflow {
namespace {

struct ArtifactFilePair {
  std::filesystem::path data_path;
  std::filesystem::path metadata_path;
  bool data_present{false};
  bool metadata_present{false};
  bool data_regular{false};
  bool metadata_regular{false};
};

auto append_reconciliation_entry(ArtifactReconciliationReport &report,
                                 std::string storage_key,
                                 ArtifactReconciliationState state) -> void {
  report.entries.push_back(ArtifactReconciliationEntry{
      .storage_key = std::move(storage_key),
      .state = state,
  });
}

} // namespace

auto ArtifactReconciliationReport::count(
    ArtifactReconciliationState state) const noexcept -> std::size_t {
  return static_cast<std::size_t>(std::ranges::count(entries, state,
                                                     &ArtifactReconciliationEntry::state));
}

auto ArtifactReconciliationReport::clean() const noexcept -> bool {
  return std::ranges::all_of(entries, [](const auto &entry) {
    return entry.state == ArtifactReconciliationState::Complete;
  });
}

auto InMemoryArtifactStore::put(std::span<const std::byte> data,
                                std::string media_type)
    -> Result<ArtifactPutResult> {
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
  return ok(ArtifactPutResult{std::move(ref)});
}

auto InMemoryArtifactStore::get(const ArtifactId &artifact_id) const
    -> Result<ArtifactBlob> {
  if (!storage_detail::valid_storage_key(artifact_id.str())) {
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
    -> Result<ArtifactEraseResult> {
  if (!storage_detail::valid_storage_key(artifact_id.str())) {
    return fail(Error::InvalidArgument);
  }
  std::lock_guard lock(mutex_);
  if (artifacts_.erase(artifact_id.str()) == 0) {
    return fail(Error::NotFound);
  }
  return ok(ArtifactEraseResult{.logical_deleted = true});
}

auto InMemoryArtifactStore::size() const -> std::size_t {
  std::lock_guard lock(mutex_);
  return artifacts_.size();
}

FileArtifactStore::FileArtifactStore(std::filesystem::path directory,
                                     std::size_t max_metadata_bytes,
                                     std::size_t max_artifact_bytes)
    : directory_(std::move(directory)),
      max_metadata_bytes_(max_metadata_bytes),
      max_artifact_bytes_(max_artifact_bytes) {}

auto FileArtifactStore::put(std::span<const std::byte> data,
                            std::string media_type)
    -> Result<ArtifactPutResult> {
  if (data.size() > max_artifact_bytes_) {
    return fail(Error::ResourceExhausted);
  }
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
  if (encoded->size() > max_metadata_bytes_) {
    return fail(Error::ResourceExhausted);
  }

  std::lock_guard lock(mutex_);
  const auto base = directory_ / ref.artifact_id.str();
  const auto data_path = base.string() + ".bin";
  auto data_result = storage_detail::store_file_atomic(data_path, data);
  if (!data_result) {
    return fail(data_result.error());
  }
  auto metadata_result =
      storage_detail::store_text_file_atomic(base.string() + ".json", *encoded);
  if (!metadata_result) {
    auto cleaned = storage_detail::remove_file_durable(data_path);
    if (!cleaned || (cleaned->removed && !cleaned->durability_confirmed())) {
      log::warn("Artifact {} data rollback is incomplete after metadata write failure",
                ref.artifact_id);
    }
    return fail(metadata_result.error());
  }
  const bool durability_deferred =
      !metadata_result->durability_confirmed();
  if (durability_deferred) {
    log::warn("Artifact {} was committed but directory durability is deferred: {}",
              ref.artifact_id, metadata_result->durability_error.message());
  }
  return ok(ArtifactPutResult{std::move(ref), durability_deferred});
}

auto FileArtifactStore::get(const ArtifactId &artifact_id) const
    -> Result<ArtifactBlob> {
  if (!storage_detail::valid_storage_key(artifact_id.str())) {
    return fail(Error::InvalidArgument);
  }
  std::lock_guard lock(mutex_);
  const auto base = directory_ / artifact_id.str();
  auto metadata_text = storage_detail::load_text_file(
      base.string() + ".json", max_metadata_bytes_);
  if (!metadata_text) {
    return fail(metadata_text.error());
  }
  auto metadata = storage_detail::decode_artifact_metadata(*metadata_text);
  if (!metadata || metadata->artifact_id != artifact_id) {
    return fail(metadata ? Error::ParseError : metadata.error());
  }
  auto data = storage_detail::load_file(base.string() + ".bin",
                                        max_artifact_bytes_);
  if (!data) {
    return fail(data.error());
  }
  ArtifactBlob blob{
      .ref = ArtifactRef{
          .artifact_id = artifact_id.clone(),
          .media_type = std::move(metadata->media_type),
          .size_bytes = metadata->size_bytes,
          .digest = std::move(metadata->digest),
      },
  };
  blob.data = std::move(*data);
  auto digest = storage_detail::compute_digest(blob.data);
  if (!digest || *digest != blob.ref.digest ||
      blob.data.size() != blob.ref.size_bytes) {
    return fail(Error::ProtocolError);
  }
  return ok(std::move(blob));
}

auto FileArtifactStore::erase(const ArtifactId &artifact_id)
    -> Result<ArtifactEraseResult> {
  if (!storage_detail::valid_storage_key(artifact_id.str())) {
    return fail(Error::InvalidArgument);
  }
  std::lock_guard lock(mutex_);
  const auto base = directory_ / artifact_id.str();
  auto removed_metadata =
      storage_detail::remove_file_durable(base.string() + ".json");
  if (!removed_metadata) {
    log::error("Artifact {} metadata deletion failed: {}", artifact_id,
               removed_metadata.error().message());
    return fail(Error::PersistenceError);
  }
  const bool metadata_durability_pending =
      removed_metadata->removed && !removed_metadata->durability_confirmed();
  auto removed_data =
      storage_detail::remove_file_durable(base.string() + ".bin");
  if (!removed_data) {
    if (removed_metadata->removed) {
      log::warn(
          "Artifact {} was logically deleted but data cleanup is deferred: {}",
          artifact_id, removed_data.error().message());
      return ok(ArtifactEraseResult{
          .logical_deleted = true,
          .cleanup_deferred = true,
          .durability_deferred = metadata_durability_pending,
      });
    }
    log::error("Artifact {} orphan data cleanup failed: {}", artifact_id,
               removed_data.error().message());
    return fail(Error::PersistenceError);
  }
  const bool data_durability_pending =
      removed_data->removed && !removed_data->durability_confirmed();
  const bool later_directory_sync_confirmed =
      removed_data->removed && removed_data->durability_confirmed();
  const bool durability_deferred =
      data_durability_pending ||
      (metadata_durability_pending && !later_directory_sync_confirmed);
  if (durability_deferred) {
    log::warn("Artifact {} data cleanup durability is deferred: {}",
              artifact_id,
              data_durability_pending
                  ? removed_data->durability_error.message()
                  : removed_metadata->durability_error.message());
    return ok(ArtifactEraseResult{
        .logical_deleted = true,
        .cleanup_deferred = data_durability_pending,
        .durability_deferred = true,
    });
  }
  if (!removed_metadata->removed && !removed_data->removed) {
    return fail(Error::NotFound);
  }
  return ok(ArtifactEraseResult{.logical_deleted = true});
}

auto FileArtifactStore::reconcile() const
    -> Result<ArtifactReconciliationReport> {
  std::lock_guard lock(mutex_);
  ArtifactReconciliationReport report;
  std::error_code error;
  const auto directory_status = std::filesystem::symlink_status(directory_, error);
  if (error) {
    if (error == std::errc::no_such_file_or_directory) {
      return ok(std::move(report));
    }
    return fail(error);
  }
  if (directory_status.type() == std::filesystem::file_type::not_found) {
    return ok(std::move(report));
  }
  if (!std::filesystem::is_directory(directory_status)) {
    return fail(Error::InvalidState);
  }

  std::map<std::string, ArtifactFilePair> pairs;
  for (std::filesystem::directory_iterator current(directory_, error), end;
       !error && current != end; current.increment(error)) {
    const auto extension = current->path().extension();
    if (extension != ".bin" && extension != ".json") {
      continue;
    }
    const auto key = current->path().stem().string();
    if (!storage_detail::valid_storage_key(key)) {
      append_reconciliation_entry(
          report, current->path().filename().string(),
          ArtifactReconciliationState::InvalidEntry);
      continue;
    }
    std::error_code status_error;
    const auto status = current->symlink_status(status_error);
    if (status_error) {
      return fail(status_error);
    }
    auto &pair = pairs[key];
    const bool regular = std::filesystem::is_regular_file(status);
    if (extension == ".bin") {
      pair.data_path = current->path();
      pair.data_present = true;
      pair.data_regular = regular;
    } else {
      pair.metadata_path = current->path();
      pair.metadata_present = true;
      pair.metadata_regular = regular;
    }
  }
  if (error) {
    return fail(error);
  }

  for (const auto &[key, pair] : pairs) {
    if (!pair.metadata_present) {
      append_reconciliation_entry(report, key,
                                  ArtifactReconciliationState::OrphanData);
      continue;
    }
    if (!pair.metadata_regular) {
      append_reconciliation_entry(
          report, key, ArtifactReconciliationState::MalformedMetadata);
      continue;
    }

    auto metadata_text = storage_detail::load_text_file(
        pair.metadata_path, max_metadata_bytes_);
    if (!metadata_text) {
      if (metadata_text.error() ==
          make_error_code(Error::ResourceExhausted)) {
        append_reconciliation_entry(
            report, key, ArtifactReconciliationState::MalformedMetadata);
        continue;
      }
      return fail(metadata_text.error());
    }
    auto metadata = storage_detail::decode_artifact_metadata(*metadata_text);
    if (!metadata || metadata->artifact_id.str() != key) {
      append_reconciliation_entry(
          report, key, ArtifactReconciliationState::MalformedMetadata);
      continue;
    }
    if (!pair.data_present) {
      append_reconciliation_entry(report, key,
                                  ArtifactReconciliationState::OrphanMetadata);
      continue;
    }
    if (!pair.data_regular) {
      append_reconciliation_entry(report, key,
                                  ArtifactReconciliationState::ContentMismatch);
      continue;
    }

    auto data = storage_detail::load_file(pair.data_path, max_artifact_bytes_);
    if (!data) {
      if (data.error() == make_error_code(Error::ResourceExhausted)) {
        append_reconciliation_entry(
            report, key, ArtifactReconciliationState::ContentMismatch);
        continue;
      }
      return fail(data.error());
    }
    auto digest = storage_detail::compute_digest(*data);
    const bool content_matches =
        digest && *digest == metadata->digest &&
        data->size() == metadata->size_bytes;
    append_reconciliation_entry(
        report, key,
        content_matches ? ArtifactReconciliationState::Complete
                        : ArtifactReconciliationState::ContentMismatch);
  }

  std::ranges::sort(report.entries, [](const auto &left, const auto &right) {
    if (left.storage_key != right.storage_key) {
      return left.storage_key < right.storage_key;
    }
    return left.state < right.state;
  });
  return ok(std::move(report));
}

} // namespace dagforge::workflow
