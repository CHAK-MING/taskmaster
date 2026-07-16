#include "dagforge/workflow/checkpoint_store.hpp"

#include "detail/durable_file.hpp"
#include "detail/json_file_catalog.hpp"
#include "detail/storage_codec.hpp"

#include <filesystem>
#include <ranges>
#include <utility>
#include <vector>

namespace dagforge::workflow {
namespace {

auto sort_checkpoints(std::vector<WorkflowCheckpoint> &checkpoints) -> void {
  std::ranges::sort(checkpoints, [](const auto &left, const auto &right) {
    if (left.created_at != right.created_at) {
      return left.created_at < right.created_at;
    }
    return left.snapshot.run_id.str() < right.snapshot.run_id.str();
  });
}

} // namespace

CheckpointStore::CheckpointStore(std::filesystem::path directory,
                                 std::size_t max_checkpoint_bytes)
    : directory_(std::move(directory)),
      max_checkpoint_bytes_(max_checkpoint_bytes) {}

auto CheckpointStore::file_path(const WorkflowRunId &run_id) const
    -> std::filesystem::path {
  return directory_ / (run_id.str() + ".json");
}

auto CheckpointStore::save(WorkflowCheckpoint checkpoint)
    -> Result<CheckpointSaveResult> {
  if (checkpoint.snapshot.run_id.empty() ||
      checkpoint.snapshot.plan_id.empty() ||
      !storage_detail::valid_storage_key(checkpoint.snapshot.run_id.str())) {
    return fail(Error::InvalidArgument);
  }
  std::lock_guard lock(mutex_);
  if (!directory_.empty()) {
    auto encoded = storage_detail::encode_checkpoint(checkpoint);
    if (!encoded) {
      return fail(encoded.error());
    }
    if (encoded->size() > max_checkpoint_bytes_) {
      return fail(Error::ResourceExhausted);
    }
    auto written = storage_detail::store_text_file_atomic(
        file_path(checkpoint.snapshot.run_id), *encoded);
    if (!written) {
      return fail(written.error());
    }
    const bool durability_deferred = !written->durability_confirmed();
    checkpoints_[checkpoint.snapshot.run_id.str()] = std::move(checkpoint);
    return ok(CheckpointSaveResult{
        .durability_deferred = durability_deferred,
    });
  } else {
    auto validated = storage_detail::validate_checkpoint(checkpoint);
    if (!validated) {
      return fail(validated.error());
    }
  }
  checkpoints_[checkpoint.snapshot.run_id.str()] = std::move(checkpoint);
  return ok(CheckpointSaveResult{});
}

auto CheckpointStore::load(const WorkflowRunId &run_id) const
    -> Result<WorkflowCheckpoint> {
  if (!storage_detail::valid_storage_key(run_id.str())) {
    return fail(Error::InvalidArgument);
  }
  std::lock_guard lock(mutex_);
  if (directory_.empty()) {
    const auto it = checkpoints_.find(run_id.str());
    if (it == checkpoints_.end()) {
      return fail(Error::NotFound);
    }
    return ok(it->second);
  }
  auto text = storage_detail::load_text_file(file_path(run_id),
                                             max_checkpoint_bytes_);
  if (!text) {
    return fail(text.error());
  }
  auto checkpoint = storage_detail::decode_checkpoint(*text);
  if (!checkpoint || checkpoint->snapshot.run_id != run_id) {
    return fail(checkpoint ? Error::ParseError : checkpoint.error());
  }
  return checkpoint;
}

auto CheckpointStore::erase(const WorkflowRunId &run_id)
    -> Result<CheckpointEraseResult> {
  if (!storage_detail::valid_storage_key(run_id.str())) {
    return fail(Error::InvalidArgument);
  }
  std::lock_guard lock(mutex_);
  bool removed = false;
  bool durability_deferred = false;
  if (!directory_.empty()) {
    auto durable = storage_detail::remove_file_durable(file_path(run_id));
    if (!durable) {
      return fail(durable.error());
    }
    removed = durable->removed;
    if (durable->removed && !durable->durability_confirmed()) {
      durability_deferred = true;
    }
  }
  const auto erased = checkpoints_.erase(run_id.str()) != 0;
  if (!erased && !removed) {
    return fail(Error::NotFound);
  }
  return ok(CheckpointEraseResult{
      .removed = true,
      .durability_deferred = durability_deferred,
  });
}

auto CheckpointStore::list() const
    -> Result<std::vector<WorkflowCheckpoint>> {
  std::lock_guard lock(mutex_);
  std::vector<WorkflowCheckpoint> checkpoints;
  if (directory_.empty()) {
    checkpoints.reserve(checkpoints_.size());
    for (const auto &[_, checkpoint] : checkpoints_) {
      checkpoints.push_back(checkpoint);
    }
    sort_checkpoints(checkpoints);
    return ok(std::move(checkpoints));
  }

  auto files =
      storage_detail::load_json_catalog(directory_, max_checkpoint_bytes_);
  if (!files) {
    return fail(files.error());
  }
  for (auto &file : *files) {
    auto checkpoint = storage_detail::decode_checkpoint(file.contents);
    if (!checkpoint) {
      return fail(checkpoint.error());
    }
    if (file.key != checkpoint->snapshot.run_id.str()) {
      return fail(Error::ParseError);
    }
    checkpoints.push_back(std::move(*checkpoint));
  }
  sort_checkpoints(checkpoints);
  return ok(std::move(checkpoints));
}
} // namespace dagforge::workflow
