#include "dagforge/workflow/checkpoint_store.hpp"

#include "detail/storage_codec.hpp"

#include <filesystem>
#include <ranges>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace dagforge::workflow {
namespace {

[[nodiscard]] auto valid_storage_key(std::string_view value) -> bool {
  return !value.empty() && value != "." && value != ".." &&
         std::filesystem::path{value}.filename() == value;
}

} // namespace

CheckpointStore::CheckpointStore(std::filesystem::path directory)
    : directory_(std::move(directory)) {
  std::error_code error;
  std::filesystem::create_directories(directory_, error);
}

auto CheckpointStore::file_path(const WorkflowRunId &run_id) const
    -> std::filesystem::path {
  return directory_ / (run_id.str() + ".json");
}

auto CheckpointStore::save(WorkflowCheckpoint checkpoint) -> Result<void> {
  if (checkpoint.snapshot.run_id.empty() ||
      checkpoint.snapshot.plan_id.empty() ||
      !valid_storage_key(checkpoint.snapshot.run_id.str())) {
    return fail(Error::InvalidArgument);
  }
  std::lock_guard lock(mutex_);
  if (!directory_.empty()) {
    auto encoded = storage_detail::encode_checkpoint(checkpoint);
    if (!encoded) {
      return fail(encoded.error());
    }
    auto written = storage_detail::store_text_file_atomic(
        file_path(checkpoint.snapshot.run_id), *encoded);
    if (!written) {
      return fail(written.error());
    }
  } else {
    auto validated = storage_detail::validate_checkpoint(checkpoint);
    if (!validated) {
      return fail(validated.error());
    }
  }
  checkpoints_[checkpoint.snapshot.run_id.str()] = std::move(checkpoint);
  return ok();
}

auto CheckpointStore::load(const WorkflowRunId &run_id) const
    -> Result<WorkflowCheckpoint> {
  if (!valid_storage_key(run_id.str())) {
    return fail(Error::InvalidArgument);
  }
  std::lock_guard lock(mutex_);
  const auto it = checkpoints_.find(run_id.str());
  if (it == checkpoints_.end()) {
    if (directory_.empty()) {
      return fail(Error::NotFound);
    }
    auto text = storage_detail::load_text_file(file_path(run_id));
    if (!text) {
      return fail(text.error());
    }
    auto checkpoint = storage_detail::decode_checkpoint(*text);
    if (!checkpoint || checkpoint->snapshot.run_id != run_id) {
      return fail(checkpoint ? Error::ParseError : checkpoint.error());
    }
    return checkpoint;
  }
  return ok(it->second);
}

auto CheckpointStore::erase(const WorkflowRunId &run_id) -> Result<void> {
  if (!valid_storage_key(run_id.str())) {
    return fail(Error::InvalidArgument);
  }
  std::lock_guard lock(mutex_);
  const auto erased = checkpoints_.erase(run_id.str()) != 0;
  bool removed = false;
  if (!directory_.empty()) {
    std::error_code error;
    removed = std::filesystem::remove(file_path(run_id), error);
    if (error) {
      return fail(error);
    }
  }
  return erased || removed ? ok() : fail(Error::NotFound);
}

auto CheckpointStore::list() const
    -> Result<std::vector<WorkflowCheckpoint>> {
  std::vector<WorkflowCheckpoint> checkpoints;
  {
    std::lock_guard lock(mutex_);
    checkpoints.reserve(checkpoints_.size());
    for (const auto &[_, checkpoint] : checkpoints_) {
      checkpoints.push_back(checkpoint);
    }
  }
  if (directory_.empty()) {
    return ok(std::move(checkpoints));
  }

  std::unordered_map<std::string, bool> known;
  for (const auto &checkpoint : checkpoints) {
    known.emplace(checkpoint.snapshot.run_id.str(), true);
  }
  std::error_code error;
  for (std::filesystem::directory_iterator it(directory_, error), end;
       !error && it != end; it.increment(error)) {
    if (!it->is_regular_file() || it->path().extension() != ".json") {
      continue;
    }
    auto text = storage_detail::load_text_file(it->path());
    if (!text) {
      return fail(text.error());
    }
    auto checkpoint = storage_detail::decode_checkpoint(*text);
    if (!checkpoint) {
      return fail(checkpoint.error());
    }
    if (it->path().stem().string() != checkpoint->snapshot.run_id.str()) {
      return fail(Error::ParseError);
    }
    if (known.emplace(checkpoint->snapshot.run_id.str(), true).second) {
      checkpoints.push_back(std::move(*checkpoint));
    }
  }
  if (error) {
    return fail(error);
  }
  std::ranges::sort(checkpoints, {}, &WorkflowCheckpoint::created_at);
  return ok(std::move(checkpoints));
}
} // namespace dagforge::workflow
