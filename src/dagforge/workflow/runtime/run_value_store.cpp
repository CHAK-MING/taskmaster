#include "dagforge/workflow/run_value_store.hpp"

#include "dagforge/core/runtime.hpp"
#include "dagforge/util/json.hpp"

#include "../detail/value_size.hpp"

#include <span>
#include <string>
#include <string_view>
#include <utility>

namespace dagforge::workflow {

RunValueStore::RunValueStore(Runtime &runtime, shard_id owner,
                             IArtifactStore &artifacts,
                             std::uint64_t max_total_output_bytes,
                             std::size_t artifact_threshold_bytes)
    : runtime_(runtime), owner_(owner), artifacts_(artifacts),
      max_total_output_bytes_(max_total_output_bytes),
      artifact_threshold_bytes_(artifact_threshold_bytes) {}

auto RunValueStore::ensure_owner() const -> Result<void> {
  if (!runtime_.is_current_shard() || runtime_.current_shard() != owner_) {
    return fail(Error::InvalidState);
  }
  return ok();
}

auto RunValueStore::maybe_externalize(WorkflowValue value)
    -> Result<PreparedValue> {
  if (detail::value_size_bytes(value) < artifact_threshold_bytes_) {
    return ok(PreparedValue{.value = std::move(value)});
  }

  if (auto *text = std::get_if<std::string>(&value)) {
    auto artifact = artifacts_.put(
        std::as_bytes(std::span{text->data(), text->size()}),
        "text/plain; charset=utf-8");
    if (!artifact) {
      return fail(artifact.error());
    }
    auto artifact_id = artifact->artifact_id.clone();
    if (artifact->durability_deferred) {
      (void)artifacts_.erase(artifact_id);
      return fail(Error::PersistenceError);
    }
    return ok(PreparedValue{
        .value = WorkflowValue{std::move(*artifact).take_ref()},
        .owned_artifact_id = std::move(artifact_id),
    });
  }
  if (auto *json = std::get_if<JsonPayload>(&value)) {
    const auto encoded = json->encoded();
    auto artifact = artifacts_.put(
        std::as_bytes(std::span{encoded.data(), encoded.size()}),
        "application/json");
    if (!artifact) {
      return fail(artifact.error());
    }
    auto artifact_id = artifact->artifact_id.clone();
    if (artifact->durability_deferred) {
      (void)artifacts_.erase(artifact_id);
      return fail(Error::PersistenceError);
    }
    return ok(PreparedValue{
        .value = WorkflowValue{std::move(*artifact).take_ref()},
        .owned_artifact_id = std::move(artifact_id),
    });
  }

  return ok(PreparedValue{.value = std::move(value)});
}

auto RunValueStore::erase_owned_artifact(
    const std::optional<ArtifactId> &artifact_id) -> Result<void> {
  if (!artifact_id) {
    return ok();
  }
  auto erased = artifacts_.erase(*artifact_id);
  if (!erased && erased.error() != make_error_code(Error::NotFound)) {
    return fail(erased.error());
  }
  return ok();
}

auto RunValueStore::put(OutputRef output, WorkflowValue value) -> Result<void> {
  auto owner_result = ensure_owner();
  if (!owner_result) {
    return fail(owner_result.error());
  }
  if (output.node_id.empty() || output.port.empty()) {
    return fail(Error::InvalidArgument);
  }

  const auto existing = values_.find(output);
  const auto existing_bytes =
      existing == values_.end() ? 0 : existing->second.accounted_bytes;
  const auto accounted_bytes = detail::value_size_bytes(value);
  const auto retained_bytes = total_output_bytes_ - existing_bytes;
  if (accounted_bytes > max_total_output_bytes_ - retained_bytes) {
    return fail(Error::ResourceExhausted);
  }
  const auto next_total = retained_bytes + accounted_bytes;

  auto prepared = maybe_externalize(std::move(value));
  if (!prepared) {
    return fail(prepared.error());
  }

  std::optional<Entry> previous;
  if (existing != values_.end()) {
    previous = existing->second;
  }

  Entry entry{
      .value =
          std::make_shared<const WorkflowValue>(std::move(prepared->value)),
      .accounted_bytes = accounted_bytes,
      .owned_artifact_id = std::move(prepared->owned_artifact_id),
  };
  const auto replacement_artifact = entry.owned_artifact_id;
  values_.insert_or_assign(output, std::move(entry));
  total_output_bytes_ = next_total;

  if (previous && previous->owned_artifact_id != replacement_artifact) {
    auto erased = erase_owned_artifact(previous->owned_artifact_id);
    if (!erased) {
      values_.insert_or_assign(output, std::move(*previous));
      total_output_bytes_ = retained_bytes + existing_bytes;
      (void)erase_owned_artifact(replacement_artifact);
      return fail(erased.error());
    }
  }
  return ok();
}

auto RunValueStore::get(const OutputRef &output) const
    -> Result<std::shared_ptr<const WorkflowValue>> {
  auto owner_result = ensure_owner();
  if (!owner_result) {
    return fail(owner_result.error());
  }
  const auto it = values_.find(output);
  if (it == values_.end()) {
    return fail(Error::NotFound);
  }
  return ok(it->second.value);
}

auto RunValueStore::contains(const OutputRef &output) const -> bool {
  if (!ensure_owner()) {
    return false;
  }
  return values_.contains(output);
}

auto RunValueStore::snapshot() const
    -> Result<std::vector<OutputValue>> {
  auto owner_result = ensure_owner();
  if (!owner_result) {
    return fail(owner_result.error());
  }

  std::vector<OutputValue> out;
  out.reserve(values_.size());
  for (const auto &[output, entry] : values_) {
    out.emplace_back(output, *entry.value);
  }
  return ok(std::move(out));
}

auto RunValueStore::erase_node(const WorkflowNodeId &node_id) -> Result<void> {
  auto owner_result = ensure_owner();
  if (!owner_result) {
    return fail(owner_result.error());
  }

  std::optional<std::error_code> cleanup_error;
  for (auto it = values_.begin(); it != values_.end();) {
    if (it->first.node_id == node_id) {
      auto erased = erase_owned_artifact(it->second.owned_artifact_id);
      if (!erased && !cleanup_error) {
        cleanup_error = erased.error();
      }
      total_output_bytes_ -= it->second.accounted_bytes;
      it = values_.erase(it);
    } else {
      ++it;
    }
  }
  return cleanup_error ? fail(*cleanup_error) : ok();
}

} // namespace dagforge::workflow
