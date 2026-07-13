#include "dagforge/workflow/run_value_store.hpp"

#include "dagforge/core/runtime.hpp"
#include "dagforge/util/json.hpp"

#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

namespace dagforge::workflow {
namespace {

[[nodiscard]] auto bytes_of(std::string_view value)
    -> std::span<const std::byte> {
  return std::as_bytes(std::span{value.data(), value.size()});
}

[[nodiscard]] auto estimate_size(const WorkflowValue &value) -> std::uint64_t {
  return std::visit(
      [](const auto &typed) -> std::uint64_t {
        using T = std::decay_t<decltype(typed)>;
        if constexpr (std::same_as<T, std::monostate>) {
          return 0;
        } else if constexpr (std::same_as<T, bool>) {
          return 1;
        } else if constexpr (std::same_as<T, std::int64_t> ||
                             std::same_as<T, double>) {
          return sizeof(T);
        } else if constexpr (std::same_as<T, std::string>) {
          return typed.size();
        } else if constexpr (std::same_as<T, JsonValue>) {
          return dump_json(typed).size();
        } else if constexpr (std::same_as<T, ArtifactRef>) {
          return typed.artifact_id.size() + typed.media_type.size() +
                 typed.digest.size();
        }
        return 0;
      },
      value);
}

} // namespace

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

auto RunValueStore::key(const OutputRef &output) -> std::string {
  std::string out;
  out.reserve(output.node_id.size() + output.port.size() + 1);
  out.append(output.node_id.value());
  out.push_back('\x1f');
  out.append(output.port.value());
  return out;
}

auto RunValueStore::maybe_externalize(WorkflowValue value)
    -> Result<std::pair<WorkflowValue, std::uint64_t>> {
  const auto accounted_bytes = estimate_size(value);
  if (accounted_bytes < artifact_threshold_bytes_) {
    return ok(std::pair{std::move(value), accounted_bytes});
  }

  if (auto *text = std::get_if<std::string>(&value)) {
    auto artifact = artifacts_.put(bytes_of(*text), "text/plain; charset=utf-8");
    if (!artifact) {
      return fail(artifact.error());
    }
    return ok(std::pair{WorkflowValue{std::move(*artifact)}, accounted_bytes});
  }
  if (auto *json = std::get_if<JsonValue>(&value)) {
    auto encoded = dump_json(*json);
    auto artifact = artifacts_.put(bytes_of(encoded), "application/json");
    if (!artifact) {
      return fail(artifact.error());
    }
    return ok(std::pair{WorkflowValue{std::move(*artifact)}, accounted_bytes});
  }

  return ok(std::pair{std::move(value), accounted_bytes});
}

auto RunValueStore::put(OutputRef output, WorkflowValue value) -> Result<void> {
  auto owner_result = ensure_owner();
  if (!owner_result) {
    return fail(owner_result.error());
  }
  if (output.node_id.empty() || output.port.empty()) {
    return fail(Error::InvalidArgument);
  }

  auto stored = maybe_externalize(std::move(value));
  if (!stored) {
    return fail(stored.error());
  }

  auto entry_key = key(output);
  const auto existing = values_.find(entry_key);
  const auto existing_bytes =
      existing == values_.end() ? 0 : existing->second.accounted_bytes;
  const auto next_total = total_output_bytes_ - existing_bytes + stored->second;
  if (next_total > max_total_output_bytes_) {
    return fail(Error::ResourceExhausted);
  }

  Entry entry{
      .output = std::move(output),
      .value = std::make_shared<const WorkflowValue>(std::move(stored->first)),
      .accounted_bytes = stored->second,
  };
  values_.insert_or_assign(std::move(entry_key), std::move(entry));
  total_output_bytes_ = next_total;
  return ok();
}

auto RunValueStore::get(const OutputRef &output) const
    -> Result<std::shared_ptr<const WorkflowValue>> {
  auto owner_result = ensure_owner();
  if (!owner_result) {
    return fail(owner_result.error());
  }
  const auto it = values_.find(key(output));
  if (it == values_.end()) {
    return fail(Error::NotFound);
  }
  return ok(it->second.value);
}

auto RunValueStore::contains(const OutputRef &output) const -> bool {
  if (!ensure_owner()) {
    return false;
  }
  return values_.contains(key(output));
}

auto RunValueStore::snapshot() const
    -> Result<std::vector<std::pair<OutputRef, WorkflowValue>>> {
  auto owner_result = ensure_owner();
  if (!owner_result) {
    return fail(owner_result.error());
  }

  std::vector<std::pair<OutputRef, WorkflowValue>> out;
  out.reserve(values_.size());
  for (const auto &[_, entry] : values_) {
    out.emplace_back(entry.output, *entry.value);
  }
  return ok(std::move(out));
}

auto RunValueStore::erase_node(const WorkflowNodeId &node_id) -> Result<void> {
  auto owner_result = ensure_owner();
  if (!owner_result) {
    return fail(owner_result.error());
  }

  for (auto it = values_.begin(); it != values_.end();) {
    if (it->second.output.node_id == node_id) {
      total_output_bytes_ -= it->second.accounted_bytes;
      it = values_.erase(it);
    } else {
      ++it;
    }
  }
  return ok();
}

} // namespace dagforge::workflow
