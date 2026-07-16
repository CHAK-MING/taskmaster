#include "dagforge/workflow/plan_store.hpp"

#include "detail/durable_file.hpp"
#include "detail/json_file_catalog.hpp"
#include "detail/storage_codec.hpp"

#include <chrono>
#include <filesystem>
#include <ranges>
#include <string>
#include <utility>

namespace dagforge::workflow {
namespace {

auto sort_plans(std::vector<StoredPlan> &plans) -> void {
  std::ranges::sort(plans, [](const StoredPlan &left,
                              const StoredPlan &right) {
    if (left.created_at != right.created_at) {
      return left.created_at < right.created_at;
    }
    return left.plan_id.str() < right.plan_id.str();
  });
}

} // namespace

PlanStore::PlanStore(std::filesystem::path directory,
                     std::size_t max_plan_bytes)
    : directory_(std::move(directory)), max_plan_bytes_(max_plan_bytes) {}

auto PlanStore::file_path(const WorkflowPlanId &plan_id) const
    -> std::filesystem::path {
  return directory_ / (plan_id.str() + ".json");
}

auto PlanStore::save(const ExecutionPlan &plan) -> Result<PlanSaveResult> {
  if (plan.plan_id.empty() || plan.workflow_id.empty() ||
      !storage_detail::valid_storage_key(plan.plan_id.str())) {
    return fail(Error::InvalidArgument);
  }
  StoredPlan stored{
      .plan_id = plan.plan_id.clone(),
      .digest = plan.digest,
      .plan = source_plan(plan),
  };
  std::lock_guard lock(mutex_);
  if (directory_.empty()) {
    if (const auto existing = plans_.find(stored.plan_id.str());
        existing != plans_.end()) {
      return existing->second.digest == stored.digest
                 ? ok(PlanSaveResult{
                       .durability_deferred =
                           durability_deferred_[stored.plan_id.str()],
                   })
                 : fail(Error::AlreadyExists);
    }
  } else {
    std::error_code exists_error;
    const auto path = file_path(stored.plan_id);
    if (std::filesystem::exists(path, exists_error)) {
      if (exists_error) {
        return fail(exists_error);
      }
      auto text = storage_detail::load_text_file(path, max_plan_bytes_);
      if (!text) {
        return fail(text.error());
      }
      auto existing = storage_detail::decode_stored_plan(*text);
      if (!existing || existing->plan_id != stored.plan_id) {
        return fail(existing ? Error::ParseError : existing.error());
      }
      if (existing->digest != stored.digest) {
        return fail(Error::AlreadyExists);
      }
      plans_.emplace(stored.plan_id.str(), std::move(*existing));
      return ok(PlanSaveResult{
          .durability_deferred = durability_deferred_[stored.plan_id.str()],
      });
    }
    if (exists_error) {
      return fail(exists_error);
    }
    auto encoded = storage_detail::encode_stored_plan(stored);
    if (!encoded) {
      return fail(encoded.error());
    }
    if (encoded->size() > max_plan_bytes_) {
      return fail(Error::ResourceExhausted);
    }
    auto written = storage_detail::store_text_file_atomic(
        path, *encoded);
    if (!written) {
      return fail(written.error());
    }
    const bool durability_deferred = !written->durability_confirmed();
    if (!durability_deferred) {
      for (auto &[_, deferred] : durability_deferred_) {
        deferred = false;
      }
    }
    durability_deferred_[stored.plan_id.str()] = durability_deferred;
    plans_.emplace(stored.plan_id.str(), std::move(stored));
    return ok(PlanSaveResult{
        .durability_deferred = durability_deferred,
    });
  }
  durability_deferred_[stored.plan_id.str()] = false;
  plans_.emplace(stored.plan_id.str(), std::move(stored));
  return ok(PlanSaveResult{});
}

auto PlanStore::load(const WorkflowPlanId &plan_id) const
    -> Result<StoredPlan> {
  if (!storage_detail::valid_storage_key(plan_id.str())) {
    return fail(Error::InvalidArgument);
  }
  std::lock_guard lock(mutex_);
  if (directory_.empty()) {
    if (const auto it = plans_.find(plan_id.str()); it != plans_.end()) {
      return ok(it->second);
    }
    return fail(Error::NotFound);
  }
  auto text = storage_detail::load_text_file(file_path(plan_id),
                                             max_plan_bytes_);
  if (!text) {
    return fail(text.error());
  }
  auto stored = storage_detail::decode_stored_plan(*text);
  if (!stored || stored->plan_id != plan_id) {
    return fail(stored ? Error::ParseError : stored.error());
  }
  return stored;
}

auto PlanStore::list() const -> Result<std::vector<StoredPlan>> {
  std::lock_guard lock(mutex_);
  std::vector<StoredPlan> plans;
  if (directory_.empty()) {
    plans.reserve(plans_.size());
    for (const auto &[_, plan] : plans_) {
      plans.push_back(plan);
    }
    sort_plans(plans);
    return ok(std::move(plans));
  }

  auto files = storage_detail::load_json_catalog(directory_, max_plan_bytes_);
  if (!files) {
    return fail(files.error());
  }
  for (auto &file : *files) {
    auto plan = storage_detail::decode_stored_plan(file.contents);
    if (!plan) {
      return fail(plan.error());
    }
    if (file.key != plan->plan_id.str()) {
      return fail(Error::ParseError);
    }
    plans.push_back(std::move(*plan));
  }
  sort_plans(plans);
  return ok(std::move(plans));
}

} // namespace dagforge::workflow
