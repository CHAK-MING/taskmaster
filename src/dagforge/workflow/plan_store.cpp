#include "dagforge/workflow/plan_store.hpp"

#include "dagforge/util/json.hpp"
#include "dagforge/workflow/workflow_plan_loader.hpp"

#include "detail/storage_codec.hpp"

#include <chrono>
#include <filesystem>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

namespace dagforge::workflow {
namespace {

[[nodiscard]] auto valid_storage_key(std::string_view value) -> bool {
  return !value.empty() && value != "." && value != ".." &&
         std::filesystem::path{value}.filename() == value;
}

auto sort_plans(std::vector<StoredPlan> &plans) -> void {
  std::ranges::sort(plans, [](const StoredPlan &left,
                              const StoredPlan &right) {
    if (left.created_at != right.created_at) {
      return left.created_at < right.created_at;
    }
    return left.plan_id.str() < right.plan_id.str();
  });
}

[[nodiscard]] auto source_plan(const ExecutionPlan &execution) -> WorkflowPlan {
  WorkflowPlan plan;
  plan.workflow_id = execution.workflow_id.clone();
  plan.nodes.reserve(execution.nodes.size());
  for (const auto &compiled : execution.nodes) {
    plan.nodes.push_back(compiled.plan);
  }
  plan.edges = execution.edges;
  plan.outputs = execution.outputs;
  plan.policy = execution.policy;
  return plan;
}

[[nodiscard]] auto encode_stored_plan(const StoredPlan &stored)
    -> Result<std::string> {
  auto plan_json = WorkflowPlanLoader::to_json(stored.plan);
  if (!plan_json) {
    return fail(plan_json.error());
  }
  auto parsed_plan = parse_json(*plan_json);
  if (!parsed_plan) {
    return fail(parsed_plan.error());
  }
  JsonValue value = JsonValue::object_t{};
  value["plan_id"] = stored.plan_id.str();
  value["digest"] = stored.digest;
  value["created_at_ms"] =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          stored.created_at.time_since_epoch())
          .count();
  value["plan"] = std::move(*parsed_plan);
  return ok(dump_json(value));
}

[[nodiscard]] auto decode_stored_plan(std::string_view text)
    -> Result<StoredPlan> {
  auto value = parse_json(text);
  if (!value || !value->is_object()) {
    return fail(Error::ParseError);
  }
  auto &object = value->get_object();
  const auto plan_id = object.find("plan_id");
  const auto digest = object.find("digest");
  const auto created_at = object.find("created_at_ms");
  const auto plan = object.find("plan");
  if (plan_id == object.end() || !plan_id->second.is_string() ||
      plan_id->second.as<std::string>().empty() || digest == object.end() ||
      !digest->second.is_string() || digest->second.as<std::string>().empty() ||
      created_at == object.end() ||
      !created_at->second.is_number() || plan == object.end() ||
      !plan->second.is_object()) {
    return fail(Error::ParseError);
  }
  auto decoded_plan = WorkflowPlanLoader::from_json(dump_json(plan->second));
  if (!decoded_plan) {
    return fail(decoded_plan.error());
  }
  return ok(StoredPlan{
      .plan_id = WorkflowPlanId{plan_id->second.as<std::string>()},
      .digest = digest->second.as<std::string>(),
      .plan = std::move(*decoded_plan),
      .created_at = std::chrono::system_clock::time_point{
          std::chrono::milliseconds{created_at->second.as<std::int64_t>()}},
  });
}

} // namespace

PlanStore::PlanStore(std::filesystem::path directory)
    : directory_(std::move(directory)) {
  std::error_code error;
  std::filesystem::create_directories(directory_, error);
}

auto PlanStore::file_path(const WorkflowPlanId &plan_id) const
    -> std::filesystem::path {
  return directory_ / (plan_id.str() + ".json");
}

auto PlanStore::save(const ExecutionPlan &plan) -> Result<void> {
  if (plan.plan_id.empty() || plan.workflow_id.empty() ||
      !valid_storage_key(plan.plan_id.str())) {
    return fail(Error::InvalidArgument);
  }
  StoredPlan stored{
      .plan_id = plan.plan_id.clone(),
      .digest = plan.digest,
      .plan = source_plan(plan),
  };
  std::lock_guard lock(mutex_);
  if (const auto existing = plans_.find(stored.plan_id.str());
      existing != plans_.end()) {
    return existing->second.digest == stored.digest
               ? ok()
               : fail(Error::AlreadyExists);
  }
  if (!directory_.empty()) {
    std::error_code exists_error;
    const auto path = file_path(stored.plan_id);
    if (std::filesystem::exists(path, exists_error)) {
      if (exists_error) {
        return fail(exists_error);
      }
      auto text = storage_detail::load_text_file(path);
      if (!text) {
        return fail(text.error());
      }
      auto existing = decode_stored_plan(*text);
      if (!existing || existing->plan_id != stored.plan_id) {
        return fail(existing ? Error::ParseError : existing.error());
      }
      if (existing->digest != stored.digest) {
        return fail(Error::AlreadyExists);
      }
      plans_.emplace(stored.plan_id.str(), std::move(*existing));
      return ok();
    }
    if (exists_error) {
      return fail(exists_error);
    }
    auto encoded = encode_stored_plan(stored);
    if (!encoded) {
      return fail(encoded.error());
    }
    auto written = storage_detail::store_text_file_atomic(
        path, *encoded);
    if (!written) {
      return fail(written.error());
    }
  }
  plans_.emplace(stored.plan_id.str(), std::move(stored));
  return ok();
}

auto PlanStore::load(const WorkflowPlanId &plan_id) const
    -> Result<StoredPlan> {
  if (!valid_storage_key(plan_id.str())) {
    return fail(Error::InvalidArgument);
  }
  std::lock_guard lock(mutex_);
  if (const auto it = plans_.find(plan_id.str()); it != plans_.end()) {
    return ok(it->second);
  }
  if (directory_.empty()) {
    return fail(Error::NotFound);
  }
  auto text = storage_detail::load_text_file(file_path(plan_id));
  if (!text) {
    return fail(text.error());
  }
  auto stored = decode_stored_plan(*text);
  if (!stored || stored->plan_id != plan_id) {
    return fail(stored ? Error::ParseError : stored.error());
  }
  return stored;
}

auto PlanStore::list() const -> Result<std::vector<StoredPlan>> {
  std::vector<StoredPlan> plans;
  {
    std::lock_guard lock(mutex_);
    plans.reserve(plans_.size());
    for (const auto &[_, plan] : plans_) {
      plans.push_back(plan);
    }
  }
  if (directory_.empty()) {
    sort_plans(plans);
    return ok(std::move(plans));
  }

  std::unordered_map<std::string, std::string> known;
  for (const auto &plan : plans) {
    known.emplace(plan.plan_id.str(), plan.digest);
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
    auto plan = decode_stored_plan(*text);
    if (!plan) {
      return fail(plan.error());
    }
    if (it->path().stem().string() != plan->plan_id.str()) {
      return fail(Error::ParseError);
    }
    auto [known_plan, inserted] =
        known.emplace(plan->plan_id.str(), plan->digest);
    if (!inserted && known_plan->second != plan->digest) {
      return fail(Error::ParseError);
    }
    if (inserted) {
      plans.push_back(std::move(*plan));
    }
  }
  if (error) {
    return fail(error);
  }
  sort_plans(plans);
  return ok(std::move(plans));
}

} // namespace dagforge::workflow
