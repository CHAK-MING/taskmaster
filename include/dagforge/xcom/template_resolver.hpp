#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/util/id.hpp"
#include "dagforge/xcom/xcom_types.hpp"

#include <ankerl/unordered_dense.h>
#include <chrono>
#include <functional>
#include <string>
#include <vector>
#endif


namespace dagforge {

struct TemplateContext {
  DAGRunId dag_run_id;
  DAGId dag_id;
  TaskId task_id;
  std::chrono::system_clock::time_point execution_date;
  std::chrono::system_clock::time_point data_interval_start;
  std::chrono::system_clock::time_point data_interval_end;
};

class TemplateResolver {
public:
  using XComLookupFn = std::move_only_function<Result<XComEntry>(
      const DAGRunId &, const TaskId &, std::string_view)>;

  TemplateResolver() = default;
  explicit TemplateResolver(XComLookupFn lookup);
  auto set_xcom_lookup(XComLookupFn lookup) -> void;

  // Pre-populate the XCom cache before executing a task. This allows the
  // resolver to work synchronously during template expansion without needing
  // a blocking DB call — the caller pre-fetches via co_await and seeds the
  // cache with rendered values here before handing off to the synchronous
  // visitor.
  auto prefetch_xcom(const DAGRunId &run_id, const TaskId &task_id,
                     std::string_view key, std::string_view value_json) -> void;

  [[nodiscard]] auto resolve_env_vars(const TemplateContext &ctx,
                                      const std::vector<XComPullConfig> &pulls)
      -> Result<ankerl::unordered_dense::map<std::string, std::string,
                                             StringHash, StringEqual>>;

  [[nodiscard]] auto resolve_template(std::string_view tmpl,
                                      const TemplateContext &ctx,
                                      const std::vector<XComPullConfig> &pulls,
                                      bool strict_whitelist = true)
      -> Result<std::string>;

  [[nodiscard]] auto resolve_xcom_pull(const TemplateContext &ctx,
                                       const XComPullConfig &pull)
      -> Result<std::optional<std::string>>;

private:
  XComLookupFn xcom_lookup_;
  XComCache xcom_cache_;
};

} // namespace dagforge
