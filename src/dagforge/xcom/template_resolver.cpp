#include "dagforge/xcom/template_resolver.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/util/time.hpp"
#include "dagforge/util/log.hpp"
#include "dagforge/xcom/xcom_util.hpp"

#include <ankerl/unordered_dense.h>
#include <boost/algorithm/string/find.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <chrono>
#include <cstdio>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

namespace dagforge {

TemplateResolver::TemplateResolver(XComLookupFn lookup)
    : xcom_lookup_(std::move(lookup)) {}

auto TemplateResolver::set_xcom_lookup(XComLookupFn lookup) -> void {
  xcom_lookup_ = std::move(lookup);
}

auto TemplateResolver::prefetch_xcom(const DAGRunId &run_id,
                                     const TaskId &task_id,
                                     std::string_view key,
                                     std::string_view value_json) -> void {
  auto rendered = xcom::render_serialized_json(value_json);
  if (!rendered) {
    return;
  }
  xcom_cache_.set(run_id, task_id, key, *rendered);
}

auto TemplateResolver::resolve_env_vars(
    const TemplateContext &ctx, const std::vector<XComPullConfig> &pulls)
    -> Result<ankerl::unordered_dense::map<std::string, std::string, StringHash,
                                           StringEqual>> {
  ankerl::unordered_dense::map<std::string, std::string, StringHash,
                               StringEqual>
      env_vars;

  for (const auto &pull : pulls) {
    auto result = resolve_xcom_pull(ctx, pull);
    if (!result) {
      return fail(result.error());
    }

    if (result->has_value()) {
      env_vars[pull.env_var] = std::move(**result);
    }
  }

  return ok(env_vars);
}

auto TemplateResolver::resolve_xcom_pull(const TemplateContext &ctx,
                                         const XComPullConfig &pull)
    -> Result<std::optional<std::string>> {
  auto cached = xcom_cache_.get(ctx.dag_run_id, pull.ref.task_id, pull.ref.key);
  if (cached.has_value()) {
    return ok(std::optional<std::string>(cached->get()));
  }

  if (!xcom_lookup_) {
    if (pull.required) {
      return fail(Error::NotFound);
    }
    if (pull.has_default_value) {
      return ok(std::optional<std::string>(pull.default_value_rendered));
    }
    return ok(std::nullopt);
  }

  auto xcom_result =
      xcom_lookup_(ctx.dag_run_id, pull.ref.task_id, pull.ref.key);

  if (!xcom_result) {
    if (xcom_result.error() == make_error_code(Error::NotFound)) {
      if (pull.required) {
        return fail(Error::NotFound);
      }
      if (pull.has_default_value) {
        return ok(std::optional<std::string>(pull.default_value_rendered));
      }
      return ok(std::nullopt);
    }
    return fail(xcom_result.error());
  }

  auto rendered = xcom::render_serialized_json(xcom_result->value);
  if (!rendered) {
    return fail(rendered.error());
  }
  xcom_cache_.set(ctx.dag_run_id, pull.ref.task_id, pull.ref.key, *rendered);
  return ok(std::optional<std::string>(std::move(*rendered)));
}

namespace {

struct XComLookupKey {
  std::string source_task;
  std::string key;

  [[nodiscard]] friend auto operator==(const XComLookupKey &lhs,
                                       const XComLookupKey &rhs) noexcept
      -> bool = default;
};

struct XComLookupKeyHash {
  [[nodiscard]] auto operator()(const XComLookupKey &value) const noexcept
      -> std::size_t {
    std::size_t seed = std::hash<std::string>{}(value.source_task);
    seed ^= std::hash<std::string>{}(value.key) + 0x9e3779b9 + (seed << 6) +
            (seed >> 2);
    return seed;
  }
};

// Pre-computed date format strings for template resolution
template <typename Alloc = std::allocator<char>> struct DateFormatsT {
  using String = std::basic_string<char, std::char_traits<char>, Alloc>;

  String ds;                  // YYYY-MM-DD
  String ds_nodash;           // YYYYMMDD
  String ts;                  // YYYY-MM-DDTHH:MM:SS
  String ts_nodash;           // YYYYMMDDTHHMMSS
  String data_interval_start; // ISO 8601
  String data_interval_end;   // ISO 8601
  bool valid{false};

  explicit DateFormatsT(Alloc alloc = {})
      : ds(alloc), ds_nodash(alloc), ts(alloc), ts_nodash(alloc),
        data_interval_start(alloc), data_interval_end(alloc) {}
};

using DateFormats = DateFormatsT<>;
using DateFormatsPmr = DateFormatsT<pmr::polymorphic_allocator<char>>;

template <typename Alloc>
auto compute_date_formats_t(
    std::chrono::system_clock::time_point execution_date, Alloc alloc)
    -> DateFormatsT<Alloc> {
  if (execution_date == std::chrono::system_clock::time_point{}) {
    return DateFormatsT<Alloc>(alloc);
  }

  auto const sec_tp = std::chrono::floor<std::chrono::seconds>(execution_date);
  auto const utc_tm = util::to_utc(sec_tp);
  const int year = utc_tm.tm_year + 1900;
  const unsigned month = static_cast<unsigned>(utc_tm.tm_mon + 1);
  const unsigned day = static_cast<unsigned>(utc_tm.tm_mday);
  const unsigned hour = static_cast<unsigned>(utc_tm.tm_hour);
  const unsigned minute = static_cast<unsigned>(utc_tm.tm_min);
  const unsigned second = static_cast<unsigned>(utc_tm.tm_sec);

  DateFormatsT<Alloc> fmts(alloc);

  {
    char buf[32]{0};
    const int n =
        std::snprintf(buf, sizeof(buf), "%04d-%02u-%02u", year, month, day);
    if (n > 0) {
      fmts.ds.assign(buf, static_cast<std::size_t>(n));
    }
  }
  {
    char buf[32]{0};
    const int n =
        std::snprintf(buf, sizeof(buf), "%04d%02u%02u", year, month, day);
    if (n > 0) {
      fmts.ds_nodash.assign(buf, static_cast<std::size_t>(n));
    }
  }

  // Build pmr::string from a temporary std::string payload.
  std::string ts_std = util::format_iso8601(execution_date);
  fmts.ts =
      typename DateFormatsT<Alloc>::String(ts_std.data(), ts_std.size(), alloc);

  {
    char buf[32]{0};
    const int n = std::snprintf(buf, sizeof(buf), "%04d%02u%02uT%02u%02u%02u",
                                year, month, day, hour, minute, second);
    if (n > 0) {
      fmts.ts_nodash.assign(buf, static_cast<std::size_t>(n));
    }
  }
  fmts.valid = true;
  return fmts;
}

auto format_time_point(std::chrono::system_clock::time_point tp)
    -> std::string {
  return util::format_iso8601(tp);
}

// Parse XCom token: "xcom.task_id.key" -> (task_id, key)
// Returns nullopt if not a valid xcom token
auto parse_xcom_token(std::string_view token)
    -> Result<std::pair<std::string_view, std::string_view>> {
  constexpr std::string_view prefix = "xcom.";
  if (!token.starts_with(prefix)) {
    return fail(Error::ParseError);
  }

  auto rest = token.substr(prefix.size());
  auto first_dot = boost::algorithm::find_first(rest, ".");
  if (first_dot.empty()) {
    return fail(Error::ParseError);
  }

  const auto dot_pos =
      static_cast<std::size_t>(first_dot.begin() - rest.begin());
  if (dot_pos == 0 || dot_pos >= rest.size() - 1) {
    return fail(Error::ParseError);
  }

  return ok(std::pair{rest.substr(0, dot_pos), rest.substr(dot_pos + 1)});
}

// Find closing }} using char-by-char scan (O(n) guaranteed)
// Returns npos if not found
auto find_closing_braces(std::string_view tmpl, size_t start) -> size_t {
  return tmpl.find("}}", start);
}

using XComWhitelist =
    ankerl::unordered_dense::map<XComLookupKey, const XComPullConfig *,
                                 XComLookupKeyHash>;

auto try_replace_date_token(pmr::string &output, std::string_view token,
                            const DateFormatsPmr &date_fmts) -> bool {
  if (!date_fmts.valid) {
    return false;
  }

  if (token == "ds") {
    output.append(date_fmts.ds);
    return true;
  }
  if (token == "ds_nodash") {
    output.append(date_fmts.ds_nodash);
    return true;
  }
  if (token == "ts") {
    output.append(date_fmts.ts);
    return true;
  }
  if (token == "ts_nodash") {
    output.append(date_fmts.ts_nodash);
    return true;
  }

  return false;
}

auto try_replace_interval_token(pmr::string &output, std::string_view token,
                                const DateFormatsPmr &date_fmts) -> bool {
  if (token == "data_interval_start" &&
      !date_fmts.data_interval_start.empty()) {
    output.append(date_fmts.data_interval_start);
    return true;
  }
  if (token == "data_interval_end" && !date_fmts.data_interval_end.empty()) {
    output.append(date_fmts.data_interval_end);
    return true;
  }
  return false;
}

auto try_replace_metadata_token(pmr::string &output, std::string_view token,
                                const TemplateContext &ctx) -> bool {
  if (token == "dag_id") {
    output.append(ctx.dag_id.value());
    return true;
  }
  if (token == "task_id") {
    output.append(ctx.task_id.value());
    return true;
  }
  if (token == "run_id") {
    output.append(ctx.dag_run_id.value());
    return true;
  }
  return false;
}

auto try_replace_xcom_token(TemplateResolver &resolver, pmr::string &output,
                            std::string_view token, const TemplateContext &ctx,
                            bool strict_whitelist,
                            const XComWhitelist &whitelist) -> Result<bool> {
  auto xcom_parts = parse_xcom_token(token);
  if (!xcom_parts) {
    return ok(false);
  }

  auto const &[task_str, key_str] = *xcom_parts;

  const XComPullConfig *config = nullptr;
  if (strict_whitelist) {
    if (has_control_chars(task_str) || has_control_chars(key_str)) {
      return fail(Error::Unauthorized);
    }
    auto it = whitelist.find(XComLookupKey{.source_task = std::string(task_str),
                                           .key = std::string(key_str)});
    if (it == whitelist.end()) {
      return fail(Error::Unauthorized);
    }
    config = it->second;
  }

  Result<std::optional<std::string>> pull_result =
      ok(std::optional<std::string>(std::nullopt));
  if (config) {
    pull_result = resolver.resolve_xcom_pull(ctx, *config);
  } else {
    XComPullConfig temp_pull{
        .ref = XComRef{.task_id = TaskId{std::string(task_str)},
                       .key = std::string(key_str)},
        .env_var = "",
        .required = true,
        .has_default_value = false};
    pull_result = resolver.resolve_xcom_pull(ctx, temp_pull);
  }

  if (!pull_result) {
    return fail(pull_result.error());
  }

  if (pull_result->has_value()) {
    output.append(**pull_result);
  } else {
    log::warn("Template variable '{{{{xcom.{}.{} }}}}' resolved empty "
              "(optional XCom missing, dag_run_id={})",
              task_str, key_str, ctx.dag_run_id);
  }

  return ok(true);
}

} // namespace

auto TemplateResolver::resolve_template(
    std::string_view tmpl, const TemplateContext &ctx,
    const std::vector<XComPullConfig> &pulls, bool strict_whitelist)
    -> Result<std::string> {

  if (tmpl.empty()) {
    return ok(std::string{});
  }

  auto *resource = current_memory_resource_or_default();

  // Build whitelist lookup for strict mode
  XComWhitelist whitelist;
  whitelist.reserve(pulls.size());
  if (strict_whitelist) {
    for (const auto &pull : pulls) {
      whitelist.emplace(
          XComLookupKey{.source_task = std::string(pull.ref.task_id.value()),
                        .key = pull.ref.key},
          &pull);
    }
  }

  // Pre-compute date formats once using PMR
  auto date_fmts = compute_date_formats_t(
      ctx.execution_date, pmr::polymorphic_allocator<char>(resource));
  date_fmts.data_interval_start =
      pmr::string(format_time_point(ctx.data_interval_start), resource);
  date_fmts.data_interval_end =
      pmr::string(format_time_point(ctx.data_interval_end), resource);

  pmr::string output(resource);
  output.reserve(tmpl.size() * 2);

  size_t pos = 0;
  while (pos < tmpl.size()) {
    // Check for escape sequence: \{{ -> {{
    if (pos + 2 < tmpl.size() && tmpl[pos] == '\\' && tmpl[pos + 1] == '{' &&
        tmpl[pos + 2] == '{') {
      output.append("{{");
      pos += 3;
      continue;
    }

    // Check for template start: {{
    if (pos + 1 < tmpl.size() && tmpl[pos] == '{' && tmpl[pos + 1] == '{') {
      auto close_pos = find_closing_braces(tmpl, pos + 2);
      if (close_pos == std::string_view::npos) {
        output.append("{{");
        pos += 2;
        continue;
      }

      auto token = boost::trim_copy(tmpl.substr(pos + 2, close_pos - pos - 2));
      bool replaced = false;

      replaced = try_replace_date_token(output, token, date_fmts);
      if (!replaced) {
        replaced = try_replace_interval_token(output, token, date_fmts);
      }
      if (!replaced) {
        replaced = try_replace_metadata_token(output, token, ctx);
      }
      if (!replaced) {
        auto xcom_replace = try_replace_xcom_token(*this, output, token, ctx,
                                                   strict_whitelist, whitelist);
        if (!xcom_replace) {
          return fail(xcom_replace.error());
        }
        replaced = *xcom_replace;
      }

      if (replaced) {
        pos = close_pos + 2;
      } else {
        output.append(tmpl.substr(pos, close_pos + 2 - pos));
        pos = close_pos + 2;
      }
      continue;
    }

    output.push_back(tmpl[pos]);
    ++pos;
  }

  return ok(std::string(output));
}

} // namespace dagforge
