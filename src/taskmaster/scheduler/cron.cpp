#include "taskmaster/scheduler/cron.hpp"

#include <array>
#include <cctype>
#include <charconv>
#include <concepts>
#include <ctime>
#include <optional>
#include <ranges>
#include <vector>

namespace taskmaster {
namespace {

// Concept for string_view consuming functions
template <typename Func>
concept StringViewConsumer = std::invocable<Func, std::string_view>;

constexpr std::array<std::pair<std::string_view, std::string_view>, 6> kMacros{{
    {"@yearly", "0 0 1 1 *"},
    {"@annually", "0 0 1 1 *"},
    {"@monthly", "0 0 1 * *"},
    {"@weekly", "0 0 * * 0"},
    {"@daily", "0 0 * * *"},
    {"@hourly", "0 * * * *"},
}};

constexpr std::array<std::string_view, 12> kMonthNames{
    "jan", "feb", "mar", "apr", "may", "jun",
    "jul", "aug", "sep", "oct", "nov", "dec"};

constexpr std::array<std::string_view, 7> kDowNames{"sun", "mon", "tue", "wed",
                                                    "thu", "fri", "sat"};

constexpr auto is_space = [](unsigned char c) { return std::isspace(c) != 0; };

auto trim(std::string_view s) -> std::string_view {
  auto start = std::ranges::find_if_not(s, is_space);
  auto end = std::ranges::find_if_not(s | std::views::reverse, is_space);
  if (start == s.end())
    return {};
  return {start, end.base()};
}

auto iequals(std::string_view a, std::string_view b) noexcept -> bool {
  if (a.size() != b.size())
    return false;
  for (auto [ca, cb] : std::views::zip(a, b)) {
    if (std::tolower(static_cast<unsigned char>(ca)) !=
        std::tolower(static_cast<unsigned char>(cb))) {
      return false;
    }
  }
  return true;
}

auto for_each_split(std::string_view s, char delim,
                    StringViewConsumer auto&& func) -> void {
  size_t start = 0;
  while (start < s.size()) {
    size_t end = s.find(delim, start);
    if (end == std::string_view::npos)
      end = s.size();
    if (end > start) {
      func(s.substr(start, end - start));
    }
    start = end + 1;
  }
}

auto split(std::string_view s, char delim) -> std::vector<std::string_view> {
  std::vector<std::string_view> result;
  for_each_split(s, delim,
                 [&](std::string_view part) { result.push_back(part); });
  return result;
}

auto parse_int(std::string_view s) -> std::optional<int> {
  int value = 0;
  auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), value);
  if (ec == std::errc{} && ptr == s.data() + s.size()) {
    return value;
  }
  return std::nullopt;
}

auto parse_name(std::string_view s,
                const std::array<std::string_view, 12>& names12,
                const std::array<std::string_view, 7>& names7, bool use_12)
    -> std::optional<int> {
  if (use_12) {
    for (auto [i, name] : std::views::enumerate(names12)) {
      if (iequals(s, name)) {
        return static_cast<int>(i + 1);
      }
    }
  } else {
    for (auto [i, name] : std::views::enumerate(names7)) {
      if (iequals(s, name)) {
        return static_cast<int>(i);
      }
    }
  }
  return std::nullopt;
}

template <std::size_t N>
bool parse_field(std::string_view field, std::bitset<N>& bs, int min_val,
                 int max_val, bool& is_restricted,
                 const std::array<std::string_view, 12>* month_names = nullptr,
                 const std::array<std::string_view, 7>* dow_names = nullptr) {
  bs.reset();
  is_restricted = true;

  auto parse_value = [&](std::string_view s) -> std::optional<int> {
    if (auto v = parse_int(s))
      return *v;
    if (month_names) {
      if (auto v = parse_name(s, *month_names, kDowNames, true))
        return *v;
    }
    if (dow_names) {
      if (auto v = parse_name(s, kMonthNames, *dow_names, false)) {
        return (*v == 7) ? 0 : *v;
      }
    }
    return std::nullopt;
  };

  for (auto part : split(field, ',')) {
    part = trim(part);
    if (part.empty())
      return false;

    int step = 1;
    if (auto slash = part.find('/'); slash != std::string_view::npos) {
      auto step_opt = parse_int(part.substr(slash + 1));
      if (!step_opt || *step_opt <= 0)
        return false;
      step = *step_opt;
      part = part.substr(0, slash);
    }

    int start, end;
    if (part == "*" || part == "?") {
      start = min_val;
      end = max_val;
      if (step == 1)
        is_restricted = false;
    } else if (auto dash = part.find('-'); dash != std::string_view::npos) {
      auto a = parse_value(trim(part.substr(0, dash)));
      auto b = parse_value(trim(part.substr(dash + 1)));
      if (!a || !b)
        return false;
      start = *a;
      end = *b;
      if (dow_names && end == 7) {
        for (int v = start; v < 7; v += step) {
          if (v >= min_val && v <= max_val)
            bs.set(static_cast<std::size_t>(v));
        }
        bs.set(0);
        continue;
      }
    } else {
      auto v = parse_value(part);
      if (!v)
        return false;
      start = end = (dow_names && *v == 7) ? 0 : *v;
    }

    if (start < min_val || end > max_val || start > end)
      return false;
    for (int v = start; v <= end; v += step) {
      bs.set(static_cast<std::size_t>(v));
    }
  }
  return bs.any();
}

constexpr int days_in_month(int year, int month) {
  constexpr std::array<int, 12> days{31, 28, 31, 30, 31, 30,
                                     31, 31, 30, 31, 30, 31};
  int d = days[static_cast<std::size_t>(month - 1)];
  if (month == 2 && ((year % 4 == 0 && year % 100 != 0) || year % 400 == 0)) {
    d = 29;
  }
  return d;
}

auto to_utc(std::chrono::system_clock::time_point tp) -> std::tm {
  auto t = std::chrono::system_clock::to_time_t(tp);
  std::tm tm{};
  gmtime_r(&t, &tm);
  return tm;
}

auto from_utc(std::tm& tm) -> std::chrono::system_clock::time_point {
  tm.tm_isdst = 0;
  return std::chrono::system_clock::from_time_t(timegm(&tm));
}

template <std::size_t N>
auto next_set(const std::bitset<N>& bs, int from, int max_val)
    -> std::optional<int> {
  auto range = std::views::iota(from, max_val + 1);
  auto it = std::ranges::find_if(
      range, [&bs](int v) { return bs.test(static_cast<std::size_t>(v)); });
  if (it != range.end()) {
    return *it;
  }
  return std::nullopt;
}

template <std::size_t N>
auto first_set(const std::bitset<N>& bs, int min_val, int max_val) -> int {
  auto range = std::views::iota(min_val, max_val + 1);
  auto it = std::ranges::find_if(
      range, [&bs](int v) { return bs.test(static_cast<std::size_t>(v)); });
  return it != range.end() ? *it : min_val;
}

}  // namespace

CronExpr::CronExpr(std::string raw, Fields fields)
    : raw_(std::move(raw)), fields_(std::move(fields)) {
}

auto CronExpr::parse(std::string_view expr) -> Result<CronExpr> {
  auto trimmed = trim(expr);
  if (trimmed.empty())
    return fail(Error::InvalidArgument);

  std::string_view to_parse = trimmed;
  if (!trimmed.empty() && trimmed[0] == '@') {
    bool found = false;
    for (const auto& [macro, expansion] : kMacros) {
      if (iequals(trimmed, macro)) {
        to_parse = expansion;
        found = true;
        break;
      }
    }
    if (!found)
      return fail(Error::ParseError);
  }

  auto parts = split(to_parse, ' ');
  std::vector<std::string_view> tokens;
  for (auto p : parts) {
    if (auto t = trim(p); !t.empty())
      tokens.push_back(t);
  }
  if (tokens.size() != 5)
    return fail(Error::ParseError);

  Fields f{};
  bool dummy;
  if (!parse_field(tokens[0], f.minute, 0, 59, dummy))
    return fail(Error::ParseError);
  if (!parse_field(tokens[1], f.hour, 0, 23, dummy))
    return fail(Error::ParseError);
  if (!parse_field(tokens[2], f.dom, 1, 31, f.dom_restricted))
    return fail(Error::ParseError);
  if (!parse_field(tokens[3], f.month, 1, 12, dummy, &kMonthNames))
    return fail(Error::ParseError);
  if (!parse_field(tokens[4], f.dow, 0, 6, f.dow_restricted, nullptr,
                   &kDowNames))
    return fail(Error::ParseError);

  return ok(CronExpr(std::string(trimmed), std::move(f)));
}

auto CronExpr::next_after(std::chrono::system_clock::time_point after) const
    -> std::chrono::system_clock::time_point {
  auto tm = to_utc(after + std::chrono::minutes(1));
  tm.tm_sec = 0;

  const int max_year = tm.tm_year + 1900 + 5;

  auto day_ok = [this](const std::tm& t) {
    bool dom_ok = !fields_.dom_restricted ||
                  fields_.dom.test(static_cast<std::size_t>(t.tm_mday));
    bool dow_ok = !fields_.dow_restricted ||
                  fields_.dow.test(static_cast<std::size_t>(t.tm_wday));
    if (!fields_.dom_restricted && !fields_.dow_restricted)
      return true;
    if (!fields_.dom_restricted)
      return dow_ok;
    if (!fields_.dow_restricted)
      return dom_ok;
    return dom_ok || dow_ok;
  };

  while (tm.tm_year + 1900 <= max_year) {
    if (auto m = next_set(fields_.month, tm.tm_mon + 1, 12)) {
      if (*m != tm.tm_mon + 1) {
        tm.tm_mon = *m - 1;
        tm.tm_mday = 1;
        tm.tm_hour = tm.tm_min = 0;
      }
    } else {
      ++tm.tm_year;
      tm.tm_mon = first_set(fields_.month, 1, 12) - 1;
      tm.tm_mday = 1;
      tm.tm_hour = tm.tm_min = 0;
      continue;
    }

    int dim = days_in_month(tm.tm_year + 1900, tm.tm_mon + 1);
    if (tm.tm_mday > dim) {
      ++tm.tm_mon;
      tm.tm_mday = 1;
      tm.tm_hour = tm.tm_min = 0;
      continue;
    }

    tm = to_utc(from_utc(tm));
    if (!day_ok(tm)) {
      ++tm.tm_mday;
      tm.tm_hour = tm.tm_min = 0;
      tm = to_utc(from_utc(tm));
      continue;
    }

    if (auto h = next_set(fields_.hour, tm.tm_hour, 23)) {
      if (*h != tm.tm_hour) {
        tm.tm_hour = *h;
        tm.tm_min = 0;
      }
    } else {
      ++tm.tm_mday;
      tm.tm_hour = first_set(fields_.hour, 0, 23);
      tm.tm_min = 0;
      tm = to_utc(from_utc(tm));
      continue;
    }

    if (auto m = next_set(fields_.minute, tm.tm_min, 59)) {
      tm.tm_min = *m;
    } else {
      ++tm.tm_hour;
      tm.tm_min = first_set(fields_.minute, 0, 59);
      tm = to_utc(from_utc(tm));
      continue;
    }

    tm = to_utc(from_utc(tm));
    if (day_ok(tm)) {
      return from_utc(tm);
    }
    ++tm.tm_mday;
    tm.tm_hour = tm.tm_min = 0;
    tm = to_utc(from_utc(tm));
  }

  return std::chrono::system_clock::time_point::max();
}

auto CronExpr::all_between(std::chrono::system_clock::time_point start,
                           std::chrono::system_clock::time_point end,
                           size_t max_count) const
    -> std::vector<std::chrono::system_clock::time_point> {
  std::vector<std::chrono::system_clock::time_point> result;
  result.reserve(std::min(max_count, size_t{64}));

  auto current = start - std::chrono::minutes(1);
  while (result.size() < max_count) {
    current = next_after(current);
    if (current >= end || current == std::chrono::system_clock::time_point::max()) {
      break;
    }
    result.push_back(current);
  }

  return result;
}

}  // namespace taskmaster
