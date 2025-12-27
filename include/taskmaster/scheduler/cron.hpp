#pragma once

#include "taskmaster/core/error.hpp"

#include <bitset>
#include <chrono>
#include <string>
#include <string_view>

namespace taskmaster {

class CronExpr {
public:
  CronExpr() = default;

  [[nodiscard]] static auto parse(std::string_view expr) -> Result<CronExpr>;

  [[nodiscard]] auto
  next_after(std::chrono::system_clock::time_point after) const
      -> std::chrono::system_clock::time_point;

  [[nodiscard]] auto raw() const noexcept -> std::string_view {
    return raw_;
  }

private:
  using MinSet = std::bitset<60>;
  using HourSet = std::bitset<24>;
  using DomSet = std::bitset<32>;
  using MonSet = std::bitset<13>;
  using DowSet = std::bitset<8>;

  struct Fields {
    MinSet minute;
    HourSet hour;
    DomSet dom;
    MonSet month;
    DowSet dow;
    bool dom_restricted{false};
    bool dow_restricted{false};
  };

  CronExpr(std::string raw, Fields fields);

  std::string raw_;
  Fields fields_;
};

}  // namespace taskmaster
