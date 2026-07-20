#include "json_adapter.hpp"

#include <memory>
#include <utility>
#include <vector>

namespace dagforge::jsonata::detail {
namespace {

[[nodiscard]] auto exceeds(std::size_t current, std::size_t increment,
                           std::size_t limit) noexcept -> bool {
  return increment > limit || current > limit - increment;
}

} // namespace

auto import_json(const JsonValue &json, const EvaluationLimits &limits,
                 std::stop_token stop_token,
                 std::chrono::steady_clock::time_point deadline,
                 std::string_view source, std::size_t byte_offset)
    -> Result<Value> {
  struct Frame {
    const JsonValue *source{};
    Value *target{};
    bool finalize{false};
  };

  const auto interrupted = [&]() -> Result<void> {
    if (stop_token.stop_requested()) {
      return std::unexpected(host_failure(
          "H1001", "JSONata evaluation cancelled", source, byte_offset));
    }
    if (limits.timeout > std::chrono::steady_clock::duration::zero() &&
        std::chrono::steady_clock::now() > deadline) {
      return std::unexpected(dynamic_failure(
          "D1012", "JSONata evaluation timeout exceeded", source, byte_offset));
    }
    return {};
  };

  auto initial_interrupt = interrupted();
  if (!initial_interrupt) {
    return std::unexpected(initial_interrupt.error());
  }

  std::size_t nodes{};
  std::size_t string_bytes{};
  std::size_t visits_until_interrupt{1024};
  const auto charge_node = [&]() -> Result<void> {
    if (exceeds(nodes, 1, limits.max_value_nodes)) {
      return std::unexpected(
          host_failure("H2100", "JSONata value graph node limit exceeded",
                       source, byte_offset));
    }
    ++nodes;
    return {};
  };
  const auto charge_string = [&](std::size_t bytes) -> Result<void> {
    if (exceeds(string_bytes, bytes, limits.max_string_bytes)) {
      return std::unexpected(
          host_failure("H2101", "JSONata value string byte limit exceeded",
                       source, byte_offset));
    }
    string_bytes += bytes;
    return {};
  };
  const auto check_interrupt_periodically = [&]() -> Result<void> {
    if (--visits_until_interrupt != 0) {
      return {};
    }
    visits_until_interrupt = 1024;
    return interrupted();
  };

  Value result;
  std::vector<Frame> stack{{.source = &json, .target = &result}};
  while (!stack.empty()) {
    auto frame = stack.back();
    stack.pop_back();
    if (frame.finalize) {
      if (auto *array =
              std::get_if<std::shared_ptr<Array>>(&frame.target->storage);
          array != nullptr && *array) {
        recompute_footprint(**array);
      } else if (auto *object = std::get_if<std::shared_ptr<Object>>(
                     &frame.target->storage);
                 object != nullptr && *object) {
        recompute_footprint(**object);
      }
      continue;
    }

    auto interrupt = check_interrupt_periodically();
    if (!interrupt) {
      return std::unexpected(interrupt.error());
    }
    auto node_charge = charge_node();
    if (!node_charge) {
      return std::unexpected(node_charge.error());
    }

    const auto &input = *frame.source;
    if (input.is_null()) {
      *frame.target = Value{nullptr};
      continue;
    }
    if (input.is_boolean()) {
      *frame.target = Value{input.get_boolean()};
      continue;
    }
    if (input.is_number()) {
      *frame.target = Value{input.as_number()};
      continue;
    }
    if (input.is_string()) {
      auto string_charge = charge_string(input.get_string().size());
      if (!string_charge) {
        return std::unexpected(string_charge.error());
      }
      *frame.target = Value{input.get_string()};
      continue;
    }
    if (input.is_array()) {
      const auto size = input.get_array().size();
      if (exceeds(nodes, size, limits.max_value_nodes)) {
        return std::unexpected(
            host_failure("H2100", "JSONata value graph node limit exceeded",
                         source, byte_offset));
      }
      auto array = std::make_shared<Array>();
      array->constructed = false;
      array->values.resize(size);
      frame.target->storage = array;
      stack.push_back(Frame{.target = frame.target, .finalize = true});
      for (std::size_t index = size; index > 0; --index) {
        stack.push_back(Frame{.source = &input.get_array()[index - 1],
                              .target = &array->values[index - 1]});
      }
      continue;
    }

    const auto size = input.get_object().size();
    if (exceeds(nodes, size, limits.max_value_nodes)) {
      return std::unexpected(
          host_failure("H2100", "JSONata value graph node limit exceeded",
                       source, byte_offset));
    }
    for (const auto &[key, value] : input.get_object()) {
      (void)value;
      auto string_charge = charge_string(key.size());
      if (!string_charge) {
        return std::unexpected(string_charge.error());
      }
      auto interrupt_check = check_interrupt_periodically();
      if (!interrupt_check) {
        return std::unexpected(interrupt_check.error());
      }
    }

    auto object = std::make_shared<Object>();
    object->members.reserve(size);
    std::vector<const JsonValue *> children;
    children.reserve(size);
    for (const auto &[key, value] : input.get_object()) {
      object->members.emplace_back(key, Value{});
      children.push_back(&value);
    }
    frame.target->storage = object;
    stack.push_back(Frame{.target = frame.target, .finalize = true});
    for (std::size_t index = children.size(); index > 0; --index) {
      stack.push_back(Frame{.source = children[index - 1],
                            .target = &object->members[index - 1].second});
    }
  }
  return result;
}

} // namespace dagforge::jsonata::detail
