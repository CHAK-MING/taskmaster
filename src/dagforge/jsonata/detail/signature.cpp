#include "model.hpp"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

namespace dagforge::jsonata::detail {

namespace {

class SignatureParser {
public:
  SignatureParser(std::string_view signature, std::string_view source,
                  std::size_t byte_offset)
      : signature_(signature), source_(source), byte_offset_(byte_offset) {}

  [[nodiscard]] auto parse()
      -> Result<std::shared_ptr<const FunctionSignature>> {
    if (!consume('<')) {
      return malformed("Function signature must begin with '<'", "S0402");
    }
    auto parsed = parse_body();
    if (!parsed) {
      return std::unexpected(parsed.error());
    }
    if (!consume('>') || index_ != signature_.size()) {
      return malformed("Malformed function signature", "S0402");
    }
    return std::make_shared<const FunctionSignature>(std::move(*parsed));
  }

private:
  [[nodiscard]] auto parse_body() -> Result<FunctionSignature> {
    FunctionSignature result;
    while (index_ < signature_.size() && signature_[index_] != ':' &&
           signature_[index_] != '>') {
      auto type = parse_type();
      if (!type) {
        return std::unexpected(type.error());
      }
      SignatureParameter parameter{.type = std::move(*type)};
      if (index_ < signature_.size()) {
        switch (signature_[index_]) {
        case '?':
          parameter.optional = true;
          ++index_;
          break;
        case '+':
          parameter.variadic = true;
          ++index_;
          break;
        case '-':
          parameter.context_default = true;
          ++index_;
          break;
        default:
          break;
        }
      }
      result.parameters.push_back(std::move(parameter));
    }
    if (consume(':')) {
      auto type = parse_type();
      if (!type) {
        return std::unexpected(type.error());
      }
      result.result = std::move(*type);
    } else {
      result.result = SignatureType{.kind = SignatureTypeKind::Any};
    }
    return result;
  }

  [[nodiscard]] auto parse_type() -> Result<SignatureType> {
    if (index_ >= signature_.size()) {
      return malformed("Missing type in function signature", "S0402");
    }
    if (signature_[index_] == '(') {
      ++index_;
      SignatureType result{.kind = SignatureTypeKind::Choice};
      while (index_ < signature_.size() && signature_[index_] != ')') {
        auto alternative = parse_type();
        if (!alternative) {
          return std::unexpected(alternative.error());
        }
        result.alternatives.push_back(std::move(*alternative));
      }
      if (result.alternatives.empty() || !consume(')')) {
        return malformed("Malformed choice type in function signature",
                         "S0402");
      }
      return result;
    }

    const auto symbol = signature_[index_++];
    SignatureType result;
    switch (symbol) {
    case 'x':
      result.kind = SignatureTypeKind::Any;
      break;
    case 'j':
      result.kind = SignatureTypeKind::Json;
      break;
    case 's':
      result.kind = SignatureTypeKind::String;
      break;
    case 'n':
      result.kind = SignatureTypeKind::Number;
      break;
    case 'b':
      result.kind = SignatureTypeKind::Boolean;
      break;
    case 'l':
      result.kind = SignatureTypeKind::Null;
      break;
    case 'o':
      result.kind = SignatureTypeKind::Object;
      break;
    case 'a':
      result.kind = SignatureTypeKind::Array;
      break;
    case 'f':
      result.kind = SignatureTypeKind::Function;
      break;
    default:
      return malformed("Unknown type in function signature", "S0402");
    }

    if (index_ < signature_.size() && signature_[index_] == '<') {
      if (result.kind == SignatureTypeKind::Array) {
        ++index_;
        auto element = parse_type();
        if (!element) {
          return std::unexpected(element.error());
        }
        if (!consume('>')) {
          return malformed("Unclosed array type in function signature",
                           "S0402");
        }
        result.element =
            std::make_shared<const SignatureType>(std::move(*element));
      } else if (result.kind == SignatureTypeKind::Function) {
        ++index_;
        auto nested = parse_body();
        if (!nested) {
          return std::unexpected(nested.error());
        }
        if (!consume('>')) {
          return malformed("Unclosed function type in function signature",
                           "S0402");
        }
      } else {
        return malformed(
            "Type parameters are only valid for arrays and functions", "S0401");
      }
    }
    return result;
  }

  [[nodiscard]] auto consume(char expected) noexcept -> bool {
    if (index_ >= signature_.size() || signature_[index_] != expected) {
      return false;
    }
    ++index_;
    return true;
  }

  [[nodiscard]] auto malformed(std::string_view message,
                               std::string_view code) const
      -> std::unexpected<Failure> {
    return std::unexpected(
        syntax_failure(std::string{code}, std::string{message}, source_,
                       byte_offset_ + std::min(index_, signature_.size())));
  }

  std::string_view signature_;
  std::string_view source_;
  std::size_t byte_offset_{};
  std::size_t index_{};
};

[[nodiscard]] auto is_function_value(const Value &value) noexcept -> bool {
  return std::holds_alternative<std::shared_ptr<Function>>(value.storage) ||
         std::holds_alternative<std::shared_ptr<RegexValue>>(value.storage);
}

[[nodiscard]] auto coerce_argument(const Value &raw, const SignatureType &type)
    -> std::optional<Value> {
  const auto value = normalize_sequence(raw);
  if (is_undefined(value)) {
    return value;
  }
  switch (type.kind) {
  case SignatureTypeKind::Any:
    return value;
  case SignatureTypeKind::Json:
    return is_function_value(value) ? std::nullopt
                                    : std::optional<Value>{value};
  case SignatureTypeKind::String:
    return std::holds_alternative<std::string>(value.storage)
               ? std::optional<Value>{value}
               : std::nullopt;
  case SignatureTypeKind::Number:
    return std::holds_alternative<double>(value.storage)
               ? std::optional<Value>{value}
               : std::nullopt;
  case SignatureTypeKind::Boolean:
    return std::holds_alternative<bool>(value.storage)
               ? std::optional<Value>{value}
               : std::nullopt;
  case SignatureTypeKind::Null:
    return std::holds_alternative<std::nullptr_t>(value.storage)
               ? std::optional<Value>{value}
               : std::nullopt;
  case SignatureTypeKind::Object:
    return std::holds_alternative<std::shared_ptr<Object>>(value.storage)
               ? std::optional<Value>{value}
               : std::nullopt;
  case SignatureTypeKind::Function:
    return is_function_value(value) ? std::optional<Value>{value}
                                    : std::nullopt;
  case SignatureTypeKind::Choice:
    for (const auto &alternative : type.alternatives) {
      if (auto converted = coerce_argument(value, alternative)) {
        return converted;
      }
    }
    return std::nullopt;
  case SignatureTypeKind::Array: {
    std::vector<Value> items;
    if (const auto *array =
            std::get_if<std::shared_ptr<Array>>(&value.storage)) {
      items = (*array)->values;
    } else if (is_sequence(value)) {
      items = as_sequence(value)->values;
    } else {
      items.push_back(value);
    }
    if (type.element) {
      for (auto &item : items) {
        auto converted = coerce_argument(item, *type.element);
        if (!converted) {
          return std::nullopt;
        }
        item = std::move(*converted);
      }
    }
    return make_array(std::move(items));
  }
  }
  return std::nullopt;
}

[[nodiscard]] auto contains_parameterized_array(const SignatureType &type)
    -> bool {
  if (type.kind == SignatureTypeKind::Array && type.element) {
    return true;
  }
  if (type.kind == SignatureTypeKind::Choice) {
    return std::ranges::any_of(type.alternatives, contains_parameterized_array);
  }
  return false;
}

struct MatchState {
  std::span<const Value> arguments;
  const Value &context;
  std::vector<Value> output;
};

struct MatchDiagnostics {
  bool array_mismatch{false};
  bool context_mismatch{false};
};

[[nodiscard]] auto match_parameters(const FunctionSignature &signature,
                                    std::size_t parameter_index,
                                    std::size_t argument_index,
                                    bool context_used, MatchState state,
                                    MatchDiagnostics &diagnostics)
    -> std::optional<MatchState> {
  if (parameter_index == signature.parameters.size()) {
    return argument_index == state.arguments.size()
               ? std::optional<MatchState>{std::move(state)}
               : std::nullopt;
  }
  const auto &parameter = signature.parameters[parameter_index];

  if (parameter.variadic) {
    std::size_t remaining_required = 0;
    for (std::size_t index = parameter_index + 1;
         index < signature.parameters.size(); ++index) {
      const auto &remaining = signature.parameters[index];
      if (!remaining.optional && !remaining.context_default) {
        ++remaining_required;
      }
    }
    const auto available = state.arguments.size() - argument_index;
    const auto maximum =
        available >= remaining_required ? available - remaining_required : 0U;
    for (std::size_t count = maximum; count >= 1; --count) {
      auto candidate = state;
      bool valid = true;
      for (std::size_t offset = 0; offset < count; ++offset) {
        auto converted = coerce_argument(
            candidate.arguments[argument_index + offset], parameter.type);
        if (!converted) {
          diagnostics.array_mismatch |=
              contains_parameterized_array(parameter.type);
          valid = false;
          break;
        }
        candidate.output.push_back(std::move(*converted));
      }
      if (valid) {
        if (auto matched = match_parameters(
                signature, parameter_index + 1, argument_index + count,
                context_used, std::move(candidate), diagnostics)) {
          return matched;
        }
      }
      if (count == 1) {
        break;
      }
    }
    return std::nullopt;
  }

  if (argument_index < state.arguments.size()) {
    auto converted =
        coerce_argument(state.arguments[argument_index], parameter.type);
    if (converted) {
      auto candidate = state;
      candidate.output.push_back(std::move(*converted));
      if (auto matched = match_parameters(signature, parameter_index + 1,
                                          argument_index + 1, context_used,
                                          std::move(candidate), diagnostics)) {
        return matched;
      }
    } else {
      diagnostics.array_mismatch |=
          contains_parameterized_array(parameter.type);
    }
  }

  if (parameter.context_default && !context_used) {
    if (auto converted = coerce_argument(state.context, parameter.type)) {
      auto candidate = state;
      candidate.output.push_back(std::move(*converted));
      if (auto matched =
              match_parameters(signature, parameter_index + 1, argument_index,
                               true, std::move(candidate), diagnostics)) {
        return matched;
      }
    } else {
      MatchDiagnostics tail_diagnostics;
      auto tail_state = state;
      if (match_parameters(signature, parameter_index + 1, argument_index, true,
                           std::move(tail_state), tail_diagnostics)) {
        diagnostics.context_mismatch = true;
      }
    }
  }

  if (parameter.optional) {
    auto candidate = state;
    if (auto matched =
            match_parameters(signature, parameter_index + 1, argument_index,
                             context_used, std::move(candidate), diagnostics)) {
      return matched;
    }
  }
  return std::nullopt;
}

} // namespace

auto parse_function_signature(std::string_view signature,
                              std::string_view source, std::size_t byte_offset)
    -> Result<std::shared_ptr<const FunctionSignature>> {
  return SignatureParser(signature, source, byte_offset).parse();
}

auto validate_function_arguments(const FunctionSignature &signature,
                                 std::span<const Value> arguments,
                                 const Value &context, std::string_view source,
                                 std::size_t byte_offset)
    -> Result<std::vector<Value>> {
  MatchState initial{.arguments = arguments, .context = context};
  MatchDiagnostics diagnostics;
  auto matched =
      match_parameters(signature, 0, 0, false, std::move(initial), diagnostics);
  if (!matched) {
    return std::unexpected(
        type_failure(diagnostics.context_mismatch ? "T0411"
                     : diagnostics.array_mismatch ? "T0412"
                                                  : "T0410",
                     diagnostics.context_mismatch
                         ? "Context value does not match function signature"
                     : diagnostics.array_mismatch
                         ? "Argument does not match the array element type"
                         : "Argument does not match function signature",
                     source, byte_offset));
  }
  return std::move(matched->output);
}

} // namespace dagforge::jsonata::detail
