#include "parser.hpp"

#include "path_ast.hpp"
#include "unicode.hpp"

#include <format>
#include <limits>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace dagforge::jsonata::detail {

namespace {

struct CompileFootprint {
  std::size_t string_bytes{};
  std::size_t program_bytes{sizeof(ProgramData)};
};

auto add_bytes(std::size_t &target, std::size_t value) noexcept -> void {
  target = value > std::numeric_limits<std::size_t>::max() - target
               ? std::numeric_limits<std::size_t>::max()
               : target + value;
}

auto add_owned_string(CompileFootprint &footprint,
                      const std::string &value) noexcept -> void {
  add_bytes(footprint.string_bytes, value.size());
  add_bytes(footprint.program_bytes, value.capacity());
}

auto add_signature_type(CompileFootprint &footprint,
                        const SignatureType &type) noexcept -> void {
  add_bytes(footprint.program_bytes, sizeof(SignatureType));
  add_bytes(footprint.program_bytes,
            type.alternatives.capacity() * sizeof(SignatureType));
  if (type.element) {
    add_signature_type(footprint, *type.element);
  }
  for (const auto &alternative : type.alternatives) {
    add_signature_type(footprint, alternative);
  }
}

[[nodiscard]] auto compile_footprint(const ProgramData &program) noexcept
    -> CompileFootprint {
  CompileFootprint footprint;
  add_bytes(footprint.program_bytes, program.source.capacity());
  add_bytes(footprint.program_bytes, program.nodes.capacity() * sizeof(Node));
  for (const auto &node : program.nodes) {
    add_owned_string(footprint, node.text);
    if (const auto *literal = std::get_if<std::string>(&node.literal)) {
      add_owned_string(footprint, *literal);
    }
    add_bytes(footprint.program_bytes,
              node.children.capacity() * sizeof(NodeId));
    add_bytes(footprint.program_bytes,
              node.pairs.capacity() * sizeof(std::pair<NodeId, NodeId>));
    add_bytes(footprint.program_bytes, node.flags.capacity() * sizeof(bool));
    add_bytes(footprint.program_bytes,
              node.path_steps.capacity() * sizeof(PathStep));
    for (const auto &step : node.path_steps) {
      add_bytes(footprint.program_bytes,
                step.stages.capacity() * sizeof(PathStage));
      for (const auto &stage : step.stages) {
        add_owned_string(footprint, stage.name);
      }
      if (step.focus_binding) {
        add_owned_string(footprint, *step.focus_binding);
      }
      if (step.index_binding) {
        add_owned_string(footprint, *step.index_binding);
      }
    }
    if (node.signature) {
      add_bytes(footprint.program_bytes, sizeof(FunctionSignature));
      add_bytes(footprint.program_bytes, node.signature->parameters.capacity() *
                                             sizeof(SignatureParameter));
      for (const auto &parameter : node.signature->parameters) {
        add_signature_type(footprint, parameter.type);
      }
      add_signature_type(footprint, node.signature->result);
    }
  }
  return footprint;
}

auto mark_tail_position(ProgramData &program, NodeId id) -> void {
  if (id == kInvalidNode || id >= program.nodes.size()) {
    return;
  }
  auto &current = program.nodes[id];
  if (current.kind == NodeKind::Call) {
    current.tail_call = true;
    return;
  }
  if (current.kind == NodeKind::Conditional) {
    if (current.children.size() >= 2) {
      mark_tail_position(program, current.children[1]);
    }
    if (current.children.size() >= 3) {
      mark_tail_position(program, current.children[2]);
    }
    return;
  }
  if (current.kind == NodeKind::Block && !current.children.empty()) {
    mark_tail_position(program, current.children.back());
  }
}

auto mark_lambda_tail_calls(ProgramData &program) -> void {
  for (std::size_t index = 0; index < program.nodes.size(); ++index) {
    const auto &current = program.nodes[index];
    if (current.kind == NodeKind::Lambda && !current.children.empty()) {
      mark_tail_position(program, current.children.front());
    }
  }
}

} // namespace

Parser::Parser(std::string source, CompileLimits limits,
               std::optional<CompileInterrupt> interrupt)
    : data_{.source = std::move(source), .compile_limits = limits},
      interrupt_(std::move(interrupt)),
      lexer_(data_.source, limits, interrupt_ ? &*interrupt_ : nullptr),
      limits_(limits) {}

auto Parser::check_interrupt() const -> Result<void> {
  if (!interrupt_) {
    return {};
  }
  if (interrupt_->stop_token.stop_requested()) {
    return std::unexpected(host_failure("H1001", "JSONata evaluation cancelled",
                                        interrupt_->diagnostic_source,
                                        interrupt_->diagnostic_byte_offset));
  }
  if (interrupt_->deadline &&
      std::chrono::steady_clock::now() > *interrupt_->deadline) {
    return std::unexpected(dynamic_failure(
        "D1012", "JSONata evaluation timeout exceeded",
        interrupt_->diagnostic_source, interrupt_->diagnostic_byte_offset));
  }
  return {};
}

auto Parser::parse() -> Result<ProgramData> {
  auto interrupted = check_interrupt();
  if (!interrupted) {
    return std::unexpected(interrupted.error());
  }
  if (data_.source.size() > limits_.max_source_bytes) {
    return std::unexpected(host_failure(
        "H1101", "JSONata source byte limit exceeded", data_.source));
  }
  if (const auto invalid = invalid_utf8_offset(data_.source)) {
    return std::unexpected(host_failure(
        "H1107", "JSONata source is not valid UTF-8", data_.source, *invalid));
  }
  auto advanced = advance(true);
  if (!advanced) {
    return std::unexpected(advanced.error());
  }
  auto expression = parse_expression(0, 0);
  if (!expression) {
    return std::unexpected(expression.error());
  }
  if (current_.kind != TokenKind::End) {
    return std::unexpected(syntax_failure("S0201", "Syntax error", data_.source,
                                          current_.span.end, current_.text));
  }
  data_.root = *expression;
  interrupted = check_interrupt();
  if (!interrupted) {
    return std::unexpected(interrupted.error());
  }
  auto lowered = lower_path_ast(data_, limits_);
  if (!lowered) {
    return std::unexpected(lowered.error());
  }
  interrupted = check_interrupt();
  if (!interrupted) {
    return std::unexpected(interrupted.error());
  }
  mark_lambda_tail_calls(data_);
  const auto footprint = compile_footprint(data_);
  if (footprint.string_bytes > limits_.max_string_bytes) {
    return std::unexpected(host_failure(
        "H1105", "JSONata owned string byte limit exceeded", data_.source));
  }
  if (footprint.program_bytes > limits_.max_program_bytes) {
    return std::unexpected(host_failure(
        "H1106", "JSONata compiled program byte limit exceeded", data_.source));
  }
  return std::move(data_);
}

auto Parser::add_node(Node node_value) -> Result<NodeId> {
  auto interrupted = check_interrupt();
  if (!interrupted) {
    return std::unexpected(interrupted.error());
  }
  if (data_.nodes.size() >= limits_.max_nodes) {
    return std::unexpected(host_failure("H1103",
                                        "JSONata syntax node limit exceeded",
                                        data_.source, node_value.span.end));
  }
  data_.nodes.push_back(std::move(node_value));
  return static_cast<NodeId>(data_.nodes.size() - 1);
}

auto Parser::advance(bool prefix) -> Result<void> {
  auto token = lexer_.next(prefix);
  if (!token) {
    return std::unexpected(token.error());
  }
  current_ = std::move(*token);
  return {};
}

auto Parser::parse_expression(int right_binding_power, std::size_t depth)
    -> Result<NodeId> {
  if (depth > limits_.max_nesting_depth) {
    return std::unexpected(host_failure("H1104",
                                        "JSONata nesting depth limit exceeded",
                                        data_.source, current_.span.begin));
  }
  const auto token = current_;
  const bool next_is_prefix =
      token.kind == TokenKind::Operator &&
      (token.text == "-" || token.text == "(" || token.text == "[" ||
       token.text == "{" || token.text == "|");
  auto advanced = advance(next_is_prefix);
  if (!advanced) {
    return std::unexpected(advanced.error());
  }
  auto left = parse_prefix(token, depth + 1);
  if (!left) {
    return std::unexpected(left.error());
  }

  while (right_binding_power < binding_power(current_)) {
    const auto infix = current_;
    auto next = advance(true);
    if (!next) {
      return std::unexpected(next.error());
    }
    left = parse_infix(infix, *left, depth + 1);
    if (!left) {
      return std::unexpected(left.error());
    }
  }
  return left;
}

auto Parser::parse_prefix(const Token &token, std::size_t depth)
    -> Result<NodeId> {
  switch (token.kind) {
  case TokenKind::Number:
    return add_node(Node{.kind = NodeKind::Literal,
                         .span = token.span,
                         .literal = token.number});
  case TokenKind::String:
    return add_node(Node{
        .kind = NodeKind::Literal, .span = token.span, .literal = token.text});
  case TokenKind::Value:
    if (token.text == "true") {
      return add_node(
          Node{.kind = NodeKind::Literal, .span = token.span, .literal = true});
    }
    if (token.text == "false") {
      return add_node(Node{
          .kind = NodeKind::Literal, .span = token.span, .literal = false});
    }
    return add_node(Node{
        .kind = NodeKind::Literal, .span = token.span, .literal = nullptr});
  case TokenKind::Name:
    if ((token.text == "function" || token.text == "λ") &&
        current_.text == "(") {
      return parse_lambda(token, depth);
    }
    return add_node(
        Node{.kind = NodeKind::Name, .span = token.span, .text = token.text});
  case TokenKind::Variable:
    return add_node(Node{
        .kind = NodeKind::Variable, .span = token.span, .text = token.text});
  case TokenKind::Regex:
    return add_node(
        Node{.kind = NodeKind::Regex, .span = token.span, .text = token.text});
  case TokenKind::Operator:
    if (token.text == "and" || token.text == "or" || token.text == "in") {
      return add_node(
          Node{.kind = NodeKind::Name, .span = token.span, .text = token.text});
    }
    break;
  case TokenKind::End:
    return std::unexpected(syntax_failure(
        "S0207", "Unexpected end of expression", data_.source, token.span.end));
  }

  if (token.text == "-") {
    auto operand = parse_expression(70, depth);
    if (!operand) {
      return std::unexpected(operand.error());
    }
    return add_node(Node{.kind = NodeKind::Unary,
                         .span = ByteSpan{.begin = token.span.begin,
                                          .end = node(*operand).span.end},
                         .text = "-",
                         .children = {*operand}});
  }
  if (token.text == "(") {
    return parse_block(token, depth);
  }
  if (token.text == "[") {
    return parse_array(token, depth);
  }
  if (token.text == "{") {
    return parse_object(token, depth, kInvalidNode);
  }
  if (token.text == "*") {
    return add_node(Node{.kind = NodeKind::Wildcard, .span = token.span});
  }
  if (token.text == "**") {
    return add_node(Node{.kind = NodeKind::Descendant, .span = token.span});
  }
  if (token.text == "%") {
    return add_node(Node{.kind = NodeKind::Parent, .span = token.span});
  }
  if (token.text == "?") {
    return add_node(Node{.kind = NodeKind::Placeholder, .span = token.span});
  }
  if (token.text == "|") {
    return parse_transform(token, depth);
  }
  return std::unexpected(
      syntax_failure("S0211", "The symbol cannot be used as a unary operator",
                     data_.source, token.span.end, token.text));
}

auto Parser::parse_infix(const Token &token, NodeId left, std::size_t depth)
    -> Result<NodeId> {
  if (token.text == "[") {
    if (current_.text == "]") {
      auto close = consume("]", false);
      if (!close) {
        return std::unexpected(close.error());
      }
      return add_node(Node{.kind = NodeKind::Filter,
                           .span = ByteSpan{.begin = node(left).span.begin,
                                            .end = close->span.end},
                           .children = {left}});
    }
    auto predicate = parse_expression(0, depth);
    if (!predicate) {
      return std::unexpected(predicate.error());
    }
    auto close = consume("]", false);
    if (!close) {
      return std::unexpected(close.error());
    }
    return add_node(Node{.kind = NodeKind::Filter,
                         .span = ByteSpan{.begin = node(left).span.begin,
                                          .end = close->span.end},
                         .children = {left, *predicate}});
  }
  if (token.text == "(") {
    return parse_call(token, left, depth);
  }
  if (token.text == "{") {
    return parse_object(token, depth, left);
  }
  if (token.text == "^") {
    return parse_sort(token, left, depth);
  }
  if (token.text == "#" || token.text == "@") {
    if (current_.kind != TokenKind::Variable || current_.text.empty()) {
      return std::unexpected(syntax_failure(
          "S0214", "The right side of the binding operator must be a variable",
          data_.source, current_.span.end, current_.text));
    }
    const auto variable = current_;
    auto next = advance(false);
    if (!next) {
      return std::unexpected(next.error());
    }
    return add_node(Node{
        .kind = token.text == "#" ? NodeKind::IndexBind : NodeKind::FocusBind,
        .span =
            ByteSpan{.begin = node(left).span.begin, .end = variable.span.end},
        .text = variable.text,
        .children = {left}});
  }
  if (token.text == "?") {
    auto consequent = parse_expression(0, depth);
    if (!consequent) {
      return std::unexpected(consequent.error());
    }
    Node conditional{.kind = NodeKind::Conditional,
                     .span = ByteSpan{.begin = node(left).span.begin,
                                      .end = node(*consequent).span.end},
                     .children = {left, *consequent}};
    if (current_.text == ":") {
      auto next = advance(true);
      if (!next) {
        return std::unexpected(next.error());
      }
      auto alternative = parse_expression(0, depth);
      if (!alternative) {
        return std::unexpected(alternative.error());
      }
      conditional.children.push_back(*alternative);
      conditional.span.end = node(*alternative).span.end;
    }
    return add_node(std::move(conditional));
  }
  if (token.text == ":=") {
    if (node(left).kind != NodeKind::Variable || node(left).text.empty() ||
        node(left).text == "$") {
      return std::unexpected(
          syntax_failure("S0212", "The left side of := must be a variable name",
                         data_.source, node(left).span.end));
    }
    auto right = parse_expression(binding_power(token) - 1, depth);
    if (!right) {
      return std::unexpected(right.error());
    }
    return add_node(Node{.kind = NodeKind::Bind,
                         .span = ByteSpan{.begin = node(left).span.begin,
                                          .end = node(*right).span.end},
                         .text = node(left).text,
                         .children = {*right}});
  }

  const auto rbp = token.text == "??" || token.text == "?:"
                       ? binding_power(token) - 1
                       : binding_power(token);
  auto right = parse_expression(rbp, depth);
  if (!right) {
    return std::unexpected(right.error());
  }
  if (token.text == "." && node(*right).kind == NodeKind::Literal &&
      std::holds_alternative<std::string>(node(*right).literal)) {
    auto selector = add_node(Node{
        .kind = NodeKind::Name,
        .span = node(*right).span,
        .text = std::get<std::string>(node(*right).literal),
    });
    if (!selector) {
      return std::unexpected(selector.error());
    }
    right = selector;
  }
  return add_node(Node{.kind = NodeKind::Binary,
                       .span = ByteSpan{.begin = node(left).span.begin,
                                        .end = node(*right).span.end},
                       .text = token.text,
                       .children = {left, *right}});
}

auto Parser::parse_block(const Token &open, std::size_t depth)
    -> Result<NodeId> {
  std::vector<NodeId> expressions;
  if (current_.text == ")") {
    auto close = consume(")", false);
    if (!close) {
      return std::unexpected(close.error());
    }
    return add_node(Node{
        .kind = NodeKind::Undefined,
        .span = ByteSpan{.begin = open.span.begin, .end = close->span.end}});
  }
  if (current_.text != ")") {
    while (true) {
      auto expression = parse_expression(0, depth);
      if (!expression) {
        return std::unexpected(expression.error());
      }
      expressions.push_back(*expression);
      if (current_.text != ";") {
        break;
      }
      auto next = advance(true);
      if (!next) {
        return std::unexpected(next.error());
      }
      if (current_.text == ")") {
        break;
      }
    }
  }
  auto close = consume(")", false);
  if (!close) {
    return std::unexpected(close.error());
  }
  return add_node(
      Node{.kind = NodeKind::Block,
           .span = ByteSpan{.begin = open.span.begin, .end = close->span.end},
           .children = std::move(expressions)});
}

auto Parser::parse_array(const Token &open, std::size_t depth)
    -> Result<NodeId> {
  std::vector<NodeId> items;
  if (current_.text != "]") {
    while (true) {
      auto item = parse_expression(0, depth);
      if (!item) {
        return std::unexpected(item.error());
      }
      items.push_back(*item);
      if (current_.text != ",") {
        break;
      }
      auto next = advance(true);
      if (!next) {
        return std::unexpected(next.error());
      }
    }
  }
  auto close = consume("]", false);
  if (!close) {
    return std::unexpected(close.error());
  }
  return add_node(
      Node{.kind = NodeKind::Array,
           .span = ByteSpan{.begin = open.span.begin, .end = close->span.end},
           .children = std::move(items)});
}

auto Parser::parse_object(const Token &open, std::size_t depth, NodeId input)
    -> Result<NodeId> {
  std::vector<std::pair<NodeId, NodeId>> pairs;
  if (current_.text != "}") {
    while (true) {
      auto key = parse_expression(0, depth);
      if (!key) {
        return std::unexpected(key.error());
      }
      auto colon = consume(":", true);
      if (!colon) {
        return std::unexpected(colon.error());
      }
      auto value = parse_expression(0, depth);
      if (!value) {
        return std::unexpected(value.error());
      }
      pairs.emplace_back(*key, *value);
      if (current_.text != ",") {
        break;
      }
      auto next = advance(true);
      if (!next) {
        return std::unexpected(next.error());
      }
    }
  }
  auto close = consume("}", false);
  if (!close) {
    return std::unexpected(close.error());
  }
  Node result{
      .kind = input == kInvalidNode ? NodeKind::Object : NodeKind::Group,
      .span = ByteSpan{.begin = input == kInvalidNode ? open.span.begin
                                                      : node(input).span.begin,
                       .end = close->span.end},
      .pairs = std::move(pairs)};
  if (input != kInvalidNode) {
    result.children.push_back(input);
  }
  return add_node(std::move(result));
}

auto Parser::parse_lambda(const Token &function_token, std::size_t depth)
    -> Result<NodeId> {
  auto open = consume("(", true);
  if (!open) {
    return std::unexpected(open.error());
  }
  std::vector<std::string> parameters;
  if (current_.text != ")") {
    while (true) {
      if (current_.kind != TokenKind::Variable || current_.text.empty()) {
        return std::unexpected(
            syntax_failure("S0208", "Function parameter must be a variable",
                           data_.source, current_.span.end, current_.text));
      }
      parameters.push_back(current_.text);
      auto next = advance(false);
      if (!next) {
        return std::unexpected(next.error());
      }
      if (current_.text != ",") {
        break;
      }
      next = advance(true);
      if (!next) {
        return std::unexpected(next.error());
      }
    }
  }
  auto close = consume(")", false);
  if (!close) {
    return std::unexpected(close.error());
  }
  std::shared_ptr<const FunctionSignature> signature;
  if (current_.text == "<") {
    const auto signature_begin = current_.span.begin;
    auto signature_end = signature_begin;
    std::size_t nesting = 0;
    while (true) {
      if (current_.kind == TokenKind::End) {
        return std::unexpected(syntax_failure("S0402",
                                              "Unclosed function signature",
                                              data_.source, current_.span.end));
      }
      if (current_.text == "<") {
        ++nesting;
      } else if (current_.text == ">") {
        if (nesting == 0) {
          return std::unexpected(
              syntax_failure("S0402", "Malformed function signature",
                             data_.source, current_.span.end));
        }
        --nesting;
        if (nesting == 0) {
          signature_end = current_.span.end;
          auto next = advance(true);
          if (!next) {
            return std::unexpected(next.error());
          }
          break;
        }
      }
      auto next = advance(false);
      if (!next) {
        return std::unexpected(next.error());
      }
    }
    auto parsed_signature = parse_function_signature(
        std::string_view{data_.source}.substr(signature_begin,
                                              signature_end - signature_begin),
        data_.source, signature_begin);
    if (!parsed_signature) {
      return std::unexpected(parsed_signature.error());
    }
    signature = std::move(*parsed_signature);
    if (current_.text != "{") {
      return std::unexpected(
          syntax_failure("S0402", "Malformed function signature", data_.source,
                         current_.span.end, current_.text));
    }
  }
  auto body_open = consume("{", true);
  if (!body_open) {
    return std::unexpected(body_open.error());
  }
  auto body = parse_expression(0, depth);
  if (!body) {
    return std::unexpected(body.error());
  }
  auto body_close = consume("}", false);
  if (!body_close) {
    return std::unexpected(body_close.error());
  }
  Node lambda{.kind = NodeKind::Lambda,
              .span = ByteSpan{.begin = function_token.span.begin,
                               .end = body_close->span.end},
              .children = {*body},
              .signature = std::move(signature)};
  for (auto &parameter : parameters) {
    auto literal = add_node(Node{.kind = NodeKind::Literal,
                                 .span = function_token.span,
                                 .literal = std::move(parameter)});
    if (!literal) {
      return std::unexpected(literal.error());
    }
    lambda.children.push_back(*literal);
  }
  return add_node(std::move(lambda));
}

auto Parser::parse_call(const Token &, NodeId function, std::size_t depth)
    -> Result<NodeId> {
  std::vector<NodeId> arguments{function};
  if (current_.text != ")") {
    while (true) {
      auto argument = parse_expression(0, depth);
      if (!argument) {
        return std::unexpected(argument.error());
      }
      arguments.push_back(*argument);
      if (current_.text != ",") {
        break;
      }
      auto next = advance(true);
      if (!next) {
        return std::unexpected(next.error());
      }
    }
  }
  auto close = consume(")", false);
  if (!close) {
    return std::unexpected(close.error());
  }
  return add_node(Node{.kind = NodeKind::Call,
                       .span = ByteSpan{.begin = node(function).span.begin,
                                        .end = close->span.end},
                       .children = std::move(arguments)});
}

auto Parser::parse_sort(const Token &, NodeId input, std::size_t depth)
    -> Result<NodeId> {
  auto open = consume("(", true);
  if (!open) {
    return std::unexpected(open.error());
  }
  std::vector<NodeId> terms{input};
  std::vector<bool> descending;
  if (current_.text == ")") {
    return std::unexpected(
        syntax_failure("S0207", "Sort expression requires at least one term",
                       data_.source, current_.span.end));
  }
  while (true) {
    bool desc = false;
    if (current_.text == "<" || current_.text == ">") {
      desc = current_.text == ">";
      auto next = advance(true);
      if (!next) {
        return std::unexpected(next.error());
      }
    }
    auto term = parse_expression(0, depth);
    if (!term) {
      return std::unexpected(term.error());
    }
    terms.push_back(*term);
    descending.push_back(desc);
    if (current_.text != ",") {
      break;
    }
    auto next = advance(true);
    if (!next) {
      return std::unexpected(next.error());
    }
  }
  auto close = consume(")", false);
  if (!close) {
    return std::unexpected(close.error());
  }
  return add_node(Node{
      .kind = NodeKind::Sort,
      .span = ByteSpan{.begin = node(input).span.begin, .end = close->span.end},
      .children = std::move(terms),
      .flags = std::move(descending)});
}

auto Parser::parse_transform(const Token &open, std::size_t depth)
    -> Result<NodeId> {
  auto location = parse_expression(0, depth);
  if (!location) {
    return std::unexpected(location.error());
  }
  auto separator = consume("|", true);
  if (!separator) {
    return std::unexpected(separator.error());
  }
  auto update = parse_expression(0, depth);
  if (!update) {
    return std::unexpected(update.error());
  }
  std::vector<NodeId> children{*location, *update};
  if (current_.text == ",") {
    auto next = advance(true);
    if (!next) {
      return std::unexpected(next.error());
    }
    auto remove = parse_expression(0, depth);
    if (!remove) {
      return std::unexpected(remove.error());
    }
    children.push_back(*remove);
  }
  auto close = consume("|", false);
  if (!close) {
    return std::unexpected(close.error());
  }
  return add_node(
      Node{.kind = NodeKind::Transform,
           .span = ByteSpan{.begin = open.span.begin, .end = close->span.end},
           .children = std::move(children)});
}

auto Parser::consume(std::string_view expected, bool prefix) -> Result<Token> {
  if (current_.text != expected) {
    if (current_.kind == TokenKind::End) {
      return std::unexpected(syntax_failure(
          "S0203",
          std::format("Expected '{}' before end of expression", expected),
          data_.source, current_.span.end, current_.text));
    }
    if (current_.text == "!") {
      return std::unexpected(syntax_failure("S0204", "Unknown operator",
                                            data_.source, current_.span.end,
                                            current_.text));
    }
    return std::unexpected(syntax_failure(
        "S0202",
        std::format("Expected '{}', got '{}'", expected, current_.text),
        data_.source, current_.span.end, current_.text));
  }
  auto consumed = current_;
  auto next = advance(prefix);
  if (!next) {
    return std::unexpected(next.error());
  }
  return consumed;
}

auto Parser::binding_power(const Token &token) -> int {
  if (token.kind != TokenKind::Operator) {
    return 0;
  }
  const auto &value = token.text;
  if (value == "[" || value == "(") {
    return 80;
  }
  if (value == "{") {
    return 70;
  }
  if (value == ".") {
    return 75;
  }
  if (value == "^") {
    return 40;
  }
  if (value == "#" || value == "@") {
    return 80;
  }
  if (value == "*" || value == "/" || value == "%") {
    return 60;
  }
  if (value == "+" || value == "-") {
    return 50;
  }
  if (value == "..") {
    return 20;
  }
  if (value == "=" || value == "!=" || value == "<" || value == "<=" ||
      value == ">" || value == ">=" || value == "in") {
    return 40;
  }
  if (value == "&") {
    return 50;
  }
  if (value == "and") {
    return 30;
  }
  if (value == "or") {
    return 25;
  }
  if (value == "~>") {
    return 40;
  }
  if (value == "?:" || value == "??") {
    return 40;
  }
  if (value == "?") {
    return 20;
  }
  if (value == ":=") {
    return 10;
  }
  return 0;
}

auto Parser::node(NodeId id) const -> const Node & { return data_.nodes[id]; }

} // namespace dagforge::jsonata::detail
