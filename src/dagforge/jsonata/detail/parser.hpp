#pragma once

#include "lexer.hpp"
#include "model.hpp"

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>

namespace dagforge::jsonata::detail {

class Parser {
public:
  Parser(std::string source, CompileLimits limits,
         std::optional<CompileInterrupt> interrupt = std::nullopt);

  [[nodiscard]] auto parse() -> Result<ProgramData>;

private:
  [[nodiscard]] auto check_interrupt() const -> Result<void>;
  [[nodiscard]] auto add_node(Node node) -> Result<NodeId>;
  [[nodiscard]] auto advance(bool prefix) -> Result<void>;
  [[nodiscard]] auto parse_expression(int right_binding_power,
                                      std::size_t depth) -> Result<NodeId>;
  [[nodiscard]] auto parse_prefix(const Token &token, std::size_t depth)
      -> Result<NodeId>;
  [[nodiscard]] auto parse_infix(const Token &token, NodeId left,
                                 std::size_t depth) -> Result<NodeId>;
  [[nodiscard]] auto parse_block(const Token &open, std::size_t depth)
      -> Result<NodeId>;
  [[nodiscard]] auto parse_array(const Token &open, std::size_t depth)
      -> Result<NodeId>;
  [[nodiscard]] auto parse_object(const Token &open, std::size_t depth,
                                  NodeId input) -> Result<NodeId>;
  [[nodiscard]] auto parse_lambda(const Token &function_token,
                                  std::size_t depth) -> Result<NodeId>;
  [[nodiscard]] auto parse_call(const Token &open, NodeId function,
                                std::size_t depth) -> Result<NodeId>;
  [[nodiscard]] auto parse_sort(const Token &sort, NodeId input,
                                std::size_t depth) -> Result<NodeId>;
  [[nodiscard]] auto parse_transform(const Token &open, std::size_t depth)
      -> Result<NodeId>;
  [[nodiscard]] auto consume(std::string_view expected, bool prefix)
      -> Result<Token>;
  [[nodiscard]] static auto binding_power(const Token &token) -> int;
  [[nodiscard]] auto node(NodeId id) const -> const Node &;

  ProgramData data_;
  std::optional<CompileInterrupt> interrupt_;
  Lexer lexer_;
  CompileLimits limits_;
  Token current_;
};

} // namespace dagforge::jsonata::detail
