#include "dagforge/jsonata/program.hpp"

#include "detail/evaluator.hpp"
#include "detail/parser.hpp"

#include <memory>
#include <string_view>
#include <utility>

namespace dagforge::jsonata {

class Program::Impl {
public:
  explicit Impl(detail::ProgramData program) : program_(std::move(program)) {}

  detail::ProgramData program_;
};

Program::Program(std::shared_ptr<const Impl> impl) : impl_(std::move(impl)) {}

auto Program::compile(CompileRequest request) -> DiagnosticResult<Program> {
  auto parsed =
      detail::Parser(std::move(request.source), request.limits).parse();
  if (!parsed) {
    return std::unexpected(parsed.error());
  }
  return Program{std::make_shared<Impl>(std::move(*parsed))};
}

auto Program::evaluate(const EvaluationRequest &request) const
    -> DiagnosticResult<EvaluationSuccess> {
  if (!impl_) {
    return std::unexpected(detail::host_failure(
        "H1000", "Cannot evaluate an empty JSONata Program", {}));
  }
  return detail::Evaluator(impl_->program_, request).run();
}

auto Program::source() const noexcept -> std::string_view {
  return impl_ ? std::string_view{impl_->program_.source} : std::string_view{};
}

auto Program::valid() const noexcept -> bool { return impl_ != nullptr; }

} // namespace dagforge::jsonata
