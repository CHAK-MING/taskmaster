#include "policy_command_runner.hpp"

#include "command_policy.hpp"

#include <utility>

namespace dagforge::sandbox::detail {
namespace {

class PolicyCommandRunner final : public ICommandRunner {
public:
  PolicyCommandRunner(std::unique_ptr<ICommandRunner> inner,
                      std::shared_ptr<const CommandPolicy> policy)
      : inner_(std::move(inner)), policy_(std::move(policy)) {}

  auto prepare(CommandPreparationRequest request) const
      -> Result<CommandSpec> override {
    for (const auto &key : request.deferred_environment_keys) {
      auto valid = policy_->validate_environment_key(key);
      if (!valid) {
        return fail(valid.error());
      }
    }
    auto valid = policy_->validate(request.command);
    if (!valid) {
      return fail(valid.error());
    }
    return ok(std::move(request.command));
  }

  auto start(CommandRunRequest request, CommandRunSink sink)
      -> Result<void> override {
    auto valid = policy_->validate(request.command);
    if (!valid) {
      return fail(valid.error());
    }
    auto environment = policy_->inherited_environment();
    for (auto entry = request.command.environment.begin();
         entry != request.command.environment.end(); ++entry) {
      environment.insert_or_assign(entry->first, entry->second);
    }
    request.command.environment.clear();
    for (auto &[key, value] : environment) {
      request.command.environment.emplace(std::move(key), std::move(value));
    }
    return inner_->start(std::move(request), std::move(sink));
  }

  auto cancel(const InstanceId &instance_id) -> void override {
    inner_->cancel(instance_id);
  }

  auto quiesce(std::chrono::milliseconds timeout) -> Result<void> override {
    return inner_->quiesce(timeout);
  }

private:
  std::unique_ptr<ICommandRunner> inner_;
  std::shared_ptr<const CommandPolicy> policy_;
};

} // namespace

auto create_policy_command_runner(std::unique_ptr<ICommandRunner> inner,
                                  config::CommandPolicyConfig policy_config)
    -> Result<std::unique_ptr<ICommandRunner>> {
  if (!inner) {
    return fail(Error::InvalidArgument);
  }
  auto policy = CommandPolicy::create(std::move(policy_config));
  if (!policy) {
    return fail(policy.error());
  }
  return ok(std::unique_ptr<ICommandRunner>{
      std::make_unique<PolicyCommandRunner>(std::move(inner),
                                            std::move(*policy))});
}

} // namespace dagforge::sandbox::detail
