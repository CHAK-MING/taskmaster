#include <string_view>

import dagforge.foundation;

auto main() -> int {
  auto result = dagforge::ok(42);

  if (!result || *result != 42) {
    return 1;
  }
  if (dagforge::timing::kShutdownPollInterval.count() <= 0) {
    return 3;
  }
  auto parsed = dagforge::util::parse_int<int>("7");
  if (!parsed || *parsed != 7) {
    return 4;
  }
  dagforge::WorkflowId workflow_id{"module"};
  if (workflow_id.empty() || workflow_id.size() != 6) {
    return 5;
  }
  auto io_ec = dagforge::io::make_error_code(dagforge::io::IoError::TimedOut);
  const char *io_name = io_ec.category().name();
  if (io_name == nullptr || io_name[0] != 'd' || io_name[1] != 'a') {
    return 6;
  }
  dagforge::SystemConfig config;
  if (config.api.port != 8888 || !config.workflow.enabled) {
    return 7;
  }
  dagforge::CommandSpec command;
  command.program = "/bin/true";
  if (command.program.size() != 9 || command.program.front() != '/' ||
      config.sandbox.tmp_bytes == 0) {
    return 19;
  }
  if (dagforge::workflow::kWorkflowSchemaVersion != 1U) {
    return 28;
  }
  dagforge::AttemptId attempt_id{"attempt"};
  if (attempt_id.empty()) {
    return 29;
  }
  return 0;
}
