#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cfg_root="${BUILD2_CONFIG_ROOT:-${XDG_DATA_HOME:-$HOME/.local/share}/build2-configs}"
jobs="${BUILD2_JOBS:-$(nproc)}"
parallel_configs="${BUILD2_AUDIT_PARALLEL:-1}"
audit_targets="${BUILD2_AUDIT_TARGETS:-}"
if [[ -z "$audit_targets" ]]; then
  audit_targets='bin/exe{modules-foundation-smoke} bin/exe{component-tests}'
fi
runtime_filter="${RUNTIME_TEST_FILTER:-WorkflowRuntimeTest.PauseDrainsActiveAttemptBeforeResume:WorkflowRuntimeTest.CancelStaysStoppingUntilAttemptIsReaped:WorkflowRuntimeTest.SynchronousCancelCompletionIsReentrantSafe:WorkflowRuntimeTest.RetryWaitingCreatesDistinctAttempts}"

run_binary() {
  local config_name="$1"
  local binary_name="$2"
  shift 2
  local binary_path="${cfg_root}/dagforge-${config_name#@}/dagforge/bin/${binary_name}"
  "${binary_path}" "$@"
}

build_sanitizer() {
  local script_name="$1"
  local build_jobs="$2"
  BUILD2_JOBS="$build_jobs" BUILD2_TARGETS="$audit_targets" BUILD2_QUIET=1 \
    "${repo_root}/scripts/${script_name}"
}

cd "$repo_root"

if [[ "$parallel_configs" == "1" ]]; then
  per_build_jobs=$((jobs / 3))
  if ((per_build_jobs < 1)); then
    per_build_jobs=1
  fi

  build_sanitizer build-asan.sh "$per_build_jobs" &
  pid_asan=$!
  build_sanitizer build-tsan.sh "$per_build_jobs" &
  pid_tsan=$!
  build_sanitizer build-ubsan.sh "$per_build_jobs" &
  pid_ubsan=$!

  build_status=0
  for pid in "$pid_asan" "$pid_tsan" "$pid_ubsan"; do
    if ! wait "$pid"; then
      build_status=1
    fi
  done
  if ((build_status != 0)); then
    exit "$build_status"
  fi
else
  build_sanitizer build-asan.sh "$jobs"
  build_sanitizer build-tsan.sh "$jobs"
  build_sanitizer build-ubsan.sh "$jobs"
fi

ASAN_OPTIONS="detect_leaks=1:halt_on_error=1:strict_string_checks=1" \
  run_binary asan-clang modules-foundation-smoke
ASAN_OPTIONS="detect_leaks=1:halt_on_error=1:strict_string_checks=1" \
  run_binary asan-clang component-tests --gtest_filter="$runtime_filter"
TSAN_OPTIONS="halt_on_error=1:history_size=7" \
  run_binary tsan-clang component-tests --gtest_filter="$runtime_filter"
UBSAN_OPTIONS="halt_on_error=1:print_stacktrace=1" \
  run_binary ubsan-clang component-tests --gtest_filter="$runtime_filter"
