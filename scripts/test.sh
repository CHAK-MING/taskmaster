#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
mode="${1:-quick}"
if (($# > 0)); then
  shift
fi

config_name="${BUILD2_CONFIG_NAME:-gcc}"
config_name="${config_name#@}"
config_root="${BUILD2_CONFIG_ROOT:-${XDG_DATA_HOME:-$HOME/.local/share}/build2-configs}"
config_dir="${BUILD2_CONFIG_DIR:-${config_root}/dagforge-${config_name}}"
build_dir="${config_dir}/dagforge"
bin_dir="${build_dir}/bin"

gtest_args=("$@")
if [[ -n "${DAGFORGE_TEST_FILTER:-}" ]]; then
  gtest_args+=("--gtest_filter=${DAGFORGE_TEST_FILTER}")
fi
if [[ -n "${DAGFORGE_TEST_REPEAT:-}" ]]; then
  gtest_args+=("--gtest_repeat=${DAGFORGE_TEST_REPEAT}")
fi
if [[ "${DAGFORGE_TEST_SHUFFLE:-0}" == "1" ]]; then
  gtest_args+=(--gtest_shuffle)
fi

usage() {
  cat <<'EOF'
usage: scripts/test.sh [unit|component|quick|integration|e2e|all] [gtest arguments...]

  unit        module smoke test and fast isolated GoogleTests
  component   in-process Runtime, Workflow, HTTP, storage, and API tests
  quick       unit + component; default local verification
  integration real Minijail tests and CLI subprocess scenarios
  e2e         real service, executor, sandbox, HTTP, and TLS workflows
  all         quick + integration + e2e

Environment:
  DAGFORGE_TEST_FILTER    GoogleTest filter applied to selected C++ suites
  DAGFORGE_TEST_REPEAT    GoogleTest repeat count
  DAGFORGE_TEST_SHUFFLE=1 shuffle GoogleTest order and print the seed
  BUILD2_CONFIG_NAME      normal build profile, default gcc
EOF
}

case "$mode" in
unit)
  build_targets='bin/exe{modules-foundation-smoke} bin/exe{unit-tests}'
  verify_targets=(unit-tests)
  ;;
component)
  build_targets='bin/exe{component-tests}'
  verify_targets=(component-tests)
  ;;
quick)
  build_targets='bin/exe{modules-foundation-smoke} bin/exe{unit-tests} bin/exe{component-tests}'
  verify_targets=(unit-tests component-tests)
  ;;
integration)
  build_targets='bin/exe{integration-tests} bin/exe{dagforge}'
  verify_targets=(integration-tests)
  ;;
e2e)
  build_targets='bin/exe{dagforge}'
  verify_targets=()
  ;;
all)
  build_targets='bin/exe{modules-foundation-smoke} bin/exe{unit-tests} bin/exe{component-tests} bin/exe{integration-tests} bin/exe{dagforge}'
  verify_targets=(unit-tests component-tests integration-tests)
  ;;
-h|--help|help)
  usage
  exit 0
  ;;
*)
  echo "unknown test mode: $mode" >&2
  usage >&2
  exit 2
  ;;
esac

run_binary() {
  local name="$1"
  shift
  local path="${bin_dir}/${name}"
  if [[ ! -x "$path" ]]; then
    echo "test binary is missing: $path" >&2
    exit 1
  fi
  printf '\n==> %s\n' "$name"
  "$path" "$@"
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command for $mode tests: $1" >&2
    exit 1
  fi
}

require_minijail() {
  local minijail="${DAGFORGE_TEST_MINIJAIL:-${HOME}/.local/libexec/dagforge/minijail/minijail0}"
  local seccomp="${DAGFORGE_TEST_SECCOMP_BPF:-${HOME}/.local/libexec/dagforge/minijail/dagforge_command.bpf}"
  if [[ ! -x "$minijail" ]]; then
    echo "Minijail executable is unavailable: $minijail" >&2
    exit 1
  fi
  if [[ ! -f "$seccomp" ]]; then
    echo "Minijail seccomp policy is unavailable: $seccomp" >&2
    exit 1
  fi
}

run_unit() {
  run_binary modules-foundation-smoke
  run_binary unit-tests "${gtest_args[@]}"
}

run_component() {
  run_binary component-tests "${gtest_args[@]}"
}

run_integration() {
  run_binary integration-tests "${gtest_args[@]}"
  printf '\n==> CLI scenarios\n'
  python3 "${repo_root}/scripts/test-cli-scenarios.py" --binary "${bin_dir}/dagforge"
}

run_e2e() {
  printf '\n==> real workflow scenarios\n'
  python3 "${repo_root}/scripts/test-real-workflows.py" --binary "${bin_dir}/dagforge"
}

case "$mode" in
integration|all)
  require_command python3
  require_command openssl
  require_minijail
  ;;
e2e)
  require_command python3
  require_command openssl
  require_minijail
  ;;
esac

case "$mode" in
unit|quick|all)
  python3 "${repo_root}/scripts/check-foundation-contracts.py" \
    --compiler "${CXX:-g++}"
  ;;
esac

if ((${#verify_targets[@]} > 0)); then
  python3 "${repo_root}/scripts/check-test-layout.py" \
    --prepare-build "$build_dir" \
    --targets "${verify_targets[@]}"
fi

BUILD2_TARGETS="$build_targets" "${repo_root}/scripts/build.sh"

verify_test_binaries() {
  if ((${#verify_targets[@]} == 0)); then
    return 0
  fi
  local status
  set +e
  python3 "${repo_root}/scripts/check-test-layout.py" \
    --verify-binaries "$bin_dir" \
    --repair-build-dir "$build_dir" \
    --record-build-dir "$build_dir" \
    --targets "${verify_targets[@]}"
  status=$?
  set -e
  if ((status == 2)); then
    echo "rebuilding stale test target artifacts" >&2
    BUILD2_TARGETS="$build_targets" "${repo_root}/scripts/build.sh"
    python3 "${repo_root}/scripts/check-test-layout.py" \
      --verify-binaries "$bin_dir" \
      --record-build-dir "$build_dir" \
      --targets "${verify_targets[@]}"
    return
  fi
  return "$status"
}

verify_test_binaries

case "$mode" in
unit)
  run_unit
  ;;
component)
  run_component
  ;;
quick)
  run_unit
  run_component
  ;;
integration)
  run_integration
  ;;
e2e)
  run_e2e
  ;;
all)
  run_unit
  run_component
  run_integration
  run_e2e
  ;;
esac
