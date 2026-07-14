#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
source "${repo_root}/scripts/build2-common.sh"

for tool in clang++ llvm-profdata llvm-cov python3; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "missing required tool: $tool" >&2
    exit 1
  fi
done

config_name="${DAGFORGE_COVERAGE_CONFIG:-coverage-clang}"
config_root="${BUILD2_CONFIG_ROOT:-${XDG_DATA_HOME:-$HOME/.local/share}/build2-configs}"
config_dir="${BUILD2_CONFIG_DIR:-${config_root}/dagforge-${config_name}}"
build_dir="${config_dir}/dagforge"
output_dir="${DAGFORGE_COVERAGE_OUTPUT:-${repo_root}/.git/coverage}"
minimum="${DAGFORGE_COVERAGE_MIN:-90}"
jobs="${BUILD2_JOBS:-$(nproc)}"

export CXX="${CXX:-clang++}"
export BUILD2_CONFIG_NAME="$config_name"
export BUILD2_CONFIG_FORWARD=0
# Clang module dependency intermediates are pruned after a successful build in
# this configuration. Reusing it after source changes can leave build2 with
# stale dependency records that reference missing *.pcm.ii.lz4 files.
export BUILD2_RECREATE_CONFIG="${BUILD2_RECREATE_CONFIG:-1}"
export BUILD2_CC_COPTIONS="${BUILD2_CC_COPTIONS:--O1 -g -fprofile-instr-generate -fcoverage-mapping -fno-omit-frame-pointer}"
export BUILD2_CC_LOPTIONS="${BUILD2_CC_LOPTIONS:--fprofile-instr-generate}"

"${repo_root}/scripts/setup-build2.sh"

cd "$repo_root"
(
  acquire_build2_lock "build-${config_name}"
  b -j "$jobs" \
    "${build_dir}/bin/exe{all-unit-tests}" \
    "${build_dir}/bin/exe{dagforge}" \
    config.dagforge.coverage=true
)

unit_binary="${build_dir}/bin/all-unit-tests"
service_binary="${build_dir}/bin/dagforge"
dagforge_library="${build_dir}/src/libdagforge.so"
foundation_library="${build_dir}/src/libdagforge-foundation.so"

for artifact in "$unit_binary" "$service_binary" "$dagforge_library" \
                "$foundation_library"; do
  if [[ ! -x "$artifact" && ! -f "$artifact" ]]; then
    echo "coverage artifact is missing: $artifact" >&2
    exit 1
  fi
done

rm -rf "$output_dir"
mkdir -p "$output_dir/raw"

LLVM_PROFILE_FILE="${output_dir}/raw/unit-%m-%p.profraw" \
  "$unit_binary" >"${output_dir}/unit-tests.log" 2>&1

LLVM_PROFILE_FILE="${output_dir}/raw/scenario-%m-%p.profraw" \
  python3 scripts/test-real-workflows.py --binary "$service_binary" \
  >"${output_dir}/real-workflows.log" 2>&1

LLVM_PROFILE_FILE="${output_dir}/raw/cli-%m-%p.profraw" \
  python3 scripts/test-cli-scenarios.py --binary "$service_binary" \
  >"${output_dir}/cli-scenarios.log" 2>&1

llvm-profdata merge -sparse "${output_dir}"/raw/*.profraw \
  -o "${output_dir}/merged.profdata"

mapfile -d '' production_sources < <(
  find src/dagforge -type f -name '*.cpp' -print0 | sort -z
)
production_sources+=(src/main.cpp)

coverage_args=(
  "$dagforge_library"
  -object "$foundation_library"
  -object "$service_binary"
  -instr-profile="${output_dir}/merged.profdata"
)

llvm-cov report "${coverage_args[@]}" "${production_sources[@]}" \
  >"${output_dir}/report.txt"

llvm-cov export "${coverage_args[@]}" "${production_sources[@]}" \
  -summary-only >"${output_dir}/summary.json"

if [[ "${DAGFORGE_COVERAGE_HTML:-0}" == "1" ]]; then
  llvm-cov show "${coverage_args[@]}" "${production_sources[@]}" \
    -format=html -output-dir="${output_dir}/html"
fi

line_coverage=$(python3 - "$output_dir/summary.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    summary = json.load(stream)

print(f"{summary['data'][0]['totals']['lines']['percent']:.2f}")
PY
)

tail -n 1 "${output_dir}/report.txt"
printf 'production source line coverage: %s%% (minimum %s%%)\n' \
  "$line_coverage" "$minimum"

python3 - "$line_coverage" "$minimum" <<'PY'
import sys

measured = float(sys.argv[1])
minimum = float(sys.argv[2])
if measured < minimum:
    raise SystemExit(
        f"coverage gate failed: {measured:.2f}% is below {minimum:.2f}%"
    )
PY
