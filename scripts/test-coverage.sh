#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
source "${repo_root}/scripts/build2-common.sh"

for tool in clang++ llvm-profdata llvm-cov python3 readelf; do
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

real_cxx="${CXX:-clang++}"
export DAGFORGE_COVERAGE_REAL_CXX="$real_cxx"
export CXX="${repo_root}/scripts/coverage-cxx-wrapper.sh"
export BUILD2_CONFIG_NAME="$config_name"
export BUILD2_CONFIG_FORWARD=0
# Coverage has its own persistent out-of-source configuration. Recreate it
# only as an explicit repair action; normal runs must remain incremental.
export BUILD2_RECREATE_CONFIG="${BUILD2_RECREATE_CONFIG:-0}"
export BUILD2_CC_COPTIONS="${BUILD2_CC_COPTIONS:--O1 -g -fprofile-instr-generate -fcoverage-mapping -fno-omit-frame-pointer}"
export BUILD2_CC_LOPTIONS="${BUILD2_CC_LOPTIONS:--fprofile-instr-generate}"

"${repo_root}/scripts/setup-build2.sh"

build_coverage_targets() {
  (
    acquire_build2_lock "build-${config_name}"
    b -j "$jobs" \
      "${build_dir}/bin/exe{unit-tests}" \
      "${build_dir}/bin/exe{component-tests}" \
      "${build_dir}/bin/exe{integration-tests}" \
      "${build_dir}/bin/exe{dagforge}" \
      config.dagforge.coverage=true
  )
}

cd "$repo_root"
build_log="${repo_root}/.git/coverage-build.log"
if ! build_coverage_targets >"$build_log" 2>&1; then
  cat "$build_log" >&2
  if grep -Eq '(\.pcm\.)?ii\.lz4 .*does not exist|consider cleaning the build state' \
      "$build_log"; then
    echo "coverage build state is stale; recreating this profile once" >&2
    BUILD2_RECREATE_CONFIG=1 "${repo_root}/scripts/setup-build2.sh"
    build_coverage_targets
  else
    exit 1
  fi
fi

unit_binary="${build_dir}/bin/unit-tests"
component_binary="${build_dir}/bin/component-tests"
integration_binary="${build_dir}/bin/integration-tests"
service_binary="${build_dir}/bin/dagforge"
dagforge_library="${build_dir}/src/libdagforge.so"
foundation_library="${build_dir}/src/libdagforge-foundation.so"

for artifact in "$unit_binary" "$component_binary" "$integration_binary" \
                "$service_binary" "$dagforge_library" "$foundation_library"; do
  if [[ ! -x "$artifact" && ! -f "$artifact" ]]; then
    echo "coverage artifact is missing: $artifact" >&2
    exit 1
  fi
done

rm -rf "$output_dir"
mkdir -p "$output_dir/raw"

LLVM_PROFILE_FILE="${output_dir}/raw/unit-%m-%p.profraw" \
  "$unit_binary" >"${output_dir}/unit-tests.log" 2>&1

LLVM_PROFILE_FILE="${output_dir}/raw/component-%m-%p.profraw" \
  "$component_binary" >"${output_dir}/component-tests.log" 2>&1

LLVM_PROFILE_FILE="${output_dir}/raw/integration-%m-%p.profraw" \
  "$integration_binary" >"${output_dir}/integration-tests.log" 2>&1

LLVM_PROFILE_FILE="${output_dir}/raw/scenario-%m-%p.profraw" \
  python3 scripts/test-real-workflows.py --binary "$service_binary" \
  >"${output_dir}/real-workflows.log" 2>&1

LLVM_PROFILE_FILE="${output_dir}/raw/cli-%m-%p.profraw" \
  python3 scripts/test-cli-scenarios.py --binary "$service_binary" \
  >"${output_dir}/cli-scenarios.log" 2>&1

object_build_id() {
  local object="$1"
  local build_id
  build_id=$(readelf -n "$object" | awk '/Build ID:/ {print $3; exit}')
  if [[ -z "$build_id" ]]; then
    echo "coverage object has no ELF Build ID: $object" >&2
    exit 1
  fi
  printf '%s\n' "$build_id"
}

dagforge_library_id=$(object_build_id "$dagforge_library")
service_binary_id=$(object_build_id "$service_binary")
library_profiles=()
service_profiles=()
for profile in "${output_dir}"/raw/*.profraw; do
  profile_ids=$(llvm-profdata show --binary-ids "$profile")
  if [[ "$profile_ids" == *"$dagforge_library_id"* ]]; then
    library_profiles+=("$profile")
  elif [[ "$profile_ids" == *"$service_binary_id"* ]]; then
    service_profiles+=("$profile")
  fi
done

if ((${#library_profiles[@]} == 0)); then
  echo "no coverage profiles matched libdagforge.so" >&2
  exit 1
fi
if ((${#service_profiles[@]} == 0)); then
  echo "no coverage profiles matched the dagforge executable" >&2
  exit 1
fi

llvm-profdata merge -sparse "${library_profiles[@]}" \
  -o "${output_dir}/library.profdata"
llvm-profdata merge -sparse "${service_profiles[@]}" \
  -o "${output_dir}/cli.profdata"

mapfile -d '' library_sources < <(
  find src/dagforge -type f -name '*.cpp' \
    ! -path 'src/dagforge/app/cli/*' -print0 | sort -z
)
mapfile -d '' cli_sources < <(
  find src/dagforge/app/cli -type f -name '*.cpp' -print0 | sort -z
)
cli_sources+=(src/main.cpp)

library_coverage_args=(
  "$dagforge_library"
  -instr-profile="${output_dir}/library.profdata"
)
cli_coverage_args=(
  "$service_binary"
  -instr-profile="${output_dir}/cli.profdata"
)

llvm-cov report "${library_coverage_args[@]}" "${library_sources[@]}" \
  >"${output_dir}/report-library.txt"
llvm-cov report "${cli_coverage_args[@]}" "${cli_sources[@]}" \
  >"${output_dir}/report-cli.txt"

llvm-cov export "${library_coverage_args[@]}" "${library_sources[@]}" \
  -summary-only >"${output_dir}/summary-library.json"
llvm-cov export "${cli_coverage_args[@]}" "${cli_sources[@]}" \
  -summary-only >"${output_dir}/summary-cli.json"

python3 - "$output_dir" <<'PY'
import json
from pathlib import Path
import sys

output = Path(sys.argv[1])
components = {}
line_count = 0
covered_lines = 0
for name in ("library", "cli"):
    with (output / f"summary-{name}.json").open(encoding="utf-8") as stream:
        summary = json.load(stream)
    lines = summary["data"][0]["totals"]["lines"]
    components[name] = lines
    line_count += int(lines["count"])
    covered_lines += int(lines["covered"])

percent = 100.0 * covered_lines / line_count if line_count else 100.0
combined = {
    "data": [
        {
            "totals": {
                "lines": {
                    "count": line_count,
                    "covered": covered_lines,
                    "notcovered": line_count - covered_lines,
                    "percent": percent,
                }
            },
            "components": components,
        }
    ],
    "type": "dagforge.coverage.summary",
    "version": "1",
}
with (output / "summary.json").open("w", encoding="utf-8") as stream:
    json.dump(combined, stream, indent=2)
    stream.write("\n")
with (output / "aggregate.txt").open("w", encoding="utf-8") as stream:
    stream.write(
        f"TOTAL production lines {covered_lines}/{line_count} "
        f"{percent:.2f}%\n"
    )
PY

{
  printf '%s\n' '== libdagforge =='
  cat "${output_dir}/report-library.txt"
  printf '\n%s\n' '== dagforge CLI =='
  cat "${output_dir}/report-cli.txt"
  printf '\n%s\n' '== aggregate =='
  cat "${output_dir}/aggregate.txt"
} >"${output_dir}/report.txt"

if [[ "${DAGFORGE_COVERAGE_HTML:-0}" == "1" ]]; then
  llvm-cov show "${library_coverage_args[@]}" "${library_sources[@]}" \
    -format=html -output-dir="${output_dir}/html/library"
  llvm-cov show "${cli_coverage_args[@]}" "${cli_sources[@]}" \
    -format=html -output-dir="${output_dir}/html/cli"
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
