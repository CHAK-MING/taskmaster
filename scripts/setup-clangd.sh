#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
source "${repo_root}/scripts/build2-common.sh"

usage() {
  cat <<'EOF'
Usage: scripts/setup-clangd.sh [options]

Create a dedicated shared-library Clang build2 configuration, capture the real
compiler commands (including C++ module BMI dependencies), and write
compile_commands.json at the repository root for clangd. The IDE configuration
is recreated so one canonical PCM graph matches the selected Clang version.

Options:
  --compiler PATH       Clang C++ compiler. Defaults to the newest installed
                        clang++-22 ... clang++-20, then clang++.
  --clangd PATH         clangd executable. Defaults to the matching major
                        version of --compiler.
  --config-name NAME    build2 configuration name (default: clangd).
  --jobs N              Parallel build jobs (default: nproc).
  --skip-verify         Skip clangd checks after generating the database.
  -h, --help            Show this help.

Environment equivalents:
  CLANGD_CXX, CLANGD_BIN, CLANGD_BUILD2_CONFIG_NAME, CLANGD_JOBS
EOF
}

compiler=${CLANGD_CXX:-}
clangd_bin=${CLANGD_BIN:-}
config_name=${CLANGD_BUILD2_CONFIG_NAME:-clangd}
jobs=${CLANGD_JOBS:-$(nproc)}
verify_database=1

while [[ $# -gt 0 ]]; do
  case "$1" in
  --compiler)
    [[ $# -ge 2 ]] || { echo "--compiler requires a value" >&2; exit 2; }
    compiler=$2
    shift 2
    ;;
  --clangd)
    [[ $# -ge 2 ]] || { echo "--clangd requires a value" >&2; exit 2; }
    clangd_bin=$2
    shift 2
    ;;
  --config-name)
    [[ $# -ge 2 ]] || { echo "--config-name requires a value" >&2; exit 2; }
    config_name=${2#@}
    shift 2
    ;;
  --jobs)
    [[ $# -ge 2 ]] || { echo "--jobs requires a value" >&2; exit 2; }
    jobs=$2
    shift 2
    ;;
  --skip-verify)
    verify_database=0
    shift
    ;;
  -h | --help)
    usage
    exit 0
    ;;
  *)
    echo "unknown option: $1" >&2
    usage >&2
    exit 2
    ;;
  esac
done

if [[ ! "$config_name" =~ ^[A-Za-z0-9_.-]+$ ]]; then
  echo "invalid build2 configuration name: $config_name" >&2
  exit 2
fi
if [[ ! "$jobs" =~ ^[1-9][0-9]*$ ]]; then
  echo "--jobs must be a positive integer" >&2
  exit 2
fi

find_clang_compiler() {
  local candidate
  for candidate in clang++-22 clang++-21 clang++-20 clang++; do
    if command -v "$candidate" >/dev/null 2>&1; then
      command -v "$candidate"
      return 0
    fi
  done
  return 1
}

resolve_tool() {
  local tool=$1
  if [[ "$tool" == */* ]]; then
    [[ -x "$tool" ]] || return 1
    realpath "$tool"
  else
    command -v "$tool"
  fi
}

clang_major() {
  "$1" --version 2>/dev/null \
    | head -n 1 \
    | sed -nE 's/.*clang(d)? version ([0-9]+).*/\2/p'
}

if [[ -z "$compiler" ]]; then
  compiler=$(find_clang_compiler || true)
fi
if [[ -z "$compiler" ]] || ! compiler=$(resolve_tool "$compiler"); then
  echo "no usable Clang C++ compiler found (need clang++ 20 or newer)" >&2
  exit 1
fi

compiler_major=$(clang_major "$compiler")
if [[ -z "$compiler_major" ]]; then
  echo "compiler is not recognized as Clang: $compiler" >&2
  exit 1
fi
if (( compiler_major < 20 )); then
  echo "Clang ${compiler_major} is too old; DAGForge clangd setup requires Clang 20+" >&2
  exit 1
fi

if [[ -z "$clangd_bin" ]]; then
  compiler_dir=$(dirname "$compiler")
  for candidate in \
    "${compiler_dir}/clangd-${compiler_major}" \
    "${compiler_dir}/clangd" \
    "clangd-${compiler_major}" \
    clangd; do
    if clangd_bin=$(resolve_tool "$candidate" 2>/dev/null); then
      break
    fi
    clangd_bin=
  done
fi
if [[ -z "$clangd_bin" ]] || ! clangd_bin=$(resolve_tool "$clangd_bin"); then
  echo "no clangd executable found for Clang ${compiler_major}" >&2
  exit 1
fi

clangd_major=$(clang_major "$clangd_bin")
if [[ -z "$clangd_major" ]]; then
  echo "unable to determine clangd version: $clangd_bin" >&2
  exit 1
fi
if [[ "$clangd_major" != "$compiler_major" ]]; then
  echo "Clang/clangd major version mismatch: compiler=${compiler_major}, clangd=${clangd_major}" >&2
  echo "C++ module BMI files are compiler-version specific; install matching binaries." >&2
  exit 1
fi

for tool in b bdep bpkg bear python3 flock realpath timeout; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "missing required tool: $tool" >&2
    if [[ "$tool" == bear ]]; then
      echo "Ubuntu/Debian: sudo apt install bear" >&2
    fi
    exit 1
  fi
done

config_root=${BUILD2_CONFIG_ROOT:-${XDG_DATA_HOME:-$HOME/.local/share}/build2-configs}
config_dir=${BUILD2_CONFIG_DIR:-${config_root}/dagforge-${config_name}}
config_alias="@${config_name}"
build_events=$(mktemp "${TMPDIR:-/tmp}/dagforge-clangd-build.XXXXXX.jsonl")
test_events=$(mktemp "${TMPDIR:-/tmp}/dagforge-clangd-tests.XXXXXX.jsonl")
normalized_database=$(mktemp "${TMPDIR:-/tmp}/dagforge-clangd-normalized.XXXXXX.json")
trap 'rm -f "$build_events" "$test_events" "$normalized_database"' EXIT

bash "${repo_root}/scripts/check-module-graph.sh"
acquire_build2_lock "build-${config_name}"

CXX="$compiler" \
BUILD2_CONFIG_NAME="$config_name" \
BUILD2_CONFIG_DIR="$config_dir" \
BUILD2_CONFIG_DEFAULT=0 \
BUILD2_CONFIG_FORWARD=0 \
BUILD2_BIN_LIB=shared \
BUILD2_RECREATE_CONFIG=1 \
  bash "${repo_root}/scripts/setup-build2.sh"

cd "$repo_root"

bear intercept --output "$build_events" -- \
  bdep update -j "$jobs" "$config_alias"

bear intercept --output "$test_events" -- \
  b -j "$jobs" "${config_dir}/dagforge/tests/"

python3 - \
  "$build_events" \
  "$test_events" \
  "$normalized_database" \
  "$repo_root" \
  "$config_dir" <<'PY'
import json
import sys
from pathlib import Path

event_paths = [Path(sys.argv[1]), Path(sys.argv[2])]
out_path = Path(sys.argv[3])
repo_root = Path(sys.argv[4]).resolve()
build_root = Path(sys.argv[5]).resolve() / "dagforge"

source_suffixes = {".c", ".cc", ".cpp", ".cxx", ".cppm", ".ixx", ".mxx"}
generated_suffixes = (
    ".a.pcm.ii",
    ".so.pcm.ii",
    ".pcm.ii",
    ".a.o.ii",
    ".so.o.ii",
    ".o.ii",
    ".a.o.i",
    ".so.o.i",
    ".o.i",
)
selected: dict[str, tuple[int, dict[str, object]]] = {}


def intercepted_commands() -> list[tuple[Path, Path, list[str]]]:
    commands: list[tuple[Path, Path, list[str]]] = []
    input_suffixes = tuple(source_suffixes) + generated_suffixes

    for event_path in event_paths:
        with event_path.open("r", encoding="utf-8") as stream:
            for line in stream:
                if not line.strip():
                    continue
                event = json.loads(line)
                execution = event.get("started", {}).get("execution")
                if not execution:
                    continue

                executable = execution.get("executable")
                arguments = execution.get("arguments") or []
                working_dir = execution.get("working_dir")
                if not executable or not arguments or not working_dir:
                    continue

                normalized_arguments = [str(executable), *map(str, arguments[1:])]
                generated = None
                for argument in reversed(normalized_arguments[1:]):
                    if argument.startswith("-"):
                        continue
                    if argument.endswith(input_suffixes):
                        candidate = Path(argument)
                        directory = Path(working_dir).resolve()
                        if not candidate.is_absolute():
                            candidate = directory / candidate
                        generated = candidate.resolve()
                        break
                if generated is None:
                    continue

                commands.append(
                    (Path(working_dir).resolve(), generated, normalized_arguments)
                )

    return commands


def resolve_source(generated: Path, arguments: list[str]) -> Path | None:
    try:
        generated.relative_to(repo_root)
        if generated.suffix.lower() in source_suffixes:
            return generated
    except ValueError:
        pass

    try:
        relative = generated.relative_to(build_root)
    except ValueError:
        return None

    stem = None
    matched_suffix = None
    for suffix in generated_suffixes:
        if relative.name.endswith(suffix):
            stem = relative.name[: -len(suffix)]
            matched_suffix = suffix
            break
    if not stem:
        return None

    is_module = (
        matched_suffix is not None
        and ".pcm.ii" in matched_suffix
        or "c++-module" in arguments
        or any(argument.startswith("-fmodule-output") for argument in arguments)
    )
    if is_module:
        extensions = (".cppm", ".ixx", ".mxx", ".cpp", ".cxx", ".cc")
    elif matched_suffix is not None and matched_suffix.endswith(".i"):
        extensions = (".c", ".cpp", ".cxx", ".cc")
    else:
        extensions = (".cpp", ".cxx", ".cc", ".cppm", ".ixx", ".mxx", ".c")

    source_dir = repo_root / relative.parent
    candidates = [source_dir / f"{stem}{extension}" for extension in extensions]
    existing = [candidate.resolve() for candidate in candidates if candidate.is_file()]
    if not existing:
        return None
    return existing[0]


def replace_generated_input(
    arguments: list[str], generated: Path, source: Path, directory: Path
) -> list[str]:
    replaced: list[str] = []
    for argument in arguments:
        if argument.startswith("-"):
            replaced.append(argument)
            continue
        candidate = Path(argument)
        if not candidate.is_absolute():
            candidate = directory / candidate
        try:
            matches = candidate.resolve() == generated
        except OSError:
            matches = False
        replaced.append(str(source) if matches else argument)
    return replaced


for directory, generated, arguments in intercepted_commands():
    source = resolve_source(generated, arguments)
    if source is None:
        continue
    arguments = replace_generated_input(arguments, generated, source, directory)

    score = 0
    compiler_name = Path(arguments[0]).name
    if compiler_name.startswith("clang++"):
        score += 50
    if "-cc1" in arguments:
        score -= 100
    if "-c" in arguments or "--precompile" in arguments:
        score += 20
    if source.suffix.lower() in {".cppm", ".ixx", ".mxx"}:
        score += 100
        if any(
            argument.startswith("-fmodule-file=")
            or argument.startswith("-fmodule-output")
            or argument == "--precompile"
            for argument in arguments
        ):
            score += 20

    normalized = {
        "directory": str(directory),
        "file": str(source),
        "arguments": arguments,
    }
    key = str(source)
    previous = selected.get(key)
    if previous is None or score >= previous[0]:
        selected[key] = (score, normalized)

entries = [value[1] for _, value in sorted(selected.items())]
if not entries:
    raise SystemExit("Bear captured no project compilation commands")

recorded = {Path(entry["file"]).resolve() for entry in entries}
expected_modules = {path.resolve() for path in (repo_root / "src/modules").glob("*.cppm")}
missing_modules = sorted(expected_modules - recorded)
if missing_modules:
    formatted = "\n".join(f"  - {path.relative_to(repo_root)}" for path in missing_modules)
    raise SystemExit(f"compilation database is missing module interfaces:\n{formatted}")

required_sources = [
    repo_root / "src/dagforge/core/runtime.cpp",
    repo_root / "tests/modules-foundation-smoke.cpp",
]
missing_required = [path for path in required_sources if path.resolve() not in recorded]
if missing_required:
    formatted = "\n".join(f"  - {path.relative_to(repo_root)}" for path in missing_required)
    raise SystemExit(f"compilation database is missing required sources:\n{formatted}")

with out_path.open("w", encoding="utf-8") as stream:
    json.dump(entries, stream, indent=2)
    stream.write("\n")

module_count = sum(Path(entry["file"]).suffix == ".cppm" for entry in entries)
print(f"normalized {len(entries)} compile commands ({module_count} module interfaces)")
PY

install -m 0644 "$normalized_database" "${repo_root}/compile_commands.json"

tool_dir="${repo_root}/.clangd-tools"
mkdir -p "$tool_dir"
ln -sfn "$compiler" "${tool_dir}/clang++"
ln -sfn "$clangd_bin" "${tool_dir}/clangd"

if [[ "$verify_database" == "1" ]]; then
  clangd_args=(
    "--compile-commands-dir=${repo_root}"
    "--query-driver=${compiler}"
    --enable-config
    --log=error
  )
  timeout 180 "$clangd_bin" \
    --check="${repo_root}/src/modules/dagforge.cppm" \
    "${clangd_args[@]}"
  timeout 180 "$clangd_bin" \
    --check="${repo_root}/tests/modules-foundation-smoke.cpp" \
    "${clangd_args[@]}"
fi

printf '\nclangd database ready: %s\n' "${repo_root}/compile_commands.json"
printf 'IDE build configuration: %s -> %s\n' "$config_alias" "$config_dir"
printf 'compiler: %s (major %s)\n' "$compiler" "$compiler_major"
printf 'clangd: %s (major %s)\n' "$clangd_bin" "$clangd_major"
printf 'workspace clangd shim: %s\n' "${tool_dir}/clangd"
printf '\nRestart clangd in VS Code after this script completes.\n'
