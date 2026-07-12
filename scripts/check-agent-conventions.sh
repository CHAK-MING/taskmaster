#!/usr/bin/env bash

set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

if ! command -v rg >/dev/null 2>&1; then
  echo "missing required tool: rg" >&2
  exit 1
fi

failures=0
cpp_roots=(include src tests bench docs/templates)
cpp_globs=(
  --glob '*.hpp'
  --glob '*.cpp'
  --glob '*.cppm'
  --glob '*.inc'
)

report_matches() {
  local description=$1
  local pattern=$2
  shift 2

  local matches
  if matches=$(rg -n "${cpp_globs[@]}" "$@" "$pattern" "${cpp_roots[@]}" 2>/dev/null); then
    echo "$description" >&2
    printf '%s\n' "$matches" >&2
    failures=1
  fi
}

report_matches \
  "exception-based integer parsing is forbidden; use util::parse_int<T>():" \
  'std::sto(i|l|ll|ul|ull|f|d|ld)[[:space:]]*\('

report_matches \
  "construct Result failures with fail(...), not std::unexpected:" \
  'std::unexpected' \
  --glob '!include/dagforge/core/error.hpp'

report_matches \
  "construct Result values with ok(...)/fail(...), not std::expected directly:" \
  'std::expected[[:space:]]*[<{]' \
  --glob '!include/dagforge/core/error.hpp'

report_matches \
  "Glaze read/write calls must stay behind DAGForge JSON/TOML wrappers:" \
  '\bglz::(read|write)[A-Za-z0-9_]*\b' \
  --glob '!include/dagforge/util/json.hpp' \
  --glob '!include/dagforge/config/toml_util.hpp' \
  --glob '!tests/**' \
  --glob '!bench/**'

report_matches \
  "first-party code must not depend on private Beast APIs:" \
  'boost::beast::detail::'

report_matches \
  "enum metadata is owned by Glaze; Boost.Describe/MP11 are forbidden:" \
  'BOOST_DESCRIBE_ENUM|boost/describe/|boost/mp11/'

report_matches \
  "use std::filesystem rather than Boost.Filesystem:" \
  'boost::filesystem|boost/filesystem'

report_matches \
  "Boost.URL parsing must use result-returning parse functions:" \
  'boost::urls::(url_view|params_view)[[:space:]]*[({]'

if obsolete_link_matches=$(rg -n -- '-lboost_(filesystem|system)' src/buildfile 2>/dev/null); then
  echo "obsolete Boost dynamic dependencies are forbidden:" >&2
  printf '%s\n' "$obsolete_link_matches" >&2
  failures=1
fi

if scaffold_matches=$(rg -n "${cpp_globs[@]}" \
    'boost::asio::(co_spawn|post)|boost::asio::steady_timer|\.io\([[:space:]]*\)' \
    docs/templates 2>/dev/null); then
  echo "scaffolds must use Runtime and DAGForge timer abstractions:" >&2
  printf '%s\n' "$scaffold_matches" >&2
  failures=1
fi

coroutine_iife_matches=$(
  while IFS= read -r file; do
    [[ "$file" != "tests/bench_mysql_storage.cpp" ]] || continue
    awk -v file="$file" '
      function count_char(text, pattern, copy, count) {
        copy = text
        count = gsub(pattern, "", copy)
        return count
      }

      BEGIN {
        candidate = 0
        active = 0
        balance = 0
        start_line = 0
        signature = ""
      }

      {
        line = $0
        if (!candidate && !active && line ~ /\[[^]]*\]/) {
          candidate = 1
          start_line = NR
          signature = line "\n"
        } else if (candidate) {
          signature = signature line "\n"
        }

        if (candidate &&
            signature ~ /->[[:space:]]*(spawn_task|task<[^>]+>)/ &&
            signature ~ /\{/) {
          active = 1
          candidate = 0
          balance = count_char(signature, "\\{") - count_char(signature, "\\}")
        } else if (candidate && NR - start_line > 8) {
          candidate = 0
          signature = ""
        } else if (active && NR != start_line) {
          balance += count_char(line, "\\{") - count_char(line, "\\}")
        }

        if (active && balance <= 0) {
          if (line ~ /\}\(\)[,;)]/) {
            print file ":" start_line ": immediately invoked coroutine lambda"
          }
          active = 0
          balance = 0
          signature = ""
        }
      }
    ' "$file"
  done < <(find "${cpp_roots[@]}" -type f \
    \( -name '*.hpp' -o -name '*.cpp' -o -name '*.inc' \) | sort)
)
if [[ -n "$coroutine_iife_matches" ]]; then
  echo "detached coroutine work must not use an immediately invoked capturing lambda:" >&2
  printf '%s\n' "$coroutine_iife_matches" >&2
  failures=1
fi

report_matches \
  "route shard ownership through util::shard_of(value, count), without pre-hashing:" \
  'util::shard_of[[:space:]]*\([[:space:]]*std::hash'

report_matches \
  "first-party C++ must use DAGForge logging instead of raw console output:" \
  'std::cout|std::cerr|\bprintf[[:space:]]*\('

report_matches \
  "forbidden duplicate dependency detected:" \
  'nlohmann|toml\+\+|<fmt/|boost/container/pmr'

if tab_matches=$(rg -n "${cpp_globs[@]}" $'\t' "${cpp_roots[@]}" 2>/dev/null); then
  echo "tabs are forbidden in first-party C++:" >&2
  printf '%s\n' "$tab_matches" >&2
  failures=1
fi

while IFS= read -r -d '' header; do
  if ! grep -q '^#pragma once$' "$header"; then
    echo "public header is missing #pragma once: $header" >&2
    failures=1
  fi
  if ! grep -q 'DAGFORGE_BUILDING_MODULE_INTERFACE' "$header"; then
    echo "public header is missing module-interface include guard: $header" >&2
    failures=1
  fi
done < <(find include/dagforge -type f -name '*.hpp' -print0)

if ((failures != 0)); then
  exit 1
fi

printf 'AGENTS.md convention check passed\n'
