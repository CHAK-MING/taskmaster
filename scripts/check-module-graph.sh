#!/usr/bin/env bash

set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
modules_dir="${repo_root}/src/modules"
buildfile="${repo_root}/src/buildfile"

declare -A expected_module=(
  [base.cppm]=dagforge.base
  [client.cppm]=dagforge.http
  [config.cppm]=dagforge.config
  [core.cppm]=dagforge.core
  [dagforge.cppm]=dagforge.foundation
  [domain.cppm]=dagforge.domain
  [executor.cppm]=dagforge.executor
  [io.cppm]=dagforge.io
  [util.cppm]=dagforge.util
  [workflow.cppm]=dagforge.workflow
)

declare -A module_rank=(
  [dagforge.base]=0
  [dagforge.domain]=0
  [dagforge.io]=0
  [dagforge.core]=1
  [dagforge.util]=1
  [dagforge.executor]=2
  [dagforge.http]=2
  [dagforge.config]=3
  [dagforge.workflow]=3
  [dagforge.foundation]=5
)

declare -A seen_file=()
declare -A seen_module=()

module_declaration() {
  awk '/^export module / {
    value = $3
    sub(/;$/, "", value)
    print value
  }' "$1"
}

module_imports() {
  awk '/^(export[[:space:]]+)?import[[:space:]]+dagforge\./ {
    value = ($1 == "export" ? $3 : $2)
    sub(/;$/, "", value)
    print value
  }' "$1"
}

implementation_module() {
  awk '/^module dagforge\./ {
    value = $2
    sub(/;$/, "", value)
    print value
    exit
  }' "$1"
}

validate_import_direction() {
  local module=$1
  local path=$2
  local module_level=${module_rank[${module}]:-}

  if [[ -z "${module_level}" ]]; then
    echo "missing architecture rank for module: ${module}" >&2
    exit 1
  fi

  while IFS= read -r imported; do
    [[ -n "${imported}" ]] || continue
    imported_rank=${module_rank[${imported}]:-}
    if [[ -z "${imported_rank}" ]]; then
      echo "module ${module} imports unregistered module ${imported}" >&2
      exit 1
    fi
    if (( imported_rank >= module_level )); then
      echo "invalid module dependency in ${path}: ${module} -> ${imported}" >&2
      echo "imports must point strictly toward a lower architecture rank" >&2
      exit 1
    fi
  done < <(module_imports "${path}")
}

for path in "${modules_dir}"/*.cppm; do
  file=$(basename "${path}")
  expected=${expected_module[${file}]:-}
  if [[ -z "${expected}" ]]; then
    echo "unregistered module interface: src/modules/${file}" >&2
    exit 1
  fi

  actual=$(module_declaration "${path}")
  if [[ -z "${actual}" ]]; then
    echo "missing export module declaration: src/modules/${file}" >&2
    exit 1
  fi
  if [[ "${actual}" != "${expected}" ]]; then
    echo "module name mismatch: src/modules/${file}: expected ${expected}, got ${actual}" >&2
    exit 1
  fi
  if [[ -n "${seen_module[${actual}]:-}" ]]; then
    echo "duplicate module declaration: ${actual}" >&2
    exit 1
  fi

  seen_file[${file}]=1
  seen_module[${actual}]=1

  validate_import_direction "${actual}" "${path}"

  stem=${file%.cppm}
  expected_mapping="mxx{modules/${stem}}@modules/: cxx.module_name = ${actual}"
  if ! grep -Fqx "${expected_mapping}" "${buildfile}"; then
    echo "missing build2 module mapping: ${expected_mapping}" >&2
    exit 1
  fi
done

for file in "${!expected_module[@]}"; do
  if [[ -z "${seen_file[${file}]:-}" ]]; then
    echo "registered module interface is missing: src/modules/${file}" >&2
    exit 1
  fi
done

for path in "${modules_dir}"/*.cpp; do
  [[ -e "${path}" ]] || continue
  module=$(implementation_module "${path}")
  [[ -n "${module}" ]] || continue
  if [[ -z "${seen_module[${module}]:-}" ]]; then
    echo "implementation unit declares unregistered module: ${module}" >&2
    exit 1
  fi
  validate_import_direction "${module}" "${path}"
done

foundation_path="${modules_dir}/dagforge.cppm"
for module in "${!module_rank[@]}"; do
  [[ "${module}" != "dagforge.foundation" ]] || continue
  if ! grep -Fqx "export import ${module};" "${foundation_path}"; then
    echo "dagforge.foundation does not re-export ${module}" >&2
    exit 1
  fi
done

if grep -RInE '^((export )?import dagforge;|export module dagforge;|export import dagforge\.(app|client|storage);)' \
    "${modules_dir}" "${repo_root}/tests/modules-foundation-smoke.cpp" \
    "${repo_root}/tests/modules_core_smoke_test.cpp"; then
  echo "legacy or semantically broad module name remains" >&2
  exit 1
fi

printf 'module graph check passed (%d modules)\n' "${#expected_module[@]}"
