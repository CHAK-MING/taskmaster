#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
source "${repo_root}/scripts/build2-common.sh"

config_name="${BUILD2_CONFIG_NAME:-glaze-fuzz-clang}"
config_root="${BUILD2_CONFIG_ROOT:-${XDG_DATA_HOME:-$HOME/.local/share}/build2-configs}"
config_dir="${BUILD2_CONFIG_DIR:-${config_root}/dagforge-${config_name}}"
fuzzer="${config_dir}/dagforge/tests/glaze-parser-fuzz"
seed_corpus="${repo_root}/tests/fuzz_corpus/glaze_parser"
artifact_dir="${FUZZ_ARTIFACT_DIR:-/tmp/dagforge-glaze-fuzz-artifacts}"

(
  export CXX="${CXX:-clang++}"
  export BUILD2_CONFIG_NAME="$config_name"
  export BUILD2_CONFIG_ROOT="$config_root"
  export BUILD2_CONFIG_FORWARD=0
  export BUILD2_CC_COPTIONS="-fsanitize=fuzzer-no-link,address,undefined -fno-omit-frame-pointer"
  export BUILD2_CC_LOPTIONS="-fsanitize=address,undefined"
  export BUILD2_TARGETS="tests/exe{glaze-parser-fuzz}"
  run_build2_config "$repo_root" "$config_name"
)

if [[ ! -x "$fuzzer" ]]; then
  echo "fuzzer binary not found: $fuzzer" >&2
  exit 1
fi

temporary_corpus=$(mktemp -d)
trap 'rm -rf "$temporary_corpus"' EXIT
mkdir -p "$artifact_dir"

for seed in valid_json valid_toml truncated_json_escape truncated_toml_array; do
  cp "${seed_corpus}/${seed}" "${temporary_corpus}/${seed}"
done

"$fuzzer" \
  -runs="${FUZZ_RUNS:-10000}" \
  -max_len="${FUZZ_MAX_LEN:-4096}" \
  -artifact_prefix="${artifact_dir}/" \
  "$temporary_corpus" \
  "$@"
