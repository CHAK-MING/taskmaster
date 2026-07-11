#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
source "${repo_root}/scripts/build2-common.sh"

export CXX="${CXX:-clang++}"
export BUILD2_CONFIG_NAME="${BUILD2_CONFIG_NAME:-glaze-fuzz-clang}"
export BUILD2_CONFIG_ROOT="${BUILD2_CONFIG_ROOT:-${XDG_DATA_HOME:-$HOME/.local/share}/build2-configs}"
export BUILD2_CONFIG_FORWARD=0
export BUILD2_CC_COPTIONS="-fsanitize=fuzzer-no-link,address,undefined -fno-omit-frame-pointer"
export BUILD2_CC_LOPTIONS="-fsanitize=address,undefined"
export BUILD2_TARGETS="tests/exe{glaze-parser-fuzz}"

run_build2_config "$repo_root" "$BUILD2_CONFIG_NAME" "$@"
