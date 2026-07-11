#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
source "${repo_root}/scripts/build2-common.sh"

export CXX="${CXX:-g++}"
export BUILD2_CONFIG_NAME="${BUILD2_CONFIG_NAME:-gcc}"

run_build2_config "$repo_root" "$BUILD2_CONFIG_NAME" "$@"
