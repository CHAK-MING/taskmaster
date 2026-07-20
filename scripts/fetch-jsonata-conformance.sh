#!/usr/bin/env bash
set -euo pipefail

readonly upstream_url="https://github.com/jsonata-js/jsonata.git"
readonly upstream_commit="6c7e95fdbf4405a1e741852a7cd8cd985b4305bb"
destination="${1:-${XDG_CACHE_HOME:-$HOME/.cache}/dagforge/jsonata-2.2.2}"

if [[ -d "$destination/.git" ]]; then
  actual=$(git -C "$destination" rev-parse HEAD)
  if [[ "$actual" != "$upstream_commit" ]]; then
    echo "JSONata checkout has unexpected commit: $actual" >&2
    exit 1
  fi
else
  mkdir -p "$(dirname -- "$destination")"
  git clone --filter=blob:none --no-checkout "$upstream_url" "$destination"
  git -C "$destination" fetch --depth 1 origin "$upstream_commit"
  git -C "$destination" checkout --detach "$upstream_commit"
fi

test -d "$destination/test/test-suite/groups"
test -d "$destination/test/test-suite/datasets"
test -f "$destination/LICENSE"
printf '%s\n' "$destination/test/test-suite"
