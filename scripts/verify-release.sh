#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/verify-release.sh <archive.tar.gz> [--no-exec]

Checks archive path safety, required files, and the declared ELF dependency
inventory. By default it also resolves shared libraries and runs the packaged
CLI. Use --no-exec on a different host after execution was verified inside the
release runtime image.
EOF
}

if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage >&2
  exit 2
fi

archive=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
execute=1
if [[ ${2:-} == "--no-exec" ]]; then
  execute=0
elif [[ $# -eq 2 ]]; then
  usage >&2
  exit 2
fi

if [[ ! -f "$archive" ]]; then
  echo "release archive not found: $archive" >&2
  exit 1
fi

if tar -tzf "$archive" | awk '
  /^\// { bad = 1 }
  /(^|\/)\.\.($|\/)/ { bad = 1 }
  END { exit bad ? 0 : 1 }
'; then
  echo "release archive contains an unsafe path" >&2
  exit 1
fi

work_dir=$(mktemp -d)
trap 'rm -rf "$work_dir"' EXIT

tar -xzf "$archive" -C "$work_dir"
mapfile -t roots < <(find "$work_dir" -mindepth 1 -maxdepth 1 -type d -print)
if [[ ${#roots[@]} -ne 1 ]]; then
  echo "release archive must contain exactly one top-level directory" >&2
  exit 1
fi

root=${roots[0]}
for required in \
  bin/dagforge \
  system_config.toml \
  LICENSE \
  README.md \
  README_CN.md \
  BUILD-INFO \
  RUNTIME-DEPENDENCIES \
  RELEASE; do
  if [[ ! -e "${root}/${required}" ]]; then
    echo "release archive is missing ${required}" >&2
    exit 1
  fi
done

if [[ ! -x "${root}/bin/dagforge" ]]; then
  echo "packaged bin/dagforge is not executable" >&2
  exit 1
fi

mapfile -t needed_libraries < <(
  readelf -d "${root}/bin/dagforge" 2>/dev/null \
    | awk '/\(NEEDED\)/ {
        value = $NF
        gsub(/^\[/, "", value)
        gsub(/\]$/, "", value)
        print value
      }'
)
if [[ ${#needed_libraries[@]} -eq 0 ]]; then
  echo "packaged bin/dagforge has no readable ELF dependency table" >&2
  exit 1
fi

for library in "${needed_libraries[@]}"; do
  if [[ "$library" == libdagforge.so* ]]; then
    echo "release binary still depends on build-tree libdagforge.so" >&2
    exit 1
  fi
  if ! grep -Fqx "$library" "${root}/RUNTIME-DEPENDENCIES"; then
    echo "runtime dependency inventory is missing ${library}" >&2
    exit 1
  fi
done

if [[ "$execute" == "1" ]]; then
  if ldd "${root}/bin/dagforge" 2>&1 | grep -Fq 'not found'; then
    ldd "${root}/bin/dagforge" >&2 || true
    echo "release binary has unresolved shared-library dependencies" >&2
    exit 1
  fi
  "${root}/bin/dagforge" --help >/dev/null
fi

printf 'release archive verification passed: %s\n' "$archive"
