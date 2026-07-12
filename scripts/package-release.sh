#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/package-release.sh <tag> <bundle-root> <output-dir>

Creates a reproducible Linux release archive and checksum file from a staged
bundle. The bundle root must contain the CLI, configuration, licenses,
documentation, build metadata, and runtime dependency inventory.
EOF
}

if [[ $# -ne 3 ]]; then
  usage >&2
  exit 2
fi

tag=$1
bundle_root=$(cd "$2" && pwd)
output_dir=$3

if [[ ! "$tag" =~ ^[0-9A-Za-z][0-9A-Za-z._-]*$ ]]; then
  echo "invalid release tag: $tag" >&2
  exit 2
fi

for required in \
  bin/dagforge \
  libexec/dagforge/minijail/minijail0 \
  libexec/dagforge/minijail/dagforge_command.bpf \
  libexec/dagforge/minijail/LICENSE.minijail \
  libexec/dagforge/minijail/REVISION \
  system_config.toml \
  LICENSE \
  README.md \
  README_CN.md \
  BUILD-INFO \
  RUNTIME-DEPENDENCIES; do
  if [[ ! -e "${bundle_root}/${required}" ]]; then
    echo "release bundle is missing ${required}" >&2
    exit 1
  fi
done

case "$(uname -m)" in
  x86_64 | amd64)
    arch=x86_64
    ;;
  aarch64 | arm64)
    arch=arm64
    ;;
  *)
    echo "unsupported release architecture: $(uname -m)" >&2
    exit 1
    ;;
esac

package_name="dagforge-${tag}-linux-${arch}"
archive_name="${package_name}.tar.gz"
source_date_epoch=${SOURCE_DATE_EPOCH:-0}

mkdir -p "$output_dir"
output_dir=$(cd "$output_dir" && pwd)
work_dir=$(mktemp -d)
trap 'rm -rf "$work_dir"' EXIT

package_root="${work_dir}/${package_name}"
mkdir -p "$package_root"
cp -a "${bundle_root}/." "$package_root/"

cat >"${package_root}/RELEASE" <<EOF
version=${tag}
platform=linux-${arch}
source_date_epoch=${source_date_epoch}
source_revision=${GITHUB_SHA:-unknown}
EOF

find "$package_root" -exec touch -h -d "@${source_date_epoch}" {} +

tar --sort=name \
    --owner=0 \
    --group=0 \
    --numeric-owner \
    --mtime="@${source_date_epoch}" \
    -C "$work_dir" \
    -cf - "$package_name" \
  | gzip -n >"${output_dir}/${archive_name}"

(
  cd "$output_dir"
  sha256sum "$archive_name" >"sha256sums-${tag}.txt"
)

printf '%s\n' "${output_dir}/${archive_name}"
