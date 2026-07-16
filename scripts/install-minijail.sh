#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
revision="$(tr -d '[:space:]' < "${repo_root}/sandbox/MINIJAIL_REVISION")"
install_dir="${MINIJAIL_INSTALL_DIR:-${HOME}/.local/libexec/dagforge/minijail}"
jobs="${BUILD_JOBS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || printf '1')}"

for tool in git make python3 tar timeout; do
  if ! command -v "${tool}" >/dev/null 2>&1; then
    printf 'missing required tool: %s\n' "${tool}" >&2
    exit 1
  fi
done

if [[ ! -f /usr/include/sys/capability.h ]]; then
  printf 'missing libcap development headers (install libcap-dev)\n' >&2
  exit 1
fi

work_dir="$(mktemp -d)"
trap 'rm -rf "${work_dir}"' EXIT
source_dir="${work_dir}/minijail"
mkdir -p "${source_dir}"

if [[ -n "${MINIJAIL_SOURCE_DIR:-}" ]]; then
  actual_revision="$(git -C "${MINIJAIL_SOURCE_DIR}" rev-parse HEAD)"
  if [[ "${actual_revision}" != "${revision}" ]]; then
    printf 'Minijail source revision mismatch: expected %s, got %s\n' \
      "${revision}" "${actual_revision}" >&2
    exit 1
  fi
  git -C "${MINIJAIL_SOURCE_DIR}" archive "${revision}" | tar -x -C "${source_dir}"
else
  git -C "${source_dir}" init -q
  git -C "${source_dir}" remote add origin invalid
  if [[ -n "${MINIJAIL_REPOSITORY_URL:-}" ]]; then
    repository_urls=("${MINIJAIL_REPOSITORY_URL}")
  else
    repository_urls=(
      "https://android.googlesource.com/platform/external/minijail"
      "https://github.com/google/minijail.git"
    )
  fi
  fetched=0
  for repository_url in "${repository_urls[@]}"; do
    git -C "${source_dir}" remote set-url origin "${repository_url}"
    for attempt in 1 2 3; do
      if timeout 45s git -C "${source_dir}" fetch -q --depth=1 origin \
          "${revision}"; then
        fetched=1
        break 2
      fi
      printf 'Minijail fetch failed from %s (attempt %s/3)\n' \
        "${repository_url}" "${attempt}" >&2
      sleep "${attempt}"
    done
  done
  if [[ "${fetched}" != "1" ]]; then
    printf 'unable to fetch pinned Minijail revision %s from configured sources\n' \
      "${revision}" >&2
    exit 1
  fi
  git -C "${source_dir}" checkout -q --detach FETCH_HEAD
fi

(
  cd "${source_dir}"
  make -j "${jobs}" all
  make constants.json
)

filtered_policy="${work_dir}/dagforge_command.policy"
python3 - \
  "${source_dir}/constants.json" \
  "${repo_root}/sandbox/dagforge_command.policy" \
  "${filtered_policy}" <<'PY'
import json
import pathlib
import sys

constants_path, policy_path, output_path = map(pathlib.Path, sys.argv[1:])
available = set(json.loads(constants_path.read_text())["syscalls"])
lines = []
for raw_line in policy_path.read_text().splitlines():
    stripped = raw_line.strip()
    if not stripped or stripped.startswith("#") or stripped.startswith("@"):
        lines.append(raw_line)
        continue
    syscall = stripped.split(":", 1)[0].strip()
    if syscall in available:
        lines.append(raw_line)
    else:
        print(f"skipping unavailable syscall for this architecture: {syscall}",
              file=sys.stderr)
output_path.write_text("\n".join(lines) + "\n")
PY

python3 "${source_dir}/tools/compile_seccomp_policy.py" \
  --arch-json "${source_dir}/constants.json" \
  --denylist \
  "${filtered_policy}" \
  "${work_dir}/dagforge_command.bpf"

install -d "${install_dir}"
install -m 0755 "${source_dir}/minijail0" "${install_dir}/minijail0"
install -m 0644 "${work_dir}/dagforge_command.bpf" \
  "${install_dir}/dagforge_command.bpf"
install -m 0644 "${source_dir}/LICENSE" "${install_dir}/LICENSE.minijail"
printf '%s\n' "${revision}" > "${install_dir}/REVISION"

printf 'installed Minijail %s in %s\n' "${revision}" "${install_dir}"
