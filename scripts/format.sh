#!/usr/bin/env bash

set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

mode=format
scope=changed
declare -a requested_paths=()

usage() {
  cat <<'EOF'
usage: bash scripts/format.sh [--check] [--all] [path ...]

默认格式化相对 HEAD 新增、修改或未跟踪的第一方 C++ 文件。

  --check  只检查，不修改文件
  --all    处理全部第一方 C++ 文件
EOF
}

while (($# > 0)); do
  case "$1" in
  --check)
    mode=check
    ;;
  --all)
    scope=all
    ;;
  -h | --help)
    usage
    exit 0
    ;;
  --)
    shift
    requested_paths+=("$@")
    break
    ;;
  -*)
    echo "unknown option: $1" >&2
    usage >&2
    exit 2
    ;;
  *)
    requested_paths+=("$1")
    ;;
  esac
  shift
done

clang_format=${CLANG_FORMAT:-clang-format}
if ! command -v "$clang_format" >/dev/null 2>&1; then
  echo "missing clang-format executable: $clang_format" >&2
  exit 1
fi

is_first_party_cpp() {
  local path=$1
  case "$path" in
  include/* | src/* | tests/* | bench/* | docs/templates/*) ;;
  *) return 1 ;;
  esac
  case "$path" in
  *.hpp | *.cpp | *.cppm | *.inc) return 0 ;;
  *) return 1 ;;
  esac
}

declare -a candidates=()
if ((${#requested_paths[@]} > 0)); then
  for path in "${requested_paths[@]}"; do
    if [[ -d "$path" ]]; then
      while IFS= read -r -d '' file; do
        candidates+=("${file#./}")
      done < <(find "$path" -type f \( -name '*.hpp' -o -name '*.cpp' -o -name '*.cppm' -o -name '*.inc' \) -print0)
    elif [[ -f "$path" ]]; then
      candidates+=("${path#./}")
    else
      echo "path does not exist: $path" >&2
      exit 1
    fi
  done
elif [[ "$scope" == all ]]; then
  while IFS= read -r -d '' file; do
    candidates+=("${file#./}")
  done < <(find include src tests bench docs/templates -type f \( -name '*.hpp' -o -name '*.cpp' -o -name '*.cppm' -o -name '*.inc' \) -print0 2>/dev/null)
else
  while IFS= read -r file; do
    candidates+=("$file")
  done < <(
    {
      git diff --name-only --diff-filter=ACMR HEAD
      git diff --cached --name-only --diff-filter=ACMR HEAD
      git ls-files --others --exclude-standard
    } | sort -u
  )
fi

declare -a files=()
while IFS= read -r file; do
  [[ -f "$file" ]] || continue
  is_first_party_cpp "$file" || continue
  files+=("$file")
done < <(printf '%s\n' "${candidates[@]}" | sed '/^$/d' | sort -u)

if ((${#files[@]} == 0)); then
  echo "no first-party C++ files selected"
  exit 0
fi

if [[ "$mode" == check ]]; then
  "$clang_format" --style=file --dry-run --Werror "${files[@]}"
  printf 'clang-format check passed (%d files)\n' "${#files[@]}"
else
  "$clang_format" --style=file -i "${files[@]}"
  printf 'clang-format applied (%d files)\n' "${#files[@]}"
fi
