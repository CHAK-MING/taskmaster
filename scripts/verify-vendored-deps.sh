#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)

hash_tree() {
  local path="$1"
  (
    cd "$path"
    find . -type f -print0 \
      | sort -z \
      | xargs -0 sha256sum \
      | sha256sum \
      | cut -d' ' -f1
  )
}

verify_tree() {
  local name="$1"
  local path="$2"
  local expected="$3"
  local actual

  actual=$(hash_tree "$path")
  if [[ "$actual" != "$expected" ]]; then
    printf 'vendored dependency hash mismatch: %s\nexpected: %s\nactual:   %s\n' \
      "$name" "$expected" "$actual" >&2
    return 1
  fi
}

verify_file() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    echo "missing vendored dependency file: ${path#"$repo_root"/}" >&2
    return 1
  fi
}

verify_file "$repo_root/third_party/dependencies.json"
verify_file "$repo_root/third_party/glaze/LICENSE"
verify_file "$repo_root/third_party/CLI11/LICENSE"
verify_file "$repo_root/third_party/unordered_dense/LICENSE"
verify_file "$repo_root/third_party/prometheus-cpp-core/licenses/LICENSE"

grep -Eq 'major = 7;' "$repo_root/third_party/glaze/include/glaze/version.hpp"
grep -Eq 'minor = 8;' "$repo_root/third_party/glaze/include/glaze/version.hpp"
grep -Eq 'patch = 4;' "$repo_root/third_party/glaze/include/glaze/version.hpp"
grep -Eq '#define CLI11_VERSION "2\.6\.2"' \
  "$repo_root/third_party/CLI11/include/CLI/Version.hpp"
grep -Eq 'ANKERL_UNORDERED_DENSE_VERSION_MAJOR 4' \
  "$repo_root/third_party/unordered_dense/include/ankerl/unordered_dense.h"
grep -Eq 'ANKERL_UNORDERED_DENSE_VERSION_MINOR 8' \
  "$repo_root/third_party/unordered_dense/include/ankerl/unordered_dense.h"
grep -Eq 'ANKERL_UNORDERED_DENSE_VERSION_PATCH 1' \
  "$repo_root/third_party/unordered_dense/include/ankerl/unordered_dense.h"
verify_tree glaze "$repo_root/third_party/glaze" \
  24bf87d23fa1f7d9da1b8a5eec477331bce84742ab5c2da866ee4ba1e53cef6a
verify_tree CLI11 "$repo_root/third_party/CLI11" \
  dc122b60764f00552f1d08ca3cc213f0fd3046752f187eb46fe27df476d63dfd
verify_tree unordered_dense "$repo_root/third_party/unordered_dense" \
  0116d9bbd17ccf3da85d35a4c93b65ad8abd625a944282d9540a3ea40f756bfe
verify_tree prometheus-cpp-core "$repo_root/third_party/prometheus-cpp-core" \
  afb86d02fe7b9fab4d0480ef0c707153f86b8d46b99b5517d1b9fb732598184a

echo "vendored dependency verification passed"
