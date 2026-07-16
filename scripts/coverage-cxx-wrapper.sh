#!/usr/bin/env bash
set -euo pipefail

real_cxx="${DAGFORGE_COVERAGE_REAL_CXX:-clang++}"
arguments=("$@")

"$real_cxx" "${arguments[@]}"

compile=false
module_interface=false
output=
source=
for ((index = 0; index < ${#arguments[@]}; ++index)); do
  argument="${arguments[index]}"
  case "$argument" in
  -c)
    compile=true
    ;;
  -o)
    if ((index + 1 < ${#arguments[@]})); then
      output="${arguments[index + 1]}"
      ((++index))
    fi
    ;;
  -x)
    if ((index + 1 < ${#arguments[@]})); then
      language="${arguments[index + 1]}"
      if [[ "$language" == "c++-module" ]]; then
        module_interface=true
      fi
      ((++index))
    fi
    ;;
  *.cpp)
    source="$argument"
    ;;
  esac
done

if [[ "$compile" != true || "$module_interface" == true ||
      -z "$output" || -z "$source" || "$output" != *.o ]]; then
  exit 0
fi

preprocess=()
skip_next=false
for argument in "${arguments[@]}"; do
  if [[ "$skip_next" == true ]]; then
    skip_next=false
    continue
  fi
  case "$argument" in
  -c)
    preprocess+=("-E")
    ;;
  -o)
    skip_next=true
    ;;
  *)
    preprocess+=("$argument")
    ;;
  esac
done

preprocessed="${output}.ii"
temporary="${preprocessed}.tmp.$$"
cleanup() { rm -f -- "$temporary"; }
trap cleanup EXIT
"$real_cxx" "${preprocess[@]}" -o "$temporary"
mv -f -- "$temporary" "$preprocessed"
trap - EXIT
