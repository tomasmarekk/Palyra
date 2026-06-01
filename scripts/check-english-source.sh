#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

source_roots=(.github apps crates fixtures fuzz infra schemas scripts)

if ! git ls-files -- "${source_roots[@]}" | grep -q .; then
  echo "English source scan skipped; no tracked source files found."
  exit 0
fi

diacritic_pattern='[\x{00C1}\x{00E1}\x{010C}\x{010D}\x{010E}\x{010F}\x{00C9}\x{00E9}\x{011A}\x{011B}\x{00CD}\x{00ED}\x{0147}\x{0148}\x{00D3}\x{00F3}\x{0158}\x{0159}\x{0160}\x{0161}\x{0164}\x{0165}\x{00DA}\x{00FA}\x{016E}\x{016F}\x{00DD}\x{00FD}\x{017D}\x{017E}]'

c="c"
s="s"
cze="Cze"
cz_lower="cze"
ch="ch"
locale_pattern="(['\"]${c}${s}(-CZ)?['\"]|${c}${s}-CZ|${cze}${ch}|${cz_lower}${ch})"

git_grep_or_empty() {
  local output
  local status

  set +e
  output="$(git grep --line-number -I "$@" -- "${source_roots[@]}" 2>&1)"
  status=$?
  set -e

  case "$status" in
    0)
      printf '%s\n' "$output"
      ;;
    1)
      ;;
    *)
      printf '%s\n' "$output" >&2
      exit "$status"
      ;;
  esac
}

diacritic_matches="$(git_grep_or_empty --perl-regexp -e "$diacritic_pattern")"
locale_matches="$(git_grep_or_empty --extended-regexp -e "$locale_pattern")"

matches="${diacritic_matches}"$'\n'"${locale_matches}"
if [[ -n "${matches//[[:space:]]/}" ]]; then
  echo "Non-English source text detected in tracked source files:" >&2
  echo "$matches" >&2
  exit 1
fi

echo "English source scan passed."
