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

build_ascii_term() {
  printf '%b' "$1"
}

blocked_plain_terms=(
  "$(build_ascii_term '\x6b\x6f\x6e\x74\x65\x78\x74\x20\x6e\x61\x63\x74\x65\x6e')"
  "$(build_ascii_term '\x68\x6f\x74\x6f\x76\x6f')"
  "$(build_ascii_term '\x64\x6f\x6b\x6f\x6e\x63\x65\x6e\x6f')"
)
blocked_plain_pattern="(^|[^[:alnum:]_])($(IFS='|'; echo "${blocked_plain_terms[*]}"))([^[:alnum:]_]|$)"

blocked_escape_terms=(
  "$(build_ascii_term '\x53\x79\x73\x74\\\x78\x63\x33\\\x78\x61\x39\x6d')"
  "$(build_ascii_term '\x6e\x65\x6d\\\x78\x63\x35\\\x78\x61\x66\\\x78\x63\x35\\\x78\x62\x65\x65')"
  "$(build_ascii_term '\x50\\\x78\x63\x35\\\x78\x39\x39\\\x78\x63\x33\\\x78\x61\x64\x73\x74\x75\x70')"
  "$(build_ascii_term '\x75\x76\x65\x64\x65\x6e\\\x78\x63\x33\\\x78\x62\x64\x20\x73\x6f\x75\x62\x6f\x72')"
)
blocked_escape_args=()
for term in "${blocked_escape_terms[@]}"; do
  blocked_escape_args+=("-e" "$term")
done

diacritic_matches="$(git_grep_or_empty --perl-regexp -e "$diacritic_pattern")"
locale_matches="$(git_grep_or_empty --extended-regexp -e "$locale_pattern")"
blocked_plain_matches="$(git_grep_or_empty --perl-regexp --ignore-case -e "$blocked_plain_pattern")"
blocked_escape_matches="$(git_grep_or_empty --fixed-strings "${blocked_escape_args[@]}")"

matches="${diacritic_matches}"$'\n'"${locale_matches}"$'\n'"${blocked_plain_matches}"$'\n'"${blocked_escape_matches}"
if [[ -n "${matches//[[:space:]]/}" ]]; then
  echo "Non-English source text detected in tracked source files:" >&2
  echo "$matches" >&2
  exit 1
fi

echo "English source scan passed."
