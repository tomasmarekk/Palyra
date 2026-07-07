#!/usr/bin/env bash
set -euo pipefail

roadmap_dir="${1:-roadmap/new_roadmap}"
summary_path="$roadmap_dir/summary.md"

if [[ ! -f "$summary_path" ]]; then
  echo "roadmap summary not found: $summary_path" >&2
  exit 1
fi

mapfile -t links < <(grep -Eo '\[M[0-9]{3} [^]]+\]\(milestones/[0-9]{3}_[a-z0-9_]+\.md\)' "$summary_path")
if [[ "${#links[@]}" -eq 0 ]]; then
  echo "summary.md does not contain milestone links" >&2
  exit 1
fi

declare -A seen_numbers=()
declare -A seen_links=()
minimum_section_count=7

for entry in "${links[@]}"; do
  number="$(sed -E 's/^\[M([0-9]{3}).*/\1/' <<<"$entry")"
  link="$(sed -E 's/^.*\((milestones\/[0-9]{3}_[a-z0-9_]+\.md)\)$/\1/' <<<"$entry")"
  file="$(basename "$link")"

  if [[ -n "${seen_numbers[$number]:-}" ]]; then
    echo "duplicate milestone number in summary.md: M$number" >&2
    exit 1
  fi
  if [[ -n "${seen_links[$link]:-}" ]]; then
    echo "duplicate milestone link in summary.md: $link" >&2
    exit 1
  fi
  seen_numbers[$number]=1
  seen_links[$link]=1

  if [[ "$file" != "${number}_"* ]]; then
    echo "milestone file does not start with its number: $file" >&2
    exit 1
  fi
  detail_path="$roadmap_dir/$link"
  if [[ ! -f "$detail_path" ]]; then
    echo "milestone detail missing: $link" >&2
    exit 1
  fi
  if ! grep -Eq "^# M${number}\b" "$detail_path"; then
    echo "milestone detail heading does not match M${number}: $link" >&2
    exit 1
  fi
  section_count="$(grep -Ec '^##[[:space:]]+[^[:space:]]' "$detail_path")"
  if [[ "$section_count" -lt "$minimum_section_count" ]]; then
    echo "milestone $link has too few detail sections: $section_count" >&2
    exit 1
  fi
done

echo "roadmap protocol ok: ${#links[@]} milestones"
