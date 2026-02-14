#!/usr/bin/env bash
set -euo pipefail

if [[ ! -x "./gradlew" ]] && ! command -v gradle >/dev/null 2>&1; then
  echo "Android lint skipped: no Gradle entrypoint found."
  exit 0
fi

if [[ -x "./gradlew" ]]; then
  ./gradlew detekt
else
  gradle detekt
fi
