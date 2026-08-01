#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

resolve_cargo() {
  if command -v cargo >/dev/null 2>&1; then
    command -v cargo
    return 0
  fi
  if command -v cargo.exe >/dev/null 2>&1; then
    command -v cargo.exe
    return 0
  fi

  local candidates=(
    "${HOME:-}/.cargo/bin/cargo"
    "${HOME:-}/.cargo/bin/cargo.exe"
    "${USERPROFILE:-}/.cargo/bin/cargo.exe"
  )
  local candidate
  for candidate in "${candidates[@]}"; do
    if [[ -n "$candidate" && -x "$candidate" ]]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done

  echo "cargo is required for performance smoke checks." >&2
  exit 1
}

cd "$ROOT_DIR"

CARGO_BIN="$(resolve_cargo)"

bash "$ROOT_DIR/scripts/test/ensure-js-workspace.sh"

bash "$ROOT_DIR/scripts/test/ensure-desktop-ui.sh"

"$CARGO_BIN" test -p palyra-daemon --lib --locked retention_housekeeping
"$CARGO_BIN" test -p palyra-daemon --lib --locked \
  application::core_stability::performance::tests::core_performance_baseline_is_release_qualified
"$CARGO_BIN" test -p palyra-daemon --lib --locked \
  application::mcp_runtime::registry::tests::capacity_soak_drains_actor_fleet_without_orphans_across_restarts
"$CARGO_BIN" test -p palyra-daemon --lib --locked \
  journal::state_health::tests::retry_sqlite_busy_retries_transient_busy_errors
"$CARGO_BIN" test -p palyra-auth --lib --locked refresh_due_profiles_marks_transport_failure_without_retry_spam
"$CARGO_BIN" test --manifest-path apps/desktop/src-tauri/Cargo.toml --lib --locked desktop_refresh_payload_reuses_single_snapshot_build_for_home_and_onboarding_views
"$CARGO_BIN" test --manifest-path apps/desktop/src-tauri/Cargo.toml --lib --locked support_bundle_export_plan_capture_does_not_hold_supervisor_lock

npm run web:perf-smoke
