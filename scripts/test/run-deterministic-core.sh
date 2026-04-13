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

  echo "cargo is required for deterministic core checks." >&2
  exit 1
}

cd "$ROOT_DIR"

CARGO_BIN="$(resolve_cargo)"

DESKTOP_TEST_TARGET_DIR="$(mktemp -d "$ROOT_DIR/target/prepush-desktop-tests.XXXXXX")"
DESKTOP_TEST_TARGET_DIR_NATIVE="$DESKTOP_TEST_TARGET_DIR"

if command -v cygpath >/dev/null 2>&1; then
  DESKTOP_TEST_TARGET_DIR_NATIVE="$(cygpath -w "$DESKTOP_TEST_TARGET_DIR")"
fi

cleanup_desktop_test_target_dir() {
  rm -rf "$DESKTOP_TEST_TARGET_DIR"
}

trap cleanup_desktop_test_target_dir EXIT

run_desktop_test() {
  "$CARGO_BIN" test \
    --manifest-path apps/desktop/src-tauri/Cargo.toml \
    --target-dir "$DESKTOP_TEST_TARGET_DIR_NATIVE" \
    --locked \
    "$@"
}

bash "$ROOT_DIR/scripts/test/ensure-js-workspace.sh"

bash "$ROOT_DIR/scripts/test/ensure-desktop-ui.sh"

bash "$ROOT_DIR/scripts/test/check-deterministic-fixtures.sh"

"$CARGO_BIN" build -p palyra-cli --locked

run_desktop_test state_file_initialization_seeds_onboarding_defaults
run_desktop_test openai_api_key_connect_bootstraps_console_session_and_posts_payload
run_desktop_test openai_oauth_bootstrap_and_callback_state_reuse_console_session
run_desktop_test openai_profile_actions_hit_expected_routes_including_reconnect
run_desktop_test discord_onboarding_preflight_apply_and_verify_use_console_session_and_csrf
run_desktop_test support_bundle_export_plan_capture_does_not_hold_supervisor_lock

"$CARGO_BIN" test -p palyra-daemon --test openai_auth_surface --locked
"$CARGO_BIN" test -p palyra-daemon --test admin_surface --locked console_support_bundle_job_lifecycle_publishes_deterministic_completion_state
"$CARGO_BIN" test -p palyra-daemon --test gateway_grpc --locked grpc_route_message_with_fake_adapter_emits_reply_and_journal_events
"$CARGO_BIN" test -p palyra-daemon --test gateway_grpc --locked grpc_route_message_preserves_attachment_metadata_in_outbound_and_journal
"$CARGO_BIN" test -p palyra-daemon --test gateway_grpc --locked grpc_approvals_service_persists_and_exports_denied_tool_approval

npm run web:test -- \
  src/App.openai-auth.test.tsx \
  src/App.config-access-support.test.tsx \
  src/App.runtime-operations.test.tsx \
  src/App.test.tsx \
  src/consoleApi.test.ts
