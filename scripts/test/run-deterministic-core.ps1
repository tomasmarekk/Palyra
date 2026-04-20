[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$rootDir = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $rootDir

& (Join-Path $PSScriptRoot "ensure-js-workspace.ps1")

& (Join-Path $PSScriptRoot "ensure-desktop-ui.ps1")

& (Join-Path $PSScriptRoot "check-deterministic-fixtures.ps1")
& (Join-Path $PSScriptRoot "run-retrieval-benchmark.ps1")

cargo build -p palyra-cli --locked

cargo test --manifest-path apps/desktop/src-tauri/Cargo.toml --locked state_file_initialization_seeds_onboarding_defaults
cargo test --manifest-path apps/desktop/src-tauri/Cargo.toml --locked openai_api_key_connect_bootstraps_console_session_and_posts_payload
cargo test --manifest-path apps/desktop/src-tauri/Cargo.toml --locked openai_oauth_bootstrap_and_callback_state_reuse_console_session
cargo test --manifest-path apps/desktop/src-tauri/Cargo.toml --locked openai_profile_actions_hit_expected_routes_including_reconnect
cargo test --manifest-path apps/desktop/src-tauri/Cargo.toml --locked discord_onboarding_preflight_apply_and_verify_use_console_session_and_csrf
cargo test --manifest-path apps/desktop/src-tauri/Cargo.toml --locked support_bundle_export_plan_capture_does_not_hold_supervisor_lock

cargo test -p palyra-cli --test workflow_regression_contract --locked
cargo test -p palyra-daemon --test openai_auth_surface --locked
cargo test -p palyra-daemon --test admin_surface --locked console_support_bundle_job_lifecycle_publishes_deterministic_completion_state
cargo test -p palyra-daemon --test gateway_grpc --locked grpc_route_message_with_fake_adapter_emits_reply_and_journal_events
cargo test -p palyra-daemon --test gateway_grpc --locked grpc_route_message_preserves_attachment_metadata_in_outbound_and_journal
cargo test -p palyra-daemon --test gateway_grpc --locked grpc_approvals_service_persists_and_exports_denied_tool_approval

npm run web:test -- `
  src/App.openai-auth.test.tsx `
  src/App.config-access-support.test.tsx `
  src/App.runtime-operations.test.tsx `
  src/App.test.tsx `
  src/consoleApi.test.ts
