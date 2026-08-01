[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$rootDir = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $rootDir

& (Join-Path $PSScriptRoot "ensure-js-workspace.ps1")

& (Join-Path $PSScriptRoot "ensure-desktop-ui.ps1")

cargo test -p palyra-daemon --locked retention_housekeeping
cargo test -p palyra-daemon --locked application::core_stability::performance::tests::core_performance_baseline_is_release_qualified
cargo test -p palyra-auth --locked refresh_due_profiles_marks_transport_failure_without_retry_spam
cargo test --manifest-path apps/desktop/src-tauri/Cargo.toml --locked desktop_refresh_payload_reuses_single_snapshot_build_for_home_and_onboarding_views
cargo test --manifest-path apps/desktop/src-tauri/Cargo.toml --locked support_bundle_export_plan_capture_does_not_hold_supervisor_lock

npm run web:perf-smoke
