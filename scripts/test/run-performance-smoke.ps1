[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$rootDir = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $rootDir

if (-not (Test-Path (Join-Path $rootDir "apps\web\node_modules"))) {
    npm --prefix apps/web run bootstrap
} else {
    npm --prefix apps/web run verify-install
}

cargo test -p palyra-daemon --locked retention_housekeeping
cargo test -p palyra-auth --locked refresh_due_profiles_marks_transport_failure_without_retry_spam
cargo test --manifest-path apps/desktop/src-tauri/Cargo.toml --locked desktop_refresh_payload_reuses_single_snapshot_build_for_home_and_onboarding_views
cargo test --manifest-path apps/desktop/src-tauri/Cargo.toml --locked support_bundle_export_plan_capture_does_not_hold_supervisor_lock

npm --prefix apps/web run perf:smoke
