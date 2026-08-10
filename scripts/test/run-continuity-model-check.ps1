[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$rootDir = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $rootDir

# This in-process matrix checks the recovery model only. The public campaign gate
# must additionally execute the real daemon termination and restart scenarios.
cargo test -p palyra-common --test continuity_campaign_contract --locked
cargo test -p palyra-daemon --lib recovery_barrier_rejects_input_until_ready --locked
cargo test -p palyra-daemon --lib startup_recovery_ --locked
cargo test -p palyra-daemon --lib "stuck_run_remediation::tests" --locked
cargo test -p palyra-daemon --lib `
    persisted_session_pin_survives_restart_config_change_and_key_rotation --locked
cargo test -p palyra-daemon --lib `
    corrupt_flow_dependencies_fail_closed_after_runtime_restart --locked
cargo test -p palyra-daemon --lib `
    persisted_live_process_identity_mismatch_fails_closed --locked
cargo run -p palyra-cli --example run_continuity_campaign_gate --locked
