[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$rootDir = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $rootDir

cargo test -p palyra-connectors --lib --locked gateway_envelope_reconnect_resume_cycles_remain_stable_under_soak
cargo test -p palyra-connectors --lib --locked repeated_dead_letter_recovery_cycles_keep_queue_accounting_stable
cargo test -p palyra-daemon --lib --locked backfill_repairs_access_registry_records_idempotently
