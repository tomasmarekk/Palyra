[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$rootDir = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $rootDir

cargo test -p palyra-safety --test critical_attack_scenarios --locked
cargo test -p palyra-egress-proxy --test critical_attack_scenarios --locked
cargo test -p palyra-workerd --test critical_attack_scenarios --locked
cargo test -p palyra-daemon --lib --locked webhook_test_integration_surfaces_safety_blocking
cargo test -p palyra-daemon --test gateway_grpc --locked grpc_run_stream_policy_decision_journal_includes_execution_gate_report_when_rollout_enabled
cargo test -p palyra-daemon --test gateway_grpc --locked grpc_route_message_pending_approval_records_execution_gate_report_when_rollout_enabled
