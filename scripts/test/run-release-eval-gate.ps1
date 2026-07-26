[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$rootDir = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $rootDir

& (Join-Path $PSScriptRoot "run-continuity-campaign-gate.ps1")
cargo test -p palyra-common --test release_eval_contract --locked
cargo run -p palyra-cli --example run_release_eval_gate --locked -- `
    --manifest fixtures/golden/release_eval_inventory.json `
    --report-dir target/release-artifacts/release-evals

cargo build -p palyra-daemon --bin palyrad --locked
if (-not $env:PALYRA_QA_PALYRAD_BIN) {
    $env:PALYRA_QA_PALYRAD_BIN = Join-Path $rootDir "target\debug\palyrad.exe"
}
cargo run -p palyra-cli --locked -- qa gate `
    --suite qa/suites/runtime_kernel_v2_shadow.yaml `
    --output-json target/qa-lab/runtime-kernel-v2-shadow/report.json `
    --output-markdown target/qa-lab/runtime-kernel-v2-shadow/report.md `
    --json
cargo run -p palyra-cli --locked -- qa gate `
    --suite qa/suites/runtime_kernel_v2_authoritative.yaml `
    --output-json target/qa-lab/runtime-kernel-v2-authoritative/report.json `
    --output-markdown target/qa-lab/runtime-kernel-v2-authoritative/report.md `
    --json
