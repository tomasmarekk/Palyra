[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$rootDir = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $rootDir

cargo test -p palyra-common --test release_eval_contract --locked
cargo run -p palyra-cli --example run_release_eval_gate --locked -- `
    --manifest fixtures/golden/release_eval_inventory.json `
    --report-dir target/release-artifacts/release-evals
