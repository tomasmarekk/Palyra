[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$rootDir = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $rootDir

$env:PALYRA_REFRESH_DETERMINISTIC_FIXTURES = "1"
try {
    cargo test -p palyra-connectors --test simulator_harness --locked
} finally {
    Remove-Item Env:PALYRA_REFRESH_DETERMINISTIC_FIXTURES -ErrorAction SilentlyContinue
}
