param()

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Push-Location $repoRoot
try {
    cargo test -p palyra-daemon --test current_state_inventory --locked
} finally {
    Pop-Location
}
