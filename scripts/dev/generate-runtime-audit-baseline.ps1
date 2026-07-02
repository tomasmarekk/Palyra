Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$previousUpdateGoldens = $env:PALYRA_UPDATE_GOLDENS

Push-Location $repoRoot
try {
    $env:PALYRA_UPDATE_GOLDENS = "1"
    cargo test -p palyra-daemon --test current_state_inventory --locked
}
finally {
    if ($null -eq $previousUpdateGoldens) {
        Remove-Item Env:PALYRA_UPDATE_GOLDENS -ErrorAction SilentlyContinue
    }
    else {
        $env:PALYRA_UPDATE_GOLDENS = $previousUpdateGoldens
    }
    Pop-Location
}
