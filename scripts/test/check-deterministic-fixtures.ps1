[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$rootDir = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$fixturePath = "crates/palyra-connectors/tests/fixtures/channel_simulator_expected.json"
$absoluteFixturePath = Join-Path $rootDir $fixturePath

$beforeHash = $null
if (Test-Path $absoluteFixturePath) {
    $beforeHash = (Get-FileHash $absoluteFixturePath -Algorithm SHA256).Hash
}

& (Join-Path $PSScriptRoot "update-deterministic-fixtures.ps1")

$afterHash = $null
if (Test-Path $absoluteFixturePath) {
    $afterHash = (Get-FileHash $absoluteFixturePath -Algorithm SHA256).Hash
}

if ($beforeHash -ne $afterHash) {
    & git -C $rootDir rev-parse --is-inside-work-tree *> $null
    $isGitRepo = ($LASTEXITCODE -eq 0)
    if ($isGitRepo) {
        & git -C $rootDir diff -- $fixturePath
    }
    Write-Error "Deterministic fixtures changed during the check. Re-run scripts/test/update-deterministic-fixtures.ps1 and commit the updated fixture."
    exit 1
}

Write-Output "Deterministic fixtures are up to date."
