[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

$rootDir = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$generatorPath = Join-Path $PSScriptRoot "generate-stubs.ps1"

& $generatorPath

& git -C $rootDir rev-parse --is-inside-work-tree *> $null
$isGitRepo = ($LASTEXITCODE -eq 0)

if ($isGitRepo) {
    & git -C $rootDir diff --quiet -- schemas/generated
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Generated stubs are out of date. Re-run scripts/protocol/generate-stubs.ps1 and commit changes."
        & git -C $rootDir diff -- schemas/generated
        exit 1
    }
}

Write-Output "Generated protocol stubs are up to date."
