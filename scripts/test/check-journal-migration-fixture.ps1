[CmdletBinding()]
param()

# Validates the immutable pre-V2 journal snapshot before exercising its upgrade path.
# The manifest check prevents a silently rewritten baseline from qualifying a migration.
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$rootDir = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$fixtureDir = Join-Path $rootDir "fixtures/golden/journal_migrations"
$fixtureName = "pre_v2_v44.sql"
$fixturePath = Join-Path $fixtureDir $fixtureName
$manifestPath = Join-Path $fixtureDir "pre_v2_v44.sha256"

foreach ($path in @($fixturePath, $manifestPath)) {
    $item = Get-Item -LiteralPath $path -Force
    if ($item.PSIsContainer -or $item.LinkType) {
        throw "journal migration fixture inputs must be regular non-symlink files: $path"
    }
}

$manifestLine = (Get-Content -Raw -LiteralPath $manifestPath).Trim()
if ($manifestLine -notmatch "^(?<hash>[0-9a-f]{64})  (?<file>[^ ]+)$") {
    throw "journal migration fixture manifest has an invalid sha256sum shape"
}
if ($Matches.file -ne $fixtureName) {
    throw "journal migration fixture manifest names an unexpected file: $($Matches.file)"
}

$expectedSha256 = $Matches.hash
$actualSha256 = (Get-FileHash -LiteralPath $fixturePath -Algorithm SHA256).Hash.ToLowerInvariant()
if ($actualSha256 -ne $expectedSha256) {
    throw @"
journal migration fixture hash mismatch
Expected: $expectedSha256
Actual:   $actualSha256
"@
}

Set-Location $rootDir
cargo test `
    -p palyra-daemon `
    journal::tests::pre_v2_golden_migration_acceptance `
    --locked `
    -- `
    --exact

Write-Output "journal migration fixture validation passed."
