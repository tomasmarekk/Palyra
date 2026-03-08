Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. "$PSScriptRoot/common.ps1"

$repoRoot = Get-RepoRoot

function Get-TomlPackageVersion {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    $content = Get-Content -Raw -LiteralPath $Path
    $match = [regex]::Match($content, '(?ms)^\[package\].*?^version\s*=\s*"(?<version>[^"]+)"')
    if (-not $match.Success) {
        throw "Unable to locate [package] version in $Path"
    }
    return $match.Groups["version"].Value
}

$versions = [ordered]@{
    workspace = Get-WorkspaceVersion
    desktop_crate = Get-TomlPackageVersion -Path (Join-Path $repoRoot "apps/desktop/src-tauri/Cargo.toml")
    desktop_tauri = (Get-Content -Raw -LiteralPath (Join-Path $repoRoot "apps/desktop/src-tauri/tauri.conf.json") | ConvertFrom-Json).version
    web_package = (Get-Content -Raw -LiteralPath (Join-Path $repoRoot "apps/web/package.json") | ConvertFrom-Json).version
}

$distinctVersions = @($versions.Values | Sort-Object -Unique)
if ($distinctVersions.Count -ne 1) {
    $pairs = $versions.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" }
    throw "Version mismatch across release surfaces: $($pairs -join ', ')"
}

Write-Output $distinctVersions[0]
