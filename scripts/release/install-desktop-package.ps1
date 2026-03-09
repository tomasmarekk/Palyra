param(
    [Parameter(Mandatory = $true)]
    [string]$ArchivePath,
    [Parameter(Mandatory = $true)]
    [string]$InstallRoot,
    [switch]$Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. "$PSScriptRoot/common.ps1"

$archivePath = Assert-FileExists -Path $ArchivePath -Label "Desktop archive"
& "$PSScriptRoot/validate-portable-archive.ps1" -Path $archivePath -ExpectedArtifactKind "desktop" | Out-Null

if ((Test-Path -LiteralPath $InstallRoot) -and -not $Force) {
    throw "Install root already exists: $InstallRoot. Pass -Force to replace it."
}

$installRoot = New-CleanDirectory -Path $InstallRoot
Expand-ZipArchiveSafely -ArchivePath $archivePath -DestinationPath $installRoot

$cliBinary = Join-Path $installRoot (Resolve-ExecutableName -BaseName "palyra")
$daemonBinary = Join-Path $installRoot (Resolve-ExecutableName -BaseName "palyrad")
$browserBinary = Join-Path $installRoot (Resolve-ExecutableName -BaseName "palyra-browserd")

Invoke-ExecutableQuiet -ExecutablePath $cliBinary -Arguments @("version")
Invoke-ExecutableQuiet -ExecutablePath $daemonBinary -Arguments @("--help")
Invoke-ExecutableQuiet -ExecutablePath $browserBinary -Arguments @("--help")

$metadata = [ordered]@{
    installed_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    archive_path = $archivePath
    install_root = $installRoot
}
$metadata | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath (Join-Path $installRoot "install-metadata.json")

Write-Output "install_root=$installRoot"
