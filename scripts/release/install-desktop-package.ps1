param(
    [Parameter(Mandatory = $true)]
    [string]$ArchivePath,
    [Parameter(Mandatory = $true)]
    [string]$InstallRoot,
    [string]$StateRoot,
    [string]$CliCommandRoot,
    [switch]$NoPersistCliPath,
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

$cliExposure = Install-PalyraCliExposure `
    -TargetBinaryPath $cliBinary `
    -CommandRoot $CliCommandRoot `
    -PersistPath:(-not $NoPersistCliPath)

$resolvedStateRoot = $null
if (-not [string]::IsNullOrWhiteSpace($StateRoot)) {
    $resolvedStateRoot = [IO.Path]::GetFullPath($StateRoot)
    New-Item -ItemType Directory -Path $resolvedStateRoot -Force | Out-Null
}

$previousStateRoot = $env:PALYRA_STATE_ROOT
$previousConfigPath = $env:PALYRA_CONFIG
try {
    if ($null -ne $resolvedStateRoot) {
        $env:PALYRA_STATE_ROOT = $resolvedStateRoot
    }

    Remove-Item Env:PALYRA_CONFIG -ErrorAction SilentlyContinue
    Invoke-CommandQuiet -Command $cliBinary -Arguments @("version")
    Invoke-CommandQuiet -Command $daemonBinary -Arguments @("--help")
    Invoke-CommandQuiet -Command $browserBinary -Arguments @("--help")
    Invoke-CommandQuiet -Command $cliExposure.command_path -Arguments @("version")
    Invoke-CommandQuiet -Command $cliExposure.command_path -Arguments @("--help")
    Invoke-CommandQuiet -Command "palyra" -Arguments @("version")
    Invoke-CommandQuiet -Command "palyra" -Arguments @("--help")
    if ($null -ne $resolvedStateRoot) {
        Invoke-CommandQuiet -Command "palyra" -Arguments @("doctor", "--json")
    }
}
finally {
    if ($null -eq $previousStateRoot) {
        Remove-Item Env:PALYRA_STATE_ROOT -ErrorAction SilentlyContinue
    } else {
        $env:PALYRA_STATE_ROOT = $previousStateRoot
    }

    if ($null -eq $previousConfigPath) {
        Remove-Item Env:PALYRA_CONFIG -ErrorAction SilentlyContinue
    } else {
        $env:PALYRA_CONFIG = $previousConfigPath
    }
}

$metadata = [ordered]@{
    schema_version = 2
    artifact_kind = "desktop"
    installed_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    archive_path = $archivePath
    install_root = $installRoot
    state_root = $resolvedStateRoot
    cli_exposure = $cliExposure
}
$metadata | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $installRoot "install-metadata.json")

Write-Output "install_root=$installRoot"
if ($null -ne $resolvedStateRoot) {
    Write-Output "state_root=$resolvedStateRoot"
}
Write-Output "cli_command_root=$($cliExposure.command_root)"
Write-Output "cli_command_path=$($cliExposure.command_path)"
Write-Output "cli_persistence_strategy=$($cliExposure.persistence_strategy)"
