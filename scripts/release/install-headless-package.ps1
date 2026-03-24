param(
    [Parameter(Mandatory = $true)]
    [string]$ArchivePath,
    [Parameter(Mandatory = $true)]
    [string]$InstallRoot,
    [Parameter(Mandatory = $true)]
    [string]$ConfigPath,
    [Parameter(Mandatory = $true)]
    [string]$StateRoot,
    [string]$CliCommandRoot,
    [switch]$NoPersistCliPath,
    [switch]$Force,
    [switch]$SkipSystemdUnit
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. "$PSScriptRoot/common.ps1"

$archivePath = Assert-FileExists -Path $ArchivePath -Label "Headless archive"
& "$PSScriptRoot/validate-portable-archive.ps1" -Path $archivePath -ExpectedArtifactKind "headless" | Out-Null

if ((Test-Path -LiteralPath $InstallRoot) -and -not $Force) {
    throw "Install root already exists: $InstallRoot. Pass -Force to replace it."
}

$installRoot = New-CleanDirectory -Path $InstallRoot
Expand-ZipArchiveSafely -ArchivePath $archivePath -DestinationPath $installRoot

$resolvedConfigPath = [IO.Path]::GetFullPath($ConfigPath)
$resolvedStateRoot = [IO.Path]::GetFullPath($StateRoot)

$configParent = Split-Path -Parent $resolvedConfigPath
if ($configParent) {
    New-Item -ItemType Directory -Path $configParent -Force | Out-Null
}
New-Item -ItemType Directory -Path $resolvedStateRoot -Force | Out-Null

$cliBinary = Join-Path $installRoot (Resolve-ExecutableName -BaseName "palyra")
$daemonBinary = Join-Path $installRoot (Resolve-ExecutableName -BaseName "palyrad")
$browserBinary = Join-Path $installRoot (Resolve-ExecutableName -BaseName "palyra-browserd")
Set-ExecutablePermissions -Path $cliBinary
Set-ExecutablePermissions -Path $daemonBinary
Set-ExecutablePermissions -Path $browserBinary

$cliExposure = Install-PalyraCliExposure `
    -TargetBinaryPath $cliBinary `
    -CommandRoot $CliCommandRoot `
    -PersistPath:(-not $NoPersistCliPath)

$previousStateRoot = $env:PALYRA_STATE_ROOT
$previousConfigPath = $env:PALYRA_CONFIG
try {
    $env:PALYRA_STATE_ROOT = $resolvedStateRoot
    $env:PALYRA_CONFIG = $resolvedConfigPath

    Invoke-CommandQuiet -Command $cliBinary -Arguments @("version")
    Invoke-CommandQuiet -Command $daemonBinary -Arguments @("--help")
    Invoke-CommandQuiet -Command $browserBinary -Arguments @("--help")
    Invoke-CommandQuiet -Command $cliBinary -Arguments @("init", "--mode", "remote", "--path", $resolvedConfigPath, "--force")
    Invoke-CommandQuiet -Command $cliBinary -Arguments @("config", "validate", "--path", $resolvedConfigPath)
    Invoke-CommandQuiet -Command $cliExposure.command_path -Arguments @("version")
    Invoke-CommandQuiet -Command $cliExposure.command_path -Arguments @("--help")
    Invoke-CommandQuiet -Command "palyra" -Arguments @("version")
    Invoke-CommandQuiet -Command "palyra" -Arguments @("--help")
    Invoke-CommandQuiet -Command "palyra" -Arguments @("doctor", "--json")
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

if (-not $SkipSystemdUnit -and -not $IsWindows) {
    $serviceRoot = Join-Path $installRoot "service"
    New-Item -ItemType Directory -Path $serviceRoot -Force | Out-Null
    $servicePath = Join-Path $serviceRoot "palyrad.service"
    $serviceBody =
@"
[Unit]
Description=Palyra daemon
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$installRoot
Environment=PALYRA_CONFIG=$resolvedConfigPath
Environment=PALYRA_STATE_ROOT=$resolvedStateRoot
ExecStart=$daemonBinary
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
"@
    Set-Content -LiteralPath $servicePath -Value $serviceBody -NoNewline
}

$metadata = [ordered]@{
    schema_version = 2
    artifact_kind = "headless"
    installed_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    archive_path = $archivePath
    install_root = $installRoot
    config_path = $resolvedConfigPath
    state_root = $resolvedStateRoot
    cli_exposure = $cliExposure
}
$metadata | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $installRoot "install-metadata.json")

Write-Output "install_root=$installRoot"
Write-Output "config_path=$resolvedConfigPath"
Write-Output "state_root=$resolvedStateRoot"
Write-Output "cli_command_root=$($cliExposure.command_root)"
Write-Output "cli_command_path=$($cliExposure.command_path)"
Write-Output "cli_persistence_strategy=$($cliExposure.persistence_strategy)"
