param(
    [Parameter(Mandatory = $true)]
    [string]$ArchivePath,
    [Parameter(Mandatory = $true)]
    [string]$InstallRoot,
    [Parameter(Mandatory = $true)]
    [string]$ConfigPath,
    [Parameter(Mandatory = $true)]
    [string]$StateRoot,
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
Expand-Archive -LiteralPath $archivePath -DestinationPath $installRoot -Force

$configParent = Split-Path -Parent $ConfigPath
if ($configParent) {
    New-Item -ItemType Directory -Path $configParent -Force | Out-Null
}
New-Item -ItemType Directory -Path $StateRoot -Force | Out-Null

$cliBinary = Join-Path $installRoot (Resolve-ExecutableName -BaseName "palyra")
$daemonBinary = Join-Path $installRoot (Resolve-ExecutableName -BaseName "palyrad")
$browserBinary = Join-Path $installRoot (Resolve-ExecutableName -BaseName "palyra-browserd")

$previousStateRoot = $env:PALYRA_STATE_ROOT
try {
    $env:PALYRA_STATE_ROOT = $StateRoot
    & $cliBinary version | Out-Null
    & $daemonBinary --help | Out-Null
    & $browserBinary --help | Out-Null
    & $cliBinary init --mode remote --path $ConfigPath --force | Out-Null
    & $cliBinary config validate --path $ConfigPath | Out-Null
}
finally {
    if ($null -eq $previousStateRoot) {
        Remove-Item Env:PALYRA_STATE_ROOT -ErrorAction SilentlyContinue
    } else {
        $env:PALYRA_STATE_ROOT = $previousStateRoot
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
Environment=PALYRA_CONFIG=$ConfigPath
Environment=PALYRA_STATE_ROOT=$StateRoot
ExecStart=$daemonBinary
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
"@
    Set-Content -LiteralPath $servicePath -Value $serviceBody -NoNewline
}

$metadata = [ordered]@{
    installed_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    archive_path = $archivePath
    install_root = $installRoot
    config_path = $ConfigPath
    state_root = $StateRoot
}
$metadata | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath (Join-Path $installRoot "install-metadata.json")

Write-Output "install_root=$installRoot"
Write-Output "config_path=$ConfigPath"
Write-Output "state_root=$StateRoot"
