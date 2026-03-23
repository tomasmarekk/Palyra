param(
    [Parameter(Mandatory = $true)]
    [string]$InstallRoot,
    [switch]$RemoveStateRoot
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. "$PSScriptRoot/common.ps1"

$resolvedInstallRoot = [IO.Path]::GetFullPath($InstallRoot)
$metadataPath = Join-Path $resolvedInstallRoot "install-metadata.json"

$stateRoot = $null
$cliCleanup = $null
if (Test-Path -LiteralPath $metadataPath -PathType Leaf) {
    $metadata = Read-JsonFile -Path $metadataPath
    if ($null -ne $metadata.state_root -and -not [string]::IsNullOrWhiteSpace([string]$metadata.state_root)) {
        $stateRoot = [string]$metadata.state_root
    }

    if ($null -ne $metadata.cli_exposure) {
        $cliCleanup = Remove-PalyraCliExposure -CliExposure $metadata.cli_exposure
    }
}

if (Test-Path -LiteralPath $resolvedInstallRoot) {
    Remove-Item -LiteralPath $resolvedInstallRoot -Recurse -Force
}

$stateRootRemoved = $false
if ($RemoveStateRoot -and -not [string]::IsNullOrWhiteSpace($stateRoot) -and (Test-Path -LiteralPath $stateRoot)) {
    Remove-Item -LiteralPath $stateRoot -Recurse -Force
    $stateRootRemoved = $true
}

Write-Output "install_root=$resolvedInstallRoot"
Write-Output "install_root_removed=$true"
Write-Output "state_root=$stateRoot"
Write-Output "state_root_removed=$stateRootRemoved"
if ($null -ne $cliCleanup) {
    Write-Output "cli_command_root_removed=$($cliCleanup.command_root_removed)"
}
