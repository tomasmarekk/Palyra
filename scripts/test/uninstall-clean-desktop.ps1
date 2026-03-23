param(
    [string]$WorkspaceRoot,
    [switch]$KeepArtifacts
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "../release/common.ps1")

function Get-DefaultHarnessRoot {
    $localAppData = [Environment]::GetFolderPath("LocalApplicationData")
    if ([string]::IsNullOrWhiteSpace($localAppData)) {
        throw "Unable to resolve LocalApplicationData for the clean desktop test harness."
    }

    return Join-Path $localAppData "Palyra-TestHarness"
}

function Stop-InstalledProcess {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ExecutablePath
    )

    $expectedPath = [IO.Path]::GetFullPath($ExecutablePath)
    $comparison = [StringComparison]::OrdinalIgnoreCase
    Get-Process -ErrorAction SilentlyContinue |
        Where-Object {
            try {
                -not [string]::IsNullOrWhiteSpace($_.Path) -and
                    [string]::Equals([IO.Path]::GetFullPath($_.Path), $expectedPath, $comparison)
            } catch {
                $false
            }
        } |
        ForEach-Object {
            Stop-Process -Id $_.Id -Force -ErrorAction Stop
        }
}

$workspaceRoot =
    if ([string]::IsNullOrWhiteSpace($WorkspaceRoot)) {
        Get-DefaultHarnessRoot
    } else {
        [IO.Path]::GetFullPath($WorkspaceRoot)
    }

$artifactsRoot = Join-Path $workspaceRoot "artifacts"
$installRoot = Join-Path $workspaceRoot "install"
$stateRoot = Join-Path $workspaceRoot "state"
$cliCommandRoot = Join-Path $workspaceRoot "cli-bin"

$desktopBinary =
    Join-Path $installRoot (Resolve-ExecutableName -BaseName "palyra-desktop-control-center")
$daemonBinary = Join-Path $installRoot (Resolve-ExecutableName -BaseName "palyrad")
$browserBinary = Join-Path $installRoot (Resolve-ExecutableName -BaseName "palyra-browserd")

foreach ($binaryPath in @($desktopBinary, $daemonBinary, $browserBinary)) {
    Stop-InstalledProcess -ExecutablePath $binaryPath
}

if (Test-Path -LiteralPath $installRoot) {
    & (Join-Path $PSScriptRoot "../release/uninstall-package.ps1") `
        -InstallRoot $installRoot `
        -RemoveStateRoot | Out-Null
}

if ((Test-Path -LiteralPath $artifactsRoot) -and -not $KeepArtifacts) {
    Remove-Item -LiteralPath $artifactsRoot -Recurse -Force
}

$metadataPath = Join-Path $workspaceRoot "clean-install-metadata.json"
if ((Test-Path -LiteralPath $metadataPath) -and -not $KeepArtifacts) {
    Remove-Item -LiteralPath $metadataPath -Force
}

if ((Test-Path -LiteralPath $workspaceRoot) -and -not (Get-ChildItem -LiteralPath $workspaceRoot -Force | Select-Object -First 1)) {
    Remove-Item -LiteralPath $workspaceRoot -Force
}

Write-Output "workspace_root=$workspaceRoot"
Write-Output "install_root=$installRoot"
Write-Output "state_root=$stateRoot"
Write-Output "cli_command_root=$cliCommandRoot"
Write-Output "artifacts_removed=$($KeepArtifacts -eq $false)"
