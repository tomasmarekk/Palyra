param(
    [string]$WorkspaceRoot,
    [switch]$SkipBuild,
    [switch]$Launch
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

. (Join-Path $PSScriptRoot "../release/common.ps1")

function Get-DefaultHarnessRoot {
    $localAppData = [Environment]::GetFolderPath("LocalApplicationData")
    if ([string]::IsNullOrWhiteSpace($localAppData)) {
        throw "Unable to resolve LocalApplicationData for the clean desktop test harness."
    }

    return Join-Path $localAppData "Palyra-TestHarness"
}

$repoRoot = Get-RepoRoot
$workspaceRoot =
    if ([string]::IsNullOrWhiteSpace($WorkspaceRoot)) {
        Get-DefaultHarnessRoot
    } else {
        [IO.Path]::GetFullPath($WorkspaceRoot)
    }

$artifactsRoot = Join-Path $workspaceRoot "artifacts"
$desktopPackageOutput = Join-Path $artifactsRoot "desktop"
$cargoTargetRoot = Join-Path $artifactsRoot "cargo-target"
$installRoot = Join-Path $workspaceRoot "install"
$stateRoot = Join-Path $workspaceRoot "state"
$cliCommandRoot = Join-Path $workspaceRoot "cli-bin"
$desktopExecutable = Resolve-ExecutableName -BaseName "palyra-desktop-control-center"
$daemonExecutable = Resolve-ExecutableName -BaseName "palyrad"
$browserExecutable = Resolve-ExecutableName -BaseName "palyra-browserd"
$cliExecutable = Resolve-ExecutableName -BaseName "palyra"

New-Item -ItemType Directory -Path $workspaceRoot -Force | Out-Null

if (-not $SkipBuild) {
    Push-Location $repoRoot
    $previousCargoTargetDir = $env:CARGO_TARGET_DIR
    try {
        New-Item -ItemType Directory -Path $cargoTargetRoot -Force | Out-Null
        $env:CARGO_TARGET_DIR = $cargoTargetRoot
        & (Join-Path $repoRoot "scripts/test/ensure-desktop-ui.ps1")
        & (Join-Path $repoRoot "scripts/test/ensure-web-ui.ps1")
        cargo build -p palyra-daemon -p palyra-browserd -p palyra-cli --release --locked
        cargo build --manifest-path apps/desktop/src-tauri/Cargo.toml --release --locked
    }
    finally {
        if ($null -eq $previousCargoTargetDir) {
            Remove-Item Env:CARGO_TARGET_DIR -ErrorAction SilentlyContinue
        } else {
            $env:CARGO_TARGET_DIR = $previousCargoTargetDir
        }
        Pop-Location
    }
}

$version = & (Join-Path $repoRoot "scripts/release/assert-version-coherence.ps1")
$platform = Get-PlatformSlug
$isolatedDesktopBinary = Join-Path $cargoTargetRoot ("release/" + $desktopExecutable)
$isolatedDaemonBinary = Join-Path $cargoTargetRoot ("release/" + $daemonExecutable)
$isolatedBrowserBinary = Join-Path $cargoTargetRoot ("release/" + $browserExecutable)
$isolatedCliBinary = Join-Path $cargoTargetRoot ("release/" + $cliExecutable)
$desktopBinary =
    if (Test-Path -LiteralPath $isolatedDesktopBinary -PathType Leaf) {
        $isolatedDesktopBinary
    } else {
        Join-Path $repoRoot ("apps/desktop/src-tauri/target/release/" + $desktopExecutable)
    }
$daemonBinary =
    if (Test-Path -LiteralPath $isolatedDaemonBinary -PathType Leaf) {
        $isolatedDaemonBinary
    } else {
        Join-Path $repoRoot ("target/release/" + $daemonExecutable)
    }
$browserBinary =
    if (Test-Path -LiteralPath $isolatedBrowserBinary -PathType Leaf) {
        $isolatedBrowserBinary
    } else {
        Join-Path $repoRoot ("target/release/" + $browserExecutable)
    }
$cliBinary =
    if (Test-Path -LiteralPath $isolatedCliBinary -PathType Leaf) {
        $isolatedCliBinary
    } else {
        Join-Path $repoRoot ("target/release/" + $cliExecutable)
    }
$webDist = Join-Path $repoRoot "apps/web/dist"

$packageOutput = & (Join-Path $repoRoot "scripts/release/package-portable.ps1") `
    -ArtifactKind desktop `
    -Version $version `
    -OutputRoot $desktopPackageOutput `
    -DesktopBinaryPath $desktopBinary `
    -DaemonBinaryPath $daemonBinary `
    -BrowserBinaryPath $browserBinary `
    -CliBinaryPath $cliBinary `
    -WebDistPath $webDist
$packageMetadata = Convert-KeyValueOutputToHashtable -Lines $packageOutput
$archivePath = $packageMetadata["archive_path"]
if ([string]::IsNullOrWhiteSpace($archivePath)) {
    $archivePath = Join-Path $desktopPackageOutput "palyra-desktop-$version-$platform.zip"
}
$stagingRoot = Join-Path $desktopPackageOutput "palyra-desktop-$version-$platform"
if (Test-Path -LiteralPath $stagingRoot) {
    Remove-Item -LiteralPath $stagingRoot -Recurse -Force
}

if (Test-Path -LiteralPath $stateRoot) {
    Remove-Item -LiteralPath $stateRoot -Recurse -Force
}
New-Item -ItemType Directory -Path $stateRoot -Force | Out-Null
$configPath = Ensure-PortableConfigFile -ConfigPath (Resolve-PortableConfigPath -StateRoot $stateRoot)

$installOutput = & (Join-Path $repoRoot "scripts/release/install-desktop-package.ps1") `
    -ArchivePath $archivePath `
    -InstallRoot $installRoot `
    -StateRoot $stateRoot `
    -CliCommandRoot $cliCommandRoot `
    -NoPersistCliPath `
    -Force
$installMetadata = Convert-KeyValueOutputToHashtable -Lines $installOutput
$resolvedInstallRoot = $installMetadata["install_root"]
if ([string]::IsNullOrWhiteSpace($resolvedInstallRoot)) {
    $resolvedInstallRoot = $installRoot
}
$resolvedCliCommandRoot = $installMetadata["cli_command_root"]
if ([string]::IsNullOrWhiteSpace($resolvedCliCommandRoot)) {
    $resolvedCliCommandRoot = $cliCommandRoot
}

$launcherPath = Join-Path $resolvedInstallRoot "Launch-Palyra-Test.ps1"

$launcherBody =
@"
param(
    [switch]`$Wait
)

Set-StrictMode -Version Latest
`$ErrorActionPreference = "Stop"

`$installRoot = Split-Path -Parent `$MyInvocation.MyCommand.Path
`$stateRoot = "$stateRoot"
`$configPath = "$configPath"
New-Item -ItemType Directory -Path `$stateRoot -Force | Out-Null

`$env:PALYRA_STATE_ROOT = `$stateRoot
`$env:PALYRA_CONFIG = `$configPath
`$env:PALYRA_DESKTOP_PALYRAD_BIN = Join-Path `$installRoot "$daemonExecutable"
`$env:PALYRA_DESKTOP_BROWSERD_BIN = Join-Path `$installRoot "$browserExecutable"
`$env:PALYRA_DESKTOP_PALYRA_BIN = Join-Path `$installRoot "$cliExecutable"

`$desktopBinary = Join-Path `$installRoot "$desktopExecutable"
if (`$Wait) {
    & `$desktopBinary
} else {
    `$process = Start-Process -FilePath `$desktopBinary -WorkingDirectory `$installRoot -PassThru
    if (`$process.WaitForExit(2000)) {
        if (`$process.ExitCode -eq 0) {
            Write-Host "Palyra desktop exited cleanly before the launcher timeout. If no new window appeared, another instance may already be running."
            return
        }
        throw "Palyra desktop exited immediately with code `$(`$process.ExitCode). Re-run Launch-Palyra-Test.ps1 with -Wait to surface the startup error directly."
    }
}
"@

Set-Content -LiteralPath $launcherPath -Value $launcherBody -NoNewline

$installSummary = [ordered]@{
    installed_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    repo_root = $repoRoot
    workspace_root = $workspaceRoot
    artifacts_root = $artifactsRoot
    archive_path = $archivePath
    install_root = $resolvedInstallRoot
    config_path = $configPath
    state_root = $stateRoot
    cli_command_root = $resolvedCliCommandRoot
    launcher_path = $launcherPath
}
$installSummary |
    ConvertTo-Json -Depth 4 |
    Set-Content -LiteralPath (Join-Path $workspaceRoot "clean-install-metadata.json")

if ($Launch) {
    & $launcherPath
}

Write-Output "workspace_root=$workspaceRoot"
Write-Output "archive_path=$archivePath"
Write-Output "install_root=$resolvedInstallRoot"
Write-Output "config_path=$configPath"
Write-Output "state_root=$stateRoot"
Write-Output "cli_command_root=$resolvedCliCommandRoot"
Write-Output "launcher_path=$launcherPath"
