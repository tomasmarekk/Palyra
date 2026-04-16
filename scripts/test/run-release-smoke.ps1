Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
. (Join-Path $repoRoot "scripts/release/common.ps1")

function Assert-CommandResolvesFromRoot {
    param(
        [Parameter(Mandatory = $true)]
        [string]$CommandName,
        [Parameter(Mandatory = $true)]
        [string]$ExpectedRoot
    )

    $commandInfo = Get-Command $CommandName -ErrorAction Stop
    $resolvedPath =
        if ($null -ne $commandInfo.Source -and -not [string]::IsNullOrWhiteSpace([string]$commandInfo.Source)) {
            [string]$commandInfo.Source
        } elseif ($null -ne $commandInfo.Path -and -not [string]::IsNullOrWhiteSpace([string]$commandInfo.Path)) {
            [string]$commandInfo.Path
        } else {
            throw "Unable to resolve source path for command '$CommandName'."
        }

    $comparison =
        if ($IsWindows) {
            [StringComparison]::OrdinalIgnoreCase
        } else {
            [StringComparison]::Ordinal
        }

    $normalizedExpectedRoot = Normalize-PathEntry -PathEntry $ExpectedRoot
    $normalizedResolvedPath = Normalize-PathEntry -PathEntry $resolvedPath
    if (-not $normalizedResolvedPath.StartsWith($normalizedExpectedRoot, $comparison)) {
        throw "Expected '$CommandName' to resolve from $normalizedExpectedRoot, got $normalizedResolvedPath."
    }
}

function Invoke-InstalledCliSmoke {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ExpectedCommandRoot,
        [Parameter(Mandatory = $true)]
        [string]$StateRoot,
        [string]$ConfigPath,
        [Parameter(Mandatory = $true)]
        [string]$InstallRoot,
        [Parameter(Mandatory = $true)]
        [string]$ArchivePath
    )

    Assert-CommandResolvesFromRoot -CommandName "palyra" -ExpectedRoot $ExpectedCommandRoot

    $helpCommands = @(
        @("setup", "--help"),
        @("init", "--help"),
        @("onboarding", "--help"),
        @("onboarding", "wizard", "--help"),
        @("onboard", "wizard", "--help"),
        @("gateway", "--help"),
        @("daemon", "--help"),
        @("dashboard", "--help"),
        @("channels", "--help"),
        @("browser", "--help"),
        @("node", "--help"),
        @("nodes", "--help"),
        @("docs", "--help"),
        @("update", "--help"),
        @("uninstall", "--help"),
        @("support-bundle", "--help")
    )

    $previousStateRoot = $env:PALYRA_STATE_ROOT
    $previousConfigPath = $env:PALYRA_CONFIG
    try {
        $env:PALYRA_STATE_ROOT = $StateRoot
        if ([string]::IsNullOrWhiteSpace($ConfigPath)) {
            Remove-Item Env:PALYRA_CONFIG -ErrorAction SilentlyContinue
        } else {
            $env:PALYRA_CONFIG = $ConfigPath
        }

        Invoke-CommandQuiet -Command "palyra" -Arguments @("version")
        Invoke-CommandQuiet -Command "palyra" -Arguments @("--help")
        Invoke-CommandQuiet -Command "palyra" -Arguments @("doctor", "--json")
        Invoke-CommandQuiet -Command "palyra" -Arguments @("docs", "search", "gateway")
        Invoke-CommandQuiet -Command "palyra" -Arguments @("docs", "search", "browser")
        Invoke-CommandQuiet -Command "palyra" -Arguments @("docs", "show", "help/docs-help")
        foreach ($command in $helpCommands) {
            Invoke-CommandQuiet -Command "palyra" -Arguments $command
        }
        Invoke-CommandQuiet -Command "palyra" -Arguments @(
            "update",
            "--install-root",
            $InstallRoot,
            "--archive",
            $ArchivePath,
            "--dry-run"
        )
        Invoke-CommandQuiet -Command "palyra" -Arguments @(
            "uninstall",
            "--install-root",
            $InstallRoot,
            "--dry-run"
        )
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
}

$version = & (Join-Path $repoRoot "scripts/release/assert-version-coherence.ps1")
$outputRoot = Join-Path $repoRoot "target/release-artifacts/smoke"
if (Test-Path -LiteralPath $outputRoot) {
    Remove-Item -LiteralPath $outputRoot -Recurse -Force
}
New-Item -ItemType Directory -Path $outputRoot -Force | Out-Null

Push-Location $repoRoot
try {
    & (Join-Path $repoRoot "scripts/test/ensure-desktop-ui.ps1")
    & (Join-Path $repoRoot "scripts/test/ensure-web-ui.ps1")
    cargo build -p palyra-daemon -p palyra-browserd -p palyra-cli --release --locked
    cargo build --manifest-path apps/desktop/src-tauri/Cargo.toml --release --locked
}
finally {
    Pop-Location
}

$desktopBinary = Join-Path $repoRoot ("apps/desktop/src-tauri/target/release/" + (Resolve-ExecutableName -BaseName "palyra-desktop-control-center"))
$daemonBinary = Join-Path $repoRoot ("target/release/" + (Resolve-ExecutableName -BaseName "palyrad"))
$browserBinary = Join-Path $repoRoot ("target/release/" + (Resolve-ExecutableName -BaseName "palyra-browserd"))
$cliBinary = Join-Path $repoRoot ("target/release/" + (Resolve-ExecutableName -BaseName "palyra"))
$webDist = Join-Path $repoRoot "apps/web/dist"

$desktopPackageOutput = Join-Path $outputRoot "desktop"
$headlessPackageOutput = Join-Path $outputRoot "headless"

& (Join-Path $repoRoot "scripts/release/package-portable.ps1") `
    -ArtifactKind desktop `
    -Version $version `
    -OutputRoot $desktopPackageOutput `
    -DesktopBinaryPath $desktopBinary `
    -DaemonBinaryPath $daemonBinary `
    -BrowserBinaryPath $browserBinary `
    -CliBinaryPath $cliBinary `
    -WebDistPath $webDist | Out-Null

& (Join-Path $repoRoot "scripts/release/package-portable.ps1") `
    -ArtifactKind headless `
    -Version $version `
    -OutputRoot $headlessPackageOutput `
    -DaemonBinaryPath $daemonBinary `
    -BrowserBinaryPath $browserBinary `
    -CliBinaryPath $cliBinary `
    -WebDistPath $webDist | Out-Null

$platform = Get-PlatformSlug
$desktopArchive = Join-Path $desktopPackageOutput "palyra-desktop-$version-$platform.zip"
$headlessArchive = Join-Path $headlessPackageOutput "palyra-headless-$version-$platform.zip"

& (Join-Path $repoRoot "scripts/release/validate-portable-archive.ps1") -Path $desktopArchive -ExpectedArtifactKind desktop | Out-Null
& (Join-Path $repoRoot "scripts/release/validate-portable-archive.ps1") -Path $headlessArchive -ExpectedArtifactKind headless | Out-Null

$desktopInstallRoot = Join-Path $outputRoot "installed-desktop"
$desktopStateRoot = Join-Path $outputRoot "installed-desktop-state"
$headlessInstallRoot = Join-Path $outputRoot "installed-headless"
$headlessConfigPath = Join-Path $outputRoot "installed-headless-config/palyra.toml"
$headlessStateRoot = Join-Path $outputRoot "installed-headless-state"
$sharedCliCommandRoot = Join-Path $outputRoot "cli-bin"

$desktopInstallOutput = & (Join-Path $repoRoot "scripts/release/install-desktop-package.ps1") `
    -ArchivePath $desktopArchive `
    -InstallRoot $desktopInstallRoot `
    -StateRoot $desktopStateRoot `
    -CliCommandRoot $sharedCliCommandRoot `
    -NoPersistCliPath `
    -Force
$desktopInstallMetadata = Convert-KeyValueOutputToHashtable -Lines $desktopInstallOutput
$resolvedDesktopCommandRoot = $desktopInstallMetadata["cli_command_root"]
Invoke-InstalledCliSmoke `
    -ExpectedCommandRoot $resolvedDesktopCommandRoot `
    -StateRoot $desktopStateRoot `
    -InstallRoot $desktopInstallRoot `
    -ArchivePath $desktopArchive

$headlessInstallOutput = & (Join-Path $repoRoot "scripts/release/install-headless-package.ps1") `
    -ArchivePath $headlessArchive `
    -InstallRoot $headlessInstallRoot `
    -ConfigPath $headlessConfigPath `
    -StateRoot $headlessStateRoot `
    -CliCommandRoot $sharedCliCommandRoot `
    -NoPersistCliPath `
    -Force `
    -SkipSystemdUnit:$IsWindows
$headlessInstallMetadata = Convert-KeyValueOutputToHashtable -Lines $headlessInstallOutput
$resolvedHeadlessCommandRoot = $headlessInstallMetadata["cli_command_root"]
Invoke-InstalledCliSmoke `
    -ExpectedCommandRoot $resolvedHeadlessCommandRoot `
    -StateRoot $headlessStateRoot `
    -ConfigPath $headlessConfigPath `
    -InstallRoot $headlessInstallRoot `
    -ArchivePath $headlessArchive

$provenancePath = Join-Path $outputRoot "release-provenance.json"
& (Join-Path $repoRoot "scripts/release/generate-release-provenance.ps1") `
    -Version $version `
    -ArtifactPaths @($desktopArchive, $headlessArchive) `
    -OutputPath $provenancePath | Out-Null

& (Join-Path $repoRoot "scripts/release/uninstall-package.ps1") `
    -InstallRoot $desktopInstallRoot `
    -RemoveStateRoot | Out-Null

Assert-CommandResolvesFromRoot -CommandName "palyra" -ExpectedRoot $resolvedHeadlessCommandRoot
Invoke-InstalledCliSmoke `
    -ExpectedCommandRoot $resolvedHeadlessCommandRoot `
    -StateRoot $headlessStateRoot `
    -ConfigPath $headlessConfigPath `
    -InstallRoot $headlessInstallRoot `
    -ArchivePath $headlessArchive

& (Join-Path $repoRoot "scripts/release/uninstall-package.ps1") `
    -InstallRoot $headlessInstallRoot `
    -RemoveStateRoot | Out-Null

if (Test-Path -LiteralPath $sharedCliCommandRoot -PathType Container) {
    if (-not (Test-DirectoryEmpty -Path $sharedCliCommandRoot)) {
        throw "CLI command root should be empty after uninstall cleanup: $sharedCliCommandRoot"
    }
}

Write-Output "release_smoke=passed"
Write-Output "version=$version"
Write-Output "desktop_archive=$desktopArchive"
Write-Output "headless_archive=$headlessArchive"
Write-Output "provenance_path=$provenancePath"
