Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
. (Join-Path $repoRoot "scripts/release/common.ps1")

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
$headlessInstallRoot = Join-Path $outputRoot "installed-headless"
$headlessConfigPath = Join-Path $outputRoot "installed-headless-config/palyra.toml"
$headlessStateRoot = Join-Path $outputRoot "installed-headless-state"

& (Join-Path $repoRoot "scripts/release/install-desktop-package.ps1") `
    -ArchivePath $desktopArchive `
    -InstallRoot $desktopInstallRoot `
    -Force | Out-Null

& (Join-Path $repoRoot "scripts/release/install-headless-package.ps1") `
    -ArchivePath $headlessArchive `
    -InstallRoot $headlessInstallRoot `
    -ConfigPath $headlessConfigPath `
    -StateRoot $headlessStateRoot `
    -Force `
    -SkipSystemdUnit:$IsWindows | Out-Null

$provenancePath = Join-Path $outputRoot "release-provenance.json"
& (Join-Path $repoRoot "scripts/release/generate-release-provenance.ps1") `
    -Version $version `
    -ArtifactPaths @($desktopArchive, $headlessArchive) `
    -OutputPath $provenancePath | Out-Null

Write-Output "release_smoke=passed"
Write-Output "version=$version"
Write-Output "desktop_archive=$desktopArchive"
Write-Output "headless_archive=$headlessArchive"
Write-Output "provenance_path=$provenancePath"
