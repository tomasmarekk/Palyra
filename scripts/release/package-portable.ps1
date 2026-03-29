param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("desktop", "headless")]
    [string]$ArtifactKind,
    [string]$Version,
    [string]$Platform,
    [string]$OutputRoot,
    [string]$DesktopBinaryPath,
    [Parameter(Mandatory = $true)]
    [string]$DaemonBinaryPath,
    [Parameter(Mandatory = $true)]
    [string]$BrowserBinaryPath,
    [Parameter(Mandatory = $true)]
    [string]$CliBinaryPath,
    [string]$WebDistPath
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. "$PSScriptRoot/common.ps1"

$repoRoot = Get-RepoRoot
$Version = if ([string]::IsNullOrWhiteSpace($Version)) { Get-WorkspaceVersion } else { $Version }
$Platform = if ([string]::IsNullOrWhiteSpace($Platform)) { Get-PlatformSlug } else { $Platform }
$OutputRoot = if ([string]::IsNullOrWhiteSpace($OutputRoot)) { Get-ReleaseOutputRoot } else { $OutputRoot }
$outputRoot = New-CleanDirectory -Path $OutputRoot
$artifactBaseName =
    if ($ArtifactKind -eq "desktop") {
        "palyra-desktop-$Version-$Platform"
    } else {
        "palyra-headless-$Version-$Platform"
    }

$stagingRoot = Join-Path $outputRoot $artifactBaseName
$payloadRoot = Join-Path $stagingRoot "payload"
New-Item -ItemType Directory -Path $payloadRoot -Force | Out-Null

$resolvedDesktopBinary = $null
if ($ArtifactKind -eq "desktop") {
    if ([string]::IsNullOrWhiteSpace($DesktopBinaryPath)) {
        throw "Desktop packaging requires -DesktopBinaryPath."
    }
    $resolvedDesktopBinary = Assert-FileExists -Path $DesktopBinaryPath -Label "Desktop binary"
}

$resolvedDaemonBinary = Assert-FileExists -Path $DaemonBinaryPath -Label "Daemon binary"
$resolvedBrowserBinary = Assert-FileExists -Path $BrowserBinaryPath -Label "Browser service binary"
$resolvedCliBinary = Assert-FileExists -Path $CliBinaryPath -Label "CLI binary"
$resolvedDocsRoot = Join-Path $repoRoot "docs"
$resolvedHelpSnapshotsRoot = Join-Path $repoRoot "crates/palyra-cli/tests/help_snapshots"
$null = Assert-FileExists -Path (Join-Path $resolvedDocsRoot "README.md") -Label "Operator docs index"
$null = Assert-FileExists -Path (Join-Path $resolvedHelpSnapshotsRoot "docs-help.txt") -Label "CLI help snapshot bundle"
$resolvedWebDistPath =
    if ([string]::IsNullOrWhiteSpace($WebDistPath)) {
        $null = Assert-FileExists -Path (Join-Path $repoRoot "apps/web/dist/index.html") -Label "Web dashboard bundle"
        Join-Path $repoRoot "apps/web/dist"
    } else {
        $null = Assert-FileExists -Path (Join-Path $WebDistPath "index.html") -Label "Web dashboard bundle"
        $WebDistPath
    }

$binaryEntries = [System.Collections.Generic.List[object]]::new()

function Copy-BinaryIntoPayload {
    param(
        [Parameter(Mandatory = $true)]
        [string]$SourcePath,
        [Parameter(Mandatory = $true)]
        [string]$LogicalName
    )

    $destinationName = Resolve-ExecutableName -BaseName $LogicalName
    $destinationPath = Join-Path $payloadRoot $destinationName
    Copy-Item -LiteralPath $SourcePath -Destination $destinationPath -Force
    $binaryEntries.Add([ordered]@{
            logical_name = $LogicalName
            file_name = $destinationName
            sha256 = Get-Sha256Hex -Path $destinationPath
            size_bytes = (Get-Item -LiteralPath $destinationPath).Length
        }) | Out-Null
}

if ($ArtifactKind -eq "desktop") {
    Copy-BinaryIntoPayload -SourcePath $resolvedDesktopBinary -LogicalName "palyra-desktop-control-center"
}
Copy-BinaryIntoPayload -SourcePath $resolvedDaemonBinary -LogicalName "palyrad"
Copy-BinaryIntoPayload -SourcePath $resolvedBrowserBinary -LogicalName "palyra-browserd"
Copy-BinaryIntoPayload -SourcePath $resolvedCliBinary -LogicalName "palyra"

Copy-Item -LiteralPath (Join-Path $repoRoot "LICENSE") -Destination (Join-Path $payloadRoot "LICENSE.txt") -Force
Copy-Item -LiteralPath $resolvedWebDistPath -Destination (Join-Path $payloadRoot "web") -Recurse -Force
Copy-Item -LiteralPath $resolvedDocsRoot -Destination (Join-Path $payloadRoot "docs") -Recurse -Force
Copy-Item -LiteralPath $resolvedHelpSnapshotsRoot -Destination (Join-Path $payloadRoot "docs/help_snapshots") -Recurse -Force

$installBody =
    if ($ArtifactKind -eq "desktop") {
@"
Palyra portable desktop bundle
Version: $Version
Platform: $Platform

Install
1. Extract this archive into a dedicated directory.
2. Keep `palyra-desktop-control-center`, `palyrad`, `palyra-browserd`, `palyra`, and the `web/` directory in the same directory.
3. Treat `palyra` as a first-class entry point: either run it directly from this directory or expose it on your shell `PATH` with a shim or symlink.
4. Review the installed operator surfaces with `palyra gateway --help`, `palyra browser --help`, `palyra docs --help`, `palyra update --help`, and `palyra uninstall --help`.
5. Use `palyra docs search migration` for bundled offline operator and migration guidance.
6. Launch the desktop control center binary from that directory.

Update
1. Close the running desktop control center and sidecars.
2. Extract the new archive into a fresh directory or replace the existing files in place.
3. Preserve the existing state root; the desktop app keeps runtime data outside the install directory.

Uninstall
1. Stop the desktop control center and sidecars.
2. Remove the extracted install directory.
3. If you also want to remove local state, delete `<state-root>/desktop-control-center` after exporting any needed support bundles.
"@
    } else {
@"
Palyra portable headless package
Version: $Version
Platform: $Platform

Install
1. Extract this archive into a dedicated directory.
2. Treat `palyra` as a first-class entry point: either run it directly from this directory or expose it on your shell `PATH` with a shim or symlink.
3. Run `palyra setup --mode remote --path <install-root>/config/palyra.toml --force`.
4. Validate the generated config with `palyra config validate --path <install-root>/config/palyra.toml`.
5. Review the installed operator surfaces with `palyra gateway --help`, `palyra browser --help`, `palyra node --help`, `palyra nodes --help`, `palyra docs --help`, `palyra update --help`, and `palyra uninstall --help`.
6. Use `palyra docs search migration` for bundled offline operator and migration guidance.
7. Start `palyrad` with `PALYRA_CONFIG=<install-root>/config/palyra.toml`.
8. Start `palyra-browserd` only when browser automation is intentionally enabled by config.

Update
1. Stop `palyrad`.
2. Replace the extracted binaries with the new archive contents.
3. Run `palyra config migrate --path <install-root>/config/palyra.toml`.
4. Restart `palyrad` and verify with `palyra doctor --json`.

Uninstall
1. Stop `palyrad`.
2. Remove the extracted install directory.
3. Remove the state root only after exporting any required support bundle with `palyra support-bundle export --output <path>`.
"@
    }

$rollbackBody =
@"
Rollback guidance
1. Stop the currently running Palyra processes.
2. Restore the previous extracted archive contents in place or switch the service/launcher back to the previous install directory.
3. Keep the state root unchanged.
4. If configuration migration was applied, run `palyra config migrate --path <config>` from the rollback target version before restart.
5. Verify health with `palyra doctor --json` and, for desktop installs, re-open the control center from the restored directory.
"@

$releaseNotesBody =
@"
Release notes for Palyra $Version

- Portable desktop bundles now ship the desktop control center, `palyrad`, `palyra-browserd`, `palyra`, and the colocated `web/` dashboard bundle, with installer support for exposing `palyra` as a user-scoped command.
- Portable headless packages now ship repeatable archive-based install/update flow with `palyra setup`, config initialization/migration validation, and installer support for exposing `palyra` as a user-scoped command.
- Portable packages now bundle offline operator docs plus CLI help snapshots so `palyra docs` remains usable outside a source checkout.
- Windows and macOS remain the supported v1 desktop runtime targets; the Linux desktop bundle continues as a release-regression/package artifact until the Tauri Linux dependency chain is unblocked.
- Release artifacts now include SHA256 manifests, release manifests, provenance sidecars, and package-boundary validation.
- Release packaging smoke validates canonical lifecycle surfaces (`setup`, `gateway`, `onboarding wizard`) plus compatibility aliases (`init`, `daemon`, `onboard`) on installed packages before publication.
- Installed packages expose release-ready browser, node, nodes, docs, update, and uninstall surfaces alongside the baseline diagnostics flows.
- Browser parity remains transparent in release packaging: `browser console`, `browser pdf`, `browser select`, and `browser highlight` stay discoverable placeholders until their implementations land.
"@

$migrationNotesBody =
@"
Migration notes for Palyra $Version

- Prefer `palyra setup` over `palyra init`; the alias remains supported for existing scripts, but new install and upgrade guidance uses `setup`.
- Prefer `palyra gateway` over `palyra daemon`; the alias remains supported, while release docs and examples use `gateway`.
- Guided onboarding remains under `palyra onboarding wizard`; `palyra onboard` stays as a shorthand compatibility alias for the onboarding family.
- Desktop updates use archive replacement; keep the existing state root and replace only the install directory contents.
- Headless updates require `palyra config migrate --path <config>` after unpacking new binaries and before restarting `palyrad`.
- Use `palyra update --archive <zip> --dry-run` and `palyra uninstall --dry-run` to preview portable lifecycle operations before mutating an install.
- Installer-driven CLI shims are user-scoped and reversible; uninstall cleanup should remove the shim only when it still points at the install being removed.
- No state-root relocation is required for this release; browser artifacts, support bundles, and runtime databases stay outside the install directory.
"@

Set-Content -LiteralPath (Join-Path $payloadRoot "README.txt") -Value $installBody -NoNewline
Set-Content -LiteralPath (Join-Path $payloadRoot "ROLLBACK.txt") -Value $rollbackBody -NoNewline
Set-Content -LiteralPath (Join-Path $payloadRoot "RELEASE_NOTES.txt") -Value $releaseNotesBody -NoNewline
Set-Content -LiteralPath (Join-Path $payloadRoot "MIGRATION_NOTES.txt") -Value $migrationNotesBody -NoNewline

$sourceSha = (& git -C $repoRoot rev-parse HEAD).Trim()
$manifest = [ordered]@{
    schema_version = 1
    generated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    artifact_kind = $ArtifactKind
    artifact_name = $artifactBaseName
    version = $Version
    platform = $Platform
    install_mode = "portable-archive"
    source_sha = $sourceSha
    binaries = @($binaryEntries)
    packaging_boundaries = [ordered]@{
        excluded_patterns = @(
            "*.sqlite",
            "*.sqlite3",
            "*.sqlite3-*",
            "*.db",
            "*.db-*",
            "*.wal",
            "*.shm",
            "*.log",
            "support-bundle*.json",
            "browser-profile/*",
            "browser-profiles/*",
            "downloads/*",
            "node_modules/*",
            "dist/*"
        )
    }
}

$manifestPath = Join-Path $payloadRoot "release-manifest.json"
$manifest | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $manifestPath

$checksumLines = [System.Collections.Generic.List[string]]::new()
Get-ChildItem -LiteralPath $payloadRoot -Recurse -File |
    Sort-Object FullName |
    ForEach-Object {
        $relativePath = Get-RelativePosixPath -BasePath $payloadRoot -TargetPath $_.FullName
        $checksumLines.Add("$(Get-Sha256Hex -Path $_.FullName)  $relativePath") | Out-Null
    }

$checksumsPath = Join-Path $payloadRoot "checksums.txt"
Set-Content -LiteralPath $checksumsPath -Value ($checksumLines -join [Environment]::NewLine) -NoNewline

$archivePath = Join-Path $outputRoot "$artifactBaseName.zip"
if (Test-Path -LiteralPath $archivePath) {
    Remove-Item -LiteralPath $archivePath -Force
}
Compress-Archive -Path (Join-Path $payloadRoot "*") -DestinationPath $archivePath -Force

$externalManifestPath = Join-Path $outputRoot "$artifactBaseName.release-manifest.json"
$externalChecksumsPath = Join-Path $outputRoot "$artifactBaseName.checksums.txt"
Copy-Item -LiteralPath $manifestPath -Destination $externalManifestPath -Force
Copy-Item -LiteralPath $checksumsPath -Destination $externalChecksumsPath -Force

$archiveHash = Get-Sha256Hex -Path $archivePath
$archiveChecksumPath = Join-Path $outputRoot "$artifactBaseName.zip.sha256"
Set-Content -LiteralPath $archiveChecksumPath -Value "$archiveHash  $(Split-Path -Leaf $archivePath)" -NoNewline

Write-Output "archive_path=$archivePath"
Write-Output "manifest_path=$externalManifestPath"
Write-Output "checksums_path=$externalChecksumsPath"
Write-Output "archive_checksum_path=$archiveChecksumPath"
