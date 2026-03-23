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
4. Launch the desktop control center binary from that directory.

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
5. Start `palyrad` with `PALYRA_CONFIG=<install-root>/config/palyra.toml`.

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
- Release artifacts now include SHA256 manifests, release manifests, provenance sidecars, and package-boundary validation.
- Release packaging smoke validates archive layout plus post-install `palyra version`, `palyra --help`, and `palyra doctor --json`.
"@

$migrationNotesBody =
@"
Migration notes for Palyra $Version

- Desktop updates use archive replacement; keep the existing state root and replace only the install directory contents.
- Headless updates require `palyra config migrate --path <config>` after unpacking new binaries and before restarting `palyrad`.
- Installer-driven CLI shims are user-scoped and reversible; uninstall cleanup should remove the shim only when it still points at the install being removed.
- No state-root relocation is required for this release; support bundles and runtime databases stay outside the install directory.
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
