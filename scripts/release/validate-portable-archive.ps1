param(
    [Parameter(Mandatory = $true)]
    [string]$Path,
    [ValidateSet("desktop", "headless")]
    [string]$ExpectedArtifactKind
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. "$PSScriptRoot/common.ps1"

$resolvedPath =
    if (Test-Path -LiteralPath $Path) {
        (Resolve-Path -LiteralPath $Path).Path
    } else {
        throw "Archive or directory does not exist: $Path"
    }

$temporaryExtractionRoot = $null
$payloadRoot = $resolvedPath

if ((Get-Item -LiteralPath $resolvedPath).PSIsContainer -eq $false) {
    $temporaryExtractionRoot = Expand-ZipToTemporaryDirectory -ArchivePath $resolvedPath
    $payloadRoot = $temporaryExtractionRoot
}

try {
    $manifestPath = Join-Path $payloadRoot "release-manifest.json"
    $checksumsPath = Join-Path $payloadRoot "checksums.txt"
    $manifestPath = Assert-FileExists -Path $manifestPath -Label "Release manifest"
    $checksumsPath = Assert-FileExists -Path $checksumsPath -Label "Checksums manifest"
    $manifest = Read-JsonFile -Path $manifestPath

    if ($ExpectedArtifactKind -and $manifest.artifact_kind -ne $ExpectedArtifactKind) {
        throw "Expected artifact kind '$ExpectedArtifactKind' but manifest reports '$($manifest.artifact_kind)'."
    }

    $requiredCommonFiles = @("README.txt", "ROLLBACK.txt", "RELEASE_NOTES.txt", "MIGRATION_NOTES.txt", "LICENSE.txt")
    foreach ($requiredFile in $requiredCommonFiles) {
        Assert-FileExists -Path (Join-Path $payloadRoot $requiredFile) -Label $requiredFile | Out-Null
    }
    Assert-FileExists -Path (Join-Path $payloadRoot "web/index.html") -Label "web/index.html" | Out-Null
    Assert-FileExists -Path (Join-Path $payloadRoot "docs/help_snapshots/docs-help.txt") -Label "docs/help_snapshots/docs-help.txt" | Out-Null

    $requiredBinaries =
        if ($manifest.artifact_kind -eq "desktop") {
            @("palyra-desktop-control-center", "palyrad", "palyra-browserd", "palyra")
        } else {
            @("palyrad", "palyra-browserd", "palyra")
        }

    foreach ($logicalName in $requiredBinaries) {
        $binaryName = Resolve-ExecutableName -BaseName $logicalName
        Assert-FileExists -Path (Join-Path $payloadRoot $binaryName) -Label $binaryName | Out-Null
    }

    $forbiddenNamePatterns = @(
        "*.sqlite",
        "*.sqlite3",
        "*.sqlite3-*",
        "*.db",
        "*.db-*",
        "*.wal",
        "*.shm",
        "*.log",
        "support-bundle*.json"
    )
    $forbiddenPathFragments = @(
        "browser-profile/",
        "browser-profiles/",
        "downloads/",
        "node_modules/",
        "dist/"
    )

    $payloadFiles = Get-ChildItem -LiteralPath $payloadRoot -Recurse -File
    foreach ($file in $payloadFiles) {
        foreach ($pattern in $forbiddenNamePatterns) {
            if ($file.Name -like $pattern) {
                throw "Forbidden runtime artifact '$($file.FullName)' found in packaged output."
            }
        }

        $relativePath = (Get-RelativePosixPath -BasePath $payloadRoot -TargetPath $file.FullName).ToLowerInvariant()
        foreach ($fragment in $forbiddenPathFragments) {
            if ($relativePath.Contains($fragment)) {
                throw "Forbidden packaged path '$relativePath' matched fragment '$fragment'."
            }
        }
    }

    $checksumEntries = Get-Content -LiteralPath $checksumsPath | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    $seen = @{}
    foreach ($entry in $checksumEntries) {
        $match = [regex]::Match($entry, '^(?<hash>[0-9a-f]{64})\s{2}(?<path>.+)$')
        if (-not $match.Success) {
            throw "Malformed checksum entry: $entry"
        }

        $expectedHash = $match.Groups["hash"].Value.ToLowerInvariant()
        $relativePath = $match.Groups["path"].Value
        $absolutePath = Join-Path $payloadRoot ($relativePath -replace '/', [IO.Path]::DirectorySeparatorChar)
        $absolutePath = Assert-FileExists -Path $absolutePath -Label "Checksum subject $relativePath"
        $actualHash = Get-Sha256Hex -Path $absolutePath
        if ($actualHash -ne $expectedHash) {
            throw "Checksum mismatch for $relativePath. Expected $expectedHash, got $actualHash."
        }
        $seen[$relativePath] = $true
    }

    foreach ($file in $payloadFiles) {
        $relativePath = Get-RelativePosixPath -BasePath $payloadRoot -TargetPath $file.FullName
        if ($relativePath -eq "checksums.txt") {
            continue
        }
        if (-not $seen.ContainsKey($relativePath)) {
            throw "Checksums manifest is missing entry for $relativePath."
        }
    }

    Write-Output "validated_path=$resolvedPath"
    Write-Output "artifact_kind=$($manifest.artifact_kind)"
    Write-Output "version=$($manifest.version)"
    Write-Output "platform=$($manifest.platform)"
}
finally {
    if ($temporaryExtractionRoot) {
        Remove-Item -LiteralPath $temporaryExtractionRoot -Recurse -Force
    }
}
