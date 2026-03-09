Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

function Get-RepoRoot {
    return Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}

function Get-ReleaseOutputRoot {
    return Join-Path (Get-RepoRoot) "target/release-artifacts"
}

function Resolve-ExecutableName {
    param(
        [Parameter(Mandatory = $true)]
        [string]$BaseName
    )

    if ($IsWindows) {
        return "$BaseName.exe"
    }

    return $BaseName
}

function Get-PlatformSlug {
    $osPart =
        if ($IsWindows) { "windows" }
        elseif ($IsMacOS) { "macos" }
        elseif ($IsLinux) { "linux" }
        else { throw "Unsupported operating system for release packaging." }

    $rawArch =
        if ($env:PROCESSOR_ARCHITEW6432) {
            $env:PROCESSOR_ARCHITEW6432
        } elseif ($env:PROCESSOR_ARCHITECTURE) {
            $env:PROCESSOR_ARCHITECTURE
        } else {
            try {
                [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
            } catch {
                (& uname -m)
            }
        }

    $archPart = switch ($rawArch.ToLowerInvariant()) {
        { $_ -in @("amd64", "x86_64", "x64") } { "x64"; break }
        { $_ -in @("arm64", "aarch64") } { "arm64"; break }
        default { $_ }
    }

    return "$osPart-$archPart"
}

function Assert-FileExists {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        [string]$Label
    )

    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "$Label does not exist: $Path"
    }

    return (Resolve-Path -LiteralPath $Path).Path
}

function New-CleanDirectory {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    if (Test-Path -LiteralPath $Path) {
        Remove-Item -LiteralPath $Path -Recurse -Force
    }
    New-Item -ItemType Directory -Path $Path -Force | Out-Null
    return (Resolve-Path -LiteralPath $Path).Path
}

function Get-Sha256Hex {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

function Get-RelativePosixPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$BasePath,
        [Parameter(Mandatory = $true)]
        [string]$TargetPath
    )

    $relativePath = [IO.Path]::GetRelativePath([IO.Path]::GetFullPath($BasePath), [IO.Path]::GetFullPath($TargetPath))
    return $relativePath -replace '\\', '/'
}

function Read-JsonFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    return Get-Content -Raw -LiteralPath $Path | ConvertFrom-Json
}

function Expand-ZipToTemporaryDirectory {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ArchivePath
    )

    $tempRoot = Join-Path ([IO.Path]::GetTempPath()) ("palyra-release-" + [guid]::NewGuid().ToString("N"))
    New-Item -ItemType Directory -Path $tempRoot -Force | Out-Null
    Expand-ZipArchiveSafely -ArchivePath $ArchivePath -DestinationPath $tempRoot
    return $tempRoot
}

function Expand-ZipArchiveSafely {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ArchivePath,
        [Parameter(Mandatory = $true)]
        [string]$DestinationPath
    )

    Add-Type -AssemblyName System.IO.Compression.FileSystem

    $archivePath = Assert-FileExists -Path $ArchivePath -Label "Archive"
    New-Item -ItemType Directory -Path $DestinationPath -Force | Out-Null

    $destinationRoot = [IO.Path]::GetFullPath($DestinationPath)
    if (-not $destinationRoot.EndsWith([IO.Path]::DirectorySeparatorChar)) {
        $destinationRoot += [IO.Path]::DirectorySeparatorChar
    }
    $pathComparison =
        if ($IsWindows) {
            [StringComparison]::OrdinalIgnoreCase
        } else {
            [StringComparison]::Ordinal
        }

    $archive = [System.IO.Compression.ZipFile]::OpenRead($archivePath)
    try {
        foreach ($entry in $archive.Entries) {
            $entryPath = $entry.FullName
            if ([IO.Path]::IsPathRooted($entryPath)) {
                throw "Archive contains an absolute path entry: '$entryPath'"
            }

            $expandedPath = [IO.Path]::GetFullPath((Join-Path $destinationRoot $entryPath))
            if (-not $expandedPath.StartsWith($destinationRoot, $pathComparison)) {
                throw "Archive contains a path traversal entry: '$entryPath'"
            }
        }
    }
    finally {
        $archive.Dispose()
    }

    [System.IO.Compression.ZipFile]::ExtractToDirectory($archivePath, $destinationRoot, $true)
}

function Get-WorkspaceVersion {
    $repoRoot = Get-RepoRoot
    $cargoTomlPath = Join-Path $repoRoot "Cargo.toml"
    $content = Get-Content -Raw -LiteralPath $cargoTomlPath
    $workspaceMatch = [regex]::Match(
        $content,
        '(?ms)^\[workspace\.package\].*?^version\s*=\s*"(?<version>[^"]+)"'
    )
    if (-not $workspaceMatch.Success) {
        throw "Unable to locate [workspace.package] version in $cargoTomlPath"
    }
    return $workspaceMatch.Groups["version"].Value
}

function Invoke-ExecutableQuiet {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ExecutablePath,
        [string[]]$Arguments = @()
    )

    $null = & $ExecutablePath @Arguments
}
