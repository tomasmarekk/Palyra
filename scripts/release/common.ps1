Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

$script:PalyraCliProfileStartMarker = "# >>> Palyra CLI >>>"
$script:PalyraCliProfileEndMarker = "# <<< Palyra CLI <<<"

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

function Convert-KeyValueOutputToHashtable {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Lines
    )

    $result = @{}
    foreach ($line in $Lines) {
        if ([string]::IsNullOrWhiteSpace($line)) {
            continue
        }

        $parts = $line -split "=", 2
        if ($parts.Count -ne 2) {
            throw "Unexpected script output line: $line"
        }

        $result[$parts[0].Trim()] = $parts[1].Trim()
    }

    return $result
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

    Invoke-CommandQuiet -Command $ExecutablePath -Arguments $Arguments
}

function Invoke-CommandQuiet {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Command,
        [string[]]$Arguments = @()
    )

    $output = @(& $Command @Arguments 2>&1)
    $exitCode = if ($null -eq $LASTEXITCODE) { 0 } else { $LASTEXITCODE }
    if ($exitCode -ne 0) {
        $detail =
            if ($output.Count -eq 0) {
                ""
            } else {
                " Output: $((($output | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine).Trim())"
            }
        throw "Command exited with code ${exitCode}: $Command$detail"
    }
}

function Normalize-PathEntry {
    param(
        [Parameter(Mandatory = $true)]
        [string]$PathEntry
    )

    $trimmed = $PathEntry.Trim().Trim('"')
    if ([string]::IsNullOrWhiteSpace($trimmed)) {
        return ""
    }

    try {
        $fullPath = [IO.Path]::GetFullPath($trimmed)
    } catch {
        $fullPath = $trimmed
    }

    return $fullPath.TrimEnd([IO.Path]::DirectorySeparatorChar, [IO.Path]::AltDirectorySeparatorChar)
}

function Test-PathEntryEquals {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Left,
        [Parameter(Mandatory = $true)]
        [string]$Right
    )

    $comparison =
        if ($IsWindows) {
            [StringComparison]::OrdinalIgnoreCase
        } else {
            [StringComparison]::Ordinal
        }

    return [string]::Equals((Normalize-PathEntry -PathEntry $Left), (Normalize-PathEntry -PathEntry $Right), $comparison)
}

function Get-PathEntries {
    param(
        [string]$PathValue = $env:PATH
    )

    if ([string]::IsNullOrWhiteSpace($PathValue)) {
        return @()
    }

    $entries = New-Object System.Collections.Generic.List[string]
    foreach ($entry in ($PathValue -split [IO.Path]::PathSeparator)) {
        if ([string]::IsNullOrWhiteSpace($entry)) {
            continue
        }

        $entries.Add($entry.Trim()) | Out-Null
    }

    return @($entries)
}

function Test-PathEntryPresent {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Entry,
        [string]$PathValue = $env:PATH
    )

    foreach ($existingEntry in (Get-PathEntries -PathValue $PathValue)) {
        if (Test-PathEntryEquals -Left $existingEntry -Right $Entry) {
            return $true
        }
    }

    return $false
}

function Prepend-PathEntry {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Entry,
        [string]$PathValue = $env:PATH
    )

    if (Test-PathEntryPresent -Entry $Entry -PathValue $PathValue) {
        return $PathValue
    }

    if ([string]::IsNullOrWhiteSpace($PathValue)) {
        return $Entry
    }

    return "$Entry$([IO.Path]::PathSeparator)$PathValue"
}

function Remove-PathEntry {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Entry,
        [string]$PathValue = $env:PATH
    )

    $remainingEntries = New-Object System.Collections.Generic.List[string]
    foreach ($existingEntry in (Get-PathEntries -PathValue $PathValue)) {
        if (-not (Test-PathEntryEquals -Left $existingEntry -Right $Entry)) {
            $remainingEntries.Add($existingEntry) | Out-Null
        }
    }

    return ($remainingEntries -join [IO.Path]::PathSeparator)
}

function Add-CurrentSessionPathEntry {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Entry
    )

    if (Test-PathEntryPresent -Entry $Entry -PathValue $env:PATH) {
        return $false
    }

    $env:PATH = Prepend-PathEntry -Entry $Entry -PathValue $env:PATH
    return $true
}

function Remove-CurrentSessionPathEntry {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Entry
    )

    if (-not (Test-PathEntryPresent -Entry $Entry -PathValue $env:PATH)) {
        return $false
    }

    $env:PATH = Remove-PathEntry -Entry $Entry -PathValue $env:PATH
    return $true
}

function Get-HomeDirectory {
    $homeDirectory = [Environment]::GetFolderPath("UserProfile")
    if ([string]::IsNullOrWhiteSpace($homeDirectory)) {
        if ([string]::IsNullOrWhiteSpace($HOME)) {
            throw "Unable to resolve the current user's home directory."
        }
        $homeDirectory = $HOME
    }

    return [IO.Path]::GetFullPath($homeDirectory)
}

function Get-PalyraCliCommandRoot {
    param(
        [string]$CommandRootOverride
    )

    if (-not [string]::IsNullOrWhiteSpace($CommandRootOverride)) {
        return [IO.Path]::GetFullPath($CommandRootOverride)
    }

    if ($IsWindows) {
        $localAppData = [Environment]::GetFolderPath("LocalApplicationData")
        if ([string]::IsNullOrWhiteSpace($localAppData)) {
            throw "Unable to resolve LocalApplicationData for Palyra CLI exposure."
        }

        return Join-Path $localAppData "Palyra/bin"
    }

    return Join-Path (Get-HomeDirectory) ".local/bin"
}

function Get-WindowsUserPathValue {
    return [Environment]::GetEnvironmentVariable("Path", "User")
}

function Set-WindowsUserPathValue {
    param(
        [AllowNull()]
        [string]$PathValue
    )

    [Environment]::SetEnvironmentVariable("Path", $PathValue, "User")
}

function Add-WindowsUserPathEntry {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Entry
    )

    $existingPath = Get-WindowsUserPathValue
    if (Test-PathEntryPresent -Entry $Entry -PathValue $existingPath) {
        return $false
    }

    Set-WindowsUserPathValue -PathValue (Prepend-PathEntry -Entry $Entry -PathValue $existingPath)
    return $true
}

function Remove-WindowsUserPathEntry {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Entry
    )

    $existingPath = Get-WindowsUserPathValue
    if (-not (Test-PathEntryPresent -Entry $Entry -PathValue $existingPath)) {
        return $false
    }

    $updatedPath = Remove-PathEntry -Entry $Entry -PathValue $existingPath
    if ([string]::IsNullOrWhiteSpace($updatedPath)) {
        $updatedPath = $null
    }
    Set-WindowsUserPathValue -PathValue $updatedPath
    return $true
}

function Get-PalyraCliManagedProfilePaths {
    $homeDirectory = Get-HomeDirectory
    $profilePaths = New-Object System.Collections.Generic.List[string]
    $profilePaths.Add((Join-Path $homeDirectory ".profile")) | Out-Null

    if ($IsMacOS) {
        $profilePaths.Add((Join-Path $homeDirectory ".zprofile")) | Out-Null
    }

    $bashProfilePath = Join-Path $homeDirectory ".bash_profile"
    if (Test-Path -LiteralPath $bashProfilePath) {
        $profilePaths.Add($bashProfilePath) | Out-Null
    }

    return @($profilePaths | Select-Object -Unique)
}

function ConvertTo-PosixSingleQuotedLiteral {
    param(
        [Parameter(Mandatory = $true)]
        [AllowEmptyString()]
        [string]$Value
    )

    $singleQuote = [string][char]39
    $doubleQuote = [string][char]34
    $escapedQuote = $singleQuote + $doubleQuote + $singleQuote + $doubleQuote + $singleQuote
    return $singleQuote + $Value.Replace($singleQuote, $escapedQuote) + $singleQuote
}

function Get-PalyraCliProfileBlock {
    param(
        [Parameter(Mandatory = $true)]
        [string]$CommandRoot
    )

    $escapedCommandRoot = ConvertTo-PosixSingleQuotedLiteral -Value $CommandRoot

    return @"
$script:PalyraCliProfileStartMarker
PALYRA_CLI_BIN=$escapedCommandRoot
case ":`$PATH:" in
  *":`$PALYRA_CLI_BIN:"*) ;;
  *) export PATH="`$PALYRA_CLI_BIN:`$PATH" ;;
esac
$script:PalyraCliProfileEndMarker
"@
}

function Ensure-ProfileBlock {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ProfilePath,
        [Parameter(Mandatory = $true)]
        [string]$CommandRoot
    )

    $profileBlock = Get-PalyraCliProfileBlock -CommandRoot $CommandRoot
    $blockPattern = [regex]::Escape($script:PalyraCliProfileStartMarker) + '.*?' + [regex]::Escape($script:PalyraCliProfileEndMarker) + '\r?\n?'
    $existingContent =
        if (Test-Path -LiteralPath $ProfilePath) {
            Get-Content -Raw -LiteralPath $ProfilePath
        } else {
            ""
        }

    $updatedContent =
        if ([string]::IsNullOrWhiteSpace($existingContent)) {
            $profileBlock
        } elseif ([regex]::IsMatch($existingContent, $blockPattern, [System.Text.RegularExpressions.RegexOptions]::Singleline)) {
            [regex]::Replace($existingContent, $blockPattern, "$profileBlock`n", [System.Text.RegularExpressions.RegexOptions]::Singleline).TrimEnd("`r", "`n")
        } else {
            $existingContent.TrimEnd("`r", "`n") + "`n`n" + $profileBlock
        }

    if ($updatedContent -eq $existingContent) {
        return $false
    }

    $profileParent = Split-Path -Parent $ProfilePath
    if ($profileParent) {
        New-Item -ItemType Directory -Path $profileParent -Force | Out-Null
    }
    Set-Content -LiteralPath $ProfilePath -Value $updatedContent -NoNewline
    return $true
}

function Remove-ProfileBlock {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ProfilePath
    )

    if (-not (Test-Path -LiteralPath $ProfilePath)) {
        return $false
    }

    $existingContent = Get-Content -Raw -LiteralPath $ProfilePath
    $blockPattern = [regex]::Escape($script:PalyraCliProfileStartMarker) + '.*?' + [regex]::Escape($script:PalyraCliProfileEndMarker) + '\r?\n?'
    $updatedContent = [regex]::Replace($existingContent, $blockPattern, "", [System.Text.RegularExpressions.RegexOptions]::Singleline).Trim()

    if ($updatedContent -eq $existingContent.Trim()) {
        return $false
    }

    if ([string]::IsNullOrWhiteSpace($updatedContent)) {
        Remove-Item -LiteralPath $ProfilePath -Force
    } else {
        Set-Content -LiteralPath $ProfilePath -Value $updatedContent -NoNewline
    }

    return $true
}

function Set-ExecutablePermissions {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    if (-not $IsWindows) {
        Invoke-CommandQuiet -Command "chmod" -Arguments @("755", $Path)
    }
}

function Test-DirectoryEmpty {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    if (-not (Test-Path -LiteralPath $Path -PathType Container)) {
        return $true
    }

    return -not (Get-ChildItem -LiteralPath $Path -Force | Select-Object -First 1)
}

function Resolve-PortableConfigPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$StateRoot
    )

    $resolvedStateRoot = [IO.Path]::GetFullPath($StateRoot)
    return Join-Path $resolvedStateRoot "config/palyra.toml"
}

function Ensure-PortableConfigFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ConfigPath
    )

    $resolvedConfigPath = [IO.Path]::GetFullPath($ConfigPath)
    $configParent = Split-Path -Parent $resolvedConfigPath
    if (-not [string]::IsNullOrWhiteSpace($configParent)) {
        New-Item -ItemType Directory -Path $configParent -Force | Out-Null
    }

    if (-not (Test-Path -LiteralPath $resolvedConfigPath)) {
        Set-Content -LiteralPath $resolvedConfigPath -Value "" -NoNewline
    }

    return $resolvedConfigPath
}

function Install-PalyraCliExposure {
    param(
        [Parameter(Mandatory = $true)]
        [string]$TargetBinaryPath,
        [string]$CommandRoot,
        [string]$StateRoot,
        [string]$ConfigPath,
        [bool]$PersistPath = $true
    )

    $resolvedTargetBinary = Assert-FileExists -Path $TargetBinaryPath -Label "CLI binary"
    $resolvedCommandRoot = Get-PalyraCliCommandRoot -CommandRootOverride $CommandRoot
    New-Item -ItemType Directory -Path $resolvedCommandRoot -Force | Out-Null
    $resolvedStateRoot = $null
    if (-not [string]::IsNullOrWhiteSpace($StateRoot)) {
        $resolvedStateRoot = [IO.Path]::GetFullPath($StateRoot)
    }
    $resolvedConfigPath = $null
    if (-not [string]::IsNullOrWhiteSpace($ConfigPath)) {
        $resolvedConfigPath = [IO.Path]::GetFullPath($ConfigPath)
    }

    $commandName = "palyra"
    $shimPaths = New-Object System.Collections.Generic.List[string]

    if ($IsWindows) {
        $cmdShimPath = Join-Path $resolvedCommandRoot "$commandName.cmd"
        $cmdShimBody =
@"
@echo off
$(if ($null -ne $resolvedStateRoot) { 'set "PALYRA_STATE_ROOT=' + $resolvedStateRoot + '"' })
$(if ($null -ne $resolvedConfigPath) { 'set "PALYRA_CONFIG=' + $resolvedConfigPath + '"' })
"$resolvedTargetBinary" %*
"@
        Set-Content -LiteralPath $cmdShimPath -Value $cmdShimBody -NoNewline
        $shimPaths.Add($cmdShimPath) | Out-Null

        $psShimPath = Join-Path $resolvedCommandRoot "$commandName.ps1"
        $psShimBody =
@"
Set-StrictMode -Version Latest
`$ErrorActionPreference = "Stop"
$(if ($null -ne $resolvedStateRoot) { '$env:PALYRA_STATE_ROOT = "' + $resolvedStateRoot + '"' })
$(if ($null -ne $resolvedConfigPath) { '$env:PALYRA_CONFIG = "' + $resolvedConfigPath + '"' })
& "$resolvedTargetBinary" @args
exit `$LASTEXITCODE
"@
        Set-Content -LiteralPath $psShimPath -Value $psShimBody -NoNewline
        $shimPaths.Add($psShimPath) | Out-Null
    } else {
        $shimPath = Join-Path $resolvedCommandRoot $commandName
        $shimBody =
@"
#!/usr/bin/env sh
set -eu
$(if ($null -ne $resolvedStateRoot) { 'export PALYRA_STATE_ROOT="' + $resolvedStateRoot + '"' })
$(if ($null -ne $resolvedConfigPath) { 'export PALYRA_CONFIG="' + $resolvedConfigPath + '"' })
exec "$resolvedTargetBinary" "$@"
"@
        Set-Content -LiteralPath $shimPath -Value $shimBody -NoNewline
        Set-ExecutablePermissions -Path $shimPath
        $shimPaths.Add($shimPath) | Out-Null
    }

    $sessionPathUpdated = Add-CurrentSessionPathEntry -Entry $resolvedCommandRoot
    $persistenceStrategy = if ($PersistPath) { if ($IsWindows) { "windows-user-path" } else { "posix-profile" } } else { "session-only" }
    $userPathUpdated = $false
    $profileFiles = New-Object System.Collections.Generic.List[string]

    if ($PersistPath) {
        if ($IsWindows) {
            $userPathUpdated = Add-WindowsUserPathEntry -Entry $resolvedCommandRoot
        } else {
            foreach ($profilePath in (Get-PalyraCliManagedProfilePaths)) {
                if (Ensure-ProfileBlock -ProfilePath $profilePath -CommandRoot $resolvedCommandRoot) {
                    $profileFiles.Add($profilePath) | Out-Null
                }
            }
        }
    }

    return [ordered]@{
        command_name = $commandName
        command_root = $resolvedCommandRoot
        command_path = $shimPaths[0]
        shim_paths = @($shimPaths)
        target_binary_path = $resolvedTargetBinary
        state_root = $resolvedStateRoot
        config_path = $resolvedConfigPath
        session_path_updated = $sessionPathUpdated
        persistent_path_requested = $PersistPath
        persistence_strategy = $persistenceStrategy
        user_path_updated = $userPathUpdated
        profile_files = @($profileFiles)
    }
}

function Remove-PalyraCliExposure {
    param(
        [Parameter(Mandatory = $true)]
        [object]$CliExposure
    )

    $commandRoot = if ($null -eq $CliExposure.command_root) { $null } else { [string]$CliExposure.command_root }
    $targetBinaryPath = if ($null -eq $CliExposure.target_binary_path) { $null } else { [string]$CliExposure.target_binary_path }
    $persistentPathRequested = $false
    if ($null -ne $CliExposure.persistent_path_requested) {
        $persistentPathRequested = [bool]$CliExposure.persistent_path_requested
    }
    $userPathUpdated = $false
    if ($null -ne $CliExposure.user_path_updated) {
        $userPathUpdated = [bool]$CliExposure.user_path_updated
    }

    $shimPaths = New-Object System.Collections.Generic.List[string]
    if ($null -ne $CliExposure.shim_paths) {
        foreach ($shimPath in $CliExposure.shim_paths) {
            if (-not [string]::IsNullOrWhiteSpace([string]$shimPath)) {
                $shimPaths.Add([string]$shimPath) | Out-Null
            }
        }
    } elseif ($null -ne $CliExposure.command_path -and -not [string]::IsNullOrWhiteSpace([string]$CliExposure.command_path)) {
        $shimPaths.Add([string]$CliExposure.command_path) | Out-Null
    }

    $removedShimPaths = New-Object System.Collections.Generic.List[string]
    foreach ($shimPath in $shimPaths) {
        if (-not (Test-Path -LiteralPath $shimPath -PathType Leaf)) {
            continue
        }

        $shouldRemove = $true
        if (-not [string]::IsNullOrWhiteSpace($targetBinaryPath)) {
            $shimContent = Get-Content -Raw -LiteralPath $shimPath
            $comparison =
                if ($IsWindows) {
                    [StringComparison]::OrdinalIgnoreCase
                } else {
                    [StringComparison]::Ordinal
                }
            $shouldRemove = $shimContent.IndexOf($targetBinaryPath, $comparison) -ge 0
        }

        if ($shouldRemove) {
            Remove-Item -LiteralPath $shimPath -Force
            $removedShimPaths.Add($shimPath) | Out-Null
        }
    }

    $profileFilesRemoved = New-Object System.Collections.Generic.List[string]
    $commandRootRemoved = $false
    $commandRootEmpty = $true
    if (-not [string]::IsNullOrWhiteSpace($commandRoot) -and (Test-Path -LiteralPath $commandRoot -PathType Container)) {
        $commandRootEmpty = Test-DirectoryEmpty -Path $commandRoot
        if ($commandRootEmpty) {
            Remove-Item -LiteralPath $commandRoot -Force
            $commandRootRemoved = $true
        }
    }

    if ($commandRootEmpty -and -not [string]::IsNullOrWhiteSpace($commandRoot)) {
        Remove-CurrentSessionPathEntry -Entry $commandRoot | Out-Null
    }

    if ($persistentPathRequested -and $commandRootEmpty -and -not [string]::IsNullOrWhiteSpace($commandRoot)) {
        if ($IsWindows) {
            if ($userPathUpdated) {
                Remove-WindowsUserPathEntry -Entry $commandRoot | Out-Null
            }
        } else {
            foreach ($profilePath in $CliExposure.profile_files) {
                $profilePathString = [string]$profilePath
                if ([string]::IsNullOrWhiteSpace($profilePathString)) {
                    continue
                }

                if (Remove-ProfileBlock -ProfilePath $profilePathString) {
                    $profileFilesRemoved.Add($profilePathString) | Out-Null
                }
            }
        }
    }

    return [ordered]@{
        removed_shim_paths = @($removedShimPaths)
        profile_files_removed = @($profileFilesRemoved)
        command_root_removed = $commandRootRemoved
        session_path_updated = (-not [string]::IsNullOrWhiteSpace($commandRoot))
    }
}
