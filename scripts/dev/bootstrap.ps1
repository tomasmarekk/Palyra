[CmdletBinding()]
param(
    [switch]$Force,
    [string]$BinDir = "$HOME\.local\bin"
)

$ErrorActionPreference = "Stop"

$CargoAuditVersion = if ($env:CARGO_AUDIT_VERSION) { $env:CARGO_AUDIT_VERSION } else { "0.22.1" }
$CargoDenyVersion = if ($env:CARGO_DENY_VERSION) { $env:CARGO_DENY_VERSION } else { "0.19.0" }
$OsvVersion = if ($env:OSV_VERSION) { $env:OSV_VERSION } else { "v2.2.2" }
$GitleaksVersion = if ($env:GITLEAKS_VERSION) { $env:GITLEAKS_VERSION } else { "v8.30.0" }

function Ensure-Command {
    param([Parameter(Mandatory = $true)][string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' is not available."
    }
}

function Get-ExpectedChecksum {
    param(
        [Parameter(Mandatory = $true)][string]$ManifestPath,
        [Parameter(Mandatory = $true)][string]$AssetName
    )

    foreach ($line in Get-Content -Path $ManifestPath) {
        $trimmed = $line.Trim()
        if ([string]::IsNullOrWhiteSpace($trimmed)) {
            continue
        }
        $parts = $trimmed -split "\s+"
        if ($parts.Count -lt 2) {
            continue
        }
        $candidate = $parts[1].TrimStart("*")
        if ($candidate -eq $AssetName) {
            return $parts[0].ToLowerInvariant()
        }
    }

    throw "Failed to find checksum for '$AssetName' in '$ManifestPath'."
}

function Assert-Checksum {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Expected
    )

    $actual = (Get-FileHash -Algorithm SHA256 -Path $Path).Hash.ToLowerInvariant()
    if ($actual -ne $Expected.ToLowerInvariant()) {
        throw "Checksum mismatch for '$Path'. Expected '$Expected', got '$actual'."
    }
}

function Install-CargoSubcommand {
    param(
        [Parameter(Mandatory = $true)][string]$Subcommand,
        [Parameter(Mandatory = $true)][string]$Crate,
        [Parameter(Mandatory = $true)][string]$Version
    )

    if (-not $Force) {
        & cargo $Subcommand --version *> $null
        if ($LASTEXITCODE -eq 0) {
            Write-Output "cargo $Subcommand already installed; skipping"
            return
        }
    }

    & cargo install --locked $Crate --version $Version
}

function Install-OsvScanner {
    $arch = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString().ToLowerInvariant()
    $assetName = switch ($arch) {
        "x64" { "osv-scanner_windows_amd64.exe" }
        "arm64" { "osv-scanner_windows_arm64.exe" }
        default { throw "Unsupported architecture '$arch' for osv-scanner bootstrap." }
    }

    if (-not $Force -and (Get-Command osv-scanner -ErrorAction SilentlyContinue)) {
        Write-Output "osv-scanner already installed; skipping"
        return
    }

    $tmp = Join-Path ([System.IO.Path]::GetTempPath()) ("palyra-osv-" + [Guid]::NewGuid().ToString("N"))
    New-Item -ItemType Directory -Path $tmp -Force | Out-Null

    try {
        $checksumsPath = Join-Path $tmp "osv-scanner_SHA256SUMS"
        $assetPath = Join-Path $tmp $assetName
        Invoke-WebRequest -Uri "https://github.com/google/osv-scanner/releases/download/$OsvVersion/osv-scanner_SHA256SUMS" -OutFile $checksumsPath
        Invoke-WebRequest -Uri "https://github.com/google/osv-scanner/releases/download/$OsvVersion/$assetName" -OutFile $assetPath

        $expected = Get-ExpectedChecksum -ManifestPath $checksumsPath -AssetName $assetName
        Assert-Checksum -Path $assetPath -Expected $expected

        New-Item -ItemType Directory -Path $BinDir -Force | Out-Null
        Copy-Item -Path $assetPath -Destination (Join-Path $BinDir "osv-scanner.exe") -Force
    } finally {
        Remove-Item -Path $tmp -Recurse -Force -ErrorAction SilentlyContinue
    }
}

function Install-Gitleaks {
    $versionNoV = $GitleaksVersion.TrimStart("v")
    $arch = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString().ToLowerInvariant()
    $assetName = switch ($arch) {
        "x64" { "gitleaks_${versionNoV}_windows_x64.zip" }
        "arm64" {
            Write-Warning "Native Windows arm64 asset is not published for gitleaks ${GitleaksVersion}; using windows_x64 package."
            "gitleaks_${versionNoV}_windows_x64.zip"
        }
        default { throw "Unsupported architecture '$arch' for gitleaks bootstrap." }
    }

    if (-not $Force -and (Get-Command gitleaks -ErrorAction SilentlyContinue)) {
        Write-Output "gitleaks already installed; skipping"
        return
    }

    $tmp = Join-Path ([System.IO.Path]::GetTempPath()) ("palyra-gitleaks-" + [Guid]::NewGuid().ToString("N"))
    $extractDir = Join-Path $tmp "extract"
    New-Item -ItemType Directory -Path $extractDir -Force | Out-Null

    try {
        $checksumsPath = Join-Path $tmp "gitleaks_checksums.txt"
        $assetPath = Join-Path $tmp $assetName
        Invoke-WebRequest -Uri "https://github.com/gitleaks/gitleaks/releases/download/$GitleaksVersion/gitleaks_${versionNoV}_checksums.txt" -OutFile $checksumsPath
        Invoke-WebRequest -Uri "https://github.com/gitleaks/gitleaks/releases/download/$GitleaksVersion/$assetName" -OutFile $assetPath

        $expected = Get-ExpectedChecksum -ManifestPath $checksumsPath -AssetName $assetName
        Assert-Checksum -Path $assetPath -Expected $expected

        Expand-Archive -Path $assetPath -DestinationPath $extractDir -Force
        $binary = Get-ChildItem -Path $extractDir -File -Recurse -Filter "gitleaks.exe" | Select-Object -First 1
        if (-not $binary) {
            throw "gitleaks archive did not contain expected gitleaks.exe binary."
        }

        New-Item -ItemType Directory -Path $BinDir -Force | Out-Null
        Copy-Item -Path $binary.FullName -Destination (Join-Path $BinDir "gitleaks.exe") -Force
    } finally {
        Remove-Item -Path $tmp -Recurse -Force -ErrorAction SilentlyContinue
    }
}

Ensure-Command -Name cargo
Ensure-Command -Name Invoke-WebRequest

Install-CargoSubcommand -Subcommand "audit" -Crate "cargo-audit" -Version $CargoAuditVersion
Install-CargoSubcommand -Subcommand "deny" -Crate "cargo-deny" -Version $CargoDenyVersion
Install-OsvScanner
Install-Gitleaks

Write-Output "Installed toolchain binaries in '$BinDir'."
$pathEntries = ($env:Path -split ";") | Where-Object { $_ -ne "" }
if (-not ($pathEntries -contains $BinDir)) {
    Write-Output "Add '$BinDir' to PATH to run osv-scanner and gitleaks from your shell."
}

& cargo audit --version
& cargo deny --version
& (Join-Path $BinDir "osv-scanner.exe") --version
& (Join-Path $BinDir "gitleaks.exe") version
