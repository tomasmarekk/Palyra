[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

$protocVersion = "34.0"
$protocArchive = "protoc-$protocVersion-win64.zip"
$protocDownloadUrl = "https://github.com/protocolbuffers/protobuf/releases/download/v$protocVersion/$protocArchive"
$protocSha256 = "76ddeb5ae7a31c8f9f7759d3b843a4cadda2150ac037ad0c1794665d6cf31fce"
$downloadRoot = if ($env:RUNNER_TEMP) { $env:RUNNER_TEMP } else { [System.IO.Path]::GetTempPath() }

function Resolve-ProtocPath {
    $command = Get-Command protoc.exe -ErrorAction SilentlyContinue
    if (-not $command) {
        $command = Get-Command protoc -ErrorAction SilentlyContinue
    }
    if ($command) {
        return $command.Source
    }

    if ($env:PROTOC -and (Test-Path $env:PROTOC)) {
        return (Resolve-Path $env:PROTOC).Path
    }

    return $null
}

function Confirm-ProtocAvailable([string]$source) {
    $protocPath = Resolve-ProtocPath
    if (-not $protocPath) {
        return $false
    }

    Write-Host "Using protoc from $protocPath ($source)."
    & $protocPath --version
    if ($LASTEXITCODE -ne 0) {
        throw "protoc verification failed after $source."
    }

    return $true
}

function Add-ProtocToPath([string]$binPath, [string]$protocPath) {
    $env:PATH = "$binPath;$env:PATH"
    $env:PROTOC = $protocPath

    if ($env:GITHUB_PATH) {
        Add-Content -Path $env:GITHUB_PATH -Value $binPath
    }
    if ($env:GITHUB_ENV) {
        Add-Content -Path $env:GITHUB_ENV -Value "PROTOC=$protocPath"
    }
}

if (Confirm-ProtocAvailable "preinstalled runner tooling") {
    exit 0
}

$chocoMaxAttempts = 3
for ($attempt = 1; $attempt -le $chocoMaxAttempts; $attempt++) {
    try {
        Write-Host "Attempt ${attempt}/${chocoMaxAttempts}: installing protoc via Chocolatey."
        & choco install protoc --yes --no-progress
        if ($LASTEXITCODE -ne 0) {
            throw "Chocolatey exited with code $LASTEXITCODE."
        }
        if (Confirm-ProtocAvailable "Chocolatey installation") {
            exit 0
        }
        throw "Chocolatey completed without exposing protoc on PATH."
    } catch {
        Write-Warning "Chocolatey protoc install attempt $attempt failed: $($_.Exception.Message)"
        if ($attempt -lt $chocoMaxAttempts) {
            Start-Sleep -Seconds (5 * $attempt)
        }
    }
}

$installRoot = Join-Path $downloadRoot "palyra-protoc-$protocVersion"
$archivePath = Join-Path $installRoot $protocArchive
$extractPath = Join-Path $installRoot "extract"
$binPath = Join-Path $extractPath "bin"
$fallbackProtocPath = Join-Path $binPath "protoc.exe"

if (Test-Path $installRoot) {
    Remove-Item -Path $installRoot -Recurse -Force
}
New-Item -ItemType Directory -Path $extractPath -Force | Out-Null

Write-Warning "Chocolatey protoc installation was unavailable. Falling back to the official protobuf release asset $protocArchive."
Invoke-WebRequest -Uri $protocDownloadUrl -OutFile $archivePath

$downloadHash = (Get-FileHash -Path $archivePath -Algorithm SHA256).Hash.ToLowerInvariant()
if ($downloadHash -ne $protocSha256) {
    throw "Downloaded protoc archive hash mismatch. Expected $protocSha256, got $downloadHash."
}

Expand-Archive -Path $archivePath -DestinationPath $extractPath -Force
if (-not (Test-Path $fallbackProtocPath)) {
    throw "The downloaded protoc archive did not contain $fallbackProtocPath."
}

Add-ProtocToPath -binPath $binPath -protocPath $fallbackProtocPath
if (-not (Confirm-ProtocAvailable "official protobuf release fallback")) {
    throw "Official protobuf release fallback did not make protoc available."
}

Write-Host "Installed protoc from the official protobuf release fallback."
