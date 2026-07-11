[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$rootDir = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $rootDir

$artifactRelative = "artifacts/qa-lab/baseline-replay"
$artifactRoot = [System.IO.Path]::GetFullPath((Join-Path $rootDir "artifacts"))
$artifactParent = [System.IO.Path]::GetFullPath((Join-Path $artifactRoot "qa-lab"))
$artifactDir = [System.IO.Path]::GetFullPath((Join-Path $rootDir $artifactRelative))
$expectedArtifactDir = [System.IO.Path]::GetFullPath(
    (Join-Path $rootDir "artifacts/qa-lab/baseline-replay")
)
if (-not $artifactDir.Equals($expectedArtifactDir, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Refusing to clean an unexpected replay baseline artifact path: $artifactDir"
}

function Assert-OrdinaryArtifactDirectory {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        [bool]$AllowMissing
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        if ($AllowMissing) {
            return
        }
        throw "Expected replay baseline artifact directory is missing: $Path"
    }
    $item = Get-Item -LiteralPath $Path -Force
    if (-not $item.PSIsContainer) {
        throw "Replay baseline artifact path is not a directory: $Path"
    }
    if (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw "Refusing an indirect replay baseline artifact directory: $Path"
    }
    if (-not $item.FullName.Equals(
        [System.IO.Path]::GetFullPath($Path),
        [System.StringComparison]::OrdinalIgnoreCase
    )) {
        throw "Replay baseline artifact directory resolved unexpectedly: $Path"
    }
}

Assert-OrdinaryArtifactDirectory -Path $artifactRoot -AllowMissing $true
if (-not (Test-Path -LiteralPath $artifactRoot)) {
    New-Item -ItemType Directory -Path $artifactRoot | Out-Null
}
Assert-OrdinaryArtifactDirectory -Path $artifactRoot -AllowMissing $false

Assert-OrdinaryArtifactDirectory -Path $artifactParent -AllowMissing $true
if (-not (Test-Path -LiteralPath $artifactParent)) {
    New-Item -ItemType Directory -Path $artifactParent | Out-Null
}
Assert-OrdinaryArtifactDirectory -Path $artifactParent -AllowMissing $false

Assert-OrdinaryArtifactDirectory -Path $artifactDir -AllowMissing $true
if (Test-Path -LiteralPath $artifactDir) {
    Remove-Item -LiteralPath $artifactDir -Recurse -Force
}
$statusPath = Join-Path $artifactDir "status.txt"
New-Item -ItemType Directory -Path $artifactDir | Out-Null
Assert-OrdinaryArtifactDirectory -Path $artifactDir -AllowMissing $false
Set-Content -LiteralPath $statusPath -Value "status=started" -Encoding utf8

$previousPalyradBin = [Environment]::GetEnvironmentVariable("PALYRA_QA_PALYRAD_BIN", "Process")
$palyradEnvironmentConfigured = $false
try {
    cargo test -p palyra-common replay_bundle --locked
    cargo test -p palyra-common --test release_eval_contract --locked
    cargo run -p palyra-cli --example run_release_eval_gate --locked -- `
        --manifest fixtures/golden/release_eval_inventory.json `
        --report-dir target/release-artifacts/release-evals
    cargo test -p palyra-cli support_bundle --locked
    cargo test -p palyra-daemon replay_capture --locked
    cargo build -p palyra-daemon --bin palyrad --locked

    $palyradBin = Join-Path $rootDir "target\debug\palyrad.exe"
    if (-not (Test-Path -LiteralPath $palyradBin)) {
        $palyradBin = Join-Path $rootDir "target\debug\palyrad"
    }
    if (-not (Test-Path -LiteralPath $palyradBin -PathType Leaf)) {
        throw "Replay baseline daemon binary is not a regular file: $palyradBin"
    }
    $palyradItem = Get-Item -LiteralPath $palyradBin -Force
    if (($palyradItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw "Replay baseline daemon binary must not be an indirect file: $palyradBin"
    }

    $env:PALYRA_QA_PALYRAD_BIN = $palyradBin
    $palyradEnvironmentConfigured = $true
    cargo run -p palyra-cli --locked -- qa gate `
        --suite qa/suites/baseline_replay.yaml `
        --output-json "$artifactRelative/baseline-replay.json" `
        --output-markdown "$artifactRelative/baseline-replay.md" `
        --json
    Set-Content -LiteralPath $statusPath -Value "status=passed" -Encoding utf8
}
catch {
    Set-Content -LiteralPath $statusPath -Value "status=failed" -Encoding utf8
    throw
}
finally {
    if ($palyradEnvironmentConfigured) {
        if ($null -eq $previousPalyradBin) {
            Remove-Item Env:PALYRA_QA_PALYRAD_BIN -ErrorAction SilentlyContinue
        }
        else {
            $env:PALYRA_QA_PALYRAD_BIN = $previousPalyradBin
        }
    }
}
