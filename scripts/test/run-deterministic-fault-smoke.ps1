[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$rootDir = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $rootDir

$artifactRelative = "artifacts/qa-lab/deterministic-fault-smoke"
$artifactRoot = [System.IO.Path]::GetFullPath((Join-Path $rootDir "artifacts"))
$artifactParent = [System.IO.Path]::GetFullPath((Join-Path $artifactRoot "qa-lab"))
$artifactDir = [System.IO.Path]::GetFullPath((Join-Path $rootDir $artifactRelative))
$expectedArtifactDir = [System.IO.Path]::GetFullPath(
    (Join-Path $rootDir "artifacts/qa-lab/deterministic-fault-smoke")
)
if (-not $artifactDir.Equals($expectedArtifactDir, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Refusing to clean an unexpected deterministic fault artifact path: $artifactDir"
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
        throw "Expected deterministic fault artifact directory is missing: $Path"
    }
    $item = Get-Item -LiteralPath $Path -Force
    if (-not $item.PSIsContainer) {
        throw "Deterministic fault artifact path is not a directory: $Path"
    }
    if (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw "Refusing an indirect deterministic fault artifact directory: $Path"
    }
    if (-not $item.FullName.Equals(
        [System.IO.Path]::GetFullPath($Path),
        [System.StringComparison]::OrdinalIgnoreCase
    )) {
        throw "Deterministic fault artifact directory resolved unexpectedly: $Path"
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
    # Campaign checkpoints are durable by design, so a smoke rerun must remove the prior campaign.
    Remove-Item -LiteralPath $artifactDir -Recurse -Force
}
$statusPath = Join-Path $artifactDir "status.txt"
New-Item -ItemType Directory -Path $artifactDir | Out-Null
Assert-OrdinaryArtifactDirectory -Path $artifactDir -AllowMissing $false
Set-Content -LiteralPath $statusPath -Value "status=started" -Encoding utf8

cargo test -p palyra-common --lib qa_fault --locked
& (Join-Path $PSScriptRoot "run-continuity-model-check.ps1")
cargo test -p palyra-cli --lib failure_diagnostics_are_persistable_before_state_root_removal_without_secrets --locked
cargo test -p palyra-daemon --lib "qa_fault_injection::tests" --locked
cargo test -p palyra-daemon --lib tools_list_changed_notification_refreshes_future_calls_without_rewriting_in_flight_call --locked
cargo test -p palyra-daemon --lib cancel_racing_blocked_final_delivery_produces_one_done_terminal_outcome --locked
cargo test -p palyra-daemon --lib qa_fault --features qa-fault-injection --locked
cargo test -p palyra-daemon --lib fixture_provider_fault_adapter_runs_through_the_real_provider_path --features qa-fault-injection --locked
cargo test -p palyra-daemon --lib managed_process_post_spawn_fault_verifies_cleanup_before_exit --features qa-fault-injection --locked
cargo test -p palyra-daemon --lib terminate_cleanup_fault_records_recovery_before_process_exit --features qa-fault-injection --locked
cargo test -p palyra-daemon --lib docker_runner_faults_only_after_verified_cleanup_and_records_recovery --features qa-fault-injection --locked
cargo test -p palyra-connectors --lib --features qa-fault-injection --locked
cargo test -p palyra-workerd --lib --features qa-fault-injection --locked
cargo build -p palyra-daemon --bin palyrad --features qa-fault-injection --locked

$env:PALYRA_QA_PALYRAD_BIN = Join-Path $rootDir "target\debug\palyrad.exe"
try {
    cargo run -p palyra-cli --locked -- qa gate `
        --suite qa/suites/fault_smoke.yaml `
        --output-json "$artifactRelative/fault-smoke.json" `
        --output-markdown "$artifactRelative/fault-smoke.md" `
        --json
    Set-Content -LiteralPath $statusPath -Value "status=passed" -Encoding utf8
}
finally {
    Remove-Item Env:PALYRA_QA_PALYRAD_BIN -ErrorAction SilentlyContinue
}
