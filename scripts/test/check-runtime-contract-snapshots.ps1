[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$rootDir = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $rootDir

function Write-SnapshotFailureGuidance {
    Write-Error @"
Runtime contract snapshot gate failed.
Next step for intentional public contract changes:
  1. Inspect the test output above and the git diff for the changed golden snapshot.
  2. Update the matching snapshot_version for the changed public contract surface.
  3. Include a changelog_note or migration note in the same change explaining compatibility.
  4. Never add secrets, tokens, private keys, or local absolute paths to snapshots.

For mechanical local refresh only, run the failing test with:
  `$env:PALYRA_UPDATE_CONTRACT_SNAPSHOTS='1'; cargo test <package-and-test> --locked
"@
}

function Invoke-CargoCapture {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$CargoArgs
    )

    $previousNativeErrorPreference = $PSNativeCommandUseErrorActionPreference
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $PSNativeCommandUseErrorActionPreference = $false
        $ErrorActionPreference = "Continue"
        $output = @(& cargo @CargoArgs 2>&1)
        $exitCode = $LASTEXITCODE
    }
    finally {
        $PSNativeCommandUseErrorActionPreference = $previousNativeErrorPreference
        $ErrorActionPreference = $previousErrorActionPreference
    }

    [PSCustomObject]@{
        Output = $output
        ExitCode = $exitCode
    }
}

function Invoke-ContractCheck {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Label,
        [Parameter(Mandatory = $true)]
        [string[]]$CargoArgs
    )

    Write-Output "==> $Label"
    $result = Invoke-CargoCapture -CargoArgs $CargoArgs
    $result.Output | ForEach-Object { Write-Output $_ }
    if ($result.ExitCode -ne 0) {
        Write-SnapshotFailureGuidance
        exit $result.ExitCode
    }
}

function Invoke-ExactContractCheck {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Label,
        [Parameter(Mandatory = $true)]
        [string]$ExpectedTest,
        [Parameter(Mandatory = $true)]
        [string[]]$CargoArgs
    )

    Write-Output "==> $Label"
    $result = Invoke-CargoCapture -CargoArgs $CargoArgs
    $result.Output | ForEach-Object { Write-Output $_ }
    if ($result.ExitCode -ne 0) {
        Write-SnapshotFailureGuidance
        exit $result.ExitCode
    }

    $expectedResult = "test $ExpectedTest ... ok"
    $executedCount = @(
        $result.Output | Where-Object { "$($_)".Trim() -eq $expectedResult }
    ).Count
    if ($executedCount -ne 1) {
        [Console]::Error.WriteLine(
            "Expected exactly one executed Rust test result for ${ExpectedTest}; observed $executedCount."
        )
        Write-SnapshotFailureGuidance
        exit 1
    }
}

Invoke-ContractCheck `
    -Label "public runtime contract snapshot" `
    -CargoArgs @("test", "-p", "palyra-common", "public_runtime_contract_snapshot", "--locked")
Invoke-ContractCheck `
    -Label "plugin SDK public contract snapshot" `
    -CargoArgs @("test", "-p", "palyra-plugins-sdk", "plugin_sdk_contract_snapshot_matches_golden", "--locked")
Invoke-ContractCheck `
    -Label "plugin SDK typed ABI fingerprint" `
    -CargoArgs @("test", "-p", "palyra-plugins-sdk", "typed_contract_abi_fingerprint_matches_golden", "--locked")
Invoke-ContractCheck `
    -Label "plugin executable ABI v2 snapshot" `
    -CargoArgs @("test", "-p", "palyra-plugins-sdk", "executable_abi_v2_snapshot_matches_golden", "--locked")
Invoke-ContractCheck `
    -Label "plugin executable ABI v2 conformance" `
    -CargoArgs @("test", "-p", "palyra-plugins-runtime", "--test", "abi_v2_conformance", "--locked")
Invoke-ContractCheck `
    -Label "skill manifest public contract snapshot" `
    -CargoArgs @("test", "-p", "palyra-skills", "skill_manifest_contract_snapshot_matches_golden", "--locked")
Invoke-ContractCheck `
    -Label "policy diagnostics safe reason-code contract" `
    -CargoArgs @("test", "-p", "palyra-policy", "explain_diagnostics_reports_safe_reason_code_and_hints", "--locked")
Invoke-ContractCheck `
    -Label "daemon aggregate runtime ABI snapshot" `
    -CargoArgs @("test", "-p", "palyra-daemon", "runtime_diagnostics::tests::contract_snapshot_suite_covers_plugin_abi_surfaces", "--locked")
Invoke-ExactContractCheck `
    -Label "managed coding compatibility snapshot" `
    -ExpectedTest "runtime_diagnostics::tests::managed_coding_contract_snapshot_matches_golden" `
    -CargoArgs @("test", "-p", "palyra-daemon", "--lib", "runtime_diagnostics::tests::managed_coding_contract_snapshot_matches_golden", "--locked", "--", "--exact")
Invoke-ExactContractCheck `
    -Label "feature rollout promotion manifest contract" `
    -ExpectedTest "feature_rollout_maturity::manifest::tests::builtin_promotion_manifest_is_valid" `
    -CargoArgs @("test", "-p", "palyra-daemon", "feature_rollout_maturity::manifest::tests::builtin_promotion_manifest_is_valid", "--locked", "--", "--exact")
Invoke-ExactContractCheck `
    -Label "feature rollout direct hot-path proof" `
    -ExpectedTest "gateway::tests::session_compaction_safeguard_rolls_back_writes_when_rollout_enforces_failure" `
    -CargoArgs @("test", "-p", "palyra-daemon", "gateway::tests::session_compaction_safeguard_rolls_back_writes_when_rollout_enforces_failure", "--locked", "--", "--exact")
Invoke-ExactContractCheck `
    -Label "feature rollout no-hidden-fallback proof" `
    -ExpectedTest "gateway::tests::session_compaction_safeguard_records_explicit_fallback_when_disabled" `
    -CargoArgs @("test", "-p", "palyra-daemon", "gateway::tests::session_compaction_safeguard_records_explicit_fallback_when_disabled", "--locked", "--", "--exact")
Invoke-ExactContractCheck `
    -Label "Docker rollout direct container-path proof" `
    -ExpectedTest "gateway::tests::docker_runtime_selects_container_process_path" `
    -CargoArgs @("test", "-p", "palyra-daemon", "gateway::tests::docker_runtime_selects_container_process_path", "--locked", "--", "--exact")
Invoke-ExactContractCheck `
    -Label "Docker rollout no-hidden-fallback proof" `
    -ExpectedTest "gateway::tests::docker_runtime_fails_closed_without_host_fallback" `
    -CargoArgs @("test", "-p", "palyra-daemon", "gateway::tests::docker_runtime_fails_closed_without_host_fallback", "--locked", "--", "--exact")
Invoke-ExactContractCheck `
    -Label "stable core evidence contract" `
    -ExpectedTest "application::core_stability::stable::tests::builtin_evidence_pack_qualifies" `
    -CargoArgs @("test", "-p", "palyra-daemon", "--lib", "application::core_stability::stable::tests::builtin_evidence_pack_qualifies", "--locked", "--", "--exact")
Invoke-ContractCheck `
    -Label "stable core maturity, fixture, and redaction conformance" `
    -CargoArgs @("test", "-p", "palyra-daemon", "--lib", "application::core_stability::stable::tests", "--locked")

Write-Output "==> release dashboard, runbook drill, and support redaction"
& (Join-Path $rootDir "scripts\ci\check-release-dashboard.ps1")
