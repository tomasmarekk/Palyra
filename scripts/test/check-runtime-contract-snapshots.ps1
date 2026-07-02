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

function Invoke-ContractCheck {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Label,
        [Parameter(Mandatory = $true)]
        [string[]]$CargoArgs
    )

    Write-Output "==> $Label"
    & cargo @CargoArgs
    if ($LASTEXITCODE -ne 0) {
        Write-SnapshotFailureGuidance
        exit $LASTEXITCODE
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
    -Label "skill manifest public contract snapshot" `
    -CargoArgs @("test", "-p", "palyra-skills", "skill_manifest_contract_snapshot_matches_golden", "--locked")
Invoke-ContractCheck `
    -Label "policy diagnostics safe reason-code contract" `
    -CargoArgs @("test", "-p", "palyra-policy", "explain_diagnostics_reports_safe_reason_code_and_hints", "--locked")
Invoke-ContractCheck `
    -Label "daemon aggregate runtime ABI snapshot" `
    -CargoArgs @("test", "-p", "palyra-daemon", "runtime_diagnostics::tests::contract_snapshot_suite_covers_phase11_abi_surfaces", "--locked")
