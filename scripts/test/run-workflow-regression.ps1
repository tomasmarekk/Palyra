Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)

Push-Location $repoRoot
try {
    $profile = if ($env:PALYRA_WORKFLOW_REGRESSION_PROFILE) {
        $env:PALYRA_WORKFLOW_REGRESSION_PROFILE
    } else {
        "fast"
    }
    $reportDir = if ($env:PALYRA_WORKFLOW_REGRESSION_REPORT_DIR) {
        $env:PALYRA_WORKFLOW_REGRESSION_REPORT_DIR
    } else {
        Join-Path $repoRoot "target/release-artifacts/workflow-regression/$profile"
    }

    $env:PALYRA_WORKFLOW_REGRESSION_CARGO_BIN = "cargo"
    cargo run -p palyra-cli --example run_workflow_regression --locked -- `
        --profile $profile `
        --report-dir $reportDir
}
finally {
    Pop-Location
}
