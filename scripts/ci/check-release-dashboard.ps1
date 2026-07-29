param(
    [string] $ReportPath = "qa/reports/release_acceptance_dashboard.md"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$fullPath = Join-Path $repoRoot $ReportPath

if (-not (Test-Path $fullPath)) {
    throw "release dashboard report not found: $fullPath"
}

$report = Get-Content -Raw -Encoding UTF8 $fullPath

foreach ($required in @(
    "# Release Acceptance Dashboard",
    "Stable candidates:",
    "Acceptance policy:",
    "Runbooks: [Runtime Incident Runbooks](runtime_incident_runbooks.md)",
    "| Area | Maturity | Code complete | Tested | Stable candidate | Blockers |"
)) {
    if (-not $report.Contains($required)) {
        throw "release dashboard report is missing required content: $required"
    }
}

$runbookPath = Join-Path (Split-Path -Parent $fullPath) "runtime_incident_runbooks.md"
if (-not (Test-Path $runbookPath)) {
    throw "runtime incident runbook not found: $runbookPath"
}

$runbook = Get-Content -Raw -Encoding UTF8 $runbookPath
foreach ($required in @(
    "# Runtime Incident Runbooks",
    "## Shared Evidence",
    "## Security Invariants",
    "## Agent Harness Runtime",
    "## Execution Gate Pipeline",
    "## Provider Recovery",
    "## Replay Capture",
    "## Verification Runtime",
    "## Compaction Safeguard",
    "## Advisor Fanout",
    "## LSP Service",
    "## Routine Scheduler",
    "## ACP Runtime"
)) {
    if (-not $runbook.Contains($required)) {
        throw "runtime incident runbook is missing required content: $required"
    }
}

Write-Host "release dashboard ok: $ReportPath"
