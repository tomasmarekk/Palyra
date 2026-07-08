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
    "Roadmap checkbox policy:",
    "| Area | Maturity | Code complete | Tested | Stable candidate | Blockers |"
)) {
    if (-not $report.Contains($required)) {
        throw "release dashboard report is missing required content: $required"
    }
}

Write-Host "release dashboard ok: $ReportPath"
