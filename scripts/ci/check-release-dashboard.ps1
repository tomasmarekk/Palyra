param(
    [string] $ReportPath = "qa/reports/release_acceptance_dashboard.md",
    [string] $EvidencePackPath = "infra/release/stable-core-evidence.json",
    [string] $RolloutGatesPath = "infra/release/rollout-gates.json",
    [string] $AlertFixturePath = "qa/fixtures/core-runtime-alert-thresholds.v1.json",
    [string] $DrillFixturePath = "qa/fixtures/core-runtime-runbook-drill.v1.json"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path

function Read-RepoText {
    param([Parameter(Mandatory = $true)][string] $RelativePath)

    $fullPath = Join-Path $repoRoot $RelativePath
    if (-not (Test-Path -LiteralPath $fullPath)) {
        throw "release evidence file not found: $fullPath"
    }
    Get-Content -Raw -Encoding UTF8 -LiteralPath $fullPath
}

function Read-RepoJson {
    param([Parameter(Mandatory = $true)][string] $RelativePath)

    (Read-RepoText -RelativePath $RelativePath) | ConvertFrom-Json -Depth 100
}

$report = Read-RepoText -RelativePath $ReportPath
$pack = Read-RepoJson -RelativePath $EvidencePackPath
$rolloutGates = Read-RepoJson -RelativePath $RolloutGatesPath
$alerts = Read-RepoJson -RelativePath $AlertFixturePath
$drill = Read-RepoJson -RelativePath $DrillFixturePath

if ($pack.schema_version -ne 1 -or
    $pack.contract_id -ne "palyra.stable-core-evidence.v1" -or
    $pack.runtime_contract_version -ne "runtime-contracts.v16") {
    throw "stable core evidence contract identity is invalid"
}

$capabilities = @($pack.capabilities)
$stable = @($capabilities | Where-Object { $_.maturity -eq "stable" })
$gated = @($capabilities | Where-Object { $_.maturity -eq "gated_production" })
$p0Blockers = @($capabilities | ForEach-Object { @($_.promotion_blockers) }).Count

if ($capabilities.Count -ne 7 -or $stable.Count -ne 1 -or
    $gated.Count -ne 6 -or $p0Blockers -ne 0) {
    throw "stable core capability counts or P0 blocker count are invalid"
}

foreach ($capability in $capabilities) {
    if ($capability.evidence_status -ne "passed" -or
        $capability.owner_signoff -ne "@tomasmarekk" -or
        -not $capability.direct_hot_path -or
        -not $capability.no_hidden_fallback -or
        -not $capability.default_for_new_runs -or
        -not $capability.rollback_preserves_durable_data -or
        $capability.rollback_repeats_confirmed_side_effects -or
        @($capability.required_gate_refs).Count -eq 0 -or
        @($capability.runbook_ids).Count -eq 0) {
        throw "incomplete release evidence for capability: $($capability.capability_id)"
    }
}

foreach ($required in @(
    '# Release Acceptance Dashboard',
    '- Core capabilities: 7',
    '- Stable capabilities: 1',
    '- Gated-production capabilities: 6',
    '- P0 blockers: 0',
    '- Evidence pack: `infra/release/stable-core-evidence.json`',
    '- Runbooks: [Runtime Incident Runbooks](runtime_incident_runbooks.md)',
    '| Capability | Maturity | Evidence | Owner sign-off | P0 blockers | Runbooks |'
)) {
    if (-not $report.Contains($required)) {
        throw "release dashboard report is missing required content: $required"
    }
}

foreach ($capability in $capabilities) {
    $rowPrefix = "| $($capability.capability_id) | $($capability.maturity) | passed | @tomasmarekk | 0 |"
    if (-not $report.Contains($rowPrefix)) {
        throw "release dashboard is out of sync for capability: $($capability.capability_id)"
    }
}

$runbookPath = Join-Path (Split-Path -Parent (Join-Path $repoRoot $ReportPath)) "runtime_incident_runbooks.md"
if (-not (Test-Path -LiteralPath $runbookPath)) {
    throw "runtime incident runbook not found: $runbookPath"
}
$runbook = Get-Content -Raw -Encoding UTF8 -LiteralPath $runbookPath
foreach ($record in @($pack.runbooks)) {
    if ($record.synthetic_drill -ne "passed" -or
        -not $runbook.Contains("## $($record.section)")) {
        throw "runbook evidence is missing or unqualified: $($record.runbook_id)"
    }
}

$alertCases = @($alerts.cases)
foreach ($definition in @($pack.sli_definitions)) {
    $metricCases = @($alertCases | Where-Object { $_.metric_id -eq $definition.metric_id })
    $rolloutMetric = @(
        $rolloutGates.success_metrics |
            Where-Object { $_.metric_id -eq $definition.metric_id }
    )
    if (@($metricCases | Where-Object { $_.expected -eq "healthy" }).Count -eq 0 -or
        @($metricCases | Where-Object { $_.expected -eq "critical" }).Count -eq 0 -or
        $rolloutMetric.Count -ne 1) {
        throw "alert threshold fixture lacks healthy and critical cases: $($definition.metric_id)"
    }
    foreach ($label in @($definition.allowed_labels)) {
        if ($label -in @("run_id", "session_id", "trace_id", "user_id", "principal", "server_id", "workspace_path", "error_message")) {
            throw "high-cardinality SLI label is forbidden: $label"
        }
    }
}

$stableGate = @(
    $rolloutGates.gates |
        Where-Object { $_.feature_id -eq "stable_core_runtime" }
)
if ($stableGate.Count -ne 1 -or
    $stableGate[0].release_stage -ne "stable" -or
    @($stableGate[0].entry_criteria).Count -eq 0 -or
    @($stableGate[0].exit_criteria).Count -eq 0 -or
    @($stableGate[0].promotion_blockers).Count -eq 0) {
    throw "stable core rollout gate is missing or incomplete"
}

$drillByRunbook = @{}
foreach ($incident in @($drill.incidents)) {
    if ($incident.result -ne "passed" -or
        $incident.contains_raw_secret -or
        -not $incident.rollback_preserves_durable_data -or
        @($incident.evidence_fields).Count -eq 0) {
        throw "synthetic incident drill failed: $($incident.incident_id)"
    }
    $drillByRunbook[$incident.runbook_id] = $true
}
foreach ($record in @($pack.runbooks)) {
    if (-not $drillByRunbook.ContainsKey($record.runbook_id)) {
        throw "synthetic incident drill is missing runbook: $($record.runbook_id)"
    }
}

foreach ($field in @("credential", "prompt", "tool_payload", "workspace_path")) {
    if ($drill.support_bundle_sample.$field -ne "<redacted>") {
        throw "support bundle fixture contains unredacted field: $field"
    }
}
$serializedSample = $drill.support_bundle_sample | ConvertTo-Json -Depth 100 -Compress
foreach ($forbidden in @("vault://", "sk-", "C:\\", "/home/")) {
    if ($serializedSample.Contains($forbidden)) {
        throw "support bundle fixture contains forbidden raw data marker: $forbidden"
    }
}

Write-Host "release dashboard and stable core evidence ok: $ReportPath"
