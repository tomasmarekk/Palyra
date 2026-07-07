param()

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Push-Location $repoRoot
try {
    $patterns = @(
        "direct_journal_authority_granted\s*:\s*true",
        "approval_authority_granted\s*:\s*true",
        "tool_executor_authority_granted\s*:\s*true",
        "journal\.write\.direct.*allowed\s*:\s*true"
    )
    $paths = @("crates/palyra-daemon/src", "crates/palyra-common/src")
    foreach ($pattern in $patterns) {
        & rg -n --glob "*.rs" $pattern @paths
        if ($LASTEXITCODE -eq 0) {
            throw "host authority bypass pattern matched: $pattern"
        }
        if ($LASTEXITCODE -gt 1) {
            throw "rg failed while checking pattern: $pattern"
        }
    }

    cargo test -p palyra-common host_authority_checklist_denies_direct_runtime_authority --locked
} finally {
    Pop-Location
}
