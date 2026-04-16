Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")

function Invoke-SmokeStep {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Label,
        [Parameter(Mandatory = $true)]
        [string]$Command,
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments
    )

    Write-Host ("==> " + $Label) -ForegroundColor Cyan
    & $Command @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Surface release smoke step failed: $Label"
    }
}

Push-Location $repoRoot
try {
    Invoke-SmokeStep `
        -Label "control-plane mobile bootstrap contract" `
        -Command "cargo" `
        -Arguments @(
            "test",
            "-p",
            "palyra-control-plane",
            "tests::mobile_bootstrap_envelope_round_trips",
            "--",
            "--exact"
        )

    Invoke-SmokeStep `
        -Label "daemon mobile session and safe-url seams" `
        -Command "cargo" `
        -Arguments @(
            "test",
            "-p",
            "palyra-daemon",
            "--test",
            "admin_surface",
            "console_mobile_endpoints_require_session_and_surface_cross_device_sessions",
            "--",
            "--exact"
        )

    Invoke-SmokeStep `
        -Label "daemon mobile voice note and approval decision seams" `
        -Command "cargo" `
        -Arguments @(
            "test",
            "-p",
            "palyra-daemon",
            "--test",
            "admin_surface",
            "console_mobile_voice_note_and_approval_endpoints_enforce_csrf_and_queue_into_existing_session",
            "--",
            "--exact"
        )

    Invoke-SmokeStep `
        -Label "TUI localization regression" `
        -Command "cargo" `
        -Arguments @("test", "-p", "palyra-cli", "tui::text", "--", "--nocapture")

    Invoke-SmokeStep `
        -Label "web localization and contract seams" `
        -Command "npm" `
        -Arguments @(
            "--prefix",
            "apps/web",
            "run",
            "test:run",
            "--",
            "src/console/i18n.test.ts",
            "src/console/ConsoleShell.snapshot.test.tsx",
            "src/consoleApi.test.ts"
        )

    Invoke-SmokeStep `
        -Label "web typecheck" `
        -Command "npm" `
        -Arguments @("--prefix", "apps/web", "run", "typecheck")

    Invoke-SmokeStep `
        -Label "desktop UI typecheck" `
        -Command "npm" `
        -Arguments @("--prefix", "apps/desktop/ui", "run", "typecheck")

    Write-Output "surface_release_smoke=passed"
}
finally {
    Pop-Location
}
