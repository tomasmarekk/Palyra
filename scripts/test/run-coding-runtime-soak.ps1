[CmdletBinding()]
param(
    [ValidateRange(1, 64)]
    [int]$Iterations = 12,
    [string]$ReportPath = "target/qa-lab/coding-runtime/coding-runtime-soak.json"
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$rootDir = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $rootDir

$report = [System.IO.Path]::GetFullPath((Join-Path $rootDir $ReportPath))
$schemaPath = Join-Path $rootDir "schemas/json/common/coding-runtime-soak-report.v1.json"
$baselinePath = Join-Path $rootDir "qa/baselines/coding-runtime-warm-lsp.v1.json"
$env:PALYRA_CODING_RUNTIME_SOAK_ITERATIONS = $Iterations.ToString(
    [System.Globalization.CultureInfo]::InvariantCulture
)
$env:PALYRA_CODING_RUNTIME_SOAK_REPORT = $report

cargo test -p palyra-daemon --test coding_runtime --locked -j 1 `
    warm_lsp_repeated_diagnostics_soak_is_bounded_and_leaves_no_services `
    -- --exact --nocapture

if (-not (Test-Path -LiteralPath $report -PathType Leaf)) {
    throw "Coding runtime soak report was not created at $report"
}

$evidence = Get-Content -LiteralPath $report -Raw | ConvertFrom-Json
$schema = Get-Content -LiteralPath $schemaPath -Raw | ConvertFrom-Json
$baseline = Get-Content -LiteralPath $baselinePath -Raw | ConvertFrom-Json
$actualKeys = @($evidence.PSObject.Properties.Name | Sort-Object)
$requiredKeys = @($schema.required | Sort-Object)
if (
    $schema.additionalProperties -ne $false -or
    (Compare-Object -ReferenceObject $requiredKeys -DifferenceObject $actualKeys)
) {
    throw "Coding runtime soak report does not match the closed schema shape."
}

foreach ($property in $schema.properties.PSObject.Properties) {
    $value = $evidence.($property.Name)
    $rule = $property.Value
    if ($rule.type -eq "integer") {
        try {
            $integerValue = [decimal]$value
        } catch {
            throw "Coding runtime soak field $($property.Name) must be an integer."
        }
        if (
            $value -is [bool] -or
            $value -is [string] -or
            [decimal]::Truncate($integerValue) -ne $integerValue
        ) {
            throw "Coding runtime soak field $($property.Name) must be an integer."
        }
    }
    if ($rule.PSObject.Properties.Name -contains "const" -and $value -ne $rule.const) {
        throw "Coding runtime soak field $($property.Name) violates its constant contract."
    }
    if (
        $rule.PSObject.Properties.Name -contains "minimum" -and
        [decimal]$value -lt [decimal]$rule.minimum
    ) {
        throw "Coding runtime soak field $($property.Name) is below its minimum."
    }
    if (
        $rule.PSObject.Properties.Name -contains "maximum" -and
        [decimal]$value -gt [decimal]$rule.maximum
    ) {
        throw "Coding runtime soak field $($property.Name) exceeds its maximum."
    }
}

if (
    $baseline.supported_runner_os -notcontains "windows" -or
    $evidence.iterations -ne $Iterations -or
    ($env:CI -and $evidence.iterations -ne $baseline.ci_iterations) -or
    $evidence.iterations -gt $baseline.max_iterations -or
    $evidence.patch_observations -ne ($evidence.iterations * 2) -or
    $evidence.introduced_total -ne $evidence.iterations -or
    $evidence.resolved_total -ne $evidence.iterations -or
    $evidence.patch_latency_p95_ms -gt $baseline.max_patch_latency_p95_ms -or
    $evidence.cleanup_active_process_count -ne $baseline.required_cleanup_active_process_count -or
    $evidence.cleanup_lsp_settled -ne $baseline.required_cleanup_lsp_settled -or
    $evidence.remaining_resource_leases -ne $baseline.required_remaining_resource_leases
) {
    throw "Coding runtime soak report failed its performance or cleanup baseline."
}

Write-Host "Coding runtime soak evidence: $report"
