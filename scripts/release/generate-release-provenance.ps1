param(
    [Parameter(Mandatory = $true)]
    [string]$Version,
    [Parameter(Mandatory = $true)]
    [string[]]$ArtifactPaths,
    [Parameter(Mandatory = $true)]
    [string]$OutputPath
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. "$PSScriptRoot/common.ps1"

$repoRoot = Get-RepoRoot
$resolvedArtifacts = $ArtifactPaths | ForEach-Object { Assert-FileExists -Path $_ -Label "Provenance subject" }

$gitSha = (& git -C $repoRoot rev-parse HEAD).Trim()
$gitRef = (& git -C $repoRoot symbolic-ref -q HEAD) 2>$null
if ([string]::IsNullOrWhiteSpace($gitRef)) {
    $gitRef = (& git -C $repoRoot describe --tags --exact-match) 2>$null
}
if ([string]::IsNullOrWhiteSpace($gitRef)) {
    $gitRef = "detached"
}

$artifacts = foreach ($artifactPath in $resolvedArtifacts) {
    $item = Get-Item -LiteralPath $artifactPath
    [ordered]@{
        name = $item.Name
        sha256 = Get-Sha256Hex -Path $artifactPath
        size_bytes = $item.Length
    }
}

$provenance = [ordered]@{
    schema_version = 1
    generated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    version = $Version
    repository = [ordered]@{
        git_sha = $gitSha
        git_ref = $gitRef
    }
    github = [ordered]@{
        repository = $env:GITHUB_REPOSITORY
        workflow = $env:GITHUB_WORKFLOW
        run_id = $env:GITHUB_RUN_ID
        run_attempt = $env:GITHUB_RUN_ATTEMPT
        actor = $env:GITHUB_ACTOR
        sha = $env:GITHUB_SHA
        ref = $env:GITHUB_REF
    }
    artifacts = @($artifacts)
}

$outputParent = Split-Path -Parent $OutputPath
if ($outputParent) {
    New-Item -ItemType Directory -Path $outputParent -Force | Out-Null
}
$provenance | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $OutputPath

Write-Output "provenance_path=$OutputPath"
