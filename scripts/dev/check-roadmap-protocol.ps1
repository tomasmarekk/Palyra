param(
    [string] $RoadmapDir = "roadmap/new_roadmap"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$roadmapPath = Join-Path $repoRoot $RoadmapDir
$summaryPath = Join-Path $roadmapPath "summary.md"

if (-not (Test-Path $summaryPath)) {
    throw "roadmap summary not found: $summaryPath"
}

$summary = Get-Content -Raw -Encoding UTF8 $summaryPath
$summaryMatches = [regex]::Matches(
    $summary,
    "\[M(?<number>\d{3}) [^\]]+\]\((?<link>milestones/(?<file>\d{3}_[a-z0-9_]+\.md))\)"
)
if ($summaryMatches.Count -eq 0) {
    throw "summary.md does not contain milestone links"
}

$seenNumbers = @{}
$seenLinks = @{}
$minimumSectionCount = 7

foreach ($match in $summaryMatches) {
    $number = $match.Groups["number"].Value
    $link = $match.Groups["link"].Value
    $file = $match.Groups["file"].Value

    if ($seenNumbers.ContainsKey($number)) {
        throw "duplicate milestone number in summary.md: M$number"
    }
    if ($seenLinks.ContainsKey($link)) {
        throw "duplicate milestone link in summary.md: $link"
    }
    $seenNumbers[$number] = $true
    $seenLinks[$link] = $true

    if (-not $file.StartsWith("${number}_")) {
        throw "milestone file does not start with its number: $file"
    }
    $detailPath = Join-Path $roadmapPath $link
    if (-not (Test-Path $detailPath)) {
        throw "milestone detail missing: $link"
    }
    $detail = Get-Content -Raw -Encoding UTF8 $detailPath
    if ($detail -notmatch "(?m)^# M$number\b") {
        throw "milestone detail heading does not match M${number}: $link"
    }
    $sectionMatches = [regex]::Matches($detail, "(?m)^##\s+\S")
    if ($sectionMatches.Count -lt $minimumSectionCount) {
        throw "milestone $link has too few detail sections: $($sectionMatches.Count)"
    }
}

Write-Host "roadmap protocol ok: $($summaryMatches.Count) milestones"
