[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

& (Join-Path $PSScriptRoot "run-deterministic-fault-smoke.ps1")
