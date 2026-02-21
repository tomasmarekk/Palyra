[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

$rootDir = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$swiftStub = Join-Path $rootDir "schemas\generated\swift\ProtocolStubs.swift"

$swiftc = Get-Command swiftc -ErrorAction SilentlyContinue
if (-not $swiftc) {
    throw "swiftc is required to validate generated Swift stubs."
}

if (-not (Test-Path -Path $swiftStub -PathType Leaf)) {
    throw "Missing generated Swift stub file: $swiftStub"
}

$tmpDir = Join-Path ([System.IO.Path]::GetTempPath()) ("palyra-swift-stubs-" + [Guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $tmpDir -Force | Out-Null

try {
    $modulePath = Join-Path $tmpDir "PalyraProtocolStubs.swiftmodule"
    & $swiftc.Source -emit-module -module-name PalyraProtocolStubs $swiftStub -o $modulePath
    if ($LASTEXITCODE -ne 0) {
        throw "Swift protocol stubs compile validation failed."
    }
} finally {
    Remove-Item -Path $tmpDir -Recurse -Force -ErrorAction SilentlyContinue
}

Write-Output "Swift protocol stubs compile validation passed."
