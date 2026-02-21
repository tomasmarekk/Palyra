[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

$rootDir = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$kotlinStub = Join-Path $rootDir "schemas\generated\kotlin\ProtocolStubs.kt"

$kotlinc = Get-Command kotlinc -ErrorAction SilentlyContinue
if (-not $kotlinc) {
    $kotlinc = Get-Command kotlinc.bat -ErrorAction SilentlyContinue
}
if (-not $kotlinc) {
    throw "kotlinc is required to validate generated Kotlin stubs."
}

if (-not (Test-Path -Path $kotlinStub -PathType Leaf)) {
    throw "Missing generated Kotlin stub file: $kotlinStub"
}

$tmpDir = Join-Path ([System.IO.Path]::GetTempPath()) ("palyra-kotlin-stubs-" + [Guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $tmpDir -Force | Out-Null

try {
    $jarPath = Join-Path $tmpDir "palyra-protocol-stubs.jar"
    & $kotlinc.Source $kotlinStub -d $jarPath
    if ($LASTEXITCODE -ne 0) {
        throw "Kotlin protocol stubs compile validation failed."
    }
} finally {
    Remove-Item -Path $tmpDir -Recurse -Force -ErrorAction SilentlyContinue
}

Write-Output "Kotlin protocol stubs compile validation passed."
