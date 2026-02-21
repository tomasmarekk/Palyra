[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

$rootDir = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$rustStub = Join-Path $rootDir "schemas\generated\rust\protocol_stubs.rs"

$rustc = Get-Command rustc.exe -ErrorAction SilentlyContinue
if (-not $rustc) {
    $rustc = Get-Command rustc -ErrorAction SilentlyContinue
}
if (-not $rustc) {
    throw "rustc is required to validate generated Rust stubs."
}

if (-not (Test-Path -Path $rustStub -PathType Leaf)) {
    throw "Missing generated Rust stub file: $rustStub"
}

$tmpDir = Join-Path ([System.IO.Path]::GetTempPath()) ("palyra-rust-stubs-" + [Guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $tmpDir -Force | Out-Null

try {
    $outPath = Join-Path $tmpDir "libpalyra_protocol_stubs.rlib"
    & $rustc.Source --edition=2021 --crate-name palyra_protocol_stubs --crate-type lib $rustStub -o $outPath
    if ($LASTEXITCODE -ne 0) {
        throw "Rust protocol stubs compile validation failed."
    }
} finally {
    Remove-Item -Path $tmpDir -Recurse -Force -ErrorAction SilentlyContinue
}

Write-Output "Rust protocol stubs compile validation passed."
