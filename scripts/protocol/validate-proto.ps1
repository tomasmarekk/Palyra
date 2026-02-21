[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

$rootDir = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$protoDir = Join-Path $rootDir "schemas\proto"

$protoc = Get-Command protoc.exe -ErrorAction SilentlyContinue
if (-not $protoc) {
    $protoc = Get-Command protoc -ErrorAction SilentlyContinue
}
if (-not $protoc) {
    throw "protoc is required to validate protocol schemas."
}

$protoFiles = Get-ChildItem -Path $protoDir -Recurse -File -Filter "*.proto" | Sort-Object FullName
if ($protoFiles.Count -eq 0) {
    throw "No .proto files found under $protoDir"
}

$tmpDir = Join-Path ([System.IO.Path]::GetTempPath()) ("palyra-proto-validate-" + [Guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $tmpDir -Force | Out-Null

try {
    $descriptorPath = Join-Path $tmpDir "palyra-protocol.pb"
    $args = @(
        "-I", $protoDir,
        "--include_imports",
        "--descriptor_set_out", $descriptorPath
    ) + ($protoFiles | ForEach-Object { $_.FullName })
    & $protoc.Source @args
    if ($LASTEXITCODE -ne 0) {
        throw "protoc schema validation failed."
    }
} finally {
    Remove-Item -Path $tmpDir -Recurse -Force -ErrorAction SilentlyContinue
}

Write-Output "Protocol schema validation passed ($($protoFiles.Count) files)."
