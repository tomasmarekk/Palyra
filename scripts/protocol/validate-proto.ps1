[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

$rootDir = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$protoDir = Join-Path $rootDir "schemas\proto"

function Resolve-ProtocPath {
    $command = Get-Command protoc.exe -ErrorAction SilentlyContinue
    if (-not $command) {
        $command = Get-Command protoc -ErrorAction SilentlyContinue
    }
    if ($command) {
        return $command.Source
    }

    if ($env:PROTOC -and (Test-Path $env:PROTOC)) {
        return (Resolve-Path $env:PROTOC).Path
    }

    $candidates = @(
        (if ($env:ChocolateyInstall) { Join-Path $env:ChocolateyInstall "bin\protoc.exe" } else { $null }),
        (if ($env:ChocolateyInstall) {
            Join-Path $env:ChocolateyInstall "lib\protoc\tools\bin\protoc.exe"
        } else {
            $null
        }),
        (if ($env:ProgramData) { Join-Path $env:ProgramData "chocolatey\bin\protoc.exe" } else { $null }),
        (if ($env:ProgramData) {
            Join-Path $env:ProgramData "chocolatey\lib\protoc\tools\bin\protoc.exe"
        } else {
            $null
        })
    )
    foreach ($candidate in $candidates) {
        if ([string]::IsNullOrWhiteSpace($candidate)) {
            continue
        }
        if (Test-Path $candidate) {
            return (Resolve-Path $candidate).Path
        }
    }

    return $null
}

$protocPath = Resolve-ProtocPath
if (-not $protocPath) {
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
    & $protocPath @args
    if ($LASTEXITCODE -ne 0) {
        throw "protoc schema validation failed."
    }
} finally {
    Remove-Item -Path $tmpDir -Recurse -Force -ErrorAction SilentlyContinue
}

Write-Output "Protocol schema validation passed ($($protoFiles.Count) files)."
