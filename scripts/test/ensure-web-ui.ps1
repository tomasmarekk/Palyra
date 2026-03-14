[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$rootDir = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $rootDir

if (-not (Test-Path (Join-Path $rootDir "apps\web\node_modules"))) {
    npm --prefix apps/web run bootstrap
} else {
    npm --prefix apps/web run verify-install
}

npm --prefix apps/web run build
