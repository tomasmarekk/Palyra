Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$cargo = if ($env:PALYRA_DEPLOYMENT_SMOKE_CARGO_BIN) {
    $env:PALYRA_DEPLOYMENT_SMOKE_CARGO_BIN
} elseif (Get-Command cargo -ErrorAction SilentlyContinue) {
    (Get-Command cargo).Source
} elseif (Get-Command cargo.exe -ErrorAction SilentlyContinue) {
    (Get-Command cargo.exe).Source
} else {
    throw "cargo is required for deployment smoke checks."
}

$smokeRoot = if ($env:PALYRA_DEPLOYMENT_SMOKE_DIR) {
    $env:PALYRA_DEPLOYMENT_SMOKE_DIR
} else {
    Join-Path ([IO.Path]::GetTempPath()) ("palyra-deployment-smoke-" + [Guid]::NewGuid().ToString("N"))
}
$cleanupSmokeRoot = [string]::IsNullOrWhiteSpace($env:PALYRA_DEPLOYMENT_SMOKE_DIR)

function Invoke-PalyraCli {
    param(
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$Arguments
    )

    & $cargo run -p palyra-cli --locked -- @Arguments
}

Push-Location $repoRoot
try {
    $env:PALYRA_STATE_ROOT = Join-Path $smokeRoot "state"
    Remove-Item Env:PALYRA_CONFIG -ErrorAction SilentlyContinue
    New-Item -ItemType Directory -Path $env:PALYRA_STATE_ROOT -Force | Out-Null
    New-Item -ItemType Directory -Path (Join-Path $smokeRoot "configs") -Force | Out-Null
    New-Item -ItemType Directory -Path (Join-Path $smokeRoot "recipes") -Force | Out-Null
    New-Item -ItemType Directory -Path (Join-Path $smokeRoot "reports") -Force | Out-Null

    Invoke-PalyraCli deployment profiles --json |
        Set-Content -LiteralPath (Join-Path $smokeRoot "reports/profiles.json")

    foreach ($profile in @("local", "single-vm", "worker-enabled")) {
        $mode = if ($profile -eq "local") { "local" } else { "remote" }
        $configPath = Join-Path $smokeRoot "configs/$profile.toml"
        $recipeDir = Join-Path $smokeRoot "recipes/$profile"

        Invoke-PalyraCli setup --mode $mode --deployment-profile $profile --path $configPath --force --tls-scaffold none |
            Set-Content -LiteralPath (Join-Path $smokeRoot "reports/setup-$profile.txt")
        Invoke-PalyraCli config validate --path $configPath |
            Set-Content -LiteralPath (Join-Path $smokeRoot "reports/validate-$profile.txt")
        Invoke-PalyraCli deployment preflight --deployment-profile $profile --path $configPath --json |
            Set-Content -LiteralPath (Join-Path $smokeRoot "reports/preflight-$profile.json")
        Invoke-PalyraCli deployment manifest --deployment-profile $profile --output (Join-Path $smokeRoot "reports/manifest-$profile.json") |
            Set-Content -LiteralPath (Join-Path $smokeRoot "reports/manifest-$profile.txt")
        if ($profile -ne "local") {
            Invoke-PalyraCli deployment recipe --deployment-profile $profile --output-dir $recipeDir |
                Set-Content -LiteralPath (Join-Path $smokeRoot "reports/recipe-$profile.txt")

            foreach ($required in @("profile-manifest.json", "env/palyra.env.example", "docker/Dockerfile.palyra")) {
                $path = Join-Path $recipeDir $required
                if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
                    throw "Missing deployment recipe file: $path"
                }
            }
        }
    }

    foreach ($required in @(
            "recipes/single-vm/compose/single-vm.yml",
            "recipes/worker-enabled/compose/worker-enabled.yml",
            "recipes/worker-enabled/systemd/palyra-workerd.service"
        )) {
        $path = Join-Path $smokeRoot $required
        if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
            throw "Missing deployment smoke artifact: $path"
        }
    }

    $workerConfig = Join-Path $smokeRoot "configs/worker-enabled.toml"
    Invoke-PalyraCli deployment upgrade-smoke --deployment-profile worker-enabled --path $workerConfig --json |
        Set-Content -LiteralPath (Join-Path $smokeRoot "reports/upgrade-smoke-worker-enabled.json")
    Invoke-PalyraCli deployment promotion-check --deployment-profile worker-enabled --json |
        Set-Content -LiteralPath (Join-Path $smokeRoot "reports/promotion-worker-enabled.json")
    Invoke-PalyraCli deployment rollback-plan --deployment-profile worker-enabled --output (Join-Path $smokeRoot "reports/rollback-worker-enabled.json") |
        Set-Content -LiteralPath (Join-Path $smokeRoot "reports/rollback-worker-enabled.txt")

    Write-Output "deployment_smoke=passed"
    Write-Output "smoke_root=$smokeRoot"
}
finally {
    Pop-Location
    if ($cleanupSmokeRoot -and (Test-Path -LiteralPath $smokeRoot)) {
        $resolvedSmokeRoot = (Resolve-Path -LiteralPath $smokeRoot).Path
        $tempRoot = [IO.Path]::GetTempPath()
        if (-not $resolvedSmokeRoot.StartsWith($tempRoot, [StringComparison]::OrdinalIgnoreCase)) {
            throw "Refusing to remove deployment smoke root outside temp: $resolvedSmokeRoot"
        }
        Remove-Item -LiteralPath $resolvedSmokeRoot -Recurse -Force
    }
}
