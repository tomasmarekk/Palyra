param(
    [string]$WorkspaceRoot,
    [switch]$SkipBuild,
    [switch]$Launch,
    [switch]$NoLaunch
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$ProgressPreference = "SilentlyContinue"
$InformationPreference = "SilentlyContinue"

. (Join-Path $PSScriptRoot "../release/common.ps1")

if ($Launch -and $NoLaunch) {
    throw "Pass either -Launch or -NoLaunch, not both."
}

function Get-DefaultHarnessRoot {
    $localAppData = [Environment]::GetFolderPath("LocalApplicationData")
    if ([string]::IsNullOrWhiteSpace($localAppData)) {
        throw "Unable to resolve LocalApplicationData for the clean desktop test harness."
    }

    return Join-Path $localAppData "Palyra-TestHarness"
}

function Resolve-WindowsCleanDesktopCliExposure {
    param(
        [Parameter(Mandatory = $true)]
        [string]$FallbackCommandRoot,
        [Parameter(Mandatory = $true)]
        [string]$FallbackCommandPath
    )

    foreach ($aliasRoot in (Get-WindowsPalyraCliAliasRoots)) {
        if ([string]::IsNullOrWhiteSpace($aliasRoot)) {
            continue
        }

        $aliasCommandPath = Join-Path $aliasRoot "palyra.cmd"
        if (Test-Path -LiteralPath $aliasCommandPath -PathType Leaf) {
            return [ordered]@{
                command_root = [IO.Path]::GetFullPath($aliasRoot)
                command_path = [IO.Path]::GetFullPath($aliasCommandPath)
            }
        }
    }

    return [ordered]@{
        command_root = [IO.Path]::GetFullPath($FallbackCommandRoot)
        command_path = [IO.Path]::GetFullPath($FallbackCommandPath)
    }
}

function Set-TextFileUtf8NoBom {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        [string]$Value
    )

    $parent = Split-Path -Parent $Path
    if (-not [string]::IsNullOrWhiteSpace($parent)) {
        New-Item -ItemType Directory -Path $parent -Force | Out-Null
    }
    [IO.File]::WriteAllText($Path, $Value, [Text.UTF8Encoding]::new($false))
}

function Get-GeneratedTomlScalar {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        [string]$Section,
        [Parameter(Mandatory = $true)]
        [string]$Key
    )

    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "Generated config file is missing: $Path"
    }

    $currentSection = ""
    $keyPattern = [regex]::Escape($Key)
    foreach ($line in Get-Content -LiteralPath $Path) {
        $trimmed = $line.Trim()
        if ($trimmed -match '^\[(?<section>[^\]]+)\]$') {
            $currentSection = $Matches.section.Trim()
            continue
        }
        if ($currentSection -ne $Section) {
            continue
        }
        if ($trimmed -match "^$keyPattern\s*=\s*(?<value>.+)$") {
            $raw = $Matches.value.Trim()
            if ($raw.StartsWith('"') -and $raw.EndsWith('"') -and $raw.Length -ge 2) {
                return $raw.Substring(1, $raw.Length - 2)
            }
            return $raw
        }
    }

    throw "Generated config file $Path does not contain [$Section].$Key"
}

function New-E2EHarnessInventorySkillFixture {
    param(
        [Parameter(Mandatory = $true)]
        [string]$FixtureRoot
    )

    New-Item -ItemType Directory -Path $FixtureRoot -Force | Out-Null
    $manifestPath = Join-Path $FixtureRoot "skill.toml"
    $modulePath = Join-Path $FixtureRoot "inventory_lookup.wasm"
    $sbomPath = Join-Path $FixtureRoot "sbom.cdx.json"
    $provenancePath = Join-Path $FixtureRoot "provenance.json"

    Set-TextFileUtf8NoBom -Path $manifestPath -Value @'
manifest_version = 1
skill_id = "inventory.lookup"
name = "Inventory Lookup Schema Fixture"
version = "1.0.0"
publisher = "inventory"

[entrypoints]
[[entrypoints.tools]]
id = "inventory.lookup"
name = "lookup"
description = "Returns a deterministic malformed inventory payload for E2E schema-validation fallback coverage."
input_schema = { type = "object", properties = { sku = { type = "string" } }, required = ["sku"] }
output_schema = { type = "object", properties = { sku = { type = "string" }, quantity = { type = "integer" }, source = { type = "string" } }, required = ["sku", "quantity", "source"] }
risk = { default_sensitive = false, requires_approval = false }

[capabilities.filesystem]
read_roots = []
write_roots = []

[capabilities]
http_egress_allowlist = []
device_capabilities = []
node_capabilities = []

[capabilities.quotas]
wall_clock_timeout_ms = 2000
fuel_budget = 1000000
max_memory_bytes = 16777216

[compat]
required_protocol_major = 1
min_palyra_version = "0.1.0"
'@

    Set-TextFileUtf8NoBom -Path $modulePath -Value @'
(module
  (func (export "run") (result i32)
    i32.const 42
  )
)
'@

    Set-TextFileUtf8NoBom -Path $sbomPath -Value @'
{
  "bomFormat": "CycloneDX",
  "specVersion": "1.6",
  "version": 1,
  "metadata": {
    "component": {
      "name": "inventory.lookup",
      "type": "application",
      "version": "1.0.0"
    }
  },
  "components": []
}
'@

    Set-TextFileUtf8NoBom -Path $provenancePath -Value @'
{
  "builder": {
    "id": "clean-desktop-e2e-harness"
  },
  "subject": [
    {
      "name": "inventory_lookup.wasm",
      "digest": {
        "sha256": "generated-at-package-build"
      }
    }
  ]
}
'@

    return [ordered]@{
        manifest_path = $manifestPath
        module_path = $modulePath
        sbom_path = $sbomPath
        provenance_path = $provenancePath
    }
}

function Install-E2EHarnessInventorySkill {
    param(
        [Parameter(Mandatory = $true)]
        [string]$WorkspaceRoot,
        [Parameter(Mandatory = $true)]
        [string]$StateRoot,
        [Parameter(Mandatory = $true)]
        [string]$CliPath
    )

    if (-not (Test-Path -LiteralPath $CliPath -PathType Leaf)) {
        throw "Installed Palyra CLI binary is missing: $CliPath"
    }

    $fixtureRoot = Join-Path $WorkspaceRoot "fixtures/skills/inventory-lookup"
    $distRoot = Join-Path $WorkspaceRoot "artifacts/skills"
    $artifactPath = Join-Path $distRoot "inventory.lookup.palyra-skill"
    $skillsRoot = Join-Path $StateRoot "skills"
    $trustStorePath = Join-Path $skillsRoot "trust-store.json"
    New-Item -ItemType Directory -Path $distRoot -Force | Out-Null
    New-Item -ItemType Directory -Path $skillsRoot -Force | Out-Null

    $fixture = New-E2EHarnessInventorySkillFixture -FixtureRoot $fixtureRoot
    if (Test-Path -LiteralPath $artifactPath -PathType Leaf) {
        Remove-Item -LiteralPath $artifactPath -Force
    }

    "0101010101010101010101010101010101010101010101010101010101010101" |
        & $CliPath skills package build `
            --manifest $fixture.manifest_path `
            --module $fixture.module_path `
            --sbom $fixture.sbom_path `
            --provenance $fixture.provenance_path `
            --output $artifactPath `
            --signing-key-stdin `
            --json | Out-Null

    & $CliPath skills install `
        --artifact $artifactPath `
        --skills-dir $skillsRoot `
        --trust-store $trustStorePath `
        --allow-untrusted `
        --non-interactive `
        --json | Out-Null

    return [ordered]@{
        installed = "true"
        skill_id = "inventory.lookup"
        version = "1.0.0"
        artifact_path = [IO.Path]::GetFullPath($artifactPath)
        skills_root = [IO.Path]::GetFullPath($skillsRoot)
        trust_store = [IO.Path]::GetFullPath($trustStorePath)
    }
}

function Wait-E2EHarnessGatewayHealth {
    param(
        [Parameter(Mandatory = $true)]
        [string]$DaemonUrl
    )

    $healthUrl = "$($DaemonUrl.TrimEnd('/'))/healthz"
    $deadline = (Get-Date).AddSeconds(90)
    $lastError = $null
    do {
        try {
            $response = Invoke-RestMethod -Uri $healthUrl -TimeoutSec 2 -ErrorAction Stop
            if ($null -eq $response -or $response.status -eq "ok") {
                return
            }
            $lastError = "health status was '$($response.status)'"
        } catch {
            $lastError = $_.Exception.Message
        }
        Start-Sleep -Milliseconds 1000
    } while ((Get-Date) -lt $deadline)

    throw "Timed out waiting for Palyra gateway health at $healthUrl. Last error: $lastError"
}

function Enable-E2EHarnessInventorySkill {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ConfigPath,
        [Parameter(Mandatory = $true)]
        [string]$SkillsRoot,
        [Parameter(Mandatory = $true)]
        [string]$CliPath
    )

    $daemonPort = [int](Get-GeneratedTomlScalar -Path $ConfigPath -Section "daemon" -Key "port")
    $adminToken = Get-GeneratedTomlScalar -Path $ConfigPath -Section "admin" -Key "auth_token"
    $daemonUrl = "http://127.0.0.1:$daemonPort"
    Wait-E2EHarnessGatewayHealth -DaemonUrl $daemonUrl

    & $CliPath skills enable inventory.lookup `
        --version 1.0.0 `
        --skills-dir $SkillsRoot `
        --override `
        --reason "clean_desktop_e2e_inventory_fixture" `
        --url $daemonUrl `
        --token $adminToken `
        --principal "admin:local" `
        --json | Out-Null

    return [ordered]@{
        activated = "true"
        daemon_url = $daemonUrl
    }
}

$repoRoot = Get-RepoRoot
$workspaceRoot =
    if ([string]::IsNullOrWhiteSpace($WorkspaceRoot)) {
        Get-DefaultHarnessRoot
    } else {
        [IO.Path]::GetFullPath($WorkspaceRoot)
    }

$artifactsRoot = Join-Path $workspaceRoot "artifacts"
$desktopPackageOutput = Join-Path $artifactsRoot "desktop"
$cargoTargetRoot = Join-Path $artifactsRoot "cargo-target"
$installRoot = Join-Path $workspaceRoot "install"
$stateRoot = Join-Path $workspaceRoot "state"
$osFileRoot = Join-Path $workspaceRoot "home"
$cliCommandRoot = Join-Path $workspaceRoot "cli-bin"
$desktopExecutable = Resolve-ExecutableName -BaseName "palyra-desktop-control-center"
$daemonExecutable = Resolve-ExecutableName -BaseName "palyrad"
$browserExecutable = Resolve-ExecutableName -BaseName "palyra-browserd"
$cliExecutable = Resolve-ExecutableName -BaseName "palyra"

New-Item -ItemType Directory -Path $workspaceRoot -Force | Out-Null
New-Item -ItemType Directory -Path $osFileRoot -Force | Out-Null

if (-not $SkipBuild) {
    Push-Location $repoRoot
    $previousCargoTargetDir = $env:CARGO_TARGET_DIR
    try {
        New-Item -ItemType Directory -Path $cargoTargetRoot -Force | Out-Null
        $env:CARGO_TARGET_DIR = $cargoTargetRoot
        & (Join-Path $repoRoot "scripts/test/ensure-desktop-ui.ps1")
        & (Join-Path $repoRoot "scripts/test/ensure-web-ui.ps1")
        cargo build -p palyra-daemon -p palyra-browserd -p palyra-cli --release --locked
        cargo build --manifest-path apps/desktop/src-tauri/Cargo.toml --release --locked
    }
    finally {
        if ($null -eq $previousCargoTargetDir) {
            Remove-Item Env:CARGO_TARGET_DIR -ErrorAction SilentlyContinue
        } else {
            $env:CARGO_TARGET_DIR = $previousCargoTargetDir
        }
        Pop-Location
    }
}

$version = & (Join-Path $repoRoot "scripts/release/assert-version-coherence.ps1")
$platform = Get-PlatformSlug
$isolatedDesktopBinary = Join-Path $cargoTargetRoot ("release/" + $desktopExecutable)
$isolatedDaemonBinary = Join-Path $cargoTargetRoot ("release/" + $daemonExecutable)
$isolatedBrowserBinary = Join-Path $cargoTargetRoot ("release/" + $browserExecutable)
$isolatedCliBinary = Join-Path $cargoTargetRoot ("release/" + $cliExecutable)
$desktopBinary =
    if (Test-Path -LiteralPath $isolatedDesktopBinary -PathType Leaf) {
        $isolatedDesktopBinary
    } else {
        Join-Path $repoRoot ("apps/desktop/src-tauri/target/release/" + $desktopExecutable)
    }
$daemonBinary =
    if (Test-Path -LiteralPath $isolatedDaemonBinary -PathType Leaf) {
        $isolatedDaemonBinary
    } else {
        Join-Path $repoRoot ("target/release/" + $daemonExecutable)
    }
$browserBinary =
    if (Test-Path -LiteralPath $isolatedBrowserBinary -PathType Leaf) {
        $isolatedBrowserBinary
    } else {
        Join-Path $repoRoot ("target/release/" + $browserExecutable)
    }
$cliBinary =
    if (Test-Path -LiteralPath $isolatedCliBinary -PathType Leaf) {
        $isolatedCliBinary
    } else {
        Join-Path $repoRoot ("target/release/" + $cliExecutable)
    }
$webDist = Join-Path $repoRoot "apps/web/dist"

$packageOutput = & (Join-Path $repoRoot "scripts/release/package-portable.ps1") `
    -ArtifactKind desktop `
    -Version $version `
    -OutputRoot $desktopPackageOutput `
    -DesktopBinaryPath $desktopBinary `
    -DaemonBinaryPath $daemonBinary `
    -BrowserBinaryPath $browserBinary `
    -CliBinaryPath $cliBinary `
    -WebDistPath $webDist
$packageMetadata = Convert-KeyValueOutputToHashtable -Lines $packageOutput
$archivePath = $packageMetadata["archive_path"]
if ([string]::IsNullOrWhiteSpace($archivePath)) {
    $archivePath = Join-Path $desktopPackageOutput "palyra-desktop-$version-$platform.zip"
}
$stagingRoot = Join-Path $desktopPackageOutput "palyra-desktop-$version-$platform"
if (Test-Path -LiteralPath $stagingRoot) {
    Remove-Item -LiteralPath $stagingRoot -Recurse -Force
}

if (Test-Path -LiteralPath $stateRoot) {
    Remove-Item -LiteralPath $stateRoot -Recurse -Force
}
New-Item -ItemType Directory -Path $stateRoot -Force | Out-Null
$configPath = Ensure-PortableConfigFile -ConfigPath (Resolve-PortableConfigPath -StateRoot $stateRoot)

$installOutput = & (Join-Path $repoRoot "scripts/release/install-desktop-package.ps1") `
    -ArchivePath $archivePath `
    -InstallRoot $installRoot `
    -StateRoot $stateRoot `
    -CliCommandRoot $cliCommandRoot `
    -Force
$installMetadata = Convert-KeyValueOutputToHashtable -Lines $installOutput
$resolvedInstallRoot = $installMetadata["install_root"]
if ([string]::IsNullOrWhiteSpace($resolvedInstallRoot)) {
    $resolvedInstallRoot = $installRoot
}
$resolvedCliCommandRoot = $installMetadata["cli_command_root"]
if ([string]::IsNullOrWhiteSpace($resolvedCliCommandRoot)) {
    $resolvedCliCommandRoot = Get-PalyraCliCommandRoot
}
$resolvedCliCommandPath = $installMetadata["cli_command_path"]
if ([string]::IsNullOrWhiteSpace($resolvedCliCommandPath)) {
    $cliCommandFileName = if ($IsWindows) { "palyra.cmd" } else { "palyra" }
    $resolvedCliCommandPath = Join-Path $resolvedCliCommandRoot $cliCommandFileName
}
$cliPersistenceStrategy = $installMetadata["cli_persistence_strategy"]
if ([string]::IsNullOrWhiteSpace($cliPersistenceStrategy)) {
    $cliPersistenceStrategy = if ($IsWindows) { "windows-user-path" } else { "posix-profile" }
}
$resolvedCliCurrentShellCommand = $installMetadata["cli_current_shell_command"]
if ([string]::IsNullOrWhiteSpace($resolvedCliCurrentShellCommand)) {
    $resolvedCliCurrentShellCommand = $resolvedCliCommandPath
}
$resolvedCliNewShellCommand = $installMetadata["cli_new_shell_command"]
if ([string]::IsNullOrWhiteSpace($resolvedCliNewShellCommand)) {
    $resolvedCliNewShellCommand = "palyra"
}
$resolvedCliParentShellCommand = $installMetadata["cli_parent_shell_command"]
if ([string]::IsNullOrWhiteSpace($resolvedCliParentShellCommand)) {
    $resolvedCliParentShellCommand = $resolvedCliCommandPath
}
$resolvedCliParentShellPathRestartRequired = $installMetadata["cli_parent_shell_path_restart_required"]
if ([string]::IsNullOrWhiteSpace($resolvedCliParentShellPathRestartRequired)) {
    $resolvedCliParentShellPathRestartRequired = "true"
}
$resolvedCliParentShellNote = $installMetadata["cli_parent_shell_note"]
if ([string]::IsNullOrWhiteSpace($resolvedCliParentShellNote)) {
    $resolvedCliParentShellNote = "Already-open parent terminals cannot inherit PATH changes made by this installer. In the parent terminal that launched the installer, run cli_parent_shell_command or cli_command_path directly; after PATH persistence, restart the terminal before running 'palyra'."
}
$cliSessionPathUpdated = $installMetadata["cli_session_path_updated"]
if ([string]::IsNullOrWhiteSpace($cliSessionPathUpdated)) {
    $cliSessionPathUpdated = "unknown"
}
$cliUserPathUpdated = $installMetadata["cli_user_path_updated"]
if ([string]::IsNullOrWhiteSpace($cliUserPathUpdated)) {
    $cliUserPathUpdated = "unknown"
}

$cliPathPreflightSource = $null
$cliPathPreflightMatchesCommandRoot = "not_checked"
if ($IsWindows) {
    $windowsCliExposure = Resolve-WindowsCleanDesktopCliExposure `
        -FallbackCommandRoot $resolvedCliCommandRoot `
        -FallbackCommandPath $resolvedCliCommandPath
    $resolvedCliCommandRoot = $windowsCliExposure.command_root
    $resolvedCliCommandPath = $windowsCliExposure.command_path
    $resolvedCliCurrentShellCommand = $resolvedCliCommandPath
    $resolvedCliParentShellCommand = $resolvedCliCommandPath

    if (Add-CurrentSessionPathEntry -Entry $resolvedCliCommandRoot) {
        $cliSessionPathUpdated = "true"
    }
    if (Move-WindowsUserPathEntryToFront -Entry $resolvedCliCommandRoot) {
        $cliUserPathUpdated = "true"
    }

    $windowsPathValue = @(
        [Environment]::GetEnvironmentVariable("Path", "User"),
        [Environment]::GetEnvironmentVariable("Path", "Machine")
    ) -join [IO.Path]::PathSeparator
    if (-not (Test-PathEntryPresent -Entry $resolvedCliCommandRoot -PathValue $windowsPathValue)) {
        throw "Clean desktop install did not persist or select a globally visible Windows CLI command root: $resolvedCliCommandRoot"
    }

    $resolvedPalyraCommand = Get-Command palyra -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($null -eq $resolvedPalyraCommand -or [string]::IsNullOrWhiteSpace($resolvedPalyraCommand.Source)) {
        throw "Clean desktop install could not resolve 'palyra' on PATH after selecting CLI command root: $resolvedCliCommandRoot"
    }
    $cliPathPreflightSource = [IO.Path]::GetFullPath($resolvedPalyraCommand.Source)
    $cliPathPreflightRoot = Split-Path -Parent $cliPathPreflightSource
    $cliPathPreflightMatchesCommandRoot =
        if (Test-PathEntryEquals -Left $cliPathPreflightRoot -Right $resolvedCliCommandRoot) {
            "true"
        } else {
            "false"
        }
    if ($cliPathPreflightMatchesCommandRoot -ne "true") {
        throw "Clean desktop install resolved 'palyra' to '$cliPathPreflightSource' instead of the selected CLI command root '$resolvedCliCommandRoot'. Use cli_command_path='$resolvedCliCommandPath' in this shell, or open a new shell after PATH persistence."
    }
}

$installedCliBinary = Join-Path $resolvedInstallRoot $cliExecutable
$harnessInventorySkill = Install-E2EHarnessInventorySkill `
    -WorkspaceRoot $workspaceRoot `
    -StateRoot $stateRoot `
    -CliPath $installedCliBinary
$harnessInventorySkillActivation = [ordered]@{
    activated = "not_attempted"
    daemon_url = ""
}

$launcherFileName =
    if ($IsWindows) {
        "Launch-Palyra-Test.ps1"
    } else {
        "launch-palyra-test.sh"
    }
$launcherPath = Join-Path $resolvedInstallRoot $launcherFileName
$shouldLaunch = $Launch -or -not $NoLaunch

if ($IsWindows) {
    $launcherBody =
@"
param(
    [switch]`$Wait
)

Set-StrictMode -Version Latest
`$ErrorActionPreference = "Stop"
`$ProgressPreference = "SilentlyContinue"
`$InformationPreference = "SilentlyContinue"

`$installRoot = Split-Path -Parent `$MyInvocation.MyCommand.Path
`$stateRoot = "$stateRoot"
`$configPath = "$configPath"
New-Item -ItemType Directory -Path `$stateRoot -Force | Out-Null
New-Item -ItemType Directory -Path "$osFileRoot" -Force | Out-Null

`$env:PALYRA_STATE_ROOT = `$stateRoot
`$env:PALYRA_CONFIG = `$configPath
`$env:PALYRA_OS_FILE_ROOTS = "$osFileRoot"
`$env:PALYRA_DESKTOP_PALYRAD_BIN = Join-Path `$installRoot "$daemonExecutable"
`$env:PALYRA_DESKTOP_BROWSERD_BIN = Join-Path `$installRoot "$browserExecutable"
`$env:PALYRA_DESKTOP_PALYRA_BIN = Join-Path `$installRoot "$cliExecutable"

`$desktopBinary = Join-Path `$installRoot "$desktopExecutable"
if (`$Wait) {
    & `$desktopBinary
} else {
    `$process = Start-Process -FilePath `$desktopBinary -WorkingDirectory `$installRoot -PassThru
    if (`$process.WaitForExit(2000)) {
        if (`$process.ExitCode -eq 0) {
            Write-Host "Palyra desktop exited cleanly before the launcher timeout. If no new window appeared, another instance may already be running."
            return
        }
        throw "Palyra desktop exited immediately with code `$(`$process.ExitCode). Re-run Launch-Palyra-Test.ps1 with -Wait to surface the startup error directly."
    }
}
"@
} else {
    $shStateRoot = ConvertTo-PosixSingleQuotedLiteral -Value $stateRoot
    $shConfigPath = ConvertTo-PosixSingleQuotedLiteral -Value $configPath
    $shOsFileRoot = ConvertTo-PosixSingleQuotedLiteral -Value $osFileRoot
    $launcherBody =
@'
#!/usr/bin/env bash
set -euo pipefail

install_root="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
state_root=__PALYRA_STATE_ROOT__
config_path=__PALYRA_CONFIG_PATH__
os_file_root=__PALYRA_OS_FILE_ROOT__
mkdir -p "$state_root"
mkdir -p "$os_file_root"

export PALYRA_STATE_ROOT="$state_root"
export PALYRA_CONFIG="$config_path"
export PALYRA_OS_FILE_ROOTS="$os_file_root"
export PALYRA_DESKTOP_PALYRAD_BIN="$install_root/__PALYRA_DAEMON_EXECUTABLE__"
export PALYRA_DESKTOP_BROWSERD_BIN="$install_root/__PALYRA_BROWSER_EXECUTABLE__"
export PALYRA_DESKTOP_PALYRA_BIN="$install_root/__PALYRA_CLI_EXECUTABLE__"

desktop_binary="$install_root/__PALYRA_DESKTOP_EXECUTABLE__"
if [[ "${1:-}" == "--wait" || "${1:-}" == "-w" ]]; then
  exec "$desktop_binary"
fi

"$desktop_binary" >/dev/null 2>&1 &
desktop_pid=$!
sleep 2
if ! kill -0 "$desktop_pid" 2>/dev/null; then
  if wait "$desktop_pid"; then
    echo "Palyra desktop exited cleanly before the launcher timeout. If no new window appeared, another instance may already be running."
    exit 0
  fi
  exit_code=$?
  echo "Palyra desktop exited immediately with code $exit_code. Re-run launch-palyra-test.sh with --wait to surface the startup error directly." >&2
  exit "$exit_code"
fi

echo "Palyra desktop launched with pid=$desktop_pid"
'@
    $launcherBody = $launcherBody.Replace("__PALYRA_STATE_ROOT__", $shStateRoot)
    $launcherBody = $launcherBody.Replace("__PALYRA_CONFIG_PATH__", $shConfigPath)
    $launcherBody = $launcherBody.Replace("__PALYRA_OS_FILE_ROOT__", $shOsFileRoot)
    $launcherBody = $launcherBody.Replace("__PALYRA_DAEMON_EXECUTABLE__", $daemonExecutable)
    $launcherBody = $launcherBody.Replace("__PALYRA_BROWSER_EXECUTABLE__", $browserExecutable)
    $launcherBody = $launcherBody.Replace("__PALYRA_CLI_EXECUTABLE__", $cliExecutable)
    $launcherBody = $launcherBody.Replace("__PALYRA_DESKTOP_EXECUTABLE__", $desktopExecutable)
}

Set-Content -LiteralPath $launcherPath -Value $launcherBody -NoNewline
if (-not $IsWindows) {
    Set-ExecutablePermissions -Path $launcherPath
}

if ($shouldLaunch) {
    & $launcherPath
    $harnessInventorySkillActivation = Enable-E2EHarnessInventorySkill `
        -ConfigPath $configPath `
        -SkillsRoot $harnessInventorySkill.skills_root `
        -CliPath $installedCliBinary
}

$installSummary = [ordered]@{
    installed_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    repo_root = $repoRoot
    workspace_root = $workspaceRoot
    artifacts_root = $artifactsRoot
    archive_path = $archivePath
    install_root = $resolvedInstallRoot
    config_path = $configPath
    state_root = $stateRoot
    os_file_root = $osFileRoot
    cli_command_root = $resolvedCliCommandRoot
    cli_command_path = $resolvedCliCommandPath
    cli_persistence_strategy = $cliPersistenceStrategy
    cli_session_path_updated = $cliSessionPathUpdated
    cli_user_path_updated = $cliUserPathUpdated
    cli_current_shell_command = $resolvedCliCurrentShellCommand
    cli_new_shell_command = $resolvedCliNewShellCommand
    cli_parent_shell_command = $resolvedCliParentShellCommand
    cli_parent_shell_path_restart_required = $resolvedCliParentShellPathRestartRequired
    cli_parent_shell_note = $resolvedCliParentShellNote
    cli_path_preflight_source = $cliPathPreflightSource
    cli_path_preflight_matches_command_root = $cliPathPreflightMatchesCommandRoot
    cli_path_preflight_matches_harness = $cliPathPreflightMatchesCommandRoot
    harness_inventory_skill_installed = $harnessInventorySkill.installed
    harness_inventory_skill_activated = $harnessInventorySkillActivation.activated
    harness_inventory_skill_artifact_path = $harnessInventorySkill.artifact_path
    harness_inventory_skill_skills_root = $harnessInventorySkill.skills_root
    harness_inventory_skill_daemon_url = $harnessInventorySkillActivation.daemon_url
    launcher_path = $launcherPath
    launched = $shouldLaunch
}
$installSummary |
    ConvertTo-Json -Depth 4 |
    Set-Content -LiteralPath (Join-Path $workspaceRoot "clean-install-metadata.json")

Write-Output "workspace_root=$workspaceRoot"
Write-Output "archive_path=$archivePath"
Write-Output "install_root=$resolvedInstallRoot"
Write-Output "config_path=$configPath"
Write-Output "state_root=$stateRoot"
Write-Output "os_file_root=$osFileRoot"
Write-Output "cli_command_root=$resolvedCliCommandRoot"
Write-Output "cli_command_path=$resolvedCliCommandPath"
Write-Output "cli_persistence_strategy=$cliPersistenceStrategy"
Write-Output "cli_session_path_updated=$cliSessionPathUpdated"
Write-Output "cli_user_path_updated=$cliUserPathUpdated"
Write-Output "cli_current_shell_command=$resolvedCliCurrentShellCommand"
Write-Output "cli_new_shell_command=$resolvedCliNewShellCommand"
Write-Output "cli_parent_shell_command=$resolvedCliParentShellCommand"
Write-Output "cli_parent_shell_path_restart_required=$resolvedCliParentShellPathRestartRequired"
Write-Output "cli_parent_shell_note=$resolvedCliParentShellNote"
Write-Output "cli_path_preflight_source=$cliPathPreflightSource"
Write-Output "cli_path_preflight_matches_command_root=$cliPathPreflightMatchesCommandRoot"
Write-Output "cli_path_preflight_matches_harness=$cliPathPreflightMatchesCommandRoot"
Write-Output "harness_inventory_skill_installed=$($harnessInventorySkill.installed)"
Write-Output "harness_inventory_skill_activated=$($harnessInventorySkillActivation.activated)"
Write-Output "harness_inventory_skill_artifact_path=$($harnessInventorySkill.artifact_path)"
Write-Output "harness_inventory_skill_skills_root=$($harnessInventorySkill.skills_root)"
Write-Output "harness_inventory_skill_daemon_url=$($harnessInventorySkillActivation.daemon_url)"
Write-Output "launcher_path=$launcherPath"
Write-Output "launched=$shouldLaunch"
