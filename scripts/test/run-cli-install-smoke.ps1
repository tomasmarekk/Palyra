Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
. (Join-Path $repoRoot "scripts/release/common.ps1")

function Get-TranscriptSlug {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Value
    )

    $slug = $Value.ToLowerInvariant() -replace "[^a-z0-9]+", "-"
    $slug = $slug.Trim("-")
    if ([string]::IsNullOrWhiteSpace($slug)) {
        return "command"
    }
    return $slug
}

function Format-CommandForDisplay {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Command,
        [string[]]$Arguments = @()
    )

    $segments = @($Command) + $Arguments
    return (
        $segments | ForEach-Object {
            if ($_ -match "[\s`"]") {
                '"' + ($_ -replace '"', '\"') + '"'
            } else {
                $_
            }
        }
    ) -join " "
}

function New-ScenarioContext {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Root,
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    $base = Join-Path $Root (Get-TranscriptSlug $Name)
    $directories = @(
        $base,
        (Join-Path $base "config"),
        (Join-Path $base "state"),
        (Join-Path $base "vault"),
        (Join-Path $base "home"),
        (Join-Path $base "localappdata"),
        (Join-Path $base "appdata"),
        (Join-Path $base "xdg-state"),
        (Join-Path $base "workspace"),
        (Join-Path $base "tls")
    )
    foreach ($directory in $directories) {
        New-Item -ItemType Directory -Path $directory -Force | Out-Null
    }

    [pscustomobject]@{
        Name         = $Name
        Root         = $base
        Workspace    = Join-Path $base "workspace"
        ConfigPath   = Join-Path $base "config/palyra.toml"
        RemoteConfig = Join-Path $base "config/remote-palyra.toml"
        StateRoot    = Join-Path $base "state"
        VaultDir     = Join-Path $base "vault"
        HomeDir      = Join-Path $base "home"
        LocalAppData = Join-Path $base "localappdata"
        AppData      = Join-Path $base "appdata"
        XdgStateHome = Join-Path $base "xdg-state"
        CertPath     = Join-Path $base "tls/gateway.crt"
        KeyPath      = Join-Path $base "tls/gateway.key"
        PatchPath    = Join-Path $base "workspace/sample.patch"
        TargetFile   = Join-Path $base "workspace/notes.txt"
    }
}

function Get-ScenarioEnvOverrides {
    param(
        [Parameter(Mandatory = $true)]
        [pscustomobject]$Context
    )

    return [ordered]@{
        PALYRA_CONFIG      = $Context.ConfigPath
        PALYRA_STATE_ROOT  = $Context.StateRoot
        PALYRA_VAULT_DIR   = $Context.VaultDir
        PALYRA_VAULT_BACKEND = "encrypted_file"
        HOME               = $Context.HomeDir
        LOCALAPPDATA       = $Context.LocalAppData
        APPDATA            = $Context.AppData
        XDG_STATE_HOME     = $Context.XdgStateHome
    }
}

function Merge-EnvironmentTables {
    param(
        [System.Collections.IDictionary]$Base = @{},
        [System.Collections.IDictionary]$Overlay = @{}
    )

    if ($null -eq $Base) {
        $Base = @{}
    }
    if ($null -eq $Overlay) {
        $Overlay = @{}
    }

    $merged = [ordered]@{}
    foreach ($key in $Base.Keys) {
        $merged[$key] = $Base[$key]
    }
    foreach ($key in $Overlay.Keys) {
        $merged[$key] = $Overlay[$key]
    }
    return $merged
}

function Invoke-TranscriptCommand {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Label,
        [Parameter(Mandatory = $true)]
        [string]$Command,
        [string[]]$Arguments = @(),
        [Parameter(Mandatory = $true)]
        [string]$WorkingDirectory,
        [Parameter(Mandatory = $true)]
        [string]$LogPath,
        [System.Collections.IDictionary]$Environment = @{},
        [string]$StdinText,
        [switch]$RedactOutput
    )

    $resolvedCommand = $Command
    $resolvedArguments = $Arguments
    if ([string]::Equals([IO.Path]::GetExtension($Command), ".ps1", [StringComparison]::OrdinalIgnoreCase)) {
        $resolvedCommand = "pwsh"
        $resolvedArguments = @("-NoLogo", "-File", $Command) + $Arguments
    }

    $displayCommand = Format-CommandForDisplay -Command $resolvedCommand -Arguments $resolvedArguments
    $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $resolvedCommand
    foreach ($argument in $resolvedArguments) {
        [void]$startInfo.ArgumentList.Add($argument)
    }
    $startInfo.WorkingDirectory = $WorkingDirectory
    $startInfo.UseShellExecute = $false
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $startInfo.RedirectStandardInput = $true
    foreach ($key in $Environment.Keys) {
        $startInfo.Environment[$key] = [string]$Environment[$key]
    }

    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    [void]$process.Start()
    $stdoutTask = $process.StandardOutput.ReadToEndAsync()
    $stderrTask = $process.StandardError.ReadToEndAsync()
    if ($PSBoundParameters.ContainsKey("StdinText")) {
        $process.StandardInput.Write($StdinText)
    }
    $process.StandardInput.Close()
    [void]$process.WaitForExit()
    $stdout = $stdoutTask.GetAwaiter().GetResult()
    $stderr = $stderrTask.GetAwaiter().GetResult()
    $exitCode = $process.ExitCode
    $loggedStdout = if ($RedactOutput -and -not [string]::IsNullOrEmpty($stdout)) {
        "[redacted by smoke harness]"
    } else {
        $stdout
    }
    $loggedStderr = if ($RedactOutput -and -not [string]::IsNullOrEmpty($stderr)) {
        "[redacted by smoke harness]"
    } else {
        $stderr
    }

    $transcript = @(
        "label: $Label"
        "working_directory: $WorkingDirectory"
        "command: $displayCommand"
        ""
        "stdout:"
        $loggedStdout
        ""
        "stderr:"
        $loggedStderr
        ""
        "exit_code: $exitCode"
    ) -join [Environment]::NewLine

    New-Item -ItemType Directory -Path (Split-Path -Parent $LogPath) -Force | Out-Null
    Set-Content -LiteralPath $LogPath -Value $transcript -Encoding utf8

    Write-Host "::group::$Label"
    Write-Host "PWD> $WorkingDirectory"
    foreach ($key in ($Environment.Keys | Sort-Object)) {
        Write-Host "ENV> $key=$($Environment[$key])"
    }
    Write-Host "CMD> $displayCommand"
    if (-not [string]::IsNullOrEmpty($loggedStdout)) {
        Write-Host "STDOUT>"
        Write-Host $loggedStdout.TrimEnd()
    }
    if (-not [string]::IsNullOrEmpty($loggedStderr)) {
        Write-Host "STDERR>"
        Write-Host $loggedStderr.TrimEnd()
    }
    Write-Host "EXIT> $exitCode"
    Write-Host "::endgroup::"

    if ($exitCode -ne 0) {
        throw "$Label failed with exit code $exitCode. See $LogPath."
    }

    return [pscustomobject]@{
        ExitCode = $exitCode
        Stdout   = $stdout
        Stderr   = $stderr
        LogPath  = $LogPath
    }
}

$outputRoot = Join-Path $repoRoot "target/release-artifacts/cli-install-smoke"
$reportRoot = Join-Path $outputRoot "report"
$logsRoot = Join-Path $outputRoot "logs"
$helpTranscriptRoot = Join-Path $reportRoot "transcripts/help"
$functionalTranscriptRoot = Join-Path $reportRoot "transcripts/functional"

if (Test-Path -LiteralPath $outputRoot) {
    Remove-Item -LiteralPath $outputRoot -Recurse -Force
}
foreach ($directory in @($outputRoot, $reportRoot, $logsRoot, $helpTranscriptRoot, $functionalTranscriptRoot)) {
    New-Item -ItemType Directory -Path $directory -Force | Out-Null
}

$version = (& (Join-Path $repoRoot "scripts/release/assert-version-coherence.ps1")).Trim()
$platform = Get-PlatformSlug
$headlessPackageOutput = Join-Path $outputRoot "headless"
$installRoot = Join-Path $outputRoot "installed-headless"
$configPath = Join-Path $outputRoot "installed-headless-config/palyra.toml"
$stateRoot = Join-Path $outputRoot "installed-headless-state"
$cliCommandRoot = Join-Path $outputRoot "cli-bin"
$archivePath = Join-Path $headlessPackageOutput "palyra-headless-$version-$platform.zip"
$installMetadataPath = Join-Path $installRoot "install-metadata.json"
$inventoryPath = Join-Path $reportRoot "inventory.json"
$summaryPath = Join-Path $reportRoot "summary.json"
$reportPath = Join-Path $reportRoot "report.md"

$baseInstallEnvironment = [ordered]@{
    PALYRA_CONFIG     = $configPath
    PALYRA_STATE_ROOT = $stateRoot
}
$transcriptStats = [ordered]@{
    help = 0
    functional = 0
}

Push-Location $repoRoot
try {
    Invoke-TranscriptCommand `
        -Label "ensure-web-ui" `
        -Command (Join-Path $repoRoot "scripts/test/ensure-web-ui.ps1") `
        -WorkingDirectory $repoRoot `
        -LogPath (Join-Path $logsRoot "ensure-web-ui.log") | Out-Null

    Invoke-TranscriptCommand `
        -Label "cargo-build-release" `
        -Command "cargo" `
        -Arguments @("build", "-p", "palyra-daemon", "-p", "palyra-browserd", "-p", "palyra-cli", "--release", "--locked") `
        -WorkingDirectory $repoRoot `
        -LogPath (Join-Path $logsRoot "cargo-build-release.log") | Out-Null

    $daemonBinary = Join-Path $repoRoot ("target/release/" + (Resolve-ExecutableName -BaseName "palyrad"))
    $browserBinary = Join-Path $repoRoot ("target/release/" + (Resolve-ExecutableName -BaseName "palyra-browserd"))
    $cliBinary = Join-Path $repoRoot ("target/release/" + (Resolve-ExecutableName -BaseName "palyra"))
    $webDist = Join-Path $repoRoot "apps/web/dist"

    Invoke-TranscriptCommand `
        -Label "package-headless" `
        -Command (Join-Path $repoRoot "scripts/release/package-portable.ps1") `
        -Arguments @(
            "-ArtifactKind", "headless",
            "-Version", $version,
            "-OutputRoot", $headlessPackageOutput,
            "-DaemonBinaryPath", $daemonBinary,
            "-BrowserBinaryPath", $browserBinary,
            "-CliBinaryPath", $cliBinary,
            "-WebDistPath", $webDist
        ) `
        -WorkingDirectory $repoRoot `
        -LogPath (Join-Path $logsRoot "package-headless.log") | Out-Null

    Invoke-TranscriptCommand `
        -Label "validate-headless-archive" `
        -Command (Join-Path $repoRoot "scripts/release/validate-portable-archive.ps1") `
        -Arguments @("-Path", $archivePath, "-ExpectedArtifactKind", "headless") `
        -WorkingDirectory $repoRoot `
        -LogPath (Join-Path $logsRoot "validate-headless-archive.log") | Out-Null

    Invoke-TranscriptCommand `
        -Label "install-headless-package" `
        -Command (Join-Path $repoRoot "scripts/release/install-headless-package.ps1") `
        -Arguments @(
            "-ArchivePath", $archivePath,
            "-InstallRoot", $installRoot,
            "-ConfigPath", $configPath,
            "-StateRoot", $stateRoot,
            "-CliCommandRoot", $cliCommandRoot,
            "-NoPersistCliPath",
            "-Force",
            "-SkipSystemdUnit:$IsWindows"
        ) `
        -WorkingDirectory $repoRoot `
        -LogPath (Join-Path $logsRoot "install-headless-package.log") | Out-Null

    $installManifest = Read-JsonFile -Path $installMetadataPath
    $binaryUnderTest = [string]$installManifest.cli_exposure.target_binary_path
    if ([string]::IsNullOrWhiteSpace($binaryUnderTest)) {
        throw "install metadata did not expose cli_exposure.target_binary_path"
    }
    $psShimUnderTest = $null
    if ($IsWindows) {
        $psShimUnderTest = @($installManifest.cli_exposure.shim_paths) |
            Where-Object {
                [string]::Equals(
                    [IO.Path]::GetExtension([string]$_),
                    ".ps1",
                    [StringComparison]::OrdinalIgnoreCase
                )
            } |
            Select-Object -First 1
        if ([string]::IsNullOrWhiteSpace($psShimUnderTest)) {
            throw "install metadata did not expose a Windows PowerShell CLI shim path"
        }
    }

    Invoke-TranscriptCommand `
        -Label "generate-cli-install-smoke-inventory" `
        -Command "cargo" `
        -Arguments @(
            "run",
            "-p", "palyra-cli",
            "--example", "emit_cli_install_smoke_inventory",
            "--locked",
            "--",
            "crates/palyra-cli/tests/cli_parity_matrix.toml",
            $inventoryPath
        ) `
        -WorkingDirectory $repoRoot `
        -LogPath (Join-Path $logsRoot "generate-cli-install-smoke-inventory.log") | Out-Null

    $inventory = Get-Content -LiteralPath $inventoryPath -Raw | ConvertFrom-Json -Depth 16
    $helpContext = New-ScenarioContext -Root (Join-Path $reportRoot "contexts") -Name "help"
    $helpEnvironment = Get-ScenarioEnvOverrides -Context $helpContext
    foreach ($entry in $inventory.help_commands) {
        $slug = Get-TranscriptSlug $entry.path
        Invoke-TranscriptCommand `
            -Label "installed-help :: $($entry.path)" `
            -Command $binaryUnderTest `
            -Arguments @($entry.args) `
            -WorkingDirectory $helpContext.Workspace `
            -LogPath (Join-Path $helpTranscriptRoot "$slug.log") `
            -Environment $helpEnvironment | Out-Null
        $transcriptStats.help += 1
    }

    $baselineContext = New-ScenarioContext -Root (Join-Path $reportRoot "contexts") -Name "baseline"
    $bootstrapContext = New-ScenarioContext -Root (Join-Path $reportRoot "contexts") -Name "bootstrap"
    $modelsContext = New-ScenarioContext -Root (Join-Path $reportRoot "contexts") -Name "models"
    $secretsContext = New-ScenarioContext -Root (Join-Path $reportRoot "contexts") -Name "secrets"
    $patchContext = New-ScenarioContext -Root (Join-Path $reportRoot "contexts") -Name "patch"

    Set-Content -LiteralPath $modelsContext.ConfigPath -Value "version = 1`n" -Encoding utf8
    Set-Content -LiteralPath $patchContext.TargetFile -Value "hello`n" -Encoding utf8

    $functionalScenarios = @(
        @{ Label = "functional :: version"; Context = $baselineContext; Args = @("version") }
        @{ Label = "functional :: root-help"; Context = $baselineContext; Args = @("--help") }
        @{ Label = "functional :: doctor-json"; Context = $baselineContext; Args = @("doctor", "--json"); Environment = $baseInstallEnvironment }
        @{ Label = "functional :: protocol-version"; Context = $baselineContext; Args = @("protocol", "version") }
        @{ Label = "functional :: protocol-validate-id"; Context = $baselineContext; Args = @("protocol", "validate-id", "--id", "01ARZ3NDEKTSV4RRFFQ69G5FAV") }
        @{ Label = "functional :: docs-search-gateway"; Context = $baselineContext; Args = @("docs", "search", "gateway") }
        @{ Label = "functional :: docs-show-help"; Context = $baselineContext; Args = @("docs", "show", "help/docs-help") }
        @{ Label = "functional :: setup-local"; Context = $bootstrapContext; Args = @("setup", "--mode", "local", "--path", $bootstrapContext.ConfigPath, "--force") }
        @{ Label = "functional :: setup-wizard-quickstart"; Context = $bootstrapContext; Args = @("setup", "--wizard", "--mode", "local", "--path", $bootstrapContext.ConfigPath, "--force", "--flow", "quickstart", "--non-interactive", "--accept-risk", "--auth-method", "api-key", "--api-key-env", "OPENAI_API_KEY", "--skip-health", "--skip-channels", "--skip-skills", "--json"); Environment = @{ OPENAI_API_KEY = "sk-installed-smoke" } }
        @{ Label = "functional :: config-validate"; Context = $bootstrapContext; Args = @("config", "validate", "--path", $bootstrapContext.ConfigPath) }
        @{ Label = "functional :: config-list"; Context = $bootstrapContext; Args = @("config", "list", "--path", $bootstrapContext.ConfigPath) }
        @{ Label = "functional :: onboarding-wizard-remote"; Context = $bootstrapContext; Args = @("onboarding", "wizard", "--path", $bootstrapContext.RemoteConfig, "--flow", "remote", "--non-interactive", "--accept-risk", "--remote-base-url", "https://dashboard.example.com/", "--remote-verification", "server-cert", "--pinned-server-cert-sha256", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "--admin-token-env", "PALYRA_REMOTE_ADMIN_TOKEN", "--skip-health", "--skip-channels", "--skip-skills", "--json"); Environment = @{ PALYRA_REMOTE_ADMIN_TOKEN = "remote-admin-token" } }
        @{ Label = "functional :: configure-gateway"; Context = $bootstrapContext; Args = @("configure", "--path", $bootstrapContext.ConfigPath, "--section", "gateway", "--non-interactive", "--accept-risk", "--bind-profile", "public-tls", "--daemon-port", "7310", "--grpc-port", "7610", "--quic-port", "7611", "--tls-scaffold", "bring-your-own", "--tls-cert-path", $bootstrapContext.CertPath, "--tls-key-path", $bootstrapContext.KeyPath, "--remote-base-url", "https://dashboard.example.com/", "--remote-verification", "gateway-ca", "--pinned-gateway-ca-sha256", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "--json") }
        @{ Label = "functional :: models-set"; Context = $modelsContext; Args = @("models", "set", "gpt-4.1-mini", "--path", $modelsContext.ConfigPath, "--json") }
        @{ Label = "functional :: models-set-embeddings"; Context = $modelsContext; Args = @("models", "set-embeddings", "text-embedding-3-large", "--path", $modelsContext.ConfigPath, "--dims", "3072", "--json") }
        @{ Label = "functional :: models-status"; Context = $modelsContext; Args = @("models", "status", "--path", $modelsContext.ConfigPath, "--json") }
        @{ Label = "functional :: bare-config-status"; Context = $modelsContext; Args = @("--config", $modelsContext.ConfigPath, "--output-format", "json", "config") }
        @{ Label = "functional :: config-set"; Context = $modelsContext; Args = @("config", "set", "--path", $modelsContext.ConfigPath, "--key", "daemon.port", "--value", "7443", "--backups", "2") }
        @{ Label = "functional :: config-get"; Context = $modelsContext; Args = @("config", "get", "--path", $modelsContext.ConfigPath, "--key", "daemon.port") }
        @{ Label = "functional :: config-unset"; Context = $modelsContext; Args = @("config", "unset", "--path", $modelsContext.ConfigPath, "--key", "daemon.port", "--backups", "2") }
        @{ Label = "functional :: setup-secrets-config"; Context = $secretsContext; Args = @("setup", "--mode", "local", "--path", $secretsContext.ConfigPath, "--force") }
        @{ Label = "functional :: secrets-set"; Context = $secretsContext; Args = @("secrets", "set", "global", "openai_api_key", "--value-stdin"); Stdin = "sk-installed-secret`nline-2`n" }
        @{ Label = "functional :: secrets-get-redacted"; Context = $secretsContext; Args = @("secrets", "get", "global", "openai_api_key") }
        @{ Label = "functional :: secrets-get-reveal"; Context = $secretsContext; Args = @("secrets", "get", "global", "openai_api_key", "--reveal"); RedactOutput = $true }
        @{ Label = "functional :: secrets-configure-openai"; Context = $secretsContext; Args = @("secrets", "configure", "openai-api-key", "global", "openai_api_key", "--value-stdin", "--path", $secretsContext.ConfigPath, "--json"); Stdin = "sk-test-openai-secret" }
        @{ Label = "functional :: secrets-audit"; Context = $secretsContext; Args = @("secrets", "audit", "--path", $secretsContext.ConfigPath, "--offline", "--json") }
        @{ Label = "functional :: secrets-apply"; Context = $secretsContext; Args = @("secrets", "apply", "--path", $secretsContext.ConfigPath, "--offline", "--json") }
        @{ Label = "functional :: patch-apply-dry-run"; Context = $patchContext; Args = @("patch", "apply", "--stdin", "--dry-run", "--json"); Stdin = "*** Begin Patch`n*** Update File: notes.txt`n@@`n-hello`n+hello world`n*** End Patch`n" }
        @{ Label = "functional :: update-dry-run"; Context = $baselineContext; Args = @("--output-format", "json", "update", "--install-root", $installRoot, "--archive", $archivePath, "--dry-run"); Environment = $baseInstallEnvironment }
        @{ Label = "functional :: uninstall-dry-run"; Context = $baselineContext; Args = @("--output-format", "json", "uninstall", "--install-root", $installRoot, "--dry-run"); Environment = $baseInstallEnvironment }
    )
    if ($null -ne $psShimUnderTest) {
        $functionalScenarios += @(
            @{
                Label = "functional :: ps-shim-patch-apply-dry-run"
                Command = [string]$psShimUnderTest
                Context = $patchContext
                Args = @("patch", "apply", "--stdin", "--dry-run", "--json")
                Stdin = "*** Begin Patch`n*** Update File: notes.txt`n@@`n-hello`n+hello from ps shim`n*** End Patch`n"
            }
        )
    }

    foreach ($scenario in $functionalScenarios) {
        $slug = Get-TranscriptSlug $scenario.Label
        $scenarioOverlay = @{}
        if ($scenario.ContainsKey("Environment")) {
            $scenarioOverlay = $scenario.Environment
        }
        $scenarioEnvironment = Merge-EnvironmentTables `
            -Base (Get-ScenarioEnvOverrides -Context $scenario.Context) `
            -Overlay $scenarioOverlay
        $scenarioCommand = if ($scenario.ContainsKey("Command")) { [string]$scenario.Command } else { $binaryUnderTest }
        $invokeParams = @{
            Label            = $scenario.Label
            Command          = $scenarioCommand
            Arguments        = $scenario.Args
            WorkingDirectory = $scenario.Context.Workspace
            LogPath          = (Join-Path $functionalTranscriptRoot "$slug.log")
            Environment      = $scenarioEnvironment
        }
        if ($scenario.ContainsKey("Stdin")) {
            $invokeParams["StdinText"] = [string]$scenario.Stdin
        }
        if ($scenario.ContainsKey("RedactOutput")) {
            $invokeParams["RedactOutput"] = [bool]$scenario.RedactOutput
        }
        Invoke-TranscriptCommand @invokeParams | Out-Null
        $transcriptStats.functional += 1
    }

    $previousBinary = $env:PALYRA_BIN_UNDER_TEST
    $previousArchive = $env:PALYRA_INSTALL_ARCHIVE_PATH
    $previousInstallRoot = $env:PALYRA_INSTALL_ROOT
    $previousConfig = $env:PALYRA_CONFIG_UNDER_TEST
    $previousStateRoot = $env:PALYRA_STATE_ROOT_UNDER_TEST
    try {
        $env:PALYRA_BIN_UNDER_TEST = $binaryUnderTest
        $env:PALYRA_INSTALL_ARCHIVE_PATH = $archivePath
        $env:PALYRA_INSTALL_ROOT = $installRoot
        $env:PALYRA_CONFIG_UNDER_TEST = $configPath
        $env:PALYRA_STATE_ROOT_UNDER_TEST = $stateRoot

        Invoke-TranscriptCommand `
            -Label "cargo-test-installed-smoke" `
            -Command "cargo" `
            -Arguments @("test", "-p", "palyra-cli", "--test", "installed_smoke", "--locked", "--", "--test-threads=1") `
            -WorkingDirectory $repoRoot `
            -LogPath (Join-Path $logsRoot "cargo-test-installed-smoke.log") | Out-Null
    }
    finally {
        if ($null -eq $previousBinary) { Remove-Item Env:PALYRA_BIN_UNDER_TEST -ErrorAction SilentlyContinue } else { $env:PALYRA_BIN_UNDER_TEST = $previousBinary }
        if ($null -eq $previousArchive) { Remove-Item Env:PALYRA_INSTALL_ARCHIVE_PATH -ErrorAction SilentlyContinue } else { $env:PALYRA_INSTALL_ARCHIVE_PATH = $previousArchive }
        if ($null -eq $previousInstallRoot) { Remove-Item Env:PALYRA_INSTALL_ROOT -ErrorAction SilentlyContinue } else { $env:PALYRA_INSTALL_ROOT = $previousInstallRoot }
        if ($null -eq $previousConfig) { Remove-Item Env:PALYRA_CONFIG_UNDER_TEST -ErrorAction SilentlyContinue } else { $env:PALYRA_CONFIG_UNDER_TEST = $previousConfig }
        if ($null -eq $previousStateRoot) { Remove-Item Env:PALYRA_STATE_ROOT_UNDER_TEST -ErrorAction SilentlyContinue } else { $env:PALYRA_STATE_ROOT_UNDER_TEST = $previousStateRoot }
    }

    $summary = [ordered]@{
        version = $version
        platform = $platform
        archive_path = $archivePath
        install_root = $installRoot
        binary_under_test = $binaryUnderTest
        help_commands = $transcriptStats.help
        functional_commands = $transcriptStats.functional
        report_path = $reportPath
        inventory_path = $inventoryPath
    }
    $summary | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $summaryPath -Encoding utf8

    $reportLines = @(
        "# CLI install smoke report"
        ""
        "- Version: $version"
        "- Platform: $platform"
        "- Installed binary: $binaryUnderTest"
        "- Help command transcripts: $($transcriptStats.help)"
        "- Functional command transcripts: $($transcriptStats.functional)"
        "- Inventory: report/inventory.json"
        "- Summary: report/summary.json"
        "- Help transcript directory: report/transcripts/help"
        "- Functional transcript directory: report/transcripts/functional"
    )
    Set-Content -LiteralPath $reportPath -Value ($reportLines -join [Environment]::NewLine) -Encoding utf8
    if ($env:GITHUB_STEP_SUMMARY) {
        Set-Content -LiteralPath $env:GITHUB_STEP_SUMMARY -Value ($reportLines -join [Environment]::NewLine) -Encoding utf8
    }
}
finally {
    try {
        if (Test-Path -LiteralPath $installRoot) {
            Invoke-TranscriptCommand `
                -Label "uninstall-headless-package" `
                -Command (Join-Path $repoRoot "scripts/release/uninstall-package.ps1") `
                -Arguments @("-InstallRoot", $installRoot, "-RemoveStateRoot") `
                -WorkingDirectory $repoRoot `
                -LogPath (Join-Path $logsRoot "uninstall-headless-package.log") | Out-Null

            if (Test-Path -LiteralPath $installRoot) {
                throw "install root should be removed after uninstall: $installRoot"
            }
            if (Test-Path -LiteralPath $stateRoot) {
                throw "state root should be removed after uninstall: $stateRoot"
            }
            if (Test-Path -LiteralPath $cliCommandRoot -PathType Container) {
                if (-not (Test-DirectoryEmpty -Path $cliCommandRoot)) {
                    throw "CLI command root should be empty after uninstall cleanup: $cliCommandRoot"
                }
            }
        }
    }
    finally {
        Pop-Location
    }
}

Write-Output "cli_install_smoke=passed"
Write-Output "version=$version"
Write-Output "platform=$platform"
Write-Output "archive_path=$archivePath"
Write-Output "summary_path=$summaryPath"
