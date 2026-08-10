Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
. (Join-Path $repoRoot "scripts/release/common.ps1")

function Assert-Equal {
    param(
        [AllowNull()]
        [object]$Actual,
        [AllowNull()]
        [object]$Expected,
        [Parameter(Mandatory = $true)]
        [string]$Message
    )

    if (-not [object]::Equals($Actual, $Expected)) {
        throw "$Message Expected: '$Expected'. Actual: '$Actual'."
    }
}

function Assert-Contains {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Haystack,
        [Parameter(Mandatory = $true)]
        [string]$Needle,
        [Parameter(Mandatory = $true)]
        [string]$Message
    )

    if (-not $Haystack.Contains($Needle)) {
        throw "$Message Missing: '$Needle'."
    }
}

function Assert-ThrowsLike {
    param(
        [Parameter(Mandatory = $true)]
        [scriptblock]$ScriptBlock,
        [Parameter(Mandatory = $true)]
        [string]$ExpectedText,
        [Parameter(Mandatory = $true)]
        [string]$Message
    )

    try {
        & $ScriptBlock
    } catch {
        if ($_.Exception.Message.Contains($ExpectedText)) {
            return
        }
        throw "$Message Unexpected error: $($_.Exception.Message)"
    }

    throw "$Message Expected an error containing '$ExpectedText'."
}

$singleQuote = [string][char]39
$doubleQuote = [string][char]34
$backtick = [string][char]96
$shellPath = "/tmp/palyra " + $singleQuote + '$(' + "touch pwn)" + $singleQuote + " " + $backtick + "literal"
$escapedSingleQuote = $singleQuote + $doubleQuote + $singleQuote + $doubleQuote + $singleQuote
$expectedPosixLiteral = $singleQuote + "/tmp/palyra " + $escapedSingleQuote + '$(' + "touch pwn)" + $escapedSingleQuote + " " + $backtick + "literal" + $singleQuote

Assert-Equal `
    -Actual (ConvertTo-PosixSingleQuotedLiteral -Value $shellPath) `
    -Expected $expectedPosixLiteral `
    -Message "POSIX shell literal must single-quote command substitutions and embedded quotes."

$profileBlock = Get-PalyraCliProfileBlock -CommandRoot $shellPath
Assert-Contains `
    -Haystack $profileBlock `
    -Needle ("PALYRA_CLI_BIN=" + $expectedPosixLiteral) `
    -Message "POSIX profile block must assign the command root as a shell literal."

Assert-ThrowsLike `
    -ScriptBlock { Get-PalyraCliProfileBlock -CommandRoot ("safe`nroot") | Out-Null } `
    -ExpectedText "line separator" `
    -Message "Profile generation must reject line separators."

$powerShellPath = "C:\Palyra " + $singleQuote + '$(' + "Invoke-Expression pwn)" + $singleQuote
$expectedPowerShellLiteral = $singleQuote + $powerShellPath.Replace($singleQuote, $singleQuote + $singleQuote) + $singleQuote
Assert-Equal `
    -Actual (ConvertTo-PowerShellSingleQuotedLiteral -Value $powerShellPath) `
    -Expected $expectedPowerShellLiteral `
    -Message "PowerShell shim literals must use non-expandable single-quoted strings."

Assert-Equal `
    -Actual (ConvertTo-CmdShimLiteral -Value "C:\Palyra & %TEMP% ^ < > |") `
    -Expected "C:\Palyra ^& %%TEMP%% ^^ ^< ^> ^|" `
    -Message "cmd shim literals must escape metacharacters used by cmd.exe."

$previousPath = $env:PATH
$previousWindowsUserPath = if ($IsWindows) { [Environment]::GetEnvironmentVariable("Path", "User") } else { $null }
$tempRoot = Join-Path ([IO.Path]::GetTempPath()) ("palyra-cli-exposure-escaping-" + [guid]::NewGuid().ToString("N"))
try {
    $targetRoot = Join-Path $tempRoot ("target " + '$(' + "pwn)")
    $commandRoot = Join-Path $tempRoot "bin"
    New-Item -ItemType Directory -Path $targetRoot -Force | Out-Null
    $targetBinary = Join-Path $targetRoot (Resolve-ExecutableName -BaseName "palyra")
    Set-Content -LiteralPath $targetBinary -Value "" -NoNewline

    if ($IsWindows) {
        New-Item -ItemType Directory -Path $commandRoot -Force | Out-Null
        $legacyShim = Join-Path $commandRoot "palyra.ps1"
        Set-Content -LiteralPath $legacyShim -Value "# obsolete Palyra shim" -NoNewline
        $legacyAliasRoot = Join-Path ([Environment]::GetFolderPath("LocalApplicationData")) "Palyra/bin"
        [Environment]::SetEnvironmentVariable(
            "Path",
            (Prepend-PathEntry -Entry $legacyAliasRoot -PathValue $previousWindowsUserPath),
            "User"
        )
    }

    $cliExposure = Install-PalyraCliExposure `
        -TargetBinaryPath $targetBinary `
        -CommandRoot $commandRoot `
        -PersistPath:$false

    Assert-Equal `
        -Actual $cliExposure.parent_shell_command `
        -Expected $cliExposure.command_path `
        -Message "Parent-shell command metadata must point to the direct shim path."
    Assert-Equal `
        -Actual $cliExposure.parent_shell_path_restart_required `
        -Expected "false" `
        -Message "Session-only CLI exposure must not claim a restart will refresh PATH."
    Assert-Contains `
        -Haystack $cliExposure.parent_shell_path_note `
        -Needle "Already-open parent terminals cannot inherit PATH changes" `
        -Message "Parent-shell note must explain why the launching terminal cannot see PATH updates."
    Assert-Contains `
        -Haystack $cliExposure.parent_shell_path_note `
        -Needle "restart the terminal before running 'palyra'" `
        -Message "Parent-shell note must tell users to restart before relying on PATH."

    if ($IsWindows) {
        Assert-Equal `
            -Actual (Test-Path -LiteralPath $legacyShim) `
            -Expected $false `
            -Message "CLI exposure must remove the legacy PowerShell shim that shadows palyra.cmd."
        Assert-Equal `
            -Actual (@($cliExposure.legacy_shim_paths_removed) -contains $legacyShim) `
            -Expected $true `
            -Message "CLI exposure metadata must report the removed legacy shim."
        Assert-Equal `
            -Actual (Test-PathEntryPresent -Entry $legacyAliasRoot -PathValue ([Environment]::GetEnvironmentVariable("Path", "User"))) `
            -Expected $true `
            -Message "Session-only CLI exposure must not remove persistent Windows user PATH entries."
        $powerShellShim = $cliExposure.shim_paths | Where-Object { [string]$_ -like "*-pwsh.ps1" } | Select-Object -First 1
        $cmdShim = $cliExposure.shim_paths | Where-Object { [string]$_ -like "*.cmd" } | Select-Object -First 1
        Assert-Contains `
            -Haystack (Get-Content -Raw -LiteralPath $powerShellShim) `
            -Needle ("& " + (ConvertTo-PowerShellSingleQuotedLiteral -Value (Resolve-Path -LiteralPath $targetBinary).Path) + " @args") `
            -Message "PowerShell shim must invoke the target through a non-expandable literal."
        Assert-Contains `
            -Haystack (Get-Content -Raw -LiteralPath $cmdShim) `
            -Needle "setlocal DisableDelayedExpansion" `
            -Message "cmd shim must disable delayed expansion before invoking escaped paths."
    } else {
        $posixShim = [string]$cliExposure.command_path
        Assert-Contains `
            -Haystack (Get-Content -Raw -LiteralPath $posixShim) `
            -Needle ("exec " + (ConvertTo-PosixSingleQuotedLiteral -Value (Resolve-Path -LiteralPath $targetBinary).Path) + ' "$@"') `
            -Message "POSIX shim must invoke the target through a shell literal."
    }

    Assert-ThrowsLike `
        -ScriptBlock {
            Install-PalyraCliExposure `
                -TargetBinaryPath $targetBinary `
                -CommandRoot ("bad`nroot") `
                -PersistPath:$false | Out-Null
        } `
        -ExpectedText "line separator" `
        -Message "CLI exposure install must reject command roots with line separators."
}
finally {
    $env:PATH = $previousPath
    if ($IsWindows) {
        [Environment]::SetEnvironmentVariable("Path", $previousWindowsUserPath, "User")
    }
    if (Test-Path -LiteralPath $tempRoot) {
        Remove-Item -LiteralPath $tempRoot -Recurse -Force
    }
}

Write-Output "release_cli_exposure_escaping=passed"
