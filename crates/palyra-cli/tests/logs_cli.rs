use anyhow::{Context, Result};

mod support;

use support::cli_harness::{run_cli, temp_workdir};

#[test]
fn logs_commands_report_missing_journal_as_notice() -> Result<()> {
    let workdir = temp_workdir()?;
    for args in [&["logs", "--lines", "50"][..], &["gateway", "logs", "--lines", "50"][..]] {
        let output = run_cli(workdir.path(), args, &[])?;
        assert!(
            output.status.success(),
            "{} should succeed without an existing journal\nstdout:\n{}\nstderr:\n{}",
            args.join(" "),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        let stdout = String::from_utf8(output.stdout).context("stdout was not UTF-8")?;
        assert!(
            stdout.contains("logs.notice"),
            "{} should emit a notice when no journal exists: {stdout}",
            args.join(" ")
        );
        assert!(
            stdout.contains("no journal or service logs exist yet"),
            "{} should explain that no logs exist yet: {stdout}",
            args.join(" ")
        );
        assert!(
            stdout.contains("palyra gateway run"),
            "{} should point users to foreground startup logs: {stdout}",
            args.join(" ")
        );
    }
    Ok(())
}
