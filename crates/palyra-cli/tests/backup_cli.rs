use std::io::Write;
use std::process::{Command, Output};

use anyhow::{Context, Result};
use tempfile::TempDir;
use zip::{write::SimpleFileOptions, ZipWriter};

fn run_cli(workdir: &TempDir, args: &[&str]) -> Result<Output> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_palyra"));
    command
        .current_dir(workdir.path())
        .args(args)
        .env("XDG_CONFIG_HOME", workdir.path().join("xdg-config"))
        .env("XDG_STATE_HOME", workdir.path().join("xdg-state"))
        .env("HOME", workdir.path().join("home"))
        .env("LOCALAPPDATA", workdir.path().join("localappdata"))
        .env("APPDATA", workdir.path().join("appdata"));
    command.output().with_context(|| format!("failed to execute palyra {}", args.join(" ")))
}

#[test]
fn backup_verify_wrong_zip_reports_validation_error() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let archive_path = workdir.path().join("desktop-release.zip");
    let file = std::fs::File::create(archive_path.as_path())
        .with_context(|| format!("failed to create {}", archive_path.display()))?;
    let mut writer = ZipWriter::new(file);
    writer.start_file("release-manifest.json", SimpleFileOptions::default())?;
    writer.write_all(br#"{"kind":"desktop-release"}"#)?;
    writer.finish()?;

    let archive_arg = archive_path.display().to_string();
    let output =
        run_cli(&workdir, &["backup", "verify", "--archive", archive_arg.as_str(), "--json"])?;

    assert_eq!(
        output.status.code(),
        Some(2),
        "wrong backup archive should fail as validation; stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8(output.stderr).context("stderr was not UTF-8")?;
    assert!(
        stderr.contains("error[validation_error]"),
        "wrong backup archive should be classified as validation: {stderr}"
    );
    assert!(
        stderr.contains("not a Palyra backup archive"),
        "wrong backup archive message should explain the archive type mismatch: {stderr}"
    );
    assert!(
        !stderr.contains("error[internal_error]"),
        "wrong backup archive must not look like an internal failure: {stderr}"
    );
    Ok(())
}

#[test]
fn backup_verify_missing_archive_reports_not_found() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let archive_path = workdir.path().join("missing-backup.zip");
    let archive_arg = archive_path.display().to_string();
    let output =
        run_cli(&workdir, &["backup", "verify", "--archive", archive_arg.as_str(), "--json"])?;

    assert_eq!(
        output.status.code(),
        Some(8),
        "missing backup archive should fail as not_found; stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8(output.stderr).context("stderr was not UTF-8")?;
    assert!(
        stderr.contains("error[not_found]"),
        "missing backup archive should be classified as not_found: {stderr}"
    );
    assert!(
        stderr.contains("backup archive not found"),
        "missing backup archive message should explain the wrong path: {stderr}"
    );
    assert!(
        !stderr.contains("error[internal_error]"),
        "missing backup archive must not look like an internal failure: {stderr}"
    );
    Ok(())
}
