use std::{collections::BTreeSet, fs, path::PathBuf};

use anyhow::{Context, Result};
use palyra_cli::cli_parity::{
    build_cli_parity_report, build_cli_root_command, build_shared_chat_command_parity_report,
    render_cli_parity_report_markdown, render_shared_chat_command_parity_markdown,
    validate_cli_parity_report, CliParityMatrix,
};

const MATRIX_PATH: &str = "tests/cli_parity_matrix.toml";
const REPORT_SNAPSHOT_PATH: &str = "tests/cli_parity_report.md";
const SHARED_CHAT_COMMAND_SNAPSHOT_PATH: &str = "tests/shared_chat_command_parity.md";

fn crate_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn load_matrix() -> Result<CliParityMatrix> {
    let matrix_path = crate_root().join(MATRIX_PATH);
    let matrix_text = fs::read_to_string(&matrix_path)
        .with_context(|| format!("failed to read {}", matrix_path.display()))?;
    let matrix: CliParityMatrix = toml::from_str(matrix_text.as_str())
        .with_context(|| format!("failed to parse {}", matrix_path.display()))?;

    let mut seen_paths = BTreeSet::new();
    let mut seen_snapshot_files = BTreeSet::new();
    for entry in &matrix.entries {
        assert!(
            seen_paths.insert(entry.path.clone()),
            "CLI parity matrix contains duplicate path {}",
            entry.path
        );
        if let Some(snapshot) = &entry.snapshot {
            let files = [
                snapshot.file.as_deref(),
                snapshot.unix_file.as_deref(),
                snapshot.windows_file.as_deref(),
            ];
            let mut declared_any = false;
            for file in files.into_iter().flatten() {
                declared_any = true;
                assert!(
                    seen_snapshot_files.insert(file.to_owned()),
                    "CLI parity matrix reuses snapshot file {}",
                    file
                );
            }
            assert!(
                declared_any,
                "CLI parity snapshot for {} must declare at least one file",
                entry.path
            );
        }
    }

    Ok(matrix)
}

fn generate_report_markdown() -> Result<String> {
    let matrix = load_matrix()?;
    let root = build_cli_root_command();
    let report = build_cli_parity_report(&matrix, &root);
    validate_cli_parity_report(&report)?;
    Ok(render_cli_parity_report_markdown(&report))
}

fn generate_shared_chat_command_markdown() -> String {
    let report = build_shared_chat_command_parity_report();
    render_shared_chat_command_parity_markdown(&report)
}

#[test]
fn cli_parity_matrix_has_no_regressions() -> Result<()> {
    let matrix = load_matrix()?;
    let root = build_cli_root_command();
    let report = build_cli_parity_report(&matrix, &root);
    validate_cli_parity_report(&report)
}

#[test]
fn cli_parity_report_matches_committed_snapshot() -> Result<()> {
    let expected_path = crate_root().join(REPORT_SNAPSHOT_PATH);
    let expected = fs::read_to_string(&expected_path)
        .with_context(|| format!("failed to read {}", expected_path.display()))?;
    assert_eq!(generate_report_markdown()?, expected.replace("\r\n", "\n"));
    Ok(())
}

#[test]
fn shared_chat_command_registry_matches_committed_snapshot() -> Result<()> {
    let expected_path = crate_root().join(SHARED_CHAT_COMMAND_SNAPSHOT_PATH);
    let expected = fs::read_to_string(&expected_path)
        .with_context(|| format!("failed to read {}", expected_path.display()))?;
    assert_eq!(generate_shared_chat_command_markdown(), expected.replace("\r\n", "\n"));
    Ok(())
}
