//! Pins the CLI parity matrix (tests/cli_parity_matrix.toml) against the real clap command
//! tree and the committed shared chat command snapshot markdown.

use std::{collections::BTreeSet, fs, path::PathBuf};

use anyhow::{Context, Result};
use palyra_cli::cli_parity::{
    build_cli_parity_report, build_cli_root_command, build_shared_chat_command_parity_report,
    render_cli_parity_report_markdown, render_shared_chat_command_parity_markdown,
    validate_cli_parity_report, CliParityMatrix,
};
use serde_json::Value;

const MATRIX_PATH: &str = "tests/cli_parity_matrix.toml";
const SHARED_CHAT_COMMAND_SNAPSHOT_PATH: &str = "tests/shared_chat_command_parity.md";
const RUNTIME_AUDIT_INVENTORY_PATH: &str =
    "../palyra-daemon/tests/golden/current_state_inventory.json";
const RUNTIME_AUDIT_REPORT_PATH: &str =
    "../palyra-daemon/tests/golden/current_state_inventory_report.md";

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
fn cli_parity_report_covers_plugin_operability_surface() -> Result<()> {
    let report = generate_report_markdown()?;
    for expected in [
        "`plugins inspect`",
        "`plugins discover`",
        "`plugins explain`",
        "`plugins doctor`",
        "`plugins update`",
        "`plugins-install-help.txt`",
        "`plugins-update-help.txt`",
        "`--config-json`",
        "`--config-json-file`",
        "`--config-json-stdin`",
        "`--clear-config`",
    ] {
        assert!(report.contains(expected), "CLI parity report should mention {expected}");
    }
    Ok(())
}

#[test]
fn runtime_audit_report_matches_cli_inventory_counts() -> Result<()> {
    let inventory_path = crate_root().join(RUNTIME_AUDIT_INVENTORY_PATH);
    let inventory_raw = fs::read_to_string(&inventory_path)
        .with_context(|| format!("failed to read {}", inventory_path.display()))?;
    let inventory: Value = serde_json::from_str(inventory_raw.as_str())
        .with_context(|| format!("failed to parse {}", inventory_path.display()))?;
    let report_path = crate_root().join(RUNTIME_AUDIT_REPORT_PATH);
    let report = fs::read_to_string(&report_path)
        .with_context(|| format!("failed to read {}", report_path.display()))?;

    let capabilities = inventory
        .get("capabilities")
        .and_then(Value::as_array)
        .context("runtime audit inventory should expose capabilities array")?;
    let cli_families = inventory
        .get("cli_families")
        .and_then(Value::as_array)
        .context("runtime audit inventory should expose cli_families array")?;
    let feature_rollouts = inventory
        .get("feature_rollouts")
        .and_then(Value::as_object)
        .context("runtime audit inventory should expose feature_rollouts object")?;

    for expected in [
        format!("- Capability catalog entries: `{}`", capabilities.len()),
        format!("- CLI families: `{}`", cli_families.len()),
        format!("- Feature rollout flags: `{}`", feature_rollouts.len()),
    ] {
        assert!(
            report.contains(expected.as_str()),
            "runtime audit report should include count line {expected}"
        );
    }

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
