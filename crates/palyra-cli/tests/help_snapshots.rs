use std::{env, fs, path::PathBuf, process::Command};

use anyhow::{Context, Result};
use palyra_cli::cli_parity::{CliParityMatrix, CliParitySnapshotSpec};

const MATRIX_PATH: &str = "tests/cli_parity_matrix.toml";
const HELP_SNAPSHOTS_DIR: &str = "tests/help_snapshots";
const UPDATE_HELP_ENV: &str = "PALYRA_UPDATE_HELP_SNAPSHOTS";

fn crate_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn load_matrix() -> Result<CliParityMatrix> {
    let matrix_path = crate_root().join(MATRIX_PATH);
    let matrix_text = fs::read_to_string(&matrix_path)
        .with_context(|| format!("failed to read {}", matrix_path.display()))?;
    toml::from_str(matrix_text.as_str())
        .with_context(|| format!("failed to parse {}", matrix_path.display()))
}

fn normalize_help_text(text: &str) -> String {
    text.replace("\r\n", "\n").replace("palyra.exe", "palyra")
}

fn run_help(args: &[&str]) -> Result<String> {
    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args(args)
        .output()
        .with_context(|| format!("failed to execute palyra {}", args.join(" ")))?;
    assert!(
        output.status.success(),
        "palyra {} should succeed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8(output.stdout)
        .context("help output must be valid UTF-8")?
        .replace("\r\n", "\n");
    let normalized = text.lines().map(str::trim_end).collect::<Vec<_>>();
    let mut collapsed = Vec::with_capacity(normalized.len());
    let mut previous_blank = false;
    for line in normalized {
        let is_blank = line.is_empty();
        if is_blank && previous_blank {
            continue;
        }
        previous_blank = is_blank;
        collapsed.push(line);
    }
    Ok(normalize_help_text(collapsed.join("\n").trim_end()))
}

fn snapshot_path(snapshot: &CliParitySnapshotSpec) -> Result<PathBuf> {
    let file = snapshot
        .expected_file()
        .context("snapshot entry must resolve to a platform-specific file")?;
    Ok(crate_root().join(HELP_SNAPSHOTS_DIR).join(file))
}

fn snapshot_args(snapshot: &CliParitySnapshotSpec) -> Vec<&str> {
    if snapshot.path == "palyra" {
        return vec!["--help"];
    }
    let mut args = snapshot.path.split(' ').collect::<Vec<_>>();
    args.push("--help");
    args
}

fn update_help_snapshots_enabled() -> bool {
    env::var_os(UPDATE_HELP_ENV).is_some()
}

#[test]
fn help_snapshots_match_cli_parity_matrix() -> Result<()> {
    let matrix = load_matrix()?;
    let mut failures = Vec::new();

    for entry in matrix
        .entries
        .iter()
        .filter_map(|entry| entry.snapshot.as_ref().map(|snapshot| (entry.path.as_str(), snapshot)))
    {
        let (path, snapshot) = entry;
        let snapshot_path = snapshot_path(snapshot)?;
        let actual = run_help(snapshot_args(snapshot).as_slice())?;

        if update_help_snapshots_enabled() {
            if let Some(parent) = snapshot_path.parent() {
                fs::create_dir_all(parent).with_context(|| {
                    format!("failed to create help snapshot directory {}", parent.display())
                })?;
            }
            fs::write(&snapshot_path, format!("{actual}\n"))
                .with_context(|| format!("failed to write {}", snapshot_path.display()))?;
            continue;
        }

        match fs::read_to_string(&snapshot_path) {
            Ok(expected) => {
                let normalized_expected = normalize_help_text(expected.trim_end());
                if normalized_expected != actual {
                    failures.push(format!(
                        "{}: snapshot mismatch ({})",
                        path,
                        snapshot_path.display()
                    ));
                }
            }
            Err(error) => {
                failures.push(format!(
                    "{}: failed to read snapshot {} ({error})",
                    path,
                    snapshot_path.display()
                ));
            }
        }
    }

    if failures.is_empty() {
        return Ok(());
    }

    Err(anyhow::anyhow!("CLI help snapshots drifted:\n{}", failures.join("\n")))
}
