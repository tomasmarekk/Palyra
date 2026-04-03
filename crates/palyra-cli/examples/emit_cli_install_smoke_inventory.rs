use std::{env, fs, path::PathBuf};

use anyhow::{Context, Result};
use palyra_cli::cli_parity::CliParityMatrix;
use serde::Serialize;

#[derive(Debug, Serialize)]
struct HelpCommandInventoryEntry {
    path: String,
    category: String,
    summary: String,
    snapshot_file: Option<String>,
    args: Vec<String>,
}

#[derive(Debug, Serialize)]
struct CliInstallSmokeInventory {
    matrix_version: u32,
    help_commands: Vec<HelpCommandInventoryEntry>,
}

fn main() -> Result<()> {
    let mut args = env::args().skip(1);
    let matrix_path = args.next().context(
        "usage: cargo run -p palyra-cli --example emit_cli_install_smoke_inventory -- <matrix-path> [output-path]",
    )?;
    let output_path = args.next();

    let matrix_path = PathBuf::from(matrix_path);
    let matrix_text = fs::read_to_string(&matrix_path)
        .with_context(|| format!("failed to read {}", matrix_path.display()))?;
    let matrix: CliParityMatrix = toml::from_str(matrix_text.as_str())
        .with_context(|| format!("failed to parse {}", matrix_path.display()))?;

    let mut help_commands = matrix
        .entries
        .iter()
        .filter_map(|entry| {
            let snapshot = entry.snapshot.as_ref()?;
            let mut args = if snapshot.path == "palyra" {
                vec!["--help".to_owned()]
            } else {
                let mut args = snapshot.path.split(' ').map(str::to_owned).collect::<Vec<_>>();
                args.push("--help".to_owned());
                args
            };
            Some(HelpCommandInventoryEntry {
                path: entry.path.clone(),
                category: entry.category.clone(),
                summary: entry.summary.clone(),
                snapshot_file: snapshot.expected_file().map(str::to_owned),
                args: std::mem::take(&mut args),
            })
        })
        .collect::<Vec<_>>();
    help_commands.sort_by(|left, right| left.path.cmp(&right.path));

    let inventory = CliInstallSmokeInventory { matrix_version: matrix.version, help_commands };
    let json = serde_json::to_string_pretty(&inventory).context("failed to serialize inventory")?;

    if let Some(output_path) = output_path {
        let output_path = PathBuf::from(output_path);
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        fs::write(&output_path, format!("{json}\n"))
            .with_context(|| format!("failed to write {}", output_path.display()))?;
    } else {
        println!("{json}");
    }

    Ok(())
}
