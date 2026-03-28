use std::{env, fs, path::PathBuf};

use anyhow::{Context, Result};
use palyra_cli::cli_parity::{
    build_cli_parity_report, build_cli_root_command, render_cli_parity_report_markdown,
    CliParityMatrix,
};

fn main() -> Result<()> {
    let mut args = env::args().skip(1);
    let matrix_path = match args.next() {
        Some(path) => PathBuf::from(path),
        None => anyhow::bail!("usage: cargo run -p palyra-cli --example render_cli_parity_report -- <matrix-path> [output-path]"),
    };
    let output_path = args.next().map(PathBuf::from);
    if args.next().is_some() {
        anyhow::bail!("expected at most two arguments: <matrix-path> [output-path]");
    }

    let matrix_text = fs::read_to_string(&matrix_path)
        .with_context(|| format!("failed to read matrix file {}", matrix_path.display()))?;
    let matrix: CliParityMatrix = toml::from_str(matrix_text.as_str())
        .with_context(|| format!("failed to parse matrix file {}", matrix_path.display()))?;
    let root = build_cli_root_command();
    let report = build_cli_parity_report(&matrix, &root);
    let markdown = render_cli_parity_report_markdown(&report);

    if let Some(output_path) = output_path {
        if let Some(parent) = output_path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).with_context(|| {
                    format!("failed to create report directory {}", parent.display())
                })?;
            }
        }
        fs::write(&output_path, markdown.as_bytes())
            .with_context(|| format!("failed to write report {}", output_path.display()))?;
    } else {
        println!("{markdown}");
    }

    Ok(())
}
