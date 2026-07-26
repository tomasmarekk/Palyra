use std::{fs, path::PathBuf};

use anyhow::{bail, Context, Result};
use palyra_common::qa_fault_injection::{
    ensure_continuity_campaign_passed, render_continuity_campaign_markdown, run_continuity_campaign,
};

const REPORT_DIR: &str = "target/release-artifacts/continuity-campaign";

fn main() -> Result<()> {
    let repo_root = repo_root()?;
    let report_dir = repo_root.join(REPORT_DIR);
    ensure_ordinary_report_directory(report_dir.as_path())?;
    fs::create_dir_all(report_dir.as_path())
        .with_context(|| format!("failed to create {}", report_dir.display()))?;

    let report = run_continuity_campaign();
    let mut json =
        serde_json::to_vec_pretty(&report).context("failed to encode continuity report")?;
    json.push(b'\n');
    fs::write(report_dir.join("report.json"), json)
        .context("failed to write continuity report JSON")?;
    fs::write(report_dir.join("report.md"), render_continuity_campaign_markdown(&report))
        .context("failed to write continuity report Markdown")?;

    println!("continuity_campaign_report={REPORT_DIR}/report.json");
    println!("continuity_campaign_status={:?}", report.status);
    println!("continuity_campaign_cases={}", report.summary.matrix_case_count);
    ensure_continuity_campaign_passed(&report).context("continuity campaign gate failed")
}

fn repo_root() -> Result<PathBuf> {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|crates| crates.parent())
        .map(PathBuf::from)
        .context("palyra-cli manifest directory is not under the workspace root")
}

fn ensure_ordinary_report_directory(path: &std::path::Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("failed to inspect {}", path.display()))?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        bail!("continuity report directory must be an ordinary directory: {}", path.display());
    }
    Ok(())
}
