use std::{
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use palyra_common::{
    release_evals::{
        build_palyra_trajectory_export, build_regression_eval_pack_index,
        build_release_eval_maturity_scorecard, ensure_release_eval_report_passed,
        evaluate_release_eval_manifest, parse_release_eval_manifest,
        release_eval_replay_bundle_filename,
    },
    replay_bundle::canonical_replay_bundle_bytes,
};
use serde::Serialize;

const DEFAULT_MANIFEST_PATH: &str = "fixtures/golden/release_eval_inventory.json";
const DEFAULT_REPORT_DIR: &str = "target/release-artifacts/release-evals";
const GENERATED_ARTIFACT_TIMESTAMP_MS: i64 = 0;

fn main() -> Result<()> {
    let options = RunnerOptions::parse()?;
    let repo_root = repo_root_from_manifest_dir()?;
    let manifest_path = resolve_repo_relative_path(repo_root.as_path(), options.manifest.as_str());
    let report_dir = resolve_repo_relative_path(repo_root.as_path(), options.report_dir.as_str());
    ensure_report_dir_under_release_artifacts(repo_root.as_path(), report_dir.as_path())?;

    recreate_directory(report_dir.as_path())?;
    let replay_dir = report_dir.join("replay-bundles");
    fs::create_dir_all(replay_dir.as_path())
        .with_context(|| format!("failed to create {}", replay_dir.display()))?;

    let manifest_bytes = fs::read(manifest_path.as_path())
        .with_context(|| format!("failed to read {}", manifest_path.display()))?;
    let manifest = parse_release_eval_manifest(manifest_bytes.as_slice())?;
    let output = evaluate_release_eval_manifest(&manifest);

    let report_path = report_dir.join("report.json");
    write_json_artifact(report_path.as_path(), &output.report)?;

    let maturity_scorecard =
        build_release_eval_maturity_scorecard(&output.report, GENERATED_ARTIFACT_TIMESTAMP_MS);
    let maturity_scorecard_path = report_dir.join("maturity-scorecard.json");
    write_json_artifact(maturity_scorecard_path.as_path(), &maturity_scorecard)?;

    let trajectory_export =
        build_palyra_trajectory_export(&output, GENERATED_ARTIFACT_TIMESTAMP_MS);
    let trajectory_export_path = report_dir.join("trajectory-export.json");
    write_json_artifact(trajectory_export_path.as_path(), &trajectory_export)?;

    let regression_packs =
        build_regression_eval_pack_index(&output, GENERATED_ARTIFACT_TIMESTAMP_MS);
    let regression_packs_path = report_dir.join("regression-packs.json");
    write_json_artifact(regression_packs_path.as_path(), &regression_packs)?;

    for generated in &output.replay_bundles {
        let bundle_filename = release_eval_replay_bundle_filename(generated.case_id.as_str())
            .with_context(|| {
                format!("invalid generated replay bundle case_id {:?}", generated.case_id)
            })?;
        let bundle_path = replay_dir.join(bundle_filename);
        fs::write(
            bundle_path.as_path(),
            canonical_replay_bundle_bytes(&generated.bundle)
                .context("failed to encode generated replay bundle")?,
        )
        .with_context(|| format!("failed to write {}", bundle_path.display()))?;
    }

    println!("release_eval_manifest={}", relative_display_path(&repo_root, &manifest_path));
    println!("release_eval_report={}", report_path.display());
    println!("release_eval_maturity_scorecard={}", maturity_scorecard_path.display());
    println!("release_eval_trajectory_export={}", trajectory_export_path.display());
    println!("release_eval_regression_packs={}", regression_packs_path.display());
    println!("release_eval_replay_bundles={}", output.replay_bundles.len());
    println!("release_eval_status={:?}", output.report.status);

    ensure_release_eval_report_passed(&output.report)
}

#[derive(Debug)]
struct RunnerOptions {
    manifest: String,
    report_dir: String,
}

impl RunnerOptions {
    fn parse() -> Result<Self> {
        let mut manifest = DEFAULT_MANIFEST_PATH.to_owned();
        let mut report_dir = DEFAULT_REPORT_DIR.to_owned();
        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--manifest" => {
                    manifest = args.next().context("expected path after --manifest")?;
                }
                "--report-dir" => {
                    report_dir = args.next().context("expected path after --report-dir")?;
                }
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => {
                    anyhow::bail!(
                        "unknown argument '{other}'. expected --manifest or --report-dir"
                    );
                }
            }
        }
        Ok(Self { manifest, report_dir })
    }
}

fn print_usage() {
    eprintln!(
        "usage: cargo run -p palyra-cli --example run_release_eval_gate -- \
[--manifest <path>] [--report-dir <path>]"
    );
}

fn repo_root_from_manifest_dir() -> Result<PathBuf> {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .context("failed to resolve repository root from Cargo manifest directory")
}

fn resolve_repo_relative_path(repo_root: &Path, raw: &str) -> PathBuf {
    let path = PathBuf::from(raw);
    if path.is_absolute() {
        path
    } else {
        repo_root.join(path)
    }
}

fn recreate_directory(path: &Path) -> Result<()> {
    if path.exists() {
        fs::remove_dir_all(path)
            .with_context(|| format!("failed to remove stale directory {}", path.display()))?;
    }
    fs::create_dir_all(path)
        .with_context(|| format!("failed to create report directory {}", path.display()))
}

fn write_json_artifact<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    fs::write(
        path,
        serde_json::to_vec_pretty(value).context("failed to encode release eval artifact")?,
    )
    .with_context(|| format!("failed to write {}", path.display()))
}

fn ensure_report_dir_under_release_artifacts(repo_root: &Path, report_dir: &Path) -> Result<()> {
    let target_root = repo_root.join("target").join("release-artifacts");
    fs::create_dir_all(target_root.as_path())
        .with_context(|| format!("failed to create {}", target_root.display()))?;
    let canonical_target = target_root
        .canonicalize()
        .with_context(|| format!("failed to canonicalize {}", target_root.display()))?;
    let canonical_report_parent =
        canonical_existing_ancestor(report_dir.parent().unwrap_or(report_dir))
            .with_context(|| format!("failed to resolve parent for {}", report_dir.display()))?;
    if canonical_report_parent.starts_with(canonical_target.as_path()) {
        return Ok(());
    }
    anyhow::bail!(
        "report directory '{}' must be under '{}'",
        report_dir.display(),
        target_root.display()
    );
}

fn canonical_existing_ancestor(path: &Path) -> Result<PathBuf> {
    let mut cursor = path;
    loop {
        if cursor.exists() {
            return cursor
                .canonicalize()
                .with_context(|| format!("failed to canonicalize {}", cursor.display()));
        }
        cursor = cursor
            .parent()
            .with_context(|| format!("no existing ancestor for {}", path.display()))?;
    }
}

fn relative_display_path(repo_root: &Path, path: &Path) -> String {
    path.strip_prefix(repo_root)
        .map(|value| value.display().to_string())
        .unwrap_or_else(|_| path.display().to_string())
}
