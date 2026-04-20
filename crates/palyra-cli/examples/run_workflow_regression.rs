use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use palyra_cli::workflow_regression::{
    build_compat_checklist_status, covered_subsystems_for_profile, load_compat_release_readiness,
    load_workflow_regression_manifest, repo_root_from_manifest_dir, resolve_repo_relative_path,
    validate_compat_release_readiness, validate_workflow_regression_manifest, CompatChecklistState,
    WorkflowRegressionCoverageSummary, WorkflowRegressionExecutionRecord,
    WorkflowRegressionExecutionStatus, WorkflowRegressionRunReport, WorkflowRegressionRunSummary,
};

const DEFAULT_MANIFEST_PATH: &str = "infra/release/workflow-regression-matrix.json";
const DEFAULT_CHECKLIST_PATH: &str = "infra/release/compat-hardening-readiness.json";
const CARGO_BIN_ENV: &str = "PALYRA_WORKFLOW_REGRESSION_CARGO_BIN";

fn main() -> Result<()> {
    let options = RunnerOptions::parse()?;
    let repo_root = repo_root_from_manifest_dir()?;
    let manifest_path =
        resolve_repo_relative_path(repo_root.as_path(), options.manifest_path.as_str());
    let checklist_path =
        resolve_repo_relative_path(repo_root.as_path(), options.checklist_path.as_str());
    let report_dir = resolve_repo_relative_path(repo_root.as_path(), options.report_dir.as_str());

    let manifest = load_workflow_regression_manifest(manifest_path.as_path())?;
    validate_workflow_regression_manifest(&manifest)?;
    if !manifest.profiles.contains_key(options.profile.as_str()) {
        anyhow::bail!(
            "workflow regression profile '{}' is not declared in {}",
            options.profile,
            manifest_path.display()
        );
    }

    let checklist = load_compat_release_readiness(checklist_path.as_path())?;
    validate_compat_release_readiness(&checklist, &manifest, repo_root.as_path())?;

    recreate_directory(report_dir.as_path())?;
    let logs_dir = report_dir.join("logs");
    fs::create_dir_all(logs_dir.as_path())
        .with_context(|| format!("failed to create {}", logs_dir.display()))?;

    let started_at_unix_ms = unix_timestamp_ms();
    let selected_setup_steps = manifest
        .setup_steps
        .iter()
        .filter(|step| step.profiles.iter().any(|value| value == &options.profile))
        .cloned()
        .collect::<Vec<_>>();
    let selected_scenarios = manifest
        .scenarios
        .iter()
        .filter(|scenario| scenario.profiles.iter().any(|value| value == &options.profile))
        .cloned()
        .collect::<Vec<_>>();

    let mut setup_records = Vec::with_capacity(selected_setup_steps.len());
    let mut setup_failed = false;
    for step in selected_setup_steps {
        let record = execute_entry(
            step.id.as_str(),
            step.label.as_str(),
            "setup",
            &[],
            false,
            step.command.as_slice(),
            logs_dir.as_path(),
            repo_root.as_path(),
        )?;
        if record.status == WorkflowRegressionExecutionStatus::Failed {
            setup_failed = true;
        }
        setup_records.push(record);
        if setup_failed {
            break;
        }
    }

    let mut scenario_records = Vec::with_capacity(selected_scenarios.len());
    if setup_failed {
        for scenario in selected_scenarios {
            scenario_records.push(skipped_record(
                scenario.id.as_str(),
                scenario.label.as_str(),
                scenario.category.as_str(),
                scenario.subsystems.as_slice(),
                scenario.chaos,
                scenario.command.as_slice(),
                logs_dir.as_path(),
                "skipped because a setup step failed",
            ));
        }
    } else {
        for scenario in selected_scenarios {
            scenario_records.push(execute_entry(
                scenario.id.as_str(),
                scenario.label.as_str(),
                scenario.category.as_str(),
                scenario.subsystems.as_slice(),
                scenario.chaos,
                scenario.command.as_slice(),
                logs_dir.as_path(),
                repo_root.as_path(),
            )?);
        }
    }

    let completed_at_unix_ms = unix_timestamp_ms();
    let required_subsystems = manifest
        .profiles
        .get(options.profile.as_str())
        .context("selected workflow regression profile is missing after validation")?
        .required_subsystems
        .clone();
    let covered_subsystems = covered_subsystems_for_profile(&manifest, options.profile.as_str())
        .into_iter()
        .collect::<Vec<_>>();
    let missing_subsystems = required_subsystems
        .iter()
        .filter(|entry| !covered_subsystems.iter().any(|covered| covered == *entry))
        .cloned()
        .collect::<Vec<_>>();
    let summary = WorkflowRegressionRunSummary {
        setup_total: setup_records.len(),
        setup_failed: setup_records
            .iter()
            .filter(|entry| entry.status == WorkflowRegressionExecutionStatus::Failed)
            .count(),
        scenario_total: scenario_records.len(),
        scenario_passed: scenario_records
            .iter()
            .filter(|entry| entry.status == WorkflowRegressionExecutionStatus::Passed)
            .count(),
        scenario_failed: scenario_records
            .iter()
            .filter(|entry| entry.status == WorkflowRegressionExecutionStatus::Failed)
            .count(),
        scenario_skipped: scenario_records
            .iter()
            .filter(|entry| entry.status == WorkflowRegressionExecutionStatus::Skipped)
            .count(),
        chaos_total: scenario_records.iter().filter(|entry| entry.chaos).count(),
        chaos_failed: scenario_records
            .iter()
            .filter(|entry| {
                entry.chaos && entry.status == WorkflowRegressionExecutionStatus::Failed
            })
            .count(),
    };
    let coverage = WorkflowRegressionCoverageSummary {
        required_subsystems,
        covered_subsystems,
        missing_subsystems,
    };

    let placeholder_report = WorkflowRegressionRunReport {
        schema_version: 1,
        release_scope: manifest.release_scope.clone(),
        manifest_path: relative_display_path(repo_root.as_path(), manifest_path.as_path()),
        checklist_path: relative_display_path(repo_root.as_path(), checklist_path.as_path()),
        profile: options.profile.clone(),
        started_at_unix_ms,
        completed_at_unix_ms,
        summary,
        coverage,
        setup_steps: setup_records,
        scenarios: scenario_records,
        release_checklist: build_compat_checklist_status_placeholder(),
    };

    let release_checklist =
        build_compat_checklist_status(&checklist, repo_root.as_path(), &placeholder_report)?;
    let report = WorkflowRegressionRunReport { release_checklist, ..placeholder_report };

    let report_path = report_dir.join("report.json");
    let report_bytes = serde_json::to_vec_pretty(&report)
        .context("failed to encode workflow regression report")?;
    fs::write(report_path.as_path(), report_bytes)
        .with_context(|| format!("failed to write {}", report_path.display()))?;

    let checklist_status_path = report_dir.join("compat-readiness-status.json");
    let checklist_bytes = serde_json::to_vec_pretty(&report.release_checklist)
        .context("failed to encode compat checklist status report")?;
    fs::write(checklist_status_path.as_path(), checklist_bytes).with_context(|| {
        format!("failed to write compat checklist status {}", checklist_status_path.display())
    })?;

    println!("workflow_regression_profile={}", report.profile);
    println!("workflow_regression_report={}", report_path.display());
    println!("compat_checklist_status={}", checklist_status_path.display());
    println!("scenarios_passed={}", report.summary.scenario_passed);
    println!("scenarios_failed={}", report.summary.scenario_failed);
    println!(
        "compat_readiness_state={}",
        serde_json::to_string(&report.release_checklist.state)?.trim_matches('"')
    );

    if report.summary.setup_failed > 0 || report.summary.scenario_failed > 0 {
        anyhow::bail!(
            "workflow regression profile '{}' reported {} setup failures and {} scenario failures",
            report.profile,
            report.summary.setup_failed,
            report.summary.scenario_failed
        );
    }
    if report.release_checklist.state == CompatChecklistState::NeedsAttention {
        anyhow::bail!(
            "compat release checklist validation reported failures for profile '{}'",
            report.profile
        );
    }

    Ok(())
}

#[derive(Debug)]
struct RunnerOptions {
    manifest_path: String,
    checklist_path: String,
    profile: String,
    report_dir: String,
}

impl RunnerOptions {
    fn parse() -> Result<Self> {
        let mut manifest_path = DEFAULT_MANIFEST_PATH.to_owned();
        let mut checklist_path = DEFAULT_CHECKLIST_PATH.to_owned();
        let mut profile = "fast".to_owned();
        let mut report_dir = String::new();
        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--manifest" => {
                    manifest_path = args.next().context("expected path after --manifest")?;
                }
                "--checklist" => {
                    checklist_path = args.next().context("expected path after --checklist")?;
                }
                "--profile" => {
                    profile = args.next().context("expected profile after --profile")?;
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
                        "unknown argument '{other}'. expected --manifest, --checklist, --profile, or --report-dir"
                    );
                }
            }
        }

        if report_dir.trim().is_empty() {
            report_dir = format!("target/release-artifacts/workflow-regression/{profile}");
        }

        Ok(Self { manifest_path, checklist_path, profile, report_dir })
    }
}

fn print_usage() {
    eprintln!(
        "usage: cargo run -p palyra-cli --example run_workflow_regression -- \
--profile <fast|full> [--manifest <path>] [--checklist <path>] [--report-dir <path>]"
    );
}

fn recreate_directory(path: &Path) -> Result<()> {
    if path.exists() {
        fs::remove_dir_all(path).with_context(|| {
            format!("failed to remove stale report directory {}", path.display())
        })?;
    }
    fs::create_dir_all(path)
        .with_context(|| format!("failed to create report directory {}", path.display()))
}

fn execute_entry(
    id: &str,
    label: &str,
    category: &str,
    subsystems: &[String],
    chaos: bool,
    command: &[String],
    logs_dir: &Path,
    repo_root: &Path,
) -> Result<WorkflowRegressionExecutionRecord> {
    let resolved_command = resolve_command(command);
    let log_path = logs_dir.join(format!("{id}.log"));
    let started_at = Instant::now();
    let output = Command::new(&resolved_command[0])
        .args(&resolved_command[1..])
        .current_dir(repo_root)
        .stdin(Stdio::null())
        .output()
        .with_context(|| format!("failed to run workflow regression command '{}'", id))?;
    let duration_ms = u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    let log_contents = format!(
        "$ {}\n\n[stdout]\n{}\n\n[stderr]\n{}\n",
        shell_render_command(resolved_command.as_slice()),
        stdout,
        stderr
    );
    fs::write(log_path.as_path(), log_contents.as_bytes())
        .with_context(|| format!("failed to write {}", log_path.display()))?;

    Ok(WorkflowRegressionExecutionRecord {
        id: id.to_owned(),
        label: label.to_owned(),
        category: category.to_owned(),
        subsystems: subsystems.to_vec(),
        chaos,
        command: resolved_command.clone(),
        status: if output.status.success() {
            WorkflowRegressionExecutionStatus::Passed
        } else {
            WorkflowRegressionExecutionStatus::Failed
        },
        duration_ms,
        log_path: log_path.display().to_string(),
        exit_code: output.status.code(),
        output_excerpt: build_output_excerpt(stdout.as_str(), stderr.as_str()),
    })
}

fn skipped_record(
    id: &str,
    label: &str,
    category: &str,
    subsystems: &[String],
    chaos: bool,
    command: &[String],
    logs_dir: &Path,
    reason: &str,
) -> WorkflowRegressionExecutionRecord {
    let log_path = logs_dir.join(format!("{id}.log"));
    let _ = fs::write(log_path.as_path(), reason.as_bytes());
    WorkflowRegressionExecutionRecord {
        id: id.to_owned(),
        label: label.to_owned(),
        category: category.to_owned(),
        subsystems: subsystems.to_vec(),
        chaos,
        command: command.to_vec(),
        status: WorkflowRegressionExecutionStatus::Skipped,
        duration_ms: 0,
        log_path: log_path.display().to_string(),
        exit_code: None,
        output_excerpt: reason.to_owned(),
    }
}

fn resolve_command(command: &[String]) -> Vec<String> {
    if command.first().is_some_and(|value| value == "cargo") {
        if let Some(value) = env::var_os(CARGO_BIN_ENV) {
            let mut resolved = command.to_vec();
            resolved[0] = PathBuf::from(value).display().to_string();
            return resolved;
        }
    }
    command.to_vec()
}

fn shell_render_command(command: &[String]) -> String {
    command
        .iter()
        .map(|value| if value.contains(' ') { format!("\"{value}\"") } else { value.clone() })
        .collect::<Vec<_>>()
        .join(" ")
}

fn build_output_excerpt(stdout: &str, stderr: &str) -> String {
    let combined = format!("[stdout]\n{stdout}\n[stderr]\n{stderr}");
    let lines = combined.lines().collect::<Vec<_>>();
    let start = lines.len().saturating_sub(40);
    let excerpt = lines[start..].join("\n");
    if excerpt.len() <= 4000 {
        excerpt
    } else {
        excerpt[excerpt.len().saturating_sub(4000)..].to_owned()
    }
}

fn relative_display_path(repo_root: &Path, path: &Path) -> String {
    path.strip_prefix(repo_root)
        .map(|value| value.display().to_string())
        .unwrap_or_else(|_| path.display().to_string())
}

fn unix_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| u64::try_from(value.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn build_compat_checklist_status_placeholder(
) -> palyra_cli::workflow_regression::CompatChecklistStatusReport {
    palyra_cli::workflow_regression::CompatChecklistStatusReport {
        state: CompatChecklistState::ProfileValidatedPendingExternalEvidence,
        validated: 0,
        pending: 0,
        failed: 0,
        evidence: Vec::new(),
    }
}
