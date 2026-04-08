use crate::{output::support_bundle as support_bundle_output, *};

pub(crate) fn run_support_bundle(command: SupportBundleCommand) -> Result<()> {
    match command {
        SupportBundleCommand::Export { output, max_bytes, journal_hash_limit, error_limit } => {
            run_support_bundle_export(output, max_bytes, journal_hash_limit, error_limit)
        }
    }
}

fn run_support_bundle_export(
    output: Option<String>,
    max_bytes: usize,
    journal_hash_limit: usize,
    error_limit: usize,
) -> Result<()> {
    if max_bytes < 2_048 {
        anyhow::bail!("support-bundle max-bytes must be at least 2048");
    }
    let generated_at_unix_ms = now_unix_ms_i64()?;
    let checks = build_doctor_checks();
    let doctor = build_doctor_report(checks.as_slice())?;
    let output_path = resolve_support_bundle_output_path(output, generated_at_unix_ms);

    let build = build_metadata();
    let diagnostics = build_support_bundle_diagnostics_snapshot();
    let mut bundle = SupportBundle {
        schema_version: 1,
        generated_at_unix_ms,
        build: SupportBundleBuildSnapshot {
            version: build.version.to_owned(),
            git_hash: build.git_hash.to_owned(),
            build_profile: build.build_profile.to_owned(),
        },
        platform: SupportBundlePlatformSnapshot {
            os: std::env::consts::OS.to_owned(),
            family: std::env::consts::FAMILY.to_owned(),
            arch: std::env::consts::ARCH.to_owned(),
        },
        doctor,
        recovery: Some(commands::doctor::build_doctor_support_bundle_value()?),
        config: build_support_bundle_config_snapshot(),
        observability: build_support_bundle_observability_snapshot(&diagnostics),
        triage: build_support_bundle_triage_snapshot(),
        diagnostics,
        journal: build_support_bundle_journal_snapshot(journal_hash_limit, error_limit),
        truncated: false,
        warnings: Vec::new(),
    };

    let encoded = encode_support_bundle_with_cap(&mut bundle, max_bytes)?;
    if let Some(parent) = output_path.parent().filter(|value| !value.as_os_str().is_empty()) {
        fs::create_dir_all(parent).with_context(|| {
            format!("failed to create support-bundle directory {}", parent.display())
        })?;
    }
    fs::write(output_path.as_path(), encoded.as_slice())
        .with_context(|| format!("failed to write support bundle {}", output_path.display()))?;
    support_bundle_output::emit_export(&output_path, encoded.len(), &bundle)
}
