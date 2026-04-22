use std::{collections::BTreeMap, path::PathBuf};

use palyra_common::deployment_profiles::{
    canonical_deployment_profiles, deployment_profile_manifest, derive_deployment_profile,
    DeploymentProfileId, DeploymentProfileManifest,
};
use serde::Serialize;

use crate::*;

pub(crate) fn run_deployment(command: DeploymentCommand) -> Result<()> {
    match command {
        DeploymentCommand::Profiles { json } => run_profiles(json),
        DeploymentCommand::Manifest { deployment_profile, output } => {
            run_manifest(deployment_profile, output)
        }
        DeploymentCommand::Preflight { deployment_profile, path, json } => {
            run_preflight(deployment_profile, path, json)
        }
        DeploymentCommand::Recipe { deployment_profile, output_dir } => {
            run_recipe(deployment_profile, output_dir)
        }
        DeploymentCommand::UpgradeSmoke { deployment_profile, path, json } => {
            run_upgrade_smoke(deployment_profile, path, json)
        }
        DeploymentCommand::PromotionCheck { deployment_profile, gates, json } => {
            run_promotion_check(deployment_profile, gates, json)
        }
        DeploymentCommand::RollbackPlan { deployment_profile, output, json } => {
            run_rollback_plan(deployment_profile, output, json)
        }
    }
}

fn run_profiles(json_output: bool) -> Result<()> {
    let profiles = canonical_deployment_profiles();
    if json_output || output::preferred_json(false) {
        return output::print_json_pretty(
            &profiles,
            "failed to encode deployment profiles as JSON",
        );
    }
    for profile in profiles {
        println!(
            "deployment.profile id={} mode={} bind_profile={} capabilities={} blockers={}",
            profile.profile_id,
            profile.deployment_mode,
            profile.bind_profile,
            profile.capabilities.len(),
            profile.blockers.len()
        );
        println!(
            "deployment.profile.summary id={} {}",
            profile.profile_id, profile.operator_summary
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn run_manifest(deployment_profile: DeploymentProfileArg, output: Option<String>) -> Result<()> {
    let profile_id = deployment_profile_id_from_arg(deployment_profile);
    let manifest = deployment_profile_manifest(profile_id);
    if let Some(output) = output {
        let output_path = PathBuf::from(output);
        write_json_file(output_path.as_path(), &manifest)?;
        println!(
            "deployment.manifest path={} profile={}",
            output_path.display(),
            manifest.profile_id
        );
        return std::io::stdout().flush().context("stdout flush failed");
    }
    output::print_json_pretty(&manifest, "failed to encode deployment profile manifest as JSON")
}

fn run_preflight(
    deployment_profile: Option<DeploymentProfileArg>,
    path: Option<String>,
    json_output: bool,
) -> Result<()> {
    let report = build_preflight_report(deployment_profile, path)?;
    emit_preflight_report(&report, json_output)
}

fn run_recipe(deployment_profile: DeploymentProfileArg, output_dir: String) -> Result<()> {
    let profile_id = deployment_profile_id_from_arg(deployment_profile);
    let bundle = build_recipe_bundle(profile_id)?;
    let output_root = PathBuf::from(output_dir);
    fs::create_dir_all(output_root.as_path()).with_context(|| {
        format!("failed to create deployment recipe directory {}", output_root.display())
    })?;
    for (relative_path, contents) in &bundle.files {
        let path = output_root.join(relative_path);
        if let Some(parent) = path.parent().filter(|value| !value.as_os_str().is_empty()) {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create deployment recipe directory {}", parent.display())
            })?;
        }
        fs::write(path.as_path(), contents)
            .with_context(|| format!("failed to write deployment recipe {}", path.display()))?;
    }
    println!(
        "deployment.recipe profile={} output_dir={} files={}",
        bundle.profile_id,
        output_root.display(),
        bundle.files.len()
    );
    for relative_path in bundle.files.keys() {
        println!("deployment.recipe.file={relative_path}");
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn run_upgrade_smoke(
    deployment_profile: DeploymentProfileArg,
    path: Option<String>,
    json_output: bool,
) -> Result<()> {
    let profile_id = deployment_profile_id_from_arg(deployment_profile);
    let preflight = build_preflight_report(Some(deployment_profile), path)?;
    let promotion = build_promotion_report(profile_id, None)?;
    let rollback = build_rollback_plan(profile_id);
    let recipe = build_recipe_bundle(profile_id)?;
    let status = if preflight.blocking_failures == 0 && promotion.blocking_failures == 0 {
        "passed"
    } else {
        "blocked"
    };
    let report = UpgradeSmokeReport {
        schema_version: 1,
        profile_id: profile_id.as_str().to_owned(),
        status: status.to_owned(),
        preflight,
        promotion,
        rollback,
        recipe_files: recipe.files.keys().cloned().collect(),
    };
    if json_output || output::preferred_json(false) {
        return output::print_json_pretty(&report, "failed to encode upgrade smoke report as JSON");
    }
    println!(
        "deployment.upgrade_smoke profile={} status={} recipe_files={} blockers={}",
        report.profile_id,
        report.status,
        report.recipe_files.len(),
        report.preflight.blocking_failures + report.promotion.blocking_failures
    );
    for check in &report.preflight.checks {
        println!(
            "deployment.upgrade_smoke.preflight id={} status={} required={} detail={}",
            check.id, check.status, check.required, check.detail
        );
    }
    for gate in &report.promotion.gates {
        println!(
            "deployment.upgrade_smoke.gate id={} status={} detail={}",
            gate.id, gate.status, gate.detail
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn run_promotion_check(
    deployment_profile: DeploymentProfileArg,
    gates: Option<String>,
    json_output: bool,
) -> Result<()> {
    let report = build_promotion_report(deployment_profile_id_from_arg(deployment_profile), gates)?;
    if json_output || output::preferred_json(false) {
        return output::print_json_pretty(&report, "failed to encode promotion report as JSON");
    }
    println!(
        "deployment.promotion profile={} status={} gates={} blockers={}",
        report.profile_id,
        report.status,
        report.gates.len(),
        report.blocking_failures
    );
    for gate in &report.gates {
        println!(
            "deployment.promotion.gate id={} status={} required={} detail={}",
            gate.id, gate.status, gate.required, gate.detail
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn run_rollback_plan(
    deployment_profile: DeploymentProfileArg,
    output: Option<String>,
    json_output: bool,
) -> Result<()> {
    let plan = build_rollback_plan(deployment_profile_id_from_arg(deployment_profile));
    if let Some(output) = output {
        let output_path = PathBuf::from(output);
        write_json_file(output_path.as_path(), &plan)?;
        println!(
            "deployment.rollback_plan path={} profile={} steps={}",
            output_path.display(),
            plan.profile_id,
            plan.steps.len()
        );
        return std::io::stdout().flush().context("stdout flush failed");
    }
    if json_output || output::preferred_json(false) {
        return output::print_json_pretty(&plan, "failed to encode rollback plan as JSON");
    }
    println!(
        "deployment.rollback_plan profile={} steps={} guarded_scopes={}",
        plan.profile_id,
        plan.steps.len(),
        plan.guarded_scopes.join(",")
    );
    for step in &plan.steps {
        println!(
            "deployment.rollback_plan.step order={} id={} action={}",
            step.order, step.id, step.action
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn build_preflight_report(
    deployment_profile: Option<DeploymentProfileArg>,
    path: Option<String>,
) -> Result<DeploymentPreflightReport> {
    let config_path = path
        .map(PathBuf::from)
        .or_else(|| {
            app::current_root_context()
                .and_then(|context| context.config_path().map(std::path::Path::to_path_buf))
        })
        .unwrap_or_else(|| PathBuf::from("palyra.toml"));
    let document = if config_path.exists() {
        Some(
            load_document_from_existing_path(config_path.as_path())
                .with_context(|| format!("failed to parse {}", config_path.display()))?
                .0,
        )
    } else {
        None
    };
    let configured_profile =
        document.as_ref().and_then(|value| toml_string_at_path(value, "deployment.profile"));
    let configured_mode =
        document.as_ref().and_then(|value| toml_string_at_path(value, "deployment.mode"));
    let worker_rollout = document
        .as_ref()
        .and_then(|value| toml_bool_at_path(value, "feature_rollouts.networked_workers"))
        .unwrap_or(false);
    let profile_id = deployment_profile.map(deployment_profile_id_from_arg).unwrap_or_else(|| {
        derive_deployment_profile(
            configured_profile.as_deref(),
            configured_mode.as_deref(),
            worker_rollout,
        )
    });
    let manifest = deployment_profile_manifest(profile_id);
    let mut checks = Vec::new();
    for preflight in &manifest.health_preflights {
        checks.push(evaluate_preflight_check(
            preflight.id.as_str(),
            preflight.required,
            document.as_ref(),
            config_path.as_path(),
            &manifest,
        ));
    }
    let blocking_failures =
        checks.iter().filter(|check| check.required && check.status == "blocked").count();
    let warnings = checks.iter().filter(|check| check.status == "warning").count();
    let status = if blocking_failures > 0 { "blocked" } else { "passed" };
    Ok(DeploymentPreflightReport {
        schema_version: 1,
        profile_id: profile_id.as_str().to_owned(),
        config_path: config_path.display().to_string(),
        status: status.to_owned(),
        blocking_failures,
        warnings,
        manifest,
        checks,
    })
}

fn evaluate_preflight_check(
    id: &str,
    required: bool,
    document: Option<&toml::Value>,
    config_path: &std::path::Path,
    manifest: &DeploymentProfileManifest,
) -> DeploymentPreflightCheck {
    let Some(document) = document else {
        return DeploymentPreflightCheck {
            id: id.to_owned(),
            required,
            status: if required { "blocked" } else { "warning" }.to_owned(),
            detail: format!("config path {} does not exist yet", config_path.display()),
            remediation: Some(format!(
                "run palyra setup --deployment-profile {} --path {}",
                manifest.profile_id,
                config_path.display()
            )),
        };
    };
    let check = match id {
        "config_schema" => validate_daemon_compatible_document(document)
            .map(|()| ("ok", "config matches the daemon schema".to_owned(), None))
            .unwrap_or_else(|error| {
                (
                    "blocked",
                    format!(
                        "config schema validation failed: {}",
                        sanitize_diagnostic_error(error.to_string().as_str())
                    ),
                    Some("run palyra config validate after repairing the config".to_owned()),
                )
            }),
        "loopback_bind" => match toml_string_at_path(document, "gateway.bind_profile").as_deref() {
            Some("loopback_only" | "loopback") => {
                ("ok", "gateway bind profile is loopback-only".to_owned(), None)
            }
            Some(value) => (
                "blocked",
                format!("gateway bind profile is {value}, expected loopback_only"),
                Some(
                    "use palyra configure --section gateway --bind-profile loopback-only"
                        .to_owned(),
                ),
            ),
            None => (
                "blocked",
                "gateway.bind_profile is missing".to_owned(),
                Some("regenerate config with palyra setup".to_owned()),
            ),
        },
        "bind_posture" => evaluate_bind_posture(document),
        "storage_paths" | "artifact_transport" => {
            let vault = toml_string_at_path(document, "storage.vault_dir");
            let identity = toml_string_at_path(document, "gateway.identity_store_dir");
            if vault.is_some() && identity.is_some() {
                ("ok", "vault and identity store paths are configured".to_owned(), None)
            } else {
                (
                    "blocked",
                    "storage.vault_dir and gateway.identity_store_dir must both be configured"
                        .to_owned(),
                    Some("rerun setup for the selected deployment profile".to_owned()),
                )
            }
        }
        "systemd_or_compose" => (
            "warning",
            "choose either generated systemd units or Compose recipes before service rollout"
                .to_owned(),
            Some("run palyra deployment recipe for the selected profile".to_owned()),
        ),
        "model_auth" => {
            let configured =
                toml_string_at_path(document, "model_provider.openai_api_key_vault_ref").is_some()
                    || toml_string_at_path(document, "model_provider.anthropic_api_key_vault_ref")
                        .is_some()
                    || toml_string_at_path(document, "model_provider.auth_profile_id").is_some();
            if configured {
                ("ok", "model-provider credentials are configured".to_owned(), None)
            } else {
                (
                    "warning",
                    "model-provider credentials are not configured yet".to_owned(),
                    Some("run palyra configure --section auth-model".to_owned()),
                )
            }
        }
        "worker_rollout" => {
            let enabled =
                toml_bool_at_path(document, "feature_rollouts.networked_workers").unwrap_or(false);
            let mode = toml_string_at_path(document, "networked_workers.mode")
                .unwrap_or_else(|| "disabled".to_owned());
            if enabled && mode != "disabled" {
                ("ok", format!("networked worker rollout is enabled in {mode} mode"), None)
            } else {
                (
                    "blocked",
                    "worker-enabled profile requires feature_rollouts.networked_workers and networked_workers.mode".to_owned(),
                    Some("rerun setup/configure with --deployment-profile worker-enabled".to_owned()),
                )
            }
        }
        "attestation" => {
            if toml_bool_at_path(document, "networked_workers.require_attestation").unwrap_or(false)
            {
                ("ok", "worker attestation is required".to_owned(), None)
            } else {
                (
                    "blocked",
                    "networked_workers.require_attestation must be true".to_owned(),
                    Some("set networked_workers.require_attestation=true".to_owned()),
                )
            }
        }
        "orphan_reaper" => {
            if toml_integer_at_path(document, "networked_workers.lease_ttl_ms").unwrap_or(0) > 0 {
                ("ok", "worker leases have a bounded TTL".to_owned(), None)
            } else {
                (
                    "blocked",
                    "networked_workers.lease_ttl_ms must be positive".to_owned(),
                    Some("set a bounded worker lease TTL".to_owned()),
                )
            }
        }
        _ => ("ok", "preflight declared by profile manifest".to_owned(), None),
    };
    let (status, detail, remediation) = check;
    DeploymentPreflightCheck {
        id: id.to_owned(),
        required,
        status: status.to_owned(),
        detail,
        remediation,
    }
}

fn evaluate_bind_posture(document: &toml::Value) -> (&'static str, String, Option<String>) {
    let bind_profile =
        toml_string_at_path(document, "gateway.bind_profile").unwrap_or_else(|| "unset".to_owned());
    let tls_enabled = toml_bool_at_path(document, "gateway.tls.enabled").unwrap_or(false);
    let admin_auth_required = toml_bool_at_path(document, "admin.require_auth").unwrap_or(false);
    let public_bind_ack =
        toml_bool_at_path(document, "deployment.dangerous_remote_bind_ack").unwrap_or(false);
    if bind_profile == "loopback_only" || bind_profile == "loopback" {
        return ("ok", "loopback-only bind posture is active".to_owned(), None);
    }
    if bind_profile == "public_tls" && tls_enabled && admin_auth_required && public_bind_ack {
        return (
            "warning",
            "public TLS posture is configured; runtime still requires PALYRA_GATEWAY_DANGEROUS_REMOTE_BIND_ACK=true".to_owned(),
            Some("verify the runtime environment acknowledgement before start".to_owned()),
        );
    }
    (
        "blocked",
        "public bind posture is incomplete".to_owned(),
        Some(
            "use loopback_only or configure TLS, admin auth, and dangerous bind acknowledgement"
                .to_owned(),
        ),
    )
}

fn emit_preflight_report(report: &DeploymentPreflightReport, json_output: bool) -> Result<()> {
    if json_output || output::preferred_json(false) {
        return output::print_json_pretty(report, "failed to encode deployment preflight as JSON");
    }
    println!(
        "deployment.preflight profile={} status={} config_path={} blockers={} warnings={}",
        report.profile_id,
        report.status,
        report.config_path,
        report.blocking_failures,
        report.warnings
    );
    for check in &report.checks {
        println!(
            "deployment.preflight.check id={} status={} required={} detail={}",
            check.id, check.status, check.required, check.detail
        );
        if let Some(remediation) = check.remediation.as_deref() {
            println!("deployment.preflight.remediation id={} {}", check.id, remediation);
        }
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn build_promotion_report(
    profile_id: DeploymentProfileId,
    gates_path: Option<String>,
) -> Result<PromotionReport> {
    let gates_source =
        gates_path.unwrap_or_else(|| "infra/release/deployment-promotion-gates.json".to_owned());
    let mut gates = built_in_promotion_gates(profile_id);
    if PathBuf::from(gates_source.as_str()).exists() {
        let raw = fs::read_to_string(gates_source.as_str())
            .with_context(|| format!("failed to read promotion gates {gates_source}"))?;
        let parsed: serde_json::Value = serde_json::from_str(raw.as_str())
            .with_context(|| format!("failed to parse promotion gates {gates_source}"))?;
        if let Some(items) = parsed.get("gates").and_then(serde_json::Value::as_array) {
            gates = items
                .iter()
                .filter(|item| promotion_gate_applies_to_profile(item, profile_id))
                .filter_map(|item| {
                    Some(PromotionGate {
                        id: item.get("id")?.as_str()?.to_owned(),
                        required: item
                            .get("required")
                            .and_then(serde_json::Value::as_bool)
                            .unwrap_or(true),
                        status: item
                            .get("default_status")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("manual")
                            .to_owned(),
                        detail: item.get("summary")?.as_str()?.to_owned(),
                    })
                })
                .collect();
        }
    }
    let blocking_failures =
        gates.iter().filter(|gate| gate.required && gate.status == "blocked").count();
    let status = if blocking_failures == 0 { "ready_for_staging" } else { "blocked" };
    Ok(PromotionReport {
        schema_version: 1,
        profile_id: profile_id.as_str().to_owned(),
        status: status.to_owned(),
        blocking_failures,
        gates_source,
        gates,
    })
}

fn promotion_gate_applies_to_profile(
    item: &serde_json::Value,
    profile_id: DeploymentProfileId,
) -> bool {
    let Some(profiles) = item.get("profiles") else {
        return true;
    };
    if let Some(profile) = profiles.as_str() {
        return profile == profile_id.as_str();
    }
    let Some(profile_list) = profiles.as_array() else {
        return true;
    };
    profile_list
        .iter()
        .filter_map(serde_json::Value::as_str)
        .any(|profile| profile == profile_id.as_str())
}

fn built_in_promotion_gates(profile_id: DeploymentProfileId) -> Vec<PromotionGate> {
    let mut gates = vec![
        PromotionGate {
            id: "deployment_preflight".to_owned(),
            required: true,
            status: "manual".to_owned(),
            detail: "deployment preflight passes for the selected profile".to_owned(),
        },
        PromotionGate {
            id: "support_bundle_export".to_owned(),
            required: true,
            status: "manual".to_owned(),
            detail: "support bundle export captures config, replay, flow, and worker posture"
                .to_owned(),
        },
        PromotionGate {
            id: "rollback_plan_reviewed".to_owned(),
            required: true,
            status: "manual".to_owned(),
            detail: "rollback plan was generated and reviewed before promotion".to_owned(),
        },
    ];
    if profile_id == DeploymentProfileId::WorkerEnabled {
        gates.push(PromotionGate {
            id: "worker_attestation_digest".to_owned(),
            required: true,
            status: "blocked".to_owned(),
            detail:
                "worker-enabled promotion requires an expected worker digest before broad rollout"
                    .to_owned(),
        });
    }
    gates
}

fn build_rollback_plan(profile_id: DeploymentProfileId) -> RollbackPlan {
    let mut guarded_scopes = vec![
        "config".to_owned(),
        "journal_migrations".to_owned(),
        "flow_tables".to_owned(),
        "replay_schema".to_owned(),
        "feature_rollouts".to_owned(),
    ];
    if profile_id == DeploymentProfileId::WorkerEnabled {
        guarded_scopes.push("networked_worker_leases".to_owned());
        guarded_scopes.push("worker_artifacts".to_owned());
    }
    RollbackPlan {
        schema_version: 1,
        profile_id: profile_id.as_str().to_owned(),
        guarded_scopes,
        steps: vec![
            rollback_step(
                1,
                "freeze_rollout",
                "pause promotion and stop enabling new guarded runtime capabilities",
            ),
            rollback_step(
                2,
                "export_support_bundle",
                "export a support bundle before mutating config or service state",
            ),
            rollback_step(
                3,
                "restore_config",
                "restore the previous config backup or release profile manifest",
            ),
            rollback_step(
                4,
                "migrate_back",
                "run config migration validation from the rollback binary before restart",
            ),
            rollback_step(
                5,
                "drain_workers",
                "drain or reap worker leases before disabling worker rollout flags",
            ),
            rollback_step(
                6,
                "restart_services",
                "restart palyrad and optional browserd/workerd services",
            ),
            rollback_step(
                7,
                "verify",
                "run doctor, deployment preflight, replay smoke, and gateway status",
            ),
        ],
    }
}

fn rollback_step(order: u32, id: &str, action: &str) -> RollbackStep {
    RollbackStep { order, id: id.to_owned(), action: action.to_owned() }
}

fn build_recipe_bundle(profile_id: DeploymentProfileId) -> Result<DeploymentRecipeBundle> {
    let manifest = deployment_profile_manifest(profile_id);
    let manifest_json = serde_json::to_string_pretty(&manifest)
        .context("failed to encode deployment profile manifest")?;
    let mut files = BTreeMap::new();
    files.insert("profile-manifest.json".to_owned(), manifest_json);
    files.insert("env/palyra.env.example".to_owned(), render_env_template(&manifest));
    files.insert("docker/Dockerfile.palyra".to_owned(), render_dockerfile());
    files.insert(format!("compose/{}.yml", manifest.profile_id), render_compose(&manifest));
    files.insert("systemd/palyrad.service".to_owned(), render_palyrad_systemd(&manifest));
    files.insert("systemd/palyra-browserd.service".to_owned(), render_browserd_systemd());
    if profile_id == DeploymentProfileId::WorkerEnabled {
        files.insert("systemd/palyra-workerd.service".to_owned(), render_workerd_systemd());
    }
    files.insert(
        "release/rollback-plan.json".to_owned(),
        serde_json::to_string_pretty(&build_rollback_plan(profile_id))
            .context("failed to encode rollback plan")?,
    );
    Ok(DeploymentRecipeBundle { profile_id: manifest.profile_id, files })
}

fn render_env_template(manifest: &DeploymentProfileManifest) -> String {
    let worker_enabled = manifest.profile_id == DeploymentProfileId::WorkerEnabled.as_str();
    format!(
        "\
PALYRA_CONFIG=/etc/palyra/palyra.toml
PALYRA_STATE_ROOT=/var/lib/palyra
PALYRA_DEPLOYMENT_PROFILE={}
PALYRA_DEPLOYMENT_MODE={}
PALYRA_GATEWAY_BIND_PROFILE={}
PALYRA_ADMIN_REQUIRE_AUTH=true
PALYRA_GATEWAY_DANGEROUS_REMOTE_BIND_ACK=false
PALYRA_GATEWAY_TLS_ENABLED=false
PALYRA_BROWSER_SERVICE_ENABLED=false
PALYRA_BROWSER_SERVICE_ENDPOINT=http://127.0.0.1:7543
PALYRA_BROWSERD_STATE_DIR=/var/lib/palyra/browserd
PALYRA_BROWSERD_ENGINE_MODE=chromium
# Set PALYRA_BROWSERD_STATE_ENCRYPTION_KEY through the service manager before enabling browser profiles.
PALYRA_EXPERIMENTAL_NETWORKED_WORKERS={}
PALYRA_EXPERIMENTAL_EXECUTION_BACKEND_NETWORKED_WORKER=false
# Set networked_workers.expected_*_digest_sha256 in palyra.toml before worker promotion.
",
        manifest.profile_id,
        manifest.deployment_mode,
        manifest.bind_profile,
        worker_enabled
    )
}

fn render_dockerfile() -> String {
    "\
FROM debian:bookworm-slim
RUN useradd --system --create-home --home-dir /var/lib/palyra palyra
WORKDIR /opt/palyra
COPY palyrad palyra-browserd palyra /opt/palyra/
COPY web /opt/palyra/web
RUN chmod 0755 /opt/palyra/palyrad /opt/palyra/palyra-browserd /opt/palyra/palyra
USER palyra
ENV PALYRA_STATE_ROOT=/var/lib/palyra
EXPOSE 7142 7443
HEALTHCHECK --interval=30s --timeout=5s --retries=3 CMD /opt/palyra/palyra health --url http://127.0.0.1:7142 || exit 1
ENTRYPOINT [\"/opt/palyra/palyrad\"]
"
    .to_owned()
}

fn render_compose(manifest: &DeploymentProfileManifest) -> String {
    let worker_service = if manifest.profile_id == DeploymentProfileId::WorkerEnabled.as_str() {
        "\
  palyra-workerd:
    image: palyra:local
    command: [\"/opt/palyra/palyra-workerd\"]
    env_file: ../env/palyra.env
    depends_on:
      palyrad:
        condition: service_healthy
    volumes:
      - palyra-state:/var/lib/palyra
    restart: unless-stopped
"
    } else {
        ""
    };
    format!(
        "\
services:
  palyrad:
    build:
      context: ../..
      dockerfile: infra/deployment/docker/Dockerfile.palyra
    image: palyra:local
    env_file: ../env/palyra.env
    ports:
      - \"127.0.0.1:7142:7142\"
      - \"127.0.0.1:7443:7443\"
    volumes:
      - palyra-state:/var/lib/palyra
      - palyra-config:/etc/palyra
    healthcheck:
      test: [\"CMD\", \"/opt/palyra/palyra\", \"health\", \"--url\", \"http://127.0.0.1:7142\"]
      interval: 30s
      timeout: 5s
      retries: 3
    restart: unless-stopped
  palyra-browserd:
    image: palyra:local
    command: [\"/opt/palyra/palyra-browserd\", \"--bind\", \"127.0.0.1\", \"--port\", \"7143\", \"--grpc-bind\", \"127.0.0.1\", \"--grpc-port\", \"7543\"]
    profiles: [\"browser\"]
    env_file: ../env/palyra.env
    volumes:
      - palyra-state:/var/lib/palyra
    restart: unless-stopped
{worker_service}
volumes:
  palyra-state:
  palyra-config:
"
    )
}

fn render_palyrad_systemd(manifest: &DeploymentProfileManifest) -> String {
    format!(
        "\
[Unit]
Description=Palyra daemon ({})
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=palyra
Group=palyra
EnvironmentFile=/etc/palyra/palyra.env
ExecStart=/opt/palyra/palyrad
Restart=on-failure
RestartSec=5s
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/palyra /etc/palyra

[Install]
WantedBy=multi-user.target
",
        manifest.profile_id
    )
}

fn render_browserd_systemd() -> String {
    "\
[Unit]
Description=Palyra browser service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=palyra
Group=palyra
EnvironmentFile=/etc/palyra/palyra.env
ExecStart=/opt/palyra/palyra-browserd --bind 127.0.0.1 --port 7143 --grpc-bind 127.0.0.1 --grpc-port 7543
Restart=on-failure
RestartSec=5s
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/palyra

[Install]
WantedBy=multi-user.target
"
    .to_owned()
}

fn render_workerd_systemd() -> String {
    "\
[Unit]
Description=Palyra worker daemon
After=network-online.target palyrad.service
Wants=network-online.target
Requires=palyrad.service

[Service]
Type=simple
User=palyra
Group=palyra
EnvironmentFile=/etc/palyra/palyra.env
ExecStart=/opt/palyra/palyra-workerd
Restart=on-failure
RestartSec=5s
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/palyra

[Install]
WantedBy=multi-user.target
"
    .to_owned()
}

fn write_json_file<T: Serialize>(path: &std::path::Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent().filter(|value| !value.as_os_str().is_empty()) {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(value).context("failed to encode JSON file")?;
    fs::write(path, bytes).with_context(|| format!("failed to write {}", path.display()))
}

fn toml_string_at_path(document: &toml::Value, path: &str) -> Option<String> {
    toml_value_at_path(document, path)
        .and_then(toml::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn toml_bool_at_path(document: &toml::Value, path: &str) -> Option<bool> {
    toml_value_at_path(document, path).and_then(toml::Value::as_bool)
}

fn toml_integer_at_path(document: &toml::Value, path: &str) -> Option<i64> {
    toml_value_at_path(document, path).and_then(toml::Value::as_integer)
}

fn toml_value_at_path<'a>(document: &'a toml::Value, path: &str) -> Option<&'a toml::Value> {
    let mut cursor = document;
    for segment in path.split('.') {
        cursor = cursor.get(segment)?;
    }
    Some(cursor)
}

#[derive(Debug, Serialize)]
struct DeploymentPreflightReport {
    schema_version: u32,
    profile_id: String,
    config_path: String,
    status: String,
    blocking_failures: usize,
    warnings: usize,
    manifest: DeploymentProfileManifest,
    checks: Vec<DeploymentPreflightCheck>,
}

#[derive(Debug, Serialize)]
struct DeploymentPreflightCheck {
    id: String,
    required: bool,
    status: String,
    detail: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    remediation: Option<String>,
}

#[derive(Debug, Serialize)]
struct DeploymentRecipeBundle {
    profile_id: String,
    files: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
struct PromotionReport {
    schema_version: u32,
    profile_id: String,
    status: String,
    blocking_failures: usize,
    gates_source: String,
    gates: Vec<PromotionGate>,
}

#[derive(Debug, Serialize)]
struct PromotionGate {
    id: String,
    required: bool,
    status: String,
    detail: String,
}

#[derive(Debug, Serialize)]
struct RollbackPlan {
    schema_version: u32,
    profile_id: String,
    guarded_scopes: Vec<String>,
    steps: Vec<RollbackStep>,
}

#[derive(Debug, Serialize)]
struct RollbackStep {
    order: u32,
    id: String,
    action: String,
}

#[derive(Debug, Serialize)]
struct UpgradeSmokeReport {
    schema_version: u32,
    profile_id: String,
    status: String,
    preflight: DeploymentPreflightReport,
    promotion: PromotionReport,
    rollback: RollbackPlan,
    recipe_files: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use palyra_common::deployment_profiles::DeploymentProfileDefaultValue;

    #[test]
    fn recipe_bundle_for_worker_profile_contains_worker_service() {
        let bundle = build_recipe_bundle(DeploymentProfileId::WorkerEnabled)
            .expect("recipe bundle should render");
        assert!(bundle.files.contains_key("systemd/palyra-workerd.service"));
        assert!(bundle
            .files
            .get("compose/worker-enabled.yml")
            .expect("compose file should exist")
            .contains("palyra-workerd"));
    }

    #[test]
    fn promotion_report_blocks_worker_profile_until_digest_gate_is_met() {
        let report = build_promotion_report(DeploymentProfileId::WorkerEnabled, None)
            .expect("promotion report should build");
        assert_eq!(report.status, "blocked");
        assert!(report.gates.iter().any(|gate| gate.id == "worker_attestation_digest"));
    }

    #[test]
    fn profile_defaults_are_toml_representable() {
        for manifest in canonical_deployment_profiles() {
            for default in manifest.defaults {
                match default.value {
                    DeploymentProfileDefaultValue::String(_)
                    | DeploymentProfileDefaultValue::Integer(_)
                    | DeploymentProfileDefaultValue::Boolean(_)
                    | DeploymentProfileDefaultValue::StringList(_) => {}
                }
            }
        }
    }
}
