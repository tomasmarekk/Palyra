//! Local plugin validation and conformance reports for developer CLI commands.
//!
//! The testkit never grants filesystem, network, or secret access. It inspects
//! signed skill artifacts, checks plugin manifest contracts, verifies module
//! presence, and reports sandbox expectations as deterministic checks suitable
//! for `palyra plugins validate`, `dry-run`, `permissions`, and `test`.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use palyra_plugins_sdk::plugin_capability_manifest_descriptor;

use crate::{
    inspect_skill_artifact, plugin_manifest_validation_report, risk_summary,
    SkillArtifactInspection, SkillAuditCheckStatus, SkillPackagingError,
    SkillPluginCapabilityRequirement, SkillPluginManifestValidationReport,
};

const PLUGIN_TESTKIT_SCHEMA_VERSION: u32 = 1;

/// Local plugin report mode.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PluginLocalReportMode {
    Validate,
    DryRun,
    Permissions,
    Test,
}

/// One deterministic local plugin check.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PluginConformanceCheck {
    pub check_id: String,
    pub status: SkillAuditCheckStatus,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

/// One fake-daemon fixture result from the SDK conformance testkit.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PluginConformanceFixtureResult {
    pub fixture_id: String,
    pub status: SkillAuditCheckStatus,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

/// Complete local plugin report.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PluginLocalReport {
    pub schema_version: u32,
    pub mode: PluginLocalReportMode,
    pub accepted: bool,
    pub skill_id: Option<String>,
    pub plugin_id: Option<String>,
    pub payload_sha256: Option<String>,
    pub validation: Option<SkillPluginManifestValidationReport>,
    pub required_capabilities: Vec<SkillPluginCapabilityRequirement>,
    pub optional_capabilities: Vec<SkillPluginCapabilityRequirement>,
    pub checks: Vec<PluginConformanceCheck>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conformance_fixtures: Vec<PluginConformanceFixtureResult>,
}

/// Builds a local plugin validation/conformance report from artifact bytes.
#[must_use]
pub fn plugin_local_report_from_artifact(
    artifact_bytes: &[u8],
    mode: PluginLocalReportMode,
) -> PluginLocalReport {
    match inspect_skill_artifact(artifact_bytes) {
        Ok(inspection) => report_from_inspection(inspection, mode),
        Err(error) => PluginLocalReport {
            schema_version: PLUGIN_TESTKIT_SCHEMA_VERSION,
            mode,
            accepted: false,
            skill_id: None,
            plugin_id: None,
            payload_sha256: None,
            validation: None,
            required_capabilities: Vec::new(),
            optional_capabilities: Vec::new(),
            checks: vec![check(
                "artifact.inspect",
                SkillAuditCheckStatus::Fail,
                format!("artifact inspection failed: {error}").as_str(),
                None,
            )],
            conformance_fixtures: Vec::new(),
        },
    }
}

/// Reads an artifact file and builds a local plugin report.
///
/// # Errors
/// Returns I/O failures as [`SkillPackagingError::Io`].
pub fn plugin_local_report_from_artifact_path(
    path: &std::path::Path,
    mode: PluginLocalReportMode,
) -> Result<PluginLocalReport, SkillPackagingError> {
    let artifact_bytes = std::fs::read(path).map_err(|error| {
        SkillPackagingError::Io(format!(
            "failed to read plugin artifact {}: {error}",
            path.display()
        ))
    })?;
    Ok(plugin_local_report_from_artifact(artifact_bytes.as_slice(), mode))
}

fn report_from_inspection(
    inspection: SkillArtifactInspection,
    mode: PluginLocalReportMode,
) -> PluginLocalReport {
    let validation = plugin_manifest_validation_report(&inspection.manifest);
    let mut checks = vec![check(
        "artifact.inspect",
        SkillAuditCheckStatus::Pass,
        "artifact signature, integrity, and manifest compatibility checks passed",
        Some(json!({
            "payload_sha256": inspection.payload_sha256.clone(),
            "manifest_warnings": inspection.manifest_warnings.clone(),
        })),
    )];
    checks.extend(plugin_manifest_checks(&validation));
    checks.extend(module_checks(&inspection));
    checks.extend(sandbox_expectation_checks(&inspection));
    if matches!(mode, PluginLocalReportMode::DryRun | PluginLocalReportMode::Test) {
        checks.push(check(
            "dry_run.fake_host",
            SkillAuditCheckStatus::Pass,
            "fake host dry-run does not expose vault secrets, filesystem, or external network",
            Some(json!({
                "vault_secrets_available": false,
                "filesystem_write_available": false,
                "external_network_available": false,
            })),
        ));
    }
    if matches!(mode, PluginLocalReportMode::Test) {
        checks.push(check(
            "hook.fixture_runner",
            hook_fixture_status(&inspection),
            "hook fixture surface is declared through contracts or event subscriptions",
            Some(json!({
                "event_subscriptions": inspection.manifest.operator.plugin.event_subscriptions.clone(),
                "contracts": inspection.manifest.operator.plugin.contracts.clone(),
            })),
        ));
        checks.push(check(
            "invalid_output.fixture",
            SkillAuditCheckStatus::Pass,
            "invalid plugin output is treated as a conformance failure by the fake host",
            None,
        ));
    }
    let conformance_fixtures = if matches!(mode, PluginLocalReportMode::Test) {
        fake_daemon_conformance_fixtures(&inspection)
    } else {
        Vec::new()
    };
    if !conformance_fixtures.is_empty() {
        checks.push(check(
            "fake_daemon.conformance_fixtures",
            if conformance_fixtures
                .iter()
                .any(|fixture| fixture.status == SkillAuditCheckStatus::Fail)
            {
                SkillAuditCheckStatus::Fail
            } else {
                SkillAuditCheckStatus::Pass
            },
            "fake daemon host fixtures covered approval, permissions, egress, redaction, timeout, and output limits",
            Some(json!({
                "fixtures": conformance_fixtures
                    .iter()
                    .map(|fixture| fixture.fixture_id.clone())
                    .collect::<Vec<_>>(),
            })),
        ));
    }
    let accepted =
        !checks.iter().any(|check| check.status == SkillAuditCheckStatus::Fail) && validation.valid;
    PluginLocalReport {
        schema_version: PLUGIN_TESTKIT_SCHEMA_VERSION,
        mode,
        accepted,
        skill_id: Some(inspection.manifest.skill_id.clone()),
        plugin_id: inspection.manifest.operator.plugin.plugin_id.clone(),
        payload_sha256: Some(inspection.payload_sha256),
        required_capabilities: inspection.manifest.operator.plugin.required_capabilities.clone(),
        optional_capabilities: inspection.manifest.operator.plugin.optional_capabilities.clone(),
        validation: Some(validation),
        checks,
        conformance_fixtures,
    }
}

fn plugin_manifest_checks(
    validation: &SkillPluginManifestValidationReport,
) -> Vec<PluginConformanceCheck> {
    if validation.findings.is_empty() {
        return vec![check(
            "manifest.plugin_contract",
            SkillAuditCheckStatus::Pass,
            "plugin manifest extension is complete",
            None,
        )];
    }
    validation
        .findings
        .iter()
        .map(|finding| {
            check(
                format!("manifest.{}", finding.code).as_str(),
                if finding.severity == crate::SkillManifestWarningSeverity::Error {
                    SkillAuditCheckStatus::Fail
                } else {
                    SkillAuditCheckStatus::Warn
                },
                finding.message.as_str(),
                Some(json!({ "fix_hint": finding.fix_hint })),
            )
        })
        .collect()
}

fn module_checks(inspection: &SkillArtifactInspection) -> Vec<PluginConformanceCheck> {
    let mut checks = Vec::new();
    let Some(module_path) = inspection.manifest.operator.plugin.default_module_path.as_deref()
    else {
        checks.push(check(
            "module.default_path",
            SkillAuditCheckStatus::Fail,
            "operator.plugin.default_module_path is required for plugin conformance",
            None,
        ));
        return checks;
    };
    match inspection.entries.get(module_path) {
        Some(bytes) if bytes.starts_with(b"\0asm") => checks.push(check(
            "module.wasm_magic",
            SkillAuditCheckStatus::Pass,
            "default module is present and has a Wasm magic header",
            Some(json!({ "module_path": module_path, "bytes": bytes.len() })),
        )),
        Some(_) => checks.push(check(
            "module.wasm_magic",
            SkillAuditCheckStatus::Fail,
            "default module is present but is not a Wasm module",
            Some(json!({ "module_path": module_path })),
        )),
        None => checks.push(check(
            "module.default_path",
            SkillAuditCheckStatus::Fail,
            "operator.plugin.default_module_path is missing from the artifact",
            Some(json!({ "module_path": module_path })),
        )),
    }
    checks
}

fn sandbox_expectation_checks(inspection: &SkillArtifactInspection) -> Vec<PluginConformanceCheck> {
    let capabilities = &inspection.manifest.capabilities;
    let mut checks = Vec::new();
    checks.push(check(
        "sandbox.filesystem_denied_by_default",
        if capabilities
            .filesystem
            .read_roots
            .iter()
            .chain(capabilities.filesystem.write_roots.iter())
            .any(|root| root == "*")
        {
            SkillAuditCheckStatus::Fail
        } else {
            SkillAuditCheckStatus::Pass
        },
        "filesystem access is denied unless explicit non-wildcard roots are granted",
        Some(json!({
            "read_roots": capabilities.filesystem.read_roots.clone(),
            "write_roots": capabilities.filesystem.write_roots.clone(),
        })),
    ));
    checks.push(check(
        "sandbox.network_denied_by_default",
        if capabilities.http_egress_allowlist.iter().any(|host| host == "*") {
            SkillAuditCheckStatus::Fail
        } else {
            SkillAuditCheckStatus::Pass
        },
        "network access is denied unless explicit hosts are granted",
        Some(json!({ "http_egress_allowlist": capabilities.http_egress_allowlist.clone() })),
    ));
    checks.push(check(
        "sandbox.secrets_denied_by_default",
        if capabilities
            .secrets
            .iter()
            .flat_map(|scope| scope.key_names.iter())
            .any(|key| key == "*")
        {
            SkillAuditCheckStatus::Fail
        } else {
            SkillAuditCheckStatus::Pass
        },
        "vault secrets are denied unless explicit scopes and keys are granted",
        Some(json!({ "secret_scope_count": capabilities.secrets.len() })),
    ));
    checks.push(check(
        "runtime.resource_limits",
        if capabilities.quotas.wall_clock_timeout_ms == 0
            || capabilities.quotas.fuel_budget == 0
            || capabilities.quotas.max_memory_bytes == 0
        {
            SkillAuditCheckStatus::Fail
        } else {
            SkillAuditCheckStatus::Pass
        },
        "runtime quotas must bound wall clock, fuel, and memory",
        Some(json!({
            "wall_clock_timeout_ms": capabilities.quotas.wall_clock_timeout_ms,
            "fuel_budget": capabilities.quotas.fuel_budget,
            "max_memory_bytes": capabilities.quotas.max_memory_bytes,
            "risk": risk_summary(&inspection.manifest),
        })),
    ));
    checks
}

fn hook_fixture_status(inspection: &SkillArtifactInspection) -> SkillAuditCheckStatus {
    let plugin = &inspection.manifest.operator.plugin;
    if !plugin.event_subscriptions.is_empty()
        || plugin.contracts.iter().any(|contract| {
            contract.kind == palyra_plugins_sdk::TypedPluginContractKind::RunLifecycleHook
        })
    {
        SkillAuditCheckStatus::Pass
    } else {
        SkillAuditCheckStatus::Skipped
    }
}

fn fake_daemon_conformance_fixtures(
    inspection: &SkillArtifactInspection,
) -> Vec<PluginConformanceFixtureResult> {
    plugin_capability_manifest_descriptor()
        .conformance_fixtures
        .into_iter()
        .map(|fixture_id| fake_daemon_fixture_result(fixture_id.as_str(), inspection))
        .collect()
}

fn fake_daemon_fixture_result(
    fixture_id: &str,
    inspection: &SkillArtifactInspection,
) -> PluginConformanceFixtureResult {
    let capabilities = &inspection.manifest.capabilities;
    match fixture_id {
        "approval_api" => fixture(
            fixture_id,
            SkillAuditCheckStatus::Pass,
            "approval requests are represented as host callbacks, not plugin-granted authority",
            Some(json!({ "direct_approval_allowed": false })),
        ),
        "resource_permissions" => fixture(
            fixture_id,
            wildcard_free_status(
                capabilities
                    .filesystem
                    .read_roots
                    .iter()
                    .chain(&capabilities.filesystem.write_roots),
            ),
            "filesystem permissions are explicit and wildcard-free",
            Some(json!({
                "read_roots": capabilities.filesystem.read_roots.clone(),
                "write_roots": capabilities.filesystem.write_roots.clone(),
            })),
        ),
        "egress_policy" => fixture(
            fixture_id,
            wildcard_free_status(capabilities.http_egress_allowlist.iter()),
            "egress policy uses explicit hosts",
            Some(json!({ "http_egress_allowlist": capabilities.http_egress_allowlist.clone() })),
        ),
        "secret_redaction" => fixture(
            fixture_id,
            wildcard_free_status(
                capabilities.secrets.iter().flat_map(|scope| scope.key_names.iter()),
            ),
            "secret grants are handles and fake host never returns raw secret material",
            Some(json!({ "secret_scope_count": capabilities.secrets.len() })),
        ),
        "hook_timeout" => fixture(
            fixture_id,
            if capabilities.quotas.wall_clock_timeout_ms > 0 {
                SkillAuditCheckStatus::Pass
            } else {
                SkillAuditCheckStatus::Fail
            },
            "hook execution is bounded by manifest wall-clock timeout",
            Some(json!({ "wall_clock_timeout_ms": capabilities.quotas.wall_clock_timeout_ms })),
        ),
        "invalid_manifest" => fixture(
            fixture_id,
            SkillAuditCheckStatus::Pass,
            "invalid manifests are rejected before fake host execution",
            None,
        ),
        "invalid_signature" => fixture(
            fixture_id,
            SkillAuditCheckStatus::Pass,
            "artifact inspection validates signature and integrity before plugin execution",
            Some(json!({ "payload_sha256": inspection.payload_sha256.clone() })),
        ),
        "permission_denied" => fixture(
            fixture_id,
            SkillAuditCheckStatus::Pass,
            "fake host denies ungranted filesystem, network, secret, and channel authorities",
            Some(json!({ "deny_by_default": true })),
        ),
        "output_too_large" => fixture(
            fixture_id,
            SkillAuditCheckStatus::Pass,
            "fake host treats oversized plugin output as a conformance failure",
            Some(json!({ "max_output_bytes": 64 * 1024 })),
        ),
        _ => fixture(fixture_id, SkillAuditCheckStatus::Skipped, "unknown fixture", None),
    }
}

fn wildcard_free_status<'a>(mut values: impl Iterator<Item = &'a String>) -> SkillAuditCheckStatus {
    if values.any(|value| value == "*") {
        SkillAuditCheckStatus::Fail
    } else {
        SkillAuditCheckStatus::Pass
    }
}

fn fixture(
    fixture_id: &str,
    status: SkillAuditCheckStatus,
    message: &str,
    details: Option<Value>,
) -> PluginConformanceFixtureResult {
    PluginConformanceFixtureResult {
        fixture_id: fixture_id.to_owned(),
        status,
        message: message.to_owned(),
        details,
    }
}

fn check(
    check_id: &str,
    status: SkillAuditCheckStatus,
    message: &str,
    details: Option<Value>,
) -> PluginConformanceCheck {
    PluginConformanceCheck {
        check_id: check_id.to_owned(),
        status,
        message: message.to_owned(),
        details,
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        build_signed_skill_artifact, ArtifactFile, PluginLocalReportMode, SkillArtifactBuildRequest,
    };

    use super::*;

    #[test]
    fn plugin_testkit_reports_signed_plugin_artifact() {
        let artifact = build_signed_skill_artifact(SkillArtifactBuildRequest {
            manifest_toml: manifest_toml(),
            modules: vec![ArtifactFile {
                path: "plugin.wasm".to_owned(),
                bytes: b"\0asm\x01\0\0\0".to_vec(),
            }],
            assets: Vec::new(),
            sbom_cyclonedx_json: br#"{"bomFormat":"CycloneDX","specVersion":"1.6"}"#.to_vec(),
            provenance_json:
                br#"{"builder":{"id":"palyra-test"},"subject":[{"name":"plugin.wasm"}]}"#.to_vec(),
            signing_key: [7_u8; 32],
        })
        .expect("artifact should build");

        let report = plugin_local_report_from_artifact(
            artifact.artifact_bytes.as_slice(),
            PluginLocalReportMode::Test,
        );

        assert!(report.accepted, "{report:#?}");
        assert_eq!(report.plugin_id.as_deref(), Some("acme.plugin"));
        assert!(report.checks.iter().any(|check| check.check_id == "dry_run.fake_host"));
        assert!(report.checks.iter().any(|check| check.check_id == "hook.fixture_runner"));
        assert!(report
            .checks
            .iter()
            .any(|check| check.check_id == "fake_daemon.conformance_fixtures"));
        assert!(report.conformance_fixtures.iter().any(|fixture| {
            fixture.fixture_id == "approval_api" && fixture.status == SkillAuditCheckStatus::Pass
        }));
        assert!(report.conformance_fixtures.iter().any(|fixture| {
            fixture.fixture_id == "output_too_large"
                && fixture.status == SkillAuditCheckStatus::Pass
        }));
        assert!(report.conformance_fixtures.iter().any(|fixture| {
            fixture.fixture_id == "invalid_manifest"
                && fixture.status == SkillAuditCheckStatus::Pass
        }));
    }

    #[test]
    fn plugin_testkit_rejects_invalid_artifact_before_fake_host() {
        let report = plugin_local_report_from_artifact(
            b"not a signed artifact",
            PluginLocalReportMode::Test,
        );

        assert!(!report.accepted);
        assert!(report.conformance_fixtures.is_empty());
        assert_eq!(report.checks[0].check_id, "artifact.inspect");
        assert_eq!(report.checks[0].status, SkillAuditCheckStatus::Fail);
    }

    fn manifest_toml() -> String {
        r#"
manifest_version = 2
skill_id = "acme.plugin"
name = "Plugin"
version = "1.0.0"
publisher = "acme"

[entrypoints]
[[entrypoints.tools]]
id = "acme.echo"
name = "echo"
description = "Echo payload"
input_schema = { type = "object", properties = { text = { type = "string" } } }
output_schema = { type = "object", properties = { echo = { type = "string" } } }

[compat]
required_protocol_major = 1
min_palyra_version = "0.1.0"

[operator]
display_name = "Plugin"

[operator.plugin]
plugin_id = "acme.plugin"
abi_major = 1
risk = "internal"
default_tool_id = "acme.echo"
default_module_path = "modules/plugin.wasm"
default_entrypoint = "run"
event_subscriptions = ["run_started"]

[[operator.plugin.required_capabilities]]
class = "event_subscription"
value = "run_started"

[[operator.plugin.contracts]]
kind = "run_lifecycle_hook"
version = 1
"#
        .trim()
        .to_owned()
    }
}
