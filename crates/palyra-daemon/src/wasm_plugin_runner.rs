use std::{
    collections::BTreeSet,
    convert::TryFrom,
    time::{Duration, Instant},
};

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine};
use palyra_plugins_runtime::{CapabilityGrantSet, RuntimeError, RuntimeLimits, WasmRuntime};
use palyra_plugins_sdk::DEFAULT_RUNTIME_ENTRYPOINT;
use palyra_skills::{
    capability_grants_from_manifest, SkillCapabilityGrantSnapshot, SkillManifest,
    SkillToolEntrypoint,
};
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmPluginRunnerPolicy {
    pub enabled: bool,
    pub allow_inline_modules: bool,
    pub max_module_size_bytes: u64,
    pub fuel_budget: u64,
    pub max_memory_bytes: u64,
    pub max_table_elements: u64,
    pub max_instances: u64,
    pub allowed_http_hosts: Vec<String>,
    pub allowed_secrets: Vec<String>,
    pub allowed_storage_prefixes: Vec<String>,
    pub allowed_channels: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmPluginRunSuccess {
    pub output_json: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmPluginRunError {
    pub kind: WasmPluginRunErrorKind,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WasmPluginRunErrorKind {
    Disabled,
    InvalidInput,
    CapabilityDenied,
    TimedOut,
    QuotaExceeded,
    RuntimeFailure,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WasmPluginRunInput {
    #[serde(default)]
    skill_id: Option<String>,
    #[serde(default)]
    skill_version: Option<String>,
    #[serde(default)]
    module_path: Option<String>,
    #[serde(default)]
    tool_id: Option<String>,
    module_wat: Option<String>,
    module_base64: Option<String>,
    entrypoint: Option<String>,
    #[serde(default)]
    capabilities: WasmPluginRequestedCapabilities,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct WasmPluginRequestedCapabilities {
    #[serde(default)]
    pub(crate) http_hosts: Vec<String>,
    #[serde(default)]
    pub(crate) secrets: Vec<String>,
    #[serde(default)]
    pub(crate) storage_prefixes: Vec<String>,
    #[serde(default)]
    pub(crate) channels: Vec<String>,
}

pub(crate) fn build_manifest_test_harness(
    manifest: &SkillManifest,
    tool: &SkillToolEntrypoint,
) -> serde_json::Value {
    json!({
        "harness_version": 1,
        "runner": "palyra.plugin.run",
        "skill_id": manifest.skill_id,
        "tool_id": tool.id,
        "entrypoint": DEFAULT_RUNTIME_ENTRYPOINT,
        "requested_capabilities": {
            "http_hosts": manifest.capabilities.http_egress_allowlist,
            "secrets": manifest
                .capabilities
                .secrets
                .iter()
                .flat_map(|scope| scope.key_names.iter().cloned())
                .collect::<Vec<_>>(),
            "storage_prefixes": manifest.capabilities.filesystem.write_roots,
            "channels": manifest.capabilities.node_capabilities,
        },
        "assertions": {
            "requires_approval": tool.risk.requires_approval,
            "experimental_builder": manifest.builder.is_some(),
        },
    })
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedInstalledSkillModule {
    pub(crate) skill_id: String,
    pub(crate) skill_version: String,
    pub(crate) manifest: SkillManifest,
    pub(crate) selected_tool: Option<SkillToolEntrypoint>,
    pub(crate) capability_grants: SkillCapabilityGrantSnapshot,
    pub(crate) module_path: String,
    pub(crate) module_bytes: Vec<u8>,
    pub(crate) entrypoint: String,
}

pub fn run_wasm_plugin(
    policy: &WasmPluginRunnerPolicy,
    input_json: &[u8],
    timeout: Duration,
) -> Result<WasmPluginRunSuccess, WasmPluginRunError> {
    if !policy.enabled {
        return Err(WasmPluginRunError {
            kind: WasmPluginRunErrorKind::Disabled,
            message: "wasm plugin runtime is disabled by runtime policy".to_owned(),
        });
    }

    let input = parse_input(input_json)?;
    validate_optional_metadata(input.skill_id.as_deref(), "skill_id")?;
    validate_optional_metadata(input.skill_version.as_deref(), "skill_version")?;
    validate_optional_metadata(input.module_path.as_deref(), "module_path")?;
    validate_optional_metadata(input.tool_id.as_deref(), "tool_id")?;
    let resolved = resolve_module_source(policy, &input)?;
    execute_module(
        policy,
        resolved.installed_skill.as_ref(),
        resolved.module_bytes.as_slice(),
        resolved.entrypoint.as_str(),
        input.capabilities,
        if resolved.execution_timeout.is_zero() {
            timeout
        } else {
            timeout.min(resolved.execution_timeout)
        },
    )
}

pub(crate) fn run_resolved_wasm_plugin(
    policy: &WasmPluginRunnerPolicy,
    resolved: &ResolvedInstalledSkillModule,
    capabilities: WasmPluginRequestedCapabilities,
    timeout: Duration,
) -> Result<WasmPluginRunSuccess, WasmPluginRunError> {
    if !policy.enabled {
        return Err(WasmPluginRunError {
            kind: WasmPluginRunErrorKind::Disabled,
            message: "wasm plugin runtime is disabled by runtime policy".to_owned(),
        });
    }
    execute_module(
        policy,
        Some(resolved),
        resolved.module_bytes.as_slice(),
        resolved.entrypoint.as_str(),
        capabilities,
        timeout,
    )
}

fn parse_input(input_json: &[u8]) -> Result<WasmPluginRunInput, WasmPluginRunError> {
    serde_json::from_slice::<WasmPluginRunInput>(input_json).map_err(|error| WasmPluginRunError {
        kind: WasmPluginRunErrorKind::InvalidInput,
        message: format!("palyra.plugin.run input must be valid JSON object: {error}"),
    })
}

fn validate_optional_metadata(
    raw: Option<&str>,
    field_name: &str,
) -> Result<(), WasmPluginRunError> {
    if let Some(value) = raw {
        if value.trim().is_empty() {
            return Err(WasmPluginRunError {
                kind: WasmPluginRunErrorKind::InvalidInput,
                message: format!("palyra.plugin.run {field_name} cannot be empty when provided"),
            });
        }
    }
    Ok(())
}

struct ResolvedModuleSource {
    module_bytes: Vec<u8>,
    entrypoint: String,
    execution_timeout: Duration,
    installed_skill: Option<ResolvedInstalledSkillModule>,
}

fn resolve_module_source(
    policy: &WasmPluginRunnerPolicy,
    input: &WasmPluginRunInput,
) -> Result<ResolvedModuleSource, WasmPluginRunError> {
    if inline_module_payload_present(input) && !policy.allow_inline_modules {
        return Err(WasmPluginRunError {
            kind: WasmPluginRunErrorKind::InvalidInput,
            message: "palyra.plugin.run inline module payloads are disabled by runtime policy; set tool_call.wasm_runtime.allow_inline_modules=true"
                .to_owned(),
        });
    }

    if inline_module_payload_present(input)
        && (input.module_path.is_some() || input.tool_id.is_some())
    {
        return Err(WasmPluginRunError {
            kind: WasmPluginRunErrorKind::InvalidInput,
            message:
                "palyra.plugin.run module_path/tool_id can only be used with installed skill artifacts"
                    .to_owned(),
        });
    }

    match (input.module_wat.as_ref(), input.module_base64.as_ref()) {
        (Some(_), Some(_)) => Err(WasmPluginRunError {
            kind: WasmPluginRunErrorKind::InvalidInput,
            message: "palyra.plugin.run accepts either module_wat or module_base64, not both"
                .to_owned(),
        }),
        (Some(module_wat), None) => Ok(ResolvedModuleSource {
            module_bytes: module_wat.as_bytes().to_vec(),
            entrypoint: parse_entrypoint(input.entrypoint.as_deref())?,
            execution_timeout: Duration::ZERO,
            installed_skill: None,
        }),
        (None, Some(module_base64)) => {
            let module_bytes =
                BASE64_STANDARD.decode(module_base64.as_bytes()).map_err(|error| {
                    WasmPluginRunError {
                        kind: WasmPluginRunErrorKind::InvalidInput,
                        message: format!("palyra.plugin.run module_base64 is invalid: {error}"),
                    }
                })?;
            Ok(ResolvedModuleSource {
                module_bytes,
                entrypoint: parse_entrypoint(input.entrypoint.as_deref())?,
                execution_timeout: Duration::ZERO,
                installed_skill: None,
            })
        }
        (None, None) => {
            let skill_id = input.skill_id.as_deref().ok_or_else(|| WasmPluginRunError {
                kind: WasmPluginRunErrorKind::InvalidInput,
                message:
                    "palyra.plugin.run requires skill_id when no inline module payload is supplied"
                        .to_owned(),
            })?;
            let resolved = resolve_installed_skill_module(
                skill_id,
                input.skill_version.as_deref(),
                input.module_path.as_deref(),
                input.entrypoint.as_deref(),
                input.tool_id.as_deref(),
            )?;
            Ok(ResolvedModuleSource {
                module_bytes: resolved.module_bytes.clone(),
                entrypoint: resolved.entrypoint.clone(),
                execution_timeout: Duration::from_millis(
                    resolved.manifest.capabilities.quotas.wall_clock_timeout_ms,
                ),
                installed_skill: Some(resolved),
            })
        }
    }
}

fn inline_module_payload_present(input: &WasmPluginRunInput) -> bool {
    input.module_wat.is_some() || input.module_base64.is_some()
}

pub(crate) fn resolve_installed_skill_module(
    skill_id: &str,
    skill_version: Option<&str>,
    module_path: Option<&str>,
    entrypoint: Option<&str>,
    tool_id: Option<&str>,
) -> Result<ResolvedInstalledSkillModule, WasmPluginRunError> {
    let skills_root = crate::resolve_skills_root().map_err(|response| WasmPluginRunError {
        kind: WasmPluginRunErrorKind::RuntimeFailure,
        message: format!(
            "failed to resolve installed skills root for palyra.plugin.run (http {})",
            response.status()
        ),
    })?;
    let index = crate::load_installed_skills_index(skills_root.as_path()).map_err(|response| {
        WasmPluginRunError {
            kind: WasmPluginRunErrorKind::RuntimeFailure,
            message: format!(
                "failed to load installed skills index for palyra.plugin.run (http {})",
                response.status()
            ),
        }
    })?;
    let resolved_version =
        crate::resolve_skill_version(&index, skill_id, skill_version).map_err(|response| {
            WasmPluginRunError {
                kind: WasmPluginRunErrorKind::InvalidInput,
                message: format!(
                    "failed to resolve installed skill version for palyra.plugin.run (http {})",
                    response.status()
                ),
            }
        })?;
    let _record = index
        .entries
        .iter()
        .find(|entry| entry.skill_id == skill_id && entry.version == resolved_version)
        .cloned()
        .ok_or_else(|| WasmPluginRunError {
            kind: WasmPluginRunErrorKind::InvalidInput,
            message: format!(
                "installed skill artifact not found for {}@{}",
                skill_id, resolved_version
            ),
        })?;
    let artifact_path = crate::managed_skill_artifact_path(
        skills_root.as_path(),
        skill_id,
        resolved_version.as_str(),
    );
    let artifact_bytes =
        std::fs::read(artifact_path.as_path()).map_err(|error| WasmPluginRunError {
            kind: WasmPluginRunErrorKind::RuntimeFailure,
            message: format!(
                "failed to read installed skill artifact {}: {error}",
                artifact_path.display()
            ),
        })?;
    let inspection =
        palyra_skills::inspect_skill_artifact(artifact_bytes.as_slice()).map_err(|error| {
            WasmPluginRunError {
                kind: WasmPluginRunErrorKind::InvalidInput,
                message: format!("installed skill artifact inspection failed: {error}"),
            }
        })?;
    let selected_tool = select_tool_entrypoint(&inspection.manifest, tool_id)?;
    let selected_module_path =
        select_module_path(inspection.entries.keys().cloned().collect::<Vec<_>>(), module_path)?;
    let module_bytes =
        inspection.entries.get(selected_module_path.as_str()).cloned().ok_or_else(|| {
            WasmPluginRunError {
                kind: WasmPluginRunErrorKind::InvalidInput,
                message: format!(
                    "installed skill artifact is missing selected module '{}'",
                    selected_module_path
                ),
            }
        })?;
    Ok(ResolvedInstalledSkillModule {
        skill_id: skill_id.to_owned(),
        skill_version: resolved_version,
        manifest: inspection.manifest.clone(),
        selected_tool,
        capability_grants: capability_grants_from_manifest(&inspection.manifest),
        module_path: selected_module_path,
        module_bytes,
        entrypoint: parse_entrypoint(entrypoint)?,
    })
}

fn select_tool_entrypoint(
    manifest: &SkillManifest,
    tool_id: Option<&str>,
) -> Result<Option<SkillToolEntrypoint>, WasmPluginRunError> {
    let Some(tool_id) = tool_id.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    manifest.entrypoints.tools.iter().find(|tool| tool.id == tool_id).cloned().map(Some).ok_or_else(
        || WasmPluginRunError {
            kind: WasmPluginRunErrorKind::InvalidInput,
            message: format!("skill artifact does not declare tool_id '{tool_id}'"),
        },
    )
}

fn select_module_path(
    artifact_paths: Vec<String>,
    requested: Option<&str>,
) -> Result<String, WasmPluginRunError> {
    let mut module_paths = artifact_paths
        .into_iter()
        .filter(|path| path.starts_with("modules/") && path.ends_with(".wasm"))
        .collect::<Vec<_>>();
    module_paths.sort();
    module_paths.dedup();
    if let Some(path) = requested.map(str::trim).filter(|value| !value.is_empty()) {
        if path.contains('\0')
            || path.contains("..")
            || path.starts_with('/')
            || path.starts_with('\\')
            || !path.starts_with("modules/")
            || !path.ends_with(".wasm")
        {
            return Err(WasmPluginRunError {
                kind: WasmPluginRunErrorKind::InvalidInput,
                message:
                    "palyra.plugin.run module_path must reference a modules/*.wasm artifact entry"
                        .to_owned(),
            });
        }
        return module_paths.into_iter().find(|candidate| candidate == path).ok_or_else(|| {
            WasmPluginRunError {
                kind: WasmPluginRunErrorKind::InvalidInput,
                message: format!("skill artifact does not contain module_path '{path}'"),
            }
        });
    }
    if module_paths.len() == 1 {
        return Ok(module_paths.remove(0));
    }
    Err(WasmPluginRunError {
        kind: WasmPluginRunErrorKind::InvalidInput,
        message: "skill artifact contains multiple modules; specify module_path to select one"
            .to_owned(),
    })
}

fn execute_module(
    policy: &WasmPluginRunnerPolicy,
    installed_skill: Option<&ResolvedInstalledSkillModule>,
    module_bytes: &[u8],
    entrypoint: &str,
    capabilities: WasmPluginRequestedCapabilities,
    timeout: Duration,
) -> Result<WasmPluginRunSuccess, WasmPluginRunError> {
    if module_bytes.is_empty() {
        return Err(WasmPluginRunError {
            kind: WasmPluginRunErrorKind::InvalidInput,
            message: "palyra.plugin.run module payload cannot be empty".to_owned(),
        });
    }
    if module_bytes.len() as u64 > policy.max_module_size_bytes {
        return Err(WasmPluginRunError {
            kind: WasmPluginRunErrorKind::QuotaExceeded,
            message: format!(
                "palyra.plugin.run module payload exceeds max_module_size_bytes={} bytes",
                policy.max_module_size_bytes
            ),
        });
    }

    let requested_http_hosts =
        normalize_host_allowlist(capabilities.http_hosts.as_slice(), "capabilities.http_hosts")?;
    let requested_secrets = normalize_identifier_allowlist(
        capabilities.secrets.as_slice(),
        "capabilities.secrets",
        "secret handle",
    )?;
    let requested_storage_prefixes = normalize_storage_prefix_allowlist(
        capabilities.storage_prefixes.as_slice(),
        "capabilities.storage_prefixes",
    )?;
    let requested_channels = normalize_identifier_allowlist(
        capabilities.channels.as_slice(),
        "capabilities.channels",
        "channel handle",
    )?;

    let allowed_http_hosts = effective_allowed_capabilities(
        installed_skill.map(|skill| skill.capability_grants.http_hosts.as_slice()),
        policy.allowed_http_hosts.as_slice(),
        "policy.allowed_http_hosts",
        normalize_host_allowlist,
    )?;
    let allowed_secrets = effective_allowed_capabilities(
        installed_skill.map(|skill| skill.capability_grants.secret_keys.as_slice()),
        policy.allowed_secrets.as_slice(),
        "policy.allowed_secrets",
        |values, source| normalize_identifier_allowlist(values, source, "secret handle"),
    )?;
    let allowed_storage_prefixes = effective_allowed_capabilities(
        installed_skill.map(|skill| skill.capability_grants.storage_prefixes.as_slice()),
        policy.allowed_storage_prefixes.as_slice(),
        "policy.allowed_storage_prefixes",
        normalize_storage_prefix_allowlist,
    )?;
    let allowed_channels = effective_allowed_capabilities(
        installed_skill.map(|skill| skill.capability_grants.channels.as_slice()),
        policy.allowed_channels.as_slice(),
        "policy.allowed_channels",
        |values, source| normalize_identifier_allowlist(values, source, "channel handle"),
    )?;

    ensure_capabilities_subset(
        requested_http_hosts.as_slice(),
        allowed_http_hosts.as_slice(),
        "http_hosts",
    )?;
    ensure_capabilities_subset(
        requested_secrets.as_slice(),
        allowed_secrets.as_slice(),
        "secrets",
    )?;
    ensure_capabilities_subset(
        requested_storage_prefixes.as_slice(),
        allowed_storage_prefixes.as_slice(),
        "storage_prefixes",
    )?;
    ensure_capabilities_subset(
        requested_channels.as_slice(),
        allowed_channels.as_slice(),
        "channels",
    )?;

    let limits = RuntimeLimits {
        fuel_budget: installed_skill
            .map(|skill| skill.manifest.capabilities.quotas.fuel_budget.min(policy.fuel_budget))
            .unwrap_or(policy.fuel_budget),
        max_memory_bytes: usize::try_from(
            installed_skill
                .map(|skill| {
                    skill.manifest.capabilities.quotas.max_memory_bytes.min(policy.max_memory_bytes)
                })
                .unwrap_or(policy.max_memory_bytes),
        )
        .map_err(|_| WasmPluginRunError {
            kind: WasmPluginRunErrorKind::InvalidInput,
            message: "wasm runtime memory quota exceeds platform usize range".to_owned(),
        })?,
        max_table_elements: usize::try_from(policy.max_table_elements).map_err(|_| {
            WasmPluginRunError {
                kind: WasmPluginRunErrorKind::InvalidInput,
                message: "wasm runtime table quota exceeds platform usize range".to_owned(),
            }
        })?,
        max_instances: usize::try_from(policy.max_instances).map_err(|_| WasmPluginRunError {
            kind: WasmPluginRunErrorKind::InvalidInput,
            message: "wasm runtime instance quota exceeds platform usize range".to_owned(),
        })?,
    };
    let grants = CapabilityGrantSet {
        http_hosts: requested_http_hosts.clone(),
        secret_keys: requested_secrets.clone(),
        storage_prefixes: requested_storage_prefixes.clone(),
        channels: requested_channels.clone(),
    };
    let runtime = WasmRuntime::new_with_limits(limits).map_err(map_runtime_error)?;
    let effective_timeout = if let Some(skill) = installed_skill {
        timeout.min(Duration::from_millis(skill.manifest.capabilities.quotas.wall_clock_timeout_ms))
    } else {
        timeout
    };
    let started_at = Instant::now();
    let execution = runtime
        .execute_i32_entrypoint_with_timeout(module_bytes, entrypoint, &grants, effective_timeout)
        .map_err(map_runtime_error)?;
    let duration = started_at.elapsed();
    let output_json = serde_json::to_vec(&json!({
        "exit_code": execution.exit_code,
        "entrypoint": entrypoint,
        "duration_ms": duration_to_millis(duration),
        "resolved_from": if installed_skill.is_some() { "installed_skill_artifact" } else { "inline_module" },
        "skill": installed_skill.map(|skill| json!({
            "skill_id": skill.skill_id,
            "version": skill.skill_version,
            "module_path": skill.module_path,
            "tool_id": skill.selected_tool.as_ref().map(|tool| tool.id.clone()),
        })),
        "capabilities": {
            "http_handles": execution.capability_handles.http_handles,
            "secret_handles_count": execution.capability_handles.secret_handles.len(),
            "storage_handles": execution.capability_handles.storage_handles,
            "channel_handles": execution.capability_handles.channel_handles,
            "granted_http_hosts": requested_http_hosts,
            "granted_storage_prefixes": requested_storage_prefixes,
            "granted_channels": requested_channels,
        },
    }))
    .map_err(|error| WasmPluginRunError {
        kind: WasmPluginRunErrorKind::RuntimeFailure,
        message: format!("failed to serialize palyra.plugin.run output JSON: {error}"),
    })?;
    Ok(WasmPluginRunSuccess { output_json })
}

fn effective_allowed_capabilities(
    skill_caps: Option<&[String]>,
    policy_caps: &[String],
    source_name: &str,
    normalize: fn(&[String], &str) -> Result<Vec<String>, WasmPluginRunError>,
) -> Result<Vec<String>, WasmPluginRunError> {
    let mut policy_values = normalize(policy_caps, source_name)?;
    if let Some(skill_caps) = skill_caps {
        let skill_values = normalize(skill_caps, "skill.manifest.capabilities")?;
        let skill_set = skill_values.iter().map(String::as_str).collect::<BTreeSet<_>>();
        policy_values.retain(|candidate| skill_set.contains(candidate.as_str()));
    }
    Ok(policy_values)
}

fn parse_entrypoint(entrypoint: Option<&str>) -> Result<String, WasmPluginRunError> {
    let entrypoint = entrypoint.map(str::trim).unwrap_or(DEFAULT_RUNTIME_ENTRYPOINT);
    if entrypoint.is_empty() {
        return Err(WasmPluginRunError {
            kind: WasmPluginRunErrorKind::InvalidInput,
            message: "palyra.plugin.run entrypoint cannot be empty".to_owned(),
        });
    }
    if !entrypoint
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '_' | '-'))
    {
        return Err(WasmPluginRunError {
            kind: WasmPluginRunErrorKind::InvalidInput,
            message: format!(
                "palyra.plugin.run entrypoint '{entrypoint}' contains invalid characters"
            ),
        });
    }
    Ok(entrypoint.to_owned())
}

fn ensure_capabilities_subset(
    requested: &[String],
    allowed: &[String],
    capability_name: &str,
) -> Result<(), WasmPluginRunError> {
    let allowed_set = allowed.iter().map(String::as_str).collect::<BTreeSet<_>>();
    let denied = requested
        .iter()
        .map(String::as_str)
        .filter(|candidate| !allowed_set.contains(candidate))
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if denied.is_empty() {
        return Ok(());
    }
    Err(WasmPluginRunError {
        kind: WasmPluginRunErrorKind::CapabilityDenied,
        message: format!(
            "palyra.plugin.run capability denied for {capability_name}: {}",
            denied.join(",")
        ),
    })
}

fn normalize_host_allowlist(
    raw: &[String],
    source_name: &str,
) -> Result<Vec<String>, WasmPluginRunError> {
    let mut normalized = BTreeSet::new();
    for candidate in raw.iter().map(String::as_str).map(str::trim).filter(|value| !value.is_empty())
    {
        let value = candidate.trim_end_matches('.').to_ascii_lowercase();
        if value.is_empty() {
            return Err(WasmPluginRunError {
                kind: WasmPluginRunErrorKind::InvalidInput,
                message: format!("{source_name} contains invalid host '{candidate}'"),
            });
        }
        if !value
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '-'))
            || value.starts_with('-')
            || value.ends_with('-')
            || value.starts_with('.')
            || value.ends_with('.')
            || value.contains("..")
        {
            return Err(WasmPluginRunError {
                kind: WasmPluginRunErrorKind::InvalidInput,
                message: format!("{source_name} contains invalid host '{candidate}'"),
            });
        }
        normalized.insert(value);
    }
    Ok(normalized.into_iter().collect())
}

fn normalize_identifier_allowlist(
    raw: &[String],
    source_name: &str,
    label: &str,
) -> Result<Vec<String>, WasmPluginRunError> {
    let mut normalized = BTreeSet::new();
    for candidate in raw.iter().map(String::as_str).map(str::trim).filter(|value| !value.is_empty())
    {
        if !candidate.chars().all(|ch| {
            ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '_' | '-' | '/')
        }) {
            return Err(WasmPluginRunError {
                kind: WasmPluginRunErrorKind::InvalidInput,
                message: format!("{source_name} contains invalid {label} '{candidate}'"),
            });
        }
        normalized.insert(candidate.to_owned());
    }
    Ok(normalized.into_iter().collect())
}

fn normalize_storage_prefix_allowlist(
    raw: &[String],
    source_name: &str,
) -> Result<Vec<String>, WasmPluginRunError> {
    let mut normalized = BTreeSet::new();
    for candidate in raw.iter().map(String::as_str).map(str::trim).filter(|value| !value.is_empty())
    {
        if candidate.contains('\0')
            || candidate.contains("..")
            || candidate.starts_with('/')
            || candidate.starts_with('\\')
            || !candidate.chars().all(|ch| {
                ch.is_ascii_lowercase()
                    || ch.is_ascii_digit()
                    || matches!(ch, '/' | '.' | '_' | '-')
            })
        {
            return Err(WasmPluginRunError {
                kind: WasmPluginRunErrorKind::InvalidInput,
                message: format!("{source_name} contains invalid storage prefix '{candidate}'"),
            });
        }
        normalized.insert(candidate.to_owned());
    }
    Ok(normalized.into_iter().collect())
}

fn map_runtime_error(error: RuntimeError) -> WasmPluginRunError {
    match error {
        RuntimeError::ExecutionTimedOut => WasmPluginRunError {
            kind: WasmPluginRunErrorKind::TimedOut,
            message: "palyra.plugin.run execution timed out".to_owned(),
        },
        RuntimeError::ExecutionLimitExceeded => WasmPluginRunError {
            kind: WasmPluginRunErrorKind::QuotaExceeded,
            message: "palyra.plugin.run execution exceeded wasm runtime quota".to_owned(),
        },
        RuntimeError::MissingExport(entrypoint) => WasmPluginRunError {
            kind: WasmPluginRunErrorKind::InvalidInput,
            message: format!("palyra.plugin.run missing exported entrypoint '{entrypoint}'"),
        },
        RuntimeError::Compile(error) => WasmPluginRunError {
            kind: WasmPluginRunErrorKind::InvalidInput,
            message: format!("palyra.plugin.run failed to compile module: {error}"),
        },
        RuntimeError::Linker(error) => WasmPluginRunError {
            kind: WasmPluginRunErrorKind::InvalidInput,
            message: format!(
                "palyra.plugin.run module does not satisfy host capability import contract: {error}"
            ),
        },
        RuntimeError::Execution(error) => WasmPluginRunError {
            kind: WasmPluginRunErrorKind::RuntimeFailure,
            message: format!("palyra.plugin.run execution failed: {error}"),
        },
    }
}

fn duration_to_millis(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{
        run_resolved_wasm_plugin, run_wasm_plugin, ResolvedInstalledSkillModule,
        WasmPluginRequestedCapabilities, WasmPluginRunErrorKind, WasmPluginRunnerPolicy,
    };
    use palyra_skills::{
        SkillCapabilities, SkillCapabilityGrantSnapshot, SkillCompat, SkillEntrypoints,
        SkillIntegrity, SkillManifest,
    };
    use serde_json::Value;

    fn test_policy() -> WasmPluginRunnerPolicy {
        WasmPluginRunnerPolicy {
            enabled: true,
            allow_inline_modules: true,
            max_module_size_bytes: 256 * 1024,
            fuel_budget: 10_000_000,
            max_memory_bytes: 64 * 1024 * 1024,
            max_table_elements: 100_000,
            max_instances: 256,
            allowed_http_hosts: vec!["api.example.com".to_owned()],
            allowed_secrets: vec!["db_password".to_owned()],
            allowed_storage_prefixes: vec!["plugins/cache".to_owned()],
            allowed_channels: vec!["cli".to_owned()],
        }
    }

    #[test]
    fn run_wasm_plugin_denies_when_disabled() {
        let mut policy = test_policy();
        policy.enabled = false;
        let error = run_wasm_plugin(
            &policy,
            br#"{"module_wat":"(module (func (export \"run\") (result i32) i32.const 1))"}"#,
            Duration::from_secs(2),
        )
        .expect_err("disabled runner must fail closed");
        assert_eq!(error.kind, WasmPluginRunErrorKind::Disabled);
    }

    #[test]
    fn run_resolved_wasm_plugin_denies_when_disabled() {
        let mut policy = test_policy();
        policy.enabled = false;
        let resolved = ResolvedInstalledSkillModule {
            skill_id: "acme.echo".to_owned(),
            skill_version: "1.0.0".to_owned(),
            manifest: SkillManifest {
                manifest_version: 1,
                skill_id: "acme.echo".to_owned(),
                name: "Acme Echo".to_owned(),
                version: "1.0.0".to_owned(),
                publisher: "Acme".to_owned(),
                entrypoints: SkillEntrypoints { tools: Vec::new() },
                capabilities: SkillCapabilities::default(),
                compat: SkillCompat {
                    required_protocol_major: 1,
                    min_palyra_version: "0.1.0".to_owned(),
                },
                integrity: SkillIntegrity::default(),
            },
            selected_tool: None,
            capability_grants: SkillCapabilityGrantSnapshot {
                http_hosts: Vec::new(),
                secret_keys: Vec::new(),
                storage_prefixes: Vec::new(),
                channels: Vec::new(),
            },
            module_path: "modules/echo.wasm".to_owned(),
            module_bytes: br#"(module (func (export "run") (result i32) i32.const 1))"#.to_vec(),
            entrypoint: "run".to_owned(),
        };
        let error = run_resolved_wasm_plugin(
            &policy,
            &resolved,
            WasmPluginRequestedCapabilities::default(),
            Duration::from_secs(2),
        )
        .expect_err("resolved runner must also fail closed when disabled");
        assert_eq!(error.kind, WasmPluginRunErrorKind::Disabled);
    }

    #[test]
    fn run_wasm_plugin_denies_inline_module_payloads_without_opt_in() {
        let mut policy = test_policy();
        policy.allow_inline_modules = false;
        let error = run_wasm_plugin(
            &policy,
            br#"{"module_wat":"(module (func (export \"run\") (result i32) i32.const 1))"}"#,
            Duration::from_secs(2),
        )
        .expect_err("inline module payloads must default to deny-by-default");
        assert_eq!(error.kind, WasmPluginRunErrorKind::InvalidInput);
        assert!(
            error.message.contains("allow_inline_modules=true"),
            "error should explain explicit runtime opt-in path: {}",
            error.message
        );
    }

    #[test]
    fn run_wasm_plugin_rejects_dev_override_flag_in_input() {
        let mut policy = test_policy();
        policy.allow_inline_modules = false;
        let error = run_wasm_plugin(
            &policy,
            br#"{
                "dev": true,
                "module_wat":"(module (func (export \"run\") (result i32) i32.const 1))"
            }"#,
            Duration::from_secs(2),
        )
        .expect_err("dev flag should be rejected from untrusted tool input");
        assert_eq!(error.kind, WasmPluginRunErrorKind::InvalidInput);
        assert!(error.message.contains("unknown field `dev`"));
    }

    #[test]
    fn run_wasm_plugin_executes_module_with_granted_capabilities() {
        let policy = test_policy();
        let input = serde_json::json!({
            "skill_id": "acme.echo_http",
            "skill_version": "1.2.3",
            "module_wat": r#"
                (module
                    (import "palyra:plugins/host-capabilities@0.1.0" "http-count" (func $http_count (result i32)))
                    (import "palyra:plugins/host-capabilities@0.1.0" "secret-count" (func $secret_count (result i32)))
                    (import "palyra:plugins/host-capabilities@0.1.0" "storage-count" (func $storage_count (result i32)))
                    (import "palyra:plugins/host-capabilities@0.1.0" "channel-count" (func $channel_count (result i32)))
                    (func (export "run") (result i32)
                        call $http_count
                        call $secret_count
                        i32.add
                        call $storage_count
                        i32.add
                        call $channel_count
                        i32.add
                    )
                )
            "#,
            "capabilities": {
                "http_hosts": ["api.example.com"],
                "secrets": ["db_password"],
                "storage_prefixes": ["plugins/cache"],
                "channels": ["cli"]
            }
        });
        let input_json = serde_json::to_vec(&input).expect("input JSON should serialize");

        let success = run_wasm_plugin(&policy, input_json.as_slice(), Duration::from_secs(2))
            .expect("capability-allowlisted wasm module should execute");
        let output: Value =
            serde_json::from_slice(&success.output_json).expect("output_json should parse");

        assert_eq!(output.get("exit_code").and_then(Value::as_i64), Some(4));
        assert_eq!(
            output.pointer("/capabilities/http_handles/0").and_then(Value::as_i64),
            Some(10_000)
        );
        assert_eq!(
            output.pointer("/capabilities/secret_handles_count").and_then(Value::as_u64),
            Some(1)
        );
    }

    #[test]
    fn run_wasm_plugin_denies_non_allowlisted_capability_request() {
        let policy = test_policy();
        let input = serde_json::json!({
            "module_wat": "(module (func (export \"run\") (result i32) i32.const 1))",
            "capabilities": {
                "http_hosts": ["blocked.example"]
            }
        });
        let input_json = serde_json::to_vec(&input).expect("input JSON should serialize");

        let error = run_wasm_plugin(&policy, input_json.as_slice(), Duration::from_secs(2))
            .expect_err("non-allowlisted capability request must fail");

        assert_eq!(error.kind, WasmPluginRunErrorKind::CapabilityDenied);
        assert!(error.message.contains("blocked.example"));
    }

    #[test]
    fn run_wasm_plugin_enforces_module_size_quota() {
        let mut policy = test_policy();
        policy.max_module_size_bytes = 32;
        let input = serde_json::json!({
            "module_wat": "(module (func (export \"run\") (result i32) i32.const 1))"
        });
        let input_json = serde_json::to_vec(&input).expect("input JSON should serialize");

        let error = run_wasm_plugin(&policy, input_json.as_slice(), Duration::from_secs(2))
            .expect_err("oversized module payload must be denied");

        assert_eq!(error.kind, WasmPluginRunErrorKind::QuotaExceeded);
    }

    #[test]
    fn run_wasm_plugin_reports_quota_exceeded_for_fuel_exhaustion() {
        let mut policy = test_policy();
        policy.fuel_budget = 5_000;
        let input = serde_json::json!({
            "module_wat": r#"
                (module
                    (func (export "run") (result i32)
                        (loop
                            br 0
                        )
                        i32.const 0
                    )
                )
            "#
        });
        let input_json = serde_json::to_vec(&input).expect("input JSON should serialize");

        let error = run_wasm_plugin(&policy, input_json.as_slice(), Duration::from_secs(2))
            .expect_err("infinite loop plugin should hit runtime fuel quota");

        assert_eq!(error.kind, WasmPluginRunErrorKind::QuotaExceeded);
    }

    #[test]
    fn run_wasm_plugin_reports_wall_clock_timeout() {
        let mut policy = test_policy();
        policy.fuel_budget = 1_000_000_000;
        let input = serde_json::json!({
            "module_wat": r#"
                (module
                    (func (export "run") (result i32)
                        (loop
                            br 0
                        )
                        i32.const 0
                    )
                )
            "#
        });
        let input_json = serde_json::to_vec(&input).expect("input JSON should serialize");

        let error = run_wasm_plugin(&policy, input_json.as_slice(), Duration::from_millis(10))
            .expect_err("infinite loop plugin should hit wall-clock timeout");

        assert_eq!(error.kind, WasmPluginRunErrorKind::TimedOut);
    }

    #[test]
    fn run_wasm_plugin_rejects_dot_only_host_entries() {
        let policy = test_policy();
        let input = serde_json::json!({
            "module_wat": "(module (func (export \"run\") (result i32) i32.const 1))",
            "capabilities": {
                "http_hosts": ["..."]
            }
        });
        let input_json = serde_json::to_vec(&input).expect("input JSON should serialize");

        let error = run_wasm_plugin(&policy, input_json.as_slice(), Duration::from_secs(2))
            .expect_err("dot-only host capability must fail fast");

        assert_eq!(error.kind, WasmPluginRunErrorKind::InvalidInput);
        assert!(error.message.contains("invalid host"), "error should name host validation");
    }

    #[test]
    fn run_wasm_plugin_rejects_unknown_root_fields() {
        let policy = test_policy();
        let input = serde_json::json!({
            "skill_id": "acme.echo_http",
            "module_wat": "(module (func (export \"run\") (result i32) i32.const 1))",
            "unexpected": true
        });
        let input_json = serde_json::to_vec(&input).expect("input JSON should serialize");

        let error = run_wasm_plugin(&policy, input_json.as_slice(), Duration::from_secs(2))
            .expect_err("unknown root fields must still be rejected");

        assert_eq!(error.kind, WasmPluginRunErrorKind::InvalidInput);
        assert!(
            error.message.contains("unknown field"),
            "parse error should identify deny_unknown_fields rejection"
        );
    }

    #[test]
    fn run_wasm_plugin_reports_import_contract_mismatch_as_invalid_input() {
        let policy = test_policy();
        let input = serde_json::json!({
            "module_wat": r#"
                (module
                    (import "palyra:plugins/host-capabilities@0.1.0" "http-count" (func $http_count (param i32) (result i32)))
                    (func (export "run") (result i32) i32.const 7)
                )
            "#
        });
        let input_json = serde_json::to_vec(&input).expect("input JSON should serialize");

        let error = run_wasm_plugin(&policy, input_json.as_slice(), Duration::from_secs(2))
            .expect_err("invalid import contract must fail");

        assert_eq!(error.kind, WasmPluginRunErrorKind::InvalidInput);
        assert!(
            error.message.contains("import contract"),
            "error should explain host capability import contract mismatch"
        );
    }
}
