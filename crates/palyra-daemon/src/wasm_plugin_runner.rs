use std::{
    collections::BTreeSet,
    convert::TryFrom,
    time::{Duration, Instant},
};

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine};
use palyra_plugins_runtime::{CapabilityGrantSet, RuntimeError, RuntimeLimits, WasmRuntime};
use palyra_plugins_sdk::DEFAULT_RUNTIME_ENTRYPOINT;
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmPluginRunnerPolicy {
    pub enabled: bool,
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
    module_wat: Option<String>,
    module_base64: Option<String>,
    entrypoint: Option<String>,
    #[serde(default)]
    capabilities: RequestedCapabilities,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestedCapabilities {
    #[serde(default)]
    http_hosts: Vec<String>,
    #[serde(default)]
    secrets: Vec<String>,
    #[serde(default)]
    storage_prefixes: Vec<String>,
    #[serde(default)]
    channels: Vec<String>,
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
    let module_bytes = decode_module_bytes(policy, &input)?;
    let entrypoint = parse_entrypoint(input.entrypoint.as_deref())?;
    let requested_http_hosts = normalize_host_allowlist(
        input.capabilities.http_hosts.as_slice(),
        "capabilities.http_hosts",
    )?;
    let requested_secrets = normalize_identifier_allowlist(
        input.capabilities.secrets.as_slice(),
        "capabilities.secrets",
        "secret handle",
    )?;
    let requested_storage_prefixes = normalize_storage_prefix_allowlist(
        input.capabilities.storage_prefixes.as_slice(),
        "capabilities.storage_prefixes",
    )?;
    let requested_channels = normalize_identifier_allowlist(
        input.capabilities.channels.as_slice(),
        "capabilities.channels",
        "channel handle",
    )?;

    let allowed_http_hosts = normalize_host_allowlist(
        policy.allowed_http_hosts.as_slice(),
        "policy.allowed_http_hosts",
    )?;
    let allowed_secrets = normalize_identifier_allowlist(
        policy.allowed_secrets.as_slice(),
        "policy.allowed_secrets",
        "secret handle",
    )?;
    let allowed_storage_prefixes = normalize_storage_prefix_allowlist(
        policy.allowed_storage_prefixes.as_slice(),
        "policy.allowed_storage_prefixes",
    )?;
    let allowed_channels = normalize_identifier_allowlist(
        policy.allowed_channels.as_slice(),
        "policy.allowed_channels",
        "channel handle",
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
        fuel_budget: policy.fuel_budget,
        max_memory_bytes: usize::try_from(policy.max_memory_bytes).map_err(|_| {
            WasmPluginRunError {
                kind: WasmPluginRunErrorKind::InvalidInput,
                message: "wasm runtime memory quota exceeds platform usize range".to_owned(),
            }
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
    let started_at = Instant::now();
    let execution = runtime
        .execute_i32_entrypoint_with_timeout(
            module_bytes.as_slice(),
            entrypoint.as_str(),
            &grants,
            timeout,
        )
        .map_err(map_runtime_error)?;
    let duration = started_at.elapsed();
    let output_json = serde_json::to_vec(&json!({
        "exit_code": execution.exit_code,
        "entrypoint": entrypoint,
        "duration_ms": duration_to_millis(duration),
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

fn decode_module_bytes(
    policy: &WasmPluginRunnerPolicy,
    input: &WasmPluginRunInput,
) -> Result<Vec<u8>, WasmPluginRunError> {
    let module_bytes = match (input.module_wat.as_ref(), input.module_base64.as_ref()) {
        (Some(_), Some(_)) => {
            return Err(WasmPluginRunError {
                kind: WasmPluginRunErrorKind::InvalidInput,
                message: "palyra.plugin.run accepts either module_wat or module_base64, not both"
                    .to_owned(),
            });
        }
        (Some(module_wat), None) => module_wat.as_bytes().to_vec(),
        (None, Some(module_base64)) => {
            BASE64_STANDARD.decode(module_base64.as_bytes()).map_err(|error| {
                WasmPluginRunError {
                    kind: WasmPluginRunErrorKind::InvalidInput,
                    message: format!("palyra.plugin.run module_base64 is invalid: {error}"),
                }
            })?
        }
        (None, None) => {
            return Err(WasmPluginRunError {
                kind: WasmPluginRunErrorKind::InvalidInput,
                message: "palyra.plugin.run requires exactly one of module_wat or module_base64"
                    .to_owned(),
            });
        }
    };
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
    Ok(module_bytes)
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
            ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '_' | '-')
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

    use super::{run_wasm_plugin, WasmPluginRunErrorKind, WasmPluginRunnerPolicy};
    use serde_json::Value;

    fn test_policy() -> WasmPluginRunnerPolicy {
        WasmPluginRunnerPolicy {
            enabled: true,
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
