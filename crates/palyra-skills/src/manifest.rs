use std::collections::BTreeSet;

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use palyra_common::{build_metadata, CANONICAL_PROTOCOL_MAJOR};
use serde_json::Value;

use crate::artifact::normalize_artifact_path;
use crate::constants::{LEGACY_SKILL_MANIFEST_VERSION, SKILL_MANIFEST_VERSION};
use crate::error::SkillPackagingError;
use crate::models::{
    SkillBuilderMetadata, SkillCompat, SkillConfigContract, SkillConfigProperty,
    SkillConfigValueType, SkillManifest, SkillManifestWarning, SkillManifestWarningSeverity,
};

pub fn parse_manifest_toml(raw: &str) -> Result<SkillManifest, SkillPackagingError> {
    let manifest = toml::from_str::<SkillManifest>(raw)
        .map_err(|error| SkillPackagingError::ManifestParse(error.to_string()))?;
    validate_manifest(&manifest)?;
    Ok(manifest)
}

pub fn parse_ed25519_signing_key(secret: &[u8]) -> Result<[u8; 32], SkillPackagingError> {
    if secret.len() == 32 {
        let mut key = [0_u8; 32];
        key.copy_from_slice(secret);
        return Ok(key);
    }
    let trimmed = trim_ascii_whitespace(secret);
    if trimmed.len() == 32 {
        let mut key = [0_u8; 32];
        key.copy_from_slice(trimmed);
        return Ok(key);
    }
    let utf8 = std::str::from_utf8(trimmed)
        .map_err(|_| SkillPackagingError::InvalidSigningKeyLength { actual: trimmed.len() })?;
    let text = utf8.trim();
    if let Ok(hex_decoded) = hex::decode(text) {
        if hex_decoded.len() == 32 {
            let mut key = [0_u8; 32];
            key.copy_from_slice(hex_decoded.as_slice());
            return Ok(key);
        }
    }
    if let Ok(base64_decoded) = BASE64_STANDARD.decode(text.as_bytes()) {
        if base64_decoded.len() == 32 {
            let mut key = [0_u8; 32];
            key.copy_from_slice(base64_decoded.as_slice());
            return Ok(key);
        }
    }
    Err(SkillPackagingError::InvalidSigningKeyLength { actual: trimmed.len() })
}

fn validate_manifest(manifest: &SkillManifest) -> Result<(), SkillPackagingError> {
    if !matches!(
        manifest.manifest_version,
        LEGACY_SKILL_MANIFEST_VERSION | SKILL_MANIFEST_VERSION
    ) {
        return Err(SkillPackagingError::ManifestValidation(format!(
            "manifest_version must equal {} or {}",
            LEGACY_SKILL_MANIFEST_VERSION, SKILL_MANIFEST_VERSION
        )));
    }
    let publisher = normalize_identifier(manifest.publisher.as_str(), "publisher")?;
    normalize_skill_id(manifest.skill_id.as_str())?;
    parse_semver(manifest.version.as_str(), "version")?;
    parse_semver(manifest.compat.min_palyra_version.as_str(), "compat.min_palyra_version")?;
    if manifest.name.trim().is_empty() {
        return Err(SkillPackagingError::ManifestValidation("name cannot be empty".to_owned()));
    }
    if manifest.entrypoints.tools.is_empty() {
        return Err(SkillPackagingError::ManifestValidation(
            "entrypoints.tools cannot be empty".to_owned(),
        ));
    }

    let mut tool_ids = BTreeSet::new();
    for tool in &manifest.entrypoints.tools {
        let tool_id = normalize_identifier(tool.id.as_str(), "entrypoints.tools[].id")?;
        if !tool_id.starts_with(format!("{publisher}.").as_str()) {
            return Err(SkillPackagingError::ManifestValidation(format!(
                "tool id '{}' must be namespaced with '{}.'",
                tool.id, publisher
            )));
        }
        if !tool_ids.insert(tool_id) {
            return Err(SkillPackagingError::ManifestValidation(format!(
                "duplicate tool id '{}'",
                tool.id
            )));
        }
        if tool.name.trim().is_empty() || tool.description.trim().is_empty() {
            return Err(SkillPackagingError::ManifestValidation(format!(
                "tool '{}' must include non-empty name and description",
                tool.id
            )));
        }
        if !tool.input_schema.is_object() || !tool.output_schema.is_object() {
            return Err(SkillPackagingError::ManifestValidation(format!(
                "tool '{}' schemas must be JSON objects",
                tool.id
            )));
        }
    }
    for path in &manifest.capabilities.filesystem.read_roots {
        validate_capability_path(path, manifest.capabilities.wildcard_opt_in.filesystem)?;
    }
    for path in &manifest.capabilities.filesystem.write_roots {
        validate_capability_path(path, manifest.capabilities.wildcard_opt_in.filesystem)?;
    }
    for host in &manifest.capabilities.http_egress_allowlist {
        validate_host(host, manifest.capabilities.wildcard_opt_in.http_egress)?;
    }
    for scope in &manifest.capabilities.secrets {
        validate_secret_scope(scope.scope.as_str())?;
        if scope.key_names.is_empty() {
            return Err(SkillPackagingError::ManifestValidation(format!(
                "secret scope '{}' must list key_names",
                scope.scope
            )));
        }
        for key in &scope.key_names {
            validate_identifier_or_wildcard(
                key,
                "capabilities.secrets[].key_names",
                manifest.capabilities.wildcard_opt_in.secrets,
            )?;
        }
    }
    for capability in &manifest.capabilities.device_capabilities {
        validate_identifier_or_wildcard(
            capability,
            "capabilities.device_capabilities",
            manifest.capabilities.wildcard_opt_in.device,
        )?;
    }
    for capability in &manifest.capabilities.node_capabilities {
        validate_identifier_or_wildcard(
            capability,
            "capabilities.node_capabilities",
            manifest.capabilities.wildcard_opt_in.node,
        )?;
    }
    if manifest.capabilities.quotas.wall_clock_timeout_ms == 0
        || manifest.capabilities.quotas.max_memory_bytes < 64 * 1024
        || manifest.capabilities.quotas.fuel_budget == 0
    {
        return Err(SkillPackagingError::ManifestValidation(
            "capabilities.quotas values must be non-zero and memory >= 65536".to_owned(),
        ));
    }
    if let Some(builder) = manifest.builder.as_ref() {
        validate_builder_metadata(builder)?;
    }
    validate_operator_metadata(manifest, publisher.as_str())?;
    Ok(())
}

pub(crate) fn assert_runtime_compatibility(
    compat: &SkillCompat,
) -> Result<(), SkillPackagingError> {
    if compat.required_protocol_major > CANONICAL_PROTOCOL_MAJOR {
        return Err(SkillPackagingError::UnsupportedProtocolMajor {
            requested: compat.required_protocol_major,
            current: CANONICAL_PROTOCOL_MAJOR,
        });
    }
    let requested = parse_semver(compat.min_palyra_version.as_str(), "compat.min_palyra_version")?;
    let current_raw = build_metadata().version.to_owned();
    let current = parse_semver(current_raw.as_str(), "runtime version")?;
    if requested > current {
        return Err(SkillPackagingError::UnsupportedRuntimeVersion {
            requested: compat.min_palyra_version.clone(),
            current: current_raw,
        });
    }
    Ok(())
}

pub(crate) fn validate_sbom_payload(bytes: &[u8]) -> Result<(), SkillPackagingError> {
    let value = serde_json::from_slice::<Value>(bytes)
        .map_err(|error| SkillPackagingError::InvalidSbom(error.to_string()))?;
    let object = value
        .as_object()
        .ok_or_else(|| SkillPackagingError::InvalidSbom("SBOM must be JSON object".to_owned()))?;
    if object.get("bomFormat").and_then(Value::as_str) != Some("CycloneDX") {
        return Err(SkillPackagingError::InvalidSbom(
            "sbom.cdx.json must declare bomFormat='CycloneDX'".to_owned(),
        ));
    }
    if object.get("specVersion").and_then(Value::as_str).unwrap_or_default().is_empty() {
        return Err(SkillPackagingError::InvalidSbom(
            "sbom.cdx.json must include specVersion".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_provenance_payload(bytes: &[u8]) -> Result<(), SkillPackagingError> {
    let value = serde_json::from_slice::<Value>(bytes)
        .map_err(|error| SkillPackagingError::InvalidProvenance(error.to_string()))?;
    let object = value.as_object().ok_or_else(|| {
        SkillPackagingError::InvalidProvenance("provenance must be JSON object".to_owned())
    })?;
    if object.get("builder").and_then(Value::as_object).is_none() {
        return Err(SkillPackagingError::InvalidProvenance(
            "provenance must include builder object".to_owned(),
        ));
    }
    if object.get("subject").and_then(Value::as_array).is_none_or(Vec::is_empty) {
        return Err(SkillPackagingError::InvalidProvenance(
            "provenance must include non-empty subject array".to_owned(),
        ));
    }
    Ok(())
}

fn validate_capability_path(path: &str, wildcard_allowed: bool) -> Result<(), SkillPackagingError> {
    if path.contains('*') && !wildcard_allowed {
        return Err(SkillPackagingError::ManifestValidation(format!(
            "capability path '{}' uses wildcard without explicit opt-in",
            path
        )));
    }
    normalize_artifact_path(path)?;
    Ok(())
}

fn validate_host(host: &str, wildcard_allowed: bool) -> Result<(), SkillPackagingError> {
    if host.contains('*') {
        if wildcard_allowed {
            return Ok(());
        }
        return Err(SkillPackagingError::ManifestValidation(format!(
            "host '{}' uses wildcard without explicit opt-in",
            host
        )));
    }
    let normalized = host.trim().trim_end_matches('.').to_ascii_lowercase();
    if normalized.is_empty()
        || normalized.contains("..")
        || normalized.starts_with('.')
        || normalized.ends_with('.')
        || normalized.starts_with('-')
        || normalized.ends_with('-')
        || !normalized.chars().all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-'))
    {
        return Err(SkillPackagingError::ManifestValidation(format!("invalid host '{}'", host)));
    }
    Ok(())
}

fn validate_secret_scope(scope: &str) -> Result<(), SkillPackagingError> {
    let normalized = scope.trim();
    if normalized == "global" {
        return Ok(());
    }
    for prefix in ["principal:", "channel:", "skill:"] {
        if let Some(suffix) = normalized.strip_prefix(prefix) {
            normalize_identifier(suffix, "capabilities.secrets[].scope")?;
            return Ok(());
        }
    }
    Err(SkillPackagingError::ManifestValidation(format!("invalid secret scope '{}'", scope)))
}

fn validate_builder_metadata(
    builder: &SkillBuilderMetadata,
) -> Result<(), SkillPackagingError> {
    if !builder.experimental {
        return Err(SkillPackagingError::ManifestValidation(
            "builder.experimental must stay true for generated builder outputs".to_owned(),
        ));
    }
    if builder.source_kind.trim().is_empty() {
        return Err(SkillPackagingError::ManifestValidation(
            "builder.source_kind cannot be empty".to_owned(),
        ));
    }
    if builder.source_ref.trim().is_empty() {
        return Err(SkillPackagingError::ManifestValidation(
            "builder.source_ref cannot be empty".to_owned(),
        ));
    }
    if builder.rollout_flag.trim().is_empty() {
        return Err(SkillPackagingError::ManifestValidation(
            "builder.rollout_flag cannot be empty".to_owned(),
        ));
    }
    validate_builder_artifact_path(
        builder.checklist.capability_declaration_path.as_str(),
        "builder.checklist.capability_declaration_path",
    )?;
    validate_builder_artifact_path(
        builder.checklist.provenance_path.as_str(),
        "builder.checklist.provenance_path",
    )?;
    validate_builder_artifact_path(
        builder.checklist.test_harness_path.as_str(),
        "builder.checklist.test_harness_path",
    )?;
    Ok(())
}

fn validate_operator_metadata(
    manifest: &SkillManifest,
    publisher: &str,
) -> Result<(), SkillPackagingError> {
    let operator = &manifest.operator;
    if manifest.manifest_version == LEGACY_SKILL_MANIFEST_VERSION && !operator.is_empty() {
        return Err(SkillPackagingError::ManifestValidation(format!(
            "operator metadata requires manifest_version {}",
            SKILL_MANIFEST_VERSION
        )));
    }
    if operator.is_empty() {
        return Ok(());
    }

    validate_optional_operator_text(operator.display_name.as_deref(), "operator.display_name")?;
    validate_optional_operator_text(operator.summary.as_deref(), "operator.summary")?;
    validate_optional_operator_text(operator.description.as_deref(), "operator.description")?;
    for category in &operator.categories {
        validate_operator_label(category.as_str(), "operator.categories")?;
    }
    for tag in &operator.tags {
        validate_operator_label(tag.as_str(), "operator.tags")?;
    }
    if let Some(url) = operator.docs_url.as_deref() {
        validate_operator_docs_url(url)?;
    }

    if let Some(tool_id) = operator.plugin.default_tool_id.as_deref() {
        let normalized = normalize_identifier(tool_id, "operator.plugin.default_tool_id")?;
        if !manifest.entrypoints.tools.iter().any(|tool| tool.id == normalized) {
            return Err(SkillPackagingError::ManifestValidation(format!(
                "operator.plugin.default_tool_id '{}' must reference an entrypoints.tools id",
                tool_id
            )));
        }
        if !normalized.starts_with(format!("{publisher}.").as_str()) {
            return Err(SkillPackagingError::ManifestValidation(format!(
                "operator.plugin.default_tool_id '{}' must be namespaced with '{}.'",
                tool_id, publisher
            )));
        }
    }
    if let Some(module_path) = operator.plugin.default_module_path.as_deref() {
        validate_plugin_module_path(module_path, "operator.plugin.default_module_path")?;
    }
    if let Some(entrypoint) = operator.plugin.default_entrypoint.as_deref() {
        validate_plugin_entrypoint(entrypoint, "operator.plugin.default_entrypoint")?;
    }
    if let Some(config) = operator.config.as_ref() {
        validate_config_contract(config)?;
    }
    Ok(())
}

pub(crate) fn collect_manifest_warnings(manifest: &SkillManifest) -> Vec<SkillManifestWarning> {
    let mut warnings = Vec::new();
    if manifest.manifest_version == LEGACY_SKILL_MANIFEST_VERSION {
        warnings.push(SkillManifestWarning {
            code: "legacy_manifest_version".to_owned(),
            severity: SkillManifestWarningSeverity::Warning,
            message: format!(
                "manifest_version {} is still supported for compatibility, but {} is now recommended",
                LEGACY_SKILL_MANIFEST_VERSION, SKILL_MANIFEST_VERSION
            ),
        });
    }
    if manifest.operator.is_empty() {
        warnings.push(SkillManifestWarning {
            code: "missing_operator_metadata".to_owned(),
            severity: SkillManifestWarningSeverity::Warning,
            message: "operator-facing metadata is missing; inventory and doctor output will be degraded"
                .to_owned(),
        });
    } else {
        if manifest.operator.display_name.is_none() {
            warnings.push(SkillManifestWarning {
                code: "missing_operator_display_name".to_owned(),
                severity: SkillManifestWarningSeverity::Warning,
                message: "operator.display_name is missing".to_owned(),
            });
        }
        if manifest
            .operator
            .config
            .as_ref()
            .is_some_and(|config| config.properties.is_empty())
        {
            warnings.push(SkillManifestWarning {
                code: "empty_operator_config_contract".to_owned(),
                severity: SkillManifestWarningSeverity::Warning,
                message: "operator.config is present but does not declare any properties".to_owned(),
            });
        }
        for (name, property) in manifest
            .operator
            .config
            .as_ref()
            .into_iter()
            .flat_map(|config| config.properties.iter())
        {
            if !property.redacted && looks_secretish(name.as_str()) {
                warnings.push(SkillManifestWarning {
                    code: "operator_config_redaction_recommended".to_owned(),
                    severity: SkillManifestWarningSeverity::Warning,
                    message: format!(
                        "operator.config property '{}' looks secret-like but is not marked redacted",
                        name
                    ),
                });
            }
        }
    }
    warnings
}

fn validate_builder_artifact_path(path: &str, field_name: &str) -> Result<(), SkillPackagingError> {
    if path.trim().is_empty() {
        return Err(SkillPackagingError::ManifestValidation(format!(
            "{field_name} cannot be empty"
        )));
    }
    normalize_artifact_path(path)?;
    Ok(())
}

fn validate_identifier_or_wildcard(
    value: &str,
    field: &str,
    wildcard_allowed: bool,
) -> Result<(), SkillPackagingError> {
    if value.contains('*') {
        if wildcard_allowed {
            return Ok(());
        }
        return Err(SkillPackagingError::ManifestValidation(format!(
            "{field} contains wildcard without explicit opt-in"
        )));
    }
    normalize_identifier(value, field).map(|_| ())
}

fn validate_optional_operator_text(
    value: Option<&str>,
    field_name: &'static str,
) -> Result<(), SkillPackagingError> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.trim().is_empty() {
        return Err(SkillPackagingError::ManifestValidation(format!(
            "{field_name} cannot be empty"
        )));
    }
    Ok(())
}

fn validate_operator_label(value: &str, field_name: &'static str) -> Result<(), SkillPackagingError> {
    let normalized = value.trim();
    if normalized.is_empty()
        || !normalized
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '_' | '-'))
    {
        return Err(SkillPackagingError::ManifestValidation(format!(
            "{field_name} entries must use [a-z0-9._-]"
        )));
    }
    Ok(())
}

fn validate_operator_docs_url(url: &str) -> Result<(), SkillPackagingError> {
    let normalized = url.trim();
    if normalized.is_empty()
        || !normalized.starts_with("https://")
        || normalized.contains(' ')
        || normalized.contains('#')
    {
        return Err(SkillPackagingError::ManifestValidation(
            "operator.docs_url must be an https URL without fragments or spaces".to_owned(),
        ));
    }
    Ok(())
}

fn validate_plugin_module_path(
    module_path: &str,
    field_name: &'static str,
) -> Result<(), SkillPackagingError> {
    if module_path.contains('\0')
        || module_path.contains("..")
        || !module_path.starts_with("modules/")
        || !module_path.ends_with(".wasm")
    {
        return Err(SkillPackagingError::ManifestValidation(format!(
            "{field_name} must reference a modules/*.wasm entry"
        )));
    }
    normalize_artifact_path(module_path)?;
    Ok(())
}

fn validate_plugin_entrypoint(
    entrypoint: &str,
    field_name: &'static str,
) -> Result<(), SkillPackagingError> {
    let normalized = entrypoint.trim();
    if normalized.is_empty()
        || !normalized
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '_' | '-'))
    {
        return Err(SkillPackagingError::ManifestValidation(format!(
            "{field_name} must use [a-z0-9_-]"
        )));
    }
    Ok(())
}

fn validate_config_contract(contract: &SkillConfigContract) -> Result<(), SkillPackagingError> {
    if contract.schema_version == 0 {
        return Err(SkillPackagingError::ManifestValidation(
            "operator.config.schema_version must be non-zero".to_owned(),
        ));
    }
    for required in &contract.required {
        validate_config_key(required.as_str(), "operator.config.required")?;
        if !contract.properties.contains_key(required) {
            return Err(SkillPackagingError::ManifestValidation(format!(
                "operator.config.required references unknown property '{}'",
                required
            )));
        }
    }
    for (name, property) in &contract.properties {
        validate_config_key(name.as_str(), "operator.config.properties")?;
        validate_config_property(name.as_str(), property)?;
    }
    Ok(())
}

fn validate_config_property(
    name: &str,
    property: &SkillConfigProperty,
) -> Result<(), SkillPackagingError> {
    validate_optional_operator_text(property.title.as_deref(), "operator.config.properties[].title")?;
    validate_optional_operator_text(
        property.description.as_deref(),
        "operator.config.properties[].description",
    )?;
    if !property.enum_values.is_empty() {
        if property.value_type != SkillConfigValueType::String {
            return Err(SkillPackagingError::ManifestValidation(format!(
                "operator.config property '{}' enum_values require type='string'",
                name
            )));
        }
        let mut unique = BTreeSet::new();
        for value in &property.enum_values {
            if value.trim().is_empty() {
                return Err(SkillPackagingError::ManifestValidation(format!(
                    "operator.config property '{}' enum_values cannot contain empty strings",
                    name
                )));
            }
            unique.insert(value.trim().to_owned());
        }
        if unique.len() != property.enum_values.len() {
            return Err(SkillPackagingError::ManifestValidation(format!(
                "operator.config property '{}' enum_values must be unique",
                name
            )));
        }
    }
    if let Some(default) = property.default.as_ref() {
        validate_config_value_type(name, &property.value_type, default)?;
        if !property.enum_values.is_empty() {
            let value = default.as_str().ok_or_else(|| {
                SkillPackagingError::ManifestValidation(format!(
                    "operator.config property '{}' default must be a string because enum_values are declared",
                    name
                ))
            })?;
            if !property.enum_values.iter().any(|candidate| candidate == value) {
                return Err(SkillPackagingError::ManifestValidation(format!(
                    "operator.config property '{}' default must be one of enum_values",
                    name
                )));
            }
        }
    }
    Ok(())
}

fn validate_config_key(name: &str, field_name: &'static str) -> Result<(), SkillPackagingError> {
    let normalized = name.trim();
    if normalized.is_empty()
        || !normalized
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '_' | '-'))
    {
        return Err(SkillPackagingError::ManifestValidation(format!(
            "{field_name} keys must use [a-z0-9._-]"
        )));
    }
    Ok(())
}

fn validate_config_value_type(
    property_name: &str,
    value_type: &SkillConfigValueType,
    value: &Value,
) -> Result<(), SkillPackagingError> {
    let valid = match value_type {
        SkillConfigValueType::String => value.is_string(),
        SkillConfigValueType::Integer => value.as_i64().is_some() || value.as_u64().is_some(),
        SkillConfigValueType::Number => value.is_number(),
        SkillConfigValueType::Boolean => value.is_boolean(),
        SkillConfigValueType::StringList => value.as_array().is_some_and(|values| {
            values.iter().all(|candidate| candidate.as_str().is_some())
        }),
    };
    if valid {
        return Ok(());
    }
    Err(SkillPackagingError::ManifestValidation(format!(
        "operator.config property '{}' default does not match declared type '{:?}'",
        property_name, value_type
    )))
}

fn looks_secretish(name: &str) -> bool {
    let normalized = name.to_ascii_lowercase();
    ["secret", "token", "password", "api_key", "client_key"]
        .iter()
        .any(|needle| normalized.contains(needle))
}

fn normalize_skill_id(value: &str) -> Result<String, SkillPackagingError> {
    let normalized = normalize_identifier(value, "skill_id")?;
    if normalized.contains(':') || normalized.split('.').any(str::is_empty) {
        return Err(SkillPackagingError::ManifestValidation(
            "skill_id must use non-empty dot-separated [a-z0-9_-] segments".to_owned(),
        ));
    }
    Ok(normalized)
}

pub(crate) fn normalize_identifier(
    value: &str,
    field: &str,
) -> Result<String, SkillPackagingError> {
    let normalized = value.trim();
    if normalized.is_empty() {
        return Err(SkillPackagingError::ManifestValidation(format!("{field} cannot be empty")));
    }
    if !normalized.chars().all(|ch| {
        ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '_' | '-' | ':')
    }) {
        return Err(SkillPackagingError::ManifestValidation(format!(
            "{field} must use [a-z0-9._-:]"
        )));
    }
    Ok(normalized.to_owned())
}

pub(crate) fn normalize_public_key_hex(value: &str) -> Result<String, SkillPackagingError> {
    let normalized = value.trim().to_ascii_lowercase();
    let decoded = hex::decode(normalized.as_str()).map_err(|_| {
        SkillPackagingError::ManifestValidation("trusted publisher key must be hex".to_owned())
    })?;
    if decoded.len() != 32 {
        return Err(SkillPackagingError::ManifestValidation(
            "trusted publisher key must decode to 32 bytes".to_owned(),
        ));
    }
    Ok(normalized)
}

fn parse_semver(value: &str, field: &str) -> Result<(u32, u32, u32), SkillPackagingError> {
    let parts = value.trim().split('.').collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(SkillPackagingError::ManifestValidation(format!(
            "{field} must use semantic version major.minor.patch"
        )));
    }
    let major = parts[0]
        .parse::<u32>()
        .map_err(|_| SkillPackagingError::ManifestValidation(format!("{field} major invalid")))?;
    let minor = parts[1]
        .parse::<u32>()
        .map_err(|_| SkillPackagingError::ManifestValidation(format!("{field} minor invalid")))?;
    let patch = parts[2]
        .parse::<u32>()
        .map_err(|_| SkillPackagingError::ManifestValidation(format!("{field} patch invalid")))?;
    Ok((major, minor, patch))
}

fn trim_ascii_whitespace(raw: &[u8]) -> &[u8] {
    let start = raw.iter().position(|value| !value.is_ascii_whitespace()).unwrap_or(raw.len());
    let end =
        raw.iter().rposition(|value| !value.is_ascii_whitespace()).map_or(start, |index| index + 1);
    &raw[start..end]
}
