//! Behavior tests for manifest validation, build/verify/TOFU flows, package
//! guardrails, trust-store handling, security audit, and lifecycle gates.
//! Several assertions pin error precedence and serialized field names that
//! external golden fixtures depend on.

use super::ExtensionPackageRegistry;
use super::{
    audit_skill_artifact_security, build_signed_skill_artifact, builder_manifest_requires_review,
    capability_grants_from_manifest, evaluate_extension_contract_fixture, evaluate_skill_fixture,
    extension_capability_grants_from_skill_manifest, extension_doctor_for_skill_artifact,
    extension_enable_gate, extension_record_from_skill_artifact,
    extract_self_improvement_candidate, inspect_skill_artifact, parse_ed25519_signing_key,
    parse_manifest_toml, plan_self_improvement_rollback, policy_requests_from_manifest,
    skill_eval_run_record, skill_extension_package_id, skill_scaffold_artifact_plan,
    verify_skill_artifact, ArtifactFile, ExtensionCapabilityClass, ExtensionCapabilityGrant,
    ExtensionContractFixture, ExtensionLifecycleTransitionRequest, ExtensionPackageSource,
    ExtensionPackageStatus, InMemoryExtensionPackageRegistry, SkillArtifactBuildRequest,
    SkillAuditCheckStatus, SkillEvalFixture, SkillEvalOutcome, SkillPackagingError,
    SkillSecurityAuditPolicy, SkillTrustStore, TrustDecision, MAX_ARTIFACT_BYTES, MAX_ENTRIES,
    SBOM_PATH, SIGNATURE_PATH, SKILL_MANIFEST_PATH, SKILL_MANIFEST_VERSION,
};
use base64::Engine as _;

fn sample_manifest() -> String {
    r#"
    manifest_version = 2
skill_id = "acme.echo_http"
name = "Echo + HTTP"
version = "1.0.0"
publisher = "acme"

[entrypoints]
[[entrypoints.tools]]
id = "acme.echo"
name = "echo"
description = "Echo payload"
input_schema = { type = "object", properties = { text = { type = "string" } } }
output_schema = { type = "object", properties = { echo = { type = "string" } } }
risk = { default_sensitive = false, requires_approval = false }

[[entrypoints.tools]]
id = "acme.http_get"
name = "http_get"
description = "Fetch one URL"
input_schema = { type = "object", properties = { url = { type = "string" } } }
output_schema = { type = "object", properties = { status = { type = "number" } } }
risk = { default_sensitive = true, requires_approval = true }

[capabilities.filesystem]
read_roots = ["skills/data"]
write_roots = ["skills/cache"]

[capabilities]
http_egress_allowlist = ["api.example.com"]
device_capabilities = []
node_capabilities = []

[[capabilities.secrets]]
scope = "skill:acme.echo_http"
key_names = ["api_token"]

[capabilities.quotas]
wall_clock_timeout_ms = 2000
fuel_budget = 5000000
max_memory_bytes = 33554432

[compat]
required_protocol_major = 1
min_palyra_version = "0.1.0"

[operator]
display_name = "Echo HTTP"
summary = "Echoes a payload and can call one external API."
description = "Operator-facing sample skill used by regression tests."
categories = ["messaging", "network"]
tags = ["sample", "echo"]
docs_url = "https://example.com/skills/echo-http"

[operator.plugin]
default_tool_id = "acme.echo"
default_module_path = "modules/module.wasm"
default_entrypoint = "run"

[operator.config]
schema_version = 1
required = ["api_base_url", "api_token"]

[operator.config.properties.api_base_url]
type = "string"
title = "API base URL"
description = "Outbound endpoint"
default = "https://api.example.com"

[operator.config.properties.api_token]
type = "string"
title = "API token"
description = "Credential used by the sample skill"
redacted = true
"#
    .trim()
    .to_owned()
}

#[test]
fn manifest_compat_aliases_accept_legacy_field_names() {
    let legacy_manifest = sample_manifest()
        .replace("required_protocol_major", "min_protocol_major")
        .replace("min_palyra_version", "min_runtime_version");
    let parsed = parse_manifest_toml(legacy_manifest.as_str())
        .expect("legacy compat fields should still parse");
    assert_eq!(parsed.compat.required_protocol_major, 1);
    assert_eq!(parsed.compat.min_palyra_version, "0.1.0");
}

#[test]
fn manifest_accepts_legacy_manifest_version_without_operator_metadata() {
    let legacy_manifest = sample_manifest()
        .replace("manifest_version = 2", "manifest_version = 1")
        .split("\n\n[operator]")
        .next()
        .expect("sample manifest should contain operator section")
        .to_owned();
    let parsed =
        parse_manifest_toml(legacy_manifest.as_str()).expect("legacy manifest should still parse");
    assert_eq!(parsed.manifest_version, 1);
    assert!(parsed.operator.is_empty(), "legacy manifest should have empty operator metadata");
}

#[test]
fn manifest_serialization_uses_new_compat_field_names() {
    let output = build_signed_skill_artifact(sample_request()).expect("artifact should build");
    let mut entries = super::decode_zip(output.artifact_bytes.as_slice()).expect("zip decode");
    let manifest_bytes = entries.remove(SKILL_MANIFEST_PATH).expect("manifest entry should exist");
    let manifest_toml = String::from_utf8(manifest_bytes).expect("manifest should be utf8");
    assert!(
        manifest_toml.contains("required_protocol_major = 1"),
        "manifest should serialize new required_protocol_major field"
    );
    assert!(
        manifest_toml.contains("min_palyra_version = \"0.1.0\""),
        "manifest should serialize new min_palyra_version field"
    );
    assert!(
        !manifest_toml.contains("min_protocol_major"),
        "manifest serialization should avoid legacy compat field name"
    );
    assert!(
        !manifest_toml.contains("min_runtime_version"),
        "manifest serialization should avoid legacy compat field name"
    );
}

#[test]
fn manifest_serialization_uses_current_manifest_version() {
    let output = build_signed_skill_artifact(sample_request()).expect("artifact should build");
    let mut entries = super::decode_zip(output.artifact_bytes.as_slice()).expect("zip decode");
    let manifest_bytes = entries.remove(SKILL_MANIFEST_PATH).expect("manifest entry should exist");
    let manifest_toml = String::from_utf8(manifest_bytes).expect("manifest should be utf8");
    assert!(
        manifest_toml.contains(format!("manifest_version = {SKILL_MANIFEST_VERSION}").as_str()),
        "manifest serialization should use the current manifest version"
    );
}

fn sample_sbom() -> Vec<u8> {
    br#"{"bomFormat":"CycloneDX","specVersion":"1.6"}"#.to_vec()
}

fn sample_provenance() -> Vec<u8> {
    br#"{"builder":{"id":"palyra-ci"},"subject":[{"name":"module.wasm"}]}"#.to_vec()
}

fn sample_request() -> SkillArtifactBuildRequest {
    SkillArtifactBuildRequest {
        manifest_toml: sample_manifest(),
        modules: vec![ArtifactFile {
            path: "module.wasm".to_owned(),
            bytes: vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00],
        }],
        assets: vec![ArtifactFile {
            path: "templates/prompt.txt".to_owned(),
            bytes: b"hello".to_vec(),
        }],
        sbom_cyclonedx_json: sample_sbom(),
        provenance_json: sample_provenance(),
        signing_key: [5_u8; 32],
    }
}

fn build_artifact_for_audit(
    manifest_toml: String,
    module_bytes: Vec<u8>,
) -> super::SkillArtifactBuildOutput {
    build_signed_skill_artifact(SkillArtifactBuildRequest {
        manifest_toml,
        modules: vec![ArtifactFile { path: "module.wasm".to_owned(), bytes: module_bytes }],
        assets: vec![ArtifactFile {
            path: "templates/prompt.txt".to_owned(),
            bytes: b"hello".to_vec(),
        }],
        sbom_cyclonedx_json: sample_sbom(),
        provenance_json: sample_provenance(),
        signing_key: [5_u8; 32],
    })
    .expect("artifact should build for audit test")
}

fn unique_temp_trust_store_path() -> std::path::PathBuf {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    std::env::temp_dir()
        .join(format!("palyra-skills-trust-store-{nonce}-{}.json", std::process::id()))
}

#[test]
fn manifest_rejects_dangerous_skill_ids() {
    for invalid_skill_id in
        [".", "..", "acme..echo_http", ".acme", "acme.", "c:temp", "acme:echo_http"]
    {
        let manifest = sample_manifest().replace(
            "skill_id = \"acme.echo_http\"",
            format!("skill_id = \"{invalid_skill_id}\"").as_str(),
        );
        let error = parse_manifest_toml(manifest.as_str()).expect_err("manifest should fail");
        assert!(
            matches!(error, SkillPackagingError::ManifestValidation(_)),
            "expected manifest validation error for '{invalid_skill_id}', got {error:?}"
        );
    }
}

#[test]
fn manifest_rejects_wildcard_without_opt_in() {
    let manifest = sample_manifest().replace("api.example.com", "*");
    let error = parse_manifest_toml(manifest.as_str()).expect_err("manifest should fail");
    assert!(matches!(error, SkillPackagingError::ManifestValidation(_)));
}

#[test]
fn manifest_accepts_wildcard_capability_path_with_opt_in() {
    let manifest = sample_manifest()
            .replace("read_roots = [\"skills/data\"]", "read_roots = [\"skills/*\"]")
            .replace(
                "[capabilities]\nhttp_egress_allowlist = [\"api.example.com\"]",
                "[capabilities]\nwildcard_opt_in = { filesystem = true }\nhttp_egress_allowlist = [\"api.example.com\"]",
            );
    parse_manifest_toml(manifest.as_str())
        .expect("filesystem wildcard path with opt-in should be accepted");
}

#[test]
fn manifest_rejects_malformed_wildcard_capability_paths() {
    for invalid_path in ["../*", "*//foo", "skills//*/data"] {
        let manifest = sample_manifest()
                .replace(
                    "read_roots = [\"skills/data\"]",
                    format!("read_roots = [\"{invalid_path}\"]").as_str(),
                )
                .replace(
                    "[capabilities]\nhttp_egress_allowlist = [\"api.example.com\"]",
                    "[capabilities]\nwildcard_opt_in = { filesystem = true }\nhttp_egress_allowlist = [\"api.example.com\"]",
                );
        let error = parse_manifest_toml(manifest.as_str()).expect_err("manifest should fail");
        assert!(
            matches!(error, SkillPackagingError::InvalidArtifactPath(_)),
            "expected InvalidArtifactPath for '{invalid_path}', got {error:?}"
        );
    }
}

#[test]
fn manifest_rejects_secret_scopes_with_empty_suffix() {
    for invalid_scope in ["principal:", "channel:", "skill:"] {
        let manifest = sample_manifest().replace(
            "scope = \"skill:acme.echo_http\"",
            format!("scope = \"{invalid_scope}\"").as_str(),
        );
        let error = parse_manifest_toml(manifest.as_str()).expect_err("manifest should fail");
        assert!(
            matches!(error, SkillPackagingError::ManifestValidation(_)),
            "expected manifest validation error for '{invalid_scope}', got {error:?}"
        );
    }
}

#[test]
fn manifest_accepts_secret_scopes_with_valid_suffixes() {
    for valid_scope in ["principal:admin:ops", "channel:discord:acct_1", "skill:acme.echo_http"] {
        let manifest = sample_manifest().replace(
            "scope = \"skill:acme.echo_http\"",
            format!("scope = \"{valid_scope}\"").as_str(),
        );
        parse_manifest_toml(manifest.as_str())
            .unwrap_or_else(|error| panic!("scope '{valid_scope}' should be valid: {error:?}"));
    }
}

#[test]
fn manifest_accepts_builder_metadata_with_required_checklist() {
    let manifest = format!(
        "{}\n\n[builder]\nexperimental = true\nsource_kind = \"procedure\"\nsource_ref = \"candidate-proc-1\"\nrollout_flag = \"PALYRA_EXPERIMENTAL_DYNAMIC_TOOL_BUILDER\"\nreview_status = \"quarantined\"\n\n[builder.checklist]\ncapability_declaration_path = \"builder-capabilities.json\"\nprovenance_path = \"provenance.json\"\ntest_harness_path = \"tests/smoke.test.json\"\nreview_notes = \"Needs signing review\"\n",
        sample_manifest()
    );
    let parsed = parse_manifest_toml(manifest.as_str()).expect("builder metadata should validate");
    assert!(parsed.builder.is_some(), "builder metadata should survive parsing");
    assert!(
        builder_manifest_requires_review(&parsed),
        "experimental builder output should require explicit review"
    );
}

#[test]
fn manifest_rejects_operator_metadata_on_legacy_manifest_version() {
    let manifest = sample_manifest().replace("manifest_version = 2", "manifest_version = 1");
    let error = parse_manifest_toml(manifest.as_str())
        .expect_err("legacy manifest with operator block must fail");
    assert!(
        matches!(error, SkillPackagingError::ManifestValidation(ref message) if message.contains("operator metadata requires manifest_version")),
        "unexpected validation error: {error:?}"
    );
}

#[test]
fn manifest_rejects_operator_plugin_default_tool_that_is_not_declared() {
    let manifest = sample_manifest()
        .replace("default_tool_id = \"acme.echo\"", "default_tool_id = \"acme.missing\"");
    let error = parse_manifest_toml(manifest.as_str())
        .expect_err("unknown operator plugin default_tool_id must fail");
    assert!(
        matches!(error, SkillPackagingError::ManifestValidation(ref message) if message.contains("default_tool_id")),
        "unexpected validation error: {error:?}"
    );
}

#[test]
fn manifest_accepts_typed_plugin_contract_declarations() {
    let manifest = format!(
        "{}\n\n[[operator.plugin.contracts]]\nkind = \"memory_provider\"\nversion = 1\n\n[[operator.plugin.contracts]]\nkind = \"context_engine\"\nversion = 1\n\n[[operator.plugin.contracts]]\nkind = \"routing_strategy\"\nversion = 1\n\n[[operator.plugin.contracts]]\nkind = \"run_lifecycle_hook\"\nversion = 1\n\n[[operator.plugin.contracts]]\nkind = \"delivery_adapter\"\nversion = 1\n\n[[operator.plugin.contracts]]\nkind = \"connector_adapter\"\nversion = 1\n",
        sample_manifest()
    );
    let parsed =
        parse_manifest_toml(manifest.as_str()).expect("typed plugin contracts should validate");
    assert_eq!(parsed.operator.plugin.contracts.len(), 6);
}

#[test]
fn manifest_rejects_duplicate_typed_plugin_contract_kinds() {
    let manifest = format!(
        "{}\n\n[[operator.plugin.contracts]]\nkind = \"memory_provider\"\nversion = 1\n\n[[operator.plugin.contracts]]\nkind = \"memory_provider\"\nversion = 1\n",
        sample_manifest()
    );
    let error = parse_manifest_toml(manifest.as_str())
        .expect_err("duplicate typed plugin contract kind must fail");
    assert!(
        matches!(error, SkillPackagingError::ManifestValidation(ref message) if message.contains("duplicate kind")),
        "unexpected validation error: {error:?}"
    );
}

#[test]
fn manifest_rejects_zero_version_typed_plugin_contracts() {
    let manifest = format!(
        "{}\n\n[[operator.plugin.contracts]]\nkind = \"memory_provider\"\nversion = 0\n",
        sample_manifest()
    );
    let error = parse_manifest_toml(manifest.as_str())
        .expect_err("zero-version typed plugin contract must fail");
    assert!(
        matches!(error, SkillPackagingError::ManifestValidation(ref message) if message.contains("non-zero version")),
        "unexpected validation error: {error:?}"
    );
}

#[test]
fn manifest_rejects_unsupported_typed_plugin_contract_versions() {
    let manifest = format!(
        "{}\n\n[[operator.plugin.contracts]]\nkind = \"run_lifecycle_hook\"\nversion = 99\n",
        sample_manifest()
    );
    let error = parse_manifest_toml(manifest.as_str())
        .expect_err("unsupported typed plugin contract version must fail");
    assert!(
        matches!(error, SkillPackagingError::ManifestValidation(ref message) if message.contains("not supported by this SDK ABI")),
        "unexpected validation error: {error:?}"
    );
}

#[test]
fn manifest_rejects_runtime_maximum_below_minimum() {
    let manifest = sample_manifest().replace(
        "min_palyra_version = \"0.1.0\"",
        "min_palyra_version = \"2.0.0\"\nmax_palyra_version = \"1.9.0\"",
    );
    let error =
        parse_manifest_toml(manifest.as_str()).expect_err("invalid runtime range must fail");
    assert!(
        matches!(error, SkillPackagingError::ManifestValidation(ref message) if message.contains("max_palyra_version")),
        "unexpected validation error: {error:?}"
    );
}

#[test]
fn manifest_rejects_builder_metadata_without_test_harness() {
    let manifest = format!(
        "{}\n\n[builder]\nexperimental = true\nsource_kind = \"prompt\"\nsource_ref = \"prompt:generate release helper\"\nrollout_flag = \"PALYRA_EXPERIMENTAL_DYNAMIC_TOOL_BUILDER\"\n\n[builder.checklist]\ncapability_declaration_path = \"builder-capabilities.json\"\nprovenance_path = \"provenance.json\"\ntest_harness_path = \"\"\n",
        sample_manifest()
    );
    let error = parse_manifest_toml(manifest.as_str()).expect_err("missing harness must fail");
    assert!(
        matches!(error, SkillPackagingError::ManifestValidation(ref message) if message.contains("builder.checklist.test_harness_path")),
        "expected builder checklist validation failure, got {error:?}"
    );
}

#[test]
fn manifest_rejects_non_namespaced_tool_ids() {
    let manifest = sample_manifest().replace("id = \"acme.echo\"", "id = \"echo\"");
    let error = parse_manifest_toml(manifest.as_str()).expect_err("manifest should fail");
    assert!(matches!(error, SkillPackagingError::ManifestValidation(_)));
}

#[test]
fn build_verify_and_tofu_flow() {
    let output = build_signed_skill_artifact(sample_request()).expect("artifact should build");
    let mut trust_store = SkillTrustStore::default();
    let first = verify_skill_artifact(output.artifact_bytes.as_slice(), &mut trust_store, true)
        .expect("verify with TOFU should pass");
    assert_eq!(first.trust_decision, TrustDecision::TofuNewlyPinned);
    let second = verify_skill_artifact(output.artifact_bytes.as_slice(), &mut trust_store, false)
        .expect("verify with pinned TOFU should pass");
    assert_eq!(second.trust_decision, TrustDecision::TofuPinned);
}

#[test]
fn verify_fails_if_sbom_missing() {
    let output = build_signed_skill_artifact(sample_request()).expect("artifact should build");
    let mut entries = super::decode_zip(output.artifact_bytes.as_slice()).expect("zip decode");
    entries.remove(SBOM_PATH);
    let rebuilt = super::encode_zip(entries.iter()).expect("zip encode");
    let mut trust_store = SkillTrustStore::default();
    let error = verify_skill_artifact(rebuilt.as_slice(), &mut trust_store, true)
        .expect_err("verify should fail");
    assert!(matches!(error, SkillPackagingError::MissingArtifactEntry(_)));
}

#[test]
fn package_guardrails_reject_artifact_without_manifest() {
    let output = build_signed_skill_artifact(sample_request()).expect("artifact should build");
    let mut entries = super::decode_zip(output.artifact_bytes.as_slice()).expect("zip decode");
    entries.remove(SKILL_MANIFEST_PATH);
    let rebuilt = super::encode_zip(entries.iter()).expect("zip encode");

    let error = inspect_skill_artifact(rebuilt.as_slice()).expect_err("inspect should fail");
    assert!(
        matches!(error, SkillPackagingError::MissingArtifactEntry(ref path) if path == SKILL_MANIFEST_PATH),
        "missing manifest should be a stable package guardrail error: {error:?}"
    );
}

#[test]
fn verify_detects_tamper() {
    let output = build_signed_skill_artifact(sample_request()).expect("artifact should build");
    let mut entries = super::decode_zip(output.artifact_bytes.as_slice()).expect("zip decode");
    let module = entries.get_mut("modules/module.wasm").expect("module entry should exist");
    module.push(0xFF);
    let rebuilt = super::encode_zip(entries.iter()).expect("zip encode");
    let mut trust_store = SkillTrustStore::default();
    let error = verify_skill_artifact(rebuilt.as_slice(), &mut trust_store, true)
        .expect_err("verify should fail");
    assert!(matches!(
        error,
        SkillPackagingError::PayloadHashMismatch | SkillPackagingError::SignatureVerificationFailed
    ));
}

#[test]
fn inspect_returns_verified_entries_for_installer() {
    let output = build_signed_skill_artifact(sample_request()).expect("artifact should build");
    let inspected =
        inspect_skill_artifact(output.artifact_bytes.as_slice()).expect("artifact should inspect");
    assert_eq!(inspected.manifest.skill_id, "acme.echo_http");
    assert_eq!(inspected.payload_sha256, output.payload_sha256);
    assert!(
        inspected.entries.contains_key(SIGNATURE_PATH),
        "signature entry should be available for extraction"
    );
    assert!(
        inspected.entries.contains_key("modules/module.wasm"),
        "module entry should be available for extraction"
    );
    assert!(
        inspected.manifest_warnings.is_empty(),
        "sample v2 manifest should not produce operator metadata warnings"
    );
}

#[test]
fn inspect_reports_warning_for_legacy_manifest_without_operator_metadata() {
    let artifact = build_signed_skill_artifact(SkillArtifactBuildRequest {
        manifest_toml: sample_manifest()
            .replace("manifest_version = 2", "manifest_version = 1")
            .split("\n\n[operator]")
            .next()
            .expect("operator section should exist")
            .to_owned(),
        ..sample_request()
    })
    .expect("legacy artifact should build");
    let inspection = inspect_skill_artifact(artifact.artifact_bytes.as_slice())
        .expect("artifact should inspect");
    assert!(
        inspection
            .manifest_warnings
            .iter()
            .map(|warning| warning.code.as_str())
            .collect::<Vec<_>>()
            .contains(&"legacy_manifest_version"),
        "legacy manifest should surface a compatibility warning"
    );
}

#[test]
fn verify_rejects_artifact_with_excessive_total_uncompressed_size() {
    let mut entries = std::collections::BTreeMap::new();
    let chunk = vec![0_u8; 1024 * 1024];
    for index in 0..65 {
        entries.insert(format!("assets/chunk-{index}.bin"), chunk.clone());
    }
    let encoded = super::encode_zip(entries.iter()).expect("zip encode");
    assert!(
        encoded.len() < MAX_ARTIFACT_BYTES,
        "test artifact should stay under compressed artifact limit"
    );
    let error =
        super::decode_zip(encoded.as_slice()).expect_err("decode should enforce total budget");
    match error {
        SkillPackagingError::ArtifactTooLarge { limit, .. } => {
            assert_eq!(limit, MAX_ARTIFACT_BYTES);
        }
        other => panic!("expected ArtifactTooLarge, got {other:?}"),
    }
}

#[test]
fn build_rejects_artifact_with_too_many_entries() {
    let mut request = sample_request();
    request.assets.clear();
    request.modules = (0..MAX_ENTRIES)
        .map(|index| ArtifactFile {
            path: format!("module-{index}.wasm"),
            bytes: vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00],
        })
        .collect();

    let error = build_signed_skill_artifact(request).expect_err("build should fail");
    match error {
        SkillPackagingError::ArtifactTooManyEntries { limit, .. } => {
            assert_eq!(limit, MAX_ENTRIES);
        }
        other => panic!("expected ArtifactTooManyEntries, got {other:?}"),
    }
}

#[test]
fn build_rejects_artifact_with_excessive_total_payload_size() {
    let mut request = sample_request();
    request.assets.clear();
    let module = vec![0_u8; 14 * 1024 * 1024];
    request.modules = (0..5)
        .map(|index| ArtifactFile { path: format!("large-{index}.wasm"), bytes: module.clone() })
        .collect();

    let error = build_signed_skill_artifact(request).expect_err("build should fail");
    match error {
        SkillPackagingError::ArtifactTooLarge { limit, .. } => {
            assert_eq!(limit, MAX_ARTIFACT_BYTES);
        }
        other => panic!("expected ArtifactTooLarge, got {other:?}"),
    }
}

#[test]
fn trust_store_load_rejects_invalid_publisher() {
    let path = unique_temp_trust_store_path();
    let payload = serde_json::json!({
        "trusted_publishers": { "Acme": [hex::encode([7_u8; 32])] },
        "tofu_publishers": {}
    });
    std::fs::write(&path, serde_json::to_vec(&payload).expect("json payload"))
        .expect("trust store should be written");

    let error = SkillTrustStore::load(path.as_path()).expect_err("load should fail");
    assert!(
        error.to_string().contains("invalid trust-store publisher"),
        "error should explain trust-store publisher validation: {error}"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn trust_store_load_rejects_invalid_key() {
    let path = unique_temp_trust_store_path();
    let payload = serde_json::json!({
        "trusted_publishers": { "acme": ["not-hex"] },
        "tofu_publishers": {}
    });
    std::fs::write(&path, serde_json::to_vec(&payload).expect("json payload"))
        .expect("trust store should be written");

    let error = SkillTrustStore::load(path.as_path()).expect_err("load should fail");
    assert!(
        error.to_string().contains("invalid trusted key for publisher"),
        "error should explain trust-store key validation: {error}"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn trust_store_load_accepts_empty_uninitialized_file() {
    let path = unique_temp_trust_store_path();
    std::fs::write(&path, b"  \n\t").expect("empty trust store fixture should be written");

    let store = SkillTrustStore::load(path.as_path()).expect("empty trust store should load");

    assert_eq!(store, SkillTrustStore::default());
    let _ = std::fs::remove_file(path);
}

#[test]
fn trust_store_save_replaces_existing_file_without_temp_artifact() {
    let path = unique_temp_trust_store_path();
    let old_payload = serde_json::json!({
        "trusted_publishers": { "old": [hex::encode([1_u8; 32])] },
        "tofu_publishers": {}
    });
    std::fs::write(&path, serde_json::to_vec(&old_payload).expect("json payload"))
        .expect("old trust store should be written");

    let trusted_key = hex::encode([7_u8; 32]);
    let mut store = SkillTrustStore::default();
    store.add_trusted_key("acme", trusted_key.as_str()).expect("trusted key should be accepted");

    store.save(path.as_path()).expect("trust store should save atomically");
    let loaded = SkillTrustStore::load(path.as_path()).expect("saved trust store should load");

    assert_eq!(loaded.trusted_publishers.get("acme"), Some(&vec![trusted_key]));
    assert!(!loaded.trusted_publishers.contains_key("old"));
    let file_name = path.file_name().expect("path has file name").to_string_lossy();
    let temp_prefix = format!(".{file_name}.");
    let leftovers = std::fs::read_dir(path.parent().expect("path has parent"))
        .expect("temp directory should be readable")
        .filter_map(Result::ok)
        .filter(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            name.starts_with(temp_prefix.as_str()) && name.ends_with(".tmp")
        })
        .collect::<Vec<_>>();
    assert!(leftovers.is_empty(), "temporary trust store files should be cleaned up");
    let _ = std::fs::remove_file(path);
}

#[test]
fn mapping_to_runtime_grants_and_policy_requests() {
    let manifest = parse_manifest_toml(sample_manifest().as_str()).expect("manifest");
    let grants = capability_grants_from_manifest(&manifest);
    assert_eq!(grants.http_hosts, vec!["api.example.com".to_owned()]);
    assert_eq!(grants.storage_prefixes, vec!["skills/cache".to_owned()]);
    let requests = policy_requests_from_manifest(&manifest);
    assert!(
        requests.iter().any(|request| request.action == "tool.execute"),
        "tool policy requests should be generated"
    );
}

#[test]
fn extension_registry_record_projects_skill_manifest_and_grants() {
    let output = build_signed_skill_artifact(sample_request()).expect("artifact should build");
    let inspection =
        inspect_skill_artifact(output.artifact_bytes.as_slice()).expect("artifact should inspect");
    let record = extension_record_from_skill_artifact(
        &inspection,
        ExtensionPackageSource::local_artifact("dist/acme.echo_http.palyra-skill"),
        ExtensionPackageStatus::Verified,
        1_700_000_000_000,
        1_700_000_000_000,
    );

    assert_eq!(record.package_id, skill_extension_package_id("acme.echo_http", "1.0.0"));
    assert!(record.compatibility.compatible);
    assert!(record.grants.iter().any(|grant| {
        grant.class == ExtensionCapabilityClass::Network && grant.value == "api.example.com"
    }));
    assert!(record.grants.iter().any(|grant| {
        grant.class == ExtensionCapabilityClass::Secret
            && grant.scope.as_deref() == Some("skill:acme.echo_http")
            && grant.value == "api_token"
    }));
}

#[test]
fn extension_doctor_reports_missing_capability_grants_without_installing() {
    let output = build_signed_skill_artifact(sample_request()).expect("artifact should build");
    let mut trust_store = SkillTrustStore::default();
    let report = extension_doctor_for_skill_artifact(
        output.artifact_bytes.as_slice(),
        ExtensionPackageSource::local_artifact("dist/acme.echo_http.palyra-skill"),
        &mut trust_store,
        true,
        &[],
        &SkillSecurityAuditPolicy::default(),
    )
    .expect("doctor should inspect artifact");
    assert_eq!(report.status, "ready");

    let manifest = parse_manifest_toml(sample_manifest().as_str()).expect("manifest");
    let granted = extension_capability_grants_from_skill_manifest(&manifest)
        .into_iter()
        .filter(|grant| grant.class != ExtensionCapabilityClass::Secret)
        .collect::<Vec<_>>();
    let report = extension_doctor_for_skill_artifact(
        output.artifact_bytes.as_slice(),
        ExtensionPackageSource::local_artifact("dist/acme.echo_http.palyra-skill"),
        &mut trust_store,
        true,
        granted.as_slice(),
        &SkillSecurityAuditPolicy::default(),
    )
    .expect("doctor should inspect artifact with explicit grants");

    assert_eq!(report.status, "blocked");
    assert!(report.reason_codes.iter().any(|code| code == "missing_capability_grant"));
    assert!(report
        .capability_diff
        .missing
        .iter()
        .any(|grant| grant.class == ExtensionCapabilityClass::Secret));
}

#[test]
fn extension_lifecycle_blocks_quarantine_enable_without_review() {
    let output = build_signed_skill_artifact(sample_request()).expect("artifact should build");
    let inspection =
        inspect_skill_artifact(output.artifact_bytes.as_slice()).expect("artifact should inspect");
    let record = extension_record_from_skill_artifact(
        &inspection,
        ExtensionPackageSource::local_artifact("dist/acme.echo_http.palyra-skill"),
        ExtensionPackageStatus::Quarantined,
        1,
        1,
    );
    let mut registry = InMemoryExtensionPackageRegistry::default();
    registry.upsert_package(record);

    let error = registry
        .transition_package(ExtensionLifecycleTransitionRequest {
            package_id: skill_extension_package_id("acme.echo_http", "1.0.0"),
            target_status: ExtensionPackageStatus::Enabled,
            actor_principal: "user:ops".to_owned(),
            reason: "operator reviewed".to_owned(),
            approved_by: None,
            requested_at_unix_ms: 2,
        })
        .expect_err("quarantine enable must require review approval");
    assert!(error.to_string().contains("approved_by"));
}

#[test]
fn extension_lifecycle_rejects_spoofed_quarantine_enable_approval() {
    let output = build_signed_skill_artifact(sample_request()).expect("artifact should build");
    let inspection =
        inspect_skill_artifact(output.artifact_bytes.as_slice()).expect("artifact should inspect");
    let record = extension_record_from_skill_artifact(
        &inspection,
        ExtensionPackageSource::local_artifact("dist/acme.echo_http.palyra-skill"),
        ExtensionPackageStatus::Quarantined,
        1,
        1,
    );
    let package_id = skill_extension_package_id("acme.echo_http", "1.0.0");

    for approved_by in ["", "   ", "user:ops", " user:ops "] {
        let mut registry = InMemoryExtensionPackageRegistry::default();
        registry.upsert_package(record.clone());
        let error = registry
            .transition_package(ExtensionLifecycleTransitionRequest {
                package_id: package_id.clone(),
                target_status: ExtensionPackageStatus::Enabled,
                actor_principal: "user:ops".to_owned(),
                reason: "operator reviewed".to_owned(),
                approved_by: Some(approved_by.to_owned()),
                requested_at_unix_ms: 2,
            })
            .expect_err("quarantine enable must reject empty or self approval");

        let message = error.to_string();
        assert!(
            message.contains("approved_by") || message.contains("distinct reviewer"),
            "unexpected lifecycle error: {message}"
        );
    }

    let mut registry = InMemoryExtensionPackageRegistry::default();
    registry.upsert_package(record);
    let updated = registry
        .transition_package(ExtensionLifecycleTransitionRequest {
            package_id,
            target_status: ExtensionPackageStatus::Enabled,
            actor_principal: "user:ops".to_owned(),
            reason: "operator reviewed".to_owned(),
            approved_by: Some("admin:reviewer".to_owned()),
            requested_at_unix_ms: 3,
        })
        .expect("distinct reviewer approval should enable quarantined extension");

    assert_eq!(updated.status, ExtensionPackageStatus::Enabled);
}

#[test]
fn extension_contract_fixture_enforces_host_version_range() {
    let fixture = ExtensionContractFixture {
        fixture_id: "future-host-range".to_owned(),
        package_kind: super::ExtensionPackageKind::Skill,
        manifest_version: SKILL_MANIFEST_VERSION,
        required_protocol_major: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        min_host_version: "999.0.0".to_owned(),
        max_host_version: None,
        expected_reason_codes: Vec::new(),
    };
    let outcome = evaluate_extension_contract_fixture(&fixture);

    assert!(!outcome.passed, "future host range must block contract compatibility");
    assert!(
        outcome.observed_reason_codes.iter().any(|code| code == "host_below_min_palyra_version"),
        "contract outcome should include host version range failure"
    );
}

#[test]
fn skill_eval_gate_blocks_failed_self_improvement_rollout() {
    let output = build_signed_skill_artifact(sample_request()).expect("artifact should build");
    let inspection =
        inspect_skill_artifact(output.artifact_bytes.as_slice()).expect("artifact should inspect");
    let record = extension_record_from_skill_artifact(
        &inspection,
        ExtensionPackageSource::local_artifact("dist/acme.echo_http.palyra-skill"),
        ExtensionPackageStatus::Verified,
        1,
        1,
    );
    let missing_eval_gate = extension_enable_gate(&record, &[]);
    assert!(!missing_eval_gate.allowed);
    assert!(missing_eval_gate.reasons.iter().any(|reason| reason == "eval_missing"));

    let fixture = SkillEvalFixture {
        fixture_id: "echo-output".to_owned(),
        label: "echo output".to_owned(),
        expected_outputs: std::collections::BTreeMap::from([(
            "result".to_owned(),
            "ok".to_owned(),
        )]),
        required_capabilities: vec![ExtensionCapabilityGrant::new(
            ExtensionCapabilityClass::Network,
            "api.example.com",
            None,
        )],
        regression_labels: vec!["self_improvement".to_owned()],
    };
    let actual_outputs =
        std::collections::BTreeMap::from([("result".to_owned(), "wrong".to_owned())]);
    let outcome =
        evaluate_skill_fixture(&fixture, &actual_outputs, record.grants.as_slice(), vec![]);
    assert!(!outcome.passed);

    let gate = extension_enable_gate(&record, std::slice::from_ref(&outcome));
    assert!(!gate.allowed);
    assert!(gate.reasons.iter().any(|reason| reason == "eval_failed:echo-output"));

    let rollback = plan_self_improvement_rollback(&record, &[outcome]);
    assert_eq!(rollback.target_status, ExtensionPackageStatus::RolledBack);
}

#[test]
fn self_improvement_candidate_extracts_review_metadata_and_grants() {
    let payload = serde_json::json!({
        "source_refs": ["run:01", "run:01", "tool:palyra.fs.apply_patch"],
        "rationale": "Observed repeated successful scaffold edits.",
        "risk": "review",
        "expected_capabilities": [
            { "class": "network", "value": "api.example.com" },
            { "class": "secret", "scope": "skill:acme.echo_http", "value": "api_token" }
        ],
        "tests": ["smoke:echo", "smoke:echo", "regression:capability-grants"],
        "sensitivity": "operator_review",
    });
    let candidate = extract_self_improvement_candidate("candidate-1", &payload)
        .expect("self-improvement candidate metadata should parse");

    assert_eq!(candidate.source_refs, vec!["run:01", "tool:palyra.fs.apply_patch"]);
    assert_eq!(candidate.tests, vec!["regression:capability-grants", "smoke:echo"]);
    assert!(candidate.expected_capabilities.iter().any(|grant| {
        grant.class == ExtensionCapabilityClass::Secret
            && grant.scope.as_deref() == Some("skill:acme.echo_http")
            && grant.value == "api_token"
    }));
}

#[test]
fn scaffold_plan_and_eval_run_track_rollout_gate_inputs() {
    let package_id = skill_extension_package_id("acme.echo_http", "1.0.0");
    let plan = skill_scaffold_artifact_plan(
        package_id.clone(),
        "skill.toml",
        "builder-capabilities.json",
        "provenance.json",
        "tests/smoke.test.json",
        "sha256:abc",
    );
    assert_eq!(plan.quarantine_status, ExtensionPackageStatus::Quarantined);
    assert_eq!(plan.artifact_status, "prepared_for_signing");
    assert!(plan.audit_events.iter().any(|event| event == "skill.eval_required"));

    let outcome = SkillEvalOutcome {
        fixture_id: "smoke".to_owned(),
        passed: true,
        failed_checks: Vec::new(),
        flaky_signal: false,
        provider_usage: std::collections::BTreeMap::from([("input_tokens".to_owned(), 12)]),
        artifact_refs: vec!["artifact://skill/acme.echo_http/1.0.0".to_owned()],
    };
    let record = skill_eval_run_record(
        "eval-1",
        package_id,
        vec![outcome],
        vec!["smoke".to_owned(), "smoke".to_owned()],
        1_700_000_000_000,
    );
    assert_eq!(record.status, "passed");
    assert_eq!(record.regression_labels, vec!["smoke"]);
    assert_eq!(record.provider_usage.get("input_tokens"), Some(&12));
}

#[test]
fn audit_reports_passing_result_for_baseline_skill() {
    let artifact = build_artifact_for_audit(
        sample_manifest(),
        vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00],
    );
    let mut trust_store = SkillTrustStore::default();
    let report = audit_skill_artifact_security(
        artifact.artifact_bytes.as_slice(),
        &mut trust_store,
        true,
        &SkillSecurityAuditPolicy::default(),
    )
    .expect("audit should succeed");

    assert!(report.passed, "baseline skill should pass security audit");
    assert!(
        !report.should_quarantine,
        "passing baseline skill should not be marked for quarantine"
    );
    assert!(report.manifest_warnings.is_empty(), "baseline v2 manifest should not emit warnings");
}

#[test]
fn audit_quarantines_when_module_size_exceeds_policy_limit() {
    let artifact = build_artifact_for_audit(
        sample_manifest(),
        vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00],
    );
    let mut trust_store = SkillTrustStore::default();
    let policy = SkillSecurityAuditPolicy {
        max_module_bytes: 4,
        max_exported_functions: 16,
        ..SkillSecurityAuditPolicy::default()
    };
    let report = audit_skill_artifact_security(
        artifact.artifact_bytes.as_slice(),
        &mut trust_store,
        true,
        &policy,
    )
    .expect("audit should return report");

    assert!(report.should_quarantine, "oversized module should trigger quarantine recommendation");
    assert!(
        report.checks.iter().any(|check| {
            check.check_id == "wasm_module_size_limit"
                && check.status == SkillAuditCheckStatus::Fail
        }),
        "audit should include failing wasm module size check"
    );
}

#[test]
fn audit_quarantines_wasi_filesystem_import_without_manifest_filesystem_capability() {
    let manifest = sample_manifest()
        .replace("read_roots = [\"skills/data\"]", "read_roots = []")
        .replace("write_roots = [\"skills/cache\"]", "write_roots = []");
    let module_wat = r#"
            (module
                (import "wasi:filesystem" "path_open" (func $path_open))
                (func (export "run") (result i32) i32.const 0)
            )
        "#;
    let artifact = build_artifact_for_audit(manifest, module_wat.as_bytes().to_vec());
    let mut trust_store = SkillTrustStore::default();
    let report = audit_skill_artifact_security(
        artifact.artifact_bytes.as_slice(),
        &mut trust_store,
        true,
        &SkillSecurityAuditPolicy::default(),
    )
    .expect("audit should return report");

    assert!(report.should_quarantine, "filesystem import mismatch must quarantine");
    assert!(
        report.checks.iter().any(|check| {
            check.check_id == "wasm_wasi_filesystem_imports"
                && check.status == SkillAuditCheckStatus::Fail
        }),
        "audit should record wasi:filesystem policy violation"
    );
}

#[test]
fn audit_quarantines_forbidden_device_capability_by_default() {
    let manifest =
        sample_manifest().replace("device_capabilities = []", "device_capabilities = [\"camera\"]");
    let artifact =
        build_artifact_for_audit(manifest, vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]);
    let mut trust_store = SkillTrustStore::default();
    let report = audit_skill_artifact_security(
        artifact.artifact_bytes.as_slice(),
        &mut trust_store,
        true,
        &SkillSecurityAuditPolicy::default(),
    )
    .expect("audit should return report");

    assert!(report.should_quarantine, "device capability should require quarantine");
    assert!(
        report.checks.iter().any(|check| {
            check.check_id == "forbidden_device_capabilities"
                && check.status == SkillAuditCheckStatus::Fail
        }),
        "audit should include forbidden device capability failure"
    );
}

#[test]
fn audit_quarantines_module_export_count_over_limit() {
    let module_wat = r#"
            (module
                (func (export "run") (result i32) i32.const 1)
                (func (export "a") (result i32) i32.const 2)
                (func (export "b") (result i32) i32.const 3)
            )
        "#;
    let artifact = build_artifact_for_audit(sample_manifest(), module_wat.as_bytes().to_vec());
    let mut trust_store = SkillTrustStore::default();
    let policy = SkillSecurityAuditPolicy {
        max_module_bytes: 64 * 1024,
        max_exported_functions: 1,
        ..SkillSecurityAuditPolicy::default()
    };
    let report = audit_skill_artifact_security(
        artifact.artifact_bytes.as_slice(),
        &mut trust_store,
        true,
        &policy,
    )
    .expect("audit should return report");

    assert!(report.should_quarantine, "export count over policy should quarantine");
    assert!(
        report.checks.iter().any(|check| {
            check.check_id == "wasm_exported_function_limit"
                && check.status == SkillAuditCheckStatus::Fail
        }),
        "audit should include exported function limit failure"
    );
}

#[test]
fn audit_surfaces_manifest_warning_checks() {
    let legacy_artifact = build_artifact_for_audit(
        sample_manifest()
            .replace("manifest_version = 2", "manifest_version = 1")
            .split("\n\n[operator]")
            .next()
            .expect("operator section should exist")
            .to_owned(),
        vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00],
    );
    let mut trust_store = SkillTrustStore::default();
    let report = audit_skill_artifact_security(
        legacy_artifact.artifact_bytes.as_slice(),
        &mut trust_store,
        true,
        &SkillSecurityAuditPolicy::default(),
    )
    .expect("audit should succeed");

    assert!(
        report
            .checks
            .iter()
            .any(|check| check.check_id == "manifest_warning:legacy_manifest_version"),
        "legacy manifest warning should be mirrored into the audit report"
    );
}

#[test]
fn signing_key_parser_accepts_raw_hex_and_base64() {
    let key = [13_u8; 32];
    assert_eq!(parse_ed25519_signing_key(key.as_slice()).expect("raw key"), key);
    let hex = hex::encode(key);
    assert_eq!(parse_ed25519_signing_key(hex.as_bytes()).expect("hex key"), key);
    let base64 = base64::engine::general_purpose::STANDARD.encode(key);
    assert_eq!(parse_ed25519_signing_key(base64.as_bytes()).expect("base64 key"), key);
}
