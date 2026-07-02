//! Public skill manifest contract snapshot used by CI and daemon diagnostics.

use palyra_common::CANONICAL_PROTOCOL_MAJOR;
use palyra_plugins_sdk::{SDK_ABI_MAX_MAJOR, SDK_ABI_MIN_MAJOR};
use serde_json::{json, Value};

use crate::constants::{
    LEGACY_SKILL_MANIFEST_VERSION, PROVENANCE_PATH, SBOM_PATH, SIGNATURE_PATH,
    SKILL_ARTIFACT_EXTENSION, SKILL_MANIFEST_PATH, SKILL_MANIFEST_VERSION,
};

/// Schema version for the public skill manifest contract snapshot.
pub const SKILL_MANIFEST_CONTRACT_SNAPSHOT_SCHEMA_VERSION: u32 = 1;
/// Version identifier for the current skill manifest contract snapshot.
pub const SKILL_MANIFEST_CONTRACT_SNAPSHOT_VERSION: &str = "skill-manifest-contracts.v1";

/// Builds the public skill manifest contract snapshot for runtime contract gates.
#[must_use]
pub fn skill_manifest_contract_snapshot() -> Value {
    json!({
        "schema_version": SKILL_MANIFEST_CONTRACT_SNAPSHOT_SCHEMA_VERSION,
        "snapshot_version": SKILL_MANIFEST_CONTRACT_SNAPSHOT_VERSION,
        "changelog_note": "Initial skill manifest contract snapshot; breaking manifest changes require a migration note.",
        "compatibility_policy": {
            "snapshot_version": "skill-manifest-contracts.compatibility_policy.v1",
            "changelog_note": "Manifest breaking changes require a contract version bump and migration note.",
            "breaking_change_requires_version_bump": true,
            "breaking_change_requires_migration_note": true,
            "deprecated_field_names_must_remain_aliases": true,
            "unknown_fields_fail_closed": true,
        },
        "manifest_versions": {
            "snapshot_version": "skill-manifest-contracts.versions.v1",
            "changelog_note": "Manifest v2 is current; v1 remains accepted without operator metadata.",
            "current": SKILL_MANIFEST_VERSION,
            "accepted_legacy": [LEGACY_SKILL_MANIFEST_VERSION],
            "canonical_protocol_major": CANONICAL_PROTOCOL_MAJOR,
        },
        "artifact_contract": {
            "snapshot_version": "skill-manifest-contracts.artifact.v1",
            "changelog_note": "Packaged skill artifacts expose fixed entry names and extension.",
            "extension": SKILL_ARTIFACT_EXTENSION,
            "required_entries": [
                SKILL_MANIFEST_PATH,
                SBOM_PATH,
                PROVENANCE_PATH,
                SIGNATURE_PATH,
            ],
        },
        "manifest_shape": {
            "snapshot_version": "skill-manifest-contracts.shape.v1",
            "changelog_note": "Top-level manifest fields and aliases are public wire contract.",
            "required_top_level_fields": [
                "manifest_version",
                "skill_id",
                "name",
                "version",
                "publisher",
                "entrypoints",
                "compat",
            ],
            "optional_top_level_fields": [
                "capabilities",
                "integrity",
                "builder",
                "operator",
            ],
            "compat_field_aliases": [
                {
                    "current": "required_protocol_major",
                    "deprecated_aliases": ["min_protocol_major"],
                },
                {
                    "current": "min_palyra_version",
                    "deprecated_aliases": ["min_runtime_version"],
                },
            ],
            "deny_unknown_fields": true,
        },
        "validation_contract": {
            "snapshot_version": "skill-manifest-contracts.validation.v1",
            "changelog_note": "Validation rejects broad grants unless the matching wildcard opt-in is explicit.",
            "tool_entries_required": true,
            "tool_ids_must_be_publisher_namespaced": true,
            "semver_fields": ["version", "compat.min_palyra_version", "compat.max_palyra_version"],
            "wildcard_opt_in_fields": [
                "filesystem",
                "http_egress",
                "secrets",
                "device",
                "node",
            ],
            "minimum_quota_memory_bytes": 65536_u64,
        },
        "plugin_manifest_extension": {
            "snapshot_version": "skill-manifest-contracts.plugin_extension.v1",
            "changelog_note": "Skill operator plugin metadata binds manifest declarations to the plugin SDK ABI range.",
            "sdk_abi_min_major": SDK_ABI_MIN_MAJOR,
            "sdk_abi_max_major": SDK_ABI_MAX_MAJOR,
            "operator_plugin_fields": [
                "plugin_id",
                "abi_major",
                "default_tool_id",
                "default_module_path",
                "default_entrypoint",
                "contracts",
                "risk",
                "required_capabilities",
                "optional_capabilities",
                "storage_prefixes",
                "outbound_hosts",
                "secret_scopes",
                "event_subscriptions",
                "compatibility",
            ],
            "contract_declaration_fields": ["kind", "version"],
            "capability_requirement_fields": ["class", "value"],
            "compatibility_matrix_fields": ["min_abi_major", "max_abi_major", "host_versions"],
        },
        "public_error_contract": {
            "snapshot_version": "skill-manifest-contracts.errors.v1",
            "changelog_note": "Manifest parse and validation failures must remain explicit errors, not warnings.",
            "parse_error_kind": "ManifestParse",
            "validation_error_kind": "ManifestValidation",
            "secret_material_allowed": false,
        },
    })
}
