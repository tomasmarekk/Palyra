//! Contract tests pinning protobuf/JSON schema invariants: versioned packages, reserved
//! fields, envelope version/limit fields, and JSON-proto parity for the message envelope.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context, Result};
use palyra_common::runtime_contracts::{
    RuntimeContractEnumValue, RuntimeErrorPhase, RuntimeEventActorKind, RuntimeEventEnvelopeV2,
    RuntimeEventId, RuntimeEventName, RuntimeEventPayloadRef, RuntimeEventRedactionClass,
    RuntimeEventValidationError, RuntimeGeneration, RuntimeIdentitySetV1, RuntimeRetryability,
    RuntimeRunId, RuntimeSessionId, RuntimeSubsystem, RuntimeTraceId, RUNTIME_EVENT_DESCRIPTORS,
};
use serde_json::Value;

fn collect_files(root: &Path, extension: &str) -> Result<Vec<PathBuf>> {
    let mut stack = vec![root.to_path_buf()];
    let mut files = Vec::new();

    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir)
            .with_context(|| format!("failed to read directory {}", dir.display()))?
        {
            let entry =
                entry.with_context(|| format!("failed to read entry in {}", dir.display()))?;
            let path = entry.path();
            let metadata = entry
                .metadata()
                .with_context(|| format!("failed to read metadata for {}", path.display()))?;
            if metadata.is_dir() {
                stack.push(path);
            } else if path.extension().and_then(|ext| ext.to_str()) == Some(extension) {
                files.push(path);
            }
        }
    }

    files.sort();
    Ok(files)
}

fn json_string_set_at(value: &Value, pointer: &str) -> Result<BTreeSet<String>> {
    value
        .pointer(pointer)
        .and_then(Value::as_array)
        .with_context(|| format!("expected string array at schema pointer {pointer}"))?
        .iter()
        .map(|item| {
            item.as_str()
                .map(str::to_owned)
                .with_context(|| format!("expected string value at schema pointer {pointer}"))
        })
        .collect()
}

fn canonical_wire_values(values: &[RuntimeContractEnumValue]) -> BTreeSet<String> {
    values.iter().map(|value| value.canonical.to_owned()).collect()
}

fn runtime_event_identity_requirements(
    schema: &Value,
) -> Result<BTreeMap<String, BTreeSet<String>>> {
    let rules = schema
        .get("allOf")
        .and_then(Value::as_array)
        .context("runtime event schema must define identity conditionals in allOf")?;
    let mut requirements = BTreeMap::new();

    for rule in rules {
        let event_names = json_string_set_at(rule, "/if/properties/event_name/enum")?;
        let required_identities = json_string_set_at(rule, "/then/properties/identities/required")?;
        for event_name in event_names {
            if requirements.insert(event_name.clone(), required_identities.clone()).is_some() {
                bail!("runtime event schema defines duplicate identity rules for {event_name}");
            }
        }
    }

    Ok(requirements)
}

#[test]
fn proto_schemas_are_versioned_and_forward_compatible() -> Result<()> {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .context("failed to resolve repository root")?;
    let proto_dir = repo_root.join("schemas").join("proto");
    let proto_files = collect_files(&proto_dir, "proto")?;

    assert!(!proto_files.is_empty(), "expected .proto files in {}", proto_dir.display());

    for file in proto_files {
        let content = fs::read_to_string(&file)
            .with_context(|| format!("failed to read {}", file.display()))?;
        assert!(
            content.contains("package palyra.") && content.contains(".v1;"),
            "proto file must define versioned package: {}",
            file.display()
        );
        assert!(
            content.contains("reserved "),
            "proto file must reserve fields for compatibility: {}",
            file.display()
        );
    }

    let common_proto =
        repo_root.join("schemas").join("proto").join("palyra").join("v1").join("common.proto");
    let common_content = fs::read_to_string(&common_proto)
        .with_context(|| format!("failed to read {}", common_proto.display()))?;
    assert!(common_content.contains("message RunStreamRequest"));
    assert!(common_content.contains("message RunStreamEvent"));
    Ok(())
}

#[test]
fn json_envelope_schemas_require_version_and_limits() -> Result<()> {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .context("failed to resolve repository root")?;
    let envelopes_dir = repo_root.join("schemas").join("json").join("envelopes");
    let envelope_files = collect_files(&envelopes_dir, "json")?;

    assert!(!envelope_files.is_empty(), "expected envelope schemas in {}", envelopes_dir.display());

    for file in envelope_files {
        let content = fs::read_to_string(&file)
            .with_context(|| format!("failed to read {}", file.display()))?;
        assert!(
            content.contains("\"v\""),
            "envelope schema must contain version field: {}",
            file.display()
        );
        assert!(
            content.contains("\"const\": 1"),
            "envelope schema must pin major version: {}",
            file.display()
        );
        assert!(
            content.contains("\"max_payload_bytes\""),
            "envelope schema must define hard payload cap: {}",
            file.display()
        );
    }
    Ok(())
}

#[test]
fn shared_runtime_schemas_are_versioned_closed_and_bounded() -> Result<()> {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .context("failed to resolve repository root")?;
    let common_dir = repo_root.join("schemas").join("json").join("common");
    let contracts = [
        ("runtime-identity-set.v1.json", 1_u64),
        ("runtime-event-envelope.v2.json", 2_u64),
        ("generation-lease.v1.json", 1_u64),
        ("generation-transition.v1.json", 1_u64),
        ("side-effect-fence.v1.json", 1_u64),
        ("cancellation-context.v1.json", 1_u64),
        ("backpressure-policy.v1.json", 1_u64),
        ("runtime-component-health.v1.json", 1_u64),
        ("health-probe-lease.v1.json", 1_u64),
        ("health-probe-result.v1.json", 1_u64),
        ("health-probe-settlement.v1.json", 1_u64),
        ("quarantine-clear-request.v1.json", 1_u64),
        ("runtime-handle-descriptor.v1.json", 1_u64),
        ("process-lease.v1.json", 1_u64),
        ("cleanup-report.v1.json", 1_u64),
        ("runtime-state-compatibility-report.v1.json", 1_u64),
        ("continuity-campaign-report.v1.json", 1_u64),
    ];

    for (name, expected_version) in contracts {
        let path = common_dir.join(name);
        let content = fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let schema: serde_json::Value = serde_json::from_str(content.as_str())
            .with_context(|| format!("failed to parse {}", path.display()))?;
        assert_eq!(
            schema.pointer("/properties/schema_version/const").and_then(serde_json::Value::as_u64),
            Some(expected_version),
            "shared runtime schema must pin schema_version: {}",
            path.display()
        );
        assert_eq!(
            schema.get("additionalProperties").and_then(serde_json::Value::as_bool),
            Some(false),
            "shared runtime schema must reject unknown top-level fields: {}",
            path.display()
        );
        assert!(
            content.contains("maxLength")
                || content.contains("maxItems")
                || content.contains("x-palyra-limits"),
            "shared runtime schema must publish hard bounds: {}",
            path.display()
        );
        assert!(!content.contains("raw_prompt"));
        assert!(!content.contains("raw_environment_values\": {"));
    }

    let compatibility_schema_path = common_dir.join("runtime-state-compatibility-report.v1.json");
    let compatibility_schema: serde_json::Value = serde_json::from_str(
        fs::read_to_string(&compatibility_schema_path)
            .with_context(|| format!("failed to read {}", compatibility_schema_path.display()))?
            .as_str(),
    )
    .with_context(|| format!("failed to parse {}", compatibility_schema_path.display()))?;
    let admission = compatibility_schema
        .pointer("/properties/admission")
        .context("compatibility schema must define admission")?;
    let description = admission
        .get("description")
        .and_then(serde_json::Value::as_str)
        .context("compatibility admission must document operational scope")?;
    assert!(description.contains("Serving admission requires ready"));
    assert!(description.contains("offline inspection and migration tooling"));

    Ok(())
}

#[test]
fn runtime_event_schema_matches_closed_rust_registry() -> Result<()> {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .context("failed to resolve repository root")?;
    let schema_path = repo_root
        .join("schemas")
        .join("json")
        .join("common")
        .join("runtime-event-envelope.v2.json");
    let schema: Value = serde_json::from_str(
        fs::read_to_string(&schema_path)
            .with_context(|| format!("failed to read {}", schema_path.display()))?
            .as_str(),
    )
    .with_context(|| format!("failed to parse {}", schema_path.display()))?;

    let expected_subsystems = RuntimeSubsystem::ALL
        .iter()
        .map(|value| value.as_str().to_owned())
        .collect::<BTreeSet<_>>();
    assert_eq!(json_string_set_at(&schema, "/properties/subsystem/enum")?, expected_subsystems);

    let expected_phases = RuntimeErrorPhase::ALL
        .iter()
        .map(|value| value.as_str().to_owned())
        .collect::<BTreeSet<_>>();
    assert_eq!(json_string_set_at(&schema, "/properties/phase/enum")?, expected_phases);

    let expected_retryability = RuntimeRetryability::ALL
        .iter()
        .map(|value| value.as_str().to_owned())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        json_string_set_at(&schema, "/properties/retryability/enum")?,
        expected_retryability
    );
    assert_eq!(
        json_string_set_at(&schema, "/properties/actor_kind/enum")?,
        canonical_wire_values(RuntimeEventActorKind::wire_contract_values())
    );
    assert_eq!(
        json_string_set_at(&schema, "/properties/redaction_class/enum")?,
        canonical_wire_values(RuntimeEventRedactionClass::wire_contract_values())
    );

    let registry_event_names = RUNTIME_EVENT_DESCRIPTORS
        .iter()
        .map(|descriptor| descriptor.name.as_str().to_owned())
        .collect::<BTreeSet<_>>();
    assert_eq!(json_string_set_at(&schema, "/properties/event_name/enum")?, registry_event_names);
    assert_eq!(
        registry_event_names,
        canonical_wire_values(RuntimeEventName::wire_contract_values())
    );

    let mut schema_identity_requirements = runtime_event_identity_requirements(&schema)?;
    for descriptor in RUNTIME_EVENT_DESCRIPTORS {
        let expected = descriptor
            .required_identity_fields
            .iter()
            .map(|field| (*field).to_owned())
            .collect::<BTreeSet<_>>();
        let actual =
            schema_identity_requirements.remove(descriptor.name.as_str()).unwrap_or_default();
        assert_eq!(
            actual, expected,
            "schema identity requirements drifted for {}",
            descriptor.name
        );
    }
    assert!(
        schema_identity_requirements.is_empty(),
        "schema identity rules contain unknown events: {schema_identity_requirements:?}"
    );

    assert_eq!(
        schema.pointer("/$defs/sha256/pattern").and_then(Value::as_str),
        Some("^[A-Fa-f0-9]{64}$")
    );
    let event_name = RuntimeEventName::RunStarted;
    let descriptor = event_name.descriptor();
    let mut event = RuntimeEventEnvelopeV2 {
        schema_version: 2,
        event_id: RuntimeEventId::parse("event_01")?,
        identities: RuntimeIdentitySetV1::for_run(
            RuntimeTraceId::parse("trace_01")?,
            RuntimeSessionId::parse("session_01")?,
            RuntimeRunId::parse("run_01")?,
            RuntimeGeneration::new(1)?,
        ),
        sequence: 1,
        causal_parent_event_id: None,
        subsystem: descriptor.subsystem,
        phase: descriptor.phase,
        event_name,
        reason_code: "runtime.schema_contract".to_owned(),
        actor_kind: descriptor.actor_kind,
        retryability: descriptor.retryability,
        redaction_class: descriptor.redaction_class,
        terminal: descriptor.terminal,
        payload: RuntimeEventPayloadRef::Artifact {
            artifact_id: "artifact_01".to_owned(),
            digest_sha256: "A".repeat(64),
            size_bytes: 1,
        },
        occurred_at_unix_ms: 1,
        extensions: BTreeMap::new(),
    };
    assert_eq!(event.validate(), Ok(()));
    event.payload = RuntimeEventPayloadRef::Artifact {
        artifact_id: "artifact_01".to_owned(),
        digest_sha256: "G".repeat(64),
        size_bytes: 1,
    };
    assert_eq!(event.validate(), Err(RuntimeEventValidationError::InvalidPayloadReference));

    Ok(())
}

#[test]
fn message_envelope_json_and_proto_contracts_stay_aligned() -> Result<()> {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .context("failed to resolve repository root")?;
    let json_schema =
        repo_root.join("schemas").join("json").join("envelopes").join("message-envelope.v1.json");
    let proto_schema =
        repo_root.join("schemas").join("proto").join("palyra").join("v1").join("common.proto");

    let json_content = fs::read_to_string(&json_schema)
        .with_context(|| format!("failed to read {}", json_schema.display()))?;
    let proto_content = fs::read_to_string(&proto_schema)
        .with_context(|| format!("failed to read {}", proto_schema.display()))?;

    for json_sender_field in ["\"display\"", "\"handle\"", "\"verified\""] {
        assert!(
            json_content.contains(json_sender_field),
            "message envelope JSON schema must include sender.{json_sender_field}"
        );
    }
    for proto_sender_field in
        ["string sender_display = 11;", "string sender_handle = 4;", "bool sender_verified = 5;"]
    {
        assert!(
            proto_content.contains(proto_sender_field),
            "common.proto must include sender parity field: {proto_sender_field}"
        );
    }

    assert!(json_content.contains("\"maxItems\": 32"));
    assert!(json_content.contains("\"maximum\": 104857600"));
    assert!(proto_content.contains("Canonical contract limit is 32 attachments."));
    assert!(proto_content.contains("Canonical contract limit is 104857600 bytes."));

    for json_trust in ["\"untrusted\"", "\"user-trusted\"", "\"system\""] {
        assert!(
            json_content.contains(json_trust),
            "message envelope JSON schema must include trust value {json_trust}"
        );
    }
    for proto_trust in
        ["TRUST_LEVEL_UNTRUSTED = 0;", "TRUST_LEVEL_USER_TRUSTED = 1;", "TRUST_LEVEL_SYSTEM = 2;"]
    {
        assert!(
            proto_content.contains(proto_trust),
            "common.proto must include trust value {proto_trust}"
        );
    }

    Ok(())
}
