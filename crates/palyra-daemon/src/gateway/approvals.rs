//! Approval wire helpers: proto enum conversion for approval records and the
//! tamper-evident NDJSON export format (per-record checksums chained from a
//! fixed seed). Consumed by the gateway gRPC service and approval export RPCs.

use super::*;

/// Returns the lowercase hex SHA-256 digest of `bytes`.
pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

// Chains the export checksums: each link commits to the schema id, the record
// position, the previous link, and the record digest, so reordering, dropping,
// or editing any exported record breaks every subsequent chain checksum.
fn approval_export_chain_checksum(
    sequence: u64,
    previous_chain_checksum_sha256: &str,
    record_checksum_sha256: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(APPROVAL_EXPORT_NDJSON_SCHEMA_ID.as_bytes());
    hasher.update(b"\n");
    hasher.update(sequence.to_string().as_bytes());
    hasher.update(b"\n");
    hasher.update(previous_chain_checksum_sha256.as_bytes());
    hasher.update(b"\n");
    hasher.update(record_checksum_sha256.as_bytes());
    hex::encode(hasher.finalize())
}

/// Serializes one approval record as an NDJSON export line and returns the
/// line bytes (newline-terminated) together with the new chain checksum to
/// feed into the next record.
///
/// The record is round-tripped through [`serde_json::Value`] before hashing
/// so the checksum is computed over canonically ordered keys; a verifier can
/// re-serialize the embedded `record` object and reproduce
/// `record_checksum_sha256` from the line alone.
///
/// # Errors
/// Returns `Status::internal` when the record cannot be serialized to JSON.
#[allow(clippy::result_large_err)]
pub(crate) fn approval_export_ndjson_record_line(
    record: &ApprovalRecord,
    sequence: u64,
    previous_chain_checksum_sha256: &str,
) -> Result<(Vec<u8>, String), Status> {
    let record_payload = serde_json::to_value(record).map_err(|error| {
        Status::internal(format!("failed to serialize approval export record payload: {error}"))
    })?;
    let record_payload_bytes = serde_json::to_vec(&record_payload).map_err(|error| {
        Status::internal(format!("failed to encode approval export record payload bytes: {error}"))
    })?;
    let record_checksum_sha256 = sha256_hex(record_payload_bytes.as_slice());
    let chain_checksum_sha256 = approval_export_chain_checksum(
        sequence,
        previous_chain_checksum_sha256,
        record_checksum_sha256.as_str(),
    );
    let mut line = serde_json::to_vec(&json!({
        "schema": APPROVAL_EXPORT_NDJSON_SCHEMA_ID,
        "record_type": APPROVAL_EXPORT_NDJSON_RECORD_TYPE_ENTRY,
        "sequence": sequence,
        "prev_checksum_sha256": previous_chain_checksum_sha256,
        "record_checksum_sha256": record_checksum_sha256,
        "chain_checksum_sha256": chain_checksum_sha256,
        "record": record_payload,
    }))
    .map_err(|error| {
        Status::internal(format!("failed to encode approval export NDJSON record line: {error}"))
    })?;
    line.push(b'\n');
    Ok((line, chain_checksum_sha256))
}

/// Serializes the export trailer line carrying the record count and the final
/// chain checksum, letting consumers detect a truncated export.
///
/// # Errors
/// Returns `Status::internal` when the trailer cannot be serialized to JSON.
#[allow(clippy::result_large_err)]
pub(crate) fn approval_export_ndjson_trailer_line(
    exported_records: usize,
    final_chain_checksum_sha256: &str,
) -> Result<Vec<u8>, Status> {
    let mut line = serde_json::to_vec(&json!({
        "schema": APPROVAL_EXPORT_NDJSON_SCHEMA_ID,
        "record_type": APPROVAL_EXPORT_NDJSON_RECORD_TYPE_TRAILER,
        "exported_records": exported_records,
        "final_chain_checksum_sha256": final_chain_checksum_sha256,
    }))
    .map_err(|error| {
        Status::internal(format!("failed to encode approval export NDJSON trailer line: {error}"))
    })?;
    line.push(b'\n');
    Ok(line)
}

/// Converts a journal approval subject type to its proto enum value.
pub(crate) fn approval_subject_type_to_proto(value: ApprovalSubjectType) -> i32 {
    match value {
        ApprovalSubjectType::Tool => gateway_v1::ApprovalSubjectType::Tool as i32,
        ApprovalSubjectType::ChannelSend => gateway_v1::ApprovalSubjectType::ChannelSend as i32,
        ApprovalSubjectType::SecretAccess => gateway_v1::ApprovalSubjectType::SecretAccess as i32,
        ApprovalSubjectType::BrowserAction => gateway_v1::ApprovalSubjectType::BrowserAction as i32,
        ApprovalSubjectType::NodeCapability => {
            gateway_v1::ApprovalSubjectType::NodeCapability as i32
        }
        ApprovalSubjectType::DevicePairing => gateway_v1::ApprovalSubjectType::DevicePairing as i32,
        ApprovalSubjectType::StartupRecovery => {
            gateway_v1::ApprovalSubjectType::StartupRecovery as i32
        }
    }
}

/// Parses a proto approval subject type; `None` for unspecified or unknown
/// values so callers must decide how to treat an absent subject explicitly.
pub(crate) fn approval_subject_type_from_proto(value: i32) -> Option<ApprovalSubjectType> {
    match gateway_v1::ApprovalSubjectType::try_from(value)
        .unwrap_or(gateway_v1::ApprovalSubjectType::Unspecified)
    {
        gateway_v1::ApprovalSubjectType::Unspecified => None,
        gateway_v1::ApprovalSubjectType::Tool => Some(ApprovalSubjectType::Tool),
        gateway_v1::ApprovalSubjectType::ChannelSend => Some(ApprovalSubjectType::ChannelSend),
        gateway_v1::ApprovalSubjectType::SecretAccess => Some(ApprovalSubjectType::SecretAccess),
        gateway_v1::ApprovalSubjectType::BrowserAction => Some(ApprovalSubjectType::BrowserAction),
        gateway_v1::ApprovalSubjectType::NodeCapability => {
            Some(ApprovalSubjectType::NodeCapability)
        }
        gateway_v1::ApprovalSubjectType::DevicePairing => Some(ApprovalSubjectType::DevicePairing),
        gateway_v1::ApprovalSubjectType::StartupRecovery => {
            Some(ApprovalSubjectType::StartupRecovery)
        }
    }
}

/// Converts a journal approval decision to its proto enum value.
pub(crate) fn approval_decision_to_proto(value: ApprovalDecision) -> i32 {
    match value {
        ApprovalDecision::Allow => gateway_v1::ApprovalDecision::Allow as i32,
        ApprovalDecision::Deny => gateway_v1::ApprovalDecision::Deny as i32,
        ApprovalDecision::Timeout => gateway_v1::ApprovalDecision::Timeout as i32,
        ApprovalDecision::Error => gateway_v1::ApprovalDecision::Error as i32,
    }
}

/// Parses a proto approval decision; `None` for unspecified or unknown values
/// (an undecided approval), never silently coerced to allow or deny.
pub(crate) fn approval_decision_from_proto(value: i32) -> Option<ApprovalDecision> {
    match gateway_v1::ApprovalDecision::try_from(value)
        .unwrap_or(gateway_v1::ApprovalDecision::Unspecified)
    {
        gateway_v1::ApprovalDecision::Unspecified => None,
        gateway_v1::ApprovalDecision::Allow => Some(ApprovalDecision::Allow),
        gateway_v1::ApprovalDecision::Deny => Some(ApprovalDecision::Deny),
        gateway_v1::ApprovalDecision::Timeout => Some(ApprovalDecision::Timeout),
        gateway_v1::ApprovalDecision::Error => Some(ApprovalDecision::Error),
    }
}

/// Converts a journal approval decision scope to its proto enum value.
pub(crate) fn approval_scope_to_proto(value: ApprovalDecisionScope) -> i32 {
    match value {
        ApprovalDecisionScope::Once => common_v1::ApprovalDecisionScope::Once as i32,
        ApprovalDecisionScope::Session => common_v1::ApprovalDecisionScope::Session as i32,
        ApprovalDecisionScope::Timeboxed => common_v1::ApprovalDecisionScope::Timeboxed as i32,
    }
}

/// Parses a proto approval decision scope, defaulting to the narrowest scope.
pub(crate) fn approval_scope_from_proto(value: i32) -> ApprovalDecisionScope {
    match common_v1::ApprovalDecisionScope::try_from(value)
        .unwrap_or(common_v1::ApprovalDecisionScope::Unspecified)
    {
        // Unspecified or unknown scopes collapse to Once: an ambiguous client
        // request must never widen a decision to session or timeboxed reuse.
        common_v1::ApprovalDecisionScope::Unspecified => ApprovalDecisionScope::Once,
        common_v1::ApprovalDecisionScope::Once => ApprovalDecisionScope::Once,
        common_v1::ApprovalDecisionScope::Session => ApprovalDecisionScope::Session,
        common_v1::ApprovalDecisionScope::Timeboxed => ApprovalDecisionScope::Timeboxed,
    }
}

/// Converts a journal approval risk level to its proto enum value.
pub(crate) fn approval_risk_to_proto(value: ApprovalRiskLevel) -> i32 {
    match value {
        ApprovalRiskLevel::Low => common_v1::ApprovalRiskLevel::Low as i32,
        ApprovalRiskLevel::Medium => common_v1::ApprovalRiskLevel::Medium as i32,
        ApprovalRiskLevel::High => common_v1::ApprovalRiskLevel::High as i32,
        ApprovalRiskLevel::Critical => common_v1::ApprovalRiskLevel::Critical as i32,
    }
}
