//! Safe projection from the rich orchestrator tape into bounded metadata traces.
//!
//! This module uses a closed field allowlist: source JSON is inspected only at
//! named machine fields and is never copied or recursively sanitized.

use std::fmt::Write as _;

use palyra_common::metadata_trace::metadata_trace_id_sha256;

use crate::journal::OrchestratorTapeRecord;

mod projection;

pub(crate) use projection::{
    metadata_trace_capacity_reached_event, project_orchestrator_tape_record,
};

/// Common closed identity domains shared by storage and projection.
pub(crate) use palyra_common::metadata_trace::MetadataTraceIdDomainV1 as MetadataTraceIdentifierDomain;

/// Metadata required to project one tape row into an ordered trace event.
#[derive(Debug, Clone, Copy)]
pub(crate) struct MetadataTraceProjectionContext<'a> {
    /// Canonical run identifier used only as domain-separated hash input.
    pub(crate) run_id: &'a str,
    /// Trace-local event sequence allocated by the trace writer.
    pub(crate) sequence: u32,
    /// Recovery generation that owns this event.
    pub(crate) generation: u32,
    /// Capture timestamp assigned by the trace writer.
    pub(crate) recorded_at_unix_ms: u64,
    /// Hash of the preceding causal event, when one exists.
    pub(crate) causal_parent_event_id_sha256: Option<&'a str>,
}

/// Hashes a run identifier without making the digest reusable in another ID domain.
#[must_use]
pub(crate) fn hash_metadata_trace_run_id(raw: &str) -> Option<String> {
    hash_metadata_trace_identifier(MetadataTraceIdentifierDomain::Run, raw)
}

/// Hashes a tool-call or proposal identifier for metadata-only persistence.
#[must_use]
pub(crate) fn hash_metadata_trace_tool_id(raw: &str) -> Option<String> {
    hash_metadata_trace_identifier(MetadataTraceIdentifierDomain::Tool, raw)
}

/// Hashes an approval identifier for metadata-only persistence.
#[must_use]
pub(crate) fn hash_metadata_trace_approval_id(raw: &str) -> Option<String> {
    hash_metadata_trace_identifier(MetadataTraceIdentifierDomain::Approval, raw)
}

/// Hashes a delivery identifier for metadata-only persistence.
#[must_use]
pub(crate) fn hash_metadata_trace_delivery_id(raw: &str) -> Option<String> {
    hash_metadata_trace_identifier(MetadataTraceIdentifierDomain::Delivery, raw)
}

/// Hashes a provider or auth-profile identifier for metadata-only persistence.
#[must_use]
pub(crate) fn hash_metadata_trace_profile_id(raw: &str) -> Option<String> {
    hash_metadata_trace_identifier(MetadataTraceIdentifierDomain::AuthProfile, raw)
}

/// Hashes a provider registry identifier for metadata-only persistence.
#[must_use]
pub(crate) fn hash_metadata_trace_provider_id(raw: &str) -> Option<String> {
    hash_metadata_trace_identifier(MetadataTraceIdentifierDomain::Provider, raw)
}

/// Hashes a provider model identifier for metadata-only persistence.
#[must_use]
pub(crate) fn hash_metadata_trace_model_id(raw: &str) -> Option<String> {
    hash_metadata_trace_identifier(MetadataTraceIdentifierDomain::Model, raw)
}

/// Hashes a bounded identifier under one closed metadata-trace domain.
///
/// Storage and projection must share this byte-level contract so trace
/// identities remain comparable without exposing their raw values.
#[must_use]
pub(crate) fn hash_metadata_trace_identifier(
    domain: MetadataTraceIdentifierDomain,
    raw: &str,
) -> Option<String> {
    metadata_trace_id_sha256(domain, raw).ok()
}

fn projected_event_id_sha256(
    context: MetadataTraceProjectionContext<'_>,
    record: &OrchestratorTapeRecord,
) -> Option<String> {
    let source_sequence = u64::try_from(record.seq).ok()?;
    let mut identity = String::with_capacity(
        context.run_id.len().saturating_add(record.event_type.len()).saturating_add(64),
    );
    write!(
        identity,
        "{}:{}:{source_sequence}:{}",
        context.run_id, context.generation, record.event_type
    )
    .ok()?;
    hash_metadata_trace_identifier(MetadataTraceIdentifierDomain::Event, identity.as_str())
}

#[cfg(test)]
mod tests;
