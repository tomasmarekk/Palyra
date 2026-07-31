//! Admin-only console mutations for persistent MCP trust evidence.
//!
//! Callers submit only bounded descriptors, conformance evidence, and exact
//! compare-and-swap decisions. Attestation material is host-created after
//! authorization and never crosses this HTTP boundary.

use std::time::{SystemTime, UNIX_EPOCH};

use crate::{
    application::mcp_runtime::{
        McpCatalogEpochPin, McpConformanceReportV1, McpExternalToolDescriptor,
        McpProductionRuntimeError, McpToolEffectClassification, McpTrustedToolApproval,
        McpTrustedToolRecordV1,
    },
    *,
};

pub(crate) const MCP_TRUSTED_MUTATION_MAX_REQUEST_BODY_BYTES: usize = 256 * 1024;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleMcpTrustedToolRegisterRequest {
    name: String,
    description: String,
    input_schema_json: Value,
    #[serde(default)]
    output_schema_json: Option<Value>,
    effect: McpToolEffectClassification,
    approval_class: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleMcpConformanceRequest {
    report: McpConformanceReportV1,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleMcpTrustedToolDecisionRequest {
    tool_name: String,
    expected_revision: u64,
    descriptor_sha256: String,
    approved: bool,
    reason_code: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
struct ConsoleMcpTrustedToolMutationResponse {
    schema_version: u32,
    server_id: String,
    tool_name: String,
    runtime_generation: u64,
    catalog_epoch: u64,
    descriptor_sha256: String,
    activation: &'static str,
    revision: u64,
    reason_code: String,
    catalog_pin: ConsoleMcpCatalogPinResponse,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
struct ConsoleMcpCatalogPinResponse {
    runtime_generation: u64,
    catalog_epoch: u64,
    catalog_digest: String,
    record_revision: u64,
}

/// Registers a bounded host-trusted descriptor in pending state.
///
/// # Errors
/// Returns an error response when the caller is not an authenticated admin,
/// the descriptor is invalid, the server is ineligible, or durable state
/// cannot be updated.
pub(crate) async fn console_mcp_trusted_tool_register_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(server_id): Path<String>,
    Json(payload): Json<ConsoleMcpTrustedToolRegisterRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    ensure_admin_principal(&session.context).map_err(runtime_status_response)?;
    let descriptor = McpExternalToolDescriptor {
        name: payload.name,
        description: payload.description,
        input_schema_json: payload.input_schema_json,
        output_schema_json: payload.output_schema_json,
        effect: payload.effect,
        approval_class: payload.approval_class,
    };
    ensure_bounded_json(&descriptor).map_err(runtime_status_response)?;
    let runtime = state.runtime.mcp_runtime().ok_or_else(mcp_runtime_unavailable)?;
    let (record, pin) = runtime
        .register_trusted_tool(server_id.as_str(), descriptor)
        .await
        .map_err(mcp_runtime_error)?;
    Ok(Json(
        serde_json::to_value(project_trusted_tool_response(record, pin))
            .map_err(|_| mcp_internal_error())?,
    ))
}

/// Persists current generation- and catalog-bound MCP conformance evidence.
///
/// # Errors
/// Returns an error response when authorization fails, the report is invalid
/// or stale, or durable evidence persistence fails.
pub(crate) async fn console_mcp_conformance_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(server_id): Path<String>,
    Json(payload): Json<ConsoleMcpConformanceRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    ensure_admin_principal(&session.context).map_err(runtime_status_response)?;
    if payload.report.server_id != server_id {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "MCP conformance server_id does not match the route",
        )));
    }
    ensure_bounded_json(&payload.report).map_err(runtime_status_response)?;
    let runtime = state.runtime.mcp_runtime().ok_or_else(mcp_runtime_unavailable)?;
    runtime.record_conformance(&payload.report).await.map_err(mcp_runtime_error)?;
    Ok(Json(json!({
        "schema_version": 1,
        "server_id": payload.report.server_id,
        "runtime_generation": payload.report.runtime_generation,
        "catalog_epoch": payload.report.catalog_epoch,
        "qualified": payload.report.qualifies_for_production(),
        "accepted": true,
    })))
}

/// Applies an exact revision- and digest-bound trusted-tool decision.
///
/// # Errors
/// Returns an error response when authorization fails, the decision is stale,
/// current conformance is incomplete, or catalog publication fails.
pub(crate) async fn console_mcp_trusted_tool_decision_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(server_id): Path<String>,
    Json(payload): Json<ConsoleMcpTrustedToolDecisionRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    ensure_admin_principal(&session.context).map_err(runtime_status_response)?;
    ensure_bounded_json(&payload).map_err(runtime_status_response)?;
    let runtime = state.runtime.mcp_runtime().ok_or_else(mcp_runtime_unavailable)?;
    let decision = McpTrustedToolApproval {
        server_id,
        tool_name: payload.tool_name,
        expected_revision: payload.expected_revision,
        descriptor_sha256: payload.descriptor_sha256,
        approved: payload.approved,
        reason_code: payload.reason_code,
        decided_at_unix_ms: now_unix_ms().map_err(runtime_status_response)?,
    };
    let (record, pin) = runtime.decide_trusted_tool(&decision).await.map_err(mcp_runtime_error)?;
    Ok(Json(
        serde_json::to_value(project_trusted_tool_response(record, pin))
            .map_err(|_| mcp_internal_error())?,
    ))
}

fn ensure_admin_principal(context: &RequestContext) -> Result<(), tonic::Status> {
    if context.principal.starts_with("admin:") {
        return Ok(());
    }
    Err(tonic::Status::permission_denied("MCP trusted-tool mutations require an admin principal"))
}

fn ensure_bounded_json(value: &impl Serialize) -> Result<(), tonic::Status> {
    let encoded = serde_json::to_vec(value)
        .map_err(|_| tonic::Status::invalid_argument("invalid MCP mutation payload"))?;
    if encoded.len() > MCP_TRUSTED_MUTATION_MAX_REQUEST_BODY_BYTES {
        return Err(tonic::Status::invalid_argument("MCP mutation payload exceeds the size limit"));
    }
    Ok(())
}

fn project_trusted_tool_response(
    record: McpTrustedToolRecordV1,
    pin: McpCatalogEpochPin,
) -> ConsoleMcpTrustedToolMutationResponse {
    ConsoleMcpTrustedToolMutationResponse {
        schema_version: 1,
        server_id: record.server_id,
        tool_name: record.tool_name,
        runtime_generation: record.runtime_generation,
        catalog_epoch: record.catalog_epoch,
        descriptor_sha256: record.descriptor_sha256,
        activation: record.activation.as_str(),
        revision: record.revision,
        reason_code: record.reason_code,
        catalog_pin: ConsoleMcpCatalogPinResponse {
            runtime_generation: pin.runtime_generation,
            catalog_epoch: pin.catalog_epoch,
            catalog_digest: pin.catalog_digest,
            record_revision: pin.record_revision,
        },
    }
}

fn now_unix_ms() -> Result<i64, tonic::Status> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| {
        tonic::Status::internal("MCP trusted-tool mutation timestamp is unavailable")
    })?;
    i64::try_from(elapsed.as_millis())
        .map_err(|_| tonic::Status::internal("MCP trusted-tool mutation timestamp is out of range"))
}

fn mcp_runtime_unavailable() -> Response {
    runtime_status_response(tonic::Status::failed_precondition(
        "persistent MCP runtime is not enabled",
    ))
}

fn mcp_internal_error() -> Response {
    runtime_status_response(tonic::Status::internal("MCP trusted-tool mutation failed"))
}

fn mcp_runtime_error(error: McpProductionRuntimeError) -> Response {
    use crate::application::mcp_runtime::{
        McpDescriptorAdmissionError, McpSecurityEvidenceStoreError, McpTrustedToolRegistryError,
    };

    let status = match error {
        McpProductionRuntimeError::TrustedRegistrationDenied => tonic::Status::permission_denied(
            "MCP trusted descriptor registration is denied for this server",
        ),
        McpProductionRuntimeError::TrustedDescriptorInvalid
        | McpProductionRuntimeError::TrustedRegistry(
            McpTrustedToolRegistryError::InvalidRecord
            | McpTrustedToolRegistryError::InvalidConformance
            | McpTrustedToolRegistryError::Admission(
                McpDescriptorAdmissionError::InvalidPolicy
                | McpDescriptorAdmissionError::InvalidDescriptor
                | McpDescriptorAdmissionError::DescriptorTooLarge
                | McpDescriptorAdmissionError::SchemaTooComplex
                | McpDescriptorAdmissionError::DigestMismatch
                | McpDescriptorAdmissionError::TrustVerificationFailed
                | McpDescriptorAdmissionError::UntrustedIssuer
                | McpDescriptorAdmissionError::MutatingToolDenied
                | McpDescriptorAdmissionError::StaleRegistration,
            ),
        ) => tonic::Status::invalid_argument("invalid MCP trusted-tool mutation"),
        McpProductionRuntimeError::TrustedRegistry(McpTrustedToolRegistryError::NotFound) => {
            tonic::Status::not_found("MCP trusted tool was not found")
        }
        McpProductionRuntimeError::TrustedRegistry(
            McpTrustedToolRegistryError::StaleCatalog
            | McpTrustedToolRegistryError::StaleApproval
            | McpTrustedToolRegistryError::ConformanceRequired
            | McpTrustedToolRegistryError::Store(McpSecurityEvidenceStoreError::RevisionConflict {
                ..
            }),
        ) => tonic::Status::failed_precondition(
            "MCP trusted-tool mutation is stale or lacks current conformance",
        ),
        McpProductionRuntimeError::Registry(_)
        | McpProductionRuntimeError::Store(_)
        | McpProductionRuntimeError::Supervisor(_)
        | McpProductionRuntimeError::Broker(_)
        | McpProductionRuntimeError::CatalogAuthorityUnavailable
        | McpProductionRuntimeError::BrokerUnavailable
        | McpProductionRuntimeError::CatalogStateUnavailable
        | McpProductionRuntimeError::TrustedRegistry(McpTrustedToolRegistryError::Store(
            McpSecurityEvidenceStoreError::Corrupt { .. }
            | McpSecurityEvidenceStoreError::Unavailable { .. },
        )) => tonic::Status::internal("MCP trusted-tool mutation failed"),
    };
    runtime_status_response(status)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_request_rejects_executable_fields() {
        let error = serde_json::from_value::<ConsoleMcpTrustedToolRegisterRequest>(json!({
            "name": "trusted.lookup",
            "description": "bounded lookup",
            "input_schema_json": {"type": "object"},
            "effect": "read_only",
            "approval_class": "read_only",
            "command": "untrusted-binary",
        }))
        .expect_err("command-bearing registration payload must fail closed");

        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn decision_request_rejects_host_attestation_material() {
        let error = serde_json::from_value::<ConsoleMcpTrustedToolDecisionRequest>(json!({
            "tool_name": "trusted.lookup",
            "expected_revision": 4,
            "descriptor_sha256": "a".repeat(64),
            "approved": true,
            "reason_code": "mcp.runtime.trusted_tool.operator_approved",
            "signature": "caller-controlled",
        }))
        .expect_err("caller-supplied signature must fail closed");

        assert!(error.to_string().contains("unknown field"));
    }
}
