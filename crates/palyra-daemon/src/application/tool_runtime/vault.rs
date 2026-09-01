//! Metadata-only vault inspection for admitted agent tool calls.
//!
//! The tool accepts one exact vault reference and returns only its existence
//! and descriptor metadata. Secret material is never resolved or copied into
//! the tool result.

use std::sync::Arc;

use palyra_vault::VaultRef;
use serde_json::{json, Value};

use crate::{
    application::service_authorization::authorize_vault_action,
    gateway::{
        enforce_vault_scope_access, GatewayRuntimeState, ToolRuntimeExecutionContext,
        VAULT_METADATA_TOOL_NAME,
    },
    tool_protocol::{build_tool_execution_outcome, ToolExecutionOutcome},
    transport::grpc::auth::RequestContext,
};

const VAULT_METADATA_EXECUTOR: &str = "vault_metadata_runtime";

fn outcome(
    proposal_id: &str,
    input_json: &[u8],
    success: bool,
    output_json: Vec<u8>,
    error: String,
) -> ToolExecutionOutcome {
    build_tool_execution_outcome(
        proposal_id,
        VAULT_METADATA_TOOL_NAME,
        input_json,
        success,
        output_json,
        error,
        false,
        VAULT_METADATA_EXECUTOR.to_owned(),
        "none".to_owned(),
    )
}

fn parse_vault_reference(input_json: &[u8]) -> Result<VaultRef, String> {
    let input = serde_json::from_slice::<Value>(input_json)
        .map_err(|error| format!("{VAULT_METADATA_TOOL_NAME} invalid JSON input: {error}"))?;
    let object = input
        .as_object()
        .ok_or_else(|| format!("{VAULT_METADATA_TOOL_NAME} requires JSON object input"))?;
    if object.keys().any(|key| !matches!(key.as_str(), "scope" | "key")) {
        return Err(format!("{VAULT_METADATA_TOOL_NAME} accepts only 'scope' and 'key'"));
    }
    let scope = object
        .get("scope")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("{VAULT_METADATA_TOOL_NAME} requires non-empty string 'scope'"))?;
    let key = object
        .get("key")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("{VAULT_METADATA_TOOL_NAME} requires non-empty string 'key'"))?;
    VaultRef::parse(format!("{scope}/{key}").as_str())
        .map_err(|error| format!("{VAULT_METADATA_TOOL_NAME} invalid vault reference: {error}"))
}

/// Verifies one exact vault reference without resolving its secret value.
pub(crate) async fn execute_vault_metadata_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
    let vault_ref = match parse_vault_reference(input_json) {
        Ok(vault_ref) => vault_ref,
        Err(error) => return outcome(proposal_id, input_json, false, b"{}".to_vec(), error),
    };
    let request_context = RequestContext {
        principal: context.principal.to_owned(),
        device_id: context.device_id.to_owned(),
        channel: context.channel.map(str::to_owned),
    };
    if let Err(error) = enforce_vault_scope_access(&vault_ref.scope, &request_context) {
        return outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("{VAULT_METADATA_TOOL_NAME} {}", error.message()),
        );
    }
    if let Err(error) = authorize_vault_action(
        context.principal,
        "vault.metadata",
        format!("secrets:{}:{}", vault_ref.scope, vault_ref.key).as_str(),
    ) {
        return outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("{VAULT_METADATA_TOOL_NAME} {}", error.message()),
        );
    }
    let secrets = match runtime_state.vault_list_secrets(vault_ref.scope.clone()).await {
        Ok(secrets) => secrets,
        Err(error) => {
            return outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("{VAULT_METADATA_TOOL_NAME} failed: {}", error.message()),
            );
        }
    };
    let metadata = secrets.into_iter().find(|secret| secret.key == vault_ref.key);
    let payload = json!({
        "schema_version": 1,
        "scope": vault_ref.scope.to_string(),
        "key": vault_ref.key,
        "exists": metadata.is_some(),
        "metadata": metadata.map(|secret| json!({
            "created_at_unix_ms": secret.created_at_unix_ms,
            "updated_at_unix_ms": secret.updated_at_unix_ms,
            "value_bytes": secret.value_bytes,
        })),
        "secret_value_included": false,
    });
    match serde_json::to_vec(&payload) {
        Ok(output_json) => outcome(proposal_id, input_json, true, output_json, String::new()),
        Err(error) => outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("{VAULT_METADATA_TOOL_NAME} failed to serialize output: {error}"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::parse_vault_reference;

    #[test]
    fn vault_metadata_input_requires_one_valid_exact_reference() {
        let parsed = parse_vault_reference(br#"{"scope":"global","key":"openai_api_key"}"#)
            .expect("exact vault reference should parse");
        assert_eq!(parsed.scope.to_string(), "global");
        assert_eq!(parsed.key, "openai_api_key");

        assert!(parse_vault_reference(br#"{"scope":"global"}"#).is_err());
        assert!(parse_vault_reference(br#"{"scope":"global","key":"x","raw":true}"#).is_err());
    }
}
