//! Harness-aware context lifecycle adapter contracts.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Host ownership posture for context assembly and compaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextEngineOwnership {
    PalyraOwned,
    HarnessProvided,
    ExternalRuntime,
}

/// Lifecycle phase in which context is being prepared or maintained.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextEngineLifecyclePhase {
    Bootstrap,
    Assemble,
    FinalizeTurn,
    Maintenance,
}

/// Capability declaration for context assembly before a harness attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextEngineHostCapabilities {
    pub ownership: ContextEngineOwnership,
    pub phase: ContextEngineLifecyclePhase,
    pub trust_labels_validated: bool,
    pub memory_boundaries_validated: bool,
    pub attachments_metadata_validated: bool,
    pub prompt_cache_metadata_validated: bool,
    pub token_budget: u64,
    pub requested_tokens: u64,
    pub surface_hash: String,
}

/// Context data attached to a prepared harness attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedAttemptContextEnvelope {
    pub context_engine_trace_id: String,
    pub context_mode: ContextEngineOwnership,
    pub phase: ContextEngineLifecyclePhase,
    pub token_budget: u64,
    pub surface_hash: String,
    pub degraded_reason: Option<String>,
}

/// Context adapter validation failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum HarnessContextError {
    #[error("context token budget exceeded")]
    TokenBudgetExceeded,
    #[error("trust labels were not validated")]
    TrustLabelsMissing,
    #[error("memory boundaries were not validated")]
    MemoryBoundariesMissing,
    #[error("context surface hash is empty")]
    EmptySurfaceHash,
}

/// Validates context capabilities and builds the prepared-attempt context envelope.
///
/// # Errors
/// Returns [`HarnessContextError`] when context preparation would cross host
/// trust, memory, or budget boundaries.
pub fn prepare_harness_context_envelope(
    trace_id: impl Into<String>,
    capabilities: &ContextEngineHostCapabilities,
) -> Result<PreparedAttemptContextEnvelope, HarnessContextError> {
    if capabilities.requested_tokens > capabilities.token_budget {
        return Err(HarnessContextError::TokenBudgetExceeded);
    }
    if !capabilities.trust_labels_validated {
        return Err(HarnessContextError::TrustLabelsMissing);
    }
    if !capabilities.memory_boundaries_validated {
        return Err(HarnessContextError::MemoryBoundariesMissing);
    }
    if capabilities.surface_hash.trim().is_empty() {
        return Err(HarnessContextError::EmptySurfaceHash);
    }

    let degraded_reason = if !capabilities.attachments_metadata_validated {
        Some("context.attachments_metadata_degraded".to_owned())
    } else if !capabilities.prompt_cache_metadata_validated {
        Some("context.prompt_cache_metadata_degraded".to_owned())
    } else {
        None
    };

    let trace_id = trace_id.into();
    Ok(PreparedAttemptContextEnvelope {
        context_engine_trace_id: palyra_common::redaction::redact_diagnostic_text(&trace_id),
        context_mode: capabilities.ownership,
        phase: capabilities.phase,
        token_budget: capabilities.token_budget,
        surface_hash: capabilities.surface_hash.clone(),
        degraded_reason,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn capabilities() -> ContextEngineHostCapabilities {
        ContextEngineHostCapabilities {
            ownership: ContextEngineOwnership::PalyraOwned,
            phase: ContextEngineLifecyclePhase::Assemble,
            trust_labels_validated: true,
            memory_boundaries_validated: true,
            attachments_metadata_validated: true,
            prompt_cache_metadata_validated: true,
            token_budget: 8_192,
            requested_tokens: 1_024,
            surface_hash: "sha256:context".to_owned(),
        }
    }

    #[test]
    fn context_envelope_preserves_mode_trace_and_surface_hash() {
        let envelope =
            prepare_harness_context_envelope("trace?access_token=secret-token", &capabilities())
                .expect("context should prepare");
        let serialized = serde_json::to_string(&envelope).expect("envelope should serialize");

        assert_eq!(envelope.context_mode, ContextEngineOwnership::PalyraOwned);
        assert_eq!(envelope.surface_hash, "sha256:context");
        assert!(!serialized.contains("secret-token"));
    }

    #[test]
    fn context_capability_mismatch_blocks_before_provider_call() {
        let mut capabilities = capabilities();
        capabilities.trust_labels_validated = false;

        let error = prepare_harness_context_envelope("trace-1", &capabilities)
            .expect_err("missing trust labels should block");

        assert_eq!(error, HarnessContextError::TrustLabelsMissing);
    }

    #[test]
    fn context_degraded_reason_is_explicit() {
        let mut capabilities = capabilities();
        capabilities.attachments_metadata_validated = false;

        let envelope = prepare_harness_context_envelope("trace-1", &capabilities)
            .expect("degraded metadata should still produce envelope");

        assert_eq!(
            envelope.degraded_reason.as_deref(),
            Some("context.attachments_metadata_degraded")
        );
    }
}
