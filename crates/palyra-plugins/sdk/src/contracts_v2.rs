//! Typed request and result schemas for the first executable plugin contracts.
//!
//! These DTOs deliberately omit direct host authority: memory can only return
//! candidates, model authentication can only return an opaque handle, and
//! middleware must preserve the host's mutation and approval classification.

use std::{error::Error, fmt};

use serde::{Deserialize, Serialize};

use crate::{
    PluginCapabilityHandleIdV2, PluginCapabilityHandleV2, PluginCapabilityScopeV2,
    PluginRuntimeGenerationV2, PluginSchemaHashV2,
};

/// Failure while encoding or decoding a bounded core-Wasm contract payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PluginContractCodecError {
    /// A length cannot be represented by the bounded wire format.
    LengthExceeded,
    /// Input ended before the declared value was complete.
    Truncated,
    /// An enum tag or boolean value is unknown.
    InvalidTag,
    /// Text is not valid UTF-8.
    InvalidUtf8,
    /// A decoded identifier, hash, or generation violates an SDK invariant.
    InvalidValue,
    /// Bytes remain after a complete value.
    TrailingBytes,
}

impl fmt::Display for PluginContractCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::LengthExceeded => "plugin contract payload length exceeded",
            Self::Truncated => "plugin contract payload is truncated",
            Self::InvalidTag => "plugin contract payload has an invalid tag",
            Self::InvalidUtf8 => "plugin contract payload is not valid utf-8",
            Self::InvalidValue => "plugin contract payload has an invalid value",
            Self::TrailingBytes => "plugin contract payload has trailing bytes",
        })
    }
}

impl Error for PluginContractCodecError {}

/// Request for an external agent-harness attempt.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentHarnessInvocationV2 {
    /// Host-owned prepared-attempt reference.
    pub prepared_attempt_ref: String,
    /// Hash of the redacted objective projection.
    pub objective_hash: PluginSchemaHashV2,
    /// Maximum harness steps approved by the host.
    pub max_steps: u32,
}

/// Terminal agent-harness classification.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum AgentHarnessOutcomeV2 {
    /// Harness completed its bounded attempt.
    Completed,
    /// Harness yielded back to the host-owned callback bridge.
    HostCallbackRequired,
    /// Harness declined the prepared attempt without side effects.
    Declined,
}

/// Typed terminal result from an agent-harness guest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentHarnessResultV2 {
    /// Terminal harness classification.
    pub outcome: AgentHarnessOutcomeV2,
    /// Optional host-owned artifact or transcript reference.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_ref: Option<String>,
    /// Number of bounded steps consumed.
    pub steps_used: u32,
}

/// Request for a context-engine projection.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ContextEngineInvocationV2 {
    /// Host-owned session reference.
    pub session_ref: String,
    /// Hash of the host-projected context state.
    pub context_state_hash: PluginSchemaHashV2,
    /// Maximum number of candidate segments.
    pub max_segments: u32,
    /// Maximum aggregate projected tokens.
    pub token_budget: u32,
}

/// Candidate segment proposed by a context engine.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ContextSegmentCandidateV2 {
    /// Host-resolvable segment reference.
    pub segment_ref: String,
    /// Hash of the immutable segment contents.
    pub content_hash: PluginSchemaHashV2,
    /// Relevance score in inclusive thousandths.
    pub relevance_millis: u16,
    /// Estimated token contribution.
    pub estimated_tokens: u32,
}

/// Typed result from a context-engine guest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ContextEngineResultV2 {
    /// Ordered candidate segments; the host performs final budget validation.
    pub candidates: Vec<ContextSegmentCandidateV2>,
}

/// Host-owned mutation classification that middleware cannot rewrite.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ToolMutationClassV2 {
    /// Read-only operation.
    ReadOnly,
    /// Durable host-owned state mutation.
    DurableWrite,
    /// External or otherwise non-idempotent side effect.
    ExternalSideEffect,
}

/// Request for bounded tool-result middleware.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ToolResultMiddlewareInvocationV2 {
    /// Mutation class assigned by the host before middleware.
    pub mutation_class: ToolMutationClassV2,
    /// Approval requirement assigned by the host.
    pub approval_required: bool,
    /// Hash of the redacted tool result projection.
    pub tool_result_hash: PluginSchemaHashV2,
    /// Maximum projected result bytes.
    pub max_projection_bytes: u32,
}

/// Visibility the middleware requests for its bounded projection.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ToolResultVisibilityV2 {
    /// Preserve the host projection.
    HostProjection,
    /// Reduce the projection to redacted content.
    Redacted,
    /// Reduce the projection to metadata only.
    MetadataOnly,
}

/// Typed result from tool-result middleware.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ToolResultMiddlewareResultV2 {
    /// Mutation class, which must equal the host input.
    pub mutation_class: ToolMutationClassV2,
    /// Approval posture, which cannot change from required to not required.
    pub approval_required: bool,
    /// Requested bounded visibility.
    pub visibility: ToolResultVisibilityV2,
    /// Serialized projection subject to the input byte bound.
    pub projected_bytes: Vec<u8>,
}

impl fmt::Debug for ToolResultMiddlewareResultV2 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolResultMiddlewareResultV2")
            .field("mutation_class", &self.mutation_class)
            .field("approval_required", &self.approval_required)
            .field("visibility", &self.visibility)
            .field(
                "projected_bytes",
                &format_args!("<redacted:{} bytes>", self.projected_bytes.len()),
            )
            .finish()
    }
}

/// Lifecycle role granted to a hook by the host.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum RunLifecycleHookRoleV2 {
    /// Observe without changing the transition.
    Observer,
    /// Attach bounded annotations without changing the transition.
    Annotator,
    /// Filter a transition through host validation.
    Filter,
    /// Request host-owned approval.
    ApprovalRequester,
    /// Block a transition under host policy.
    Blocker,
    /// Request a bounded transform that the host validates.
    LimitedTransformer,
}

/// Request for a run lifecycle hook.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RunLifecycleHookInvocationV2 {
    /// Hook role approved in the binding.
    pub role: RunLifecycleHookRoleV2,
    /// Stable lifecycle phase.
    pub phase: String,
    /// Hash of the redacted host event.
    pub event_hash: PluginSchemaHashV2,
}

/// Lifecycle action proposed by a hook.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum RunLifecycleActionV2 {
    /// Do not alter host processing.
    Continue,
    /// Attach a bounded annotation.
    Annotate,
    /// Reject the projected item under host validation.
    Filter,
    /// Request an approval through the host gate.
    RequestApproval,
    /// Block the lifecycle transition.
    Block,
    /// Request a bounded transformation through host validation.
    Transform,
}

/// Typed result from a run lifecycle hook.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RunLifecycleHookResultV2 {
    /// Role echoed by the guest and validated against the input.
    pub role: RunLifecycleHookRoleV2,
    /// Proposed action constrained by the role.
    pub action: RunLifecycleActionV2,
    /// Optional hash of a host-resolvable annotation or transform.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact_hash: Option<PluginSchemaHashV2>,
}

/// Request for candidate-only memory retrieval.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MemoryProviderInvocationV2 {
    /// Hash of the host-owned query projection.
    pub query_hash: PluginSchemaHashV2,
    /// Maximum candidates the host will review.
    pub max_candidates: u32,
    /// Host-owned namespace reference.
    pub namespace_ref: String,
}

/// Candidate memory proposed for host policy and durable-write review.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MemoryCandidateV2 {
    /// Host-resolvable candidate reference.
    pub candidate_ref: String,
    /// Hash of candidate contents.
    pub content_hash: PluginSchemaHashV2,
    /// Relevance score in inclusive thousandths.
    pub relevance_millis: u16,
}

/// Typed memory-provider result containing candidates, never writes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MemoryProviderResultV2 {
    /// Candidates to pass through the host-owned review workflow.
    pub candidates: Vec<MemoryCandidateV2>,
}

/// Request for an opaque model-auth handle.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ModelAuthProviderInvocationV2 {
    /// Stable provider identifier.
    pub provider_id: String,
    /// Hash of the host-owned profile selection inputs.
    pub profile_selector_hash: PluginSchemaHashV2,
}

/// Typed model-auth result containing only host-mediated handle metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ModelAuthProviderResultV2 {
    /// Opaque, scoped, expiring credential lease handle.
    pub credential_handle: PluginCapabilityHandleV2,
}

macro_rules! core_codec {
    ($type:ty, $encode_body:expr, $decode_body:expr) => {
        impl $type {
            /// Encodes this value for the bounded core-Wasm contract ABI.
            ///
            /// # Errors
            /// Returns [`PluginContractCodecError::LengthExceeded`] when a
            /// string, byte vector, or collection exceeds its wire bound.
            pub fn encode_core_bytes(&self) -> Result<Vec<u8>, PluginContractCodecError> {
                let mut writer = ContractWriter::default();
                ($encode_body)(&mut writer, self)?;
                Ok(writer.into_bytes())
            }

            /// Decodes this value from the bounded core-Wasm contract ABI.
            ///
            /// # Errors
            /// Returns [`PluginContractCodecError`] for malformed, truncated,
            /// noncanonical, or trailing input.
            pub fn decode_core_bytes(bytes: &[u8]) -> Result<Self, PluginContractCodecError> {
                let mut reader = ContractReader::new(bytes);
                let value = ($decode_body)(&mut reader)?;
                reader.finish()?;
                Ok(value)
            }
        }
    };
}

core_codec!(
    AgentHarnessInvocationV2,
    |writer: &mut ContractWriter, value: &AgentHarnessInvocationV2| {
        writer.string(&value.prepared_attempt_ref)?;
        writer.hash(&value.objective_hash);
        writer.u32(value.max_steps);
        Ok(())
    },
    |reader: &mut ContractReader<'_>| {
        Ok(AgentHarnessInvocationV2 {
            prepared_attempt_ref: reader.string()?,
            objective_hash: reader.hash("objective_hash")?,
            max_steps: reader.u32()?,
        })
    }
);

core_codec!(
    AgentHarnessResultV2,
    |writer: &mut ContractWriter, value: &AgentHarnessResultV2| {
        writer.u8(agent_harness_outcome_tag(value.outcome));
        writer.optional_string(value.output_ref.as_deref())?;
        writer.u32(value.steps_used);
        Ok(())
    },
    |reader: &mut ContractReader<'_>| {
        Ok(AgentHarnessResultV2 {
            outcome: decode_agent_harness_outcome(reader.u8()?)?,
            output_ref: reader.optional_string()?,
            steps_used: reader.u32()?,
        })
    }
);

core_codec!(
    ContextEngineInvocationV2,
    |writer: &mut ContractWriter, value: &ContextEngineInvocationV2| {
        writer.string(&value.session_ref)?;
        writer.hash(&value.context_state_hash);
        writer.u32(value.max_segments);
        writer.u32(value.token_budget);
        Ok(())
    },
    |reader: &mut ContractReader<'_>| {
        Ok(ContextEngineInvocationV2 {
            session_ref: reader.string()?,
            context_state_hash: reader.hash("context_state_hash")?,
            max_segments: reader.u32()?,
            token_budget: reader.u32()?,
        })
    }
);

core_codec!(
    ContextEngineResultV2,
    |writer: &mut ContractWriter, value: &ContextEngineResultV2| {
        writer.count(value.candidates.len())?;
        for candidate in &value.candidates {
            writer.string(&candidate.segment_ref)?;
            writer.hash(&candidate.content_hash);
            writer.u16(candidate.relevance_millis);
            writer.u32(candidate.estimated_tokens);
        }
        Ok(())
    },
    |reader: &mut ContractReader<'_>| {
        let count = reader.count()?;
        let mut candidates = Vec::with_capacity(count);
        for _ in 0..count {
            candidates.push(ContextSegmentCandidateV2 {
                segment_ref: reader.string()?,
                content_hash: reader.hash("content_hash")?,
                relevance_millis: reader.u16()?,
                estimated_tokens: reader.u32()?,
            });
        }
        Ok(ContextEngineResultV2 { candidates })
    }
);

core_codec!(
    ToolResultMiddlewareInvocationV2,
    |writer: &mut ContractWriter, value: &ToolResultMiddlewareInvocationV2| {
        writer.u8(tool_mutation_class_tag(value.mutation_class));
        writer.boolean(value.approval_required);
        writer.hash(&value.tool_result_hash);
        writer.u32(value.max_projection_bytes);
        Ok(())
    },
    |reader: &mut ContractReader<'_>| {
        Ok(ToolResultMiddlewareInvocationV2 {
            mutation_class: decode_tool_mutation_class(reader.u8()?)?,
            approval_required: reader.boolean()?,
            tool_result_hash: reader.hash("tool_result_hash")?,
            max_projection_bytes: reader.u32()?,
        })
    }
);

core_codec!(
    ToolResultMiddlewareResultV2,
    |writer: &mut ContractWriter, value: &ToolResultMiddlewareResultV2| {
        writer.u8(tool_mutation_class_tag(value.mutation_class));
        writer.boolean(value.approval_required);
        writer.u8(tool_result_visibility_tag(value.visibility));
        writer.bytes(&value.projected_bytes)?;
        Ok(())
    },
    |reader: &mut ContractReader<'_>| {
        Ok(ToolResultMiddlewareResultV2 {
            mutation_class: decode_tool_mutation_class(reader.u8()?)?,
            approval_required: reader.boolean()?,
            visibility: decode_tool_result_visibility(reader.u8()?)?,
            projected_bytes: reader.bytes()?,
        })
    }
);

core_codec!(
    RunLifecycleHookInvocationV2,
    |writer: &mut ContractWriter, value: &RunLifecycleHookInvocationV2| {
        writer.u8(run_lifecycle_role_tag(value.role));
        writer.string(&value.phase)?;
        writer.hash(&value.event_hash);
        Ok(())
    },
    |reader: &mut ContractReader<'_>| {
        Ok(RunLifecycleHookInvocationV2 {
            role: decode_run_lifecycle_role(reader.u8()?)?,
            phase: reader.string()?,
            event_hash: reader.hash("event_hash")?,
        })
    }
);

core_codec!(
    RunLifecycleHookResultV2,
    |writer: &mut ContractWriter, value: &RunLifecycleHookResultV2| {
        writer.u8(run_lifecycle_role_tag(value.role));
        writer.u8(run_lifecycle_action_tag(value.action));
        writer.optional_hash(value.artifact_hash.as_ref());
        Ok(())
    },
    |reader: &mut ContractReader<'_>| {
        Ok(RunLifecycleHookResultV2 {
            role: decode_run_lifecycle_role(reader.u8()?)?,
            action: decode_run_lifecycle_action(reader.u8()?)?,
            artifact_hash: reader.optional_hash("artifact_hash")?,
        })
    }
);

core_codec!(
    MemoryProviderInvocationV2,
    |writer: &mut ContractWriter, value: &MemoryProviderInvocationV2| {
        writer.hash(&value.query_hash);
        writer.u32(value.max_candidates);
        writer.string(&value.namespace_ref)?;
        Ok(())
    },
    |reader: &mut ContractReader<'_>| {
        Ok(MemoryProviderInvocationV2 {
            query_hash: reader.hash("query_hash")?,
            max_candidates: reader.u32()?,
            namespace_ref: reader.string()?,
        })
    }
);

core_codec!(
    MemoryProviderResultV2,
    |writer: &mut ContractWriter, value: &MemoryProviderResultV2| {
        writer.count(value.candidates.len())?;
        for candidate in &value.candidates {
            writer.string(&candidate.candidate_ref)?;
            writer.hash(&candidate.content_hash);
            writer.u16(candidate.relevance_millis);
        }
        Ok(())
    },
    |reader: &mut ContractReader<'_>| {
        let count = reader.count()?;
        let mut candidates = Vec::with_capacity(count);
        for _ in 0..count {
            candidates.push(MemoryCandidateV2 {
                candidate_ref: reader.string()?,
                content_hash: reader.hash("content_hash")?,
                relevance_millis: reader.u16()?,
            });
        }
        Ok(MemoryProviderResultV2 { candidates })
    }
);

core_codec!(
    ModelAuthProviderInvocationV2,
    |writer: &mut ContractWriter, value: &ModelAuthProviderInvocationV2| {
        writer.string(&value.provider_id)?;
        writer.hash(&value.profile_selector_hash);
        Ok(())
    },
    |reader: &mut ContractReader<'_>| {
        Ok(ModelAuthProviderInvocationV2 {
            provider_id: reader.string()?,
            profile_selector_hash: reader.hash("profile_selector_hash")?,
        })
    }
);

core_codec!(
    ModelAuthProviderResultV2,
    |writer: &mut ContractWriter, value: &ModelAuthProviderResultV2| {
        writer.capability_handle(&value.credential_handle)?;
        Ok(())
    },
    |reader: &mut ContractReader<'_>| {
        Ok(ModelAuthProviderResultV2 { credential_handle: reader.capability_handle()? })
    }
);

#[derive(Default)]
struct ContractWriter {
    bytes: Vec<u8>,
}

impl ContractWriter {
    fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn u16(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn boolean(&mut self, value: bool) {
        self.u8(u8::from(value));
    }

    fn count(&mut self, value: usize) -> Result<(), PluginContractCodecError> {
        let value = u16::try_from(value).map_err(|_| PluginContractCodecError::LengthExceeded)?;
        self.u16(value);
        Ok(())
    }

    fn string(&mut self, value: &str) -> Result<(), PluginContractCodecError> {
        self.count(value.len())?;
        self.bytes.extend_from_slice(value.as_bytes());
        Ok(())
    }

    fn optional_string(&mut self, value: Option<&str>) -> Result<(), PluginContractCodecError> {
        self.boolean(value.is_some());
        if let Some(value) = value {
            self.string(value)?;
        }
        Ok(())
    }

    fn bytes(&mut self, value: &[u8]) -> Result<(), PluginContractCodecError> {
        let length =
            u32::try_from(value.len()).map_err(|_| PluginContractCodecError::LengthExceeded)?;
        self.u32(length);
        self.bytes.extend_from_slice(value);
        Ok(())
    }

    fn hash(&mut self, value: &PluginSchemaHashV2) {
        self.bytes.extend_from_slice(value.as_str().as_bytes());
    }

    fn optional_hash(&mut self, value: Option<&PluginSchemaHashV2>) {
        self.boolean(value.is_some());
        if let Some(value) = value {
            self.hash(value);
        }
    }

    fn capability_handle(
        &mut self,
        value: &PluginCapabilityHandleV2,
    ) -> Result<(), PluginContractCodecError> {
        self.string(value.handle_id().as_str())?;
        self.u8(capability_scope_tag(value.scope));
        self.hash(&value.scope_hash);
        self.u64(value.runtime_generation.get());
        self.u64(value.expires_at_unix_ms);
        Ok(())
    }
}

struct ContractReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> ContractReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn finish(self) -> Result<(), PluginContractCodecError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(PluginContractCodecError::TrailingBytes)
        }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], PluginContractCodecError> {
        let end = self.offset.checked_add(length).ok_or(PluginContractCodecError::Truncated)?;
        let value = self.bytes.get(self.offset..end).ok_or(PluginContractCodecError::Truncated)?;
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, PluginContractCodecError> {
        self.take(1)?.first().copied().ok_or(PluginContractCodecError::Truncated)
    }

    fn u16(&mut self) -> Result<u16, PluginContractCodecError> {
        let bytes: [u8; 2] =
            self.take(2)?.try_into().map_err(|_| PluginContractCodecError::Truncated)?;
        Ok(u16::from_le_bytes(bytes))
    }

    fn u32(&mut self) -> Result<u32, PluginContractCodecError> {
        let bytes: [u8; 4] =
            self.take(4)?.try_into().map_err(|_| PluginContractCodecError::Truncated)?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn u64(&mut self) -> Result<u64, PluginContractCodecError> {
        let bytes: [u8; 8] =
            self.take(8)?.try_into().map_err(|_| PluginContractCodecError::Truncated)?;
        Ok(u64::from_le_bytes(bytes))
    }

    fn boolean(&mut self) -> Result<bool, PluginContractCodecError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(PluginContractCodecError::InvalidTag),
        }
    }

    fn count(&mut self) -> Result<usize, PluginContractCodecError> {
        Ok(usize::from(self.u16()?))
    }

    fn string(&mut self) -> Result<String, PluginContractCodecError> {
        let length = self.count()?;
        let bytes = self.take(length)?;
        let value =
            std::str::from_utf8(bytes).map_err(|_| PluginContractCodecError::InvalidUtf8)?;
        Ok(value.to_owned())
    }

    fn optional_string(&mut self) -> Result<Option<String>, PluginContractCodecError> {
        if self.boolean()? {
            Ok(Some(self.string()?))
        } else {
            Ok(None)
        }
    }

    fn bytes(&mut self) -> Result<Vec<u8>, PluginContractCodecError> {
        let length =
            usize::try_from(self.u32()?).map_err(|_| PluginContractCodecError::LengthExceeded)?;
        Ok(self.take(length)?.to_vec())
    }

    fn hash(
        &mut self,
        field: &'static str,
    ) -> Result<PluginSchemaHashV2, PluginContractCodecError> {
        let bytes = self.take(64)?;
        let value =
            std::str::from_utf8(bytes).map_err(|_| PluginContractCodecError::InvalidUtf8)?;
        PluginSchemaHashV2::parse(field, value).map_err(|_| PluginContractCodecError::InvalidValue)
    }

    fn optional_hash(
        &mut self,
        field: &'static str,
    ) -> Result<Option<PluginSchemaHashV2>, PluginContractCodecError> {
        if self.boolean()? {
            Ok(Some(self.hash(field)?))
        } else {
            Ok(None)
        }
    }

    fn capability_handle(&mut self) -> Result<PluginCapabilityHandleV2, PluginContractCodecError> {
        let handle_id = PluginCapabilityHandleIdV2::new(self.string()?)
            .map_err(|_| PluginContractCodecError::InvalidValue)?;
        let scope = decode_capability_scope(self.u8()?)?;
        let scope_hash = self.hash("scope_hash")?;
        let runtime_generation = PluginRuntimeGenerationV2::new(self.u64()?)
            .map_err(|_| PluginContractCodecError::InvalidValue)?;
        let expires_at_unix_ms = self.u64()?;
        PluginCapabilityHandleV2::new(
            handle_id,
            scope,
            scope_hash,
            runtime_generation,
            expires_at_unix_ms,
        )
        .map_err(|_| PluginContractCodecError::InvalidValue)
    }
}

fn agent_harness_outcome_tag(value: AgentHarnessOutcomeV2) -> u8 {
    match value {
        AgentHarnessOutcomeV2::Completed => 1,
        AgentHarnessOutcomeV2::HostCallbackRequired => 2,
        AgentHarnessOutcomeV2::Declined => 3,
    }
}

fn decode_agent_harness_outcome(
    value: u8,
) -> Result<AgentHarnessOutcomeV2, PluginContractCodecError> {
    match value {
        1 => Ok(AgentHarnessOutcomeV2::Completed),
        2 => Ok(AgentHarnessOutcomeV2::HostCallbackRequired),
        3 => Ok(AgentHarnessOutcomeV2::Declined),
        _ => Err(PluginContractCodecError::InvalidTag),
    }
}

fn tool_mutation_class_tag(value: ToolMutationClassV2) -> u8 {
    match value {
        ToolMutationClassV2::ReadOnly => 1,
        ToolMutationClassV2::DurableWrite => 2,
        ToolMutationClassV2::ExternalSideEffect => 3,
    }
}

fn decode_tool_mutation_class(value: u8) -> Result<ToolMutationClassV2, PluginContractCodecError> {
    match value {
        1 => Ok(ToolMutationClassV2::ReadOnly),
        2 => Ok(ToolMutationClassV2::DurableWrite),
        3 => Ok(ToolMutationClassV2::ExternalSideEffect),
        _ => Err(PluginContractCodecError::InvalidTag),
    }
}

fn tool_result_visibility_tag(value: ToolResultVisibilityV2) -> u8 {
    match value {
        ToolResultVisibilityV2::HostProjection => 1,
        ToolResultVisibilityV2::Redacted => 2,
        ToolResultVisibilityV2::MetadataOnly => 3,
    }
}

fn decode_tool_result_visibility(
    value: u8,
) -> Result<ToolResultVisibilityV2, PluginContractCodecError> {
    match value {
        1 => Ok(ToolResultVisibilityV2::HostProjection),
        2 => Ok(ToolResultVisibilityV2::Redacted),
        3 => Ok(ToolResultVisibilityV2::MetadataOnly),
        _ => Err(PluginContractCodecError::InvalidTag),
    }
}

fn run_lifecycle_role_tag(value: RunLifecycleHookRoleV2) -> u8 {
    match value {
        RunLifecycleHookRoleV2::Observer => 1,
        RunLifecycleHookRoleV2::Annotator => 2,
        RunLifecycleHookRoleV2::Filter => 3,
        RunLifecycleHookRoleV2::ApprovalRequester => 4,
        RunLifecycleHookRoleV2::Blocker => 5,
        RunLifecycleHookRoleV2::LimitedTransformer => 6,
    }
}

fn decode_run_lifecycle_role(
    value: u8,
) -> Result<RunLifecycleHookRoleV2, PluginContractCodecError> {
    match value {
        1 => Ok(RunLifecycleHookRoleV2::Observer),
        2 => Ok(RunLifecycleHookRoleV2::Annotator),
        3 => Ok(RunLifecycleHookRoleV2::Filter),
        4 => Ok(RunLifecycleHookRoleV2::ApprovalRequester),
        5 => Ok(RunLifecycleHookRoleV2::Blocker),
        6 => Ok(RunLifecycleHookRoleV2::LimitedTransformer),
        _ => Err(PluginContractCodecError::InvalidTag),
    }
}

fn run_lifecycle_action_tag(value: RunLifecycleActionV2) -> u8 {
    match value {
        RunLifecycleActionV2::Continue => 1,
        RunLifecycleActionV2::Annotate => 2,
        RunLifecycleActionV2::Filter => 3,
        RunLifecycleActionV2::RequestApproval => 4,
        RunLifecycleActionV2::Block => 5,
        RunLifecycleActionV2::Transform => 6,
    }
}

fn decode_run_lifecycle_action(
    value: u8,
) -> Result<RunLifecycleActionV2, PluginContractCodecError> {
    match value {
        1 => Ok(RunLifecycleActionV2::Continue),
        2 => Ok(RunLifecycleActionV2::Annotate),
        3 => Ok(RunLifecycleActionV2::Filter),
        4 => Ok(RunLifecycleActionV2::RequestApproval),
        5 => Ok(RunLifecycleActionV2::Block),
        6 => Ok(RunLifecycleActionV2::Transform),
        _ => Err(PluginContractCodecError::InvalidTag),
    }
}

fn capability_scope_tag(value: PluginCapabilityScopeV2) -> u8 {
    match value {
        PluginCapabilityScopeV2::HttpHost => 1,
        PluginCapabilityScopeV2::SecretLease => 2,
        PluginCapabilityScopeV2::StoragePrefix => 3,
        PluginCapabilityScopeV2::Channel => 4,
        PluginCapabilityScopeV2::HarnessCallback => 5,
    }
}

fn decode_capability_scope(value: u8) -> Result<PluginCapabilityScopeV2, PluginContractCodecError> {
    match value {
        1 => Ok(PluginCapabilityScopeV2::HttpHost),
        2 => Ok(PluginCapabilityScopeV2::SecretLease),
        3 => Ok(PluginCapabilityScopeV2::StoragePrefix),
        4 => Ok(PluginCapabilityScopeV2::Channel),
        5 => Ok(PluginCapabilityScopeV2::HarnessCallback),
        _ => Err(PluginContractCodecError::InvalidTag),
    }
}

#[cfg(test)]
mod codec_tests {
    use super::{
        AgentHarnessInvocationV2, ContextEngineResultV2, ContextSegmentCandidateV2,
        ModelAuthProviderResultV2, PluginContractCodecError,
    };
    use crate::{
        PluginCapabilityHandleIdV2, PluginCapabilityHandleV2, PluginCapabilityScopeV2,
        PluginRuntimeGenerationV2, PluginSchemaHashV2,
    };

    fn hash(value: char) -> PluginSchemaHashV2 {
        PluginSchemaHashV2::parse("fixture_hash", value.to_string().repeat(64))
            .expect("fixture hash is canonical")
    }

    #[test]
    fn contract_core_codecs_round_trip_nested_and_opaque_values() {
        let context = ContextEngineResultV2 {
            candidates: vec![ContextSegmentCandidateV2 {
                segment_ref: "segment-1".to_owned(),
                content_hash: hash('a'),
                relevance_millis: 900,
                estimated_tokens: 42,
            }],
        };
        let encoded = context.encode_core_bytes().expect("context should encode");
        assert_eq!(
            ContextEngineResultV2::decode_core_bytes(&encoded).expect("context should decode"),
            context
        );

        let handle = PluginCapabilityHandleV2::new(
            PluginCapabilityHandleIdV2::new("opaque-auth-1").expect("handle id is valid"),
            PluginCapabilityScopeV2::SecretLease,
            hash('b'),
            PluginRuntimeGenerationV2::new(2).expect("generation is nonzero"),
            10_000,
        )
        .expect("handle lifetime is valid");
        let auth = ModelAuthProviderResultV2 { credential_handle: handle };
        let encoded = auth.encode_core_bytes().expect("auth should encode");
        assert_eq!(
            ModelAuthProviderResultV2::decode_core_bytes(&encoded).expect("auth should decode"),
            auth
        );
    }

    #[test]
    fn contract_core_codec_rejects_trailing_bytes() {
        let input = AgentHarnessInvocationV2 {
            prepared_attempt_ref: "attempt-1".to_owned(),
            objective_hash: hash('c'),
            max_steps: 4,
        };
        let mut encoded = input.encode_core_bytes().expect("input should encode");
        encoded.push(0);
        assert_eq!(
            AgentHarnessInvocationV2::decode_core_bytes(&encoded),
            Err(PluginContractCodecError::TrailingBytes)
        );
    }
}
