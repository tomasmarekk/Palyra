//! Cross-field validation and domain-separated hashing for metadata traces.
//!
//! Keeping validation separate preserves a small wire-type module while every
//! deserialized trace still crosses the same fail-closed invariant checks.

use std::collections::BTreeSet;

use sha2::{Digest, Sha256};

use super::*;

impl MetadataTraceV1 {
    /// Validates schema identity, hard caps, segment continuity, and causal ordering.
    ///
    /// # Errors
    /// Returns [`MetadataTraceValidationError`] when any field or cross-segment
    /// invariant is invalid.
    pub fn validate_shape(&self) -> Result<(), MetadataTraceValidationError> {
        if self.schema_version != METADATA_TRACE_SCHEMA_VERSION {
            return Err(validation_error(
                "metadata_trace_schema_version_unsupported",
                "$.schema_version",
                format!("expected schema version {METADATA_TRACE_SCHEMA_VERSION}"),
            ));
        }
        validate_sha256(self.run_id_sha256.as_str(), "$.run_id_sha256")?;
        validate_sha256(self.session_id_sha256.as_str(), "$.session_id_sha256")?;
        if self.run_id_sha256 == self.session_id_sha256 {
            return Err(validation_error(
                "metadata_trace_identity_domain_collision",
                "$.session_id_sha256",
                "run and session digests must use distinct identity domains",
            ));
        }
        if self.segments.is_empty() || self.segments.len() > METADATA_TRACE_MAX_SEGMENTS {
            return Err(validation_error(
                "metadata_trace_segment_count_invalid",
                "$.segments",
                format!("segment count must be 1..={METADATA_TRACE_MAX_SEGMENTS}"),
            ));
        }

        let mut expected_sequence = 0_u32;
        let mut expected_generation = 1_u32;
        let mut event_count = 0_usize;
        let mut segment_ids = BTreeSet::new();
        let mut event_ids = BTreeSet::new();
        let mut previous_segment_id: Option<&str> = None;
        let mut previous_event_id: Option<&str> = None;

        for (segment_index, segment) in self.segments.iter().enumerate() {
            let path = format!("$.segments[{segment_index}]");
            segment.validate_shape_at(path.as_str())?;

            let expected_index = u16::try_from(segment_index).map_err(|_| {
                validation_error(
                    "metadata_trace_segment_index_invalid",
                    format!("{path}.segment_index"),
                    "segment index exceeds the supported integer range",
                )
            })?;
            if segment.segment_index != expected_index {
                return Err(validation_error(
                    "metadata_trace_segment_index_non_contiguous",
                    format!("{path}.segment_index"),
                    format!("expected segment index {expected_index}"),
                ));
            }
            if segment.generation != expected_generation {
                return Err(validation_error(
                    "metadata_trace_generation_non_contiguous",
                    format!("{path}.generation"),
                    format!("expected generation {expected_generation}"),
                ));
            }
            expected_generation = expected_generation.checked_add(1).ok_or_else(|| {
                validation_error(
                    "metadata_trace_generation_overflow",
                    format!("{path}.generation"),
                    "generation counter overflowed",
                )
            })?;
            if !segment_ids.insert(segment.segment_id_sha256.as_str()) {
                return Err(validation_error(
                    "metadata_trace_segment_id_duplicate",
                    format!("{path}.segment_id_sha256"),
                    "segment identifiers must be unique",
                ));
            }
            if segment.segment_id_sha256 == self.run_id_sha256
                || segment.segment_id_sha256 == self.session_id_sha256
            {
                return Err(validation_error(
                    "metadata_trace_identity_domain_collision",
                    format!("{path}.segment_id_sha256"),
                    "segment digest must use its own identity domain",
                ));
            }

            let first_event = segment.events.first().ok_or_else(|| {
                validation_error(
                    "metadata_trace_segment_empty",
                    format!("{path}.events"),
                    "segments must contain at least one event",
                )
            })?;
            if segment_index == 0 {
                if !matches!(first_event.event, MetadataTraceEventDataV1::RunStarted(_)) {
                    return Err(validation_error(
                        "metadata_trace_root_event_missing",
                        format!("{path}.events[0].event"),
                        "the first trace event must be run_started",
                    ));
                }
                if first_event.causal_parent_event_id_sha256.is_some() {
                    return Err(validation_error(
                        "metadata_trace_root_causal_parent_forbidden",
                        format!("{path}.events[0].causal_parent_event_id_sha256"),
                        "the root event cannot have a causal parent",
                    ));
                }
            } else {
                let MetadataTraceEventDataV1::RecoveryContinuation(continuation) =
                    &first_event.event
                else {
                    return Err(validation_error(
                        "metadata_trace_continuation_event_missing",
                        format!("{path}.events[0].event"),
                        "continuation segments must begin with recovery_continuation",
                    ));
                };
                if continuation.previous_segment_id_sha256 != previous_segment_id.unwrap_or("") {
                    return Err(validation_error(
                        "metadata_trace_continuation_segment_mismatch",
                        format!("{path}.events[0].event.metadata.previous_segment_id_sha256"),
                        "continuation must reference the immediately preceding segment",
                    ));
                }
                if first_event.causal_parent_event_id_sha256.as_deref() != previous_event_id {
                    return Err(validation_error(
                        "metadata_trace_continuation_parent_mismatch",
                        format!("{path}.events[0].causal_parent_event_id_sha256"),
                        "continuation must descend from the preceding segment's final event",
                    ));
                }
            }

            if segment_index + 1 < self.segments.len()
                && segment.status == MetadataTraceSegmentStatusV1::Complete
            {
                return Err(validation_error(
                    "metadata_trace_completed_segment_not_final",
                    format!("{path}.status"),
                    "only the final segment may be complete",
                ));
            }

            for (event_index, event) in segment.events.iter().enumerate() {
                let event_path = format!("{path}.events[{event_index}]");
                if event.sequence != expected_sequence {
                    return Err(validation_error(
                        "metadata_trace_event_sequence_non_contiguous",
                        format!("{event_path}.sequence"),
                        format!("expected sequence {expected_sequence}"),
                    ));
                }
                expected_sequence = expected_sequence.checked_add(1).ok_or_else(|| {
                    validation_error(
                        "metadata_trace_event_sequence_overflow",
                        format!("{event_path}.sequence"),
                        "event sequence overflowed",
                    )
                })?;
                event_count = event_count.checked_add(1).ok_or_else(|| {
                    validation_error(
                        "metadata_trace_event_count_overflow",
                        format!("{event_path}.sequence"),
                        "event count overflowed",
                    )
                })?;
                if event_count > METADATA_TRACE_MAX_EVENTS {
                    return Err(validation_error(
                        "metadata_trace_event_count_exceeded",
                        "$.segments",
                        format!(
                            "trace must not contain more than {METADATA_TRACE_MAX_EVENTS} events"
                        ),
                    ));
                }
                if event.event_id_sha256 == self.run_id_sha256
                    || event.event_id_sha256 == self.session_id_sha256
                    || segment_ids.contains(event.event_id_sha256.as_str())
                {
                    return Err(validation_error(
                        "metadata_trace_identity_domain_collision",
                        format!("{event_path}.event_id_sha256"),
                        "event digest must use its own identity domain",
                    ));
                }
                if !event_ids.insert(event.event_id_sha256.as_str()) {
                    return Err(validation_error(
                        "metadata_trace_event_id_duplicate",
                        format!("{event_path}.event_id_sha256"),
                        "event identifiers must be unique",
                    ));
                }
                if event.sequence > 0 {
                    let parent =
                        event.causal_parent_event_id_sha256.as_deref().ok_or_else(|| {
                            validation_error(
                                "metadata_trace_causal_parent_missing",
                                format!("{event_path}.causal_parent_event_id_sha256"),
                                "every non-root event requires a causal parent",
                            )
                        })?;
                    if !event_ids.contains(parent) {
                        return Err(validation_error(
                            "metadata_trace_causal_parent_unknown",
                            format!("{event_path}.causal_parent_event_id_sha256"),
                            "causal parent must reference an earlier event",
                        ));
                    }
                }
                if event_index > 0 && matches!(event.event, MetadataTraceEventDataV1::RunStarted(_))
                {
                    return Err(validation_error(
                        "metadata_trace_root_event_repeated",
                        format!("{event_path}.event"),
                        "run_started is allowed only as the root event",
                    ));
                }
                if event_index > 0
                    && matches!(event.event, MetadataTraceEventDataV1::RecoveryContinuation(_))
                {
                    return Err(validation_error(
                        "metadata_trace_continuation_event_misplaced",
                        format!("{event_path}.event"),
                        "recovery_continuation is allowed only at a segment boundary",
                    ));
                }
                previous_event_id = Some(event.event_id_sha256.as_str());
            }
            previous_segment_id = Some(segment.segment_id_sha256.as_str());
        }
        Ok(())
    }
}

impl MetadataTraceSegmentV1 {
    /// Validates segment identity, event caps, generation binding, and terminal shape.
    ///
    /// # Errors
    /// Returns [`MetadataTraceValidationError`] when the segment is malformed.
    pub fn validate_shape(&self) -> Result<(), MetadataTraceValidationError> {
        self.validate_shape_at("$")
    }

    fn validate_shape_at(&self, path: &str) -> Result<(), MetadataTraceValidationError> {
        validate_sha256(self.segment_id_sha256.as_str(), format!("{path}.segment_id_sha256"))?;
        if self.generation == 0
            || usize::try_from(self.generation)
                .map_or(true, |generation| generation > METADATA_TRACE_MAX_SEGMENTS)
        {
            return Err(validation_error(
                "metadata_trace_generation_invalid",
                format!("{path}.generation"),
                format!("generation must be 1..={METADATA_TRACE_MAX_SEGMENTS}"),
            ));
        }
        if self.events.is_empty() || self.events.len() > METADATA_TRACE_MAX_EVENTS {
            return Err(validation_error(
                "metadata_trace_segment_event_count_invalid",
                format!("{path}.events"),
                format!("segment event count must be 1..={METADATA_TRACE_MAX_EVENTS}"),
            ));
        }

        let first_sequence = self.events.first().map_or(0, |event| event.sequence);
        for (index, event) in self.events.iter().enumerate() {
            let event_path = format!("{path}.events[{index}]");
            event.validate_shape_at(event_path.as_str())?;
            if event.generation != self.generation {
                return Err(validation_error(
                    "metadata_trace_event_generation_mismatch",
                    format!("{event_path}.generation"),
                    "event generation must match its segment",
                ));
            }
            let offset = u32::try_from(index).map_err(|_| {
                validation_error(
                    "metadata_trace_event_sequence_invalid",
                    format!("{event_path}.sequence"),
                    "event index exceeds the supported integer range",
                )
            })?;
            let expected_sequence = first_sequence.checked_add(offset).ok_or_else(|| {
                validation_error(
                    "metadata_trace_event_sequence_overflow",
                    format!("{event_path}.sequence"),
                    "event sequence overflowed",
                )
            })?;
            if event.sequence != expected_sequence {
                return Err(validation_error(
                    "metadata_trace_segment_sequence_non_contiguous",
                    format!("{event_path}.sequence"),
                    format!("expected sequence {expected_sequence}"),
                ));
            }
        }

        let terminal_count = self
            .events
            .iter()
            .filter(|event| matches!(event.event, MetadataTraceEventDataV1::Terminalization(_)))
            .count();
        let ends_terminal = self.events.last().is_some_and(|event| {
            matches!(event.event, MetadataTraceEventDataV1::Terminalization(_))
        });
        match self.status {
            MetadataTraceSegmentStatusV1::Complete if terminal_count == 1 && ends_terminal => {}
            MetadataTraceSegmentStatusV1::Complete => {
                return Err(validation_error(
                    "metadata_trace_complete_terminal_invalid",
                    format!("{path}.events"),
                    "complete segments require exactly one final terminalization event",
                ));
            }
            MetadataTraceSegmentStatusV1::Interrupted
            | MetadataTraceSegmentStatusV1::CorruptSuffixIsolated
                if terminal_count == 0 => {}
            MetadataTraceSegmentStatusV1::Interrupted
            | MetadataTraceSegmentStatusV1::CorruptSuffixIsolated => {
                return Err(validation_error(
                    "metadata_trace_noncomplete_terminal_forbidden",
                    format!("{path}.events"),
                    "non-complete segments cannot contain terminalization",
                ));
            }
        }
        Ok(())
    }
}

impl MetadataTraceEventV1 {
    /// Returns the stable kind of this event.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        self.event.kind()
    }

    /// Validates identity hashes, hard caps, and the closed event payload.
    ///
    /// # Errors
    /// Returns [`MetadataTraceValidationError`] when the event is malformed.
    pub fn validate_shape(&self) -> Result<(), MetadataTraceValidationError> {
        self.validate_shape_at("$")
    }

    fn validate_shape_at(&self, path: &str) -> Result<(), MetadataTraceValidationError> {
        if usize::try_from(self.sequence)
            .map_or(true, |sequence| sequence >= METADATA_TRACE_MAX_EVENTS)
        {
            return Err(validation_error(
                "metadata_trace_event_sequence_invalid",
                format!("{path}.sequence"),
                format!("sequence must be less than {METADATA_TRACE_MAX_EVENTS}"),
            ));
        }
        if self.generation == 0
            || usize::try_from(self.generation)
                .map_or(true, |generation| generation > METADATA_TRACE_MAX_SEGMENTS)
        {
            return Err(validation_error(
                "metadata_trace_generation_invalid",
                format!("{path}.generation"),
                format!("generation must be 1..={METADATA_TRACE_MAX_SEGMENTS}"),
            ));
        }
        if self.recorded_at_unix_ms > METADATA_TRACE_MAX_UNIX_MS {
            return Err(validation_error(
                "metadata_trace_timestamp_invalid",
                format!("{path}.recorded_at_unix_ms"),
                format!("timestamp must not exceed {METADATA_TRACE_MAX_UNIX_MS}"),
            ));
        }
        validate_sha256(self.event_id_sha256.as_str(), format!("{path}.event_id_sha256"))?;
        if let Some(parent) = self.causal_parent_event_id_sha256.as_deref() {
            validate_sha256(parent, format!("{path}.causal_parent_event_id_sha256"))?;
            if parent == self.event_id_sha256 {
                return Err(validation_error(
                    "metadata_trace_event_self_parent",
                    format!("{path}.causal_parent_event_id_sha256"),
                    "an event cannot be its own causal parent",
                ));
            }
        }
        if self
            .stage_duration_ms
            .is_some_and(|duration| duration > METADATA_TRACE_MAX_STAGE_DURATION_MS)
        {
            return Err(validation_error(
                "metadata_trace_stage_duration_exceeded",
                format!("{path}.stage_duration_ms"),
                format!("stage duration must not exceed {METADATA_TRACE_MAX_STAGE_DURATION_MS}"),
            ));
        }
        self.event.validate_shape_at(format!("{path}.event").as_str())?;

        let serialized = serde_json::to_vec(self).map_err(|_| {
            validation_error(
                "metadata_trace_event_serialization_failed",
                path,
                "event could not be serialized canonically",
            )
        })?;
        if serialized.len() > METADATA_TRACE_MAX_EVENT_BYTES {
            return Err(validation_error(
                "metadata_trace_event_bytes_exceeded",
                path,
                format!("event must not exceed {METADATA_TRACE_MAX_EVENT_BYTES} JSON bytes"),
            ));
        }
        Ok(())
    }
}

impl MetadataTraceEventDataV1 {
    fn validate_shape_at(&self, path: &str) -> Result<(), MetadataTraceValidationError> {
        match self {
            Self::RunStarted(_) => Ok(()),
            Self::RuntimeSelected(metadata) => metadata.validate_shape_at(path),
            Self::ContextAssembled(metadata) => metadata.validate_shape_at(path),
            Self::ProviderAttempt(metadata) => metadata.validate_shape_at(path),
            Self::ToolGate(metadata) => {
                validate_sha256(
                    metadata.tool_id_sha256.as_str(),
                    format!("{path}.metadata.tool_id_sha256"),
                )?;
                validate_reason_code(
                    metadata.reason_code.as_str(),
                    format!("{path}.metadata.reason_code"),
                )
            }
            Self::Approval(metadata) => {
                validate_sha256(
                    metadata.approval_id_sha256.as_str(),
                    format!("{path}.metadata.approval_id_sha256"),
                )?;
                validate_reason_code(
                    metadata.reason_code.as_str(),
                    format!("{path}.metadata.reason_code"),
                )
            }
            Self::ToolOutcome(metadata) => {
                validate_sha256(
                    metadata.tool_id_sha256.as_str(),
                    format!("{path}.metadata.tool_id_sha256"),
                )?;
                validate_attempt(metadata.attempt, format!("{path}.metadata.attempt"))?;
                validate_reason_code(
                    metadata.reason_code.as_str(),
                    format!("{path}.metadata.reason_code"),
                )
            }
            Self::Recovery(metadata) => {
                validate_attempt(metadata.attempt, format!("{path}.metadata.attempt"))?;
                validate_reason_code(
                    metadata.reason_code.as_str(),
                    format!("{path}.metadata.reason_code"),
                )
            }
            Self::DeliveryIntent(metadata) => {
                validate_sha256(
                    metadata.delivery_id_sha256.as_str(),
                    format!("{path}.metadata.delivery_id_sha256"),
                )?;
                validate_reason_code(
                    metadata.reason_code.as_str(),
                    format!("{path}.metadata.reason_code"),
                )
            }
            Self::Terminalization(metadata) => validate_reason_code(
                metadata.reason_code.as_str(),
                format!("{path}.metadata.reason_code"),
            ),
            Self::RecoveryContinuation(metadata) => {
                validate_sha256(
                    metadata.previous_segment_id_sha256.as_str(),
                    format!("{path}.metadata.previous_segment_id_sha256"),
                )?;
                validate_reason_code(
                    metadata.reason_code.as_str(),
                    format!("{path}.metadata.reason_code"),
                )
            }
            Self::CapacityReached(metadata) => metadata.validate_shape_at(path),
        }
    }
}

impl RuntimeSelectedMetadataV1 {
    fn validate_shape_at(&self, path: &str) -> Result<(), MetadataTraceValidationError> {
        validate_machine_identifier(
            self.harness_id.as_str(),
            format!("{path}.metadata.harness_id"),
        )?;
        validate_version(
            self.harness_version.as_str(),
            format!("{path}.metadata.harness_version"),
        )?;
        validate_machine_identifier(
            self.runtime_id.as_str(),
            format!("{path}.metadata.runtime_id"),
        )?;
        validate_version(
            self.runtime_version.as_str(),
            format!("{path}.metadata.runtime_version"),
        )?;
        if let Some(auth_profile) = self.auth_profile_id_sha256.as_deref() {
            validate_sha256(auth_profile, format!("{path}.metadata.auth_profile_id_sha256"))?;
        }
        if self.schema_hashes.is_empty()
            || self.schema_hashes.len() > METADATA_TRACE_MAX_SCHEMA_HASHES
        {
            return Err(validation_error(
                "metadata_trace_schema_hash_count_invalid",
                format!("{path}.metadata.schema_hashes"),
                format!("schema hash count must be 1..={METADATA_TRACE_MAX_SCHEMA_HASHES}"),
            ));
        }
        let mut previous_schema_id: Option<&str> = None;
        for (index, schema) in self.schema_hashes.iter().enumerate() {
            let schema_path = format!("{path}.metadata.schema_hashes[{index}]");
            validate_machine_identifier(
                schema.schema_id.as_str(),
                format!("{schema_path}.schema_id"),
            )?;
            validate_sha256(schema.sha256.as_str(), format!("{schema_path}.sha256"))?;
            if previous_schema_id.is_some_and(|previous| previous >= schema.schema_id.as_str()) {
                return Err(validation_error(
                    "metadata_trace_schema_hashes_not_ordered",
                    format!("{schema_path}.schema_id"),
                    "schema hashes must be strictly ordered by schema_id",
                ));
            }
            previous_schema_id = Some(schema.schema_id.as_str());
        }
        Ok(())
    }
}

impl ContextAssembledMetadataV1 {
    fn validate_shape_at(&self, path: &str) -> Result<(), MetadataTraceValidationError> {
        validate_machine_identifier(
            self.context_engine_id.as_str(),
            format!("{path}.metadata.context_engine_id"),
        )?;
        validate_version(
            self.context_engine_version.as_str(),
            format!("{path}.metadata.context_engine_version"),
        )?;
        validate_sha256(
            self.context_schema_sha256.as_str(),
            format!("{path}.metadata.context_schema_sha256"),
        )?;
        if self.input_item_count > METADATA_TRACE_MAX_CONTEXT_ITEMS
            || self.retained_item_count > self.input_item_count
        {
            return Err(validation_error(
                "metadata_trace_context_count_invalid",
                format!("{path}.metadata.retained_item_count"),
                format!(
                    "context counts must be ordered and no greater than {METADATA_TRACE_MAX_CONTEXT_ITEMS}"
                ),
            ));
        }
        Ok(())
    }
}

impl ProviderAttemptMetadataV1 {
    fn validate_shape_at(&self, path: &str) -> Result<(), MetadataTraceValidationError> {
        validate_sha256(
            self.provider_id_sha256.as_str(),
            format!("{path}.metadata.provider_id_sha256"),
        )?;
        validate_sha256(self.model_id_sha256.as_str(), format!("{path}.metadata.model_id_sha256"))?;
        if let Some(auth_profile) = self.auth_profile_id_sha256.as_deref() {
            validate_sha256(auth_profile, format!("{path}.metadata.auth_profile_id_sha256"))?;
        }
        validate_attempt(self.attempt, format!("{path}.metadata.attempt"))?;
        validate_reason_code(self.reason_code.as_str(), format!("{path}.metadata.reason_code"))
    }
}

impl CapacityReachedMetadataV1 {
    fn validate_shape_at(&self, path: &str) -> Result<(), MetadataTraceValidationError> {
        let expected_limit = match self.limit_kind {
            MetadataTraceCapacityLimitV1::EventCount => METADATA_TRACE_MAX_EVENTS,
            MetadataTraceCapacityLimitV1::SegmentCount => METADATA_TRACE_MAX_SEGMENTS,
            MetadataTraceCapacityLimitV1::EventBytes => METADATA_TRACE_MAX_EVENT_BYTES,
            MetadataTraceCapacityLimitV1::SchemaHashCount => METADATA_TRACE_MAX_SCHEMA_HASHES,
            MetadataTraceCapacityLimitV1::StageDuration => {
                usize::try_from(METADATA_TRACE_MAX_STAGE_DURATION_MS).map_err(|_| {
                    validation_error(
                        "metadata_trace_capacity_limit_invalid",
                        format!("{path}.metadata.limit"),
                        "stage-duration limit exceeds the supported integer range",
                    )
                })?
            }
        };
        let expected_limit = u32::try_from(expected_limit).map_err(|_| {
            validation_error(
                "metadata_trace_capacity_limit_invalid",
                format!("{path}.metadata.limit"),
                "contract limit exceeds the supported integer range",
            )
        })?;
        if self.limit != expected_limit
            || self.observed < self.limit
            || self.observed > MAX_CAPACITY_OBSERVED
        {
            return Err(validation_error(
                "metadata_trace_capacity_value_invalid",
                format!("{path}.metadata.observed"),
                "capacity metadata must use the exact contract limit and a bounded observed value",
            ));
        }
        validate_reason_code(self.reason_code.as_str(), format!("{path}.metadata.reason_code"))
    }
}

/// Computes a domain-separated SHA-256 digest for a trace identity.
///
/// The source identifier is never returned or persisted by this helper.
///
/// # Errors
/// Returns [`MetadataTraceValidationError`] when the source is empty or exceeds
/// [`METADATA_TRACE_MAX_ID_SOURCE_BYTES`].
pub fn metadata_trace_id_sha256(
    domain: MetadataTraceIdDomainV1,
    source_identifier: &str,
) -> Result<String, MetadataTraceValidationError> {
    if source_identifier.is_empty() || source_identifier.len() > METADATA_TRACE_MAX_ID_SOURCE_BYTES
    {
        return Err(validation_error(
            "metadata_trace_identity_source_invalid",
            "$.source_identifier",
            format!(
                "identity source must contain 1..={METADATA_TRACE_MAX_ID_SOURCE_BYTES} UTF-8 bytes"
            ),
        ));
    }
    let source_len = u64::try_from(source_identifier.len()).map_err(|_| {
        validation_error(
            "metadata_trace_identity_source_invalid",
            "$.source_identifier",
            "identity source length exceeds the supported integer range",
        )
    })?;
    let mut hasher = Sha256::new();
    hasher.update(b"palyra.metadata_trace.id.v1\0");
    hasher.update(domain.as_str().as_bytes());
    hasher.update([0]);
    hasher.update(source_len.to_be_bytes());
    hasher.update(source_identifier.as_bytes());
    Ok(hex::encode(hasher.finalize()))
}

fn validate_sha256(
    value: &str,
    path: impl Into<String>,
) -> Result<(), MetadataTraceValidationError> {
    if value.len() == 64
        && value.bytes().all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Ok(());
    }
    Err(validation_error(
        "metadata_trace_sha256_invalid",
        path,
        "expected exactly 64 lowercase hexadecimal characters",
    ))
}

fn validate_machine_identifier(
    value: &str,
    path: impl Into<String>,
) -> Result<(), MetadataTraceValidationError> {
    let bytes = value.as_bytes();
    let valid = !value.is_empty()
        && value.len() <= MAX_MACHINE_IDENTIFIER_BYTES
        && bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && !value.contains("..")
        && !looks_like_secret(value)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));
    if valid {
        return Ok(());
    }
    Err(validation_error(
        "metadata_trace_machine_identifier_invalid",
        path,
        format!("machine identifier must be 1..={MAX_MACHINE_IDENTIFIER_BYTES} safe ASCII bytes"),
    ))
}

fn validate_version(
    value: &str,
    path: impl Into<String>,
) -> Result<(), MetadataTraceValidationError> {
    let bytes = value.as_bytes();
    let valid = !value.is_empty()
        && value.len() <= MAX_MACHINE_IDENTIFIER_BYTES
        && bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && !value.contains("..")
        && !looks_like_secret(value)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'+'));
    if valid {
        return Ok(());
    }
    Err(validation_error(
        "metadata_trace_version_invalid",
        path,
        format!("version must be 1..={MAX_MACHINE_IDENTIFIER_BYTES} safe ASCII bytes"),
    ))
}

fn looks_like_secret(value: &str) -> bool {
    let normalized = value.to_ascii_lowercase();
    ["sk-", "ghp_", "github_pat_", "xoxb-", "xoxp-", "bearer-", "api-key-", "apikey-"]
        .iter()
        .any(|prefix| normalized.starts_with(prefix))
}

fn validate_reason_code(
    value: &str,
    path: impl Into<String>,
) -> Result<(), MetadataTraceValidationError> {
    let bytes = value.as_bytes();
    let valid = value.len() >= 3
        && value.len() <= MAX_REASON_CODE_BYTES
        && bytes.first().is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        && bytes.last().is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        && !value.contains("..")
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        });
    if valid {
        return Ok(());
    }
    Err(validation_error(
        "metadata_trace_reason_code_invalid",
        path,
        format!("reason code must be 3..={MAX_REASON_CODE_BYTES} safe lowercase ASCII bytes"),
    ))
}

fn validate_attempt(
    attempt: u16,
    path: impl Into<String>,
) -> Result<(), MetadataTraceValidationError> {
    if (1..=METADATA_TRACE_MAX_ATTEMPTS).contains(&attempt) {
        return Ok(());
    }
    Err(validation_error(
        "metadata_trace_attempt_invalid",
        path,
        format!("attempt must be 1..={METADATA_TRACE_MAX_ATTEMPTS}"),
    ))
}

fn validation_error(
    code: &'static str,
    path: impl Into<String>,
    message: impl Into<String>,
) -> MetadataTraceValidationError {
    MetadataTraceValidationError { code, path: path.into(), message: message.into() }
}
