//! Versioned QA fault-injection plans and deterministic checkpoint contracts.
//!
//! Production callers receive [`QaFaultProbeHandle::default`], which is a
//! side-effect-free disabled probe. QA runtimes must explicitly construct and
//! inject a [`DeterministicQaFaultController`]; this module intentionally has
//! no mutable global activation state.

use sha2::{Digest, Sha256};

/// Current fault-injection plan schema version.
pub const QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION: u32 = 1;

/// Stable format label embedded in fault-injection plans.
pub const QA_FAULT_INJECTION_PLAN_FORMAT: &str = "palyra-qa-fault-injection-plan";

/// Reserved process exit code for a controlled fault-injection termination.
///
/// Daemon adapters must use this value and QA runners must classify only this
/// value as an expected injected exit. Other non-zero exits remain failures.
pub const QA_FAULT_TERMINATE_EXIT_CODE: i32 = 86;

/// Current schema for the private runner-to-daemon launch handshake.
pub const QA_FAULT_LAUNCH_SCHEMA_VERSION: u32 = 1;

/// Environment variable containing the private launch-document path.
pub const QA_FAULT_LAUNCH_PATH_ENV: &str = "PALYRA_QA_FAULT_LAUNCH_PATH";

/// Environment variable containing the separate launch capability path.
pub const QA_FAULT_CAPABILITY_PATH_ENV: &str = "PALYRA_QA_FAULT_CAPABILITY_PATH";

/// Prefix required in the capability file before its high-entropy payload.
pub const QA_FAULT_CAPABILITY_PREFIX: &str = "palyra-qa-fault-v1:";

/// Current schema for append-only private fault-evidence sidecar records.
pub const QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION: u32 = 1;

/// Maximum accepted sidecar size before parsing.
pub const QA_FAULT_EVIDENCE_SIDECAR_MAX_BYTES: usize = 1_048_576;

/// Maximum number of records accepted from one sidecar.
pub const QA_FAULT_EVIDENCE_SIDECAR_MAX_RECORDS: usize = 2_048;

/// Maximum encoded size of one NDJSON sidecar record.
pub const QA_FAULT_EVIDENCE_SIDECAR_MAX_RECORD_BYTES: usize = 16_384;

const QA_FAULT_EVIDENCE_STANDARD_RECORD_BUDGET_BYTES: u64 = 2_048;
const QA_FAULT_EVIDENCE_ACTIVATION_RECORD_BUDGET_BYTES: u64 = 8_192;

/// Maximum number of activation entries accepted in one plan.
pub const QA_FAULT_INJECTION_MAX_ACTIVATIONS: usize = 64;

/// Maximum supported occurrence for one checkpoint and actor.
pub const QA_FAULT_INJECTION_MAX_OCCURRENCE: u32 = 1_000_000;

/// Maximum number of actors admitted to one deterministic barrier.
pub const QA_FAULT_INJECTION_MAX_BARRIER_PARTICIPANTS: u16 = 16;

const MAX_IDENTIFIER_BYTES: usize = 96;
const MAX_LOGICAL_TIME_ADVANCE_MS: u64 = 3_600_000;
const MAX_PRIVATE_PATH_BYTES: usize = 4_096;

mod controller;
mod evidence;
mod evidence_validation;
mod launch;
mod plan;

pub use controller::*;
pub use evidence::*;
pub use launch::*;
pub use plan::*;

#[cfg(test)]
mod tests;

fn is_bounded_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_IDENTIFIER_BYTES
        && value.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
}

fn is_bounded_actor(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_IDENTIFIER_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':'))
}

fn update_length_delimited_hash(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(bytes);
}
