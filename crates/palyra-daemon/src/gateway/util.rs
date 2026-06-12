//! Small shared gateway helpers: clock reads, canonical-id and identifier
//! validation, session-id redaction, constant-time comparison, and pairing
//! command parsing. Re-exported through `crate::gateway` for all transports.

use super::*;

/// Returns the current unix time in milliseconds, or 0 when the system clock
/// reads before the epoch (infallible variant for log/journal timestamps).
pub(crate) fn current_unix_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64
}

/// Redacts a session identifier for logs and console payloads, keeping only
/// the first and last four characters (`abcd***wxyz`); ids of 8 characters
/// or fewer collapse entirely to `***`.
pub(crate) fn redact_session_id(session_id: &str) -> String {
    let char_count = session_id.chars().count();
    if char_count <= 8 {
        return "***".to_owned();
    }
    let prefix = session_id.chars().take(4).collect::<String>();
    let suffix =
        session_id.chars().rev().take(4).collect::<Vec<_>>().into_iter().rev().collect::<String>();
    format!("{prefix}***{suffix}")
}

/// Stable snake_case label for a stream status kind. These strings land in
/// journal payloads and fixtures, so they are wire contract.
pub(crate) const fn status_kind_name(kind: common_v1::stream_status::StatusKind) -> &'static str {
    match kind {
        common_v1::stream_status::StatusKind::Unspecified => "unspecified",
        common_v1::stream_status::StatusKind::Accepted => "accepted",
        common_v1::stream_status::StatusKind::InProgress => "in_progress",
        common_v1::stream_status::StatusKind::Done => "done",
        common_v1::stream_status::StatusKind::Failed => "failed",
    }
}

/// Returns the current unix time in milliseconds, saturating at `i64::MAX`.
///
/// # Errors
/// Returns `Status::internal` when the system clock reads before the epoch.
#[allow(clippy::result_large_err)]
pub(crate) fn unix_ms_now_for_status() -> Result<i64, Status> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| Status::internal(format!("failed to read system clock: {error}")))?;
    Ok(i64::try_from(now.as_millis()).unwrap_or(i64::MAX))
}

/// Extracts and validates a required canonical ULID from a proto id field.
///
/// # Errors
/// Returns `Status::invalid_argument` naming `field_name` when the id is
/// missing, empty, or not a canonical ULID.
#[allow(clippy::result_large_err)]
pub(crate) fn canonical_id(
    value: Option<common_v1::CanonicalId>,
    field_name: &'static str,
) -> Result<String, Status> {
    let id = value
        .and_then(|id| non_empty(id.ulid))
        .ok_or_else(|| Status::invalid_argument(format!("{field_name} is required")))?;
    validate_canonical_id(id.as_str())
        .map_err(|_| Status::invalid_argument(format!("{field_name} must be a canonical ULID")))?;
    Ok(id)
}

/// Validates an optional canonical ULID proto field: `None` stays `None`, but
/// a present wrapper must carry a valid id (an empty wrapper is rejected
/// rather than treated as absent, surfacing client encoding bugs).
///
/// # Errors
/// Returns `Status::invalid_argument` naming `field_name` when a present id
/// is empty or not a canonical ULID.
#[allow(clippy::result_large_err)]
pub(crate) fn optional_canonical_id(
    value: Option<common_v1::CanonicalId>,
    field_name: &'static str,
) -> Result<Option<String>, Status> {
    let Some(value) = value else {
        return Ok(None);
    };
    let id = non_empty(value.ulid)
        .ok_or_else(|| Status::invalid_argument(format!("{field_name} must be non-empty")))?;
    validate_canonical_id(id.as_str())
        .map_err(|_| Status::invalid_argument(format!("{field_name} must be a canonical ULID")))?;
    Ok(Some(id))
}

/// Normalizes an agent identifier: trims, enforces 1..=64 bytes drawn from
/// ASCII alphanumerics plus `-`, `_`, `.`, and lowercases the result so agent
/// ids compare case-insensitively everywhere.
///
/// # Errors
/// Returns `Status::invalid_argument` naming `field_name` when the value is
/// empty, too long, or contains an unsupported character.
#[allow(clippy::result_large_err)]
pub(crate) fn normalize_agent_identifier(
    raw: &str,
    field_name: &'static str,
) -> Result<String, Status> {
    let value = raw.trim();
    if value.is_empty() {
        return Err(Status::invalid_argument(format!("{field_name} cannot be empty")));
    }
    if value.len() > 64 {
        return Err(Status::invalid_argument(format!("{field_name} cannot exceed 64 bytes")));
    }
    for character in value.chars() {
        if !(character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.')) {
            return Err(Status::invalid_argument(format!(
                "{field_name} contains unsupported character '{character}'"
            )));
        }
    }
    Ok(value.to_ascii_lowercase())
}

/// Compares two byte slices without short-circuiting on the first mismatch,
/// for token/signature checks where a data-dependent early return would leak
/// how much of the secret matched.
pub(crate) fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    // Always scan to the longer length, padding the shorter side with zeros,
    // and fold the length mismatch into the accumulator so unequal lengths
    // cost the same time as unequal bytes.
    let max_len = left.len().max(right.len());
    let mut diff = left.len() ^ right.len();
    for index in 0..max_len {
        let lhs = left.get(index).copied().unwrap_or_default();
        let rhs = right.get(index).copied().unwrap_or_default();
        diff |= usize::from(lhs ^ rhs);
    }
    diff == 0
}

/// Extracts the code from a `pair <code>` chat command (case-insensitive
/// keyword); returns `None` for any other message. Tokens after the code are
/// ignored so trailing chatter does not invalidate the command.
pub(crate) fn extract_pairing_code_command(raw: &str) -> Option<String> {
    let mut parts = raw.split_whitespace();
    let command = parts.next()?.trim().to_ascii_lowercase();
    if command != "pair" {
        return None;
    }
    let code = parts.next()?.trim();
    if code.is_empty() {
        return None;
    }
    Some(code.to_owned())
}

/// Returns `None` for empty or whitespace-only strings. Note the original
/// string is returned untrimmed - trimming for the check only, so callers
/// keep whatever surrounding whitespace the value legitimately carries.
pub(crate) fn non_empty(input: String) -> Option<String> {
    if input.trim().is_empty() {
        None
    } else {
        Some(input)
    }
}

#[cfg(test)]
mod tests {
    use super::redact_session_id;

    #[test]
    fn redact_session_id_preserves_ascii_shape() {
        assert_eq!(redact_session_id("01HABCDEFGHJKLMNPQRS"), "01HA***PQRS");
        assert_eq!(redact_session_id("12345678"), "***");
    }

    #[test]
    fn redact_session_id_handles_multibyte_input() {
        assert_eq!(redact_session_id("áβçďEFGHíjkl"), "áβçď***íjkl");
        assert_eq!(redact_session_id("áβçďEFGH"), "***");
    }
}
