//! Cron RPC support: request validation (names, prompts, jitter, ownership,
//! channel context) and journal-record/proto conversion for jobs and runs.
//! Ownership checks here are the authorization boundary for all cron RPCs.

use super::*;

/// Validates and trims a cron job name (non-empty, at most
/// `MAX_CRON_JOB_NAME_BYTES`).
///
/// # Errors
/// Returns `Status::invalid_argument` when the name is empty or too long.
#[allow(clippy::result_large_err)]
pub(crate) fn validate_cron_job_name(name: String) -> Result<String, Status> {
    let value = name.trim();
    if value.is_empty() {
        return Err(Status::invalid_argument("cron job name cannot be empty"));
    }
    if value.len() > MAX_CRON_JOB_NAME_BYTES {
        return Err(Status::invalid_argument(format!(
            "cron job name exceeds maximum bytes ({} > {MAX_CRON_JOB_NAME_BYTES})",
            value.len()
        )));
    }
    Ok(value.to_owned())
}

/// Validates and trims a cron job prompt (non-empty, at most
/// `MAX_CRON_PROMPT_BYTES`).
///
/// # Errors
/// Returns `Status::invalid_argument` when the prompt is empty or too long.
#[allow(clippy::result_large_err)]
pub(crate) fn validate_cron_job_prompt(prompt: String) -> Result<String, Status> {
    let value = prompt.trim();
    if value.is_empty() {
        return Err(Status::invalid_argument("cron job prompt cannot be empty"));
    }
    if value.len() > MAX_CRON_PROMPT_BYTES {
        return Err(Status::invalid_argument(format!(
            "cron job prompt exceeds maximum bytes ({} > {MAX_CRON_PROMPT_BYTES})",
            value.len()
        )));
    }
    Ok(value.to_owned())
}

/// Validates a cron jitter value against `MAX_CRON_JITTER_MS`.
///
/// # Errors
/// Returns `Status::invalid_argument` when the jitter exceeds the maximum.
#[allow(clippy::result_large_err)]
pub(crate) fn validate_cron_jitter_ms(jitter_ms: u64) -> Result<u64, Status> {
    if jitter_ms > MAX_CRON_JITTER_MS {
        return Err(Status::invalid_argument(format!(
            "jitter_ms exceeds maximum ({MAX_CRON_JITTER_MS})"
        )));
    }
    Ok(jitter_ms)
}

/// Resolves the owner principal for cron job creation: an empty request
/// defaults to the authenticated principal, and a non-empty request must
/// match it - a caller can never create a job owned by someone else.
///
/// # Errors
/// Returns `Status::permission_denied` when the requested owner differs from
/// the authenticated principal.
#[allow(clippy::result_large_err)]
pub(crate) fn validate_cron_job_owner_principal(
    authenticated_principal: &str,
    requested_owner_principal: String,
) -> Result<String, Status> {
    match non_empty(requested_owner_principal) {
        Some(owner_principal) if owner_principal == authenticated_principal => Ok(owner_principal),
        Some(_) => {
            Err(Status::permission_denied("owner_principal must match authenticated principal"))
        }
        None => Ok(authenticated_principal.to_owned()),
    }
}

/// Validates the owner principal on cron job updates. Unlike creation, an
/// empty owner is rejected here: updates address an existing job, so a
/// missing owner indicates a malformed request rather than "default to me".
///
/// # Errors
/// Returns `Status::invalid_argument` for an empty owner and
/// `Status::permission_denied` when it differs from the authenticated
/// principal.
#[allow(clippy::result_large_err)]
pub(crate) fn validate_cron_job_owner_principal_for_update(
    authenticated_principal: &str,
    requested_owner_principal: String,
) -> Result<String, Status> {
    let owner_principal = non_empty(requested_owner_principal)
        .ok_or_else(|| Status::invalid_argument("owner_principal cannot be empty"))?;
    if owner_principal != authenticated_principal {
        return Err(Status::permission_denied(
            "owner_principal must match authenticated principal",
        ));
    }
    Ok(owner_principal)
}

/// Checks that a requested cron delivery channel is permitted for the
/// caller's authenticated channel context. Passes when either side is absent
/// (no channel to pin against, or nothing requested).
///
/// # Errors
/// Returns `Status::permission_denied` when the requested channel neither
/// matches the context channel nor is the internal cron channel.
#[allow(clippy::result_large_err)]
pub(crate) fn validate_cron_job_channel_context(
    context_channel: Option<&str>,
    requested_channel: Option<&str>,
) -> Result<(), Status> {
    let Some(requested_channel) = requested_channel else {
        return Ok(());
    };
    let Some(context_channel) = context_channel else {
        return Ok(());
    };
    // "system:cron" is the daemon-internal delivery channel; any caller may
    // target it because output stays inside the daemon instead of being sent
    // to a channel the caller is not bound to.
    if context_channel != requested_channel && requested_channel != "system:cron" {
        return Err(Status::permission_denied(
            "cron channel must match authenticated channel context",
        ));
    }
    Ok(())
}

/// Resolves the delivery channel for a new cron job: the validated requested
/// channel when present, else the caller's context channel, else the
/// internal cron channel.
///
/// # Errors
/// Returns `Status::permission_denied` when the requested channel fails
/// [`validate_cron_job_channel_context`].
#[allow(clippy::result_large_err)]
pub(crate) fn resolve_cron_job_channel_for_create(
    context_channel: Option<&str>,
    requested_channel: String,
) -> Result<String, Status> {
    let requested_channel = non_empty(requested_channel);
    validate_cron_job_channel_context(context_channel, requested_channel.as_deref())?;
    Ok(requested_channel
        .or_else(|| context_channel.map(str::to_owned))
        .unwrap_or_else(|| "system:cron".to_owned()))
}

/// Validates the channel patch on a cron job update. `None` (empty request)
/// means "leave the stored channel unchanged" rather than defaulting it.
///
/// # Errors
/// Returns `Status::permission_denied` when the requested channel fails
/// [`validate_cron_job_channel_context`].
#[allow(clippy::result_large_err)]
pub(crate) fn validate_cron_job_channel_for_update(
    context_channel: Option<&str>,
    requested_channel: String,
) -> Result<Option<String>, Status> {
    let requested_channel = non_empty(requested_channel);
    validate_cron_job_channel_context(context_channel, requested_channel.as_deref())?;
    Ok(requested_channel)
}

/// Requires the authenticated principal to own the cron job; every mutating
/// and run-control cron RPC must pass through this gate.
///
/// # Errors
/// Returns `Status::permission_denied` on owner mismatch.
#[allow(clippy::result_large_err)]
pub(crate) fn enforce_cron_job_owner(
    authenticated_principal: &str,
    job_owner_principal: &str,
) -> Result<(), Status> {
    if authenticated_principal == job_owner_principal {
        return Ok(());
    }
    Err(Status::permission_denied("cron job owner mismatch for authenticated principal"))
}

/// Parses a proto concurrency policy into the journal enum.
///
/// # Errors
/// Returns `Status::invalid_argument` for unspecified or unknown values; a
/// concurrency policy has no safe default, so the client must choose one.
#[allow(clippy::result_large_err)]
pub(crate) fn cron_concurrency_from_proto(raw: i32) -> Result<CronConcurrencyPolicy, Status> {
    match cron_v1::ConcurrencyPolicy::try_from(raw)
        .unwrap_or(cron_v1::ConcurrencyPolicy::Unspecified)
    {
        cron_v1::ConcurrencyPolicy::Forbid => Ok(CronConcurrencyPolicy::Forbid),
        cron_v1::ConcurrencyPolicy::Replace => Ok(CronConcurrencyPolicy::Replace),
        cron_v1::ConcurrencyPolicy::QueueOne => Ok(CronConcurrencyPolicy::QueueOne),
        cron_v1::ConcurrencyPolicy::Unspecified => {
            Err(Status::invalid_argument("concurrency_policy must be specified"))
        }
    }
}

/// Converts a journal concurrency policy to its proto enum value.
pub(crate) fn cron_concurrency_to_proto(policy: CronConcurrencyPolicy) -> i32 {
    match policy {
        CronConcurrencyPolicy::Forbid => cron_v1::ConcurrencyPolicy::Forbid as i32,
        CronConcurrencyPolicy::Replace => cron_v1::ConcurrencyPolicy::Replace as i32,
        CronConcurrencyPolicy::QueueOne => cron_v1::ConcurrencyPolicy::QueueOne as i32,
    }
}

/// Parses a proto misfire policy into the journal enum.
///
/// # Errors
/// Returns `Status::invalid_argument` for unspecified or unknown values.
#[allow(clippy::result_large_err)]
pub(crate) fn cron_misfire_from_proto(
    raw: i32,
) -> Result<crate::journal::CronMisfirePolicy, Status> {
    match cron_v1::MisfirePolicy::try_from(raw).unwrap_or(cron_v1::MisfirePolicy::Unspecified) {
        cron_v1::MisfirePolicy::Skip => Ok(crate::journal::CronMisfirePolicy::Skip),
        cron_v1::MisfirePolicy::CatchUp => Ok(crate::journal::CronMisfirePolicy::CatchUp),
        cron_v1::MisfirePolicy::Unspecified => {
            Err(Status::invalid_argument("misfire_policy must be specified"))
        }
    }
}

/// Converts a journal misfire policy to its proto enum value.
pub(crate) fn cron_misfire_to_proto(policy: crate::journal::CronMisfirePolicy) -> i32 {
    match policy {
        crate::journal::CronMisfirePolicy::Skip => cron_v1::MisfirePolicy::Skip as i32,
        crate::journal::CronMisfirePolicy::CatchUp => cron_v1::MisfirePolicy::CatchUp as i32,
    }
}

/// Parses the required proto retry policy, silently clamping `max_attempts`
/// to 1..=16 and `backoff_ms` to 1..=60000 instead of rejecting - out-of-range
/// retry settings are bounded to safe values rather than failing the request.
///
/// # Errors
/// Returns `Status::invalid_argument` when the retry policy is missing.
#[allow(clippy::result_large_err)]
pub(crate) fn cron_retry_from_proto(
    value: Option<cron_v1::RetryPolicy>,
) -> Result<crate::journal::CronRetryPolicy, Status> {
    let value = value.ok_or_else(|| Status::invalid_argument("retry_policy is required"))?;
    let max_attempts = value.max_attempts.clamp(1, 16);
    let backoff_ms = value.backoff_ms.clamp(1, 60_000);
    Ok(crate::journal::CronRetryPolicy { max_attempts, backoff_ms })
}

fn optional_canonical_id(value: &Option<String>) -> Option<common_v1::CanonicalId> {
    value.as_deref().map(|ulid| common_v1::CanonicalId { ulid: ulid.to_owned() })
}

/// Builds the proto job message from a journal record. `enabled` and
/// `next_run_at_unix_ms` go through the `crate::cron` visibility helpers so
/// the wire view reflects effective scheduler state, not raw stored fields.
///
/// # Errors
/// Returns an error when the stored schedule payload cannot be converted to
/// its proto representation.
#[allow(clippy::result_large_err)]
pub(crate) fn cron_job_message(job: &CronJobRecord) -> Result<cron_v1::Job, Status> {
    let schedule = schedule_to_proto(job.schedule_type, job.schedule_payload_json.as_str())?;
    Ok(cron_v1::Job {
        v: CANONICAL_PROTOCOL_MAJOR,
        job_id: Some(common_v1::CanonicalId { ulid: job.job_id.clone() }),
        name: job.name.clone(),
        prompt: job.prompt.clone(),
        owner_principal: job.owner_principal.clone(),
        channel: job.channel.clone(),
        session_key: job.session_key.clone().unwrap_or_default(),
        session_label: job.session_label.clone().unwrap_or_default(),
        schedule: Some(schedule),
        enabled: crate::cron::visible_cron_job_enabled(job),
        concurrency_policy: cron_concurrency_to_proto(job.concurrency_policy),
        retry_policy: Some(cron_v1::RetryPolicy {
            max_attempts: job.retry_policy.max_attempts,
            backoff_ms: job.retry_policy.backoff_ms,
        }),
        misfire_policy: cron_misfire_to_proto(job.misfire_policy),
        jitter_ms: job.jitter_ms,
        next_run_at_unix_ms: crate::cron::visible_next_run_at_unix_ms(job).unwrap_or_default(),
        last_run_at_unix_ms: job.last_run_at_unix_ms.unwrap_or_default(),
        created_at_unix_ms: job.created_at_unix_ms,
        updated_at_unix_ms: job.updated_at_unix_ms,
    })
}

/// Builds the proto run message from a journal record; absent optionals are
/// flattened to proto3 defaults (empty string / zero).
pub(crate) fn cron_run_message(run: &CronRunRecord) -> cron_v1::JobRun {
    let session_reference = optional_canonical_id(&run.session_id);
    let orchestrator_reference = optional_canonical_id(&run.orchestrator_run_id);
    cron_v1::JobRun {
        v: CANONICAL_PROTOCOL_MAJOR,
        run_id: Some(common_v1::CanonicalId { ulid: run.run_id.clone() }),
        job_id: Some(common_v1::CanonicalId { ulid: run.job_id.clone() }),
        session_id: session_reference,
        orchestrator_run_id: orchestrator_reference,
        attempt: run.attempt,
        started_at_unix_ms: run.started_at_unix_ms,
        finished_at_unix_ms: run.finished_at_unix_ms.unwrap_or_default(),
        status: cron_run_status_to_proto(run.status),
        error_kind: run.error_kind.clone().unwrap_or_default(),
        error_message_redacted: run.error_message_redacted.clone().unwrap_or_default(),
        model_tokens_in: run.model_tokens_in,
        model_tokens_out: run.model_tokens_out,
        tool_calls: run.tool_calls,
        tool_denies: run.tool_denies,
    }
}

/// Converts a journal run status to its proto enum value.
pub(crate) fn cron_run_status_to_proto(status: CronRunStatus) -> i32 {
    match status {
        CronRunStatus::Accepted => cron_v1::JobRunStatus::Accepted as i32,
        CronRunStatus::Running => cron_v1::JobRunStatus::Running as i32,
        CronRunStatus::Succeeded => cron_v1::JobRunStatus::Succeeded as i32,
        CronRunStatus::Failed => cron_v1::JobRunStatus::Failed as i32,
        CronRunStatus::Skipped => cron_v1::JobRunStatus::Skipped as i32,
        CronRunStatus::Denied => cron_v1::JobRunStatus::Denied as i32,
    }
}
