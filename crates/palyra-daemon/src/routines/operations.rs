//! Operational routine projections for leases, restart catch-up, and cron security.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::journal::{CronJobRecord, CronMisfirePolicy, CronRunRecord, CronRunStatus};

use super::{
    effective_run_outcome_kind, routine_delivery_contract, RoutineDeliveryContract,
    RoutineExecutionConfig, RoutineMetadataRecord, RoutineRunMetadataRecord, RoutineRunOutcomeKind,
    RoutineTriggerKind, ROUTINE_RUN_LEASE_TTL_MS,
};

pub const ROUTINE_LEASE_LEDGER_SCHEMA_VERSION: u64 = 1;
pub const ROUTINE_STARTUP_CATCH_UP_SCHEMA_VERSION: u64 = 1;
pub const ROUTINE_CRON_SECURITY_SCHEMA_VERSION: u64 = 1;
pub const DEFAULT_ROUTINE_MAX_RUN_DURATION_MS: i64 = 30 * 60 * 1_000;
pub const DEFAULT_ROUTINE_CATCH_UP_STAGGER_MS: i64 = 30_000;
pub const DEFAULT_ROUTINE_MAX_MISSED_JOBS_PER_RESTART: usize = 8;
pub const DEFAULT_ROUTINE_OUTPUT_RETENTION_MS: i64 = 7 * 24 * 60 * 60 * 1_000;

/// Stable event kinds surfaced in the routine lease ledger projection.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RoutineLeaseLedgerEventKind {
    Fire,
    Claim,
    Start,
    Heartbeat,
    Finish,
    Fail,
    Retry,
}

/// Why an active routine run needs recovery.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RoutineLeaseRecoveryReason {
    HeartbeatExpired,
    MaxDurationExceeded,
}

/// Bounded routine scheduler policy projected into diagnostics and run history.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutineLeasePolicy {
    pub heartbeat_ttl_ms: i64,
    pub max_run_duration_ms: i64,
    pub max_retry_count: u32,
    pub max_missed_jobs_per_restart: usize,
    pub catch_up_stagger_ms: i64,
    pub output_retention_ms: i64,
}

/// Provider route recorded for a routine run without exposing credentials.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutineProviderSnapshot {
    pub mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_profile_id: Option<String>,
}

/// One run entry in the routine lease ledger projection.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutineLeaseLedgerEntry {
    pub routine_run_id: String,
    pub idempotency_key: String,
    pub attempt: u32,
    pub status: String,
    pub event_kinds: Vec<RoutineLeaseLedgerEventKind>,
    pub heartbeat_epoch_unix_ms: i64,
    pub heartbeat_age_ms: i64,
    pub lease_expired: bool,
    pub max_duration_exceeded: bool,
    pub terminal: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recovery_reason: Option<RoutineLeaseRecoveryReason>,
    pub provider_snapshot: RoutineProviderSnapshot,
    pub delivery_target: RoutineDeliveryContract,
    pub redaction_level: String,
}

/// Metadata-only lease ledger for a routine's recent runs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutineLeaseLedgerProjection {
    pub schema_version: u64,
    pub routine_id: String,
    pub job_id: String,
    pub policy: RoutineLeasePolicy,
    pub entries: Vec<RoutineLeaseLedgerEntry>,
    pub active_count: usize,
    pub recovery_required_count: usize,
    pub redaction_level: String,
}

/// One missed fire that startup catch-up may dispatch after restart.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutineCatchUpFire {
    pub missed_at_unix_ms: i64,
    pub dispatch_after_unix_ms: i64,
    pub idempotency_key: String,
}

/// Startup catch-up plan for a routine after daemon restart.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutineStartupCatchUpPlan {
    pub schema_version: u64,
    pub routine_id: String,
    pub policy: String,
    pub planned_fires: Vec<RoutineCatchUpFire>,
    pub dropped_missed_jobs: usize,
    pub stagger_ms: i64,
    pub operator_notification_route: String,
    pub redaction_level: String,
}

/// Security guard projection for unattended routine execution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoutineCronSecurityProjection {
    pub schema_version: u64,
    pub routine_id: String,
    pub credential_exfil_risk: bool,
    pub lifecycle_self_modification_blocked: bool,
    pub provider_snapshot: RoutineProviderSnapshot,
    pub delivery_target: RoutineDeliveryContract,
    pub output_retention_expires_at_unix_ms: i64,
    pub reason_codes: Vec<String>,
    pub redaction_level: String,
}

/// Builds the routine scheduler lease policy from a cron job.
#[must_use]
pub fn routine_lease_policy_from_job(job: &CronJobRecord) -> RoutineLeasePolicy {
    RoutineLeasePolicy {
        heartbeat_ttl_ms: ROUTINE_RUN_LEASE_TTL_MS,
        max_run_duration_ms: DEFAULT_ROUTINE_MAX_RUN_DURATION_MS,
        max_retry_count: job.retry_policy.max_attempts.saturating_sub(1),
        max_missed_jobs_per_restart: DEFAULT_ROUTINE_MAX_MISSED_JOBS_PER_RESTART,
        catch_up_stagger_ms: DEFAULT_ROUTINE_CATCH_UP_STAGGER_MS,
        output_retention_ms: DEFAULT_ROUTINE_OUTPUT_RETENTION_MS,
    }
}

/// Produces a stable idempotency key for a routine fire without embedding raw
/// trigger payloads.
#[must_use]
pub fn routine_fire_idempotency_key(
    routine_id: &str,
    trigger_kind: RoutineTriggerKind,
    trigger_dedupe_key: Option<&str>,
    scheduled_at_unix_ms: i64,
) -> String {
    let dedupe_key = trigger_dedupe_key.unwrap_or("none");
    let digest = Sha256::digest(
        format!(
            "routine:{routine_id}|trigger:{}|dedupe:{dedupe_key}|scheduled_at:{scheduled_at_unix_ms}",
            trigger_kind.as_str()
        )
        .as_bytes(),
    );
    format!("routine_fire:{}", hex::encode(digest))
}

/// Builds the provider route snapshot for a routine execution config.
#[must_use]
pub fn routine_provider_snapshot(execution: &RoutineExecutionConfig) -> RoutineProviderSnapshot {
    if let Some(profile_id) = execution.provider_profile_id.as_ref() {
        RoutineProviderSnapshot {
            mode: "pinned".to_owned(),
            provider_profile_id: Some(profile_id.clone()),
        }
    } else {
        RoutineProviderSnapshot { mode: "auto".to_owned(), provider_profile_id: None }
    }
}

/// Builds one metadata-only lease ledger entry for a routine run.
#[must_use]
pub fn routine_lease_ledger_entry(
    routine_id: &str,
    run: &CronRunRecord,
    metadata: Option<&RoutineRunMetadataRecord>,
    policy: &RoutineLeasePolicy,
    now_unix_ms: i64,
) -> RoutineLeaseLedgerEntry {
    let trigger_kind =
        metadata.map(|entry| entry.trigger_kind).unwrap_or(RoutineTriggerKind::Schedule);
    let idempotency_key = routine_fire_idempotency_key(
        routine_id,
        trigger_kind,
        metadata.and_then(|entry| entry.trigger_dedupe_key.as_deref()),
        run.started_at_unix_ms,
    );
    let terminal = !run.status.is_active();
    let heartbeat_age_ms = now_unix_ms.saturating_sub(run.updated_at_unix_ms);
    let run_age_ms = now_unix_ms.saturating_sub(run.started_at_unix_ms);
    let lease_expired = !terminal && heartbeat_age_ms > policy.heartbeat_ttl_ms;
    let max_duration_exceeded = !terminal && run_age_ms > policy.max_run_duration_ms;
    let recovery_reason = if max_duration_exceeded {
        Some(RoutineLeaseRecoveryReason::MaxDurationExceeded)
    } else if lease_expired {
        Some(RoutineLeaseRecoveryReason::HeartbeatExpired)
    } else {
        None
    };
    let outcome_kind = effective_run_outcome_kind(run.status, metadata);
    let execution = metadata.map(|entry| entry.execution.clone()).unwrap_or_default();
    let delivery = metadata.map(|entry| entry.delivery.clone()).unwrap_or_default();
    let mut event_kinds =
        vec![RoutineLeaseLedgerEventKind::Fire, RoutineLeaseLedgerEventKind::Claim];
    if run.attempt > 1 {
        event_kinds.push(RoutineLeaseLedgerEventKind::Retry);
    }
    event_kinds.push(RoutineLeaseLedgerEventKind::Start);
    if terminal {
        if matches!(run.status, CronRunStatus::Succeeded) {
            event_kinds.push(RoutineLeaseLedgerEventKind::Finish);
        } else {
            event_kinds.push(RoutineLeaseLedgerEventKind::Fail);
        }
    } else {
        event_kinds.push(RoutineLeaseLedgerEventKind::Heartbeat);
    }
    RoutineLeaseLedgerEntry {
        routine_run_id: run.run_id.clone(),
        idempotency_key,
        attempt: run.attempt,
        status: run.status.as_str().to_owned(),
        event_kinds,
        heartbeat_epoch_unix_ms: run.updated_at_unix_ms,
        heartbeat_age_ms,
        lease_expired,
        max_duration_exceeded,
        terminal,
        recovery_reason,
        provider_snapshot: routine_provider_snapshot(&execution),
        delivery_target: routine_delivery_contract(&delivery, outcome_kind, false),
        redaction_level: "metadata_only".to_owned(),
    }
}

/// Builds the routine lease ledger projection for recent run history.
#[must_use]
pub fn routine_lease_ledger_projection(
    routine_id: &str,
    job: &CronJobRecord,
    runs: &[CronRunRecord],
    run_metadata: &[RoutineRunMetadataRecord],
    now_unix_ms: i64,
) -> RoutineLeaseLedgerProjection {
    let policy = routine_lease_policy_from_job(job);
    let entries = runs
        .iter()
        .map(|run| {
            let metadata = run_metadata.iter().find(|entry| entry.run_id == run.run_id);
            routine_lease_ledger_entry(routine_id, run, metadata, &policy, now_unix_ms)
        })
        .collect::<Vec<_>>();
    let active_count = entries.iter().filter(|entry| !entry.terminal).count();
    let recovery_required_count =
        entries.iter().filter(|entry| entry.recovery_reason.is_some()).count();
    RoutineLeaseLedgerProjection {
        schema_version: ROUTINE_LEASE_LEDGER_SCHEMA_VERSION,
        routine_id: routine_id.to_owned(),
        job_id: job.job_id.clone(),
        policy,
        entries,
        active_count,
        recovery_required_count,
        redaction_level: "metadata_only".to_owned(),
    }
}

/// Builds a bounded startup catch-up plan from missed fire timestamps.
#[must_use]
pub fn routine_startup_catch_up_plan(
    job: &CronJobRecord,
    trigger_kind: RoutineTriggerKind,
    missed_fire_times: &[i64],
    policy: &RoutineLeasePolicy,
    now_unix_ms: i64,
) -> RoutineStartupCatchUpPlan {
    let catch_up_enabled = job.enabled && matches!(job.misfire_policy, CronMisfirePolicy::CatchUp);
    let max_fires = if catch_up_enabled { policy.max_missed_jobs_per_restart } else { 0 };
    let planned_fires = missed_fire_times
        .iter()
        .copied()
        .take(max_fires)
        .enumerate()
        .map(|(index, missed_at_unix_ms)| RoutineCatchUpFire {
            missed_at_unix_ms,
            dispatch_after_unix_ms: now_unix_ms
                .saturating_add(policy.catch_up_stagger_ms.saturating_mul(index as i64)),
            idempotency_key: routine_fire_idempotency_key(
                job.job_id.as_str(),
                trigger_kind,
                Some("startup_catch_up"),
                missed_at_unix_ms,
            ),
        })
        .collect::<Vec<_>>();
    let dropped_missed_jobs = missed_fire_times.len().saturating_sub(planned_fires.len());
    RoutineStartupCatchUpPlan {
        schema_version: ROUTINE_STARTUP_CATCH_UP_SCHEMA_VERSION,
        routine_id: job.job_id.clone(),
        policy: if catch_up_enabled { "catch_up" } else { "skip" }.to_owned(),
        planned_fires,
        dropped_missed_jobs,
        stagger_ms: policy.catch_up_stagger_ms,
        operator_notification_route: "console:routines.dead_letter".to_owned(),
        redaction_level: "metadata_only".to_owned(),
    }
}

/// Builds the routine cron security projection used by views and runbooks.
#[must_use]
pub fn routine_cron_security_projection(
    job: &CronJobRecord,
    routine: &RoutineMetadataRecord,
    now_unix_ms: i64,
) -> RoutineCronSecurityProjection {
    let prompt_lower = job.prompt.to_ascii_lowercase();
    let credential_exfil_risk =
        ["api key", "access token", "refresh token", "secret", "credential", "password"]
            .iter()
            .any(|marker| prompt_lower.contains(marker));
    let lifecycle_self_modification_blocked = [
        "disable routine",
        "delete routine",
        "shutdown daemon",
        "stop palyrad",
        "overwrite scheduler",
    ]
    .iter()
    .any(|marker| prompt_lower.contains(marker));
    let provider_snapshot = routine_provider_snapshot(&routine.execution);
    let delivery_target = routine_delivery_contract(
        &routine.delivery,
        RoutineRunOutcomeKind::SuccessWithOutput,
        false,
    );
    let mut reason_codes = vec!["routine_security.provider_snapshot_recorded".to_owned()];
    reason_codes.push("routine_security.delivery_target_resolved".to_owned());
    reason_codes.push("routine_security.output_retention_bounded".to_owned());
    if credential_exfil_risk {
        reason_codes.push("routine_security.credential_exfil_review_required".to_owned());
    }
    if lifecycle_self_modification_blocked {
        reason_codes.push("routine_security.lifecycle_self_modification_blocked".to_owned());
    }
    RoutineCronSecurityProjection {
        schema_version: ROUTINE_CRON_SECURITY_SCHEMA_VERSION,
        routine_id: routine.routine_id.clone(),
        credential_exfil_risk,
        lifecycle_self_modification_blocked,
        provider_snapshot,
        delivery_target,
        output_retention_expires_at_unix_ms: now_unix_ms
            .saturating_add(DEFAULT_ROUTINE_OUTPUT_RETENTION_MS),
        reason_codes,
        redaction_level: "metadata_only".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::journal::{
        CronConcurrencyPolicy, CronMisfirePolicy, CronRetryPolicy, CronRunRecord, CronRunStatus,
        CronScheduleType,
    };

    use super::*;
    use crate::routines::{
        RoutineApprovalPolicy, RoutineDeliveryConfig, RoutineDispatchMode, RoutineExecutionConfig,
        RoutineExecutionPosture, RoutineMetadataRecord, RoutineRunMetadataRecord,
        RoutineTriggerKind,
    };

    fn sample_cron_run(status: CronRunStatus, updated_at_unix_ms: i64) -> CronRunRecord {
        CronRunRecord {
            run_id: "run-1".to_owned(),
            job_id: "routine-1".to_owned(),
            attempt: 1,
            session_id: Some("session-1".to_owned()),
            orchestrator_run_id: Some("orchestrator-1".to_owned()),
            started_at_unix_ms: 1_000,
            finished_at_unix_ms: if status.is_active() { None } else { Some(2_000) },
            status,
            error_kind: None,
            error_message_redacted: None,
            model_tokens_in: 0,
            model_tokens_out: 0,
            tool_calls: 0,
            tool_denies: 0,
            created_at_unix_ms: 1_000,
            updated_at_unix_ms,
        }
    }

    fn sample_run_metadata(
        run_id: &str,
        routine_id: &str,
        updated_at_unix_ms: i64,
    ) -> RoutineRunMetadataRecord {
        RoutineRunMetadataRecord {
            run_id: run_id.to_owned(),
            routine_id: routine_id.to_owned(),
            trigger_kind: RoutineTriggerKind::Manual,
            trigger_reason: None,
            trigger_payload_json: json!({ "source": "test" }).to_string(),
            trigger_dedupe_key: None,
            execution: RoutineExecutionConfig::default(),
            delivery: RoutineDeliveryConfig::default(),
            dispatch_mode: RoutineDispatchMode::Normal,
            source_run_id: None,
            outcome_override: None,
            outcome_message: None,
            output_delivered: None,
            skip_reason: None,
            delivery_reason: None,
            approval_note: None,
            safety_note: None,
            created_at_unix_ms: updated_at_unix_ms,
            updated_at_unix_ms,
        }
    }

    fn sample_routine_metadata(routine_id: &str) -> RoutineMetadataRecord {
        RoutineMetadataRecord {
            routine_id: routine_id.to_owned(),
            trigger_kind: RoutineTriggerKind::Manual,
            trigger_payload_json: json!({ "kind": "manual" }).to_string(),
            execution: RoutineExecutionConfig::default(),
            delivery: RoutineDeliveryConfig::default(),
            quiet_hours: None,
            cooldown_ms: 0,
            approval_policy: RoutineApprovalPolicy::default(),
            template_id: None,
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        }
    }

    fn sample_cron_job(job_id: &str) -> CronJobRecord {
        CronJobRecord {
            job_id: job_id.to_owned(),
            name: "routine".to_owned(),
            prompt: "test".to_owned(),
            owner_principal: "user:ops".to_owned(),
            channel: "system:routines".to_owned(),
            session_key: None,
            session_label: None,
            workdir: None,
            schedule_type: CronScheduleType::At,
            schedule_payload_json: json!({"at":"2100-01-01T00:00:00Z"}).to_string(),
            enabled: true,
            concurrency_policy: CronConcurrencyPolicy::Forbid,
            retry_policy: CronRetryPolicy { max_attempts: 1, backoff_ms: 1 },
            misfire_policy: CronMisfirePolicy::Skip,
            jitter_ms: 0,
            next_run_at_unix_ms: None,
            last_run_at_unix_ms: None,
            queued_run: false,
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        }
    }

    #[test]
    fn routine_lease_ledger_flags_stuck_run_and_stable_idempotency() {
        let mut run = sample_cron_run(CronRunStatus::Running, 10_000);
        run.started_at_unix_ms = 1_000;
        let metadata = RoutineRunMetadataRecord {
            trigger_dedupe_key: Some("daily-report".to_owned()),
            execution: RoutineExecutionConfig {
                provider_profile_id: Some("provider-primary".to_owned()),
                ..RoutineExecutionConfig::default()
            },
            ..sample_run_metadata(run.run_id.as_str(), "routine-1", 10_000)
        };
        let policy = routine_lease_policy_from_job(&sample_cron_job("routine-1"));

        let entry = routine_lease_ledger_entry(
            "routine-1",
            &run,
            Some(&metadata),
            &policy,
            10_000 + ROUTINE_RUN_LEASE_TTL_MS + 1,
        );

        assert_eq!(entry.routine_run_id, "run-1");
        assert!(entry.lease_expired);
        assert_eq!(entry.recovery_reason, Some(RoutineLeaseRecoveryReason::HeartbeatExpired));
        assert!(entry.event_kinds.contains(&RoutineLeaseLedgerEventKind::Heartbeat));
        assert_eq!(entry.provider_snapshot.mode, "pinned");
        assert_eq!(
            entry.provider_snapshot.provider_profile_id.as_deref(),
            Some("provider-primary")
        );
        assert_eq!(
            entry.idempotency_key,
            routine_fire_idempotency_key(
                "routine-1",
                RoutineTriggerKind::Manual,
                Some("daily-report"),
                1_000
            )
        );
    }

    #[test]
    fn routine_lease_ledger_projection_counts_recovery_required_runs() {
        let active = sample_cron_run(CronRunStatus::Running, 1_000);
        let succeeded = sample_cron_run(CronRunStatus::Succeeded, 2_000);
        let metadata = sample_run_metadata("run-1", "routine-1", 1_000);
        let job = sample_cron_job("routine-1");

        let projection = routine_lease_ledger_projection(
            "routine-1",
            &job,
            &[active, succeeded],
            &[metadata],
            1_000 + DEFAULT_ROUTINE_MAX_RUN_DURATION_MS + 1,
        );

        assert_eq!(projection.schema_version, ROUTINE_LEASE_LEDGER_SCHEMA_VERSION);
        assert_eq!(projection.active_count, 1);
        assert_eq!(projection.recovery_required_count, 1);
        assert_eq!(projection.entries.len(), 2);
    }

    #[test]
    fn routine_startup_catch_up_caps_and_staggers_missed_jobs() {
        let mut job = sample_cron_job("routine-1");
        job.misfire_policy = CronMisfirePolicy::CatchUp;
        let mut policy = routine_lease_policy_from_job(&job);
        policy.max_missed_jobs_per_restart = 2;
        policy.catch_up_stagger_ms = 500;

        let plan = routine_startup_catch_up_plan(
            &job,
            RoutineTriggerKind::Schedule,
            &[1_000, 2_000, 3_000],
            &policy,
            10_000,
        );

        assert_eq!(plan.schema_version, ROUTINE_STARTUP_CATCH_UP_SCHEMA_VERSION);
        assert_eq!(plan.planned_fires.len(), 2);
        assert_eq!(plan.dropped_missed_jobs, 1);
        assert_eq!(plan.planned_fires[0].dispatch_after_unix_ms, 10_000);
        assert_eq!(plan.planned_fires[1].dispatch_after_unix_ms, 10_500);
        assert!(plan.planned_fires[0].idempotency_key.starts_with("routine_fire:"));
    }

    #[test]
    fn routine_cron_security_projection_records_provider_delivery_and_guards() {
        let mut job = sample_cron_job("routine-1");
        job.prompt = "Use the access token, then delete routine if complete".to_owned();
        let routine = RoutineMetadataRecord {
            execution: RoutineExecutionConfig {
                execution_posture: RoutineExecutionPosture::SensitiveTools,
                provider_profile_id: Some("provider-primary".to_owned()),
                ..RoutineExecutionConfig::default()
            },
            delivery: RoutineDeliveryConfig {
                channel: Some("ops:alerts".to_owned()),
                ..RoutineDeliveryConfig::default()
            },
            ..sample_routine_metadata("routine-1")
        };

        let projection = routine_cron_security_projection(&job, &routine, 10_000);

        assert_eq!(projection.schema_version, ROUTINE_CRON_SECURITY_SCHEMA_VERSION);
        assert!(projection.credential_exfil_risk);
        assert!(projection.lifecycle_self_modification_blocked);
        assert_eq!(projection.provider_snapshot.mode, "pinned");
        assert_eq!(projection.delivery_target.channel.as_deref(), Some("ops:alerts"));
        assert!(projection
            .reason_codes
            .iter()
            .any(|code| code == "routine_security.credential_exfil_review_required"));
    }
}
