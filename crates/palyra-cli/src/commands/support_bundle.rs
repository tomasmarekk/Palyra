//! `palyra support-bundle`: export size-capped diagnostics bundles and manage
//! offline replay bundles (journal export, import, run, baseline).
//! Replay exports read the daemon's SQLite journal directly and redact
//! embedded config/journal payloads before anything is written to disk.

use crate::{output::support_bundle as support_bundle_output, *};
use palyra_common::runtime_contracts::{
    IdempotencyOperationState, IdempotencyRecordSnapshot, RunLifecyclePhase,
    RunLifecycleTransitionRecord, RuntimeActorKind, RuntimeActorRef, StableErrorEnvelope,
};

/// Runs a `palyra support-bundle` subcommand.
///
/// # Errors
/// Returns an error when limits are out of range, the journal database or
/// replay bundle cannot be read, offline replay fails, or artifacts cannot be
/// written.
pub(crate) fn run_support_bundle(command: SupportBundleCommand) -> Result<()> {
    match command {
        SupportBundleCommand::Export {
            output,
            max_bytes,
            journal_hash_limit,
            error_limit,
            json,
        } => run_support_bundle_export(output, max_bytes, journal_hash_limit, error_limit, json),
        SupportBundleCommand::ReplayExport { run_id, output, journal_db, max_events } => {
            run_replay_export(run_id, output, journal_db, max_events)
        }
        SupportBundleCommand::ReplayImport { input, output_dir } => {
            run_replay_import(input, output_dir)
        }
        SupportBundleCommand::ReplayRun { input, diff_output } => {
            run_replay_run(input, diff_output)
        }
        SupportBundleCommand::ReplayBaseline { input, output } => {
            run_replay_baseline(input, output)
        }
    }
}

fn run_support_bundle_export(
    output: Option<String>,
    max_bytes: usize,
    journal_hash_limit: usize,
    error_limit: usize,
    json: bool,
) -> Result<()> {
    if max_bytes < 2_048 {
        anyhow::bail!("support-bundle max-bytes must be at least 2048");
    }
    let generated_at_unix_ms = now_unix_ms_i64()?;
    let checks = build_doctor_checks();
    let doctor = build_doctor_report(checks.as_slice())?;
    let output_path = resolve_support_bundle_output_path(output, generated_at_unix_ms);

    let build = build_metadata();
    let diagnostics = build_support_bundle_diagnostics_snapshot();
    let profile = app::current_root_context().and_then(|context| context.active_profile_context());
    let mut bundle = SupportBundle {
        schema_version: 1,
        generated_at_unix_ms,
        profile,
        build: SupportBundleBuildSnapshot {
            version: build.version.to_owned(),
            git_hash: build.git_hash.to_owned(),
            build_profile: build.build_profile.to_owned(),
        },
        platform: SupportBundlePlatformSnapshot {
            os: std::env::consts::OS.to_owned(),
            family: std::env::consts::FAMILY.to_owned(),
            arch: std::env::consts::ARCH.to_owned(),
        },
        doctor,
        recovery: Some(commands::doctor::build_doctor_support_bundle_value()?),
        config: build_support_bundle_config_snapshot(),
        observability: build_support_bundle_observability_snapshot(&diagnostics),
        triage: build_support_bundle_triage_snapshot(),
        replay: build_support_bundle_replay_snapshot(),
        diagnostics,
        journal: build_support_bundle_journal_snapshot(journal_hash_limit, error_limit),
        truncated: false,
        warnings: Vec::new(),
    };

    let encoded = encode_support_bundle_with_cap(&mut bundle, max_bytes)?;
    if let Some(parent) = output_path.parent().filter(|value| !value.as_os_str().is_empty()) {
        fs::create_dir_all(parent).with_context(|| {
            format!("failed to create support-bundle directory {}", parent.display())
        })?;
    }
    fs::write(output_path.as_path(), encoded.as_slice())
        .with_context(|| format!("failed to write support bundle {}", output_path.display()))?;
    support_bundle_output::emit_export(&output_path, encoded.len(), &bundle, json)
}

fn run_replay_export(
    run_id: String,
    output: String,
    journal_db: Option<String>,
    max_events: usize,
) -> Result<()> {
    if max_events == 0 || max_events > 4_096 {
        anyhow::bail!("support-bundle replay-export --max-events must be in range 1..=4096");
    }
    let output_path = PathBuf::from(output);
    let bundle = build_replay_bundle_from_journal(run_id.as_str(), journal_db, max_events)?;
    let encoded = canonical_replay_bundle_bytes(&bundle)?;
    write_replay_artifact(output_path.as_path(), encoded.as_slice())?;
    println!(
        "support_bundle.replay_export path={} run_id={} bytes={} tape_events={} canonical_sha256={}",
        output_path.display(),
        bundle.source.run_id,
        encoded.len(),
        bundle.tape_events.len(),
        bundle
            .integrity
            .canonical_sha256
            .as_deref()
            .unwrap_or("<missing>")
    );
    std::io::stdout().flush().context("stdout flush failed")
}

fn run_replay_import(input: String, output_dir: String) -> Result<()> {
    let input_path = PathBuf::from(input);
    let bytes = fs::read(input_path.as_path())
        .with_context(|| format!("failed to read replay bundle {}", input_path.display()))?;
    let bundle = parse_replay_bundle(bytes.as_slice())?;
    let report = replay_bundle_offline(&bundle);
    ensure_replay_report_passed(&report)?;
    let output_dir = PathBuf::from(output_dir);
    fs::create_dir_all(output_dir.as_path()).with_context(|| {
        format!("failed to create replay import directory {}", output_dir.display())
    })?;
    let output_path = replay_import_artifact_path(output_dir.as_path(), bytes.as_slice());
    let hash = sha256_hex(bytes.as_slice());
    write_replay_artifact(output_path.as_path(), bytes.as_slice())?;
    println!(
        "support_bundle.replay_import path={} status=passed canonical_sha256={}",
        output_path.display(),
        hash
    );
    std::io::stdout().flush().context("stdout flush failed")
}

// INTENTIONAL: the artifact name is derived from a locally recomputed hash of
// the raw bytes, never from any hash or path the bundle itself supplies, so a
// malicious bundle cannot steer where the import lands; a unit test pins this.
fn replay_import_artifact_path(output_dir: &Path, bundle_bytes: &[u8]) -> PathBuf {
    let hash = sha256_hex(bundle_bytes);
    output_dir.join(format!("{}.replay.json", hash.chars().take(16).collect::<String>()))
}

fn run_replay_run(input: String, diff_output: Option<String>) -> Result<()> {
    let input_path = PathBuf::from(input);
    let bytes = fs::read(input_path.as_path())
        .with_context(|| format!("failed to read replay bundle {}", input_path.display()))?;
    let bundle = parse_replay_bundle(bytes.as_slice())?;
    let report = replay_bundle_offline(&bundle);
    if let Some(output) = diff_output {
        let output_path = PathBuf::from(output);
        let encoded =
            serde_json::to_vec_pretty(&report).context("failed to encode replay diff report")?;
        write_replay_artifact(output_path.as_path(), encoded.as_slice())?;
    }
    println!(
        "support_bundle.replay_run status={:?} diffs={} checked_categories={} validation_issues={}",
        report.status,
        report.diffs.len(),
        report.checked_categories.len(),
        report.validation.issues.len()
    );
    ensure_replay_report_passed(&report)?;
    std::io::stdout().flush().context("stdout flush failed")
}

fn run_replay_baseline(input: String, output: String) -> Result<()> {
    let input_path = PathBuf::from(input);
    let bytes = fs::read(input_path.as_path())
        .with_context(|| format!("failed to read replay bundle {}", input_path.display()))?;
    let mut bundle = parse_replay_bundle(bytes.as_slice())?;
    let report = replay_bundle_offline(&bundle);
    ensure_replay_report_passed(&report)?;
    finalize_replay_bundle(&mut bundle)?;
    let output_path = PathBuf::from(output);
    let encoded = canonical_replay_bundle_bytes(&bundle)?;
    write_replay_artifact(output_path.as_path(), encoded.as_slice())?;
    println!(
        "support_bundle.replay_baseline path={} canonical_sha256={}",
        output_path.display(),
        bundle.integrity.canonical_sha256.as_deref().unwrap_or("<missing>")
    );
    std::io::stdout().flush().context("stdout flush failed")
}

pub(crate) fn build_replay_bundle_from_journal(
    run_id: &str,
    journal_db: Option<String>,
    max_events: usize,
) -> Result<ReplayBundle> {
    let db_path = resolve_daemon_journal_db_path(journal_db)?;
    let connection = Connection::open(db_path.as_path())
        .with_context(|| format!("failed to open journal database {}", db_path.display()))?;
    let run = read_replay_journal_run(&connection, run_id)?
        .with_context(|| format!("orchestrator run not found: {run_id}"))?;
    let tape_events = read_replay_journal_tape(&connection, run_id, max_events)?;
    let lifecycle_transitions = read_replay_lifecycle_transitions(&connection, run_id)?;
    let idempotency_records = read_replay_idempotency_records(&connection, run_id)?;
    let artifact_refs = read_replay_tool_result_artifact_refs(&connection, run_id)?;
    // The tape query fetches max_events + 1 rows precisely so truncation can be
    // detected here and recorded as a capture warning.
    let truncated = tape_events.len() > max_events;
    let tape_events = tape_events.into_iter().take(max_events).collect::<Vec<_>>();
    let mut config_snapshot = json!({
        "config": build_support_bundle_config_snapshot(),
        "contract": replay_contract_snapshot(),
        "journal": {
            "db_path": db_path.to_string_lossy(),
            "source": "sqlite_orchestrator_tape",
        }
    });
    // Config values may embed tokens or credentialed URLs; redact before the
    // snapshot is serialized into a shareable bundle.
    redact_json_value_tree(&mut config_snapshot, None);

    build_replay_bundle(ReplayBundleBuildInput {
        generated_at_unix_ms: now_unix_ms_i64()?,
        source: ReplaySource {
            product: "palyra".to_owned(),
            run_id: run.run_id.clone(),
            session_id: Some(run.session_id.clone()),
            origin_kind: run.origin_kind.clone(),
            schema_policy: "reject_future_schema_versions_additive_backward_compat".to_owned(),
        },
        capture: ReplayCaptureMetadata {
            captured_at_unix_ms: now_unix_ms_i64()?,
            capture_mode: "cli_support_bundle_replay_export".to_owned(),
            max_events_per_run: max_events,
            truncated,
            inline_sections: vec![
                "run".to_owned(),
                "config_snapshot".to_owned(),
                "tape_events".to_owned(),
                "tool_exchanges".to_owned(),
                "http_exchanges".to_owned(),
                "approvals".to_owned(),
                "expected".to_owned(),
            ],
            referenced_sections: vec![
                "large_binary_artifacts".to_owned(),
                "workspace_files".to_owned(),
                "journal_events_outside_run".to_owned(),
            ],
            warnings: if truncated {
                vec![format!("tape truncated at {max_events} events for replay bundle export")]
            } else {
                Vec::new()
            },
        },
        run: ReplayRunSnapshot {
            state: run.state,
            principal: run.principal,
            device_id: run.device_id,
            channel: run.channel,
            normalized_user_input: extract_replay_user_input(run.parameter_delta_json.as_deref()),
            prompt_tokens: run.prompt_tokens,
            completion_tokens: run.completion_tokens,
            total_tokens: run.total_tokens,
            last_error: run.last_error,
            parent_run_id: run.parent_run_id,
            origin_run_id: run.origin_run_id,
            parameter_delta: run
                .parameter_delta_json
                .as_deref()
                .and_then(|raw| serde_json::from_str::<Value>(raw).ok()),
        },
        config_snapshot,
        tape_events,
        lifecycle_transitions,
        idempotency_records,
        artifact_refs,
    })
}

pub(crate) fn write_replay_artifact(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent().filter(|value| !value.as_os_str().is_empty()) {
        fs::create_dir_all(parent).with_context(|| {
            format!("failed to create replay artifact directory {}", parent.display())
        })?;
    }
    fs::write(path, bytes)
        .with_context(|| format!("failed to write replay artifact {}", path.display()))
}

#[derive(Debug)]
struct ReplayJournalRunRow {
    run_id: String,
    session_id: String,
    state: String,
    principal: String,
    device_id: String,
    channel: Option<String>,
    prompt_tokens: u64,
    completion_tokens: u64,
    total_tokens: u64,
    last_error: Option<String>,
    origin_kind: String,
    origin_run_id: Option<String>,
    parent_run_id: Option<String>,
    parameter_delta_json: Option<String>,
}

fn read_replay_journal_run(
    connection: &Connection,
    run_id: &str,
) -> Result<Option<ReplayJournalRunRow>> {
    connection
        .query_row(
            r#"
                SELECT
                    runs.run_ulid,
                    runs.session_ulid,
                    runs.state,
                    sessions.principal,
                    sessions.device_id,
                    sessions.channel,
                    runs.prompt_tokens,
                    runs.completion_tokens,
                    runs.total_tokens,
                    runs.last_error,
                    runs.origin_kind,
                    runs.origin_run_ulid,
                    runs.parent_run_ulid,
                    runs.parameter_delta_json
                FROM orchestrator_runs AS runs
                INNER JOIN orchestrator_sessions AS sessions
                    ON sessions.session_ulid = runs.session_ulid
                WHERE runs.run_ulid = ?1
            "#,
            rusqlite::params![run_id],
            |row| {
                let prompt_tokens: i64 = row.get(6)?;
                let completion_tokens: i64 = row.get(7)?;
                let total_tokens: i64 = row.get(8)?;
                Ok(ReplayJournalRunRow {
                    run_id: row.get(0)?,
                    session_id: row.get(1)?,
                    state: row.get(2)?,
                    principal: row.get(3)?,
                    device_id: row.get(4)?,
                    channel: row.get(5)?,
                    prompt_tokens: prompt_tokens.max(0) as u64,
                    completion_tokens: completion_tokens.max(0) as u64,
                    total_tokens: total_tokens.max(0) as u64,
                    last_error: row.get(9)?,
                    origin_kind: row.get(10)?,
                    origin_run_id: row.get(11)?,
                    parent_run_id: row.get(12)?,
                    parameter_delta_json: row.get(13)?,
                })
            },
        )
        .optional()
        .context("failed to read orchestrator run for replay export")
}

fn read_replay_journal_tape(
    connection: &Connection,
    run_id: &str,
    max_events: usize,
) -> Result<Vec<ReplayTapeEvent>> {
    let mut statement = connection
        .prepare(
            r#"
                SELECT seq, event_type, payload_json
                FROM orchestrator_tape
                WHERE run_ulid = ?1
                ORDER BY seq ASC
                LIMIT ?2
            "#,
        )
        .context("failed to prepare replay tape query")?;
    let rows = statement.query_map(rusqlite::params![run_id, (max_events + 1) as i64], |row| {
        let seq: i64 = row.get(0)?;
        let event_type: String = row.get(1)?;
        let payload_json: String = row.get(2)?;
        let payload = serde_json::from_str::<Value>(payload_json.as_str()).unwrap_or_else(|_| {
            json!({
                "raw": payload_json,
            })
        });
        Ok(ReplayTapeEvent { seq, event_type, payload })
    })?;
    let mut events = Vec::new();
    for row in rows {
        events.push(row.context("failed to read replay tape event")?);
    }
    Ok(events)
}

fn read_replay_lifecycle_transitions(
    connection: &Connection,
    run_id: &str,
) -> Result<Vec<RunLifecycleTransitionRecord>> {
    if !sqlite_table_exists(connection, "run_lifecycle_events")? {
        return Ok(Vec::new());
    }
    let mut statement = connection
        .prepare(
            r#"
                SELECT
                    event_ulid,
                    run_ulid,
                    session_ulid,
                    from_state,
                    to_state,
                    actor_kind,
                    actor_id,
                    correlation_id,
                    parent_run_ulid,
                    idempotency_key,
                    reason,
                    created_at_unix_ms
                FROM run_lifecycle_events
                WHERE run_ulid = ?1
                ORDER BY created_at_unix_ms ASC, event_ulid ASC
            "#,
        )
        .context("failed to prepare replay lifecycle query")?;
    let rows = statement.query_map(rusqlite::params![run_id], |row| {
        let from_state: Option<String> = row.get(3)?;
        let to_state: String = row.get(4)?;
        let actor_kind: String = row.get(5)?;
        let to_state = RunLifecyclePhase::parse(to_state.as_str()).ok_or_else(|| {
            rusqlite::Error::InvalidColumnType(
                4,
                "to_state".to_owned(),
                rusqlite::types::Type::Text,
            )
        })?;
        let actor_kind = RuntimeActorKind::parse(actor_kind.as_str()).ok_or_else(|| {
            rusqlite::Error::InvalidColumnType(
                5,
                "actor_kind".to_owned(),
                rusqlite::types::Type::Text,
            )
        })?;
        Ok(RunLifecycleTransitionRecord {
            schema_version: 1,
            event_id: row.get(0)?,
            run_id: row.get(1)?,
            session_id: row.get(2)?,
            from_state: from_state.as_deref().and_then(RunLifecyclePhase::parse),
            to_state,
            actor: RuntimeActorRef { kind: actor_kind, id: row.get(6)? },
            correlation_id: row.get(7)?,
            parent_run_id: row.get(8)?,
            idempotency_key: row.get(9)?,
            reason: row.get(10)?,
            occurred_at_unix_ms: row.get(11)?,
        })
    })?;
    let mut transitions = Vec::new();
    for row in rows {
        transitions.push(row.context("failed to read replay lifecycle transition")?);
    }
    Ok(transitions)
}

fn read_replay_idempotency_records(
    connection: &Connection,
    run_id: &str,
) -> Result<Vec<IdempotencyRecordSnapshot>> {
    if !sqlite_table_exists(connection, "idempotency_records")? {
        return Ok(Vec::new());
    }
    // idempotency_records has no run_ulid column, so rows are matched
    // heuristically by run-id substring across the key and payload columns.
    let run_pattern = format!("%{run_id}%");
    let mut statement = connection
        .prepare(
            r#"
                SELECT
                    idempotency_key,
                    scope,
                    operation_kind,
                    payload_sha256,
                    state,
                    result_json,
                    error_json,
                    first_seen_at_unix_ms,
                    updated_at_unix_ms,
                    expires_at_unix_ms
                FROM idempotency_records
                WHERE idempotency_key LIKE ?1
                   OR result_json LIKE ?1
                   OR error_json LIKE ?1
                ORDER BY first_seen_at_unix_ms ASC, idempotency_key ASC
            "#,
        )
        .context("failed to prepare replay idempotency query")?;
    let rows = statement.query_map(rusqlite::params![run_pattern], |row| {
        let state: String = row.get(4)?;
        let error_json: Option<String> = row.get(6)?;
        let state = IdempotencyOperationState::parse(state.as_str()).ok_or_else(|| {
            rusqlite::Error::InvalidColumnType(4, "state".to_owned(), rusqlite::types::Type::Text)
        })?;
        let error = error_json
            .as_deref()
            .map(serde_json::from_str::<StableErrorEnvelope>)
            .transpose()
            .map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    6,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?;
        Ok(IdempotencyRecordSnapshot {
            key: row.get(0)?,
            scope: row.get(1)?,
            operation_kind: row.get(2)?,
            payload_sha256: row.get(3)?,
            state,
            result_json: row.get(5)?,
            error,
            first_seen_at_unix_ms: row.get(7)?,
            updated_at_unix_ms: row.get(8)?,
            expires_at_unix_ms: row.get(9)?,
        })
    })?;
    let mut records = Vec::new();
    for row in rows {
        records.push(row.context("failed to read replay idempotency record")?);
    }
    Ok(records)
}

fn read_replay_tool_result_artifact_refs(
    connection: &Connection,
    run_id: &str,
) -> Result<Vec<ReplayArtifactRef>> {
    if !sqlite_table_exists(connection, "tool_result_artifacts")? {
        return Ok(Vec::new());
    }
    let mut statement = connection
        .prepare(
            r#"
                SELECT artifact_ulid, storage_backend, digest_sha256, size_bytes
                FROM tool_result_artifacts
                WHERE run_ulid = ?1
                ORDER BY created_at_unix_ms ASC, artifact_ulid ASC
            "#,
        )
        .context("failed to prepare replay artifact ref query")?;
    let rows = statement.query_map(rusqlite::params![run_id], |row| {
        let artifact_id: String = row.get(0)?;
        let storage_backend: String = row.get(1)?;
        let size_bytes: i64 = row.get(3)?;
        Ok(ReplayArtifactRef {
            artifact_id: artifact_id.clone(),
            kind: "tool_result".to_owned(),
            reference: format!("tool-result-artifact://{storage_backend}/{artifact_id}"),
            sha256: row.get(2)?,
            size_bytes: Some(size_bytes.max(0) as u64),
        })
    })?;
    let mut refs = Vec::new();
    for row in rows {
        refs.push(row.context("failed to read replay artifact ref")?);
    }
    Ok(refs)
}

fn sqlite_table_exists(connection: &Connection, table_name: &str) -> Result<bool> {
    let exists = connection
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1 LIMIT 1",
            rusqlite::params![table_name],
            |_| Ok(()),
        )
        .optional()
        .with_context(|| format!("failed to inspect sqlite table {table_name}"))?
        .is_some();
    Ok(exists)
}

fn extract_replay_user_input(parameter_delta_json: Option<&str>) -> Option<Value> {
    let value = parameter_delta_json.and_then(|raw| serde_json::from_str::<Value>(raw).ok())?;
    value.get("user_input").or_else(|| value.get("input")).or_else(|| value.get("prompt")).cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use palyra_common::replay_bundle::{replay_bundle_offline, ReplayRunStatus};

    #[test]
    fn replay_import_artifact_path_ignores_bundle_supplied_hash() {
        let output_dir = PathBuf::from("imports");
        let bytes = br#"{"integrity":{"canonical_sha256":"/tmp/pwn"}}"#;

        let path = replay_import_artifact_path(output_dir.as_path(), bytes);

        assert!(path.starts_with(output_dir.as_path()));
        assert_eq!(path.parent(), Some(output_dir.as_path()));
        assert_eq!(
            path.file_name().and_then(|value| value.to_str()),
            Some(format!("{}.replay.json", &sha256_hex(bytes)[..16]).as_str())
        );
        assert!(!path.to_string_lossy().contains("pwn"));
    }

    #[test]
    fn replay_export_from_journal_redacts_and_replays_offline() {
        let temp = tempfile::tempdir().expect("tempdir should be created");
        let db_path = temp.path().join("journal.sqlite3");
        let connection = Connection::open(db_path.as_path()).expect("sqlite should open");
        connection
            .execute_batch(
                r#"
                CREATE TABLE orchestrator_sessions (
                    session_ulid TEXT PRIMARY KEY,
                    principal TEXT NOT NULL,
                    device_id TEXT NOT NULL,
                    channel TEXT
                );
                CREATE TABLE orchestrator_runs (
                    run_ulid TEXT PRIMARY KEY,
                    session_ulid TEXT NOT NULL,
                    state TEXT NOT NULL,
                    prompt_tokens INTEGER NOT NULL,
                    completion_tokens INTEGER NOT NULL,
                    total_tokens INTEGER NOT NULL,
                    last_error TEXT,
                    origin_kind TEXT NOT NULL,
                    origin_run_ulid TEXT,
                    parent_run_ulid TEXT,
                    parameter_delta_json TEXT
                );
                CREATE TABLE orchestrator_tape (
                    run_ulid TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    PRIMARY KEY (run_ulid, seq)
                );
                "#,
            )
            .expect("schema should initialize");
        connection
            .execute(
                "INSERT INTO orchestrator_sessions (session_ulid, principal, device_id, channel) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![
                    "01ARZ3NDEKTSV4RRFFQ69G5FB1",
                    "user:ops",
                    "device:local",
                    "cli",
                ],
            )
            .expect("session should insert");
        connection
            .execute(
                "INSERT INTO orchestrator_runs (
                    run_ulid, session_ulid, state, prompt_tokens, completion_tokens, total_tokens,
                    last_error, origin_kind, origin_run_ulid, parent_run_ulid, parameter_delta_json
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                rusqlite::params![
                    "01ARZ3NDEKTSV4RRFFQ69G5FB2",
                    "01ARZ3NDEKTSV4RRFFQ69G5FB1",
                    "completed",
                    12_i64,
                    4_i64,
                    16_i64,
                    Option::<String>::None,
                    "run_stream",
                    Option::<String>::None,
                    Option::<String>::None,
                    json!({ "user_input": { "text": "fetch https://example.test?token=raw" } })
                        .to_string(),
                ],
            )
            .expect("run should insert");
        connection
            .execute(
                "INSERT INTO orchestrator_tape (run_ulid, seq, event_type, payload_json) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![
                    "01ARZ3NDEKTSV4RRFFQ69G5FB2",
                    0_i64,
                    "tool_proposal",
                    json!({
                        "proposal_id": "01ARZ3NDEKTSV4RRFFQ69G5FB3",
                        "tool_name": "palyra.http.fetch",
                        "input_json": {
                            "url": "https://example.test/callback?access_token=raw&mode=ok",
                            "headers": { "authorization": "Bearer raw" }
                        }
                    })
                    .to_string(),
                ],
            )
            .expect("proposal should insert");
        connection
            .execute(
                "INSERT INTO orchestrator_tape (run_ulid, seq, event_type, payload_json) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![
                    "01ARZ3NDEKTSV4RRFFQ69G5FB2",
                    1_i64,
                    "tool_result",
                    json!({
                        "proposal_id": "01ARZ3NDEKTSV4RRFFQ69G5FB3",
                        "success": true,
                        "output_json": { "status": 200 },
                        "error": "",
                    })
                    .to_string(),
                ],
            )
            .expect("result should insert");

        let bundle = build_replay_bundle_from_journal(
            "01ARZ3NDEKTSV4RRFFQ69G5FB2",
            Some(db_path.to_string_lossy().into_owned()),
            128,
        )
        .expect("replay bundle should export from journal");
        let encoded = serde_json::to_string(&bundle).expect("bundle should serialize");
        assert!(!encoded.contains("access_token=raw"));
        assert!(!encoded.contains("Bearer raw"));
        let report = replay_bundle_offline(&bundle);
        assert_eq!(report.status, ReplayRunStatus::Passed, "{report:#?}");
    }
}
