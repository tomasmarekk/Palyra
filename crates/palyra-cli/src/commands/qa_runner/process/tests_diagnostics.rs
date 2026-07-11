use super::*;

#[test]
fn durable_payload_scanner_covers_runtime_control_secrets() {
    const RAW_CAPABILITY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    let (mut sandbox, _) = test_sandbox();
    let admin = SecretBytes::new(b"qa-admin-secret-token".to_vec())
        .expect("admin token should be long enough");
    let capability = SecretBytes::new(RAW_CAPABILITY.as_bytes().to_vec())
        .expect("capability should be long enough");
    sandbox.secret_sentinels.extend([admin, capability]);
    let raw_payload = format!("raw={RAW_CAPABILITY}");
    let prefixed_payload = format!("{QA_FAULT_CAPABILITY_PREFIX}{RAW_CAPABILITY}");

    assert!(sandbox.contains_secret(b"prefix qa-admin-secret-token suffix"));
    assert!(sandbox.contains_secret(raw_payload.as_bytes()));
    assert!(sandbox.contains_secret(prefixed_payload.as_bytes()));
    assert!(!sandbox.contains_secret(b"redacted runtime evidence"));
}

#[test]
fn failure_diagnostics_are_persistable_before_state_root_removal_without_secrets() {
    const ADMIN_TOKEN: &str = "qa-admin-secret-token";
    const RUN_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAW";

    let (mut sandbox, state_root) = test_sandbox();
    sandbox.launch.admin_token = ADMIN_TOKEN.to_owned();
    sandbox
        .secret_sentinels
        .push(SecretBytes::new(ADMIN_TOKEN.as_bytes().to_vec()).expect("admin sentinel"));
    sandbox.record_run_id(RUN_ID);

    let manifest = parse_scenario(FAULT_MUTATION_SCENARIO);
    let context = prepare_fault_context(state_root.as_path(), manifest.fault_injection.as_ref())
        .expect("fault context should materialize")
        .expect("fault scenario should create a context");
    let launch = prepare_fault_launch(&context).expect("fault launch should materialize");
    let raw_capability =
        String::from_utf8_lossy(launch.capability_sentinel.as_slice()).into_owned();
    let sidecar_record = QaFaultEvidenceSidecarRecord::LaunchLoaded(
        palyra_common::qa_fault_injection::QaFaultLaunchLoadedRecord {
            schema_version: 1,
            sequence: 1,
            launch_id: launch.document.launch_id.clone(),
            plan_sha256: launch.document.plan_sha256.clone(),
            capability_sha256: launch.document.capability_sha256.clone(),
        },
    );
    let mut sidecar_bytes =
        serde_json::to_vec(&sidecar_record).expect("sidecar record should serialize");
    sidecar_bytes.push(b'\n');
    write_owner_only_new_file(
        context.evidence_path.as_path(),
        sidecar_bytes.as_slice(),
        "qa.runner.test_sidecar_write_failed",
    )
    .expect("sidecar should be written");
    fs::remove_file(launch.capability_path.as_path())
        .expect("consumed capability should be removed");
    sandbox.secret_sentinels.push(launch.capability_sentinel);
    sandbox.fault_launch_documents.push(launch.document);
    sandbox.launch.fault = Some(context);

    let data_dir = state_root.join("data");
    fs::create_dir_all(data_dir.as_path()).expect("journal directory should exist");
    let connection =
        Connection::open(data_dir.join("journal.sqlite3")).expect("diagnostic journal should open");
    connection
        .execute_batch(
            r#"
                CREATE TABLE orchestrator_runs (
                    run_ulid TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    cancel_requested INTEGER NOT NULL,
                    last_error TEXT
                );
                CREATE TABLE orchestrator_tape (
                    run_ulid TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );
                CREATE TABLE journal_events (
                    seq INTEGER NOT NULL,
                    run_ulid TEXT NOT NULL,
                    kind INTEGER NOT NULL,
                    actor INTEGER NOT NULL,
                    redacted INTEGER NOT NULL,
                    payload_json TEXT NOT NULL
                );
                "#,
        )
        .expect("diagnostic journal schema should be created");
    connection
        .execute(
            "INSERT INTO orchestrator_runs (run_ulid, state, cancel_requested, last_error)\
                 VALUES (?1, 'in_progress', 0, ?2)",
            rusqlite::params![
                RUN_ID,
                "policy evaluation diagnostics triggered fail closed: recursion limit reached"
            ],
        )
        .expect("run row should be inserted");
    let tape_payload = serde_json::json!({
        "proposal_id": "proposal-1",
        "tool_name": "palyra.fs.apply_patch",
        "allowed": false,
        "reason": "policy recursion limit reached",
        "api_key": ADMIN_TOKEN,
        "input_json": {
            "reason": raw_capability,
            "path": "C:\\Users\\fixture-user\\private.txt"
        }
    })
    .to_string();
    connection
        .execute(
            "INSERT INTO orchestrator_tape (run_ulid, seq, event_type, payload_json)\
                 VALUES (?1, 1, 'tool_decision', ?2)",
            rusqlite::params![RUN_ID, tape_payload],
        )
        .expect("tape row should be inserted");
    connection
        .execute(
            "INSERT INTO journal_events (seq, run_ulid, kind, actor, redacted, payload_json)\
                 VALUES (1, ?1, 7, 2, 1, ?2)",
            rusqlite::params![
                RUN_ID,
                serde_json::json!({
                    "reason_code": "policy_recursion_limit",
                    "error": format!("token={ADMIN_TOKEN}")
                })
                .to_string()
            ],
        )
        .expect("journal row should be inserted");
    drop(connection);

    let workspace_file = sandbox.workspace().join("src/app.txt");
    fs::create_dir_all(workspace_file.parent().expect("workspace parent"))
        .expect("workspace parent should exist");
    fs::write(workspace_file.as_path(), "workspace contents must not persist in diagnostics")
        .expect("workspace fixture should be written");
    fs::write(sandbox.workspace().join(ADMIN_TOKEN), "secret-named artifact")
        .expect("secret-named artifact should be written");
    push_log_tail(&sandbox.log_tail, "policy evaluation diagnostics triggered fail closed");
    push_log_tail(&sandbox.log_tail, format!("token={ADMIN_TOKEN}").as_str());
    push_log_tail(&sandbox.log_tail, format!("journal at {}", data_dir.display()).as_str());
    push_log_tail(&sandbox.log_tail, "fallback at C:\\Users\\fixture-user\\private.log");

    assert!(sandbox.terminate_for_failure_diagnostics());
    assert!(state_root.exists(), "state must survive until diagnostics are persisted");
    let diagnostics =
        sandbox.failure_diagnostics("qa-runner.v3", "0.1.0", "qa.runner.stream_failed", true);
    let bytes =
        serde_json::to_vec_pretty(&diagnostics).expect("failure diagnostics should serialize");
    let text = String::from_utf8(bytes.clone()).expect("diagnostics should be UTF-8");
    let escaped_root = state_root.to_string_lossy().replace('\\', "\\\\");

    assert!(!sandbox.contains_secret(bytes.as_slice()));
    assert!(text.contains("policy evaluation diagnostics triggered fail closed"));
    assert!(text.contains("policy recursion limit reached"));
    assert!(text.contains("palyra.fs.apply_patch"));
    assert!(text.contains("policy_recursion_limit"));
    assert!(text.contains("src/app.txt"));
    assert!(text.contains("\"fault_sidecar\""));
    assert!(text.contains("\"status\": \"available\""));
    assert!(!text.contains(ADMIN_TOKEN));
    assert!(!text.contains(raw_capability.as_str()));
    assert!(!text.contains(QA_FAULT_CAPABILITY_PREFIX));
    assert!(!text.contains("workspace contents must not persist in diagnostics"));
    assert!(!text.contains(state_root.to_string_lossy().as_ref()));
    assert!(!text.contains(escaped_root.as_str()));
    assert!(!text.contains("C:\\Users\\fixture-user"));
    assert!(!text.contains("C:\\\\Users\\\\fixture-user"));

    assert!(sandbox.remove_state_root());
    assert!(!state_root.exists());
}

#[test]
fn failure_payload_completeness_tracks_every_projection_loss() {
    let (sandbox, _) = test_sandbox();
    let (fields, complete) =
        project_failure_payload(&sandbox, Some(r#"{"allowed":true,"reason":"policy_denied"}"#));
    assert!(complete);
    assert_eq!(fields.get("allowed"), Some(&Value::Bool(true)));

    for payload in [
        serde_json::json!({"input_json": {"reason": "denied"}}),
        serde_json::json!({"unknown": "unprojected"}),
        serde_json::json!({"reason": "x".repeat(MAX_FAILURE_TEXT_CHARS + 1)}),
        serde_json::json!({
            "reason": (0..=MAX_FAILURE_PAYLOAD_ARRAY_ITEMS).collect::<Vec<_>>()
        }),
    ] {
        let (_, complete) = project_failure_payload(&sandbox, Some(payload.to_string().as_str()));
        assert!(!complete, "projection loss must make payload_complete false: {payload}");
    }

    let mut fields = (0..MAX_FAILURE_PAYLOAD_FIELDS)
        .map(|index| (format!("field-{index}"), Value::Null))
        .collect::<Map<_, _>>();
    let mut complete = true;
    collect_failure_payload_fields(
        &sandbox,
        &serde_json::json!({"allowed": true}),
        0,
        &mut fields,
        &mut complete,
    );
    assert!(!complete, "field budget exhaustion must be explicit");

    let mut fields = Map::new();
    let mut complete = true;
    collect_failure_payload_fields(
        &sandbox,
        &serde_json::json!({"allowed": true}),
        MAX_FAILURE_PAYLOAD_DEPTH + 1,
        &mut fields,
        &mut complete,
    );
    assert!(!complete, "depth budget exhaustion must be explicit");
}

#[test]
fn diagnostic_path_redaction_recognizes_punctuation_and_unicode_whitespace() {
    let (sandbox, _) = test_sandbox();
    for diagnostic in [
        "journal->/home/palyra/journal.sqlite3",
        "journal→/home/palyra/journal.sqlite3",
        "journal\u{2003}/home/palyra/journal.sqlite3",
        r"journal->C:\Users\fixture-user\journal.sqlite3",
        r"journal->\\server\share\journal.sqlite3",
    ] {
        assert!(contains_absolute_path_marker(diagnostic), "path should be detected: {diagnostic}");
        assert_eq!(sandbox.sanitize_diagnostic_text(diagnostic), REDACTED_ABSOLUTE_PATH);
    }
    assert!(!contains_absolute_path_marker("journal=relative/path.sqlite3"));
}

#[test]
fn journal_text_columns_are_bounded_before_allocation() {
    const RUN_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAX";

    let (mut sandbox, state_root) = test_sandbox();
    assert!(sandbox.terminate_for_failure_diagnostics());
    sandbox.record_run_id(RUN_ID);
    let connection = create_test_failure_journal(state_root.as_path());
    connection
        .execute(
            "INSERT INTO orchestrator_runs (run_ulid, state, cancel_requested, last_error)\
                 VALUES (?1, 'in_progress', 0, ?2)",
            rusqlite::params![RUN_ID, "x".repeat(MAX_FAILURE_SQL_TEXT_BYTES + 1)],
        )
        .expect("run row should be inserted");
    connection
        .execute(
            "INSERT INTO orchestrator_tape (run_ulid, seq, event_type, payload_json)\
                 VALUES (?1, 1, ?2, '{}')",
            rusqlite::params![RUN_ID, "x".repeat(MAX_FAILURE_SQL_TEXT_BYTES + 1)],
        )
        .expect("oversized event type should be inserted");
    connection
        .execute(
            "INSERT INTO journal_events (seq, run_ulid, kind, actor, redacted, payload_json)\
                 VALUES (1, ?1, 1, 1, 1, ?2)",
            rusqlite::params![RUN_ID, "x".repeat(MAX_FAILURE_PAYLOAD_BYTES + 1)],
        )
        .expect("oversized payload should be inserted");
    drop(connection);

    let run = load_failure_run_projection(&sandbox, state_root.as_path(), RUN_ID)
        .expect("bounded immutable journal should open")
        .expect("run projection should exist");
    assert!(!run.tape_events_complete);
    assert!(run.tape_events.is_empty());
    assert!(run.last_error.is_none());
    assert!(!run.last_error_complete);
    assert!(run.journal_events_complete);
    assert_eq!(run.journal_events.len(), 1);
    assert!(!run.journal_events[0].payload_complete);
    assert!(!state_root.join("data/journal.sqlite3-wal").exists());
    assert!(!state_root.join("data/journal.sqlite3-shm").exists());
}

#[test]
fn last_error_completeness_tracks_secret_and_path_redaction() {
    const RUN_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FB0";
    const SECRET: &str = "qa-last-error-secret";

    let (mut sandbox, state_root) = test_sandbox();
    assert!(sandbox.terminate_for_failure_diagnostics());
    sandbox
        .secret_sentinels
        .push(SecretBytes::new(SECRET.as_bytes().to_vec()).expect("secret sentinel"));
    let connection = create_test_failure_journal(state_root.as_path());
    let absolute_path = state_root.join("private/journal.sqlite3");
    let last_error = format!("token={SECRET}; journal->{}", absolute_path.display());
    connection
        .execute(
            "INSERT INTO orchestrator_runs (run_ulid, state, cancel_requested, last_error)\
                 VALUES (?1, 'failed', 0, ?2)",
            rusqlite::params![RUN_ID, last_error],
        )
        .expect("run row should be inserted");
    drop(connection);

    let run = load_failure_run_projection(&sandbox, state_root.as_path(), RUN_ID)
        .expect("journal should project")
        .expect("run projection should exist");
    assert_eq!(run.last_error.as_deref(), Some(REDACTED_SECRET_SENTINEL));
    assert!(!run.last_error_complete);
    let projected = run.last_error.expect("last error should remain explicitly redacted");
    assert!(!projected.contains(SECRET));
    assert!(!projected.contains(state_root.to_string_lossy().as_ref()));
}

#[test]
fn read_only_journal_projection_reads_existing_wal_without_creation() {
    const RUN_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAZ";

    let (mut sandbox, state_root) = test_sandbox();
    assert!(sandbox.terminate_for_failure_diagnostics());
    let connection = create_test_failure_journal(state_root.as_path());
    connection
        .execute_batch("PRAGMA journal_mode = WAL; PRAGMA wal_autocheckpoint = 0;")
        .expect("WAL mode should be enabled");
    connection
        .execute(
            "INSERT INTO orchestrator_runs (run_ulid, state, cancel_requested, last_error)\
                 VALUES (?1, 'in_progress', 0, NULL)",
            [RUN_ID],
        )
        .expect("WAL-backed run should be inserted");
    let wal_path = state_root.join("data/journal.sqlite3-wal");
    let shm_path = state_root.join("data/journal.sqlite3-shm");
    let wal_bytes = fs::metadata(wal_path.as_path()).expect("WAL should exist").len();
    let shm_bytes = fs::metadata(shm_path.as_path()).expect("SHM should exist").len();

    let run = load_failure_run_projection(&sandbox, state_root.as_path(), RUN_ID)
        .expect("read-only reader should open the existing WAL set")
        .expect("committed WAL row should remain visible");
    assert_eq!(run.state, "in_progress");
    assert_eq!(fs::metadata(wal_path.as_path()).expect("WAL should remain").len(), wal_bytes);
    assert_eq!(fs::metadata(shm_path.as_path()).expect("SHM should remain").len(), shm_bytes);
    assert!(!state_root.join("data/journal.sqlite3-journal").exists());
    drop(connection);
}

#[test]
fn oversized_journal_state_fails_closed_before_string_allocation() {
    const RUN_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAY";

    let (mut sandbox, state_root) = test_sandbox();
    assert!(sandbox.terminate_for_failure_diagnostics());
    let connection = create_test_failure_journal(state_root.as_path());
    connection
        .execute(
            "INSERT INTO orchestrator_runs (run_ulid, state, cancel_requested, last_error)\
                 VALUES (?1, ?2, 0, NULL)",
            rusqlite::params![RUN_ID, "x".repeat(MAX_FAILURE_SQL_TEXT_BYTES + 1)],
        )
        .expect("oversized state should be inserted");
    drop(connection);

    let error = load_failure_run_projection(&sandbox, state_root.as_path(), RUN_ID)
        .expect_err("oversized state must make the journal projection unavailable");
    assert_eq!(error.to_string(), "qa.runner.failure_diagnostics_journal_text_invalid");
}

#[test]
fn journal_sidecars_are_individually_bounded() {
    let root = tempfile::tempdir().expect("journal root should exist");
    let data = root.path().join("data");
    fs::create_dir_all(data.as_path()).expect("journal directory should exist");
    fs::write(data.join("journal.sqlite3"), b"database")
        .expect("main database placeholder should exist");
    let shm =
        fs::File::create(data.join("journal.sqlite3-shm")).expect("SHM placeholder should exist");
    shm.set_len(MAX_FAILURE_JOURNAL_SHM_BYTES + 1).expect("sparse oversized SHM should be created");

    let error = validate_failure_journal_files(root.path())
        .expect_err("oversized SHM must make diagnostics unavailable");
    assert!(error.to_string().contains("qa.runner.failure_diagnostics_journal_invalid"));
}

#[test]
fn workspace_projection_rejects_a_hard_link_escape() {
    let (mut sandbox, state_root) = test_sandbox();
    assert!(sandbox.terminate_for_failure_diagnostics());
    let outside = state_root.join("outside-workspace-secret.txt");
    fs::write(outside.as_path(), b"outside workspace").expect("outside file should exist");
    fs::hard_link(outside.as_path(), sandbox.workspace().join("hard-link-escape"))
        .expect("hard-link escape should be created");

    let error = load_failure_workspace_projection(&sandbox)
        .expect_err("workspace hard-link escape must make diagnostics unavailable");
    assert!(error.to_string().contains("qa.runner.failure_diagnostics_workspace_hard_link_denied"));
}

#[test]
fn journal_projection_rejects_hard_links_for_database_wal_and_shm() {
    for target in ["journal.sqlite3", "journal.sqlite3-wal", "journal.sqlite3-shm"] {
        let root = tempfile::tempdir().expect("journal root should exist");
        let data = root.path().join("data");
        fs::create_dir_all(data.as_path()).expect("journal directory should exist");
        let outside = root.path().join("outside-journal-file");
        fs::write(outside.as_path(), b"hard-linked journal bytes")
            .expect("outside journal file should exist");
        if target == "journal.sqlite3" {
            fs::hard_link(outside.as_path(), data.join(target))
                .expect("hard-linked database should be created");
        } else {
            fs::write(data.join("journal.sqlite3"), b"database")
                .expect("main database should exist");
            for sidecar in ["journal.sqlite3-wal", "journal.sqlite3-shm"] {
                if sidecar == target {
                    fs::hard_link(outside.as_path(), data.join(sidecar))
                        .expect("hard-linked sidecar should be created");
                } else {
                    fs::write(data.join(sidecar), b"sidecar").expect("paired sidecar should exist");
                }
            }
        }

        let error = validate_failure_journal_files(root.path())
            .expect_err("journal hard links must make diagnostics unavailable");
        assert!(
            error.to_string().contains("qa.runner.failure_diagnostics_journal_changed"),
            "unexpected validation error for {target}: {error:#}"
        );
    }
}

#[test]
fn journal_snapshot_rejects_same_size_identity_swap() {
    let root = tempfile::tempdir().expect("journal root should exist");
    let data = root.path().join("data");
    fs::create_dir_all(data.as_path()).expect("journal directory should exist");
    let database = data.join("journal.sqlite3");
    let replacement = data.join("replacement.sqlite3");
    let displaced = data.join("displaced.sqlite3");
    fs::write(database.as_path(), b"database").expect("database should exist");
    fs::write(replacement.as_path(), b"replaced").expect("replacement should exist");
    assert_eq!(
        fs::metadata(database.as_path()).expect("database metadata").len(),
        fs::metadata(replacement.as_path()).expect("replacement metadata").len()
    );

    let error = materialize_failure_journal_snapshot_with_hook(root.path(), || {
        fs::rename(database.as_path(), displaced.as_path())
            .context("test database displacement failed")?;
        fs::rename(replacement.as_path(), database.as_path())
            .context("test database replacement failed")?;
        Ok(())
    })
    .expect_err("same-size inode replacement must invalidate the journal snapshot");
    assert!(error.to_string().contains("qa.runner.failure_diagnostics_journal_changed"));
}

#[test]
fn journal_snapshot_rejects_same_inode_same_size_rewrite() {
    let root = tempfile::tempdir().expect("journal root should exist");
    let data = root.path().join("data");
    fs::create_dir_all(data.as_path()).expect("journal directory should exist");
    let database = data.join("journal.sqlite3");
    fs::write(database.as_path(), b"database").expect("database should exist");
    let original = fs::File::open(database.as_path()).expect("database should open");
    let original_identity = open_file_identity(&original).expect("database identity");

    let error = materialize_failure_journal_snapshot_with_hook(root.path(), || {
        let mut rewritten = fs::OpenOptions::new()
            .write(true)
            .open(database.as_path())
            .context("test database rewrite open failed")?;
        rewritten.seek(SeekFrom::Start(0)).context("test database rewrite seek failed")?;
        rewritten.write_all(b"replaced").context("test database rewrite failed")?;
        rewritten.sync_all().context("test database rewrite sync failed")?;
        Ok(())
    })
    .expect_err("same-inode same-size rewrite must invalidate the journal snapshot");
    let rewritten = fs::File::open(database.as_path()).expect("rewritten database should open");
    assert_eq!(
        open_file_identity(&rewritten).expect("rewritten database identity"),
        original_identity
    );
    assert!(error.to_string().contains("qa.runner.failure_diagnostics_journal_changed"));
}

#[test]
fn journal_projection_rejects_materialized_snapshot_rewrite_before_query() {
    const RUN_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FB1";

    let (mut sandbox, state_root) = test_sandbox();
    assert!(sandbox.terminate_for_failure_diagnostics());
    let connection = create_test_failure_journal(state_root.as_path());
    connection
        .execute(
            "INSERT INTO orchestrator_runs (run_ulid, state, cancel_requested, last_error)\
                 VALUES (?1, 'in_progress', 0, NULL)",
            [RUN_ID],
        )
        .expect("run row should be inserted");
    drop(connection);

    let error =
        load_failure_run_projection_with_hook(&sandbox, state_root.as_path(), RUN_ID, |snapshot| {
            let database_path = snapshot.database_path()?.to_path_buf();
            let original_bytes =
                fs::read(database_path.as_path()).context("test snapshot database read failed")?;
            let mut rewritten = fs::OpenOptions::new()
                .write(true)
                .open(database_path.as_path())
                .context("test snapshot database rewrite open failed")?;
            rewritten
                .seek(SeekFrom::Start(0))
                .context("test snapshot database rewrite seek failed")?;
            let replacement = vec![0xA5; original_bytes.len()];
            rewritten
                .write_all(replacement.as_slice())
                .context("test snapshot database rewrite failed")?;
            rewritten.sync_all().context("test snapshot database rewrite sync failed")?;
            Ok(())
        })
        .expect_err("same-inode snapshot rewrite must invalidate the projection");

    assert!(error.to_string().contains("qa.runner.failure_diagnostics_journal_snapshot_changed"));
}

#[test]
fn journal_snapshot_cleanup_is_explicit_and_verified_inside_state_root() {
    let root = tempfile::tempdir().expect("journal root should exist");
    let data = root.path().join("data");
    fs::create_dir_all(data.as_path()).expect("journal directory should exist");
    fs::write(data.join("journal.sqlite3"), b"database").expect("database should exist");

    let snapshot = materialize_failure_journal_snapshot(root.path())
        .expect("journal snapshot should materialize");
    let snapshot_path = snapshot.path().expect("snapshot path should exist").to_path_buf();
    assert_eq!(snapshot_path.parent(), Some(root.path()));
    assert!(snapshot.database_path().expect("snapshot database path").exists());
    assert!(snapshot
        .path()
        .expect("snapshot path should exist")
        .file_name()
        .is_some_and(|name| name.to_string_lossy().starts_with(FAILURE_JOURNAL_SNAPSHOT_PREFIX)));

    snapshot.close_verified().expect("snapshot cleanup should be verified");
    assert!(!snapshot_path.exists());
}

#[test]
fn journal_snapshot_cleanup_rejects_path_substitution() {
    let root = tempfile::tempdir().expect("journal root should exist");
    let data = root.path().join("data");
    fs::create_dir_all(data.as_path()).expect("journal directory should exist");
    fs::write(data.join("journal.sqlite3"), b"database").expect("database should exist");
    let snapshot = materialize_failure_journal_snapshot(root.path())
        .expect("journal snapshot should materialize");
    let snapshot_path = snapshot.path().expect("snapshot path should exist").to_path_buf();
    let moved_snapshot = snapshot_path.with_file_name(format!(
        "{}-moved",
        snapshot_path.file_name().expect("snapshot should have a name").to_string_lossy()
    ));
    let error = snapshot
        .close_verified_with_hook(|snapshot_path| {
            fs::rename(snapshot_path, moved_snapshot.as_path())
                .context("snapshot should be displaced")?;
            fs::create_dir(snapshot_path).context("replacement snapshot should exist")?;
            Ok(())
        })
        .expect_err("cleanup must reject a substituted snapshot path");
    assert!(error
        .to_string()
        .contains("qa.runner.failure_diagnostics_journal_snapshot_cleanup_failed"));
    assert!(moved_snapshot.join("journal.sqlite3").exists());
    assert!(snapshot_path.exists());
    fs::remove_dir_all(snapshot_path.as_path()).expect("replacement snapshot cleanup");
    fs::remove_dir_all(moved_snapshot.as_path()).expect("moved snapshot cleanup");
}

#[cfg(unix)]
#[test]
fn workspace_and_journal_projections_reject_symlink_escape() {
    use std::os::unix::fs::symlink;

    let (mut sandbox, state_root) = test_sandbox();
    assert!(sandbox.terminate_for_failure_diagnostics());
    let outside = tempfile::tempdir().expect("outside root should exist");
    let outside_file = outside.path().join("host-secret.txt");
    fs::write(outside_file.as_path(), b"host-secret").expect("outside file should exist");
    symlink(outside_file.as_path(), sandbox.workspace().join("escape"))
        .expect("workspace symlink should exist");
    assert!(load_failure_workspace_projection(&sandbox).is_err());

    let data = state_root.join("data");
    fs::create_dir_all(data.as_path()).expect("journal directory should exist");
    fs::write(data.join("journal.sqlite3"), b"database")
        .expect("main database placeholder should exist");
    symlink(outside_file.as_path(), data.join("journal.sqlite3-wal"))
        .expect("journal WAL symlink should exist");
    assert!(validate_failure_journal_files(state_root.as_path()).is_err());

    let manifest = parse_scenario(FAULT_MUTATION_SCENARIO);
    let context = prepare_fault_context(state_root.as_path(), manifest.fault_injection.as_ref())
        .expect("fault context should materialize")
        .expect("fault context should exist");
    let launch = prepare_fault_launch(&context).expect("fault launch should materialize");
    symlink(outside_file.as_path(), context.evidence_path.as_path())
        .expect("fault evidence symlink should exist");
    assert!(load_fault_evidence_sidecar(&context, &launch.document).is_err());
}

#[cfg(windows)]
#[test]
fn workspace_and_journal_projections_reject_reparse_escape() {
    use std::os::windows::fs::symlink_file;

    let (mut sandbox, state_root) = test_sandbox();
    assert!(sandbox.terminate_for_failure_diagnostics());
    let outside = tempfile::tempdir().expect("outside root should exist");
    let outside_file = outside.path().join("host-secret.txt");
    fs::write(outside_file.as_path(), b"host-secret").expect("outside file should exist");
    let workspace_link = sandbox.workspace().join("escape");
    if let Err(error) = symlink_file(outside_file.as_path(), workspace_link.as_path()) {
        if error.kind() == io::ErrorKind::PermissionDenied {
            return;
        }
        panic!("workspace reparse point should be created: {error}");
    }
    assert!(load_failure_workspace_projection(&sandbox).is_err());

    let data = state_root.join("data");
    fs::create_dir_all(data.as_path()).expect("journal directory should exist");
    fs::write(data.join("journal.sqlite3"), b"database")
        .expect("main database placeholder should exist");
    symlink_file(outside_file.as_path(), data.join("journal.sqlite3-wal"))
        .expect("journal WAL reparse point should be created");
    assert!(validate_failure_journal_files(state_root.as_path()).is_err());

    let manifest = parse_scenario(FAULT_MUTATION_SCENARIO);
    let context = prepare_fault_context(state_root.as_path(), manifest.fault_injection.as_ref())
        .expect("fault context should materialize")
        .expect("fault context should exist");
    let launch = prepare_fault_launch(&context).expect("fault launch should materialize");
    symlink_file(outside_file.as_path(), context.evidence_path.as_path())
        .expect("fault evidence reparse point should be created");
    assert!(load_fault_evidence_sidecar(&context, &launch.document).is_err());
}
