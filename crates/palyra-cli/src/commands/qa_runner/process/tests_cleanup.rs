use super::*;

#[test]
fn shutdown_kills_worker_and_removes_workspace() {
    let (mut sandbox, root_path) = test_sandbox();

    let shutdown = sandbox.shutdown();

    assert!(shutdown.daemon_terminated);
    assert!(shutdown.workspace_removed);
    assert!(!root_path.exists());
}

#[test]
fn log_drain_timeout_retains_ownership_for_bounded_retry() {
    let (release_tx, release_rx) = mpsc::channel();
    let blocked_reader = thread::spawn(move || {
        let _ = release_rx.recv();
    });
    let mut readers = vec![blocked_reader];
    let started = Instant::now();

    let first = join_owned_log_threads_bounded(&mut readers, Duration::from_millis(20));

    assert!(!first.all_joined);
    assert!(!first.join_failed);
    assert_eq!(readers.len(), 1, "unfinished log drain must remain owned");
    assert!(started.elapsed() < Duration::from_millis(500));
    release_tx.send(()).expect("blocked reader should still be owned");
    let second = join_owned_log_threads_bounded(&mut readers, Duration::from_secs(1));
    assert!(second.all_joined);
    assert!(!second.join_failed);
    assert!(readers.is_empty());
}

#[test]
fn startup_cleanup_failure_retains_process_and_log_ownership() {
    let process = long_running_test_process();
    let process_id = process.child.id();
    let (state_root, state_root_path) = shared_test_state_root();
    let (release_tx, release_rx) = mpsc::channel();
    let blocked_reader = thread::spawn(move || {
        let _ = release_rx.recv();
    });
    let mut ownership = StartupCleanupOwnership {
        process: Some(process),
        log_threads: vec![blocked_reader],
        log_join_failed: false,
        state_root: Some(state_root),
    };

    let first = ownership.attempt(|_| false, Duration::from_millis(20));

    assert!(!first.resources_released);
    assert!(!first.daemon_terminated);
    assert!(ownership.process.is_some(), "failed cleanup must retain the process handle");
    assert_eq!(ownership.log_threads.len(), 1, "failed cleanup must retain log handles");
    assert!(process_is_alive(process_id, Duration::from_secs(1)));
    assert!(state_root_path.exists(), "state root must outlive process and log cleanup");

    release_tx.send(()).expect("blocked reader should be released");
    let second = ownership
        .attempt(|process| process.terminate_tree(Duration::from_secs(2)), Duration::from_secs(1));
    assert!(second.resources_released);
    assert!(second.daemon_terminated);
    assert!(second.log_threads_joined);
    assert!(!second.log_join_failed);
    assert!(second.state_root_removed);
    assert!(wait_for_process_exit(process_id, Duration::from_secs(2)));
    assert!(!state_root_path.exists());
}

#[test]
fn delegated_startup_cleanup_blocks_external_state_root_removal() {
    let (state_root, state_root_path) = shared_test_state_root();
    lock_unpoisoned(&state_root).startup_cleanup_delegated = true;
    assert!(!lock_unpoisoned(&state_root).remove_verified());
    assert!(state_root_path.exists());
    let mut ownership = StartupCleanupOwnership {
        process: None,
        log_threads: Vec::new(),
        log_join_failed: false,
        state_root: Some(Arc::clone(&state_root)),
    };

    let cleanup = ownership.attempt(|_| true, Duration::from_millis(1));
    assert!(cleanup.resources_released);
    assert!(cleanup.state_root_removed);
    assert!(!state_root_path.exists());
}

#[test]
fn startup_reaper_spawn_failure_keeps_queue_owned_and_recoverable() {
    let (state_root, state_root_path) = shared_test_state_root();
    let ownership = StartupCleanupOwnership {
        process: None,
        log_threads: Vec::new(),
        log_join_failed: false,
        state_root: Some(state_root),
    };
    let reaper = Arc::new(Mutex::new(StartupCleanupReaperState::default()));
    let admission = acquire_startup_cleanup_admission_with(Arc::clone(&reaper))
        .expect("cleanup slot should be admitted");

    let worker_started = register_startup_cleanup_with(&admission, ownership, |_| {
        Err(io::Error::other("injected worker spawn failure"))
    })
    .unwrap_or_else(|_| panic!("matching admission should accept cleanup ownership"));

    assert!(!worker_started);
    {
        let state = lock_unpoisoned(&reaper);
        assert!(!state.worker_running);
        assert!(state.pending.is_some());
        assert!(state.retained_failure.is_none());
    }
    assert!(state_root_path.exists(), "failed worker spawn must retain root ownership");

    drive_startup_cleanup_reaper_inline(Arc::clone(&reaper));
    let state = lock_unpoisoned(&reaper);
    assert!(!state.worker_running);
    assert!(state.pending.is_none());
    assert!(state.retained_failure.is_none());
    assert!(state.admitted_generation.is_none());
    assert!(!state_root_path.exists());
}

#[test]
fn startup_admission_bounds_repeated_cleanup_ownership() {
    let reaper = Arc::new(Mutex::new(StartupCleanupReaperState::default()));
    let admission = acquire_startup_cleanup_admission_with(Arc::clone(&reaper))
        .expect("first startup should reserve the cleanup slot");

    for _ in 0..24 {
        let error = acquire_startup_cleanup_admission_with(Arc::clone(&reaper))
            .err()
            .expect("additional startups must fail before creating sensitive roots");
        assert!(error.to_string().contains("qa.runner.daemon_start_cleanup_quarantined"));
    }
    {
        let state = lock_unpoisoned(&reaper);
        assert!(state.pending.is_none());
        assert!(state.retained_failure.is_none());
        assert_eq!(state.admitted_generation, Some(admission.generation));
    }

    drop(admission);
    let replacement = acquire_startup_cleanup_admission_with(Arc::clone(&reaper))
        .expect("releasing a clean admission should permit one new startup");
    drop(replacement);
}

#[test]
fn stale_admission_drop_cannot_release_a_new_generation() {
    let reaper = Arc::new(Mutex::new(StartupCleanupReaperState::default()));
    let admission_a = acquire_startup_cleanup_admission_with(Arc::clone(&reaper))
        .expect("generation A should be admitted");
    let (state_root, state_root_path) = shared_test_state_root();
    let ownership = StartupCleanupOwnership {
        process: None,
        log_threads: Vec::new(),
        log_join_failed: false,
        state_root: Some(state_root),
    };

    let worker_started = register_startup_cleanup_with(&admission_a, ownership, |job| {
        job();
        Ok(())
    })
    .unwrap_or_else(|_| panic!("generation A should transfer its cleanup ownership"));
    assert!(worker_started);
    assert!(!state_root_path.exists());
    assert!(lock_unpoisoned(&reaper).admitted_generation.is_none());

    let admission_b = acquire_startup_cleanup_admission_with(Arc::clone(&reaper))
        .expect("generation B should be admitted after A cleanup");
    let generation_b = admission_b.generation;
    drop(admission_a);

    assert_eq!(lock_unpoisoned(&reaper).admitted_generation, Some(generation_b));
    assert!(
        acquire_startup_cleanup_admission_with(Arc::clone(&reaper)).is_err(),
        "generation C must remain denied while generation B is live"
    );
    drop(admission_b);
}

#[test]
fn cleanup_registration_never_overwrites_an_occupied_slot() {
    let reaper = Arc::new(Mutex::new(StartupCleanupReaperState::default()));
    let admission = acquire_startup_cleanup_admission_with(Arc::clone(&reaper))
        .expect("cleanup slot should be admitted");
    let (first_root, first_root_path) = shared_test_state_root();
    let first_root_observer = Arc::clone(&first_root);
    let first = StartupCleanupOwnership {
        process: None,
        log_threads: Vec::new(),
        log_join_failed: false,
        state_root: Some(first_root),
    };
    register_startup_cleanup_with(&admission, first, |_| Ok(()))
        .unwrap_or_else(|_| panic!("first ownership should occupy the cleanup slot"));

    let (second_root, second_root_path) = shared_test_state_root();
    let second = StartupCleanupOwnership {
        process: None,
        log_threads: Vec::new(),
        log_join_failed: false,
        state_root: Some(second_root),
    };
    let rejected = register_startup_cleanup_with(&admission, second, |_| {
        panic!("an occupied slot must reject before spawning another worker")
    })
    .expect_err("second ownership must be returned to the caller");

    let mut state = lock_unpoisoned(&reaper);
    let pending_root = state
        .pending
        .as_ref()
        .and_then(|registered| registered.ownership.state_root.as_ref())
        .expect("first ownership should remain pending");
    assert!(Arc::ptr_eq(pending_root, &first_root_observer));
    assert!(state.quarantined);
    let pending = state.pending.take();
    state.worker_running = false;
    state.admitted_generation = None;
    drop(state);
    drop(pending);
    drop(rejected);
    drop(first_root_observer);
    drop(admission);
    drop(reaper);
    fs::remove_dir_all(first_root_path).expect("first synthetic cleanup root should be removed");
    fs::remove_dir_all(second_root_path).expect("second synthetic cleanup root should be removed");
}

#[test]
fn startup_reaper_bounds_unresolved_cleanup_and_retains_ownership() {
    let (state_root, state_root_path) = shared_test_state_root();
    let moved_root = state_root_path.with_file_name(format!(
        "{}-reaper-moved",
        state_root_path.file_name().expect("state root should have a name").to_string_lossy()
    ));
    fs::rename(state_root_path.as_path(), moved_root.as_path())
        .expect("state root should be displaced");
    fs::create_dir(state_root_path.as_path()).expect("replacement root should exist");
    let ownership = StartupCleanupOwnership {
        process: None,
        log_threads: Vec::new(),
        log_join_failed: false,
        state_root: Some(state_root),
    };
    let reaper = Arc::new(Mutex::new(StartupCleanupReaperState::default()));
    let admission = acquire_startup_cleanup_admission_with(Arc::clone(&reaper))
        .expect("cleanup slot should be admitted");

    let worker_started = register_startup_cleanup_with(&admission, ownership, |job| {
        job();
        Ok(())
    })
    .unwrap_or_else(|_| panic!("matching admission should accept cleanup ownership"));

    assert!(worker_started);
    {
        let state = lock_unpoisoned(&reaper);
        assert!(!state.worker_running);
        assert!(state.pending.is_none());
        assert!(state.retained_failure.is_some());
        assert!(state.quarantined);
        assert_eq!(state.admitted_generation, Some(admission.generation));
    }
    assert!(acquire_startup_cleanup_admission_with(Arc::clone(&reaper)).is_err());
    assert!(moved_root.exists());
    assert!(state_root_path.exists());
    drop(reaper);
    fs::remove_dir_all(state_root_path.as_path()).expect("replacement root cleanup");
    fs::remove_dir_all(moved_root.as_path()).expect("moved root cleanup");
}

#[test]
fn post_start_cleanup_failure_transfers_into_the_bounded_quarantine() {
    let reaper = Arc::new(Mutex::new(StartupCleanupReaperState::default()));
    let admission = acquire_startup_cleanup_admission_with(Arc::clone(&reaper))
        .expect("sandbox lifecycle should reserve the cleanup slot");
    let (mut sandbox, state_root_path) = test_sandbox();
    sandbox.cleanup_admission = Some(admission);
    assert!(sandbox.terminate_for_failure_diagnostics());
    let moved_root = state_root_path.with_file_name(format!(
        "{}-post-start-moved",
        state_root_path.file_name().expect("state root should have a name").to_string_lossy()
    ));
    fs::rename(state_root_path.as_path(), moved_root.as_path())
        .expect("state root should be displaced");
    fs::create_dir(state_root_path.as_path()).expect("replacement root should exist");

    drop(sandbox);

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if !lock_unpoisoned(&reaper).worker_running {
            break;
        }
        assert!(Instant::now() < deadline, "post-start cleanup worker should be bounded");
        thread::sleep(SHUTDOWN_POLL_INTERVAL);
    }
    {
        let state = lock_unpoisoned(&reaper);
        assert!(state.pending.is_none());
        assert!(state.retained_failure.is_some());
        assert!(state.quarantined);
        assert!(state.admitted_generation.is_some());
    }
    for _ in 0..24 {
        assert!(
            acquire_startup_cleanup_admission_with(Arc::clone(&reaper)).is_err(),
            "repeated post-start failures must not allocate another cleanup slot"
        );
    }

    drop(reaper);
    fs::remove_dir_all(state_root_path.as_path()).expect("replacement root cleanup");
    fs::remove_dir_all(moved_root.as_path()).expect("moved root cleanup");
}

#[cfg(windows)]
#[test]
fn attach_failure_returns_suspended_child_for_verified_cleanup() {
    let (state_root, state_root_path) = shared_test_state_root();
    let mut command = Command::new("powershell.exe");
    command.args(["-NoProfile", "-NonInteractive", "-Command", "Start-Sleep -Seconds 30"]);
    let _preparation = configure_daemon_process_tree(&mut command)
        .expect("attach failure process tree should configure");
    let child = command.spawn().expect("suspended attach fixture should start");
    let process_id = child.id();
    let failure = match attach_windows_daemon_process_tree_with(child, || {
        Err(io::Error::other("injected job creation failure"))
    }) {
        Ok(_) => panic!("job creation failure should return process ownership"),
        Err(failure) => failure,
    };
    let AttachDaemonProcessFailure { error, process } = *failure;
    assert!(error.to_string().contains("qa.runner.daemon_job_create_failed"));
    let mut ownership = StartupCleanupOwnership {
        process: Some(process),
        log_threads: Vec::new(),
        log_join_failed: false,
        state_root: Some(state_root),
    };

    let cleanup = ownership
        .attempt(|process| process.terminate_tree(Duration::from_secs(2)), Duration::from_secs(1));
    assert!(cleanup.resources_released);
    assert!(cleanup.daemon_terminated);
    assert!(cleanup.state_root_removed);
    assert!(wait_for_process_exit(process_id, Duration::from_secs(2)));
    assert!(!state_root_path.exists());
}

#[test]
fn daemon_tree_cleanup_terminates_a_grandchild() {
    let root = tempfile::tempdir().expect("grandchild fixture root should exist");
    let pid_path = root.path().join("grandchild.pid");
    let mut process = test_process_with_grandchild(pid_path.as_path());
    let parent_id = process.child.id();
    let grandchild_id = wait_for_recorded_process_id(pid_path.as_path(), Duration::from_secs(5));
    assert!(process_is_alive(parent_id, Duration::from_secs(1)));
    assert!(process_is_alive(grandchild_id, Duration::from_secs(1)));

    assert!(process.terminate_tree(Duration::from_secs(5)));
    assert!(wait_for_process_exit(parent_id, Duration::from_secs(2)));
    assert!(wait_for_process_exit(grandchild_id, Duration::from_secs(5)));
}

#[cfg(unix)]
#[test]
fn daemon_tree_cleanup_terminates_a_setsid_grandchild() {
    let root = tempfile::tempdir().expect("escaped-grandchild fixture root should exist");
    let pid_path = root.path().join("escaped-grandchild.pid");
    let mut process = test_process_with_escaped_grandchild(pid_path.as_path(), false);
    let parent_id = process.child.id();
    let grandchild_id = wait_for_recorded_process_id(pid_path.as_path(), Duration::from_secs(5));
    assert!(process_is_alive(parent_id, Duration::from_secs(1)));
    assert!(process_is_alive(grandchild_id, Duration::from_secs(1)));

    assert!(process.terminate_tree(Duration::from_secs(5)));
    assert!(wait_for_process_exit(parent_id, Duration::from_secs(2)));
    assert!(wait_for_process_exit(grandchild_id, Duration::from_secs(5)));
}

#[cfg(unix)]
#[test]
fn unix_process_enumeration_rejects_an_expired_cleanup_deadline() {
    let started = Instant::now();
    let error = unix_process_table(Instant::now())
        .expect_err("an expired deadline must fail before process enumeration");

    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert!(started.elapsed() < Duration::from_millis(250));
}

#[cfg(any(target_os = "linux", target_os = "android"))]
#[test]
fn linux_process_identity_parser_handles_closing_parentheses_in_command_name() {
    let mut fields = vec!["S".to_owned()];
    for index in 1..=19 {
        fields.push(match index {
            1 => "7".to_owned(),
            2 => "9".to_owned(),
            19 => "123456".to_owned(),
            _ => "0".to_owned(),
        });
    }
    let stat = format!("42 (worker ) nested) {}", fields.join(" "));

    let snapshot = parse_linux_process_stat(42, stat.as_bytes(), 1000)
        .expect("process stat should preserve the final command terminator");
    assert_eq!(snapshot.identity.process_id, 42);
    assert_eq!(snapshot.identity.start_token_low, 123456);
    assert_eq!(snapshot.parent_id, 7);
    assert_eq!(snapshot.process_group_id, 9);
}

#[cfg(unix)]
#[test]
fn stale_tracked_process_identity_is_never_signaled() {
    let expected =
        UnixProcessIdentity { process_id: 4242, start_token_high: 10, start_token_low: 20 };
    let replacement = UnixProcessIdentity { start_token_low: 21, ..expected };
    let signal_sent = std::cell::Cell::new(false);

    let signaled = unix_signal_process_identity_with(
        &expected,
        UNIX_SIGKILL,
        |_| Ok(Some(replacement)),
        |_, _| {
            signal_sent.set(true);
            Ok(true)
        },
    )
    .expect("identity mismatch should be handled without signaling");

    assert!(!signaled);
    assert!(!signal_sent.get());
}

#[cfg(any(target_os = "linux", target_os = "android"))]
#[test]
fn marker_scan_targets_only_unclassified_processes() {
    let root = UnixProcessIdentity { process_id: 42, start_token_high: 10, start_token_low: 20 };
    let empty_baseline = BTreeMap::new();
    let empty_descendants = BTreeMap::new();
    let snapshot = |identity, owner_id| UnixProcessSnapshot {
        identity,
        parent_id: 1,
        process_group_id: 1,
        owner_id,
    };
    let older = snapshot(UnixProcessIdentity { start_token_low: 19, ..root }, 1000);
    let same_start = snapshot(UnixProcessIdentity { process_id: 43, ..root }, 1000);
    let newer = snapshot(
        UnixProcessIdentity { process_id: 44, start_token_high: 11, start_token_low: 0 },
        1000,
    );
    let root_snapshot = snapshot(root, 1000);
    let other_owner = snapshot(UnixProcessIdentity { process_id: 45, ..root }, 1001);
    let requires_scan = |candidate, baseline, descendants| {
        unix_process_requires_marker_scan(candidate, &root, baseline, descendants, 1000)
    };

    assert!(!requires_scan(&older, &empty_baseline, &empty_descendants));
    assert!(!requires_scan(&root_snapshot, &empty_baseline, &empty_descendants));
    assert!(requires_scan(&same_start, &empty_baseline, &empty_descendants));
    assert!(requires_scan(&newer, &empty_baseline, &empty_descendants));
    assert!(!requires_scan(&other_owner, &empty_baseline, &empty_descendants));

    let preexisting = BTreeMap::from([(same_start.identity.process_id, same_start.identity)]);
    assert!(!requires_scan(&same_start, &preexisting, &empty_descendants));
    let reused_pid =
        snapshot(UnixProcessIdentity { start_token_low: 21, ..same_start.identity }, 1000);
    assert!(requires_scan(&reused_pid, &preexisting, &empty_descendants));

    let known_descendants = BTreeMap::from([(same_start.identity.process_id, same_start.identity)]);
    assert!(!requires_scan(&same_start, &empty_baseline, &known_descendants));
    assert!(requires_scan(&reused_pid, &empty_baseline, &known_descendants));
}

#[cfg(target_os = "macos")]
#[test]
fn marker_scan_does_not_order_wall_clock_start_tokens() {
    let root = UnixProcessIdentity { process_id: 42, start_token_high: 10, start_token_low: 20 };
    let empty_baseline = BTreeMap::new();
    let empty_descendants = BTreeMap::new();
    let older_same_owner = UnixProcessSnapshot {
        identity: UnixProcessIdentity { process_id: 43, start_token_low: 19, ..root },
        parent_id: 1,
        process_group_id: 1,
        owner_id: 1000,
    };
    let older_other_owner = UnixProcessSnapshot { owner_id: 1001, ..older_same_owner };
    let requires_scan = |candidate| {
        unix_process_requires_marker_scan(
            candidate,
            &root,
            &empty_baseline,
            &empty_descendants,
            1000,
        )
    };

    assert!(requires_scan(&older_same_owner));
    assert!(!requires_scan(&older_other_owner));
}

#[cfg(target_os = "macos")]
#[test]
fn mac_baseline_leaves_protected_processes_unclassified() {
    let owned = UnixProcessSnapshot {
        identity: UnixProcessIdentity { process_id: 41, start_token_high: 10, start_token_low: 20 },
        parent_id: 1,
        process_group_id: 41,
        owner_id: 1000,
    };
    let other_owner = UnixProcessSnapshot {
        identity: UnixProcessIdentity { process_id: 42, ..owned.identity },
        owner_id: 1001,
        ..owned
    };

    let baseline = mac_process_baseline_with(&[41, 42, 43], 1000, |process_id| match process_id {
        41 => Ok(Some(owned)),
        42 => Ok(Some(other_owner)),
        43 => Err(io::Error::new(io::ErrorKind::PermissionDenied, "protected process")),
        _ => Ok(None),
    })
    .expect("protected processes should remain outside the baseline");

    assert_eq!(baseline, BTreeMap::from([(owned.identity.process_id, owned.identity)]));
    let error = mac_process_baseline_with(&[44], 1000, |_| {
        Err(io::Error::new(io::ErrorKind::InvalidData, "invalid process metadata"))
    })
    .expect_err("non-permission lookup errors must remain fatal");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
}

#[cfg(unix)]
#[test]
fn recycled_root_and_tracked_pids_do_not_seed_unrelated_descendants() {
    let expected_root =
        UnixProcessIdentity { process_id: 4100, start_token_high: 10, start_token_low: 1 };
    let expected_tracked =
        UnixProcessIdentity { process_id: 4200, start_token_high: 10, start_token_low: 2 };
    let recycled_root = UnixProcessIdentity { start_token_low: 11, ..expected_root };
    let recycled_tracked = UnixProcessIdentity { start_token_low: 12, ..expected_tracked };
    let unrelated_root_child =
        UnixProcessIdentity { process_id: 4300, start_token_high: 10, start_token_low: 3 };
    let unrelated_tracked_child =
        UnixProcessIdentity { process_id: 4400, start_token_high: 10, start_token_low: 4 };
    let process_table = vec![
        UnixProcessSnapshot {
            identity: recycled_root,
            parent_id: 1,
            process_group_id: recycled_root.process_id,
            owner_id: 1000,
        },
        UnixProcessSnapshot {
            identity: recycled_tracked,
            parent_id: 1,
            process_group_id: recycled_tracked.process_id,
            owner_id: 1000,
        },
        UnixProcessSnapshot {
            identity: unrelated_root_child,
            parent_id: recycled_root.process_id,
            process_group_id: recycled_root.process_id,
            owner_id: 1000,
        },
        UnixProcessSnapshot {
            identity: unrelated_tracked_child,
            parent_id: recycled_tracked.process_id,
            process_group_id: recycled_tracked.process_id,
            owner_id: 1000,
        },
    ];
    let tracked = BTreeMap::from([(expected_tracked.process_id, expected_tracked)]);

    let roots = unix_identity_matching_roots(&process_table, &expected_root, &tracked);
    let descendants = unix_recursive_descendants(&process_table, &roots);

    assert!(roots.is_empty());
    assert!(descendants.is_empty());
}

#[cfg(unix)]
#[test]
fn liveness_eof_does_not_hide_a_marker_bound_double_fork() {
    let root = tempfile::tempdir().expect("escaped-grandchild fixture root should exist");
    let pid_path = root.path().join("closed-liveness-orphan.pid");
    let mut process = test_process_with_escape_mode(pid_path.as_path(), "detached_close_fds");
    let orphan_id = wait_for_recorded_process_id(pid_path.as_path(), Duration::from_secs(5));
    assert!(wait_for_child_exit(&mut process.child, Duration::from_secs(5)));
    let tree = process.tree.as_ref().expect("Unix process tree should remain owned");
    let eof_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if unix_descendant_liveness_closed(&tree.descendant_liveness_read)
            .expect("liveness pipe should remain readable")
        {
            break;
        }
        assert!(Instant::now() < eof_deadline, "closed-FD leaf should release liveness pipe");
        thread::sleep(SHUTDOWN_POLL_INTERVAL);
    }
    assert!(process_is_alive(orphan_id, Duration::from_secs(1)));

    tree.freeze_recursive_descendants(Instant::now() + Duration::from_secs(5))
        .expect("marker scan should discover the escaped leaf");
    assert!(!tree
        .wait_until_inactive(Instant::now() + Duration::from_millis(100))
        .expect("live marker-bound leaf must not be reported inactive"));
    assert!(process.terminate_tree(Duration::from_secs(5)));
    assert!(wait_for_process_exit(orphan_id, Duration::from_secs(5)));
}

#[cfg(unix)]
#[test]
fn daemon_tree_cleanup_recovers_a_marker_bound_detached_orphan() {
    let (mut sandbox, state_root_path) = test_sandbox();
    assert!(sandbox.terminate_for_failure_diagnostics());
    let pid_path = state_root_path.join("detached-orphan.pid");
    sandbox.child = Some(test_process_with_escaped_grandchild(pid_path.as_path(), true));
    let orphan_id = wait_for_recorded_process_id(pid_path.as_path(), Duration::from_secs(5));
    assert!(process_is_alive(orphan_id, Duration::from_secs(1)));
    assert!(
        wait_for_child_exit(
            &mut sandbox.child.as_mut().expect("launcher should be owned").child,
            Duration::from_secs(5),
        ),
        "detached launcher should exit before cleanup discovery"
    );

    let shutdown = sandbox.shutdown();
    assert!(shutdown.daemon_terminated);
    assert!(shutdown.workspace_removed);
    assert!(wait_for_process_exit(orphan_id, Duration::from_secs(5)));
    assert!(!state_root_path.exists());
}

#[test]
fn state_root_removal_is_retry_safe_while_child_is_owned() {
    let (mut sandbox, root_path) = test_sandbox();

    assert!(!sandbox.remove_state_root());
    assert!(
        !lock_unpoisoned(&sandbox.state_root).is_removed(),
        "failed removal must retain TempDir ownership"
    );
    assert!(root_path.exists());

    assert!(sandbox.terminate_for_failure_diagnostics());
    assert!(sandbox.remove_state_root());
    assert!(lock_unpoisoned(&sandbox.state_root).is_removed());
    assert!(!root_path.exists());
}

#[test]
fn state_root_path_substitution_cannot_report_workspace_removed() {
    let (mut sandbox, root_path) = test_sandbox();
    assert!(sandbox.terminate_for_failure_diagnostics());
    let manifest = parse_scenario(FAULT_MUTATION_SCENARIO);
    sandbox.launch.fault =
        prepare_fault_context(root_path.as_path(), manifest.fault_injection.as_ref())
            .expect("fault context should materialize");
    let moved_root = root_path.with_file_name(format!(
        "{}-moved",
        root_path.file_name().expect("state root should have a name").to_string_lossy()
    ));
    fs::rename(root_path.as_path(), moved_root.as_path())
        .expect("pinned state root should be renamed");
    fs::create_dir(root_path.as_path()).expect("replacement state root should be created");

    let restart_error = sandbox
        .restart_preserving_state(Duration::from_secs(1))
        .expect_err("restart must reject a substituted state root before spawning");
    assert!(restart_error.to_string().contains("qa.runner.restart_state_root_identity_invalid"));
    assert!(sandbox.child.is_none());
    assert_eq!(sandbox.daemon_restarts(), 0);
    assert!(sandbox.fault_evidence_sidecar().is_err());
    let diagnostics = sandbox.failure_diagnostics("qa-runner.v4", "0.1.0", "qa.runner.test", true);
    assert_eq!(diagnostics.fault_sidecar.status, "unavailable");
    assert_eq!(
        diagnostics.fault_sidecar.reason_code,
        Some("qa.runner.failure_diagnostics_state_root_identity_invalid")
    );
    assert_eq!(diagnostics.journal.status, "unavailable");
    assert_eq!(
        diagnostics.journal.reason_code,
        Some("qa.runner.failure_diagnostics_state_root_identity_invalid")
    );
    assert_eq!(diagnostics.workspace.status, "unavailable");
    assert_eq!(
        diagnostics.workspace.reason_code,
        Some("qa.runner.failure_diagnostics_state_root_identity_invalid")
    );
    assert!(!sandbox.remove_state_root());
    assert!(lock_unpoisoned(&sandbox.state_root).path_substituted);
    assert!(moved_root.join("workspace").exists(), "the pinned tree must remain observable");
    assert!(root_path.exists(), "the replacement tree must not be deleted");

    drop(sandbox);
    assert!(moved_root.exists(), "drop must not hide cleanup failure for the pinned tree");
    assert!(root_path.exists(), "drop must not delete the substituted path");
    fs::remove_dir_all(root_path.as_path()).expect("replacement root should be cleaned up");
    fs::remove_dir_all(moved_root.as_path()).expect("moved state root should be cleaned up");
}

#[cfg(windows)]
#[test]
fn state_root_delete_failure_retains_tempdir_for_retry() {
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem::{FILE_SHARE_READ, FILE_SHARE_WRITE};

    let (mut sandbox, root_path) = test_sandbox();
    assert!(sandbox.terminate_for_failure_diagnostics());
    let locked_path = sandbox.workspace().join("locked.txt");
    fs::write(locked_path.as_path(), b"locked").expect("locked fixture should exist");
    let locked = fs::OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .open(locked_path.as_path())
        .expect("fixture should be opened without delete sharing");

    assert!(!sandbox.remove_state_root());
    assert!(
        !lock_unpoisoned(&sandbox.state_root).is_removed(),
        "failed filesystem removal must remain retryable"
    );
    assert!(root_path.exists());

    drop(locked);
    assert!(sandbox.remove_state_root());
    assert!(lock_unpoisoned(&sandbox.state_root).is_removed());
    assert!(!root_path.exists());
}

#[test]
fn panic_unwind_still_removes_isolated_workspace() {
    let (sandbox, root_path) = test_sandbox();
    let process_id =
        sandbox.child.as_ref().expect("sandbox should own its child before unwind").child.id();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _sandbox = sandbox;
        panic!("exercise QA sandbox unwind cleanup");
    }));

    assert!(outcome.is_err());
    assert!(
        wait_for_process_exit(process_id, Duration::from_secs(2)),
        "sandbox child {process_id} should be dead after unwind"
    );
    assert!(!root_path.exists());
}
