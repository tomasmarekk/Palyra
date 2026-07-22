use super::*;

#[test]
fn parse_port_requires_a_complete_socket_address() {
    assert_eq!(
        parse_port_from_log(r#"{"listen_addr":"127.0.0.1:43210"}"#, "\"listen_addr\":\""),
        Some(43_210)
    );
    assert_eq!(parse_port_from_log("listen_addr=43210", "\"listen_addr\":\""), None);
}

#[test]
fn port_reader_eof_does_not_mask_an_early_daemon_exit() {
    #[cfg(windows)]
    let mut child = Command::new("cmd.exe")
        .args(["/D", "/C", "exit", "7"])
        .spawn()
        .expect("short-lived Windows child should start");
    #[cfg(not(windows))]
    let mut child = Command::new("sh")
        .args(["-c", "exit 7"])
        .spawn()
        .expect("short-lived Unix child should start");
    let status = child.wait().expect("short-lived child should exit");
    assert!(!status.success());
    let (sender, receiver) = mpsc::sync_channel(1);
    sender
        .send(Err("qa.runner.daemon_ports_not_published".to_owned()))
        .expect("reader failure should be queued");
    drop(sender);
    let log_tail = Mutex::new(VecDeque::from(["secret-shaped diagnostic".to_owned()]));

    let error = wait_for_listen_ports(
        &receiver,
        &mut child,
        &log_tail,
        Instant::now() + Duration::from_secs(1),
    )
    .expect_err("early exit should fail startup");
    let message = error.to_string();

    assert!(message.starts_with("qa.runner.daemon_exited_early:"));
    assert!(message.contains("diagnostics=captured_lines=1"));
    assert!(!message.contains("secret-shaped diagnostic"));
}

#[test]
fn daemon_health_requires_current_contract_handshake_and_git_hash() {
    let body = serde_json::json!({
        "service": "palyrad",
        "status": "ok",
        "version": "0.1.0",
        "git_hash": "abcdef123456",
        "build_profile": "debug",
        "uptime_seconds": 1,
        "public_runtime_contract_version": PUBLIC_RUNTIME_CONTRACT_SNAPSHOT_VERSION,
        "qa_scenario_schema_version": QA_SCENARIO_SCHEMA_VERSION,
        "qa_mock_provider_fixture_schema_version":
            QA_MOCK_PROVIDER_FIXTURE_SCHEMA_VERSION,
    });
    let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{body}");
    let mut health = parse_health_response(response.as_str()).expect("current health should parse");
    validate_daemon_contract(&health, PUBLIC_RUNTIME_CONTRACT_SNAPSHOT_VERSION, "abcdef123456")
        .expect("matching daemon contract should be accepted");

    health.public_runtime_contract_version = "runtime-contracts.stale".to_owned();
    assert!(validate_daemon_contract(
        &health,
        PUBLIC_RUNTIME_CONTRACT_SNAPSHOT_VERSION,
        "abcdef123456",
    )
    .is_err());
    health.public_runtime_contract_version = PUBLIC_RUNTIME_CONTRACT_SNAPSHOT_VERSION.to_owned();
    assert!(validate_daemon_contract(
        &health,
        PUBLIC_RUNTIME_CONTRACT_SNAPSHOT_VERSION,
        "fedcba654321",
    )
    .is_err());

    let legacy = "HTTP/1.1 200 OK\r\n\r\n{\"service\":\"palyrad\",\"status\":\"ok\",\"version\":\"0.1.0\",\"git_hash\":\"abcdef123456\",\"build_profile\":\"debug\",\"uptime_seconds\":1}";
    let error = parse_health_response(legacy)
        .expect_err("legacy health without contract fields must be rejected");
    assert!(error.to_string().contains("qa.runner.daemon_contract_mismatch"));
}

#[test]
fn health_reader_enforces_one_deadline_for_a_dripping_peer() {
    use std::net::TcpListener;

    let listener =
        TcpListener::bind(("127.0.0.1", 0)).expect("loopback health listener should bind");
    let address = listener.local_addr().expect("listener address should be available");
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("health peer should connect");
        let started = Instant::now();
        while started.elapsed() < Duration::from_millis(250) {
            if stream.write_all(b"x").is_err() {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
    });
    let mut stream = TcpStream::connect(address).expect("health client should connect");
    let started = Instant::now();

    let error = read_health_response(&mut stream, started + Duration::from_millis(50))
        .expect_err("a peer that never finishes must not reset the total deadline");

    assert!(error.to_string().contains("qa.runner.daemon_health_timeout"));
    assert!(started.elapsed() < Duration::from_millis(500));
    drop(stream);
    server.join().expect("dripping peer should stop after disconnect");
}

#[test]
fn health_reader_rejects_a_body_before_buffering_past_its_limit() {
    use std::net::TcpListener;

    let listener =
        TcpListener::bind(("127.0.0.1", 0)).expect("loopback health listener should bind");
    let address = listener.local_addr().expect("listener address should be available");
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("health peer should connect");
        stream
            .write_all(vec![b'x'; MAX_HEALTH_RESPONSE_BYTES + 1].as_slice())
            .expect("oversized response should reach the client");
    });
    let mut stream = TcpStream::connect(address).expect("health client should connect");

    let error = read_health_response(&mut stream, Instant::now() + Duration::from_secs(1))
        .expect_err("an oversized health response must fail closed");

    assert!(error.to_string().contains("qa.runner.daemon_health_response_too_large"));
    server.join().expect("oversized health peer should finish");
}

#[test]
fn runner_principal_uses_the_console_admin_namespace() {
    assert!(QA_RUNNER_PRINCIPAL.starts_with("admin:"));
}

#[test]
fn active_runtime_ids_remain_available_for_cleanup_evidence() {
    let (mut sandbox, _) = test_sandbox();
    sandbox.record_session_id("01ARZ3NDEKTSV4RRFFQ69G5FAA");
    sandbox.record_run_id("01ARZ3NDEKTSV4RRFFQ69G5FAB");

    assert_eq!(sandbox.active_session_id(), Some("01ARZ3NDEKTSV4RRFFQ69G5FAA"));
    assert_eq!(sandbox.active_run_id(), Some("01ARZ3NDEKTSV4RRFFQ69G5FAB"));
}

#[tokio::test]
async fn cleanup_without_an_active_session_is_a_successful_no_op() {
    let (sandbox, _) = test_sandbox();

    assert!(sandbox.cleanup_active_session().await);
}

#[test]
fn no_tool_profiles_reject_every_tool_allowlist() {
    let mut no_tools = parse_scenario(NO_TOOLS_SCENARIO);
    assert!(validate_policy_profile(&no_tools).is_ok());
    no_tools.requires.tools.push("palyra.fs.read_file".to_owned());
    assert!(validate_policy_profile(&no_tools).is_err());

    let mut restricted = parse_scenario_with_policy_profile(NO_TOOLS_SCENARIO, "qa_restricted");
    assert!(validate_policy_profile(&restricted).is_ok());
    restricted.requires.tools.push("palyra.fs.read_file".to_owned());
    assert!(validate_policy_profile(&restricted).is_err());

    let mut recovery = parse_scenario(PROVIDER_RECOVERY_SCENARIO);
    assert!(validate_policy_profile(&recovery).is_ok());
    recovery.requires.tools.push("palyra.fs.read_file".to_owned());
    assert!(validate_policy_profile(&recovery).is_err());
}

#[test]
fn explicit_runtime_kernel_shadow_profile_is_closed_and_tool_free() {
    let mut manifest = parse_scenario(RUNTIME_KERNEL_SHADOW_SCENARIO);
    validate_policy_profile(&manifest)
        .expect("the dedicated shadow scenario should pass closed profile validation");

    manifest.requires.tools.push("palyra.fs.read_file".to_owned());
    assert!(validate_policy_profile(&manifest).is_err());

    let unsupported =
        parse_scenario_with_policy_profile(RUNTIME_KERNEL_SHADOW_SCENARIO, "v2_shadow");
    assert!(validate_policy_profile(&unsupported).is_err());
}

#[test]
fn authoritative_runtime_kernel_v2_profiles_preserve_exact_authority_boundaries() {
    for source in [
        RUNTIME_KERNEL_V2_TEXT_SCENARIO,
        RUNTIME_KERNEL_V2_CANCEL_SCENARIO,
        RUNTIME_KERNEL_V2_COMPACTION_SCENARIO,
    ] {
        let mut manifest = parse_scenario(source);
        validate_policy_profile(&manifest)
            .expect("authoritative V2 no-tool profile should remain closed");
        manifest.requires.tools.push("palyra.fs.read_file".to_owned());
        assert!(validate_policy_profile(&manifest).is_err());
    }

    let mut read_only = parse_scenario(RUNTIME_KERNEL_V2_TOOL_SCENARIO);
    validate_policy_profile(&read_only)
        .expect("authoritative V2 read-only profile should accept its exact tool");
    read_only.requires.tools.push("palyra.fs.apply_patch".to_owned());
    assert!(validate_policy_profile(&read_only).is_err());

    let mut approval = parse_scenario(RUNTIME_KERNEL_V2_APPROVAL_SCENARIO);
    validate_policy_profile(&approval)
        .expect("authoritative V2 approval profile should accept deny-only mutation authority");
    let decision = approval
        .steps
        .iter_mut()
        .find(|step| step.action == QaScenarioStepAction::ApprovalDecision)
        .expect("approval qualification should contain its deny decision");
    decision.decision = Some(QaScenarioApprovalDecision::Allow);
    assert!(validate_policy_profile(&approval).is_err());
}

#[test]
fn read_only_profile_requires_a_unique_explicit_read_tool_subset() {
    let mut manifest = parse_scenario(READ_ONLY_SCENARIO);
    assert!(validate_policy_profile(&manifest).is_ok());

    manifest.requires.tools.clear();
    assert!(validate_policy_profile(&manifest).is_err());

    manifest.requires.tools = vec!["palyra.fs.search".to_owned()];
    assert!(validate_policy_profile(&manifest).is_ok());

    manifest.requires.tools.push("palyra.fs.apply_patch".to_owned());
    assert!(validate_policy_profile(&manifest).is_err());

    manifest.requires.tools =
        vec!["palyra.fs.read_file".to_owned(), "palyra.fs.read_file".to_owned()];
    assert!(validate_policy_profile(&manifest).is_err());
}

#[test]
fn approval_denied_profile_requires_exact_mutation_tool_and_deny_only_steps() {
    let mut manifest = parse_scenario(APPROVAL_DENIED_SCENARIO);
    assert!(validate_policy_profile(&manifest).is_ok());

    manifest.requires.tools.push("palyra.process.run".to_owned());
    assert!(validate_policy_profile(&manifest).is_err());

    manifest.requires.tools = vec!["palyra.fs.apply_patch".to_owned()];
    let decision = manifest
        .steps
        .iter_mut()
        .find(|step| step.action == QaScenarioStepAction::ApprovalDecision)
        .expect("approval scenario should contain a decision step");
    decision.decision = Some(QaScenarioApprovalDecision::Allow);
    assert!(validate_policy_profile(&manifest).is_err());
}

#[test]
fn fault_mutation_profile_requires_a_plan_and_allow_only_steps() {
    let mut manifest = parse_scenario(FAULT_MUTATION_SCENARIO);
    assert!(validate_policy_profile(&manifest).is_ok());

    for tool_name in QA_FAULT_MUTATION_TOOLS {
        manifest.requires.tools = vec![(*tool_name).to_owned()];
        assert!(
            validate_policy_profile(&manifest).is_ok(),
            "audited fault tool should be allowed: {tool_name}"
        );
    }
    manifest.requires.tools =
        vec!["palyra.fs.apply_patch".to_owned(), "palyra.process.run".to_owned()];
    assert!(validate_policy_profile(&manifest).is_err());
    manifest.requires.tools = vec!["palyra.browser.click".to_owned()];
    assert!(validate_policy_profile(&manifest).is_err());

    let mut manifest = parse_scenario(FAULT_MUTATION_SCENARIO);
    manifest.fault_injection = None;
    assert!(validate_policy_profile(&manifest).is_err());

    let mut manifest = parse_scenario(FAULT_MUTATION_SCENARIO);
    let decision = manifest
        .steps
        .iter_mut()
        .find(|step| step.action == QaScenarioStepAction::ApprovalDecision)
        .expect("fault scenario should contain a decision step");
    decision.decision = Some(QaScenarioApprovalDecision::Deny);
    assert!(validate_policy_profile(&manifest).is_err());
}

#[test]
fn fault_delivery_profile_rejects_synthetic_approval_steps() {
    let mut manifest = parse_scenario(FAULT_DELIVERY_SCENARIO);
    assert!(validate_policy_profile(&manifest).is_ok());

    manifest.requires.tools = vec!["palyra.http.fetch".to_owned()];
    assert!(validate_policy_profile(&manifest).is_err());

    let mut manifest = parse_scenario(FAULT_DELIVERY_SCENARIO);
    manifest.steps.push(QaScenarioStep {
        id: "unexpected-approval".to_owned(),
        action: QaScenarioStepAction::ApprovalDecision,
        prompt: None,
        tool: None,
        event: None,
        proposal_id: Some("qa-fault-delivery".to_owned()),
        decision: Some(QaScenarioApprovalDecision::Allow),
    });
    assert!(validate_policy_profile(&manifest).is_err());
}

#[test]
fn fault_launch_rotates_capability_while_preserving_the_plan() {
    let manifest = parse_scenario(FAULT_MUTATION_SCENARIO);
    let root = tempfile::tempdir().expect("fault launch root should exist");
    let context = prepare_fault_context(root.path(), manifest.fault_injection.as_ref())
        .expect("fault plan should materialize")
        .expect("fault scenario should produce a launch context");

    let first = prepare_fault_launch(&context).expect("first launch should materialize");
    let second = prepare_fault_launch(&context).expect("restart launch should rotate");

    assert_ne!(first.document.launch_id, second.document.launch_id);
    assert_ne!(first.document.capability_sha256, second.document.capability_sha256);
    assert_eq!(first.document.plan_sha256, second.document.plan_sha256);
    assert_eq!(first.document.plan_path, second.document.plan_path);
    assert_eq!(first.document.evidence_path, second.document.evidence_path);
    for launch in [&first, &second] {
        let launch_bytes = fs::read(root.path().join(launch.launch_relative_path.as_path()))
            .expect("owner-only launch document should exist");
        let decoded = palyra_common::qa_fault_injection::parse_qa_fault_launch_document_json(
            launch_bytes.as_slice(),
        )
        .expect("runner launch document should satisfy the shared contract");
        assert_eq!(decoded, launch.document);
        let capability = fs::read(root.path().join(launch.capability_relative_path.as_path()))
            .expect("separate capability file should exist");
        assert!(capability.starts_with(QA_FAULT_CAPABILITY_PREFIX.as_bytes()));
        assert_eq!(
            String::from_utf8_lossy(capability.as_slice()).trim_end().len(),
            QA_FAULT_CAPABILITY_PREFIX.len() + 64
        );
        assert!(capability
            .windows(launch.capability_sentinel.as_slice().len())
            .any(|window| window == launch.capability_sentinel.as_slice()));
    }
}

#[test]
fn fault_sidecar_rejects_hard_links() {
    let manifest = parse_scenario(FAULT_MUTATION_SCENARIO);
    let root = tempfile::tempdir().expect("fault sidecar root should exist");
    let context = prepare_fault_context(root.path(), manifest.fault_injection.as_ref())
        .expect("fault context should materialize")
        .expect("fault scenario should produce a context");
    let launch = prepare_fault_launch(&context).expect("fault launch should materialize");
    let outside = root.path().join("outside-evidence.ndjson");
    fs::write(outside.as_path(), b"forged evidence\n").expect("outside evidence should exist");
    fs::hard_link(outside.as_path(), context.evidence_path.as_path())
        .expect("hard-linked evidence should exist");

    let error = load_fault_evidence_sidecar(&context, &launch.document)
        .expect_err("hard-linked fault evidence must be rejected");
    assert!(error.to_string().contains("qa.runner.fault_evidence_file_changed"));
}

#[test]
fn workspace_projection_rechecks_state_root_after_read() {
    let (mut sandbox, state_root) = test_sandbox();
    assert!(sandbox.terminate_for_failure_diagnostics());
    let moved_root = state_root.with_file_name(format!(
        "{}-workspace-race-moved",
        state_root.file_name().expect("state root should have a name").to_string_lossy()
    ));

    let error = load_failure_workspace_projection_with_hook(&sandbox, || {
        fs::rename(state_root.as_path(), moved_root.as_path())
            .context("test state root displacement failed")?;
        fs::create_dir_all(state_root.join("workspace"))
            .context("test replacement workspace creation failed")?;
        fs::write(state_root.join("workspace/forged.txt"), b"forged")
            .context("test replacement workspace write failed")?;
        Ok(())
    })
    .expect_err("root substitution during workspace traversal must fail closed");

    assert!(error
        .to_string()
        .contains("qa.runner.failure_diagnostics_state_root_identity_invalid"));
    drop(sandbox);
    fs::remove_dir_all(state_root.as_path()).expect("replacement root cleanup");
    fs::remove_dir_all(moved_root.as_path()).expect("moved root cleanup");
}

#[test]
fn fault_handshake_rechecks_state_root_after_sidecar_read() {
    let (mut sandbox, state_root_path) = test_sandbox();
    assert!(sandbox.terminate_for_failure_diagnostics());
    let manifest = parse_scenario(FAULT_MUTATION_SCENARIO);
    let context =
        prepare_fault_context(state_root_path.as_path(), manifest.fault_injection.as_ref())
            .expect("fault context should materialize")
            .expect("fault scenario should produce a context");
    let launch = prepare_fault_launch(&context).expect("fault launch should materialize");
    let moved_root = state_root_path.with_file_name(format!(
        "{}-handshake-race-moved",
        state_root_path.file_name().expect("state root should have a name").to_string_lossy()
    ));
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

    let error = verify_bound_fault_launch_handshake_with_hook(
        &sandbox.state_root,
        &context,
        &launch,
        || {
            fs::rename(state_root_path.as_path(), moved_root.as_path())
                .context("test state root displacement failed")?;
            let replacement_fault_dir = state_root_path.join(QA_FAULT_DIRECTORY);
            fs::create_dir_all(replacement_fault_dir.as_path())
                .context("test replacement fault directory creation failed")?;
            write_owner_only_new_file(
                state_root_path.join(QA_FAULT_DIRECTORY).join(QA_FAULT_EVIDENCE_FILE).as_path(),
                sidecar_bytes.as_slice(),
                "qa.runner.test_sidecar_write_failed",
            )?;
            Ok(())
        },
    )
    .expect_err("root substitution during the fault handshake must fail closed");

    assert!(
        error.to_string().contains("qa.runner.daemon_start_state_root_identity_invalid"),
        "unexpected handshake race error: {error:#}"
    );
    drop(launch);
    drop(sandbox);
    fs::remove_dir_all(state_root_path.as_path()).expect("replacement root cleanup");
    fs::remove_dir_all(moved_root.as_path()).expect("moved root cleanup");
}
