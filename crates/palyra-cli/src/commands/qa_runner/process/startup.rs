use super::*;

pub(super) fn start_daemon(
    launch: &mut QaDaemonLaunchContext,
    timeout: Duration,
    state_root: SharedStateRoot,
    cleanup_admission: &StartupCleanupAdmission,
) -> Result<QaStartedDaemon> {
    lock_unpoisoned(&state_root)
        .verify_identity()
        .context("qa.runner.daemon_start_state_root_identity_invalid")?;
    let startup_deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| anyhow::anyhow!("qa.runner.daemon_start_deadline_overflow"))?;
    let fault_launch = launch.fault.as_ref().map(prepare_fault_launch).transpose()?;
    ensure_before_deadline(startup_deadline, "qa.runner.daemon_start_timeout")?;
    let port_deadline = phase_deadline(startup_deadline, DAEMON_START_TIMEOUT)?;
    let mut command = Command::new(launch.binary.as_path());
    command
        .args([
            "--bind",
            "127.0.0.1",
            "--port",
            "0",
            "--grpc-bind",
            "127.0.0.1",
            "--grpc-port",
            "0",
        ])
        .current_dir(launch.workspace.as_path())
        .env_clear();
    preserve_platform_environment(&mut command);
    configure_isolated_environment(
        &mut command,
        QaDaemonEnvironment {
            allowed_tools: launch.allowed_tools.as_str(),
            policy_profile: launch.policy_profile.as_str(),
            state_root: launch.state_root.as_path(),
            identity_root: launch.identity_root.as_path(),
            config_path: launch.config_path.as_path(),
            vault_dir: launch.vault_dir.as_path(),
            provider: &launch.provider,
            execution_key_digest: launch.execution_key_digest.as_str(),
            provider_binding_sha256: launch.provider_binding_sha256.as_str(),
            admin_token: launch.admin_token.as_str(),
            principal: launch.principal.as_str(),
            fault_launch: fault_launch.as_ref(),
        },
    );
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let process_tree_preparation = configure_daemon_process_tree(&mut command)?;

    let child = match command.spawn().with_context(|| {
        format!("qa.runner.daemon_start_failed: failed to start {}", launch.binary.display())
    }) {
        Ok(child) => child,
        Err(error) => {
            return Err(daemon_startup_error(
                error,
                None,
                Vec::new(),
                Arc::clone(&state_root),
                cleanup_admission,
            ));
        }
    };
    let mut process = match attach_daemon_process_tree(child, process_tree_preparation) {
        Ok(process) => process,
        Err(failure) => {
            let AttachDaemonProcessFailure { error, process } = *failure;
            return Err(daemon_startup_error(
                error,
                Some(process),
                Vec::new(),
                Arc::clone(&state_root),
                cleanup_admission,
            ));
        }
    };
    let stdout = match process.child.stdout.take() {
        Some(stdout) => stdout,
        None => {
            let error = anyhow::anyhow!(
                "qa.runner.daemon_stdout_unavailable: failed to capture daemon stdout"
            );
            return Err(daemon_startup_error(
                error,
                Some(process),
                Vec::new(),
                Arc::clone(&state_root),
                cleanup_admission,
            ));
        }
    };
    let stderr = match process.child.stderr.take() {
        Some(stderr) => stderr,
        None => {
            let error = anyhow::anyhow!(
                "qa.runner.daemon_stderr_unavailable: failed to capture daemon stderr"
            );
            return Err(daemon_startup_error(
                error,
                Some(process),
                Vec::new(),
                Arc::clone(&state_root),
                cleanup_admission,
            ));
        }
    };
    let log_tail = Arc::new(Mutex::new(VecDeque::new()));
    let (ports_tx, ports_rx) = mpsc::sync_channel(1);
    let stdout_thread = spawn_stdout_reader(stdout, ports_tx, Arc::clone(&log_tail));
    let stderr_thread = spawn_stderr_reader(stderr, Arc::clone(&log_tail));
    let (admin_port, grpc_port) =
        match wait_for_listen_ports(&ports_rx, &mut process.child, &log_tail, port_deadline) {
            Ok(ports) => ports,
            Err(error) => {
                return Err(daemon_startup_error(
                    error,
                    Some(process),
                    vec![stdout_thread, stderr_thread],
                    Arc::clone(&state_root),
                    cleanup_admission,
                ));
            }
        };
    let health_deadline = match phase_deadline(startup_deadline, DAEMON_HEALTH_TIMEOUT) {
        Ok(deadline) => deadline,
        Err(error) => {
            return Err(daemon_startup_error(
                error,
                Some(process),
                vec![stdout_thread, stderr_thread],
                Arc::clone(&state_root),
                cleanup_admission,
            ));
        }
    };
    let runtime_health = match wait_for_health(
        admin_port,
        &mut process.child,
        &log_tail,
        launch.expected_runtime_contract_version.as_str(),
        launch.expected_git_hash.as_str(),
        health_deadline,
    ) {
        Ok(health) => health,
        Err(error) => {
            return Err(daemon_startup_error(
                error,
                Some(process),
                vec![stdout_thread, stderr_thread],
                Arc::clone(&state_root),
                cleanup_admission,
            ));
        }
    };
    if let Err(error) = lock_unpoisoned(&state_root)
        .verify_identity()
        .context("qa.runner.daemon_start_state_root_identity_invalid")
    {
        return Err(daemon_startup_error(
            error,
            Some(process),
            vec![stdout_thread, stderr_thread],
            Arc::clone(&state_root),
            cleanup_admission,
        ));
    }
    if let (Some(context), Some(fault_launch)) = (launch.fault.as_ref(), fault_launch.as_ref()) {
        if let Err(error) = verify_bound_fault_launch_handshake_with_hook(
            &state_root,
            context,
            fault_launch,
            || Ok(()),
        ) {
            return Err(daemon_startup_error(
                error,
                Some(process),
                vec![stdout_thread, stderr_thread],
                Arc::clone(&state_root),
                cleanup_admission,
            ));
        }
    }
    if let Err(error) = ensure_before_deadline(startup_deadline, "qa.runner.daemon_start_timeout") {
        return Err(daemon_startup_error(
            error,
            Some(process),
            vec![stdout_thread, stderr_thread],
            Arc::clone(&state_root),
            cleanup_admission,
        ));
    }

    let (fault_launch_document, fault_secret_sentinel) = fault_launch.map_or_else(
        || (None, None),
        |fault_launch| (Some(fault_launch.document), Some(fault_launch.capability_sentinel)),
    );
    Ok(QaStartedDaemon {
        process,
        admin_port,
        grpc_port,
        log_threads: vec![stdout_thread, stderr_thread],
        log_tail,
        runtime_health,
        fault_launch_document,
        fault_secret_sentinel,
    })
}

fn phase_deadline(run_deadline: Instant, phase_timeout: Duration) -> Result<Instant> {
    ensure_before_deadline(run_deadline, "qa.runner.daemon_start_timeout")?;
    Ok(Instant::now().checked_add(phase_timeout).unwrap_or(run_deadline).min(run_deadline))
}

fn ensure_before_deadline(deadline: Instant, code: &'static str) -> Result<()> {
    if Instant::now() >= deadline {
        anyhow::bail!(code);
    }
    Ok(())
}

pub(super) fn configure_isolated_environment(
    command: &mut Command,
    environment: QaDaemonEnvironment<'_>,
) {
    command
        .env("PALYRA_CONFIG", environment.config_path)
        .env("PALYRA_STATE_ROOT", environment.state_root)
        .env("PALYRA_JOURNAL_DB_PATH", environment.state_root.join("data/journal.sqlite3"))
        .env("PALYRA_GATEWAY_IDENTITY_STORE_DIR", environment.identity_root)
        .env("PALYRA_VAULT_DIR", environment.vault_dir)
        .env("PALYRA_QA_LAB_MODE", "preview_only")
        .env("PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED", "true")
        .env("PALYRA_GATEWAY_QUIC_ENABLED", "false")
        .env("PALYRA_ALLOW_INSECURE_NODE_RPC_WITHOUT_MTLS", "true")
        .env("PALYRA_ADMIN_REQUIRE_AUTH", "true")
        .env("PALYRA_ADMIN_TOKEN", environment.admin_token)
        .env("PALYRA_ADMIN_BOUND_PRINCIPAL", environment.principal)
        .env("PALYRA_TOOL_CALL_ALLOWED_TOOLS", environment.allowed_tools)
        .env("PALYRA_QA_EXECUTION_KEY_DIGEST", environment.execution_key_digest)
        .env("PALYRA_QA_PROVIDER_BINDING_SHA256", environment.provider_binding_sha256)
        .env("RUST_LOG", "info");
    match environment.policy_profile {
        "runtime_kernel_v2_shadow_explicit" => {
            command
                .env("PALYRA_RUNTIME_KERNEL_PROFILE", "v2_shadow")
                .env("PALYRA_RUNTIME_KERNEL_SHADOW_SAMPLE_BASIS_POINTS", "1")
                .env("PALYRA_RUNTIME_KERNEL_SAMPLING_KEY_HEX", environment.execution_key_digest)
                .env("PALYRA_RUNTIME_KERNEL_EXISTING_SESSION_POLICY", "migrate_at_safe_boundary")
                .env(
                    "PALYRA_QA_RUNTIME_KERNEL_SHADOW_EXPLICIT_BINDING",
                    environment.execution_key_digest,
                );
        }
        profile if is_runtime_kernel_v2_authoritative_profile(profile) => {
            command
                .env("PALYRA_RUNTIME_KERNEL_PROFILE", "v2")
                .env("PALYRA_RUNTIME_KERNEL_CANARY_BASIS_POINTS", "0")
                .env("PALYRA_RUNTIME_KERNEL_SHADOW_SAMPLE_BASIS_POINTS", "0")
                .env("PALYRA_RUNTIME_KERNEL_SAMPLING_KEY_HEX", environment.execution_key_digest)
                .env("PALYRA_RUNTIME_KERNEL_EXISTING_SESSION_POLICY", "migrate_at_safe_boundary")
                .env("PALYRA_RUNTIME_KERNEL_ROLLBACK_POLICY", "finish_read_only_suspend_mutating");
        }
        _ => {}
    }
    if let Some(fault_launch) = environment.fault_launch {
        command
            .env(QA_FAULT_LAUNCH_PATH_ENV, fault_launch.launch_relative_path.as_os_str())
            .env(QA_FAULT_CAPABILITY_PATH_ENV, fault_launch.capability_relative_path.as_os_str());
    }
    match environment.provider {
        QaDaemonProviderEnvironment::Deterministic { provider_fixture } => {
            command
                .env("PALYRA_MODEL_PROVIDER_KIND", "deterministic")
                .env("PALYRA_QA_MOCK_PROVIDER_FIXTURE_PATH", provider_fixture)
                .env("PALYRA_OFFLINE", "true");
        }
        QaDaemonProviderEnvironment::Live { registry_path, auth_provider_kind, transport } => {
            command
                .env("PALYRA_MODEL_PROVIDER_AUTH_PROFILE_ID", QA_LIVE_PROFILE_ALIAS)
                .env("PALYRA_MODEL_PROVIDER_AUTH_PROVIDER_KIND", auth_provider_kind)
                .env("PALYRA_AUTH_PROFILES_PATH", registry_path)
                .env("PALYRA_OFFLINE", "false");
            configure_live_transport_environment(command, transport);
        }
    }
}

pub(super) fn configure_live_transport_environment(
    command: &mut Command,
    transport: &QaLiveTransportEnvironment,
) {
    match transport {
        QaLiveTransportEnvironment::OpenAiCompatible { model, base_url } => {
            command
                .env("PALYRA_MODEL_PROVIDER_KIND", "openai_compatible")
                .env("PALYRA_MODEL_PROVIDER_OPENAI_MODEL", model);
            if let Some(base_url) = base_url {
                command.env("PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL", base_url);
            }
        }
        QaLiveTransportEnvironment::Anthropic { model, base_url } => {
            command
                .env("PALYRA_MODEL_PROVIDER_KIND", "anthropic")
                .env("PALYRA_MODEL_PROVIDER_ANTHROPIC_MODEL", model);
            if let Some(base_url) = base_url {
                command.env("PALYRA_MODEL_PROVIDER_ANTHROPIC_BASE_URL", base_url);
            }
        }
    }
}

fn preserve_platform_environment(command: &mut Command) {
    const SAFE_KEYS: &[&str] = &[
        "PATH",
        "HOME",
        "USERPROFILE",
        "SYSTEMROOT",
        "WINDIR",
        "TEMP",
        "TMP",
        "LOCALAPPDATA",
        "APPDATA",
        "LD_LIBRARY_PATH",
        "DYLD_LIBRARY_PATH",
    ];
    for key in SAFE_KEYS {
        if let Some(value) = env::var_os(key) {
            command.env(key, value);
        }
    }
}

pub(super) fn validate_policy_profile(manifest: &QaScenarioManifest) -> Result<()> {
    let profile = manifest
        .runner
        .as_ref()
        .and_then(|runner| runner.policy_profile())
        .unwrap_or("qa_restricted");
    match profile {
        "qa_restricted" if manifest.requires.tools.is_empty() => Ok(()),
        "qa_restricted" => {
            anyhow::bail!("qa.runner.policy_profile_mismatch: qa_restricted cannot expose tools")
        }
        "qa_provider_recovery" if manifest.requires.tools.is_empty() => Ok(()),
        "qa_provider_recovery" => anyhow::bail!(
            "qa.runner.policy_profile_mismatch: qa_provider_recovery cannot expose tools"
        ),
        "qa_no_tools" if manifest.requires.tools.is_empty() => Ok(()),
        "qa_no_tools" => {
            anyhow::bail!("qa.runner.policy_profile_mismatch: qa_no_tools cannot expose tools")
        }
        "runtime_kernel_v2_shadow_explicit" if manifest.requires.tools.is_empty() => Ok(()),
        "runtime_kernel_v2_shadow_explicit" => anyhow::bail!(
            "qa.runner.policy_profile_mismatch: runtime_kernel_v2_shadow_explicit cannot expose tools"
        ),
        "runtime_kernel_v2_authoritative_no_tools"
        | "runtime_kernel_v2_authoritative_cancel"
        | "runtime_kernel_v2_authoritative_compaction"
            if manifest.requires.tools.is_empty() =>
        {
            Ok(())
        }
        "runtime_kernel_v2_authoritative_no_tools"
        | "runtime_kernel_v2_authoritative_cancel"
        | "runtime_kernel_v2_authoritative_compaction" => anyhow::bail!(
            "qa.runner.policy_profile_mismatch: authoritative V2 no-tool profiles cannot expose tools"
        ),
        "runtime_kernel_v2_authoritative_read_only"
            if has_exact_tool_subset(&manifest.requires.tools, QA_READ_ONLY_TOOLS) =>
        {
            Ok(())
        }
        "runtime_kernel_v2_authoritative_read_only" => anyhow::bail!(
            "qa.runner.policy_profile_mismatch: authoritative V2 read-only profile requires explicit workspace read tools"
        ),
        "runtime_kernel_v2_authoritative_approval_denied"
            if has_exact_tools(&manifest.requires.tools, QA_APPROVAL_MUTATION_TOOLS)
                && approval_steps_deny_only(manifest) =>
        {
            Ok(())
        }
        "runtime_kernel_v2_authoritative_approval_denied" => anyhow::bail!(
            "qa.runner.policy_profile_mismatch: authoritative V2 approval profile requires only the mutation tool and explicit deny decisions"
        ),
        "qa_read_only" if has_exact_tool_subset(&manifest.requires.tools, QA_READ_ONLY_TOOLS) => {
            Ok(())
        }
        "qa_read_only" => anyhow::bail!(
            "qa.runner.policy_profile_mismatch: qa_read_only requires explicit workspace read tools"
        ),
        "qa_approval_denied"
            if has_exact_tools(&manifest.requires.tools, QA_APPROVAL_MUTATION_TOOLS)
                && approval_steps_deny_only(manifest) =>
        {
            Ok(())
        }
        "qa_approval_denied" => anyhow::bail!(
            "qa.runner.policy_profile_mismatch: qa_approval_denied requires only the approved mutation tool and explicit deny decisions"
        ),
        "qa_fault_mutation"
            if manifest.fault_injection.is_some()
                && has_single_allowed_tool(&manifest.requires.tools, QA_FAULT_MUTATION_TOOLS)
                && approval_steps_allow_only(manifest) =>
        {
            Ok(())
        }
        "qa_fault_mutation" => anyhow::bail!(
            "qa.runner.policy_profile_mismatch: qa_fault_mutation requires a fault plan, one audited mutation tool, and explicit allow decisions"
        ),
        "qa_fault_delivery"
            if manifest.fault_injection.is_some()
                && has_exact_tools(&manifest.requires.tools, QA_FAULT_DELIVERY_TOOLS)
                && approval_steps_absent(manifest) =>
        {
            Ok(())
        }
        "qa_fault_delivery" => anyhow::bail!(
            "qa.runner.policy_profile_mismatch: qa_fault_delivery requires a fault plan, only the audited delivery tool, and no synthetic approval step"
        ),
        _ => anyhow::bail!(
            "qa.runner.unsupported_policy_profile: unsupported fixture policy profile"
        ),
    }
}

fn is_runtime_kernel_v2_authoritative_profile(profile: &str) -> bool {
    matches!(
        profile,
        "runtime_kernel_v2_authoritative_no_tools"
            | "runtime_kernel_v2_authoritative_read_only"
            | "runtime_kernel_v2_authoritative_approval_denied"
            | "runtime_kernel_v2_authoritative_cancel"
            | "runtime_kernel_v2_authoritative_compaction"
    )
}

fn has_exact_tool_subset(tools: &[String], allowed: &[&str]) -> bool {
    !tools.is_empty()
        && tools.iter().all(|tool| allowed.contains(&tool.as_str()))
        && tools.iter().enumerate().all(|(index, tool)| !tools[..index].contains(tool))
}

fn has_exact_tools(tools: &[String], expected: &[&str]) -> bool {
    tools.len() == expected.len()
        && tools.iter().zip(expected).all(|(actual, expected)| actual == expected)
}

fn has_single_allowed_tool(tools: &[String], allowed: &[&str]) -> bool {
    matches!(tools, [tool] if allowed.contains(&tool.as_str()))
}

fn approval_steps_deny_only(manifest: &QaScenarioManifest) -> bool {
    let mut saw_deny = false;
    for step in
        manifest.steps.iter().filter(|step| step.action == QaScenarioStepAction::ApprovalDecision)
    {
        if !matches!(step.decision.as_ref(), Some(QaScenarioApprovalDecision::Deny)) {
            return false;
        }
        saw_deny = true;
    }
    saw_deny
}

fn approval_steps_allow_only(manifest: &QaScenarioManifest) -> bool {
    let mut saw_allow = false;
    for step in
        manifest.steps.iter().filter(|step| step.action == QaScenarioStepAction::ApprovalDecision)
    {
        if !matches!(step.decision.as_ref(), Some(QaScenarioApprovalDecision::Allow)) {
            return false;
        }
        saw_allow = true;
    }
    saw_allow
}

fn approval_steps_absent(manifest: &QaScenarioManifest) -> bool {
    manifest.steps.iter().all(|step| step.action != QaScenarioStepAction::ApprovalDecision)
}

pub(super) async fn cleanup_session_with_timeout<F>(cleanup: F, timeout: Duration) -> bool
where
    F: Future<Output = Result<gateway_v1::CleanupSessionResponse>>,
{
    matches!(tokio::time::timeout(timeout, cleanup).await, Ok(Ok(response)) if response.cleaned)
}

fn spawn_stdout_reader(
    stdout: ChildStdout,
    ports_tx: mpsc::SyncSender<Result<(u16, u16), String>>,
    log_tail: Arc<Mutex<VecDeque<String>>>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut admin_port = None;
        let mut grpc_port = None;
        let mut ports_tx = Some(ports_tx);
        let mut reader = BufReader::new(stdout);
        loop {
            let line = match read_bounded_line(&mut reader, MAX_LOG_LINE_BYTES) {
                Ok(Some(line)) => String::from_utf8_lossy(line.as_slice()).into_owned(),
                Ok(None) => break,
                Err(_) => {
                    if let Some(sender) = ports_tx.take() {
                        let _ = sender.send(Err("qa.runner.daemon_log_read_failed".to_owned()));
                    }
                    break;
                }
            };
            push_log_tail(&log_tail, line.as_str());
            admin_port = admin_port.or_else(|| parse_port_from_log(&line, "\"listen_addr\":\""));
            grpc_port = grpc_port.or_else(|| parse_port_from_log(&line, "\"grpc_listen_addr\":\""));
            if let (Some(admin), Some(grpc)) = (admin_port, grpc_port) {
                if let Some(sender) = ports_tx.take() {
                    let _ = sender.send(Ok((admin, grpc)));
                }
            }
        }
        if let Some(sender) = ports_tx.take() {
            let _ = sender.send(Err("qa.runner.daemon_ports_not_published".to_owned()));
        }
    })
}

fn spawn_stderr_reader(
    stderr: ChildStderr,
    log_tail: Arc<Mutex<VecDeque<String>>>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut reader = BufReader::new(stderr);
        while let Ok(Some(line)) = read_bounded_line(&mut reader, MAX_LOG_LINE_BYTES) {
            push_log_tail(&log_tail, String::from_utf8_lossy(line.as_slice()).as_ref());
        }
    })
}

fn read_bounded_line(reader: &mut impl BufRead, max_bytes: usize) -> io::Result<Option<Vec<u8>>> {
    let mut line = Vec::with_capacity(max_bytes.min(1_024));
    let mut observed_input = false;
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok(observed_input.then_some(line));
        }
        observed_input = true;
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |index| index.saturating_add(1));
        let retained = consumed.min(max_bytes.saturating_sub(line.len()));
        line.extend_from_slice(&available[..retained]);
        reader.consume(consumed);
        if newline.is_some() {
            return Ok(Some(line));
        }
    }
}

pub(super) fn push_log_tail(log_tail: &Mutex<VecDeque<String>>, line: &str) {
    let mut tail = lock_unpoisoned(log_tail);
    let bounded = line.chars().take(MAX_LOG_LINE_CHARS).collect::<String>();
    tail.push_back(bounded);
    while tail.len() > MAX_LOG_TAIL_LINES {
        tail.pop_front();
    }
}

pub(super) fn parse_port_from_log(line: &str, prefix: &str) -> Option<u16> {
    let start = line.find(prefix)? + prefix.len();
    let rest = &line[start..];
    let end = rest.find('"')?;
    rest[..end].parse::<SocketAddr>().ok().map(|address| address.port())
}

pub(super) fn wait_for_listen_ports(
    receiver: &mpsc::Receiver<Result<(u16, u16), String>>,
    child: &mut Child,
    log_tail: &Mutex<VecDeque<String>>,
    deadline: Instant,
) -> Result<(u16, u16)> {
    // Stdout EOF can race process reaping; retain the reader failure until
    // the loop has a chance to report the more actionable child exit status.
    let mut reader_failure = None;
    loop {
        let now = Instant::now();
        if now >= deadline {
            let reason = reader_failure.as_deref().unwrap_or("qa.runner.daemon_start_timeout");
            anyhow::bail!("{reason}: diagnostics={}", bounded_log_summary(log_tail));
        }
        if reader_failure.is_none() {
            match receiver.recv_timeout(
                Duration::from_millis(100).min(deadline.saturating_duration_since(now)),
            ) {
                Ok(Ok(ports)) => return Ok(ports),
                Ok(Err(code)) => reader_failure = Some(code),
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    reader_failure = Some("qa.runner.daemon_log_reader_disconnected".to_owned());
                }
            }
        } else {
            thread::sleep(
                SHUTDOWN_POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now())),
            );
        }
        if let Some(status) = child.try_wait().context("failed to inspect QA daemon status")? {
            anyhow::bail!(
                "qa.runner.daemon_exited_early: status={status}; diagnostics={}",
                bounded_log_summary(log_tail)
            );
        }
    }
}

fn wait_for_health(
    port: u16,
    child: &mut Child,
    log_tail: &Mutex<VecDeque<String>>,
    expected_runtime_contract_version: &str,
    expected_git_hash: &str,
    deadline: Instant,
) -> Result<QaDaemonRuntimeHealth> {
    let request = b"GET /healthz HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n";
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().context("failed to inspect QA daemon status")? {
            anyhow::bail!(
                "qa.runner.daemon_exited_before_health: status={status}; diagnostics={}",
                bounded_log_summary(log_tail)
            );
        }
        let address = SocketAddr::from(([127, 0, 0, 1], port));
        let connect_timeout =
            Duration::from_millis(300).min(deadline.saturating_duration_since(Instant::now()));
        if connect_timeout.is_zero() {
            break;
        }
        if let Ok(mut stream) = TcpStream::connect_timeout(&address, connect_timeout) {
            let write_timeout =
                Duration::from_millis(300).min(deadline.saturating_duration_since(Instant::now()));
            if write_timeout.is_zero() {
                break;
            }
            let _ = stream.set_write_timeout(Some(write_timeout));
            if stream.write_all(request).is_ok() {
                if let Ok(response) = read_health_response(&mut stream, deadline) {
                    let response = String::from_utf8(response)
                        .context("qa.runner.daemon_health_response_not_utf8")?;
                    if !response.starts_with("HTTP/1.1 200") {
                        sleep_until_deadline(deadline, Duration::from_millis(100));
                        continue;
                    }
                    let health = parse_health_response(response.as_str())?;
                    validate_daemon_contract(
                        &health,
                        expected_runtime_contract_version,
                        expected_git_hash,
                    )?;
                    return Ok(health);
                }
            }
        }
        sleep_until_deadline(deadline, Duration::from_millis(100));
    }
    anyhow::bail!("qa.runner.daemon_health_timeout: diagnostics={}", bounded_log_summary(log_tail))
}

pub(super) fn read_health_response(stream: &mut TcpStream, deadline: Instant) -> Result<Vec<u8>> {
    let mut response = Vec::with_capacity(4_096);
    let mut buffer = [0_u8; 4_096];
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            anyhow::bail!("qa.runner.daemon_health_timeout");
        }
        stream
            .set_read_timeout(Some(Duration::from_millis(300).min(remaining)))
            .context("qa.runner.daemon_health_read_timeout_failed")?;
        let available = MAX_HEALTH_RESPONSE_BYTES
            .saturating_add(1)
            .saturating_sub(response.len())
            .min(buffer.len());
        match stream.read(&mut buffer[..available]) {
            Ok(0) => return Ok(response),
            Ok(read) => {
                response.extend_from_slice(&buffer[..read]);
                if response.len() > MAX_HEALTH_RESPONSE_BYTES {
                    anyhow::bail!("qa.runner.daemon_health_response_too_large");
                }
            }
            Err(error)
                if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut)
                    && Instant::now() >= deadline =>
            {
                anyhow::bail!("qa.runner.daemon_health_timeout");
            }
            Err(error) => return Err(error).context("qa.runner.daemon_health_read_failed"),
        }
    }
}

fn sleep_until_deadline(deadline: Instant, duration: Duration) {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if !remaining.is_zero() {
        thread::sleep(duration.min(remaining));
    }
}

pub(super) fn parse_health_response(response: &str) -> Result<QaDaemonRuntimeHealth> {
    let (_, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| anyhow::anyhow!("qa.runner.daemon_health_invalid"))?;
    let health = serde_json::from_str::<QaDaemonRuntimeHealth>(body.trim())
        .context("qa.runner.daemon_contract_mismatch")?;
    if health.service != "palyrad"
        || health.status != "ok"
        || health.version.trim().is_empty()
        || health.git_hash.trim().is_empty()
        || health.build_profile.trim().is_empty()
    {
        anyhow::bail!("qa.runner.daemon_health_invalid");
    }
    Ok(health)
}

pub(super) fn validate_daemon_contract(
    health: &QaDaemonRuntimeHealth,
    expected_runtime_contract_version: &str,
    expected_git_hash: &str,
) -> Result<()> {
    let git_hash_mismatch = is_concrete_git_hash(expected_git_hash)
        && is_concrete_git_hash(health.git_hash.as_str())
        && health.git_hash != expected_git_hash;
    if health.public_runtime_contract_version != expected_runtime_contract_version
        || health.public_runtime_contract_version != PUBLIC_RUNTIME_CONTRACT_SNAPSHOT_VERSION
        || health.qa_scenario_schema_version != QA_SCENARIO_SCHEMA_VERSION
        || health.qa_mock_provider_fixture_schema_version != QA_MOCK_PROVIDER_FIXTURE_SCHEMA_VERSION
        || git_hash_mismatch
    {
        anyhow::bail!("qa.runner.daemon_contract_mismatch");
    }
    Ok(())
}

fn is_concrete_git_hash(value: &str) -> bool {
    value.len() >= 7 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub(super) fn bounded_log_summary(log_tail: &Mutex<VecDeque<String>>) -> String {
    let tail = lock_unpoisoned(log_tail);
    if tail.is_empty() {
        return "unavailable".to_owned();
    }
    // The durable runner descriptor must never inherit raw daemon output.
    format!("captured_lines={}", tail.len())
}

fn daemon_startup_error(
    error: anyhow::Error,
    process: Option<OwnedDaemonProcess>,
    threads: Vec<JoinHandle<()>>,
    state_root: SharedStateRoot,
    cleanup_admission: &StartupCleanupAdmission,
) -> anyhow::Error {
    lock_unpoisoned(&state_root).startup_cleanup_delegated = true;
    let mut ownership = StartupCleanupOwnership {
        process,
        log_threads: threads,
        log_join_failed: false,
        state_root: Some(state_root),
    };
    let attempt = ownership.attempt(
        |process| process.terminate_tree(DAEMON_TERMINATION_TIMEOUT),
        LOG_DRAIN_JOIN_TIMEOUT,
    );
    if attempt.resources_released && !attempt.log_join_failed {
        error
    } else {
        let cleanup_deferred = !attempt.resources_released;
        let reaper_started = if cleanup_deferred {
            register_startup_cleanup(cleanup_admission, ownership)
        } else {
            false
        };
        anyhow::anyhow!(
            "{error:#}; qa.runner.daemon_start_cleanup_failed: daemon_terminated={}, log_threads_joined={}, log_join_failed={}, state_root_removed={}, cleanup_deferred={cleanup_deferred}, reaper_started={reaper_started}",
            attempt.daemon_terminated,
            attempt.log_threads_joined,
            attempt.log_join_failed,
            attempt.state_root_removed,
        )
    }
}

pub(super) struct StartupCleanupAttempt {
    pub(super) daemon_terminated: bool,
    pub(super) log_threads_joined: bool,
    pub(super) log_join_failed: bool,
    pub(super) state_root_removed: bool,
    pub(super) resources_released: bool,
}

impl StartupCleanupOwnership {
    pub(super) fn attempt<Terminate>(
        &mut self,
        terminate: Terminate,
        log_timeout: Duration,
    ) -> StartupCleanupAttempt
    where
        Terminate: FnOnce(&mut OwnedDaemonProcess) -> bool,
    {
        let daemon_terminated = self.process.as_mut().is_none_or(terminate);
        let log_drains = if daemon_terminated {
            join_owned_log_threads_bounded(&mut self.log_threads, log_timeout)
        } else {
            OwnedLogDrainJoin { all_joined: false, join_failed: false }
        };
        self.log_join_failed |= log_drains.join_failed;
        let log_threads_joined = daemon_terminated && log_drains.all_joined;
        if log_threads_joined {
            self.process.take();
        }
        let state_root_removed = if self.process.is_none() && self.log_threads.is_empty() {
            self.state_root
                .as_ref()
                .is_none_or(|root| lock_unpoisoned(root).remove_after_startup_cleanup())
        } else {
            false
        };
        if state_root_removed {
            self.state_root.take();
        }
        StartupCleanupAttempt {
            daemon_terminated,
            log_threads_joined,
            log_join_failed: self.log_join_failed,
            state_root_removed,
            resources_released: self.process.is_none()
                && self.log_threads.is_empty()
                && self.state_root.is_none(),
        }
    }
}

#[derive(Default)]
pub(super) struct StartupCleanupReaperState {
    pub(super) pending: Option<RegisteredStartupCleanup>,
    pub(super) retained_failure: Option<RegisteredStartupCleanup>,
    pub(super) worker_running: bool,
    pub(super) admitted_generation: Option<u64>,
    next_generation: u64,
    pub(super) quarantined: bool,
}

pub(super) struct RegisteredStartupCleanup {
    pub(super) generation: u64,
    pub(super) ownership: StartupCleanupOwnership,
}

pub(super) type StartupCleanupReaper = Arc<Mutex<StartupCleanupReaperState>>;
pub(super) type StartupCleanupJob = Box<dyn FnOnce() + Send + 'static>;

pub(super) struct StartupCleanupAdmission {
    pub(super) reaper: StartupCleanupReaper,
    pub(super) generation: u64,
}

impl Drop for StartupCleanupAdmission {
    fn drop(&mut self) {
        let mut state = lock_unpoisoned(&self.reaper);
        if !state.worker_running
            && state.pending.is_none()
            && state.retained_failure.is_none()
            && !state.quarantined
            && state.admitted_generation == Some(self.generation)
        {
            state.admitted_generation = None;
        }
    }
}

fn startup_cleanup_reaper() -> &'static StartupCleanupReaper {
    static REAPER: OnceLock<StartupCleanupReaper> = OnceLock::new();
    REAPER.get_or_init(|| Arc::new(Mutex::new(StartupCleanupReaperState::default())))
}

pub(super) fn acquire_startup_cleanup_admission() -> Result<StartupCleanupAdmission> {
    acquire_startup_cleanup_admission_with(Arc::clone(startup_cleanup_reaper()))
}

pub(super) fn acquire_startup_cleanup_admission_with(
    reaper: StartupCleanupReaper,
) -> Result<StartupCleanupAdmission> {
    let generation = {
        let mut state = lock_unpoisoned(&reaper);
        if state.admitted_generation.is_some()
            || state.worker_running
            || state.pending.is_some()
            || state.retained_failure.is_some()
            || state.quarantined
        {
            anyhow::bail!("qa.runner.daemon_start_cleanup_quarantined");
        }
        let Some(generation) = state.next_generation.checked_add(1) else {
            state.quarantined = true;
            anyhow::bail!("qa.runner.daemon_start_cleanup_generation_exhausted");
        };
        state.next_generation = generation;
        state.admitted_generation = Some(generation);
        generation
    };
    Ok(StartupCleanupAdmission { reaper, generation })
}

pub(super) fn register_startup_cleanup(
    admission: &StartupCleanupAdmission,
    ownership: StartupCleanupOwnership,
) -> bool {
    let reaper = Arc::clone(&admission.reaper);
    match register_startup_cleanup_with(admission, ownership, |job| {
        thread::Builder::new().name("palyra-qa-cleanup".to_owned()).spawn(job).map(|_| ())
    }) {
        Ok(worker_started) => {
            if !worker_started {
                drive_startup_cleanup_reaper_inline(reaper);
            }
            worker_started
        }
        Err(ownership) => {
            // A rejected transfer means an internal ownership invariant was violated. The reaper
            // is already quarantined, so retaining this one allocation for process lifetime is
            // safer than dropping handles while a daemon or pinned state root may still be live.
            std::mem::forget(ownership);
            false
        }
    }
}

pub(super) fn register_startup_cleanup_with<Spawn>(
    admission: &StartupCleanupAdmission,
    ownership: StartupCleanupOwnership,
    spawn: Spawn,
) -> std::result::Result<bool, Box<StartupCleanupOwnership>>
where
    Spawn: FnOnce(StartupCleanupJob) -> io::Result<()>,
{
    let reaper = Arc::clone(&admission.reaper);
    {
        let mut state = lock_unpoisoned(&reaper);
        if state.admitted_generation != Some(admission.generation)
            || state.worker_running
            || state.pending.is_some()
            || state.retained_failure.is_some()
            || state.quarantined
        {
            state.quarantined = true;
            return Err(Box::new(ownership));
        }
        state.pending =
            Some(RegisteredStartupCleanup { generation: admission.generation, ownership });
        state.worker_running = true;
    }
    let worker_reaper = Arc::clone(&reaper);
    let job: StartupCleanupJob = Box::new(move || run_startup_cleanup_reaper(worker_reaper));
    if spawn(job).is_ok() {
        Ok(true)
    } else {
        lock_unpoisoned(&reaper).worker_running = false;
        Ok(false)
    }
}

pub(super) fn drive_startup_cleanup_reaper_inline(reaper: StartupCleanupReaper) {
    {
        let mut state = lock_unpoisoned(&reaper);
        if state.worker_running {
            return;
        }
        state.worker_running = true;
    }
    run_startup_cleanup_reaper(reaper);
}

fn run_startup_cleanup_reaper(reaper: StartupCleanupReaper) {
    // Serialized admission makes one optional slot a global hard cap. An irrecoverable ownership
    // remains quarantined in that slot and prevents any later QA start from allocating resources.
    let registered = {
        let mut state = lock_unpoisoned(&reaper);
        let Some(registered) = state.pending.take() else {
            state.worker_running = false;
            return;
        };
        registered
    };
    let generation = registered.generation;
    let mut ownership = registered.ownership;
    let mut resources_released = false;
    for attempt in 0..STARTUP_REAPER_MAX_ATTEMPTS {
        resources_released = ownership
            .attempt(
                |process| process.terminate_tree(DAEMON_TERMINATION_TIMEOUT),
                LOG_DRAIN_JOIN_TIMEOUT,
            )
            .resources_released;
        if resources_released {
            break;
        }
        if attempt + 1 < STARTUP_REAPER_MAX_ATTEMPTS {
            thread::sleep(STARTUP_REAPER_BACKOFF);
        }
    }
    let mut state = lock_unpoisoned(&reaper);
    if !resources_released {
        if state.retained_failure.is_none() {
            state.retained_failure = Some(RegisteredStartupCleanup { generation, ownership });
        } else {
            // The slot must never be overwritten, even if a future caller violates admission.
            std::mem::forget(ownership);
        }
        state.quarantined = true;
    } else if state.admitted_generation == Some(generation) {
        state.admitted_generation = None;
    }
    state.worker_running = false;
}

#[cfg(test)]
pub(super) fn terminate_child_with_timeout(child: &mut Child, timeout: Duration) -> bool {
    match child.try_wait() {
        Ok(Some(_)) => return true,
        Ok(None) => {}
        Err(_) => return false,
    }
    if child.kill().is_err() {
        return false;
    }

    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return true,
            Ok(None) => {}
            Err(_) => return false,
        }
        let now = Instant::now();
        if now >= deadline {
            return false;
        }
        thread::sleep(SHUTDOWN_POLL_INTERVAL.min(deadline.saturating_duration_since(now)));
    }
}

pub(super) struct OwnedLogDrainJoin {
    pub(super) all_joined: bool,
    pub(super) join_failed: bool,
}

pub(super) fn join_owned_log_threads_bounded(
    threads: &mut Vec<JoinHandle<()>>,
    timeout: Duration,
) -> OwnedLogDrainJoin {
    let deadline = Instant::now().checked_add(timeout).unwrap_or_else(Instant::now);
    let mut join_failed = false;
    let mut index = 0;
    while index < threads.len() {
        while !threads[index].is_finished() && Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            thread::sleep(SHUTDOWN_POLL_INTERVAL.min(remaining));
        }
        if threads[index].is_finished() {
            let handle = threads.swap_remove(index);
            join_failed |= handle.join().is_err();
        } else {
            // Retaining the handle lets shutdown retry after the child closes its pipe.
            index = index.saturating_add(1);
        }
    }
    OwnedLogDrainJoin { all_joined: threads.is_empty(), join_failed }
}
