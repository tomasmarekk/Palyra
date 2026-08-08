//! Multi-process conformance test for the reference canonical worker.
//! The child receives an authenticated-task projection over bounded stdio and
//! must enforce workspace scope before returning a content-addressed outcome.

use std::{
    io::Write,
    process::{Command, Stdio},
};

use palyra_common::runtime_contracts::RuntimeGeneration;
use palyra_workerd::{
    remote_protocol::{
        RemoteWorkerProtocolV1, WorkerTaskEnvelope, REMOTE_WORKER_PROTOCOL_SCHEMA_VERSION,
        REMOTE_WORKER_PROTOCOL_V1,
    },
    transport_adapters::DesktopNodeBindingV2,
    WorkerArtifactTransport, WorkerRemoteIdentity, WorkerRemoteLeaseBinding, WorkerRemoteToolKind,
    WorkerRemoteToolRequestEnvelope, WorkerRemoteWorkspaceEntry, WorkerRemoteWorkspaceEntryKind,
    WorkerRemoteWorkspaceTransfer, WorkerWorkspaceScope, WORKER_REMOTE_TOOL_PROTOCOL,
    WORKER_REMOTE_TOOL_SCHEMA_VERSION,
};
use sha2::{Digest, Sha256};

#[test]
fn reference_worker_executes_canonical_task_in_a_separate_process() {
    let workspace = tempfile::tempdir().expect("workspace");
    std::fs::write(workspace.path().join("note.txt"), "hello worker").expect("fixture");
    let now = unix_time_ms();
    let input_json = r#"{"path":"note.txt"}"#.to_owned();
    let task = WorkerTaskEnvelope {
        task_id: "task-process-1".to_owned(),
        request_id: "request-process-1".to_owned(),
        idempotency_key: "a".repeat(64),
        cancellation_id: "b".repeat(64),
        issued_at_unix_ms: now.saturating_sub(1_000),
        deadline_unix_ms: now.saturating_add(30_000),
        policy_sha256: "c".repeat(64),
        workspace_manifest_sha256: "d".repeat(64),
        input_sha256: sha256_hex(input_json.as_bytes()),
        tool_name: "palyra.fs.read_file".to_owned(),
        input_json,
        input_artifacts: Vec::new(),
        secret_lease: None,
        run_generation: RuntimeGeneration::new(1).expect("generation"),
        fence_generation: 1,
        work_graph_claim: None,
        work_graph_posture: Default::default(),
        resource_limits: palyra_workerd::remote_protocol::RemoteResourceLimits {
            wall_time_ms: 30_000,
            memory_bytes: 128 * 1_024 * 1_024,
            cpu_time_ms: 30_000,
            input_artifact_bytes: 64 * 1_024,
            output_artifact_bytes: 64 * 1_024,
        },
        max_output_bytes: 64 * 1024,
    };
    let protocol = RemoteWorkerProtocolV1 {
        protocol: REMOTE_WORKER_PROTOCOL_V1.to_owned(),
        schema_version: REMOTE_WORKER_PROTOCOL_SCHEMA_VERSION,
        mutual_auth_binding_sha256: "e".repeat(64),
        worker_attestation_sha256: "f".repeat(64),
        task,
    };
    let mut child = Command::new(env!("CARGO_BIN_EXE_palyra-workerd"))
        .arg(workspace.path())
        .arg("worker-process-1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("reference worker should spawn");
    child
        .stdin
        .take()
        .expect("worker stdin")
        .write_all(&serde_json::to_vec(&protocol).expect("protocol JSON"))
        .expect("protocol should be written");
    let output = child.wait_with_output().expect("worker should exit");

    assert!(output.status.success(), "{}", String::from_utf8_lossy(output.stderr.as_slice()));
    let response: palyra_workerd::network_runtime::ReferenceWorkerResponse =
        serde_json::from_slice(output.stdout.as_slice()).expect("worker response");
    assert!(response.outcome.success);
    assert!(response.output_json.contains("hello worker"));
    assert!(response.outcome.cleanup.is_verified());
}

#[test]
fn desktop_adapter_executes_the_same_scoped_stdio_protocol() {
    let now = unix_time_ms();
    let input_json = r#"{"path":"note.txt"}"#.to_owned();
    let entry = WorkerRemoteWorkspaceEntry {
        path: "note.txt".to_owned(),
        kind: WorkerRemoteWorkspaceEntryKind::File,
        sha256: sha256_hex(b"hello desktop adapter"),
        bytes: b"hello desktop adapter".to_vec(),
    };
    let mut request = WorkerRemoteToolRequestEnvelope {
        protocol: WORKER_REMOTE_TOOL_PROTOCOL.to_owned(),
        schema_version: WORKER_REMOTE_TOOL_SCHEMA_VERSION,
        request_id: "request-desktop-1".to_owned(),
        proposal_id: "proposal-desktop-1".to_owned(),
        tool_name: "palyra.fs.read_file".to_owned(),
        tool_kind: WorkerRemoteToolKind::FsRead,
        input_json_sha256: sha256_hex(input_json.as_bytes()),
        input_json,
        lease: WorkerRemoteLeaseBinding {
            lease_id: "lease-desktop-1".to_owned(),
            worker_id: "desktop-worker-1".to_owned(),
            session_id: "session-desktop-1".to_owned(),
            run_id: "run-desktop-1".to_owned(),
            run_generation: RuntimeGeneration::new(1).expect("generation"),
            grant_id: "grant-desktop-1".to_owned(),
            grant_tool_name: "palyra.fs.read_file".to_owned(),
            expires_at_unix_ms: now.saturating_add(30_000),
            required_capabilities: vec!["tool:palyra.fs.read_file".to_owned()],
            work_graph_claim: None,
            work_graph_posture: Default::default(),
            workspace_scope: WorkerWorkspaceScope {
                workspace_root: "/workspace".to_owned(),
                allowed_paths: vec!["note.txt".to_owned()],
                read_only: true,
            },
            artifact_transport: WorkerArtifactTransport {
                input_manifest_sha256: "1".repeat(64),
                output_manifest_sha256: "2".repeat(64),
                log_stream_id: "logs-desktop-1".to_owned(),
                scratch_directory_id: "scratch-desktop-1".to_owned(),
            },
        },
        worker_identity: WorkerRemoteIdentity {
            worker_id: "desktop-worker-1".to_owned(),
            image_digest_sha256: "3".repeat(64),
            build_digest_sha256: "4".repeat(64),
            artifact_digest_sha256: "5".repeat(64),
            capability_authority_sha256: Some("6".repeat(64)),
            sdk_protocol_version: 1,
            wit_abi_version: "palyra-worker-abi/v1".to_owned(),
        },
        workspace_transfer: WorkerRemoteWorkspaceTransfer::scoped("7".repeat(64), vec![entry])
            .expect("scoped transfer"),
        encrypted_secret_artifact: None,
        canonical_protocol: None,
    };
    request.canonical_protocol = Some(RemoteWorkerProtocolV1::from_remote_request(&request));
    let binding = DesktopNodeBindingV2 {
        device_id: "desktop-worker-1".to_owned(),
        identity_fingerprint_sha256: "8".repeat(64),
        platform: std::env::consts::OS.to_owned(),
        capabilities: vec!["tool:palyra.fs.read_file".to_owned()],
        user_presence_required: false,
        user_presence_confirmed_at_unix_ms: None,
        user_presence_ttl_ms: 30_000,
        computer_use_authorized: false,
        generation: 1,
        expires_at_unix_ms: now.saturating_add(30_000),
        revoked: false,
    };
    let adapter = binding
        .stdio_adapter(
            env!("CARGO_BIN_EXE_palyra-workerd"),
            &["tool:palyra.fs.read_file".to_owned()],
            now,
            10_000,
        )
        .expect("desktop binding should authorize canonical adapter");

    let result = adapter.execute(&request, now).expect("desktop adapter execution");

    assert!(result.success);
    assert!(result.output_json.contains("hello desktop adapter"));
    assert!(result.cleanup_report.is_verified());
}

fn sha256_hex(value: &[u8]) -> String {
    hex::encode(Sha256::digest(value))
}

fn unix_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(i64::MAX)
}
