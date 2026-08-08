//! Multi-process conformance test for the reference canonical worker.
//! The child receives an authenticated-task projection over bounded stdio and
//! must enforce workspace scope before returning a content-addressed outcome.

use std::{
    io::Write,
    process::{Command, Stdio},
};

use palyra_common::runtime_contracts::RuntimeGeneration;
use palyra_workerd::remote_protocol::{
    RemoteWorkerProtocolV1, WorkerTaskEnvelope, REMOTE_WORKER_PROTOCOL_SCHEMA_VERSION,
    REMOTE_WORKER_PROTOCOL_V1,
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
        work_graph_claim_id: None,
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
