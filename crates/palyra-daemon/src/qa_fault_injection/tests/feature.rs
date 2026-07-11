//! Shared fixtures for authenticated loader, runtime adapter, and recovery tests.
//!
//! Child modules keep behavioral groups small while reusing the same durable fixtures.

use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use palyra_common::qa_fault_injection::{
    parse_qa_fault_evidence_sidecar_ndjson, DeterministicQaFaultController, QaFaultAction,
    QaFaultActivation, QaFaultBarrierJoinedRecord, QaFaultDirective, QaFaultEvidenceSidecarRecord,
    QaFaultInjectionPlan, QaFaultLaunchDocument, QaFaultLaunchLoadedRecord, QaFaultProbeError,
    QaFaultProbeHandle, QaFaultRecoveryClass, QaFaultRuleActivatedRecord,
    QA_FAULT_CAPABILITY_PREFIX, QA_FAULT_EVIDENCE_SIDECAR_MAX_BYTES,
    QA_FAULT_EVIDENCE_SIDECAR_MAX_RECORDS, QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
    QA_FAULT_INJECTION_PLAN_FORMAT, QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
    QA_FAULT_LAUNCH_SCHEMA_VERSION,
};
use palyra_connectors::{
    ConnectorAdapter, ConnectorAdapterError, ConnectorAvailability, ConnectorInstanceRecord,
    ConnectorInstanceSpec, ConnectorKind, ConnectorRouter, ConnectorRouterError, ConnectorStore,
    ConnectorSupervisor, ConnectorSupervisorConfig, DeliveryOutcome, InboundMessageEvent,
    OutboundMessageRequest, OutboxEffectState, RouteInboundResult,
};
use palyra_vault::{ensure_owner_only_dir, ensure_owner_only_file};
use palyra_workerd::{
    WorkerArtifactTransport, WorkerAttestation, WorkerFleetManager, WorkerFleetPolicy,
    WorkerLeaseRequest, WorkerLifecycleError, WorkerRunGrant, WorkerWorkspaceScope,
};
use sha2::{Digest as _, Sha256};
use tempfile::tempdir;

use crate::{
    channels::ChannelPlatform,
    journal::{
        HashMemoryEmbeddingProvider, JournalAppendRequest, JournalConfig, JournalError,
        JournalStore, MemoryEmbeddingProvider,
    },
    media::MediaRuntimeConfig,
    retrieval::MemoryEmbeddingsRuntimeProfile,
    transport::grpc::auth::GatewayAuthConfig,
};

use super::*;

const QA_LAB_MODE_ENV: &str = "PALYRA_QA_LAB_MODE";
const BRIDGE_TERMINATE_CHILD_ENV: &str = "PALYRA_QA_BRIDGE_TERMINATE_CHILD";
const BRIDGE_TERMINATE_ROOT_ENV: &str = "PALYRA_QA_BRIDGE_TERMINATE_ROOT";
const JOURNAL_TERMINATE_CHILD_ENV: &str = "PALYRA_QA_JOURNAL_TERMINATE_CHILD";
const JOURNAL_TERMINATE_ROOT_ENV: &str = "PALYRA_QA_JOURNAL_TERMINATE_ROOT";

struct ConnectorQaRouter;

#[async_trait]
impl ConnectorRouter for ConnectorQaRouter {
    async fn route_inbound(
        &self,
        _principal: &str,
        _event: &InboundMessageEvent,
    ) -> Result<RouteInboundResult, ConnectorRouterError> {
        Ok(RouteInboundResult {
            accepted: true,
            queued_for_retry: false,
            decision_reason: "qa_connector_route".to_owned(),
            outputs: Vec::new(),
            route_key: None,
            session_id: None,
            run_id: None,
            retry_attempt: 0,
            route_message_latency_ms: None,
        })
    }
}

#[derive(Default)]
struct ConnectorQaAdapter {
    delivery_order: Mutex<Vec<String>>,
}

impl ConnectorQaAdapter {
    fn delivery_order(&self) -> Vec<String> {
        self.delivery_order
            .lock()
            .expect("connector QA delivery order lock should not be poisoned")
            .clone()
    }
}

#[async_trait]
impl ConnectorAdapter for ConnectorQaAdapter {
    fn kind(&self) -> ConnectorKind {
        ConnectorKind::Echo
    }

    fn availability(&self) -> ConnectorAvailability {
        ConnectorAvailability::InternalTestOnly
    }

    async fn send_outbound(
        &self,
        _instance: &ConnectorInstanceRecord,
        request: &OutboundMessageRequest,
    ) -> Result<DeliveryOutcome, ConnectorAdapterError> {
        self.delivery_order
            .lock()
            .map_err(|_| {
                ConnectorAdapterError::Backend(
                    "connector QA delivery order lock poisoned".to_owned(),
                )
            })?
            .push(request.envelope_id.clone());
        Ok(DeliveryOutcome::Delivered {
            native_message_id: format!("native-{}", request.envelope_id),
        })
    }
}

struct EnvironmentRestore {
    values: Vec<(&'static str, Option<OsString>)>,
}

impl EnvironmentRestore {
    fn capture(names: &[&'static str]) -> Self {
        Self { values: names.iter().map(|name| (*name, std::env::var_os(name))).collect() }
    }
}

impl Drop for EnvironmentRestore {
    fn drop(&mut self) {
        for (name, value) in self.values.drain(..) {
            if let Some(value) = value {
                std::env::set_var(name, value);
            } else {
                std::env::remove_var(name);
            }
        }
    }
}

fn delivery_unknown_plan() -> QaFaultInjectionPlan {
    QaFaultInjectionPlan {
        schema_version: QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
        format: QA_FAULT_INJECTION_PLAN_FORMAT.to_owned(),
        seed: 20260711,
        activations: vec![QaFaultActivation {
            id: "final-delivery-unknown".to_owned(),
            point_id: "run.final_delivery.after_effect_before_ack".to_owned(),
            actor: Some("delivery".to_owned()),
            occurrence: 1,
            action: QaFaultAction::TerminateProcess,
        }],
    }
}

fn provider_malformed_plan() -> QaFaultInjectionPlan {
    QaFaultInjectionPlan {
        schema_version: QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
        format: QA_FAULT_INJECTION_PLAN_FORMAT.to_owned(),
        seed: 20260711,
        activations: vec![QaFaultActivation {
            id: "provider-outcome-unknown".to_owned(),
            point_id: "provider.fixture.after_effect_before_ack".to_owned(),
            actor: Some("provider".to_owned()),
            occurrence: 1,
            action: QaFaultAction::MalformedEvent,
        }],
    }
}

fn worker_claim_barrier_plan() -> QaFaultInjectionPlan {
    QaFaultInjectionPlan {
        schema_version: QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
        format: QA_FAULT_INJECTION_PLAN_FORMAT.to_owned(),
        seed: 41,
        activations: vec![QaFaultActivation {
            id: "worker-claim-race".to_owned(),
            point_id: "worker.claim.before_effect".to_owned(),
            actor: None,
            occurrence: 1,
            action: QaFaultAction::Barrier { participants: 2 },
        }],
    }
}

fn worker_stale_reclaim_barrier_plan() -> QaFaultInjectionPlan {
    QaFaultInjectionPlan {
        schema_version: QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
        format: QA_FAULT_INJECTION_PLAN_FORMAT.to_owned(),
        seed: 43,
        activations: vec![QaFaultActivation {
            id: "worker-stale-barrier".to_owned(),
            point_id: "worker.stale_reclaim.batch_before_effect".to_owned(),
            actor: None,
            occurrence: 1,
            action: QaFaultAction::Barrier { participants: 2 },
        }],
    }
}

fn restart_sequence_plan() -> QaFaultInjectionPlan {
    QaFaultInjectionPlan {
        schema_version: QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
        format: QA_FAULT_INJECTION_PLAN_FORMAT.to_owned(),
        seed: 42,
        activations: vec![
            QaFaultActivation {
                id: "provider-delayed".to_owned(),
                point_id: "provider.fixture.before_effect".to_owned(),
                actor: Some("provider".to_owned()),
                occurrence: 3,
                action: QaFaultAction::Timeout,
            },
            QaFaultActivation {
                id: "tool-crash".to_owned(),
                point_id: "tool.before_effect".to_owned(),
                actor: Some("tool".to_owned()),
                occurrence: 1,
                action: QaFaultAction::TerminateProcess,
            },
        ],
    }
}

fn connector_timeout_plan() -> QaFaultInjectionPlan {
    QaFaultInjectionPlan {
        schema_version: QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
        format: QA_FAULT_INJECTION_PLAN_FORMAT.to_owned(),
        seed: 84,
        activations: vec![QaFaultActivation {
            id: "connector-timeout".to_owned(),
            point_id: "connector.outbox.before_effect".to_owned(),
            actor: Some("outbox-1".to_owned()),
            occurrence: 1,
            action: QaFaultAction::Timeout,
        }],
    }
}

fn connector_barrier_plan() -> QaFaultInjectionPlan {
    QaFaultInjectionPlan {
        schema_version: QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
        format: QA_FAULT_INJECTION_PLAN_FORMAT.to_owned(),
        seed: 85,
        activations: vec![QaFaultActivation {
            id: "connector-batch-barrier".to_owned(),
            point_id: "connector.outbox.batch_before_effect".to_owned(),
            actor: None,
            occurrence: 1,
            action: QaFaultAction::Barrier { participants: 2 },
        }],
    }
}

fn connector_runtime_fixture(
    root: &Path,
    plan: &QaFaultInjectionPlan,
    launch_id: &str,
) -> (QaFaultRuntime, QaFaultLaunchDocument, PathBuf) {
    let plan_path = root.join(format!("{launch_id}-plan.json"));
    fs::write(plan_path.as_path(), plan.canonical_json().expect("connector plan should serialize"))
        .expect("connector plan should be written");
    let evidence_path = root.join(format!("{launch_id}-evidence.ndjson"));
    let launch = QaFaultLaunchDocument {
        schema_version: QA_FAULT_LAUNCH_SCHEMA_VERSION,
        launch_id: launch_id.to_owned(),
        plan_path: plan_path.to_string_lossy().into_owned(),
        plan_sha256: plan.canonical_sha256().expect("connector plan should hash"),
        capability_sha256: "e".repeat(64),
        evidence_path: evidence_path.to_string_lossy().into_owned(),
        expires_at_unix_ms: current_unix_ms().saturating_add(60_000),
    };
    let loaded = QaFaultEvidenceSidecarRecord::LaunchLoaded(QaFaultLaunchLoadedRecord {
        schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
        sequence: 1,
        launch_id: launch.launch_id.clone(),
        plan_sha256: launch.plan_sha256.clone(),
        capability_sha256: launch.capability_sha256.clone(),
    });
    let mut initial_evidence =
        serde_json::to_vec(&loaded).expect("connector launch evidence should serialize");
    initial_evidence.push(b'\n');
    fs::write(evidence_path.as_path(), initial_evidence)
        .expect("connector launch evidence should be written");
    let runtime = QaFaultRuntime::active(
        QaFaultProbeHandle::from_probe(
            DeterministicQaFaultController::new(plan.clone())
                .expect("connector controller should initialize"),
        ),
        QaFaultEvidenceState {
            launch: launch.clone(),
            path: evidence_path.clone(),
            next_sequence: 2,
            activated_rules: BTreeMap::new(),
            activation_actors: BTreeMap::new(),
            barrier_joins: BTreeMap::new(),
            barrier_join_points: BTreeMap::new(),
            barrier_participants: BTreeMap::new(),
            barrier_release_orders: BTreeMap::new(),
            barrier_releases: BTreeMap::new(),
            observed_occurrences: BTreeMap::new(),
            occurrence_targets: occurrence_targets(plan),
            recovered_rule_ids: BTreeSet::new(),
        },
        plan.seed,
    );
    (runtime, launch, evidence_path)
}

fn connector_supervisor_fixture(
    root: &Path,
    probe: QaFaultProbeHandle,
) -> (ConnectorSupervisor, Arc<ConnectorQaAdapter>, Arc<ConnectorStore>) {
    let store = Arc::new(
        ConnectorStore::open(root.join("connector-runtime.sqlite3"))
            .expect("connector store should initialize"),
    );
    let adapter = Arc::new(ConnectorQaAdapter::default());
    let supervisor = ConnectorSupervisor::new(
        store.clone(),
        Arc::new(ConnectorQaRouter),
        vec![adapter.clone()],
        ConnectorSupervisorConfig::default(),
    )
    .with_qa_fault_probe(probe);
    supervisor
        .register_connector(&ConnectorInstanceSpec {
            connector_id: "echo:qa".to_owned(),
            kind: ConnectorKind::Echo,
            principal: "connector-qa".to_owned(),
            auth_profile_ref: None,
            token_vault_ref: None,
            egress_allowlist: Vec::new(),
            enabled: true,
        })
        .expect("connector QA instance should register");
    (supervisor, adapter, store)
}

fn connector_outbound(envelope_id: &str) -> OutboundMessageRequest {
    OutboundMessageRequest {
        envelope_id: envelope_id.to_owned(),
        connector_id: "echo:qa".to_owned(),
        conversation_id: "connector-qa-conversation".to_owned(),
        reply_thread_id: None,
        in_reply_to_message_id: None,
        text: "connector QA delivery".to_owned(),
        broadcast: false,
        auto_ack_text: None,
        auto_reaction: None,
        attachments: Vec::new(),
        structured_json: None,
        a2ui_update: None,
        timeout_ms: 1_000,
        max_payload_bytes: 4_096,
    }
}

fn connector_terminate_plan() -> QaFaultInjectionPlan {
    QaFaultInjectionPlan {
        schema_version: QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
        format: QA_FAULT_INJECTION_PLAN_FORMAT.to_owned(),
        seed: 86,
        activations: vec![QaFaultActivation {
            id: "connector-terminate".to_owned(),
            point_id: "connector.outbox.before_intent".to_owned(),
            actor: Some("outbox-1".to_owned()),
            occurrence: 1,
            action: QaFaultAction::TerminateProcess,
        }],
    }
}

fn journal_post_commit_terminate_plan() -> QaFaultInjectionPlan {
    QaFaultInjectionPlan {
        schema_version: QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
        format: QA_FAULT_INJECTION_PLAN_FORMAT.to_owned(),
        seed: 87,
        activations: vec![QaFaultActivation {
            id: "journal-post-commit".to_owned(),
            point_id: "journal.after_effect_before_ack".to_owned(),
            actor: Some("journal-event".to_owned()),
            occurrence: 1,
            action: QaFaultAction::TerminateProcess,
        }],
    }
}

fn journal_terminate_launch(root: &Path, plan: &QaFaultInjectionPlan) -> QaFaultLaunchDocument {
    QaFaultLaunchDocument {
        schema_version: QA_FAULT_LAUNCH_SCHEMA_VERSION,
        launch_id: "journal-terminate-launch".to_owned(),
        plan_path: root.join("journal-plan.json").to_string_lossy().into_owned(),
        plan_sha256: plan.canonical_sha256().expect("journal plan should hash"),
        capability_sha256: "d".repeat(64),
        evidence_path: root.join("evidence.ndjson").to_string_lossy().into_owned(),
        expires_at_unix_ms: current_unix_ms().saturating_add(60_000),
    }
}

fn run_bridge_terminate_child() -> ! {
    let root = PathBuf::from(
        std::env::var_os(BRIDGE_TERMINATE_ROOT_ENV)
            .expect("terminate bridge child root should be provided"),
    );
    let state_root = fs::canonicalize(root.join("state"))
        .expect("terminate bridge state root should canonicalize");
    let runtime = load_fault_injection(state_root.as_path())
        .expect("terminate bridge child should load the authenticated fault plan");
    let (supervisor, _adapter, _store) =
        connector_supervisor_fixture(root.as_path(), runtime.probe_handle());
    supervisor
        .enqueue_outbound(&connector_outbound("connector-envelope"))
        .expect("terminate connector row should enqueue");
    let executor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("terminate connector executor should initialize");
    let _ = executor.block_on(supervisor.drain_due_outbox(1));
    panic!("terminate directive must exit immediately after activation evidence is synced");
}

fn run_journal_terminate_child() -> ! {
    let root = PathBuf::from(
        std::env::var_os(JOURNAL_TERMINATE_ROOT_ENV)
            .expect("journal terminate child root should be provided"),
    );
    let plan = journal_post_commit_terminate_plan();
    fs::write(
        root.join("journal-plan.json"),
        plan.canonical_json().expect("journal plan should serialize"),
    )
    .expect("journal plan should be written");
    let launch = journal_terminate_launch(root.as_path(), &plan);
    let loaded = QaFaultEvidenceSidecarRecord::LaunchLoaded(QaFaultLaunchLoadedRecord {
        schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
        sequence: 1,
        launch_id: launch.launch_id.clone(),
        plan_sha256: launch.plan_sha256.clone(),
        capability_sha256: launch.capability_sha256.clone(),
    });
    let mut initial_evidence =
        serde_json::to_vec(&loaded).expect("journal launch evidence should serialize");
    initial_evidence.push(b'\n');
    let evidence_path = root.join("evidence.ndjson");
    fs::write(evidence_path.as_path(), initial_evidence)
        .expect("journal launch evidence should be written");
    let runtime = QaFaultRuntime::active(
        QaFaultProbeHandle::from_probe(
            DeterministicQaFaultController::new(plan.clone())
                .expect("journal controller should initialize"),
        ),
        QaFaultEvidenceState {
            launch,
            path: evidence_path,
            next_sequence: 2,
            activated_rules: BTreeMap::new(),
            activation_actors: BTreeMap::new(),
            barrier_joins: BTreeMap::new(),
            barrier_join_points: BTreeMap::new(),
            barrier_participants: BTreeMap::new(),
            barrier_release_orders: BTreeMap::new(),
            barrier_releases: BTreeMap::new(),
            observed_occurrences: BTreeMap::new(),
            occurrence_targets: occurrence_targets(&plan),
            recovered_rule_ids: BTreeSet::new(),
        },
        plan.seed,
    );
    let store = open_faulted_test_journal(root.as_path(), runtime);
    let _ = store.append(&journal_fault_request());
    panic!("post-commit journal terminate directive must exit before append returns");
}

fn open_faulted_test_journal(root: &Path, runtime: QaFaultRuntime) -> JournalStore {
    let embedding_provider = Arc::new(HashMemoryEmbeddingProvider::default());
    let embedding_runtime = MemoryEmbeddingsRuntimeProfile::legacy_from_provider(
        embedding_provider.model_name(),
        embedding_provider.dimensions(),
    );
    JournalStore::open_with_memory_embedding_runtime_and_fault_injection(
        JournalConfig {
            db_path: root.join("journal.sqlite3"),
            hash_chain_enabled: true,
            max_payload_bytes: 256 * 1024,
            max_events: 100,
        },
        embedding_provider,
        embedding_runtime,
        runtime,
    )
    .expect("faulted test journal should open")
}

fn journal_fault_request() -> JournalAppendRequest {
    JournalAppendRequest {
        event_id: "journal-event".to_owned(),
        session_id: "journal-session".to_owned(),
        run_id: "journal-run".to_owned(),
        kind: 1,
        actor: 1,
        timestamp_unix_ms: 1_730_000_000_000,
        payload_json: br#"{"kind":"qa-journal-crash"}"#.to_vec(),
        principal: "operator:test".to_owned(),
        device_id: "device:test".to_owned(),
        channel: Some("test".to_owned()),
    }
}

fn worker_attestation() -> WorkerAttestation {
    WorkerAttestation {
        worker_id: "worker-race".to_owned(),
        image_digest_sha256: "1".repeat(64),
        build_digest_sha256: "2".repeat(64),
        artifact_digest_sha256: "3".repeat(64),
        egress_proxy_attested: true,
        supported_capabilities: vec!["tool:palyra.echo".to_owned()],
        capability_authority_sha256: None,
        sdk_protocol_version: 1,
        wit_abi_version: "palyra-worker-abi/v1".to_owned(),
        heartbeat_unix_ms: 2_000,
        issued_at_unix_ms: 1_000,
        expires_at_unix_ms: 10_000,
    }
}

fn worker_lease_request(run_id: &str) -> WorkerLeaseRequest {
    WorkerLeaseRequest {
        run_id: run_id.to_owned(),
        ttl_ms: 500,
        required_capabilities: vec!["tool:palyra.echo".to_owned()],
        workspace_scope: WorkerWorkspaceScope {
            workspace_root: "/workspace".to_owned(),
            allowed_paths: vec![".".to_owned()],
            read_only: false,
        },
        artifact_transport: WorkerArtifactTransport {
            input_manifest_sha256: "4".repeat(64),
            output_manifest_sha256: "5".repeat(64),
            log_stream_id: "worker-race-log".to_owned(),
            scratch_directory_id: "worker-race-scratch".to_owned(),
        },
        grant: WorkerRunGrant {
            grant_id: format!("grant-{run_id}"),
            run_id: run_id.to_owned(),
            tool_name: "palyra.echo".to_owned(),
            expires_at_unix_ms: 9_000,
        },
    }
}

struct LoaderFixture {
    state_root: PathBuf,
    plan_path: PathBuf,
    evidence_path: PathBuf,
    launch_path: PathBuf,
    capability_path: PathBuf,
    launch: QaFaultLaunchDocument,
}

fn prepare_loader_fixture(
    root: &Path,
    plan: &QaFaultInjectionPlan,
    launch_id: &str,
    capability_hex: &str,
) -> LoaderFixture {
    let state_root = root.join("state");
    let private_root = state_root.join("qa-fault");
    ensure_owner_only_dir(private_root.as_path()).expect("private QA directory should be hardened");
    let state_root =
        fs::canonicalize(state_root.as_path()).expect("state root should canonicalize");
    let private_root =
        fs::canonicalize(private_root.as_path()).expect("private QA directory should canonicalize");
    let plan_path = private_root.join(format!("{launch_id}-plan.json"));
    fs::write(plan_path.as_path(), plan.canonical_json().expect("loader plan should serialize"))
        .expect("loader plan should be written");
    ensure_owner_only_file(plan_path.as_path()).expect("loader plan should be hardened");
    let evidence_path = private_root.join(format!("{launch_id}-evidence.ndjson"));
    let launch = write_launch(
        state_root.as_path(),
        plan_path.as_path(),
        evidence_path.as_path(),
        plan.canonical_sha256().expect("loader plan should hash").as_str(),
        launch_id,
        capability_hex,
    );
    LoaderFixture {
        launch_path: private_root.join(format!("{launch_id}.json")),
        capability_path: private_root.join(format!("{launch_id}.cap")),
        state_root,
        plan_path,
        evidence_path,
        launch,
    }
}

fn assert_no_launch_loaded(evidence_path: &Path) {
    if let Ok(bytes) = fs::read(evidence_path) {
        assert!(
            !bytes.windows(b"launch_loaded".len()).any(|window| window == b"launch_loaded"),
            "invalid loader input must not append launch evidence"
        );
    }
}

fn write_launch(
    state_root: &Path,
    plan_path: &Path,
    evidence_path: &Path,
    plan_sha256: &str,
    launch_id: &str,
    capability_hex: &str,
) -> QaFaultLaunchDocument {
    let relative_root = PathBuf::from("qa-fault");
    let launch_relative = relative_root.join(format!("{launch_id}.json"));
    let capability_relative = relative_root.join(format!("{launch_id}.cap"));
    let capability_path = state_root.join(capability_relative.as_path());
    let capability = hex::decode(capability_hex).expect("test capability should decode");
    fs::write(capability_path.as_path(), format!("{QA_FAULT_CAPABILITY_PREFIX}{capability_hex}\n"))
        .expect("test capability should be written");
    ensure_owner_only_file(capability_path.as_path()).expect("test capability should be hardened");

    let launch = QaFaultLaunchDocument {
        schema_version: QA_FAULT_LAUNCH_SCHEMA_VERSION,
        launch_id: launch_id.to_owned(),
        plan_path: plan_path.to_string_lossy().into_owned(),
        plan_sha256: plan_sha256.to_owned(),
        capability_sha256: hex::encode(Sha256::digest(capability)),
        evidence_path: evidence_path.to_string_lossy().into_owned(),
        expires_at_unix_ms: current_unix_ms().saturating_add(60_000),
    };
    let launch_path = state_root.join(launch_relative.as_path());
    fs::write(
        launch_path.as_path(),
        serde_json::to_vec(&launch).expect("test launch should serialize"),
    )
    .expect("test launch should be written");
    ensure_owner_only_file(launch_path.as_path()).expect("test launch should be hardened");
    std::env::set_var(QA_FAULT_LAUNCH_PATH_ENV, launch_relative);
    std::env::set_var(QA_FAULT_CAPABILITY_PATH_ENV, capability_relative);
    launch
}

fn write_evidence_records(path: &Path, records: &[QaFaultEvidenceSidecarRecord]) {
    let mut encoded = Vec::new();
    for record in records {
        serde_json::to_writer(&mut encoded, record).expect("test evidence should serialize");
        encoded.push(b'\n');
    }
    fs::write(path, encoded).expect("test evidence should be written");
    ensure_owner_only_file(path).expect("test evidence should be hardened");
}

fn current_unix_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after Unix epoch")
            .as_millis(),
    )
    .expect("test time should fit i64")
}

mod journal;
mod loader;
mod runtime_adapters;
