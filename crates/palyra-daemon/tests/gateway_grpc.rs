use std::{
    fs,
    io::{BufRead, BufReader, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::PathBuf,
    process::{Child, ChildStdout, Command, Stdio},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc, Arc,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use reqwest::blocking::Client;
use rusqlite::{params, Connection};
use serde_json::Value;
use sha2::{Digest, Sha256};
#[cfg(unix)]
use std::path::Path;
use tokio::sync::mpsc as tokio_mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::Code;

const ADMIN_TOKEN: &str = "test-admin-token";
const DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const SESSION_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAW";
const RUN_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAX";
const RUN_ID_ALT: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAZ";
const ENVELOPE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAY";
const OPENAI_API_KEY: &str = "sk-openai-integration-test";
const MAX_TEST_CRON_JITTER_MS: u64 = 60_000;
const GRPC_OVERSIZED_PAYLOAD_BYTES: usize = (4 * 1024 * 1024) + 8 * 1024;
const TRANSPORT_LIMIT_TEST_JOURNAL_MAX_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;
static TEMP_JOURNAL_COUNTER: AtomicU64 = AtomicU64::new(0);
static TEMP_CONFIG_COUNTER: AtomicU64 = AtomicU64::new(0);
static TEMP_IDENTITY_COUNTER: AtomicU64 = AtomicU64::new(0);
static TEMP_VAULT_COUNTER: AtomicU64 = AtomicU64::new(0);

pub mod proto {
    pub mod palyra {
        pub mod common {
            pub mod v1 {
                tonic::include_proto!("palyra.common.v1");
            }
        }

        pub mod gateway {
            pub mod v1 {
                tonic::include_proto!("palyra.gateway.v1");
            }
        }

        pub mod cron {
            pub mod v1 {
                tonic::include_proto!("palyra.cron.v1");
            }
        }

        pub mod memory {
            pub mod v1 {
                tonic::include_proto!("palyra.memory.v1");
            }
        }
    }
}

use proto::palyra::{
    common::v1 as common_v1, cron::v1 as cron_v1, gateway::v1 as gateway_v1,
    memory::v1 as memory_v1,
};

#[tokio::test(flavor = "multi_thread")]
async fn grpc_gateway_enforces_auth_and_streams_status() -> Result<()> {
    let (child, admin_port, grpc_port, _journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");

    let mut unauthorized_client =
        gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint.clone())
            .await
            .context("failed to connect unauthorized gRPC client")?;
    let denied = unauthorized_client
        .run_stream(tonic::Request::new(tokio_stream::iter(vec![sample_run_stream_request()])))
        .await
        .expect_err("run_stream should reject requests without auth context");
    assert_eq!(denied.code(), Code::PermissionDenied);

    let mut client =
        gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint.clone())
            .await
            .context("failed to connect gRPC client")?;
    let health = client
        .get_health(gateway_v1::HealthRequest { v: 1 })
        .await
        .context("failed to call GetHealth")?
        .into_inner();
    assert_eq!(health.status, "ok");
    assert_eq!(health.service, "palyrad");

    let mut stream_request =
        tonic::Request::new(tokio_stream::iter(vec![sample_run_stream_request()]));
    stream_request.metadata_mut().insert("authorization", format!("Bearer {ADMIN_TOKEN}").parse()?);
    stream_request.metadata_mut().insert("x-palyra-principal", "user:ops".parse()?);
    stream_request.metadata_mut().insert("x-palyra-device-id", DEVICE_ID.parse()?);
    stream_request.metadata_mut().insert("x-palyra-channel", "cli".parse()?);

    let mut response_stream =
        client.run_stream(stream_request).await.context("failed to call RunStream")?.into_inner();

    let mut saw_accepted = false;
    let mut saw_done = false;
    while let Some(event) = response_stream.next().await {
        let event = event.context("failed to read RunStream event")?;
        if let Some(common_v1::run_stream_event::Body::Status(status)) = event.body {
            if status.kind == common_v1::stream_status::StatusKind::Accepted as i32 {
                saw_accepted = true;
            }
            if status.kind == common_v1::stream_status::StatusKind::Done as i32 {
                saw_done = true;
            }
        }
    }
    assert!(saw_accepted, "run stream should emit accepted status");
    assert!(saw_done, "run stream should emit done status");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_resolve_session_and_list_sessions_roundtrip() -> Result<()> {
    let (child, admin_port, grpc_port, _journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
        .await
        .context("failed to connect gRPC client")?;

    let mut resolve_request = tonic::Request::new(gateway_v1::ResolveSessionRequest {
        v: 1,
        session_id: None,
        session_key: "agent:main:main".to_owned(),
        session_label: "Main".to_owned(),
        require_existing: false,
        reset_session: false,
    });
    authorize_metadata(resolve_request.metadata_mut())?;
    let resolved = client
        .resolve_session(resolve_request)
        .await
        .context("failed to call ResolveSession")?
        .into_inner();
    assert!(resolved.created, "first resolve should create a session");
    let summary = resolved.session.context("resolve response missing session summary")?;
    assert_eq!(summary.session_key, "agent:main:main");
    assert_eq!(summary.session_label, "Main");
    assert!(summary.session_id.is_some(), "resolve response must include canonical session id");

    let mut second_resolve_request = tonic::Request::new(gateway_v1::ResolveSessionRequest {
        v: 1,
        session_id: None,
        session_key: "agent:main:main".to_owned(),
        session_label: String::new(),
        require_existing: true,
        reset_session: false,
    });
    authorize_metadata(second_resolve_request.metadata_mut())?;
    let second = client
        .resolve_session(second_resolve_request)
        .await
        .context("failed to call ResolveSession for existing key")?
        .into_inner();
    assert!(!second.created, "existing session key should resolve without creating a new session");

    let mut list_request = tonic::Request::new(gateway_v1::ListSessionsRequest {
        v: 1,
        after_session_key: String::new(),
        limit: 10,
    });
    authorize_metadata(list_request.metadata_mut())?;
    let listed = client
        .list_sessions(list_request)
        .await
        .context("failed to call ListSessions")?
        .into_inner();
    assert!(
        listed.sessions.iter().any(|session| session.session_key == "agent:main:main"),
        "listed sessions must include resolved session key"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_cron_create_run_now_and_list_runs_roundtrip() -> Result<()> {
    let (child, admin_port, grpc_port, _journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut cron_client = cron_v1::cron_service_client::CronServiceClient::connect(endpoint)
        .await
        .context("failed to connect cron gRPC client")?;

    let mut create_request = tonic::Request::new(cron_v1::CreateJobRequest {
        v: 1,
        name: "Health summary".to_owned(),
        prompt: "Summarize daemon health".to_owned(),
        owner_principal: "user:ops".to_owned(),
        channel: "system:cron".to_owned(),
        session_key: "cron:health-summary".to_owned(),
        session_label: "Health summary".to_owned(),
        schedule: Some(cron_v1::Schedule {
            r#type: cron_v1::ScheduleType::Every as i32,
            spec: Some(cron_v1::schedule::Spec::Every(cron_v1::EverySchedule {
                interval_ms: 3_600_000,
            })),
        }),
        enabled: true,
        concurrency_policy: cron_v1::ConcurrencyPolicy::Forbid as i32,
        retry_policy: Some(cron_v1::RetryPolicy { max_attempts: 1, backoff_ms: 1 }),
        misfire_policy: cron_v1::MisfirePolicy::Skip as i32,
        jitter_ms: 0,
    });
    authorize_metadata(create_request.metadata_mut())?;
    let created = cron_client
        .create_job(create_request)
        .await
        .context("failed to call cron CreateJob")?
        .into_inner();
    let job = created.job.context("CreateJob must return job payload")?;
    let job_id = job
        .job_id
        .as_ref()
        .map(|value| value.ulid.clone())
        .context("CreateJob must return canonical job id")?;

    let mut run_now_request = tonic::Request::new(cron_v1::RunJobNowRequest {
        v: 1,
        job_id: Some(common_v1::CanonicalId { ulid: job_id.clone() }),
    });
    authorize_metadata(run_now_request.metadata_mut())?;
    let run_now = cron_client
        .run_job_now(run_now_request)
        .await
        .context("failed to call cron RunJobNow")?
        .into_inner();
    let run_id = run_now
        .run_id
        .as_ref()
        .map(|value| value.ulid.clone())
        .context("RunJobNow must return canonical run id")?;

    let terminal_statuses = [
        cron_v1::JobRunStatus::Succeeded as i32,
        cron_v1::JobRunStatus::Failed as i32,
        cron_v1::JobRunStatus::Denied as i32,
        cron_v1::JobRunStatus::Skipped as i32,
    ];
    let mut observed_status = None::<i32>;
    for _ in 0..40 {
        let mut list_runs_request = tonic::Request::new(cron_v1::ListJobRunsRequest {
            v: 1,
            job_id: Some(common_v1::CanonicalId { ulid: job_id.clone() }),
            after_run_ulid: String::new(),
            limit: 25,
        });
        authorize_metadata(list_runs_request.metadata_mut())?;
        let listed = cron_client
            .list_job_runs(list_runs_request)
            .await
            .context("failed to call cron ListJobRuns")?
            .into_inner();
        if let Some(run) = listed
            .runs
            .iter()
            .find(|run| run.run_id.as_ref().map(|id| id.ulid.as_str()) == Some(run_id.as_str()))
        {
            observed_status = Some(run.status);
            if terminal_statuses.contains(&run.status) {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let status = observed_status.context("cron run should be visible in list_job_runs")?;
    assert!(
        terminal_statuses.contains(&status),
        "cron run should eventually reach a terminal status, observed={status}"
    );

    let mut get_run_request = tonic::Request::new(cron_v1::GetJobRunRequest {
        v: 1,
        run_id: Some(common_v1::CanonicalId { ulid: run_id.clone() }),
    });
    authorize_metadata(get_run_request.metadata_mut())?;
    let get_run = cron_client
        .get_job_run(get_run_request)
        .await
        .context("failed to call cron GetJobRun")?
        .into_inner();
    let run = get_run.run.context("GetJobRun must return run payload")?;
    assert_eq!(
        run.run_id.as_ref().map(|value| value.ulid.as_str()),
        Some(run_id.as_str()),
        "GetJobRun should return the requested run"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_cron_create_rejects_invalid_schedule_expression() -> Result<()> {
    let (child, admin_port, grpc_port, _journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut cron_client = cron_v1::cron_service_client::CronServiceClient::connect(endpoint)
        .await
        .context("failed to connect cron gRPC client")?;

    let mut create_request = tonic::Request::new(cron_v1::CreateJobRequest {
        v: 1,
        name: "Invalid cron".to_owned(),
        prompt: "This should fail".to_owned(),
        owner_principal: "user:ops".to_owned(),
        channel: "system:cron".to_owned(),
        session_key: "cron:invalid-expression".to_owned(),
        session_label: "Invalid cron".to_owned(),
        schedule: Some(cron_v1::Schedule {
            r#type: cron_v1::ScheduleType::Cron as i32,
            spec: Some(cron_v1::schedule::Spec::Cron(cron_v1::CronSchedule {
                expression: "*/0 * * * *".to_owned(),
            })),
        }),
        enabled: true,
        concurrency_policy: cron_v1::ConcurrencyPolicy::Forbid as i32,
        retry_policy: Some(cron_v1::RetryPolicy { max_attempts: 1, backoff_ms: 1 }),
        misfire_policy: cron_v1::MisfirePolicy::Skip as i32,
        jitter_ms: 0,
    });
    authorize_metadata(create_request.metadata_mut())?;

    let error = cron_client
        .create_job(create_request)
        .await
        .expect_err("CreateJob should reject invalid cron expressions");
    assert_eq!(error.code(), Code::InvalidArgument);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_cron_create_rejects_owner_principal_impersonation() -> Result<()> {
    let (child, admin_port, grpc_port, _journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut cron_client = cron_v1::cron_service_client::CronServiceClient::connect(endpoint)
        .await
        .context("failed to connect cron gRPC client")?;

    let mut create_request = tonic::Request::new(cron_v1::CreateJobRequest {
        v: 1,
        name: "Impersonation attempt".to_owned(),
        prompt: "This should be denied".to_owned(),
        owner_principal: "user:finance".to_owned(),
        channel: "system:cron".to_owned(),
        session_key: "cron:impersonation-attempt".to_owned(),
        session_label: "Impersonation attempt".to_owned(),
        schedule: Some(cron_v1::Schedule {
            r#type: cron_v1::ScheduleType::Every as i32,
            spec: Some(cron_v1::schedule::Spec::Every(cron_v1::EverySchedule {
                interval_ms: 3_600_000,
            })),
        }),
        enabled: true,
        concurrency_policy: cron_v1::ConcurrencyPolicy::Forbid as i32,
        retry_policy: Some(cron_v1::RetryPolicy { max_attempts: 1, backoff_ms: 1 }),
        misfire_policy: cron_v1::MisfirePolicy::Skip as i32,
        jitter_ms: 0,
    });
    authorize_metadata_with_principal(create_request.metadata_mut(), "user:ops")?;

    let error = cron_client
        .create_job(create_request)
        .await
        .expect_err("CreateJob should reject mismatched owner_principal values");
    assert_eq!(error.code(), Code::PermissionDenied);
    assert!(
        error.message().contains("owner_principal"),
        "error should explain owner principal mismatch"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_cron_update_rejects_owner_principal_impersonation() -> Result<()> {
    let (child, admin_port, grpc_port, _journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut cron_client = cron_v1::cron_service_client::CronServiceClient::connect(endpoint)
        .await
        .context("failed to connect cron gRPC client")?;

    let mut create_request = tonic::Request::new(cron_v1::CreateJobRequest {
        v: 1,
        name: "Owned by ops".to_owned(),
        prompt: "Create before update".to_owned(),
        owner_principal: "user:ops".to_owned(),
        channel: "system:cron".to_owned(),
        session_key: "cron:update-owner-guard".to_owned(),
        session_label: "Owned by ops".to_owned(),
        schedule: Some(cron_v1::Schedule {
            r#type: cron_v1::ScheduleType::Every as i32,
            spec: Some(cron_v1::schedule::Spec::Every(cron_v1::EverySchedule {
                interval_ms: 3_600_000,
            })),
        }),
        enabled: true,
        concurrency_policy: cron_v1::ConcurrencyPolicy::Forbid as i32,
        retry_policy: Some(cron_v1::RetryPolicy { max_attempts: 1, backoff_ms: 1 }),
        misfire_policy: cron_v1::MisfirePolicy::Skip as i32,
        jitter_ms: 0,
    });
    authorize_metadata_with_principal(create_request.metadata_mut(), "user:ops")?;
    let created = cron_client
        .create_job(create_request)
        .await
        .context("failed to create baseline cron job")?
        .into_inner();
    let job_id = created
        .job
        .as_ref()
        .and_then(|job| job.job_id.as_ref())
        .map(|id| id.ulid.clone())
        .context("CreateJob must return canonical job id")?;

    let mut update_request = tonic::Request::new(cron_v1::UpdateJobRequest {
        v: 1,
        job_id: Some(common_v1::CanonicalId { ulid: job_id }),
        name: None,
        prompt: None,
        owner_principal: Some("user:finance".to_owned()),
        channel: None,
        session_key: None,
        session_label: None,
        schedule: None,
        enabled: None,
        concurrency_policy: None,
        retry_policy: None,
        misfire_policy: None,
        jitter_ms: None,
    });
    authorize_metadata_with_principal(update_request.metadata_mut(), "user:ops")?;

    let error = cron_client
        .update_job(update_request)
        .await
        .expect_err("UpdateJob should reject mismatched owner_principal values");
    assert_eq!(error.code(), Code::PermissionDenied);
    assert!(
        error.message().contains("owner_principal"),
        "error should explain owner principal mismatch"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_cron_non_owner_access_is_denied_for_job_and_run_endpoints() -> Result<()> {
    let (child, admin_port, grpc_port, _journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut cron_client = cron_v1::cron_service_client::CronServiceClient::connect(endpoint)
        .await
        .context("failed to connect cron gRPC client")?;

    let mut create_request = tonic::Request::new(cron_v1::CreateJobRequest {
        v: 1,
        name: "Owner-bound cron job".to_owned(),
        prompt: "Owner-only access".to_owned(),
        owner_principal: "user:ops".to_owned(),
        channel: "system:cron".to_owned(),
        session_key: "cron:owner-bound-job".to_owned(),
        session_label: "Owner-bound".to_owned(),
        schedule: Some(cron_v1::Schedule {
            r#type: cron_v1::ScheduleType::Every as i32,
            spec: Some(cron_v1::schedule::Spec::Every(cron_v1::EverySchedule {
                interval_ms: 3_600_000,
            })),
        }),
        enabled: true,
        concurrency_policy: cron_v1::ConcurrencyPolicy::Forbid as i32,
        retry_policy: Some(cron_v1::RetryPolicy { max_attempts: 1, backoff_ms: 1 }),
        misfire_policy: cron_v1::MisfirePolicy::Skip as i32,
        jitter_ms: 0,
    });
    authorize_metadata_with_principal(create_request.metadata_mut(), "user:ops")?;
    let created = cron_client
        .create_job(create_request)
        .await
        .context("failed to create baseline owner-bound cron job")?
        .into_inner();
    let job_id = created
        .job
        .as_ref()
        .and_then(|job| job.job_id.as_ref())
        .map(|id| id.ulid.clone())
        .context("CreateJob must return canonical job id")?;

    let mut owner_run_now_request = tonic::Request::new(cron_v1::RunJobNowRequest {
        v: 1,
        job_id: Some(common_v1::CanonicalId { ulid: job_id.clone() }),
    });
    authorize_metadata_with_principal(owner_run_now_request.metadata_mut(), "user:ops")?;
    let owner_run_now = cron_client
        .run_job_now(owner_run_now_request)
        .await
        .context("failed to call cron RunJobNow as owner")?
        .into_inner();
    let run_id = owner_run_now
        .run_id
        .as_ref()
        .map(|id| id.ulid.clone())
        .context("RunJobNow as owner must return run id when dispatch starts")?;

    let mut get_request = tonic::Request::new(cron_v1::GetJobRequest {
        v: 1,
        job_id: Some(common_v1::CanonicalId { ulid: job_id.clone() }),
    });
    authorize_metadata_with_principal(get_request.metadata_mut(), "user:auditor")?;
    let error =
        cron_client.get_job(get_request).await.expect_err("cross-principal GetJob must be denied");
    assert_eq!(error.code(), Code::PermissionDenied);

    let mut update_request = tonic::Request::new(cron_v1::UpdateJobRequest {
        v: 1,
        job_id: Some(common_v1::CanonicalId { ulid: job_id.clone() }),
        name: Some("Intrusive rename".to_owned()),
        prompt: None,
        owner_principal: None,
        channel: None,
        session_key: None,
        session_label: None,
        schedule: None,
        enabled: None,
        concurrency_policy: None,
        retry_policy: None,
        misfire_policy: None,
        jitter_ms: None,
    });
    authorize_metadata_with_principal(update_request.metadata_mut(), "user:auditor")?;
    let error = cron_client
        .update_job(update_request)
        .await
        .expect_err("cross-principal UpdateJob must be denied");
    assert_eq!(error.code(), Code::PermissionDenied);

    let mut run_now_request = tonic::Request::new(cron_v1::RunJobNowRequest {
        v: 1,
        job_id: Some(common_v1::CanonicalId { ulid: job_id.clone() }),
    });
    authorize_metadata_with_principal(run_now_request.metadata_mut(), "user:auditor")?;
    let error = cron_client
        .run_job_now(run_now_request)
        .await
        .expect_err("cross-principal RunJobNow must be denied");
    assert_eq!(error.code(), Code::PermissionDenied);

    let mut list_runs_request = tonic::Request::new(cron_v1::ListJobRunsRequest {
        v: 1,
        job_id: Some(common_v1::CanonicalId { ulid: job_id.clone() }),
        after_run_ulid: String::new(),
        limit: 10,
    });
    authorize_metadata_with_principal(list_runs_request.metadata_mut(), "user:auditor")?;
    let error = cron_client
        .list_job_runs(list_runs_request)
        .await
        .expect_err("cross-principal ListJobRuns must be denied");
    assert_eq!(error.code(), Code::PermissionDenied);

    let mut get_run_request = tonic::Request::new(cron_v1::GetJobRunRequest {
        v: 1,
        run_id: Some(common_v1::CanonicalId { ulid: run_id }),
    });
    authorize_metadata_with_principal(get_run_request.metadata_mut(), "user:auditor")?;
    let error = cron_client
        .get_job_run(get_run_request)
        .await
        .expect_err("cross-principal GetJobRun must be denied");
    assert_eq!(error.code(), Code::PermissionDenied);

    let mut delete_request = tonic::Request::new(cron_v1::DeleteJobRequest {
        v: 1,
        job_id: Some(common_v1::CanonicalId { ulid: job_id }),
    });
    authorize_metadata_with_principal(delete_request.metadata_mut(), "user:auditor")?;
    let error = cron_client
        .delete_job(delete_request)
        .await
        .expect_err("cross-principal DeleteJob must be denied");
    assert_eq!(error.code(), Code::PermissionDenied);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_cron_list_jobs_is_scoped_to_authenticated_principal() -> Result<()> {
    let (child, admin_port, grpc_port, _journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut cron_client = cron_v1::cron_service_client::CronServiceClient::connect(endpoint)
        .await
        .context("failed to connect cron gRPC client")?;

    for principal in ["user:ops", "user:finance"] {
        let mut create_request = tonic::Request::new(cron_v1::CreateJobRequest {
            v: 1,
            name: format!("Job for {principal}"),
            prompt: "List ownership scope".to_owned(),
            owner_principal: principal.to_owned(),
            channel: "system:cron".to_owned(),
            session_key: format!("cron:list-scope:{principal}"),
            session_label: principal.to_owned(),
            schedule: Some(cron_v1::Schedule {
                r#type: cron_v1::ScheduleType::Every as i32,
                spec: Some(cron_v1::schedule::Spec::Every(cron_v1::EverySchedule {
                    interval_ms: 3_600_000,
                })),
            }),
            enabled: true,
            concurrency_policy: cron_v1::ConcurrencyPolicy::Forbid as i32,
            retry_policy: Some(cron_v1::RetryPolicy { max_attempts: 1, backoff_ms: 1 }),
            misfire_policy: cron_v1::MisfirePolicy::Skip as i32,
            jitter_ms: 0,
        });
        authorize_metadata_with_principal(create_request.metadata_mut(), principal)?;
        cron_client
            .create_job(create_request)
            .await
            .with_context(|| format!("failed to create cron job for {principal}"))?;
    }

    let mut list_request = tonic::Request::new(cron_v1::ListJobsRequest {
        v: 1,
        after_job_ulid: String::new(),
        limit: 100,
        enabled: None,
        owner_principal: None,
        channel: None,
    });
    authorize_metadata_with_principal(list_request.metadata_mut(), "user:ops")?;
    let listed = cron_client
        .list_jobs(list_request)
        .await
        .context("failed to list cron jobs for user:ops")?
        .into_inner();
    assert!(
        !listed.jobs.is_empty(),
        "list response for user:ops should contain at least one owned job"
    );
    assert!(
        listed.jobs.iter().all(|job| job.owner_principal == "user:ops"),
        "list_jobs should return only jobs owned by authenticated principal"
    );

    let mut mismatched_owner_request = tonic::Request::new(cron_v1::ListJobsRequest {
        v: 1,
        after_job_ulid: String::new(),
        limit: 100,
        enabled: None,
        owner_principal: Some("user:finance".to_owned()),
        channel: None,
    });
    authorize_metadata_with_principal(mismatched_owner_request.metadata_mut(), "user:ops")?;
    let error = cron_client
        .list_jobs(mismatched_owner_request)
        .await
        .expect_err("list_jobs should reject owner filters that mismatch authenticated principal");
    assert_eq!(error.code(), Code::PermissionDenied);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_cron_create_rejects_jitter_above_limit() -> Result<()> {
    let (child, admin_port, grpc_port, _journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut cron_client = cron_v1::cron_service_client::CronServiceClient::connect(endpoint)
        .await
        .context("failed to connect cron gRPC client")?;

    let mut create_request = tonic::Request::new(cron_v1::CreateJobRequest {
        v: 1,
        name: "Jitter too high".to_owned(),
        prompt: "Reject oversized jitter".to_owned(),
        owner_principal: "user:ops".to_owned(),
        channel: "system:cron".to_owned(),
        session_key: "cron:jitter-too-high".to_owned(),
        session_label: "Jitter too high".to_owned(),
        schedule: Some(cron_v1::Schedule {
            r#type: cron_v1::ScheduleType::Every as i32,
            spec: Some(cron_v1::schedule::Spec::Every(cron_v1::EverySchedule {
                interval_ms: 3_600_000,
            })),
        }),
        enabled: true,
        concurrency_policy: cron_v1::ConcurrencyPolicy::Forbid as i32,
        retry_policy: Some(cron_v1::RetryPolicy { max_attempts: 1, backoff_ms: 1 }),
        misfire_policy: cron_v1::MisfirePolicy::Skip as i32,
        jitter_ms: MAX_TEST_CRON_JITTER_MS + 1,
    });
    authorize_metadata(create_request.metadata_mut())?;

    let error = cron_client
        .create_job(create_request)
        .await
        .expect_err("CreateJob should reject jitter above maximum limit");
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(error.message().contains("jitter_ms"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_cron_update_rejects_jitter_above_limit() -> Result<()> {
    let (child, admin_port, grpc_port, _journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut cron_client = cron_v1::cron_service_client::CronServiceClient::connect(endpoint)
        .await
        .context("failed to connect cron gRPC client")?;

    let mut create_request = tonic::Request::new(cron_v1::CreateJobRequest {
        v: 1,
        name: "Jitter update baseline".to_owned(),
        prompt: "Update jitter validation".to_owned(),
        owner_principal: "user:ops".to_owned(),
        channel: "system:cron".to_owned(),
        session_key: "cron:jitter-update".to_owned(),
        session_label: "Jitter update".to_owned(),
        schedule: Some(cron_v1::Schedule {
            r#type: cron_v1::ScheduleType::Every as i32,
            spec: Some(cron_v1::schedule::Spec::Every(cron_v1::EverySchedule {
                interval_ms: 3_600_000,
            })),
        }),
        enabled: true,
        concurrency_policy: cron_v1::ConcurrencyPolicy::Forbid as i32,
        retry_policy: Some(cron_v1::RetryPolicy { max_attempts: 1, backoff_ms: 1 }),
        misfire_policy: cron_v1::MisfirePolicy::Skip as i32,
        jitter_ms: MAX_TEST_CRON_JITTER_MS,
    });
    authorize_metadata(create_request.metadata_mut())?;
    let created = cron_client
        .create_job(create_request)
        .await
        .context("failed to create baseline cron job for jitter update test")?
        .into_inner();
    let job_id = created
        .job
        .as_ref()
        .and_then(|job| job.job_id.as_ref())
        .map(|id| id.ulid.clone())
        .context("CreateJob must return canonical job id")?;

    let mut update_request = tonic::Request::new(cron_v1::UpdateJobRequest {
        v: 1,
        job_id: Some(common_v1::CanonicalId { ulid: job_id }),
        name: None,
        prompt: None,
        owner_principal: None,
        channel: None,
        session_key: None,
        session_label: None,
        schedule: None,
        enabled: None,
        concurrency_policy: None,
        retry_policy: None,
        misfire_policy: None,
        jitter_ms: Some(MAX_TEST_CRON_JITTER_MS + 1),
    });
    authorize_metadata(update_request.metadata_mut())?;
    let error = cron_client
        .update_job(update_request)
        .await
        .expect_err("UpdateJob should reject jitter above maximum limit");
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(error.message().contains("jitter_ms"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_cron_run_now_queue_one_returns_no_run_id_when_queued() -> Result<()> {
    let (child, admin_port, grpc_port, journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut cron_client = cron_v1::cron_service_client::CronServiceClient::connect(endpoint)
        .await
        .context("failed to connect cron gRPC client")?;

    let mut create_request = tonic::Request::new(cron_v1::CreateJobRequest {
        v: 1,
        name: "Queue one response".to_owned(),
        prompt: "Queue one should not return phantom run id".to_owned(),
        owner_principal: "user:ops".to_owned(),
        channel: "system:cron".to_owned(),
        session_key: "cron:queue-one-response".to_owned(),
        session_label: "Queue one response".to_owned(),
        schedule: Some(cron_v1::Schedule {
            r#type: cron_v1::ScheduleType::Every as i32,
            spec: Some(cron_v1::schedule::Spec::Every(cron_v1::EverySchedule {
                interval_ms: 3_600_000,
            })),
        }),
        enabled: true,
        concurrency_policy: cron_v1::ConcurrencyPolicy::QueueOne as i32,
        retry_policy: Some(cron_v1::RetryPolicy { max_attempts: 1, backoff_ms: 1 }),
        misfire_policy: cron_v1::MisfirePolicy::Skip as i32,
        jitter_ms: 0,
    });
    authorize_metadata(create_request.metadata_mut())?;
    let created = cron_client
        .create_job(create_request)
        .await
        .context("failed to create queue-one cron job")?
        .into_inner();
    let job_id = created
        .job
        .as_ref()
        .and_then(|job| job.job_id.as_ref())
        .map(|id| id.ulid.clone())
        .context("CreateJob must return canonical job id")?;

    let now_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock should be after unix epoch")?
        .as_millis() as i64;
    let connection = Connection::open(journal_db_path)
        .context("failed to open journal sqlite db for queue-one seed run")?;
    connection
        .execute(
            r#"
                INSERT INTO cron_runs (
                    run_ulid,
                    job_ulid,
                    attempt,
                    session_ulid,
                    orchestrator_run_ulid,
                    started_at_unix_ms,
                    finished_at_unix_ms,
                    status,
                    error_kind,
                    error_message_redacted,
                    model_tokens_in,
                    model_tokens_out,
                    tool_calls,
                    tool_denies,
                    created_at_unix_ms,
                    updated_at_unix_ms
                ) VALUES (?1, ?2, 1, NULL, NULL, ?3, NULL, 'running', NULL, NULL, 0, 0, 0, 0, ?3, ?3)
            "#,
            params!["01ARZ3NDEKTSV4RRFFQ69G5FBC", job_id, now_unix_ms],
        )
        .context("failed to seed active cron run for queue-one test")?;

    let mut run_now_request = tonic::Request::new(cron_v1::RunJobNowRequest {
        v: 1,
        job_id: Some(common_v1::CanonicalId { ulid: job_id }),
    });
    authorize_metadata(run_now_request.metadata_mut())?;
    let run_now = cron_client
        .run_job_now(run_now_request)
        .await
        .context("failed to call cron RunJobNow for queue-one test")?
        .into_inner();
    assert_eq!(
        run_now.status,
        cron_v1::JobRunStatus::Accepted as i32,
        "QueueOne with active run should accept and queue the execution"
    );
    assert!(run_now.run_id.is_none(), "queued dispatch must not return a non-existent run id");
    assert!(run_now.message.contains("queued"), "response should explain the run was queued");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_cron_run_now_skips_when_forbid_policy_has_active_run() -> Result<()> {
    let (child, admin_port, grpc_port, journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut cron_client = cron_v1::cron_service_client::CronServiceClient::connect(endpoint)
        .await
        .context("failed to connect cron gRPC client")?;

    let mut create_request = tonic::Request::new(cron_v1::CreateJobRequest {
        v: 1,
        name: "Concurrency forbid".to_owned(),
        prompt: "Prevent overlap".to_owned(),
        owner_principal: "user:ops".to_owned(),
        channel: "system:cron".to_owned(),
        session_key: "cron:forbid-concurrency".to_owned(),
        session_label: "Concurrency forbid".to_owned(),
        schedule: Some(cron_v1::Schedule {
            r#type: cron_v1::ScheduleType::Every as i32,
            spec: Some(cron_v1::schedule::Spec::Every(cron_v1::EverySchedule {
                interval_ms: 3_600_000,
            })),
        }),
        enabled: true,
        concurrency_policy: cron_v1::ConcurrencyPolicy::Forbid as i32,
        retry_policy: Some(cron_v1::RetryPolicy { max_attempts: 1, backoff_ms: 1 }),
        misfire_policy: cron_v1::MisfirePolicy::Skip as i32,
        jitter_ms: 0,
    });
    authorize_metadata(create_request.metadata_mut())?;
    let created = cron_client
        .create_job(create_request)
        .await
        .context("failed to create cron job for concurrency test")?
        .into_inner();
    let job = created.job.context("CreateJob must return job payload")?;
    let job_id = job
        .job_id
        .as_ref()
        .map(|value| value.ulid.clone())
        .context("CreateJob must return canonical job id")?;

    let now_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock should be after unix epoch")?
        .as_millis() as i64;
    let connection = Connection::open(journal_db_path)
        .context("failed to open journal sqlite db for seed run")?;
    connection
        .execute(
            r#"
                INSERT INTO cron_runs (
                    run_ulid,
                    job_ulid,
                    attempt,
                    session_ulid,
                    orchestrator_run_ulid,
                    started_at_unix_ms,
                    finished_at_unix_ms,
                    status,
                    error_kind,
                    error_message_redacted,
                    model_tokens_in,
                    model_tokens_out,
                    tool_calls,
                    tool_denies,
                    created_at_unix_ms,
                    updated_at_unix_ms
                ) VALUES (?1, ?2, 1, NULL, NULL, ?3, NULL, 'running', NULL, NULL, 0, 0, 0, 0, ?3, ?3)
            "#,
            params!["01ARZ3NDEKTSV4RRFFQ69G5FBB", job_id, now_unix_ms],
        )
        .context("failed to seed active cron run")?;

    let mut run_now_request = tonic::Request::new(cron_v1::RunJobNowRequest {
        v: 1,
        job_id: Some(common_v1::CanonicalId { ulid: job_id }),
    });
    authorize_metadata(run_now_request.metadata_mut())?;
    let run_now = cron_client
        .run_job_now(run_now_request)
        .await
        .context("failed to call cron RunJobNow for concurrency test")?
        .into_inner();
    assert_eq!(
        run_now.status,
        cron_v1::JobRunStatus::Skipped as i32,
        "RunJobNow should skip when policy=forbid and an active run exists"
    );
    assert!(
        run_now.message.contains("forbids overlapping runs"),
        "skip reason should explain forbid overlap policy"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_cron_jobs_survive_daemon_restart() -> Result<()> {
    let (child, admin_port, grpc_port, journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut cron_client = cron_v1::cron_service_client::CronServiceClient::connect(endpoint)
        .await
        .context("failed to connect cron gRPC client")?;

    let mut create_request = tonic::Request::new(cron_v1::CreateJobRequest {
        v: 1,
        name: "Persistent cron job".to_owned(),
        prompt: "Persist me".to_owned(),
        owner_principal: "user:ops".to_owned(),
        channel: "system:cron".to_owned(),
        session_key: "cron:persistent-job".to_owned(),
        session_label: "Persistent cron job".to_owned(),
        schedule: Some(cron_v1::Schedule {
            r#type: cron_v1::ScheduleType::Every as i32,
            spec: Some(cron_v1::schedule::Spec::Every(cron_v1::EverySchedule {
                interval_ms: 60_000,
            })),
        }),
        enabled: true,
        concurrency_policy: cron_v1::ConcurrencyPolicy::Forbid as i32,
        retry_policy: Some(cron_v1::RetryPolicy { max_attempts: 1, backoff_ms: 1 }),
        misfire_policy: cron_v1::MisfirePolicy::Skip as i32,
        jitter_ms: 0,
    });
    authorize_metadata(create_request.metadata_mut())?;
    let created = cron_client
        .create_job(create_request)
        .await
        .context("failed to create persistent cron job")?
        .into_inner();
    let created_job_id = created
        .job
        .as_ref()
        .and_then(|job| job.job_id.as_ref())
        .map(|id| id.ulid.clone())
        .context("CreateJob must return canonical job id")?;

    daemon.child_mut().kill().context("failed to stop daemon before restart")?;
    daemon.child_mut().wait().context("failed to wait for daemon shutdown")?;
    drop(daemon);

    let (child, restarted_admin_port, restarted_grpc_port) =
        spawn_palyrad_with_existing_journal(journal_db_path.clone())?;
    let mut restarted = ChildGuard::new(child);
    wait_for_health(restarted_admin_port, restarted.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{restarted_grpc_port}");
    let mut cron_client = cron_v1::cron_service_client::CronServiceClient::connect(endpoint)
        .await
        .context("failed to connect cron gRPC client after restart")?;

    let mut list_request = tonic::Request::new(cron_v1::ListJobsRequest {
        v: 1,
        after_job_ulid: String::new(),
        limit: 100,
        enabled: None,
        owner_principal: None,
        channel: None,
    });
    authorize_metadata(list_request.metadata_mut())?;
    let listed = cron_client
        .list_jobs(list_request)
        .await
        .context("failed to list cron jobs after restart")?
        .into_inner();
    assert!(
        listed.jobs.iter().any(|job| {
            job.job_id.as_ref().map(|id| id.ulid.as_str()) == Some(created_job_id.as_str())
        }),
        "cron job should survive daemon restart when journal database is reused"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_memory_ingest_search_list_and_purge_roundtrip() -> Result<()> {
    let (child, admin_port, grpc_port, _journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut memory_client =
        memory_v1::memory_service_client::MemoryServiceClient::connect(endpoint)
            .await
            .context("failed to connect memory gRPC client")?;

    let mut ingest_request = tonic::Request::new(memory_v1::IngestMemoryRequest {
        v: 1,
        source: memory_v1::MemorySource::Manual as i32,
        content_text: "release train rollback checklist".to_owned(),
        channel: "cli".to_owned(),
        session_id: Some(common_v1::CanonicalId { ulid: SESSION_ID.to_owned() }),
        tags: vec!["release".to_owned()],
        confidence: 0.9,
        ttl_unix_ms: 0,
    });
    authorize_metadata(ingest_request.metadata_mut())?;
    let ingested = memory_client
        .ingest_memory(ingest_request)
        .await
        .context("failed to call memory IngestMemory")?
        .into_inner();
    let memory_id = ingested
        .item
        .as_ref()
        .and_then(|item| item.memory_id.as_ref())
        .map(|value| value.ulid.clone())
        .context("memory ingest should return canonical memory id")?;

    let mut search_request = tonic::Request::new(memory_v1::SearchMemoryRequest {
        v: 1,
        query: "rollback checklist".to_owned(),
        channel: "cli".to_owned(),
        session_id: Some(common_v1::CanonicalId { ulid: SESSION_ID.to_owned() }),
        top_k: 5,
        min_score: 0.0,
        tags: Vec::new(),
        sources: Vec::new(),
        include_score_breakdown: true,
    });
    authorize_metadata(search_request.metadata_mut())?;
    let search = memory_client
        .search_memory(search_request)
        .await
        .context("failed to call memory SearchMemory")?
        .into_inner();
    assert!(
        search.hits.iter().any(|hit| {
            hit.item.as_ref().and_then(|item| item.memory_id.as_ref()).map(|id| id.ulid.as_str())
                == Some(memory_id.as_str())
        }),
        "memory search should return the ingested memory record"
    );

    let mut list_request = tonic::Request::new(memory_v1::ListMemoryItemsRequest {
        v: 1,
        after_memory_ulid: String::new(),
        limit: 50,
        channel: "cli".to_owned(),
        session_id: Some(common_v1::CanonicalId { ulid: SESSION_ID.to_owned() }),
        tags: Vec::new(),
        sources: Vec::new(),
    });
    authorize_metadata(list_request.metadata_mut())?;
    let listed = memory_client
        .list_memory_items(list_request)
        .await
        .context("failed to call memory ListMemoryItems")?
        .into_inner();
    assert!(
        listed.items.iter().any(|item| {
            item.memory_id.as_ref().map(|id| id.ulid.as_str()) == Some(memory_id.as_str())
        }),
        "list memory should include ingested record"
    );

    let mut purge_request = tonic::Request::new(memory_v1::PurgeMemoryRequest {
        v: 1,
        channel: "cli".to_owned(),
        session_id: Some(common_v1::CanonicalId { ulid: SESSION_ID.to_owned() }),
        purge_all_principal: false,
    });
    authorize_metadata(purge_request.metadata_mut())?;
    let purged = memory_client
        .purge_memory(purge_request)
        .await
        .context("failed to call memory PurgeMemory")?
        .into_inner();
    assert!(
        purged.deleted_count >= 1,
        "session purge should delete at least the memory created in this test"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_memory_scope_isolation_blocks_cross_principal_get() -> Result<()> {
    let (child, admin_port, grpc_port, _journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut memory_client =
        memory_v1::memory_service_client::MemoryServiceClient::connect(endpoint)
            .await
            .context("failed to connect memory gRPC client")?;

    let mut ingest_request = tonic::Request::new(memory_v1::IngestMemoryRequest {
        v: 1,
        source: memory_v1::MemorySource::Manual as i32,
        content_text: "owner-private memory item".to_owned(),
        channel: "cli".to_owned(),
        session_id: Some(common_v1::CanonicalId { ulid: SESSION_ID.to_owned() }),
        tags: Vec::new(),
        confidence: 0.8,
        ttl_unix_ms: 0,
    });
    authorize_metadata_with_principal(ingest_request.metadata_mut(), "user:ops")?;
    let ingested = memory_client
        .ingest_memory(ingest_request)
        .await
        .context("failed to ingest owner memory item")?
        .into_inner();
    let memory_id = ingested
        .item
        .as_ref()
        .and_then(|item| item.memory_id.as_ref())
        .map(|value| value.ulid.clone())
        .context("memory ingest should return canonical memory id")?;

    let mut denied_get_request = tonic::Request::new(memory_v1::GetMemoryItemRequest {
        v: 1,
        memory_id: Some(common_v1::CanonicalId { ulid: memory_id.clone() }),
    });
    authorize_metadata_with_principal(denied_get_request.metadata_mut(), "user:auditor")?;
    let denied = memory_client
        .get_memory_item(denied_get_request)
        .await
        .expect_err("cross-principal get should be denied");
    assert_eq!(denied.code(), Code::PermissionDenied);

    let mut search_request = tonic::Request::new(memory_v1::SearchMemoryRequest {
        v: 1,
        query: "owner-private".to_owned(),
        channel: "cli".to_owned(),
        session_id: Some(common_v1::CanonicalId { ulid: SESSION_ID.to_owned() }),
        top_k: 10,
        min_score: 0.0,
        tags: Vec::new(),
        sources: Vec::new(),
        include_score_breakdown: false,
    });
    authorize_metadata_with_principal(search_request.metadata_mut(), "user:auditor")?;
    let search = memory_client
        .search_memory(search_request)
        .await
        .context("cross-principal search request should succeed with scoped empty result")?
        .into_inner();
    assert!(
        search.hits.is_empty(),
        "cross-principal memory search must not return data owned by another principal"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_memory_purge_all_respects_authenticated_channel_scope() -> Result<()> {
    let (child, admin_port, grpc_port, _journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut memory_client =
        memory_v1::memory_service_client::MemoryServiceClient::connect(endpoint)
            .await
            .context("failed to connect memory gRPC client")?;

    let mut cli_ingest_request = tonic::Request::new(memory_v1::IngestMemoryRequest {
        v: 1,
        source: memory_v1::MemorySource::Manual as i32,
        content_text: "cli memory for scoped purge".to_owned(),
        channel: "cli".to_owned(),
        session_id: Some(common_v1::CanonicalId { ulid: SESSION_ID.to_owned() }),
        tags: Vec::new(),
        confidence: 0.8,
        ttl_unix_ms: 0,
    });
    authorize_metadata_with_principal_and_channel(
        cli_ingest_request.metadata_mut(),
        "user:ops",
        "cli",
    )?;
    let cli_memory_id = memory_client
        .ingest_memory(cli_ingest_request)
        .await
        .context("failed to ingest cli-scoped memory")?
        .into_inner()
        .item
        .and_then(|item| item.memory_id)
        .map(|id| id.ulid)
        .context("cli ingest should return memory id")?;

    let mut slack_ingest_request = tonic::Request::new(memory_v1::IngestMemoryRequest {
        v: 1,
        source: memory_v1::MemorySource::Manual as i32,
        content_text: "slack memory that must survive cli purge".to_owned(),
        channel: "slack".to_owned(),
        session_id: Some(common_v1::CanonicalId { ulid: SESSION_ID.to_owned() }),
        tags: Vec::new(),
        confidence: 0.8,
        ttl_unix_ms: 0,
    });
    authorize_metadata_with_principal_and_channel(
        slack_ingest_request.metadata_mut(),
        "user:ops",
        "slack",
    )?;
    let slack_memory_id = memory_client
        .ingest_memory(slack_ingest_request)
        .await
        .context("failed to ingest slack-scoped memory")?
        .into_inner()
        .item
        .and_then(|item| item.memory_id)
        .map(|id| id.ulid)
        .context("slack ingest should return memory id")?;

    let mut purge_request = tonic::Request::new(memory_v1::PurgeMemoryRequest {
        v: 1,
        channel: String::new(),
        session_id: None,
        purge_all_principal: true,
    });
    authorize_metadata_with_principal_and_channel(purge_request.metadata_mut(), "user:ops", "cli")?;
    let purge_response = memory_client
        .purge_memory(purge_request)
        .await
        .context("failed to purge memory with purge_all_principal=true")?
        .into_inner();
    assert_eq!(
        purge_response.deleted_count, 1,
        "channel-authenticated purge-all must only remove channel-scoped memories"
    );

    let mut deleted_cli_get = tonic::Request::new(memory_v1::GetMemoryItemRequest {
        v: 1,
        memory_id: Some(common_v1::CanonicalId { ulid: cli_memory_id }),
    });
    authorize_metadata_with_principal_and_channel(
        deleted_cli_get.metadata_mut(),
        "user:ops",
        "cli",
    )?;
    let deleted_cli_error = memory_client
        .get_memory_item(deleted_cli_get)
        .await
        .expect_err("purged cli memory should not be retrievable");
    assert_eq!(deleted_cli_error.code(), Code::NotFound);

    let mut surviving_slack_get = tonic::Request::new(memory_v1::GetMemoryItemRequest {
        v: 1,
        memory_id: Some(common_v1::CanonicalId { ulid: slack_memory_id }),
    });
    authorize_metadata_with_principal_and_channel(
        surviving_slack_get.metadata_mut(),
        "user:ops",
        "slack",
    )?;
    let surviving = memory_client
        .get_memory_item(surviving_slack_get)
        .await
        .context("slack memory should survive cli-scoped purge-all")?
        .into_inner();
    assert!(
        surviving.item.is_some(),
        "slack-scoped memory should remain accessible after cli purge-all"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_memory_delete_rejects_cross_channel_access() -> Result<()> {
    let (child, admin_port, grpc_port, _journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut memory_client =
        memory_v1::memory_service_client::MemoryServiceClient::connect(endpoint)
            .await
            .context("failed to connect memory gRPC client")?;

    let mut ingest_request = tonic::Request::new(memory_v1::IngestMemoryRequest {
        v: 1,
        source: memory_v1::MemorySource::Manual as i32,
        content_text: "slack memory that cli must not delete".to_owned(),
        channel: "slack".to_owned(),
        session_id: Some(common_v1::CanonicalId { ulid: SESSION_ID.to_owned() }),
        tags: Vec::new(),
        confidence: 0.8,
        ttl_unix_ms: 0,
    });
    authorize_metadata_with_principal_and_channel(
        ingest_request.metadata_mut(),
        "user:ops",
        "slack",
    )?;
    let memory_id = memory_client
        .ingest_memory(ingest_request)
        .await
        .context("failed to ingest slack memory")?
        .into_inner()
        .item
        .and_then(|item| item.memory_id)
        .map(|id| id.ulid)
        .context("ingest should return memory id")?;

    let mut denied_delete = tonic::Request::new(memory_v1::DeleteMemoryItemRequest {
        v: 1,
        memory_id: Some(common_v1::CanonicalId { ulid: memory_id.clone() }),
    });
    authorize_metadata_with_principal_and_channel(denied_delete.metadata_mut(), "user:ops", "cli")?;
    let denied_error = memory_client
        .delete_memory_item(denied_delete)
        .await
        .expect_err("cross-channel delete must be denied");
    assert_eq!(denied_error.code(), Code::PermissionDenied);

    let mut verify_get = tonic::Request::new(memory_v1::GetMemoryItemRequest {
        v: 1,
        memory_id: Some(common_v1::CanonicalId { ulid: memory_id.clone() }),
    });
    authorize_metadata_with_principal_and_channel(verify_get.metadata_mut(), "user:ops", "slack")?;
    let verify = memory_client
        .get_memory_item(verify_get)
        .await
        .context("slack memory should still exist after denied delete")?
        .into_inner();
    assert!(verify.item.is_some(), "denied delete must not remove the target memory item");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_memory_get_hides_ttl_expired_item() -> Result<()> {
    let (child, admin_port, grpc_port, _journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut memory_client =
        memory_v1::memory_service_client::MemoryServiceClient::connect(endpoint)
            .await
            .context("failed to connect memory gRPC client")?;

    let now_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time should be after unix epoch")?
        .as_millis() as i64;
    let mut ingest_request = tonic::Request::new(memory_v1::IngestMemoryRequest {
        v: 1,
        source: memory_v1::MemorySource::Manual as i32,
        content_text: "expiring memory item".to_owned(),
        channel: "cli".to_owned(),
        session_id: Some(common_v1::CanonicalId { ulid: SESSION_ID.to_owned() }),
        tags: Vec::new(),
        confidence: 0.8,
        ttl_unix_ms: now_unix_ms.saturating_add(120),
    });
    authorize_metadata(ingest_request.metadata_mut())?;
    let memory_id = memory_client
        .ingest_memory(ingest_request)
        .await
        .context("failed to ingest expiring memory item")?
        .into_inner()
        .item
        .and_then(|item| item.memory_id)
        .map(|id| id.ulid)
        .context("expiring ingest should return memory id")?;

    tokio::time::sleep(Duration::from_millis(220)).await;

    let mut get_request = tonic::Request::new(memory_v1::GetMemoryItemRequest {
        v: 1,
        memory_id: Some(common_v1::CanonicalId { ulid: memory_id }),
    });
    authorize_metadata(get_request.metadata_mut())?;
    let error = memory_client
        .get_memory_item(get_request)
        .await
        .expect_err("expired memory item should not be visible through get path");
    assert_eq!(error.code(), Code::NotFound);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_abort_run_requests_cancellation() -> Result<()> {
    let (child, admin_port, grpc_port, _journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
        .await
        .context("failed to connect gRPC client")?;

    let mut stream_request =
        tonic::Request::new(tokio_stream::iter(vec![sample_run_stream_request_with_text(
            "abort me later".to_owned(),
        )]));
    authorize_metadata(stream_request.metadata_mut())?;
    let mut response_stream =
        client.run_stream(stream_request).await.context("failed to call RunStream")?.into_inner();
    while let Some(event) = response_stream.next().await {
        let _ = event.context("failed to read RunStream event")?;
    }

    let mut abort_request = tonic::Request::new(gateway_v1::AbortRunRequest {
        v: 1,
        run_id: Some(common_v1::CanonicalId { ulid: RUN_ID.to_owned() }),
        reason: "grpc_abort_requested".to_owned(),
    });
    authorize_metadata(abort_request.metadata_mut())?;
    let aborted =
        client.abort_run(abort_request).await.context("failed to call AbortRun")?.into_inner();
    assert!(aborted.cancel_requested, "abort RPC should mark run as cancel requested");
    assert_eq!(aborted.reason, "grpc_abort_requested");

    let run_snapshot = admin_get_json_async(admin_port, format!("/admin/v1/runs/{RUN_ID}")).await?;
    assert_eq!(
        run_snapshot.get("cancel_requested").and_then(Value::as_bool),
        Some(true),
        "run snapshot should expose cancel_requested after AbortRun"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_gateway_run_stream_emits_at_most_sixteen_model_tokens() -> Result<()> {
    let (child, admin_port, grpc_port, _journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
        .await
        .context("failed to connect gRPC client")?;

    let long_input = (0..64).map(|index| format!("token{index}")).collect::<Vec<_>>().join(" ");
    let mut stream_request =
        tonic::Request::new(tokio_stream::iter(vec![sample_run_stream_request_with_text(
            long_input,
        )]));
    stream_request.metadata_mut().insert("authorization", format!("Bearer {ADMIN_TOKEN}").parse()?);
    stream_request.metadata_mut().insert("x-palyra-principal", "user:ops".parse()?);
    stream_request.metadata_mut().insert("x-palyra-device-id", DEVICE_ID.parse()?);
    stream_request.metadata_mut().insert("x-palyra-channel", "cli".parse()?);

    let mut response_stream =
        client.run_stream(stream_request).await.context("failed to call RunStream")?.into_inner();

    let mut model_tokens = Vec::new();
    while let Some(event) = response_stream.next().await {
        let event = event.context("failed to read RunStream event")?;
        if let Some(common_v1::run_stream_event::Body::ModelToken(token)) = event.body {
            model_tokens.push(token);
        }
    }

    assert_eq!(model_tokens.len(), 16, "run stream should emit at most 16 model tokens");
    assert!(
        model_tokens.last().map(|token| token.is_final).unwrap_or(false),
        "last emitted token should be final"
    );
    assert!(
        model_tokens.iter().take(15).all(|token| !token.is_final),
        "only the last emitted token should be marked final"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_run_stream_uses_openai_compatible_provider_when_configured() -> Result<()> {
    let (openai_base_url, request_count, server_handle) =
        spawn_scripted_openai_server(vec![ScriptedOpenAiResponse::immediate(
            200,
            r#"{"choices":[{"message":{"content":"provider says hello"}}]}"#.to_owned(),
        )])?;
    let (child, admin_port, grpc_port, _journal_db_path) =
        spawn_palyrad_with_openai_provider(openai_base_url.as_str(), OPENAI_API_KEY)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
        .await
        .context("failed to connect gRPC client")?;

    let mut stream_request =
        tonic::Request::new(tokio_stream::iter(vec![sample_run_stream_request_with_text(
            "ignored by deterministic fallback".to_owned(),
        )]));
    stream_request.metadata_mut().insert("authorization", format!("Bearer {ADMIN_TOKEN}").parse()?);
    stream_request.metadata_mut().insert("x-palyra-principal", "user:ops".parse()?);
    stream_request.metadata_mut().insert("x-palyra-device-id", DEVICE_ID.parse()?);
    stream_request.metadata_mut().insert("x-palyra-channel", "cli".parse()?);

    let mut response_stream =
        client.run_stream(stream_request).await.context("failed to call RunStream")?.into_inner();
    let mut model_tokens = Vec::new();
    while let Some(event) = response_stream.next().await {
        let event = event.context("failed to read RunStream event")?;
        if let Some(common_v1::run_stream_event::Body::ModelToken(token)) = event.body {
            model_tokens.push(token.token);
        }
    }
    assert_eq!(model_tokens, vec!["provider", "says", "hello"]);
    assert_eq!(
        request_count.load(Ordering::Relaxed),
        1,
        "openai-compatible provider should perform one upstream call"
    );

    let status_snapshot = admin_get_json_async(admin_port, "/admin/v1/status".to_owned()).await?;
    assert_eq!(
        status_snapshot.pointer("/model_provider/kind").and_then(Value::as_str),
        Some("openai_compatible")
    );
    assert_eq!(
        status_snapshot.pointer("/model_provider/api_key_configured").and_then(Value::as_bool),
        Some(true)
    );
    let serialized_status =
        serde_json::to_string(&status_snapshot).context("failed to serialize status snapshot")?;
    assert!(
        !serialized_status.contains(OPENAI_API_KEY),
        "admin status snapshot must not leak provider API key"
    );

    server_handle.join().expect("scripted openai server thread should exit");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_run_stream_executes_allowlisted_tool_and_emits_attestation() -> Result<()> {
    let (openai_base_url, _request_count, server_handle) =
        spawn_scripted_openai_server(vec![ScriptedOpenAiResponse::immediate(
            200,
            r#"{"choices":[{"message":{"tool_calls":[{"function":{"name":"palyra.echo","arguments":"{\"text\":\"hello tool\"}"}}]}}]}"#
                .to_owned(),
        )])?;
    let (child, admin_port, grpc_port, _journal_db_path) =
        spawn_palyrad_with_openai_provider_and_tool_policy(
            openai_base_url.as_str(),
            OPENAI_API_KEY,
            "palyra.echo",
            2,
            250,
        )?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
        .await
        .context("failed to connect gRPC client")?;

    let mut stream_request =
        tonic::Request::new(tokio_stream::iter(vec![sample_run_stream_request_with_text(
            "trigger tool call".to_owned(),
        )]));
    stream_request.metadata_mut().insert("authorization", format!("Bearer {ADMIN_TOKEN}").parse()?);
    stream_request.metadata_mut().insert("x-palyra-principal", "user:ops".parse()?);
    stream_request.metadata_mut().insert("x-palyra-device-id", DEVICE_ID.parse()?);
    stream_request.metadata_mut().insert("x-palyra-channel", "cli".parse()?);

    let mut response_stream =
        client.run_stream(stream_request).await.context("failed to call RunStream")?.into_inner();

    let mut saw_allow_decision = false;
    let mut saw_success_result = false;
    let mut saw_attestation = false;
    while let Some(event) = response_stream.next().await {
        let event = event.context("failed to read RunStream event")?;
        if let Some(body) = event.body {
            match body {
                common_v1::run_stream_event::Body::ToolDecision(decision) => {
                    if decision.kind == common_v1::tool_decision::DecisionKind::Allow as i32 {
                        saw_allow_decision = true;
                    }
                }
                common_v1::run_stream_event::Body::ToolResult(result) => {
                    if result.success {
                        let output = serde_json::from_slice::<Value>(&result.output_json)
                            .context("tool result output_json should be valid JSON")?;
                        assert_eq!(output, serde_json::json!({ "echo": "hello tool" }));
                        saw_success_result = true;
                    }
                }
                common_v1::run_stream_event::Body::ToolAttestation(attestation) => {
                    assert!(!attestation.timed_out, "echo tool should not time out");
                    assert_eq!(attestation.executor, "builtin");
                    assert_eq!(
                        attestation.execution_sha256.len(),
                        64,
                        "execution attestation hash should be sha256 hex"
                    );
                    saw_attestation = true;
                }
                _ => {}
            }
        }
    }

    assert!(saw_allow_decision, "allowlisted tool call should produce an allow decision");
    assert!(saw_success_result, "allowlisted tool call should execute successfully");
    assert!(saw_attestation, "allowlisted tool call should emit an attestation");

    let run_snapshot = admin_get_json_async(admin_port, format!("/admin/v1/runs/{RUN_ID}")).await?;
    assert_eq!(
        run_snapshot.get("state").and_then(Value::as_str).context("run snapshot missing state")?,
        "done"
    );

    server_handle.join().expect("scripted openai server thread should exit");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_run_stream_executes_memory_search_tool_and_emits_memory_attestation() -> Result<()> {
    let response_body = openai_tool_call_response(
        "palyra.memory.search",
        &serde_json::json!({
            "query": "rollback checklist",
            "scope": "session",
            "top_k": 5,
            "min_score": 0.0
        }),
    )?;
    let (openai_base_url, _request_count, server_handle) =
        spawn_scripted_openai_server(vec![ScriptedOpenAiResponse::immediate(200, response_body)])?;
    let (child, admin_port, grpc_port, _journal_db_path) =
        spawn_palyrad_with_openai_provider_and_tool_policy(
            openai_base_url.as_str(),
            OPENAI_API_KEY,
            "palyra.memory.search",
            2,
            250,
        )?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut memory_client =
        memory_v1::memory_service_client::MemoryServiceClient::connect(endpoint.clone())
            .await
            .context("failed to connect memory gRPC client")?;
    let mut ingest_request = tonic::Request::new(memory_v1::IngestMemoryRequest {
        v: 1,
        source: memory_v1::MemorySource::Manual as i32,
        content_text: "rollback checklist for release train".to_owned(),
        channel: "cli".to_owned(),
        session_id: Some(common_v1::CanonicalId { ulid: SESSION_ID.to_owned() }),
        tags: vec!["release".to_owned()],
        confidence: 0.9,
        ttl_unix_ms: 0,
    });
    authorize_metadata(ingest_request.metadata_mut())?;
    let ingested = memory_client
        .ingest_memory(ingest_request)
        .await
        .context("failed to ingest memory item for tool search test")?
        .into_inner();
    let ingested_memory_id = ingested
        .item
        .as_ref()
        .and_then(|item| item.memory_id.as_ref())
        .map(|id| id.ulid.clone())
        .context("ingested memory should include canonical id")?;

    let mut gateway_client =
        gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
            .await
            .context("failed to connect gateway gRPC client")?;
    let mut stream_request =
        tonic::Request::new(tokio_stream::iter(vec![sample_run_stream_request_with_text(
            "trigger memory search tool".to_owned(),
        )]));
    authorize_metadata(stream_request.metadata_mut())?;
    let mut response_stream = gateway_client
        .run_stream(stream_request)
        .await
        .context("failed to call RunStream")?
        .into_inner();

    let mut saw_allow_decision = false;
    let mut saw_memory_result = false;
    let mut saw_memory_attestation = false;
    while let Some(event) = response_stream.next().await {
        let event = event.context("failed to read RunStream event")?;
        if let Some(body) = event.body {
            match body {
                common_v1::run_stream_event::Body::ToolDecision(decision) => {
                    if decision.kind == common_v1::tool_decision::DecisionKind::Allow as i32 {
                        saw_allow_decision = true;
                    }
                }
                common_v1::run_stream_event::Body::ToolResult(result) => {
                    if result.success {
                        let output = serde_json::from_slice::<Value>(&result.output_json)
                            .context("memory tool output_json should be valid JSON")?;
                        let hits = output
                            .get("hits")
                            .and_then(Value::as_array)
                            .context("memory tool output must contain hits array")?;
                        let contains_ingested = hits.iter().any(|hit| {
                            hit.get("memory_id").and_then(Value::as_str)
                                == Some(ingested_memory_id.as_str())
                        });
                        assert!(
                            contains_ingested,
                            "memory search tool output should include the ingested memory item"
                        );
                        saw_memory_result = true;
                    }
                }
                common_v1::run_stream_event::Body::ToolAttestation(attestation) => {
                    if attestation.executor == "memory_runtime" {
                        saw_memory_attestation = true;
                    }
                }
                _ => {}
            }
        }
    }

    assert!(saw_allow_decision, "memory search tool should produce allow decision");
    assert!(saw_memory_result, "memory search tool should produce successful tool result");
    assert!(saw_memory_attestation, "memory search tool should emit memory_runtime attestation");

    server_handle.join().expect("scripted openai server thread should exit");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_run_stream_memory_search_principal_scope_stays_channel_bounded() -> Result<()> {
    let response_body = openai_tool_call_response(
        "palyra.memory.search",
        &serde_json::json!({
            "query": "cross-channel marker",
            "scope": "principal",
            "top_k": 10,
            "min_score": 0.0
        }),
    )?;
    let (openai_base_url, _request_count, server_handle) =
        spawn_scripted_openai_server(vec![ScriptedOpenAiResponse::immediate(200, response_body)])?;
    let (child, admin_port, grpc_port, _journal_db_path) =
        spawn_palyrad_with_openai_provider_and_tool_policy(
            openai_base_url.as_str(),
            OPENAI_API_KEY,
            "palyra.memory.search",
            2,
            250,
        )?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut memory_client =
        memory_v1::memory_service_client::MemoryServiceClient::connect(endpoint.clone())
            .await
            .context("failed to connect memory gRPC client")?;

    let mut cli_ingest = tonic::Request::new(memory_v1::IngestMemoryRequest {
        v: 1,
        source: memory_v1::MemorySource::Manual as i32,
        content_text: "cross-channel marker cli".to_owned(),
        channel: "cli".to_owned(),
        session_id: Some(common_v1::CanonicalId { ulid: SESSION_ID.to_owned() }),
        tags: Vec::new(),
        confidence: 0.9,
        ttl_unix_ms: 0,
    });
    authorize_metadata_with_principal_and_channel(cli_ingest.metadata_mut(), "user:ops", "cli")?;
    let cli_memory_id = memory_client
        .ingest_memory(cli_ingest)
        .await
        .context("failed to ingest cli memory for principal-scope test")?
        .into_inner()
        .item
        .and_then(|item| item.memory_id)
        .map(|id| id.ulid)
        .context("cli ingest should return memory id")?;

    let mut slack_ingest = tonic::Request::new(memory_v1::IngestMemoryRequest {
        v: 1,
        source: memory_v1::MemorySource::Manual as i32,
        content_text: "cross-channel marker slack".to_owned(),
        channel: "slack".to_owned(),
        session_id: Some(common_v1::CanonicalId { ulid: SESSION_ID.to_owned() }),
        tags: Vec::new(),
        confidence: 0.9,
        ttl_unix_ms: 0,
    });
    authorize_metadata_with_principal_and_channel(
        slack_ingest.metadata_mut(),
        "user:ops",
        "slack",
    )?;
    let slack_memory_id = memory_client
        .ingest_memory(slack_ingest)
        .await
        .context("failed to ingest slack memory for principal-scope test")?
        .into_inner()
        .item
        .and_then(|item| item.memory_id)
        .map(|id| id.ulid)
        .context("slack ingest should return memory id")?;

    let mut gateway_client =
        gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
            .await
            .context("failed to connect gateway gRPC client")?;
    let mut stream_request =
        tonic::Request::new(tokio_stream::iter(vec![sample_run_stream_request_with_text(
            "trigger principal scope memory search".to_owned(),
        )]));
    authorize_metadata_with_principal_and_channel(
        stream_request.metadata_mut(),
        "user:ops",
        "cli",
    )?;
    let mut response_stream = gateway_client
        .run_stream(stream_request)
        .await
        .context("failed to call RunStream")?
        .into_inner();

    let mut saw_memory_result = false;
    while let Some(event) = response_stream.next().await {
        let event = event.context("failed to read RunStream event")?;
        if let Some(common_v1::run_stream_event::Body::ToolResult(result)) = event.body {
            if result.success {
                let output = serde_json::from_slice::<Value>(&result.output_json)
                    .context("memory tool output_json should be valid JSON")?;
                let hits = output
                    .get("hits")
                    .and_then(Value::as_array)
                    .context("memory tool output must contain hits array")?;
                let returned_ids = hits
                    .iter()
                    .filter_map(|hit| hit.get("memory_id").and_then(Value::as_str))
                    .collect::<Vec<_>>();
                assert!(
                    returned_ids.contains(&cli_memory_id.as_str()),
                    "principal-scope tool search from cli channel should include cli memory"
                );
                assert!(
                    returned_ids.iter().all(|memory_id| *memory_id != slack_memory_id.as_str()),
                    "principal-scope tool search from cli channel must not return slack memory"
                );
                saw_memory_result = true;
            }
        }
    }

    assert!(
        saw_memory_result,
        "principal-scope memory tool search should produce a successful tool result"
    );
    server_handle.join().expect("scripted openai server thread should exit");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_run_stream_records_memory_auto_inject_tape_event() -> Result<()> {
    let (openai_base_url, _request_count, server_handle) =
        spawn_scripted_openai_server(vec![ScriptedOpenAiResponse::immediate(
            200,
            r#"{"choices":[{"message":{"content":"model response"}}]}"#.to_owned(),
        )])?;
    let (child, admin_port, grpc_port, _journal_db_path) =
        spawn_palyrad_with_openai_provider_tool_policy_and_memory_auto_inject(
            openai_base_url.as_str(),
            OPENAI_API_KEY,
            "",
            2,
            250,
            3,
        )?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut gateway_client =
        gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
            .await
            .context("failed to connect gateway gRPC client")?;
    let mut stream_request =
        tonic::Request::new(tokio_stream::iter(vec![sample_run_stream_request_with_text(
            "please recall the release rollback checklist".to_owned(),
        )]));
    authorize_metadata(stream_request.metadata_mut())?;
    let mut response_stream = gateway_client
        .run_stream(stream_request)
        .await
        .context("failed to call RunStream")?
        .into_inner();
    while let Some(event) = response_stream.next().await {
        let _event = event.context("failed to read RunStream event")?;
    }

    let tape_snapshot =
        admin_get_json_async(admin_port, format!("/admin/v1/runs/{RUN_ID}/tape")).await?;
    let events = tape_snapshot
        .get("events")
        .and_then(Value::as_array)
        .context("run tape snapshot missing events")?;
    let memory_auto_inject_event = events
        .iter()
        .find(|event| event.get("event_type").and_then(Value::as_str) == Some("memory_auto_inject"))
        .context("run tape must contain memory_auto_inject event when auto-inject is enabled")?;
    let payload_json = memory_auto_inject_event
        .get("payload_json")
        .and_then(Value::as_str)
        .context("memory_auto_inject event missing payload_json")?;
    let payload: Value = serde_json::from_str(payload_json)
        .context("memory_auto_inject payload_json must be valid JSON")?;
    let injected_count = payload.get("injected_count").and_then(Value::as_u64).unwrap_or_default();
    assert!(injected_count >= 1, "memory auto-inject should include at least one matching item");
    assert_eq!(
        payload.get("query").and_then(Value::as_str),
        Some("please recall the release rollback checklist"),
        "memory auto-inject payload should keep the search query for auditability"
    );
    let hits = payload
        .get("hits")
        .and_then(Value::as_array)
        .context("memory_auto_inject payload must include hits array")?;
    let contains_user_memory_hit = hits
        .iter()
        .any(|hit| hit.get("source").and_then(Value::as_str) == Some("tape:user_message"));
    assert!(
        contains_user_memory_hit,
        "memory_auto_inject should be able to reuse scoped user-message memories"
    );

    let status_snapshot = admin_get_json_async(admin_port, "/admin/v1/status".to_owned()).await?;
    assert_eq!(
        status_snapshot.pointer("/counters/memory_auto_inject_events").and_then(Value::as_u64),
        Some(1),
        "runtime counters should report one memory auto-inject event"
    );
    let search_requests = status_snapshot
        .pointer("/counters/memory_search_requests")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    assert!(
        search_requests >= 1,
        "memory search requests counter should increase when auto-inject runs"
    );

    server_handle.join().expect("scripted openai server thread should exit");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_run_stream_denies_non_allowlisted_tool_by_default() -> Result<()> {
    let (openai_base_url, _request_count, server_handle) =
        spawn_scripted_openai_server(vec![ScriptedOpenAiResponse::immediate(
            200,
            r#"{"choices":[{"message":{"tool_calls":[{"function":{"name":"custom.noop","arguments":"{\"payload\":\"x\"}"}}]}}]}"#
                .to_owned(),
        )])?;
    let (child, admin_port, grpc_port, journal_db_path) =
        spawn_palyrad_with_openai_provider_and_tool_policy(
            openai_base_url.as_str(),
            OPENAI_API_KEY,
            "palyra.echo",
            2,
            250,
        )?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
        .await
        .context("failed to connect gRPC client")?;

    let mut stream_request =
        tonic::Request::new(tokio_stream::iter(vec![sample_run_stream_request_with_text(
            "tool denial path".to_owned(),
        )]));
    stream_request.metadata_mut().insert("authorization", format!("Bearer {ADMIN_TOKEN}").parse()?);
    stream_request.metadata_mut().insert("x-palyra-principal", "user:ops".parse()?);
    stream_request.metadata_mut().insert("x-palyra-device-id", DEVICE_ID.parse()?);
    stream_request.metadata_mut().insert("x-palyra-channel", "cli".parse()?);

    let mut response_stream =
        client.run_stream(stream_request).await.context("failed to call RunStream")?.into_inner();

    let mut saw_deny_decision = false;
    let mut saw_failed_result = false;
    let mut saw_policy_attestation = false;
    while let Some(event) = response_stream.next().await {
        let event = event.context("failed to read RunStream event")?;
        if let Some(body) = event.body {
            match body {
                common_v1::run_stream_event::Body::ToolDecision(decision) => {
                    if decision.kind == common_v1::tool_decision::DecisionKind::Deny as i32 {
                        assert!(
                            decision.reason.contains("denied by default"),
                            "deny decision should include policy reason"
                        );
                        saw_deny_decision = true;
                    }
                }
                common_v1::run_stream_event::Body::ToolResult(result) => {
                    if !result.success {
                        assert!(
                            result.error.contains("denied by default"),
                            "denied result should carry policy explanation"
                        );
                        saw_failed_result = true;
                    }
                }
                common_v1::run_stream_event::Body::ToolAttestation(attestation) => {
                    if attestation.executor == "policy" {
                        saw_policy_attestation = true;
                    }
                }
                _ => {}
            }
        }
    }
    assert!(saw_deny_decision, "non-allowlisted tool should be denied");
    assert!(saw_failed_result, "denied tool should emit failed tool result");
    assert!(saw_policy_attestation, "denied tool should emit policy attestation");

    let status_snapshot = admin_get_json_async(admin_port, "/admin/v1/status".to_owned()).await?;
    assert_eq!(
        status_snapshot.pointer("/counters/tool_decisions_denied").and_then(Value::as_u64),
        Some(1)
    );
    assert_eq!(
        status_snapshot.pointer("/counters/tool_execution_attempts").and_then(Value::as_u64),
        Some(0)
    );
    let connection =
        Connection::open(journal_db_path).context("failed to open journal sqlite db")?;
    let mut statement = connection
        .prepare(
            r#"
                SELECT kind, payload_json
                FROM journal_events
                ORDER BY seq ASC
            "#,
        )
        .context("failed to prepare journal decision query")?;
    let mut rows = statement.query([]).context("failed to query journal decision rows")?;
    let mut saw_policy_decision_event = false;
    let mut saw_policy_decision_kind = false;
    let mut saw_denied_policy_payload = false;
    while let Some(row) = rows.next().context("failed to iterate journal decision rows")? {
        let kind: i32 = row.get(0).context("journal kind should be readable")?;
        let payload_json: String = row.get(1).context("journal payload_json should be readable")?;
        let payload: Value = serde_json::from_str(payload_json.as_str())
            .context("journal payload_json must be valid json")?;
        if payload.get("event").and_then(Value::as_str) == Some("policy_decision") {
            saw_policy_decision_event = true;
            if kind == common_v1::journal_event::EventKind::ToolProposed as i32 {
                saw_policy_decision_kind = true;
            }
            if payload.get("kind").and_then(Value::as_str) == Some("deny")
                && payload
                    .get("reason")
                    .and_then(Value::as_str)
                    .map(|reason| reason.contains("denied by default"))
                    .unwrap_or(false)
            {
                saw_denied_policy_payload = true;
            }
        }
    }
    assert!(saw_policy_decision_event, "policy decisions must be persisted in journal entries");
    assert!(
        saw_policy_decision_kind,
        "policy decision journal entries must use EVENT_KIND_TOOL_PROPOSED"
    );
    assert!(
        saw_denied_policy_payload,
        "denied policy decision should be persisted with explainable denial reason"
    );

    server_handle.join().expect("scripted openai server thread should exit");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_run_stream_denies_allowlisted_unsupported_tool() -> Result<()> {
    let (openai_base_url, _request_count, server_handle) =
        spawn_scripted_openai_server(vec![ScriptedOpenAiResponse::immediate(
            200,
            r#"{"choices":[{"message":{"tool_calls":[{"function":{"name":"custom.noop","arguments":"{\"payload\":\"x\"}"}}]}}]}"#
                .to_owned(),
        )])?;
    let (child, admin_port, grpc_port, _journal_db_path) =
        spawn_palyrad_with_openai_provider_and_tool_policy(
            openai_base_url.as_str(),
            OPENAI_API_KEY,
            "custom.noop",
            2,
            250,
        )?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
        .await
        .context("failed to connect gRPC client")?;

    let (request_sender, request_receiver) = tokio_mpsc::channel(4);
    request_sender
        .send(sample_run_stream_request_with_text("unsupported tool path".to_owned()))
        .await
        .context("failed to send initial unsupported tool request")?;
    let mut stream_request = tonic::Request::new(ReceiverStream::new(request_receiver));
    stream_request.metadata_mut().insert("authorization", format!("Bearer {ADMIN_TOKEN}").parse()?);
    stream_request.metadata_mut().insert("x-palyra-principal", "user:ops".parse()?);
    stream_request.metadata_mut().insert("x-palyra-device-id", DEVICE_ID.parse()?);
    stream_request.metadata_mut().insert("x-palyra-channel", "cli".parse()?);

    let mut response_stream =
        client.run_stream(stream_request).await.context("failed to call RunStream")?.into_inner();

    let mut saw_approval_request = false;
    let mut saw_deny_decision = false;
    let mut saw_failed_result = false;
    loop {
        let next_event = tokio::time::timeout(Duration::from_secs(5), response_stream.next())
            .await
            .context("unsupported tool stream stalled before expected events")?;
        let Some(event) = next_event else {
            break;
        };
        let event = event.context("failed to read RunStream event")?;
        if let Some(body) = event.body {
            match body {
                common_v1::run_stream_event::Body::ToolApprovalRequest(approval_request) => {
                    let proposal_id = approval_request
                        .proposal_id
                        .as_ref()
                        .map(|proposal_id| proposal_id.ulid.as_str())
                        .context("tool approval request missing proposal_id")?;
                    request_sender
                        .send(sample_tool_approval_response_request(
                            proposal_id,
                            true,
                            "allow_once",
                        ))
                        .await
                        .context("failed to send unsupported-tool approval response")?;
                    saw_approval_request = true;
                }
                common_v1::run_stream_event::Body::ToolDecision(decision) => {
                    if decision.kind == common_v1::tool_decision::DecisionKind::Deny as i32 {
                        assert!(
                            decision.reason.contains("unsupported by runtime executor"),
                            "deny decision should describe unsupported runtime tool"
                        );
                        saw_deny_decision = true;
                    }
                }
                common_v1::run_stream_event::Body::ToolResult(result) => {
                    if !result.success {
                        assert!(
                            result.error.contains("unsupported by runtime executor"),
                            "denied result should carry unsupported runtime reason"
                        );
                        saw_failed_result = true;
                    }
                }
                _ => {}
            }
        }
        if saw_approval_request && saw_deny_decision && saw_failed_result {
            break;
        }
    }
    assert!(saw_approval_request, "unsupported tool proposal should request explicit approval");
    assert!(saw_deny_decision, "unsupported tool should be denied before execution");
    assert!(saw_failed_result, "denied tool should emit failed tool result");

    let status_snapshot = admin_get_json_async(admin_port, "/admin/v1/status".to_owned()).await?;
    assert_eq!(
        status_snapshot.pointer("/counters/tool_decisions_denied").and_then(Value::as_u64),
        Some(1)
    );
    assert_eq!(
        status_snapshot.pointer("/counters/tool_execution_attempts").and_then(Value::as_u64),
        Some(0)
    );

    server_handle.join().expect("scripted openai server thread should exit");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_approvals_service_persists_and_exports_denied_tool_approval() -> Result<()> {
    let (openai_base_url, _request_count, server_handle) =
        spawn_scripted_openai_server(vec![ScriptedOpenAiResponse::immediate(
            200,
            r#"{"choices":[{"message":{"tool_calls":[{"function":{"name":"custom.noop","arguments":"{\"payload\":\"secret-token\",\"cookie\":\"sessionid=abc123\"}"}}]}}]}"#
                .to_owned(),
        )])?;
    let (child, admin_port, grpc_port, _journal_db_path) =
        spawn_palyrad_with_openai_provider_and_tool_policy(
            openai_base_url.as_str(),
            OPENAI_API_KEY,
            "custom.noop",
            2,
            250,
        )?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut gateway_client =
        gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint.clone())
            .await
            .context("failed to connect gateway gRPC client")?;
    let mut approvals_client =
        gateway_v1::approvals_service_client::ApprovalsServiceClient::connect(endpoint)
            .await
            .context("failed to connect approvals gRPC client")?;

    let (request_sender, request_receiver) = tokio_mpsc::channel(4);
    request_sender
        .send(sample_run_stream_request_with_text("approval audit deny path".to_owned()))
        .await
        .context("failed to send initial deny-path stream request")?;
    let mut stream_request = tonic::Request::new(ReceiverStream::new(request_receiver));
    authorize_metadata(stream_request.metadata_mut())?;
    let mut response_stream = gateway_client
        .run_stream(stream_request)
        .await
        .context("failed to call RunStream")?
        .into_inner();

    let mut captured_approval_id: Option<String> = None;
    let mut saw_deny_decision = false;
    loop {
        let next_event = tokio::time::timeout(Duration::from_secs(5), response_stream.next())
            .await
            .context("deny-path stream stalled before expected events")?;
        let Some(event) = next_event else {
            break;
        };
        let event = event.context("failed to read deny-path RunStream event")?;
        if let Some(body) = event.body {
            match body {
                common_v1::run_stream_event::Body::ToolApprovalRequest(approval_request) => {
                    let proposal_id = approval_request
                        .proposal_id
                        .as_ref()
                        .map(|proposal_id| proposal_id.ulid.as_str())
                        .context("tool approval request missing proposal_id")?;
                    captured_approval_id =
                        approval_request.approval_id.as_ref().map(|value| value.ulid.clone());
                    request_sender
                        .send(sample_tool_approval_response_request(
                            proposal_id,
                            false,
                            "deny token=abc cookie:sessionid=abc123",
                        ))
                        .await
                        .context("failed to send deny approval response")?;
                }
                common_v1::run_stream_event::Body::ToolDecision(decision) => {
                    if decision.kind == common_v1::tool_decision::DecisionKind::Deny as i32 {
                        saw_deny_decision = true;
                        break;
                    }
                }
                _ => {}
            }
        }
    }
    assert!(saw_deny_decision, "tool proposal should be denied after explicit approval rejection");
    let approval_id = captured_approval_id.context("approval stream should include approval_id")?;

    let mut list_request = tonic::Request::new(gateway_v1::ListApprovalsRequest {
        v: 1,
        after_approval_ulid: String::new(),
        limit: 20,
        since_unix_ms: 0,
        until_unix_ms: 0,
        subject_id: "tool:custom.noop".to_owned(),
        principal: "user:ops".to_owned(),
        decision: gateway_v1::ApprovalDecision::Deny as i32,
        subject_type: gateway_v1::ApprovalSubjectType::Tool as i32,
    });
    authorize_metadata(list_request.metadata_mut())?;
    let list_response = approvals_client
        .list_approvals(list_request)
        .await
        .context("failed to call ListApprovals")?
        .into_inner();
    assert!(!list_response.approvals.is_empty(), "list approvals should return denied records");
    let listed = list_response
        .approvals
        .iter()
        .find(|record| {
            record
                .approval_id
                .as_ref()
                .map(|value| value.ulid.as_str() == approval_id.as_str())
                .unwrap_or(false)
        })
        .context("list approvals should include the stream approval_id")?;
    assert_eq!(listed.subject_type, gateway_v1::ApprovalSubjectType::Tool as i32);
    assert_eq!(listed.decision, gateway_v1::ApprovalDecision::Deny as i32);
    assert!(
        !listed.request_summary.contains("token=abc"),
        "stored request summary must redact token-like values"
    );
    assert!(
        !listed.request_summary.contains("sessionid=abc123"),
        "stored request summary must redact cookie-like values"
    );

    let mut get_request = tonic::Request::new(gateway_v1::GetApprovalRequest {
        v: 1,
        approval_id: Some(common_v1::CanonicalId { ulid: approval_id.clone() }),
    });
    authorize_metadata(get_request.metadata_mut())?;
    let get_response = approvals_client
        .get_approval(get_request)
        .await
        .context("failed to call GetApproval")?
        .into_inner();
    let fetched = get_response.approval.context("GetApproval must return approval payload")?;
    assert_eq!(
        fetched.approval_id.as_ref().map(|value| value.ulid.as_str()).unwrap_or_default(),
        approval_id.as_str()
    );

    let mut export_request = tonic::Request::new(gateway_v1::ExportApprovalsRequest {
        v: 1,
        format: gateway_v1::ApprovalExportFormat::Ndjson as i32,
        limit: 50,
        since_unix_ms: 0,
        until_unix_ms: 0,
        subject_id: "tool:custom.noop".to_owned(),
        principal: "user:ops".to_owned(),
        decision: gateway_v1::ApprovalDecision::Deny as i32,
        subject_type: gateway_v1::ApprovalSubjectType::Tool as i32,
    });
    authorize_metadata(export_request.metadata_mut())?;
    let mut export_stream = approvals_client
        .export_approvals(export_request)
        .await
        .context("failed to call ExportApprovals")?
        .into_inner();
    let mut exported = Vec::new();
    while let Some(chunk) = export_stream.next().await {
        let chunk = chunk.context("failed to read ExportApprovals chunk")?;
        if !chunk.chunk.is_empty() {
            exported.extend_from_slice(chunk.chunk.as_slice());
        }
        if chunk.done {
            break;
        }
    }
    let exported_text =
        String::from_utf8(exported).context("exported approvals payload must be UTF-8 NDJSON")?;
    assert!(
        exported_text.contains(approval_id.as_str()),
        "export output must contain persisted approval_id"
    );
    let mut ndjson_record_count = 0_usize;
    let mut ndjson_previous_chain =
        "0000000000000000000000000000000000000000000000000000000000000000".to_owned();
    let mut saw_ndjson_trailer = false;
    for line in exported_text.lines().filter(|line| !line.trim().is_empty()) {
        let envelope = serde_json::from_str::<Value>(line)
            .context("approval export line must be valid JSON")?;
        let record_type = envelope
            .get("record_type")
            .and_then(Value::as_str)
            .context("approval export line missing record_type")?;
        assert_eq!(
            envelope.get("schema").and_then(Value::as_str),
            Some("palyra.approvals.export.ndjson.v1"),
            "approval export NDJSON schema id mismatch"
        );
        match record_type {
            "approval_record" => {
                ndjson_record_count = ndjson_record_count.saturating_add(1);
                let sequence = envelope
                    .get("sequence")
                    .and_then(Value::as_u64)
                    .context("approval record line missing sequence")?;
                assert_eq!(
                    sequence as usize, ndjson_record_count,
                    "approval export NDJSON sequence must be contiguous starting at 1"
                );
                let prev_checksum = envelope
                    .get("prev_checksum_sha256")
                    .and_then(Value::as_str)
                    .context("approval record line missing prev checksum")?;
                assert_eq!(
                    prev_checksum,
                    ndjson_previous_chain.as_str(),
                    "approval export NDJSON chain previous checksum mismatch"
                );
                let record_payload = envelope
                    .get("record")
                    .context("approval record line missing nested record payload")?;
                let record_payload_bytes = serde_json::to_vec(record_payload)
                    .context("failed to serialize exported approval record payload bytes")?;
                let mut record_hasher = Sha256::new();
                record_hasher.update(record_payload_bytes.as_slice());
                let expected_record_checksum = format!("{:x}", record_hasher.finalize());
                let record_checksum = envelope
                    .get("record_checksum_sha256")
                    .and_then(Value::as_str)
                    .context("approval record line missing record checksum")?;
                assert_eq!(
                    record_checksum, expected_record_checksum,
                    "approval export NDJSON record checksum must match serialized record payload"
                );
                let mut chain_hasher = Sha256::new();
                chain_hasher.update(b"palyra.approvals.export.ndjson.v1");
                chain_hasher.update(b"\n");
                chain_hasher.update(sequence.to_string().as_bytes());
                chain_hasher.update(b"\n");
                chain_hasher.update(prev_checksum.as_bytes());
                chain_hasher.update(b"\n");
                chain_hasher.update(record_checksum.as_bytes());
                let expected_chain_checksum = format!("{:x}", chain_hasher.finalize());
                let chain_checksum = envelope
                    .get("chain_checksum_sha256")
                    .and_then(Value::as_str)
                    .context("approval record line missing chain checksum")?;
                assert_eq!(
                    chain_checksum, expected_chain_checksum,
                    "approval export NDJSON chain checksum must match computed value"
                );
                ndjson_previous_chain = chain_checksum.to_owned();
            }
            "export_trailer" => {
                saw_ndjson_trailer = true;
                let exported_records = envelope
                    .get("exported_records")
                    .and_then(Value::as_u64)
                    .context("approval export trailer missing exported_records")?;
                assert_eq!(
                    exported_records as usize, ndjson_record_count,
                    "approval export trailer must report all exported approval records"
                );
                let final_chain_checksum = envelope
                    .get("final_chain_checksum_sha256")
                    .and_then(Value::as_str)
                    .context("approval export trailer missing final chain checksum")?;
                assert_eq!(
                    final_chain_checksum,
                    ndjson_previous_chain.as_str(),
                    "approval export trailer final chain checksum must match record chain tip"
                );
            }
            _ => {
                panic!("unexpected approval export NDJSON record_type value: {record_type}");
            }
        }
    }
    assert!(ndjson_record_count > 0, "approval export NDJSON should include at least one record");
    assert!(
        saw_ndjson_trailer,
        "approval export NDJSON must include terminal trailer line for tamper-evident chain"
    );
    assert!(!exported_text.contains("token=abc"), "export output must keep redacted token values");
    assert!(
        !exported_text.contains("sessionid=abc123"),
        "export output must keep redacted cookie values"
    );

    let mut export_json_request = tonic::Request::new(gateway_v1::ExportApprovalsRequest {
        v: 1,
        format: gateway_v1::ApprovalExportFormat::Json as i32,
        limit: 50,
        since_unix_ms: 0,
        until_unix_ms: 0,
        subject_id: "tool:custom.noop".to_owned(),
        principal: "user:ops".to_owned(),
        decision: gateway_v1::ApprovalDecision::Deny as i32,
        subject_type: gateway_v1::ApprovalSubjectType::Tool as i32,
    });
    authorize_metadata(export_json_request.metadata_mut())?;
    let mut export_json_stream = approvals_client
        .export_approvals(export_json_request)
        .await
        .context("failed to call ExportApprovals JSON")?
        .into_inner();
    let mut exported_json = Vec::new();
    while let Some(chunk) = export_json_stream.next().await {
        let chunk = chunk.context("failed to read ExportApprovals JSON chunk")?;
        if !chunk.chunk.is_empty() {
            exported_json.extend_from_slice(chunk.chunk.as_slice());
        }
        if chunk.done {
            break;
        }
    }
    let exported_json_text = String::from_utf8(exported_json.clone())
        .context("exported approvals JSON must be UTF-8")?;
    let exported_json_records = serde_json::from_slice::<Vec<Value>>(exported_json.as_slice())
        .context("exported approvals JSON should parse as an array")?;
    assert!(
        !exported_json_records.is_empty(),
        "JSON export should contain at least one approval record"
    );
    assert!(
        exported_json_records.iter().any(|record| {
            record
                .get("approval_id")
                .and_then(Value::as_str)
                .map(|value| value == approval_id.as_str())
                .unwrap_or(false)
        }),
        "JSON export should include persisted approval_id"
    );
    assert!(
        !exported_json_text.contains("token=abc"),
        "JSON export output must keep redacted token values"
    );
    assert!(
        !exported_json_text.contains("sessionid=abc123"),
        "JSON export output must keep redacted cookie values"
    );

    let mut export_json_empty_request = tonic::Request::new(gateway_v1::ExportApprovalsRequest {
        v: 1,
        format: gateway_v1::ApprovalExportFormat::Json as i32,
        limit: 50,
        since_unix_ms: 0,
        until_unix_ms: 0,
        subject_id: "tool:custom.none".to_owned(),
        principal: "user:ops".to_owned(),
        decision: gateway_v1::ApprovalDecision::Deny as i32,
        subject_type: gateway_v1::ApprovalSubjectType::Tool as i32,
    });
    authorize_metadata(export_json_empty_request.metadata_mut())?;
    let mut export_json_empty_stream = approvals_client
        .export_approvals(export_json_empty_request)
        .await
        .context("failed to call ExportApprovals empty JSON")?
        .into_inner();
    let mut exported_empty_json = Vec::new();
    while let Some(chunk) = export_json_empty_stream.next().await {
        let chunk = chunk.context("failed to read ExportApprovals empty JSON chunk")?;
        if !chunk.chunk.is_empty() {
            exported_empty_json.extend_from_slice(chunk.chunk.as_slice());
        }
        if chunk.done {
            break;
        }
    }
    let exported_empty_records =
        serde_json::from_slice::<Vec<Value>>(exported_empty_json.as_slice())
            .context("empty JSON export should still parse as an array")?;
    assert!(exported_empty_records.is_empty(), "empty JSON export should be represented as []");

    server_handle.join().expect("scripted openai server thread should exit");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(unix)]
async fn grpc_run_stream_executes_sandbox_process_runner_within_workspace_scope() -> Result<()> {
    let (openai_base_url, _request_count, server_handle) =
        spawn_scripted_openai_server(vec![ScriptedOpenAiResponse::immediate(
            200,
            r#"{"choices":[{"message":{"tool_calls":[{"function":{"name":"palyra.process.run","arguments":"{\"command\":\"uname\",\"args\":[]}"}}]}}]}"#
                .to_owned(),
        )])?;
    let workspace_root =
        std::env::current_dir().context("failed to resolve workspace root for process runner")?;
    let (child, admin_port, grpc_port, _journal_db_path, config_path) =
        spawn_palyrad_with_openai_provider_tool_policy_and_process_runner(
            ProcessRunnerSpawnConfig {
                openai_base_url: openai_base_url.as_str(),
                openai_api_key: OPENAI_API_KEY,
                allowed_tools: "palyra.process.run",
                max_calls_per_run: 2,
                execution_timeout_ms: 2_000,
                workspace_root: workspace_root.as_path(),
                allowed_executables: "uname",
                allowed_egress_hosts: "",
                allowed_dns_suffixes: "",
            },
        )?;
    let _config_guard = TempFileGuard::new(config_path);
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
        .await
        .context("failed to connect gRPC client")?;

    let (request_sender, request_receiver) = tokio_mpsc::channel(4);
    request_sender
        .send(sample_run_stream_request_with_text("sandbox process runner success path".to_owned()))
        .await
        .context("failed to send initial sandbox process runner request")?;
    let mut stream_request = tonic::Request::new(ReceiverStream::new(request_receiver));
    stream_request.metadata_mut().insert("authorization", format!("Bearer {ADMIN_TOKEN}").parse()?);
    stream_request.metadata_mut().insert("x-palyra-principal", "user:ops".parse()?);
    stream_request.metadata_mut().insert("x-palyra-device-id", DEVICE_ID.parse()?);
    stream_request.metadata_mut().insert("x-palyra-channel", "cli".parse()?);

    let mut response_stream =
        client.run_stream(stream_request).await.context("failed to call RunStream")?.into_inner();

    let mut saw_allow_decision = false;
    let mut saw_approval_request = false;
    let mut saw_success_result = false;
    let mut saw_sandbox_attestation = false;
    loop {
        let next_event = tokio::time::timeout(Duration::from_secs(5), response_stream.next())
            .await
            .context("sandbox process runner success stream stalled before expected events")?;
        let Some(event) = next_event else {
            break;
        };
        let event = event.context("failed to read RunStream event")?;
        if let Some(body) = event.body {
            match body {
                common_v1::run_stream_event::Body::ToolApprovalRequest(approval_request) => {
                    let proposal_id = approval_request
                        .proposal_id
                        .as_ref()
                        .map(|proposal_id| proposal_id.ulid.as_str())
                        .context("tool approval request missing proposal_id")?;
                    request_sender
                        .send(sample_tool_approval_response_request(
                            proposal_id,
                            true,
                            "allow_once",
                        ))
                        .await
                        .context("failed to send tool approval response")?;
                    saw_approval_request = true;
                }
                common_v1::run_stream_event::Body::ToolDecision(decision) => {
                    if decision.kind == common_v1::tool_decision::DecisionKind::Allow as i32 {
                        saw_allow_decision = true;
                    }
                }
                common_v1::run_stream_event::Body::ToolResult(result) => {
                    if result.success {
                        let output = serde_json::from_slice::<Value>(&result.output_json)
                            .context("sandbox tool result output_json should be valid JSON")?;
                        assert_eq!(output.get("exit_code").and_then(Value::as_i64), Some(0));
                        assert!(
                            output
                                .get("stdout")
                                .and_then(Value::as_str)
                                .map(|stdout| !stdout.trim().is_empty())
                                .unwrap_or(false),
                            "sandbox process stdout should include uname output"
                        );
                        saw_success_result = true;
                    }
                }
                common_v1::run_stream_event::Body::ToolAttestation(attestation) => {
                    if attestation.executor == "sandbox_tier_b" {
                        assert!(!attestation.timed_out, "sandbox success path must not time out");
                        saw_sandbox_attestation = true;
                    }
                }
                _ => {}
            }
        }
        if saw_approval_request
            && saw_allow_decision
            && saw_success_result
            && saw_sandbox_attestation
        {
            break;
        }
    }

    assert!(
        saw_approval_request,
        "sensitive process runner tool call should request explicit approval"
    );
    assert!(saw_allow_decision, "sandbox process tool call should be allowed by policy");
    assert!(saw_success_result, "sandbox process tool call should execute successfully");
    assert!(saw_sandbox_attestation, "sandbox process tool call should emit sandbox attestation");

    server_handle.join().expect("scripted openai server thread should exit");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(unix)]
async fn grpc_run_stream_blocks_sandbox_process_runner_path_traversal() -> Result<()> {
    let (openai_base_url, _request_count, server_handle) =
        spawn_scripted_openai_server(vec![ScriptedOpenAiResponse::immediate(
            200,
            r#"{"choices":[{"message":{"tool_calls":[{"function":{"name":"palyra.process.run","arguments":"{\"command\":\"uname\",\"args\":[\"../outside.txt\"]}"}}]}}]}"#
                .to_owned(),
        )])?;
    let workspace_root =
        std::env::current_dir().context("failed to resolve workspace root for process runner")?;
    let (child, admin_port, grpc_port, _journal_db_path, config_path) =
        spawn_palyrad_with_openai_provider_tool_policy_and_process_runner(
            ProcessRunnerSpawnConfig {
                openai_base_url: openai_base_url.as_str(),
                openai_api_key: OPENAI_API_KEY,
                allowed_tools: "palyra.process.run",
                max_calls_per_run: 2,
                execution_timeout_ms: 2_000,
                workspace_root: workspace_root.as_path(),
                allowed_executables: "uname",
                allowed_egress_hosts: "allowed.example",
                allowed_dns_suffixes: ".corp.local",
            },
        )?;
    let _config_guard = TempFileGuard::new(config_path);
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
        .await
        .context("failed to connect gRPC client")?;

    let (request_sender, request_receiver) = tokio_mpsc::channel(4);
    request_sender
        .send(sample_run_stream_request_with_text("sandbox traversal deny path".to_owned()))
        .await
        .context("failed to send initial sandbox traversal request")?;
    let mut stream_request = tonic::Request::new(ReceiverStream::new(request_receiver));
    stream_request.metadata_mut().insert("authorization", format!("Bearer {ADMIN_TOKEN}").parse()?);
    stream_request.metadata_mut().insert("x-palyra-principal", "user:ops".parse()?);
    stream_request.metadata_mut().insert("x-palyra-device-id", DEVICE_ID.parse()?);
    stream_request.metadata_mut().insert("x-palyra-channel", "cli".parse()?);

    let mut response_stream =
        client.run_stream(stream_request).await.context("failed to call RunStream")?.into_inner();

    let mut saw_approval_request = false;
    let mut saw_failed_result = false;
    loop {
        let next_event = tokio::time::timeout(Duration::from_secs(5), response_stream.next())
            .await
            .context("sandbox traversal stream stalled before expected events")?;
        let Some(event) = next_event else {
            break;
        };
        let event = event.context("failed to read RunStream event")?;
        if let Some(body) = event.body {
            match body {
                common_v1::run_stream_event::Body::ToolApprovalRequest(approval_request) => {
                    let proposal_id = approval_request
                        .proposal_id
                        .as_ref()
                        .map(|proposal_id| proposal_id.ulid.as_str())
                        .context("tool approval request missing proposal_id")?;
                    request_sender
                        .send(sample_tool_approval_response_request(
                            proposal_id,
                            true,
                            "allow_once",
                        ))
                        .await
                        .context("failed to send tool approval response")?;
                    saw_approval_request = true;
                }
                common_v1::run_stream_event::Body::ToolResult(result) => {
                    if !result.success {
                        assert!(
                            result.error.contains("path traversal"),
                            "sandbox denial should explain traversal block"
                        );
                        saw_failed_result = true;
                    }
                }
                _ => {}
            }
        }
        if saw_approval_request && saw_failed_result {
            break;
        }
    }

    assert!(
        saw_approval_request,
        "sensitive process runner tool call should request explicit approval"
    );
    assert!(saw_failed_result, "sandbox traversal must produce failed tool result");

    server_handle.join().expect("scripted openai server thread should exit");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(unix)]
async fn grpc_run_stream_blocks_sandbox_process_runner_non_allowlisted_egress_host() -> Result<()> {
    let (openai_base_url, _request_count, server_handle) =
        spawn_scripted_openai_server(vec![ScriptedOpenAiResponse::immediate(
            200,
            r#"{"choices":[{"message":{"tool_calls":[{"function":{"name":"palyra.process.run","arguments":"{\"command\":\"uname\",\"args\":[\"https://blocked.example/path\"]}"}}]}}]}"#
                .to_owned(),
        )])?;
    let workspace_root =
        std::env::current_dir().context("failed to resolve workspace root for process runner")?;
    let (child, admin_port, grpc_port, _journal_db_path, config_path) =
        spawn_palyrad_with_openai_provider_tool_policy_and_process_runner(
            ProcessRunnerSpawnConfig {
                openai_base_url: openai_base_url.as_str(),
                openai_api_key: OPENAI_API_KEY,
                allowed_tools: "palyra.process.run",
                max_calls_per_run: 2,
                execution_timeout_ms: 2_000,
                workspace_root: workspace_root.as_path(),
                allowed_executables: "uname",
                allowed_egress_hosts: "allowed.example",
                allowed_dns_suffixes: ".corp.local",
            },
        )?;
    let _config_guard = TempFileGuard::new(config_path);
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
        .await
        .context("failed to connect gRPC client")?;

    let (request_sender, request_receiver) = tokio_mpsc::channel(4);
    request_sender
        .send(sample_run_stream_request_with_text("sandbox egress deny path".to_owned()))
        .await
        .context("failed to send initial sandbox egress request")?;
    let mut stream_request = tonic::Request::new(ReceiverStream::new(request_receiver));
    stream_request.metadata_mut().insert("authorization", format!("Bearer {ADMIN_TOKEN}").parse()?);
    stream_request.metadata_mut().insert("x-palyra-principal", "user:ops".parse()?);
    stream_request.metadata_mut().insert("x-palyra-device-id", DEVICE_ID.parse()?);
    stream_request.metadata_mut().insert("x-palyra-channel", "cli".parse()?);

    let mut response_stream =
        client.run_stream(stream_request).await.context("failed to call RunStream")?.into_inner();

    let mut saw_approval_request = false;
    let mut saw_failed_result = false;
    loop {
        let next_event = tokio::time::timeout(Duration::from_secs(5), response_stream.next())
            .await
            .context("sandbox egress stream stalled before expected events")?;
        let Some(event) = next_event else {
            break;
        };
        let event = event.context("failed to read RunStream event")?;
        if let Some(body) = event.body {
            match body {
                common_v1::run_stream_event::Body::ToolApprovalRequest(approval_request) => {
                    let proposal_id = approval_request
                        .proposal_id
                        .as_ref()
                        .map(|proposal_id| proposal_id.ulid.as_str())
                        .context("tool approval request missing proposal_id")?;
                    request_sender
                        .send(sample_tool_approval_response_request(
                            proposal_id,
                            true,
                            "allow_once",
                        ))
                        .await
                        .context("failed to send tool approval response")?;
                    saw_approval_request = true;
                }
                common_v1::run_stream_event::Body::ToolResult(result) => {
                    if !result.success {
                        assert!(
                            result.error.contains("blocked.example"),
                            "sandbox denial should include denied host context"
                        );
                        saw_failed_result = true;
                    }
                }
                _ => {}
            }
        }
        if saw_approval_request && saw_failed_result {
            break;
        }
    }

    assert!(
        saw_approval_request,
        "sensitive process runner tool call should request explicit approval"
    );
    assert!(saw_failed_result, "sandbox denied egress host must produce failed tool result");

    server_handle.join().expect("scripted openai server thread should exit");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_run_stream_denies_wasm_plugin_runtime_without_approval_channel() -> Result<()> {
    let skill_id = "acme.echo_http";
    let skill_version = "1.2.3";
    let tool_arguments = serde_json::json!({
        "skill_id": skill_id,
        "skill_version": skill_version,
        "module_wat": r#"
            (module
                (import "palyra:plugins/host-capabilities@0.1.0" "http-count" (func $http_count (result i32)))
                (func (export "run") (result i32)
                    call $http_count
                )
            )
        "#,
        "capabilities": {
            "http_hosts": ["api.example.com"]
        }
    });
    let response_body = openai_tool_call_response("palyra.plugin.run", &tool_arguments)?;
    let (openai_base_url, _request_count, server_handle) =
        spawn_scripted_openai_server(vec![ScriptedOpenAiResponse::immediate(200, response_body)])?;
    let (child, admin_port, grpc_port, _journal_db_path, config_path) =
        spawn_palyrad_with_openai_provider_tool_policy_and_wasm_runtime(WasmRuntimeSpawnConfig {
            openai_base_url: openai_base_url.as_str(),
            openai_api_key: OPENAI_API_KEY,
            allowed_tools: "palyra.plugin.run",
            max_calls_per_run: 2,
            execution_timeout_ms: 2_000,
            allowed_http_hosts: "api.example.com",
            allowed_secrets: "db_password",
            allowed_storage_prefixes: "plugins/cache",
            allowed_channels: "cli",
            fuel_budget: 10_000_000,
            max_memory_bytes: 64 * 1024 * 1024,
            max_table_elements: 100_000,
            max_instances: 256,
        })?;
    let _config_guard = TempFileGuard::new(config_path);
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;
    let enable_response = admin_post_json_async(
        admin_port,
        format!("/admin/v1/skills/{skill_id}/enable"),
        serde_json::json!({
            "version": skill_version,
            "reason": "integration-test activation for approval-gate path",
            "override": true,
        }),
    )
    .await?;
    assert_eq!(
        enable_response.get("status").and_then(Value::as_str),
        Some("active"),
        "admin enable endpoint should activate skill before approval-gate test"
    );

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
        .await
        .context("failed to connect gRPC client")?;

    let mut stream_request =
        tonic::Request::new(tokio_stream::iter(vec![sample_run_stream_request_with_text(
            "wasm plugin runtime success path".to_owned(),
        )]));
    stream_request.metadata_mut().insert("authorization", format!("Bearer {ADMIN_TOKEN}").parse()?);
    stream_request.metadata_mut().insert("x-palyra-principal", "user:ops".parse()?);
    stream_request.metadata_mut().insert("x-palyra-device-id", DEVICE_ID.parse()?);
    stream_request.metadata_mut().insert("x-palyra-channel", "cli".parse()?);

    let mut response_stream =
        client.run_stream(stream_request).await.context("failed to call RunStream")?.into_inner();

    let mut saw_deny_decision = false;
    let mut saw_failed_result = false;
    let mut saw_policy_attestation = false;
    while let Some(event) = response_stream.next().await {
        let event = event.context("failed to read RunStream event")?;
        if let Some(body) = event.body {
            match body {
                common_v1::run_stream_event::Body::ToolDecision(decision) => {
                    if decision.kind == common_v1::tool_decision::DecisionKind::Deny as i32 {
                        assert!(
                            decision.approval_required,
                            "sensitive wasm plugin tools must keep approval_required=true"
                        );
                        assert!(
                            decision.reason.contains("approval required"),
                            "decision reason should explain missing approval channel"
                        );
                        saw_deny_decision = true;
                    }
                }
                common_v1::run_stream_event::Body::ToolResult(result) => {
                    if !result.success {
                        assert!(
                            result.error.contains("approval required"),
                            "failed result should explain approval gating"
                        );
                        saw_failed_result = true;
                    }
                }
                common_v1::run_stream_event::Body::ToolAttestation(attestation) => {
                    if attestation.executor == "policy" {
                        saw_policy_attestation = true;
                    }
                }
                _ => {}
            }
        }
    }

    assert!(saw_deny_decision, "sensitive wasm plugin tool call must be denied without approval");
    assert!(saw_failed_result, "denied wasm plugin tool call must return failed result");
    assert!(saw_policy_attestation, "approval denial must emit policy attestation");

    let status_snapshot = admin_get_json_async(admin_port, "/admin/v1/status".to_owned()).await?;
    assert_eq!(
        status_snapshot.pointer("/tool_call_policy/wasm_runtime/enabled").and_then(Value::as_bool),
        Some(true)
    );

    server_handle.join().expect("scripted openai server thread should exit");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_run_stream_blocks_wasm_plugin_runtime_non_allowlisted_capability() -> Result<()> {
    let skill_id = "acme.echo_http";
    let skill_version = "1.2.3";
    let tool_arguments = serde_json::json!({
        "skill_id": skill_id,
        "skill_version": skill_version,
        "module_wat": "(module (func (export \"run\") (result i32) i32.const 1))",
        "capabilities": {
            "http_hosts": ["blocked.example"]
        }
    });
    let response_body = openai_tool_call_response("palyra.plugin.run", &tool_arguments)?;
    let (openai_base_url, _request_count, server_handle) =
        spawn_scripted_openai_server(vec![ScriptedOpenAiResponse::immediate(200, response_body)])?;
    let (child, admin_port, grpc_port, _journal_db_path, config_path) =
        spawn_palyrad_with_openai_provider_tool_policy_and_wasm_runtime(WasmRuntimeSpawnConfig {
            openai_base_url: openai_base_url.as_str(),
            openai_api_key: OPENAI_API_KEY,
            allowed_tools: "palyra.plugin.run",
            max_calls_per_run: 2,
            execution_timeout_ms: 2_000,
            allowed_http_hosts: "api.example.com",
            allowed_secrets: "",
            allowed_storage_prefixes: "",
            allowed_channels: "",
            fuel_budget: 10_000_000,
            max_memory_bytes: 64 * 1024 * 1024,
            max_table_elements: 100_000,
            max_instances: 256,
        })?;
    let _config_guard = TempFileGuard::new(config_path);
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;
    let enable_response = admin_post_json_async(
        admin_port,
        format!("/admin/v1/skills/{skill_id}/enable"),
        serde_json::json!({
            "version": skill_version,
            "reason": "integration-test activation for approval-gate path",
            "override": true,
        }),
    )
    .await?;
    assert_eq!(
        enable_response.get("status").and_then(Value::as_str),
        Some("active"),
        "admin enable endpoint should activate skill before approval-gate test"
    );

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
        .await
        .context("failed to connect gRPC client")?;

    let mut stream_request =
        tonic::Request::new(tokio_stream::iter(vec![sample_run_stream_request_with_text(
            "wasm plugin capability deny path".to_owned(),
        )]));
    stream_request.metadata_mut().insert("authorization", format!("Bearer {ADMIN_TOKEN}").parse()?);
    stream_request.metadata_mut().insert("x-palyra-principal", "user:ops".parse()?);
    stream_request.metadata_mut().insert("x-palyra-device-id", DEVICE_ID.parse()?);
    stream_request.metadata_mut().insert("x-palyra-channel", "cli".parse()?);

    let mut response_stream =
        client.run_stream(stream_request).await.context("failed to call RunStream")?.into_inner();

    let mut saw_deny_decision = false;
    let mut saw_failed_result = false;
    let mut saw_policy_attestation = false;
    let mut saw_sandbox_attestation = false;
    while let Some(event) = response_stream.next().await {
        let event = event.context("failed to read RunStream event")?;
        if let Some(body) = event.body {
            match body {
                common_v1::run_stream_event::Body::ToolDecision(decision) => {
                    if decision.kind == common_v1::tool_decision::DecisionKind::Deny as i32 {
                        assert!(
                            decision.reason.contains("approval required"),
                            "approval gating should run before wasm capability evaluation"
                        );
                        saw_deny_decision = true;
                    }
                }
                common_v1::run_stream_event::Body::ToolResult(result) => {
                    if !result.success {
                        assert!(
                            result.error.contains("approval required"),
                            "denied result should explain missing approval"
                        );
                        saw_failed_result = true;
                    }
                }
                common_v1::run_stream_event::Body::ToolAttestation(attestation) => {
                    if attestation.executor == "policy" {
                        saw_policy_attestation = true;
                    }
                    if attestation.executor == "sandbox_tier_a" {
                        saw_sandbox_attestation = true;
                    }
                }
                _ => {}
            }
        }
    }

    assert!(saw_deny_decision, "sensitive wasm proposal must emit deny decision");
    assert!(saw_failed_result, "denied wasm proposal must produce failed tool result");
    assert!(saw_policy_attestation, "denied wasm proposal must emit policy attestation");
    assert!(!saw_sandbox_attestation, "sandbox runtime must not execute when approval is missing");

    server_handle.join().expect("scripted openai server thread should exit");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_run_stream_denies_unknown_skill_before_approval() -> Result<()> {
    let skill_id = "acme.unknown_skill";
    let skill_version = "9.9.9";
    let tool_arguments = serde_json::json!({
        "skill_id": skill_id,
        "skill_version": skill_version,
        "module_wat": "(module (func (export \"run\") (result i32) i32.const 1))",
        "capabilities": {
            "http_hosts": ["api.example.com"]
        }
    });
    let response_body = openai_tool_call_response("palyra.plugin.run", &tool_arguments)?;
    let (openai_base_url, _request_count, server_handle) =
        spawn_scripted_openai_server(vec![ScriptedOpenAiResponse::immediate(200, response_body)])?;
    let (child, admin_port, grpc_port, _journal_db_path, config_path) =
        spawn_palyrad_with_openai_provider_tool_policy_and_wasm_runtime(WasmRuntimeSpawnConfig {
            openai_base_url: openai_base_url.as_str(),
            openai_api_key: OPENAI_API_KEY,
            allowed_tools: "palyra.plugin.run",
            max_calls_per_run: 2,
            execution_timeout_ms: 2_000,
            allowed_http_hosts: "api.example.com",
            allowed_secrets: "",
            allowed_storage_prefixes: "",
            allowed_channels: "cli",
            fuel_budget: 10_000_000,
            max_memory_bytes: 64 * 1024 * 1024,
            max_table_elements: 100_000,
            max_instances: 256,
        })?;
    let _config_guard = TempFileGuard::new(config_path);
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
        .await
        .context("failed to connect gRPC client")?;

    let (request_sender, request_receiver) = tokio_mpsc::channel(4);
    request_sender
        .send(sample_run_stream_request_with_text("unknown skill deny path".to_owned()))
        .await
        .context("failed to send initial unknown skill request")?;
    let mut stream_request = tonic::Request::new(ReceiverStream::new(request_receiver));
    stream_request.metadata_mut().insert("authorization", format!("Bearer {ADMIN_TOKEN}").parse()?);
    stream_request.metadata_mut().insert("x-palyra-principal", "user:ops".parse()?);
    stream_request.metadata_mut().insert("x-palyra-device-id", DEVICE_ID.parse()?);
    stream_request.metadata_mut().insert("x-palyra-channel", "cli".parse()?);

    let mut response_stream =
        client.run_stream(stream_request).await.context("failed to call RunStream")?.into_inner();

    let mut saw_approval_request = false;
    let mut saw_deny_decision = false;
    let mut saw_failed_result = false;
    loop {
        let next_event = tokio::time::timeout(Duration::from_secs(5), response_stream.next())
            .await
            .context("unknown-skill stream stalled before expected events")?;
        let Some(event) = next_event else {
            break;
        };
        let event = event.context("failed to read RunStream event")?;
        if let Some(body) = event.body {
            match body {
                common_v1::run_stream_event::Body::ToolApprovalRequest(approval_request) => {
                    let proposal_id = approval_request
                        .proposal_id
                        .as_ref()
                        .map(|proposal_id| proposal_id.ulid.as_str())
                        .context("tool approval request missing proposal_id")?;
                    request_sender
                        .send(sample_tool_approval_response_request(
                            proposal_id,
                            true,
                            "allow_once",
                        ))
                        .await
                        .context("failed to send tool approval response")?;
                    saw_approval_request = true;
                }
                common_v1::run_stream_event::Body::ToolDecision(decision) => {
                    if decision.kind == common_v1::tool_decision::DecisionKind::Deny as i32 {
                        assert!(
                            decision.reason.contains("skill execution blocked by security gate"),
                            "unknown skill denial should be attributed to skill security gate"
                        );
                        assert!(
                            decision.reason.contains("status=missing"),
                            "unknown skill denial should describe missing status record"
                        );
                        assert!(
                            !decision.approval_required,
                            "unknown skill denial should short-circuit before approval workflow"
                        );
                        saw_deny_decision = true;
                    }
                }
                common_v1::run_stream_event::Body::ToolResult(result) => {
                    if !result.success {
                        assert!(
                            result.error.contains("skill execution blocked by security gate"),
                            "failed result should include skill gate denial context"
                        );
                        saw_failed_result = true;
                    }
                }
                _ => {}
            }
        }
        if saw_deny_decision && saw_failed_result {
            break;
        }
    }

    assert!(!saw_approval_request, "unknown skills must be denied before approval workflow");
    assert!(saw_deny_decision, "unknown skill should emit deny decision");
    assert!(saw_failed_result, "unknown skill should emit failed tool result");

    server_handle.join().expect("scripted openai server thread should exit");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_run_stream_denies_quarantined_skill_before_approval_and_records_event() -> Result<()>
{
    let skill_id = "acme.echo_http";
    let runtime_skill_id = "Acme.Echo_Http";
    let skill_version = "1.2.3";
    let tool_arguments = serde_json::json!({
        "skill_id": runtime_skill_id,
        "skill_version": skill_version,
        "module_wat": "(module (func (export \"run\") (result i32) i32.const 1))",
        "capabilities": {
            "http_hosts": ["api.example.com"]
        }
    });
    let response_body = openai_tool_call_response("palyra.plugin.run", &tool_arguments)?;
    let (openai_base_url, _request_count, server_handle) =
        spawn_scripted_openai_server(vec![ScriptedOpenAiResponse::immediate(200, response_body)])?;
    let (child, admin_port, grpc_port, journal_db_path, config_path) =
        spawn_palyrad_with_openai_provider_tool_policy_and_wasm_runtime(WasmRuntimeSpawnConfig {
            openai_base_url: openai_base_url.as_str(),
            openai_api_key: OPENAI_API_KEY,
            allowed_tools: "palyra.plugin.run",
            max_calls_per_run: 2,
            execution_timeout_ms: 2_000,
            allowed_http_hosts: "api.example.com",
            allowed_secrets: "",
            allowed_storage_prefixes: "",
            allowed_channels: "cli",
            fuel_budget: 10_000_000,
            max_memory_bytes: 64 * 1024 * 1024,
            max_table_elements: 100_000,
            max_instances: 256,
        })?;
    let _config_guard = TempFileGuard::new(config_path);
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let quarantine_response = admin_post_json_async(
        admin_port,
        format!("/admin/v1/skills/{skill_id}/quarantine"),
        serde_json::json!({
            "version": skill_version,
            "reason": "integration-test quarantine",
        }),
    )
    .await?;
    assert_eq!(
        quarantine_response.get("status").and_then(Value::as_str),
        Some("quarantined"),
        "admin quarantine endpoint should persist quarantined status"
    );

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
        .await
        .context("failed to connect gRPC client")?;

    let (request_sender, request_receiver) = tokio_mpsc::channel(4);
    request_sender
        .send(sample_run_stream_request_with_text("quarantined skill deny path".to_owned()))
        .await
        .context("failed to send initial quarantined skill request")?;
    let mut stream_request = tonic::Request::new(ReceiverStream::new(request_receiver));
    stream_request.metadata_mut().insert("authorization", format!("Bearer {ADMIN_TOKEN}").parse()?);
    stream_request.metadata_mut().insert("x-palyra-principal", "user:ops".parse()?);
    stream_request.metadata_mut().insert("x-palyra-device-id", DEVICE_ID.parse()?);
    stream_request.metadata_mut().insert("x-palyra-channel", "cli".parse()?);

    let mut response_stream =
        client.run_stream(stream_request).await.context("failed to call RunStream")?.into_inner();

    let mut saw_approval_request = false;
    let mut saw_deny_decision = false;
    let mut saw_failed_result = false;
    loop {
        let next_event = tokio::time::timeout(Duration::from_secs(5), response_stream.next())
            .await
            .context("quarantined skill stream stalled before expected events")?;
        let Some(event) = next_event else {
            break;
        };
        let event = event.context("failed to read RunStream event")?;
        if let Some(body) = event.body {
            match body {
                common_v1::run_stream_event::Body::ToolApprovalRequest(approval_request) => {
                    let proposal_id = approval_request
                        .proposal_id
                        .as_ref()
                        .map(|proposal_id| proposal_id.ulid.as_str())
                        .context("tool approval request missing proposal_id")?;
                    request_sender
                        .send(sample_tool_approval_response_request(
                            proposal_id,
                            true,
                            "allow_once",
                        ))
                        .await
                        .context("failed to send tool approval response")?;
                    saw_approval_request = true;
                }
                common_v1::run_stream_event::Body::ToolDecision(decision) => {
                    if decision.kind == common_v1::tool_decision::DecisionKind::Deny as i32 {
                        assert!(
                            decision.reason.contains("skill execution blocked by security gate"),
                            "deny decision should explain skill security gate denial"
                        );
                        assert!(
                            !decision.approval_required,
                            "quarantined skill denial should short-circuit before approval"
                        );
                        saw_deny_decision = true;
                    }
                }
                common_v1::run_stream_event::Body::ToolResult(result) => {
                    if !result.success {
                        assert!(
                            result.error.contains("skill execution blocked by security gate"),
                            "failed result should include skill gate denial context"
                        );
                        saw_failed_result = true;
                    }
                }
                _ => {}
            }
        }
        if saw_deny_decision && saw_failed_result {
            break;
        }
    }

    assert!(!saw_approval_request, "quarantined skills must be denied before approval workflow");
    assert!(saw_deny_decision, "quarantined skill should emit deny decision");
    assert!(saw_failed_result, "quarantined skill should emit failed tool result");

    let status_snapshot = admin_get_json_async(admin_port, "/admin/v1/status".to_owned()).await?;
    assert_eq!(
        status_snapshot.pointer("/counters/skill_execution_denied").and_then(Value::as_u64),
        Some(1),
        "skill_execution_denied counter should increment for quarantine denial"
    );

    let connection =
        Connection::open(journal_db_path).context("failed to open journal sqlite db")?;
    let mut statement = connection
        .prepare(
            r#"
                SELECT kind, payload_json
                FROM journal_events
                ORDER BY seq ASC
            "#,
        )
        .context("failed to prepare journal event query")?;
    let mut rows = statement.query([]).context("failed to query journal event rows")?;
    let mut saw_skill_execution_denied = false;
    let mut saw_skill_execution_denied_kind = false;
    let mut saw_skill_execution_denied_payload = false;
    while let Some(row) = rows.next().context("failed to iterate journal rows")? {
        let kind: i32 = row.get(0).context("journal kind should be readable")?;
        let payload_json: String = row.get(1).context("journal payload_json should be readable")?;
        let payload: Value = serde_json::from_str(payload_json.as_str())
            .context("journal payload_json must be valid json")?;
        if payload.get("event").and_then(Value::as_str) == Some("skill.execution_denied") {
            saw_skill_execution_denied = true;
            if kind == common_v1::journal_event::EventKind::ToolProposed as i32 {
                saw_skill_execution_denied_kind = true;
            }
            if payload.get("skill_id").and_then(Value::as_str) == Some(skill_id)
                && payload.get("skill_version").and_then(Value::as_str) == Some(skill_version)
                && payload
                    .get("reason")
                    .and_then(Value::as_str)
                    .map(|reason| reason.contains("skill execution blocked by security gate"))
                    .unwrap_or(false)
            {
                saw_skill_execution_denied_payload = true;
            }
        }
    }
    assert!(
        saw_skill_execution_denied,
        "quarantine denials must persist skill.execution_denied journal event"
    );
    assert!(
        saw_skill_execution_denied_kind,
        "skill.execution_denied event should use EVENT_KIND_TOOL_PROPOSED"
    );
    assert!(
        saw_skill_execution_denied_payload,
        "skill.execution_denied payload must include skill id/version and denial reason"
    );

    server_handle.join().expect("scripted openai server thread should exit");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_run_stream_admin_cancel_preempts_inflight_provider_call() -> Result<()> {
    let (openai_base_url, request_count, server_handle) =
        spawn_scripted_openai_server(vec![ScriptedOpenAiResponse::delayed(
            200,
            r#"{"choices":[{"message":{"content":"slow provider response"}}]}"#.to_owned(),
            Duration::from_secs(5),
        )])?;
    let (child, admin_port, grpc_port, _journal_db_path) =
        spawn_palyrad_with_openai_provider(openai_base_url.as_str(), OPENAI_API_KEY)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
        .await
        .context("failed to connect gRPC client")?;

    let mut stream_request =
        tonic::Request::new(tokio_stream::iter(vec![sample_run_stream_request_with_text(
            "this request should be cancelled while provider call is in-flight".to_owned(),
        )]));
    stream_request.metadata_mut().insert("authorization", format!("Bearer {ADMIN_TOKEN}").parse()?);
    stream_request.metadata_mut().insert("x-palyra-principal", "user:ops".parse()?);
    stream_request.metadata_mut().insert("x-palyra-device-id", DEVICE_ID.parse()?);
    stream_request.metadata_mut().insert("x-palyra-channel", "cli".parse()?);

    let mut response_stream =
        client.run_stream(stream_request).await.context("failed to call RunStream")?.into_inner();

    let in_progress_kind = common_v1::stream_status::StatusKind::InProgress as i32;
    loop {
        let next_event = tokio::time::timeout(Duration::from_secs(2), response_stream.next())
            .await
            .context("run stream did not emit in-progress status before timeout")?;
        let Some(event) = next_event else {
            anyhow::bail!("run stream ended before entering in-progress state");
        };
        let event = event.context("failed to read RunStream event while waiting in-progress")?;
        if let Some(common_v1::run_stream_event::Body::Status(status)) = event.body {
            if status.kind == in_progress_kind {
                break;
            }
        }
    }

    let cancel_started_at = Instant::now();
    let cancel_snapshot = admin_post_json_async(
        admin_port,
        format!("/admin/v1/runs/{RUN_ID}/cancel"),
        serde_json::json!({ "reason": "integration_cancel_request" }),
    )
    .await?;
    assert_eq!(
        cancel_snapshot.get("cancel_requested").and_then(Value::as_bool),
        Some(true),
        "admin cancel endpoint should persist cancel flag"
    );

    let mut saw_failed = false;
    let mut saw_done = false;
    let failed_kind = common_v1::stream_status::StatusKind::Failed as i32;
    let done_kind = common_v1::stream_status::StatusKind::Done as i32;
    loop {
        let next_event = tokio::time::timeout(Duration::from_secs(2), response_stream.next())
            .await
            .context("run stream did not terminate quickly after cancellation")?;
        let Some(event) = next_event else {
            break;
        };
        let event = event.context("failed to read RunStream event after cancellation")?;
        if let Some(common_v1::run_stream_event::Body::Status(status)) = event.body {
            if status.kind == failed_kind {
                saw_failed = true;
                break;
            }
            if status.kind == done_kind {
                saw_done = true;
                break;
            }
        }
    }

    assert!(
        cancel_started_at.elapsed() < Duration::from_secs(2),
        "run cancellation should preempt in-flight provider call without waiting for upstream timeout"
    );
    assert!(saw_failed, "cancelled run should emit failed status");
    assert!(!saw_done, "cancelled run must not emit done status");
    assert_eq!(
        request_count.load(Ordering::Relaxed),
        1,
        "cancel preemption should not trigger extra upstream retries in this scenario"
    );

    let run_snapshot = admin_get_json_async(admin_port, format!("/admin/v1/runs/{RUN_ID}")).await?;
    assert_eq!(
        run_snapshot.get("state").and_then(Value::as_str).context("run snapshot missing state")?,
        "cancelled"
    );

    server_handle.join().expect("scripted openai server thread should exit");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_append_event_persists_redacted_payload_and_hash_chain() -> Result<()> {
    let (child, admin_port, grpc_port, journal_db_path) =
        spawn_palyrad_with_dynamic_ports_and_hash_chain(true)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
        .await
        .context("failed to connect gRPC client")?;

    let first_request = authorized_append_event_request(sample_journal_event(
        "01ARZ3NDEKTSV4RRFFQ69G5FB0",
        br#"{"stage":"first","note":"safe"}"#,
    ))?;
    client.append_event(first_request).await.context("failed to append first journal event")?;

    let second_request = authorized_append_event_request(sample_journal_event(
        "01ARZ3NDEKTSV4RRFFQ69G5FB1",
        br#"{"api_token":"SECRET_TOKEN_VALUE","nested":{"password":"123456"}}"#,
    ))?;
    client.append_event(second_request).await.context("failed to append second journal event")?;

    let connection =
        Connection::open(journal_db_path).context("failed to open journal sqlite db")?;
    let mut statement = connection
        .prepare(
            r#"
                SELECT payload_json, redacted, hash, prev_hash
                FROM journal_events
                ORDER BY seq ASC
            "#,
        )
        .context("failed to prepare journal query")?;
    let mut rows = statement.query([]).context("failed to query journal rows")?;
    let first = rows
        .next()
        .context("failed to read first row")?
        .context("first journal row should exist")?;
    let first_hash: Option<String> = first.get(2).context("first hash should be readable")?;
    let first_prev_hash: Option<String> =
        first.get(3).context("first prev_hash should be readable")?;
    assert!(first_hash.is_some(), "hash-chain mode should generate first hash");
    assert!(first_prev_hash.is_none(), "first event must not have prev_hash");

    let second = rows
        .next()
        .context("failed to read second row")?
        .context("second journal row should exist")?;
    let second_payload: String = second.get(0).context("second payload should be readable")?;
    let second_redacted: i64 = second.get(1).context("second redaction flag should be readable")?;
    let second_hash: Option<String> = second.get(2).context("second hash should be readable")?;
    let second_prev_hash: Option<String> =
        second.get(3).context("second prev_hash should be readable")?;

    assert_eq!(second_redacted, 1, "secret-bearing payload must be marked redacted");
    assert!(
        !second_payload.contains("SECRET_TOKEN_VALUE") && !second_payload.contains("123456"),
        "journal payload must not contain raw secret values"
    );
    assert!(
        second_payload.contains("<redacted>"),
        "journal payload should include redaction marker"
    );
    assert!(second_hash.is_some(), "second event should include hash");
    assert_eq!(
        second_prev_hash, first_hash,
        "second event prev_hash must reference first event hash"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_append_event_duplicate_event_id_returns_already_exists() -> Result<()> {
    let (child, admin_port, grpc_port, _journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
        .await
        .context("failed to connect gRPC client")?;

    let duplicate_event_id = "01ARZ3NDEKTSV4RRFFQ69G5FB7";
    let first_request = authorized_append_event_request(sample_journal_event(
        duplicate_event_id,
        br#"{"attempt":1}"#,
    ))?;
    client.append_event(first_request).await.context("first append should succeed")?;

    let second_request = authorized_append_event_request(sample_journal_event(
        duplicate_event_id,
        br#"{"attempt":2}"#,
    ))?;
    let error =
        client.append_event(second_request).await.expect_err("duplicate append must be rejected");
    assert_eq!(error.code(), Code::AlreadyExists, "duplicate event IDs should be deterministic");
    assert!(
        error.message().contains(duplicate_event_id),
        "duplicate error should include conflicting event id"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_append_event_rejects_transport_oversized_payload() -> Result<()> {
    let (child, admin_port, grpc_port, _journal_db_path) =
        spawn_palyrad_with_dynamic_ports_and_journal_payload_limit(
            TRANSPORT_LIMIT_TEST_JOURNAL_MAX_PAYLOAD_BYTES,
        )?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
        .await
        .context("failed to connect gRPC client")?;

    let oversized_payload = vec![b'x'; GRPC_OVERSIZED_PAYLOAD_BYTES];
    let request = authorized_append_event_request(sample_journal_event(
        "01ARZ3NDEKTSV4RRFFQ69G5FBA",
        oversized_payload.as_slice(),
    ))?;
    let error = client
        .append_event(request)
        .await
        .expect_err("oversized append payload should be rejected by transport decode limits");
    assert_eq!(
        error.code(),
        Code::OutOfRange,
        "oversized append payload should be rejected as an out-of-range request"
    );
    assert!(
        error.message().contains("limit"),
        "oversized gRPC rejection should explain configured message limit"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_append_event_rejects_mismatched_embedded_event_version() -> Result<()> {
    let (child, admin_port, grpc_port, _journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
        .await
        .context("failed to connect gRPC client")?;

    let mut event = sample_journal_event("01ARZ3NDEKTSV4RRFFQ69G5FB8", br#"{"state":"invalid"}"#);
    event.v = 0;
    let request = authorized_append_event_request(event)?;
    let error =
        client.append_event(request).await.expect_err("mismatched event.v should be rejected");
    assert_eq!(error.code(), Code::FailedPrecondition, "event.v mismatch should fail precondition");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_run_stream_persists_orchestrator_snapshot_and_matches_golden_tape() -> Result<()> {
    let (child, admin_port, grpc_port, _journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
        .await
        .context("failed to connect gRPC client")?;

    let mut stream_request =
        tonic::Request::new(tokio_stream::iter(vec![sample_run_stream_request_with_text(
            "alpha beta gamma".to_owned(),
        )]));
    stream_request.metadata_mut().insert("authorization", format!("Bearer {ADMIN_TOKEN}").parse()?);
    stream_request.metadata_mut().insert("x-palyra-principal", "user:ops".parse()?);
    stream_request.metadata_mut().insert("x-palyra-device-id", DEVICE_ID.parse()?);
    stream_request.metadata_mut().insert("x-palyra-channel", "cli".parse()?);

    let mut response_stream =
        client.run_stream(stream_request).await.context("failed to call RunStream")?.into_inner();
    while let Some(event) = response_stream.next().await {
        let _event = event.context("failed to read RunStream event")?;
    }

    let run_snapshot = admin_get_json_async(admin_port, format!("/admin/v1/runs/{RUN_ID}")).await?;
    assert_eq!(
        run_snapshot.get("state").and_then(Value::as_str).context("run snapshot missing state")?,
        "done"
    );
    assert_eq!(
        run_snapshot
            .get("prompt_tokens")
            .and_then(Value::as_u64)
            .context("run snapshot missing prompt_tokens")?,
        3
    );
    assert_eq!(
        run_snapshot
            .get("completion_tokens")
            .and_then(Value::as_u64)
            .context("run snapshot missing completion_tokens")?,
        3
    );

    let expected_tape = load_golden_json("run_tape_basic.json")?;
    assert_eq!(
        run_snapshot
            .get("tape_events")
            .and_then(Value::as_u64)
            .context("run snapshot missing tape_events")?,
        expected_tape.as_array().context("golden tape must be a JSON array")?.len() as u64
    );
    assert!(
        run_snapshot.get("tape").is_none(),
        "run status endpoint should not include full tape payload"
    );

    let tape_snapshot =
        admin_get_json_async(admin_port, format!("/admin/v1/runs/{RUN_ID}/tape")).await?;
    assert_eq!(
        tape_snapshot.get("events").cloned().context("run tape snapshot missing events")?,
        expected_tape
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_run_stream_honors_cancel_command_and_marks_run_cancelled() -> Result<()> {
    let (child, admin_port, grpc_port, _journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
        .await
        .context("failed to connect gRPC client")?;

    let mut stream_request = tonic::Request::new(tokio_stream::iter(vec![
        sample_run_stream_request_with_text(
            "one two three four five six seven eight nine ten".to_owned(),
        ),
        sample_run_stream_request_with_text("/cancel".to_owned()),
    ]));
    stream_request.metadata_mut().insert("authorization", format!("Bearer {ADMIN_TOKEN}").parse()?);
    stream_request.metadata_mut().insert("x-palyra-principal", "user:ops".parse()?);
    stream_request.metadata_mut().insert("x-palyra-device-id", DEVICE_ID.parse()?);
    stream_request.metadata_mut().insert("x-palyra-channel", "cli".parse()?);

    let mut response_stream =
        client.run_stream(stream_request).await.context("failed to call RunStream")?.into_inner();
    let mut saw_failed = false;
    let mut saw_done = false;
    while let Some(event) = response_stream.next().await {
        let event = event.context("failed to read RunStream event")?;
        if let Some(common_v1::run_stream_event::Body::Status(status)) = event.body {
            if status.kind == common_v1::stream_status::StatusKind::Failed as i32 {
                saw_failed = true;
            }
            if status.kind == common_v1::stream_status::StatusKind::Done as i32 {
                saw_done = true;
            }
        }
    }
    assert!(saw_failed, "cancelled run should emit failed status");
    assert!(!saw_done, "cancelled run must not emit done status");

    let run_snapshot = admin_get_json_async(admin_port, format!("/admin/v1/runs/{RUN_ID}")).await?;
    assert_eq!(
        run_snapshot.get("state").and_then(Value::as_str).context("run snapshot missing state")?,
        "cancelled"
    );
    assert!(
        run_snapshot
            .get("cancel_requested")
            .and_then(Value::as_bool)
            .context("run snapshot missing cancel_requested")?,
        "cancelled run should persist cancel_requested=true"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_run_stream_rejects_session_identity_mismatch_as_failed_precondition() -> Result<()> {
    let (child, admin_port, grpc_port, _journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client =
        gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint.clone())
            .await
            .context("failed to connect gRPC client for initial stream")?;

    let mut first_stream =
        tonic::Request::new(tokio_stream::iter(vec![sample_run_stream_request_with_ids(
            SESSION_ID,
            RUN_ID,
            "seed-session".to_owned(),
        )]));
    first_stream.metadata_mut().insert("authorization", format!("Bearer {ADMIN_TOKEN}").parse()?);
    first_stream.metadata_mut().insert("x-palyra-principal", "user:ops".parse()?);
    first_stream.metadata_mut().insert("x-palyra-device-id", DEVICE_ID.parse()?);
    first_stream.metadata_mut().insert("x-palyra-channel", "cli".parse()?);
    let mut first_response = client
        .run_stream(first_stream)
        .await
        .context("failed to call first RunStream")?
        .into_inner();
    while let Some(event) = first_response.next().await {
        let _ = event.context("first run stream should finish without errors")?;
    }

    let mut second_client =
        gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
            .await
            .context("failed to connect gRPC client for mismatch stream")?;
    let mut second_stream =
        tonic::Request::new(tokio_stream::iter(vec![sample_run_stream_request_with_ids(
            SESSION_ID,
            RUN_ID_ALT,
            "mismatch-session".to_owned(),
        )]));
    second_stream.metadata_mut().insert("authorization", format!("Bearer {ADMIN_TOKEN}").parse()?);
    second_stream.metadata_mut().insert("x-palyra-principal", "user:other".parse()?);
    second_stream.metadata_mut().insert("x-palyra-device-id", DEVICE_ID.parse()?);
    second_stream.metadata_mut().insert("x-palyra-channel", "cli".parse()?);

    let mut second_response = second_client
        .run_stream(second_stream)
        .await
        .context("failed to call second RunStream")?
        .into_inner();
    let mismatch_error = second_response
        .next()
        .await
        .transpose()
        .expect_err("second run stream should fail before emitting any events");
    assert_eq!(mismatch_error.code(), Code::FailedPrecondition);
    assert!(
        mismatch_error.message().contains("session identity mismatch"),
        "expected session identity mismatch message, got: {}",
        mismatch_error.message()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_run_stream_protocol_error_after_accept_marks_run_failed() -> Result<()> {
    let (child, admin_port, grpc_port, _journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
        .await
        .context("failed to connect gRPC client")?;

    let mut invalid_protocol_message =
        sample_run_stream_request_with_ids(SESSION_ID, RUN_ID, "second-message".to_owned());
    invalid_protocol_message.v = 0;
    let mut stream_request = tonic::Request::new(tokio_stream::iter(vec![
        sample_run_stream_request_with_ids(SESSION_ID, RUN_ID, "first-message".to_owned()),
        invalid_protocol_message,
    ]));
    stream_request.metadata_mut().insert("authorization", format!("Bearer {ADMIN_TOKEN}").parse()?);
    stream_request.metadata_mut().insert("x-palyra-principal", "user:ops".parse()?);
    stream_request.metadata_mut().insert("x-palyra-device-id", DEVICE_ID.parse()?);
    stream_request.metadata_mut().insert("x-palyra-channel", "cli".parse()?);

    let mut response_stream =
        client.run_stream(stream_request).await.context("failed to call RunStream")?.into_inner();
    let mut terminal_error = None;
    while let Some(event) = response_stream.next().await {
        if let Err(status) = event {
            terminal_error = Some(status);
            break;
        }
    }
    let status = terminal_error.context("run stream should terminate with a protocol error")?;
    assert_eq!(status.code(), Code::FailedPrecondition);

    let run_snapshot = admin_get_json_async(admin_port, format!("/admin/v1/runs/{RUN_ID}")).await?;
    assert_eq!(
        run_snapshot.get("state").and_then(Value::as_str).context("run snapshot missing state")?,
        "failed"
    );
    assert!(
        run_snapshot
            .get("last_error")
            .and_then(Value::as_str)
            .context("run snapshot missing last_error")?
            .contains("unsupported protocol major version"),
        "failure reason should be persisted for protocol errors"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_run_stream_mid_stream_run_id_switch_marks_original_run_failed() -> Result<()> {
    let (child, admin_port, grpc_port, _journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
        .await
        .context("failed to connect gRPC client")?;

    let mut stream_request = tonic::Request::new(tokio_stream::iter(vec![
        sample_run_stream_request_with_ids(SESSION_ID, RUN_ID, "first-message".to_owned()),
        sample_run_stream_request_with_ids(SESSION_ID, RUN_ID_ALT, "switch-run".to_owned()),
    ]));
    stream_request.metadata_mut().insert("authorization", format!("Bearer {ADMIN_TOKEN}").parse()?);
    stream_request.metadata_mut().insert("x-palyra-principal", "user:ops".parse()?);
    stream_request.metadata_mut().insert("x-palyra-device-id", DEVICE_ID.parse()?);
    stream_request.metadata_mut().insert("x-palyra-channel", "cli".parse()?);

    let mut response_stream =
        client.run_stream(stream_request).await.context("failed to call RunStream")?.into_inner();
    let mut terminal_error = None;
    while let Some(event) = response_stream.next().await {
        if let Err(status) = event {
            terminal_error = Some(status);
            break;
        }
    }
    let status =
        terminal_error.context("run stream should terminate with a run_id mismatch error")?;
    assert_eq!(status.code(), Code::InvalidArgument);

    let run_snapshot = admin_get_json_async(admin_port, format!("/admin/v1/runs/{RUN_ID}")).await?;
    assert_eq!(
        run_snapshot.get("state").and_then(Value::as_str).context("run snapshot missing state")?,
        "failed"
    );
    assert!(
        run_snapshot
            .get("last_error")
            .and_then(Value::as_str)
            .context("run snapshot missing last_error")?
            .contains("cannot switch run_id mid-stream"),
        "failure reason should be persisted for run_id mismatch"
    );
    Ok(())
}

fn sample_run_stream_request() -> common_v1::RunStreamRequest {
    sample_run_stream_request_with_text("hello from grpc integration".to_owned())
}

fn sample_run_stream_request_with_text(text: String) -> common_v1::RunStreamRequest {
    sample_run_stream_request_with_ids(SESSION_ID, RUN_ID, text)
}

fn sample_run_stream_request_with_ids(
    session_id: &str,
    run_id: &str,
    text: String,
) -> common_v1::RunStreamRequest {
    common_v1::RunStreamRequest {
        v: 1,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.to_owned() }),
        run_id: Some(common_v1::CanonicalId { ulid: run_id.to_owned() }),
        input: Some(common_v1::MessageEnvelope {
            v: 1,
            envelope_id: Some(common_v1::CanonicalId { ulid: ENVELOPE_ID.to_owned() }),
            content: Some(common_v1::MessageContent { text, attachments: Vec::new() }),
            ..Default::default()
        }),
        allow_sensitive_tools: false,
        session_key: String::new(),
        session_label: String::new(),
        reset_session: false,
        require_existing: false,
        tool_approval_response: None,
    }
}

fn sample_tool_approval_response_request(
    proposal_id: &str,
    approved: bool,
    reason: &str,
) -> common_v1::RunStreamRequest {
    common_v1::RunStreamRequest {
        v: 1,
        session_id: Some(common_v1::CanonicalId { ulid: SESSION_ID.to_owned() }),
        run_id: Some(common_v1::CanonicalId { ulid: RUN_ID.to_owned() }),
        input: None,
        allow_sensitive_tools: false,
        session_key: String::new(),
        session_label: String::new(),
        reset_session: false,
        require_existing: false,
        tool_approval_response: Some(common_v1::ToolApprovalResponse {
            proposal_id: Some(common_v1::CanonicalId { ulid: proposal_id.to_owned() }),
            approved,
            reason: reason.to_owned(),
            approval_id: None,
            decision_scope: common_v1::ApprovalDecisionScope::Once as i32,
            decision_scope_ttl_ms: 0,
        }),
    }
}

fn admin_get_json(admin_port: u16, path: &str) -> Result<Value> {
    let endpoint = format!("http://127.0.0.1:{admin_port}{path}");
    Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to build admin HTTP client")?
        .get(endpoint)
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .header("x-palyra-principal", "user:ops")
        .header("x-palyra-device-id", DEVICE_ID)
        .header("x-palyra-channel", "cli")
        .send()
        .context("failed to call daemon admin endpoint")?
        .error_for_status()
        .context("daemon admin endpoint returned non-success status")?
        .json()
        .context("failed to parse daemon admin JSON response")
}

async fn admin_get_json_async(admin_port: u16, path: String) -> Result<Value> {
    tokio::task::spawn_blocking(move || admin_get_json(admin_port, path.as_str()))
        .await
        .context("admin JSON worker panicked")?
}

fn admin_post_json(admin_port: u16, path: &str, payload: Value) -> Result<Value> {
    let endpoint = format!("http://127.0.0.1:{admin_port}{path}");
    Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to build admin HTTP client")?
        .post(endpoint)
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .header("x-palyra-principal", "user:ops")
        .header("x-palyra-device-id", DEVICE_ID)
        .header("x-palyra-channel", "cli")
        .json(&payload)
        .send()
        .context("failed to call daemon admin endpoint")?
        .error_for_status()
        .context("daemon admin endpoint returned non-success status")?
        .json()
        .context("failed to parse daemon admin JSON response")
}

async fn admin_post_json_async(admin_port: u16, path: String, payload: Value) -> Result<Value> {
    tokio::task::spawn_blocking(move || admin_post_json(admin_port, path.as_str(), payload))
        .await
        .context("admin JSON worker panicked")?
}

fn load_golden_json(name: &str) -> Result<Value> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests").join("golden").join(name);
    let content =
        fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_str(content.as_str())
        .with_context(|| format!("failed to parse golden JSON {}", path.display()))
}

fn spawn_palyrad_with_dynamic_ports() -> Result<(Child, u16, u16, PathBuf)> {
    spawn_palyrad_with_dynamic_ports_options(false, None)
}

fn spawn_palyrad_with_dynamic_ports_and_hash_chain(
    hash_chain_enabled: bool,
) -> Result<(Child, u16, u16, PathBuf)> {
    spawn_palyrad_with_dynamic_ports_options(hash_chain_enabled, None)
}

fn spawn_palyrad_with_dynamic_ports_and_journal_payload_limit(
    max_journal_payload_bytes: usize,
) -> Result<(Child, u16, u16, PathBuf)> {
    spawn_palyrad_with_dynamic_ports_options(false, Some(max_journal_payload_bytes))
}

fn spawn_palyrad_with_dynamic_ports_options(
    hash_chain_enabled: bool,
    max_journal_payload_bytes: Option<usize>,
) -> Result<(Child, u16, u16, PathBuf)> {
    let journal_db_path = unique_temp_journal_db_path();
    let identity_store_dir = unique_temp_identity_store_dir();
    let vault_dir = unique_temp_vault_dir();
    prepare_test_vault_dir(&vault_dir)?;
    let mut command = Command::new(env!("CARGO_BIN_EXE_palyrad"));
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
        .env("PALYRA_ADMIN_TOKEN", ADMIN_TOKEN)
        .env("PALYRA_JOURNAL_DB_PATH", journal_db_path.to_string_lossy().to_string())
        .env("PALYRA_GATEWAY_IDENTITY_STORE_DIR", identity_store_dir.to_string_lossy().to_string())
        .env("PALYRA_VAULT_DIR", vault_dir.to_string_lossy().to_string())
        .env("PALYRA_JOURNAL_HASH_CHAIN_ENABLED", if hash_chain_enabled { "true" } else { "false" })
        .env("PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED", "true")
        .env("RUST_LOG", "info")
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    if let Some(max_journal_payload_bytes) = max_journal_payload_bytes {
        command.env("PALYRA_JOURNAL_MAX_PAYLOAD_BYTES", max_journal_payload_bytes.to_string());
    }
    let mut child = command.spawn().context("failed to start palyrad")?;
    let stdout = child.stdout.take().context("failed to capture palyrad stdout")?;
    let (admin_port, grpc_port) = wait_for_listen_ports(stdout, &mut child)?;
    Ok((child, admin_port, grpc_port, journal_db_path))
}

fn spawn_palyrad_with_existing_journal(journal_db_path: PathBuf) -> Result<(Child, u16, u16)> {
    let identity_store_dir = unique_temp_identity_store_dir();
    let vault_dir = unique_temp_vault_dir();
    prepare_test_vault_dir(&vault_dir)?;
    let mut child = Command::new(env!("CARGO_BIN_EXE_palyrad"))
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
        .env("PALYRA_ADMIN_TOKEN", ADMIN_TOKEN)
        .env("PALYRA_JOURNAL_DB_PATH", journal_db_path.to_string_lossy().to_string())
        .env("PALYRA_GATEWAY_IDENTITY_STORE_DIR", identity_store_dir.to_string_lossy().to_string())
        .env("PALYRA_VAULT_DIR", vault_dir.to_string_lossy().to_string())
        .env("PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED", "true")
        .env("RUST_LOG", "info")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .context("failed to restart palyrad with existing journal")?;
    let stdout = child.stdout.take().context("failed to capture restarted palyrad stdout")?;
    let (admin_port, grpc_port) = wait_for_listen_ports(stdout, &mut child)?;
    Ok((child, admin_port, grpc_port))
}

fn spawn_palyrad_with_openai_provider(
    openai_base_url: &str,
    openai_api_key: &str,
) -> Result<(Child, u16, u16, PathBuf)> {
    spawn_palyrad_with_openai_provider_and_tool_policy(openai_base_url, openai_api_key, "", 4, 750)
}

fn spawn_palyrad_with_openai_provider_and_tool_policy(
    openai_base_url: &str,
    openai_api_key: &str,
    allowed_tools: &str,
    max_calls_per_run: u32,
    execution_timeout_ms: u64,
) -> Result<(Child, u16, u16, PathBuf)> {
    let journal_db_path = unique_temp_journal_db_path();
    let identity_store_dir = unique_temp_identity_store_dir();
    let vault_dir = unique_temp_vault_dir();
    prepare_test_vault_dir(&vault_dir)?;
    let mut child = Command::new(env!("CARGO_BIN_EXE_palyrad"))
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
        .env("PALYRA_ADMIN_TOKEN", ADMIN_TOKEN)
        .env("PALYRA_JOURNAL_DB_PATH", journal_db_path.to_string_lossy().to_string())
        .env("PALYRA_GATEWAY_IDENTITY_STORE_DIR", identity_store_dir.to_string_lossy().to_string())
        .env("PALYRA_VAULT_DIR", vault_dir.to_string_lossy().to_string())
        .env("PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED", "true")
        .env("PALYRA_MODEL_PROVIDER_KIND", "openai_compatible")
        .env("PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL", openai_base_url)
        .env("PALYRA_MODEL_PROVIDER_OPENAI_API_KEY", openai_api_key)
        .env("PALYRA_MODEL_PROVIDER_MAX_RETRIES", "0")
        .env("PALYRA_MODEL_PROVIDER_RETRY_BACKOFF_MS", "1")
        .env("PALYRA_MODEL_PROVIDER_CIRCUIT_BREAKER_FAILURE_THRESHOLD", "1")
        .env("PALYRA_MODEL_PROVIDER_CIRCUIT_BREAKER_COOLDOWN_MS", "30000")
        .env("PALYRA_TOOL_CALL_ALLOWED_TOOLS", allowed_tools)
        .env("PALYRA_TOOL_CALL_MAX_CALLS_PER_RUN", max_calls_per_run.to_string())
        .env("PALYRA_TOOL_CALL_TIMEOUT_MS", execution_timeout_ms.to_string())
        .env("RUST_LOG", "info")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .context("failed to start palyrad with openai-compatible provider")?;
    let stdout = child.stdout.take().context("failed to capture palyrad stdout")?;
    let (admin_port, grpc_port) = wait_for_listen_ports(stdout, &mut child)?;
    Ok((child, admin_port, grpc_port, journal_db_path))
}

fn spawn_palyrad_with_openai_provider_tool_policy_and_memory_auto_inject(
    openai_base_url: &str,
    openai_api_key: &str,
    allowed_tools: &str,
    max_calls_per_run: u32,
    execution_timeout_ms: u64,
    auto_inject_max_items: u32,
) -> Result<(Child, u16, u16, PathBuf)> {
    let journal_db_path = unique_temp_journal_db_path();
    let identity_store_dir = unique_temp_identity_store_dir();
    let vault_dir = unique_temp_vault_dir();
    prepare_test_vault_dir(&vault_dir)?;
    let mut child = Command::new(env!("CARGO_BIN_EXE_palyrad"))
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
        .env("PALYRA_ADMIN_TOKEN", ADMIN_TOKEN)
        .env("PALYRA_JOURNAL_DB_PATH", journal_db_path.to_string_lossy().to_string())
        .env("PALYRA_GATEWAY_IDENTITY_STORE_DIR", identity_store_dir.to_string_lossy().to_string())
        .env("PALYRA_VAULT_DIR", vault_dir.to_string_lossy().to_string())
        .env("PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED", "true")
        .env("PALYRA_MODEL_PROVIDER_KIND", "openai_compatible")
        .env("PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL", openai_base_url)
        .env("PALYRA_MODEL_PROVIDER_OPENAI_API_KEY", openai_api_key)
        .env("PALYRA_MODEL_PROVIDER_MAX_RETRIES", "0")
        .env("PALYRA_MODEL_PROVIDER_RETRY_BACKOFF_MS", "1")
        .env("PALYRA_MODEL_PROVIDER_CIRCUIT_BREAKER_FAILURE_THRESHOLD", "1")
        .env("PALYRA_MODEL_PROVIDER_CIRCUIT_BREAKER_COOLDOWN_MS", "30000")
        .env("PALYRA_TOOL_CALL_ALLOWED_TOOLS", allowed_tools)
        .env("PALYRA_TOOL_CALL_MAX_CALLS_PER_RUN", max_calls_per_run.to_string())
        .env("PALYRA_TOOL_CALL_TIMEOUT_MS", execution_timeout_ms.to_string())
        .env("PALYRA_MEMORY_AUTO_INJECT_ENABLED", "true")
        .env("PALYRA_MEMORY_AUTO_INJECT_MAX_ITEMS", auto_inject_max_items.to_string())
        .env("RUST_LOG", "info")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .context("failed to start palyrad with memory auto-inject enabled")?;
    let stdout = child.stdout.take().context("failed to capture palyrad stdout")?;
    let (admin_port, grpc_port) = wait_for_listen_ports(stdout, &mut child)?;
    Ok((child, admin_port, grpc_port, journal_db_path))
}

#[cfg(unix)]
struct ProcessRunnerSpawnConfig<'a> {
    openai_base_url: &'a str,
    openai_api_key: &'a str,
    allowed_tools: &'a str,
    max_calls_per_run: u32,
    execution_timeout_ms: u64,
    workspace_root: &'a Path,
    allowed_executables: &'a str,
    allowed_egress_hosts: &'a str,
    allowed_dns_suffixes: &'a str,
}

#[cfg(unix)]
fn spawn_palyrad_with_openai_provider_tool_policy_and_process_runner(
    config: ProcessRunnerSpawnConfig<'_>,
) -> Result<(Child, u16, u16, PathBuf, PathBuf)> {
    let config_path = write_process_runner_config(
        config.workspace_root,
        config.allowed_executables,
        config.allowed_egress_hosts,
        config.allowed_dns_suffixes,
    )?;

    let journal_db_path = unique_temp_journal_db_path();
    let identity_store_dir = unique_temp_identity_store_dir();
    let vault_dir = unique_temp_vault_dir();
    prepare_test_vault_dir(&vault_dir)?;
    let mut child = Command::new(env!("CARGO_BIN_EXE_palyrad"))
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
        .env("PALYRA_CONFIG", config_path.to_string_lossy().to_string())
        .env("PALYRA_ADMIN_TOKEN", ADMIN_TOKEN)
        .env("PALYRA_JOURNAL_DB_PATH", journal_db_path.to_string_lossy().to_string())
        .env("PALYRA_GATEWAY_IDENTITY_STORE_DIR", identity_store_dir.to_string_lossy().to_string())
        .env("PALYRA_VAULT_DIR", vault_dir.to_string_lossy().to_string())
        .env("PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED", "true")
        .env("PALYRA_MODEL_PROVIDER_KIND", "openai_compatible")
        .env("PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL", config.openai_base_url)
        .env("PALYRA_MODEL_PROVIDER_OPENAI_API_KEY", config.openai_api_key)
        .env("PALYRA_MODEL_PROVIDER_MAX_RETRIES", "0")
        .env("PALYRA_MODEL_PROVIDER_RETRY_BACKOFF_MS", "1")
        .env("PALYRA_MODEL_PROVIDER_CIRCUIT_BREAKER_FAILURE_THRESHOLD", "1")
        .env("PALYRA_MODEL_PROVIDER_CIRCUIT_BREAKER_COOLDOWN_MS", "30000")
        .env("PALYRA_TOOL_CALL_ALLOWED_TOOLS", config.allowed_tools)
        .env("PALYRA_TOOL_CALL_MAX_CALLS_PER_RUN", config.max_calls_per_run.to_string())
        .env("PALYRA_TOOL_CALL_TIMEOUT_MS", config.execution_timeout_ms.to_string())
        .env("RUST_LOG", "info")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .context("failed to start palyrad with process runner policy")?;
    let stdout = child.stdout.take().context("failed to capture palyrad stdout")?;
    let (admin_port, grpc_port) = wait_for_listen_ports(stdout, &mut child)?;
    Ok((child, admin_port, grpc_port, journal_db_path, config_path))
}

struct WasmRuntimeSpawnConfig<'a> {
    openai_base_url: &'a str,
    openai_api_key: &'a str,
    allowed_tools: &'a str,
    max_calls_per_run: u32,
    execution_timeout_ms: u64,
    allowed_http_hosts: &'a str,
    allowed_secrets: &'a str,
    allowed_storage_prefixes: &'a str,
    allowed_channels: &'a str,
    fuel_budget: u64,
    max_memory_bytes: u64,
    max_table_elements: u64,
    max_instances: u64,
}

fn spawn_palyrad_with_openai_provider_tool_policy_and_wasm_runtime(
    config: WasmRuntimeSpawnConfig<'_>,
) -> Result<(Child, u16, u16, PathBuf, PathBuf)> {
    let config_path = write_wasm_runtime_config(&config)?;

    let journal_db_path = unique_temp_journal_db_path();
    let identity_store_dir = unique_temp_identity_store_dir();
    let vault_dir = unique_temp_vault_dir();
    prepare_test_vault_dir(&vault_dir)?;
    let mut child = Command::new(env!("CARGO_BIN_EXE_palyrad"))
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
        .env("PALYRA_CONFIG", config_path.to_string_lossy().to_string())
        .env("PALYRA_ADMIN_TOKEN", ADMIN_TOKEN)
        .env("PALYRA_JOURNAL_DB_PATH", journal_db_path.to_string_lossy().to_string())
        .env("PALYRA_GATEWAY_IDENTITY_STORE_DIR", identity_store_dir.to_string_lossy().to_string())
        .env("PALYRA_VAULT_DIR", vault_dir.to_string_lossy().to_string())
        .env("PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED", "true")
        .env("PALYRA_MODEL_PROVIDER_KIND", "openai_compatible")
        .env("PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL", config.openai_base_url)
        .env("PALYRA_MODEL_PROVIDER_OPENAI_API_KEY", config.openai_api_key)
        .env("PALYRA_MODEL_PROVIDER_MAX_RETRIES", "0")
        .env("PALYRA_MODEL_PROVIDER_RETRY_BACKOFF_MS", "1")
        .env("PALYRA_MODEL_PROVIDER_CIRCUIT_BREAKER_FAILURE_THRESHOLD", "1")
        .env("PALYRA_MODEL_PROVIDER_CIRCUIT_BREAKER_COOLDOWN_MS", "30000")
        .env("PALYRA_TOOL_CALL_ALLOWED_TOOLS", config.allowed_tools)
        .env("PALYRA_TOOL_CALL_MAX_CALLS_PER_RUN", config.max_calls_per_run.to_string())
        .env("PALYRA_TOOL_CALL_TIMEOUT_MS", config.execution_timeout_ms.to_string())
        .env("RUST_LOG", "info")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .context("failed to start palyrad with wasm runtime policy")?;
    let stdout = child.stdout.take().context("failed to capture palyrad stdout")?;
    let (admin_port, grpc_port) = wait_for_listen_ports(stdout, &mut child)?;
    Ok((child, admin_port, grpc_port, journal_db_path, config_path))
}

#[cfg(unix)]
fn write_process_runner_config(
    workspace_root: &Path,
    allowed_executables: &str,
    allowed_egress_hosts: &str,
    allowed_dns_suffixes: &str,
) -> Result<PathBuf> {
    let config_path = unique_temp_daemon_config_path();
    let config_body = format!(
        "\
[tool_call.process_runner]
enabled = true
workspace_root = {workspace_root}
allowed_executables = {allowed_executables}
allowed_egress_hosts = {allowed_egress_hosts}
allowed_dns_suffixes = {allowed_dns_suffixes}
cpu_time_limit_ms = 2000
memory_limit_bytes = 134217728
max_output_bytes = 65536
",
        workspace_root = toml_string(workspace_root.to_string_lossy().as_ref()),
        allowed_executables = toml_string_array(allowed_executables),
        allowed_egress_hosts = toml_string_array(allowed_egress_hosts),
        allowed_dns_suffixes = toml_string_array(allowed_dns_suffixes),
    );
    fs::write(&config_path, config_body).with_context(|| {
        format!("failed to write process runner test config at {}", config_path.display())
    })?;
    Ok(config_path)
}

fn write_wasm_runtime_config(config: &WasmRuntimeSpawnConfig<'_>) -> Result<PathBuf> {
    let config_path = unique_temp_daemon_config_path();
    let config_body = format!(
        "\
[tool_call.wasm_runtime]
enabled = true
max_module_size_bytes = 262144
fuel_budget = {fuel_budget}
max_memory_bytes = {max_memory_bytes}
max_table_elements = {max_table_elements}
max_instances = {max_instances}
allowed_http_hosts = {allowed_http_hosts}
allowed_secrets = {allowed_secrets}
allowed_storage_prefixes = {allowed_storage_prefixes}
allowed_channels = {allowed_channels}
",
        fuel_budget = config.fuel_budget,
        max_memory_bytes = config.max_memory_bytes,
        max_table_elements = config.max_table_elements,
        max_instances = config.max_instances,
        allowed_http_hosts = toml_string_array(config.allowed_http_hosts),
        allowed_secrets = toml_string_array(config.allowed_secrets),
        allowed_storage_prefixes = toml_string_array(config.allowed_storage_prefixes),
        allowed_channels = toml_string_array(config.allowed_channels),
    );
    fs::write(&config_path, config_body).with_context(|| {
        format!("failed to write wasm runtime test config at {}", config_path.display())
    })?;
    Ok(config_path)
}

fn unique_temp_daemon_config_path() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    let counter = TEMP_CONFIG_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!("palyra-gateway-config-{nonce}-{}-{counter}.toml", std::process::id()))
}

fn toml_string_array(raw: &str) -> String {
    let values = raw
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(toml_string)
        .collect::<Vec<_>>();
    format!("[{}]", values.join(", "))
}

fn toml_string(raw: &str) -> String {
    format!("\"{}\"", raw.replace('\\', "\\\\").replace('"', "\\\""))
}

#[derive(Debug, Clone)]
struct ScriptedOpenAiResponse {
    status_code: u16,
    body: String,
    delay_before_response: Duration,
}

impl ScriptedOpenAiResponse {
    fn immediate(status_code: u16, body: String) -> Self {
        Self { status_code, body, delay_before_response: Duration::ZERO }
    }

    fn delayed(status_code: u16, body: String, delay_before_response: Duration) -> Self {
        Self { status_code, body, delay_before_response }
    }
}

fn openai_tool_call_response(tool_name: &str, arguments: &Value) -> Result<String> {
    let arguments_json =
        serde_json::to_string(arguments).context("failed to serialize tool arguments string")?;
    Ok(serde_json::json!({
        "choices": [{
            "message": {
                "tool_calls": [{
                    "function": {
                        "name": tool_name,
                        "arguments": arguments_json
                    }
                }]
            }
        }]
    })
    .to_string())
}

fn spawn_scripted_openai_server(
    responses: Vec<ScriptedOpenAiResponse>,
) -> Result<(String, Arc<AtomicUsize>, thread::JoinHandle<()>)> {
    let listener =
        TcpListener::bind("127.0.0.1:0").context("failed to bind scripted openai listener")?;
    let address = listener.local_addr().context("failed to resolve scripted listener address")?;
    let request_count = Arc::new(AtomicUsize::new(0));
    let request_count_for_thread = Arc::clone(&request_count);
    let handle = thread::spawn(move || {
        for response_spec in responses {
            let (mut stream, _) =
                listener.accept().expect("scripted openai listener should accept connection");
            request_count_for_thread.fetch_add(1, Ordering::Relaxed);
            if read_http_request_for_scripted_server(&mut stream).is_err() {
                continue;
            }
            if !response_spec.delay_before_response.is_zero() {
                thread::sleep(response_spec.delay_before_response);
            }
            let reason = match response_spec.status_code {
                200 => "OK",
                429 => "Too Many Requests",
                500 => "Internal Server Error",
                502 => "Bad Gateway",
                503 => "Service Unavailable",
                504 => "Gateway Timeout",
                _ => "Error",
            };
            let response = format!(
                "HTTP/1.1 {} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_spec.status_code,
                response_spec.body.len(),
                response_spec.body
            );
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
        }
    });
    Ok((format!("http://{address}/v1"), request_count, handle))
}

fn read_http_request_for_scripted_server(stream: &mut TcpStream) -> Result<()> {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .context("failed to configure scripted server read timeout")?;
    let mut reader = BufReader::new(stream);
    let mut content_length = 0_usize;
    loop {
        let mut line = String::new();
        let bytes_read =
            reader.read_line(&mut line).context("failed to read scripted request line")?;
        if bytes_read == 0 || line == "\r\n" {
            break;
        }
        let line_trimmed = line.trim_end_matches(['\r', '\n']);
        if let Some(value) = line_trimmed.strip_prefix("Content-Length:") {
            content_length =
                value.trim().parse::<usize>().context("invalid Content-Length in request")?;
        }
    }

    if content_length > 0 {
        let mut body = vec![0_u8; content_length];
        reader.read_exact(&mut body).context("failed to read scripted request body")?;
        if body.is_empty() {
            anyhow::bail!("scripted openai request body should not be empty");
        }
    }

    Ok(())
}

fn unique_temp_journal_db_path() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    let counter = TEMP_JOURNAL_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!("palyra-gateway-grpc-{nonce}-{}-{counter}.sqlite3", std::process::id()))
}

fn unique_temp_identity_store_dir() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    let counter = TEMP_IDENTITY_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!("palyra-gateway-identity-{nonce}-{}-{counter}", std::process::id()))
}

fn unique_temp_vault_dir() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    let counter = TEMP_VAULT_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!("palyra-gateway-vault-{nonce}-{}-{counter}", std::process::id()))
}

fn prepare_test_vault_dir(vault_dir: &PathBuf) -> Result<()> {
    fs::create_dir_all(vault_dir)
        .with_context(|| format!("failed to create test vault dir {}", vault_dir.display()))?;
    let backend_marker = vault_dir.join("backend.kind");
    fs::write(&backend_marker, b"encrypted_file").with_context(|| {
        format!("failed to write vault backend marker {}", backend_marker.display())
    })?;
    Ok(())
}

fn sample_journal_event(event_id: &str, payload_json: &[u8]) -> common_v1::JournalEvent {
    common_v1::JournalEvent {
        v: 1,
        event_id: Some(common_v1::CanonicalId { ulid: event_id.to_owned() }),
        session_id: Some(common_v1::CanonicalId { ulid: SESSION_ID.to_owned() }),
        run_id: Some(common_v1::CanonicalId { ulid: RUN_ID.to_owned() }),
        kind: common_v1::journal_event::EventKind::ToolExecuted as i32,
        actor: common_v1::journal_event::EventActor::User as i32,
        timestamp_unix_ms: 1_730_000_000_000,
        payload_json: payload_json.to_vec(),
        hash: String::new(),
        prev_hash: String::new(),
    }
}

fn authorize_metadata(metadata: &mut tonic::metadata::MetadataMap) -> Result<()> {
    authorize_metadata_with_principal(metadata, "user:ops")
}

fn authorize_metadata_with_principal(
    metadata: &mut tonic::metadata::MetadataMap,
    principal: &str,
) -> Result<()> {
    authorize_metadata_with_principal_and_channel(metadata, principal, "cli")
}

fn authorize_metadata_with_principal_and_channel(
    metadata: &mut tonic::metadata::MetadataMap,
    principal: &str,
    channel: &str,
) -> Result<()> {
    metadata.insert("authorization", format!("Bearer {ADMIN_TOKEN}").parse()?);
    metadata.insert("x-palyra-principal", principal.parse()?);
    metadata.insert("x-palyra-device-id", DEVICE_ID.parse()?);
    metadata.insert("x-palyra-channel", channel.parse()?);
    Ok(())
}

fn authorized_append_event_request(
    event: common_v1::JournalEvent,
) -> Result<tonic::Request<gateway_v1::AppendEventRequest>> {
    let mut request =
        tonic::Request::new(gateway_v1::AppendEventRequest { v: 1, event: Some(event) });
    request.metadata_mut().insert("authorization", format!("Bearer {ADMIN_TOKEN}").parse()?);
    request.metadata_mut().insert("x-palyra-principal", "user:ops".parse()?);
    request.metadata_mut().insert("x-palyra-device-id", DEVICE_ID.parse()?);
    request.metadata_mut().insert("x-palyra-channel", "cli".parse()?);
    Ok(request)
}

fn wait_for_listen_ports(stdout: ChildStdout, daemon: &mut Child) -> Result<(u16, u16)> {
    let (sender, receiver) = mpsc::channel::<Result<(u16, u16), String>>();
    thread::spawn(move || {
        let mut sender = Some(sender);
        let mut admin_port = None::<u16>;
        let mut grpc_port = None::<u16>;
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else {
                if let Some(sender) = sender.take() {
                    let _ = sender.send(Err("failed to read palyrad stdout line".to_owned()));
                }
                return;
            };

            if admin_port.is_none() {
                admin_port = parse_port_from_log(&line, "\"listen_addr\":\"");
            }
            if grpc_port.is_none() {
                grpc_port = parse_port_from_log(&line, "\"grpc_listen_addr\":\"");
            }

            if let (Some(admin_port), Some(grpc_port)) = (admin_port, grpc_port) {
                if let Some(sender) = sender.take() {
                    let _ = sender.send(Ok((admin_port, grpc_port)));
                }
                return;
            }
        }

        if let Some(sender) = sender.take() {
            let _ = sender.send(Err(
                "palyrad stdout closed before admin and gRPC listen addresses were published"
                    .to_owned(),
            ));
        }
    });

    let timeout_at = Instant::now() + Duration::from_secs(10);
    loop {
        match receiver.recv_timeout(Duration::from_millis(100)) {
            Ok(Ok(ports)) => return Ok(ports),
            Ok(Err(message)) => anyhow::bail!("{message}"),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                anyhow::bail!("listen-address reader disconnected before publishing ports");
            }
        }

        if Instant::now() > timeout_at {
            anyhow::bail!("timed out waiting for palyrad listen address logs");
        }
        if let Some(status) = daemon.try_wait().context("failed to check palyrad status")? {
            anyhow::bail!(
                "palyrad exited before publishing listen addresses with status: {status}"
            );
        }
    }
}

fn parse_port_from_log(line: &str, prefix: &str) -> Option<u16> {
    let start = line.find(prefix)? + prefix.len();
    let rest = &line[start..];
    let end = rest.find('"')?;
    rest[..end].parse::<SocketAddr>().ok().map(|address| address.port())
}

fn wait_for_health(port: u16, daemon: &mut Child) -> Result<()> {
    let timeout_at = Instant::now() + Duration::from_secs(10);
    let request = b"GET /healthz HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n";

    loop {
        if Instant::now() > timeout_at {
            anyhow::bail!("timed out waiting for palyrad health endpoint");
        }
        if let Some(status) = daemon.try_wait().context("failed to check palyrad status")? {
            anyhow::bail!("palyrad exited before becoming healthy with status: {status}");
        }
        if let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)) {
            let _ = stream.set_write_timeout(Some(Duration::from_millis(300)));
            let _ = stream.set_read_timeout(Some(Duration::from_millis(300)));
            if stream.write_all(request).is_ok() {
                let mut response = String::new();
                if stream.read_to_string(&mut response).is_ok()
                    && response.starts_with("HTTP/1.1 200")
                {
                    return Ok(());
                }
            }
        }
        thread::sleep(Duration::from_millis(100));
    }
}

struct ChildGuard {
    child: Child,
}

impl ChildGuard {
    fn new(child: Child) -> Self {
        Self { child }
    }

    fn child_mut(&mut self) -> &mut Child {
        &mut self.child
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

struct TempFileGuard {
    path: PathBuf,
}

impl TempFileGuard {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}
