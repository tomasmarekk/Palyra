use super::*;

#[test]
fn fault_metadata_projection_covers_checkpoint_and_barrier_release() {
    let (sandbox, _) = test_sandbox();
    let checkpoint = QaFaultEvidenceSidecarRecord::CheckpointObserved(
        palyra_common::qa_fault_injection::QaFaultCheckpointObservedRecord {
            schema_version: 1,
            sequence: 2,
            launch_id: "launch".to_owned(),
            plan_sha256: "a".repeat(64),
            point_id: "tool.before_effect".to_owned(),
            actor: "worker-a".to_owned(),
            occurrence: 2,
        },
    );
    let released = QaFaultEvidenceSidecarRecord::BarrierReleased(
        palyra_common::qa_fault_injection::QaFaultBarrierReleasedRecord {
            schema_version: 1,
            sequence: 4,
            launch_id: "launch".to_owned(),
            plan_sha256: "a".repeat(64),
            activation_id: "barrier".to_owned(),
            point_id: "tool.before_effect".to_owned(),
            actor: "worker-b".to_owned(),
            release_position: 2,
        },
    );

    let checkpoint = project_failure_fault_record(&sandbox, &checkpoint);
    assert_eq!(checkpoint.record_type, "checkpoint_observed");
    assert_eq!(checkpoint.occurrence, Some(2));
    let released = project_failure_fault_record(&sandbox, &released);
    assert_eq!(released.record_type, "barrier_released");
    assert_eq!(released.release_position, Some(2));
}

#[test]
fn no_tool_environment_uses_an_empty_allowlist() {
    let manifest = parse_scenario(NO_TOOLS_SCENARIO);
    let root = tempfile::tempdir().expect("environment root should exist");
    let mut command = Command::new("palyrad");
    let provider = QaDaemonProviderEnvironment::Deterministic {
        provider_fixture: root.path().join("provider.yaml"),
    };
    let allowed_tools = manifest.requires.tools.join(",");
    configure_isolated_environment(
        &mut command,
        QaDaemonEnvironment {
            allowed_tools: allowed_tools.as_str(),
            policy_profile: "qa_no_tools",
            state_root: root.path(),
            identity_root: &root.path().join("identity"),
            config_path: &root.path().join("palyra.toml"),
            vault_dir: &root.path().join("vault"),
            provider: &provider,
            execution_key_digest: &"a".repeat(64),
            provider_binding_sha256: &"b".repeat(64),
            admin_token: "test-token",
            principal: "admin:test",
            fault_launch: None,
        },
    );

    let allowed_tools = command
        .get_envs()
        .find(|(key, _)| *key == std::ffi::OsStr::new("PALYRA_TOOL_CALL_ALLOWED_TOOLS"))
        .and_then(|(_, value)| value)
        .expect("tool allowlist environment should be configured");
    assert!(allowed_tools.is_empty());
    assert_eq!(
        command_env(&command, "PALYRA_QA_EXECUTION_KEY_DIGEST"),
        Some(OsStr::new(&"a".repeat(64)))
    );
    assert_eq!(
        command_env(&command, "PALYRA_QA_PROVIDER_BINDING_SHA256"),
        Some(OsStr::new(&"b".repeat(64)))
    );
}

#[test]
fn explicit_shadow_profile_binds_closed_runtime_environment_to_qa_execution() {
    let root = tempfile::tempdir().expect("environment root should exist");
    let mut command = Command::new("palyrad");
    let provider = QaDaemonProviderEnvironment::Deterministic {
        provider_fixture: root.path().join("provider.yaml"),
    };
    let execution_digest = "a".repeat(64);
    configure_isolated_environment(
        &mut command,
        QaDaemonEnvironment {
            allowed_tools: "",
            policy_profile: "runtime_kernel_v2_shadow_explicit",
            state_root: root.path(),
            identity_root: &root.path().join("identity"),
            config_path: &root.path().join("palyra.toml"),
            vault_dir: &root.path().join("vault"),
            provider: &provider,
            execution_key_digest: execution_digest.as_str(),
            provider_binding_sha256: &"b".repeat(64),
            admin_token: "test-token",
            principal: "admin:test",
            fault_launch: None,
        },
    );

    assert_eq!(
        command_env(&command, "PALYRA_RUNTIME_KERNEL_PROFILE"),
        Some(OsStr::new("v2_shadow"))
    );
    assert_eq!(
        command_env(&command, "PALYRA_QA_RUNTIME_KERNEL_SHADOW_EXPLICIT_BINDING"),
        Some(OsStr::new(execution_digest.as_str()))
    );
    assert_eq!(
        command_env(&command, "PALYRA_RUNTIME_KERNEL_EXISTING_SESSION_POLICY"),
        Some(OsStr::new("migrate_at_safe_boundary"))
    );
    assert_eq!(command_env(&command, "PALYRA_TOOL_CALL_ALLOWED_TOOLS"), Some(OsStr::new("")));
}

#[test]
fn authoritative_v2_profile_binds_closed_runtime_environment_to_qa_execution() {
    let root = tempfile::tempdir().expect("environment root should exist");
    let mut command = Command::new("palyrad");
    let provider = QaDaemonProviderEnvironment::Deterministic {
        provider_fixture: root.path().join("provider.yaml"),
    };
    let execution_digest = "a".repeat(64);
    configure_isolated_environment(
        &mut command,
        QaDaemonEnvironment {
            allowed_tools: "",
            policy_profile: "runtime_kernel_v2_authoritative_cancel",
            state_root: root.path(),
            identity_root: &root.path().join("identity"),
            config_path: &root.path().join("palyra.toml"),
            vault_dir: &root.path().join("vault"),
            provider: &provider,
            execution_key_digest: execution_digest.as_str(),
            provider_binding_sha256: &"b".repeat(64),
            admin_token: "test-token",
            principal: "admin:test",
            fault_launch: None,
        },
    );

    assert_eq!(command_env(&command, "PALYRA_RUNTIME_KERNEL_PROFILE"), Some(OsStr::new("v2")));
    assert_eq!(
        command_env(&command, "PALYRA_RUNTIME_KERNEL_CANARY_BASIS_POINTS"),
        Some(OsStr::new("0"))
    );
    assert_eq!(
        command_env(&command, "PALYRA_RUNTIME_KERNEL_SHADOW_SAMPLE_BASIS_POINTS"),
        Some(OsStr::new("0"))
    );
    assert_eq!(
        command_env(&command, "PALYRA_RUNTIME_KERNEL_SAMPLING_KEY_HEX"),
        Some(OsStr::new(execution_digest.as_str()))
    );
    assert_eq!(
        command_env(&command, "PALYRA_RUNTIME_KERNEL_EXISTING_SESSION_POLICY"),
        Some(OsStr::new("migrate_at_safe_boundary"))
    );
    assert_eq!(
        command_env(&command, "PALYRA_RUNTIME_KERNEL_ROLLBACK_POLICY"),
        Some(OsStr::new("finish_read_only_suspend_mutating"))
    );
}

#[test]
fn projected_live_secret_reopens_with_the_child_identity_root() {
    const SECRET: &[u8] = b"qa-live-projection-secret";

    let source_root = tempfile::tempdir().expect("source vault root should exist");
    let source_vault = Vault::open_with_config(VaultConfig {
        root: Some(source_root.path().join("vault")),
        identity_store_root: Some(source_root.path().join("identity")),
        backend_preference: BackendPreference::EncryptedFile,
        ..VaultConfig::default()
    })
    .expect("source vault should open without host state");
    source_vault
        .put_secret(&VaultScope::Global, "source_api_key", SECRET)
        .expect("source secret should be stored");

    let child_root = tempfile::tempdir().expect("child state root should exist");
    let child_identity_root = child_root.path().join("identity");
    let child_vault_dir = child_root.path().join("vault");
    let projected_vault =
        open_isolated_live_vault(child_vault_dir.as_path(), child_identity_root.as_path())
            .expect("projected vault should bootstrap the child identity");
    let mut sentinels = Vec::new();
    let projected_reference = copy_live_secret(
        &source_vault,
        &projected_vault,
        &VaultScope::Global,
        "global/source_api_key",
        "api_key",
        &mut sentinels,
    )
    .expect("live secret should project into the child vault");
    drop(projected_vault);

    let child_vault = Vault::open_with_config(VaultConfig {
        root: Some(child_vault_dir),
        identity_store_root: Some(child_identity_root),
        ..VaultConfig::default()
    })
    .expect("child runtime should reopen its projected vault");
    let projected_reference =
        VaultRef::parse(projected_reference.as_str()).expect("projected reference should parse");
    let resolved = child_vault
        .get_secret(&projected_reference.scope, projected_reference.key.as_str())
        .expect("child runtime should decrypt the projected secret");

    assert_eq!(resolved, SECRET);
    assert_eq!(sentinels.len(), 1);
    assert_eq!(sentinels[0].as_slice(), SECRET);
}

#[test]
fn openai_live_transport_sets_only_openai_model_and_endpoint_variables() {
    let mut command = Command::new("palyrad");
    configure_live_transport_environment(
        &mut command,
        &QaLiveTransportEnvironment::OpenAiCompatible {
            model: "gpt-test".to_owned(),
            base_url: Some("https://api.openai.example/v1".to_owned()),
        },
    );

    assert_eq!(
        command_env(&command, "PALYRA_MODEL_PROVIDER_KIND"),
        Some(OsStr::new("openai_compatible"))
    );
    assert_eq!(
        command_env(&command, "PALYRA_MODEL_PROVIDER_OPENAI_MODEL"),
        Some(OsStr::new("gpt-test"))
    );
    assert_eq!(
        command_env(&command, "PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL"),
        Some(OsStr::new("https://api.openai.example/v1"))
    );
    assert_eq!(command_env(&command, "PALYRA_MODEL_PROVIDER_ANTHROPIC_MODEL"), None);
    assert_eq!(command_env(&command, "PALYRA_MODEL_PROVIDER_ANTHROPIC_BASE_URL"), None);
}

#[test]
fn anthropic_live_transport_sets_only_anthropic_model_and_endpoint_variables() {
    let mut command = Command::new("palyrad");
    configure_live_transport_environment(
        &mut command,
        &QaLiveTransportEnvironment::Anthropic {
            model: "claude-test".to_owned(),
            base_url: Some("https://api.anthropic.example".to_owned()),
        },
    );

    assert_eq!(command_env(&command, "PALYRA_MODEL_PROVIDER_KIND"), Some(OsStr::new("anthropic")));
    assert_eq!(
        command_env(&command, "PALYRA_MODEL_PROVIDER_ANTHROPIC_MODEL"),
        Some(OsStr::new("claude-test"))
    );
    assert_eq!(
        command_env(&command, "PALYRA_MODEL_PROVIDER_ANTHROPIC_BASE_URL"),
        Some(OsStr::new("https://api.anthropic.example"))
    );
    assert_eq!(command_env(&command, "PALYRA_MODEL_PROVIDER_OPENAI_MODEL"), None);
    assert_eq!(command_env(&command, "PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL"), None);
}

#[tokio::test]
async fn session_cleanup_requires_a_positive_cleaned_response() {
    let rejected = cleanup_session_with_timeout(
        async { Ok(gateway_v1::CleanupSessionResponse { cleaned: false, ..Default::default() }) },
        Duration::from_millis(100),
    )
    .await;
    assert!(!rejected);

    let accepted = cleanup_session_with_timeout(
        async { Ok(gateway_v1::CleanupSessionResponse { cleaned: true, ..Default::default() }) },
        Duration::from_millis(100),
    )
    .await;
    assert!(accepted);
}

#[tokio::test]
async fn session_cleanup_timeout_covers_the_whole_operation() {
    let started = Instant::now();
    let cleaned = cleanup_session_with_timeout(
        std::future::pending::<Result<gateway_v1::CleanupSessionResponse>>(),
        Duration::from_millis(20),
    )
    .await;

    assert!(!cleaned);
    assert!(started.elapsed() < Duration::from_millis(500));
}

#[test]
fn workspace_copy_is_bounded_and_preserves_regular_files() {
    let source = tempfile::tempdir().expect("source tempdir should exist");
    let destination = tempfile::tempdir().expect("destination tempdir should exist");
    fs::create_dir_all(source.path().join("src")).expect("fixture directory should exist");
    fs::write(source.path().join("src/app.txt"), "fixture").expect("fixture file should exist");

    copy_workspace_fixture(source.path(), destination.path()).expect("regular fixture should copy");

    assert_eq!(
        fs::read_to_string(destination.path().join("src/app.txt"))
            .expect("copied file should be readable"),
        "fixture"
    );
}

#[test]
fn fixture_snapshot_is_immutable_and_rejects_pre_copy_changes() {
    let repository = tempfile::tempdir().expect("repository root should exist");
    let repository_root =
        fs::canonicalize(repository.path()).expect("repository root should canonicalize");
    let fixture_path = repository_root.join("fixtures/provider.yaml");
    fs::create_dir_all(fixture_path.parent().expect("fixture should have a parent"))
        .expect("fixture directory should exist");
    fs::write(fixture_path.as_path(), b"original").expect("original fixture should be written");
    let paths = vec!["fixtures/provider.yaml".to_owned()];
    let expected = super::super::super::digest_repository_fixture_set(
        repository_root.as_path(),
        paths.iter().map(String::as_str),
    )
    .expect("fixture set should hash");
    let snapshot_root = repository_root.join("snapshot-a");
    let snapshot = materialize_fixture_snapshot(
        repository_root.as_path(),
        paths.as_slice(),
        expected.as_str(),
        snapshot_root.as_path(),
    )
    .expect("fixture snapshot should materialize");

    fs::write(fixture_path.as_path(), b"changed-after-snapshot")
        .expect("origin fixture should change");
    assert_eq!(
        fs::read(snapshot.path("fixtures/provider.yaml").expect("snapshot path should exist"))
            .expect("snapshot should remain readable"),
        b"original"
    );

    let stale_digest = super::super::super::digest_repository_fixture_set(
        repository_root.as_path(),
        paths.iter().map(String::as_str),
    )
    .expect("changed fixture set should hash");
    fs::write(fixture_path.as_path(), b"changed-before-copy")
        .expect("origin fixture should change again");
    let error = materialize_fixture_snapshot(
        repository_root.as_path(),
        paths.as_slice(),
        stale_digest.as_str(),
        repository_root.join("snapshot-b").as_path(),
    )
    .expect_err("changed input must not run under a stale execution key");
    assert!(error.to_string().contains("qa.runner.fixture_changed"));
}
