use super::*;

impl Drop for QaDaemonSandbox {
    fn drop(&mut self) {
        for _ in 0..DROP_CLEANUP_ATTEMPTS {
            let _ = self.shutdown_inner();
            if self.child.is_none()
                && self.log_threads.is_empty()
                && lock_unpoisoned(&self.state_root).is_removed()
            {
                return;
            }
        }

        let Some(admission) = self.cleanup_admission.take() else {
            return;
        };
        {
            let mut state_root = lock_unpoisoned(&self.state_root);
            if state_root.startup_cleanup_delegated {
                return;
            }
            state_root.startup_cleanup_delegated = true;
        }
        let ownership = StartupCleanupOwnership {
            process: self.child.take(),
            log_threads: std::mem::take(&mut self.log_threads),
            log_join_failed: self.log_drain_join_failed,
            state_root: Some(Arc::clone(&self.state_root)),
        };
        self.log_drain_join_failed = false;
        let _ = register_startup_cleanup(&admission, ownership);
    }
}

pub(super) struct SecretBytes(Vec<u8>);

impl SecretBytes {
    pub(super) fn new(bytes: Vec<u8>) -> Result<Self> {
        if bytes.len() < MIN_SECRET_SENTINEL_BYTES {
            anyhow::bail!("qa.runner.secret_sentinel_too_short");
        }
        Ok(Self(bytes))
    }

    pub(super) fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl Drop for SecretBytes {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

pub(super) struct QaPreparedProviderEnvironment {
    pub(super) provider: QaDaemonProviderEnvironment,
    pub(super) live_secret_sentinels: Vec<SecretBytes>,
}

pub(super) enum QaDaemonProviderEnvironment {
    Deterministic {
        provider_fixture: PathBuf,
    },
    Live {
        registry_path: PathBuf,
        auth_provider_kind: String,
        transport: QaLiveTransportEnvironment,
    },
}

pub(super) enum QaLiveTransportEnvironment {
    OpenAiCompatible { model: String, base_url: Option<String> },
    Anthropic { model: String, base_url: Option<String> },
}

#[derive(Serialize)]
pub(super) struct QaAuthRegistryDocument<'a> {
    pub(super) version: u32,
    profiles: &'a [AuthProfileRecord],
}

#[derive(Debug)]
pub(super) struct QaMaterializedFixtureSnapshot {
    pub(super) paths: BTreeMap<String, PathBuf>,
}

impl QaMaterializedFixtureSnapshot {
    pub(super) fn path(&self, relative: &str) -> Result<&PathBuf> {
        self.paths
            .get(relative)
            .ok_or_else(|| anyhow::anyhow!("qa.runner.fixture_snapshot_missing"))
    }
}

pub(super) fn materialize_fixture_snapshot(
    repository_root: &Path,
    fixture_paths: &[String],
    expected_digest: &str,
    snapshot_root: &Path,
) -> Result<QaMaterializedFixtureSnapshot> {
    fs::create_dir_all(snapshot_root).context("qa.runner.fixture_snapshot_create_failed")?;
    let mut budget = WorkspaceCopyBudget::default();
    let mut paths = BTreeMap::new();
    let mut digest_entries = Vec::with_capacity(fixture_paths.len());
    for (index, relative) in fixture_paths.iter().enumerate() {
        if paths.contains_key(relative) {
            anyhow::bail!("qa.runner.fixture_snapshot_duplicate");
        }
        let source = resolve_runner_path(repository_root, relative, "declared fixture")?;
        let destination = snapshot_root.join(format!("{index:04}"));
        copy_fixture_input(source.as_path(), destination.as_path(), &mut budget, 0)?;
        digest_entries.push((relative.clone(), destination.clone()));
        paths.insert(relative.clone(), destination);
    }
    let actual_digest = digest_materialized_fixture_set(digest_entries.as_slice())?;
    if actual_digest != expected_digest {
        anyhow::bail!("qa.runner.fixture_changed");
    }
    Ok(QaMaterializedFixtureSnapshot { paths })
}

fn copy_fixture_input(
    source: &Path,
    destination: &Path,
    budget: &mut WorkspaceCopyBudget,
    depth: usize,
) -> Result<()> {
    if depth > MAX_WORKSPACE_DEPTH {
        anyhow::bail!("qa.runner.fixture_snapshot_too_deep");
    }
    let metadata =
        fs::symlink_metadata(source).context("qa.runner.fixture_snapshot_metadata_failed")?;
    if metadata.file_type().is_symlink() {
        anyhow::bail!("qa.runner.fixture_symlink_denied");
    }
    budget.entries = budget
        .entries
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("qa.runner.fixture_snapshot_too_many_entries"))?;
    if budget.entries > MAX_WORKSPACE_ENTRIES {
        anyhow::bail!("qa.runner.fixture_snapshot_too_many_entries");
    }
    if metadata.is_file() {
        budget.bytes = budget
            .bytes
            .checked_add(metadata.len())
            .ok_or_else(|| anyhow::anyhow!("qa.runner.fixture_snapshot_too_large"))?;
        if budget.bytes > MAX_WORKSPACE_BYTES {
            anyhow::bail!("qa.runner.fixture_snapshot_too_large");
        }
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).context("qa.runner.fixture_snapshot_create_failed")?;
        }
        fs::copy(source, destination).context("qa.runner.fixture_snapshot_copy_failed")?;
        return Ok(());
    }
    if !metadata.is_dir() {
        anyhow::bail!("qa.runner.fixture_special_file_denied");
    }
    fs::create_dir_all(destination).context("qa.runner.fixture_snapshot_create_failed")?;
    let mut children = fs::read_dir(source)
        .context("qa.runner.fixture_snapshot_read_failed")?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<_>>>()
        .context("qa.runner.fixture_snapshot_read_failed")?;
    children.sort_by_key(|path| path.file_name().map(std::ffi::OsStr::to_os_string));
    for child in children {
        let name = child
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("qa.runner.fixture_snapshot_path_invalid"))?;
        copy_fixture_input(child.as_path(), destination.join(name).as_path(), budget, depth + 1)?;
    }
    Ok(())
}

pub(super) fn prepare_provider_environment(
    prepared: &QaPreparedScenarioExecution,
    fixture_snapshot: &QaMaterializedFixtureSnapshot,
    state_root: &Path,
    identity_root: &Path,
    vault_dir: &Path,
) -> Result<QaPreparedProviderEnvironment> {
    match &prepared.binding {
        QaPreparedRunnerBinding::Fixture { provider_fixture } => {
            Ok(QaPreparedProviderEnvironment {
                provider: QaDaemonProviderEnvironment::Deterministic {
                    provider_fixture: fixture_snapshot.path(provider_fixture)?.to_path_buf(),
                },
                live_secret_sentinels: Vec::new(),
            })
        }
        QaPreparedRunnerBinding::RecordReplay { replay_fixture } => {
            Ok(QaPreparedProviderEnvironment {
                provider: QaDaemonProviderEnvironment::Deterministic {
                    provider_fixture: fixture_snapshot.path(replay_fixture)?.to_path_buf(),
                },
                live_secret_sentinels: Vec::new(),
            })
        }
        QaPreparedRunnerBinding::Live(binding) => {
            prepare_live_provider_environment(binding, state_root, identity_root, vault_dir)
        }
    }
}

fn prepare_live_provider_environment(
    binding: &QaPreparedLiveBinding,
    state_root: &Path,
    identity_root: &Path,
    vault_dir: &Path,
) -> Result<QaPreparedProviderEnvironment> {
    let source_vault = Vault::open_default().context("qa.runner.live_source_vault_unavailable")?;
    let scoped_vault = open_isolated_live_vault(vault_dir, identity_root)?;
    let scope = VaultScope::Global;
    let mut sentinels = Vec::new();
    let credential = match &binding.profile.credential {
        AuthCredential::ApiKey { api_key_vault_ref } => AuthCredential::ApiKey {
            api_key_vault_ref: copy_live_secret(
                &source_vault,
                &scoped_vault,
                &scope,
                api_key_vault_ref,
                "api_key",
                &mut sentinels,
            )?,
        },
        AuthCredential::Oauth {
            access_token_vault_ref,
            refresh_token_vault_ref,
            token_endpoint,
            client_id,
            client_secret_vault_ref,
            scopes,
            expires_at_unix_ms,
            refresh_state,
        } => AuthCredential::Oauth {
            access_token_vault_ref: copy_live_secret(
                &source_vault,
                &scoped_vault,
                &scope,
                access_token_vault_ref,
                "access_token",
                &mut sentinels,
            )?,
            refresh_token_vault_ref: copy_live_secret(
                &source_vault,
                &scoped_vault,
                &scope,
                refresh_token_vault_ref,
                "refresh_token",
                &mut sentinels,
            )?,
            token_endpoint: token_endpoint.clone(),
            client_id: client_id.clone(),
            client_secret_vault_ref: client_secret_vault_ref
                .as_deref()
                .map(|secret_ref| {
                    copy_live_secret(
                        &source_vault,
                        &scoped_vault,
                        &scope,
                        secret_ref,
                        "client_secret",
                        &mut sentinels,
                    )
                })
                .transpose()?,
            scopes: scopes.clone(),
            expires_at_unix_ms: *expires_at_unix_ms,
            refresh_state: refresh_state.clone(),
        },
    };
    let projected_profile = AuthProfileRecord {
        profile_id: QA_LIVE_PROFILE_ALIAS.to_owned(),
        provider: binding.profile.provider.clone(),
        profile_name: "QA live selected profile".to_owned(),
        scope: AuthProfileScope::Global,
        credential,
        created_at_unix_ms: binding.profile.created_at_unix_ms,
        updated_at_unix_ms: binding.profile.updated_at_unix_ms,
    };
    let registry_path = state_root.join("auth_profiles.toml");
    let registry = QaAuthRegistryDocument {
        version: QA_AUTH_REGISTRY_SCHEMA_VERSION,
        profiles: std::slice::from_ref(&projected_profile),
    };
    let registry_text =
        toml::to_string_pretty(&registry).context("qa.runner.live_registry_encode_failed")?;
    fs::write(registry_path.as_path(), registry_text.as_bytes())
        .context("qa.runner.live_registry_write_failed")?;
    palyra_vault::ensure_owner_only_file(registry_path.as_path())
        .context("qa.runner.live_registry_permissions_failed")?;

    Ok(QaPreparedProviderEnvironment {
        provider: QaDaemonProviderEnvironment::Live {
            registry_path,
            auth_provider_kind: binding.auth_provider_kind.clone(),
            transport: match binding.provider_kind {
                QaScenarioLiveProviderKind::OpenAiCompatible => {
                    QaLiveTransportEnvironment::OpenAiCompatible {
                        model: binding.model.clone(),
                        base_url: binding.base_url.clone(),
                    }
                }
                QaScenarioLiveProviderKind::Anthropic => QaLiveTransportEnvironment::Anthropic {
                    model: binding.model.clone(),
                    base_url: binding.base_url.clone(),
                },
            },
        },
        live_secret_sentinels: sentinels,
    })
}

pub(super) fn open_isolated_live_vault(vault_dir: &Path, identity_root: &Path) -> Result<Vault> {
    Vault::open_with_config(VaultConfig {
        root: Some(vault_dir.to_path_buf()),
        identity_store_root: Some(identity_root.to_path_buf()),
        backend_preference: BackendPreference::EncryptedFile,
        ..VaultConfig::default()
    })
    .context("qa.runner.live_scoped_vault_unavailable")
}

pub(super) fn copy_live_secret(
    source_vault: &Vault,
    scoped_vault: &Vault,
    scope: &VaultScope,
    source_reference: &str,
    label: &str,
    sentinels: &mut Vec<SecretBytes>,
) -> Result<String> {
    let source =
        VaultRef::parse(source_reference).context("qa.runner.live_secret_reference_invalid")?;
    let secret = SecretBytes::new(
        source_vault
            .get_secret(&source.scope, source.key.as_str())
            .context("qa.runner.live_secret_unavailable")?,
    )?;
    let key = format!("qa_live_{label}_{}", Ulid::new().to_string().to_ascii_lowercase());
    scoped_vault
        .put_secret(scope, key.as_str(), secret.as_slice())
        .context("qa.runner.live_secret_projection_failed")?;
    let reference = format!("{scope}/{key}");
    sentinels.push(secret);
    Ok(reference)
}

pub(super) fn prepare_fault_context(
    state_root: &Path,
    plan: Option<&QaFaultInjectionPlan>,
) -> Result<Option<QaRunnerFaultContext>> {
    let Some(plan) = plan else {
        return Ok(None);
    };
    ensure_owner_only_dir(state_root).context("qa.runner.fault_state_root_harden_failed")?;
    let directory = state_root.join(QA_FAULT_DIRECTORY);
    ensure_owner_only_dir(directory.as_path())
        .context("qa.runner.fault_directory_harden_failed")?;
    let directory = fs::canonicalize(directory.as_path())
        .context("qa.runner.fault_directory_canonicalize_failed")?;
    let plan_path = directory.join(QA_FAULT_PLAN_FILE);
    let plan_bytes = plan.canonical_json().context("qa.runner.fault_plan_invalid")?;
    write_owner_only_new_file(
        plan_path.as_path(),
        plan_bytes.as_slice(),
        "qa.runner.fault_plan_write_failed",
    )?;
    let plan_path = fs::canonicalize(plan_path.as_path())
        .context("qa.runner.fault_plan_canonicalize_failed")?;
    Ok(Some(QaRunnerFaultContext {
        directory: directory.clone(),
        plan: plan.clone(),
        plan_path,
        plan_sha256: plan.canonical_sha256().context("qa.runner.fault_plan_invalid")?,
        evidence_path: directory.join(QA_FAULT_EVIDENCE_FILE),
    }))
}

pub(super) fn prepare_fault_launch(
    context: &QaRunnerFaultContext,
) -> Result<QaPreparedFaultLaunch> {
    let launch_id = Ulid::new().to_string();
    let launch_file_name = format!("launch-{launch_id}.json");
    let capability_file_name = format!("capability-{launch_id}.txt");
    let launch_relative_path = PathBuf::from(QA_FAULT_DIRECTORY).join(launch_file_name.as_str());
    let capability_relative_path =
        PathBuf::from(QA_FAULT_DIRECTORY).join(capability_file_name.as_str());
    let launch_path = context.directory.join(launch_file_name);
    let capability_path = context.directory.join(capability_file_name);

    let mut capability = [0_u8; QA_FAULT_CAPABILITY_BYTES];
    SystemRandom::new()
        .fill(&mut capability)
        .map_err(|_| anyhow::anyhow!("qa.runner.fault_capability_generation_failed"))?;
    let capability_hex = digest_to_hex(capability.as_slice());
    let capability_text = format!("{QA_FAULT_CAPABILITY_PREFIX}{capability_hex}\n");
    // Scanning the raw token also catches it inside the prefixed wire form.
    let capability_sentinel = SecretBytes::new(capability_hex.into_bytes())?;
    write_owner_only_new_file(
        capability_path.as_path(),
        capability_text.as_bytes(),
        "qa.runner.fault_capability_write_failed",
    )?;

    let expires_at_unix_ms = current_unix_ms()?
        .checked_add(QA_FAULT_LAUNCH_LIFETIME_MS)
        .ok_or_else(|| anyhow::anyhow!("qa.runner.fault_launch_expiry_overflow"))?;
    let document = QaFaultLaunchDocument {
        schema_version: QA_FAULT_LAUNCH_SCHEMA_VERSION,
        launch_id: launch_id.clone(),
        plan_path: utf8_absolute_path(context.plan_path.as_path(), "fault plan")?,
        plan_sha256: context.plan_sha256.clone(),
        capability_sha256: digest_to_hex(Sha256::digest(capability).as_slice()),
        evidence_path: utf8_absolute_path(context.evidence_path.as_path(), "fault evidence")?,
        expires_at_unix_ms,
    };
    document.validate().context("qa.runner.fault_launch_invalid")?;
    let launch_bytes =
        serde_json::to_vec(&document).context("qa.runner.fault_launch_encode_failed")?;
    write_owner_only_new_file(
        launch_path.as_path(),
        launch_bytes.as_slice(),
        "qa.runner.fault_launch_write_failed",
    )?;
    Ok(QaPreparedFaultLaunch {
        document,
        launch_relative_path,
        capability_relative_path,
        capability_path,
        capability_sentinel,
    })
}

fn verify_fault_launch_handshake(
    context: &QaRunnerFaultContext,
    launch: &QaPreparedFaultLaunch,
) -> Result<()> {
    ensure_fault_capability_absent(launch.capability_path.as_path())?;
    let sidecar = load_fault_evidence_sidecar(context, &launch.document);
    ensure_fault_capability_absent(launch.capability_path.as_path())?;
    sidecar.map(|_| ()).context("qa.runner.fault_launch_handshake_invalid")
}

fn ensure_fault_capability_absent(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Ok(_) => anyhow::bail!("qa.runner.fault_capability_not_consumed"),
        Err(error) => Err(error).context("qa.runner.fault_capability_status_failed"),
    }
}

pub(super) fn verify_bound_fault_launch_handshake_with_hook<Hook>(
    state_root: &SharedStateRoot,
    context: &QaRunnerFaultContext,
    launch: &QaPreparedFaultLaunch,
    after_initial_identity: Hook,
) -> Result<()>
where
    Hook: FnOnce() -> Result<()>,
{
    let mut ownership = lock_unpoisoned(state_root);
    ownership.verify_identity().context("qa.runner.daemon_start_state_root_identity_invalid")?;
    let owned_root =
        ownership.root.as_ref().map(TempDir::path).context("qa.runner.state_root_removed")?;
    ensure_fault_context_within_state_root(owned_root, context)?;
    after_initial_identity()?;
    let handshake = verify_fault_launch_handshake(context, launch);
    ownership
        .verify_path_identity()
        .context("qa.runner.daemon_start_state_root_identity_invalid")?;
    handshake
}

pub(super) fn ensure_fault_context_within_state_root(
    state_root: &Path,
    context: &QaRunnerFaultContext,
) -> Result<()> {
    let canonical_root =
        fs::canonicalize(state_root).context("qa.runner.fault_evidence_state_root_unavailable")?;
    let canonical_directory = fs::canonicalize(context.directory.as_path())
        .context("qa.runner.fault_evidence_path_invalid")?;
    if !canonical_directory.starts_with(canonical_root.as_path()) {
        anyhow::bail!("qa.runner.fault_evidence_path_invalid");
    }
    Ok(())
}

pub(super) fn load_fault_evidence_sidecar(
    context: &QaRunnerFaultContext,
    launch: &QaFaultLaunchDocument,
) -> Result<QaFaultEvidenceSidecar> {
    let bytes = read_bound_fault_evidence_file(context)?;
    parse_qa_fault_evidence_sidecar_ndjson(bytes.as_slice(), launch, &context.plan)
        .context("qa.runner.fault_evidence_invalid")
}

fn read_bound_fault_evidence_file(context: &QaRunnerFaultContext) -> Result<Vec<u8>> {
    validate_existing_path_components(context.directory.as_path(), context.evidence_path.as_path())
        .context("qa.runner.fault_evidence_path_invalid")?;
    let metadata = fs::symlink_metadata(context.evidence_path.as_path())
        .context("qa.runner.fault_evidence_missing")?;
    if !metadata.is_file()
        || metadata_is_indirection(&metadata)
        || metadata.len() > QA_FAULT_EVIDENCE_SIDECAR_MAX_BYTES as u64
    {
        anyhow::bail!("qa.runner.fault_evidence_file_invalid");
    }
    ensure_owner_only_file(context.evidence_path.as_path())
        .context("qa.runner.fault_evidence_harden_failed")?;
    let file = open_failure_workspace_file_no_follow(context.evidence_path.as_path())
        .context("qa.runner.fault_evidence_open_failed")?;
    let opened_metadata = file.metadata().context("qa.runner.fault_evidence_open_failed")?;
    if !opened_metadata.is_file()
        || metadata_is_indirection(&opened_metadata)
        || opened_metadata.len() != metadata.len()
        || open_file_link_count(&file)? != 1
    {
        anyhow::bail!("qa.runner.fault_evidence_file_changed");
    }
    let identity = open_file_identity(&file)?;
    let comparison = open_failure_workspace_file_no_follow(context.evidence_path.as_path())
        .context("qa.runner.fault_evidence_open_failed")?;
    if !same_open_file_identity(&file, &comparison)? || open_file_link_count(&comparison)? != 1 {
        anyhow::bail!("qa.runner.fault_evidence_file_changed");
    }
    drop(comparison);

    let mut bytes = Vec::with_capacity(
        usize::try_from(opened_metadata.len()).context("qa.runner.fault_evidence_file_invalid")?,
    );
    file.try_clone()
        .context("qa.runner.fault_evidence_open_failed")?
        .take((QA_FAULT_EVIDENCE_SIDECAR_MAX_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .context("qa.runner.fault_evidence_read_failed")?;
    if bytes.len() > QA_FAULT_EVIDENCE_SIDECAR_MAX_BYTES
        || u64::try_from(bytes.len()).ok() != Some(opened_metadata.len())
    {
        anyhow::bail!("qa.runner.fault_evidence_file_invalid");
    }
    let expected_sha256 = digest_to_hex(Sha256::digest(bytes.as_slice()).as_slice());
    let observed_sha256 = digest_validated_journal_file(&file, opened_metadata.len())
        .context("qa.runner.fault_evidence_digest_failed")?;
    let final_comparison = open_failure_workspace_file_no_follow(context.evidence_path.as_path())
        .context("qa.runner.fault_evidence_open_failed")?;
    let final_sha256 = digest_validated_journal_file(&final_comparison, opened_metadata.len())
        .context("qa.runner.fault_evidence_digest_failed")?;
    if identity != open_file_identity(&final_comparison)?
        || open_file_link_count(&final_comparison)? != 1
        || expected_sha256 != observed_sha256
        || expected_sha256 != final_sha256
    {
        anyhow::bail!("qa.runner.fault_evidence_file_changed");
    }
    Ok(bytes)
}

pub(super) fn write_owner_only_new_file(
    path: &Path,
    bytes: &[u8],
    error_code: &'static str,
) -> Result<()> {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| error_code)?;
    file.write_all(bytes).with_context(|| error_code)?;
    file.flush().with_context(|| error_code)?;
    file.sync_all().with_context(|| error_code)?;
    drop(file);
    ensure_owner_only_file(path).with_context(|| error_code)
}

fn utf8_absolute_path(path: &Path, label: &'static str) -> Result<String> {
    if !path.is_absolute() {
        anyhow::bail!("qa.runner.fault_private_path_not_absolute: {label}");
    }
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| anyhow::anyhow!("qa.runner.fault_private_path_not_utf8: {label}"))
}

fn current_unix_ms() -> Result<i64> {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("qa.runner.system_clock_before_unix_epoch")?;
    i64::try_from(duration.as_millis()).context("qa.runner.system_time_out_of_range")
}
