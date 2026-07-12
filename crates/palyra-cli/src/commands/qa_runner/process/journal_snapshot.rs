use super::*;

#[derive(Debug)]
pub(super) struct ValidatedFailureJournalFile {
    pub(super) path: PathBuf,
    pub(super) file: fs::File,
    pub(super) bytes: u64,
    pub(super) identity: OpenFileIdentity,
    pub(super) sha256: String,
}

#[derive(Debug)]
pub(super) struct ValidatedFailureJournal {
    pub(super) database: ValidatedFailureJournalFile,
    pub(super) wal: Option<ValidatedFailureJournalFile>,
    pub(super) shm: Option<ValidatedFailureJournalFile>,
}

#[derive(Debug)]
pub(super) struct FailureJournalSnapshot {
    pub(super) root: Option<TempDir>,
    pub(super) pin: Option<PinnedStateRoot>,
    files: Option<ValidatedFailureJournal>,
}

impl FailureJournalSnapshot {
    #[cfg(test)]
    pub(super) fn path(&self) -> Result<&Path> {
        self.root
            .as_ref()
            .map(TempDir::path)
            .context("qa.runner.failure_diagnostics_journal_snapshot_cleanup_failed")
    }

    fn files(&self) -> Result<&ValidatedFailureJournal> {
        self.files.as_ref().context("qa.runner.failure_diagnostics_journal_snapshot_invalid")
    }

    pub(super) fn database_path(&self) -> Result<&Path> {
        Ok(self.files()?.database.path.as_path())
    }

    fn has_wal(&self) -> Result<bool> {
        Ok(self.files()?.wal.is_some())
    }

    fn verify_unchanged(&self) -> Result<()> {
        let root =
            self.root.as_ref().context("qa.runner.failure_diagnostics_journal_snapshot_invalid")?;
        let expected = self.files()?;
        let observed = validate_failure_journal_files_at(
            root.path(),
            root.path().join("journal.sqlite3").as_path(),
        )?;
        if !validated_journal_file_matches(&expected.database, &observed.database)
            || !validated_optional_journal_file_matches(
                expected.wal.as_ref(),
                observed.wal.as_ref(),
            )
            || !validated_optional_journal_file_matches(
                expected.shm.as_ref(),
                observed.shm.as_ref(),
            )
        {
            anyhow::bail!("qa.runner.failure_diagnostics_journal_snapshot_changed");
        }
        Ok(())
    }

    pub(super) fn close_verified(self) -> Result<()> {
        self.close_verified_with_hook(|_| Ok(()))
    }

    pub(super) fn close_verified_with_hook<Hook>(mut self, after_file_release: Hook) -> Result<()>
    where
        Hook: FnOnce(&Path) -> Result<()>,
    {
        // Windows may deny directory removal while a child file handle remains open.
        self.files.take();
        let root = self
            .root
            .take()
            .context("qa.runner.failure_diagnostics_journal_snapshot_cleanup_failed")?;
        let path = root.path().to_path_buf();
        after_file_release(path.as_path())?;
        let pin = self
            .pin
            .take()
            .context("qa.runner.failure_diagnostics_journal_snapshot_cleanup_failed")?;
        let path_matches_pin = open_directory_no_follow(path.as_path())
            .and_then(|current| same_open_file_identity(&pin.directory, &current))
            .unwrap_or(false);
        if !path_matches_pin {
            let _retained_snapshot_path = root.keep();
            anyhow::bail!("qa.runner.failure_diagnostics_journal_snapshot_cleanup_failed");
        }
        root.close().context("qa.runner.failure_diagnostics_journal_snapshot_cleanup_failed")?;
        let path_removed = matches!(
            fs::symlink_metadata(path.as_path()),
            Err(error) if error.kind() == io::ErrorKind::NotFound
        );
        if !path_removed || !pinned_directory_removed(&pin.directory).unwrap_or(false) {
            anyhow::bail!("qa.runner.failure_diagnostics_journal_snapshot_cleanup_failed");
        }
        Ok(())
    }
}

impl Drop for FailureJournalSnapshot {
    fn drop(&mut self) {
        // Raw journal bytes must only disappear through `close_verified`. If a new return path
        // forgets that accounting step, retain the snapshot under the owned state root instead.
        if let Some(root) = self.root.take() {
            let _retained_snapshot_path = root.keep();
        }
        self.pin.take();
    }
}

pub(super) fn validate_failure_journal_files(state_root: &Path) -> Result<ValidatedFailureJournal> {
    validate_failure_journal_files_at(state_root, state_root.join("data/journal.sqlite3").as_path())
}

fn validate_failure_journal_files_at(
    state_root: &Path,
    database_path: &Path,
) -> Result<ValidatedFailureJournal> {
    let root_metadata = fs::symlink_metadata(state_root)
        .context("qa.runner.failure_diagnostics_state_root_unavailable")?;
    if !root_metadata.is_dir() || metadata_is_indirection(&root_metadata) {
        anyhow::bail!("qa.runner.failure_diagnostics_state_root_invalid");
    }
    let canonical_root = fs::canonicalize(state_root)
        .context("qa.runner.failure_diagnostics_state_root_unavailable")?;
    let database = validate_failure_journal_file(
        state_root,
        canonical_root.as_path(),
        database_path,
        MAX_FAILURE_JOURNAL_DB_BYTES,
        false,
    )?
    .context("qa.runner.failure_diagnostics_journal_unavailable")?;
    let wal = validate_failure_journal_file(
        state_root,
        canonical_root.as_path(),
        journal_sidecar_path(database_path, "-wal")?.as_path(),
        MAX_FAILURE_JOURNAL_WAL_BYTES,
        true,
    )?;
    let shm = validate_failure_journal_file(
        state_root,
        canonical_root.as_path(),
        journal_sidecar_path(database_path, "-shm")?.as_path(),
        MAX_FAILURE_JOURNAL_SHM_BYTES,
        true,
    )?;
    // SQLite may otherwise create SHM while opening WAL. Diagnostics require an
    // already complete pair so a read-only URI never needs to create coordination state.
    if wal.is_some() != shm.is_some() {
        anyhow::bail!("qa.runner.failure_diagnostics_journal_sidecar_pair_invalid");
    }
    let total_bytes = [
        Some(database.bytes),
        wal.as_ref().map(|file| file.bytes),
        shm.as_ref().map(|file| file.bytes),
    ]
    .into_iter()
    .flatten()
    .try_fold(0_u64, |total, bytes| total.checked_add(bytes))
    .context("qa.runner.failure_diagnostics_journal_invalid")?;
    if total_bytes > MAX_FAILURE_JOURNAL_TOTAL_BYTES {
        anyhow::bail!("qa.runner.failure_diagnostics_journal_invalid");
    }
    Ok(ValidatedFailureJournal { database, wal, shm })
}

fn journal_sidecar_path(database_path: &Path, suffix: &str) -> Result<PathBuf> {
    let mut file_name = database_path
        .file_name()
        .context("qa.runner.failure_diagnostics_journal_invalid")?
        .to_os_string();
    file_name.push(suffix);
    Ok(database_path.with_file_name(file_name))
}

fn validate_failure_journal_file(
    state_root: &Path,
    canonical_root: &Path,
    path: &Path,
    max_bytes: u64,
    optional: bool,
) -> Result<Option<ValidatedFailureJournalFile>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if optional && error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).context("qa.runner.failure_diagnostics_journal_unavailable");
        }
    };
    validate_existing_path_components(state_root, path)?;
    if !metadata.is_file() || metadata_is_indirection(&metadata) || metadata.len() > max_bytes {
        anyhow::bail!("qa.runner.failure_diagnostics_journal_invalid");
    }
    let canonical =
        fs::canonicalize(path).context("qa.runner.failure_diagnostics_journal_unavailable")?;
    if !canonical.starts_with(canonical_root) {
        anyhow::bail!("qa.runner.failure_diagnostics_journal_outside_state_root");
    }
    let file = open_failure_workspace_file_no_follow(path)
        .context("qa.runner.failure_diagnostics_journal_no_follow_open_failed")?;
    let opened_metadata =
        file.metadata().context("qa.runner.failure_diagnostics_journal_unavailable")?;
    if !opened_metadata.is_file()
        || metadata_is_indirection(&opened_metadata)
        || opened_metadata.len() != metadata.len()
        || open_file_link_count(&file)? != 1
    {
        anyhow::bail!("qa.runner.failure_diagnostics_journal_changed");
    }
    let comparison = open_failure_workspace_file_no_follow(canonical.as_path())
        .context("qa.runner.failure_diagnostics_journal_no_follow_open_failed")?;
    if !same_open_file_identity(&file, &comparison)? || open_file_link_count(&comparison)? != 1 {
        anyhow::bail!("qa.runner.failure_diagnostics_journal_changed");
    }
    let sha256 = digest_validated_journal_file(&file, opened_metadata.len())?;
    Ok(Some(ValidatedFailureJournalFile {
        path: canonical,
        identity: open_file_identity(&file)?,
        file,
        bytes: opened_metadata.len(),
        sha256,
    }))
}

pub(super) fn validate_existing_path_components(root: &Path, path: &Path) -> Result<()> {
    let relative =
        path.strip_prefix(root).context("qa.runner.failure_diagnostics_path_outside_root")?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        let std::path::Component::Normal(component) = component else {
            anyhow::bail!("qa.runner.failure_diagnostics_path_invalid");
        };
        current.push(component);
        let metadata = fs::symlink_metadata(current.as_path())
            .context("qa.runner.failure_diagnostics_path_unavailable")?;
        if metadata_is_indirection(&metadata) {
            anyhow::bail!("qa.runner.failure_diagnostics_path_indirection_denied");
        }
    }
    Ok(())
}

pub(super) fn sqlite_read_only_uri(path: &Path, immutable: bool) -> Result<String> {
    let raw = path.to_str().context("qa.runner.failure_diagnostics_journal_path_invalid")?;
    #[cfg(windows)]
    let normalized = {
        let without_verbatim = raw.strip_prefix(r"\\?\").unwrap_or(raw);
        let normalized = without_verbatim.replace('\\', "/");
        if normalized.as_bytes().get(1) == Some(&b':') {
            format!("/{normalized}")
        } else {
            normalized
        }
    };
    #[cfg(not(windows))]
    let normalized = raw.to_owned();
    let mut uri = String::with_capacity(normalized.len().saturating_mul(3).saturating_add(32));
    uri.push_str("file:");
    for byte in normalized.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b':' | b'-' | b'.' | b'_' | b'~') {
            uri.push(char::from(byte));
        } else {
            let _ = write!(uri, "%{byte:02X}");
        }
    }
    uri.push_str("?mode=ro");
    if immutable {
        uri.push_str("&immutable=1");
    }
    Ok(uri)
}

fn ensure_failure_journal_unchanged(
    expected: &ValidatedFailureJournal,
    state_root: &Path,
) -> Result<()> {
    let observed = validate_failure_journal_files(state_root)?;
    if !validated_journal_file_matches(&expected.database, &observed.database)
        || !validated_optional_journal_file_matches(expected.wal.as_ref(), observed.wal.as_ref())
        || !validated_optional_journal_file_matches(expected.shm.as_ref(), observed.shm.as_ref())
    {
        anyhow::bail!("qa.runner.failure_diagnostics_journal_changed");
    }
    Ok(())
}

fn validated_optional_journal_file_matches(
    expected: Option<&ValidatedFailureJournalFile>,
    observed: Option<&ValidatedFailureJournalFile>,
) -> bool {
    match (expected, observed) {
        (Some(expected), Some(observed)) => validated_journal_file_matches(expected, observed),
        (None, None) => true,
        (Some(_), None) | (None, Some(_)) => false,
    }
}

fn validated_journal_file_matches(
    expected: &ValidatedFailureJournalFile,
    observed: &ValidatedFailureJournalFile,
) -> bool {
    expected.path == observed.path
        && expected.bytes == observed.bytes
        && expected.identity == observed.identity
        && expected.sha256 == observed.sha256
}

pub(super) fn digest_validated_journal_file(
    file: &fs::File,
    expected_bytes: u64,
) -> Result<String> {
    // Identity and length do not detect an in-place SQLite/WAL rewrite, so every validation pass
    // binds the pinned handle to bounded content as well.
    let mut reader =
        file.try_clone().context("qa.runner.failure_diagnostics_journal_digest_failed")?;
    reader
        .seek(SeekFrom::Start(0))
        .context("qa.runner.failure_diagnostics_journal_digest_failed")?;
    let mut hasher = Sha256::new();
    let mut observed_bytes = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .context("qa.runner.failure_diagnostics_journal_digest_failed")?;
        if read == 0 {
            break;
        }
        observed_bytes = observed_bytes
            .checked_add(
                u64::try_from(read)
                    .context("qa.runner.failure_diagnostics_journal_digest_failed")?,
            )
            .context("qa.runner.failure_diagnostics_journal_changed")?;
        if observed_bytes > expected_bytes {
            anyhow::bail!("qa.runner.failure_diagnostics_journal_changed");
        }
        hasher.update(&buffer[..read]);
    }
    if observed_bytes != expected_bytes
        || file.metadata().context("qa.runner.failure_diagnostics_journal_digest_failed")?.len()
            != expected_bytes
    {
        anyhow::bail!("qa.runner.failure_diagnostics_journal_changed");
    }
    Ok(digest_to_hex(hasher.finalize().as_slice()))
}

pub(super) fn materialize_failure_journal_snapshot(
    state_root: &Path,
) -> Result<FailureJournalSnapshot> {
    materialize_failure_journal_snapshot_with_hook(state_root, || Ok(()))
}

pub(super) fn materialize_failure_journal_snapshot_with_hook<Hook>(
    state_root: &Path,
    after_validation: Hook,
) -> Result<FailureJournalSnapshot>
where
    Hook: FnOnce() -> Result<()>,
{
    let validated = validate_failure_journal_files(state_root)?;
    after_validation()?;
    ensure_failure_journal_unchanged(&validated, state_root)?;
    let snapshot_root = tempfile::Builder::new()
        .prefix(FAILURE_JOURNAL_SNAPSHOT_PREFIX)
        .tempdir_in(state_root)
        .context("qa.runner.failure_diagnostics_journal_snapshot_failed")?;
    let snapshot_pin = match pin_state_root(snapshot_root.path()) {
        Ok(pin) => pin,
        Err(error) => {
            snapshot_root
                .close()
                .context("qa.runner.failure_diagnostics_journal_snapshot_cleanup_failed")?;
            return Err(error).context("qa.runner.failure_diagnostics_journal_snapshot_failed");
        }
    };
    let database_path = snapshot_root.path().join("journal.sqlite3");
    let materialized = (|| {
        copy_validated_journal_file(&validated.database, database_path.as_path())?;
        if let Some(wal) = validated.wal.as_ref() {
            copy_validated_journal_file(
                wal,
                snapshot_root.path().join("journal.sqlite3-wal").as_path(),
            )?;
        }
        if let Some(shm) = validated.shm.as_ref() {
            copy_validated_journal_file(
                shm,
                snapshot_root.path().join("journal.sqlite3-shm").as_path(),
            )?;
        }
        ensure_failure_journal_unchanged(&validated, state_root)?;
        let copied_files =
            validate_failure_journal_files_at(snapshot_root.path(), database_path.as_path())?;
        if !validated_journal_contents_match(&validated, &copied_files) {
            anyhow::bail!("qa.runner.failure_diagnostics_journal_snapshot_changed");
        }
        let has_wal = copied_files.wal.is_some();
        // Preserve SQLite NOFOLLOW while avoiding ambient path aliases (such as macOS `/var`) by
        // opening the canonical path already bound to the validated snapshot file identity.
        let sqlite_database_path = copied_files.database.path.clone();
        drop(copied_files);
        if has_wal {
            consolidate_failure_journal_snapshot(sqlite_database_path.as_path())?;
        }
        let snapshot_files =
            validate_failure_journal_files_at(snapshot_root.path(), database_path.as_path())?;
        if snapshot_files.wal.is_some() || snapshot_files.shm.is_some() {
            anyhow::bail!("qa.runner.failure_diagnostics_journal_snapshot_changed");
        }
        Ok(snapshot_files)
    })();
    let snapshot_files = match materialized {
        Ok(snapshot_files) => snapshot_files,
        Err(error) => {
            FailureJournalSnapshot {
                root: Some(snapshot_root),
                pin: Some(snapshot_pin),
                files: None,
            }
            .close_verified()?;
            return Err(error);
        }
    };
    Ok(FailureJournalSnapshot {
        root: Some(snapshot_root),
        pin: Some(snapshot_pin),
        files: Some(snapshot_files),
    })
}

fn validated_journal_contents_match(
    expected: &ValidatedFailureJournal,
    observed: &ValidatedFailureJournal,
) -> bool {
    validated_journal_file_contents_match(&expected.database, &observed.database)
        && validated_optional_journal_file_contents_match(
            expected.wal.as_ref(),
            observed.wal.as_ref(),
        )
        && validated_optional_journal_file_contents_match(
            expected.shm.as_ref(),
            observed.shm.as_ref(),
        )
}

fn validated_optional_journal_file_contents_match(
    expected: Option<&ValidatedFailureJournalFile>,
    observed: Option<&ValidatedFailureJournalFile>,
) -> bool {
    match (expected, observed) {
        (Some(expected), Some(observed)) => {
            validated_journal_file_contents_match(expected, observed)
        }
        (None, None) => true,
        (Some(_), None) | (None, Some(_)) => false,
    }
}

fn validated_journal_file_contents_match(
    expected: &ValidatedFailureJournalFile,
    observed: &ValidatedFailureJournalFile,
) -> bool {
    expected.bytes == observed.bytes && expected.sha256 == observed.sha256
}

fn copy_validated_journal_file(
    source: &ValidatedFailureJournalFile,
    destination: &Path,
) -> Result<()> {
    let mut reader =
        source.file.try_clone().context("qa.runner.failure_diagnostics_journal_snapshot_failed")?;
    reader
        .seek(SeekFrom::Start(0))
        .context("qa.runner.failure_diagnostics_journal_snapshot_failed")?;
    let mut writer = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)
        .context("qa.runner.failure_diagnostics_journal_snapshot_failed")?;
    let mut remaining = source.bytes;
    let mut source_hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    let buffer_bytes = u64::try_from(buffer.len())
        .context("qa.runner.failure_diagnostics_journal_snapshot_failed")?;
    while remaining > 0 {
        let requested = usize::try_from(remaining.min(buffer_bytes))
            .context("qa.runner.failure_diagnostics_journal_snapshot_failed")?;
        let read = reader
            .read(&mut buffer[..requested])
            .context("qa.runner.failure_diagnostics_journal_snapshot_failed")?;
        if read == 0 {
            anyhow::bail!("qa.runner.failure_diagnostics_journal_changed");
        }
        writer
            .write_all(&buffer[..read])
            .context("qa.runner.failure_diagnostics_journal_snapshot_failed")?;
        source_hasher.update(&buffer[..read]);
        remaining = remaining
            .checked_sub(
                u64::try_from(read)
                    .context("qa.runner.failure_diagnostics_journal_snapshot_failed")?,
            )
            .context("qa.runner.failure_diagnostics_journal_changed")?;
    }
    let mut trailing = [0_u8; 1];
    let source_bytes = source
        .file
        .metadata()
        .context("qa.runner.failure_diagnostics_journal_snapshot_failed")?
        .len();
    if reader
        .read(&mut trailing)
        .context("qa.runner.failure_diagnostics_journal_snapshot_failed")?
        != 0
        || open_file_identity(&source.file)? != source.identity
        || source_bytes != source.bytes
        || open_file_link_count(&source.file)? != 1
        || digest_to_hex(source_hasher.finalize().as_slice()) != source.sha256
    {
        anyhow::bail!("qa.runner.failure_diagnostics_journal_changed");
    }
    writer.sync_all().context("qa.runner.failure_diagnostics_journal_snapshot_failed")?;
    Ok(())
}

fn consolidate_failure_journal_snapshot(database_path: &Path) -> Result<()> {
    let connection = Connection::open_with_flags(
        database_path,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )
    .context("qa.runner.failure_diagnostics_journal_snapshot_consolidation_open_failed")?;
    connection
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode = DELETE;")
        .context("qa.runner.failure_diagnostics_journal_snapshot_consolidation_failed")?;
    drop(connection);
    for suffix in ["-wal", "-shm"] {
        let sidecar = journal_sidecar_path(database_path, suffix)?;
        match fs::remove_file(sidecar.as_path()) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).context(
                    "qa.runner.failure_diagnostics_journal_snapshot_sidecar_cleanup_failed",
                );
            }
        }
    }
    fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(database_path)
        .context("qa.runner.failure_diagnostics_journal_snapshot_sync_open_failed")?
        .sync_all()
        .context("qa.runner.failure_diagnostics_journal_snapshot_sync_failed")
}

pub(super) fn load_failure_run_projection(
    sandbox: &QaDaemonSandbox,
    state_root: &Path,
    run_id: &str,
) -> Result<Option<QaFailureRunProjection>> {
    load_failure_run_projection_with_hook(sandbox, state_root, run_id, |_| Ok(()))
}

pub(super) fn load_failure_run_projection_with_hook<Hook>(
    sandbox: &QaDaemonSandbox,
    state_root: &Path,
    run_id: &str,
    after_materialization: Hook,
) -> Result<Option<QaFailureRunProjection>>
where
    Hook: FnOnce(&FailureJournalSnapshot) -> Result<()>,
{
    sandbox.with_pinned_state_root_read(
        "qa.runner.failure_diagnostics_state_root_identity_invalid",
        |owned_state_root| {
            if owned_state_root != state_root {
                anyhow::bail!("qa.runner.failure_diagnostics_state_root_identity_invalid");
            }
            load_failure_run_projection_bound(
                sandbox,
                owned_state_root,
                run_id,
                after_materialization,
            )
        },
    )
}

fn load_failure_run_projection_bound<Hook>(
    sandbox: &QaDaemonSandbox,
    state_root: &Path,
    run_id: &str,
    after_materialization: Hook,
) -> Result<Option<QaFailureRunProjection>>
where
    Hook: FnOnce(&FailureJournalSnapshot) -> Result<()>,
{
    let snapshot = materialize_failure_journal_snapshot(state_root)?;
    let before_query = (|| {
        after_materialization(&snapshot)?;
        snapshot.verify_unchanged()?;
        Ok((snapshot.database_path()?.to_path_buf(), snapshot.has_wal()?))
    })();
    let (database_path, has_wal) = match before_query {
        Ok(snapshot_configuration) => snapshot_configuration,
        Err(error) => {
            snapshot.close_verified()?;
            return Err(error);
        }
    };
    let projection = load_failure_run_projection_from_snapshot(
        sandbox,
        database_path.as_path(),
        has_wal,
        run_id,
    );
    let snapshot_integrity = snapshot.verify_unchanged();
    snapshot.close_verified()?;
    snapshot_integrity?;
    projection
}
