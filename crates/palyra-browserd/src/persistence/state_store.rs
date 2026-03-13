use crate::*;

#[cfg(windows)]
use std::os::windows::process::CommandExt;

#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x0800_0000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PersistedSessionSnapshot {
    pub(crate) v: u32,
    pub(crate) principal: String,
    pub(crate) channel: Option<String>,
    pub(crate) tabs: Vec<BrowserTabRecord>,
    pub(crate) tab_order: Vec<String>,
    pub(crate) active_tab_id: String,
    pub(crate) permissions: SessionPermissionsInternal,
    pub(crate) cookie_jar: HashMap<String, HashMap<String, String>>,
    pub(crate) storage_entries: HashMap<String, HashMap<String, String>>,
    #[serde(default)]
    pub(crate) state_revision: u64,
    pub(crate) saved_at_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct BrowserTabRecordForHash {
    pub(crate) tab_id: String,
    pub(crate) last_title: String,
    pub(crate) last_url: Option<String>,
    pub(crate) last_page_body: String,
    pub(crate) scroll_x: i64,
    pub(crate) scroll_y: i64,
    pub(crate) typed_inputs: BTreeMap<String, String>,
    pub(crate) network_log: VecDeque<NetworkLogEntryInternal>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct PersistedSessionSnapshotLegacyForHash {
    pub(crate) v: u32,
    pub(crate) principal: String,
    pub(crate) channel: Option<String>,
    pub(crate) tabs: Vec<BrowserTabRecord>,
    pub(crate) tab_order: Vec<String>,
    pub(crate) active_tab_id: String,
    pub(crate) permissions: SessionPermissionsInternal,
    pub(crate) cookie_jar: HashMap<String, HashMap<String, String>>,
    pub(crate) storage_entries: HashMap<String, HashMap<String, String>>,
    pub(crate) saved_at_unix_ms: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct LoadedPersistedSessionSnapshot {
    pub(crate) snapshot: PersistedSessionSnapshot,
    pub(crate) raw_hash_sha256: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct PersistedSessionSnapshotForHash {
    pub(crate) v: u32,
    pub(crate) principal: String,
    pub(crate) channel: Option<String>,
    pub(crate) tabs: Vec<BrowserTabRecordForHash>,
    pub(crate) tab_order: Vec<String>,
    pub(crate) active_tab_id: String,
    pub(crate) permissions: SessionPermissionsInternal,
    pub(crate) cookie_jar: BTreeMap<String, BTreeMap<String, String>>,
    pub(crate) storage_entries: BTreeMap<String, BTreeMap<String, String>>,
    pub(crate) state_revision: u64,
    pub(crate) saved_at_unix_ms: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct PersistedStateStore {
    pub(crate) root_dir: PathBuf,
    pub(crate) key: [u8; STATE_KEY_LEN],
}

pub(crate) fn build_state_store_from_env() -> Result<Option<PersistedStateStore>> {
    let key_raw = match std::env::var(STATE_KEY_ENV) {
        Ok(value) => value.trim().to_owned(),
        Err(_) => return Ok(None),
    };
    if key_raw.is_empty() {
        return Ok(None);
    }
    let key = decode_state_key(key_raw.as_str())?;
    let state_dir = std::env::var(STATE_DIR_ENV)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .map(|value| normalize_configured_state_path(value.as_str(), STATE_DIR_ENV))
        .transpose()?
        .unwrap_or(default_browserd_state_dir()?);
    Ok(Some(PersistedStateStore::new(state_dir, key)?))
}

pub(crate) fn normalize_configured_state_path(raw: &str, field: &'static str) -> Result<PathBuf> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        anyhow::bail!("{field} cannot be empty");
    }
    let path = PathBuf::from(trimmed);
    for component in path.components() {
        if matches!(component, std::path::Component::ParentDir) {
            anyhow::bail!("{field} cannot contain '..' path segments");
        }
    }
    Ok(path)
}

pub(crate) fn default_browserd_state_dir() -> Result<PathBuf> {
    default_browserd_state_dir_from_env(
        std::env::var_os(STATE_ROOT_ENV),
        std::env::var_os("APPDATA"),
        std::env::var_os("LOCALAPPDATA"),
        std::env::var_os("XDG_STATE_HOME"),
        std::env::var_os("HOME"),
    )
}

pub(crate) fn default_browserd_state_dir_from_env(
    state_root: Option<OsString>,
    appdata: Option<OsString>,
    local_appdata: Option<OsString>,
    xdg_state_home: Option<OsString>,
    home: Option<OsString>,
) -> Result<PathBuf> {
    if let Some(state_root_raw) = state_root {
        let normalized = normalize_configured_state_path(
            state_root_raw.to_string_lossy().as_ref(),
            STATE_ROOT_ENV,
        )?;
        return Ok(normalized.join("browserd"));
    }
    #[cfg(windows)]
    {
        let _ = xdg_state_home;
        let _ = home;
        if let Some(appdata) = appdata {
            return Ok(PathBuf::from(appdata).join("Palyra").join("browserd"));
        }
        if let Some(local_appdata) = local_appdata {
            return Ok(PathBuf::from(local_appdata).join("Palyra").join("browserd"));
        }
        anyhow::bail!(
            "failed to resolve browserd state dir: APPDATA/LOCALAPPDATA are unset and {STATE_ROOT_ENV} is not configured"
        );
    }
    #[cfg(target_os = "macos")]
    {
        let _ = appdata;
        let _ = local_appdata;
        let _ = xdg_state_home;
        let home = home.ok_or_else(|| {
            anyhow::anyhow!(
                "failed to resolve browserd state dir: HOME is unset and {STATE_ROOT_ENV} is not configured"
            )
        })?;
        return Ok(PathBuf::from(home)
            .join("Library")
            .join("Application Support")
            .join("Palyra")
            .join("browserd"));
    }
    #[cfg(all(not(windows), not(target_os = "macos")))]
    {
        let _ = appdata;
        let _ = local_appdata;
        if let Some(xdg_state_home) = xdg_state_home {
            return Ok(PathBuf::from(xdg_state_home).join("palyra").join("browserd"));
        }
        let home = home.ok_or_else(|| {
            anyhow::anyhow!(
                "failed to resolve browserd state dir: XDG_STATE_HOME/HOME are unset and {STATE_ROOT_ENV} is not configured"
            )
        })?;
        Ok(PathBuf::from(home).join(".local").join("state").join("palyra").join("browserd"))
    }
}

pub(crate) fn ensure_owner_only_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path)
        .with_context(|| format!("failed to create browserd state dir '{}'", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).with_context(|| {
            format!(
                "failed to enforce owner-only directory permissions on browserd state dir '{}'",
                path.display()
            )
        })?;
    }
    #[cfg(windows)]
    {
        let owner_sid = current_user_sid()?;
        harden_windows_path_permissions(path, owner_sid.as_str(), true)?;
    }
    Ok(())
}

pub(crate) fn ensure_owner_only_file(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).with_context(|| {
            format!(
                "failed to enforce owner-only permissions on browserd state file '{}'",
                path.display()
            )
        })?;
    }
    #[cfg(windows)]
    {
        let owner_sid = current_user_sid()?;
        harden_windows_path_permissions(path, owner_sid.as_str(), false)?;
    }
    Ok(())
}

#[cfg(windows)]
pub(crate) fn current_user_sid() -> Result<String> {
    let output = windows_background_command("whoami")
        .args(["/user", "/fo", "csv", "/nh"])
        .output()
        .context("failed to execute whoami while resolving browserd state ACL SID")?;
    if !output.status.success() {
        anyhow::bail!(
            "whoami returned non-success status {} while resolving browserd state ACL SID: stdout={} stderr={}",
            output.status.code().map_or_else(|| "unknown".to_owned(), |code| code.to_string()),
            String::from_utf8_lossy(&output.stdout).trim(),
            String::from_utf8_lossy(&output.stderr).trim(),
        );
    }
    parse_whoami_sid_csv(String::from_utf8_lossy(&output.stdout).trim())
        .ok_or_else(|| anyhow::anyhow!("failed to parse user SID from whoami output"))
}

#[cfg(windows)]
pub(crate) fn parse_whoami_sid_csv(raw: &str) -> Option<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    for ch in raw.chars() {
        match ch {
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                fields.push(current.trim().to_owned());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    fields.push(current.trim().to_owned());
    if fields.len() < 2 {
        return None;
    }
    let sid = fields[1].trim().trim_matches('"').to_owned();
    if sid.starts_with("S-1-") {
        Some(sid)
    } else {
        None
    }
}

#[cfg(windows)]
pub(crate) fn harden_windows_path_permissions(
    path: &Path,
    owner_sid: &str,
    is_directory: bool,
) -> Result<()> {
    let grant_mask = if is_directory { "(OI)(CI)F" } else { "F" };
    let owner_grant = format!("*{owner_sid}:{grant_mask}");
    let system_grant = format!("*{WINDOWS_SYSTEM_SID}:{grant_mask}");
    let output = windows_background_command("icacls")
        .arg(path)
        .args(["/inheritance:r", "/grant:r"])
        .arg(owner_grant)
        .args(["/grant:r"])
        .arg(system_grant)
        .output()
        .with_context(|| {
            format!("failed to execute icacls for browserd state path '{}'", path.display())
        })?;
    if !output.status.success() {
        anyhow::bail!(
            "icacls returned non-success status {} for '{}': stdout={} stderr={}",
            output.status.code().map_or_else(|| "unknown".to_owned(), |code| code.to_string()),
            path.display(),
            String::from_utf8_lossy(&output.stdout).trim(),
            String::from_utf8_lossy(&output.stderr).trim(),
        );
    }
    Ok(())
}

#[cfg(windows)]
fn windows_background_command(program: &str) -> Command {
    let mut command = Command::new(program);
    command.creation_flags(CREATE_NO_WINDOW);
    command
}

pub(crate) fn decode_state_key(raw: &str) -> Result<[u8; STATE_KEY_LEN]> {
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(raw)
        .context("failed to decode PALYRA_BROWSERD_STATE_ENCRYPTION_KEY as base64")?;
    if decoded.len() != STATE_KEY_LEN {
        anyhow::bail!(
            "PALYRA_BROWSERD_STATE_ENCRYPTION_KEY must decode to exactly {STATE_KEY_LEN} bytes"
        );
    }
    let mut key = [0_u8; STATE_KEY_LEN];
    key.copy_from_slice(decoded.as_slice());
    Ok(key)
}

impl PersistedStateStore {
    pub(crate) fn new(root_dir: PathBuf, key: [u8; STATE_KEY_LEN]) -> Result<Self> {
        ensure_path_is_not_symlink(root_dir.as_path(), "browserd state dir")?;
        ensure_owner_only_dir(root_dir.as_path())?;
        ensure_path_is_secure_directory(root_dir.as_path(), "browserd state dir")?;
        let store = Self { root_dir, key };
        store.cleanup_tmp_files()?;
        Ok(store)
    }

    pub(crate) fn snapshot_path(&self, state_id: &str) -> PathBuf {
        self.root_dir.join(format!("{state_id}.enc"))
    }

    pub(crate) fn tmp_snapshot_path(&self, state_id: &str) -> PathBuf {
        self.root_dir.join(format!("{state_id}.{}.{}", Ulid::new(), STATE_TMP_EXTENSION))
    }

    pub(crate) fn profile_registry_path(&self) -> PathBuf {
        self.root_dir.join(PROFILE_REGISTRY_FILE_NAME)
    }

    pub(crate) fn cleanup_tmp_files(&self) -> Result<()> {
        let entries = match fs::read_dir(self.root_dir.as_path()) {
            Ok(value) => value,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "failed to enumerate browser state dir '{}' for tmp cleanup",
                        self.root_dir.display()
                    )
                })
            }
        };
        for entry in entries {
            let entry = entry.with_context(|| {
                format!("failed to read browser state entry in '{}'", self.root_dir.display())
            })?;
            let path = entry.path();
            let file_type = entry.file_type().with_context(|| {
                format!("failed to inspect browser state entry type for '{}'", path.display())
            })?;
            if file_type.is_symlink() {
                anyhow::bail!(
                    "browser state dir '{}' contains unexpected symlink entry '{}'",
                    self.root_dir.display(),
                    path.display()
                );
            }
            if path
                .extension()
                .and_then(|value| value.to_str())
                .map(|value| value.eq_ignore_ascii_case(STATE_TMP_EXTENSION))
                .unwrap_or(false)
            {
                let _ = fs::remove_file(path.as_path());
            }
        }
        Ok(())
    }

    pub(crate) fn load_snapshot(
        &self,
        state_id: &str,
        profile_id: Option<&str>,
    ) -> Result<Option<LoadedPersistedSessionSnapshot>> {
        let path = self.snapshot_path(state_id);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = read_hardened_file(path.as_path(), "persisted browser state")?;
        let key = derive_state_encryption_key(&self.key, profile_id);
        let decrypted = decrypt_state_blob(&key, bytes.as_slice()).with_context(|| {
            format!("failed to decrypt persisted browser state '{}'", path.display())
        })?;
        let snapshot: PersistedSessionSnapshot = serde_json::from_slice(decrypted.as_slice())
            .with_context(|| {
                format!("failed to deserialize persisted browser state '{}'", path.display())
            })?;
        Ok(Some(LoadedPersistedSessionSnapshot {
            snapshot,
            raw_hash_sha256: sha256_hex(decrypted.as_slice()),
        }))
    }

    pub(crate) fn save_snapshot(
        &self,
        state_id: &str,
        profile_id: Option<&str>,
        snapshot: &PersistedSessionSnapshot,
    ) -> Result<()> {
        let serialized =
            serde_json::to_vec(snapshot).context("failed to serialize persisted browser state")?;
        let key = derive_state_encryption_key(&self.key, profile_id);
        let encrypted =
            encrypt_state_blob(&key, serialized.as_slice()).context("failed to encrypt state")?;
        let target_path = self.snapshot_path(state_id);
        let tmp_path = self.tmp_snapshot_path(state_id);
        write_hardened_file_atomic(
            self.root_dir.as_path(),
            target_path.as_path(),
            tmp_path.as_path(),
            encrypted.as_slice(),
            "persisted browser state",
        )?;
        Ok(())
    }

    pub(crate) fn delete_snapshot(&self, state_id: &str) -> Result<()> {
        let path = self.snapshot_path(state_id);
        if !path.exists() {
            return Ok(());
        }
        ensure_path_is_not_symlink(path.as_path(), "persisted browser state")?;
        fs::remove_file(path.as_path()).with_context(|| {
            format!("failed to delete persisted browser state '{}'", path.display())
        })?;
        Ok(())
    }

    pub(crate) fn load_profile_registry(&self) -> Result<BrowserProfileRegistryDocument> {
        let path = self.profile_registry_path();
        if !path.exists() {
            return Ok(BrowserProfileRegistryDocument::default());
        }
        let bytes = read_hardened_file(path.as_path(), "browser profile registry")?;
        let decrypted = decrypt_state_blob(&self.key, bytes.as_slice()).with_context(|| {
            format!("failed to decrypt browser profile registry '{}'", path.display())
        })?;
        let mut registry: BrowserProfileRegistryDocument =
            serde_json::from_slice(decrypted.as_slice()).with_context(|| {
                format!("failed to deserialize browser profile registry '{}'", path.display())
            })?;
        normalize_profile_registry(&mut registry);
        Ok(registry)
    }

    pub(crate) fn save_profile_registry(
        &self,
        registry: &BrowserProfileRegistryDocument,
    ) -> Result<()> {
        let serialized = serde_json::to_vec(registry)
            .context("failed to serialize browser profile registry document")?;
        if serialized.len() > MAX_PROFILE_REGISTRY_BYTES {
            anyhow::bail!(
                "browser profile registry exceeds max bytes ({} > {})",
                serialized.len(),
                MAX_PROFILE_REGISTRY_BYTES
            );
        }
        let encrypted = encrypt_state_blob(&self.key, serialized.as_slice())
            .context("failed to encrypt browser profile registry")?;
        let target_path = self.profile_registry_path();
        let tmp_path = self.root_dir.join(format!(
            "{}.{}.{}",
            PROFILE_REGISTRY_FILE_NAME,
            Ulid::new(),
            STATE_TMP_EXTENSION
        ));
        write_hardened_file_atomic(
            self.root_dir.as_path(),
            target_path.as_path(),
            tmp_path.as_path(),
            encrypted.as_slice(),
            "browser profile registry",
        )?;
        Ok(())
    }
}

pub(crate) fn ensure_path_is_not_symlink(path: &Path, context: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                anyhow::bail!("{context} '{}' must not be a symlink", path.display());
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| {
            format!("failed to inspect {context} path '{}' for symlink checks", path.display())
        }),
    }
}

pub(crate) fn ensure_path_is_secure_directory(path: &Path, context: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("failed to inspect {context} '{}'", path.display()))?;
    if metadata.file_type().is_symlink() {
        anyhow::bail!("{context} '{}' must not be a symlink", path.display());
    }
    if !metadata.is_dir() {
        anyhow::bail!("{context} '{}' must be a directory", path.display());
    }
    Ok(())
}

pub(crate) fn read_hardened_file(path: &Path, context: &str) -> Result<Vec<u8>> {
    ensure_path_is_not_symlink(path, context)?;
    #[cfg(unix)]
    {
        use std::io::Read;
        use std::os::unix::fs::OpenOptionsExt;

        let mut file = fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
            .with_context(|| format!("failed to open {context} '{}' for read", path.display()))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .with_context(|| format!("failed to read {context} '{}'", path.display()))?;
        Ok(bytes)
    }
    #[cfg(not(unix))]
    {
        fs::read(path).with_context(|| format!("failed to read {context} '{}'", path.display()))
    }
}

pub(crate) fn write_hardened_file_atomic(
    root_dir: &Path,
    target_path: &Path,
    tmp_path: &Path,
    payload: &[u8],
    context: &str,
) -> Result<()> {
    ensure_path_is_secure_directory(root_dir, "browserd state dir")?;
    ensure_path_is_not_symlink(target_path, context)?;
    ensure_path_is_not_symlink(tmp_path, "browserd temporary state file")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(tmp_path)
            .with_context(|| format!("failed to create tmp {context} '{}'", tmp_path.display()))?;
        file.write_all(payload)
            .with_context(|| format!("failed to write tmp {context} '{}'", tmp_path.display()))?;
        file.sync_all()
            .with_context(|| format!("failed to fsync tmp {context} '{}'", tmp_path.display()))?;
    }
    #[cfg(not(unix))]
    {
        let mut file =
            fs::OpenOptions::new().create_new(true).write(true).open(tmp_path).with_context(
                || format!("failed to create tmp {context} '{}'", tmp_path.display()),
            )?;
        file.write_all(payload)
            .with_context(|| format!("failed to write tmp {context} '{}'", tmp_path.display()))?;
        file.sync_all()
            .with_context(|| format!("failed to fsync tmp {context} '{}'", tmp_path.display()))?;
    }
    ensure_owner_only_file(tmp_path)?;
    fs::rename(tmp_path, target_path).with_context(|| {
        format!(
            "failed to atomically move tmp {context} '{}' into '{}'",
            tmp_path.display(),
            target_path.display()
        )
    })?;
    ensure_owner_only_file(target_path)?;
    sync_directory(root_dir)?;
    Ok(())
}

pub(crate) fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let directory = fs::File::open(path)
            .with_context(|| format!("failed to open directory '{}' for fsync", path.display()))?;
        directory
            .sync_all()
            .with_context(|| format!("failed to fsync directory '{}'", path.display()))?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

pub(crate) fn encrypt_state_blob(key: &[u8; STATE_KEY_LEN], plaintext: &[u8]) -> Result<Vec<u8>> {
    let unbound_key = UnboundKey::new(&CHACHA20_POLY1305, key)
        .map_err(|_| anyhow::anyhow!("failed to initialize state cipher key"))?;
    let key = LessSafeKey::new(unbound_key);
    let mut nonce_bytes = [0_u8; STATE_NONCE_LEN];
    SystemRandom::new()
        .fill(&mut nonce_bytes)
        .map_err(|_| anyhow::anyhow!("failed to generate state encryption nonce"))?;
    let nonce = Nonce::assume_unique_for_key(nonce_bytes);
    let mut in_out = plaintext.to_vec();
    key.seal_in_place_append_tag(nonce, Aad::empty(), &mut in_out)
        .map_err(|_| anyhow::anyhow!("failed to seal state payload"))?;
    let mut output = Vec::with_capacity(STATE_FILE_MAGIC.len() + STATE_NONCE_LEN + in_out.len());
    output.extend_from_slice(STATE_FILE_MAGIC);
    output.extend_from_slice(&nonce_bytes);
    output.extend_from_slice(in_out.as_slice());
    Ok(output)
}

pub(crate) fn decrypt_state_blob(key: &[u8; STATE_KEY_LEN], encrypted: &[u8]) -> Result<Vec<u8>> {
    if encrypted.len() < STATE_FILE_MAGIC.len() + STATE_NONCE_LEN {
        anyhow::bail!("state payload is too short");
    }
    if &encrypted[..STATE_FILE_MAGIC.len()] != STATE_FILE_MAGIC {
        anyhow::bail!("state payload magic header is invalid");
    }
    let mut nonce_bytes = [0_u8; STATE_NONCE_LEN];
    nonce_bytes.copy_from_slice(
        &encrypted[STATE_FILE_MAGIC.len()..STATE_FILE_MAGIC.len() + STATE_NONCE_LEN],
    );
    let mut in_out = encrypted[STATE_FILE_MAGIC.len() + STATE_NONCE_LEN..].to_vec();
    let unbound_key = UnboundKey::new(&CHACHA20_POLY1305, key)
        .map_err(|_| anyhow::anyhow!("failed to initialize state cipher key"))?;
    let key = LessSafeKey::new(unbound_key);
    let plaintext = key
        .open_in_place(Nonce::assume_unique_for_key(nonce_bytes), Aad::empty(), &mut in_out)
        .map_err(|_| anyhow::anyhow!("failed to open state payload"))?;
    Ok(plaintext.to_vec())
}

pub(crate) fn derive_state_encryption_key(
    master_key: &[u8; STATE_KEY_LEN],
    profile_id: Option<&str>,
) -> [u8; STATE_KEY_LEN] {
    let Some(profile_id) = profile_id else {
        return *master_key;
    };
    let mut context = DigestContext::new(&SHA256);
    context.update(STATE_PROFILE_DEK_NAMESPACE);
    context.update(master_key);
    context.update(profile_id.as_bytes());
    let digest = context.finish();
    let mut key = [0_u8; STATE_KEY_LEN];
    key.copy_from_slice(digest.as_ref());
    key
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    let mut context = DigestContext::new(&SHA256);
    context.update(bytes);
    encode_hex(context.finish().as_ref())
}

pub(crate) fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|value| format!("{value:02x}")).collect::<String>()
}
