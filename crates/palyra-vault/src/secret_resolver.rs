use std::{
    collections::BTreeSet,
    env, fs,
    fs::OpenOptions,
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

use palyra_common::secret_refs::{SecretRef, SecretRefRedactedView, SecretSource};
use serde::Serialize;
use ulid::Ulid;

use crate::{
    canonicalize_existing_dir, current_unix_ms, ensure_path_within_root, SensitiveBytes, Vault,
    VaultError, VaultRef,
};

const DEFAULT_EXEC_TIMEOUT_MS: u64 = 1_000;
const EXEC_POLL_INTERVAL_MS: u64 = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SecretResolutionStatus {
    Resolved,
    Missing,
    Blocked,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretResolveErrorKind {
    Missing,
    InvalidReference,
    PolicyBlocked,
    Io,
    TooLarge,
    Timeout,
    ExecFailed,
    DecodeFailed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SecretResolutionMetadata {
    pub status: SecretResolutionStatus,
    pub fingerprint: String,
    pub source_kind: String,
    pub required: bool,
    pub resolved_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value_bytes: Option<usize>,
    pub source: SecretRefRedactedView,
}

pub struct SecretResolution {
    pub metadata: SecretResolutionMetadata,
    pub value: Option<SensitiveBytes>,
}

impl std::fmt::Debug for SecretResolution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecretResolution")
            .field("metadata", &self.metadata)
            .field("value_present", &self.value.is_some())
            .finish()
    }
}

impl SecretResolution {
    pub fn require_bytes(self) -> Result<SensitiveBytes, SecretResolveError> {
        let SecretResolution { metadata, value } = self;
        match value {
            Some(value) => Ok(value),
            None => Err(SecretResolveError {
                kind: SecretResolveErrorKind::Missing,
                message: "secret value is not present in the current snapshot".to_owned(),
                metadata: Box::new(SecretResolutionMetadata {
                    status: SecretResolutionStatus::Missing,
                    ..metadata
                }),
            }),
        }
    }

    pub fn decode_utf8(self, context: &str) -> Result<String, SecretResolveError> {
        let SecretResolution { metadata, value } = self;
        let Some(value) = value else {
            return Err(SecretResolveError {
                kind: SecretResolveErrorKind::Missing,
                message: "secret value is not present in the current snapshot".to_owned(),
                metadata: Box::new(SecretResolutionMetadata {
                    status: SecretResolutionStatus::Missing,
                    ..metadata
                }),
            });
        };
        let decoded =
            String::from_utf8(value.as_ref().to_vec()).map_err(|error| SecretResolveError {
                kind: SecretResolveErrorKind::DecodeFailed,
                message: format!("{context} must be valid UTF-8: {error}"),
                metadata: Box::new(SecretResolutionMetadata {
                    status: SecretResolutionStatus::Failed,
                    value_bytes: Some(value.as_ref().len()),
                    ..metadata.clone()
                }),
            })?;
        if decoded.trim().is_empty() {
            return Err(SecretResolveError {
                kind: SecretResolveErrorKind::Missing,
                message: format!("{context} resolved to an empty value"),
                metadata: Box::new(SecretResolutionMetadata {
                    status: SecretResolutionStatus::Missing,
                    value_bytes: Some(0),
                    ..metadata
                }),
            });
        }
        Ok(decoded)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretResolveError {
    pub kind: SecretResolveErrorKind,
    pub message: String,
    pub metadata: Box<SecretResolutionMetadata>,
}

impl std::fmt::Display for SecretResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message.as_str())
    }
}

impl std::error::Error for SecretResolveError {}

pub struct SecretResolver<'a> {
    vault: Option<&'a Vault>,
    working_dir: PathBuf,
    default_exec_timeout: Duration,
}

impl<'a> SecretResolver<'a> {
    pub fn with_working_dir(vault: Option<&'a Vault>, working_dir: impl Into<PathBuf>) -> Self {
        Self {
            vault,
            working_dir: working_dir.into(),
            default_exec_timeout: Duration::from_millis(DEFAULT_EXEC_TIMEOUT_MS),
        }
    }

    pub fn resolve(&self, secret_ref: &SecretRef) -> Result<SecretResolution, SecretResolveError> {
        secret_ref.validate().map_err(|error| {
            self.invalid_reference(secret_ref, format!("invalid secret reference: {error}"))
        })?;

        match &secret_ref.source {
            SecretSource::Vault { vault_ref } => self.resolve_vault(secret_ref, vault_ref.as_str()),
            SecretSource::Env { variable } => self.resolve_env(secret_ref, variable.as_str()),
            SecretSource::File { path, trusted_dirs, allow_symlinks } => self.resolve_file(
                secret_ref,
                path.as_str(),
                trusted_dirs.as_slice(),
                *allow_symlinks,
            ),
            SecretSource::Exec { command, inherited_env, cwd } => self.resolve_exec(
                secret_ref,
                command.as_slice(),
                inherited_env.as_slice(),
                cwd.as_deref(),
            ),
        }
    }

    fn resolve_vault(
        &self,
        secret_ref: &SecretRef,
        raw_vault_ref: &str,
    ) -> Result<SecretResolution, SecretResolveError> {
        let Some(vault) = self.vault else {
            return Err(self.blocked(
                secret_ref,
                "vault-backed secret resolution is unavailable because no vault runtime is configured",
            ));
        };
        let parsed = VaultRef::parse(raw_vault_ref).map_err(|error| {
            self.invalid_reference(
                secret_ref,
                format!("vault-backed secret reference is invalid: {error}"),
            )
        })?;
        match vault.get_secret(&parsed.scope, parsed.key.as_str()) {
            Ok(value) => self.resolved(secret_ref, value),
            Err(VaultError::NotFound) => self.missing_or_error(
                secret_ref,
                "vault-backed secret is missing from the configured vault scope",
            ),
            Err(error) => Err(self
                .io_error(secret_ref, format!("vault-backed secret could not be loaded: {error}"))),
        }
    }

    fn resolve_env(
        &self,
        secret_ref: &SecretRef,
        variable: &str,
    ) -> Result<SecretResolution, SecretResolveError> {
        match env::var(variable) {
            Ok(value) if !value.trim().is_empty() => self.resolved(secret_ref, value.into_bytes()),
            Ok(_) | Err(env::VarError::NotPresent) => {
                self.missing_or_error(secret_ref, "environment-backed secret is missing or empty")
            }
            Err(env::VarError::NotUnicode(_)) => Err(self.failed(
                secret_ref,
                SecretResolveErrorKind::DecodeFailed,
                "environment-backed secret is not valid Unicode text",
            )),
        }
    }

    fn resolve_file(
        &self,
        secret_ref: &SecretRef,
        raw_path: &str,
        trusted_dirs: &[String],
        allow_symlinks: bool,
    ) -> Result<SecretResolution, SecretResolveError> {
        let path = self.resolve_relative_path(raw_path);
        if !path.exists() {
            return self.missing_or_error(secret_ref, "file-backed secret path does not exist");
        }
        if path.is_dir() {
            return Err(self.blocked(
                secret_ref,
                "file-backed secret path must point to a file, not a directory",
            ));
        }

        let trusted_roots = trusted_dirs
            .iter()
            .map(|trusted_dir| {
                let candidate = self.resolve_relative_path(trusted_dir.as_str());
                canonicalize_existing_dir(candidate.as_path(), "secret resolver trusted directory")
                    .map_err(|error| {
                        self.blocked(
                            secret_ref,
                            format!("trusted secret directory is invalid: {error}"),
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let canonical = fs::canonicalize(&path).map_err(|error| {
            self.io_error(
                secret_ref,
                format!(
                    "failed to canonicalize file-backed secret path {}: {error}",
                    path.display()
                ),
            )
        })?;
        let within_trusted_root = trusted_roots.iter().any(|root| {
            ensure_path_within_root(root.as_path(), canonical.as_path(), "secret path").is_ok()
        });
        if !within_trusted_root {
            return Err(self.blocked(
                secret_ref,
                "file-backed secret path escapes the configured trusted directories",
            ));
        }
        if !allow_symlinks
            && path_contains_symlink(path.as_path()).map_err(|error| {
                self.io_error(
                    secret_ref,
                    format!("failed to inspect file-backed secret path for symlinks: {error}"),
                )
            })?
        {
            return Err(self.blocked(
                secret_ref,
                "file-backed secret path contains a symlink but symlinks are disabled",
            ));
        }

        let bytes = fs::read(&canonical).map_err(|error| {
            self.io_error(
                secret_ref,
                format!("failed to read file-backed secret {}: {error}", canonical.display()),
            )
        })?;
        self.resolved(secret_ref, bytes)
    }

    fn resolve_exec(
        &self,
        secret_ref: &SecretRef,
        command: &[String],
        inherited_env: &[String],
        cwd: Option<&str>,
    ) -> Result<SecretResolution, SecretResolveError> {
        let timeout = Duration::from_millis(secret_ref.exec_timeout_ms.unwrap_or(
            self.default_exec_timeout.as_millis().try_into().unwrap_or(DEFAULT_EXEC_TIMEOUT_MS),
        ));
        let current_dir = cwd
            .map(|value| self.resolve_relative_path(value))
            .unwrap_or_else(|| self.working_dir.clone());
        if !current_dir.exists() || !current_dir.is_dir() {
            return Err(self.blocked(
                secret_ref,
                "exec-backed secret working directory must exist and be a directory",
            ));
        }

        let stdout_path = temp_output_path("stdout");
        let stderr_path = temp_output_path("stderr");
        let stdout =
            OpenOptions::new().create_new(true).write(true).open(stdout_path.as_path()).map_err(
                |error| {
                    self.io_error(
                        secret_ref,
                        format!("failed to create exec-backed secret stdout capture file: {error}"),
                    )
                },
            )?;
        let stderr =
            OpenOptions::new().create_new(true).write(true).open(stderr_path.as_path()).map_err(
                |error| {
                    let _ = fs::remove_file(stdout_path.as_path());
                    self.io_error(
                        secret_ref,
                        format!("failed to create exec-backed secret stderr capture file: {error}"),
                    )
                },
            )?;

        let mut process =
            Command::new(command.first().expect("validated exec command is non-empty"));
        process
            .args(&command[1..])
            .current_dir(current_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .env_clear();
        for variable in default_exec_inherited_env()
            .into_iter()
            .chain(inherited_env.iter().map(String::as_str))
            .collect::<BTreeSet<_>>()
        {
            if let Ok(value) = env::var(variable) {
                process.env(variable, value);
            }
        }

        let mut child = process.spawn().map_err(|error| {
            cleanup_temp_exec_outputs(stdout_path.as_path(), stderr_path.as_path());
            self.io_error(
                secret_ref,
                format!("failed to spawn exec-backed secret command: {error}"),
            )
        })?;
        let start = Instant::now();
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) => {
                    if start.elapsed() >= timeout {
                        let _ = child.kill();
                        let _ = child.wait();
                        cleanup_temp_exec_outputs(stdout_path.as_path(), stderr_path.as_path());
                        return Err(self.failed(
                            secret_ref,
                            SecretResolveErrorKind::Timeout,
                            format!(
                                "exec-backed secret timed out after {} ms",
                                timeout.as_millis()
                            ),
                        ));
                    }
                    thread::sleep(Duration::from_millis(EXEC_POLL_INTERVAL_MS));
                }
                Err(error) => {
                    cleanup_temp_exec_outputs(stdout_path.as_path(), stderr_path.as_path());
                    return Err(self.io_error(
                        secret_ref,
                        format!("failed while waiting for exec-backed secret command: {error}"),
                    ));
                }
            }
        };
        let stdout_bytes = read_limited_file(
            stdout_path.as_path(),
            secret_ref.effective_max_bytes(),
        )
        .map_err(|error| match error {
            ReadLimitedFileError::TooLarge { actual_bytes, max_bytes } => self.failed(
                secret_ref,
                SecretResolveErrorKind::TooLarge,
                format!("exec-backed secret exceeded max bytes ({actual_bytes} > {max_bytes})"),
            ),
            ReadLimitedFileError::Io(error) => self
                .io_error(secret_ref, format!("failed to read exec-backed secret stdout: {error}")),
        })?;
        let stderr_len =
            fs::metadata(stderr_path.as_path()).map(|metadata| metadata.len()).unwrap_or_default();
        cleanup_temp_exec_outputs(stdout_path.as_path(), stderr_path.as_path());

        if !status.success() {
            return Err(self.failed(
                secret_ref,
                SecretResolveErrorKind::ExecFailed,
                format!(
                    "exec-backed secret command exited unsuccessfully (status={}, stderr_bytes={stderr_len})",
                    render_exit_status(status)
                ),
            ));
        }

        if stdout_bytes.is_empty() {
            return self.missing_or_error(
                secret_ref,
                "exec-backed secret command produced an empty stdout payload",
            );
        }

        self.resolved(secret_ref, stdout_bytes)
    }

    fn resolved(
        &self,
        secret_ref: &SecretRef,
        value: Vec<u8>,
    ) -> Result<SecretResolution, SecretResolveError> {
        let max_bytes = usize::try_from(secret_ref.effective_max_bytes()).unwrap_or(usize::MAX);
        if value.len() > max_bytes {
            return Err(self.failed(
                secret_ref,
                SecretResolveErrorKind::TooLarge,
                format!(
                    "{}-backed secret exceeded max bytes ({} > {})",
                    secret_ref.source_kind(),
                    value.len(),
                    max_bytes
                ),
            ));
        }
        let metadata =
            self.metadata(secret_ref, SecretResolutionStatus::Resolved, Some(value.len()));
        Ok(SecretResolution { metadata, value: Some(SensitiveBytes::new(value)) })
    }

    fn missing_or_error(
        &self,
        secret_ref: &SecretRef,
        message: &str,
    ) -> Result<SecretResolution, SecretResolveError> {
        if secret_ref.required {
            return Err(self.failed(secret_ref, SecretResolveErrorKind::Missing, message));
        }
        Ok(SecretResolution {
            metadata: self.metadata(secret_ref, SecretResolutionStatus::Missing, None),
            value: None,
        })
    }

    fn invalid_reference(
        &self,
        secret_ref: &SecretRef,
        message: impl Into<String>,
    ) -> SecretResolveError {
        self.error(
            secret_ref,
            SecretResolveErrorKind::InvalidReference,
            SecretResolutionStatus::Failed,
            message,
        )
    }

    fn blocked(&self, secret_ref: &SecretRef, message: impl Into<String>) -> SecretResolveError {
        self.error(
            secret_ref,
            SecretResolveErrorKind::PolicyBlocked,
            SecretResolutionStatus::Blocked,
            message,
        )
    }

    fn io_error(&self, secret_ref: &SecretRef, message: impl Into<String>) -> SecretResolveError {
        self.error(secret_ref, SecretResolveErrorKind::Io, SecretResolutionStatus::Failed, message)
    }

    fn failed(
        &self,
        secret_ref: &SecretRef,
        kind: SecretResolveErrorKind,
        message: impl Into<String>,
    ) -> SecretResolveError {
        self.error(secret_ref, kind, SecretResolutionStatus::Failed, message)
    }

    fn error(
        &self,
        secret_ref: &SecretRef,
        kind: SecretResolveErrorKind,
        status: SecretResolutionStatus,
        message: impl Into<String>,
    ) -> SecretResolveError {
        SecretResolveError {
            kind,
            message: message.into(),
            metadata: Box::new(self.metadata(secret_ref, status, None)),
        }
    }

    fn metadata(
        &self,
        secret_ref: &SecretRef,
        status: SecretResolutionStatus,
        value_bytes: Option<usize>,
    ) -> SecretResolutionMetadata {
        SecretResolutionMetadata {
            status,
            fingerprint: secret_ref.fingerprint(),
            source_kind: secret_ref.source_kind().to_owned(),
            required: secret_ref.required,
            resolved_at_unix_ms: current_unix_ms().unwrap_or_default(),
            value_bytes,
            source: secret_ref.redacted_view(),
        }
    }

    fn resolve_relative_path(&self, raw: &str) -> PathBuf {
        let path = PathBuf::from(raw.trim());
        if path.is_absolute() {
            path
        } else {
            self.working_dir.join(path)
        }
    }
}

enum ReadLimitedFileError {
    Io(std::io::Error),
    TooLarge { actual_bytes: u64, max_bytes: u64 },
}

fn read_limited_file(path: &Path, max_bytes: u64) -> Result<Vec<u8>, ReadLimitedFileError> {
    let metadata = fs::metadata(path).map_err(ReadLimitedFileError::Io)?;
    if metadata.len() > max_bytes {
        return Err(ReadLimitedFileError::TooLarge { actual_bytes: metadata.len(), max_bytes });
    }
    fs::read(path).map_err(ReadLimitedFileError::Io)
}

fn cleanup_temp_exec_outputs(stdout_path: &Path, stderr_path: &Path) {
    let _ = fs::remove_file(stdout_path);
    let _ = fs::remove_file(stderr_path);
}

fn temp_output_path(label: &str) -> PathBuf {
    env::temp_dir().join(format!("palyra-secret-ref-{}-{label}.tmp", Ulid::new()))
}

fn render_exit_status(status: ExitStatus) -> String {
    status.code().map(|value| value.to_string()).unwrap_or_else(|| "terminated".to_owned())
}

fn path_contains_symlink(path: &Path) -> std::io::Result<bool> {
    let absolute =
        if path.is_absolute() { path.to_path_buf() } else { env::current_dir()?.join(path) };
    let mut current = PathBuf::new();
    for component in absolute.components() {
        current.push(component.as_os_str());
        match fs::symlink_metadata(current.as_path()) {
            Ok(metadata) if metadata.file_type().is_symlink() => return Ok(true),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => return Err(error),
        }
    }
    Ok(false)
}

fn default_exec_inherited_env() -> Vec<&'static str> {
    #[cfg(windows)]
    {
        vec!["PATH", "PATHEXT", "SYSTEMROOT", "WINDIR", "COMSPEC", "TEMP", "TMP"]
    }
    #[cfg(not(windows))]
    {
        vec!["PATH", "HOME", "LANG", "LC_ALL", "LC_CTYPE", "TMPDIR"]
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    #[cfg(unix)]
    use std::os::unix::fs as unix_fs;

    use palyra_common::secret_refs::{
        SecretRef, SecretRefreshPolicy, SecretSnapshotPolicy, SecretSource,
    };
    use tempfile::TempDir;
    use ulid::Ulid;

    use super::{SecretResolveErrorKind, SecretResolver};
    use crate::{BackendPreference, VaultConfig, VaultScope};

    fn temp_vault(dir: &TempDir) -> crate::Vault {
        crate::Vault::open_with_config(VaultConfig {
            root: Some(dir.path().join("vault")),
            identity_store_root: Some(dir.path().join("identity")),
            backend_preference: BackendPreference::EncryptedFile,
            max_secret_bytes: 64 * 1024,
        })
        .expect("vault should initialize for tests")
    }

    fn make_ref(source: SecretSource) -> SecretRef {
        SecretRef {
            source,
            required: true,
            refresh_policy: SecretRefreshPolicy::OnStartup,
            snapshot_policy: SecretSnapshotPolicy::FreezeUntilReload,
            max_bytes: Some(4096),
            exec_timeout_ms: None,
            redaction_label: Some("test.secret".to_owned()),
            display_name: Some("Test secret".to_owned()),
        }
    }
    #[cfg(windows)]
    fn oversized_exec_command() -> Vec<String> {
        vec!["cmd.exe".to_owned(), "/C".to_owned(), "echo 12345".to_owned()]
    }
    #[cfg(unix)]
    fn oversized_exec_command() -> Vec<String> {
        vec!["sh".to_owned(), "-c".to_owned(), "printf 12345".to_owned()]
    }
    #[test]
    fn resolves_env_source() {
        let key = format!("PALYRA_SECRET_REF_TEST_{}", Ulid::new());
        std::env::set_var(key.as_str(), "env-secret-value");
        let temp = TempDir::new().expect("temp dir should initialize");
        let resolver = SecretResolver::with_working_dir(None, temp.path());
        let resolution = resolver
            .resolve(&make_ref(SecretSource::Env { variable: key.clone() }))
            .expect("env-backed secret should resolve");
        assert_eq!(resolution.metadata.source_kind, "env");
        assert_eq!(
            String::from_utf8(
                resolution.require_bytes().expect("bytes should exist").as_ref().to_vec()
            )
            .expect("resolved env bytes should decode"),
            "env-secret-value"
        );
        std::env::remove_var(key.as_str());
    }
    #[test]
    fn resolves_vault_source() {
        let temp = TempDir::new().expect("temp dir should initialize");
        let vault = temp_vault(&temp);
        vault
            .put_secret(&VaultScope::Global, "openai_api_key", b"vault-secret")
            .expect("secret should store");
        let resolver = SecretResolver::with_working_dir(Some(&vault), temp.path());
        let resolution = resolver
            .resolve(&make_ref(SecretSource::Vault {
                vault_ref: "global/openai_api_key".to_owned(),
            }))
            .expect("vault-backed secret should resolve");
        assert_eq!(
            String::from_utf8(
                resolution.require_bytes().expect("bytes should exist").as_ref().to_vec()
            )
            .expect("vault bytes should decode"),
            "vault-secret"
        );
    }
    #[test]
    fn file_source_blocks_paths_outside_trusted_dirs() {
        let temp = TempDir::new().expect("temp dir should initialize");
        let trusted = temp.path().join("trusted");
        let outside = temp.path().join("outside");
        fs::create_dir_all(&trusted).expect("trusted dir should create");
        fs::create_dir_all(&outside).expect("outside dir should create");
        let secret_path = outside.join("secret.txt");
        fs::write(&secret_path, b"outside-secret").expect("secret file should write");

        let resolver = SecretResolver::with_working_dir(None, temp.path());
        let error = resolver
            .resolve(&make_ref(SecretSource::File {
                path: secret_path.to_string_lossy().into_owned(),
                trusted_dirs: vec![trusted.to_string_lossy().into_owned()],
                allow_symlinks: false,
            }))
            .expect_err("file-backed secret outside trusted dir must be blocked");
        assert_eq!(error.kind, SecretResolveErrorKind::PolicyBlocked);
    }
    #[cfg(unix)]
    #[test]
    fn file_source_blocks_symlink_when_symlinks_disabled() {
        let temp = TempDir::new().expect("temp dir should initialize");
        let trusted = temp.path().join("trusted");
        let outside = temp.path().join("outside");
        fs::create_dir_all(&trusted).expect("trusted dir should create");
        fs::create_dir_all(&outside).expect("outside dir should create");
        let outside_secret = outside.join("secret.txt");
        fs::write(&outside_secret, b"symlink-secret").expect("outside secret should write");
        let symlink_path = trusted.join("linked-secret.txt");
        unix_fs::symlink(&outside_secret, &symlink_path).expect("symlink should create");

        let resolver = SecretResolver::with_working_dir(None, temp.path());
        let error = resolver
            .resolve(&make_ref(SecretSource::File {
                path: symlink_path.to_string_lossy().into_owned(),
                trusted_dirs: vec![trusted.to_string_lossy().into_owned()],
                allow_symlinks: false,
            }))
            .expect_err("symlinked secret should be blocked");
        assert_eq!(error.kind, SecretResolveErrorKind::PolicyBlocked);
    }
    #[test]
    fn exec_source_uses_argv_without_shell_interpolation() {
        let temp = TempDir::new().expect("temp dir should initialize");
        let resolver = SecretResolver::with_working_dir(None, temp.path());
        let mut reference = make_ref(SecretSource::Exec {
            command: vec!["git --version && echo pwned".to_owned()],
            inherited_env: vec![],
            cwd: None,
        });
        reference.exec_timeout_ms = Some(500);
        let error =
            resolver.resolve(&reference).expect_err("extra argv should not execute a shell");
        assert!(matches!(
            error.kind,
            SecretResolveErrorKind::ExecFailed
                | SecretResolveErrorKind::Io
                | SecretResolveErrorKind::PolicyBlocked
        ));
    }
    #[test]
    fn exec_source_respects_max_bytes_limit() {
        let temp = TempDir::new().expect("temp dir should initialize");
        let resolver = SecretResolver::with_working_dir(None, temp.path());
        let mut reference = make_ref(SecretSource::Exec {
            command: oversized_exec_command(),
            inherited_env: vec![],
            cwd: None,
        });
        reference.max_bytes = Some(4);
        reference.exec_timeout_ms = Some(2_000);
        let error = resolver.resolve(&reference).expect_err("oversized exec stdout should fail");
        assert_eq!(error.kind, SecretResolveErrorKind::TooLarge);
    }
    #[cfg(windows)]
    #[test]
    fn exec_source_times_out() {
        let temp = TempDir::new().expect("temp dir should initialize");
        let resolver = SecretResolver::with_working_dir(None, temp.path());
        let mut reference = make_ref(SecretSource::Exec {
            command: vec![
                "powershell.exe".to_owned(),
                "-NoProfile".to_owned(),
                "-Command".to_owned(),
                "Start-Sleep -Seconds 2; Write-Output delayed".to_owned(),
            ],
            inherited_env: vec![],
            cwd: None,
        });
        reference.exec_timeout_ms = Some(50);
        let error = resolver.resolve(&reference).expect_err("slow exec should time out");
        assert_eq!(error.kind, SecretResolveErrorKind::Timeout);
    }
    #[cfg(unix)]
    #[test]
    fn exec_source_times_out() {
        let temp = TempDir::new().expect("temp dir should initialize");
        let resolver = SecretResolver::with_working_dir(None, temp.path());
        let mut reference = make_ref(SecretSource::Exec {
            command: vec!["sh".to_owned(), "-c".to_owned(), "sleep 2; printf delayed".to_owned()],
            inherited_env: vec![],
            cwd: None,
        });
        reference.exec_timeout_ms = Some(50);
        let error = resolver.resolve(&reference).expect_err("slow exec should time out");
        assert_eq!(error.kind, SecretResolveErrorKind::Timeout);
    }

    #[test]
    fn file_source_reads_relative_paths_against_working_dir() {
        let temp = TempDir::new().expect("temp dir should initialize");
        let trusted = temp.path().join("trusted");
        fs::create_dir_all(&trusted).expect("trusted dir should create");
        let secret_path = trusted.join("secret.txt");
        fs::write(&secret_path, b"file-secret").expect("secret file should write");

        let resolver = SecretResolver::with_working_dir(None, temp.path());
        let resolution = resolver
            .resolve(&make_ref(SecretSource::File {
                path: PathBuf::from("trusted/secret.txt").to_string_lossy().into_owned(),
                trusted_dirs: vec![PathBuf::from("trusted").to_string_lossy().into_owned()],
                allow_symlinks: true,
            }))
            .expect("file-backed secret should resolve");
        assert_eq!(
            String::from_utf8(
                resolution.require_bytes().expect("bytes should exist").as_ref().to_vec()
            )
            .expect("file bytes should decode"),
            "file-secret"
        );
    }
}
