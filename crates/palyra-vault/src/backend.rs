use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use base64::{engine::general_purpose::STANDARD_NO_PAD, Engine as _};
use ulid::Ulid;

use crate::{ensure_owner_only_dir, ensure_owner_only_file, VaultError};

const BACKEND_MARKER_FILE: &str = "backend.kind";
const OBJECTS_DIR: &str = "objects";
#[cfg(target_os = "macos")]
const KEYCHAIN_SERVICE_NAME: &str = "palyra.vault.v1";
#[cfg(target_os = "linux")]
const SECRET_TOOL_SERVICE_ATTR: &str = "service";
#[cfg(target_os = "linux")]
const SECRET_TOOL_SERVICE_NAME: &str = "palyra.vault.v1";
#[cfg(target_os = "linux")]
const SECRET_TOOL_KEY_ATTR: &str = "key";
#[cfg(windows)]
const WINDOWS_DPAPI_OBJECTS_DIR: &str = "objects_dpapi";

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendKind {
    EncryptedFile,
    #[cfg(target_os = "macos")]
    MacosKeychain,
    #[cfg(target_os = "linux")]
    LinuxSecretService,
    #[cfg(windows)]
    WindowsDpapi,
}

impl BackendKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::EncryptedFile => "encrypted_file",
            #[cfg(target_os = "macos")]
            Self::MacosKeychain => "macos_keychain",
            #[cfg(target_os = "linux")]
            Self::LinuxSecretService => "linux_secret_service",
            #[cfg(windows)]
            Self::WindowsDpapi => "windows_dpapi",
        }
    }

    pub fn parse(raw: &str) -> Result<Self, VaultError> {
        let normalized = raw.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "encrypted_file" => Ok(Self::EncryptedFile),
            #[cfg(target_os = "macos")]
            "macos_keychain" => Ok(Self::MacosKeychain),
            #[cfg(target_os = "linux")]
            "linux_secret_service" => Ok(Self::LinuxSecretService),
            #[cfg(windows)]
            "windows_dpapi" => Ok(Self::WindowsDpapi),
            _ => Err(VaultError::BackendUnavailable(format!(
                "unsupported vault backend kind marker '{raw}'"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendPreference {
    Auto,
    EncryptedFile,
}

pub trait BlobBackend: Send + Sync {
    fn kind(&self) -> BackendKind;
    fn put_blob(&self, object_id: &str, payload: &[u8]) -> Result<(), VaultError>;
    fn get_blob(&self, object_id: &str) -> Result<Vec<u8>, VaultError>;
    fn delete_blob(&self, object_id: &str) -> Result<(), VaultError>;
}

pub fn select_backend(
    root: &Path,
    preference: BackendPreference,
) -> Result<Box<dyn BlobBackend>, VaultError> {
    ensure_owner_only_dir(root)?;
    let marker_path = root.join(BACKEND_MARKER_FILE);
    if marker_path.exists() {
        let marker = fs::read_to_string(&marker_path).map_err(|error| {
            VaultError::Io(format!(
                "failed to read backend marker {}: {error}",
                marker_path.display()
            ))
        })?;
        let kind = BackendKind::parse(marker.trim())?;
        let backend = backend_for_kind(kind, root)?;
        return Ok(backend);
    }

    let backend = match preference {
        BackendPreference::EncryptedFile => backend_for_kind(BackendKind::EncryptedFile, root)?,
        BackendPreference::Auto => choose_auto_backend(root)?,
    };
    let marker_tmp = marker_path.with_extension(format!("tmp.{}", Ulid::new()));
    fs::write(&marker_tmp, backend.kind().as_str().as_bytes()).map_err(|error| {
        VaultError::Io(format!("failed to write backend marker {}: {error}", marker_tmp.display()))
    })?;
    ensure_owner_only_file(&marker_tmp)?;
    fs::rename(&marker_tmp, &marker_path).map_err(|error| {
        VaultError::Io(format!(
            "failed to finalize backend marker {}: {error}",
            marker_path.display()
        ))
    })?;
    ensure_owner_only_file(&marker_path)?;
    Ok(backend)
}

fn choose_auto_backend(root: &Path) -> Result<Box<dyn BlobBackend>, VaultError> {
    #[cfg(target_os = "macos")]
    {
        if MacosKeychainBackend::is_available() {
            return Ok(Box::new(MacosKeychainBackend::new()));
        }
    }

    #[cfg(target_os = "linux")]
    {
        if LinuxSecretServiceBackend::is_available() {
            return Ok(Box::new(LinuxSecretServiceBackend::new()));
        }
    }

    #[cfg(windows)]
    {
        if WindowsDpapiBackend::is_available() {
            return Ok(Box::new(WindowsDpapiBackend::new(root)?));
        }
    }

    backend_for_kind(BackendKind::EncryptedFile, root)
}

fn backend_for_kind(kind: BackendKind, root: &Path) -> Result<Box<dyn BlobBackend>, VaultError> {
    match kind {
        BackendKind::EncryptedFile => Ok(Box::new(EncryptedFileBackend::new(root)?)),
        #[cfg(target_os = "macos")]
        BackendKind::MacosKeychain => {
            if !MacosKeychainBackend::is_available() {
                return Err(VaultError::BackendUnavailable(
                    "macOS keychain backend is unavailable".to_owned(),
                ));
            }
            Ok(Box::new(MacosKeychainBackend::new()))
        }
        #[cfg(target_os = "linux")]
        BackendKind::LinuxSecretService => {
            if !LinuxSecretServiceBackend::is_available() {
                return Err(VaultError::BackendUnavailable(
                    "linux secret service backend is unavailable".to_owned(),
                ));
            }
            Ok(Box::new(LinuxSecretServiceBackend::new()))
        }
        #[cfg(windows)]
        BackendKind::WindowsDpapi => {
            if !WindowsDpapiBackend::is_available() {
                return Err(VaultError::BackendUnavailable(
                    "windows DPAPI backend is unavailable".to_owned(),
                ));
            }
            Ok(Box::new(WindowsDpapiBackend::new(root)?))
        }
    }
}

#[derive(Debug, Clone)]
struct EncryptedFileBackend {
    objects_root: PathBuf,
}

impl EncryptedFileBackend {
    fn new(root: &Path) -> Result<Self, VaultError> {
        let objects_root = root.join(OBJECTS_DIR);
        ensure_owner_only_dir(&objects_root)?;
        Ok(Self { objects_root })
    }

    fn object_path(&self, object_id: &str) -> Result<PathBuf, VaultError> {
        if object_id.is_empty()
            || !object_id
                .chars()
                .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '_' | '-'))
        {
            return Err(VaultError::InvalidObjectId(
                "object id must only contain lowercase alnum, '_' or '-'".to_owned(),
            ));
        }
        Ok(self.objects_root.join(object_id))
    }
}

impl BlobBackend for EncryptedFileBackend {
    fn kind(&self) -> BackendKind {
        BackendKind::EncryptedFile
    }

    fn put_blob(&self, object_id: &str, payload: &[u8]) -> Result<(), VaultError> {
        let path = self.object_path(object_id)?;
        let tmp_path = path.with_extension(format!("tmp.{}", Ulid::new()));
        let mut file =
            fs::OpenOptions::new().create_new(true).write(true).open(&tmp_path).map_err(
                |error| {
                    VaultError::Io(format!(
                        "failed to create encrypted-file temporary object {}: {error}",
                        tmp_path.display()
                    ))
                },
            )?;
        ensure_owner_only_file(&tmp_path)?;
        file.write_all(payload).map_err(|error| {
            VaultError::Io(format!(
                "failed to write encrypted-file object {}: {error}",
                tmp_path.display()
            ))
        })?;
        file.sync_all().map_err(|error| {
            VaultError::Io(format!(
                "failed to fsync encrypted-file object {}: {error}",
                tmp_path.display()
            ))
        })?;
        fs::rename(&tmp_path, &path).map_err(|error| {
            VaultError::Io(format!(
                "failed to finalize encrypted-file object {}: {error}",
                path.display()
            ))
        })?;
        ensure_owner_only_file(&path)?;
        Ok(())
    }

    fn get_blob(&self, object_id: &str) -> Result<Vec<u8>, VaultError> {
        let path = self.object_path(object_id)?;
        fs::read(&path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                VaultError::NotFound
            } else {
                VaultError::Io(format!(
                    "failed to read encrypted-file object {}: {error}",
                    path.display()
                ))
            }
        })
    }

    fn delete_blob(&self, object_id: &str) -> Result<(), VaultError> {
        let path = self.object_path(object_id)?;
        if !path.exists() {
            return Ok(());
        }
        fs::remove_file(&path).map_err(|error| {
            VaultError::Io(format!(
                "failed to delete encrypted-file object {}: {error}",
                path.display()
            ))
        })
    }
}

#[cfg(target_os = "macos")]
#[derive(Debug, Default, Clone)]
struct MacosKeychainBackend;

#[cfg(target_os = "macos")]
impl MacosKeychainBackend {
    fn new() -> Self {
        Self
    }

    fn is_available() -> bool {
        Command::new("security")
            .arg("-h")
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
    }
}

#[cfg(target_os = "macos")]
fn keychain_add_args<'a>(object_id: &'a str) -> [&'a str; 7] {
    ["add-generic-password", "-U", "-a", object_id, "-s", KEYCHAIN_SERVICE_NAME, "-w"]
}

#[cfg(target_os = "macos")]
impl BlobBackend for MacosKeychainBackend {
    fn kind(&self) -> BackendKind {
        BackendKind::MacosKeychain
    }

    fn put_blob(&self, object_id: &str, payload: &[u8]) -> Result<(), VaultError> {
        let encoded = STANDARD_NO_PAD.encode(payload);
        let mut child = Command::new("security")
            .args(keychain_add_args(object_id))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| {
                VaultError::Io(format!("failed to execute security add-generic-password: {error}"))
            })?;
        {
            let mut stdin = child.stdin.take().ok_or_else(|| {
                VaultError::Io("security add-generic-password did not expose stdin".to_owned())
            })?;
            stdin.write_all(encoded.as_bytes()).map_err(|error| {
                VaultError::Io(format!(
                    "failed to write security add-generic-password payload: {error}"
                ))
            })?;
        }
        let output = child.wait_with_output().map_err(|error| {
            VaultError::Io(format!("failed waiting for security add-generic-password: {error}"))
        })?;
        if !output.status.success() {
            return Err(VaultError::Io(format!(
                "security add-generic-password failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        Ok(())
    }

    fn get_blob(&self, object_id: &str) -> Result<Vec<u8>, VaultError> {
        let output = Command::new("security")
            .args(["find-generic-password", "-w", "-a", object_id, "-s", KEYCHAIN_SERVICE_NAME])
            .output()
            .map_err(|error| {
                VaultError::Io(format!("failed to execute security find-generic-password: {error}"))
            })?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).to_ascii_lowercase();
            if stderr.contains("could not be found") {
                return Err(VaultError::NotFound);
            }
            return Err(VaultError::Io(format!(
                "security find-generic-password failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        let encoded = String::from_utf8(output.stdout)
            .map_err(|error| {
                VaultError::Io(format!("keychain returned non-UTF8 payload: {error}"))
            })?
            .trim()
            .to_owned();
        STANDARD_NO_PAD
            .decode(encoded.as_bytes())
            .map_err(|error| VaultError::Io(format!("failed to decode keychain payload: {error}")))
    }

    fn delete_blob(&self, object_id: &str) -> Result<(), VaultError> {
        let output = Command::new("security")
            .args(["delete-generic-password", "-a", object_id, "-s", KEYCHAIN_SERVICE_NAME])
            .output()
            .map_err(|error| {
                VaultError::Io(format!(
                    "failed to execute security delete-generic-password: {error}"
                ))
            })?;
        if output.status.success() {
            return Ok(());
        }
        let stderr = String::from_utf8_lossy(&output.stderr).to_ascii_lowercase();
        if stderr.contains("could not be found") {
            return Ok(());
        }
        Err(VaultError::Io(format!(
            "security delete-generic-password failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )))
    }
}

#[cfg(all(test, target_os = "macos"))]
mod macos_tests {
    use super::keychain_add_args;

    #[test]
    fn keychain_add_args_keep_password_out_of_argv() {
        let args = keychain_add_args("obj_123");
        assert_eq!(args[0], "add-generic-password");
        assert_eq!(args[3], "obj_123");
        let password_flag_index =
            args.iter().position(|arg| *arg == "-w").expect("password flag should exist");
        assert_eq!(
            password_flag_index,
            args.len() - 1,
            "password must be supplied via stdin, not argv"
        );
    }
}

#[cfg(target_os = "linux")]
#[derive(Debug, Default, Clone)]
struct LinuxSecretServiceBackend;

#[cfg(target_os = "linux")]
impl LinuxSecretServiceBackend {
    fn new() -> Self {
        Self
    }

    fn is_available() -> bool {
        Command::new("secret-tool")
            .arg("--help")
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
    }
}

#[cfg(target_os = "linux")]
fn secret_tool_stderr_is_not_found(stderr: &[u8]) -> bool {
    let normalized = String::from_utf8_lossy(stderr).to_ascii_lowercase();
    normalized.contains("not found")
        || normalized.contains("no such secret")
        || normalized.contains("no such item")
        || normalized.contains("could not be found")
}

#[cfg(target_os = "linux")]
impl BlobBackend for LinuxSecretServiceBackend {
    fn kind(&self) -> BackendKind {
        BackendKind::LinuxSecretService
    }

    fn put_blob(&self, object_id: &str, payload: &[u8]) -> Result<(), VaultError> {
        let encoded = STANDARD_NO_PAD.encode(payload);
        let mut child = Command::new("secret-tool")
            .args([
                "store",
                "--label",
                "Palyra Vault Secret",
                SECRET_TOOL_SERVICE_ATTR,
                SECRET_TOOL_SERVICE_NAME,
                SECRET_TOOL_KEY_ATTR,
                object_id,
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| {
                VaultError::Io(format!("failed to execute secret-tool store: {error}"))
            })?;
        let stdin = child
            .stdin
            .as_mut()
            .ok_or_else(|| VaultError::Io("secret-tool store did not expose stdin".to_owned()))?;
        stdin.write_all(encoded.as_bytes()).map_err(|error| {
            VaultError::Io(format!("failed to write secret-tool store payload: {error}"))
        })?;
        let output = child.wait_with_output().map_err(|error| {
            VaultError::Io(format!("failed waiting for secret-tool store: {error}"))
        })?;
        if !output.status.success() {
            return Err(VaultError::Io(format!(
                "secret-tool store failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        Ok(())
    }

    fn get_blob(&self, object_id: &str) -> Result<Vec<u8>, VaultError> {
        let output = Command::new("secret-tool")
            .args([
                "lookup",
                SECRET_TOOL_SERVICE_ATTR,
                SECRET_TOOL_SERVICE_NAME,
                SECRET_TOOL_KEY_ATTR,
                object_id,
            ])
            .output()
            .map_err(|error| {
                VaultError::Io(format!("failed to execute secret-tool lookup: {error}"))
            })?;
        if !output.status.success() {
            if secret_tool_stderr_is_not_found(output.stderr.as_slice()) {
                return Err(VaultError::NotFound);
            }
            return Err(VaultError::Io(format!(
                "secret-tool lookup failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        let encoded = String::from_utf8(output.stdout)
            .map_err(|error| {
                VaultError::Io(format!("secret-tool returned non-UTF8 payload: {error}"))
            })?
            .trim()
            .to_owned();
        STANDARD_NO_PAD.decode(encoded.as_bytes()).map_err(|error| {
            VaultError::Io(format!("failed to decode secret-tool payload: {error}"))
        })
    }

    fn delete_blob(&self, object_id: &str) -> Result<(), VaultError> {
        let output = Command::new("secret-tool")
            .args([
                "clear",
                SECRET_TOOL_SERVICE_ATTR,
                SECRET_TOOL_SERVICE_NAME,
                SECRET_TOOL_KEY_ATTR,
                object_id,
            ])
            .output()
            .map_err(|error| {
                VaultError::Io(format!("failed to execute secret-tool clear: {error}"))
            })?;
        if output.status.success() {
            return Ok(());
        }
        if secret_tool_stderr_is_not_found(output.stderr.as_slice()) {
            return Ok(());
        }
        Err(VaultError::Io(format!(
            "secret-tool clear failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )))
    }
}

#[cfg(all(test, target_os = "linux"))]
mod linux_tests {
    use super::secret_tool_stderr_is_not_found;

    #[test]
    fn secret_tool_not_found_detection_matches_expected_phrases() {
        assert!(secret_tool_stderr_is_not_found(
            b"No such secret item at path /org/freedesktop/secrets"
        ));
        assert!(secret_tool_stderr_is_not_found(b"could not be found"));
        assert!(secret_tool_stderr_is_not_found(b"NOT FOUND"));
    }

    #[test]
    fn secret_tool_not_found_detection_ignores_unrelated_failures() {
        assert!(!secret_tool_stderr_is_not_found(b"Cannot autolaunch D-Bus without X11 $DISPLAY"));
    }
}

#[cfg(windows)]
#[derive(Debug, Clone)]
struct WindowsDpapiBackend {
    objects_root: PathBuf,
}

#[cfg(windows)]
impl WindowsDpapiBackend {
    fn new(root: &Path) -> Result<Self, VaultError> {
        let objects_root = root.join(WINDOWS_DPAPI_OBJECTS_DIR);
        ensure_owner_only_dir(&objects_root)?;
        Ok(Self { objects_root })
    }

    fn is_available() -> bool {
        Command::new("powershell")
            .args(["-NoProfile", "-NonInteractive", "-Command", "$PSVersionTable.PSVersion.Major"])
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
    }

    fn object_path(&self, object_id: &str) -> Result<PathBuf, VaultError> {
        if object_id.is_empty()
            || !object_id
                .chars()
                .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '_' | '-'))
        {
            return Err(VaultError::InvalidObjectId(
                "object id must only contain lowercase alnum, '_' or '-'".to_owned(),
            ));
        }
        Ok(self.objects_root.join(object_id))
    }
}

#[cfg(windows)]
impl BlobBackend for WindowsDpapiBackend {
    fn kind(&self) -> BackendKind {
        BackendKind::WindowsDpapi
    }

    fn put_blob(&self, object_id: &str, payload: &[u8]) -> Result<(), VaultError> {
        let protected = dpapi_protect(payload)?;
        let path = self.object_path(object_id)?;
        let tmp_path = path.with_extension(format!("tmp.{}", Ulid::new()));
        fs::write(&tmp_path, protected).map_err(|error| {
            VaultError::Io(format!("failed to write DPAPI object {}: {error}", tmp_path.display()))
        })?;
        ensure_owner_only_file(&tmp_path)?;
        fs::rename(&tmp_path, &path).map_err(|error| {
            VaultError::Io(format!("failed to finalize DPAPI object {}: {error}", path.display()))
        })?;
        ensure_owner_only_file(&path)?;
        Ok(())
    }

    fn get_blob(&self, object_id: &str) -> Result<Vec<u8>, VaultError> {
        let path = self.object_path(object_id)?;
        let protected = fs::read(&path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                VaultError::NotFound
            } else {
                VaultError::Io(format!("failed to read DPAPI object {}: {error}", path.display()))
            }
        })?;
        dpapi_unprotect(protected.as_slice())
    }

    fn delete_blob(&self, object_id: &str) -> Result<(), VaultError> {
        let path = self.object_path(object_id)?;
        if path.exists() {
            fs::remove_file(&path).map_err(|error| {
                VaultError::Io(format!("failed to delete DPAPI object {}: {error}", path.display()))
            })?;
        }
        Ok(())
    }
}

#[cfg(windows)]
fn dpapi_protect(raw: &[u8]) -> Result<Vec<u8>, VaultError> {
    let script = "$inputB64=[Console]::In.ReadToEnd();$bytes=[Convert]::FromBase64String($inputB64);$out=[System.Security.Cryptography.ProtectedData]::Protect($bytes,$null,[System.Security.Cryptography.DataProtectionScope]::CurrentUser);[Console]::Out.Write([Convert]::ToBase64String($out));";
    let input = STANDARD_NO_PAD.encode(raw);
    run_powershell_dpapi(script, input.as_bytes())
}

#[cfg(windows)]
fn dpapi_unprotect(protected: &[u8]) -> Result<Vec<u8>, VaultError> {
    let script = "$inputB64=[Console]::In.ReadToEnd();$bytes=[Convert]::FromBase64String($inputB64);$out=[System.Security.Cryptography.ProtectedData]::Unprotect($bytes,$null,[System.Security.Cryptography.DataProtectionScope]::CurrentUser);[Console]::Out.Write([Convert]::ToBase64String($out));";
    let input = STANDARD_NO_PAD.encode(protected);
    run_powershell_dpapi(script, input.as_bytes())
}

#[cfg(windows)]
fn run_powershell_dpapi(script: &str, stdin_payload: &[u8]) -> Result<Vec<u8>, VaultError> {
    let mut child = Command::new("powershell")
        .args(["-NoProfile", "-NonInteractive", "-Command", script])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| {
            VaultError::Io(format!("failed to execute powershell DPAPI command: {error}"))
        })?;
    let stdin = child.stdin.as_mut().ok_or_else(|| {
        VaultError::Io("powershell DPAPI command did not expose stdin".to_owned())
    })?;
    stdin.write_all(stdin_payload).map_err(|error| {
        VaultError::Io(format!("failed to write powershell DPAPI stdin: {error}"))
    })?;
    let output = child.wait_with_output().map_err(|error| {
        VaultError::Io(format!("failed waiting for powershell DPAPI command: {error}"))
    })?;
    if !output.status.success() {
        return Err(VaultError::Io(format!(
            "powershell DPAPI command failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let encoded = String::from_utf8(output.stdout)
        .map_err(|error| {
            VaultError::Io(format!("powershell DPAPI command output was non-UTF8: {error}"))
        })?
        .trim()
        .to_owned();
    STANDARD_NO_PAD.decode(encoded.as_bytes()).map_err(|error| {
        VaultError::Io(format!("failed to decode powershell DPAPI output: {error}"))
    })
}
