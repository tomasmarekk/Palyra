use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::Serialize;
use sha2::{Digest as _, Sha256};

use super::contracts::DiagnosticsArtifactRefV2;
use super::LspDocumentSyncError;

const ARTIFACT_CONTENT_TYPE: &str = "application/vnd.palyra.lsp-diagnostics-v2+json";

pub(super) struct DiagnosticsArtifactStore {
    owner_root: PathBuf,
    max_artifacts: usize,
    max_artifact_bytes: usize,
}

impl DiagnosticsArtifactStore {
    pub(super) fn open(
        artifact_root: &Path,
        owner_id: &str,
        max_artifacts: usize,
        max_artifact_bytes: usize,
    ) -> Result<Self, LspDocumentSyncError> {
        if !artifact_root.is_absolute()
            || owner_id.trim().is_empty()
            || owner_id.len() > 256
            || owner_id.chars().any(char::is_control)
            || max_artifacts == 0
            || max_artifact_bytes == 0
        {
            return Err(LspDocumentSyncError::InvalidConfiguration);
        }
        create_private_dir(artifact_root)?;
        reject_link(artifact_root)?;
        let owner_root = artifact_root.join(sha256(owner_id.as_bytes()));
        create_private_dir(owner_root.as_path())?;
        reject_link(owner_root.as_path())?;
        Ok(Self { owner_root, max_artifacts, max_artifact_bytes })
    }

    pub(super) fn write<T: Serialize>(
        &self,
        kind: &str,
        value: &T,
    ) -> Result<DiagnosticsArtifactRefV2, LspDocumentSyncError> {
        validate_kind(kind)?;
        let existing_count = self.artifact_count()?;
        if existing_count >= self.max_artifacts {
            return Err(LspDocumentSyncError::ArtifactCapacityExhausted);
        }
        let bytes = serde_json::to_vec(value)
            .map_err(|error| LspDocumentSyncError::Persistence(error.to_string()))?;
        if bytes.is_empty() || bytes.len() > self.max_artifact_bytes {
            return Err(LspDocumentSyncError::ArtifactTooLarge);
        }
        let artifact_id = format!("diag_{kind}_{}", ulid::Ulid::new());
        let target = self.artifact_path(artifact_id.as_str())?;
        let temporary = self.owner_root.join(format!(".{artifact_id}.tmp"));
        let result = (|| {
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(temporary.as_path())
                .map_err(|error| LspDocumentSyncError::Persistence(error.to_string()))?;
            harden_file(temporary.as_path())?;
            file.write_all(bytes.as_slice())
                .and_then(|()| file.sync_all())
                .map_err(|error| LspDocumentSyncError::Persistence(error.to_string()))?;
            fs::rename(temporary.as_path(), target.as_path())
                .map_err(|error| LspDocumentSyncError::Persistence(error.to_string()))?;
            harden_file(target.as_path())
        })();
        if result.is_err() {
            let _ = fs::remove_file(temporary.as_path());
            let _ = fs::remove_file(target.as_path());
        }
        result?;
        Ok(DiagnosticsArtifactRefV2 {
            artifact_id,
            sha256: sha256(bytes.as_slice()),
            byte_count: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            content_type: ARTIFACT_CONTENT_TYPE.to_owned(),
        })
    }

    pub(super) fn read<T: DeserializeOwned>(
        &self,
        reference: &DiagnosticsArtifactRefV2,
    ) -> Result<T, LspDocumentSyncError> {
        if reference.content_type != ARTIFACT_CONTENT_TYPE
            || reference.byte_count == 0
            || reference.byte_count > u64::try_from(self.max_artifact_bytes).unwrap_or(u64::MAX)
        {
            return Err(LspDocumentSyncError::ArtifactIntegrity);
        }
        let path = self.artifact_path(reference.artifact_id.as_str())?;
        reject_link(path.as_path())?;
        let bytes = fs::read(path.as_path())
            .map_err(|error| LspDocumentSyncError::Persistence(error.to_string()))?;
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != reference.byte_count
            || sha256(bytes.as_slice()) != reference.sha256
        {
            return Err(LspDocumentSyncError::ArtifactIntegrity);
        }
        serde_json::from_slice(bytes.as_slice())
            .map_err(|_| LspDocumentSyncError::ArtifactIntegrity)
    }

    pub(super) fn remove_unreferenced(
        &self,
        retained_ids: &[String],
    ) -> Result<usize, LspDocumentSyncError> {
        let retained =
            retained_ids.iter().map(String::as_str).collect::<std::collections::BTreeSet<_>>();
        let mut removed = 0;
        for entry in fs::read_dir(self.owner_root.as_path())
            .map_err(|error| LspDocumentSyncError::Persistence(error.to_string()))?
        {
            let entry =
                entry.map_err(|error| LspDocumentSyncError::Persistence(error.to_string()))?;
            let name = entry.file_name().to_string_lossy().to_string();
            let Some(artifact_id) = name.strip_suffix(".json") else {
                continue;
            };
            if retained.contains(artifact_id) {
                continue;
            }
            reject_link(entry.path().as_path())?;
            fs::remove_file(entry.path())
                .map_err(|error| LspDocumentSyncError::Persistence(error.to_string()))?;
            removed += 1;
        }
        Ok(removed)
    }

    fn artifact_path(&self, artifact_id: &str) -> Result<PathBuf, LspDocumentSyncError> {
        if !artifact_id.starts_with("diag_")
            || artifact_id.len() > 128
            || !artifact_id.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '_' | '-')
            })
        {
            return Err(LspDocumentSyncError::ArtifactIntegrity);
        }
        Ok(self.owner_root.join(format!("{artifact_id}.json")))
    }

    fn artifact_count(&self) -> Result<usize, LspDocumentSyncError> {
        let mut count = 0_usize;
        for entry in fs::read_dir(self.owner_root.as_path())
            .map_err(|error| LspDocumentSyncError::Persistence(error.to_string()))?
        {
            let entry =
                entry.map_err(|error| LspDocumentSyncError::Persistence(error.to_string()))?;
            if entry.file_type().map(|kind| kind.is_file()).unwrap_or(false)
                && entry.path().extension().is_some_and(|extension| extension == "json")
            {
                count = count.saturating_add(1);
            }
        }
        Ok(count)
    }
}

fn validate_kind(kind: &str) -> Result<(), LspDocumentSyncError> {
    if kind.is_empty()
        || kind.len() > 32
        || !kind.chars().all(|character| character.is_ascii_lowercase() || character == '_')
    {
        return Err(LspDocumentSyncError::InvalidConfiguration);
    }
    Ok(())
}

fn create_private_dir(path: &Path) -> Result<(), LspDocumentSyncError> {
    fs::create_dir_all(path)
        .map_err(|error| LspDocumentSyncError::Persistence(error.to_string()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(|error| LspDocumentSyncError::Persistence(error.to_string()))?;
    }
    Ok(())
}

fn harden_file(_path: &Path) -> Result<(), LspDocumentSyncError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(_path, fs::Permissions::from_mode(0o600))
            .map_err(|error| LspDocumentSyncError::Persistence(error.to_string()))?;
    }
    Ok(())
}

fn reject_link(path: &Path) -> Result<(), LspDocumentSyncError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| LspDocumentSyncError::Persistence(error.to_string()))?;
    if metadata.file_type().is_symlink() {
        return Err(LspDocumentSyncError::ArtifactIntegrity);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt as _;
        use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(LspDocumentSyncError::ArtifactIntegrity);
        }
    }
    Ok(())
}

fn sha256(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}
