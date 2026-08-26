//! Bounded inspection of operator-selected ZIP archives.
//!
//! Backup verification and release inspection share these budgets so a
//! compressed archive cannot turn a one-shot CLI check into unbounded memory,
//! CPU, or metadata work.

use std::{
    collections::HashSet,
    fs,
    io::{Read, Seek, SeekFrom},
    path::Path,
};

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use zip::{read::ZipArchive, result::ZipError};

pub(crate) const MAX_ARCHIVE_MEMBER_BYTES: u64 = 512 * 1024 * 1024;
pub(crate) const MAX_BACKUP_MANIFEST_BYTES: u64 = 16 * 1024 * 1024;
pub(crate) const MAX_UPDATE_MANIFEST_BYTES: u64 = 2 * 1024 * 1024;
pub(crate) const MAX_UPDATE_TEXT_BYTES: u64 = 4 * 1024 * 1024;

const MAX_ARCHIVE_FILE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MAX_ARCHIVE_MEMBERS: usize = 32_768;
const MAX_ARCHIVE_MEMBER_NAME_BYTES: usize = 8 * 1024 * 1024;
const MAX_ARCHIVE_EXPANDED_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const MAX_ARCHIVE_COMPRESSION_RATIO: u64 = 500;
const READ_BUFFER_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Copy)]
struct ZipInspectionLimits {
    archive_bytes: u64,
    members: usize,
    member_name_bytes: usize,
    member_expanded_bytes: u64,
    total_expanded_bytes: u64,
    compression_ratio: u64,
}

const CLI_ZIP_LIMITS: ZipInspectionLimits = ZipInspectionLimits {
    archive_bytes: MAX_ARCHIVE_FILE_BYTES,
    members: MAX_ARCHIVE_MEMBERS,
    member_name_bytes: MAX_ARCHIVE_MEMBER_NAME_BYTES,
    member_expanded_bytes: MAX_ARCHIVE_MEMBER_BYTES,
    total_expanded_bytes: MAX_ARCHIVE_EXPANDED_BYTES,
    compression_ratio: MAX_ARCHIVE_COMPRESSION_RATIO,
};

/// ZIP archive whose catalog and decompressed reads stay within fixed budgets.
pub(crate) struct BoundedZipArchive {
    archive: ZipArchive<fs::File>,
    member_names: Vec<String>,
    expanded_bytes_read: u64,
    limits: ZipInspectionLimits,
}

impl BoundedZipArchive {
    /// Opens and validates an archive catalog before any member is decompressed.
    pub(crate) fn open(path: &Path) -> Result<Self> {
        Self::open_with_limits(path, CLI_ZIP_LIMITS)
    }

    fn open_with_limits(path: &Path, limits: ZipInspectionLimits) -> Result<Self> {
        let metadata = fs::metadata(path)
            .with_context(|| format!("failed to inspect archive {}", path.display()))?;
        if !metadata.is_file() {
            anyhow::bail!("archive path is not a regular file: {}", path.display());
        }
        if metadata.len() > limits.archive_bytes {
            anyhow::bail!(
                "archive {} exceeds the {} byte compressed-size limit",
                path.display(),
                limits.archive_bytes
            );
        }

        let file = fs::File::open(path)
            .with_context(|| format!("failed to open archive {}", path.display()))?;
        let mut archive = ZipArchive::new(file)
            .with_context(|| format!("failed to parse archive {}", path.display()))?;
        let member_names = validate_catalog(&mut archive, limits)
            .with_context(|| format!("unsafe archive {}", path.display()))?;

        Ok(Self { archive, member_names, expanded_bytes_read: 0, limits })
    }

    /// Returns the validated member names in archive order.
    pub(crate) fn member_names(&self) -> &[String] {
        self.member_names.as_slice()
    }

    /// Consumes the archive and hashes the exact file handle that was inspected.
    pub(crate) fn into_sha256(self) -> Result<String> {
        let mut file = self.archive.into_inner();
        file.seek(SeekFrom::Start(0)).context("failed to rewind inspected archive")?;
        let mut hasher = Sha256::new();
        let mut limited = file.take(self.limits.archive_bytes.saturating_add(1));
        let mut buffer = [0_u8; READ_BUFFER_BYTES];
        let mut read_bytes = 0_u64;
        loop {
            let read = limited.read(&mut buffer).context("failed to hash inspected archive")?;
            if read == 0 {
                break;
            }
            read_bytes = read_bytes.saturating_add(read as u64);
            if read_bytes > self.limits.archive_bytes {
                anyhow::bail!(
                    "archive exceeds the {} byte compressed-size limit while hashing",
                    self.limits.archive_bytes
                );
            }
            hasher.update(&buffer[..read]);
        }
        Ok(hex::encode(hasher.finalize()))
    }

    /// Reads an optional member into a bounded buffer.
    pub(crate) fn read_optional_bytes(
        &mut self,
        path: &str,
        max_bytes: u64,
    ) -> Result<Option<Vec<u8>>> {
        let mut bytes = Vec::new();
        let Some(_) = self.read_optional_with(path, max_bytes, |chunk| {
            bytes.extend_from_slice(chunk);
            Ok(())
        })?
        else {
            return Ok(None);
        };
        Ok(Some(bytes))
    }

    /// Streams a required member through a caller-provided consumer.
    pub(crate) fn read_required_with<F>(
        &mut self,
        path: &str,
        max_bytes: u64,
        consume: F,
    ) -> Result<u64>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        self.read_optional_with(path, max_bytes, consume)?
            .ok_or_else(|| anyhow::anyhow!("archive is missing required member {path}"))
    }

    fn read_optional_with<F>(
        &mut self,
        path: &str,
        max_bytes: u64,
        mut consume: F,
    ) -> Result<Option<u64>>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        let member_limit = max_bytes.min(self.limits.member_expanded_bytes);
        let aggregate_remaining =
            self.limits.total_expanded_bytes.saturating_sub(self.expanded_bytes_read);
        let effective_limit = member_limit.min(aggregate_remaining);
        let member_bytes = {
            let mut file = match self.archive.by_name(path) {
                Ok(file) => file,
                Err(ZipError::FileNotFound) => return Ok(None),
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("failed to locate archive member {path}"));
                }
            };
            if file.size() > member_limit {
                anyhow::bail!(
                    "archive member {path} exceeds the {member_limit} byte expanded-size limit"
                );
            }
            if file.size() > aggregate_remaining {
                anyhow::bail!(
                    "archive member {path} exceeds the remaining aggregate expanded-size budget"
                );
            }

            let mut limited = (&mut file).take(effective_limit.saturating_add(1));
            let mut buffer = [0_u8; READ_BUFFER_BYTES];
            let mut member_bytes = 0_u64;
            loop {
                let read = limited
                    .read(&mut buffer)
                    .with_context(|| format!("failed to decompress archive member {path}"))?;
                if read == 0 {
                    break;
                }
                member_bytes = member_bytes.saturating_add(read as u64);
                if member_bytes > member_limit {
                    anyhow::bail!(
                        "archive member {path} exceeds the {member_limit} byte expanded-size limit"
                    );
                }
                if member_bytes > aggregate_remaining {
                    anyhow::bail!("archive exceeds the aggregate expanded-size limit");
                }
                consume(&buffer[..read])?;
            }
            member_bytes
        };
        self.expanded_bytes_read = self.expanded_bytes_read.saturating_add(member_bytes);
        Ok(Some(member_bytes))
    }
}

fn validate_catalog(
    archive: &mut ZipArchive<fs::File>,
    limits: ZipInspectionLimits,
) -> Result<Vec<String>> {
    if archive.len() > limits.members {
        anyhow::bail!(
            "archive contains {} members, exceeding the {} member limit",
            archive.len(),
            limits.members
        );
    }

    let mut member_names = Vec::with_capacity(archive.len());
    let mut normalized_names = HashSet::with_capacity(archive.len());
    let mut member_name_bytes = 0_usize;
    let mut declared_expanded_bytes = 0_u64;
    for index in 0..archive.len() {
        let file = archive
            .by_index(index)
            .with_context(|| format!("failed to inspect archive member at index {index}"))?;
        let name = file.name().to_owned();
        member_name_bytes = member_name_bytes.saturating_add(name.len());
        if member_name_bytes > limits.member_name_bytes {
            anyhow::bail!(
                "archive member names exceed the {} byte aggregate limit",
                limits.member_name_bytes
            );
        }
        if !normalized_names.insert(name.to_ascii_lowercase()) {
            anyhow::bail!("archive contains duplicate member name {name}");
        }
        if file.size() > limits.member_expanded_bytes {
            anyhow::bail!(
                "archive member {name} declares {} expanded bytes, exceeding the {} byte limit",
                file.size(),
                limits.member_expanded_bytes
            );
        }
        declared_expanded_bytes = declared_expanded_bytes.saturating_add(file.size());
        if declared_expanded_bytes > limits.total_expanded_bytes {
            anyhow::bail!(
                "archive declares more than {} aggregate expanded bytes",
                limits.total_expanded_bytes
            );
        }
        if compression_ratio_exceeded(file.size(), file.compressed_size(), limits.compression_ratio)
        {
            anyhow::bail!(
                "archive member {name} exceeds the {}:1 compression-ratio limit",
                limits.compression_ratio
            );
        }
        member_names.push(name);
    }
    Ok(member_names)
}

fn compression_ratio_exceeded(expanded: u64, compressed: u64, max_ratio: u64) -> bool {
    expanded > 0
        && (compressed == 0
            || u128::from(expanded) > u128::from(compressed) * u128::from(max_ratio))
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::tempdir;
    use zip::{write::SimpleFileOptions, CompressionMethod, ZipWriter};

    use super::{BoundedZipArchive, ZipInspectionLimits};

    fn limits() -> ZipInspectionLimits {
        ZipInspectionLimits {
            archive_bytes: 1024 * 1024,
            members: 2,
            member_name_bytes: 32,
            member_expanded_bytes: 256,
            total_expanded_bytes: 384,
            compression_ratio: 8,
        }
    }

    #[test]
    fn bounded_zip_rejects_excessive_member_count() -> anyhow::Result<()> {
        let temp = tempdir()?;
        let path = temp.path().join("members.zip");
        let file = std::fs::File::create(path.as_path())?;
        let mut writer = ZipWriter::new(file);
        for name in ["one", "two", "three"] {
            writer.start_file(name, SimpleFileOptions::default())?;
            writer.write_all(b"x")?;
        }
        writer.finish()?;

        let error = BoundedZipArchive::open_with_limits(path.as_path(), limits())
            .err()
            .expect("member count must be bounded");
        assert!(format!("{error:#}").contains("member limit"));
        Ok(())
    }

    #[test]
    fn bounded_zip_rejects_high_compression_ratio() -> anyhow::Result<()> {
        let temp = tempdir()?;
        let path = temp.path().join("ratio.zip");
        let file = std::fs::File::create(path.as_path())?;
        let mut writer = ZipWriter::new(file);
        writer.start_file(
            "payload",
            SimpleFileOptions::default().compression_method(CompressionMethod::Deflated),
        )?;
        writer.write_all(&[b'a'; 256])?;
        writer.finish()?;

        let error = BoundedZipArchive::open_with_limits(path.as_path(), limits())
            .err()
            .expect("compression ratio must be bounded");
        assert!(format!("{error:#}").contains("compression-ratio"));
        Ok(())
    }

    #[test]
    fn bounded_zip_rejects_duplicate_case_insensitive_names() -> anyhow::Result<()> {
        let temp = tempdir()?;
        let path = temp.path().join("duplicates.zip");
        let file = std::fs::File::create(path.as_path())?;
        let mut writer = ZipWriter::new(file);
        writer.start_file("manifest.json", SimpleFileOptions::default())?;
        writer.write_all(b"{}")?;
        writer.start_file("MANIFEST.JSON", SimpleFileOptions::default())?;
        writer.write_all(b"{}")?;
        writer.finish()?;

        let error = BoundedZipArchive::open_with_limits(path.as_path(), limits())
            .err()
            .expect("ambiguous member names must be rejected");
        assert!(format!("{error:#}").contains("duplicate member name"));
        Ok(())
    }

    #[test]
    fn bounded_zip_enforces_the_caller_member_budget() -> anyhow::Result<()> {
        let temp = tempdir()?;
        let path = temp.path().join("bounded-read.zip");
        let file = std::fs::File::create(path.as_path())?;
        let mut writer = ZipWriter::new(file);
        writer.start_file(
            "notes.txt",
            SimpleFileOptions::default().compression_method(CompressionMethod::Stored),
        )?;
        writer.write_all(&[b'n'; 64])?;
        writer.finish()?;

        let mut archive = BoundedZipArchive::open_with_limits(path.as_path(), limits())?;
        let error = archive
            .read_optional_bytes("notes.txt", 32)
            .expect_err("caller-specific member limits must be enforced");
        assert!(error.to_string().contains("32 byte expanded-size limit"));
        Ok(())
    }
}
