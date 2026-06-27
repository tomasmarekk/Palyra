//! `palyra backup`: create and verify portable ZIP archives of config, state,
//! optional workspace, and an embedded support bundle.
//! Safety rules: the archive must live outside the live state root, unsupported
//! symlinked sources are rejected, and every entry is hash-pinned in
//! `manifest.json`.

use std::{
    env, fs,
    io::{Read, Write},
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use zip::{
    read::ZipArchive,
    result::ZipError,
    write::{SimpleFileOptions, ZipWriter},
    CompressionMethod,
};

use crate::cli::{BackupCommand, BackupComponentArg};
use crate::*;

const BACKUP_MANIFEST_PATH: &str = "manifest.json";
const SKILL_CURRENT_LINK_NAME: &str = "current";
const SKILLS_STATE_DIR: &str = "skills";
const VAULT_METADATA_LOCK_FILE: &str = "metadata.lock";
const VAULT_METADATA_TEMP_PREFIX: &str = "metadata.tmp.";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BackupManifest {
    schema_version: u32,
    generated_at_unix_ms: i64,
    created_by_version: String,
    created_by_git_hash: String,
    config_path: Option<String>,
    state_root: String,
    install_root: Option<String>,
    included_workspace: bool,
    included_support_bundle: bool,
    entries: Vec<BackupEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BackupEntry {
    archive_path: String,
    source_path: String,
    size_bytes: u64,
    sha256: String,
}

#[derive(Debug, Clone, Serialize)]
struct BackupCreateReport {
    archive_path: String,
    generated_at_unix_ms: i64,
    entry_count: usize,
    included_workspace: bool,
    included_support_bundle: bool,
    install_root: Option<String>,
    config_path: Option<String>,
    state_root: String,
    next_steps: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct BackupVerifyReport {
    archive_path: String,
    generated_at_unix_ms: i64,
    entry_count: usize,
    verified_entries: usize,
    ok: bool,
}

/// Runs a `palyra backup` subcommand.
///
/// # Errors
/// Returns an error when source paths are missing or unsafe, the archive
/// cannot be written, or verification finds hash/size mismatches.
pub(crate) fn run_backup(command: BackupCommand) -> Result<()> {
    match command {
        BackupCommand::Create {
            output,
            config_path,
            state_root,
            workspace_root,
            include,
            include_workspace,
            include_support_bundle,
            force,
            json,
        } => run_backup_create(
            output,
            config_path,
            state_root,
            workspace_root,
            include,
            include_workspace,
            include_support_bundle,
            force,
            json,
        ),
        BackupCommand::Verify { archive, json } => run_backup_verify(archive, json),
    }
}

#[allow(clippy::too_many_arguments)]
fn run_backup_create(
    output: Option<String>,
    config_path: Option<String>,
    state_root: Option<String>,
    workspace_root: Option<String>,
    include: Vec<BackupComponentArg>,
    include_workspace: bool,
    include_support_bundle: bool,
    force: bool,
    json: bool,
) -> Result<()> {
    let context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for backup command"))?;
    let output_path = resolve_backup_output_path(output)?;
    let selected = BackupSelection::from_flags(include, include_workspace, include_support_bundle);
    let config_path = resolve_optional_existing_file(
        config_path.or_else(|| context.config_path().map(|value| value.display().to_string())),
        "config_path",
    )?;
    let state_root = resolve_existing_directory(
        state_root.unwrap_or_else(|| context.state_root().display().to_string()),
        "state_root",
    )?;
    reject_live_state_root_backup_output(output_path.as_path(), state_root.as_path())?;
    if output_path.exists() && !force {
        anyhow::bail!(
            "backup archive already exists: {} (pass --force to replace it)",
            output_path.display()
        );
    }
    if let Some(parent) = output_path.parent().filter(|value| !value.as_os_str().is_empty()) {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let output_path = normalize_backup_comparison_path(output_path.as_path())?;
    let workspace_root =
        if selected.workspace { Some(resolve_workspace_root(workspace_root)?) } else { None };

    let generated_at_unix_ms = now_unix_ms_i64()?;
    let build = build_metadata();
    let file = fs::File::create(output_path.as_path())
        .with_context(|| format!("failed to create backup archive {}", output_path.display()))?;
    let mut writer = ZipWriter::new(file);
    let options = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
    let mut entries = Vec::new();

    if selected.config {
        if let Some(config_path) = config_path.as_ref() {
            let file_name =
                config_path.file_name().and_then(|value| value.to_str()).unwrap_or("config.toml");
            add_file_to_zip(
                &mut writer,
                options,
                config_path.as_path(),
                None,
                format!("config/{file_name}").as_str(),
                output_path.as_path(),
                &mut entries,
            )?;
        }
    }
    if selected.state {
        add_directory_to_zip(
            &mut writer,
            options,
            state_root.as_path(),
            "state",
            output_path.as_path(),
            &mut entries,
        )?;
    }
    if let Some(workspace_root) = workspace_root.as_ref() {
        add_directory_to_zip(
            &mut writer,
            options,
            workspace_root.as_path(),
            "workspace",
            output_path.as_path(),
            &mut entries,
        )?;
    }
    if selected.support_bundle {
        let support_bundle = build_embedded_support_bundle(generated_at_unix_ms)?;
        let bytes = serde_json::to_vec_pretty(&support_bundle)
            .context("failed to encode embedded support bundle")?;
        add_bytes_to_zip(
            &mut writer,
            options,
            "exports/support-bundle.json",
            bytes.as_slice(),
            "<generated:support-bundle>",
            &mut entries,
        )?;
    }

    let manifest = BackupManifest {
        schema_version: 1,
        generated_at_unix_ms,
        created_by_version: build.version.to_owned(),
        created_by_git_hash: build.git_hash.to_owned(),
        config_path: config_path.as_ref().map(|value| value.display().to_string()),
        state_root: state_root.display().to_string(),
        install_root: None,
        included_workspace: selected.workspace,
        included_support_bundle: selected.support_bundle,
        entries,
    };
    let manifest_bytes =
        serde_json::to_vec_pretty(&manifest).context("failed to encode backup manifest")?;
    writer
        .start_file(BACKUP_MANIFEST_PATH, options)
        .context("failed to start backup manifest entry")?;
    writer.write_all(manifest_bytes.as_slice()).context("failed to write backup manifest")?;
    writer.finish().context("failed to finalize backup archive")?;

    let report = BackupCreateReport {
        archive_path: output_path.display().to_string(),
        generated_at_unix_ms,
        entry_count: manifest.entries.len(),
        included_workspace: selected.workspace,
        included_support_bundle: selected.support_bundle,
        install_root: None,
        config_path: manifest.config_path,
        state_root: manifest.state_root,
        next_steps: vec![
            "Run `palyra backup verify --archive <path>` before depending on this archive.".to_owned(),
            "Keep the archive outside the live state root and install root.".to_owned(),
            "Use `palyra support-bundle export` before destructive recovery if runtime health is degraded.".to_owned(),
        ],
    };
    emit_backup_create_report(&report, json)
}

fn run_backup_verify(archive: String, json: bool) -> Result<()> {
    let archive_path = PathBuf::from(archive);
    let file = match fs::File::open(archive_path.as_path()) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            anyhow::bail!(
                "backup archive not found: {}; provide an existing Palyra backup archive path",
                archive_path.display()
            );
        }
        Err(error) => {
            return Err(error).with_context(|| {
                format!("failed to open backup archive {}", archive_path.display())
            });
        }
    };
    let mut archive = ZipArchive::new(file)
        .with_context(|| format!("failed to parse {}", archive_path.display()))?;
    let manifest = read_backup_manifest(&mut archive)?;

    for entry in manifest.entries.as_slice() {
        let mut file = archive
            .by_name(entry.archive_path.as_str())
            .with_context(|| format!("backup archive is missing {}", entry.archive_path))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .with_context(|| format!("failed to read archive entry {}", entry.archive_path))?;
        let actual_hash = sha256_hex(bytes.as_slice());
        if actual_hash != entry.sha256 {
            anyhow::bail!(
                "backup entry {} failed SHA256 verification (expected {}, observed {})",
                entry.archive_path,
                entry.sha256,
                actual_hash
            );
        }
        if bytes.len() as u64 != entry.size_bytes {
            anyhow::bail!(
                "backup entry {} failed size verification (expected {}, observed {})",
                entry.archive_path,
                entry.size_bytes,
                bytes.len()
            );
        }
    }

    let report = BackupVerifyReport {
        archive_path: archive_path.display().to_string(),
        generated_at_unix_ms: manifest.generated_at_unix_ms,
        entry_count: manifest.entries.len(),
        verified_entries: manifest.entries.len(),
        ok: true,
    };
    emit_backup_verify_report(&report, json)
}

fn build_embedded_support_bundle(generated_at_unix_ms: i64) -> Result<SupportBundle> {
    let checks = build_doctor_checks();
    let doctor = build_doctor_report(checks.as_slice())?;
    let build = build_metadata();
    let diagnostics = build_support_bundle_diagnostics_snapshot();
    let profile = app::current_root_context().and_then(|context| context.active_profile_context());
    Ok(SupportBundle {
        schema_version: 1,
        generated_at_unix_ms,
        profile,
        build: SupportBundleBuildSnapshot {
            version: build.version.to_owned(),
            git_hash: build.git_hash.to_owned(),
            build_profile: build.build_profile.to_owned(),
        },
        platform: SupportBundlePlatformSnapshot {
            os: std::env::consts::OS.to_owned(),
            family: std::env::consts::FAMILY.to_owned(),
            arch: std::env::consts::ARCH.to_owned(),
        },
        doctor,
        recovery: Some(commands::doctor::build_doctor_support_bundle_value()?),
        config: build_support_bundle_config_snapshot(),
        observability: build_support_bundle_observability_snapshot(&diagnostics),
        triage: build_support_bundle_triage_snapshot(),
        replay: build_support_bundle_replay_snapshot(),
        diagnostics,
        journal: build_support_bundle_journal_snapshot(32, 16),
        truncated: false,
        warnings: Vec::new(),
    })
}

fn resolve_backup_output_path(output: Option<String>) -> Result<PathBuf> {
    if let Some(output) = output {
        let trimmed = output.trim();
        if !trimmed.is_empty() {
            return Ok(PathBuf::from(trimmed));
        }
    }
    Ok(PathBuf::from(format!("palyra-backup-{}.zip", now_unix_ms_i64()?)))
}

fn resolve_workspace_root(explicit: Option<String>) -> Result<PathBuf> {
    let root = explicit
        .map(PathBuf::from)
        .unwrap_or(env::current_dir().context("failed to resolve current directory")?);
    if !root.is_dir() {
        anyhow::bail!("workspace root does not exist: {}", root.display());
    }
    support::lifecycle::canonicalize_lossy(root.as_path())
}

#[derive(Debug, Clone, Copy)]
struct BackupSelection {
    config: bool,
    state: bool,
    workspace: bool,
    support_bundle: bool,
}

impl BackupSelection {
    fn from_flags(
        include: Vec<BackupComponentArg>,
        include_workspace: bool,
        include_support_bundle: bool,
    ) -> Self {
        let config = include.is_empty() || include.contains(&BackupComponentArg::Config);
        let state = include.is_empty() || include.contains(&BackupComponentArg::State);
        let workspace = include_workspace || include.contains(&BackupComponentArg::Workspace);
        let support_bundle =
            include_support_bundle || include.contains(&BackupComponentArg::SupportBundle);
        Self { config, state, workspace, support_bundle }
    }
}

fn resolve_optional_existing_file(raw: Option<String>, label: &str) -> Result<Option<PathBuf>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let path = PathBuf::from(raw);
    if !path.is_file() {
        anyhow::bail!("{label} does not exist: {}", path.display());
    }
    support::lifecycle::canonicalize_lossy(path.as_path()).map(Some)
}

fn resolve_existing_directory(raw: String, label: &str) -> Result<PathBuf> {
    let path = PathBuf::from(raw);
    if !path.is_dir() {
        anyhow::bail!("{label} does not exist: {}", path.display());
    }
    support::lifecycle::canonicalize_lossy(path.as_path())
}

fn add_directory_to_zip(
    writer: &mut ZipWriter<fs::File>,
    options: SimpleFileOptions,
    source_root: &Path,
    archive_prefix: &str,
    output_path: &Path,
    entries: &mut Vec<BackupEntry>,
) -> Result<()> {
    if !source_root.is_dir() {
        return Ok(());
    }
    let source_root = support::lifecycle::canonicalize_lossy(source_root)?;
    let mut stack = vec![source_root.clone()];
    while let Some(current) = stack.pop() {
        let mut children = fs::read_dir(current.as_path())
            .with_context(|| format!("failed to list {}", current.display()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .with_context(|| format!("failed to read {}", current.display()))?;
        children.sort_by_key(|entry| entry.path());
        for child in children {
            let path = child.path();
            // Never archive the archive itself; the output file may live
            // inside the workspace tree being captured.
            if path == output_path {
                continue;
            }
            let file_type = child
                .file_type()
                .with_context(|| format!("failed to inspect backup entry {}", path.display()))?;
            if should_skip_state_backup_entry(path.as_path(), archive_prefix, &file_type) {
                continue;
            }
            // Symlinks are rejected outright (not followed) so a planted link
            // cannot pull files from outside the selected source root into the
            // archive; a unit test pins this behavior.
            if file_type.is_symlink() {
                anyhow::bail!(
                    "backup source contains unsupported symlink entry: {}",
                    path.display()
                );
            }
            if file_type.is_dir() {
                stack.push(path);
                continue;
            }
            if file_type.is_file() {
                let relative = path
                    .strip_prefix(source_root.as_path())
                    .with_context(|| {
                        format!("failed to compute backup path for {}", path.display())
                    })?
                    .components()
                    .map(|component| component.as_os_str().to_string_lossy().to_string())
                    .collect::<Vec<_>>()
                    .join("/");
                let archive_path = format!("{archive_prefix}/{relative}");
                add_file_to_zip(
                    writer,
                    options,
                    path.as_path(),
                    Some(source_root.as_path()),
                    archive_path.as_str(),
                    output_path,
                    entries,
                )?;
            }
        }
    }
    Ok(())
}

// Vault metadata lock and temp files are transient and may be mid-write while
// the daemon runs; capturing them would embed torn or stale vault metadata in
// the archive.
fn should_skip_state_backup_entry(
    path: &Path,
    archive_prefix: &str,
    file_type: &fs::FileType,
) -> bool {
    if archive_prefix != "state" {
        return false;
    }
    if file_type.is_symlink() && is_managed_skill_current_link(path) {
        return true;
    }
    let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
        return false;
    };
    file_name == VAULT_METADATA_LOCK_FILE || file_name.starts_with(VAULT_METADATA_TEMP_PREFIX)
}

// Skill installs maintain `skills/<id>/current` as a convenience pointer; the
// pointed-to version directories are archived directly, so the pointer itself
// should not make a default state backup fail.
fn is_managed_skill_current_link(path: &Path) -> bool {
    let mut normal_components = path.components().rev().filter_map(|component| {
        if let std::path::Component::Normal(value) = component {
            value.to_str()
        } else {
            None
        }
    });
    let Some(file_name) = normal_components.next() else {
        return false;
    };
    if file_name != SKILL_CURRENT_LINK_NAME {
        return false;
    }
    let Some(skill_id) = normal_components.next() else {
        return false;
    };
    if skill_id.is_empty() {
        return false;
    }
    normal_components.next() == Some(SKILLS_STATE_DIR)
}

fn add_file_to_zip(
    writer: &mut ZipWriter<fs::File>,
    options: SimpleFileOptions,
    source_path: &Path,
    allowed_root: Option<&Path>,
    archive_path: &str,
    output_path: &Path,
    entries: &mut Vec<BackupEntry>,
) -> Result<()> {
    let source_path = support::lifecycle::canonicalize_lossy(source_path)?;
    if let Some(allowed_root) = allowed_root {
        if !support::lifecycle::path_starts_with(source_path.as_path(), allowed_root) {
            anyhow::bail!("backup source escapes the allowed root: {}", source_path.display());
        }
    }
    if source_path == output_path {
        return Ok(());
    }
    let bytes = fs::read(source_path.as_path())
        .with_context(|| format!("failed to read {}", source_path.display()))?;
    add_bytes_to_zip(
        writer,
        options,
        archive_path,
        bytes.as_slice(),
        source_path.display().to_string().as_str(),
        entries,
    )
}

fn add_bytes_to_zip(
    writer: &mut ZipWriter<fs::File>,
    options: SimpleFileOptions,
    archive_path: &str,
    bytes: &[u8],
    source_path: &str,
    entries: &mut Vec<BackupEntry>,
) -> Result<()> {
    writer
        .start_file(archive_path, options)
        .with_context(|| format!("failed to start backup entry {}", archive_path))?;
    writer
        .write_all(bytes)
        .with_context(|| format!("failed to write backup entry {}", archive_path))?;
    entries.push(BackupEntry {
        archive_path: archive_path.to_owned(),
        source_path: source_path.to_owned(),
        size_bytes: bytes.len() as u64,
        sha256: crate::sha256_hex(bytes),
    });
    Ok(())
}

fn read_backup_manifest(archive: &mut ZipArchive<fs::File>) -> Result<BackupManifest> {
    let mut file = archive.by_name(BACKUP_MANIFEST_PATH).map_err(|error| match error {
        ZipError::FileNotFound => anyhow!(
            "invalid backup archive: missing manifest.json; selected ZIP is not a Palyra backup archive"
        ),
        other => anyhow!(other).context("failed to locate backup manifest"),
    })?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).context("failed to read backup manifest")?;
    serde_json::from_slice::<BackupManifest>(bytes.as_slice())
        .context("invalid backup archive: failed to parse manifest.json")
}

fn emit_backup_create_report(report: &BackupCreateReport, json: bool) -> Result<()> {
    let context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for backup command"))?;
    if json || context.prefers_json() {
        return output::print_json_pretty(report, "failed to encode backup output as JSON");
    }
    if context.prefers_ndjson() {
        return output::print_json_line(report, "failed to encode backup output as NDJSON");
    }
    println!(
        "backup.create archive_path={} entries={} state_root={} install_root={} included_workspace={} included_support_bundle={}",
        report.archive_path,
        report.entry_count,
        report.state_root,
        report.install_root.as_deref().unwrap_or("none"),
        report.included_workspace,
        report.included_support_bundle
    );
    if let Some(config_path) = report.config_path.as_deref() {
        println!("backup.create.config_path={config_path}");
    }
    for step in report.next_steps.as_slice() {
        println!("backup.create.next_step={step}");
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_backup_verify_report(report: &BackupVerifyReport, json: bool) -> Result<()> {
    let context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for backup command"))?;
    if json || context.prefers_json() {
        return output::print_json_pretty(report, "failed to encode backup verify output as JSON");
    }
    if context.prefers_ndjson() {
        return output::print_json_line(report, "failed to encode backup verify output as NDJSON");
    }
    println!(
        "backup.verify archive_path={} ok={} entries={} verified_entries={} generated_at_unix_ms={}",
        report.archive_path,
        report.ok,
        report.entry_count,
        report.verified_entries,
        report.generated_at_unix_ms
    );
    std::io::stdout().flush().context("stdout flush failed")
}

// An archive written into the live state root would be swept up by later
// backups and destroyed by state-root recovery/uninstall flows, so it is
// rejected up front rather than silently skipped.
fn reject_live_state_root_backup_output(output_path: &Path, state_root: &Path) -> Result<()> {
    let normalized_output = normalize_backup_comparison_path(output_path)?;
    let normalized_state_root = normalize_backup_comparison_path(state_root)?;
    if normalized_output.starts_with(normalized_state_root.as_path()) {
        anyhow::bail!(
            "backup archive output {} must stay outside the live state root {}",
            normalized_output.display(),
            normalized_state_root.display()
        );
    }
    Ok(())
}

fn normalize_backup_comparison_path(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()
            .context("failed to resolve current directory for backup path normalization")?
            .join(path)
    };
    Ok(normalize_backup_path_components(absolute.as_path()))
}

fn normalize_backup_path_components(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[cfg(unix)]
    fn create_directory_symlink(target: &Path, link: &Path) -> std::io::Result<()> {
        std::os::unix::fs::symlink(target, link)
    }

    #[cfg(windows)]
    fn create_directory_symlink(target: &Path, link: &Path) -> std::io::Result<()> {
        std::os::windows::fs::symlink_dir(target, link)
    }

    #[cfg(not(any(unix, windows)))]
    fn create_directory_symlink(_target: &Path, _link: &Path) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "directory symlinks are unsupported on this platform",
        ))
    }

    #[cfg(unix)]
    fn create_file_symlink(target: &Path, link: &Path) -> std::io::Result<()> {
        std::os::unix::fs::symlink(target, link)
    }

    #[cfg(windows)]
    fn create_file_symlink(target: &Path, link: &Path) -> std::io::Result<()> {
        std::os::windows::fs::symlink_file(target, link)
    }

    #[cfg(not(any(unix, windows)))]
    fn create_file_symlink(_target: &Path, _link: &Path) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "file symlinks are unsupported on this platform",
        ))
    }

    #[test]
    fn backup_manifest_round_trips() -> Result<()> {
        let temp = tempdir()?;
        let archive_path = temp.path().join("backup.zip");
        let file = fs::File::create(archive_path.as_path())?;
        let mut writer = ZipWriter::new(file);
        let options = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
        let manifest = BackupManifest {
            schema_version: 1,
            generated_at_unix_ms: 1,
            created_by_version: "0.1.0".to_owned(),
            created_by_git_hash: "abc".to_owned(),
            config_path: None,
            state_root: "state".to_owned(),
            install_root: None,
            included_workspace: false,
            included_support_bundle: false,
            entries: vec![BackupEntry {
                archive_path: "state/file.txt".to_owned(),
                source_path: "C:/state/file.txt".to_owned(),
                size_bytes: 3,
                sha256: crate::sha256_hex(b"abc"),
            }],
        };
        writer.start_file("state/file.txt", options)?;
        writer.write_all(b"abc")?;
        writer.start_file(BACKUP_MANIFEST_PATH, options)?;
        writer.write_all(serde_json::to_vec(&manifest)?.as_slice())?;
        writer.finish()?;

        let file = fs::File::open(archive_path.as_path())?;
        let mut archive = ZipArchive::new(file)?;
        let loaded = read_backup_manifest(&mut archive)?;
        assert_eq!(loaded.entries.len(), 1);
        assert_eq!(loaded.entries[0].sha256, crate::sha256_hex(b"abc"));
        Ok(())
    }

    #[test]
    fn add_file_to_zip_rejects_sources_outside_allowed_root() -> Result<()> {
        let temp = tempdir()?;
        let allowed_root = temp.path().join("allowed");
        let outside_root = temp.path().join("outside");
        fs::create_dir_all(allowed_root.as_path())?;
        fs::create_dir_all(outside_root.as_path())?;

        let outside_file = outside_root.join("secret.txt");
        fs::write(outside_file.as_path(), b"secret")?;

        let archive_path = temp.path().join("backup.zip");
        let file = fs::File::create(archive_path.as_path())?;
        let mut writer = ZipWriter::new(file);
        let options = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
        let mut entries = Vec::new();

        let error = add_file_to_zip(
            &mut writer,
            options,
            outside_file.as_path(),
            Some(allowed_root.as_path()),
            "state/secret.txt",
            archive_path.as_path(),
            &mut entries,
        )
        .expect_err("outside sources must be rejected");
        assert!(
            error.to_string().contains("escapes the allowed root"),
            "unexpected error: {error}"
        );
        Ok(())
    }

    #[test]
    fn add_directory_to_zip_skips_transient_vault_metadata_files() -> Result<()> {
        let temp = tempdir()?;
        let source_root = temp.path().join("state");
        let vault_root = source_root.join("vault");
        fs::create_dir_all(vault_root.as_path())?;
        fs::write(source_root.join("runtime.json").as_path(), b"stable")?;
        fs::write(vault_root.join(VAULT_METADATA_LOCK_FILE).as_path(), b"pid=1")?;
        fs::write(vault_root.join("metadata.tmp.01ABC").as_path(), b"temporary")?;

        let archive_path = temp.path().join("backup.zip");
        let file = fs::File::create(archive_path.as_path())?;
        let mut writer = ZipWriter::new(file);
        let options = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
        let mut entries = Vec::new();

        add_directory_to_zip(
            &mut writer,
            options,
            source_root.as_path(),
            "state",
            archive_path.as_path(),
            &mut entries,
        )?;

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].archive_path, "state/runtime.json");
        Ok(())
    }

    #[test]
    fn add_directory_to_zip_skips_managed_skill_current_symlink() -> Result<()> {
        let temp = tempdir()?;
        let source_root = temp.path().join("state");
        let skill_root = source_root.join(SKILLS_STATE_DIR).join("e2e.reporter");
        let version_root = skill_root.join("1.0.0");
        fs::create_dir_all(version_root.as_path())?;
        fs::write(version_root.join("manifest.json").as_path(), b"{}")?;
        let current_link = skill_root.join(SKILL_CURRENT_LINK_NAME);
        if let Err(error) = create_directory_symlink(version_root.as_path(), current_link.as_path())
        {
            eprintln!("skipping managed skill current symlink backup test: {error}");
            return Ok(());
        }

        let archive_path = temp.path().join("backup.zip");
        let file = fs::File::create(archive_path.as_path())?;
        let mut writer = ZipWriter::new(file);
        let options = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
        let mut entries = Vec::new();

        add_directory_to_zip(
            &mut writer,
            options,
            source_root.as_path(),
            "state",
            archive_path.as_path(),
            &mut entries,
        )?;

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].archive_path, "state/skills/e2e.reporter/1.0.0/manifest.json");
        assert!(entries.iter().all(|entry| !entry.archive_path.contains("/current")));
        Ok(())
    }

    #[test]
    fn reject_live_state_root_backup_output_blocks_archives_inside_state_root() -> Result<()> {
        let temp = tempdir()?;
        let state_root = temp.path().join("state");
        fs::create_dir_all(state_root.as_path())?;

        let error = reject_live_state_root_backup_output(
            state_root.join("portable-backup.zip").as_path(),
            state_root.as_path(),
        )
        .expect_err("backup archives inside the live state root must be rejected");

        assert!(
            error.to_string().contains("must stay outside the live state root"),
            "unexpected error: {error}"
        );
        Ok(())
    }

    #[test]
    fn reject_live_state_root_backup_output_allows_archives_outside_state_root() -> Result<()> {
        let temp = tempdir()?;
        let state_root = temp.path().join("state");
        let export_root = temp.path().join("exports");
        fs::create_dir_all(state_root.as_path())?;
        fs::create_dir_all(export_root.as_path())?;

        reject_live_state_root_backup_output(
            export_root.join("portable-backup.zip").as_path(),
            state_root.as_path(),
        )?;
        Ok(())
    }

    #[test]
    fn add_directory_to_zip_rejects_symlink_entries() -> Result<()> {
        let temp = tempdir()?;
        let source_root = temp.path().join("state");
        let outside_root = temp.path().join("outside");
        fs::create_dir_all(source_root.as_path())?;
        fs::create_dir_all(outside_root.as_path())?;

        let outside_file = outside_root.join("secret.txt");
        fs::write(outside_file.as_path(), b"secret")?;
        if let Err(error) =
            create_file_symlink(outside_file.as_path(), source_root.join("leak.txt").as_path())
        {
            eprintln!("skipping unsupported symlink rejection test: {error}");
            return Ok(());
        }

        let archive_path = temp.path().join("backup.zip");
        let file = fs::File::create(archive_path.as_path())?;
        let mut writer = ZipWriter::new(file);
        let options = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
        let mut entries = Vec::new();

        let error = add_directory_to_zip(
            &mut writer,
            options,
            source_root.as_path(),
            "state",
            archive_path.as_path(),
            &mut entries,
        )
        .expect_err("symlink entries must be rejected");
        assert!(
            error.to_string().contains("unsupported symlink entry"),
            "unexpected error: {error}"
        );
        Ok(())
    }
}
