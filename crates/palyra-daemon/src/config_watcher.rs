//! Debounced native config watcher with an independent polling fallback.

use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use palyra_common::config_system::{backup_path, write_secret_content_with_backups};
use palyra_control_plane::{ConfigReloadApplyRequest, ConfigReloadPlanRequest};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tracing::{info, warn};
use ulid::Ulid;

use crate::{
    app::state::AppState,
    application::restart_coordinator::{ConfigWatchEventKind, ConfigWatchEventV1, RestartRequest},
    transport::{
        grpc::auth::RequestContext,
        http::handlers::console::config::{
            apply_config_reload_for_context, plan_config_reload_for_context,
        },
    },
};

const CONFIG_WATCH_DEBOUNCE: Duration = Duration::from_millis(500);
const CONFIG_POLL_INTERVAL: Duration = Duration::from_secs(2);
const CONFIG_WATCH_TICK: Duration = Duration::from_millis(100);
const CONFIG_WATCH_BACKOFF_MAX: Duration = Duration::from_secs(30);
const CONFIG_FILE_MAX_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
struct FileFingerprint {
    len: u64,
    modified_unix_nanos: u128,
    content_sha256: String,
}

#[derive(Debug, Default)]
struct DebounceState {
    deadline: Option<Instant>,
}

impl DebounceState {
    fn observe(&mut self, now: Instant) {
        self.deadline = Some(now + CONFIG_WATCH_DEBOUNCE);
    }

    fn take_ready(&mut self, now: Instant) -> bool {
        if self.deadline.is_some_and(|deadline| now >= deadline) {
            self.deadline = None;
            true
        } else {
            false
        }
    }
}

/// Resolves the file component of the loader's human-readable source string.
#[must_use]
pub(crate) fn path_from_loaded_source(source: &str) -> Option<PathBuf> {
    let provenance_start = [" +migration(", " +env("]
        .into_iter()
        .filter_map(|marker| source.find(marker))
        .min()
        .unwrap_or(source.len());
    let source = source[..provenance_start].trim();
    (!source.is_empty() && source != "defaults").then(|| PathBuf::from(source))
}

/// Starts a native directory watcher plus content-fingerprint polling.
///
/// # Errors
/// Returns an error when the configured file cannot establish its initial
/// last-known-good reference. Native watcher failure is non-fatal because the
/// polling lane remains active.
pub(crate) fn spawn_config_watcher(
    state: AppState,
    path: PathBuf,
) -> Result<tokio::task::JoinHandle<()>> {
    let source_identity_sha256 = source_identity_sha256(path.as_path());
    let mut initial = read_fingerprint(path.as_path()).ok();
    let accepted = if crate::config::load_config_from_path(path.as_path()).is_ok() {
        let refreshed = seed_backup_and_refresh_fingerprint(path.as_path())?;
        initial = Some(refreshed.clone());
        refreshed
    } else {
        valid_backup_fingerprint(path.as_path()).with_context(|| {
            format!(
                "active config {} is invalid and no valid watcher backup exists",
                path.display()
            )
        })?
    };
    state.runtime.journal_store.record_config_last_known_good(
        accepted.content_sha256.as_str(),
        source_identity_sha256.as_str(),
        None,
        "daemon.config.startup_validated",
    )?;
    Ok(tokio::spawn(run_config_watcher(state, path, source_identity_sha256, initial)))
}

async fn run_config_watcher(
    state: AppState,
    path: PathBuf,
    source_identity_sha256: String,
    mut last_fingerprint: Option<FileFingerprint>,
) {
    let (native_tx, mut native_rx) = mpsc::unbounded_channel();
    let mut watcher_generation = 1_u64;
    let mut watcher = match create_native_watcher(path.as_path(), native_tx.clone()) {
        Ok(watcher) => Some(watcher),
        Err(error) => {
            warn!(message = %error, "native config watcher unavailable; polling fallback active");
            record_watch_event(
                &state,
                ConfigWatchEventKind::PollingFallback,
                source_identity_sha256.as_str(),
                None,
                "daemon.config_watch.native_unavailable",
                watcher_generation,
            );
            None
        }
    };
    let mut watcher_backoff = Duration::from_secs(1);
    let mut watcher_retry_at = Instant::now() + watcher_backoff;
    let mut poll_at = Instant::now() + CONFIG_POLL_INTERVAL;
    let mut debounce = DebounceState::default();
    let mut lifecycle = state.runtime.daemon_lifecycle.subscribe();
    let mut ticker = tokio::time::interval(CONFIG_WATCH_TICK);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let now = Instant::now();
                if watcher.is_none() && now >= watcher_retry_at {
                    watcher_generation = watcher_generation.saturating_add(1);
                    match create_native_watcher(path.as_path(), native_tx.clone()) {
                        Ok(recreated) => {
                            watcher = Some(recreated);
                            watcher_backoff = Duration::from_secs(1);
                            record_watch_event(
                                &state,
                                ConfigWatchEventKind::WatcherRestarted,
                                source_identity_sha256.as_str(),
                                None,
                                "daemon.config_watch.native_restarted",
                                watcher_generation,
                            );
                        }
                        Err(error) => {
                            warn!(message = %error, "native config watcher recreate failed");
                            watcher_backoff =
                                watcher_backoff.saturating_mul(2).min(CONFIG_WATCH_BACKOFF_MAX);
                            watcher_retry_at = now + watcher_backoff;
                        }
                    }
                }
                if now >= poll_at {
                    poll_at = now + CONFIG_POLL_INTERVAL;
                    match read_fingerprint(path.as_path()) {
                        Ok(fingerprint)
                            if last_fingerprint.as_ref() != Some(&fingerprint) =>
                        {
                            last_fingerprint = Some(fingerprint.clone());
                            record_watch_event(
                                &state,
                                ConfigWatchEventKind::PollingChange,
                                source_identity_sha256.as_str(),
                                Some(fingerprint.content_sha256.as_str()),
                                "daemon.config_watch.poll_change",
                                watcher_generation,
                            );
                            debounce.observe(now);
                        }
                        Ok(_) => {}
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                            if last_fingerprint.take().is_some() {
                                record_watch_event(
                                    &state,
                                    ConfigWatchEventKind::Missing,
                                    source_identity_sha256.as_str(),
                                    None,
                                    "daemon.config_watch.file_missing",
                                    watcher_generation,
                                );
                            }
                        }
                        Err(error) => {
                            warn!(message = %error, "config polling fingerprint failed");
                        }
                    }
                }
                if debounce.take_ready(now) {
                    process_candidate(
                        &state,
                        path.as_path(),
                        source_identity_sha256.as_str(),
                        watcher_generation,
                    )
                    .await;
                }
            }
            native = native_rx.recv() => {
                match native {
                    Some(Ok(event)) if native_event_matches(&event, path.as_path()) => {
                        record_watch_event(
                            &state,
                            ConfigWatchEventKind::NativeEvent,
                            source_identity_sha256.as_str(),
                            None,
                            "daemon.config_watch.native_change",
                            watcher_generation,
                        );
                        debounce.observe(Instant::now());
                    }
                    Some(Ok(_)) => {}
                    Some(Err(error)) => {
                        warn!(message = %error, "native config watcher failed; recreating with backoff");
                        watcher = None;
                        watcher_retry_at = Instant::now() + watcher_backoff;
                        record_watch_event(
                            &state,
                            ConfigWatchEventKind::PollingFallback,
                            source_identity_sha256.as_str(),
                            None,
                            "daemon.config_watch.native_failed",
                            watcher_generation,
                        );
                    }
                    None => {
                        watcher = None;
                        watcher_retry_at = Instant::now() + watcher_backoff;
                    }
                }
            }
            changed = lifecycle.changed() => {
                if changed.is_err() || lifecycle.borrow().phase.blocks_admission() {
                    return;
                }
            }
        }
    }
}

async fn process_candidate(
    state: &AppState,
    path: &Path,
    source_identity_sha256: &str,
    watcher_generation: u64,
) {
    let bytes = match fs::read(path) {
        Ok(bytes) if bytes.len() <= CONFIG_FILE_MAX_BYTES => bytes,
        Ok(_) => {
            record_watch_event(
                state,
                ConfigWatchEventKind::Invalid,
                source_identity_sha256,
                None,
                "daemon.config_watch.file_too_large",
                watcher_generation,
            );
            return;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            record_watch_event(
                state,
                ConfigWatchEventKind::Missing,
                source_identity_sha256,
                None,
                "daemon.config_watch.file_missing",
                watcher_generation,
            );
            return;
        }
        Err(error) => {
            warn!(message = %error, "failed to read config watcher candidate");
            return;
        }
    };
    let config_sha256 = hex::encode(Sha256::digest(bytes.as_slice()));
    match tokio::task::spawn_blocking(crate::config::load_config).await {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => {
            warn!(message = %error, "config watcher candidate validation failed");
            record_watch_event(
                state,
                ConfigWatchEventKind::Invalid,
                source_identity_sha256,
                Some(config_sha256.as_str()),
                "daemon.config_watch.candidate_invalid",
                watcher_generation,
            );
            return;
        }
        Err(error) => {
            warn!(message = %error, "config watcher validation worker failed");
            return;
        }
    }
    record_watch_event(
        state,
        ConfigWatchEventKind::Validated,
        source_identity_sha256,
        Some(config_sha256.as_str()),
        "daemon.config_watch.candidate_valid",
        watcher_generation,
    );

    let context = RequestContext {
        principal: "system:config_watcher".to_owned(),
        device_id: "daemon".to_owned(),
        channel: Some("internal".to_owned()),
    };
    let path_text = path.to_string_lossy().into_owned();
    let plan = match plan_config_reload_for_context(
        state,
        &context,
        ConfigReloadPlanRequest { path: Some(path_text.clone()) },
    )
    .await
    {
        Ok(plan) => plan,
        Err(response) => {
            warn!(status = %response.status(), "config watcher reload planning failed");
            return;
        }
    };
    if plan.hot_safe_applicable {
        let apply = ConfigReloadApplyRequest {
            path: Some(path_text),
            plan_id: Some(plan.plan_id.clone()),
            idempotency_key: Some(config_sha256.clone()),
            dry_run: false,
            force: false,
        };
        if let Err(response) = apply_config_reload_for_context(state, &context, apply).await {
            warn!(status = %response.status(), "config watcher hot-safe apply failed");
            return;
        }
    }
    let last_known_good_sha256 =
        match state.runtime.journal_store.latest_config_last_known_good(source_identity_sha256) {
            Ok(Some(digest)) => digest,
            Ok(None) => {
                warn!("config watcher last-known-good reference is missing");
                return;
            }
            Err(error) => {
                warn!(message = %error, "config watcher last-known-good lookup failed");
                return;
            }
        };
    let coalescing_key = restart_coalescing_key(
        source_identity_sha256,
        config_sha256.as_str(),
        last_known_good_sha256.as_str(),
    );
    let request = RestartRequest {
        request_id: Ulid::new().to_string(),
        coalescing_key,
        config_sha256,
        source_identity_sha256: source_identity_sha256.to_owned(),
        last_known_good_sha256,
        restart_required_steps: plan.summary.restart_required,
        hot_safe_steps: plan.summary.hot_safe,
        requested_at_unix_ms: unix_ms_now(),
    };
    match state
        .runtime
        .coordinate_config_restart(
            request,
            plan.summary.blocked_while_runs_active,
            plan.summary.manual_review,
        )
        .await
    {
        Ok(decision) => {
            if matches!(
                decision.kind,
                crate::application::restart_coordinator::RestartDecisionKind::ReadyNow
                    | crate::application::restart_coordinator::RestartDecisionKind::ScheduledAfterDrain
                    | crate::application::restart_coordinator::RestartDecisionKind::Cancelled
            ) && decision.request.config_sha256 != decision.request.last_known_good_sha256
            {
                let content = match String::from_utf8(bytes) {
                    Ok(content) => content,
                    Err(error) => {
                        warn!(message = %error, "validated config was not UTF-8 at backup boundary");
                        return;
                    }
                };
                if let Err(error) =
                    write_secret_content_with_backups(path, content.as_str(), 3)
                {
                    warn!(message = %error, "failed to rotate accepted config backup");
                    return;
                }
                if let Err(error) = state.runtime.journal_store.record_config_last_known_good(
                    decision.request.config_sha256.as_str(),
                    source_identity_sha256,
                    Some(decision.request.request_id.as_str()),
                    "daemon.config.accepted",
                ) {
                    warn!(message = %error, "failed to persist accepted config reference");
                    return;
                }
            }
            info!(
                outcome = decision.kind.as_str(),
                reason_code = decision.reason_code,
                "config watcher restart decision committed"
            );
        }
        Err(error) => {
            warn!(
                code = ?error.code(),
                message = %error.message(),
                "config watcher restart coordination failed"
            );
        }
    }
}

fn create_native_watcher(
    path: &Path,
    sender: mpsc::UnboundedSender<notify::Result<Event>>,
) -> notify::Result<RecommendedWatcher> {
    let watch_root = path.parent().unwrap_or_else(|| Path::new("."));
    let mut watcher = RecommendedWatcher::new(
        move |event| {
            let _ = sender.send(event);
        },
        Config::default(),
    )?;
    watcher.watch(watch_root, RecursiveMode::NonRecursive)?;
    Ok(watcher)
}

fn native_event_matches(event: &Event, target: &Path) -> bool {
    if matches!(event.kind, EventKind::Access(_)) {
        return false;
    }
    event.paths.is_empty() || event.paths.iter().any(|path| path == target)
}

fn read_fingerprint(path: &Path) -> std::io::Result<FileFingerprint> {
    let metadata = fs::metadata(path)?;
    let bytes = fs::read(path)?;
    if bytes.len() > CONFIG_FILE_MAX_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "config file exceeds watcher size limit",
        ));
    }
    let modified_unix_nanos = metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map_or(0, |duration| duration.as_nanos());
    Ok(FileFingerprint {
        len: metadata.len(),
        modified_unix_nanos,
        content_sha256: hex::encode(Sha256::digest(bytes)),
    })
}

fn ensure_seeded_backup(path: &Path) -> Result<()> {
    let active = read_fingerprint(path)
        .with_context(|| format!("failed to fingerprint validated config {}", path.display()))?;
    if (1..=3).any(|index| {
        let candidate = backup_path(path, index);
        candidate.exists()
            && crate::config::load_config_from_path(candidate.as_path()).is_ok()
            && read_fingerprint(candidate.as_path())
                .is_ok_and(|fingerprint| fingerprint.content_sha256 == active.content_sha256)
    }) {
        return Ok(());
    }
    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read validated config {}", path.display()))?;
    write_secret_content_with_backups(path, content.as_str(), 3)
        .context("failed to seed config last-known-good backup")
}

fn seed_backup_and_refresh_fingerprint(path: &Path) -> Result<FileFingerprint> {
    ensure_seeded_backup(path)?;
    read_fingerprint(path).with_context(|| {
        format!("validated config disappeared before watcher initialization: {}", path.display())
    })
}

fn valid_backup_fingerprint(path: &Path) -> Result<FileFingerprint> {
    for index in 1..=3 {
        let candidate = backup_path(path, index);
        if !candidate.exists() || crate::config::load_config_from_path(candidate.as_path()).is_err()
        {
            continue;
        }
        return read_fingerprint(candidate.as_path()).map_err(anyhow::Error::from);
    }
    anyhow::bail!("no valid config backup was found")
}

fn source_identity_sha256(path: &Path) -> String {
    let normalized = path
        .canonicalize()
        .unwrap_or_else(|_| path.to_path_buf())
        .to_string_lossy()
        .replace('\\', "/");
    hex::encode(Sha256::digest(normalized.as_bytes()))
}

fn restart_coalescing_key(
    source_identity_sha256: &str,
    config_sha256: &str,
    last_known_good_sha256: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"palyra-config-restart-v1\0");
    hasher.update(source_identity_sha256.as_bytes());
    hasher.update(b"\0");
    hasher.update(config_sha256.as_bytes());
    hasher.update(b"\0");
    hasher.update(last_known_good_sha256.as_bytes());
    hex::encode(hasher.finalize())
}

fn record_watch_event(
    state: &AppState,
    kind: ConfigWatchEventKind,
    source_identity_sha256: &str,
    config_sha256: Option<&str>,
    reason_code: &str,
    watcher_generation: u64,
) {
    let event = ConfigWatchEventV1 {
        event_id: Ulid::new().to_string(),
        kind,
        source_identity_sha256: source_identity_sha256.to_owned(),
        config_sha256: config_sha256.map(str::to_owned),
        reason_code: reason_code.to_owned(),
        watcher_generation,
        observed_at_unix_ms: unix_ms_now(),
        schema_version: 1,
    };
    if let Err(error) = state.runtime.journal_store.record_config_watch_event(&event) {
        warn!(message = %error, "failed to persist config watcher evidence");
    }
}

fn unix_ms_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rapid_events_extend_one_debounce_deadline() {
        let now = Instant::now();
        let mut debounce = DebounceState::default();
        debounce.observe(now);
        debounce.observe(now + Duration::from_millis(400));
        assert!(!debounce.take_ready(now + Duration::from_millis(500)));
        assert!(debounce.take_ready(now + Duration::from_millis(900)));
        assert!(!debounce.take_ready(now + Duration::from_secs(1)));
    }

    #[test]
    fn polling_fingerprint_detects_missing_and_recreated_file() {
        let directory = tempfile::tempdir().expect("temp directory should exist");
        let path = directory.path().join("config.toml");
        fs::write(&path, b"config_version = 1").expect("fixture should write");
        let first = read_fingerprint(&path).expect("fixture should fingerprint");
        fs::remove_file(&path).expect("fixture should be removed");
        assert_eq!(
            read_fingerprint(&path).expect_err("missing file should fail").kind(),
            std::io::ErrorKind::NotFound
        );
        fs::write(&path, b"config_version = 2").expect("fixture should recreate");
        let second = read_fingerprint(&path).expect("recreated fixture should fingerprint");
        assert_ne!(first.content_sha256, second.content_sha256);
    }

    #[test]
    fn native_access_events_do_not_trigger_reload() {
        let event = Event {
            kind: EventKind::Access(notify::event::AccessKind::Any),
            paths: vec![PathBuf::from("config.toml")],
            attrs: notify::event::EventAttributes::default(),
        };
        assert!(!native_event_matches(&event, Path::new("config.toml")));
    }

    #[test]
    fn loaded_source_path_excludes_migration_and_environment_provenance() {
        assert_eq!(
            path_from_loaded_source("config.toml +migration(v0->v1) +env(PALYRA_DAEMON_BIND_ADDR)"),
            Some(PathBuf::from("config.toml"))
        );
        assert_eq!(
            path_from_loaded_source("config.toml +env(PALYRA_DAEMON_PORT)"),
            Some(PathBuf::from("config.toml"))
        );
        assert_eq!(path_from_loaded_source("defaults"), None);
    }

    #[test]
    fn backup_seeding_refreshes_the_polling_baseline() {
        let directory = tempfile::tempdir().expect("temp directory should exist");
        let path = directory.path().join("config.toml");
        fs::write(&path, b"config_version = 1").expect("fixture should write");

        let baseline = seed_backup_and_refresh_fingerprint(&path)
            .expect("valid config should establish a watcher baseline");
        let live = read_fingerprint(&path).expect("seeded config should remain readable");

        assert_eq!(baseline, live);
        assert!(backup_path(&path, 1).exists());
    }
}
