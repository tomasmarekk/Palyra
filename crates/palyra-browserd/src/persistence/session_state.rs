//! Session snapshot hashing, persistence triggers, and cookie/storage mutation rules.
//!
//! Snapshot hashes are computed over a canonical sorted-map form so they stay stable across
//! `HashMap` iteration orders; network logs are intentionally never persisted.

use crate::*;

fn map_to_sorted_map(map: &HashMap<String, String>) -> BTreeMap<String, String> {
    map.iter().map(|(key, value)| (key.clone(), value.clone())).collect()
}

fn nested_map_to_sorted_map(
    map: &HashMap<String, HashMap<String, String>>,
) -> BTreeMap<String, BTreeMap<String, String>> {
    map.iter().map(|(key, value)| (key.clone(), map_to_sorted_map(value))).collect()
}

fn tab_record_for_hash(tab: &BrowserTabRecord) -> BrowserTabRecordForHash {
    BrowserTabRecordForHash {
        tab_id: tab.tab_id.clone(),
        last_title: tab.last_title.clone(),
        last_url: tab.last_url.clone(),
        last_page_body: tab.last_page_body.clone(),
        scroll_x: tab.scroll_x,
        scroll_y: tab.scroll_y,
        typed_inputs: map_to_sorted_map(&tab.typed_inputs),
        network_log: tab.network_log.clone(),
    }
}

/// Computes the canonical SHA-256 hash of a snapshot, stable across map iteration orders.
///
/// This hash is what gets recorded in the profile registry and re-checked on restore.
///
/// # Errors
/// Fails when the canonical form does not serialize.
pub(crate) fn persisted_snapshot_hash(snapshot: &PersistedSessionSnapshot) -> Result<String> {
    let canonical = PersistedSessionSnapshotForHash {
        v: snapshot.v,
        principal: snapshot.principal.clone(),
        channel: snapshot.channel.clone(),
        tabs: snapshot.tabs.iter().map(tab_record_for_hash).collect(),
        tab_order: snapshot.tab_order.clone(),
        active_tab_id: snapshot.active_tab_id.clone(),
        permissions: snapshot.permissions.clone(),
        cookie_jar: nested_map_to_sorted_map(&snapshot.cookie_jar),
        storage_entries: nested_map_to_sorted_map(&snapshot.storage_entries),
        state_revision: snapshot.state_revision,
        saved_at_unix_ms: snapshot.saved_at_unix_ms,
    };
    let bytes = serde_json::to_vec(&canonical)
        .context("failed to serialize persisted browser state snapshot hash payload")?;
    Ok(sha256_hex(bytes.as_slice()))
}

/// Computes the hash in the pre-`state_revision` layout for backward-compatible validation.
///
/// Only consulted for revision-0 snapshots; see [`validate_restored_snapshot_against_profile`].
///
/// # Errors
/// Fails when the legacy form does not serialize.
pub(crate) fn persisted_snapshot_legacy_hash(
    snapshot: &PersistedSessionSnapshot,
) -> Result<String> {
    let legacy = PersistedSessionSnapshotLegacyForHash {
        v: snapshot.v,
        principal: snapshot.principal.clone(),
        channel: snapshot.channel.clone(),
        tabs: snapshot.tabs.clone(),
        tab_order: snapshot.tab_order.clone(),
        active_tab_id: snapshot.active_tab_id.clone(),
        permissions: snapshot.permissions.clone(),
        cookie_jar: snapshot.cookie_jar.clone(),
        storage_entries: snapshot.storage_entries.clone(),
        saved_at_unix_ms: snapshot.saved_at_unix_ms,
    };
    let bytes = serde_json::to_vec(&legacy)
        .context("failed to serialize legacy persisted browser state snapshot hash payload")?;
    Ok(sha256_hex(bytes.as_slice()))
}

/// Verifies a restored snapshot against the profile's recorded revision and state hash.
///
/// Acceptance order: raw plaintext hash, canonical hash, then — only for revision-0
/// (pre-revision) snapshots — the legacy hash layout. A profile without a recorded hash
/// accepts any snapshot at or above its revision.
///
/// # Errors
/// Fails when the snapshot revision is older than the profile's (rollback) or no hash form
/// matches (tampering or corruption).
pub(crate) fn validate_restored_snapshot_against_profile(
    snapshot: &PersistedSessionSnapshot,
    raw_hash_sha256: Option<&str>,
    profile: &BrowserProfileRecord,
) -> Result<()> {
    if snapshot.state_revision < profile.state_revision {
        anyhow::bail!(
            "snapshot revision {} is older than profile revision {}",
            snapshot.state_revision,
            profile.state_revision
        );
    }
    let Some(expected_hash) = profile.state_hash_sha256.as_deref() else {
        return Ok(());
    };
    if raw_hash_sha256.is_some_and(|raw_hash| raw_hash == expected_hash) {
        return Ok(());
    }
    let current_hash = persisted_snapshot_hash(snapshot)?;
    if current_hash == expected_hash {
        return Ok(());
    }
    if snapshot.state_revision == 0 {
        let legacy_hash = persisted_snapshot_legacy_hash(snapshot)?;
        if legacy_hash == expected_hash {
            return Ok(());
        }
    }
    anyhow::bail!("snapshot hash mismatch for profile '{}'", profile.profile_id);
}

/// Writes the session's encrypted snapshot and bumps the profile state revision.
///
/// No-op when persistence is disabled. Tab order is preserved and tabs missing from
/// `tab_order` are appended so no tab state is silently lost; network logs are stripped before
/// writing. A failed profile-metadata update is logged but does not fail the persist.
///
/// # Errors
/// Fails when the persistence id is missing, revision lookup fails, or hashing/encryption/
/// writing of the snapshot fails.
pub(crate) fn persist_session_snapshot(
    store: &PersistedStateStore,
    session: &BrowserSessionRecord,
) -> Result<()> {
    if !session.persistence.enabled {
        return Ok(());
    }
    let Some(persistence_id) = session.persistence.persistence_id.as_ref() else {
        anyhow::bail!("state persistence is enabled but persistence_id is missing");
    };
    let mut tabs = session
        .tab_order
        .iter()
        .filter_map(|tab_id| session.tabs.get(tab_id.as_str()).cloned())
        .collect::<Vec<_>>();
    for (tab_id, tab) in &session.tabs {
        if !tabs.iter().any(|entry| entry.tab_id == *tab_id) {
            tabs.push(tab.clone());
        }
    }
    for tab in &mut tabs {
        tab.network_log.clear();
    }
    let state_revision = next_profile_state_revision(store, session.profile_id.as_deref())?;
    let snapshot = PersistedSessionSnapshot {
        v: CANONICAL_PROTOCOL_MAJOR,
        principal: session.principal.clone(),
        channel: session.channel.clone(),
        tabs,
        tab_order: session.tab_order.clone(),
        active_tab_id: session.active_tab_id.clone(),
        permissions: session.permissions.clone(),
        cookie_jar: session.cookie_jar.clone(),
        storage_entries: session.storage_entries.clone(),
        state_revision,
        saved_at_unix_ms: current_unix_ms(),
    };
    let snapshot_hash = persisted_snapshot_hash(&snapshot)?;
    store.save_snapshot(persistence_id.as_str(), session.profile_id.as_deref(), &snapshot)?;
    if let Some(profile_id) = session.profile_id.as_ref() {
        if let Err(error) = update_profile_state_metadata(
            store,
            profile_id.as_str(),
            PROFILE_RECORD_SCHEMA_VERSION,
            state_revision,
            snapshot_hash.as_str(),
        ) {
            // Best-effort: the snapshot itself is already persisted; a stale profile hash will
            // surface at restore validation rather than losing the newer state here.
            warn!(
                profile_id = profile_id.as_str(),
                error = %error,
                "failed to update browser profile state metadata after snapshot persist"
            );
        }
    }
    Ok(())
}

/// Persists a session copy taken after a state mutation, if persistence applies.
///
/// No-op when there is no state store, no session copy, or persistence is disabled for the
/// session.
///
/// # Errors
/// Fails when [`persist_session_snapshot`] fails; `operation` names the mutation in the error
/// context.
pub(crate) fn persist_session_after_mutation(
    runtime: &BrowserRuntimeState,
    session_for_persist: Option<BrowserSessionRecord>,
    operation: &str,
) -> Result<()> {
    if let (Some(store), Some(session)) = (runtime.state_store.as_ref(), session_for_persist) {
        if session.persistence.enabled {
            persist_session_snapshot(store, &session)
                .with_context(|| format!("failed to persist state after {operation}"))?;
        }
    }
    Ok(())
}

/// Maps a persistence failure to a gRPC internal-error status.
pub(crate) fn map_persist_error_to_status(error: anyhow::Error) -> Status {
    Status::internal(error.to_string())
}

/// Builds a Cookie header value for a URL from the session jar, or `None` when no cookies
/// apply.
///
/// Only cookies stored under the URL's exact host are included (no parent-domain matching);
/// pairs are sorted for deterministic output.
pub(crate) fn cookie_header_for_url(
    session: &BrowserSessionRecord,
    raw_url: &str,
) -> Option<String> {
    let domain = Url::parse(raw_url).ok()?.host_str()?.to_ascii_lowercase();
    let cookies = session.cookie_jar.get(domain.as_str())?;
    if cookies.is_empty() {
        return None;
    }
    let mut pairs =
        cookies.iter().map(|(name, value)| format!("{name}={value}")).collect::<Vec<_>>();
    pairs.sort();
    Some(pairs.join("; "))
}

/// Parses a Set-Cookie header into a [`CookieUpdate`] for `domain`.
///
/// Only the leading name=value pair is kept; cookie attributes (Path, Expires, ...) are
/// ignored, the name is lowercased, and the value is capped at 1024 bytes.
pub(crate) fn parse_set_cookie_update(domain: &str, raw_set_cookie: &str) -> Option<CookieUpdate> {
    let normalized_domain = domain.trim().trim_matches('.').to_ascii_lowercase();
    if normalized_domain.is_empty() {
        return None;
    }
    let first_pair = raw_set_cookie.split(';').next()?.trim();
    let (name, value) = first_pair.split_once('=')?;
    let name = name.trim().to_ascii_lowercase();
    if name.is_empty() {
        return None;
    }
    Some(CookieUpdate {
        domain: normalized_domain,
        name,
        value: truncate_utf8_bytes(value.trim(), 1024),
    })
}

/// Applies cookie updates to the session jar under the per-session/per-domain caps.
///
/// An empty value deletes the cookie (and the domain once empty). When a cap is reached, new
/// entries are dropped rather than evicting existing ones.
pub(crate) fn apply_cookie_updates(session: &mut BrowserSessionRecord, updates: &[CookieUpdate]) {
    for update in updates {
        if update.domain.is_empty() || update.name.is_empty() {
            continue;
        }
        if update.value.is_empty() {
            if let Some(domain_cookies) = session.cookie_jar.get_mut(update.domain.as_str()) {
                domain_cookies.remove(update.name.as_str());
                if domain_cookies.is_empty() {
                    session.cookie_jar.remove(update.domain.as_str());
                }
            }
            continue;
        }
        if !session.cookie_jar.contains_key(update.domain.as_str())
            && session.cookie_jar.len() >= MAX_COOKIE_DOMAINS_PER_SESSION
        {
            continue;
        }
        let domain_cookies = session.cookie_jar.entry(update.domain.clone()).or_default();
        if !domain_cookies.contains_key(update.name.as_str())
            && domain_cookies.len() >= MAX_COOKIES_PER_DOMAIN
        {
            continue;
        }
        domain_cookies.insert(update.name.clone(), update.value.clone());
    }
}

/// Test-only helper: mutates one storage entry, either replacing or appending to its value.
///
/// Mirrors the cap-and-truncate rules of [`replace_storage_entries_for_origin`] at single-entry
/// granularity.
#[cfg(test)]
pub(crate) fn apply_storage_entry_update(
    session: &mut BrowserSessionRecord,
    origin: &str,
    key: &str,
    value: &str,
    clear_existing: bool,
) {
    let origin = origin.trim();
    let key = key.trim();
    if origin.is_empty() || key.is_empty() {
        return;
    }
    if !session.storage_entries.contains_key(origin)
        && session.storage_entries.len() >= MAX_STORAGE_ORIGINS_PER_SESSION
    {
        return;
    }
    let storage = session.storage_entries.entry(origin.to_owned()).or_default();
    if !storage.contains_key(key) && storage.len() >= MAX_STORAGE_ENTRIES_PER_ORIGIN {
        return;
    }
    if clear_existing {
        storage.insert(key.to_owned(), truncate_utf8_bytes(value, MAX_STORAGE_ENTRY_VALUE_BYTES));
        return;
    }
    let existing = storage.entry(key.to_owned()).or_default();
    let mut combined = String::with_capacity(existing.len() + value.len());
    combined.push_str(existing.as_str());
    combined.push_str(value);
    *existing = truncate_utf8_bytes(combined.as_str(), MAX_STORAGE_ENTRY_VALUE_BYTES);
}

/// Replaces an origin's storage entries wholesale, clamping to the per-origin caps.
///
/// An empty (or fully-clamped-away) entry map removes the origin. New origins are dropped when
/// the per-session origin cap is reached.
pub(crate) fn replace_storage_entries_for_origin(
    session: &mut BrowserSessionRecord,
    origin: &str,
    entries: HashMap<String, String>,
) {
    let origin = origin.trim();
    if origin.is_empty() {
        return;
    }
    if entries.is_empty() {
        session.storage_entries.remove(origin);
        return;
    }
    if !session.storage_entries.contains_key(origin)
        && session.storage_entries.len() >= MAX_STORAGE_ORIGINS_PER_SESSION
    {
        return;
    }
    let mut clamped_entries = HashMap::new();
    for (key, value) in entries {
        let key = key.trim();
        if key.is_empty() {
            continue;
        }
        if !clamped_entries.contains_key(key)
            && clamped_entries.len() >= MAX_STORAGE_ENTRIES_PER_ORIGIN
        {
            break;
        }
        clamped_entries.insert(
            key.to_owned(),
            truncate_utf8_bytes(value.as_str(), MAX_STORAGE_ENTRY_VALUE_BYTES),
        );
    }
    if clamped_entries.is_empty() {
        session.storage_entries.remove(origin);
    } else {
        session.storage_entries.insert(origin.to_owned(), clamped_entries);
    }
}

/// Clamps a restored cookie jar to current domain/cookie caps and value byte limits.
///
/// Which entries survive when over a cap is arbitrary (`HashMap` iteration order); restored
/// snapshots are untrusted input, so the caps matter more than the selection.
pub(crate) fn clamp_cookie_jar(
    cookie_jar: HashMap<String, HashMap<String, String>>,
) -> HashMap<String, HashMap<String, String>> {
    let mut clamped = HashMap::new();
    for (domain, cookies) in cookie_jar {
        if domain.trim().is_empty() {
            continue;
        }
        if clamped.len() >= MAX_COOKIE_DOMAINS_PER_SESSION {
            break;
        }
        let mut clamped_cookies = HashMap::new();
        for (name, value) in cookies {
            if name.trim().is_empty() {
                continue;
            }
            if clamped_cookies.len() >= MAX_COOKIES_PER_DOMAIN {
                break;
            }
            clamped_cookies.insert(name, truncate_utf8_bytes(value.as_str(), 1024));
        }
        if !clamped_cookies.is_empty() {
            clamped.insert(domain, clamped_cookies);
        }
    }
    clamped
}

/// Clamps restored storage state to current origin/entry caps and value byte limits.
///
/// Same arbitrary-survivor caveat as [`clamp_cookie_jar`].
pub(crate) fn clamp_storage_entries(
    storage_entries: HashMap<String, HashMap<String, String>>,
) -> HashMap<String, HashMap<String, String>> {
    let mut clamped = HashMap::new();
    for (origin, entries) in storage_entries {
        if origin.trim().is_empty() {
            continue;
        }
        if clamped.len() >= MAX_STORAGE_ORIGINS_PER_SESSION {
            break;
        }
        let mut clamped_entries = HashMap::new();
        for (key, value) in entries {
            if key.trim().is_empty() {
                continue;
            }
            if clamped_entries.len() >= MAX_STORAGE_ENTRIES_PER_ORIGIN {
                break;
            }
            clamped_entries
                .insert(key, truncate_utf8_bytes(value.as_str(), MAX_STORAGE_ENTRY_VALUE_BYTES));
        }
        if !clamped_entries.is_empty() {
            clamped.insert(origin, clamped_entries);
        }
    }
    clamped
}

/// Normalizes a URL to its storage-origin key (`scheme://host[:port]`, default ports omitted).
pub(crate) fn url_origin_key(raw_url: &str) -> Option<String> {
    let url = Url::parse(raw_url).ok()?;
    let host = url.host_str()?.to_ascii_lowercase();
    let mut origin = format!("{}://{host}", url.scheme());
    if let Some(port) = url.port() {
        if !is_default_port(url.scheme(), port) {
            origin.push(':');
            origin.push_str(port.to_string().as_str());
        }
    }
    Some(origin)
}
