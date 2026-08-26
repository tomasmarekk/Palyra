//! Session snapshot hashing, persistence triggers, and cookie/storage mutation rules.
//!
//! Snapshot hashes are computed over a canonical sorted-map form so they stay stable across
//! `HashMap` iteration orders; network logs are intentionally never persisted.

use std::io::{BufReader, Cursor};

use crate::*;

const MAX_COOKIE_NAME_BYTES: usize = 128;
const MAX_COOKIE_VALUE_BYTES: usize = 1024;
const MAX_PERSISTED_COOKIE_ENTRY_BYTES: usize = 4 * 1024;

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
        cookie_store: snapshot.cookie_store.clone(),
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
/// writing. Profile-backed snapshots hold the profile registry lock across revision lookup,
/// snapshot write, and metadata refresh. A failed profile-metadata update is logged but does not
/// fail the persist.
///
/// # Errors
/// Fails when the persistence id is missing, revision lookup fails, or hashing/encryption/
/// writing of the snapshot fails.
pub(crate) async fn persist_session_snapshot(
    runtime: &BrowserRuntimeState,
    session: &BrowserSessionRecord,
) -> Result<()> {
    if !session.persistence.enabled {
        return Ok(());
    }
    let Some(store) = runtime.state_store.as_ref() else {
        return Ok(());
    };
    let Some(persistence_id) = session.persistence.persistence_id.as_ref() else {
        anyhow::bail!("state persistence is enabled but persistence_id is missing");
    };
    let _profile_registry_guard = if session.profile_id.is_some() {
        Some(runtime.profile_registry_lock.lock().await)
    } else {
        None
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
    let cookie_store = serialize_cookie_store(&session.cookie_store)?;
    let snapshot = PersistedSessionSnapshot {
        v: CANONICAL_PROTOCOL_MAJOR,
        principal: session.principal.clone(),
        channel: session.channel.clone(),
        tabs,
        tab_order: session.tab_order.clone(),
        active_tab_id: session.active_tab_id.clone(),
        permissions: session.permissions.clone(),
        cookie_store,
        cookie_jar: session.cookie_jar.clone(),
        storage_entries: session.storage_entries.clone(),
        state_revision,
        saved_at_unix_ms: current_unix_ms(),
    };
    let snapshot_hash = persisted_snapshot_hash(&snapshot)?;
    store.save_snapshot(persistence_id.as_str(), session.profile_id.as_deref(), &snapshot)?;
    if let Some(profile_id) = session.profile_id.as_ref() {
        if let Err(error) = update_profile_state_metadata_locked(
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
pub(crate) async fn persist_session_after_mutation(
    runtime: &BrowserRuntimeState,
    session_for_persist: Option<BrowserSessionRecord>,
    operation: &str,
) -> Result<()> {
    if let Some(session) = session_for_persist {
        if session.persistence.enabled {
            persist_session_snapshot(runtime, &session)
                .await
                .with_context(|| format!("failed to persist state after {operation}"))?;
        }
    }
    Ok(())
}

/// Maps a persistence failure to a gRPC internal-error status.
pub(crate) fn map_persist_error_to_status(error: anyhow::Error) -> Status {
    Status::internal(error.to_string())
}

/// Builds a Cookie header for a URL using RFC6265 scheme, domain, path, and expiry rules.
///
/// Pairs are sorted for deterministic output. Legacy display-only snapshot entries
/// without complete attributes are intentionally not replayed.
pub(crate) fn cookie_header_for_url(
    session: &BrowserSessionRecord,
    raw_url: &str,
) -> Option<String> {
    let url = Url::parse(raw_url).ok()?;
    let mut pairs = session
        .cookie_store
        .get_request_values(&url)
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>();
    if pairs.is_empty() {
        return None;
    }
    pairs.sort();
    Some(pairs.join("; "))
}

/// Parses a bounded Set-Cookie header and binds it to its response URL.
///
/// The RFC6265 store later validates Domain/host-only semantics and enforces
/// Secure, Path, Expires, and Max-Age when selecting request cookies.
pub(crate) fn parse_set_cookie_update(
    request_url: &Url,
    raw_set_cookie: &str,
) -> Option<CookieUpdate> {
    if !matches!(request_url.scheme(), "http" | "https")
        || raw_set_cookie.len() > MAX_PERSISTED_COOKIE_ENTRY_BYTES
    {
        return None;
    }
    let mut cookie = RawCookie::parse(raw_set_cookie).ok()?.into_owned();
    if cookie.name().is_empty() || cookie.name().len() > MAX_COOKIE_NAME_BYTES {
        return None;
    }
    cookie.set_value(truncate_utf8_bytes(cookie.value(), MAX_COOKIE_VALUE_BYTES));
    Some(CookieUpdate { request_url: request_url.clone(), cookie })
}

/// Applies RFC6265 cookie updates under the per-session/per-domain caps.
///
/// Invalid updates and updates that would cross a quota are dropped rather than
/// weakening cookie scoping or evicting existing state.
pub(crate) fn apply_cookie_updates(session: &mut BrowserSessionRecord, updates: &[CookieUpdate]) {
    let mut store = session.cookie_store.clone();
    for update in updates {
        let previous = store.clone();
        if store.insert_raw(&update.cookie, &update.request_url).is_err()
            || !cookie_store_within_quotas(&store)
        {
            store = previous;
            continue;
        }
    }
    session.cookie_jar = cookie_debug_jar(&store);
    session.cookie_store = store;
}

fn cookie_store_within_quotas(store: &CookieStore) -> bool {
    let mut cookies_per_domain = HashMap::<String, usize>::new();
    let mut total = 0_usize;
    for cookie in store.iter_unexpired() {
        total = total.saturating_add(1);
        if total > MAX_COOKIE_DOMAINS_PER_SESSION.saturating_mul(MAX_COOKIES_PER_DOMAIN) {
            return false;
        }
        let domain = String::from(&cookie.domain);
        let count = cookies_per_domain.entry(domain).or_default();
        *count = count.saturating_add(1);
        if *count > MAX_COOKIES_PER_DOMAIN {
            return false;
        }
    }
    cookies_per_domain.len() <= MAX_COOKIE_DOMAINS_PER_SESSION
}

/// Builds the bounded compatibility view returned by inspect/session APIs.
///
/// Request replay never consults this path-collapsed map; the attribute-aware
/// store above remains authoritative.
pub(crate) fn cookie_debug_jar(store: &CookieStore) -> HashMap<String, HashMap<String, String>> {
    let mut cookies = store
        .iter_unexpired()
        .map(|cookie| {
            (
                String::from(&cookie.domain),
                cookie.name().to_owned(),
                String::from(&cookie.path),
                truncate_utf8_bytes(cookie.value(), MAX_COOKIE_VALUE_BYTES),
            )
        })
        .collect::<Vec<_>>();
    cookies.sort();
    let mut jar = HashMap::<String, HashMap<String, String>>::new();
    for (domain, name, _path, value) in cookies {
        jar.entry(domain).or_default().insert(name, value);
    }
    jar
}

/// Serializes complete, unexpired cookie records for the encrypted snapshot.
pub(crate) fn serialize_cookie_store(store: &CookieStore) -> Result<Vec<String>> {
    let mut entries = store
        .iter_unexpired()
        .map(serde_json::to_string)
        .collect::<serde_json::Result<Vec<_>>>()
        .context("failed to serialize browser cookie store")?;
    entries.sort();
    Ok(entries)
}

/// Restores a bounded attribute-aware store, failing closed to an empty store.
pub(crate) fn restore_cookie_store(entries: Vec<String>) -> CookieStore {
    let maximum_entries = MAX_COOKIE_DOMAINS_PER_SESSION.saturating_mul(MAX_COOKIES_PER_DOMAIN);
    if entries.len() > maximum_entries
        || entries.iter().any(|entry| entry.len() > MAX_PERSISTED_COOKIE_ENTRY_BYTES)
    {
        return CookieStore::default();
    }
    let serialized = entries.join("\n");
    let Ok(store) = CookieStore::load(BufReader::new(Cursor::new(serialized.as_bytes())), |line| {
        serde_json::from_str::<cookie_store::Cookie<'static>>(line)
    }) else {
        return CookieStore::default();
    };
    if cookie_store_within_quotas(&store) {
        store
    } else {
        CookieStore::default()
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
            clamped_cookies
                .insert(name, truncate_utf8_bytes(value.as_str(), MAX_COOKIE_VALUE_BYTES));
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
