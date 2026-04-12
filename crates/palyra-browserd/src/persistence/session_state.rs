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
            warn!(
                profile_id = profile_id.as_str(),
                error = %error,
                "failed to update browser profile state metadata after snapshot persist"
            );
        }
    }
    Ok(())
}

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

pub(crate) fn map_persist_error_to_status(error: anyhow::Error) -> Status {
    Status::internal(error.to_string())
}

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
