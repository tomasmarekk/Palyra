//! Browser profile registry: tamper-evident profile records and LRU pruning.
//!
//! Each record carries a self-hash; records failing both the current and the legacy hash check
//! are dropped on load. Pruning never evicts a principal's active profile. Registry mutations,
//! including snapshot metadata refreshes, are serialized through
//! `BrowserRuntimeState::profile_registry_lock`.

use crate::*;

/// One persisted browser profile.
///
/// `record_hash_sha256` is a self-hash over the other fields (see
/// [`profile_record_hash_with_namespace`]); serde field names are the on-disk registry format.
/// `state_revision` defaults to 0 so pre-revision records still deserialize.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BrowserProfileRecord {
    pub(crate) profile_id: String,
    pub(crate) principal: String,
    pub(crate) name: String,
    pub(crate) theme_color: Option<String>,
    pub(crate) created_at_unix_ms: u64,
    pub(crate) updated_at_unix_ms: u64,
    pub(crate) last_used_unix_ms: u64,
    pub(crate) persistence_enabled: bool,
    pub(crate) private_profile: bool,
    pub(crate) state_schema_version: u32,
    #[serde(default)]
    pub(crate) state_revision: u64,
    pub(crate) state_hash_sha256: Option<String>,
    pub(crate) record_hash_sha256: String,
}

/// The persisted registry document: all profile records plus per-principal active selection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BrowserProfileRegistryDocument {
    pub(crate) v: u32,
    pub(crate) profiles: Vec<BrowserProfileRecord>,
    pub(crate) active_profile_by_principal: HashMap<String, String>,
}

impl Default for BrowserProfileRegistryDocument {
    fn default() -> Self {
        Self {
            v: PROFILE_REGISTRY_SCHEMA_VERSION,
            profiles: Vec::new(),
            active_profile_by_principal: HashMap::new(),
        }
    }
}

/// Sanitizes a freshly loaded registry: drops invalid or tampered records, migrates
/// legacy-hash records, dedupes by profile id, and prunes to size caps.
///
/// Records whose self-hash matches only the legacy namespace are re-hashed in the current one
/// (the v1 to v2 migration path); records matching neither are silently discarded.
pub(crate) fn normalize_profile_registry(registry: &mut BrowserProfileRegistryDocument) {
    registry.v = PROFILE_REGISTRY_SCHEMA_VERSION;
    let mut deduped = HashMap::new();
    for mut profile in registry.profiles.drain(..) {
        if validate_canonical_id(profile.profile_id.as_str()).is_err() {
            continue;
        }
        profile.principal = profile.principal.trim().to_owned();
        if profile.principal.is_empty() {
            continue;
        }
        if profile.name.trim().is_empty() {
            continue;
        }
        if !profile_record_hash_matches(&profile) {
            if profile_record_legacy_hash_matches(&profile) {
                refresh_profile_record_hash(&mut profile);
            } else {
                continue;
            }
        }
        if profile.state_schema_version < PROFILE_RECORD_SCHEMA_VERSION {
            profile.state_schema_version = PROFILE_RECORD_SCHEMA_VERSION;
            refresh_profile_record_hash(&mut profile);
        }
        deduped.insert(profile.profile_id.clone(), profile);
    }
    registry.profiles = deduped.into_values().collect();
    prune_profile_registry(registry);
}

/// Prunes the registry to its per-principal and total-size caps.
///
/// Eviction is least-recently-used by `last_used_unix_ms` and never removes a profile that is
/// any principal's active profile. Dangling active-profile references are dropped first.
pub(crate) fn prune_profile_registry(registry: &mut BrowserProfileRegistryDocument) {
    let principals =
        registry.profiles.iter().map(|profile| profile.principal.clone()).collect::<Vec<_>>();
    for principal in principals {
        prune_profiles_for_principal(registry, principal.as_str());
    }
    registry.active_profile_by_principal.retain(|principal, profile_id| {
        registry
            .profiles
            .iter()
            .any(|profile| profile.principal == *principal && profile.profile_id == *profile_id)
    });

    loop {
        // A serialization failure yields length 0 and just skips size-based pruning here;
        // save_profile_registry will surface the real error.
        let serialized_len = serde_json::to_vec(registry).map(|value| value.len()).unwrap_or(0);
        if serialized_len <= MAX_PROFILE_REGISTRY_BYTES {
            break;
        }
        let removable = registry
            .profiles
            .iter()
            .filter(|profile| !is_active_profile(registry, profile.profile_id.as_str()))
            .min_by(|left, right| left.last_used_unix_ms.cmp(&right.last_used_unix_ms))
            .map(|profile| profile.profile_id.clone());
        let Some(profile_id) = removable else {
            break;
        };
        registry.profiles.retain(|profile| profile.profile_id != profile_id);
    }
}

/// Evicts a principal's least-recently-used non-active profiles down to the per-principal cap.
pub(crate) fn prune_profiles_for_principal(
    registry: &mut BrowserProfileRegistryDocument,
    principal: &str,
) {
    loop {
        let principal_count =
            registry.profiles.iter().filter(|profile| profile.principal == principal).count();
        if principal_count <= MAX_PROFILES_PER_PRINCIPAL {
            break;
        }
        let active_profile_id =
            registry.active_profile_by_principal.get(principal).cloned().unwrap_or_default();
        let removable = registry
            .profiles
            .iter()
            .filter(|profile| {
                profile.principal == principal && profile.profile_id != active_profile_id
            })
            .min_by(|left, right| left.last_used_unix_ms.cmp(&right.last_used_unix_ms))
            .map(|profile| profile.profile_id.clone());
        let Some(profile_id) = removable else {
            break;
        };
        registry.profiles.retain(|profile| profile.profile_id != profile_id);
    }
}

/// Reports whether `profile_id` is the active profile of any principal.
pub(crate) fn is_active_profile(
    registry: &BrowserProfileRegistryDocument,
    profile_id: &str,
) -> bool {
    registry.active_profile_by_principal.values().any(|value| value == profile_id)
}

/// Computes the current-format (v2) self-hash of a profile record.
pub(crate) fn profile_record_hash(record: &BrowserProfileRecord) -> String {
    profile_record_hash_with_namespace(record, PROFILE_RECORD_HASH_NAMESPACE, true)
}

/// Computes the legacy (v1, revision-less) self-hash for records written by older builds.
pub(crate) fn profile_record_legacy_hash(record: &BrowserProfileRecord) -> String {
    profile_record_hash_with_namespace(record, PROFILE_RECORD_HASH_NAMESPACE_LEGACY, false)
}

/// Computes a record self-hash over all tamper-protected fields under `namespace`.
///
/// INTENTIONAL: the `update` call order below is the hash format. Reordering or adding fields
/// requires a new namespace plus a legacy fallback, like the v1 to v2 migration.
pub(crate) fn profile_record_hash_with_namespace(
    record: &BrowserProfileRecord,
    namespace: &[u8],
    include_revision: bool,
) -> String {
    let mut context = DigestContext::new(&SHA256);
    context.update(namespace);
    context.update(record.profile_id.as_bytes());
    context.update(record.principal.as_bytes());
    context.update(record.name.as_bytes());
    context.update(record.theme_color.as_deref().unwrap_or_default().as_bytes());
    context.update(record.created_at_unix_ms.to_string().as_bytes());
    context.update(record.updated_at_unix_ms.to_string().as_bytes());
    context.update(record.last_used_unix_ms.to_string().as_bytes());
    context.update(if record.persistence_enabled { b"1" } else { b"0" });
    context.update(if record.private_profile { b"1" } else { b"0" });
    context.update(record.state_schema_version.to_string().as_bytes());
    if include_revision {
        context.update(record.state_revision.to_string().as_bytes());
    }
    context.update(record.state_hash_sha256.as_deref().unwrap_or_default().as_bytes());
    encode_hex(context.finish().as_ref())
}

/// Recomputes and stores the record's self-hash; call after any field mutation.
pub(crate) fn refresh_profile_record_hash(record: &mut BrowserProfileRecord) {
    record.record_hash_sha256 = profile_record_hash(record);
}

/// Reports whether the record's stored self-hash matches the current-format hash.
pub(crate) fn profile_record_hash_matches(record: &BrowserProfileRecord) -> bool {
    record.record_hash_sha256 == profile_record_hash(record)
}

/// Reports whether the record's stored self-hash matches the legacy-format hash.
pub(crate) fn profile_record_legacy_hash_matches(record: &BrowserProfileRecord) -> bool {
    record.record_hash_sha256 == profile_record_legacy_hash(record)
}

/// Validates a profile principal (trimmed, non-empty, at most 128 bytes).
///
/// # Errors
/// Returns an error string when the principal is empty or too long.
pub(crate) fn normalize_profile_principal(raw: &str) -> Result<String, String> {
    let value = raw.trim();
    if value.is_empty() {
        return Err("principal is required".to_owned());
    }
    if value.len() > 128 {
        return Err("principal exceeds max bytes".to_owned());
    }
    Ok(value.to_owned())
}

/// Validates a profile display name (trimmed, non-empty, within the byte cap).
///
/// # Errors
/// Returns an error string when the name is empty or exceeds `MAX_PROFILE_NAME_BYTES`.
pub(crate) fn normalize_profile_name(raw: &str) -> Result<String, String> {
    let value = raw.trim();
    if value.is_empty() {
        return Err("profile name is required".to_owned());
    }
    if value.len() > MAX_PROFILE_NAME_BYTES {
        return Err(format!("profile name exceeds {MAX_PROFILE_NAME_BYTES} bytes"));
    }
    Ok(value.to_owned())
}

/// Validates an optional theme color string; empty input means "no theme".
///
/// # Errors
/// Returns an error string when the value exceeds `MAX_PROFILE_THEME_BYTES` or contains
/// characters outside `A-Z a-z 0-9 # - _`.
pub(crate) fn normalize_profile_theme(raw: &str) -> Result<Option<String>, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.len() > MAX_PROFILE_THEME_BYTES {
        return Err(format!("profile theme exceeds {MAX_PROFILE_THEME_BYTES} bytes"));
    }
    if !trimmed
        .bytes()
        .all(|value| value.is_ascii_alphanumeric() || matches!(value, b'#' | b'-' | b'_'))
    {
        return Err("profile theme contains unsupported characters".to_owned());
    }
    Ok(Some(trimmed.to_owned()))
}

/// Converts a profile record into its proto form, marking whether it is the active profile.
pub(crate) fn profile_record_to_proto(
    record: &BrowserProfileRecord,
    active: bool,
) -> browser_v1::BrowserProfile {
    browser_v1::BrowserProfile {
        v: CANONICAL_PROTOCOL_MAJOR,
        profile_id: Some(proto::palyra::common::v1::CanonicalId {
            ulid: record.profile_id.clone(),
        }),
        principal: record.principal.clone(),
        name: record.name.clone(),
        theme_color: record.theme_color.clone().unwrap_or_default(),
        created_at_unix_ms: record.created_at_unix_ms,
        updated_at_unix_ms: record.updated_at_unix_ms,
        last_used_unix_ms: record.last_used_unix_ms,
        persistence_enabled: record.persistence_enabled,
        private_profile: record.private_profile,
        active,
    }
}

/// Parses an optional profile id from its proto wrapper; missing or empty ids yield `None`.
///
/// # Errors
/// Returns an error string when a non-empty id is not a canonical id.
pub(crate) fn parse_optional_profile_id_from_proto(
    raw: Option<proto::palyra::common::v1::CanonicalId>,
) -> Result<Option<String>, String> {
    let Some(value) = raw else {
        return Ok(None);
    };
    let trimmed = value.ulid.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    validate_canonical_id(trimmed).map_err(|error| format!("invalid profile_id: {error}"))?;
    Ok(Some(trimmed.to_owned()))
}

/// Parses a mandatory profile id from its proto wrapper.
///
/// # Errors
/// Returns an error string when the id is missing, empty, or not a canonical id.
pub(crate) fn parse_required_profile_id_from_proto(
    raw: Option<proto::palyra::common::v1::CanonicalId>,
) -> Result<String, String> {
    let Some(value) = parse_optional_profile_id_from_proto(raw)? else {
        return Err("profile_id is required".to_owned());
    };
    Ok(value)
}

/// Resolves the profile a new session should use and touches its last-used timestamp.
///
/// Without a configured state store, explicit profile requests fail and profile-less sessions
/// resolve to `None`. With a store: an explicit `requested_profile_id` is used when it exists
/// and belongs to `principal` (otherwise `None`); without one, the principal's active profile
/// (if any) is used. The registry is saved back so the last-used update sticks.
///
/// # Errors
/// Returns an error string when profiles are requested without a state store, the principal is
/// invalid, or the registry cannot be loaded/saved.
pub(crate) async fn resolve_session_profile(
    runtime: &BrowserRuntimeState,
    principal: &str,
    requested_profile_id: Option<&str>,
) -> Result<Option<BrowserProfileRecord>, String> {
    let Some(store) = runtime.state_store.as_ref() else {
        if requested_profile_id.is_some() {
            return Err(
                "browser profiles require PALYRA_BROWSERD_STATE_ENCRYPTION_KEY to be configured"
                    .to_owned(),
            );
        }
        return Ok(None);
    };
    let principal = normalize_profile_principal(principal)?;
    let _guard = runtime.profile_registry_lock.lock().await;
    let mut registry = store.load_profile_registry().map_err(|error| error.to_string())?;
    let selected_profile_id = if let Some(profile_id) = requested_profile_id {
        profile_id.to_owned()
    } else if let Some(active) = registry.active_profile_by_principal.get(principal.as_str()) {
        active.clone()
    } else {
        return Ok(None);
    };
    let Some(profile) = registry.profiles.iter_mut().find(|profile| {
        profile.profile_id == selected_profile_id && profile.principal == principal
    }) else {
        return Ok(None);
    };
    profile.last_used_unix_ms = current_unix_ms();
    profile.updated_at_unix_ms = profile.last_used_unix_ms;
    refresh_profile_record_hash(profile);
    let resolved = profile.clone();
    prune_profile_registry(&mut registry);
    store.save_profile_registry(&registry).map_err(|error| error.to_string())?;
    Ok(Some(resolved))
}

/// Inserts or replaces a profile record, refreshing its timestamps and self-hash.
///
/// With `set_active_if_missing`, the record becomes the principal's active profile only when
/// the principal has none yet.
///
/// # Errors
/// Returns an error string when the registry cannot be loaded or saved.
pub(crate) async fn upsert_profile_record(
    store: &PersistedStateStore,
    registry_lock: &Mutex<()>,
    mut record: BrowserProfileRecord,
    set_active_if_missing: bool,
) -> Result<(), String> {
    let _guard = registry_lock.lock().await;
    let mut registry = store.load_profile_registry().map_err(|error| error.to_string())?;
    record.updated_at_unix_ms = current_unix_ms();
    record.last_used_unix_ms = record.updated_at_unix_ms;
    refresh_profile_record_hash(&mut record);
    if let Some(existing) =
        registry.profiles.iter_mut().find(|profile| profile.profile_id == record.profile_id)
    {
        *existing = record.clone();
    } else {
        registry.profiles.push(record.clone());
    }
    if set_active_if_missing {
        registry
            .active_profile_by_principal
            .entry(record.principal.clone())
            .or_insert_with(|| record.profile_id.clone());
    }
    prune_profile_registry(&mut registry);
    store.save_profile_registry(&registry).map_err(|error| error.to_string())
}

/// Records a freshly persisted snapshot's schema version, revision, and hash on its profile.
///
/// Silently no-ops for unknown profile ids (the profile may have been pruned meanwhile).
///
/// The caller must hold `BrowserRuntimeState::profile_registry_lock`; snapshot persistence keeps
/// that lock across revision lookup, snapshot write, and this metadata refresh so concurrent
/// profile mutations cannot be clobbered by a stale load-modify-save cycle.
///
/// # Errors
/// Fails when the registry cannot be loaded or saved.
pub(crate) fn update_profile_state_metadata_locked(
    store: &PersistedStateStore,
    profile_id: &str,
    state_schema_version: u32,
    state_revision: u64,
    state_hash_sha256: &str,
) -> Result<()> {
    let mut registry = store.load_profile_registry()?;
    if let Some(profile) =
        registry.profiles.iter_mut().find(|profile| profile.profile_id == profile_id)
    {
        profile.state_schema_version = state_schema_version;
        profile.state_revision = state_revision;
        profile.state_hash_sha256 = Some(state_hash_sha256.to_owned());
        profile.updated_at_unix_ms = current_unix_ms();
        refresh_profile_record_hash(profile);
        prune_profile_registry(&mut registry);
        store.save_profile_registry(&registry)?;
    }
    Ok(())
}

/// Returns the next snapshot revision for a profile: current + 1, never below 1.
///
/// Profile-less sessions always get revision 0.
///
/// # Errors
/// Fails when the registry cannot be loaded.
pub(crate) fn next_profile_state_revision(
    store: &PersistedStateStore,
    profile_id: Option<&str>,
) -> Result<u64> {
    let Some(profile_id) = profile_id else {
        return Ok(0);
    };
    let registry = store.load_profile_registry()?;
    let current = registry
        .profiles
        .iter()
        .find(|profile| profile.profile_id == profile_id)
        .map_or(0, |profile| profile.state_revision);
    Ok(current.saturating_add(1).max(1))
}
