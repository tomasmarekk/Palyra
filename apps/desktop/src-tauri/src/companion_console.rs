use anyhow::{anyhow, Result};
use palyra_control_plane::{self as control_plane};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::future::Future;
use std::sync::Mutex;

use super::snapshot::{
    build_control_plane_client, ensure_console_session_with_cache, sanitize_log_line,
};
use super::supervisor::{CachedConsolePayload, ConsolePayloadCache, ConsoleSessionCache};
use super::{unix_ms_now, RuntimeConfig};

const CHAT_SESSION_LIMIT: usize = 16;
const APPROVAL_LIMIT: usize = 24;
const INVENTORY_DEVICE_LIMIT: usize = 16;
const COMPANION_PAYLOAD_CACHE_TTL_MS: i64 = 15_000;
const COMPANION_PAYLOAD_STALE_WARNING_MS: i64 = 60_000;

#[derive(Debug, Deserialize, Serialize)]
struct ApprovalsEnvelope {
    #[serde(default)]
    approvals: Vec<Value>,
}

#[derive(Debug)]
pub(crate) struct FetchedCompanionConsoleData {
    pub(crate) console_session: control_plane::ConsoleSession,
    pub(crate) session_catalog: control_plane::SessionCatalogListEnvelope,
    pub(crate) approvals: Vec<Value>,
    pub(crate) inventory: control_plane::InventoryListEnvelope,
    pub(crate) warnings: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
enum CompanionPayloadKind {
    SessionCatalog,
    Approvals,
    Inventory,
}

impl CompanionPayloadKind {
    const fn label(self) -> &'static str {
        match self {
            Self::SessionCatalog => "desktop companion session catalog",
            Self::Approvals => "desktop companion approvals",
            Self::Inventory => "desktop companion inventory",
        }
    }
}

pub(crate) async fn fetch_companion_console_data(
    http_client: &Client,
    runtime: &RuntimeConfig,
    admin_token: &str,
    console_session_cache: &Mutex<Option<ConsoleSessionCache>>,
    console_payload_cache: &Mutex<ConsolePayloadCache>,
) -> Result<FetchedCompanionConsoleData> {
    let mut control_plane = build_control_plane_client(http_client.clone(), runtime)?;
    let console_session =
        ensure_console_session_with_cache(&mut control_plane, admin_token, console_session_cache)
            .await?;
    let mut warnings = Vec::new();

    let (session_catalog, session_catalog_warning) = get_companion_payload_with_cache(
        CompanionPayloadKind::SessionCatalog,
        console_payload_cache,
        || async {
            control_plane
                .list_session_catalog(vec![
                    ("limit", Some(CHAT_SESSION_LIMIT.to_string())),
                    ("sort", Some("updated_desc".to_owned())),
                    ("include_archived", Some("false".to_owned())),
                ])
                .await
                .map_err(anyhow::Error::from)
        },
    )
    .await?;
    if let Some(warning) = session_catalog_warning {
        warnings.push(warning);
    }

    let (approvals_envelope, approvals_warning) = get_companion_payload_with_cache(
        CompanionPayloadKind::Approvals,
        console_payload_cache,
        || async {
            control_plane
                .get_json_value(format!("console/v1/approvals?limit={APPROVAL_LIMIT}"))
                .await
                .map_err(anyhow::Error::from)
                .and_then(|raw| {
                    serde_json::from_value::<ApprovalsEnvelope>(raw).map_err(|error| {
                        anyhow!(
                            "desktop companion approvals response did not match the expected contract: {error}"
                        )
                    })
                })
        },
    )
    .await?;
    if let Some(warning) = approvals_warning {
        warnings.push(warning);
    }

    let (inventory, inventory_warning) = get_companion_payload_with_cache(
        CompanionPayloadKind::Inventory,
        console_payload_cache,
        || async { control_plane.list_inventory().await.map_err(anyhow::Error::from) },
    )
    .await?;
    if let Some(warning) = inventory_warning {
        warnings.push(warning);
    }

    Ok(FetchedCompanionConsoleData {
        console_session,
        session_catalog,
        approvals: approvals_envelope.approvals,
        inventory: trim_inventory_list(inventory),
        warnings,
    })
}

async fn get_companion_payload_with_cache<T, F, Fut>(
    kind: CompanionPayloadKind,
    console_payload_cache: &Mutex<ConsolePayloadCache>,
    fetch: F,
) -> Result<(T, Option<String>)>
where
    T: serde::de::DeserializeOwned + Serialize,
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    if let Some(cached) =
        load_cached_companion_payload(kind, console_payload_cache, COMPANION_PAYLOAD_CACHE_TTL_MS)
    {
        return Ok((cached, None));
    }

    match fetch().await {
        Ok(value) => {
            store_cached_companion_payload(kind, console_payload_cache, &value);
            Ok((value, None))
        }
        Err(error) => {
            if let Some(cached) = load_cached_companion_payload(
                kind,
                console_payload_cache,
                COMPANION_PAYLOAD_STALE_WARNING_MS,
            ) {
                return Ok((
                    cached,
                    Some(format!(
                        "using cached {} after refresh failure: {}",
                        kind.label(),
                        sanitize_log_line(error.to_string().as_str())
                    )),
                ));
            }
            Err(anyhow!(
                "failed to fetch {}: {}",
                kind.label(),
                sanitize_log_line(error.to_string().as_str())
            ))
        }
    }
}

fn load_cached_companion_payload<T>(
    kind: CompanionPayloadKind,
    console_payload_cache: &Mutex<ConsolePayloadCache>,
    max_age_ms: i64,
) -> Option<T>
where
    T: serde::de::DeserializeOwned,
{
    let now = unix_ms_now();
    let Ok(cache) = console_payload_cache.lock() else {
        return None;
    };
    let cached = cached_companion_payload_for_kind(&cache, kind)?;
    let fetched_at_unix_ms = cached.fetched_at_unix_ms?;
    if now.saturating_sub(fetched_at_unix_ms) > max_age_ms {
        return None;
    }
    cached
        .payload
        .clone()
        .and_then(|payload| serde_json::from_value::<T>(payload).ok())
}

fn store_cached_companion_payload<T>(
    kind: CompanionPayloadKind,
    console_payload_cache: &Mutex<ConsolePayloadCache>,
    value: &T,
) where
    T: Serialize,
{
    let Ok(payload) = serde_json::to_value(value) else {
        return;
    };
    let Ok(mut cache) = console_payload_cache.lock() else {
        return;
    };
    let cached = cached_companion_payload_for_kind_mut(&mut cache, kind);
    cached.payload = Some(payload);
    cached.fetched_at_unix_ms = Some(unix_ms_now());
}

fn cached_companion_payload_for_kind(
    cache: &ConsolePayloadCache,
    kind: CompanionPayloadKind,
) -> Option<&CachedConsolePayload> {
    match kind {
        CompanionPayloadKind::SessionCatalog => Some(&cache.companion.session_catalog),
        CompanionPayloadKind::Approvals => Some(&cache.companion.approvals),
        CompanionPayloadKind::Inventory => Some(&cache.companion.inventory),
    }
}

fn cached_companion_payload_for_kind_mut(
    cache: &mut ConsolePayloadCache,
    kind: CompanionPayloadKind,
) -> &mut CachedConsolePayload {
    match kind {
        CompanionPayloadKind::SessionCatalog => &mut cache.companion.session_catalog,
        CompanionPayloadKind::Approvals => &mut cache.companion.approvals,
        CompanionPayloadKind::Inventory => &mut cache.companion.inventory,
    }
}

fn trim_inventory_list(
    mut inventory: control_plane::InventoryListEnvelope,
) -> control_plane::InventoryListEnvelope {
    if inventory.devices.len() > INVENTORY_DEVICE_LIMIT {
        inventory.devices.truncate(INVENTORY_DEVICE_LIMIT);
    }
    inventory
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use serde_json::json;

    use super::{get_companion_payload_with_cache, unix_ms_now, CompanionPayloadKind};
    use crate::console_cache::DesktopCompanionPayloadCache;
    use crate::supervisor::{CachedConsolePayload, ConsolePayloadCache};

    #[tokio::test(flavor = "current_thread")]
    async fn companion_payload_cache_uses_stale_value_after_rate_limit_failure() {
        let now = unix_ms_now();
        let cache = Mutex::new(ConsolePayloadCache {
            companion: DesktopCompanionPayloadCache {
                session_catalog: CachedConsolePayload {
                    payload: Some(json!({
                        "summary": {
                            "total_sessions": 1
                        }
                    })),
                    fetched_at_unix_ms: Some(now.saturating_sub(20_000)),
                },
                ..DesktopCompanionPayloadCache::default()
            },
            ..ConsolePayloadCache::default()
        });

        let (payload, warning) = get_companion_payload_with_cache::<serde_json::Value, _, _>(
            CompanionPayloadKind::SessionCatalog,
            &cache,
            || async {
                Err(anyhow::anyhow!(
                    "request failed with HTTP 429: admin API rate limit exceeded for 127.0.0.1"
                ))
            },
        )
        .await
        .expect("stale companion cache should satisfy the request");

        assert_eq!(
            payload
                .pointer("/summary/total_sessions")
                .and_then(serde_json::Value::as_i64),
            Some(1)
        );
        assert!(
            warning.as_deref().is_some_and(|value| {
                value.contains("using cached desktop companion session catalog")
            }),
            "stale cache warning should mention the reused payload: {warning:?}"
        );
    }
}
