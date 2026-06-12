//! SSRF-hardened target validation and guarded navigation for browserd.
//!
//! Every outbound navigation target is resolved and checked against the
//! private/local address policy here before any connection is made. Validated
//! DNS answers are pinned into the HTTP client (no re-resolution at connect
//! time) so a host cannot rebind to a private address between validation and
//! connect, and redirects are re-validated hop by hop.

use crate::*;

/// DNS resolution result pre-classified against the deny-private default policy.
#[derive(Debug, Clone)]
pub(crate) struct ResolvedHostAddresses {
    pub(crate) addresses: Vec<IpAddr>,
    pub(crate) blocked_for_default_policy: bool,
}

impl ResolvedHostAddresses {
    /// Classifies resolved addresses against the deny-private default policy.
    ///
    /// A single private/local address in the answer marks the whole result as
    /// blocked: mixed public+private answers are a common DNS rebinding shape,
    /// so partial matches must not slip through.
    ///
    /// # Errors
    /// Returns an error when the address list is empty.
    pub(crate) fn from_addresses(addresses: Vec<IpAddr>) -> Result<Self, String> {
        if addresses.is_empty() {
            return Err("DNS resolution returned no addresses".to_owned());
        }
        let blocked_for_default_policy =
            addresses.iter().copied().any(netguard::is_private_or_local_ip);
        Ok(Self { addresses, blocked_for_default_policy })
    }
}

/// Negative-cache entry: expiry deadline plus last-touch tick for LRU pruning.
#[derive(Debug, Clone)]
pub(crate) struct DnsValidationCacheEntry {
    expires_at: Instant,
    last_access_tick: u64,
}

/// Bounded negative (NXDOMAIN-only) DNS cache.
///
/// Only failed resolutions are cached, to absorb repeated lookups of
/// nonexistent hosts; successful answers are never cached, so the private
/// address policy always evaluates a fresh resolution. Eviction is LRU via a
/// monotonically increasing access tick.
#[derive(Debug)]
pub(crate) struct DnsValidationCache {
    entries: HashMap<String, DnsValidationCacheEntry>,
    max_entries: usize,
    negative_ttl: Duration,
    next_access_tick: u64,
}

impl DnsValidationCache {
    /// Creates a cache enforcing a floor of one entry and a one-second TTL.
    pub(crate) fn new(max_entries: usize, negative_ttl: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            max_entries: max_entries.max(1),
            negative_ttl: negative_ttl.max(Duration::from_secs(1)),
            next_access_tick: 0,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether `key` has an unexpired entry, refreshing its LRU tick.
    ///
    /// Deliberately mutating: an expired entry found here is removed on the
    /// way out so dead keys do not occupy capacity until the next insert.
    pub(crate) fn contains(&mut self, key: &str, now: Instant) -> bool {
        let mut should_remove = false;
        let mut found = false;
        let access_tick = self.next_access_tick();
        if let Some(entry) = self.entries.get_mut(key) {
            if now > entry.expires_at {
                should_remove = true;
            } else {
                entry.last_access_tick = access_tick;
                found = true;
            }
        }
        if should_remove {
            self.entries.remove(key);
        }
        found
    }

    /// Records a fresh NXDOMAIN observation, evicting expired then LRU entries.
    pub(crate) fn insert_nxdomain(&mut self, key: String, now: Instant) {
        self.remove_expired(now);
        let last_access_tick = self.next_access_tick();
        self.entries.insert(
            key,
            DnsValidationCacheEntry { expires_at: now + self.negative_ttl, last_access_tick },
        );
        self.prune_lru();
    }

    fn next_access_tick(&mut self) -> u64 {
        self.next_access_tick = self.next_access_tick.saturating_add(1);
        self.next_access_tick
    }

    fn remove_expired(&mut self, now: Instant) {
        self.entries.retain(|_, entry| now <= entry.expires_at);
    }

    fn prune_lru(&mut self) {
        while self.entries.len() > self.max_entries {
            let Some((candidate, _)) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_access_tick)
                .map(|(key, entry)| (key.clone(), entry.last_access_tick))
            else {
                break;
            };
            self.entries.remove(candidate.as_str());
        }
    }
}

/// Process-wide counters for DNS validation outcomes, logged periodically.
#[derive(Debug, Default)]
pub(crate) struct DnsValidationMetrics {
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    dns_lookups: AtomicU64,
    dns_lookup_latency_ms_total: AtomicU64,
    blocked_total: AtomicU64,
    blocked_private_targets: AtomicU64,
    blocked_dns_failures: AtomicU64,
    observations: AtomicU64,
}

/// Point-in-time copy of the DNS validation counters plus current cache size.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct DnsValidationMetricsSnapshot {
    cache_hits: u64,
    cache_misses: u64,
    dns_lookups: u64,
    dns_lookup_latency_ms_total: u64,
    blocked_total: u64,
    blocked_private_targets: u64,
    blocked_dns_failures: u64,
    cache_entries: usize,
}

impl DnsValidationMetricsSnapshot {
    fn cache_hit_ratio(self) -> f64 {
        let denominator = self.cache_hits.saturating_add(self.cache_misses);
        if denominator == 0 {
            return 0.0;
        }
        self.cache_hits as f64 / denominator as f64
    }

    fn lookup_avg_latency_ms(self) -> f64 {
        if self.dns_lookups == 0 {
            return 0.0;
        }
        self.dns_lookup_latency_ms_total as f64 / self.dns_lookups as f64
    }
}

impl DnsValidationMetrics {
    fn snapshot(&self, cache_entries: usize) -> DnsValidationMetricsSnapshot {
        DnsValidationMetricsSnapshot {
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            dns_lookups: self.dns_lookups.load(Ordering::Relaxed),
            dns_lookup_latency_ms_total: self.dns_lookup_latency_ms_total.load(Ordering::Relaxed),
            blocked_total: self.blocked_total.load(Ordering::Relaxed),
            blocked_private_targets: self.blocked_private_targets.load(Ordering::Relaxed),
            blocked_dns_failures: self.blocked_dns_failures.load(Ordering::Relaxed),
            cache_entries,
        }
    }

    #[cfg(test)]
    fn reset_for_tests(&self) {
        self.cache_hits.store(0, Ordering::Relaxed);
        self.cache_misses.store(0, Ordering::Relaxed);
        self.dns_lookups.store(0, Ordering::Relaxed);
        self.dns_lookup_latency_ms_total.store(0, Ordering::Relaxed);
        self.blocked_total.store(0, Ordering::Relaxed);
        self.blocked_private_targets.store(0, Ordering::Relaxed);
        self.blocked_dns_failures.store(0, Ordering::Relaxed);
        self.observations.store(0, Ordering::Relaxed);
    }
}

static DNS_VALIDATION_CACHE: LazyLock<std::sync::Mutex<DnsValidationCache>> = LazyLock::new(|| {
    std::sync::Mutex::new(DnsValidationCache::new(
        DNS_VALIDATION_CACHE_MAX_ENTRIES,
        DNS_VALIDATION_NEGATIVE_TTL,
    ))
});

static DNS_VALIDATION_METRICS: LazyLock<DnsValidationMetrics> =
    LazyLock::new(DnsValidationMetrics::default);

/// A target that passed policy checks, carrying the exact addresses to pin.
///
/// `host` is `None` when the URL host was an IP literal (nothing to pin).
/// Otherwise `resolved_socket_addrs` must be installed into the HTTP client so
/// the connection reuses the validated answer instead of re-resolving — the
/// DNS rebinding defense (see `build_pinned_http_client`).
#[derive(Debug, Clone)]
pub(crate) struct ValidatedTargetUrl {
    pub(crate) host: Option<String>,
    pub(crate) resolved_socket_addrs: Vec<SocketAddr>,
}

/// Validates a raw URL string against target policy using blocking DNS resolution.
///
/// `about:blank` is always allowed. Intended for non-async callers; async
/// paths use `validate_target_url`, which additionally returns pinning data.
///
/// # Errors
/// Returns a human-readable reason when the URL is malformed or blocked.
pub(crate) fn validate_target_url_blocking(
    raw_url: &str,
    allow_private_targets: bool,
) -> Result<(), String> {
    if raw_url.eq_ignore_ascii_case("about:blank") {
        return Ok(());
    }
    let url = Url::parse(raw_url).map_err(|error| format!("invalid URL: {error}"))?;
    validate_target_url_parts_blocking(&url, allow_private_targets)
}

/// Validates an already-parsed URL against target policy (blocking variant).
///
/// `file` URLs go through the local-file gate; everything else must pass
/// scheme/credential checks, DNS resolution, and the private-address policy.
///
/// # Errors
/// Returns the policy or resolution failure reason.
pub(crate) fn validate_target_url_parts_blocking(
    url: &Url,
    allow_private_targets: bool,
) -> Result<(), String> {
    if url.scheme() == "file" {
        validate_local_file_url_target(url, allow_private_targets)?;
        return Ok(());
    }
    // Immediately-invoked closure keeps `?` local so the metrics observation
    // below runs on the failure path too.
    let result = (|| {
        let (host, port) = extract_target_host_port(url)?;
        let resolved = resolve_host_addresses_blocking(host, port)?;
        enforce_resolved_host_policy(host, resolved, allow_private_targets)
    })();
    maybe_log_dns_validation_metrics();
    result
}

/// Locks the global negative DNS cache, recovering from lock poisoning.
///
/// The cache is purely advisory (worst case: one extra DNS lookup), so a panic
/// in another thread must not take navigation down with it.
pub(crate) fn lock_dns_validation_cache() -> std::sync::MutexGuard<'static, DnsValidationCache> {
    DNS_VALIDATION_CACHE.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Canonicalizes a host for cache keying: trims whitespace and the FQDN
/// trailing dot, then lowercases.
pub(crate) fn normalize_dns_host_cache_key(host: &str) -> String {
    host.trim().trim_end_matches('.').to_ascii_lowercase()
}

/// Heuristically detects name-not-found resolver failures.
///
/// `std::io::Error` from the system resolver carries no structured NXDOMAIN
/// code, so this matches the known message shapes for Windows, glibc, and
/// macOS/BSD in addition to `ErrorKind::NotFound`. Only these failures are
/// negative-cached; transient resolver errors must stay immediately retryable.
pub(crate) fn is_nxdomain_lookup_error(error: &std::io::Error) -> bool {
    if error.kind() == std::io::ErrorKind::NotFound {
        return true;
    }
    let message = error.to_string().to_ascii_lowercase();
    message.contains("no such host")
        || message.contains("host not found")
        || message.contains("name or service not known")
        || message.contains("nodename nor servname provided")
}

/// Formats a live DNS failure for `host`; keep wording aligned with
/// `dns_cached_nxdomain_error_for_host` so callers see one message family.
pub(crate) fn dns_resolution_error_for_host(host: &str, error: &std::io::Error) -> String {
    format!("DNS resolution failed for host '{host}': {error}")
}

/// Formats the failure reported when a cached NXDOMAIN entry short-circuits lookup.
pub(crate) fn dns_cached_nxdomain_error_for_host(host: &str) -> String {
    format!("DNS resolution failed for host '{host}': cached NXDOMAIN")
}

/// Returns whether `host` has a live cached NXDOMAIN entry, counting hit/miss.
pub(crate) fn lookup_cached_nxdomain(host: &str) -> bool {
    let key = normalize_dns_host_cache_key(host);
    let now = Instant::now();
    let mut cache = lock_dns_validation_cache();
    let cached = cache.contains(key.as_str(), now);
    if cached {
        DNS_VALIDATION_METRICS.cache_hits.fetch_add(1, Ordering::Relaxed);
    } else {
        DNS_VALIDATION_METRICS.cache_misses.fetch_add(1, Ordering::Relaxed);
    }
    cached
}

/// Caches an NXDOMAIN observation for `host` in the global negative cache.
pub(crate) fn store_dns_nxdomain_cache(host: &str) {
    let key = normalize_dns_host_cache_key(host);
    let now = Instant::now();
    let mut cache = lock_dns_validation_cache();
    cache.insert_nxdomain(key, now);
}

/// Extracts host and effective port after scheme and credential checks.
///
/// Only `http`/`https` may reach DNS validation (`file` is gated separately,
/// every other scheme is blocked), and URLs with embedded credentials are
/// rejected outright so secrets cannot ride along in navigation targets.
///
/// # Errors
/// Returns the policy reason when the scheme is blocked, credentials are
/// present, or the host/port cannot be determined.
pub(crate) fn extract_target_host_port(url: &Url) -> Result<(&str, u16), String> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(format!("blocked URL scheme '{}'", url.scheme()));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err("URL credentials are not allowed".to_owned());
    }
    let host = url.host_str().ok_or_else(|| "URL host is required".to_owned())?;
    let port =
        url.port_or_known_default().ok_or_else(|| "URL port could not be resolved".to_owned())?;
    Ok((host, port))
}

/// Records one DNS lookup and its elapsed milliseconds in the metrics counters.
pub(crate) fn track_dns_lookup_latency(lookup_started: Instant) {
    let lookup_latency_ms = lookup_started.elapsed().as_millis() as u64;
    DNS_VALIDATION_METRICS.dns_lookups.fetch_add(1, Ordering::Relaxed);
    DNS_VALIDATION_METRICS
        .dns_lookup_latency_ms_total
        .fetch_add(lookup_latency_ms, Ordering::Relaxed);
}

/// Resolves `host` to IP addresses with negative caching and metrics applied
/// (blocking variant; keep in sync with `resolve_host_addresses_async`).
///
/// IP literals bypass DNS entirely. NXDOMAIN-shaped failures are negative
/// cached; every failure increments the blocked counters.
///
/// # Errors
/// Returns the resolution failure reason; never yields an empty address list.
pub(crate) fn resolve_host_addresses_blocking(
    host: &str,
    port: u16,
) -> Result<ResolvedHostAddresses, String> {
    if let Some(address) = netguard::parse_host_ip_literal(host)? {
        return ResolvedHostAddresses::from_addresses(vec![address]);
    }

    if lookup_cached_nxdomain(host) {
        DNS_VALIDATION_METRICS.blocked_total.fetch_add(1, Ordering::Relaxed);
        DNS_VALIDATION_METRICS.blocked_dns_failures.fetch_add(1, Ordering::Relaxed);
        return Err(dns_cached_nxdomain_error_for_host(host));
    }

    let lookup_started = Instant::now();
    let addresses = (host, port)
        .to_socket_addrs()
        .map_err(|error| {
            track_dns_lookup_latency(lookup_started);
            if is_nxdomain_lookup_error(&error) {
                store_dns_nxdomain_cache(host);
            }
            DNS_VALIDATION_METRICS.blocked_total.fetch_add(1, Ordering::Relaxed);
            DNS_VALIDATION_METRICS.blocked_dns_failures.fetch_add(1, Ordering::Relaxed);
            dns_resolution_error_for_host(host, &error)
        })?
        .map(|socket| socket.ip())
        .collect::<Vec<_>>();
    track_dns_lookup_latency(lookup_started);
    let resolved = ResolvedHostAddresses::from_addresses(addresses).map_err(|error| {
        DNS_VALIDATION_METRICS.blocked_total.fetch_add(1, Ordering::Relaxed);
        DNS_VALIDATION_METRICS.blocked_dns_failures.fetch_add(1, Ordering::Relaxed);
        format!("{error} for host '{host}'")
    })?;
    Ok(resolved)
}

/// Async twin of `resolve_host_addresses_blocking`; behavior must stay identical.
///
/// # Errors
/// Returns the resolution failure reason; never yields an empty address list.
pub(crate) async fn resolve_host_addresses_async(
    host: &str,
    port: u16,
) -> Result<ResolvedHostAddresses, String> {
    if let Some(address) = netguard::parse_host_ip_literal(host)? {
        return ResolvedHostAddresses::from_addresses(vec![address]);
    }

    if lookup_cached_nxdomain(host) {
        DNS_VALIDATION_METRICS.blocked_total.fetch_add(1, Ordering::Relaxed);
        DNS_VALIDATION_METRICS.blocked_dns_failures.fetch_add(1, Ordering::Relaxed);
        return Err(dns_cached_nxdomain_error_for_host(host));
    }

    let lookup_started = Instant::now();
    let addresses = tokio::net::lookup_host((host, port))
        .await
        .map_err(|error| {
            track_dns_lookup_latency(lookup_started);
            if is_nxdomain_lookup_error(&error) {
                store_dns_nxdomain_cache(host);
            }
            DNS_VALIDATION_METRICS.blocked_total.fetch_add(1, Ordering::Relaxed);
            DNS_VALIDATION_METRICS.blocked_dns_failures.fetch_add(1, Ordering::Relaxed);
            dns_resolution_error_for_host(host, &error)
        })?
        .map(|socket| socket.ip())
        .collect::<Vec<_>>();
    track_dns_lookup_latency(lookup_started);
    let resolved = ResolvedHostAddresses::from_addresses(addresses).map_err(|error| {
        DNS_VALIDATION_METRICS.blocked_total.fetch_add(1, Ordering::Relaxed);
        DNS_VALIDATION_METRICS.blocked_dns_failures.fetch_add(1, Ordering::Relaxed);
        format!("{error} for host '{host}'")
    })?;
    Ok(resolved)
}

/// Applies the deny-private default policy to a resolution result.
///
/// # Errors
/// Returns a blocked-by-policy message when private targets are not allowed
/// and the answer contained a private/local address.
pub(crate) fn enforce_resolved_host_policy(
    host: &str,
    resolved: ResolvedHostAddresses,
    allow_private_targets: bool,
) -> Result<(), String> {
    if !allow_private_targets && resolved.blocked_for_default_policy {
        // Cap the address preview so a hostile resolver returning hundreds of
        // records cannot inflate error messages and logs.
        let preview = resolved
            .addresses
            .iter()
            .take(4)
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        DNS_VALIDATION_METRICS.blocked_total.fetch_add(1, Ordering::Relaxed);
        DNS_VALIDATION_METRICS.blocked_private_targets.fetch_add(1, Ordering::Relaxed);
        return Err(format!(
            "target resolves to private/local address and is blocked by policy (host '{host}', addresses [{preview}])"
        ));
    }
    Ok(())
}

/// Captures the current DNS validation counters together with the cache size.
pub(crate) fn dns_validation_metrics_snapshot() -> DnsValidationMetricsSnapshot {
    let cache_entries = lock_dns_validation_cache().len();
    DNS_VALIDATION_METRICS.snapshot(cache_entries)
}

/// Emits a metrics snapshot every `DNS_VALIDATION_METRICS_LOG_INTERVAL`
/// validations, keeping steady-state log volume bounded.
pub(crate) fn maybe_log_dns_validation_metrics() {
    let observations = DNS_VALIDATION_METRICS.observations.fetch_add(1, Ordering::Relaxed) + 1;
    if !observations.is_multiple_of(DNS_VALIDATION_METRICS_LOG_INTERVAL) {
        return;
    }
    let snapshot = dns_validation_metrics_snapshot();
    info!(
        dns_cache_entries = snapshot.cache_entries,
        dns_cache_hits = snapshot.cache_hits,
        dns_cache_misses = snapshot.cache_misses,
        dns_cache_hit_ratio = snapshot.cache_hit_ratio(),
        dns_lookup_count = snapshot.dns_lookups,
        dns_lookup_avg_latency_ms = snapshot.lookup_avg_latency_ms(),
        dns_blocked_total = snapshot.blocked_total,
        dns_blocked_private_targets = snapshot.blocked_private_targets,
        dns_blocked_dns_failures = snapshot.blocked_dns_failures,
        "browserd DNS validation metrics snapshot"
    );
}

/// Clears the global cache and counters so tests stay order-independent.
#[cfg(test)]
pub(crate) fn reset_dns_validation_tracking_for_tests() {
    let mut cache = lock_dns_validation_cache();
    cache.entries.clear();
    cache.next_access_tick = 0;
    drop(cache);
    DNS_VALIDATION_METRICS.reset_for_tests();
}

/// Performs a guarded HTTP(S) or local-file navigation with manual redirect handling.
///
/// Each redirect hop is independently policy-validated and fetched with a
/// client pinned to that hop's resolved addresses; the body is streamed and
/// truncated at `max_response_bytes`. Failures are reported inside the
/// returned `NavigateOutcome` (never as `Err`) so callers always receive the
/// network log and cookie updates accumulated up to the failure.
pub(crate) async fn navigate_with_guards(
    raw_url: &str,
    timeout_ms: u64,
    allow_redirects: bool,
    max_redirects: u32,
    allow_private_targets: bool,
    max_response_bytes: u64,
    cookie_header: Option<&str>,
) -> NavigateOutcome {
    let started_at = Instant::now();
    let mut network_log = Vec::new();
    let mut cookie_updates = Vec::new();
    let mut current_url = match Url::parse(raw_url) {
        Ok(value) => value,
        Err(error) => {
            return NavigateOutcome {
                success: false,
                final_url: String::new(),
                status_code: 0,
                title: String::new(),
                page_body: String::new(),
                body_bytes: 0,
                latency_ms: started_at.elapsed().as_millis() as u64,
                error: format!("invalid URL: {error}"),
                network_log,
                cookie_updates,
            }
        }
    };
    // Hard ceiling of 10 hops no matter what the caller requests.
    let redirect_limit = max_redirects.clamp(1, 10);
    let mut redirects = 0_u32;
    let initial_scheme = current_url.scheme().to_owned();
    let initial_cookie_host = current_url.host_str().map(str::to_ascii_lowercase);
    loop {
        if current_url.scheme() == "file" {
            return navigate_local_file_with_guards(
                &current_url,
                allow_private_targets,
                max_response_bytes,
                started_at,
                network_log,
                cookie_updates,
            );
        }
        let validated_target = match validate_target_url(&current_url, allow_private_targets).await
        {
            Ok(value) => value,
            Err(error) => {
                return NavigateOutcome {
                    success: false,
                    final_url: current_url.to_string(),
                    status_code: 0,
                    title: String::new(),
                    page_body: String::new(),
                    body_bytes: 0,
                    latency_ms: started_at.elapsed().as_millis() as u64,
                    error,
                    network_log,
                    cookie_updates,
                };
            }
        };
        let client = match build_pinned_http_client(timeout_ms, &validated_target) {
            Ok(value) => value,
            Err(error) => {
                return NavigateOutcome {
                    success: false,
                    final_url: current_url.to_string(),
                    status_code: 0,
                    title: String::new(),
                    page_body: String::new(),
                    body_bytes: 0,
                    latency_ms: started_at.elapsed().as_millis() as u64,
                    error: format!("failed to build HTTP client: {error}"),
                    network_log,
                    cookie_updates,
                };
            }
        };

        let request_started = Instant::now();
        let mut request_builder = client.get(current_url.clone());
        if let Some(value) = cookie_header_for_redirect_hop(
            &current_url,
            initial_cookie_host.as_deref(),
            cookie_header,
        ) {
            request_builder = request_builder.header(COOKIE_HEADER, value);
        }
        let mut response = match request_builder.send().await {
            Ok(value) => value,
            Err(error) => {
                return NavigateOutcome {
                    success: false,
                    final_url: current_url.to_string(),
                    status_code: 0,
                    title: String::new(),
                    page_body: String::new(),
                    body_bytes: 0,
                    latency_ms: started_at.elapsed().as_millis() as u64,
                    error: format!("request failed: {error}"),
                    network_log,
                    cookie_updates,
                }
            }
        };
        if let Err(error) =
            enforce_remote_response_ip_policy(response.remote_addr(), allow_private_targets)
        {
            return NavigateOutcome {
                success: false,
                final_url: current_url.to_string(),
                status_code: 0,
                title: String::new(),
                page_body: String::new(),
                body_bytes: 0,
                latency_ms: started_at.elapsed().as_millis() as u64,
                error,
                network_log,
                cookie_updates,
            };
        }
        if let Some(domain) = current_url.host_str() {
            for raw_set_cookie in response.headers().get_all(SET_COOKIE_HEADER).iter() {
                if let Ok(value) = raw_set_cookie.to_str() {
                    if let Some(update) = parse_set_cookie_update(domain, value) {
                        cookie_updates.push(update);
                    }
                }
            }
        }
        let request_latency_ms = request_started.elapsed().as_millis() as u64;
        network_log.push(NetworkLogEntryInternal {
            request_url: normalize_url_with_redaction(current_url.as_str()),
            status_code: response.status().as_u16(),
            timing_bucket: timing_bucket_for_latency(request_latency_ms).to_owned(),
            latency_ms: request_latency_ms,
            captured_at_unix_ms: current_unix_ms(),
            headers: sanitize_network_headers(response.headers()),
        });

        if response.status().is_redirection() {
            if !allow_redirects {
                return NavigateOutcome {
                    success: false,
                    final_url: current_url.to_string(),
                    status_code: response.status().as_u16(),
                    title: String::new(),
                    page_body: String::new(),
                    body_bytes: 0,
                    latency_ms: started_at.elapsed().as_millis() as u64,
                    error: "redirect response blocked by policy".to_owned(),
                    network_log,
                    cookie_updates,
                };
            }
            if redirects >= redirect_limit {
                return NavigateOutcome {
                    success: false,
                    final_url: current_url.to_string(),
                    status_code: response.status().as_u16(),
                    title: String::new(),
                    page_body: String::new(),
                    body_bytes: 0,
                    latency_ms: started_at.elapsed().as_millis() as u64,
                    error: format!("redirect limit exceeded ({redirect_limit})"),
                    network_log,
                    cookie_updates,
                };
            }
            let Some(location) = response.headers().get(reqwest::header::LOCATION) else {
                return NavigateOutcome {
                    success: false,
                    final_url: current_url.to_string(),
                    status_code: response.status().as_u16(),
                    title: String::new(),
                    page_body: String::new(),
                    body_bytes: 0,
                    latency_ms: started_at.elapsed().as_millis() as u64,
                    error: "redirect missing Location header".to_owned(),
                    network_log,
                    cookie_updates,
                };
            };
            let Ok(location_str) = location.to_str() else {
                return NavigateOutcome {
                    success: false,
                    final_url: current_url.to_string(),
                    status_code: response.status().as_u16(),
                    title: String::new(),
                    page_body: String::new(),
                    body_bytes: 0,
                    latency_ms: started_at.elapsed().as_millis() as u64,
                    error: "redirect location header contains invalid UTF-8".to_owned(),
                    network_log,
                    cookie_updates,
                };
            };
            current_url = match current_url.join(location_str) {
                Ok(value) => value,
                Err(error) => {
                    return NavigateOutcome {
                        success: false,
                        final_url: current_url.to_string(),
                        status_code: response.status().as_u16(),
                        title: String::new(),
                        page_body: String::new(),
                        body_bytes: 0,
                        latency_ms: started_at.elapsed().as_millis() as u64,
                        error: format!("invalid redirect target: {error}"),
                        network_log,
                        cookie_updates,
                    }
                }
            };
            // A remote response must never steer navigation onto the local
            // filesystem, even when the session may read files directly.
            if initial_scheme != "file" && current_url.scheme() == "file" {
                return NavigateOutcome {
                    success: false,
                    final_url: current_url.to_string(),
                    status_code: response.status().as_u16(),
                    title: String::new(),
                    page_body: String::new(),
                    body_bytes: 0,
                    latency_ms: started_at.elapsed().as_millis() as u64,
                    error: "redirect to file:// URL is blocked by policy".to_owned(),
                    network_log,
                    cookie_updates,
                };
            }
            redirects = redirects.saturating_add(1);
            continue;
        }

        let status_code = response.status().as_u16();
        let mut body = Vec::new();
        loop {
            let next_chunk = match response.chunk().await {
                Ok(value) => value,
                Err(error) => {
                    return NavigateOutcome {
                        success: false,
                        final_url: current_url.to_string(),
                        status_code,
                        title: String::new(),
                        page_body: String::new(),
                        body_bytes: body.len() as u64,
                        latency_ms: started_at.elapsed().as_millis() as u64,
                        error: format!("failed to read response body: {error}"),
                        network_log,
                        cookie_updates,
                    }
                }
            };
            let Some(chunk) = next_chunk else {
                break;
            };
            let projected_len = (body.len() as u64).saturating_add(chunk.len() as u64);
            if projected_len > max_response_bytes {
                let remaining = max_response_bytes.saturating_sub(body.len() as u64) as usize;
                if remaining > 0 {
                    body.extend_from_slice(&chunk[..remaining.min(chunk.len())]);
                }
                let page_body = String::from_utf8_lossy(body.as_slice()).to_string();
                return NavigateOutcome {
                    success: (200..400).contains(&status_code),
                    final_url: current_url.to_string(),
                    status_code,
                    title: extract_html_title(page_body.as_str()).unwrap_or_default().to_owned(),
                    page_body,
                    body_bytes: projected_len,
                    latency_ms: started_at.elapsed().as_millis() as u64,
                    error: if status_code >= 400 {
                        format!("navigation returned HTTP {status_code}")
                    } else {
                        format!(
                            "response exceeds max_response_bytes ({projected_len} > {max_response_bytes}); page_body truncated"
                        )
                    },
                    network_log,
                    cookie_updates,
                };
            }
            body.extend_from_slice(chunk.as_ref());
        }

        let body_len = body.len() as u64;
        let page_body = String::from_utf8_lossy(body.as_slice()).to_string();

        return NavigateOutcome {
            success: (200..400).contains(&status_code),
            final_url: current_url.to_string(),
            status_code,
            title: extract_html_title(page_body.as_str()).unwrap_or_default().to_owned(),
            page_body,
            body_bytes: body_len,
            latency_ms: started_at.elapsed().as_millis() as u64,
            error: if status_code >= 400 {
                format!("navigation returned HTTP {status_code}")
            } else {
                String::new()
            },
            network_log,
            cookie_updates,
        };
    }
}

fn cookie_header_for_redirect_hop<'a>(
    current_url: &Url,
    initial_cookie_host: Option<&str>,
    cookie_header: Option<&'a str>,
) -> Option<&'a str> {
    let value = cookie_header.filter(|value| !value.trim().is_empty())?;
    let current_host = current_url.host_str()?;
    initial_cookie_host
        .filter(|initial_host| current_host.eq_ignore_ascii_case(initial_host))
        .map(|_| value)
}

/// Serves a `file://` navigation after the local-file gate passes, enforcing
/// the response byte cap against the file size before reading it.
fn navigate_local_file_with_guards(
    url: &Url,
    allow_private_targets: bool,
    max_response_bytes: u64,
    started_at: Instant,
    network_log: Vec<NetworkLogEntryInternal>,
    cookie_updates: Vec<CookieUpdate>,
) -> NavigateOutcome {
    let file_path = match validate_local_file_url_target(url, allow_private_targets) {
        Ok(path) => path,
        Err(error) => {
            return NavigateOutcome {
                success: false,
                final_url: url.to_string(),
                status_code: 0,
                title: String::new(),
                page_body: String::new(),
                body_bytes: 0,
                latency_ms: started_at.elapsed().as_millis() as u64,
                error,
                network_log,
                cookie_updates,
            };
        }
    };

    let metadata = match fs::metadata(file_path.as_path()) {
        Ok(value) => value,
        Err(error) => {
            return NavigateOutcome {
                success: false,
                final_url: url.to_string(),
                status_code: 0,
                title: String::new(),
                page_body: String::new(),
                body_bytes: 0,
                latency_ms: started_at.elapsed().as_millis() as u64,
                error: format!("failed to inspect local file target: {error}"),
                network_log,
                cookie_updates,
            };
        }
    };
    let body_bytes = metadata.len();
    if body_bytes > max_response_bytes {
        return NavigateOutcome {
            success: false,
            final_url: url.to_string(),
            status_code: 0,
            title: String::new(),
            page_body: String::new(),
            body_bytes,
            latency_ms: started_at.elapsed().as_millis() as u64,
            error: format!(
                "response exceeds max_response_bytes ({body_bytes} > {max_response_bytes})"
            ),
            network_log,
            cookie_updates,
        };
    }
    let page_body = match fs::read(file_path.as_path()) {
        Ok(bytes) => String::from_utf8_lossy(bytes.as_slice()).into_owned(),
        Err(error) => {
            return NavigateOutcome {
                success: false,
                final_url: url.to_string(),
                status_code: 0,
                title: String::new(),
                page_body: String::new(),
                body_bytes,
                latency_ms: started_at.elapsed().as_millis() as u64,
                error: format!("failed to read local file target: {error}"),
                network_log,
                cookie_updates,
            };
        }
    };
    NavigateOutcome {
        success: true,
        final_url: url.to_string(),
        status_code: 0,
        title: String::new(),
        page_body,
        body_bytes,
        latency_ms: started_at.elapsed().as_millis() as u64,
        error: String::new(),
        network_log,
        cookie_updates,
    }
}

/// Gate for `file://` targets, only open to sessions allowed private targets.
///
/// Credentials and query strings are rejected, the path is canonicalized so
/// reads operate on the resolved real file (symlinks and `..` collapse here),
/// and the target must be a regular file - directories and special files are
/// refused.
///
/// # Errors
/// Returns the policy reason or the filesystem failure for invalid targets.
fn validate_local_file_url_target(
    url: &Url,
    allow_private_targets: bool,
) -> Result<PathBuf, String> {
    if !allow_private_targets {
        return Err(
            "blocked URL scheme 'file'; local file navigation requires allow_private_targets=true"
                .to_owned(),
        );
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err("file URL credentials are not allowed".to_owned());
    }
    if url.query().is_some() {
        return Err("file URL query strings are not allowed".to_owned());
    }
    let file_path = url.to_file_path().map_err(|_| "file URL path is invalid".to_owned())?;
    let canonical = fs::canonicalize(file_path.as_path())
        .map_err(|error| format!("failed to resolve local file target: {error}"))?;
    let metadata = fs::metadata(canonical.as_path())
        .map_err(|error| format!("failed to inspect local file target: {error}"))?;
    if !metadata.is_file() {
        return Err("file URL target is not a regular file".to_owned());
    }
    Ok(canonical)
}

/// Post-connect re-check that the connected peer is not private/local.
///
/// Defense in depth behind DNS pinning: the actual peer address is verified
/// again before the response is consumed, so anything that changes the
/// effective peer after validation still fails closed. Skipped only when the
/// transport exposes no peer address.
///
/// # Errors
/// Returns the policy violation when the peer IP is private/local and private
/// targets are not allowed.
pub(crate) fn enforce_remote_response_ip_policy(
    remote_addr: Option<SocketAddr>,
    allow_private_targets: bool,
) -> Result<(), String> {
    if allow_private_targets {
        return Ok(());
    }
    let Some(remote_addr) = remote_addr else {
        return Ok(());
    };
    let remote_ip = remote_addr.ip();
    if !netguard::is_private_or_local_ip(remote_ip) {
        return Ok(());
    }
    DNS_VALIDATION_METRICS.blocked_total.fetch_add(1, Ordering::Relaxed);
    DNS_VALIDATION_METRICS.blocked_private_targets.fetch_add(1, Ordering::Relaxed);
    Err(format!(
        "remote response IP {remote_ip} is private/local and violates browser session policy"
    ))
}

/// Validates an HTTP(S) URL and returns the exact addresses to pin for it.
///
/// Resolution, policy enforcement, and pinning data are produced in one step
/// so callers cannot accidentally connect using a second, unvalidated DNS
/// answer.
///
/// # Errors
/// Returns the policy or resolution failure reason.
pub(crate) async fn validate_target_url(
    url: &Url,
    allow_private_targets: bool,
) -> Result<ValidatedTargetUrl, String> {
    let result = async {
        let (host, port) = extract_target_host_port(url)?;
        let resolved = resolve_host_addresses_async(host, port).await?;
        let resolved_addresses = resolved.addresses.clone();
        enforce_resolved_host_policy(host, resolved, allow_private_targets)?;
        let resolved_socket_addrs = resolved_addresses
            .into_iter()
            .map(|address| SocketAddr::new(address, port))
            .collect::<Vec<_>>();
        let host = if host.parse::<IpAddr>().is_ok() { None } else { Some(host.to_owned()) };
        Ok(ValidatedTargetUrl { host, resolved_socket_addrs })
    }
    .await;
    maybe_log_dns_validation_metrics();
    result
}

/// Builds a reqwest client locked to the validated target.
///
/// Redirects are disabled because the caller re-validates every hop itself,
/// and for named hosts resolution is overridden with the already-validated
/// socket addresses so the connection cannot follow a fresh - possibly
/// rebound - DNS answer.
///
/// # Errors
/// Returns the underlying client construction error.
pub(crate) fn build_pinned_http_client(
    timeout_ms: u64,
    validated_target: &ValidatedTargetUrl,
) -> Result<reqwest::Client, reqwest::Error> {
    let mut client_builder = reqwest::Client::builder()
        .redirect(Policy::none())
        .timeout(Duration::from_millis(timeout_ms.max(1)));
    if let Some(host) = validated_target.host.as_ref() {
        client_builder = client_builder
            .resolve_to_addrs(host.as_str(), validated_target.resolved_socket_addrs.as_slice());
    }
    client_builder.build()
}
