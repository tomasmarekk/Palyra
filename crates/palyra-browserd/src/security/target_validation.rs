use crate::*;

#[derive(Debug, Clone)]
pub(crate) struct ResolvedHostAddresses {
    pub(crate) addresses: Vec<IpAddr>,
    pub(crate) blocked_for_default_policy: bool,
}

impl ResolvedHostAddresses {
    pub(crate) fn from_addresses(addresses: Vec<IpAddr>) -> Result<Self, String> {
        if addresses.is_empty() {
            return Err("DNS resolution returned no addresses".to_owned());
        }
        let blocked_for_default_policy =
            addresses.iter().copied().any(netguard::is_private_or_local_ip);
        Ok(Self { addresses, blocked_for_default_policy })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DnsValidationCacheEntry {
    expires_at: Instant,
    last_access_tick: u64,
}

#[derive(Debug)]
pub(crate) struct DnsValidationCache {
    entries: HashMap<String, DnsValidationCacheEntry>,
    max_entries: usize,
    negative_ttl: Duration,
    next_access_tick: u64,
}

impl DnsValidationCache {
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

#[derive(Debug, Clone)]
pub(crate) struct ValidatedTargetUrl {
    pub(crate) host: Option<String>,
    pub(crate) resolved_socket_addrs: Vec<SocketAddr>,
}

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

pub(crate) fn validate_target_url_parts_blocking(
    url: &Url,
    allow_private_targets: bool,
) -> Result<(), String> {
    let result = (|| {
        let (host, port) = extract_target_host_port(url)?;
        let resolved = resolve_host_addresses_blocking(host, port)?;
        enforce_resolved_host_policy(host, resolved, allow_private_targets)
    })();
    maybe_log_dns_validation_metrics();
    result
}

pub(crate) fn lock_dns_validation_cache() -> std::sync::MutexGuard<'static, DnsValidationCache> {
    DNS_VALIDATION_CACHE.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(crate) fn normalize_dns_host_cache_key(host: &str) -> String {
    host.trim().trim_end_matches('.').to_ascii_lowercase()
}

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

pub(crate) fn dns_resolution_error_for_host(host: &str, error: &std::io::Error) -> String {
    format!("DNS resolution failed for host '{host}': {error}")
}

pub(crate) fn dns_cached_nxdomain_error_for_host(host: &str) -> String {
    format!("DNS resolution failed for host '{host}': cached NXDOMAIN")
}

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

pub(crate) fn store_dns_nxdomain_cache(host: &str) {
    let key = normalize_dns_host_cache_key(host);
    let now = Instant::now();
    let mut cache = lock_dns_validation_cache();
    cache.insert_nxdomain(key, now);
}

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

pub(crate) fn track_dns_lookup_latency(lookup_started: Instant) {
    let lookup_latency_ms = lookup_started.elapsed().as_millis() as u64;
    DNS_VALIDATION_METRICS.dns_lookups.fetch_add(1, Ordering::Relaxed);
    DNS_VALIDATION_METRICS
        .dns_lookup_latency_ms_total
        .fetch_add(lookup_latency_ms, Ordering::Relaxed);
}

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

pub(crate) fn enforce_resolved_host_policy(
    host: &str,
    resolved: ResolvedHostAddresses,
    allow_private_targets: bool,
) -> Result<(), String> {
    if !allow_private_targets && resolved.blocked_for_default_policy {
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

pub(crate) fn dns_validation_metrics_snapshot() -> DnsValidationMetricsSnapshot {
    let cache_entries = lock_dns_validation_cache().len();
    DNS_VALIDATION_METRICS.snapshot(cache_entries)
}

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

#[cfg(test)]
pub(crate) fn reset_dns_validation_tracking_for_tests() {
    let mut cache = lock_dns_validation_cache();
    cache.entries.clear();
    cache.next_access_tick = 0;
    drop(cache);
    DNS_VALIDATION_METRICS.reset_for_tests();
}

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
    let redirect_limit = max_redirects.clamp(1, 10);
    let mut redirects = 0_u32;
    loop {
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
        if let Some(value) = cookie_header.filter(|value| !value.trim().is_empty()) {
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
                return NavigateOutcome {
                    success: false,
                    final_url: current_url.to_string(),
                    status_code,
                    title: String::new(),
                    page_body: String::new(),
                    body_bytes: projected_len,
                    latency_ms: started_at.elapsed().as_millis() as u64,
                    error: format!(
                        "response exceeds max_response_bytes ({projected_len} > {max_response_bytes})"
                    ),
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
