use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
};

use palyra_common::{netguard, redaction::redact_url};
use palyra_egress_proxy::{
    CredentialBindingPlan, EgressPolicyVerdict, EgressProxyPolicyService, EgressProxyRequest,
};
use palyra_safety::{redact_text_for_export, SafetyContentKind, SafetySourceKind, TrustLabel};
use palyra_vault::SecretResolver;
use reqwest::{header::HeaderValue, redirect::Policy, Url};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use ulid::Ulid;

use crate::{
    gateway::{
        current_unix_ms, CachedHttpFetchEntry, GatewayRuntimeState, MAX_HTTP_FETCH_BODY_BYTES,
        MAX_HTTP_FETCH_CACHE_KEY_BYTES, MAX_HTTP_FETCH_REDIRECTS, MAX_HTTP_FETCH_TOOL_INPUT_BYTES,
    },
    tool_protocol::{ToolAttestation, ToolExecutionOutcome},
};

pub(crate) async fn execute_http_fetch_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    proposal_id: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
    if input_json.len() > MAX_HTTP_FETCH_TOOL_INPUT_BYTES {
        return http_fetch_tool_execution_outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("palyra.http.fetch input exceeds {MAX_HTTP_FETCH_TOOL_INPUT_BYTES} bytes"),
        );
    }

    let payload = match serde_json::from_slice::<Value>(input_json) {
        Ok(Value::Object(map)) => map,
        Ok(_) => {
            return http_fetch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.http.fetch requires JSON object input".to_owned(),
            );
        }
        Err(error) => {
            return http_fetch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.http.fetch invalid JSON input: {error}"),
            );
        }
    };

    let url_raw = match payload.get("url").and_then(Value::as_str).map(str::trim) {
        Some(value) if !value.is_empty() => value.to_owned(),
        _ => {
            return http_fetch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.http.fetch requires non-empty string field 'url'".to_owned(),
            );
        }
    };
    let method = payload
        .get("method")
        .and_then(Value::as_str)
        .map(|value| value.trim().to_ascii_uppercase())
        .unwrap_or_else(|| "GET".to_owned());
    if !matches!(method.as_str(), "GET" | "HEAD" | "POST") {
        return http_fetch_tool_execution_outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            "palyra.http.fetch method must be one of: GET|HEAD|POST".to_owned(),
        );
    }

    let body = match payload.get("body") {
        Some(Value::String(value)) => value.clone(),
        Some(_) => {
            return http_fetch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.http.fetch body must be a string".to_owned(),
            );
        }
        None => String::new(),
    };

    let request_headers = match payload.get("headers") {
        Some(Value::Object(values)) => {
            let mut headers = Vec::new();
            for (name, value) in values {
                let Value::String(raw_value) = value else {
                    return http_fetch_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        format!("palyra.http.fetch header '{name}' must be a string"),
                    );
                };
                let normalized_name = name.trim().to_ascii_lowercase();
                if normalized_name.is_empty() {
                    return http_fetch_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        "palyra.http.fetch header names cannot be empty".to_owned(),
                    );
                }
                if !runtime_state
                    .config
                    .http_fetch
                    .allowed_request_headers
                    .iter()
                    .any(|allowed| allowed == &normalized_name)
                {
                    return http_fetch_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        format!(
                            "palyra.http.fetch header '{normalized_name}' is not allowed by policy"
                        ),
                    );
                }
                headers.push((normalized_name, raw_value.clone()));
            }
            headers
        }
        Some(_) => {
            return http_fetch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.http.fetch headers must be an object map".to_owned(),
            );
        }
        None => Vec::new(),
    };
    let credential_bindings = match parse_credential_bindings(&payload) {
        Ok(bindings) => bindings,
        Err(error) => {
            return http_fetch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                error,
            );
        }
    };
    if let Some(duplicate_header) = credential_bindings.iter().find_map(|binding| {
        let normalized = binding.header_name.trim().to_ascii_lowercase();
        request_headers
            .iter()
            .any(|(header_name, _)| header_name == &normalized)
            .then_some(normalized)
    }) {
        return http_fetch_tool_execution_outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!(
                "palyra.http.fetch credential binding duplicates explicit header '{duplicate_header}'"
            ),
        );
    }

    let allow_redirects = payload
        .get("allow_redirects")
        .and_then(Value::as_bool)
        .unwrap_or(runtime_state.config.http_fetch.allow_redirects);
    let max_redirects = payload
        .get("max_redirects")
        .and_then(Value::as_u64)
        .map(|value| value as usize)
        .unwrap_or(runtime_state.config.http_fetch.max_redirects)
        .clamp(1, MAX_HTTP_FETCH_REDIRECTS);
    let allow_private_targets = runtime_state.config.http_fetch.allow_private_targets
        && payload.get("allow_private_targets").and_then(Value::as_bool).unwrap_or(true);
    let max_response_bytes = payload
        .get("max_response_bytes")
        .and_then(Value::as_u64)
        .map(|value| value as usize)
        .unwrap_or(runtime_state.config.http_fetch.max_response_bytes)
        .clamp(1, MAX_HTTP_FETCH_BODY_BYTES);
    let cache_enabled = payload
        .get("cache")
        .and_then(Value::as_bool)
        .unwrap_or(runtime_state.config.http_fetch.cache_enabled)
        && matches!(method.as_str(), "GET" | "HEAD");
    let cache_ttl_ms = payload
        .get("cache_ttl_ms")
        .and_then(Value::as_u64)
        .unwrap_or(runtime_state.config.http_fetch.cache_ttl_ms)
        .max(1);
    let allowed_content_types = match payload.get("allowed_content_types") {
        Some(Value::Array(values)) => {
            let mut parsed = Vec::new();
            for value in values {
                let Some(content_type) = value.as_str() else {
                    return http_fetch_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        "palyra.http.fetch allowed_content_types must be strings".to_owned(),
                    );
                };
                let normalized =
                    content_type.split(';').next().unwrap_or_default().trim().to_ascii_lowercase();
                if normalized.is_empty() {
                    continue;
                }
                if !runtime_state
                    .config
                    .http_fetch
                    .allowed_content_types
                    .iter()
                    .any(|allowed| allowed == &normalized)
                {
                    return http_fetch_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        format!(
                            "palyra.http.fetch content type '{normalized}' is not allowed by policy"
                        ),
                    );
                }
                if !parsed.iter().any(|existing| existing == &normalized) {
                    parsed.push(normalized);
                }
            }
            if parsed.is_empty() {
                runtime_state.config.http_fetch.allowed_content_types.clone()
            } else {
                parsed
            }
        }
        Some(_) => {
            return http_fetch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.http.fetch allowed_content_types must be an array of strings".to_owned(),
            );
        }
        None => runtime_state.config.http_fetch.allowed_content_types.clone(),
    };

    let url = match Url::parse(url_raw.as_str()) {
        Ok(value) => value,
        Err(error) => {
            return http_fetch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.http.fetch URL is invalid: {error}"),
            );
        }
    };
    if !matches!(url.scheme(), "http" | "https") {
        return http_fetch_tool_execution_outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("palyra.http.fetch blocked URL scheme '{}'", url.scheme()),
        );
    }
    if !url.username().is_empty() || url.password().is_some() {
        return http_fetch_tool_execution_outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            "palyra.http.fetch URL credentials are not allowed".to_owned(),
        );
    }

    let initial_egress_verdict = match evaluate_http_fetch_egress(
        runtime_state,
        method.as_str(),
        &url,
        allow_private_targets,
        max_response_bytes,
        credential_bindings.as_slice(),
    ) {
        Ok(value) => value,
        Err(error) => {
            return http_fetch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                error,
            );
        }
    };
    let resolved_credential_headers =
        match resolve_credential_bindings(runtime_state, credential_bindings.as_slice()) {
            Ok(value) => value,
            Err(error) => {
                return http_fetch_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
        };
    let initial_resolved_addrs = initial_egress_verdict.resolved_addresses.clone();
    let mut current_egress_verdict = initial_egress_verdict;
    let mut next_egress_verdict =
        Some((current_egress_verdict.clone(), initial_resolved_addrs.clone()));

    let cache_policy = HttpFetchCachePolicy {
        allow_private_targets,
        allow_redirects,
        max_redirects,
        max_response_bytes,
        allowed_content_types: allowed_content_types.as_slice(),
    };
    let cache_key = http_fetch_cache_key(
        method.as_str(),
        url.as_str(),
        request_headers.as_slice(),
        body.as_str(),
        &cache_policy,
    );
    if cache_enabled {
        let now = current_unix_ms();
        if let Ok(mut cache) = runtime_state.http_fetch_cache.lock() {
            cache.retain(|_, entry| entry.expires_at_unix_ms > now);
            if let Some(cached) = cache.get(cache_key.as_str()) {
                return http_fetch_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    true,
                    cached.output_json.clone(),
                    String::new(),
                );
            }
        }
    }

    let started_at = Instant::now();
    let mut current_url = url;
    let mut redirects_followed = 0_usize;
    loop {
        let (egress_verdict, resolved_addrs) = if let Some(resolved) = next_egress_verdict.take() {
            resolved
        } else {
            match evaluate_http_fetch_egress(
                runtime_state,
                method.as_str(),
                &current_url,
                allow_private_targets,
                max_response_bytes,
                credential_bindings.as_slice(),
            ) {
                Ok(value) => {
                    let resolved = value.resolved_addresses.clone();
                    (value, resolved)
                }
                Err(error) => {
                    return http_fetch_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        error,
                    );
                }
            }
        };
        current_egress_verdict = egress_verdict;

        let host = current_url.host_str().unwrap_or_default().to_owned();
        let mut client_builder = reqwest::Client::builder()
            .redirect(Policy::none())
            .connect_timeout(Duration::from_millis(
                runtime_state.config.http_fetch.connect_timeout_ms,
            ))
            .timeout(Duration::from_millis(runtime_state.config.http_fetch.request_timeout_ms));
        if !host.is_empty() && host.parse::<IpAddr>().is_err() {
            for address in resolved_addrs {
                client_builder = client_builder.resolve(host.as_str(), address);
            }
        }
        let client = match client_builder.build() {
            Ok(value) => value,
            Err(error) => {
                return http_fetch_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    format!("palyra.http.fetch failed to build HTTP client: {error}"),
                );
            }
        };

        let method_value = match method.parse::<reqwest::Method>() {
            Ok(value) => value,
            Err(error) => {
                return http_fetch_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    format!("palyra.http.fetch invalid method: {error}"),
                );
            }
        };
        let mut request = client.request(method_value, current_url.clone());
        for (name, value) in request_headers.as_slice() {
            request = request.header(name, value);
        }
        for (name, value) in resolved_credential_headers.as_slice() {
            request = request.header(name, value.clone());
        }
        if method == "POST" && !body.is_empty() {
            request = request.body(body.clone());
        }
        let mut response = match request.send().await {
            Ok(value) => value,
            Err(error) => {
                return http_fetch_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    format!("palyra.http.fetch request failed: {error}"),
                );
            }
        };

        if response.status().is_redirection() {
            if !allow_redirects {
                return http_fetch_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    "palyra.http.fetch redirect blocked by policy".to_owned(),
                );
            }
            if redirects_followed >= max_redirects {
                return http_fetch_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    format!("palyra.http.fetch redirect limit exceeded ({max_redirects})"),
                );
            }
            let Some(location) = response.headers().get(reqwest::header::LOCATION) else {
                return http_fetch_tool_execution_outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    "palyra.http.fetch redirect response missing Location header".to_owned(),
                );
            };
            let location_str = match location.to_str() {
                Ok(value) => value,
                Err(_) => {
                    return http_fetch_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        "palyra.http.fetch redirect Location header is invalid UTF-8".to_owned(),
                    );
                }
            };
            current_url = match current_url.join(location_str) {
                Ok(value) => value,
                Err(error) => {
                    return http_fetch_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        format!("palyra.http.fetch redirect URL is invalid: {error}"),
                    );
                }
            };
            redirects_followed = redirects_followed.saturating_add(1);
            next_egress_verdict = None;
            continue;
        }

        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.split(';').next().unwrap_or_default().trim().to_ascii_lowercase())
            .unwrap_or_default();
        if !content_type.is_empty()
            && !allowed_content_types.iter().any(|allowed| allowed == &content_type)
        {
            return http_fetch_tool_execution_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.http.fetch content type '{content_type}' is blocked by policy"),
            );
        }

        let mut body_bytes = Vec::new();
        if method != "HEAD" {
            loop {
                let chunk = match response.chunk().await {
                    Ok(value) => value,
                    Err(error) => {
                        return http_fetch_tool_execution_outcome(
                            proposal_id,
                            input_json,
                            false,
                            b"{}".to_vec(),
                            format!("palyra.http.fetch failed to stream response body: {error}"),
                        );
                    }
                };
                let Some(chunk) = chunk else {
                    break;
                };
                if body_bytes.len().saturating_add(chunk.len()) > max_response_bytes {
                    return http_fetch_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        format!(
                            "palyra.http.fetch response exceeds max_response_bytes ({max_response_bytes})"
                        ),
                    );
                }
                body_bytes.extend_from_slice(chunk.as_ref());
            }
        }

        let status_code = response.status().as_u16();
        let success = response.status().is_success();
        let body_text = String::from_utf8_lossy(body_bytes.as_slice()).to_string();
        let body_export = export_http_fetch_body(body_text.as_str());
        let output_json = json!({
            "url": redact_url(current_url.as_str()),
            "method": method,
            "status_code": status_code,
            "redirects_followed": redirects_followed,
            "content_type": content_type,
            "body_bytes": body_bytes.len(),
            "body_text": body_export.body_text,
            "latency_ms": started_at.elapsed().as_millis() as u64,
            "request_headers": redacted_http_headers(request_headers.as_slice()),
            "safety": body_export.safety_json,
            "egress": {
                "request_fingerprint_sha256": current_egress_verdict.request_fingerprint_sha256,
                "reason_code": current_egress_verdict.reason_code,
                "host": current_egress_verdict.host,
                "resolved_socket_addrs": current_egress_verdict.resolved_socket_addrs,
                "injected_credential_headers": current_egress_verdict.injected_credential_headers,
            },
        });
        let serialized = serde_json::to_vec(&output_json).unwrap_or_else(|_| b"{}".to_vec());
        if cache_enabled && success {
            if let Ok(mut cache) = runtime_state.http_fetch_cache.lock() {
                let now = current_unix_ms();
                cache.retain(|_, entry| entry.expires_at_unix_ms > now);
                while cache.len() >= runtime_state.config.http_fetch.max_cache_entries {
                    let Some(first_key) = cache.keys().next().cloned() else {
                        break;
                    };
                    cache.remove(first_key.as_str());
                }
                cache.insert(
                    cache_key.clone(),
                    CachedHttpFetchEntry {
                        expires_at_unix_ms: now.saturating_add(cache_ttl_ms as i64),
                        output_json: serialized.clone(),
                    },
                );
            }
        }
        return http_fetch_tool_execution_outcome(
            proposal_id,
            input_json,
            success,
            serialized,
            if success {
                String::new()
            } else {
                format!("palyra.http.fetch returned HTTP {status_code}")
            },
        );
    }
}

pub(crate) struct HttpFetchCachePolicy<'a> {
    pub(crate) allow_private_targets: bool,
    pub(crate) allow_redirects: bool,
    pub(crate) max_redirects: usize,
    pub(crate) max_response_bytes: usize,
    pub(crate) allowed_content_types: &'a [String],
}

pub(crate) fn http_fetch_cache_key(
    method: &str,
    url: &str,
    headers: &[(String, String)],
    body: &str,
    policy: &HttpFetchCachePolicy<'_>,
) -> String {
    let mut normalized_headers =
        headers.iter().map(|(name, value)| format!("{name}:{value}")).collect::<Vec<_>>();
    normalized_headers.sort();
    let mut normalized_content_types = policy
        .allowed_content_types
        .iter()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    normalized_content_types.sort();
    normalized_content_types.dedup();
    let policy_fingerprint = format!(
        "allow_private_targets={};allow_redirects={};max_redirects={};max_response_bytes={};allowed_content_types={}",
        policy.allow_private_targets,
        policy.allow_redirects,
        policy.max_redirects,
        policy.max_response_bytes,
        normalized_content_types.join(",")
    );
    let mut key = format!(
        "{method}|{url}|{}|{}|{}",
        normalized_headers.join("&"),
        sha256_hex(body.as_bytes()),
        sha256_hex(policy_fingerprint.as_bytes())
    );
    if key.len() > MAX_HTTP_FETCH_CACHE_KEY_BYTES {
        key = format!("sha256:{}", sha256_hex(key.as_bytes()));
    }
    key
}

#[allow(dead_code)]
pub(crate) async fn resolve_fetch_target_addresses(
    url: &Url,
    allow_private_targets: bool,
) -> Result<Vec<SocketAddr>, String> {
    let host = url.host_str().ok_or_else(|| "URL host is required".to_owned())?;
    let port = url.port_or_known_default().ok_or_else(|| "URL port is required".to_owned())?;
    if let Some(ip) = netguard::parse_host_ip_literal(host)? {
        let resolved = vec![SocketAddr::new(ip, port)];
        validate_resolved_fetch_addresses(&resolved, allow_private_targets)?;
        return Ok(resolved);
    }
    let resolved = tokio::net::lookup_host((host, port))
        .await
        .map_err(|error| format!("DNS resolution failed for '{host}:{port}': {error}"))?
        .collect::<Vec<_>>();
    validate_resolved_fetch_addresses(&resolved, allow_private_targets)?;
    Ok(resolved)
}

#[allow(dead_code)]
pub(crate) fn validate_resolved_fetch_addresses(
    addrs: &[SocketAddr],
    allow_private_targets: bool,
) -> Result<(), String> {
    let ips = addrs.iter().map(|address| address.ip()).collect::<Vec<_>>();
    netguard::validate_resolved_ip_addrs(ips.as_slice(), allow_private_targets)
}

fn parse_credential_bindings(
    payload: &serde_json::Map<String, Value>,
) -> Result<Vec<CredentialBindingPlan>, String> {
    match payload.get("credential_bindings") {
        Some(Value::Array(_)) => serde_json::from_value::<Vec<CredentialBindingPlan>>(
            payload.get("credential_bindings").cloned().unwrap_or(Value::Null),
        )
        .map_err(|error| format!("palyra.http.fetch credential_bindings are invalid: {error}")),
        Some(_) => {
            Err("palyra.http.fetch credential_bindings must be an array of binding objects"
                .to_owned())
        }
        None => Ok(Vec::new()),
    }
}

fn evaluate_http_fetch_egress(
    runtime_state: &Arc<GatewayRuntimeState>,
    method: &str,
    url: &Url,
    allow_private_targets: bool,
    max_response_bytes: usize,
    credential_bindings: &[CredentialBindingPlan],
) -> Result<EgressPolicyVerdict, String> {
    EgressProxyPolicyService
        .evaluate_request(&EgressProxyRequest {
            method,
            url: url.as_str(),
            allow_private_targets,
            allowed_hosts: runtime_state
                .config
                .tool_call
                .process_runner
                .allowed_egress_hosts
                .as_slice(),
            allowed_dns_suffixes: runtime_state
                .config
                .tool_call
                .process_runner
                .allowed_dns_suffixes
                .as_slice(),
            max_response_bytes,
            credential_bindings,
        })
        .map_err(|error| format!("palyra.http.fetch target blocked: {error}"))
}

fn resolve_credential_bindings(
    runtime_state: &Arc<GatewayRuntimeState>,
    credential_bindings: &[CredentialBindingPlan],
) -> Result<Vec<(String, HeaderValue)>, String> {
    if credential_bindings.is_empty() {
        return Ok(Vec::new());
    }
    let resolver = SecretResolver::with_working_dir(
        Some(runtime_state.vault.as_ref()),
        runtime_state.config.tool_call.process_runner.workspace_root.as_path(),
    );
    let mut resolved = Vec::with_capacity(credential_bindings.len());
    for binding in credential_bindings {
        let resolution = resolver.resolve(&binding.secret_ref).map_err(|error| {
            format!(
                "palyra.http.fetch credential binding '{}' could not resolve secret: {}",
                binding.header_name, error
            )
        })?;
        let Some(secret_value) = resolution.value else {
            if binding.required || binding.secret_ref.required {
                return Err(format!(
                    "palyra.http.fetch credential binding '{}' requires a present secret snapshot",
                    binding.header_name
                ));
            }
            continue;
        };
        let value = HeaderValue::from_bytes(secret_value.as_ref()).map_err(|error| {
            format!(
                "palyra.http.fetch credential binding '{}' produced an invalid header value: {error}",
                binding.header_name
            )
        })?;
        resolved.push((binding.header_name.trim().to_ascii_lowercase(), value));
    }
    Ok(resolved)
}

fn redacted_http_headers(headers: &[(String, String)]) -> Vec<serde_json::Value> {
    headers
        .iter()
        .map(|(name, value)| {
            let sensitive = name.contains("authorization")
                || name.contains("cookie")
                || name.contains("token")
                || name.contains("api-key")
                || name.contains("apikey");
            json!({
                "name": name,
                "value": if sensitive { "<redacted>" } else { value.as_str() }
            })
        })
        .collect()
}

fn http_fetch_tool_execution_outcome(
    proposal_id: &str,
    input_json: &[u8],
    success: bool,
    output_json: Vec<u8>,
    error: String,
) -> ToolExecutionOutcome {
    let executed_at_unix_ms = current_unix_ms();
    let mut hasher = Sha256::new();
    hasher.update(b"palyra.http.fetch.attestation.v1");
    hasher.update((proposal_id.len() as u64).to_be_bytes());
    hasher.update(proposal_id.as_bytes());
    hasher.update((input_json.len() as u64).to_be_bytes());
    hasher.update(input_json);
    hasher.update([u8::from(success)]);
    hasher.update((output_json.len() as u64).to_be_bytes());
    hasher.update(output_json.as_slice());
    hasher.update((error.len() as u64).to_be_bytes());
    hasher.update(error.as_bytes());
    hasher.update(executed_at_unix_ms.to_be_bytes());
    let execution_sha256 = hex::encode(hasher.finalize());

    ToolExecutionOutcome {
        success,
        output_json,
        error,
        attestation: ToolAttestation {
            attestation_id: Ulid::new().to_string(),
            execution_sha256,
            executed_at_unix_ms,
            timed_out: false,
            executor: "gateway_http_fetch".to_owned(),
            sandbox_enforcement: "ssrf_guard".to_owned(),
        },
    }
}

struct HttpFetchBodyExport {
    body_text: String,
    safety_json: Value,
}

fn export_http_fetch_body(body_text: &str) -> HttpFetchBodyExport {
    let outcome = redact_text_for_export(
        body_text,
        SafetySourceKind::HttpFetch,
        SafetyContentKind::HttpResponse,
        TrustLabel::ExternalUntrusted,
    );
    HttpFetchBodyExport {
        body_text: outcome.redacted_text,
        safety_json: json!({
            "trust_label": outcome.scan.trust_label.as_str(),
            "action": outcome.scan.recommended_action.as_str(),
            "findings": outcome.scan.finding_codes(),
            "redacted": outcome.redacted,
        }),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::export_http_fetch_body;

    #[test]
    fn http_fetch_export_redacts_sensitive_body_text() {
        let exported = export_http_fetch_body("Authorization: Bearer super-secret-token-value");
        assert_eq!(exported.body_text, "Authorization: [REDACTED_SECRET]");
        assert_eq!(exported.safety_json["trust_label"], "external_untrusted");
        assert_eq!(exported.safety_json["action"], "redact");
        assert!(exported.safety_json["redacted"].as_bool().unwrap_or(false));
        let findings = exported.safety_json["findings"]
            .as_array()
            .expect("findings should serialize as an array")
            .iter()
            .filter_map(|value| value.as_str())
            .collect::<Vec<_>>();
        assert!(
            findings.contains(&"secret_leak.header.authorization"),
            "authorization header leak should be reported"
        );
    }
}
