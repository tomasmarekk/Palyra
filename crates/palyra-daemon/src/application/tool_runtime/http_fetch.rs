//! `palyra.http.fetch` tool backend: policy-gated outbound HTTP.
//!
//! Every hop -- the initial request and each followed redirect -- is
//! re-evaluated by [`palyra_egress_proxy::EgressProxyPolicyService`] (scheme,
//! host/DNS-suffix allowlists, private-target rules, vault-only credential
//! bindings), and the verdict's resolved addresses are pinned into the HTTP
//! client so the connection cannot be rebound between policy check and
//! connect. Redirects are followed manually (`Policy::none`) precisely so
//! that no hop escapes evaluation.
//!
//! Responses are streamed under a byte cap, filtered by a content-type
//! allowlist, reduced to visible text for HTML, and passed through the
//! safety redaction scan before reaching the model. Successful uncached
//! GET/HEAD responses may be cached under a key that fingerprints both the
//! request and the active policy.

use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
};

use palyra_common::{netguard, redaction::redact_url, secret_refs::SecretSource};
use palyra_egress_proxy::{
    CredentialBindingPlan, EgressPolicyVerdict, EgressProxyPolicyService, EgressProxyRequest,
};
use palyra_safety::{redact_text_for_export, SafetyContentKind, SafetySourceKind, TrustLabel};
use palyra_vault::{SecretResolver, VaultRef};
use reqwest::{header::HeaderValue, redirect::Policy, Url};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use ulid::Ulid;

use crate::{
    gateway::{
        current_unix_ms, CachedHttpFetchEntry, GatewayRuntimeState, MAX_HTTP_FETCH_BODY_BYTES,
        MAX_HTTP_FETCH_CACHE_KEY_BYTES, MAX_HTTP_FETCH_REDIRECTS, MAX_HTTP_FETCH_TOOL_INPUT_BYTES,
    },
    sandbox_runner::{process_runner_allows_host_access, SandboxProcessRunnerPolicy},
    tool_protocol::{ToolAttestation, ToolExecutionOutcome},
};

const HTTP_FETCH_HTML_SKIP_TAGS: &[&str] =
    &["head", "script", "style", "noscript", "template", "svg"];
const HTTP_FETCH_MODEL_BODY_INLINE_BYTES: usize = 12 * 1024;
const HTTP_FETCH_MODEL_BODY_HEAD_BYTES: usize = 4 * 1024;
const HTTP_FETCH_MODEL_BODY_TAIL_BYTES: usize = 4 * 1024;

/// Executes a `palyra.http.fetch` tool call.
///
/// Validates the request against the configured HTTP-fetch policy (method,
/// header and content-type allowlists, vault-backed credential bindings),
/// then runs the egress-gated fetch loop described in the module docs. Every
/// failure -- invalid input, policy denial, transport error, non-2xx status
/// -- is reported as an unsuccessful [`ToolExecutionOutcome`] rather than an
/// error so the tool loop stays alive.
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
    let credential_bindings = match parse_credential_bindings(
        &payload,
        runtime_state.config.http_fetch.allowed_credential_vault_refs.as_slice(),
    ) {
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
    // A header supplied both explicitly and via a credential binding would
    // let the model shadow or duplicate an injected credential; fail closed.
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
    // The `as usize` casts below are safe even where they could truncate:
    // the immediate clamp to the MAX_HTTP_FETCH_* ceilings bounds the result,
    // so an attacker-supplied huge value can only shrink the limit.
    let max_redirects = payload
        .get("max_redirects")
        .and_then(Value::as_u64)
        .map(|value| value as usize)
        .unwrap_or(runtime_state.config.http_fetch.max_redirects)
        .clamp(1, MAX_HTTP_FETCH_REDIRECTS);
    let max_response_bytes = payload
        .get("max_response_bytes")
        .and_then(Value::as_u64)
        .map(|value| value as usize)
        .unwrap_or(runtime_state.config.http_fetch.max_response_bytes)
        .clamp(1, MAX_HTTP_FETCH_BODY_BYTES);
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
    let cache_request = payload.get("cache").and_then(Value::as_bool);
    let credential_bound_fetch = !credential_bindings.is_empty();
    let cache_target_is_loopback = http_fetch_url_targets_loopback(&url);
    // Never cache credential-bound responses (a later uncredentialed call
    // could replay them), and skip the cache by default for loopback targets,
    // which are typically fast-changing local dev servers.
    let cache_enabled = matches!(method.as_str(), "GET" | "HEAD")
        && !credential_bound_fetch
        && cache_request.unwrap_or_else(|| {
            runtime_state.config.http_fetch.cache_enabled && !cache_target_is_loopback
        });
    let cache_bypassed_loopback_default = matches!(method.as_str(), "GET" | "HEAD")
        && cache_request.is_none()
        && runtime_state.config.http_fetch.cache_enabled
        && cache_target_is_loopback;
    let cache_ttl_ms = payload
        .get("cache_ttl_ms")
        .and_then(Value::as_u64)
        .unwrap_or(runtime_state.config.http_fetch.cache_ttl_ms)
        .max(1);
    let allowed_content_types = match payload.get("allowed_content_types") {
        Some(Value::Array(values)) => {
            let mut parsed = Vec::new();
            let mut rejected = Vec::new();
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
                    rejected.push(normalized);
                    continue;
                }
                if !parsed.iter().any(|existing| existing == &normalized) {
                    parsed.push(normalized);
                }
            }
            if parsed.is_empty() {
                if rejected.is_empty() {
                    runtime_state.config.http_fetch.allowed_content_types.clone()
                } else {
                    return http_fetch_tool_execution_outcome(
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        format!(
                            "palyra.http.fetch no requested allowed_content_types are permitted by policy: {}",
                            rejected.join(", ")
                        ),
                    );
                }
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

    let requested_allow_private_targets =
        payload.get("allow_private_targets").and_then(Value::as_bool);
    let allow_private_targets = http_fetch_allows_private_targets_for_url(
        runtime_state.config.http_fetch.allow_private_targets,
        &runtime_state.config.tool_call.process_runner,
        requested_allow_private_targets,
        &url,
    );

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
                    http_fetch_cached_output_with_hit_metadata(cached),
                    String::new(),
                );
            }
        }
    }

    let started_at = Instant::now();
    let mut current_url = url;
    let mut redirects_followed = 0_usize;
    loop {
        // The first iteration consumes the verdict already computed above
        // (so a redirect-free fetch evaluates policy exactly once); redirect
        // iterations find `None` here and re-evaluate for the new target.
        let (egress_verdict, resolved_addrs) = if let Some(resolved) = next_egress_verdict.take() {
            resolved
        } else {
            let allow_private_targets_for_current_url = http_fetch_allows_private_targets_for_url(
                runtime_state.config.http_fetch.allow_private_targets,
                &runtime_state.config.tool_call.process_runner,
                requested_allow_private_targets,
                &current_url,
            );
            match evaluate_http_fetch_egress(
                runtime_state,
                method.as_str(),
                &current_url,
                allow_private_targets_for_current_url,
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
        // `Policy::none()` is load-bearing: redirects are followed manually
        // below so each hop goes back through egress evaluation.
        let mut client_builder = reqwest::Client::builder()
            .redirect(Policy::none())
            .connect_timeout(Duration::from_millis(
                runtime_state.config.http_fetch.connect_timeout_ms,
            ))
            .timeout(Duration::from_millis(runtime_state.config.http_fetch.request_timeout_ms));
        // Pin the connection to the addresses the egress verdict validated;
        // without this, a second DNS lookup inside reqwest could be rebound
        // to a private address after the policy check (DNS-rebinding SSRF).
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
            // Drop the consumed verdict so the next loop iteration
            // re-evaluates egress policy for the redirect target.
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

        // Stream the body chunk by chunk and stop at the cap, so a hostile
        // server cannot balloon memory by ignoring Content-Length.
        let mut body_bytes = Vec::new();
        let mut body_truncated = false;
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
                    let remaining = max_response_bytes.saturating_sub(body_bytes.len());
                    if remaining > 0 {
                        body_bytes.extend_from_slice(&chunk[..remaining]);
                    }
                    body_truncated = true;
                    break;
                }
                body_bytes.extend_from_slice(chunk.as_ref());
            }
        }

        let status_code = response.status().as_u16();
        let success = response.status().is_success();
        let body_text = String::from_utf8_lossy(body_bytes.as_slice()).to_string();
        let model_body = http_fetch_model_body_text(content_type.as_str(), body_text.as_str());
        let body_export = export_http_fetch_body(model_body.body_text.as_str());
        let output_json = json!({
            "url": redact_url(current_url.as_str()),
            "method": method,
            "status_code": status_code,
            "redirects_followed": redirects_followed,
            "content_type": content_type,
            "body_bytes": body_bytes.len(),
            "max_response_bytes": max_response_bytes,
            "truncated": body_truncated,
            "body_text": body_export.body_text,
            "body_text_format": model_body.format,
            "body_text_model_truncated": body_export.model_truncated,
            "body_text_original_bytes": body_export.original_bytes,
            "body_text_sha256": body_export.sha256,
            "latency_ms": started_at.elapsed().as_millis() as u64,
            "request_headers": redacted_http_headers(request_headers.as_slice()),
            "cache": {
                "enabled": cache_enabled,
                "status": http_fetch_cache_status(cache_enabled, cache_bypassed_loopback_default),
                "ttl_ms": if cache_enabled { cache_ttl_ms } else { 0 },
            },
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
            // A poisoned cache lock is deliberately ignored: caching is
            // best-effort and must never fail an otherwise successful fetch.
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
                        // `cache_ttl_ms as i64` cannot produce a negative
                        // expiry for any realistic TTL; an absurd operator
                        // value would only expire the entry immediately.
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

/// Policy dimensions folded into the cache key so requests evaluated under
/// different policies can never share a cached response.
pub(crate) struct HttpFetchCachePolicy<'a> {
    pub(crate) allow_private_targets: bool,
    pub(crate) allow_redirects: bool,
    pub(crate) max_redirects: usize,
    pub(crate) max_response_bytes: usize,
    pub(crate) allowed_content_types: &'a [String],
}

fn http_fetch_cache_status(cache_enabled: bool, bypassed_loopback_default: bool) -> &'static str {
    if cache_enabled {
        "miss"
    } else if bypassed_loopback_default {
        "bypassed_loopback_default"
    } else {
        "disabled"
    }
}

/// Replays a cached response with its `cache` block rewritten to a hit
/// marker; falls back to the stored bytes verbatim if they fail to re-parse.
fn http_fetch_cached_output_with_hit_metadata(cached: &CachedHttpFetchEntry) -> Vec<u8> {
    let mut payload = serde_json::from_slice::<Value>(cached.output_json.as_slice())
        .unwrap_or_else(|_| json!({}));
    if let Value::Object(ref mut object) = payload {
        object.insert(
            "cache".to_owned(),
            json!({
                "enabled": true,
                "status": "hit",
                "expires_at_unix_ms": cached.expires_at_unix_ms,
            }),
        );
        serde_json::to_vec(&payload).unwrap_or_else(|_| cached.output_json.clone())
    } else {
        cached.output_json.clone()
    }
}

/// Builds the deterministic cache key for one fetch: method, URL, sorted
/// headers, body hash, and a hash of the [`HttpFetchCachePolicy`]
/// fingerprint. Oversized keys collapse to their own SHA-256.
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

/// Resolves a fetch target to socket addresses and rejects any address the
/// netguard private/SSRF rules disallow.
///
/// Not called on the production fetch path (which takes resolved addresses
/// from the egress verdict); kept for the gateway SSRF test suite that
/// exercises netguard validation directly.
///
/// # Errors
/// Returns an error when the URL lacks a host/port, DNS resolution fails, or
/// any resolved address is blocked.
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

/// Applies the netguard private/SSRF address rules to already-resolved
/// addresses. Retained for the gateway SSRF test suite (see
/// [`resolve_fetch_target_addresses`]).
///
/// # Errors
/// Returns an error when any address falls in a blocked range and
/// `allow_private_targets` is false.
#[allow(dead_code)]
pub(crate) fn validate_resolved_fetch_addresses(
    addrs: &[SocketAddr],
    allow_private_targets: bool,
) -> Result<(), String> {
    let ips = addrs.iter().map(|address| address.ip()).collect::<Vec<_>>();
    netguard::validate_resolved_ip_addrs(ips.as_slice(), allow_private_targets)
}

/// Parses and validates `credential_bindings` from the tool input.
///
/// Bindings fail closed: each must reference a vault-backed secret whose
/// normalized `scope/key` appears in the operator-configured allowlist, so
/// the model can only ever attach pre-approved credentials.
///
/// # Errors
/// Returns a model-facing message when the field is malformed, the allowlist
/// is empty while bindings are present, a secret ref is invalid or
/// non-vault, or a vault ref is not allowlisted.
fn parse_credential_bindings(
    payload: &serde_json::Map<String, Value>,
    allowed_credential_vault_refs: &[String],
) -> Result<Vec<CredentialBindingPlan>, String> {
    match payload.get("credential_bindings") {
        Some(Value::Array(values)) => {
            let bindings =
                serde_json::from_value::<Vec<CredentialBindingPlan>>(Value::Array(values.clone()))
                    .map_err(|error| {
                        format!("palyra.http.fetch credential_bindings are invalid: {error}")
                    })?;
            if !bindings.is_empty() && allowed_credential_vault_refs.is_empty() {
                return Err(
                    "palyra.http.fetch credential_bindings require configured tool_call.http_fetch.allowed_credential_vault_refs"
                        .to_owned(),
                );
            }
            for binding in &bindings {
                binding.secret_ref.validate().map_err(|error| {
                    format!(
                        "palyra.http.fetch credential binding '{}' has invalid secret_ref: {error}",
                        binding.header_name
                    )
                })?;
                let normalized_vault_ref =
                    http_fetch_credential_vault_ref(binding).ok_or_else(|| {
                        format!(
                            "palyra.http.fetch credential binding '{}' must use a vault-backed secret_ref",
                            binding.header_name
                        )
                    })??;
                if !allowed_credential_vault_refs
                    .iter()
                    .any(|allowed| allowed == &normalized_vault_ref)
                {
                    return Err(format!(
                        "palyra.http.fetch credential binding '{}' uses vault ref '{}' that is not allowed by tool_call.http_fetch.allowed_credential_vault_refs",
                        binding.header_name, normalized_vault_ref
                    ));
                }
            }
            Ok(bindings)
        }
        Some(_) => {
            Err("palyra.http.fetch credential_bindings must be an array of binding objects"
                .to_owned())
        }
        None => Ok(Vec::new()),
    }
}

/// Returns the binding's normalized `scope/key` vault ref, `None` when the
/// secret source is not vault-backed, or `Some(Err)` when the vault ref is
/// syntactically invalid.
fn http_fetch_credential_vault_ref(
    binding: &CredentialBindingPlan,
) -> Option<Result<String, String>> {
    let SecretSource::Vault { vault_ref } = &binding.secret_ref.source else {
        return None;
    };
    Some(normalize_http_fetch_credential_vault_ref(
        binding.header_name.as_str(),
        vault_ref.as_str(),
    ))
}

fn normalize_http_fetch_credential_vault_ref(
    header_name: &str,
    vault_ref: &str,
) -> Result<String, String> {
    let parsed = VaultRef::parse(vault_ref).map_err(|error| {
        format!(
            "palyra.http.fetch credential binding '{header_name}' has invalid vault ref: {error}"
        )
    })?;
    Ok(format!("{}/{}", parsed.scope, parsed.key))
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

/// Resolves allowlisted credential bindings into header values via the
/// vault. Missing optional secrets are skipped; missing required secrets
/// fail the call. Error messages carry only the header name, never the
/// secret value.
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

/// Decides whether a fetch may target private/loopback addresses.
///
/// When the daemon config already allows private targets, the request may
/// only opt *out*. Otherwise an explicit `allow_private_targets=true` request
/// is honored solely for loopback URLs, and only when the process-runner
/// sandbox policy grants host access -- private LAN targets stay blocked.
pub(crate) fn http_fetch_allows_private_targets_for_url(
    config_allow_private_targets: bool,
    process_runner_policy: &SandboxProcessRunnerPolicy,
    requested_allow_private_targets: Option<bool>,
    url: &Url,
) -> bool {
    if config_allow_private_targets {
        return requested_allow_private_targets.unwrap_or(true);
    }
    requested_allow_private_targets.unwrap_or(false)
        && process_runner_allows_host_access(process_runner_policy)
        && http_fetch_url_targets_loopback(url)
}

fn http_fetch_url_targets_loopback(url: &Url) -> bool {
    if !matches!(url.scheme(), "http" | "https") {
        return false;
    }
    let Some(host) = url.host_str().map(str::trim).filter(|host| !host.is_empty()) else {
        return false;
    };
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    host.trim_start_matches('[')
        .trim_end_matches(']')
        .parse::<IpAddr>()
        .map(|ip| ip.is_loopback())
        .unwrap_or(false)
}

/// Echoes the request headers into the tool output with values of
/// credential-looking headers replaced, so journaled outputs never persist
/// tokens the model supplied explicitly.
fn redacted_http_headers(headers: &[(String, String)]) -> Vec<serde_json::Value> {
    headers
        .iter()
        .map(|(name, value)| {
            // Header names are normalized to lowercase at parse time, so
            // plain substring checks are case-insensitive in effect.
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
    // Length-prefix every variable-size field before hashing so distinct
    // (input, output, error) triples can never produce colliding digests by
    // shifting bytes across field boundaries.
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
            execution_manifest: None,
        },
    }
}

/// Body text prepared for the model plus the format tag describing how it
/// was derived (`plain_text`, `html_text`, or `html_raw`).
struct HttpFetchModelBody {
    body_text: String,
    format: &'static str,
}

fn http_fetch_model_body_text(content_type: &str, raw_body_text: &str) -> HttpFetchModelBody {
    if !is_html_content_type(content_type) {
        return HttpFetchModelBody { body_text: raw_body_text.to_owned(), format: "plain_text" };
    }

    let extracted = extract_html_visible_text(raw_body_text);
    if extracted.is_empty() {
        return HttpFetchModelBody { body_text: raw_body_text.to_owned(), format: "html_raw" };
    }

    HttpFetchModelBody { body_text: extracted, format: "html_text" }
}

fn is_html_content_type(content_type: &str) -> bool {
    content_type.split(';').next().unwrap_or_default().trim().eq_ignore_ascii_case("text/html")
}

/// Minimal hand-rolled extraction of human-visible text from HTML.
///
/// Deliberately not a full parser (no new dependency for this): it tracks a
/// stack of skip-tags ([`HTTP_FETCH_HTML_SKIP_TAGS`]) whose content is
/// dropped, inserts line boundaries at block-level tags, and decodes basic
/// entities. Malformed markup degrades to truncation, never to a panic.
fn extract_html_visible_text(html: &str) -> String {
    let mut output = String::new();
    let mut index = 0_usize;
    let mut skipped_tags = Vec::<String>::new();

    while index < html.len() {
        let Some(relative_tag_start) = html[index..].find('<') else {
            if skipped_tags.is_empty() {
                append_html_text(&mut output, &html[index..]);
            }
            break;
        };
        let tag_start = index.saturating_add(relative_tag_start);
        if skipped_tags.is_empty() {
            append_html_text(&mut output, &html[index..tag_start]);
        }
        if html[tag_start..].starts_with("<!--") {
            let Some(relative_comment_end) = html[tag_start + 4..].find("-->") else {
                break;
            };
            index =
                tag_start.saturating_add(4).saturating_add(relative_comment_end).saturating_add(3);
            continue;
        }
        let Some(relative_tag_end) = html[tag_start..].find('>') else {
            break;
        };
        let tag_end = tag_start.saturating_add(relative_tag_end);
        let tag_source = &html[tag_start + 1..tag_end];
        handle_html_tag(tag_source, &mut output, &mut skipped_tags);
        index = tag_end.saturating_add(1);
    }

    normalize_html_text(output.as_str())
}

fn handle_html_tag(tag_source: &str, output: &mut String, skipped_tags: &mut Vec<String>) {
    let trimmed = tag_source.trim();
    if trimmed.starts_with("!--") || trimmed.starts_with('!') || trimmed.starts_with('?') {
        return;
    }
    let closing = trimmed.starts_with('/');
    let self_closing = trimmed.ends_with('/');
    let Some(tag_name) = html_tag_name(trimmed) else {
        return;
    };

    if closing {
        if let Some(position) = skipped_tags.iter().rposition(|tag| tag == &tag_name) {
            skipped_tags.truncate(position);
        }
        if skipped_tags.is_empty() && html_tag_adds_boundary(tag_name.as_str()) {
            append_html_boundary(output);
        }
        return;
    }

    if HTTP_FETCH_HTML_SKIP_TAGS.iter().any(|tag| *tag == tag_name) && !self_closing {
        skipped_tags.push(tag_name);
        return;
    }
    if skipped_tags.is_empty() && html_tag_adds_boundary(tag_name.as_str()) {
        append_html_boundary(output);
    }
}

fn html_tag_name(tag_source: &str) -> Option<String> {
    let source = tag_source.trim_start_matches('/').trim_start();
    let name = source
        .split(|ch: char| ch.is_ascii_whitespace() || ch == '/' || ch == '>')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    (!name.is_empty()).then_some(name)
}

fn html_tag_adds_boundary(tag_name: &str) -> bool {
    matches!(
        tag_name,
        "address"
            | "article"
            | "aside"
            | "blockquote"
            | "br"
            | "dd"
            | "div"
            | "dl"
            | "dt"
            | "fieldset"
            | "figcaption"
            | "figure"
            | "footer"
            | "form"
            | "h1"
            | "h2"
            | "h3"
            | "h4"
            | "h5"
            | "h6"
            | "header"
            | "hr"
            | "li"
            | "main"
            | "nav"
            | "ol"
            | "p"
            | "pre"
            | "section"
            | "table"
            | "tbody"
            | "td"
            | "tfoot"
            | "th"
            | "thead"
            | "tr"
            | "ul"
    )
}

fn append_html_text(output: &mut String, text: &str) {
    let decoded = decode_basic_html_entities(text);
    let collapsed = decoded.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.is_empty() {
        return;
    }
    if !output.is_empty() && !output.chars().last().is_some_and(char::is_whitespace) {
        output.push(' ');
    }
    output.push_str(collapsed.as_str());
}

fn append_html_boundary(output: &mut String) {
    if output.trim().is_empty() {
        return;
    }
    if !output.ends_with('\n') {
        output.push('\n');
    }
}

fn normalize_html_text(text: &str) -> String {
    text.lines().map(str::trim).filter(|line| !line.is_empty()).collect::<Vec<_>>().join("\n")
}

fn decode_basic_html_entities(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    let mut index = 0_usize;

    while index < text.len() {
        let Some(relative_ampersand) = text[index..].find('&') else {
            output.push_str(&text[index..]);
            break;
        };
        let ampersand = index.saturating_add(relative_ampersand);
        output.push_str(&text[index..ampersand]);
        let entity_start = ampersand.saturating_add(1);
        let Some(relative_semicolon) = text[entity_start..].find(';') else {
            output.push('&');
            index = entity_start;
            continue;
        };
        let semicolon = entity_start.saturating_add(relative_semicolon);
        let entity = &text[entity_start..semicolon];
        // Real entities are short; a distant stray ';' would otherwise make
        // this swallow a long span of legitimate text as one bogus entity.
        if entity.len() > 32 {
            output.push('&');
            index = entity_start;
            continue;
        }
        if let Some(decoded) = decode_basic_html_entity(entity) {
            output.push_str(decoded.as_str());
        } else {
            output.push('&');
            output.push_str(entity);
            output.push(';');
        }
        index = semicolon.saturating_add(1);
    }

    output
}

fn decode_basic_html_entity(entity: &str) -> Option<String> {
    match entity {
        "amp" => Some("&".to_owned()),
        "apos" => Some("'".to_owned()),
        "gt" => Some(">".to_owned()),
        "lt" => Some("<".to_owned()),
        "nbsp" => Some(" ".to_owned()),
        "quot" => Some("\"".to_owned()),
        value if value.strip_prefix("#x").or_else(|| value.strip_prefix("#X")).is_some() => {
            let digits = value.strip_prefix("#x").or_else(|| value.strip_prefix("#X"))?;
            decode_numeric_html_entity(digits, 16)
        }
        value if value.starts_with('#') => decode_numeric_html_entity(&value[1..], 10),
        _ => None,
    }
}

fn decode_numeric_html_entity(digits: &str, radix: u32) -> Option<String> {
    let value = u32::from_str_radix(digits, radix).ok()?;
    let character = char::from_u32(value)?;
    Some(character.to_string())
}

/// Redacted body text plus the safety-scan metadata attached to the output.
struct HttpFetchBodyExport {
    body_text: String,
    model_truncated: bool,
    original_bytes: usize,
    sha256: String,
    safety_json: Value,
}

/// Runs the safety redaction scan over the model-bound body text. HTTP
/// responses are always labeled `ExternalUntrusted`, so secret-looking
/// values are redacted before the model or journal sees them.
fn export_http_fetch_body(body_text: &str) -> HttpFetchBodyExport {
    let outcome = redact_text_for_export(
        body_text,
        SafetySourceKind::HttpFetch,
        SafetyContentKind::HttpResponse,
        TrustLabel::ExternalUntrusted,
    );
    let sha256 = sha256_hex(outcome.redacted_text.as_bytes());
    let original_bytes = outcome.redacted_text.len();
    let (body_text, model_truncated) =
        bounded_http_fetch_model_body_text(outcome.redacted_text.as_str());
    HttpFetchBodyExport {
        body_text,
        model_truncated,
        original_bytes,
        sha256,
        safety_json: json!({
            "trust_label": outcome.scan.trust_label.as_str(),
            "action": outcome.scan.recommended_action.as_str(),
            "findings": outcome.scan.finding_codes(),
            "redacted": outcome.redacted,
        }),
    }
}

fn bounded_http_fetch_model_body_text(body_text: &str) -> (String, bool) {
    if body_text.len() <= HTTP_FETCH_MODEL_BODY_INLINE_BYTES {
        return (body_text.to_owned(), false);
    }
    let sha256 = sha256_hex(body_text.as_bytes());
    let head = http_fetch_text_prefix(body_text, HTTP_FETCH_MODEL_BODY_HEAD_BYTES);
    let tail = http_fetch_text_suffix(body_text, HTTP_FETCH_MODEL_BODY_TAIL_BYTES);
    (
        format!(
            "{head}\n\n<http.fetch body omitted: original_bytes={} sha256={}>\n\n{tail}",
            body_text.len(),
            sha256
        ),
        true,
    )
}

fn http_fetch_text_prefix(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_owned();
    }
    text.char_indices()
        .take_while(|(index, character)| index.saturating_add(character.len_utf8()) <= max_bytes)
        .map(|(_, character)| character)
        .collect()
}

fn http_fetch_text_suffix(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_owned();
    }
    let mut start = text.len().saturating_sub(max_bytes);
    while start < text.len() && !text.is_char_boundary(start) {
        start += 1;
    }
    text[start..].to_owned()
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{export_http_fetch_body, http_fetch_model_body_text, parse_credential_bindings};

    #[test]
    fn http_fetch_export_redacts_sensitive_body_text() {
        let exported = export_http_fetch_body("Authorization: Bearer super-secret-token-value");
        assert_eq!(exported.body_text, "Authorization: [REDACTED_SECRET]");
        assert!(!exported.model_truncated);
        assert_eq!(exported.original_bytes, exported.body_text.len());
        assert_eq!(exported.sha256.len(), 64);
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

    #[test]
    fn http_fetch_export_bounds_large_body_text() {
        let body =
            format!("{}MIDDLE_SHOULD_BE_OMITTED{}", "head\n".repeat(3_000), "tail\n".repeat(3_000));

        let exported = export_http_fetch_body(body.as_str());

        assert!(exported.model_truncated);
        assert_eq!(exported.original_bytes, body.len());
        assert_eq!(exported.sha256.len(), 64);
        assert!(exported.body_text.len() < 9 * 1024, "bounded body should stay compact");
        assert!(exported.body_text.contains("head"));
        assert!(exported.body_text.contains("tail"));
        assert!(exported.body_text.contains("http.fetch body omitted"));
        assert!(!exported.body_text.contains("MIDDLE_SHOULD_BE_OMITTED"));
    }

    #[test]
    fn http_fetch_html_body_text_skips_asset_markup_and_extracts_visible_text() {
        let html = r#"<!doctype html>
            <html>
                <head>
                    <title>Preload bundle</title>
                    <link rel="preload" href="/assets/app.js">
                    <script>window.__BOOT="secret-looking-token";</script>
                    <style>body { display: grid; }</style>
                </head>
                <body>
                    <main>
                        <h1>Release notes</h1>
                        <p>Node.js v22.18.0 &amp; v24.9.0 are available.</p>
                    </main>
                </body>
            </html>"#;

        let model_body = http_fetch_model_body_text("text/html", html);

        assert_eq!(model_body.format, "html_text");
        assert!(model_body.body_text.contains("Release notes"));
        assert!(model_body.body_text.contains("Node.js v22.18.0 & v24.9.0 are available."));
        assert!(!model_body.body_text.contains("Preload bundle"));
        assert!(!model_body.body_text.contains("window.__BOOT"));
        assert!(!model_body.body_text.contains("display: grid"));
    }

    #[test]
    fn http_fetch_non_html_body_text_keeps_plain_text_format() {
        let model_body = http_fetch_model_body_text("application/json", r#"{"ok":true}"#);

        assert_eq!(model_body.format, "plain_text");
        assert_eq!(model_body.body_text, r#"{"ok":true}"#);
    }

    #[test]
    fn http_fetch_credential_bindings_require_configured_vault_ref_allowlist() {
        let payload = json!({
            "credential_bindings": [
                {
                    "header_name": "authorization",
                    "secret_ref": {"kind": "vault", "vault_ref": "global/github_token"},
                    "required": true
                }
            ]
        });
        let payload = payload.as_object().expect("payload should be an object");

        let error = parse_credential_bindings(payload, &[])
            .expect_err("credential binding must fail closed without configured vault refs");

        assert!(error.contains("allowed_credential_vault_refs"));
    }

    #[test]
    fn http_fetch_credential_bindings_reject_non_vault_sources() {
        let payload = json!({
            "credential_bindings": [
                {
                    "header_name": "authorization",
                    "secret_ref": {"kind": "env", "variable": "PALYRA_SECRET"},
                    "required": true
                }
            ]
        });
        let payload = payload.as_object().expect("payload should be an object");

        let error = parse_credential_bindings(payload, &["global/github_token".to_owned()])
            .expect_err("env-backed credential binding must be rejected");

        assert!(error.contains("must use a vault-backed secret_ref"));
    }

    #[test]
    fn http_fetch_credential_bindings_reject_unlisted_vault_refs() {
        let payload = json!({
            "credential_bindings": [
                {
                    "header_name": "authorization",
                    "secret_ref": {"kind": "vault", "vault_ref": "global/unlisted_token"},
                    "required": true
                }
            ]
        });
        let payload = payload.as_object().expect("payload should be an object");

        let error = parse_credential_bindings(payload, &["global/github_token".to_owned()])
            .expect_err("unlisted vault ref must be rejected");

        assert!(error.contains("is not allowed"));
    }

    #[test]
    fn http_fetch_credential_bindings_accept_allowed_vault_refs() {
        let payload = json!({
            "credential_bindings": [
                {
                    "header_name": "authorization",
                    "secret_ref": {"kind": "vault", "vault_ref": "global/github_token"},
                    "required": true
                }
            ]
        });
        let payload = payload.as_object().expect("payload should be an object");

        let bindings = parse_credential_bindings(payload, &["global/github_token".to_owned()])
            .expect("allowlisted vault ref should parse");

        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].header_name, "authorization");
    }
}
