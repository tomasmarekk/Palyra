use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
    time::Instant,
};

use axum::{
    extract::{ConnectInfo, Request, State},
    http::{
        header::{CACHE_CONTROL, SET_COOKIE},
        HeaderMap, HeaderName, HeaderValue, Method, StatusCode,
    },
    middleware::Next,
    response::Response,
};

use crate::observability::CorrelationSnapshot as ObservabilityCorrelationSnapshot;
use crate::{
    app::state::{AdminRateLimitEntry, AppState, CanvasRateLimitEntry, RemoteAdminAccessAttempt},
    classify_console_mutation_failure, refresh_console_session_cookie, runtime_status_response,
    sha256_hex, unix_ms_now, ADMIN_RATE_LIMIT_LOOPBACK_MAX_REQUESTS_PER_WINDOW,
    ADMIN_RATE_LIMIT_MAX_IP_BUCKETS, ADMIN_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW,
    ADMIN_RATE_LIMIT_WINDOW_MS, CANVAS_RATE_LIMIT_MAX_IP_BUCKETS,
    CANVAS_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW, CANVAS_RATE_LIMIT_WINDOW_MS,
};

pub(crate) async fn admin_console_security_headers_middleware(
    request: Request,
    next: Next,
) -> Response {
    let mut response = next.run(request).await;
    apply_admin_console_security_headers(response.headers_mut());
    response
}

pub(crate) fn apply_admin_console_security_headers(headers: &mut HeaderMap) {
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(HeaderName::from_static("x-frame-options"), HeaderValue::from_static("DENY"));
    headers.insert(
        HeaderName::from_static("content-security-policy"),
        HeaderValue::from_static("frame-ancestors 'none'"),
    );
    headers.insert(
        HeaderName::from_static("referrer-policy"),
        HeaderValue::from_static("no-referrer"),
    );
}

pub(crate) async fn console_observability_middleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let response = next.run(request).await;
    if !path.starts_with("/console/v1/")
        || matches!(method, Method::GET | Method::HEAD | Method::OPTIONS)
    {
        return response;
    }
    let observed_at_unix_ms = unix_ms_now().unwrap_or_default();
    let operation = format!("dashboard.mutation {} {}", method, path);
    let success = response.status().is_success();
    let failure_class = classify_console_mutation_failure(response.status());
    let message = if success {
        "ok".to_owned()
    } else {
        format!("request failed with http {}", response.status().as_u16())
    };
    state.observability.record_dashboard_mutation_result(
        success,
        operation,
        failure_class,
        message,
        observed_at_unix_ms,
        ObservabilityCorrelationSnapshot::default(),
    );
    response
}

pub(crate) async fn console_session_cookie_refresh_middleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let request_headers = request.headers().clone();
    let mut response = next.run(request).await;
    if response.headers().contains_key(SET_COOKIE)
        || matches!(response.status(), StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN)
    {
        return response;
    }
    match refresh_console_session_cookie(&state, &request_headers) {
        Ok(Some(cookie)) => {
            response.headers_mut().append(SET_COOKIE, cookie);
            response
        }
        Ok(None) => response,
        Err(error_response) => error_response,
    }
}

pub(crate) async fn canvas_security_headers_middleware(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        HeaderName::from_static("referrer-policy"),
        HeaderValue::from_static("no-referrer"),
    );
    response
}

fn consume_admin_rate_limit(state: &AppState, remote_addr: SocketAddr) -> bool {
    consume_admin_rate_limit_with_now(&state.admin_rate_limit, remote_addr.ip(), Instant::now())
}

pub(crate) fn is_loopback_admin_rate_limit_exempt(
    remote_ip: IpAddr,
    method: &Method,
    path: &str,
) -> bool {
    if !remote_ip.is_loopback() {
        return false;
    }
    matches!(
        (method, path),
        (&Method::POST, "/console/v1/auth/login")
            | (&Method::GET, "/console/v1/auth/session")
            | (&Method::POST, "/console/v1/auth/browser-handoff")
            | (&Method::GET, "/console/v1/auth/browser-handoff/consume")
            | (&Method::POST, "/console/v1/auth/browser-handoff/session")
    )
}

fn admin_rate_limit_budget(remote_ip: IpAddr) -> u32 {
    if remote_ip.is_loopback() {
        return ADMIN_RATE_LIMIT_LOOPBACK_MAX_REQUESTS_PER_WINDOW;
    }
    ADMIN_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW
}

pub(crate) fn consume_admin_rate_limit_with_now(
    buckets: &Mutex<HashMap<IpAddr, AdminRateLimitEntry>>,
    remote_ip: IpAddr,
    now: Instant,
) -> bool {
    let mut buckets = match buckets.lock() {
        Ok(guard) => guard,
        Err(_) => return false,
    };
    if !buckets.contains_key(&remote_ip) && buckets.len() >= ADMIN_RATE_LIMIT_MAX_IP_BUCKETS {
        buckets.retain(|_, entry| {
            now.duration_since(entry.window_started_at).as_millis() as u64
                <= ADMIN_RATE_LIMIT_WINDOW_MS
        });
        if buckets.len() >= ADMIN_RATE_LIMIT_MAX_IP_BUCKETS {
            let evicted_ip =
                buckets.iter().min_by_key(|(_, entry)| entry.window_started_at).map(|(ip, _)| *ip);
            let Some(evicted_ip) = evicted_ip else {
                return false;
            };
            buckets.remove(&evicted_ip);
        }
    }
    let entry = buckets
        .entry(remote_ip)
        .or_insert(AdminRateLimitEntry { window_started_at: now, requests_in_window: 0 });
    if now.duration_since(entry.window_started_at).as_millis() as u64 > ADMIN_RATE_LIMIT_WINDOW_MS {
        entry.window_started_at = now;
        entry.requests_in_window = 0;
    }
    if entry.requests_in_window >= admin_rate_limit_budget(remote_ip) {
        return false;
    }
    entry.requests_in_window = entry.requests_in_window.saturating_add(1);
    true
}

pub(crate) async fn admin_rate_limit_middleware(
    State(state): State<AppState>,
    ConnectInfo(remote_addr): ConnectInfo<SocketAddr>,
    request: Request,
    next: Next,
) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    if !is_loopback_admin_rate_limit_exempt(remote_addr.ip(), &method, path.as_str())
        && !consume_admin_rate_limit(&state, remote_addr)
    {
        state.runtime.record_denied();
        let response = runtime_status_response(tonic::Status::resource_exhausted(format!(
            "admin API rate limit exceeded for {}",
            remote_addr.ip()
        )));
        record_remote_admin_access_attempt(
            &state,
            remote_addr,
            method.as_str(),
            path.as_str(),
            response.status(),
        );
        return response;
    }
    let response = next.run(request).await;
    record_remote_admin_access_attempt(
        &state,
        remote_addr,
        method.as_str(),
        path.as_str(),
        response.status(),
    );
    response
}

fn record_remote_admin_access_attempt(
    state: &AppState,
    remote_addr: SocketAddr,
    method: &str,
    path: &str,
    status: StatusCode,
) {
    if remote_addr.ip().is_loopback() {
        return;
    }
    let observed_at_unix_ms = unix_ms_now().unwrap_or_default();
    let attempt = RemoteAdminAccessAttempt {
        observed_at_unix_ms,
        remote_ip_fingerprint: fingerprint_remote_ip(remote_addr.ip()),
        method: method.to_owned(),
        path: path.to_owned(),
        status_code: status.as_u16(),
        outcome: admin_access_outcome(status).to_owned(),
    };
    let mut slot = lock_remote_admin_access(&state.remote_admin_access);
    *slot = Some(attempt);
}

fn fingerprint_remote_ip(ip: IpAddr) -> String {
    let digest = sha256_hex(ip.to_string().as_bytes());
    let prefix = &digest[..16];
    format!("sha256:{prefix}")
}

fn admin_access_outcome(status: StatusCode) -> &'static str {
    if status == StatusCode::TOO_MANY_REQUESTS {
        "rate_limited"
    } else if status.is_success() || status.is_redirection() {
        "allowed"
    } else if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        "denied"
    } else {
        "error"
    }
}

pub(crate) fn lock_remote_admin_access<'a>(
    slot: &'a Arc<Mutex<Option<RemoteAdminAccessAttempt>>>,
) -> std::sync::MutexGuard<'a, Option<RemoteAdminAccessAttempt>> {
    match slot.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::warn!("remote admin access record lock poisoned; recovering");
            poisoned.into_inner()
        }
    }
}

fn consume_canvas_rate_limit(state: &AppState, remote_addr: SocketAddr) -> bool {
    consume_canvas_rate_limit_with_now(&state.canvas_rate_limit, remote_addr.ip(), Instant::now())
}

pub(crate) fn consume_canvas_rate_limit_with_now(
    buckets: &Mutex<HashMap<IpAddr, CanvasRateLimitEntry>>,
    remote_ip: IpAddr,
    now: Instant,
) -> bool {
    let mut buckets = match buckets.lock() {
        Ok(guard) => guard,
        Err(_) => return false,
    };
    if !buckets.contains_key(&remote_ip) && buckets.len() >= CANVAS_RATE_LIMIT_MAX_IP_BUCKETS {
        buckets.retain(|_, entry| {
            now.duration_since(entry.window_started_at).as_millis() as u64
                <= CANVAS_RATE_LIMIT_WINDOW_MS
        });
        if buckets.len() >= CANVAS_RATE_LIMIT_MAX_IP_BUCKETS {
            let evicted_ip =
                buckets.iter().min_by_key(|(_, entry)| entry.window_started_at).map(|(ip, _)| *ip);
            let Some(evicted_ip) = evicted_ip else {
                return false;
            };
            buckets.remove(&evicted_ip);
        }
    }
    let entry = buckets
        .entry(remote_ip)
        .or_insert(CanvasRateLimitEntry { window_started_at: now, requests_in_window: 0 });
    if now.duration_since(entry.window_started_at).as_millis() as u64 > CANVAS_RATE_LIMIT_WINDOW_MS
    {
        entry.window_started_at = now;
        entry.requests_in_window = 0;
    }
    if entry.requests_in_window >= CANVAS_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW {
        return false;
    }
    entry.requests_in_window = entry.requests_in_window.saturating_add(1);
    true
}

pub(crate) async fn canvas_rate_limit_middleware(
    State(state): State<AppState>,
    ConnectInfo(remote_addr): ConnectInfo<SocketAddr>,
    request: Request,
    next: Next,
) -> Response {
    if !consume_canvas_rate_limit(&state, remote_addr) {
        state.runtime.record_denied();
        return runtime_status_response(tonic::Status::resource_exhausted(format!(
            "canvas API rate limit exceeded for {}",
            remote_addr.ip()
        )));
    }
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use std::{net::IpAddr, str::FromStr};

    use axum::http::Method;

    use super::is_loopback_admin_rate_limit_exempt;

    #[test]
    fn admin_rate_limit_exempts_loopback_console_auth_bootstrap_paths() {
        let loopback_ip = IpAddr::from_str("127.0.0.1").expect("IP literal should parse");
        for (method, path) in [
            (Method::POST, "/console/v1/auth/login"),
            (Method::GET, "/console/v1/auth/session"),
            (Method::POST, "/console/v1/auth/browser-handoff"),
            (Method::GET, "/console/v1/auth/browser-handoff/consume"),
            (Method::POST, "/console/v1/auth/browser-handoff/session"),
        ] {
            assert!(
                is_loopback_admin_rate_limit_exempt(loopback_ip, &method, path),
                "loopback auth bootstrap path should bypass the generic admin rate limit: {} {}",
                method,
                path
            );
        }

        assert!(
            !is_loopback_admin_rate_limit_exempt(
                loopback_ip,
                &Method::GET,
                "/console/v1/diagnostics"
            ),
            "non-auth console routes must remain rate limited on loopback"
        );
        assert!(
            !is_loopback_admin_rate_limit_exempt(
                IpAddr::from_str("203.0.113.10").expect("IP literal should parse"),
                &Method::POST,
                "/console/v1/auth/browser-handoff"
            ),
            "remote clients must not inherit the loopback auth bootstrap exemption"
        );
    }
}
