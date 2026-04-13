use std::{
    io::Read,
    net::TcpListener,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use super::super::snapshot::{build_dashboard_open_url, OverallStatus};
use super::super::build_snapshot_from_inputs;
use super::support::{build_test_control_center, write_json_response, TempFixtureDir};

#[tokio::test(flavor = "current_thread")]
async fn snapshot_console_reads_reuse_session_cookie_across_refreshes() {
    let fixture = TempFixtureDir::new();
    let listener = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
    let port = listener.local_addr().expect("listener address should resolve").port();
    let login_count = Arc::new(AtomicU64::new(0));
    let login_count_server = Arc::clone(&login_count);

    let server = std::thread::spawn(move || {
        for _ in 0..6 {
            let (mut stream, _) = listener.accept().expect("listener should accept request");
            let mut buffer = [0_u8; 8192];
            let read = stream.read(&mut buffer).expect("request should be readable");
            let request = String::from_utf8_lossy(&buffer[..read]);
            let request_line = request.lines().next().unwrap_or_default().to_owned();
            let has_cookie = request.contains("palyra_console_session=session-desktop");

            if request_line.starts_with("GET /healthz ") {
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"status":"ok","version":"0.1.0","git_hash":"desktop-test","uptime_seconds":42}"#,
                    &[],
                );
                continue;
            }
            if request_line.starts_with("GET /console/v1/diagnostics ") {
                if has_cookie {
                    write_json_response(
                        &mut stream,
                        "200 OK",
                        r#"{"generated_at_unix_ms":1700000000000,"observability":{},"errors":[]}"#,
                        &[],
                    );
                } else {
                    write_json_response(
                        &mut stream,
                        "403 Forbidden",
                        r#"{"error":"console session cookie is missing"}"#,
                        &[],
                    );
                }
                continue;
            }
            if request_line.starts_with("POST /console/v1/auth/login ") {
                login_count_server.fetch_add(1, Ordering::Relaxed);
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"principal":"admin:desktop-control-center","device_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","csrf_token":"csrf-desktop","issued_at_unix_ms":1,"expires_at_unix_ms":2}"#,
                    &["Set-Cookie: palyra_console_session=session-desktop; Path=/; HttpOnly; SameSite=Strict"],
                );
                continue;
            }
            if request_line.starts_with("GET /console/v1/channels/discord%3Adefault ") {
                assert!(
                    has_cookie,
                    "Discord status fetch should reuse the authenticated console session"
                );
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"connector":{"connector_id":"discord:default","enabled":false,"readiness":"disabled","liveness":"stopped"}}"#,
                    &[],
                );
                continue;
            }
            panic!("unexpected desktop snapshot request: {request_line}");
        }
    });

    let mut control_center = build_test_control_center(fixture.path());
    control_center.persisted.browser_service_enabled = false;
    control_center.runtime.gateway_admin_port = port;

    let first = build_snapshot_from_inputs(control_center.capture_snapshot_inputs())
        .await
        .expect("first snapshot should build");
    let second = build_snapshot_from_inputs(control_center.capture_snapshot_inputs())
        .await
        .expect("second snapshot should build");

    assert_eq!(first.overall_status, OverallStatus::Healthy);
    assert_eq!(second.overall_status, OverallStatus::Healthy);
    assert_eq!(login_count.load(Ordering::Relaxed), 1);

    server.join().expect("test server thread should exit");
}

#[tokio::test(flavor = "current_thread")]
async fn dashboard_open_url_reuses_cached_console_session_for_local_browser_handoff() {
    let fixture = TempFixtureDir::new();
    let listener = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
    let port = listener.local_addr().expect("listener address should resolve").port();

    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("listener should accept request");
        let mut buffer = [0_u8; 8192];
        let read = stream.read(&mut buffer).expect("request should be readable");
        let request = String::from_utf8_lossy(&buffer[..read]);
        let request_line = request.lines().next().unwrap_or_default().to_owned();
        assert!(
            request_line.starts_with("POST /console/v1/auth/browser-handoff "),
            "dashboard open should go straight to browser handoff when the console session is cached: {request_line}"
        );
        assert!(
            request.contains("x-palyra-csrf-token: csrf-dashboard"),
            "dashboard handoff must reuse the cached CSRF token"
        );
        write_json_response(
            &mut stream,
            "200 OK",
            r#"{"handoff_url":"http://127.0.0.1:7142/console/v1/auth/browser-handoff/consume?token=dashboard-token","expires_at_unix_ms":1700000000000}"#,
            &[],
        );
    });

    let control_center = build_test_control_center(fixture.path());
    let now = super::super::unix_ms_now();
    if let Ok(mut session_cache) = control_center.console_session_cache.lock() {
        *session_cache = Some(super::super::supervisor::ConsoleSessionCache {
            session: palyra_control_plane::ConsoleSession {
                principal: "admin:desktop-control-center".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                channel: None,
                profile: None,
                csrf_token: "csrf-dashboard".to_owned(),
                issued_at_unix_ms: now.saturating_sub(10_000),
                expires_at_unix_ms: now.saturating_add(120_000),
            },
        });
    }

    let mut dashboard_inputs = control_center.capture_dashboard_open_inputs();
    dashboard_inputs.runtime.gateway_admin_port = port;
    let handoff_url = build_dashboard_open_url(
        dashboard_inputs,
        "http://127.0.0.1:7142/#/control/overview",
        "local",
    )
    .await
    .expect("dashboard handoff URL should build from cached session");

    assert_eq!(
        handoff_url,
        "http://127.0.0.1:7142/console/v1/auth/browser-handoff/consume?token=dashboard-token"
    );

    server.join().expect("test server thread should exit");
}

#[tokio::test(flavor = "current_thread")]
async fn snapshot_keeps_launcher_status_stable_when_optional_diagnostics_fetch_fails() {
    let fixture = TempFixtureDir::new();
    let listener = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
    let port = listener.local_addr().expect("listener address should resolve").port();

    let server = std::thread::spawn(move || {
        listener.set_nonblocking(true).expect("listener should support nonblocking mode");
        let mut idle_since = Instant::now();
        loop {
            let (mut stream, _) = match listener.accept() {
                Ok(connection) => {
                    idle_since = Instant::now();
                    connection
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if idle_since.elapsed() >= Duration::from_millis(250) {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                }
                Err(error) => panic!("listener should accept request: {error}"),
            };
            stream
                .set_nonblocking(false)
                .expect("accepted stream should switch back to blocking mode");
            let mut buffer = [0_u8; 8192];
            let read = stream.read(&mut buffer).expect("request should be readable");
            let request = String::from_utf8_lossy(&buffer[..read]);
            let request_line = request.lines().next().unwrap_or_default().to_owned();
            let has_cookie = request.contains("palyra_console_session=session-desktop");

            if request_line.starts_with("GET /healthz ") {
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"status":"ok","version":"0.1.0","git_hash":"desktop-test","uptime_seconds":42}"#,
                    &[],
                );
                continue;
            }
            if request_line.starts_with("GET /console/v1/diagnostics ") {
                if has_cookie {
                    write_json_response(
                        &mut stream,
                        "429 Too Many Requests",
                        r#"{"error":"admin API rate limit exceeded for 127.0.0.1"}"#,
                        &[],
                    );
                } else {
                    write_json_response(
                        &mut stream,
                        "403 Forbidden",
                        r#"{"error":"console session cookie is missing"}"#,
                        &[],
                    );
                }
                continue;
            }
            if request_line.starts_with("POST /console/v1/auth/login ") {
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"principal":"admin:desktop-control-center","device_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","csrf_token":"csrf-desktop","issued_at_unix_ms":1,"expires_at_unix_ms":2}"#,
                    &["Set-Cookie: palyra_console_session=session-desktop; Path=/; HttpOnly; SameSite=Strict"],
                );
                continue;
            }
            if request_line.starts_with("GET /console/v1/channels/discord%3Adefault ") {
                assert!(
                    has_cookie,
                    "Discord status fetch should reuse the authenticated console session"
                );
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"connector":{"connector_id":"discord:default","enabled":false,"readiness":"disabled","liveness":"stopped"}}"#,
                    &[],
                );
                continue;
            }
            panic!("unexpected diagnostics stability request: {request_line}");
        }
    });

    let mut control_center = build_test_control_center(fixture.path());
    control_center.persisted.browser_service_enabled = false;
    control_center.runtime.gateway_admin_port = port;

    let snapshot = build_snapshot_from_inputs(control_center.capture_snapshot_inputs())
        .await
        .expect("snapshot should build even when diagnostics fetch fails");

    assert_eq!(snapshot.overall_status, OverallStatus::Healthy);
    assert!(
        snapshot
            .warnings
            .iter()
            .any(|entry| entry.contains("diagnostics payload") && entry.contains("429")),
        "diagnostics failure warning should be surfaced: {:?}",
        snapshot.warnings
    );

    server.join().expect("test server thread should exit");
}
