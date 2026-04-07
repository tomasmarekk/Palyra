use crate::*;

#[derive(Debug)]
pub(crate) struct ChromiumActionOutcome {
    pub(crate) success: bool,
    pub(crate) outcome: String,
    pub(crate) error: String,
    pub(crate) attempts: u32,
}

#[derive(Debug)]
pub(crate) struct ChromiumScrollOutcome {
    pub(crate) success: bool,
    pub(crate) scroll_x: i64,
    pub(crate) scroll_y: i64,
    pub(crate) error: String,
}

#[derive(Debug)]
pub(crate) struct ChromiumWaitOutcome {
    pub(crate) success: bool,
    pub(crate) matched_selector: String,
    pub(crate) matched_text: String,
    pub(crate) attempts: u32,
    pub(crate) waited_ms: u64,
    pub(crate) error: String,
}

#[derive(Debug)]
pub(crate) struct ChromiumObserveSnapshot {
    pub(crate) page_body: String,
    pub(crate) title: String,
    pub(crate) page_url: String,
}

#[derive(Debug, Clone)]
pub(crate) struct ChromiumNavigateParams {
    pub(crate) raw_url: String,
    pub(crate) timeout_ms: u64,
    pub(crate) allow_redirects: bool,
    pub(crate) max_redirects: u32,
    pub(crate) allow_private_targets: bool,
    pub(crate) max_response_bytes: u64,
    pub(crate) cookie_header: Option<String>,
}

fn clamp_chromium_snapshot(
    snapshot: ChromiumObserveSnapshot,
    max_response_bytes: u64,
    max_title_bytes: u64,
) -> ChromiumObserveSnapshot {
    ChromiumObserveSnapshot {
        page_body: truncate_utf8_bytes(snapshot.page_body.as_str(), max_response_bytes as usize),
        title: truncate_utf8_bytes(snapshot.title.as_str(), max_title_bytes as usize),
        page_url: snapshot.page_url,
    }
}

pub(crate) async fn run_chromium_blocking<T, F>(operation: &str, task: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, String> + Send + 'static,
{
    tokio::task::spawn_blocking(task)
        .await
        .map_err(|error| format!("{operation} task join failure: {error}"))?
}

#[derive(Debug)]
pub(crate) struct ChromiumSessionProxy {
    pub(crate) proxy_uri: String,
    pub(crate) shutdown_tx: Option<oneshot::Sender<()>>,
    pub(crate) task: tokio::task::JoinHandle<()>,
}

impl ChromiumSessionProxy {
    pub(crate) async fn spawn(allow_private_targets: bool) -> Result<Self, String> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|error| format!("failed to bind Chromium session SOCKS5 proxy: {error}"))?;
        let local_addr = listener.local_addr().map_err(|error| {
            format!("failed to resolve Chromium session SOCKS5 proxy addr: {error}")
        })?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(run_chromium_session_socks5_proxy(
            listener,
            allow_private_targets,
            shutdown_rx,
        ));
        Ok(Self {
            proxy_uri: format!("socks5://{local_addr}"),
            shutdown_tx: Some(shutdown_tx),
            task,
        })
    }
}

impl Drop for ChromiumSessionProxy {
    fn drop(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        self.task.abort();
    }
}

#[derive(Debug)]
pub(crate) enum Socks5TargetHost {
    Ip(IpAddr),
    Domain(String),
}

pub(crate) async fn run_chromium_session_socks5_proxy(
    listener: tokio::net::TcpListener,
    allow_private_targets: bool,
    mut shutdown_rx: oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            _ = &mut shutdown_rx => {
                break;
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, client_addr)) => {
                        tokio::spawn(async move {
                            if let Err(error) = handle_chromium_session_socks5_client(stream, allow_private_targets).await {
                                warn!(
                                    client_addr = %client_addr,
                                    error = error.as_str(),
                                    "Chromium session SOCKS5 proxy request failed"
                                );
                            }
                        });
                    }
                    Err(error) => {
                        warn!(error = %error, "Chromium session SOCKS5 proxy accept failed");
                        break;
                    }
                }
            }
        }
    }
}

pub(crate) fn socks5_reply(status: u8) -> [u8; 10] {
    [0x05, status, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
}

pub(crate) async fn read_socks5_target_host(
    stream: &mut tokio::net::TcpStream,
    atyp: u8,
) -> Result<Socks5TargetHost, String> {
    match atyp {
        0x01 => {
            let mut octets = [0_u8; 4];
            stream
                .read_exact(&mut octets)
                .await
                .map_err(|error| format!("failed to read SOCKS5 IPv4 target: {error}"))?;
            Ok(Socks5TargetHost::Ip(IpAddr::from(octets)))
        }
        0x04 => {
            let mut octets = [0_u8; 16];
            stream
                .read_exact(&mut octets)
                .await
                .map_err(|error| format!("failed to read SOCKS5 IPv6 target: {error}"))?;
            Ok(Socks5TargetHost::Ip(IpAddr::from(octets)))
        }
        0x03 => {
            let mut length = [0_u8; 1];
            stream
                .read_exact(&mut length)
                .await
                .map_err(|error| format!("failed to read SOCKS5 domain length: {error}"))?;
            let host_len = usize::from(length[0]);
            if host_len == 0 {
                return Err("SOCKS5 domain target must not be empty".to_owned());
            }
            let mut raw_host = vec![0_u8; host_len];
            stream
                .read_exact(raw_host.as_mut_slice())
                .await
                .map_err(|error| format!("failed to read SOCKS5 domain target: {error}"))?;
            let host = String::from_utf8(raw_host)
                .map_err(|error| format!("SOCKS5 domain target is not valid UTF-8: {error}"))?;
            if host.trim().is_empty() {
                return Err("SOCKS5 domain target must not be whitespace".to_owned());
            }
            Ok(Socks5TargetHost::Domain(host))
        }
        _ => Err(format!("unsupported SOCKS5 address type: {atyp}")),
    }
}

pub(crate) async fn handle_chromium_session_socks5_client(
    mut stream: tokio::net::TcpStream,
    allow_private_targets: bool,
) -> Result<(), String> {
    let mut greeting = [0_u8; 2];
    stream
        .read_exact(&mut greeting)
        .await
        .map_err(|error| format!("failed to read SOCKS5 greeting header: {error}"))?;
    if greeting[0] != 0x05 {
        return Err(format!("unsupported SOCKS5 version: {}", greeting[0]));
    }
    let methods_len = usize::from(greeting[1]);
    let mut methods = vec![0_u8; methods_len];
    stream
        .read_exact(methods.as_mut_slice())
        .await
        .map_err(|error| format!("failed to read SOCKS5 auth methods: {error}"))?;
    let supports_no_auth = methods.contains(&0x00);
    if !supports_no_auth {
        stream
            .write_all(&[0x05, 0xFF])
            .await
            .map_err(|error| format!("failed to reject unsupported SOCKS5 auth method: {error}"))?;
        return Err("SOCKS5 client does not support no-auth mode".to_owned());
    }
    stream
        .write_all(&[0x05, 0x00])
        .await
        .map_err(|error| format!("failed to acknowledge SOCKS5 auth method: {error}"))?;

    let mut request_header = [0_u8; 4];
    stream
        .read_exact(&mut request_header)
        .await
        .map_err(|error| format!("failed to read SOCKS5 request header: {error}"))?;
    if request_header[0] != 0x05 {
        return Err(format!("SOCKS5 request used unsupported version {}", request_header[0]));
    }
    if request_header[1] != 0x01 {
        let _ = stream.write_all(socks5_reply(0x07).as_slice()).await;
        return Err(format!("SOCKS5 proxy supports CONNECT only (command {})", request_header[1]));
    }

    let target_host = read_socks5_target_host(&mut stream, request_header[3]).await?;
    let mut raw_port = [0_u8; 2];
    stream
        .read_exact(&mut raw_port)
        .await
        .map_err(|error| format!("failed to read SOCKS5 target port: {error}"))?;
    let target_port = u16::from_be_bytes(raw_port);

    let (target_label, resolved) = match target_host {
        Socks5TargetHost::Ip(ip) => {
            let resolved = ResolvedHostAddresses::from_addresses(vec![ip])?;
            (ip.to_string(), resolved)
        }
        Socks5TargetHost::Domain(host) => {
            let resolved = resolve_host_addresses_async(host.as_str(), target_port).await?;
            (host, resolved)
        }
    };

    if let Err(error) =
        enforce_resolved_host_policy(target_label.as_str(), resolved.clone(), allow_private_targets)
    {
        let _ = stream.write_all(socks5_reply(0x02).as_slice()).await;
        return Err(error);
    }

    let connect_addr = SocketAddr::new(resolved.addresses[0], target_port);
    let mut upstream = match tokio::net::TcpStream::connect(connect_addr).await {
        Ok(value) => value,
        Err(error) => {
            let _ = stream.write_all(socks5_reply(0x04).as_slice()).await;
            return Err(format!(
                "SOCKS5 proxy failed to connect to {}:{} via {}: {error}",
                target_label, target_port, connect_addr
            ));
        }
    };

    stream
        .write_all(socks5_reply(0x00).as_slice())
        .await
        .map_err(|error| format!("failed to acknowledge SOCKS5 CONNECT success: {error}"))?;
    tokio::io::copy_bidirectional(&mut stream, &mut upstream)
        .await
        .map_err(|error| format!("SOCKS5 proxy stream relay failed: {error}"))?;
    Ok(())
}

pub(crate) fn build_chromium_launch_options<'a>(
    chromium: &ChromiumEngineConfig,
    profile_dir: &TempDir,
    proxy_server: Option<&'a str>,
) -> Result<headless_chrome::LaunchOptions<'a>, String> {
    let chromium_path = chromium.executable_path.clone();
    let mut chromium_args = vec![
        OsStr::new("--disable-dev-shm-usage"),
        OsStr::new("--disable-gpu"),
        OsStr::new("--no-first-run"),
        OsStr::new("--no-default-browser-check"),
        OsStr::new("--window-size=1280,800"),
        OsStr::new("--disable-blink-features=AutomationControlled"),
    ];
    if proxy_server.is_some() {
        chromium_args.push(OsStr::new("--proxy-bypass-list=<-loopback>"));
    }
    let mut builder = LaunchOptionsBuilder::default();
    builder
        .headless(true)
        .sandbox(true)
        .enable_gpu(false)
        .ignore_certificate_errors(false)
        .idle_browser_timeout(chromium.startup_timeout)
        .user_data_dir(Some(profile_dir.path().to_path_buf()))
        .args(chromium_args)
        .proxy_server(proxy_server);
    if let Some(path) = chromium_path {
        builder.path(Some(path));
    }
    builder.build().map_err(|error| format!("failed to build Chromium launch options: {error}"))
}

pub(crate) fn parse_chromium_remote_ip_literal(raw: &str) -> Option<IpAddr> {
    let trimmed = raw.trim().trim_start_matches('[').trim_end_matches(']');
    trimmed.parse::<IpAddr>().ok()
}

pub(crate) fn record_chromium_remote_ip_incident(
    remote_ip: Option<&str>,
    allow_private_targets: bool,
    security_incident: &Arc<std::sync::Mutex<Option<String>>>,
) {
    if allow_private_targets {
        return;
    }
    let Some(remote_ip_raw) = remote_ip else {
        return;
    };
    let Some(parsed_remote_ip) = parse_chromium_remote_ip_literal(remote_ip_raw) else {
        return;
    };
    if !netguard::is_private_or_local_ip(parsed_remote_ip) {
        return;
    }
    if let Ok(mut guard) = security_incident.lock() {
        if guard.is_none() {
            *guard = Some(format!(
                "remote response IP {} is private/local and violates browser session policy",
                parsed_remote_ip
            ));
        }
    }
}

pub(crate) fn configure_chromium_tab(
    tab: &Arc<HeadlessTab>,
    allow_private_targets: bool,
    timeout: Duration,
    security_incident: Arc<std::sync::Mutex<Option<String>>>,
) -> Result<(), String> {
    tab.set_default_timeout(timeout);
    tab.enable_fetch(None, Some(false))
        .map_err(|error| format!("failed to enable Chromium fetch interception: {error}"))?;
    let request_interceptor =
        Arc::new(move |_transport, _session_id, intercepted: Fetch::events::RequestPausedEvent| {
            let request_url = intercepted.params.request.url.as_str();
            if validate_target_url_blocking(request_url, allow_private_targets).is_ok() {
                RequestPausedDecision::Continue(None)
            } else {
                RequestPausedDecision::Fail(Fetch::FailRequest {
                    request_id: intercepted.params.request_id,
                    error_reason: Network::ErrorReason::BlockedByClient,
                })
            }
        });
    tab.enable_request_interception(request_interceptor).map_err(|error| {
        format!("failed to register Chromium request interception callback: {error}")
    })?;
    let remote_ip_guard = Arc::clone(&security_incident);
    tab.register_response_handling(
        CHROMIUM_REMOTE_IP_GUARD_HANDLER_NAME,
        Box::new(move |response, _fetch_body| {
            record_chromium_remote_ip_incident(
                response.response.remote_ip_address.as_deref(),
                allow_private_targets,
                &remote_ip_guard,
            );
        }),
    )
    .map_err(|error| format!("failed to register Chromium response guard callback: {error}"))?;
    Ok(())
}

pub(crate) fn chromium_new_tab_error_is_retryable(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    normalized.contains("event waited for never came")
        || (normalized.contains("websocket protocol error")
            && normalized.contains("sending after closing is not allowed"))
        || normalized.contains("underlying connection is closed")
}

pub(crate) fn create_configured_chromium_tab_with_retry(
    browser: &Arc<HeadlessBrowser>,
    allow_private_targets: bool,
    timeout: Duration,
    security_incident: Arc<std::sync::Mutex<Option<String>>>,
    failure_prefix: &str,
) -> Result<Arc<HeadlessTab>, String> {
    for attempt in 1..=CHROMIUM_NEW_TAB_MAX_ATTEMPTS {
        match browser.new_tab() {
            Ok(tab) => {
                configure_chromium_tab(&tab, allow_private_targets, timeout, security_incident)?;
                return Ok(tab);
            }
            Err(error) => {
                let error_message = error.to_string();
                if attempt < CHROMIUM_NEW_TAB_MAX_ATTEMPTS
                    && chromium_new_tab_error_is_retryable(error_message.as_str())
                {
                    warn!(
                        attempt,
                        max_attempts = CHROMIUM_NEW_TAB_MAX_ATTEMPTS,
                        error = error_message.as_str(),
                        "chromium new_tab reported retryable startup race; retrying"
                    );
                    std::thread::sleep(Duration::from_millis(CHROMIUM_NEW_TAB_RETRY_DELAY_MS));
                    continue;
                }
                return Err(format!("{failure_prefix}: {error_message}"));
            }
        }
    }
    Err(format!(
        "{failure_prefix}: tab creation exhausted retry attempts without a terminal result"
    ))
}

pub(crate) async fn initialize_chromium_session_runtime(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    session: &BrowserSessionRecord,
) -> Result<(), String> {
    let chromium = runtime.chromium.clone();
    let allow_private_targets = session.allow_private_targets;
    let navigation_timeout = Duration::from_millis(session.budget.max_navigation_timeout_ms.max(1));
    let active_tab_id = session.active_tab_id.clone();
    let mut tab_order = session.tab_order.clone();
    if tab_order.is_empty() {
        tab_order.push(active_tab_id.clone());
    } else if !tab_order.iter().any(|tab_id| tab_id == &active_tab_id) {
        tab_order.insert(0, active_tab_id.clone());
    }
    let proxy = ChromiumSessionProxy::spawn(allow_private_targets).await?;
    let proxy_uri = proxy.proxy_uri.clone();
    let security_incident = Arc::new(std::sync::Mutex::new(None::<String>));
    let mut chromium_session =
        run_chromium_blocking("chromium session initialization", move || {
            let profile_dir = tempfile::Builder::new()
                .prefix("palyra-browserd-session-")
                .tempdir()
                .map_err(|error| format!("failed to allocate Chromium profile dir: {error}"))?;
            let launch_options =
                build_chromium_launch_options(&chromium, &profile_dir, Some(proxy_uri.as_str()))?;
            let browser =
                Arc::new(HeadlessBrowser::new(launch_options).map_err(|error| {
                    format!("failed to launch Chromium browser process: {error}")
                })?);
            let mut tabs = HashMap::new();
            for tab_id in tab_order.iter() {
                let tab = create_configured_chromium_tab_with_retry(
                    &browser,
                    allow_private_targets,
                    navigation_timeout,
                    Arc::clone(&security_incident),
                    "failed to create Chromium tab for session restore",
                )?;
                tabs.insert(tab_id.clone(), tab);
            }
            Ok(ChromiumSessionState {
                browser,
                tabs,
                security_incident,
                _profile_dir: profile_dir,
                _proxy: None,
            })
        })
        .await?;
    info!(
        session_id = session_id,
        proxy_uri = proxy.proxy_uri.as_str(),
        allow_private_targets,
        "started per-session Chromium SOCKS5 proxy with NetGuard enforcement"
    );
    chromium_session._proxy = Some(proxy);
    runtime.chromium_sessions.lock().await.insert(session_id.to_owned(), chromium_session);
    Ok(())
}

pub(crate) async fn chromium_open_tab_runtime(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
) -> Result<(), String> {
    let (allow_private_targets, timeout_ms) = {
        let sessions = runtime.sessions.lock().await;
        let Some(session) = sessions.get(session_id) else {
            return Err("session_not_found".to_owned());
        };
        (session.allow_private_targets, session.budget.max_navigation_timeout_ms.max(1))
    };
    let (browser, security_incident) = {
        let chromium_sessions = runtime.chromium_sessions.lock().await;
        let Some(chromium_session) = chromium_sessions.get(session_id) else {
            return Err("chromium_session_not_found".to_owned());
        };
        (Arc::clone(&chromium_session.browser), Arc::clone(&chromium_session.security_incident))
    };
    let tab = run_chromium_blocking("chromium open tab", move || {
        create_configured_chromium_tab_with_retry(
            &browser,
            allow_private_targets,
            Duration::from_millis(timeout_ms),
            security_incident,
            "failed to allocate Chromium tab",
        )
    })
    .await?;
    let mut chromium_sessions = runtime.chromium_sessions.lock().await;
    let Some(chromium_session) = chromium_sessions.get_mut(session_id) else {
        return Err("chromium_session_not_found".to_owned());
    };
    chromium_session.tabs.insert(tab_id.to_owned(), tab);
    Ok(())
}

pub(crate) async fn chromium_close_tab_runtime(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
) -> Result<(), String> {
    let tab = {
        let mut chromium_sessions = runtime.chromium_sessions.lock().await;
        let Some(chromium_session) = chromium_sessions.get_mut(session_id) else {
            return Err("chromium_session_not_found".to_owned());
        };
        chromium_session.tabs.remove(tab_id)
    };
    if let Some(tab) = tab {
        let _ = run_chromium_blocking("chromium close tab", move || {
            tab.close(true).map_err(|error| format!("failed to close Chromium tab: {error}"))?;
            Ok(())
        })
        .await;
    }
    Ok(())
}

pub(crate) async fn enforce_chromium_remote_ip_guard(
    runtime: &BrowserRuntimeState,
    session_id: &str,
) -> Result<(), String> {
    let incident = {
        let chromium_sessions = runtime.chromium_sessions.lock().await;
        let Some(chromium_session) = chromium_sessions.get(session_id) else {
            return Ok(());
        };
        let mut guard = chromium_session
            .security_incident
            .lock()
            .map_err(|_| "failed to inspect Chromium security incident state".to_owned())?;
        guard.take()
    };
    let Some(reason) = incident else {
        return Ok(());
    };

    runtime.sessions.lock().await.remove(session_id);
    runtime.chromium_sessions.lock().await.remove(session_id);
    runtime.download_sessions.lock().await.remove(session_id);
    warn!(
        session_id = session_id,
        reason = reason.as_str(),
        "terminated browser session after Chromium remote IP guard incident"
    );
    Err(format!("chromium remote IP guard blocked request: {reason}"))
}

pub(crate) async fn chromium_tab_for_session(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
) -> Result<Arc<HeadlessTab>, String> {
    let chromium_sessions = runtime.chromium_sessions.lock().await;
    let Some(chromium_session) = chromium_sessions.get(session_id) else {
        return Err("chromium_session_not_found".to_owned());
    };
    chromium_session.tabs.get(tab_id).cloned().ok_or_else(|| "chromium_tab_not_found".to_owned())
}

pub(crate) async fn chromium_active_tab_for_session(
    runtime: &BrowserRuntimeState,
    session_id: &str,
) -> Result<(String, Arc<HeadlessTab>), String> {
    let active_tab_id = {
        let sessions = runtime.sessions.lock().await;
        let Some(session) = sessions.get(session_id) else {
            return Err("session_not_found".to_owned());
        };
        session.active_tab_id.clone()
    };
    let tab = chromium_tab_for_session(runtime, session_id, active_tab_id.as_str()).await?;
    Ok((active_tab_id, tab))
}

pub(crate) async fn chromium_observe_snapshot(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
) -> Result<ChromiumObserveSnapshot, String> {
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    let (max_response_bytes, max_title_bytes) = {
        let sessions = runtime.sessions.lock().await;
        let Some(session) = sessions.get(session_id) else {
            return Err("session_not_found".to_owned());
        };
        (session.budget.max_response_bytes, session.budget.max_title_bytes)
    };
    let tab = chromium_tab_for_session(runtime, session_id, tab_id).await?;
    let snapshot = run_chromium_blocking("chromium observe snapshot", move || {
        let page_body = tab
            .get_content()
            .map_err(|error| format!("failed to read Chromium DOM content: {error}"))?;
        let title = tab.get_title().unwrap_or_default();
        let page_url = tab.get_url();
        Ok(ChromiumObserveSnapshot { page_body, title, page_url })
    })
    .await?;
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    Ok(clamp_chromium_snapshot(snapshot, max_response_bytes, max_title_bytes))
}

pub(crate) async fn chromium_refresh_tab_snapshot(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
) -> Result<(), String> {
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    let snapshot = chromium_observe_snapshot(runtime, session_id, tab_id).await?;
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    let mut sessions = runtime.sessions.lock().await;
    let Some(session) = sessions.get_mut(session_id) else {
        return Err("session_not_found".to_owned());
    };
    let Some(tab) = session.tabs.get_mut(tab_id) else {
        return Err("tab_not_found".to_owned());
    };
    tab.last_page_body = snapshot.page_body;
    tab.last_title = snapshot.title;
    tab.last_url = Some(snapshot.page_url);
    Ok(())
}

pub(crate) async fn chromium_get_title(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
) -> Result<String, String> {
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    let tab = chromium_tab_for_session(runtime, session_id, tab_id).await?;
    let title = run_chromium_blocking("chromium get title", move || {
        tab.get_title().map_err(|error| format!("failed to read Chromium page title: {error}"))
    })
    .await?;
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    Ok(title)
}

pub(crate) async fn chromium_screenshot(
    runtime: &BrowserRuntimeState,
    session_id: &str,
) -> Result<Vec<u8>, String> {
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    let (_tab_id, tab) = chromium_active_tab_for_session(runtime, session_id).await?;
    let screenshot = run_chromium_blocking("chromium screenshot", move || {
        tab.capture_screenshot(Page::CaptureScreenshotFormatOption::Png, None, None, true)
            .map_err(|error| format!("failed to capture Chromium screenshot: {error}"))
    })
    .await?;
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    Ok(screenshot)
}

pub(crate) async fn navigate_with_chromium(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    params: ChromiumNavigateParams,
) -> NavigateOutcome {
    let (tab_id, _tab) = match chromium_active_tab_for_session(runtime, session_id).await {
        Ok(value) => value,
        Err(error) => {
            return NavigateOutcome {
                success: false,
                final_url: String::new(),
                status_code: 0,
                title: String::new(),
                page_body: String::new(),
                body_bytes: 0,
                latency_ms: 0,
                error: format!("chromium runtime unavailable: {error}"),
                network_log: Vec::new(),
                cookie_updates: Vec::new(),
            }
        }
    };
    navigate_tab_with_chromium(runtime, session_id, tab_id.as_str(), &params).await
}

pub(crate) async fn navigate_tab_with_chromium(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
    params: &ChromiumNavigateParams,
) -> NavigateOutcome {
    let mut outcome = navigate_with_guards(
        params.raw_url.as_str(),
        params.timeout_ms,
        params.allow_redirects,
        params.max_redirects,
        params.allow_private_targets,
        params.max_response_bytes,
        params.cookie_header.as_deref(),
    )
    .await;
    if !outcome.success {
        return outcome;
    }
    let tab = match chromium_tab_for_session(runtime, session_id, tab_id).await {
        Ok(value) => value,
        Err(error) => {
            outcome.success = false;
            outcome.error = format!("chromium tab runtime unavailable: {error}");
            return outcome;
        }
    };
    let target_url = outcome.final_url.clone();
    let chromium_timeout_ms = params.timeout_ms;
    let chromium_snapshot = run_chromium_blocking("chromium navigate", move || {
        tab.set_default_timeout(Duration::from_millis(chromium_timeout_ms.max(1)));
        tab.navigate_to(target_url.as_str())
            .map_err(|error| format!("failed to issue Chromium navigation command: {error}"))?;
        tab.wait_until_navigated()
            .map_err(|error| format!("Chromium navigation timeout or failure: {error}"))?;
        let page_body = tab.get_content().map_err(|error| {
            format!("failed to read Chromium page HTML after navigation: {error}")
        })?;
        let title = tab.get_title().unwrap_or_default();
        let page_url = tab.get_url();
        Ok(ChromiumObserveSnapshot { page_body, title, page_url })
    })
    .await;
    let snapshot = match chromium_snapshot {
        Ok(value) => value,
        Err(error) => {
            outcome.success = false;
            outcome.error = error;
            return outcome;
        }
    };
    if let Err(error) = enforce_chromium_remote_ip_guard(runtime, session_id).await {
        outcome.success = false;
        outcome.error = error;
        return outcome;
    }
    let body_bytes = snapshot.page_body.len() as u64;
    if body_bytes > params.max_response_bytes {
        outcome.success = false;
        outcome.error = format!(
            "response exceeds max_response_bytes ({} > {})",
            body_bytes, params.max_response_bytes
        );
        outcome.body_bytes = body_bytes;
        outcome.page_body.clear();
        outcome.title.clear();
        return outcome;
    }
    outcome.final_url = snapshot.page_url;
    outcome.title = snapshot.title;
    outcome.page_body = snapshot.page_body;
    outcome.body_bytes = body_bytes;
    outcome
}

pub(crate) async fn click_with_chromium(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    selector: &str,
    timeout_ms: u64,
    max_attempts: u32,
    allow_downloads: bool,
) -> ChromiumActionOutcome {
    enum ClickAttempt {
        Clicked { download_like: bool },
        DownloadBlocked,
        NotFound,
    }

    let (tab_id, tab) = match chromium_active_tab_for_session(runtime, session_id).await {
        Ok(value) => value,
        Err(error) => {
            return ChromiumActionOutcome {
                success: false,
                outcome: "chromium_runtime_missing".to_owned(),
                error,
                attempts: 1,
            }
        }
    };
    let started = Instant::now();
    let mut attempts = 0_u32;
    loop {
        attempts = attempts.saturating_add(1);
        let selector_for_attempt = selector.to_owned();
        let tab_for_attempt = Arc::clone(&tab);
        let attempt = run_chromium_blocking("chromium click", move || {
            let page_body = tab_for_attempt
                .get_content()
                .map_err(|error| format!("failed to read Chromium DOM before click: {error}"))?;
            if let Some(tag) =
                find_matching_html_tag(selector_for_attempt.as_str(), page_body.as_str())
            {
                if is_download_like_tag(tag.as_str()) && !allow_downloads {
                    return Ok(ClickAttempt::DownloadBlocked);
                }
                let element = tab_for_attempt.find_element(selector_for_attempt.as_str()).map_err(
                    |error| {
                        format!(
                            "failed to resolve selector '{}' on Chromium page: {error}",
                            selector_for_attempt
                        )
                    },
                )?;
                element.click().map_err(|error| {
                    format!(
                        "failed to click selector '{}' on Chromium page: {error}",
                        selector_for_attempt
                    )
                })?;
                Ok(ClickAttempt::Clicked { download_like: is_download_like_tag(tag.as_str()) })
            } else {
                Ok(ClickAttempt::NotFound)
            }
        })
        .await;

        match attempt {
            Ok(ClickAttempt::Clicked { download_like }) => {
                let _ = chromium_refresh_tab_snapshot(runtime, session_id, tab_id.as_str()).await;
                return ChromiumActionOutcome {
                    success: true,
                    outcome: if download_like {
                        "download_allowed".to_owned()
                    } else {
                        "clicked".to_owned()
                    },
                    error: String::new(),
                    attempts,
                };
            }
            Ok(ClickAttempt::DownloadBlocked) => {
                return ChromiumActionOutcome {
                    success: false,
                    outcome: "download_blocked".to_owned(),
                    error:
                        "download-like click is blocked by session policy (allow_downloads=false)"
                            .to_owned(),
                    attempts,
                };
            }
            Ok(ClickAttempt::NotFound) => {}
            Err(error) => {
                return ChromiumActionOutcome {
                    success: false,
                    outcome: "click_failed".to_owned(),
                    error,
                    attempts,
                };
            }
        }

        if attempts >= max_attempts || started.elapsed() >= Duration::from_millis(timeout_ms) {
            break;
        }
        let remaining_ms = timeout_ms.saturating_sub(started.elapsed().as_millis() as u64);
        let sleep_ms = DEFAULT_ACTION_RETRY_INTERVAL_MS.min(remaining_ms.max(1));
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    }
    ChromiumActionOutcome {
        success: false,
        outcome: "selector_not_found".to_owned(),
        error: format!("selector '{selector}' was not found"),
        attempts,
    }
}

pub(crate) async fn type_with_chromium(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    selector: &str,
    text: &str,
    clear_existing: bool,
    timeout_ms: u64,
) -> ChromiumActionOutcome {
    enum TypeAttempt {
        Typed,
        NotFound,
        NotTypable,
    }

    let (tab_id, tab) = match chromium_active_tab_for_session(runtime, session_id).await {
        Ok(value) => value,
        Err(error) => {
            return ChromiumActionOutcome {
                success: false,
                outcome: "chromium_runtime_missing".to_owned(),
                error,
                attempts: 1,
            }
        }
    };
    let started = Instant::now();
    let mut attempts = 0_u32;
    loop {
        attempts = attempts.saturating_add(1);
        let selector_for_attempt = selector.to_owned();
        let text_for_attempt = text.to_owned();
        let tab_for_attempt = Arc::clone(&tab);
        let clear_existing_for_attempt = clear_existing;
        let attempt = run_chromium_blocking("chromium type", move || {
            let page_body = tab_for_attempt
                .get_content()
                .map_err(|error| format!("failed to read Chromium DOM before type action: {error}"))?;
            let Some(tag) = find_matching_html_tag(selector_for_attempt.as_str(), page_body.as_str()) else {
                return Ok(TypeAttempt::NotFound);
            };
            if !is_typable_tag(tag.as_str()) {
                return Ok(TypeAttempt::NotTypable);
            }
            let element = tab_for_attempt.find_element(selector_for_attempt.as_str()).map_err(
                |error| format!("failed to resolve selector '{}' on Chromium page: {error}", selector_for_attempt),
            )?;
            if clear_existing_for_attempt {
                let _ = element.call_js_fn(
                    "function () { if (this && this.value !== undefined) { this.value = ''; } if (this && this.textContent !== undefined) { this.textContent = ''; } }",
                    Vec::new(),
                    false,
                );
            }
            element
                .click()
                .map_err(|error| format!("failed to focus selector '{}' for type action: {error}", selector_for_attempt))?;
            element
                .type_into(text_for_attempt.as_str())
                .map_err(|error| format!("failed to type into selector '{}' on Chromium page: {error}", selector_for_attempt))?;
            Ok(TypeAttempt::Typed)
        })
        .await;

        match attempt {
            Ok(TypeAttempt::Typed) => {
                let _ = chromium_refresh_tab_snapshot(runtime, session_id, tab_id.as_str()).await;
                return ChromiumActionOutcome {
                    success: true,
                    outcome: "typed".to_owned(),
                    error: String::new(),
                    attempts,
                };
            }
            Ok(TypeAttempt::NotTypable) => {
                return ChromiumActionOutcome {
                    success: false,
                    outcome: "selector_not_typable".to_owned(),
                    error: format!("selector '{selector}' does not target an input-like element"),
                    attempts,
                };
            }
            Ok(TypeAttempt::NotFound) => {}
            Err(error) => {
                return ChromiumActionOutcome {
                    success: false,
                    outcome: "type_failed".to_owned(),
                    error,
                    attempts,
                };
            }
        }
        if started.elapsed() >= Duration::from_millis(timeout_ms) {
            break;
        }
        let remaining_ms = timeout_ms.saturating_sub(started.elapsed().as_millis() as u64);
        let sleep_ms = DEFAULT_ACTION_RETRY_INTERVAL_MS.min(remaining_ms.max(1));
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    }
    ChromiumActionOutcome {
        success: false,
        outcome: "selector_not_found".to_owned(),
        error: format!("selector '{selector}' was not found"),
        attempts,
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::{clamp_chromium_snapshot, ChromiumObserveSnapshot};

    #[test]
    fn clamp_chromium_snapshot_enforces_body_and_title_budgets() {
        let snapshot = ChromiumObserveSnapshot {
            page_body: "α".repeat(12),
            title: "ß".repeat(4),
            page_url: "https://example.invalid/oversized".to_owned(),
        };

        let clamped = clamp_chromium_snapshot(snapshot, 17, 5);

        assert_eq!(clamped.page_body, "α".repeat(8));
        assert_eq!(clamped.title, "ß".repeat(2));
        assert_eq!(clamped.page_url, "https://example.invalid/oversized");
        assert!(clamped.page_body.len() <= 17);
        assert!(clamped.title.len() <= 5);
    }
}

pub(crate) async fn scroll_with_chromium(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    delta_x: i64,
    delta_y: i64,
) -> ChromiumScrollOutcome {
    let (tab_id, tab) = match chromium_active_tab_for_session(runtime, session_id).await {
        Ok(value) => value,
        Err(error) => {
            return ChromiumScrollOutcome { success: false, scroll_x: 0, scroll_y: 0, error }
        }
    };
    let scroll_script = format!(
        "(() => {{ window.scrollBy({delta_x}, {delta_y}); return {{ x: Math.trunc(window.scrollX || window.pageXOffset || 0), y: Math.trunc(window.scrollY || window.pageYOffset || 0) }}; }})()"
    );
    let positions = run_chromium_blocking("chromium scroll", move || {
        let value = tab
            .evaluate(scroll_script.as_str(), false)
            .map_err(|error| format!("failed to execute Chromium scroll script: {error}"))?
            .value
            .unwrap_or(serde_json::Value::Null);
        let x = value.get("x").and_then(serde_json::Value::as_i64).unwrap_or(0);
        let y = value.get("y").and_then(serde_json::Value::as_i64).unwrap_or(0);
        Ok((x, y))
    })
    .await;

    match positions {
        Ok((scroll_x, scroll_y)) => {
            let mut sessions = runtime.sessions.lock().await;
            if let Some(session) = sessions.get_mut(session_id) {
                if let Some(tab_record) = session.tabs.get_mut(tab_id.as_str()) {
                    tab_record.scroll_x = scroll_x;
                    tab_record.scroll_y = scroll_y;
                }
            }
            ChromiumScrollOutcome { success: true, scroll_x, scroll_y, error: String::new() }
        }
        Err(error) => ChromiumScrollOutcome { success: false, scroll_x: 0, scroll_y: 0, error },
    }
}

pub(crate) async fn wait_for_with_chromium(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    selector: &str,
    text: &str,
    timeout_ms: u64,
    poll_interval_ms: u64,
) -> ChromiumWaitOutcome {
    let (_tab_id, tab) = match chromium_active_tab_for_session(runtime, session_id).await {
        Ok(value) => value,
        Err(error) => {
            return ChromiumWaitOutcome {
                success: false,
                matched_selector: String::new(),
                matched_text: String::new(),
                attempts: 1,
                waited_ms: 0,
                error,
            }
        }
    };
    let started = Instant::now();
    let mut attempts = 0_u32;
    let selector_owned = selector.to_owned();
    let text_owned = text.to_owned();
    loop {
        attempts = attempts.saturating_add(1);
        let tab_for_attempt = Arc::clone(&tab);
        let selector_for_attempt = selector_owned.clone();
        let text_for_attempt = text_owned.clone();
        let check = run_chromium_blocking("chromium wait_for probe", move || {
            let mut matched_selector = false;
            let mut matched_text = false;
            if !selector_for_attempt.is_empty() {
                matched_selector = tab_for_attempt.find_element(selector_for_attempt.as_str()).is_ok();
            }
            if !text_for_attempt.trim().is_empty() {
                let text_json = serde_json::to_string(text_for_attempt.as_str())
                    .map_err(|error| format!("failed to encode wait_for text query: {error}"))?;
                let script = format!(
                    "(() => {{ const text = (document.body && document.body.innerText) ? document.body.innerText : ''; return text.includes({text_json}); }})()"
                );
                matched_text = tab_for_attempt
                    .evaluate(script.as_str(), false)
                    .map_err(|error| format!("failed to evaluate Chromium wait_for text probe: {error}"))?
                    .value
                    .and_then(|value| value.as_bool())
                    .unwrap_or(false);
            }
            Ok((matched_selector, matched_text))
        })
        .await;

        match check {
            Ok((selector_hit, text_hit)) => {
                if selector_hit {
                    return ChromiumWaitOutcome {
                        success: true,
                        matched_selector: selector_owned.clone(),
                        matched_text: String::new(),
                        attempts,
                        waited_ms: started.elapsed().as_millis() as u64,
                        error: String::new(),
                    };
                }
                if text_hit {
                    return ChromiumWaitOutcome {
                        success: true,
                        matched_selector: String::new(),
                        matched_text: text_owned.clone(),
                        attempts,
                        waited_ms: started.elapsed().as_millis() as u64,
                        error: String::new(),
                    };
                }
            }
            Err(error) => {
                return ChromiumWaitOutcome {
                    success: false,
                    matched_selector: String::new(),
                    matched_text: String::new(),
                    attempts,
                    waited_ms: started.elapsed().as_millis() as u64,
                    error,
                };
            }
        }
        if started.elapsed() >= Duration::from_millis(timeout_ms) {
            break;
        }
        let remaining_ms = timeout_ms.saturating_sub(started.elapsed().as_millis() as u64);
        let sleep_ms = poll_interval_ms.min(remaining_ms.max(1));
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    }
    ChromiumWaitOutcome {
        success: false,
        matched_selector: String::new(),
        matched_text: String::new(),
        attempts,
        waited_ms: started.elapsed().as_millis() as u64,
        error: "wait_for condition was not satisfied before timeout".to_owned(),
    }
}
