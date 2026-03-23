use anyhow::{Context, Result};
use tonic::Request;

use crate::*;

pub(crate) async fn fetch_health_with_retry(
    grpc_url: String,
) -> Result<gateway_v1::HealthResponse> {
    let mut last_error = None;
    for attempt in 1..=MAX_GRPC_ATTEMPTS {
        match fetch_health_once(grpc_url.as_str()).await {
            Ok(response) => return Ok(response),
            Err(error) => {
                let retryable = is_retryable_error(&error);
                last_error = Some(error);
                if attempt < MAX_GRPC_ATTEMPTS && retryable {
                    let delay_ms = BASE_GRPC_BACKOFF_MS * (1_u64 << (attempt - 1));
                    sleep(Duration::from_millis(delay_ms)).await;
                } else {
                    break;
                }
            }
        }
    }

    if let Some(error) = last_error {
        Err(error).context(format!("gRPC health check failed after {MAX_GRPC_ATTEMPTS} attempts"))
    } else {
        anyhow::bail!("gRPC health check failed with no captured error")
    }
}

pub(crate) async fn fetch_health_once(grpc_url: &str) -> Result<gateway_v1::HealthResponse> {
    let mut client =
        gateway_v1::gateway_service_client::GatewayServiceClient::connect(grpc_url.to_owned())
            .await
            .with_context(|| format!("failed to connect gateway gRPC endpoint {grpc_url}"))?;
    let response = client
        .get_health(gateway_v1::HealthRequest { v: RUN_STREAM_REQUEST_VERSION })
        .await
        .context("failed to call gateway GetHealth")?;
    Ok(response.into_inner())
}

pub(crate) async fn run_stream_with_retry(
    connection: &AgentConnection,
    request: &AgentRunInput,
) -> Result<tonic::Streaming<common_v1::RunStreamEvent>> {
    let mut last_error = None;
    for attempt in 1..=MAX_GRPC_ATTEMPTS {
        match run_stream_once(connection, request).await {
            Ok(stream) => return Ok(stream),
            Err(error) => {
                let retryable = is_retryable_error(&error);
                last_error = Some(error);
                if attempt < MAX_GRPC_ATTEMPTS && retryable {
                    let delay_ms = BASE_GRPC_BACKOFF_MS * (1_u64 << (attempt - 1));
                    sleep(Duration::from_millis(delay_ms)).await;
                } else {
                    break;
                }
            }
        }
    }

    if let Some(error) = last_error {
        Err(error).context(format!("agent stream failed after {MAX_GRPC_ATTEMPTS} attempts"))
    } else {
        anyhow::bail!("agent stream failed with no captured error")
    }
}

pub(crate) async fn run_stream_once(
    connection: &AgentConnection,
    input: &AgentRunInput,
) -> Result<tonic::Streaming<common_v1::RunStreamEvent>> {
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(
        connection.grpc_url.clone(),
    )
    .await
    .with_context(|| format!("failed to connect gateway gRPC endpoint {}", connection.grpc_url))?;
    let request = build_run_stream_request(input)?;
    let mut stream_request = Request::new(iter(vec![request]));
    inject_run_stream_metadata(stream_request.metadata_mut(), connection)?;
    let stream = client
        .run_stream(stream_request)
        .await
        .context("failed to call gateway RunStream")?
        .into_inner();
    Ok(stream)
}

pub(crate) fn is_retryable_error(error: &anyhow::Error) -> bool {
    if error.chain().any(|cause| cause.is::<tonic::transport::Error>()) {
        return true;
    }
    error.chain().find_map(|cause| cause.downcast_ref::<tonic::Status>()).is_some_and(|status| {
        matches!(
            status.code(),
            tonic::Code::Unavailable
                | tonic::Code::DeadlineExceeded
                | tonic::Code::ResourceExhausted
                | tonic::Code::Internal
        )
    })
}

pub(crate) fn inject_run_stream_metadata(
    metadata: &mut tonic::metadata::MetadataMap,
    connection: &AgentConnection,
) -> Result<()> {
    if let Some(token) = connection.token.as_ref() {
        metadata.insert(
            "authorization",
            format!("Bearer {token}").parse().context("invalid admin token metadata")?,
        );
    }
    metadata.insert(
        "x-palyra-principal",
        connection.principal.parse().context("invalid principal metadata value")?,
    );
    metadata.insert(
        "x-palyra-device-id",
        connection.device_id.parse().context("invalid device_id metadata value")?,
    );
    metadata.insert(
        "x-palyra-channel",
        connection.channel.parse().context("invalid channel metadata value")?,
    );
    metadata.insert(
        "x-palyra-trace-id",
        connection
            .trace_id
            .parse()
            .context("invalid trace_id metadata value")?,
    );
    Ok(())
}

pub(crate) fn resolve_url(explicit: Option<String>) -> Result<String> {
    if let Some(url) = explicit {
        return Ok(url);
    }
    if let Ok(url) = env::var("PALYRA_GATEWAY_GRPC_URL") {
        if !url.trim().is_empty() {
            return Ok(url);
        }
    }
    let bind = env::var("PALYRA_GATEWAY_GRPC_BIND_ADDR")
        .unwrap_or_else(|_| DEFAULT_GATEWAY_GRPC_BIND_ADDR.to_owned());
    let port = env::var("PALYRA_GATEWAY_GRPC_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(DEFAULT_GATEWAY_GRPC_PORT);
    let socket = parse_daemon_bind_socket(bind.as_str(), port)
        .context("invalid gateway gRPC bind config")?;
    let socket = normalize_client_socket(socket);
    Ok(format!("http://{socket}"))
}

pub(crate) fn normalize_client_socket(socket: SocketAddr) -> SocketAddr {
    match socket {
        SocketAddr::V4(v4) if v4.ip().is_unspecified() => {
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), v4.port())
        }
        SocketAddr::V6(v6) if v6.ip().is_unspecified() => {
            SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), v6.port())
        }
        other => other,
    }
}

pub(crate) fn build_runtime() -> Result<tokio::runtime::Runtime> {
    RuntimeBuilder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to initialize async runtime")
}
