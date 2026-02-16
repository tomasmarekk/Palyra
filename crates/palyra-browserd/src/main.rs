use std::time::Instant;

use anyhow::{Context, Result};
use axum::{extract::State, response::IntoResponse, routing::get, Json, Router};
use clap::Parser;
use palyra_common::{build_metadata, health_response, parse_daemon_bind_socket, HealthResponse};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone, Parser)]
#[command(name = "palyra-browserd", about = "Palyra browser service bootstrap stub")]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    bind: String,
    #[arg(long, default_value_t = 7143)]
    port: u16,
}

#[derive(Clone)]
struct AppState {
    started_at: Instant,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let args = Args::parse();
    let build = build_metadata();
    info!(
        service = "palyra-browserd",
        version = build.version,
        git_hash = build.git_hash,
        build_profile = build.build_profile,
        bind_addr = %args.bind,
        port = args.port,
        "browser service startup"
    );

    let state = AppState { started_at: Instant::now() };
    let app = Router::new().route("/healthz", get(health_handler)).with_state(state);

    let address =
        parse_daemon_bind_socket(&args.bind, args.port).context("invalid bind address or port")?;
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .context("failed to bind palyra-browserd listener")?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("palyra-browserd server failed")?;

    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().json().with_env_filter(filter).init();
}

async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    Json::<HealthResponse>(health_response("palyra-browserd", state.started_at))
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::error!(error = %error, "failed to register Ctrl+C handler");
        std::future::pending::<()>().await;
    }
}
