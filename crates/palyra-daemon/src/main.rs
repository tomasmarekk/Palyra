mod config;

use std::time::Instant;

use anyhow::{Context, Result};
use axum::{extract::State, response::IntoResponse, routing::get, Json, Router};
use clap::Parser;
use config::load_config;
use palyra_common::{build_metadata, health_response, parse_daemon_bind_socket, HealthResponse};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone, Parser)]
#[command(name = "palyrad", about = "Palyra daemon bootstrap stub")]
struct Args {
    #[arg(long)]
    bind: Option<String>,
    #[arg(long)]
    port: Option<u16>,
}

#[derive(Clone)]
struct AppState {
    started_at: Instant,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let args = Args::parse();
    let mut loaded = load_config()?;
    if let Some(bind) = args.bind {
        loaded.daemon.bind_addr = bind;
        loaded.source.push_str(" +cli(--bind)");
    }
    if let Some(port) = args.port {
        loaded.daemon.port = port;
        loaded.source.push_str(" +cli(--port)");
    }

    let build = build_metadata();
    info!(
        service = "palyrad",
        version = build.version,
        git_hash = build.git_hash,
        build_profile = build.build_profile,
        config_source = %loaded.source,
        bind_addr = %loaded.daemon.bind_addr,
        port = loaded.daemon.port,
        node_rpc_mtls_required = !loaded.identity.allow_insecure_node_rpc_without_mtls,
        "daemon startup"
    );

    let started_at = Instant::now();
    let state = AppState { started_at };
    let app = Router::new().route("/healthz", get(health_handler)).with_state(state);

    let address = parse_daemon_bind_socket(&loaded.daemon.bind_addr, loaded.daemon.port)
        .context("invalid bind address or port")?;
    let listener =
        tokio::net::TcpListener::bind(address).await.context("failed to bind palyrad listener")?;
    let bound_address =
        listener.local_addr().context("failed to resolve palyrad bound listener address")?;
    info!(listen_addr = %bound_address, "daemon listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("palyrad server failed")?;
    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().json().with_env_filter(filter).init();
}

async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    Json::<HealthResponse>(health_response("palyrad", state.started_at))
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::error!(error = %error, "failed to register Ctrl+C handler");
        std::future::pending::<()>().await;
    }
}
