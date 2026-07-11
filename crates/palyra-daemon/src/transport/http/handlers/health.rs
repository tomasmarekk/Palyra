//! Health and legacy runtime handoff handlers for the daemon HTTP server.
//!
//! `/healthz` is the machine-readable liveness probe; `/runtime` is a narrow
//! HTML handoff that points operators toward the authenticated dashboard.

use axum::{
    extract::State,
    response::{Html, IntoResponse},
    Json,
};
use palyra_common::{
    health_response, qa_scenarios::QA_SCENARIO_SCHEMA_VERSION,
    runtime_contracts::PUBLIC_RUNTIME_CONTRACT_SNAPSHOT_VERSION, HealthResponse,
};
use palyra_model_providers::QA_MOCK_PROVIDER_FIXTURE_SCHEMA_VERSION;
use serde::Serialize;

use crate::app::state::AppState;

/// Daemon-owned health handshake with the legacy health shape flattened intact.
#[derive(Debug, Serialize)]
struct DaemonHealthResponse {
    #[serde(flatten)]
    health: HealthResponse,
    public_runtime_contract_version: &'static str,
    qa_scenario_schema_version: u32,
    qa_mock_provider_fixture_schema_version: u32,
}

impl DaemonHealthResponse {
    fn new(health: HealthResponse) -> Self {
        Self {
            health,
            public_runtime_contract_version: PUBLIC_RUNTIME_CONTRACT_SNAPSHOT_VERSION,
            qa_scenario_schema_version: QA_SCENARIO_SCHEMA_VERSION,
            qa_mock_provider_fixture_schema_version: QA_MOCK_PROVIDER_FIXTURE_SCHEMA_VERSION,
        }
    }
}

/// Returns the JSON daemon health response.
pub(crate) async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    Json(DaemonHealthResponse::new(health_response("palyrad", state.started_at)))
}

/// Renders the legacy runtime landing page with links to dashboard surfaces.
pub(crate) async fn dashboard_handoff_handler(State(state): State<AppState>) -> impl IntoResponse {
    let health = health_response("palyrad", state.started_at);
    Html(format!(
        r#"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Palyra Runtime Status</title>
    <style>
      :root {{
        color-scheme: dark;
        font-family: "Segoe UI", "Helvetica Neue", sans-serif;
        background: #09131b;
        color: #eff7fa;
      }}
      body {{
        margin: 0;
        min-height: 100vh;
        background:
          radial-gradient(circle at top right, rgba(42, 163, 155, 0.18), transparent 32rem),
          linear-gradient(180deg, #0c1821 0%, #142530 100%);
      }}
      main {{
        max-width: 48rem;
        margin: 0 auto;
        padding: 3rem 1.5rem 4rem;
      }}
      .panel {{
        background: rgba(13, 24, 33, 0.9);
        border: 1px solid rgba(155, 190, 204, 0.16);
        border-radius: 1.25rem;
        box-shadow: 0 1.25rem 3rem rgba(0, 0, 0, 0.32);
        padding: 1.5rem;
      }}
      h1 {{
        margin: 0 0 0.75rem;
        font-size: clamp(2rem, 6vw, 3.25rem);
        line-height: 1;
      }}
      p {{
        margin: 0 0 1rem;
        line-height: 1.55;
      }}
      ul {{
        margin: 1.25rem 0 0;
        padding-left: 1.2rem;
      }}
      li + li {{
        margin-top: 0.6rem;
      }}
      .badge {{
        display: inline-flex;
        align-items: center;
        gap: 0.45rem;
        border-radius: 999px;
        padding: 0.35rem 0.75rem;
        background: rgba(83, 212, 198, 0.14);
        color: #8be8da;
        font-size: 0.92rem;
        font-weight: 600;
      }}
      a {{
        color: #63d4c6;
      }}
      code {{
        font-family: "Cascadia Code", "Fira Code", monospace;
        font-size: 0.95em;
      }}
    </style>
  </head>
  <body>
    <main>
      <div class="panel">
        <div class="badge">Runtime {status}</div>
        <h1>Palyra Local Runtime</h1>
        <p>
          The local control plane is responding. The full operator dashboard now lives at the root
          URL, while this page remains a narrow runtime and diagnostics surface.
        </p>
        <p>
          Use <a href="/">the dashboard</a> for the operator workspace. The authenticated operator
          APIs remain available under <code>/console/v1/*</code> once a console session is
          established.
        </p>
        <ul>
          <li><a href="/">Open dashboard</a></li>
          <li><a href="/healthz">Health endpoint</a></li>
          <li><a href="/console/v1/control-plane/capabilities">Capability catalog</a></li>
          <li><a href="/console/v1/diagnostics">Diagnostics snapshot</a></li>
        </ul>
      </div>
    </main>
  </body>
</html>"#,
        status = health.status
    ))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn daemon_health_serialization_preserves_legacy_fields_and_pins_qa_contracts() {
        let response = DaemonHealthResponse::new(HealthResponse {
            service: "palyrad".to_owned(),
            status: "ok".to_owned(),
            version: "1.2.3".to_owned(),
            git_hash: "abc123".to_owned(),
            build_profile: "test".to_owned(),
            uptime_seconds: 42,
        });

        let value = serde_json::to_value(response).expect("daemon health should serialize");

        assert_eq!(
            value,
            json!({
                "service": "palyrad",
                "status": "ok",
                "version": "1.2.3",
                "git_hash": "abc123",
                "build_profile": "test",
                "uptime_seconds": 42,
                "public_runtime_contract_version": PUBLIC_RUNTIME_CONTRACT_SNAPSHOT_VERSION,
                "qa_scenario_schema_version": QA_SCENARIO_SCHEMA_VERSION,
                "qa_mock_provider_fixture_schema_version":
                    QA_MOCK_PROVIDER_FIXTURE_SCHEMA_VERSION,
            })
        );
        let legacy = serde_json::from_value::<HealthResponse>(value)
            .expect("legacy health consumers should ignore additive handshake fields");
        assert_eq!(legacy.service, "palyrad");
        assert_eq!(legacy.status, "ok");
        assert_eq!(legacy.version, "1.2.3");
        assert_eq!(legacy.git_hash, "abc123");
        assert_eq!(legacy.build_profile, "test");
        assert_eq!(legacy.uptime_seconds, 42);
    }
}
