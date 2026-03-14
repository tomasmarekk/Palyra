use axum::{
    extract::State,
    response::{Html, IntoResponse},
    Json,
};
use palyra_common::{health_response, HealthResponse};

use crate::app::state::AppState;

pub(crate) async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    Json::<HealthResponse>(health_response("palyrad", state.started_at))
}

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
