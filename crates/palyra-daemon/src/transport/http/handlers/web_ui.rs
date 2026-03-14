use std::{
    env, fs,
    path::{Path, PathBuf},
};

use axum::{
    http::{header::CONTENT_TYPE, HeaderValue, StatusCode, Uri},
    response::{Html, IntoResponse, Response},
};

const WEB_UI_ROOT_ENV: &str = "PALYRA_WEB_DIST_DIR";
const WEB_UI_ENTRYPOINT: &str = "index.html";

pub(crate) async fn web_ui_entry_handler(uri: Uri) -> Response {
    let Some(root) = resolve_web_ui_root() else {
        return missing_web_ui_response();
    };

    match load_web_ui_response(root.as_path(), uri.path()).await {
        Ok(response) => response,
        Err(WebUiLoadError::InvalidPath) => {
            (StatusCode::BAD_REQUEST, "invalid dashboard asset path").into_response()
        }
        Err(WebUiLoadError::NotFound) => {
            (StatusCode::NOT_FOUND, "dashboard asset not found").into_response()
        }
        Err(WebUiLoadError::Io(error)) => {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("dashboard asset read failed: {error}"))
                .into_response()
        }
    }
}

fn missing_web_ui_response() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Html(
            r#"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Palyra Dashboard Unavailable</title>
    <style>
      :root {
        color-scheme: dark;
        background: #09131b;
        color: #eff7fa;
        font-family: "Segoe UI", "Helvetica Neue", sans-serif;
      }
      body {
        margin: 0;
        min-height: 100vh;
        background:
          radial-gradient(circle at top left, rgba(42, 163, 155, 0.18), transparent 28rem),
          linear-gradient(180deg, #0c1821 0%, #142530 100%);
      }
      main {
        max-width: 44rem;
        margin: 0 auto;
        padding: 3rem 1.5rem 4rem;
      }
      .panel {
        padding: 1.5rem;
        border-radius: 1.25rem;
        border: 1px solid rgba(155, 190, 204, 0.16);
        background: rgba(13, 24, 33, 0.88);
        box-shadow: 0 24px 48px rgba(0, 0, 0, 0.28);
      }
      h1 {
        margin: 0 0 0.75rem;
        font-size: clamp(2rem, 6vw, 3rem);
        line-height: 1;
      }
      p {
        margin: 0 0 1rem;
        line-height: 1.6;
      }
      a {
        color: #63d4c6;
      }
      code {
        font-family: "Cascadia Code", "Fira Code", monospace;
      }
    </style>
  </head>
  <body>
    <main>
      <div class="panel">
        <h1>Dashboard bundle missing</h1>
        <p>
          <code>palyrad</code> could not find the built web dashboard assets. Build
          <code>apps/web</code> or package the portable bundle with the colocated <code>web/</code>
          directory before opening the dashboard.
        </p>
        <p>
          Machine health remains available at <a href="/healthz">/healthz</a>. The legacy runtime
          handoff page is available at <a href="/runtime">/runtime</a>.
        </p>
      </div>
    </main>
  </body>
</html>"#,
        ),
    )
        .into_response()
}

async fn load_web_ui_response(root: &Path, request_path: &str) -> Result<Response, WebUiLoadError> {
    let asset_path = resolve_web_ui_asset_path(root, request_path)?;
    let bytes = fs::read(asset_path.as_path()).map_err(WebUiLoadError::Io)?;
    let mut response = bytes.into_response();
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static(content_type_for_path(asset_path.as_path())),
    );
    Ok(response)
}

fn resolve_web_ui_root() -> Option<PathBuf> {
    let mut candidates = Vec::new();
    if let Ok(explicit) = env::var(WEB_UI_ROOT_ENV) {
        if let Some(path) = normalize_optional_text(explicit.as_str()) {
            candidates.push(PathBuf::from(path));
        }
    }

    if let Ok(current_exe) = env::current_exe() {
        if let Some(parent) = current_exe.parent() {
            candidates.push(parent.join("web"));
        }
        for ancestor in current_exe.ancestors().take(8) {
            candidates.push(ancestor.join("apps").join("web").join("dist"));
        }
    }

    if let Ok(current_dir) = env::current_dir() {
        candidates.push(current_dir.join("web"));
        candidates.push(current_dir.join("apps").join("web").join("dist"));
    }

    candidates.into_iter().find_map(|candidate| canonicalize_web_ui_root(candidate.as_path()))
}

fn canonicalize_web_ui_root(candidate: &Path) -> Option<PathBuf> {
    let canonical = fs::canonicalize(candidate).ok()?;
    if !canonical.is_dir() {
        return None;
    }
    if !canonical.join(WEB_UI_ENTRYPOINT).is_file() {
        return None;
    }
    Some(canonical)
}

fn resolve_web_ui_asset_path(root: &Path, request_path: &str) -> Result<PathBuf, WebUiLoadError> {
    let trimmed = request_path.trim();
    if trimmed.is_empty() || trimmed == "/" {
        return Ok(root.join(WEB_UI_ENTRYPOINT));
    }

    let mut relative = PathBuf::new();
    for segment in trimmed.trim_start_matches('/').split('/') {
        if segment.is_empty() {
            continue;
        }
        if segment == "." || segment == ".." || segment.contains('\\') {
            return Err(WebUiLoadError::InvalidPath);
        }
        relative.push(segment);
    }

    if relative.as_os_str().is_empty() {
        return Ok(root.join(WEB_UI_ENTRYPOINT));
    }

    let candidate = root.join(relative.as_path());
    if candidate.is_file() {
        return Ok(candidate);
    }

    let has_extension = candidate.extension().is_some();
    if has_extension {
        return Err(WebUiLoadError::NotFound);
    }

    Ok(root.join(WEB_UI_ENTRYPOINT))
}

fn content_type_for_path(path: &Path) -> &'static str {
    match path.extension().and_then(|value| value.to_str()).unwrap_or_default() {
        "css" => "text/css; charset=utf-8",
        "html" => "text/html; charset=utf-8",
        "ico" => "image/x-icon",
        "jpeg" | "jpg" => "image/jpeg",
        "js" => "text/javascript; charset=utf-8",
        "json" => "application/json; charset=utf-8",
        "map" => "application/json; charset=utf-8",
        "png" => "image/png",
        "svg" => "image/svg+xml",
        "txt" => "text/plain; charset=utf-8",
        "webp" => "image/webp",
        _ => "application/octet-stream",
    }
}

fn normalize_optional_text(raw: &str) -> Option<&str> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

enum WebUiLoadError {
    InvalidPath,
    Io(std::io::Error),
    NotFound,
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{canonicalize_web_ui_root, content_type_for_path, resolve_web_ui_asset_path};

    fn create_fixture() -> tempfile::TempDir {
        let fixture = tempfile::tempdir().expect("tempdir should initialize");
        std::fs::write(fixture.path().join("index.html"), "<!doctype html><title>Palyra</title>")
            .expect("index.html should be written");
        std::fs::create_dir_all(fixture.path().join("assets")).expect("assets dir should exist");
        std::fs::write(fixture.path().join("assets/app.js"), "console.log('ok');")
            .expect("app.js should be written");
        fixture
    }

    #[test]
    fn canonicalize_web_ui_root_requires_index_html() {
        let fixture = create_fixture();
        let resolved =
            canonicalize_web_ui_root(fixture.path()).expect("web ui root should resolve");
        assert!(resolved.is_dir());
        assert!(resolved.join("index.html").is_file());
    }

    #[test]
    fn resolve_web_ui_asset_path_serves_root_and_assets() {
        let fixture = create_fixture();
        let entry = resolve_web_ui_asset_path(fixture.path(), "/").expect("root should resolve");
        let asset = resolve_web_ui_asset_path(fixture.path(), "/assets/app.js")
            .expect("asset path should resolve");
        assert_eq!(entry.file_name().and_then(|value| value.to_str()), Some("index.html"));
        assert_eq!(asset.file_name().and_then(|value| value.to_str()), Some("app.js"));
    }

    #[test]
    fn resolve_web_ui_asset_path_uses_spa_fallback_for_routes() {
        let fixture = create_fixture();
        let entry = resolve_web_ui_asset_path(fixture.path(), "/settings/access")
            .expect("spa routes should fall back to the entrypoint");
        assert_eq!(entry.file_name().and_then(|value| value.to_str()), Some("index.html"));
    }

    #[test]
    fn resolve_web_ui_asset_path_rejects_path_traversal() {
        let fixture = create_fixture();
        assert!(resolve_web_ui_asset_path(fixture.path(), "/../secrets").is_err());
        assert!(resolve_web_ui_asset_path(fixture.path(), "/assets\\app.js").is_err());
    }

    #[test]
    fn content_type_for_known_dashboard_assets_is_stable() {
        assert_eq!(content_type_for_path(Path::new("index.html")), "text/html; charset=utf-8");
        assert_eq!(
            content_type_for_path(Path::new("assets/app.js")),
            "text/javascript; charset=utf-8"
        );
        assert_eq!(content_type_for_path(Path::new("assets/app.css")), "text/css; charset=utf-8");
    }
}
