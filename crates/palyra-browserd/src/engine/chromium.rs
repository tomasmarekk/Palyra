use crate::*;
use headless_chrome::protocol::cdp::Emulation;
use headless_chrome::{
    browser::tab::ModifierKey,
    types::{Bounds, PrintToPdfOptions},
};

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
pub(crate) struct ChromiumViewportOutcome {
    pub(crate) success: bool,
    pub(crate) width: u32,
    pub(crate) height: u32,
    pub(crate) device_scale_factor: f64,
    pub(crate) mobile: bool,
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

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ChromiumLayoutMetrics {
    pub(crate) viewport_width: u32,
    pub(crate) viewport_height: u32,
    pub(crate) device_scale_factor: f64,
    pub(crate) document_scroll_width: u32,
    pub(crate) document_scroll_height: u32,
    pub(crate) document_client_width: u32,
    pub(crate) document_client_height: u32,
    pub(crate) horizontal_overflow: bool,
    pub(crate) vertical_overflow: bool,
}

#[derive(Debug)]
pub(crate) struct ChromiumClientDownload {
    pub(crate) source_url: String,
    pub(crate) file_name: String,
    pub(crate) mime_type: String,
    pub(crate) content: Vec<u8>,
}

type ChromiumLocalStorageSnapshot = Option<(String, HashMap<String, String>)>;

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

const CHROMIUM_PAGE_DIAGNOSTICS_SCRIPT: &str = r#"
(() => {
  const rootKey = "__palyraDiagnostics";
  const state = window[rootKey] = window[rootKey] || {};
  if (state.installed) {
    return true;
  }
  state.installed = true;
  state.entries = Array.isArray(state.entries) ? state.entries : [];
  const MAX_CONSOLE_ENTRIES = 256;
  const MAX_CONSOLE_CHARS = 32 * 1024;
  const MAX_CONSOLE_KIND_CHARS = 64;
  const MAX_CONSOLE_MESSAGE_CHARS = 1024;
  const MAX_CONSOLE_SOURCE_CHARS = 256;
  const MAX_CONSOLE_STACK_CHARS = 1024;
  const MAX_CONSOLE_URL_CHARS = 2048;
  const clampString = (value, maxChars) => {
    const text = String(value || "");
    return text.length > maxChars ? text.slice(0, maxChars) : text;
  };
  const stringify = (value) => {
    try {
      if (typeof value === "string") return clampString(value, MAX_CONSOLE_MESSAGE_CHARS);
      if (value && typeof value === "object") {
        if (value instanceof Error) {
          return clampString(value.stack || value.message || "Error", MAX_CONSOLE_MESSAGE_CHARS);
        }
        return clampString(Object.prototype.toString.call(value), MAX_CONSOLE_MESSAGE_CHARS);
      }
      return clampString(value, MAX_CONSOLE_MESSAGE_CHARS);
    } catch (_) {
      return "";
    }
  };
  const normalizeEntry = (severity, kind, message, source, stackTrace) => ({
    severity: clampString(severity, 16),
    kind: clampString(kind, MAX_CONSOLE_KIND_CHARS),
    message: clampString(message, MAX_CONSOLE_MESSAGE_CHARS),
    captured_at_unix_ms: Date.now(),
    source: clampString(source, MAX_CONSOLE_SOURCE_CHARS),
    stack_trace: clampString(stackTrace, MAX_CONSOLE_STACK_CHARS),
    page_url: clampString((window.location && window.location.href) || "", MAX_CONSOLE_URL_CHARS)
  });
  const entryChars = (entry) => (
    String(entry.severity || "").length +
    String(entry.kind || "").length +
    String(entry.message || "").length +
    String(entry.source || "").length +
    String(entry.stack_trace || "").length +
    String(entry.page_url || "").length +
    96
  );
  const trimEntries = () => {
    try {
      if (!Array.isArray(state.entries)) {
        state.entries = [];
      }
      while (state.entries.length > MAX_CONSOLE_ENTRIES) {
        state.entries.shift();
      }
      let total = state.entries.reduce((sum, entry) => sum + entryChars(entry), 0);
      while (state.entries.length > 0 && total > MAX_CONSOLE_CHARS) {
        const removed = state.entries.shift();
        total -= entryChars(removed);
      }
    } catch (_) {
      state.entries = [];
    }
  };
  const push = (severity, kind, message, source, stackTrace) => {
    try {
      state.entries.push(normalizeEntry(severity, kind, message, source, stackTrace));
      trimEntries();
    } catch (_) {}
  };
  const mapSeverity = (level) => {
    if (level === "warn") return "warn";
    if (level === "error") return "error";
    if (level === "debug") return "debug";
    return "info";
  };
  ["debug", "info", "warn", "error", "log"].forEach((level) => {
    const originalKey = `original_${level}`;
    if (typeof console[level] !== "function" || state[originalKey]) {
      return;
    }
    state[originalKey] = console[level].bind(console);
    console[level] = (...args) => {
      const message = args.map((value) => stringify(value)).join(" ");
      push(mapSeverity(level), "console", message, `console.${level}`, "");
      return state[originalKey](...args);
    };
  });
  window.addEventListener("error", (event) => {
    push(
      "error",
      "page_error",
      event.message || "page error",
      event.filename || "window.onerror",
      (event.error && event.error.stack) || ""
    );
  });
  window.addEventListener("unhandledrejection", (event) => {
    push(
      "error",
      "unhandled_rejection",
      stringify(event.reason),
      "window.unhandledrejection",
      ""
    );
  });
  state.network_entries = Array.isArray(state.network_entries) ? state.network_entries : [];
  state.client_download_entries = Array.isArray(state.client_download_entries) ? state.client_download_entries : [];
  state.object_urls = state.object_urls || {};
  state.pending_client_downloads = Number(state.pending_client_downloads || 0);
  const MAX_CLIENT_DOWNLOAD_ENTRIES = 32;
  const MAX_CLIENT_DOWNLOAD_BYTES = 8 * 1024 * 1024;
  const normalizeNetworkUrl = (raw) => {
    try {
      return new URL(String(raw || ""), window.location.href).href;
    } catch (_) {
      return String(raw || "");
    }
  };
  const pushNetwork = (requestUrl, statusCode, startedAt, headers) => {
    try {
      state.network_entries.push({
        request_url: normalizeNetworkUrl(requestUrl),
        status_code: Number(statusCode || 0),
        latency_ms: Math.max(0, Date.now() - Number(startedAt || Date.now())),
        captured_at_unix_ms: Date.now(),
        headers: Array.isArray(headers) ? headers.slice(0, 24) : []
      });
      if (state.network_entries.length > 512) {
        state.network_entries.splice(0, state.network_entries.length - 512);
      }
    } catch (_) {}
  };
  const normalizeDownloadUrl = (raw) => {
    try {
      return new URL(String(raw || ""), window.location.href).href;
    } catch (_) {
      return String(raw || "");
    }
  };
  const clampDownloadFileName = (raw) => {
    const text = String(raw || "download.bin").replace(/[^A-Za-z0-9._-]/g, "_").replace(/^[._]+|[._]+$/g, "");
    return (text || "download.bin").slice(0, 96);
  };
  const blobToBase64 = async (blob) => {
    const buffer = await blob.arrayBuffer();
    if (buffer.byteLength > MAX_CLIENT_DOWNLOAD_BYTES) {
      throw new Error(`client-side download exceeds max bytes (${buffer.byteLength} > ${MAX_CLIENT_DOWNLOAD_BYTES})`);
    }
    const bytes = new Uint8Array(buffer);
    let binary = "";
    for (let offset = 0; offset < bytes.length; offset += 0x8000) {
      binary += String.fromCharCode(...bytes.subarray(offset, offset + 0x8000));
    }
    return btoa(binary);
  };
  const trimClientDownloads = () => {
    while (state.client_download_entries.length > MAX_CLIENT_DOWNLOAD_ENTRIES) {
      state.client_download_entries.shift();
    }
  };
  const captureClientDownload = (anchor, source) => {
    try {
      const href = normalizeDownloadUrl(anchor && anchor.getAttribute ? anchor.getAttribute("href") : "");
      if (!href || !href.startsWith("blob:")) {
        return;
      }
      const now = Date.now();
      if (anchor.__palyraLastDownloadCaptureUrl === href && now - Number(anchor.__palyraLastDownloadCaptureAt || 0) < 500) {
        return;
      }
      anchor.__palyraLastDownloadCaptureUrl = href;
      anchor.__palyraLastDownloadCaptureAt = now;
      const blob = state.object_urls[href];
      if (!blob || typeof blob.arrayBuffer !== "function") {
        return;
      }
      const fileName = clampDownloadFileName(anchor.getAttribute("download") || "");
      state.pending_client_downloads += 1;
      Promise.resolve()
        .then(() => blobToBase64(blob))
        .then((contentBase64) => {
          state.client_download_entries.push({
            source_url: href,
            file_name: fileName,
            mime_type: String(blob.type || ""),
            content_base64: contentBase64,
            size_bytes: Number(blob.size || 0),
            captured_at_unix_ms: Date.now(),
            source: String(source || "browser")
          });
          trimClientDownloads();
        })
        .catch((error) => {
          push("warn", "client_download_capture_failed", error && error.message ? error.message : "client-side download capture failed", "palyra.downloads", "");
        })
        .finally(() => {
          state.pending_client_downloads = Math.max(0, Number(state.pending_client_downloads || 0) - 1);
        });
    } catch (_) {}
  };
  const anchorFromEventTarget = (target) => {
    let node = target;
    while (node && node !== document) {
      if (node.tagName && String(node.tagName).toLowerCase() === "a") {
        return node;
      }
      node = node.parentElement;
    }
    return null;
  };
  if (window.URL && typeof window.URL.createObjectURL === "function" && !state.original_create_object_url) {
    state.original_create_object_url = window.URL.createObjectURL.bind(window.URL);
    window.URL.createObjectURL = (object) => {
      const objectUrl = state.original_create_object_url(object);
      try {
        if (object && typeof Blob !== "undefined" && object instanceof Blob) {
          state.object_urls[objectUrl] = object;
        }
      } catch (_) {}
      return objectUrl;
    };
  }
  if (window.URL && typeof window.URL.revokeObjectURL === "function" && !state.original_revoke_object_url) {
    state.original_revoke_object_url = window.URL.revokeObjectURL.bind(window.URL);
    window.URL.revokeObjectURL = (objectUrl) => {
      try {
        delete state.object_urls[String(objectUrl || "")];
      } catch (_) {}
      return state.original_revoke_object_url(objectUrl);
    };
  }
  if (typeof window.HTMLAnchorElement === "function" && !state.original_anchor_click) {
    state.original_anchor_click = window.HTMLAnchorElement.prototype.click;
    window.HTMLAnchorElement.prototype.click = function(...args) {
      captureClientDownload(this, "anchor.click");
      return state.original_anchor_click.apply(this, args);
    };
  }
  if (!state.client_download_listener_installed) {
    state.client_download_listener_installed = true;
    document.addEventListener("click", (event) => {
      const anchor = anchorFromEventTarget(event && event.target);
      if (anchor) {
        captureClientDownload(anchor, "click");
      }
    }, true);
  }
  const responseHeaders = (headers) => {
    const output = [];
    try {
      if (headers && typeof headers.forEach === "function") {
        headers.forEach((value, name) => output.push({ name: String(name || ""), value: String(value || "") }));
      }
    } catch (_) {}
    return output;
  };
  if (typeof window.fetch === "function" && !state.original_fetch) {
    state.original_fetch = window.fetch.bind(window);
    window.fetch = (...args) => {
      const input = args[0];
      const requestUrl = input && typeof input === "object" && "url" in input ? input.url : input;
      const startedAt = Date.now();
      return state.original_fetch(...args).then((response) => {
        pushNetwork(response && response.url ? response.url : requestUrl, response && response.status, startedAt, responseHeaders(response && response.headers));
        return response;
      }, (error) => {
        pushNetwork(requestUrl, 0, startedAt, []);
        throw error;
      });
    };
  }
  if (typeof window.XMLHttpRequest === "function" && !state.original_xhr_open) {
    state.original_xhr_open = window.XMLHttpRequest.prototype.open;
    state.original_xhr_send = window.XMLHttpRequest.prototype.send;
    window.XMLHttpRequest.prototype.open = function(_method, url, ...rest) {
      this.__palyraNetwork = { url: normalizeNetworkUrl(url), started_at: 0 };
      return state.original_xhr_open.call(this, _method, url, ...rest);
    };
    window.XMLHttpRequest.prototype.send = function(...args) {
      const details = this.__palyraNetwork || { url: "", started_at: 0 };
      details.started_at = Date.now();
      this.addEventListener("loadend", () => {
        const headers = [];
        try {
          String(this.getAllResponseHeaders() || "").split(/\r?\n/).forEach((line) => {
            const index = line.indexOf(":");
            if (index > 0) {
              headers.push({ name: line.slice(0, index).trim(), value: line.slice(index + 1).trim() });
            }
          });
        } catch (_) {}
        pushNetwork(this.responseURL || details.url, this.status || 0, details.started_at, headers);
      }, { once: true });
      return state.original_xhr_send.apply(this, args);
    };
  }
  return true;
})()
"#;

const CHROMIUM_READ_CONSOLE_LOG_SCRIPT: &str = r#"
(() => {
  const state = window.__palyraDiagnostics;
  if (!state || !Array.isArray(state.entries)) {
    return "[]";
  }
  const MAX_CONSOLE_ENTRIES = 256;
  const MAX_CONSOLE_JSON_CHARS = 32 * 1024;
  const clampScalar = (value, maxChars) => {
    if (typeof value === "string") {
      return value.length > maxChars ? value.slice(0, maxChars) : value;
    }
    if (typeof value === "number" || typeof value === "boolean") {
      const text = String(value);
      return text.length > maxChars ? text.slice(0, maxChars) : text;
    }
    return "";
  };
  const normalizeEntry = (entry) => {
    const object = entry && typeof entry === "object" ? entry : {};
    const capturedAt = typeof object.captured_at_unix_ms === "number" && Number.isFinite(object.captured_at_unix_ms)
      ? Math.max(0, object.captured_at_unix_ms)
      : 0;
    return {
      severity: clampScalar(object.severity, 16),
      kind: clampScalar(object.kind, 64),
      message: clampScalar(object.message, 1024),
      captured_at_unix_ms: capturedAt,
      source: clampScalar(object.source, 256),
      stack_trace: clampScalar(object.stack_trace, 1024),
      page_url: clampScalar(object.page_url, 2048)
    };
  };
  const source = Array.prototype.slice.call(
    state.entries,
    Math.max(0, state.entries.length - MAX_CONSOLE_ENTRIES)
  );
  const entries = [];
  let totalChars = 2;
  for (let index = source.length - 1; index >= 0; index -= 1) {
    const entry = normalizeEntry(source[index]);
    const entryChars = JSON.stringify(entry).length + (entries.length > 0 ? 1 : 0);
    if (entries.length > 0 && totalChars + entryChars > MAX_CONSOLE_JSON_CHARS) {
      break;
    }
    if (totalChars + entryChars > MAX_CONSOLE_JSON_CHARS) {
      continue;
    }
    entries.unshift(entry);
    totalChars += entryChars;
  }
  return JSON.stringify(entries);
})()
"#;

const CHROMIUM_DRAIN_NETWORK_LOG_SCRIPT: &str = r#"
(() => {
  const state = window.__palyraDiagnostics;
  if (!state || !Array.isArray(state.network_entries)) {
    return "[]";
  }
  const MAX_NETWORK_ENTRIES = 256;
  const MAX_NETWORK_JSON_CHARS = 64 * 1024;
  const MAX_NETWORK_URL_CHARS = 2048;
  const MAX_NETWORK_HEADER_COUNT = 24;
  const MAX_NETWORK_HEADER_NAME_CHARS = 128;
  const MAX_NETWORK_HEADER_VALUE_CHARS = 256;
  const clampScalar = (value, maxChars) => {
    if (typeof value === "string") {
      return value.length > maxChars ? value.slice(0, maxChars) : value;
    }
    if (typeof value === "number" || typeof value === "boolean") {
      const text = String(value);
      return text.length > maxChars ? text.slice(0, maxChars) : text;
    }
    return "";
  };
  const normalizeHeader = (header) => {
    const object = header && typeof header === "object" ? header : {};
    return {
      name: clampScalar(object.name, MAX_NETWORK_HEADER_NAME_CHARS),
      value: clampScalar(object.value, MAX_NETWORK_HEADER_VALUE_CHARS)
    };
  };
  const normalizeEntry = (entry) => {
    const object = entry && typeof entry === "object" ? entry : {};
    const headers = Array.isArray(object.headers)
      ? Array.prototype.slice.call(object.headers, 0, MAX_NETWORK_HEADER_COUNT).map((header) => normalizeHeader(header))
      : [];
    const statusCode = typeof object.status_code === "number" && Number.isFinite(object.status_code)
      ? Math.max(0, Math.min(65535, object.status_code))
      : 0;
    const latencyMs = typeof object.latency_ms === "number" && Number.isFinite(object.latency_ms)
      ? Math.max(0, object.latency_ms)
      : 0;
    const capturedAt = typeof object.captured_at_unix_ms === "number" && Number.isFinite(object.captured_at_unix_ms)
      ? Math.max(0, object.captured_at_unix_ms)
      : 0;
    return {
      request_url: clampScalar(object.request_url, MAX_NETWORK_URL_CHARS),
      status_code: statusCode,
      latency_ms: latencyMs,
      captured_at_unix_ms: capturedAt,
      headers
    };
  };
  const source = Array.prototype.slice.call(
    state.network_entries,
    Math.max(0, state.network_entries.length - MAX_NETWORK_ENTRIES)
  );
  state.network_entries.length = 0;
  const entries = [];
  let totalChars = 2;
  for (let index = source.length - 1; index >= 0; index -= 1) {
    const entry = normalizeEntry(source[index]);
    const entryChars = JSON.stringify(entry).length + (entries.length > 0 ? 1 : 0);
    if (entries.length > 0 && totalChars + entryChars > MAX_NETWORK_JSON_CHARS) {
      break;
    }
    if (totalChars + entryChars > MAX_NETWORK_JSON_CHARS) {
      continue;
    }
    entries.unshift(entry);
    totalChars += entryChars;
  }
  return JSON.stringify(entries);
})()
"#;

const CHROMIUM_DRAIN_CLIENT_DOWNLOADS_SCRIPT: &str = r#"
(async () => {
  const state = window.__palyraDiagnostics;
  if (!state || !Array.isArray(state.client_download_entries)) {
    return "[]";
  }
  const deadline = Date.now() + 750;
  while (Number(state.pending_client_downloads || 0) > 0 && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  const MAX_CLIENT_DOWNLOAD_ENTRIES = 32;
  const MAX_CLIENT_DOWNLOAD_JSON_CHARS = 18 * 1024 * 1024;
  const MAX_URL_CHARS = 2048;
  const MAX_FILE_NAME_CHARS = 96;
  const MAX_MIME_CHARS = 128;
  const MAX_BASE64_CHARS = 12 * 1024 * 1024;
  const clampScalar = (value, maxChars) => {
    if (typeof value === "string") {
      return value.length > maxChars ? value.slice(0, maxChars) : value;
    }
    if (typeof value === "number" || typeof value === "boolean") {
      const text = String(value);
      return text.length > maxChars ? text.slice(0, maxChars) : text;
    }
    return "";
  };
  const normalizeEntry = (entry) => {
    const object = entry && typeof entry === "object" ? entry : {};
    const sizeBytes = typeof object.size_bytes === "number" && Number.isFinite(object.size_bytes)
      ? Math.max(0, object.size_bytes)
      : 0;
    const capturedAt = typeof object.captured_at_unix_ms === "number" && Number.isFinite(object.captured_at_unix_ms)
      ? Math.max(0, object.captured_at_unix_ms)
      : 0;
    return {
      source_url: clampScalar(object.source_url, MAX_URL_CHARS),
      file_name: clampScalar(object.file_name, MAX_FILE_NAME_CHARS),
      mime_type: clampScalar(object.mime_type, MAX_MIME_CHARS),
      content_base64: clampScalar(object.content_base64, MAX_BASE64_CHARS),
      size_bytes: sizeBytes,
      captured_at_unix_ms: capturedAt,
      source: clampScalar(object.source, 64)
    };
  };
  const source = Array.prototype.slice.call(
    state.client_download_entries,
    Math.max(0, state.client_download_entries.length - MAX_CLIENT_DOWNLOAD_ENTRIES)
  );
  state.client_download_entries.length = 0;
  const entries = [];
  let totalChars = 2;
  for (let index = source.length - 1; index >= 0; index -= 1) {
    const entry = normalizeEntry(source[index]);
    const entryChars = JSON.stringify(entry).length + (entries.length > 0 ? 1 : 0);
    if (entries.length > 0 && totalChars + entryChars > MAX_CLIENT_DOWNLOAD_JSON_CHARS) {
      break;
    }
    if (totalChars + entryChars > MAX_CLIENT_DOWNLOAD_JSON_CHARS) {
      continue;
    }
    entries.unshift(entry);
    totalChars += entryChars;
  }
  return JSON.stringify(entries);
})()
"#;

const MAX_CHROMIUM_CONSOLE_JSON_BYTES: usize = (DEFAULT_MAX_CONSOLE_LOG_BYTES as usize) * 4;
const MAX_CHROMIUM_NETWORK_JSON_BYTES: usize = (DEFAULT_MAX_NETWORK_LOG_BYTES as usize) * 4;
const MAX_CHROMIUM_CLIENT_DOWNLOAD_JSON_BYTES: usize =
    (DOWNLOAD_MAX_FILE_BYTES as usize * 2) + 16 * 1024;
const MAX_CHROMIUM_DOCUMENT_COOKIE_JSON_BYTES: usize = (MAX_COOKIES_PER_DOMAIN * 1536) + 4096;
const MAX_CHROMIUM_LOCAL_STORAGE_JSON_BYTES: usize =
    (MAX_STORAGE_ENTRY_VALUE_BYTES * MAX_STORAGE_ENTRIES_PER_ORIGIN * 2) + 4096;
const MAX_CHROMIUM_OBSERVE_FORM_CONTROLS: usize = 128;
const MAX_CHROMIUM_OBSERVE_FORM_VALUE_CHARS: usize = 1024;
const MAX_CHROMIUM_OBSERVE_STATE_TEXT_BYTES: usize = 16 * 1024;

#[derive(Debug, Default, Deserialize)]
struct ChromiumObserveStatePayload {
    #[serde(default)]
    html: String,
    #[serde(default)]
    form_controls: Vec<ChromiumObservedFormControl>,
    #[serde(default)]
    local_storage: ChromiumObservedStorage,
    #[serde(default)]
    session_storage: ChromiumObservedStorage,
}

#[derive(Debug, Default, Deserialize)]
struct ChromiumObservedFormControl {
    #[serde(default)]
    tag: String,
    #[serde(default, rename = "type")]
    control_type: String,
    #[serde(default)]
    id: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    selector: String,
    #[serde(default)]
    value: String,
    #[serde(default)]
    checked: Option<bool>,
    #[serde(default)]
    selected_options: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ChromiumObservedStorage {
    #[serde(default)]
    ok: bool,
    #[serde(default)]
    origin: String,
    #[serde(default)]
    entries: HashMap<String, String>,
    #[serde(default)]
    error: String,
}

fn chromium_observe_state_script() -> String {
    format!(
        r#"
(() => {{
  const MAX_FORM_CONTROLS = {max_form_controls};
  const MAX_FORM_VALUE_CHARS = {max_form_value_chars};
  const MAX_STORAGE_ENTRIES = {max_storage_entries};
  const MAX_STORAGE_KEY_CHARS = 512;
  const MAX_STORAGE_VALUE_CHARS = {max_storage_value_chars};
  const MAX_STORAGE_JSON_CHARS = {max_storage_json_chars};
  const clampScalar = (value, maxChars) => {{
    if (value === null || value === undefined) {{
      return "";
    }}
    const text = String(value);
    return text.length > maxChars ? text.slice(0, maxChars) : text;
  }};
  const selectorFor = (element, tag) => {{
    const id = clampScalar(element && element.id, 128).trim();
    if (id) {{
      return `#${{id}}`;
    }}
    const name = clampScalar(element && element.getAttribute && element.getAttribute("name"), 128).trim();
    if (name) {{
      return `[name="${{name.replace(/"/g, '\\"')}}"]`;
    }}
    return tag || "control";
  }};
  const sensitiveHint = (value) => {{
    const text = clampScalar(value, 256).toLowerCase();
    return ["auth", "cookie", "csrf", "jwt", "password", "passwd", "secret", "session", "token"].some((needle) => text.includes(needle));
  }};
  const sensitiveControl = (element, type) => {{
    if (type === "password" || type === "hidden" || type === "file") {{
      return true;
    }}
    return ["name", "id", "autocomplete", "placeholder", "aria-label", "title"].some((attr) => {{
      try {{
        return sensitiveHint(element && element.getAttribute && element.getAttribute(attr));
      }} catch (_) {{
        return false;
      }}
    }});
  }};
  const cloneRoot = document.documentElement ? document.documentElement.cloneNode(true) : null;
  const liveControls = Array.prototype.slice.call(
    document.querySelectorAll("input, textarea, select"),
    0,
    MAX_FORM_CONTROLS
  );
  const clonedControls = cloneRoot
    ? Array.prototype.slice.call(cloneRoot.querySelectorAll("input, textarea, select"), 0, MAX_FORM_CONTROLS)
    : [];
  const formControls = [];
  liveControls.forEach((element, index) => {{
    const cloned = clonedControls[index];
    const tag = clampScalar((element.tagName || "").toLowerCase(), 32);
    const type = tag === "input"
      ? clampScalar((element.getAttribute("type") || "text").toLowerCase(), 64)
      : tag;
    const value = clampScalar(element.value, MAX_FORM_VALUE_CHARS);
    const clonedValue = sensitiveControl(element, type) ? "<redacted>" : value;
    if (cloned) {{
      try {{
        cloned.setAttribute("value", clonedValue);
        if (tag === "textarea") {{
          cloned.textContent = clonedValue;
        }}
        if (tag === "input" && (type === "checkbox" || type === "radio")) {{
          if (element.checked) {{
            cloned.setAttribute("checked", "true");
          }} else {{
            cloned.removeAttribute("checked");
          }}
        }}
        if (tag === "select") {{
          const liveOptions = Array.prototype.slice.call(element.options || []);
          const clonedOptions = Array.prototype.slice.call(cloned.options || []);
          liveOptions.forEach((option, optionIndex) => {{
            const clonedOption = clonedOptions[optionIndex];
            if (!clonedOption) {{
              return;
            }}
            if (option.selected) {{
              clonedOption.setAttribute("selected", "true");
            }} else {{
              clonedOption.removeAttribute("selected");
            }}
          }});
        }}
      }} catch (_) {{}}
    }}
    const selectedOptions = tag === "select"
      ? Array.prototype.slice.call(element.selectedOptions || [], 0, 16)
          .map((option) => clampScalar(option.value || option.textContent, MAX_FORM_VALUE_CHARS))
      : [];
    formControls.push({{
      tag,
      type,
      id: clampScalar(element.id, 128),
      name: clampScalar(element.getAttribute("name"), 128),
      selector: selectorFor(element, tag),
      value,
      checked: tag === "input" && (type === "checkbox" || type === "radio") ? Boolean(element.checked) : null,
      selected_options: selectedOptions
    }});
  }});
  const readStorage = (storageGetter) => {{
    try {{
      const storage = storageGetter();
      if (!storage) {{
        return {{ ok: true, entries: {{}} }};
      }}
      const entries = {{}};
      let totalChars = 2;
      let count = 0;
      const length = Math.min(Number(storage.length || 0), MAX_STORAGE_ENTRIES * 4);
      for (let index = 0; index < length; index += 1) {{
        const rawKey = storage.key(index);
        const key = clampScalar(rawKey, MAX_STORAGE_KEY_CHARS).trim();
        if (!key || Object.prototype.hasOwnProperty.call(entries, key)) {{
          continue;
        }}
        const value = clampScalar(storage.getItem(rawKey), MAX_STORAGE_VALUE_CHARS);
        const entryChars = JSON.stringify(key).length + JSON.stringify(value).length + 4;
        if (count > 0 && totalChars + entryChars > MAX_STORAGE_JSON_CHARS) {{
          break;
        }}
        if (totalChars + entryChars > MAX_STORAGE_JSON_CHARS) {{
          continue;
        }}
        entries[key] = value;
        totalChars += entryChars;
        count += 1;
        if (count >= MAX_STORAGE_ENTRIES) {{
          break;
        }}
      }}
      return {{ ok: true, entries }};
    }} catch (error) {{
      return {{
        ok: false,
        entries: {{}},
        error: clampScalar((error && (error.message || error)) || "", 256)
      }};
    }}
  }};
  const origin = clampScalar((window.location && window.location.origin) || "", 2048);
  return JSON.stringify({{
    html: cloneRoot ? cloneRoot.outerHTML : (document.documentElement ? document.documentElement.outerHTML : ""),
    origin,
    form_controls: formControls,
    local_storage: Object.assign({{ origin }}, readStorage(() => window.localStorage)),
    session_storage: Object.assign({{ origin }}, readStorage(() => window.sessionStorage))
  }});
}})()
"#,
        max_form_controls = MAX_CHROMIUM_OBSERVE_FORM_CONTROLS,
        max_form_value_chars = MAX_CHROMIUM_OBSERVE_FORM_VALUE_CHARS,
        max_storage_entries = MAX_STORAGE_ENTRIES_PER_ORIGIN,
        max_storage_value_chars = MAX_STORAGE_ENTRY_VALUE_BYTES,
        max_storage_json_chars = MAX_CHROMIUM_LOCAL_STORAGE_JSON_BYTES
    )
}

fn decode_chromium_observe_state_value(
    value: serde_json::Value,
) -> Result<ChromiumObserveStatePayload, String> {
    serde_json::from_value::<ChromiumObserveStatePayload>(decode_chromium_json_script_value(value))
        .map_err(|error| format!("failed to parse Chromium observe state: {error}"))
}

fn page_body_with_chromium_observe_state(payload: ChromiumObserveStatePayload) -> String {
    let page_body = payload.html.clone();
    let summary = build_chromium_observe_state_summary(&payload);
    if summary.trim().is_empty() {
        return page_body;
    }
    format!(
        "{page_body}\n<section id=\"palyra-observe-state\" aria-label=\"Palyra observed browser state\"><pre>{}</pre></section>",
        escape_html_text(summary.as_str())
    )
}

fn build_chromium_observe_state_summary(payload: &ChromiumObserveStatePayload) -> String {
    let mut lines = Vec::new();
    for control in payload.form_controls.iter().take(MAX_CHROMIUM_OBSERVE_FORM_CONTROLS) {
        lines.push(chromium_observed_form_control_line(control));
    }
    append_chromium_observed_storage_lines(&mut lines, "localStorage", &payload.local_storage);
    append_chromium_observed_storage_lines(&mut lines, "sessionStorage", &payload.session_storage);
    truncate_utf8_bytes(lines.join("\n").as_str(), MAX_CHROMIUM_OBSERVE_STATE_TEXT_BYTES)
}

fn chromium_observed_form_control_line(control: &ChromiumObservedFormControl) -> String {
    let mut parts = vec!["browser_form_control".to_owned()];
    append_observe_part(&mut parts, "selector", control.selector.as_str(), 128);
    append_observe_part(&mut parts, "tag", control.tag.as_str(), 32);
    append_observe_part(&mut parts, "type", control.control_type.as_str(), 64);
    append_observe_part(&mut parts, "name", control.name.as_str(), 128);
    append_observe_part(&mut parts, "id", control.id.as_str(), 128);
    if let Some(checked) = control.checked {
        parts.push(format!("checked={checked}"));
    }
    if !control.selected_options.is_empty() {
        let selected = control
            .selected_options
            .iter()
            .take(16)
            .map(|value| line_quote(sanitize_chromium_observed_form_value(control, value).as_str()))
            .collect::<Vec<_>>()
            .join(",");
        parts.push(format!("selected_options=[{selected}]"));
    }
    parts.push(format!(
        "value={}",
        line_quote(sanitize_chromium_observed_form_value(control, control.value.as_str()).as_str())
    ));
    parts.join(" ")
}

fn sanitize_chromium_observed_form_value(
    control: &ChromiumObservedFormControl,
    raw_value: &str,
) -> String {
    let control_type = control.control_type.to_ascii_lowercase();
    if matches!(control_type.as_str(), "password" | "hidden" | "file") {
        return "<redacted>".to_owned();
    }
    let key = [
        control.name.as_str(),
        control.id.as_str(),
        control.selector.as_str(),
        control.tag.as_str(),
        control.control_type.as_str(),
    ]
    .into_iter()
    .filter(|value| !value.trim().is_empty())
    .collect::<Vec<_>>()
    .join(" ");
    sanitize_debug_map_value(key.as_str(), raw_value, MAX_CHROMIUM_OBSERVE_FORM_VALUE_CHARS)
}

fn append_chromium_observed_storage_lines(
    lines: &mut Vec<String>,
    storage_kind: &str,
    storage: &ChromiumObservedStorage,
) {
    if !storage.ok {
        if !storage.error.trim().is_empty() {
            lines.push(format!(
                "browser_storage kind={} error={}",
                storage_kind,
                line_quote(sanitize_debug_text(storage.error.as_str(), 256).as_str())
            ));
        }
        return;
    }
    if storage.entries.is_empty() {
        return;
    }
    let origin = if storage.origin.trim().is_empty() {
        String::new()
    } else {
        normalize_url_with_redaction(storage.origin.as_str())
    };
    let mut entries = storage.entries.iter().collect::<Vec<_>>();
    entries.sort_by(|left, right| left.0.cmp(right.0));
    for (key, value) in entries.into_iter().take(MAX_STORAGE_ENTRIES_PER_ORIGIN) {
        let key_display = sanitize_debug_text(key.as_str(), 128);
        let value_display =
            sanitize_debug_map_value(key.as_str(), value.as_str(), MAX_STORAGE_ENTRY_VALUE_BYTES);
        lines.push(format!(
            "browser_storage kind={} origin={} key={} value={}",
            storage_kind,
            line_quote(origin.as_str()),
            line_quote(key_display.as_str()),
            line_quote(value_display.as_str())
        ));
    }
}

fn append_observe_part(parts: &mut Vec<String>, name: &str, value: &str, max_bytes: usize) {
    if value.trim().is_empty() {
        return;
    }
    parts.push(format!("{}={}", name, line_quote(truncate_utf8_bytes(value, max_bytes).as_str())));
}

fn line_quote(value: &str) -> String {
    format!("\"{}\"", line_escape(value))
}

fn line_escape(value: &str) -> String {
    let mut output = String::new();
    for character in value.chars() {
        match character {
            '\\' => output.push_str("\\\\"),
            '"' => output.push_str("\\\""),
            '\r' | '\n' | '\t' => output.push(' '),
            _ => output.push(character),
        }
    }
    output
}

fn escape_html_text(value: &str) -> String {
    let mut output = String::new();
    for character in value.chars() {
        match character {
            '&' => output.push_str("&amp;"),
            '<' => output.push_str("&lt;"),
            '>' => output.push_str("&gt;"),
            _ => output.push(character),
        }
    }
    output
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
    private_target_policy: Arc<ChromiumPrivateTargetPolicy>,
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
        let private_target_policy =
            Arc::new(ChromiumPrivateTargetPolicy::new(allow_private_targets));
        let task = tokio::spawn(run_chromium_session_socks5_proxy(
            listener,
            Arc::clone(&private_target_policy),
            shutdown_rx,
        ));
        Ok(Self {
            proxy_uri: format!("socks5://{local_addr}"),
            private_target_policy,
            shutdown_tx: Some(shutdown_tx),
            task,
        })
    }

    pub(crate) fn private_target_policy(&self) -> Arc<ChromiumPrivateTargetPolicy> {
        Arc::clone(&self.private_target_policy)
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

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub(crate) enum ChromiumPrivateTargetScope {
    Network { host: String, port: u16 },
    File(PathBuf),
}

#[derive(Debug)]
pub(crate) struct ChromiumPrivateTargetPolicy {
    allow_session_private_targets: bool,
    scoped_targets: std::sync::Mutex<HashMap<ChromiumPrivateTargetScope, usize>>,
    retained_targets: std::sync::Mutex<HashSet<ChromiumPrivateTargetScope>>,
}

#[derive(Debug)]
pub(crate) struct ChromiumScopedPrivateTarget {
    policy: Arc<ChromiumPrivateTargetPolicy>,
    scope: ChromiumPrivateTargetScope,
}

impl ChromiumPrivateTargetPolicy {
    pub(crate) fn new(allow_session_private_targets: bool) -> Self {
        Self {
            allow_session_private_targets,
            scoped_targets: std::sync::Mutex::new(HashMap::new()),
            retained_targets: std::sync::Mutex::new(HashSet::new()),
        }
    }

    pub(crate) fn allows_url(&self, raw_url: &str) -> bool {
        if self.allow_session_private_targets {
            return true;
        }
        let Ok(Some(scope)) = ChromiumPrivateTargetScope::from_url(raw_url) else {
            return false;
        };
        self.allows_scope(&scope)
    }

    pub(crate) fn allows_host_port(&self, host: &str, port: u16) -> bool {
        if self.allow_session_private_targets {
            return true;
        }
        let Ok(scope) = ChromiumPrivateTargetScope::network(host, port) else {
            return false;
        };
        self.allows_scope(&scope)
    }

    pub(crate) fn scoped_url_allowance(
        self: &Arc<Self>,
        raw_url: &str,
    ) -> Result<Option<ChromiumScopedPrivateTarget>, String> {
        if self.allow_session_private_targets {
            return Ok(None);
        }
        let Some(scope) = ChromiumPrivateTargetScope::from_url(raw_url)? else {
            return Ok(None);
        };
        let mut scoped_targets = self
            .scoped_targets
            .lock()
            .map_err(|_| "private-target policy lock was poisoned".to_owned())?;
        let count = scoped_targets.entry(scope.clone()).or_insert(0);
        *count = count.saturating_add(1);
        Ok(Some(ChromiumScopedPrivateTarget { policy: Arc::clone(self), scope }))
    }

    pub(crate) fn retain_url_allowance(&self, raw_url: &str) -> Result<(), String> {
        if self.allow_session_private_targets {
            return Ok(());
        }
        let Some(scope) = ChromiumPrivateTargetScope::from_url(raw_url)? else {
            return Ok(());
        };
        self.retained_targets
            .lock()
            .map_err(|_| "private-target policy lock was poisoned".to_owned())?
            .insert(scope);
        Ok(())
    }

    fn allows_scope(&self, scope: &ChromiumPrivateTargetScope) -> bool {
        if self
            .retained_targets
            .lock()
            .map(|retained_targets| retained_targets.contains(scope))
            .unwrap_or(false)
        {
            return true;
        }
        self.scoped_targets
            .lock()
            .map(|scoped_targets| scoped_targets.contains_key(scope))
            .unwrap_or(false)
    }

    fn release_scope(&self, scope: &ChromiumPrivateTargetScope) {
        let Ok(mut scoped_targets) = self.scoped_targets.lock() else {
            return;
        };
        match scoped_targets.get_mut(scope) {
            Some(count) if *count > 1 => *count -= 1,
            Some(_) => {
                scoped_targets.remove(scope);
            }
            None => {}
        }
    }
}

impl ChromiumPrivateTargetScope {
    fn from_url(raw_url: &str) -> Result<Option<Self>, String> {
        if raw_url.eq_ignore_ascii_case("about:blank") {
            return Ok(None);
        }
        let url = Url::parse(raw_url).map_err(|error| format!("invalid URL: {error}"))?;
        if url.scheme() == "file" {
            let file_path =
                url.to_file_path().map_err(|_| "file URL path is invalid".to_owned())?;
            let canonical = fs::canonicalize(file_path.as_path())
                .map_err(|error| format!("failed to resolve local file target: {error}"))?;
            return Ok(Some(Self::File(canonical)));
        }
        let (host, port) = extract_target_host_port(&url)?;
        Ok(Some(Self::network(host, port)?))
    }

    fn network(host: &str, port: u16) -> Result<Self, String> {
        let normalized_host = if let Some(address) = netguard::parse_host_ip_literal(host)? {
            address.to_string()
        } else {
            normalize_dns_host_cache_key(host)
        };
        if normalized_host.is_empty() {
            return Err("private-target scope host must not be empty".to_owned());
        }
        Ok(Self::Network { host: normalized_host, port })
    }
}

impl Drop for ChromiumScopedPrivateTarget {
    fn drop(&mut self) {
        self.policy.release_scope(&self.scope);
    }
}

pub(crate) async fn run_chromium_session_socks5_proxy(
    listener: tokio::net::TcpListener,
    private_target_policy: Arc<ChromiumPrivateTargetPolicy>,
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
                        let private_target_policy = Arc::clone(&private_target_policy);
                        tokio::spawn(async move {
                            if let Err(error) =
                                handle_chromium_session_socks5_client(stream, private_target_policy)
                                    .await
                            {
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
    private_target_policy: Arc<ChromiumPrivateTargetPolicy>,
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

    let allow_private_targets =
        private_target_policy.allows_host_port(target_label.as_str(), target_port);
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
        .idle_browser_timeout(chromium_transport_idle_timeout(chromium.startup_timeout))
        .user_data_dir(Some(profile_dir.path().to_path_buf()))
        .args(chromium_args)
        .proxy_server(proxy_server);
    if let Some(path) = chromium_path {
        builder.path(Some(path));
    }
    builder.build().map_err(|error| format!("failed to build Chromium launch options: {error}"))
}

fn chromium_transport_idle_timeout(startup_timeout: Duration) -> Duration {
    startup_timeout.max(Duration::from_millis(DEFAULT_SESSION_IDLE_TTL_MS))
}

pub(crate) fn parse_chromium_remote_ip_literal(raw: &str) -> Option<IpAddr> {
    let trimmed = raw.trim().trim_start_matches('[').trim_end_matches(']');
    trimmed.parse::<IpAddr>().ok()
}

pub(crate) fn record_chromium_remote_ip_incident(
    response_url: Option<&str>,
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
    if parsed_remote_ip.is_loopback()
        && chromium_loopback_remote_ip_is_expected_proxy_hop(response_url, allow_private_targets)
    {
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

pub(crate) fn chromium_loopback_remote_ip_is_expected_proxy_hop(
    response_url: Option<&str>,
    allow_private_targets: bool,
) -> bool {
    let Some(response_url) = response_url else {
        return false;
    };

    // Chromium reports the local SOCKS5 proxy as the response remote IP. The
    // actual origin address is enforced by request interception and by the
    // per-session proxy before CONNECT succeeds.
    validate_target_url_blocking(response_url, allow_private_targets).is_ok()
}

pub(crate) fn configure_chromium_tab(
    tab: &Arc<HeadlessTab>,
    private_target_policy: Arc<ChromiumPrivateTargetPolicy>,
    network_log: Arc<std::sync::Mutex<VecDeque<NetworkLogEntryInternal>>>,
    download_captures: Arc<std::sync::Mutex<VecDeque<ChromiumClientDownload>>>,
    timeout: Duration,
    security_incident: Arc<std::sync::Mutex<Option<String>>>,
) -> Result<(), String> {
    tab.set_default_timeout(timeout);
    tab.enable_fetch(None, Some(false))
        .map_err(|error| format!("failed to enable Chromium fetch interception: {error}"))?;
    let request_policy = Arc::clone(&private_target_policy);
    let request_interceptor =
        Arc::new(move |_transport, _session_id, intercepted: Fetch::events::RequestPausedEvent| {
            let request_url = intercepted.params.request.url.as_str();
            let allow_private_targets = request_policy.allows_url(request_url);
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
    let network_log_buffer = Arc::clone(&network_log);
    tab.register_response_handling(
        CHROMIUM_NETWORK_LOG_HANDLER_NAME,
        Box::new(move |response, _fetch_body| {
            let entry = chromium_network_log_entry_from_response(&response);
            if let Ok(mut guard) = network_log_buffer.lock() {
                guard.push_back(entry);
                while guard.len() > CHROMIUM_PENDING_NETWORK_LOG_MAX_ENTRIES {
                    let _ = guard.pop_front();
                }
            }
        }),
    )
    .map_err(|error| format!("failed to register Chromium network log callback: {error}"))?;
    let download_capture_buffer = Arc::clone(&download_captures);
    tab.register_response_handling(
        CHROMIUM_DOWNLOAD_CAPTURE_HANDLER_NAME,
        Box::new(move |response, fetch_body| {
            let Some(content_disposition) =
                chromium_header_value(&response.response.headers, "content-disposition")
            else {
                return;
            };
            if !content_disposition_is_attachment(content_disposition.as_str()) {
                return;
            }
            let file_name = content_disposition_attachment_file_name(content_disposition.as_str())
                .unwrap_or_else(|| infer_download_file_name(response.response.url.as_str()));
            let body = match fetch_body() {
                Ok(value) => value,
                Err(error) => {
                    warn!(
                        error = %error,
                        url = normalize_url_with_redaction(response.response.url.as_str()).as_str(),
                        "failed to read Chromium attachment response body"
                    );
                    return;
                }
            };
            let content = if body.base_64_encoded {
                match base64::engine::general_purpose::STANDARD.decode(body.body.as_bytes()) {
                    Ok(value) => value,
                    Err(error) => {
                        warn!(
                            error = %error,
                            url = normalize_url_with_redaction(response.response.url.as_str()).as_str(),
                            "failed to decode Chromium attachment response body"
                        );
                        return;
                    }
                }
            } else {
                body.body.into_bytes()
            };
            if content.is_empty() || content.len() as u64 > DOWNLOAD_MAX_FILE_BYTES {
                return;
            }
            let header_content_type = chromium_header_value(&response.response.headers, "content-type")
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| response.response.mime_type.clone());
            let mime_type = sniff_download_mime_type(
                Some(header_content_type.as_str()),
                file_name.as_str(),
                content.as_slice(),
            );
            if let Ok(mut guard) = download_capture_buffer.lock() {
                guard.push_back(ChromiumClientDownload {
                    source_url: response.response.url,
                    file_name,
                    mime_type,
                    content,
                });
                while guard.len() > CHROMIUM_PENDING_DOWNLOAD_CAPTURE_MAX_ENTRIES {
                    let _ = guard.pop_front();
                }
            }
        }),
    )
    .map_err(|error| format!("failed to register Chromium download capture callback: {error}"))?;
    tab.call_method(Page::AddScriptToEvaluateOnNewDocument {
        source: CHROMIUM_PAGE_DIAGNOSTICS_SCRIPT.to_owned(),
        world_name: None,
        include_command_line_api: None,
        run_immediately: None,
    })
    .map_err(|error| format!("failed to register Chromium page diagnostics hooks: {error}"))?;
    let remote_ip_guard = Arc::clone(&security_incident);
    let response_policy = Arc::clone(&private_target_policy);
    tab.register_response_handling(
        CHROMIUM_REMOTE_IP_GUARD_HANDLER_NAME,
        Box::new(move |response, _fetch_body| {
            let allow_private_targets = response_policy.allows_url(response.response.url.as_str());
            record_chromium_remote_ip_incident(
                Some(response.response.url.as_str()),
                response.response.remote_ip_address.as_deref(),
                allow_private_targets,
                &remote_ip_guard,
            );
        }),
    )
    .map_err(|error| format!("failed to register Chromium response guard callback: {error}"))?;
    Ok(())
}

fn chromium_network_log_entry_from_response(
    response: &Network::events::ResponseReceivedEventParams,
) -> NetworkLogEntryInternal {
    let latency_ms =
        response.response.timing.as_ref().map(chromium_response_latency_ms).unwrap_or(0);
    NetworkLogEntryInternal {
        request_url: normalize_url_with_redaction(response.response.url.as_str()),
        status_code: response.response.status.min(u32::from(u16::MAX)) as u16,
        timing_bucket: timing_bucket_for_latency(latency_ms).to_owned(),
        latency_ms,
        captured_at_unix_ms: current_unix_ms(),
        headers: chromium_network_log_headers(&response.response.headers),
    }
}

fn chromium_response_latency_ms(timing: &Network::ResourceTiming) -> u64 {
    if timing.receive_headers_end.is_sign_positive() {
        timing.receive_headers_end.round().max(0.0) as u64
    } else {
        0
    }
}

fn chromium_network_log_headers(headers: &Network::Headers) -> Vec<NetworkLogHeaderInternal> {
    let Some(value) = headers.0.as_ref() else {
        return Vec::new();
    };
    let Some(object) = value.as_object() else {
        return Vec::new();
    };
    let mut output = object
        .iter()
        .take(MAX_NETWORK_LOG_HEADER_COUNT)
        .map(|(name, value)| {
            let header_name = name.to_ascii_lowercase();
            let raw_value =
                value.as_str().map(ToOwned::to_owned).unwrap_or_else(|| value.to_string());
            let sanitized =
                sanitize_single_network_header(header_name.as_str(), raw_value.as_str());
            NetworkLogHeaderInternal { name: header_name, value: sanitized }
        })
        .collect::<Vec<_>>();
    output.sort_by(|left, right| left.name.cmp(&right.name));
    output
}

fn chromium_header_value(headers: &Network::Headers, target_name: &str) -> Option<String> {
    headers.0.as_ref().and_then(serde_json::Value::as_object).and_then(|object| {
        object.iter().find_map(|(name, value)| {
            if name.eq_ignore_ascii_case(target_name) {
                value.as_str().map(str::to_owned).or_else(|| Some(value.to_string()))
            } else {
                None
            }
        })
    })
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
    private_target_policy: Arc<ChromiumPrivateTargetPolicy>,
    network_log: Arc<std::sync::Mutex<VecDeque<NetworkLogEntryInternal>>>,
    download_captures: Arc<std::sync::Mutex<VecDeque<ChromiumClientDownload>>>,
    timeout: Duration,
    security_incident: Arc<std::sync::Mutex<Option<String>>>,
    failure_prefix: &str,
) -> Result<Arc<HeadlessTab>, String> {
    for attempt in 1..=CHROMIUM_NEW_TAB_MAX_ATTEMPTS {
        match browser.new_tab() {
            Ok(tab) => {
                configure_chromium_tab(
                    &tab,
                    Arc::clone(&private_target_policy),
                    Arc::clone(&network_log),
                    Arc::clone(&download_captures),
                    timeout,
                    security_incident,
                )?;
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
    let private_target_policy = proxy.private_target_policy();
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
            let mut network_logs = HashMap::new();
            let mut download_captures = HashMap::new();
            for tab_id in tab_order.iter() {
                let network_log = Arc::new(std::sync::Mutex::new(VecDeque::new()));
                let download_capture = Arc::new(std::sync::Mutex::new(VecDeque::new()));
                let tab = create_configured_chromium_tab_with_retry(
                    &browser,
                    Arc::clone(&private_target_policy),
                    Arc::clone(&network_log),
                    Arc::clone(&download_capture),
                    navigation_timeout,
                    Arc::clone(&security_incident),
                    "failed to create Chromium tab for session restore",
                )?;
                tabs.insert(tab_id.clone(), tab);
                network_logs.insert(tab_id.clone(), network_log);
                download_captures.insert(tab_id.clone(), download_capture);
            }
            Ok(ChromiumSessionState {
                browser,
                tabs,
                network_logs,
                download_captures,
                private_target_policy,
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
    let timeout_ms = {
        let sessions = runtime.sessions.lock().await;
        let Some(session) = sessions.get(session_id) else {
            return Err("session_not_found".to_owned());
        };
        session.budget.max_navigation_timeout_ms.max(1)
    };
    let (browser, private_target_policy, security_incident) = {
        let chromium_sessions = runtime.chromium_sessions.lock().await;
        let Some(chromium_session) = chromium_sessions.get(session_id) else {
            return Err("chromium_session_not_found".to_owned());
        };
        (
            Arc::clone(&chromium_session.browser),
            Arc::clone(&chromium_session.private_target_policy),
            Arc::clone(&chromium_session.security_incident),
        )
    };
    let tab = run_chromium_blocking("chromium open tab", move || {
        let network_log = Arc::new(std::sync::Mutex::new(VecDeque::new()));
        let download_capture = Arc::new(std::sync::Mutex::new(VecDeque::new()));
        let tab = create_configured_chromium_tab_with_retry(
            &browser,
            private_target_policy,
            Arc::clone(&network_log),
            Arc::clone(&download_capture),
            Duration::from_millis(timeout_ms),
            security_incident,
            "failed to allocate Chromium tab",
        )?;
        Ok((tab, network_log, download_capture))
    })
    .await?;
    let (tab, network_log, download_capture) = tab;
    let mut chromium_sessions = runtime.chromium_sessions.lock().await;
    let Some(chromium_session) = chromium_sessions.get_mut(session_id) else {
        return Err("chromium_session_not_found".to_owned());
    };
    chromium_session.tabs.insert(tab_id.to_owned(), tab);
    chromium_session.network_logs.insert(tab_id.to_owned(), network_log);
    chromium_session.download_captures.insert(tab_id.to_owned(), download_capture);
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
        chromium_session.network_logs.remove(tab_id);
        chromium_session.download_captures.remove(tab_id);
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

pub(crate) async fn chromium_drain_pending_network_log(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
) -> Result<Vec<NetworkLogEntryInternal>, String> {
    let network_log = {
        let chromium_sessions = runtime.chromium_sessions.lock().await;
        let Some(chromium_session) = chromium_sessions.get(session_id) else {
            return Err("chromium_session_not_found".to_owned());
        };
        chromium_session
            .network_logs
            .get(tab_id)
            .cloned()
            .ok_or_else(|| "chromium_network_log_not_found".to_owned())?
    };
    let mut guard = network_log
        .lock()
        .map_err(|_| "failed to inspect Chromium network log state".to_owned())?;
    Ok(guard.drain(..).collect())
}

async fn chromium_drain_response_downloads(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
) -> Result<Vec<ChromiumClientDownload>, String> {
    let download_capture = {
        let chromium_sessions = runtime.chromium_sessions.lock().await;
        let Some(chromium_session) = chromium_sessions.get(session_id) else {
            return Err("chromium_session_not_found".to_owned());
        };
        chromium_session
            .download_captures
            .get(tab_id)
            .cloned()
            .ok_or_else(|| "chromium_download_capture_not_found".to_owned())?
    };
    let mut guard = download_capture
        .lock()
        .map_err(|_| "chromium download capture lock poisoned".to_owned())?;
    Ok(guard.drain(..).collect())
}

pub(crate) async fn chromium_tab_and_private_target_policy_for_session(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
) -> Result<(Arc<HeadlessTab>, Arc<ChromiumPrivateTargetPolicy>), String> {
    let chromium_sessions = runtime.chromium_sessions.lock().await;
    let Some(chromium_session) = chromium_sessions.get(session_id) else {
        return Err("chromium_session_not_found".to_owned());
    };
    let Some(tab) = chromium_session.tabs.get(tab_id) else {
        return Err("chromium_tab_not_found".to_owned());
    };
    Ok((Arc::clone(tab), Arc::clone(&chromium_session.private_target_policy)))
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

async fn chromium_selector_not_found_error(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
    selector: &str,
    action_name: &str,
) -> String {
    let sessions = runtime.sessions.lock().await;
    let cached_tab = sessions.get(session_id).and_then(|session| session.tabs.get(tab_id));
    let cached_body = cached_tab.map(|tab| tab.last_page_body.as_str()).unwrap_or_default();
    let cached_url = cached_tab.and_then(|tab| tab.last_url.as_deref()).unwrap_or_default();
    selector_not_found_error_from_cached_snapshot(
        selector,
        action_name,
        tab_id,
        cached_body,
        cached_url,
    )
}

fn selector_not_found_error_from_cached_snapshot(
    selector: &str,
    action_name: &str,
    tab_id: &str,
    cached_page_body: &str,
    cached_url: &str,
) -> String {
    if find_matching_html_tag(selector, cached_page_body).is_some() {
        let location = chromium_action_context(tab_id, cached_url);
        return format!(
            "selector '{selector}' was not found in the live Chromium DOM for {action_name}, but the last observe snapshot for {location} still contained it; call observe or reload to refresh the active tab, verify any local server is still running, and retry only if the selector is present in the current snapshot"
        );
    }
    format!("selector '{selector}' was not found")
}

fn chromium_action_context(tab_id: &str, page_url: &str) -> String {
    let page_url = normalize_url_with_redaction(page_url);
    if page_url.is_empty() {
        format!("active tab {tab_id}")
    } else {
        format!("active tab {tab_id} at {page_url}")
    }
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
        let observe_state_script = chromium_observe_state_script();
        let page_body = match tab.evaluate(observe_state_script.as_str(), false) {
            Ok(result) => result
                .value
                .ok_or_else(|| "Chromium observe state returned no value".to_owned())
                .and_then(decode_chromium_observe_state_value)
                .map(page_body_with_chromium_observe_state)
                .or_else(|_| {
                    tab.get_content()
                        .map_err(|error| format!("failed to read Chromium DOM content: {error}"))
                })?,
            Err(_) => tab
                .get_content()
                .map_err(|error| format!("failed to read Chromium DOM content: {error}"))?,
        };
        let title = tab.get_title().unwrap_or_default();
        let page_url = tab.get_url();
        Ok(ChromiumObserveSnapshot { page_body, title, page_url })
    })
    .await?;
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    Ok(clamp_chromium_snapshot(snapshot, max_response_bytes, max_title_bytes))
}

async fn chromium_install_page_diagnostics(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
) -> Result<(), String> {
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    let tab = chromium_tab_for_session(runtime, session_id, tab_id).await?;
    run_chromium_blocking("chromium install page diagnostics", move || {
        tab.evaluate(CHROMIUM_PAGE_DIAGNOSTICS_SCRIPT, false).map_err(|error| {
            format!("failed to install Chromium page diagnostics hooks: {error}")
        })?;
        Ok(())
    })
    .await?;
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    Ok(())
}

async fn chromium_read_console_log(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
) -> Result<Vec<BrowserConsoleEntryInternal>, String> {
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    let tab = chromium_tab_for_session(runtime, session_id, tab_id).await?;
    let value = run_chromium_blocking("chromium read console log", move || {
        let value = tab
            .evaluate(CHROMIUM_READ_CONSOLE_LOG_SCRIPT, false)
            .map_err(|error| format!("failed to read Chromium console diagnostics: {error}"))?
            .value
            .unwrap_or_else(|| serde_json::Value::String("[]".to_owned()));
        Ok(decode_chromium_console_entries_value(value))
    })
    .await?;
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    Ok(parse_chromium_console_entries(value))
}

async fn chromium_read_local_storage(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
) -> Result<Option<(String, HashMap<String, String>)>, String> {
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    let tab = chromium_tab_for_session(runtime, session_id, tab_id).await?;
    let script = chromium_read_local_storage_script();
    let value = run_chromium_blocking("chromium read localStorage", move || {
        let value = tab
            .evaluate(script.as_str(), false)
            .map_err(|error| format!("failed to read Chromium localStorage: {error}"))?
            .value
            .unwrap_or_else(|| serde_json::Value::String("{}".to_owned()));
        Ok(decode_chromium_json_script_value(value))
    })
    .await?;
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    parse_chromium_local_storage_snapshot(value)
}

async fn chromium_read_document_cookies(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
) -> Result<Vec<CookieUpdate>, String> {
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    let tab = chromium_tab_for_session(runtime, session_id, tab_id).await?;
    let script = chromium_read_document_cookies_script();
    let value = run_chromium_blocking("chromium read document.cookie", move || {
        let value = tab
            .evaluate(script.as_str(), false)
            .map_err(|error| format!("failed to read Chromium document.cookie: {error}"))?
            .value
            .unwrap_or_else(|| serde_json::Value::String("{}".to_owned()));
        Ok(decode_chromium_json_script_value(value))
    })
    .await?;
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    parse_chromium_document_cookie_snapshot(value)
}

async fn chromium_drain_page_network_log(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
) -> Result<Vec<NetworkLogEntryInternal>, String> {
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    let tab = chromium_tab_for_session(runtime, session_id, tab_id).await?;
    let value = run_chromium_blocking("chromium drain page network log", move || {
        let value = tab
            .evaluate(CHROMIUM_DRAIN_NETWORK_LOG_SCRIPT, false)
            .map_err(|error| format!("failed to read Chromium page network diagnostics: {error}"))?
            .value
            .unwrap_or_else(|| serde_json::Value::String("[]".to_owned()));
        Ok(decode_chromium_network_entries_value(value))
    })
    .await?;
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    Ok(parse_chromium_page_network_entries(value))
}

pub(crate) async fn chromium_drain_client_downloads(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
) -> Result<Vec<ChromiumClientDownload>, String> {
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    let tab = chromium_tab_for_session(runtime, session_id, tab_id).await?;
    let value = run_chromium_blocking("chromium drain client downloads", move || {
        let value = tab
            .evaluate(CHROMIUM_DRAIN_CLIENT_DOWNLOADS_SCRIPT, true)
            .map_err(|error| format!("failed to read Chromium client downloads: {error}"))?
            .value
            .unwrap_or_else(|| serde_json::Value::String("[]".to_owned()));
        Ok(decode_chromium_client_download_entries_value(value))
    })
    .await?;
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    let mut downloads = parse_chromium_client_download_entries(value);
    downloads.extend(chromium_drain_response_downloads(runtime, session_id, tab_id).await?);
    Ok(downloads)
}

fn decode_chromium_console_entries_value(value: serde_json::Value) -> serde_json::Value {
    decode_chromium_json_array_string_value(value, MAX_CHROMIUM_CONSOLE_JSON_BYTES)
}

fn decode_chromium_network_entries_value(value: serde_json::Value) -> serde_json::Value {
    decode_chromium_json_array_string_value(value, MAX_CHROMIUM_NETWORK_JSON_BYTES)
}

fn decode_chromium_client_download_entries_value(value: serde_json::Value) -> serde_json::Value {
    decode_chromium_json_array_string_value(value, MAX_CHROMIUM_CLIENT_DOWNLOAD_JSON_BYTES)
}

fn decode_chromium_json_array_string_value(
    value: serde_json::Value,
    max_json_bytes: usize,
) -> serde_json::Value {
    match value {
        serde_json::Value::String(raw) if raw.len() <= max_json_bytes => {
            serde_json::from_str::<serde_json::Value>(raw.as_str())
                .unwrap_or_else(|_| serde_json::Value::Array(Vec::new()))
        }
        serde_json::Value::String(_) => serde_json::Value::Array(Vec::new()),
        serde_json::Value::Array(_) => value,
        _ => serde_json::Value::Array(Vec::new()),
    }
}

fn decode_chromium_json_script_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::String(raw) => serde_json::from_str::<serde_json::Value>(raw.as_str())
            .unwrap_or(serde_json::Value::Null),
        value => value,
    }
}

fn chromium_read_local_storage_script() -> String {
    format!(
        r#"
(() => {{
  const MAX_STORAGE_ENTRIES = {max_entries};
  const MAX_STORAGE_KEY_CHARS = 512;
  const MAX_STORAGE_VALUE_CHARS = {max_value_chars};
  const MAX_STORAGE_JSON_CHARS = {max_json_chars};
  const clampScalar = (value, maxChars) => {{
    if (typeof value === "string") {{
      return value.length > maxChars ? value.slice(0, maxChars) : value;
    }}
    if (typeof value === "number" || typeof value === "boolean") {{
      const text = String(value);
      return text.length > maxChars ? text.slice(0, maxChars) : text;
    }}
    return "";
  }};
  try {{
    const origin = String((window.location && window.location.origin) || "");
    if (!origin || origin === "null") {{
      return JSON.stringify({{ ok: true, origin: "", entries: {{}} }});
    }}
    const storage = window.localStorage;
    if (!storage) {{
      return JSON.stringify({{ ok: true, origin, entries: {{}} }});
    }}
    const entries = {{}};
    let totalChars = 2;
    let count = 0;
    const length = Math.min(Number(storage.length || 0), MAX_STORAGE_ENTRIES * 4);
    for (let index = 0; index < length; index += 1) {{
      const rawKey = storage.key(index);
      const key = clampScalar(rawKey, MAX_STORAGE_KEY_CHARS).trim();
      if (!key || Object.prototype.hasOwnProperty.call(entries, key)) {{
        continue;
      }}
      const value = clampScalar(storage.getItem(rawKey), MAX_STORAGE_VALUE_CHARS);
      const entryChars = JSON.stringify(key).length + JSON.stringify(value).length + 4;
      if (count > 0 && totalChars + entryChars > MAX_STORAGE_JSON_CHARS) {{
        break;
      }}
      if (totalChars + entryChars > MAX_STORAGE_JSON_CHARS) {{
        continue;
      }}
      entries[key] = value;
      totalChars += entryChars;
      count += 1;
      if (count >= MAX_STORAGE_ENTRIES) {{
        break;
      }}
    }}
    return JSON.stringify({{ ok: true, origin, entries }});
  }} catch (error) {{
    return JSON.stringify({{
      ok: false,
      origin: "",
      entries: {{}},
      error: String((error && (error.message || error)) || "")
    }});
  }}
}})()
"#,
        max_entries = MAX_STORAGE_ENTRIES_PER_ORIGIN,
        max_value_chars = MAX_STORAGE_ENTRY_VALUE_BYTES,
        max_json_chars = MAX_CHROMIUM_LOCAL_STORAGE_JSON_BYTES
    )
}

fn chromium_read_document_cookies_script() -> String {
    format!(
        r#"
(() => {{
  const MAX_COOKIE_CHARS = {max_cookie_chars};
  try {{
    const location = window.location || {{}};
    const domain = String(location.hostname || "").trim().toLowerCase();
    const rawCookie = String(document.cookie || "");
    const cookie = rawCookie.length > MAX_COOKIE_CHARS
      ? rawCookie.slice(0, MAX_COOKIE_CHARS)
      : rawCookie;
    return JSON.stringify({{ ok: true, domain, cookie }});
  }} catch (error) {{
    return JSON.stringify({{
      ok: false,
      domain: "",
      cookie: "",
      error: String((error && (error.message || error)) || "")
    }});
  }}
}})()
"#,
        max_cookie_chars = MAX_CHROMIUM_DOCUMENT_COOKIE_JSON_BYTES
    )
}

fn chromium_restore_local_storage_script(
    entries: &HashMap<String, String>,
) -> Result<String, String> {
    let entries_json = serde_json::to_string(entries)
        .map_err(|error| format!("failed to encode localStorage restore entries: {error}"))?;
    Ok(format!(
        r#"
(() => {{
  const entries = {entries_json};
  try {{
    const storage = window.localStorage;
    if (!storage) {{
      return JSON.stringify({{ ok: false, error: "localStorage unavailable" }});
    }}
    storage.clear();
    Object.keys(entries).forEach((key) => {{
      const value = entries[key];
      if (typeof value === "string") {{
        storage.setItem(key, value);
      }}
    }});
    return JSON.stringify({{ ok: true }});
  }} catch (error) {{
    return JSON.stringify({{
      ok: false,
      error: String((error && (error.message || error)) || "")
    }});
  }}
}})()
"#
    ))
}

fn parse_chromium_local_storage_snapshot(
    value: serde_json::Value,
) -> Result<ChromiumLocalStorageSnapshot, String> {
    let object = value
        .as_object()
        .ok_or_else(|| "localStorage read returned non-object payload".to_owned())?;
    if !object.get("ok").and_then(serde_json::Value::as_bool).unwrap_or(false) {
        let error = object
            .get("error")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown localStorage read failure");
        return Err(format!("localStorage read failed: {error}"));
    }
    let origin = object
        .get("origin")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_owned();
    if origin.is_empty() || origin == "null" {
        return Ok(None);
    }
    let mut entries = HashMap::new();
    for (key, value) in
        object.get("entries").and_then(serde_json::Value::as_object).into_iter().flatten()
    {
        if let Some(value) = value.as_str() {
            entries.insert(key.clone(), value.to_owned());
        }
    }
    Ok(Some((origin, entries)))
}

fn parse_chromium_document_cookie_snapshot(
    value: serde_json::Value,
) -> Result<Vec<CookieUpdate>, String> {
    let object = value
        .as_object()
        .ok_or_else(|| "document.cookie read returned non-object payload".to_owned())?;
    if !object.get("ok").and_then(serde_json::Value::as_bool).unwrap_or(false) {
        let error = object
            .get("error")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown document.cookie read failure");
        return Err(format!("document.cookie read failed: {error}"));
    }
    let domain = object
        .get("domain")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .trim()
        .trim_matches('.')
        .to_ascii_lowercase();
    if domain.is_empty() {
        return Ok(Vec::new());
    }
    let cookie = object.get("cookie").and_then(serde_json::Value::as_str).unwrap_or_default();
    let mut updates = Vec::new();
    for pair in cookie.split(';').take(MAX_COOKIES_PER_DOMAIN * 4) {
        let Some((name, value)) = pair.trim().split_once('=') else {
            continue;
        };
        let name = name.trim().to_ascii_lowercase();
        let value = value.trim();
        if name.is_empty() || value.is_empty() {
            continue;
        }
        if updates.iter().any(|update: &CookieUpdate| update.name == name) {
            continue;
        }
        updates.push(CookieUpdate {
            domain: domain.clone(),
            name,
            value: truncate_utf8_bytes(value, 1024),
        });
        if updates.len() >= MAX_COOKIES_PER_DOMAIN {
            break;
        }
    }
    Ok(updates)
}

fn parse_chromium_local_storage_restore_status(value: serde_json::Value) -> Result<(), String> {
    let object = value
        .as_object()
        .ok_or_else(|| "localStorage restore returned non-object payload".to_owned())?;
    if object.get("ok").and_then(serde_json::Value::as_bool).unwrap_or(false) {
        return Ok(());
    }
    let error = object
        .get("error")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown localStorage restore failure");
    Err(format!("localStorage restore failed: {error}"))
}

fn bounded_chromium_json_string(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
    default: &str,
    max_bytes: usize,
) -> String {
    truncate_utf8_bytes(
        object.get(field).and_then(serde_json::Value::as_str).unwrap_or(default),
        max_bytes,
    )
}

fn parse_chromium_page_network_entries(value: serde_json::Value) -> Vec<NetworkLogEntryInternal> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            let object = entry.as_object()?;
            let request_url =
                object.get("request_url").and_then(serde_json::Value::as_str).unwrap_or_default();
            if request_url.trim().is_empty() {
                return None;
            }
            let latency_ms =
                object.get("latency_ms").and_then(serde_json::Value::as_u64).unwrap_or(0);
            Some(NetworkLogEntryInternal {
                request_url: normalize_url_with_redaction(request_url),
                status_code: object
                    .get("status_code")
                    .and_then(serde_json::Value::as_u64)
                    .and_then(|value| u16::try_from(value).ok())
                    .unwrap_or(0),
                timing_bucket: timing_bucket_for_latency(latency_ms).to_owned(),
                latency_ms,
                captured_at_unix_ms: object
                    .get("captured_at_unix_ms")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_else(current_unix_ms),
                headers: parse_chromium_page_network_headers(object.get("headers")),
            })
        })
        .collect()
}

fn parse_chromium_client_download_entries(value: serde_json::Value) -> Vec<ChromiumClientDownload> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            let object = entry.as_object()?;
            let source_url = bounded_chromium_json_string(object, "source_url", "", 2048);
            if !source_url.starts_with("blob:") {
                return None;
            }
            let file_name = sanitize_download_file_name(
                bounded_chromium_json_string(object, "file_name", DOWNLOAD_FILE_NAME_FALLBACK, 256)
                    .as_str(),
            );
            let content_base64 =
                object.get("content_base64").and_then(serde_json::Value::as_str).unwrap_or("");
            let content = base64::engine::general_purpose::STANDARD.decode(content_base64).ok()?;
            if content.is_empty() || content.len() as u64 > DOWNLOAD_MAX_FILE_BYTES {
                return None;
            }
            let mime_type = bounded_chromium_json_string(object, "mime_type", "", 128);
            Some(ChromiumClientDownload { source_url, file_name, mime_type, content })
        })
        .collect()
}

fn parse_chromium_page_network_headers(
    value: Option<&serde_json::Value>,
) -> Vec<NetworkLogHeaderInternal> {
    let mut output = value
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .take(MAX_NETWORK_LOG_HEADER_COUNT)
        .filter_map(|header| {
            let object = header.as_object()?;
            let name = object
                .get("name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase();
            if name.is_empty() {
                return None;
            }
            let raw_value =
                object.get("value").and_then(serde_json::Value::as_str).unwrap_or_default();
            let value = sanitize_single_network_header(name.as_str(), raw_value);
            Some(NetworkLogHeaderInternal { name, value })
        })
        .collect::<Vec<_>>();
    output.sort_by(|left, right| left.name.cmp(&right.name));
    output
}

fn parse_chromium_viewport_metrics(
    value: serde_json::Value,
    requested_width: u32,
    requested_height: u32,
    requested_device_scale_factor: f64,
) -> (u32, u32, f64) {
    let actual_width =
        chromium_u32_metric_prefer(&value, "width", "visual_width").unwrap_or(requested_width);
    let actual_height =
        chromium_u32_metric_prefer(&value, "height", "visual_height").unwrap_or(requested_height);
    let actual_device_scale_factor = value
        .get("device_scale_factor")
        .and_then(serde_json::Value::as_f64)
        .filter(|value| value.is_finite() && *value > 0.0)
        .unwrap_or(requested_device_scale_factor);
    (actual_width, actual_height, actual_device_scale_factor)
}

fn chromium_u32_metric_option(value: &serde_json::Value, field: &str) -> Option<u32> {
    value
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .filter(|value| *value > 0)
}

fn chromium_u32_metric_prefer(
    value: &serde_json::Value,
    preferred_field: &str,
    fallback_field: &str,
) -> Option<u32> {
    chromium_u32_metric_option(value, preferred_field)
        .or_else(|| chromium_u32_metric_option(value, fallback_field))
}

fn chromium_u32_metric(value: &serde_json::Value, field: &str) -> u32 {
    chromium_u32_metric_option(value, field).unwrap_or(0)
}

fn parse_chromium_layout_metrics(value: serde_json::Value) -> ChromiumLayoutMetrics {
    let viewport_width =
        chromium_u32_metric_prefer(&value, "viewport_width", "visual_viewport_width").unwrap_or(0);
    let viewport_height =
        chromium_u32_metric_prefer(&value, "viewport_height", "visual_viewport_height")
            .unwrap_or(0);
    let document_scroll_width = chromium_u32_metric(&value, "document_scroll_width");
    let document_scroll_height = chromium_u32_metric(&value, "document_scroll_height");
    let document_client_width = chromium_u32_metric(&value, "document_client_width");
    let document_client_height = chromium_u32_metric(&value, "document_client_height");
    let device_scale_factor = value
        .get("device_scale_factor")
        .and_then(serde_json::Value::as_f64)
        .filter(|value| value.is_finite() && *value > 0.0)
        .unwrap_or(1.0);
    let measured_horizontal_overflow = {
        let comparison_width =
            if viewport_width > 0 { viewport_width } else { document_client_width };
        document_scroll_width > 0
            && comparison_width > 0
            && document_scroll_width > comparison_width.saturating_add(1)
    };
    let horizontal_overflow =
        value.get("horizontal_overflow").and_then(serde_json::Value::as_bool).unwrap_or(false)
            || measured_horizontal_overflow;
    let measured_vertical_overflow = {
        let comparison_height =
            if viewport_height > 0 { viewport_height } else { document_client_height };
        document_scroll_height > 0
            && comparison_height > 0
            && document_scroll_height > comparison_height.saturating_add(1)
    };
    let vertical_overflow =
        value.get("vertical_overflow").and_then(serde_json::Value::as_bool).unwrap_or(false)
            || measured_vertical_overflow;

    ChromiumLayoutMetrics {
        viewport_width,
        viewport_height,
        device_scale_factor,
        document_scroll_width,
        document_scroll_height,
        document_client_width,
        document_client_height,
        horizontal_overflow,
        vertical_overflow,
    }
}

fn chromium_touch_emulation_max_touch_points(mobile: bool) -> Option<u32> {
    mobile.then_some(1)
}

fn parse_chromium_console_entries(value: serde_json::Value) -> Vec<BrowserConsoleEntryInternal> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            let object = entry.as_object()?;
            Some(BrowserConsoleEntryInternal {
                severity: BrowserDiagnosticSeverityInternal::from_proto(
                    match object
                        .get("severity")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("info")
                    {
                        "debug" => browser_v1::BrowserDiagnosticSeverity::Debug as i32,
                        "warn" => browser_v1::BrowserDiagnosticSeverity::Warn as i32,
                        "error" => browser_v1::BrowserDiagnosticSeverity::Error as i32,
                        _ => browser_v1::BrowserDiagnosticSeverity::Info as i32,
                    },
                ),
                kind: bounded_chromium_json_string(
                    object,
                    "kind",
                    "console",
                    MAX_INSPECT_CONSOLE_KIND_BYTES,
                ),
                message: bounded_chromium_json_string(
                    object,
                    "message",
                    "",
                    MAX_CONSOLE_MESSAGE_BYTES,
                ),
                captured_at_unix_ms: object
                    .get("captured_at_unix_ms")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0),
                source: bounded_chromium_json_string(
                    object,
                    "source",
                    "",
                    MAX_CONSOLE_SOURCE_BYTES,
                ),
                stack_trace: bounded_chromium_json_string(
                    object,
                    "stack_trace",
                    "",
                    MAX_CONSOLE_STACK_BYTES,
                ),
                page_url: bounded_chromium_json_string(
                    object,
                    "page_url",
                    "",
                    MAX_NETWORK_LOG_URL_BYTES,
                ),
            })
        })
        .collect()
}

pub(crate) async fn chromium_refresh_tab_snapshot(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
) -> Result<(), String> {
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    let _ = chromium_install_page_diagnostics(runtime, session_id, tab_id).await;
    let snapshot = chromium_observe_snapshot(runtime, session_id, tab_id).await?;
    let console_log =
        chromium_read_console_log(runtime, session_id, tab_id).await.unwrap_or_default();
    let storage_snapshot = match chromium_read_local_storage(runtime, session_id, tab_id).await {
        Ok(value) => value,
        Err(error) => {
            warn!(
                session_id,
                tab_id,
                error = error.as_str(),
                "failed to refresh Chromium localStorage snapshot"
            );
            None
        }
    };
    let document_cookie_updates =
        match chromium_read_document_cookies(runtime, session_id, tab_id).await {
            Ok(value) => value,
            Err(error) => {
                warn!(
                    session_id,
                    tab_id,
                    error = error.as_str(),
                    "failed to refresh Chromium document.cookie snapshot"
                );
                Vec::new()
            }
        };
    let mut network_log =
        chromium_drain_pending_network_log(runtime, session_id, tab_id).await.unwrap_or_default();
    network_log.extend(
        chromium_drain_page_network_log(runtime, session_id, tab_id).await.unwrap_or_default(),
    );
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    let mut sessions = runtime.sessions.lock().await;
    let Some(session) = sessions.get_mut(session_id) else {
        return Err("session_not_found".to_owned());
    };
    let max_network_log_entries = session.budget.max_network_log_entries;
    let max_network_log_bytes = session.budget.max_network_log_bytes;
    if let Some((origin, entries)) = storage_snapshot {
        replace_storage_entries_for_origin(session, origin.as_str(), entries);
    }
    apply_cookie_updates(session, document_cookie_updates.as_slice());
    let Some(tab) = session.tabs.get_mut(tab_id) else {
        return Err("tab_not_found".to_owned());
    };
    tab.last_page_body = snapshot.page_body;
    tab.last_title = snapshot.title;
    tab.last_url = Some(snapshot.page_url);
    tab.console_log = clamp_console_log_entries(
        console_log,
        DEFAULT_MAX_CONSOLE_LOG_ENTRIES,
        DEFAULT_MAX_CONSOLE_LOG_BYTES,
    );
    append_network_log_entries(
        tab,
        network_log.as_slice(),
        max_network_log_entries,
        max_network_log_bytes,
    );
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

pub(crate) async fn chromium_layout_metrics(
    runtime: &BrowserRuntimeState,
    session_id: &str,
) -> Result<ChromiumLayoutMetrics, String> {
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    let (_tab_id, tab) = chromium_active_tab_for_session(runtime, session_id).await?;
    let value = run_chromium_blocking("chromium layout metrics", move || {
        let raw_value = tab
            .evaluate(
                r#"JSON.stringify((() => {
              const doc = document.documentElement || {};
              const body = document.body || {};
              const scrollWidth = Math.max(doc.scrollWidth || 0, body.scrollWidth || 0);
              const scrollHeight = Math.max(doc.scrollHeight || 0, body.scrollHeight || 0);
              const clientWidth = Math.max(doc.clientWidth || 0, window.innerWidth || 0);
              const clientHeight = Math.max(doc.clientHeight || 0, window.innerHeight || 0);
              const visualViewport = window.visualViewport || {};
              const visualViewportWidth = Math.trunc(visualViewport.width || 0);
              const visualViewportHeight = Math.trunc(visualViewport.height || 0);
              const layoutViewportWidth = Math.trunc(window.innerWidth || clientWidth || 0);
              const layoutViewportHeight = Math.trunc(window.innerHeight || clientHeight || 0);
              const effectiveViewportWidth = layoutViewportWidth || visualViewportWidth;
              const effectiveViewportHeight = layoutViewportHeight || visualViewportHeight;
              return {
                viewport_width: layoutViewportWidth,
                viewport_height: layoutViewportHeight,
                visual_viewport_width: visualViewportWidth,
                visual_viewport_height: visualViewportHeight,
                device_scale_factor: Number(window.devicePixelRatio || 1),
                document_scroll_width: Math.trunc(scrollWidth),
                document_scroll_height: Math.trunc(scrollHeight),
                document_client_width: Math.trunc(clientWidth),
                document_client_height: Math.trunc(clientHeight),
                horizontal_overflow: scrollWidth > effectiveViewportWidth + 1,
                vertical_overflow: scrollHeight > effectiveViewportHeight + 1
              };
            })())"#,
                false,
            )
            .map_err(|error| format!("failed to read Chromium layout metrics: {error}"))?
            .value
            .unwrap_or(serde_json::Value::Null);
        Ok(parse_chromium_layout_metrics(decode_chromium_json_script_value(raw_value)))
    })
    .await?;
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    Ok(value)
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
    let (tab, private_target_policy) =
        match chromium_tab_and_private_target_policy_for_session(runtime, session_id, tab_id).await
        {
            Ok(value) => value,
            Err(error) => {
                outcome.success = false;
                outcome.error = format!("chromium tab runtime unavailable: {error}");
                return outcome;
            }
        };
    let _scoped_private_target = if params.allow_private_targets {
        match private_target_policy.scoped_url_allowance(outcome.final_url.as_str()) {
            Ok(value) => value,
            Err(error) => {
                outcome.success = false;
                outcome.error = format!("failed to scope private-target policy: {error}");
                return outcome;
            }
        }
    } else {
        None
    };
    let storage_entries_by_origin = {
        let sessions = runtime.sessions.lock().await;
        sessions.get(session_id).map(|session| session.storage_entries.clone()).unwrap_or_default()
    };
    let target_url = outcome.final_url.clone();
    let chromium_timeout_ms = params.timeout_ms;
    let chromium_snapshot = run_chromium_blocking("chromium navigate", move || {
        tab.set_default_timeout(Duration::from_millis(chromium_timeout_ms.max(1)));
        tab.navigate_to(target_url.as_str())
            .map_err(|error| format!("failed to issue Chromium navigation command: {error}"))?;
        tab.wait_until_navigated()
            .map_err(|error| format!("Chromium navigation timeout or failure: {error}"))?;
        tab.evaluate(
            r#"
(() => {
  const state = window.__palyraDiagnostics;
  if (state && state.installed) {
    return true;
  }
  return false;
})()
"#,
            false,
        )
        .ok();
        let mut page_url = tab.get_url();
        if let Some(origin) = url_origin_key(page_url.as_str()) {
            if let Some(entries) =
                storage_entries_by_origin.get(origin.as_str()).filter(|entries| !entries.is_empty())
            {
                let script = chromium_restore_local_storage_script(entries)?;
                let raw_value = tab
                    .evaluate(script.as_str(), false)
                    .map_err(|error| {
                        format!("failed to restore Chromium localStorage for {origin}: {error}")
                    })?
                    .value
                    .unwrap_or_else(|| serde_json::Value::String("{}".to_owned()));
                parse_chromium_local_storage_restore_status(decode_chromium_json_script_value(
                    raw_value,
                ))
                .map_err(|error| format!("{error} for {origin}"))?;
                tab.navigate_to(page_url.as_str()).map_err(|error| {
                    format!("failed to reload Chromium page after localStorage restore: {error}")
                })?;
                tab.wait_until_navigated().map_err(|error| {
                    format!("Chromium reload after localStorage restore timed out: {error}")
                })?;
                page_url = tab.get_url();
            }
        }
        let page_body = tab.get_content().map_err(|error| {
            format!("failed to read Chromium page HTML after navigation: {error}")
        })?;
        let title = tab.get_title().unwrap_or_default();
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
    let page_body = if body_bytes > params.max_response_bytes {
        if outcome.error.is_empty() {
            outcome.error = format!(
                "response exceeds max_response_bytes ({} > {}); page_body truncated",
                body_bytes, params.max_response_bytes
            );
        }
        truncate_utf8_bytes(snapshot.page_body.as_str(), params.max_response_bytes as usize)
    } else {
        snapshot.page_body
    };
    outcome.final_url = snapshot.page_url;
    outcome.title = snapshot.title;
    outcome.page_body = page_body;
    outcome.body_bytes = body_bytes;
    if params.allow_private_targets {
        if let Err(error) = private_target_policy.retain_url_allowance(outcome.final_url.as_str()) {
            outcome.success = false;
            outcome.error = format!("failed to retain navigated private-target scope: {error}");
            return outcome;
        }
    }
    let _ = chromium_install_page_diagnostics(runtime, session_id, tab_id).await;
    let _ = chromium_refresh_tab_snapshot(runtime, session_id, tab_id).await;
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
        let tab_id_for_attempt = tab_id.clone();
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
                        let action_context =
                            chromium_action_context(tab_id_for_attempt.as_str(), tab_for_attempt.get_url().as_str());
                        format!(
                            "selector '{}' was present in the live Chromium DOM for {action_context}, but Chromium could not resolve it for click: {error}; call observe to verify the active tab and retry with a current selector",
                            selector_for_attempt,
                        )
                    },
                )?;
                element.click().map_err(|error| {
                    let action_context =
                        chromium_action_context(tab_id_for_attempt.as_str(), tab_for_attempt.get_url().as_str());
                    format!(
                        "selector '{}' was present in the live Chromium DOM for {action_context}, but click did not complete before the actionability timeout: {error}; inspect visibility, disabled state, overlays, active tab, and viewport before retrying",
                        selector_for_attempt,
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
        error: chromium_selector_not_found_error(
            runtime,
            session_id,
            tab_id.as_str(),
            selector,
            "click",
        )
        .await,
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
        Disabled,
        ReadOnly,
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
            let script = chromium_type_script(
                selector_for_attempt.as_str(),
                text_for_attempt.as_str(),
                clear_existing_for_attempt,
            )?;
            let raw_value = tab_for_attempt
                .evaluate(script.as_str(), true)
                .map_err(|error| {
                    format!(
                        "failed to execute Chromium type script for selector '{}': {error}",
                        selector_for_attempt
                    )
                })?
                .value
                .unwrap_or(serde_json::Value::Null);
            let value = decode_chromium_json_script_value(raw_value);
            let status =
                value.get("status").and_then(serde_json::Value::as_str).unwrap_or_default();
            match status {
                "typed" => Ok(TypeAttempt::Typed),
                "not_found" => Ok(TypeAttempt::NotFound),
                "not_typable" => Ok(TypeAttempt::NotTypable),
                "disabled" => Ok(TypeAttempt::Disabled),
                "readonly" => Ok(TypeAttempt::ReadOnly),
                _ => Err(format!(
                    "Chromium type script returned unexpected status '{}' for selector '{}'",
                    status, selector_for_attempt
                )),
            }
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
            Ok(TypeAttempt::Disabled) => {
                return ChromiumActionOutcome {
                    success: false,
                    outcome: "selector_disabled".to_owned(),
                    error: format!("selector '{selector}' is disabled"),
                    attempts,
                };
            }
            Ok(TypeAttempt::ReadOnly) => {
                return ChromiumActionOutcome {
                    success: false,
                    outcome: "selector_readonly".to_owned(),
                    error: format!("selector '{selector}' is read-only"),
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

pub(crate) async fn set_file_input_with_chromium(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    selector: &str,
    file_name: &str,
    file_bytes: &[u8],
    timeout_ms: u64,
) -> ChromiumActionOutcome {
    enum FileInputAttempt {
        Set,
        NotFound,
        NotFileInput,
        Disabled,
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
    let upload_path =
        match write_chromium_upload_file(runtime, session_id, file_name, file_bytes).await {
            Ok(value) => value,
            Err(error) => {
                return ChromiumActionOutcome {
                    success: false,
                    outcome: "upload_file_prepare_failed".to_owned(),
                    error,
                    attempts: 1,
                }
            }
        };
    let upload_path_text = upload_path.to_string_lossy().to_string();
    let started = Instant::now();
    let mut attempts = 0_u32;
    loop {
        attempts = attempts.saturating_add(1);
        let selector_for_attempt = selector.to_owned();
        let upload_path_for_attempt = upload_path_text.clone();
        let tab_for_attempt = Arc::clone(&tab);
        let attempt = run_chromium_blocking("chromium set file input", move || {
            let page_body = tab_for_attempt.get_content().map_err(|error| {
                format!("failed to read Chromium DOM before file upload: {error}")
            })?;
            let Some(tag) =
                find_matching_html_tag(selector_for_attempt.as_str(), page_body.as_str())
            else {
                return Ok(FileInputAttempt::NotFound);
            };
            if !is_file_input_tag(tag.as_str()) {
                return Ok(FileInputAttempt::NotFileInput);
            }
            if tag.to_ascii_lowercase().contains(" disabled") {
                return Ok(FileInputAttempt::Disabled);
            }
            let element =
                tab_for_attempt.find_element(selector_for_attempt.as_str()).map_err(|error| {
                    format!(
                        "failed to resolve selector '{}' on Chromium page: {error}",
                        selector_for_attempt
                    )
                })?;
            element.set_input_files(&[upload_path_for_attempt.as_str()]).map_err(|error| {
                format!(
                    "failed to set file input '{}' on Chromium page: {error}",
                    selector_for_attempt
                )
            })?;
            Ok(FileInputAttempt::Set)
        })
        .await;

        match attempt {
            Ok(FileInputAttempt::Set) => {
                let _ = chromium_refresh_tab_snapshot(runtime, session_id, tab_id.as_str()).await;
                return ChromiumActionOutcome {
                    success: true,
                    outcome: "file_input_set".to_owned(),
                    error: String::new(),
                    attempts,
                };
            }
            Ok(FileInputAttempt::NotFileInput) => {
                return ChromiumActionOutcome {
                    success: false,
                    outcome: "selector_not_file_input".to_owned(),
                    error: format!(
                        "selector '{selector}' does not target an input[type=file] element"
                    ),
                    attempts,
                };
            }
            Ok(FileInputAttempt::Disabled) => {
                return ChromiumActionOutcome {
                    success: false,
                    outcome: "selector_disabled".to_owned(),
                    error: format!("selector '{selector}' is disabled"),
                    attempts,
                };
            }
            Ok(FileInputAttempt::NotFound) => {}
            Err(error) => {
                return ChromiumActionOutcome {
                    success: false,
                    outcome: "file_input_failed".to_owned(),
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

async fn write_chromium_upload_file(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    file_name: &str,
    file_bytes: &[u8],
) -> Result<PathBuf, String> {
    let upload_dir = {
        let chromium_sessions = runtime.chromium_sessions.lock().await;
        let Some(chromium_session) = chromium_sessions.get(session_id) else {
            return Err("chromium_session_not_found".to_owned());
        };
        chromium_session._profile_dir.path().join(UPLOADS_DIR)
    };
    fs::create_dir_all(upload_dir.as_path()).map_err(|error| {
        format!(
            "failed to initialize Chromium upload directory '{}': {error}",
            upload_dir.display()
        )
    })?;
    let path = chromium_upload_staging_path(upload_dir.as_path(), file_name)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            format!(
                "failed to initialize Chromium upload staging directory '{}': {error}",
                parent.display()
            )
        })?;
    }
    fs::write(path.as_path(), file_bytes).map_err(|error| {
        format!("failed to stage uploaded browser file '{}': {error}", path.display())
    })?;
    Ok(path)
}

fn chromium_upload_staging_path(upload_dir: &Path, file_name: &str) -> Result<PathBuf, String> {
    let file_name = sanitize_download_file_name(file_name);
    if file_name.is_empty() {
        return Err("upload file name is empty after sanitization".to_owned());
    }
    Ok(upload_dir.join(Ulid::new().to_string()).join(file_name))
}

fn chromium_type_script(
    selector: &str,
    text: &str,
    clear_existing: bool,
) -> Result<String, String> {
    let selector_json = serde_json::to_string(selector)
        .map_err(|error| format!("failed to encode selector for Chromium type: {error}"))?;
    let text_json = serde_json::to_string(text)
        .map_err(|error| format!("failed to encode text for Chromium type: {error}"))?;
    let clear_existing_json = if clear_existing { "true" } else { "false" };
    Ok(format!(
        r#"
(() => {{
  const selector = {selector_json};
  const text = {text_json};
  const clearExisting = {clear_existing_json};
  const respond = (payload) => JSON.stringify(payload);
  const element = document.querySelector(selector);
  if (!element) {{
    return respond({{ status: "not_found" }});
  }}
  const tagName = (element.tagName || "").toLowerCase();
  const inputLike = tagName === "input" || tagName === "textarea";
  const editable = element.isContentEditable === true;
  if (!inputLike && !editable) {{
    return respond({{ status: "not_typable", tagName }});
  }}
  if (element.disabled) {{
    return respond({{ status: "disabled" }});
  }}
  if (element.readOnly) {{
    return respond({{ status: "readonly" }});
  }}
  if (typeof element.focus === "function") {{
    element.focus();
  }}
  const dispatchInputEvent = () => {{
    let event;
    try {{
      event = new InputEvent("input", {{
        bubbles: true,
        cancelable: true,
        data: text,
        inputType: clearExisting ? "insertReplacementText" : "insertText",
      }});
    }} catch (_) {{
      event = new Event("input", {{ bubbles: true, cancelable: true }});
    }}
    element.dispatchEvent(event);
    element.dispatchEvent(new Event("change", {{ bubbles: true }}));
  }};
  if (inputLike) {{
    const current = clearExisting ? "" : String(element.value ?? "");
    const next = current + text;
    const proto = tagName === "textarea" ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype;
    const descriptor = Object.getOwnPropertyDescriptor(proto, "value");
    if (descriptor && typeof descriptor.set === "function") {{
      descriptor.set.call(element, next);
    }} else {{
      element.value = next;
    }}
    if (typeof element.setSelectionRange === "function") {{
      const end = String(element.value ?? "").length;
      try {{ element.setSelectionRange(end, end); }} catch (_) {{}}
    }}
    dispatchInputEvent();
    return respond({{ status: "typed", value: String(element.value ?? "") }});
  }}
  const currentText = clearExisting ? "" : String(element.textContent ?? "");
  element.textContent = currentText + text;
  dispatchInputEvent();
  return respond({{ status: "typed", value: String(element.textContent ?? "") }});
}})()
"#
    ))
}

fn parse_key_press_spec(raw: &str) -> Result<(String, Vec<ModifierKey>), String> {
    if raw == " " {
        return Ok((" ".to_owned(), Vec::new()));
    }
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("key press requires non-empty key".to_owned());
    }
    let mut parts =
        trimmed.split('+').map(str::trim).filter(|value| !value.is_empty()).collect::<Vec<_>>();
    if parts.is_empty() {
        return Err("key press requires non-empty key".to_owned());
    }
    let key = parts.pop().unwrap_or_default();
    if key.is_empty() {
        return Err("key press requires terminal key segment".to_owned());
    }
    let key = normalize_key_press_terminal_key(key);
    let mut modifiers = Vec::new();
    for modifier in parts {
        let value = match modifier.to_ascii_lowercase().as_str() {
            "alt" => ModifierKey::Alt,
            "ctrl" | "control" => ModifierKey::Ctrl,
            "meta" | "cmd" | "command" => ModifierKey::Meta,
            "shift" => ModifierKey::Shift,
            other => {
                return Err(format!("unsupported key modifier '{other}'"));
            }
        };
        modifiers.push(value);
    }
    Ok((key, modifiers))
}

fn normalize_key_press_terminal_key(key: &str) -> String {
    match key.to_ascii_lowercase().as_str() {
        "space" | "spacebar" => " ".to_owned(),
        _ => key.to_owned(),
    }
}

pub(crate) async fn press_with_chromium(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    key_spec: &str,
    timeout_ms: u64,
) -> ChromiumActionOutcome {
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
    let (key, modifiers) = match parse_key_press_spec(key_spec) {
        Ok(value) => value,
        Err(error) => {
            return ChromiumActionOutcome {
                success: false,
                outcome: "invalid_key_spec".to_owned(),
                error,
                attempts: 1,
            }
        }
    };
    let result = run_chromium_blocking("chromium press", move || {
        tab.set_default_timeout(Duration::from_millis(timeout_ms.max(1)));
        if modifiers.is_empty() {
            tab.press_key(key.as_str())
                .map_err(|error| format!("failed to press Chromium key '{}': {error}", key))?;
        } else {
            tab.press_key_with_modifiers(key.as_str(), Some(modifiers.as_slice()))
                .map_err(|error| format!("failed to press Chromium key '{}': {error}", key))?;
        }
        Ok(())
    })
    .await;
    match result {
        Ok(()) => {
            let _ = chromium_refresh_tab_snapshot(runtime, session_id, tab_id.as_str()).await;
            ChromiumActionOutcome {
                success: true,
                outcome: "pressed".to_owned(),
                error: String::new(),
                attempts: 1,
            }
        }
        Err(error) => ChromiumActionOutcome {
            success: false,
            outcome: "press_failed".to_owned(),
            error,
            attempts: 1,
        },
    }
}

pub(crate) async fn select_with_chromium(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    selector: &str,
    value: &str,
    timeout_ms: u64,
) -> ChromiumActionOutcome {
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
    let selector_json = match serde_json::to_string(selector) {
        Ok(value) => value,
        Err(error) => {
            return ChromiumActionOutcome {
                success: false,
                outcome: "select_failed".to_owned(),
                error: format!("failed to encode selector for Chromium select: {error}"),
                attempts: 1,
            }
        }
    };
    let value_json = match serde_json::to_string(value) {
        Ok(value) => value,
        Err(error) => {
            return ChromiumActionOutcome {
                success: false,
                outcome: "select_failed".to_owned(),
                error: format!("failed to encode select value for Chromium select: {error}"),
                attempts: 1,
            }
        }
    };
    let script = format!(
        r#"
(() => {{
  const selector = {selector_json};
  const value = {value_json};
  const respond = (payload) => JSON.stringify(payload);
  const element = document.querySelector(selector);
  if (!element) {{
    return respond({{ status: "not_found" }});
  }}
  if ((element.tagName || "").toLowerCase() !== "select") {{
    return respond({{ status: "not_select" }});
  }}
  if (element.disabled) {{
    return respond({{ status: "disabled" }});
  }}
  const option = Array.from(element.options || []).find((candidate) => candidate.value === value);
  if (!option) {{
    return respond({{ status: "value_not_found" }});
  }}
  element.value = value;
  element.dispatchEvent(new Event("input", {{ bubbles: true }}));
  element.dispatchEvent(new Event("change", {{ bubbles: true }}));
  return respond({{ status: "selected", value: element.value }});
}})()
"#
    );
    let result = run_chromium_blocking("chromium select", move || {
        tab.set_default_timeout(Duration::from_millis(timeout_ms.max(1)));
        let value = tab
            .evaluate(script.as_str(), true)
            .map_err(|error| format!("failed to execute Chromium select script: {error}"))?
            .value
            .unwrap_or(serde_json::Value::Null);
        Ok(decode_chromium_json_script_value(value))
    })
    .await;
    match result {
        Ok(value) => {
            let status =
                value.get("status").and_then(serde_json::Value::as_str).unwrap_or_default();
            match status {
                "selected" => {
                    let _ =
                        chromium_refresh_tab_snapshot(runtime, session_id, tab_id.as_str()).await;
                    ChromiumActionOutcome {
                        success: true,
                        outcome: "selected".to_owned(),
                        error: String::new(),
                        attempts: 1,
                    }
                }
                "disabled" => ChromiumActionOutcome {
                    success: false,
                    outcome: "selector_disabled".to_owned(),
                    error: format!("selector '{selector}' is disabled"),
                    attempts: 1,
                },
                "not_select" => ChromiumActionOutcome {
                    success: false,
                    outcome: "selector_not_select".to_owned(),
                    error: format!("selector '{selector}' does not target a <select> element"),
                    attempts: 1,
                },
                "value_not_found" => ChromiumActionOutcome {
                    success: false,
                    outcome: "value_not_found".to_owned(),
                    error: format!("value '{value}' was not found for selector '{selector}'"),
                    attempts: 1,
                },
                _ => ChromiumActionOutcome {
                    success: false,
                    outcome: "selector_not_found".to_owned(),
                    error: format!("selector '{selector}' was not found"),
                    attempts: 1,
                },
            }
        }
        Err(error) => ChromiumActionOutcome {
            success: false,
            outcome: "select_failed".to_owned(),
            error,
            attempts: 1,
        },
    }
}

pub(crate) async fn highlight_with_chromium(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    selector: &str,
    timeout_ms: u64,
    duration_ms: u64,
) -> ChromiumActionOutcome {
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
    let selector_json = match serde_json::to_string(selector) {
        Ok(value) => value,
        Err(error) => {
            return ChromiumActionOutcome {
                success: false,
                outcome: "highlight_failed".to_owned(),
                error: format!("failed to encode selector for Chromium highlight: {error}"),
                attempts: 1,
            }
        }
    };
    let duration_ms = duration_ms.clamp(250, 10_000);
    let script = format!(
        r#"
(() => {{
  const selector = {selector_json};
  const durationMs = {duration_ms};
  const element = document.querySelector(selector);
  if (!element) {{
    return {{ status: "not_found" }};
  }}
  const rect = element.getBoundingClientRect();
  const existing = document.getElementById("__palyra-highlight-overlay");
  if (existing) {{
    existing.remove();
  }}
  const overlay = document.createElement("div");
  overlay.id = "__palyra-highlight-overlay";
  overlay.style.position = "fixed";
  overlay.style.left = `${{Math.max(0, rect.left - 4)}}px`;
  overlay.style.top = `${{Math.max(0, rect.top - 4)}}px`;
  overlay.style.width = `${{Math.max(8, rect.width + 8)}}px`;
  overlay.style.height = `${{Math.max(8, rect.height + 8)}}px`;
  overlay.style.border = "3px solid #ff6b00";
  overlay.style.borderRadius = "6px";
  overlay.style.background = "rgba(255, 107, 0, 0.08)";
  overlay.style.pointerEvents = "none";
  overlay.style.zIndex = "2147483647";
  document.body.appendChild(overlay);
  window.setTimeout(() => {{
    const current = document.getElementById("__palyra-highlight-overlay");
    if (current) {{
      current.remove();
    }}
  }}, durationMs);
  return {{ status: "highlighted" }};
}})()
"#
    );
    let result = run_chromium_blocking("chromium highlight", move || {
        tab.set_default_timeout(Duration::from_millis(timeout_ms.max(1)));
        let value = tab
            .evaluate(script.as_str(), false)
            .map_err(|error| format!("failed to execute Chromium highlight script: {error}"))?
            .value
            .unwrap_or(serde_json::Value::Null);
        Ok(value)
    })
    .await;
    match result {
        Ok(value) => {
            let status =
                value.get("status").and_then(serde_json::Value::as_str).unwrap_or_default();
            if status == "highlighted" {
                ChromiumActionOutcome {
                    success: true,
                    outcome: "highlighted".to_owned(),
                    error: String::new(),
                    attempts: 1,
                }
            } else {
                ChromiumActionOutcome {
                    success: false,
                    outcome: "selector_not_found".to_owned(),
                    error: chromium_selector_not_found_error(
                        runtime,
                        session_id,
                        tab_id.as_str(),
                        selector,
                        "highlight",
                    )
                    .await,
                    attempts: 1,
                }
            }
        }
        Err(error) => ChromiumActionOutcome {
            success: false,
            outcome: "highlight_failed".to_owned(),
            error,
            attempts: 1,
        },
    }
}

pub(crate) async fn export_pdf_with_chromium(
    runtime: &BrowserRuntimeState,
    session_id: &str,
) -> Result<Vec<u8>, String> {
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    let (_tab_id, tab) = chromium_active_tab_for_session(runtime, session_id).await?;
    let pdf = run_chromium_blocking("chromium print pdf", move || {
        tab.print_to_pdf(Some(PrintToPdfOptions::default()))
            .map_err(|error| format!("failed to export Chromium page as PDF: {error}"))
    })
    .await?;
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    Ok(pdf)
}

pub(crate) async fn set_viewport_with_chromium(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    width: u32,
    height: u32,
    device_scale_factor: f64,
    mobile: bool,
) -> ChromiumViewportOutcome {
    if let Err(error) = enforce_chromium_remote_ip_guard(runtime, session_id).await {
        return ChromiumViewportOutcome {
            success: false,
            width: 0,
            height: 0,
            device_scale_factor: 0.0,
            mobile,
            error,
        };
    }
    let (_tab_id, tab) = match chromium_active_tab_for_session(runtime, session_id).await {
        Ok(value) => value,
        Err(error) => {
            return ChromiumViewportOutcome {
                success: false,
                width: 0,
                height: 0,
                device_scale_factor: 0.0,
                mobile,
                error,
            }
        }
    };
    let result = run_chromium_blocking("chromium set viewport", move || {
        let _ = tab.set_bounds(Bounds::Normal {
            left: None,
            top: None,
            width: Some(f64::from(width)),
            height: Some(f64::from(height)),
        });
        tab.call_method(Emulation::SetDeviceMetricsOverride {
            width,
            height,
            device_scale_factor,
            mobile,
            scale: None,
            screen_width: Some(width),
            screen_height: Some(height),
            position_x: None,
            position_y: None,
            dont_set_visible_size: None,
            screen_orientation: None,
            viewport: None,
            display_feature: None,
            device_posture: None,
        })
        .map_err(|error| format!("failed to set Chromium viewport metrics: {error}"))?;
        tab.call_method(Emulation::SetTouchEmulationEnabled {
            enabled: mobile,
            max_touch_points: chromium_touch_emulation_max_touch_points(mobile),
        })
        .map_err(|error| format!("failed to set Chromium touch emulation: {error}"))?;
        let _ = tab.call_method(Emulation::SetVisibleSize { width, height });
        let value = tab
            .evaluate(
                r#"JSON.stringify({
                    visual_width: Math.trunc((window.visualViewport && window.visualViewport.width) || 0),
                    visual_height: Math.trunc((window.visualViewport && window.visualViewport.height) || 0),
                    width: Math.trunc(window.innerWidth || 0),
                    height: Math.trunc(window.innerHeight || 0),
                    device_scale_factor: Number(window.devicePixelRatio || 1)
                })"#,
                false,
            )
            .map_err(|error| format!("failed to verify Chromium viewport metrics: {error}"))?
            .value
            .unwrap_or(serde_json::Value::Null);
        Ok(parse_chromium_viewport_metrics(
            decode_chromium_json_script_value(value),
            width,
            height,
            device_scale_factor,
        ))
    })
    .await;

    match result {
        Ok((actual_width, actual_height, actual_device_scale_factor)) => {
            if let Err(error) = enforce_chromium_remote_ip_guard(runtime, session_id).await {
                return ChromiumViewportOutcome {
                    success: false,
                    width: actual_width,
                    height: actual_height,
                    device_scale_factor: actual_device_scale_factor,
                    mobile,
                    error,
                };
            }
            ChromiumViewportOutcome {
                success: true,
                width: actual_width,
                height: actual_height,
                device_scale_factor: actual_device_scale_factor,
                mobile,
                error: String::new(),
            }
        }
        Err(error) => ChromiumViewportOutcome {
            success: false,
            width: 0,
            height: 0,
            device_scale_factor: 0.0,
            mobile,
            error,
        },
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::{
        chromium_network_log_headers, chromium_read_document_cookies_script,
        chromium_read_local_storage_script, chromium_restore_local_storage_script,
        chromium_touch_emulation_max_touch_points, chromium_transport_idle_timeout,
        chromium_upload_staging_path, clamp_chromium_snapshot,
        decode_chromium_console_entries_value, decode_chromium_json_script_value,
        decode_chromium_network_entries_value, decode_chromium_observe_state_value,
        page_body_with_chromium_observe_state, parse_chromium_client_download_entries,
        parse_chromium_console_entries, parse_chromium_document_cookie_snapshot,
        parse_chromium_layout_metrics, parse_chromium_local_storage_restore_status,
        parse_chromium_local_storage_snapshot, parse_chromium_page_network_entries,
        parse_chromium_viewport_metrics, parse_key_press_spec,
        selector_not_found_error_from_cached_snapshot, ChromiumLayoutMetrics,
        ChromiumObserveSnapshot, CHROMIUM_DRAIN_NETWORK_LOG_SCRIPT,
        CHROMIUM_PAGE_DIAGNOSTICS_SCRIPT, CHROMIUM_READ_CONSOLE_LOG_SCRIPT,
        MAX_CHROMIUM_CONSOLE_JSON_BYTES, MAX_CHROMIUM_DOCUMENT_COOKIE_JSON_BYTES,
        MAX_CHROMIUM_LOCAL_STORAGE_JSON_BYTES, MAX_CHROMIUM_NETWORK_JSON_BYTES,
    };
    use crate::{
        DEFAULT_SESSION_IDLE_TTL_MS, MAX_CONSOLE_MESSAGE_BYTES, MAX_CONSOLE_SOURCE_BYTES,
        MAX_CONSOLE_STACK_BYTES, MAX_NETWORK_LOG_URL_BYTES,
    };
    use base64::Engine as _;
    use std::collections::HashMap;
    use std::path::Path;
    use std::time::Duration;

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

    #[test]
    fn chromium_observe_state_summary_exposes_safe_form_and_storage_values() {
        let raw = serde_json::json!({
            "html": "<html><body><input id=\"owner\" name=\"owner\" value=\"owner@example.test\"></body></html>",
            "origin": "http://127.0.0.1:8786",
            "form_controls": [{
                "tag": "input",
                "type": "email",
                "id": "owner",
                "name": "owner",
                "selector": "#owner",
                "value": "owner@example.test",
                "checked": null,
                "selected_options": []
            }],
            "local_storage": {
                "ok": true,
                "origin": "http://127.0.0.1:8786",
                "entries": {"wizard": "{\"owner\":\"owner@example.test\"}"}
            },
            "session_storage": {
                "ok": true,
                "origin": "http://127.0.0.1:8786",
                "entries": {"step": "2"}
            }
        });

        let payload =
            decode_chromium_observe_state_value(serde_json::Value::String(raw.to_string()))
                .expect("observe state should parse");
        let page_body = page_body_with_chromium_observe_state(payload);

        assert!(
            page_body.contains("browser_form_control") && page_body.contains("owner@example.test"),
            "form state summary should expose safe current values: {page_body}"
        );
        assert!(
            page_body.contains("localStorage") && page_body.contains("sessionStorage"),
            "storage summary should expose bounded storage state: {page_body}"
        );
    }

    #[test]
    fn chromium_observe_state_summary_redacts_sensitive_values() {
        let raw = serde_json::json!({
            "html": "<html><body><input id=\"password\" type=\"password\" value=\"<redacted>\"></body></html>",
            "origin": "https://example.com",
            "form_controls": [{
                "tag": "input",
                "type": "password",
                "id": "password",
                "name": "password",
                "selector": "#password",
                "value": "supersecret",
                "checked": null,
                "selected_options": []
            }],
            "local_storage": {
                "ok": true,
                "origin": "https://example.com",
                "entries": {"token": "supersecret"}
            },
            "session_storage": {"ok": true, "origin": "https://example.com", "entries": {}}
        });

        let payload =
            decode_chromium_observe_state_value(serde_json::Value::String(raw.to_string()))
                .expect("observe state should parse");
        let page_body = page_body_with_chromium_observe_state(payload);

        assert!(page_body.contains("value=\"&lt;redacted&gt;\""));
        assert!(
            !page_body.contains("supersecret"),
            "observe state summary must not leak sensitive values: {page_body}"
        );
    }

    #[test]
    fn selector_not_found_error_mentions_cached_observe_mismatch() {
        let error = selector_not_found_error_from_cached_snapshot(
            "#save-user",
            "highlight",
            "tab-active",
            r#"<html><body><button id="save-user">Save user</button></body></html>"#,
            "http://127.0.0.1:8790/?token=secret",
        );

        assert!(error.contains("live Chromium DOM for highlight"), "{error}");
        assert!(error.contains("last observe snapshot"), "{error}");
        assert!(
            error.contains("active tab tab-active at http://127.0.0.1:8790/?token=<redacted>"),
            "{error}"
        );
        assert!(!error.contains("token=secret"), "{error}");
        assert!(error.contains("verify any local server is still running"), "{error}");
    }

    #[test]
    fn selector_not_found_error_stays_short_without_cached_match() {
        let error = selector_not_found_error_from_cached_snapshot(
            "#save-user",
            "click",
            "tab-active",
            r#"<html><body><button id="cancel">Cancel</button></body></html>"#,
            "http://127.0.0.1:8790/",
        );

        assert_eq!(error, "selector '#save-user' was not found");
    }

    #[test]
    fn chromium_transport_idle_timeout_keeps_cdp_alive_for_session_ttl() {
        let configured_startup_timeout = Duration::from_secs(20);

        let timeout = chromium_transport_idle_timeout(configured_startup_timeout);

        assert_eq!(timeout, Duration::from_millis(DEFAULT_SESSION_IDLE_TTL_MS));
    }

    #[test]
    fn chromium_upload_staging_path_preserves_visible_basename() {
        let upload_dir = Path::new("/tmp/palyra-uploads");
        let staged = chromium_upload_staging_path(upload_dir, "upload-source.txt")
            .expect("upload staging path should be created");

        assert_eq!(staged.file_name().and_then(|value| value.to_str()), Some("upload-source.txt"));
        assert_ne!(staged.parent(), Some(upload_dir));
        assert!(staged.starts_with(upload_dir));
    }

    #[test]
    fn chromium_upload_staging_path_sanitizes_visible_basename_without_prefixing_it() {
        let upload_dir = Path::new("/tmp/palyra-uploads");
        let staged = chromium_upload_staging_path(upload_dir, "../upload source.csv")
            .expect("upload staging path should be created");
        let file_name = staged.file_name().and_then(|value| value.to_str()).unwrap_or_default();
        let staging_dir_name = staged
            .parent()
            .and_then(|value| value.file_name())
            .and_then(|value| value.to_str())
            .unwrap_or_default();

        assert_eq!(file_name, "upload_source.csv");
        assert_ne!(staging_dir_name, file_name);
        assert_eq!(staged.parent().and_then(|value| value.parent()), Some(upload_dir));
    }

    #[test]
    fn parse_chromium_console_entries_accepts_json_string_payload() {
        let raw = serde_json::Value::String(
            r#"[{"severity":"error","kind":"console","message":"boom","captured_at_unix_ms":42,"source":"console.error","stack_trace":"","page_url":"http://127.0.0.1/"}]"#
                .to_owned(),
        );

        let entries = parse_chromium_console_entries(decode_chromium_console_entries_value(raw));

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].message, "boom");
        assert_eq!(entries[0].source, "console.error");
        assert_eq!(entries[0].captured_at_unix_ms, 42);
    }

    #[test]
    fn decode_chromium_console_entries_rejects_oversized_string_payload() {
        let raw = serde_json::Value::String(format!(
            "[{}]",
            " ".repeat(MAX_CHROMIUM_CONSOLE_JSON_BYTES + 1)
        ));

        let decoded = decode_chromium_console_entries_value(raw);

        assert!(
            decoded.as_array().is_some_and(Vec::is_empty),
            "oversized console payload must be dropped before serde parsing"
        );
    }

    #[test]
    fn chromium_diagnostics_read_scripts_bound_page_controlled_payloads() {
        assert!(
            !CHROMIUM_PAGE_DIAGNOSTICS_SCRIPT.contains("snapshotEntries"),
            "the page hook should not export callable diagnostics snapshots into page state"
        );
        assert!(
            !CHROMIUM_READ_CONSOLE_LOG_SCRIPT.contains("snapshotEntries"),
            "console reads must not call page-defined snapshot functions"
        );
        assert!(
            CHROMIUM_READ_CONSOLE_LOG_SCRIPT.contains("MAX_CONSOLE_JSON_CHARS"),
            "console reads should enforce a page-side aggregate JSON budget"
        );
        assert!(
            CHROMIUM_DRAIN_NETWORK_LOG_SCRIPT.contains("MAX_NETWORK_JSON_CHARS"),
            "network diagnostics reads should enforce a page-side aggregate JSON budget"
        );
        assert!(
            CHROMIUM_READ_CONSOLE_LOG_SCRIPT.contains("clampScalar")
                && CHROMIUM_DRAIN_NETWORK_LOG_SCRIPT.contains("clampScalar"),
            "diagnostics reads should only serialize bounded scalar fields"
        );
        assert!(
            CHROMIUM_READ_CONSOLE_LOG_SCRIPT.contains("Array.prototype.slice.call")
                && CHROMIUM_DRAIN_NETWORK_LOG_SCRIPT.contains("Array.prototype.slice.call"),
            "diagnostics reads should not call page-overridable array slice methods"
        );
    }

    #[test]
    fn decode_chromium_network_entries_rejects_oversized_string_payload() {
        let raw = serde_json::Value::String(format!(
            "[{}]",
            " ".repeat(MAX_CHROMIUM_NETWORK_JSON_BYTES + 1)
        ));

        let decoded = decode_chromium_network_entries_value(raw);

        assert!(
            decoded.as_array().is_some_and(Vec::is_empty),
            "oversized network diagnostics payload must be dropped before serde parsing"
        );
    }

    #[test]
    fn parse_chromium_console_entries_truncates_fields_before_storage() {
        let raw = serde_json::json!([{
            "severity": "warn",
            "kind": "console",
            "message": "m".repeat(MAX_CONSOLE_MESSAGE_BYTES + 128),
            "captured_at_unix_ms": 42_u64,
            "source": "s".repeat(MAX_CONSOLE_SOURCE_BYTES + 128),
            "stack_trace": "t".repeat(MAX_CONSOLE_STACK_BYTES + 128),
            "page_url": "u".repeat(MAX_NETWORK_LOG_URL_BYTES + 128)
        }]);

        let entries = parse_chromium_console_entries(raw);

        assert_eq!(entries.len(), 1);
        assert!(entries[0].message.len() <= MAX_CONSOLE_MESSAGE_BYTES);
        assert!(entries[0].source.len() <= MAX_CONSOLE_SOURCE_BYTES);
        assert!(entries[0].stack_trace.len() <= MAX_CONSOLE_STACK_BYTES);
        assert!(entries[0].page_url.len() <= MAX_NETWORK_LOG_URL_BYTES);
    }

    #[test]
    fn decode_chromium_json_script_value_accepts_stringified_status() {
        let raw = serde_json::Value::String(r#"{"status":"selected","value":"north"}"#.to_owned());

        let decoded = decode_chromium_json_script_value(raw);

        assert_eq!(decoded["status"], "selected");
        assert_eq!(decoded["value"], "north");
    }

    #[test]
    fn parse_chromium_local_storage_snapshot_accepts_bounded_entries() {
        let raw = serde_json::Value::String(
            r#"{"ok":true,"origin":"http://127.0.0.1:49152","entries":{"cart":"1","theme":"dark"}}"#
                .to_owned(),
        );

        let (origin, entries) =
            parse_chromium_local_storage_snapshot(decode_chromium_json_script_value(raw))
                .expect("snapshot payload should parse")
                .expect("origin should be present");

        assert_eq!(origin, "http://127.0.0.1:49152");
        assert_eq!(entries.get("cart").map(String::as_str), Some("1"));
        assert_eq!(entries.get("theme").map(String::as_str), Some("dark"));
    }

    #[test]
    fn parse_chromium_document_cookie_snapshot_accepts_visible_cookies() {
        let raw = serde_json::Value::String(
            r#"{"ok":true,"domain":"LOCALHOST","cookie":"qaCookie=visible; theme=dark"}"#
                .to_owned(),
        );

        let updates =
            parse_chromium_document_cookie_snapshot(decode_chromium_json_script_value(raw))
                .expect("document.cookie payload should parse");

        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].domain, "localhost");
        assert_eq!(updates[0].name, "qacookie");
        assert_eq!(updates[0].value, "visible");
        assert_eq!(updates[1].name, "theme");
        assert_eq!(updates[1].value, "dark");
    }

    #[test]
    fn chromium_document_cookie_script_bounds_page_controlled_payload() {
        let read_script = chromium_read_document_cookies_script();

        assert!(read_script.contains("MAX_COOKIE_CHARS"));
        assert!(read_script.contains(MAX_CHROMIUM_DOCUMENT_COOKIE_JSON_BYTES.to_string().as_str()));
        assert!(
            read_script.contains("JSON.stringify"),
            "document.cookie reads should return a machine-readable bounded payload"
        );
    }

    #[test]
    fn parse_chromium_local_storage_restore_status_surfaces_page_errors() {
        let raw = serde_json::json!({
            "ok": false,
            "error": "quota exceeded"
        });

        let error = parse_chromium_local_storage_restore_status(raw)
            .expect_err("restore failure should remain visible");

        assert!(error.contains("quota exceeded"));
    }

    #[test]
    fn chromium_local_storage_scripts_bound_and_escape_persisted_payloads() {
        let read_script = chromium_read_local_storage_script();
        assert!(
            read_script.contains("MAX_STORAGE_JSON_CHARS"),
            "localStorage reads should enforce a page-side aggregate JSON budget"
        );
        assert!(
            read_script.contains(MAX_CHROMIUM_LOCAL_STORAGE_JSON_BYTES.to_string().as_str()),
            "localStorage read script should use the Rust-side JSON budget"
        );

        let script = chromium_restore_local_storage_script(&HashMap::from([(
            "quote'\"".to_owned(),
            "</script><b>x</b>".to_owned(),
        )]))
        .expect("restore script should encode entries");

        assert!(script.contains(r#""quote'\"":"</script><b>x</b>""#));
        assert!(
            script.contains("JSON.stringify"),
            "restore script should return a machine-readable status payload"
        );
    }

    #[test]
    fn parse_chromium_viewport_metrics_falls_back_to_requested_values() {
        let raw = serde_json::Value::String(r#"{"width":375,"height":667}"#.to_owned());

        let (width, height, device_scale_factor) =
            parse_chromium_viewport_metrics(decode_chromium_json_script_value(raw), 390, 844, 2.0);

        assert_eq!(width, 375);
        assert_eq!(height, 667);
        assert_eq!(device_scale_factor, 2.0);
    }

    #[test]
    fn parse_chromium_viewport_metrics_keeps_layout_viewport_size() {
        let raw = serde_json::json!({
            "visual_width": 531,
            "visual_height": 944,
            "width": 375,
            "height": 667,
            "device_scale_factor": 2.0
        });

        let (width, height, device_scale_factor) =
            parse_chromium_viewport_metrics(raw, 375, 667, 1.0);

        assert_eq!(width, 375);
        assert_eq!(height, 667);
        assert_eq!(device_scale_factor, 2.0);
    }

    #[test]
    fn desktop_touch_emulation_omits_invalid_zero_touch_points() {
        assert_eq!(chromium_touch_emulation_max_touch_points(false), None);
        assert_eq!(chromium_touch_emulation_max_touch_points(true), Some(1));
    }

    #[test]
    fn parse_chromium_layout_metrics_reports_overflow() {
        let raw = serde_json::json!({
            "viewport_width": 390,
            "viewport_height": 844,
            "device_scale_factor": 2.0,
            "document_scroll_width": 980,
            "document_scroll_height": 1200,
            "document_client_width": 390,
            "document_client_height": 844
        });

        let metrics = parse_chromium_layout_metrics(raw);

        assert_eq!(
            metrics,
            ChromiumLayoutMetrics {
                viewport_width: 390,
                viewport_height: 844,
                device_scale_factor: 2.0,
                document_scroll_width: 980,
                document_scroll_height: 1200,
                document_client_width: 390,
                document_client_height: 844,
                horizontal_overflow: true,
                vertical_overflow: true,
            }
        );
    }

    #[test]
    fn parse_chromium_layout_metrics_keeps_layout_viewport_for_overflow() {
        let raw = serde_json::json!({
            "viewport_width": 375,
            "viewport_height": 667,
            "visual_viewport_width": 531,
            "visual_viewport_height": 944,
            "device_scale_factor": 2.0,
            "document_scroll_width": 531,
            "document_scroll_height": 1200,
            "document_client_width": 531,
            "document_client_height": 944,
            "horizontal_overflow": false,
            "vertical_overflow": false
        });

        let metrics = parse_chromium_layout_metrics(raw);

        assert_eq!(metrics.viewport_width, 375);
        assert_eq!(metrics.viewport_height, 667);
        assert_eq!(metrics.document_client_width, 531);
        assert!(metrics.horizontal_overflow);
        assert!(metrics.vertical_overflow);
    }

    #[test]
    fn parse_key_press_spec_accepts_common_space_aliases() {
        let (key, modifiers) =
            parse_key_press_spec("Space").expect("Space alias should be accepted");
        assert_eq!(key, " ");
        assert!(modifiers.is_empty());

        let (key, modifiers) =
            parse_key_press_spec("Spacebar").expect("Spacebar alias should be accepted");
        assert_eq!(key, " ");
        assert!(modifiers.is_empty());

        let (key, modifiers) =
            parse_key_press_spec("Ctrl+Space").expect("modified Space alias should be accepted");
        assert_eq!(key, " ");
        assert_eq!(modifiers.len(), 1);
        assert_eq!(modifiers[0] as u32, 2);

        let (key, modifiers) =
            parse_key_press_spec(" ").expect("literal space key should be accepted");
        assert_eq!(key, " ");
        assert!(modifiers.is_empty());
    }

    #[test]
    fn chromium_network_log_headers_redact_sensitive_values() {
        let headers = crate::Network::Headers(Some(serde_json::json!({
            "Set-Cookie": "session=abc123",
            "Location": "https://example.test/callback?token=secret",
            "X-Trace": "ok"
        })));

        let parsed = chromium_network_log_headers(&headers);

        assert!(parsed
            .iter()
            .any(|header| { header.name == "set-cookie" && header.value == "<redacted>" }));
        assert!(parsed.iter().any(|header| {
            header.name == "location" && header.value.contains("token=<redacted>")
        }));
        assert!(parsed.iter().any(|header| header.name == "x-trace" && header.value == "ok"));
    }

    #[test]
    fn parse_chromium_page_network_entries_preserves_failed_fetch_status() {
        let raw = serde_json::json!([
            {
                "request_url": "http://127.0.0.1:4242/api/profile?token=secret",
                "status_code": 500,
                "latency_ms": 37,
                "captured_at_unix_ms": 42,
                "headers": [{"name": "Set-Cookie", "value": "session=abc123"}]
            }
        ]);

        let entries = parse_chromium_page_network_entries(raw);

        assert_eq!(entries.len(), 1);
        assert!(entries[0].request_url.contains("token=<redacted>"));
        assert_eq!(entries[0].status_code, 500);
        assert_eq!(entries[0].latency_ms, 37);
        assert!(entries[0]
            .headers
            .iter()
            .any(|header| header.name == "set-cookie" && header.value == "<redacted>"));
    }

    #[test]
    fn parse_chromium_client_download_entries_decodes_blob_payloads() {
        let raw = serde_json::json!([
            {
                "source_url": "blob:http://127.0.0.1:4338/01234567-89ab-cdef-0123-456789abcdef",
                "file_name": "upload export.csv",
                "mime_type": "text/csv;charset=utf-8",
                "content_base64": base64::engine::general_purpose::STANDARD.encode("id,name\n1,Ada\n")
            },
            {
                "source_url": "https://example.test/report.csv",
                "file_name": "ignored.csv",
                "mime_type": "text/csv",
                "content_base64": "aWdub3JlZA=="
            }
        ]);

        let entries = parse_chromium_client_download_entries(raw);

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].file_name, "upload_export.csv");
        assert_eq!(entries[0].mime_type, "text/csv;charset=utf-8");
        assert_eq!(entries[0].content, b"id,name\n1,Ada\n");
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
    let (tab_id, tab) = match chromium_active_tab_for_session(runtime, session_id).await {
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
                    let _ =
                        chromium_refresh_tab_snapshot(runtime, session_id, tab_id.as_str()).await;
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
                    let _ =
                        chromium_refresh_tab_snapshot(runtime, session_id, tab_id.as_str()).await;
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
