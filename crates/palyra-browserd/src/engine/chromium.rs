//! Chromium-backed browser engine driven over CDP via `headless_chrome`.
//!
//! Owns per-session browser processes, tabs, page-side diagnostics hooks, the
//! per-session SOCKS5 egress proxy, and the remote-IP security guard. The CDP
//! client is fully synchronous, so every browser call in this module must run
//! through [`run_chromium_blocking`].

use crate::*;
use headless_chrome::protocol::cdp::{types::Event, Browser, Emulation, Page};
use headless_chrome::{
    browser::tab::ModifierKey,
    types::{Bounds, PrintToPdfOptions},
};

/// Outcome of a Chromium DOM action (click, type, press, select, highlight, file input).
///
/// `outcome` is a stable machine-readable status label; `error` is empty on success.
#[derive(Debug)]
pub(crate) struct ChromiumActionOutcome {
    pub(crate) success: bool,
    pub(crate) outcome: String,
    pub(crate) error: String,
    pub(crate) attempts: u32,
}

/// Scroll positions reported by the page after a Chromium scroll action.
#[derive(Debug)]
pub(crate) struct ChromiumScrollOutcome {
    pub(crate) success: bool,
    pub(crate) scroll_x: i64,
    pub(crate) scroll_y: i64,
    pub(crate) error: String,
}

/// Result of applying device-metrics emulation to the active tab.
///
/// `metric_mismatch` flags when the page reports a viewport different from the
/// requested dimensions.
#[derive(Debug)]
pub(crate) struct ChromiumViewportOutcome {
    pub(crate) success: bool,
    pub(crate) width: u32,
    pub(crate) height: u32,
    pub(crate) device_scale_factor: f64,
    pub(crate) mobile: bool,
    pub(crate) metric_mismatch: bool,
    pub(crate) error: String,
}

/// Result of polling the active tab for a selector and/or text condition.
#[derive(Debug)]
pub(crate) struct ChromiumWaitOutcome {
    pub(crate) success: bool,
    pub(crate) matched_selector: String,
    pub(crate) matched_text: String,
    pub(crate) attempts: u32,
    pub(crate) waited_ms: u64,
    pub(crate) error: String,
}

/// Native dialog inspection or mutation result returned to the gRPC layer.
#[derive(Debug)]
pub(crate) struct ChromiumDialogOutcome {
    pub(crate) success: bool,
    pub(crate) present: bool,
    pub(crate) event: Option<BrowserDialogEvent>,
    pub(crate) mutated_page: bool,
    pub(crate) timed_out: bool,
    pub(crate) error_code: String,
    pub(crate) error: String,
}

/// Raw page snapshot (HTML body, title, URL) read from a live tab.
#[derive(Debug)]
pub(crate) struct ChromiumObserveSnapshot {
    pub(crate) page_body: String,
    pub(crate) title: String,
    pub(crate) page_url: String,
}

/// Layout and visual viewport metrics with derived overflow flags.
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

const CHROMIUM_VIEWPORT_HEIGHT_TOLERANCE_PX: u32 = 80;

#[derive(Debug, Default, Deserialize)]
struct ChromiumElementCapturePayload {
    #[serde(default)]
    selector: String,
    #[serde(default)]
    found: bool,
    #[serde(default)]
    rect: ChromiumElementRectPayload,
    #[serde(default)]
    visible: bool,
    #[serde(default)]
    tag_name: String,
    #[serde(default)]
    id: String,
    #[serde(default)]
    class_name: String,
    #[serde(default)]
    text: String,
    #[serde(default)]
    text_truncated: bool,
    #[serde(default)]
    computed_styles: Vec<ChromiumComputedStylePayload>,
    #[serde(default)]
    error: String,
}

#[derive(Debug, Default, Deserialize)]
struct ChromiumElementRectPayload {
    #[serde(default)]
    x: f64,
    #[serde(default)]
    y: f64,
    #[serde(default)]
    width: f64,
    #[serde(default)]
    height: f64,
    #[serde(default)]
    top: f64,
    #[serde(default)]
    right: f64,
    #[serde(default)]
    bottom: f64,
    #[serde(default)]
    left: f64,
}

#[derive(Debug, Default, Deserialize)]
struct ChromiumComputedStylePayload {
    #[serde(default)]
    name: String,
    #[serde(default)]
    value: String,
}

/// A page-captured client-side download with decoded bytes.
#[derive(Debug)]
pub(crate) struct ChromiumClientDownload {
    pub(crate) source_url: String,
    pub(crate) file_name: String,
    pub(crate) mime_type: String,
    pub(crate) content: Vec<u8>,
}

struct DiscoveredChromiumTab {
    tab: Arc<HeadlessTab>,
    network_log: Arc<std::sync::Mutex<VecDeque<NetworkLogEntryInternal>>>,
    dialog_tracker: Arc<std::sync::Mutex<ChromiumDialogTracker>>,
    url: String,
    title: String,
}

struct ChromiumTabRuntimeHooks {
    tab_id: String,
    network_log: Arc<std::sync::Mutex<VecDeque<NetworkLogEntryInternal>>>,
    dialog_tracker: Arc<std::sync::Mutex<ChromiumDialogTracker>>,
    health: Arc<std::sync::Mutex<BrowserSessionHealth>>,
    resilience_profile: BrowserResilienceProfile,
}

type ChromiumLocalStorageSnapshot = Option<(String, HashMap<String, String>)>;

/// Parameters for a guarded Chromium navigation.
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

/// Page-side hook installed on every new document: wraps console, fetch, XHR,
/// object-URL creation, and anchor clicks to buffer bounded console, network,
/// and client-download (blob) entries under a window global that the
/// drain/read scripts below read back.
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
      const valueType = typeof value;
      if (valueType === "string") return clampString(value, MAX_CONSOLE_MESSAGE_CHARS);
      if (valueType === "bigint") return clampString(`${value}n`, MAX_CONSOLE_MESSAGE_CHARS);
      if (valueType === "symbol") return clampString(String(value), MAX_CONSOLE_MESSAGE_CHARS);
      if (valueType === "function") return "[Function]";
      if (value === null) return "null";
      if (valueType === "object") return Array.isArray(value) ? "[Array]" : "[Object]";
      return clampString(value, MAX_CONSOLE_MESSAGE_CHARS);
    } catch (_) {
      return "[Unserializable]";
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
  state.client_download_generation = Number(state.client_download_generation || 0);
  state.object_urls = state.object_urls || {};
  state.pending_client_downloads = Number(state.pending_client_downloads || 0);
  const MAX_CLIENT_DOWNLOAD_ENTRIES = 32;
  const MAX_CLIENT_DOWNLOAD_BYTES = 8 * 1024 * 1024;
  const blobSizeGetter = typeof Blob === "function"
    ? Object.getOwnPropertyDescriptor(Blob.prototype, "size")?.get
    : null;
  const blobArrayBuffer = typeof Blob === "function" ? Blob.prototype.arrayBuffer : null;
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
    if (typeof blobSizeGetter !== "function" || typeof blobArrayBuffer !== "function") {
      throw new Error("client-side Blob inspection is unavailable");
    }
    const sizeBytes = Number(blobSizeGetter.call(blob));
    if (!Number.isSafeInteger(sizeBytes) || sizeBytes < 0) {
      throw new Error("client-side download reported an invalid size");
    }
    if (sizeBytes > MAX_CLIENT_DOWNLOAD_BYTES) {
      throw new Error(`client-side download exceeds max bytes (${sizeBytes} > ${MAX_CLIENT_DOWNLOAD_BYTES})`);
    }
    const buffer = await blobArrayBuffer.call(blob);
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
      if (!blob || typeof blobSizeGetter !== "function" || typeof blobArrayBuffer !== "function") {
        return;
      }
      const fileName = clampDownloadFileName(anchor.getAttribute("download") || "");
      const captureGeneration = Number(state.client_download_generation || 0);
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
            capture_generation: captureGeneration,
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

/// Reads buffered console entries back as bounded JSON (newest entries win the budget).
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

/// Drains buffered page network entries as bounded JSON, clearing the page-side buffer.
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

const CHROMIUM_CLEAR_NETWORK_LOG_SCRIPT: &str = r#"
(() => {
  const state = window.__palyraDiagnostics;
  if (state && Array.isArray(state.network_entries)) {
    state.network_entries.length = 0;
  }
  return true;
})()
"#;

/// Drains captured client-side (blob) downloads, waiting up to 750ms for
/// in-flight blob reads started by recent anchor clicks to settle.
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
  const captureGeneration = Number(state.client_download_generation || 0);
  const source = Array.prototype.slice.call(
    state.client_download_entries,
    Math.max(0, state.client_download_entries.length - MAX_CLIENT_DOWNLOAD_ENTRIES)
  ).filter((entry) => Number((entry && entry.capture_generation) || 0) === captureGeneration);
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

const CHROMIUM_BEGIN_CLIENT_DOWNLOAD_CAPTURE_SCRIPT: &str = r#"
(() => {
  const diagnostics = window.__palyraDiagnostics;
  if (!diagnostics || !Array.isArray(diagnostics.client_download_entries)) {
    return false;
  }
  const previousGeneration = Number(diagnostics.client_download_generation || 0);
  diagnostics.client_download_generation =
    Number.isSafeInteger(previousGeneration) && previousGeneration >= 0
      ? previousGeneration + 1
      : 1;
  diagnostics.client_download_entries.length = 0;
  return true;
})()
"#;

// Page-side JSON budgets get headroom over the decoded-entry budgets because
// the raw JSON also carries field names and escaping overhead before parsing.
const MAX_CHROMIUM_CONSOLE_JSON_BYTES: usize = (DEFAULT_MAX_CONSOLE_LOG_BYTES as usize) * 4;
const MAX_CHROMIUM_NETWORK_JSON_BYTES: usize = (DEFAULT_MAX_NETWORK_LOG_BYTES as usize) * 4;
const MAX_CHROMIUM_CLIENT_DOWNLOAD_JSON_BYTES: usize =
    (DOWNLOAD_MAX_FILE_BYTES as usize * 2) + 16 * 1024;
const MAX_CHROMIUM_DOCUMENT_COOKIE_JSON_BYTES: usize = (MAX_COOKIES_PER_DOMAIN * 1536) + 4096;
const MAX_CHROMIUM_LOCAL_STORAGE_JSON_BYTES: usize =
    (MAX_STORAGE_ENTRY_VALUE_BYTES * MAX_STORAGE_ENTRIES_PER_ORIGIN * 2) + 4096;
const MAX_CHROMIUM_ELEMENT_CAPTURE_JSON_BYTES: usize = 64 * 1024;
const MAX_CHROMIUM_OBSERVE_FORM_CONTROLS: usize = 128;
const MAX_CHROMIUM_OBSERVE_STATE_TEXT_BYTES: usize = 16 * 1024;
const CHROMIUM_SELECT_STATUS_NOT_FOUND: u64 = 0;
const CHROMIUM_SELECT_STATUS_NOT_SELECT: u64 = 1;
const CHROMIUM_SELECT_STATUS_DISABLED: u64 = 2;
const CHROMIUM_SELECT_STATUS_VALUE_NOT_FOUND: u64 = 3;
const CHROMIUM_SELECT_STATUS_SELECTED: u64 = 4;

#[derive(Debug, Default, Deserialize)]
struct ChromiumObserveStatePayload {
    #[serde(default)]
    html: String,
    #[serde(default)]
    form_controls: Vec<ChromiumObservedFormControl>,
    #[serde(default)]
    state_elements: Vec<ChromiumObservedStateElement>,
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
struct ChromiumObservedStateElement {
    #[serde(default)]
    tag: String,
    #[serde(default)]
    id: String,
    #[serde(default)]
    selector: String,
    #[serde(default)]
    hidden: bool,
    #[serde(default)]
    visible: bool,
    #[serde(default)]
    reason: String,
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

/// Builds the observe-state script: clones the DOM with form/storage values
/// withheld and collects bounded state metadata alongside the serialized HTML.
fn chromium_observe_state_script() -> String {
    format!(
        r#"
(() => {{
  const MAX_FORM_CONTROLS = {max_form_controls};
  const MAX_STATE_ELEMENTS = {max_state_elements};
  const MAX_STORAGE_ENTRIES = {max_storage_entries};
  const MAX_STORAGE_KEY_CHARS = 512;
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
  const selectorForStateElement = (element, tag) => {{
    const id = clampScalar(element && element.id, 128).trim();
    if (id) {{
      return `#${{id}}`;
    }}
    const testId = clampScalar(element && element.getAttribute && element.getAttribute("data-testid"), 128).trim();
    if (testId) {{
      return `[data-testid="${{testId.replace(/"/g, '\\"')}}"]`;
    }}
    return tag || "element";
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
    const valuePresent = Boolean(element.value);
    const clonedValue = valuePresent ? "<redacted>" : "";
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
      ? Array.prototype.slice.call(element.selectedOptions || [], 0, 16).map(() => "<redacted>")
      : [];
    formControls.push({{
      tag,
      type,
      id: clampScalar(element.id, 128),
      name: clampScalar(element.getAttribute("name"), 128),
      selector: selectorFor(element, tag),
      value: clonedValue,
      checked: tag === "input" && (type === "checkbox" || type === "radio") ? Boolean(element.checked) : null,
      selected_options: selectedOptions
    }});
  }});
  const stateElements = [];
  const seenStateSelectors = new Set();
  const stateCandidates = Array.prototype.slice.call(
    document.querySelectorAll("[hidden], [aria-hidden], section[id], dialog[id], form[id], [role='tabpanel'][id], [role='dialog'][id], [data-testid]"),
    0,
    MAX_STATE_ELEMENTS * 4
  );
  stateCandidates.forEach((element) => {{
    if (stateElements.length >= MAX_STATE_ELEMENTS) {{
      return;
    }}
    const tag = clampScalar((element.tagName || "").toLowerCase(), 32);
    const selector = selectorForStateElement(element, tag);
    if (!selector || seenStateSelectors.has(selector)) {{
      return;
    }}
    seenStateSelectors.add(selector);
    const hiddenAttr = Boolean(element.hidden) || Boolean(element.hasAttribute && element.hasAttribute("hidden"));
    const ariaHidden = clampScalar(element.getAttribute && element.getAttribute("aria-hidden"), 16).toLowerCase() === "true";
    let cssHidden = false;
    let hasLayoutBox = true;
    try {{
      const style = window.getComputedStyle ? window.getComputedStyle(element) : null;
      cssHidden = Boolean(style && (style.display === "none" || style.visibility === "hidden" || style.visibility === "collapse"));
      const rects = element.getClientRects ? Array.prototype.slice.call(element.getClientRects()) : [];
      hasLayoutBox = rects.some((rect) => Number(rect.width || 0) > 0 && Number(rect.height || 0) > 0);
    }} catch (_) {{
      hasLayoutBox = true;
    }}
    const visible = !(hiddenAttr || ariaHidden || cssHidden) && hasLayoutBox;
    const reason = hiddenAttr
      ? "hidden_attribute"
      : ariaHidden
        ? "aria_hidden"
        : cssHidden
          ? "css_hidden"
          : hasLayoutBox
            ? "visible"
            : "no_layout_box";
    stateElements.push({{
      tag,
      id: clampScalar(element.id, 128),
      selector,
      hidden: hiddenAttr || ariaHidden || cssHidden || !hasLayoutBox,
      visible,
      reason
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
        const entryChars = JSON.stringify(key).length + 4;
        if (count > 0 && totalChars + entryChars > MAX_STORAGE_JSON_CHARS) {{
          break;
        }}
        if (totalChars + entryChars > MAX_STORAGE_JSON_CHARS) {{
          continue;
        }}
        entries[key] = "";
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
    state_elements: stateElements,
    local_storage: Object.assign({{ origin }}, readStorage(() => window.localStorage)),
    session_storage: Object.assign({{ origin }}, readStorage(() => window.sessionStorage))
  }});
}})()
"#,
        max_form_controls = MAX_CHROMIUM_OBSERVE_FORM_CONTROLS,
        max_state_elements = MAX_CHROMIUM_OBSERVE_FORM_CONTROLS,
        max_storage_entries = MAX_STORAGE_ENTRIES_PER_ORIGIN,
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
    let summary = build_chromium_observe_state_summary(&payload);
    let page_body = payload.html;
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
    for element in payload.state_elements.iter().take(MAX_CHROMIUM_OBSERVE_FORM_CONTROLS) {
        lines.push(chromium_observed_state_element_line(element));
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
        parts.push(format!(
            "selected_options_count={}",
            control.selected_options.iter().take(16).count()
        ));
    }
    let value_display = if control.value.trim().is_empty() { "" } else { "<redacted>" };
    parts.push(format!("value={}", line_quote(value_display)));
    parts.join(" ")
}

fn chromium_observed_state_element_line(element: &ChromiumObservedStateElement) -> String {
    let mut parts = vec!["browser_state_element".to_owned()];
    append_observe_part(&mut parts, "selector", element.selector.as_str(), 128);
    append_observe_part(&mut parts, "tag", element.tag.as_str(), 32);
    append_observe_part(&mut parts, "id", element.id.as_str(), 128);
    parts.push(format!("hidden={}", element.hidden));
    parts.push(format!("visible={}", element.visible));
    append_observe_part(&mut parts, "reason", element.reason.as_str(), 64);
    parts.join(" ")
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
    for (key, _value) in entries.into_iter().take(MAX_STORAGE_ENTRIES_PER_ORIGIN) {
        let key_display = sanitize_debug_text(key.as_str(), 128);
        lines.push(format!(
            "browser_storage kind={} origin={} key={} value={}",
            storage_kind,
            line_quote(origin.as_str()),
            line_quote(key_display.as_str()),
            line_quote("<redacted>")
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

/// Runs a synchronous `headless_chrome` operation on the blocking thread pool.
///
/// The CDP client blocks on its transport, so every browser call must hop off
/// the async runtime through this helper to avoid stalling executor threads.
///
/// # Errors
/// Returns the task's own error, or a join-failure message when the blocking
/// task panicked or was cancelled.
pub(crate) async fn run_chromium_blocking<T, F>(operation: &str, task: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, String> + Send + 'static,
{
    tokio::task::spawn_blocking(task)
        .await
        .map_err(|error| format!("{operation} task join failure: {error}"))?
}

/// Per-session loopback SOCKS5 proxy that enforces private-target policy on
/// all Chromium egress before a CONNECT succeeds.
///
/// Dropping the proxy signals shutdown and aborts the accept-loop task.
#[derive(Debug)]
pub(crate) struct ChromiumSessionProxy {
    pub(crate) proxy_uri: String,
    private_target_policy: Arc<ChromiumPrivateTargetPolicy>,
    pub(crate) shutdown_tx: Option<oneshot::Sender<()>>,
    pub(crate) task: tokio::task::JoinHandle<()>,
}

impl ChromiumSessionProxy {
    /// Binds a loopback listener and spawns the SOCKS5 accept loop.
    ///
    /// # Errors
    /// Returns an error string when the listener cannot be bound or its local
    /// address cannot be resolved.
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

    /// Returns the policy handle shared with request interception and response guards.
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

/// Target host parsed from a SOCKS5 CONNECT request.
#[derive(Debug)]
pub(crate) enum Socks5TargetHost {
    Ip(IpAddr),
    Domain(String),
}

/// A normalized private target (network host:port, or a canonicalized local
/// file) that a session can be explicitly allowed to reach.
#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub(crate) enum ChromiumPrivateTargetScope {
    Network { host: String, port: u16 },
    File(PathBuf),
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
struct ChromiumPrivateTargetRequestScope {
    tab_target_id: String,
    url: ChromiumPrivateTargetUrlScope,
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
struct ChromiumPrivateTargetTabScope {
    tab_target_id: String,
    target: ChromiumPrivateTargetScope,
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
enum ChromiumPrivateTargetUrlScope {
    Network { scheme: String, host: String, port: u16, path: String, query: Option<String> },
    File(PathBuf),
}

#[derive(Debug, Default)]
struct ChromiumPrivateTargetState {
    scoped_requests: HashMap<ChromiumPrivateTargetRequestScope, usize>,
    pending_proxy_targets: HashMap<ChromiumPrivateTargetTabScope, usize>,
}

/// Tracks which private/local targets a session may reach.
///
/// Deny-by-default: unless the whole session allows private targets, a private
/// destination is reachable only while its tab-bound navigation scope is alive.
#[derive(Debug)]
pub(crate) struct ChromiumPrivateTargetPolicy {
    allow_session_private_targets: bool,
    state: std::sync::Mutex<ChromiumPrivateTargetState>,
}

/// RAII allowance for one private target; the allowance is released on drop.
#[derive(Debug)]
pub(crate) struct ChromiumScopedPrivateTarget {
    policy: Arc<ChromiumPrivateTargetPolicy>,
    scope: ChromiumPrivateTargetRequestScope,
}

impl ChromiumPrivateTargetPolicy {
    /// Creates a policy; `allow_session_private_targets` bypasses all scoping.
    pub(crate) fn new(allow_session_private_targets: bool) -> Self {
        Self {
            allow_session_private_targets,
            state: std::sync::Mutex::new(ChromiumPrivateTargetState::default()),
        }
    }

    /// Returns whether the URL's target is allowed without a tab-bound scope.
    ///
    /// Unparseable URLs are denied (fail closed) when the session does not
    /// allow private targets wholesale.
    pub(crate) fn allows_url(&self, _raw_url: &str) -> bool {
        self.allow_session_private_targets
    }

    #[cfg(test)]
    pub(crate) fn allows_tab_url(&self, tab_target_id: &str, raw_url: &str) -> bool {
        if self.allow_session_private_targets {
            return true;
        }
        let Ok(Some(scope)) =
            ChromiumPrivateTargetRequestScope::from_tab_url(tab_target_id, raw_url)
        else {
            return false;
        };
        self.allows_exact_request_scope(&scope)
    }

    pub(crate) fn allows_tab_request_target(&self, tab_target_id: &str, raw_url: &str) -> bool {
        if self.allow_session_private_targets {
            return true;
        }
        let Ok(Some(scope)) =
            ChromiumPrivateTargetRequestScope::from_tab_url(tab_target_id, raw_url)
        else {
            return false;
        };
        self.allows_exact_request_scope(&scope)
    }

    pub(crate) fn authorize_tab_request_url(&self, tab_target_id: &str, raw_url: &str) -> bool {
        if self.allow_session_private_targets {
            return true;
        }
        let Ok(Some(scope)) =
            ChromiumPrivateTargetRequestScope::from_tab_url(tab_target_id, raw_url)
        else {
            return false;
        };
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        if !state.scoped_requests.contains_key(&scope) {
            return false;
        }
        let requested_target = scope.target_scope();
        if matches!(requested_target, ChromiumPrivateTargetScope::Network { .. }) {
            let count = state.pending_proxy_targets.entry(scope.tab_scope()).or_insert(0);
            *count = count.saturating_add(1);
        }
        true
    }

    /// Returns whether the host/port pair is currently allowed; invalid hosts are denied.
    ///
    /// Scoped allowances are consumed one CONNECT at a time after a tab-bound
    /// request interceptor authorizes the matching URL.
    pub(crate) fn allows_host_port(&self, host: &str, port: u16) -> bool {
        if self.allow_session_private_targets {
            return true;
        }
        let Ok(scope) = ChromiumPrivateTargetScope::network(host, port) else {
            return false;
        };
        self.consume_proxy_scope(&scope)
    }

    /// Grants a temporary allowance for the URL's target, released when the
    /// returned guard drops. Returns `None` when no allowance is needed
    /// (session-wide allow, or a non-private target such as `about:blank`).
    ///
    /// # Errors
    /// Returns an error string when the URL cannot be normalized into a scope
    /// or the policy lock is poisoned.
    pub(crate) fn scoped_url_allowance(
        self: &Arc<Self>,
        tab_target_id: &str,
        raw_url: &str,
    ) -> Result<Option<ChromiumScopedPrivateTarget>, String> {
        if self.allow_session_private_targets {
            return Ok(None);
        }
        if validate_target_url_blocking(raw_url, false).is_ok() {
            return Ok(None);
        }
        let Some(scope) = ChromiumPrivateTargetRequestScope::from_tab_url(tab_target_id, raw_url)?
        else {
            return Ok(None);
        };
        let mut state =
            self.state.lock().map_err(|_| "private-target policy lock was poisoned".to_owned())?;
        let count = state.scoped_requests.entry(scope.clone()).or_insert(0);
        *count = count.saturating_add(1);
        Ok(Some(ChromiumScopedPrivateTarget { policy: Arc::clone(self), scope }))
    }

    fn allows_exact_request_scope(&self, scope: &ChromiumPrivateTargetRequestScope) -> bool {
        self.state.lock().map(|state| state.scoped_requests.contains_key(scope)).unwrap_or(false)
    }

    fn consume_proxy_scope(&self, scope: &ChromiumPrivateTargetScope) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        let Some(tab_scope) =
            state.pending_proxy_targets.keys().find(|pending| pending.target == *scope).cloned()
        else {
            return false;
        };
        match state.pending_proxy_targets.get_mut(&tab_scope) {
            Some(count) if *count > 1 => {
                *count -= 1;
                true
            }
            Some(_) => {
                state.pending_proxy_targets.remove(&tab_scope);
                true
            }
            None => false,
        }
    }

    fn release_scope(&self, scope: &ChromiumPrivateTargetRequestScope) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        match state.scoped_requests.get_mut(scope) {
            Some(count) if *count > 1 => *count -= 1,
            Some(_) => {
                state.scoped_requests.remove(scope);
            }
            None => {}
        }
        let tab_scope = scope.tab_scope();
        let target_still_scoped = state.scoped_requests.keys().any(|active| {
            active.tab_target_id == tab_scope.tab_target_id
                && active.target_scope() == tab_scope.target
        });
        if !target_still_scoped {
            state.pending_proxy_targets.remove(&tab_scope);
        }
    }
}

impl ChromiumPrivateTargetScope {
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

impl ChromiumPrivateTargetRequestScope {
    fn from_tab_url(tab_target_id: &str, raw_url: &str) -> Result<Option<Self>, String> {
        if tab_target_id.trim().is_empty() {
            return Err("private-target tab scope id must not be empty".to_owned());
        }
        let Some(url) = ChromiumPrivateTargetUrlScope::from_url(raw_url)? else {
            return Ok(None);
        };
        Ok(Some(Self { tab_target_id: tab_target_id.to_owned(), url }))
    }

    fn target_scope(&self) -> ChromiumPrivateTargetScope {
        self.url.target_scope()
    }

    fn tab_scope(&self) -> ChromiumPrivateTargetTabScope {
        ChromiumPrivateTargetTabScope {
            tab_target_id: self.tab_target_id.clone(),
            target: self.target_scope(),
        }
    }
}

impl ChromiumPrivateTargetUrlScope {
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
        let normalized_host = if let Some(address) = netguard::parse_host_ip_literal(host)? {
            address.to_string()
        } else {
            normalize_dns_host_cache_key(host)
        };
        if normalized_host.is_empty() {
            return Err("private-target scope host must not be empty".to_owned());
        }
        Ok(Some(Self::Network {
            scheme: url.scheme().to_ascii_lowercase(),
            host: normalized_host,
            port,
            path: url.path().to_owned(),
            query: url.query().map(ToOwned::to_owned),
        }))
    }

    fn target_scope(&self) -> ChromiumPrivateTargetScope {
        match self {
            Self::Network { host, port, .. } => {
                ChromiumPrivateTargetScope::Network { host: host.clone(), port: *port }
            }
            Self::File(path) => ChromiumPrivateTargetScope::File(path.clone()),
        }
    }
}

impl Drop for ChromiumScopedPrivateTarget {
    fn drop(&mut self) {
        self.policy.release_scope(&self.scope);
    }
}

/// Accept loop for the per-session SOCKS5 proxy.
///
/// Exits on the shutdown signal or on a listener accept failure; individual
/// client failures are logged and do not stop the loop.
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

/// Builds a SOCKS5 reply with the given status code and a zeroed IPv4 bind address.
pub(crate) fn socks5_reply(status: u8) -> [u8; 10] {
    [0x05, status, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
}

/// Reads the SOCKS5 CONNECT target host for the given address type byte.
///
/// # Errors
/// Returns an error string on read failures, unsupported address types, and
/// empty or non-UTF-8 domain targets.
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

/// Serves one SOCKS5 client: no-auth handshake, CONNECT-only command parsing,
/// resolved-host policy enforcement, then bidirectional byte relay.
///
/// # Errors
/// Returns an error string on protocol violations, policy denials (after a
/// failure reply has been sent best-effort), connect failures, and relay IO
/// errors.
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

/// Builds hardened launch options for a per-session Chromium process.
///
/// # Errors
/// Returns an error string when the launch options builder rejects the
/// configuration.
pub(crate) fn build_chromium_launch_options<'a>(
    chromium: &ChromiumEngineConfig,
    profile_dir: &TempDir,
    proxy_server: Option<&'a str>,
) -> Result<headless_chrome::LaunchOptions<'a>, String> {
    let chromium_path = chromium.executable_path.clone();
    // `--disable-dev-shm-usage` avoids tiny /dev/shm limits in containers;
    // `--disable-blink-features=AutomationControlled` keeps pages from
    // trivially fingerprinting the session as automated.
    let mut chromium_args = vec![
        OsStr::new("--disable-dev-shm-usage"),
        OsStr::new("--disable-gpu"),
        OsStr::new("--no-first-run"),
        OsStr::new("--no-default-browser-check"),
        OsStr::new("--window-size=1280,800"),
        OsStr::new("--disable-blink-features=AutomationControlled"),
    ];
    if proxy_server.is_some() {
        // Chromium bypasses proxies for loopback by default; `<-loopback>`
        // forces loopback traffic through the per-session SOCKS5 proxy so
        // NetGuard policy also covers local targets.
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

// The CDP websocket must stay alive at least as long as an idle session may be
// resumed, otherwise headless_chrome tears down the transport mid-session.
fn chromium_transport_idle_timeout(startup_timeout: Duration) -> Duration {
    startup_timeout.max(Duration::from_millis(DEFAULT_SESSION_IDLE_TTL_MS))
}

/// Parses an IP literal as reported by CDP, tolerating bracketed IPv6 forms.
pub(crate) fn parse_chromium_remote_ip_literal(raw: &str) -> Option<IpAddr> {
    let trimmed = raw.trim().trim_start_matches('[').trim_end_matches(']');
    trimmed.parse::<IpAddr>().ok()
}

/// Records a security incident when a response was served from a private or
/// local IP that the session's policy does not allow.
///
/// Only the first incident is kept; loopback hits matching the expected
/// per-session proxy hop are ignored.
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

/// Reuses the incident cell when a Chromium process is replaced so a response
/// recorded by the old process cannot disappear during reconnect.
pub(crate) fn chromium_security_incident_for_launch(
    existing: Option<&Arc<std::sync::Mutex<Option<String>>>>,
) -> Arc<std::sync::Mutex<Option<String>>> {
    existing.cloned().unwrap_or_else(|| Arc::new(std::sync::Mutex::new(None::<String>)))
}

/// Returns true when a loopback remote IP is just the local SOCKS5 proxy hop
/// for an otherwise policy-clean response URL.
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

fn install_chromium_dialog_listener(
    tab: &Arc<HeadlessTab>,
    tab_id: String,
    tracker: Arc<std::sync::Mutex<ChromiumDialogTracker>>,
    health: Arc<std::sync::Mutex<BrowserSessionHealth>>,
    profile: BrowserResilienceProfile,
) -> Result<(), String> {
    let weak_tab = Arc::downgrade(tab);
    tab.add_event_listener(Arc::new(move |event: &Event| match event {
        Event::PageJavascriptDialogOpening(opening) => {
            let dialog_type = format!("{:?}", opening.params.Type).to_ascii_lowercase();
            let default_prompt = opening.params.default_prompt.as_deref().unwrap_or_default();
            let generation = match tracker.lock() {
                Ok(mut guard) => {
                    guard
                        .record_opening(
                            tab_id.as_str(),
                            dialog_type.as_str(),
                            opening.params.message.as_str(),
                            default_prompt,
                            opening.params.url.as_str(),
                            profile,
                        )
                        .generation
                }
                Err(_) => {
                    warn!(
                        "failed to record Chromium dialog opening because tracker lock is poisoned"
                    );
                    return;
                }
            };
            let Some(tab) = weak_tab.upgrade() else {
                return;
            };
            let timeout_tracker = Arc::clone(&tracker);
            let timeout_health = Arc::clone(&health);
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(profile.dialog_timeout_ms));
                let pending_event = timeout_tracker
                    .lock()
                    .ok()
                    .and_then(|guard| guard.pending())
                    .filter(|event| event.generation == generation);
                let Some(pending_event) = pending_event else {
                    return;
                };
                let dismissed = tab.get_dialog().dismiss().is_ok();
                if dismissed {
                    if let Ok(mut guard) = timeout_tracker.lock() {
                        guard.remember_resolution(
                            pending_event,
                            BrowserDialogResolutionKind::TimedOut,
                        );
                    }
                } else {
                    warn!(generation, "failed to apply safe default to expired Chromium dialog");
                }
                if let Ok(mut health) = timeout_health.lock() {
                    health.record_dialog_timeout(dismissed);
                }
            });
        }
        Event::PageJavascriptDialogClosed(_) => {
            if let Ok(mut guard) = tracker.lock() {
                guard.clear();
            }
        }
        _ => {}
    }))
    .map(|_| ())
    .map_err(|error| format!("failed to register Chromium dialog callback: {error}"))
}

/// Wires a fresh tab with policy and diagnostics: request interception that
/// fails disallowed targets, network log capture, page diagnostics hooks, and
/// the remote-IP response guard.
///
/// # Errors
/// Returns an error string when any CDP registration call fails.
fn configure_chromium_tab(
    tab: &Arc<HeadlessTab>,
    hooks: ChromiumTabRuntimeHooks,
    private_target_policy: Arc<ChromiumPrivateTargetPolicy>,
    timeout: Duration,
    security_incident: Arc<std::sync::Mutex<Option<String>>>,
) -> Result<(), String> {
    tab.set_default_timeout(timeout);
    tab.enable_fetch(None, Some(false))
        .map_err(|error| format!("failed to enable Chromium fetch interception: {error}"))?;
    let tab_target_id = tab.get_target_id().to_string();
    let request_policy = Arc::clone(&private_target_policy);
    let request_tab_target_id = tab_target_id.clone();
    let request_interceptor =
        Arc::new(move |_transport, _session_id, intercepted: Fetch::events::RequestPausedEvent| {
            let request_url = intercepted.params.request.url.as_str();
            let allow_private_targets = request_policy
                .authorize_tab_request_url(request_tab_target_id.as_str(), request_url);
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
    let network_log_buffer = Arc::clone(&hooks.network_log);
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
    tab.call_method(Page::AddScriptToEvaluateOnNewDocument {
        source: CHROMIUM_PAGE_DIAGNOSTICS_SCRIPT.to_owned(),
        world_name: None,
        include_command_line_api: None,
        run_immediately: None,
    })
    .map_err(|error| format!("failed to register Chromium page diagnostics hooks: {error}"))?;
    let remote_ip_guard = Arc::clone(&security_incident);
    let response_policy = Arc::clone(&private_target_policy);
    let response_tab_target_id = tab_target_id;
    tab.register_response_handling(
        CHROMIUM_REMOTE_IP_GUARD_HANDLER_NAME,
        Box::new(move |response, _fetch_body| {
            let allow_private_targets = response_policy.allows_tab_request_target(
                response_tab_target_id.as_str(),
                response.response.url.as_str(),
            );
            record_chromium_remote_ip_incident(
                Some(response.response.url.as_str()),
                response.response.remote_ip_address.as_deref(),
                allow_private_targets,
                &remote_ip_guard,
            );
        }),
    )
    .map_err(|error| format!("failed to register Chromium response guard callback: {error}"))?;
    install_chromium_dialog_listener(
        tab,
        hooks.tab_id,
        hooks.dialog_tracker,
        hooks.health,
        hooks.resilience_profile,
    )?;
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

/// Returns true for transient `new_tab` failures seen during Chromium startup races.
pub(crate) fn chromium_new_tab_error_is_retryable(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    normalized.contains("event waited for never came")
        || (normalized.contains("websocket protocol error")
            && normalized.contains("sending after closing is not allowed"))
        || normalized.contains("underlying connection is closed")
}

/// Creates and configures a tab, retrying transient startup-race failures.
///
/// Runs synchronously and must be called from a blocking context (see
/// [`run_chromium_blocking`]); the retry delay is a blocking sleep on purpose.
///
/// # Errors
/// Returns `{failure_prefix}: ...` when tab creation fails terminally or
/// exhausts its retry budget, or the configuration error from
/// [`configure_chromium_tab`].
fn create_configured_chromium_tab_with_retry(
    browser: &Arc<HeadlessBrowser>,
    hooks: ChromiumTabRuntimeHooks,
    private_target_policy: Arc<ChromiumPrivateTargetPolicy>,
    timeout: Duration,
    security_incident: Arc<std::sync::Mutex<Option<String>>>,
    failure_prefix: &str,
) -> Result<Arc<HeadlessTab>, String> {
    for attempt in 1..=CHROMIUM_NEW_TAB_MAX_ATTEMPTS {
        match browser.new_tab() {
            Ok(tab) => {
                configure_chromium_tab(
                    &tab,
                    hooks,
                    Arc::clone(&private_target_policy),
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
                    // Blocking sleep is correct: this helper always runs
                    // inside spawn_blocking (see run_chromium_blocking).
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

async fn launch_chromium_session_runtime(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    session: &BrowserSessionRecord,
    health: Arc<std::sync::Mutex<BrowserSessionHealth>>,
) -> Result<(), String> {
    let chromium = runtime.chromium.clone();
    let resilience_profile = runtime.resilience_profile;
    let allow_private_targets = session.allow_private_targets;
    let navigation_timeout = Duration::from_millis(session.budget.max_navigation_timeout_ms.max(1));
    let active_tab_id = session.active_tab_id.clone();
    let restored_tabs = session.tabs.clone();
    let storage_entries_by_origin = session.storage_entries.clone();
    let mut tab_order = session.tab_order.clone();
    if tab_order.is_empty() {
        tab_order.push(active_tab_id.clone());
    } else if !tab_order.iter().any(|tab_id| tab_id == &active_tab_id) {
        tab_order.insert(0, active_tab_id.clone());
    }
    // `proxy` stays owned here until the launch succeeds so a failed launch
    // still shuts the SOCKS5 task down via Drop; it is attached to the session
    // state only afterwards.
    let proxy = ChromiumSessionProxy::spawn(allow_private_targets).await?;
    let proxy_uri = proxy.proxy_uri.clone();
    let private_target_policy = proxy.private_target_policy();
    let security_incident = {
        let chromium_sessions = runtime.chromium_sessions.lock().await;
        chromium_security_incident_for_launch(
            chromium_sessions.get(session_id).map(|session| &session.security_incident),
        )
    };
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
            let mut dialog_trackers = HashMap::new();
            for tab_id in tab_order.iter() {
                let network_log = Arc::new(std::sync::Mutex::new(VecDeque::new()));
                let dialog_tracker =
                    Arc::new(std::sync::Mutex::new(ChromiumDialogTracker::default()));
                let tab = create_configured_chromium_tab_with_retry(
                    &browser,
                    ChromiumTabRuntimeHooks {
                        tab_id: tab_id.clone(),
                        network_log: Arc::clone(&network_log),
                        dialog_tracker: Arc::clone(&dialog_tracker),
                        health: Arc::clone(&health),
                        resilience_profile,
                    },
                    Arc::clone(&private_target_policy),
                    navigation_timeout,
                    Arc::clone(&security_incident),
                    "failed to create Chromium tab for session restore",
                )?;
                if let Some(restored_tab) = restored_tabs.get(tab_id.as_str()) {
                    if let Err(error) = restore_chromium_tab_live_state(
                        &tab,
                        tab_id.as_str(),
                        restored_tab,
                        &storage_entries_by_origin,
                        navigation_timeout,
                    ) {
                        warn!(
                            tab_id = tab_id.as_str(),
                            error = error.as_str(),
                            "failed to restore live Chromium tab state from persisted snapshot"
                        );
                    }
                }
                tabs.insert(tab_id.clone(), tab);
                network_logs.insert(tab_id.clone(), network_log);
                dialog_trackers.insert(tab_id.clone(), dialog_tracker);
            }
            Ok(ChromiumSessionState {
                browser,
                tabs,
                network_logs,
                dialog_trackers,
                health,
                private_target_policy,
                security_incident,
                device_scale_factor: 1.0,
                staged_upload_bytes: Arc::new(AtomicU64::new(0)),
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

/// Launches the per-session Chromium process and restores persisted tabs.
///
/// # Errors
/// Returns an error string when the proxy spawn, profile-dir allocation, browser launch, or tab
/// creation fails; per-tab live-state restore failures are logged and skipped.
pub(crate) async fn initialize_chromium_session_runtime(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    session: &BrowserSessionRecord,
) -> Result<(), String> {
    let health = Arc::new(std::sync::Mutex::new(BrowserSessionHealth::default()));
    runtime.browser_session_health.lock().await.insert(session_id.to_owned(), Arc::clone(&health));
    match launch_chromium_session_runtime(runtime, session_id, session, Arc::clone(&health)).await {
        Ok(()) => {
            if let Ok(mut health) = health.lock() {
                health.mark_initial_ready();
            }
            Ok(())
        }
        Err(error) => {
            if let Ok(mut health) = health.lock() {
                health.mark_reconnect_failed();
            }
            Err(error)
        }
    }
}

fn restore_chromium_tab_live_state(
    tab: &Arc<HeadlessTab>,
    tab_id: &str,
    restored_tab: &BrowserTabRecord,
    storage_entries_by_origin: &HashMap<String, HashMap<String, String>>,
    navigation_timeout: Duration,
) -> Result<(), String> {
    let Some(raw_url) = restored_tab
        .last_url
        .as_deref()
        .map(str::trim)
        .filter(|url| !url.is_empty() && !url.eq_ignore_ascii_case("about:blank"))
    else {
        return Ok(());
    };
    tab.set_default_timeout(navigation_timeout);
    tab.navigate_to(raw_url).map_err(|error| {
        format!("failed to restore Chromium tab {tab_id} URL {raw_url}: {error}")
    })?;
    tab.wait_until_navigated().map_err(|error| {
        format!("Chromium tab {tab_id} restore navigation timed out for {raw_url}: {error}")
    })?;
    let page_url = tab.get_url();
    if let Some(origin) = url_origin_key(page_url.as_str()) {
        if let Some(entries) =
            storage_entries_by_origin.get(origin.as_str()).filter(|entries| !entries.is_empty())
        {
            // localStorage can only be written from a document on the target
            // origin, and scripts that already ran will not see the restored
            // values; navigate first, write storage, then reload so the app
            // boots with the restored state.
            let script = chromium_restore_local_storage_script(entries)?;
            let raw_value = tab
                .evaluate(script.as_str(), true)
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
                format!(
                    "failed to reload Chromium tab {tab_id} after localStorage restore: {error}"
                )
            })?;
            tab.wait_until_navigated().map_err(|error| {
                format!(
                    "Chromium tab {tab_id} reload after localStorage restore timed out: {error}"
                )
            })?;
        }
    }
    if restored_tab.scroll_x != 0 || restored_tab.scroll_y != 0 {
        let script =
            format!("window.scrollTo({}, {}); true", restored_tab.scroll_x, restored_tab.scroll_y);
        let _ = tab.evaluate(script.as_str(), true);
    }
    Ok(())
}

/// Allocates and configures a new live Chromium tab for an existing session.
///
/// # Errors
/// Returns the `session_not_found`/`chromium_session_not_found` sentinels when
/// the session is gone, or a descriptive message when tab allocation fails.
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
    let (browser, private_target_policy, security_incident, health) = {
        let chromium_sessions = runtime.chromium_sessions.lock().await;
        let Some(chromium_session) = chromium_sessions.get(session_id) else {
            return Err("chromium_session_not_found".to_owned());
        };
        (
            Arc::clone(&chromium_session.browser),
            Arc::clone(&chromium_session.private_target_policy),
            Arc::clone(&chromium_session.security_incident),
            Arc::clone(&chromium_session.health),
        )
    };
    let owned_tab_id = tab_id.to_owned();
    let resilience_profile = runtime.resilience_profile;
    let (tab, network_log, dialog_tracker) =
        run_chromium_blocking("chromium open tab", move || {
            let network_log = Arc::new(std::sync::Mutex::new(VecDeque::new()));
            let dialog_tracker = Arc::new(std::sync::Mutex::new(ChromiumDialogTracker::default()));
            let tab = create_configured_chromium_tab_with_retry(
                &browser,
                ChromiumTabRuntimeHooks {
                    tab_id: owned_tab_id,
                    network_log: Arc::clone(&network_log),
                    dialog_tracker: Arc::clone(&dialog_tracker),
                    health,
                    resilience_profile,
                },
                private_target_policy,
                Duration::from_millis(timeout_ms),
                security_incident,
                "failed to allocate Chromium tab",
            )?;
            Ok((tab, network_log, dialog_tracker))
        })
        .await?;
    let mut chromium_sessions = runtime.chromium_sessions.lock().await;
    let Some(chromium_session) = chromium_sessions.get_mut(session_id) else {
        return Err("chromium_session_not_found".to_owned());
    };
    chromium_session.tabs.insert(tab_id.to_owned(), tab);
    chromium_session.network_logs.insert(tab_id.to_owned(), network_log);
    chromium_session.dialog_trackers.insert(tab_id.to_owned(), dialog_tracker);
    Ok(())
}

/// Registers live Chromium tabs opened outside `palyra.browser.tabs.open`
/// (for example, `window.open(..., "_blank")`) into the Palyra session model.
///
/// # Errors
/// Returns lookup sentinels, tab-budget errors, remote-IP guard incidents, or a
/// CDP/configuration failure while discovering new Chromium targets.
pub(crate) async fn chromium_sync_session_tabs(
    runtime: &BrowserRuntimeState,
    session_id: &str,
) -> Result<u32, String> {
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    let (browser, private_target_policy, security_incident, health, timeout, existing_target_ids) = {
        let sessions = runtime.sessions.lock().await;
        let Some(session) = sessions.get(session_id) else {
            return Err("session_not_found".to_owned());
        };
        let timeout = Duration::from_millis(session.budget.max_navigation_timeout_ms.max(1));
        let chromium_sessions = runtime.chromium_sessions.lock().await;
        let Some(chromium_session) = chromium_sessions.get(session_id) else {
            return Err("chromium_session_not_found".to_owned());
        };
        let existing_target_ids = chromium_session
            .tabs
            .values()
            .map(|tab| tab.get_target_id().to_string())
            .collect::<HashSet<_>>();
        (
            Arc::clone(&chromium_session.browser),
            Arc::clone(&chromium_session.private_target_policy),
            Arc::clone(&chromium_session.security_incident),
            Arc::clone(&chromium_session.health),
            timeout,
            existing_target_ids,
        )
    };
    let resilience_profile = runtime.resilience_profile;
    let discovered = run_chromium_blocking("chromium sync session tabs", move || {
        browser.register_missing_tabs();
        let live_tabs = browser
            .get_tabs()
            .lock()
            .map_err(|_| "failed to inspect Chromium live tabs".to_owned())?
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let mut discovered = Vec::new();
        for tab in live_tabs {
            let target_id = tab.get_target_id().to_string();
            if existing_target_ids.contains(target_id.as_str()) {
                continue;
            }
            let _ = tab.wait_until_navigated();
            let url = tab.get_url();
            match chromium_sync_tab_url_is_allowed(url.as_str(), private_target_policy.as_ref()) {
                Ok(true) => {}
                Ok(false) => continue,
                Err(error) => {
                    let scheme = Url::parse(url.as_str())
                        .map(|url| url.scheme().to_owned())
                        .unwrap_or_else(|_| "invalid".to_owned());
                    warn!(
                        scheme = scheme.as_str(),
                        error = error.as_str(),
                        "ignored Chromium popup tab that failed target validation"
                    );
                    continue;
                }
            }
            let network_log = Arc::new(std::sync::Mutex::new(VecDeque::new()));
            let dialog_tracker = Arc::new(std::sync::Mutex::new(ChromiumDialogTracker::default()));
            configure_chromium_tab(
                &tab,
                ChromiumTabRuntimeHooks {
                    tab_id: target_id,
                    network_log: Arc::clone(&network_log),
                    dialog_tracker: Arc::clone(&dialog_tracker),
                    health: Arc::clone(&health),
                    resilience_profile,
                },
                Arc::clone(&private_target_policy),
                timeout,
                Arc::clone(&security_incident),
            )?;
            let title = tab.get_title().unwrap_or_default();
            discovered.push(DiscoveredChromiumTab { tab, network_log, dialog_tracker, url, title });
        }
        Ok(discovered)
    })
    .await?;
    if discovered.is_empty() {
        return Ok(0);
    }

    let mut new_tab_ids = Vec::new();
    {
        let mut sessions = runtime.sessions.lock().await;
        let Some(session) = sessions.get_mut(session_id) else {
            return Err("session_not_found".to_owned());
        };
        let mut chromium_sessions = runtime.chromium_sessions.lock().await;
        let Some(chromium_session) = chromium_sessions.get_mut(session_id) else {
            return Err("chromium_session_not_found".to_owned());
        };
        for discovered_tab in discovered {
            if !session.can_create_tab() {
                return Err("tab budget exceeded while registering Chromium popup tab".to_owned());
            }
            let tab_id = session.create_tab();
            if let Some(record) = session.tabs.get_mut(tab_id.as_str()) {
                record.last_url =
                    (!discovered_tab.url.trim().is_empty()).then(|| discovered_tab.url.clone());
                record.last_title = discovered_tab.title.clone();
            }
            session.active_tab_id = tab_id.clone();
            chromium_session.tabs.insert(tab_id.clone(), discovered_tab.tab);
            chromium_session.network_logs.insert(tab_id.clone(), discovered_tab.network_log);
            chromium_session.dialog_trackers.insert(tab_id.clone(), discovered_tab.dialog_tracker);
            new_tab_ids.push(tab_id);
        }
    }

    if let Err(error) = chromium_apply_current_session_permissions(runtime, session_id).await {
        return Err(format!(
            "failed to apply Chromium page permissions after syncing tabs: {error}"
        ));
    }

    for tab_id in &new_tab_ids {
        let _ = chromium_install_page_diagnostics(runtime, session_id, tab_id.as_str()).await;
        let _ = chromium_refresh_tab_snapshot(runtime, session_id, tab_id.as_str()).await;
    }
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    Ok(u32::try_from(new_tab_ids.len()).unwrap_or(u32::MAX))
}

/// Removes a tab's runtime state and closes the live tab best-effort.
///
/// # Errors
/// Returns the `chromium_session_not_found` sentinel when the session is gone;
/// failures while closing the already-detached tab are ignored.
pub(crate) async fn chromium_close_tab_runtime(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
) -> Result<(), String> {
    let (tab, tracker, health) = {
        let chromium_sessions = runtime.chromium_sessions.lock().await;
        let Some(chromium_session) = chromium_sessions.get(session_id) else {
            return Err("chromium_session_not_found".to_owned());
        };
        (
            chromium_session.tabs.get(tab_id).cloned(),
            chromium_session.dialog_trackers.get(tab_id).cloned(),
            Arc::clone(&chromium_session.health),
        )
    };
    if let (Some(tab), Some(tracker)) = (tab.as_ref(), tracker) {
        let pending = tracker
            .lock()
            .map_err(|_| "failed to inspect Chromium dialog state before page close".to_owned())?
            .pending();
        if let Some(event) = pending {
            let close_tab = Arc::clone(tab);
            let _ = run_chromium_blocking("chromium dialog page-close cleanup", move || {
                close_tab.get_dialog().dismiss().map_err(|error| {
                    format!("failed to dismiss Chromium dialog before page close: {error}")
                })
            })
            .await;
            if let Ok(mut tracker) = tracker.lock() {
                tracker.remember_resolution(event, BrowserDialogResolutionKind::PageCloseCleanup);
            }
            if let Ok(mut health) = health.lock() {
                health.record_dialog_close_cleanup();
            }
        }
    }
    let detached_tab = {
        let mut chromium_sessions = runtime.chromium_sessions.lock().await;
        let Some(chromium_session) = chromium_sessions.get_mut(session_id) else {
            return Err("chromium_session_not_found".to_owned());
        };
        chromium_session.network_logs.remove(tab_id);
        chromium_session.dialog_trackers.remove(tab_id);
        chromium_session.tabs.remove(tab_id)
    };
    if let Some(tab) = detached_tab {
        let _ = run_chromium_blocking("chromium close tab", move || {
            tab.close(true).map_err(|error| format!("failed to close Chromium tab: {error}"))?;
            Ok(())
        })
        .await;
    }
    Ok(())
}

/// Fails closed when the response guard recorded a private/local remote-IP
/// incident, tearing down the entire browser session.
///
/// Callers invoke this both before and after each Chromium operation so an
/// incident observed by the response handler mid-operation still fails the
/// call that triggered it.
///
/// # Errors
/// Returns the incident reason after terminating the session, or a lock
/// failure message when the incident state cannot be inspected.
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
    runtime.browser_session_health.lock().await.remove(session_id);
    runtime.download_sessions.lock().await.remove(session_id);
    warn!(
        session_id = session_id,
        reason = reason.as_str(),
        "terminated browser session after Chromium remote IP guard incident"
    );
    Err(format!("chromium remote IP guard blocked request: {reason}"))
}

enum ChromiumRuntimeProbe {
    Healthy(Arc<HeadlessTab>),
    ProcessUnavailable(String),
    TargetUnavailable(String),
}

async fn chromium_session_health_tracker(
    runtime: &BrowserRuntimeState,
    session_id: &str,
) -> Arc<std::sync::Mutex<BrowserSessionHealth>> {
    let mut health = runtime.browser_session_health.lock().await;
    Arc::clone(
        health
            .entry(session_id.to_owned())
            .or_insert_with(|| Arc::new(std::sync::Mutex::new(BrowserSessionHealth::default()))),
    )
}

async fn probe_chromium_runtime(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
) -> ChromiumRuntimeProbe {
    let (browser, tab) = {
        let chromium_sessions = runtime.chromium_sessions.lock().await;
        let Some(chromium_session) = chromium_sessions.get(session_id) else {
            return ChromiumRuntimeProbe::ProcessUnavailable(
                "chromium_session_not_found".to_owned(),
            );
        };
        let Some(tab) = chromium_session.tabs.get(tab_id) else {
            return ChromiumRuntimeProbe::TargetUnavailable("chromium_tab_not_found".to_owned());
        };
        (Arc::clone(&chromium_session.browser), Arc::clone(tab))
    };
    let probe_tab = Arc::clone(&tab);
    match run_chromium_blocking("chromium runtime health probe", move || {
        if let Err(error) = browser.get_version() {
            return Ok(ChromiumRuntimeProbe::ProcessUnavailable(sanitize_debug_text(
                error.to_string().as_str(),
                512,
            )));
        }
        if let Err(error) = probe_tab.get_target_info() {
            return Ok(ChromiumRuntimeProbe::TargetUnavailable(sanitize_debug_text(
                error.to_string().as_str(),
                512,
            )));
        }
        Ok(ChromiumRuntimeProbe::Healthy(probe_tab))
    })
    .await
    {
        Ok(result) => result,
        Err(error) => ChromiumRuntimeProbe::ProcessUnavailable(error),
    }
}

async fn reconnect_chromium_tab_runtime(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
) -> Result<Arc<HeadlessTab>, String> {
    let (restored_tab, storage_entries, navigation_timeout) = {
        let sessions = runtime.sessions.lock().await;
        let session = sessions.get(session_id).ok_or_else(|| "session_not_found".to_owned())?;
        let restored_tab =
            session.tabs.get(tab_id).cloned().ok_or_else(|| "tab_not_found".to_owned())?;
        (
            restored_tab,
            session.storage_entries.clone(),
            Duration::from_millis(session.budget.max_navigation_timeout_ms.max(1)),
        )
    };
    let (browser, private_target_policy, security_incident, health) = {
        let chromium_sessions = runtime.chromium_sessions.lock().await;
        let session = chromium_sessions
            .get(session_id)
            .ok_or_else(|| "chromium_session_not_found".to_owned())?;
        (
            Arc::clone(&session.browser),
            Arc::clone(&session.private_target_policy),
            Arc::clone(&session.security_incident),
            Arc::clone(&session.health),
        )
    };
    let owned_tab_id = tab_id.to_owned();
    let health_for_tab = Arc::clone(&health);
    let resilience_profile = runtime.resilience_profile;
    let (tab, network_log, dialog_tracker) =
        run_chromium_blocking("chromium target reconnect", move || {
            let network_log = Arc::new(std::sync::Mutex::new(VecDeque::new()));
            let dialog_tracker = Arc::new(std::sync::Mutex::new(ChromiumDialogTracker::default()));
            let tab = create_configured_chromium_tab_with_retry(
                &browser,
                ChromiumTabRuntimeHooks {
                    tab_id: owned_tab_id.clone(),
                    network_log: Arc::clone(&network_log),
                    dialog_tracker: Arc::clone(&dialog_tracker),
                    health: health_for_tab,
                    resilience_profile,
                },
                private_target_policy,
                navigation_timeout,
                security_incident,
                "failed to reconnect Chromium target",
            )?;
            restore_chromium_tab_live_state(
                &tab,
                owned_tab_id.as_str(),
                &restored_tab,
                &storage_entries,
                navigation_timeout,
            )?;
            Ok((tab, network_log, dialog_tracker))
        })
        .await?;

    let replaced_tab = {
        let mut chromium_sessions = runtime.chromium_sessions.lock().await;
        let session = chromium_sessions
            .get_mut(session_id)
            .ok_or_else(|| "chromium_session_not_found".to_owned())?;
        session.network_logs.insert(tab_id.to_owned(), network_log);
        session.dialog_trackers.insert(tab_id.to_owned(), dialog_tracker);
        session.tabs.insert(tab_id.to_owned(), Arc::clone(&tab))
    };
    if let Some(replaced_tab) = replaced_tab {
        let _ = run_chromium_blocking("chromium stale target close", move || {
            let _ = replaced_tab.close(false);
            Ok(())
        })
        .await;
    }
    if let Ok(mut health) = health.lock() {
        health.mark_target_reconnected();
    }
    Ok(tab)
}

async fn reconnect_chromium_process_runtime(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
) -> Result<Arc<HeadlessTab>, String> {
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    let session = {
        let sessions = runtime.sessions.lock().await;
        sessions.get(session_id).cloned().ok_or_else(|| "session_not_found".to_owned())?
    };
    let health = chromium_session_health_tracker(runtime, session_id).await;
    if let Ok(mut health) = health.lock() {
        health.mark_reconnecting();
    }
    if let Err(error) =
        launch_chromium_session_runtime(runtime, session_id, &session, Arc::clone(&health)).await
    {
        if let Ok(mut health) = health.lock() {
            health.mark_reconnect_failed();
        }
        return Err(format!("chromium process reconnect failed: {error}"));
    }
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    if let Ok(mut health) = health.lock() {
        health.mark_process_reconnected();
    }
    let chromium_sessions = runtime.chromium_sessions.lock().await;
    chromium_sessions
        .get(session_id)
        .and_then(|session| session.tabs.get(tab_id))
        .cloned()
        .ok_or_else(|| "chromium tab missing after process reconnect".to_owned())
}

/// Looks up the live tab handle for a session/tab pair.
///
/// # Errors
/// Returns lookup sentinels when resilience is disabled. With the resilient profile enabled,
/// probes process and target health, then performs one serialized target or process recovery.
pub(crate) async fn chromium_tab_for_session(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
) -> Result<Arc<HeadlessTab>, String> {
    if !runtime.resilience_profile.automatic_reconnect {
        let chromium_sessions = runtime.chromium_sessions.lock().await;
        let result = chromium_sessions
            .get(session_id)
            .ok_or_else(|| "chromium_session_not_found".to_owned())
            .and_then(|session| {
                session.tabs.get(tab_id).cloned().ok_or_else(|| "chromium_tab_not_found".to_owned())
            });
        drop(chromium_sessions);
        if result.is_err() {
            let health = chromium_session_health_tracker(runtime, session_id).await;
            if let Ok(mut health) = health.lock() {
                health.mark_reconnect_disabled();
            };
        }
        return result;
    }

    if let ChromiumRuntimeProbe::Healthy(tab) =
        probe_chromium_runtime(runtime, session_id, tab_id).await
    {
        return Ok(tab);
    }

    let _reconnect_guard = runtime.chromium_reconnect_lock.lock().await;
    match probe_chromium_runtime(runtime, session_id, tab_id).await {
        ChromiumRuntimeProbe::Healthy(tab) => Ok(tab),
        ChromiumRuntimeProbe::TargetUnavailable(reason) => {
            warn!(
                session_id,
                tab_id,
                reason = reason.as_str(),
                "reconnecting unavailable Chromium target"
            );
            match reconnect_chromium_tab_runtime(runtime, session_id, tab_id).await {
                Ok(tab) => Ok(tab),
                Err(target_error) => {
                    warn!(
                        session_id,
                        tab_id,
                        error = target_error.as_str(),
                        "target reconnect failed; replacing Chromium process"
                    );
                    reconnect_chromium_process_runtime(runtime, session_id, tab_id).await
                }
            }
        }
        ChromiumRuntimeProbe::ProcessUnavailable(reason) => {
            warn!(
                session_id,
                tab_id,
                reason = reason.as_str(),
                "reconnecting unavailable Chromium process"
            );
            reconnect_chromium_process_runtime(runtime, session_id, tab_id).await
        }
    }
}

/// Drains the CDP-captured network log buffered for a tab.
///
/// # Errors
/// Returns lookup sentinels when the session/log is gone, or a lock failure
/// message.
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

/// Clears all buffered network diagnostics for a session, both the Rust-side
/// CDP buffers and the page-side hook buffers (the latter best-effort).
///
/// # Errors
/// Returns the `chromium_session_not_found` sentinel or a lock failure message.
pub(crate) async fn chromium_clear_network_diagnostics(
    runtime: &BrowserRuntimeState,
    session_id: &str,
) -> Result<(), String> {
    let (tabs, network_logs) = {
        let chromium_sessions = runtime.chromium_sessions.lock().await;
        let Some(chromium_session) = chromium_sessions.get(session_id) else {
            return Err("chromium_session_not_found".to_owned());
        };
        (
            chromium_session
                .tabs
                .iter()
                .map(|(tab_id, tab)| (tab_id.clone(), Arc::clone(tab)))
                .collect::<Vec<_>>(),
            chromium_session.network_logs.values().cloned().collect::<Vec<_>>(),
        )
    };

    for network_log in network_logs {
        let mut guard = network_log
            .lock()
            .map_err(|_| "failed to clear Chromium network log state".to_owned())?;
        guard.clear();
    }
    for (tab_id, tab) in tabs {
        if let Err(error) = run_chromium_blocking("chromium clear page network log", move || {
            tab.evaluate(CHROMIUM_CLEAR_NETWORK_LOG_SCRIPT, false).map(|_| ()).map_err(|error| {
                format!("failed to clear Chromium page network diagnostics: {error}")
            })
        })
        .await
        {
            warn!(
                session_id,
                tab_id = tab_id.as_str(),
                error = error.as_str(),
                "failed to clear Chromium page network diagnostics"
            );
        }
    }

    Ok(())
}

/// Looks up a live tab together with the session's private-target policy.
///
/// # Errors
/// Returns the `chromium_session_not_found`/`chromium_tab_not_found` sentinels.
pub(crate) async fn chromium_tab_and_private_target_policy_for_session(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
) -> Result<(Arc<HeadlessTab>, Arc<ChromiumPrivateTargetPolicy>), String> {
    let tab = chromium_tab_for_session(runtime, session_id, tab_id).await?;
    let chromium_sessions = runtime.chromium_sessions.lock().await;
    let Some(chromium_session) = chromium_sessions.get(session_id) else {
        return Err("chromium_session_not_found".to_owned());
    };
    Ok((tab, Arc::clone(&chromium_session.private_target_policy)))
}

async fn chromium_cleanup_dialog_before_navigation(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
    tab: &Arc<HeadlessTab>,
) -> Result<(), String> {
    let (tracker, health) = {
        let chromium_sessions = runtime.chromium_sessions.lock().await;
        let session = chromium_sessions
            .get(session_id)
            .ok_or_else(|| "chromium_session_not_found".to_owned())?;
        (
            session
                .dialog_trackers
                .get(tab_id)
                .cloned()
                .ok_or_else(|| "chromium_dialog_tracker_not_found".to_owned())?,
            Arc::clone(&session.health),
        )
    };
    let pending =
        tracker.lock().map_err(|_| "failed to inspect Chromium dialog state".to_owned())?.pending();
    let Some(event) = pending else {
        return Ok(());
    };
    let cleanup_tab = Arc::clone(tab);
    run_chromium_blocking("chromium dialog navigation cleanup", move || {
        cleanup_tab.get_dialog().dismiss().map_err(|error| {
            format!("failed to dismiss Chromium dialog before navigation: {error}")
        })
    })
    .await?;
    tracker
        .lock()
        .map_err(|_| "failed to update Chromium dialog state".to_owned())?
        .remember_resolution(event, BrowserDialogResolutionKind::NavigationCleanup);
    if let Ok(mut health) = health.lock() {
        health.record_dialog_navigation_cleanup();
    }
    Ok(())
}

/// Resolves the session's active tab ID and its live tab handle.
///
/// # Errors
/// Returns the `session_not_found` sentinel or the lookup sentinels of
/// [`chromium_tab_for_session`].
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

/// Inspects or resolves the active tab's generation-fenced native dialog.
///
/// # Errors
/// Returns session/tab lookup sentinels or a CDP mutation failure. Stale
/// generations and invalid prompt operations are returned as typed outcomes.
pub(crate) async fn chromium_handle_dialog(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    action: BrowserDialogAction,
    expected_generation: u64,
    prompt_text: Option<String>,
) -> Result<ChromiumDialogOutcome, String> {
    let profile = runtime.resilience_profile;
    let (tab_id, tab) = chromium_active_tab_for_session(runtime, session_id).await?;
    let tracker = {
        let chromium_sessions = runtime.chromium_sessions.lock().await;
        let Some(chromium_session) = chromium_sessions.get(session_id) else {
            return Err("chromium_session_not_found".to_owned());
        };
        chromium_session
            .dialog_trackers
            .get(tab_id.as_str())
            .cloned()
            .ok_or_else(|| "chromium_dialog_tracker_not_found".to_owned())?
    };
    let (event, resolution) = {
        let tracker =
            tracker.lock().map_err(|_| "failed to inspect Chromium dialog state".to_owned())?;
        (
            tracker.pending(),
            (expected_generation != 0)
                .then(|| tracker.resolution_for_generation(expected_generation))
                .flatten(),
        )
    };
    let Some(mut event) = event else {
        if let Some(resolution) = resolution {
            let timed_out = resolution.kind == BrowserDialogResolutionKind::TimedOut;
            let (error_code, error) = if timed_out {
                (
                    "dialog_timed_out_safe_dismiss",
                    "dialog expired and was dismissed with the safe default",
                )
            } else {
                ("stale_dialog_generation", "dialog was cleared by browser lifecycle cleanup")
            };
            return Ok(ChromiumDialogOutcome {
                success: false,
                present: false,
                event: Some(resolution.event),
                mutated_page: false,
                timed_out,
                error_code: error_code.to_owned(),
                error: error.to_owned(),
            });
        }
        let (error_code, error) = if action.mutates_page() {
            ("dialog_not_found", "no native dialog is pending on the active tab")
        } else {
            ("", "")
        };
        return Ok(ChromiumDialogOutcome {
            success: !action.mutates_page(),
            present: false,
            event: None,
            mutated_page: false,
            timed_out: false,
            error_code: error_code.to_owned(),
            error: error.to_owned(),
        });
    };
    event.tab_id.clone_from(&tab_id);

    if expected_generation != 0 && expected_generation != event.generation {
        return Ok(ChromiumDialogOutcome {
            success: false,
            present: true,
            event: Some(event),
            mutated_page: false,
            timed_out: false,
            error_code: "stale_dialog_generation".to_owned(),
            error: "dialog generation changed before the requested action".to_owned(),
        });
    }
    if !action.mutates_page() {
        return Ok(ChromiumDialogOutcome {
            success: true,
            present: true,
            event: Some(event),
            mutated_page: false,
            timed_out: false,
            error_code: String::new(),
            error: String::new(),
        });
    }
    if action == BrowserDialogAction::Respond && event.dialog_type != "prompt" {
        return Ok(ChromiumDialogOutcome {
            success: false,
            present: true,
            event: Some(event),
            mutated_page: false,
            timed_out: false,
            error_code: "dialog_response_not_supported".to_owned(),
            error: "respond is valid only for a native prompt dialog".to_owned(),
        });
    }
    let prompt_text = prompt_text.unwrap_or_default();
    if prompt_text.len() > profile.max_prompt_response_bytes {
        return Ok(ChromiumDialogOutcome {
            success: false,
            present: true,
            event: Some(event),
            mutated_page: false,
            timed_out: false,
            error_code: "dialog_prompt_too_large".to_owned(),
            error: format!(
                "dialog prompt response exceeds {} bytes",
                profile.max_prompt_response_bytes
            ),
        });
    }

    let generation = event.generation;
    let dialog_action = action;
    run_chromium_blocking("chromium handle dialog", move || {
        let dialog = tab.get_dialog();
        match dialog_action {
            BrowserDialogAction::Accept => dialog.accept(None),
            BrowserDialogAction::Dismiss => dialog.dismiss(),
            BrowserDialogAction::Respond => dialog.accept(Some(prompt_text)),
            BrowserDialogAction::Inspect => unreachable!("inspection returned before CDP mutation"),
        }
        .map_err(|error| format!("failed to handle Chromium dialog: {error}"))
    })
    .await?;
    tracker
        .lock()
        .map_err(|_| "failed to update Chromium dialog state".to_owned())?
        .clear_generation(generation);
    Ok(ChromiumDialogOutcome {
        success: true,
        present: false,
        event: Some(event),
        mutated_page: true,
        timed_out: false,
        error_code: String::new(),
        error: String::new(),
    })
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
    if let Some(cached_tag) = find_matching_html_tag(selector, cached_page_body) {
        let location = chromium_action_context(tab_id, cached_url);
        let state_hint = cached_html_tag_actionability_hint(cached_tag.as_str())
            .map(|hint| format!("; cached element appeared {hint}"));
        return format!(
            "selector '{selector}' was not found in the live Chromium DOM for {action_name}, but the last observe snapshot for {location} still contained it{}; call observe or reload to refresh the active tab, verify visibility/actionability, any local server state, and retry only if the selector is present and actionable in the current snapshot",
            state_hint.unwrap_or_default()
        );
    }
    format!("selector '{selector}' was not found")
}

fn cached_html_tag_actionability_hint(tag: &str) -> Option<&'static str> {
    let lower = tag.to_ascii_lowercase();
    if lower.contains(" hidden")
        || lower.contains("\thidden")
        || lower.contains("\nhidden")
        || lower.contains("hidden=")
        || lower.contains("aria-hidden=\"true\"")
        || lower.contains("aria-hidden='true'")
    {
        return Some("hidden or aria-hidden");
    }
    if lower.contains("disabled") || lower.contains("aria-disabled=\"true\"") {
        return Some("disabled");
    }
    if lower.contains("display:none")
        || lower.contains("display: none")
        || lower.contains("visibility:hidden")
        || lower.contains("visibility: hidden")
    {
        return Some("hidden by inline style");
    }
    None
}

fn chromium_action_context(tab_id: &str, page_url: &str) -> String {
    let page_url = normalize_url_with_redaction(page_url);
    if page_url.is_empty() {
        format!("active tab {tab_id}")
    } else {
        format!("active tab {tab_id} at {page_url}")
    }
}

/// Captures a tab's page body, title, and URL, clamped to session budgets.
///
/// The body includes the observed-state summary section when the observe-state
/// script succeeds; otherwise it degrades to the raw DOM content.
///
/// # Errors
/// Returns lookup sentinels, remote-IP guard incidents, or a message when even
/// the raw DOM content cannot be read.
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

/// Captures geometry, visibility, text, and computed styles for the given
/// selectors on a live tab. Returns an empty list for an empty selector set.
///
/// # Errors
/// Returns lookup sentinels, remote-IP guard incidents, script encoding or
/// evaluation failures, and payload parse failures.
pub(crate) async fn chromium_capture_element_captures(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
    selectors: &[String],
    computed_style_properties: &[String],
    max_text_bytes: usize,
) -> Result<Vec<browser_v1::BrowserElementCapture>, String> {
    if selectors.is_empty() {
        return Ok(Vec::new());
    }
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    let tab = chromium_tab_for_session(runtime, session_id, tab_id).await?;
    let script =
        chromium_element_capture_script(selectors, computed_style_properties, max_text_bytes)?;
    let value = run_chromium_blocking("chromium observe element captures", move || {
        let value = tab
            .evaluate(script.as_str(), false)
            .map_err(|error| format!("failed to read Chromium element captures: {error}"))?
            .value
            .unwrap_or_else(|| serde_json::Value::String("[]".to_owned()));
        Ok(decode_chromium_json_array_string_value(value, MAX_CHROMIUM_ELEMENT_CAPTURE_JSON_BYTES))
    })
    .await?;
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    parse_chromium_element_captures(value, max_text_bytes)
}

fn chromium_element_capture_script(
    selectors: &[String],
    computed_style_properties: &[String],
    max_text_bytes: usize,
) -> Result<String, String> {
    let selectors_json = serde_json::to_string(selectors)
        .map_err(|error| format!("failed to encode observe capture selectors: {error}"))?;
    let styles_json = serde_json::to_string(computed_style_properties)
        .map_err(|error| format!("failed to encode observe computed styles: {error}"))?;
    let max_text_chars = max_text_bytes.clamp(1, 8 * 1024);
    Ok(format!(
        r#"
(() => {{
  const selectors = {selectors_json};
  const styleNames = {styles_json};
  const maxTextChars = {max_text_chars};
  const clamp = (value, maxChars) => {{
    const text = String(value || "");
    return text.length > maxChars ? text.slice(0, maxChars) : text;
  }};
  const rectPayload = (rect) => ({{
    x: Number(rect.x) || 0,
    y: Number(rect.y) || 0,
    width: Number(rect.width) || 0,
    height: Number(rect.height) || 0,
    top: Number(rect.top) || 0,
    right: Number(rect.right) || 0,
    bottom: Number(rect.bottom) || 0,
    left: Number(rect.left) || 0
  }});
  const className = (element) => {{
    const raw = element && element.className;
    if (typeof raw === "string") return raw;
    if (raw && typeof raw.baseVal === "string") return raw.baseVal;
    return "";
  }};
  const sensitiveTags = new Set(["script", "style", "template", "noscript", "head", "meta", "link", "title"]);
  const isHiddenByAttributes = (element) => (
    Boolean(element && element.hidden) ||
    String((element && element.getAttribute("aria-hidden")) || "").toLowerCase() === "true"
  );
  const visibleFrom = (element, rect, computed) => {{
    const tagName = String((element && element.tagName) || "").toLowerCase();
    if (
      sensitiveTags.has(tagName) ||
      (tagName === "input" && String(element.getAttribute("type") || "").toLowerCase() === "hidden") ||
      isHiddenByAttributes(element) ||
      rect.width <= 0 ||
      rect.height <= 0
    ) {{
      return false;
    }}
    let cursor = element;
    while (cursor && cursor.nodeType === 1) {{
      if (isHiddenByAttributes(cursor)) return false;
      const current = cursor === element ? computed : window.getComputedStyle(cursor);
      if (
        current.display === "none" ||
        current.visibility === "hidden" ||
        Number(current.opacity || "1") <= 0
      ) {{
        return false;
      }}
      cursor = cursor.parentElement;
    }}
    return true;
  }};
  const elementText = (element, visible) => {{
    if (!element || !visible) {{
      return {{ text: "", truncated: false }};
    }}
    const raw = element.innerText || "";
    const normalized = String(raw).replace(/\s+/g, " ").trim();
    return {{
      text: clamp(normalized, maxTextChars),
      truncated: normalized.length > maxTextChars
    }};
  }};
  const capture = (selector) => {{
    const rawSelector = String(selector || "").trim();
    if (!rawSelector) {{
      return {{ selector: rawSelector, found: false, error: "selector_empty" }};
    }}
    let element = null;
    try {{
      element = document.querySelector(rawSelector);
    }} catch (error) {{
      return {{ selector: rawSelector, found: false, error: "selector_invalid" }};
    }}
    if (!element) {{
      return {{ selector: rawSelector, found: false, error: "selector_not_found" }};
    }}
    const rect = element.getBoundingClientRect();
    const computed = window.getComputedStyle(element);
    const visible = visibleFrom(element, rect, computed);
    const text = elementText(element, visible);
    const styles = styleNames.map((name) => {{
      const key = String(name || "").trim();
      return {{ name: key, value: clamp(computed.getPropertyValue(key) || computed[key] || "", 512) }};
    }});
    return {{
      selector: rawSelector,
      found: true,
      rect: rectPayload(rect),
      visible,
      tag_name: clamp((element.tagName || "").toLowerCase(), 64),
      id: clamp(element.id || "", 128),
      class_name: clamp(className(element), 256),
      text: text.text,
      text_truncated: text.truncated,
      computed_styles: styles,
      error: ""
    }};
  }};
  return JSON.stringify(selectors.map(capture));
}})()
"#
    ))
}

fn parse_chromium_element_captures(
    value: serde_json::Value,
    max_text_bytes: usize,
) -> Result<Vec<browser_v1::BrowserElementCapture>, String> {
    let payloads = serde_json::from_value::<Vec<ChromiumElementCapturePayload>>(value)
        .map_err(|error| format!("failed to parse Chromium element captures: {error}"))?;
    Ok(payloads
        .into_iter()
        .map(|payload| chromium_element_capture_to_proto(payload, max_text_bytes))
        .collect())
}

fn chromium_element_capture_to_proto(
    payload: ChromiumElementCapturePayload,
    max_text_bytes: usize,
) -> browser_v1::BrowserElementCapture {
    let (text, truncated_by_bytes) =
        truncate_utf8_bytes_with_flag(payload.text.as_str(), max_text_bytes.max(1));
    browser_v1::BrowserElementCapture {
        v: CANONICAL_PROTOCOL_MAJOR,
        selector: truncate_utf8_bytes(payload.selector.as_str(), 512),
        found: payload.found,
        bounding_rect: payload.found.then(|| browser_v1::BrowserRect {
            v: CANONICAL_PROTOCOL_MAJOR,
            x: finite_browser_metric(payload.rect.x),
            y: finite_browser_metric(payload.rect.y),
            width: finite_browser_metric(payload.rect.width),
            height: finite_browser_metric(payload.rect.height),
            top: finite_browser_metric(payload.rect.top),
            right: finite_browser_metric(payload.rect.right),
            bottom: finite_browser_metric(payload.rect.bottom),
            left: finite_browser_metric(payload.rect.left),
        }),
        visible: payload.visible,
        tag_name: truncate_utf8_bytes(payload.tag_name.as_str(), 64),
        id: truncate_utf8_bytes(payload.id.as_str(), 128),
        class_name: truncate_utf8_bytes(payload.class_name.as_str(), 256),
        text,
        text_truncated: payload.text_truncated || truncated_by_bytes,
        computed_styles: payload
            .computed_styles
            .into_iter()
            .take(16)
            .map(|style| browser_v1::BrowserComputedStyle {
                v: CANONICAL_PROTOCOL_MAJOR,
                name: truncate_utf8_bytes(style.name.as_str(), 64),
                value: truncate_utf8_bytes(style.value.as_str(), 512),
            })
            .collect(),
        error: truncate_utf8_bytes(payload.error.as_str(), 256),
    }
}

fn finite_browser_metric(value: f64) -> f64 {
    if value.is_finite() {
        value
    } else {
        0.0
    }
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
        Ok(decode_chromium_bounded_json_script_value(value, MAX_CHROMIUM_LOCAL_STORAGE_JSON_BYTES))
    })
    .await?;
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    parse_chromium_local_storage_snapshot(value)
}

/// Clears localStorage and sessionStorage for the active tab's origin and
/// returns the number of entries removed.
///
/// # Errors
/// Returns lookup sentinels, remote-IP guard incidents, or a page-side storage
/// failure message.
pub(crate) async fn chromium_clear_active_origin_storage(
    runtime: &BrowserRuntimeState,
    session_id: &str,
) -> Result<u32, String> {
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    let (_tab_id, tab) = chromium_active_tab_for_session(runtime, session_id).await?;
    let value = run_chromium_blocking("chromium clear active origin storage", move || {
        let value = tab
            .evaluate(CHROMIUM_CLEAR_ACTIVE_ORIGIN_STORAGE_SCRIPT, false)
            .map_err(|error| format!("failed to clear Chromium origin storage: {error}"))?
            .value
            .unwrap_or_else(|| serde_json::Value::String("{}".to_owned()));
        Ok(decode_chromium_json_script_value(value))
    })
    .await?;
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    parse_chromium_clear_storage_status(value)
}

/// Clears cookies visible to Chromium for the active tab's current URL and
/// returns the number of delete requests sent to CDP.
///
/// # Errors
/// Returns lookup sentinels, remote-IP guard incidents, or a CDP cookie read or
/// delete failure.
pub(crate) async fn chromium_clear_active_tab_cookies(
    runtime: &BrowserRuntimeState,
    session_id: &str,
) -> Result<u32, String> {
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    let (_tab_id, tab) = chromium_active_tab_for_session(runtime, session_id).await?;
    let deleted = run_chromium_blocking("chromium clear active tab cookies", move || {
        let cookies = tab
            .get_cookies()
            .map_err(|error| format!("failed to read Chromium cookies: {error}"))?;
        let delete_requests = chromium_cookie_delete_requests(cookies.as_slice());
        let deleted = u32::try_from(delete_requests.len()).unwrap_or(u32::MAX);
        if !delete_requests.is_empty() {
            tab.delete_cookies(delete_requests)
                .map_err(|error| format!("failed to delete Chromium cookies: {error}"))?;
        }
        Ok(deleted)
    })
    .await?;
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    Ok(deleted)
}

fn chromium_cookie_delete_requests(cookies: &[Network::Cookie]) -> Vec<Network::DeleteCookies> {
    cookies
        .iter()
        .filter(|cookie| !cookie.name.trim().is_empty())
        .map(|cookie| Network::DeleteCookies {
            name: cookie.name.clone(),
            url: None,
            domain: (!cookie.domain.trim().is_empty()).then(|| cookie.domain.clone()),
            path: (!cookie.path.trim().is_empty()).then(|| cookie.path.clone()),
            partition_key: cookie.partition_key.clone(),
        })
        .collect()
}

#[derive(Debug, Clone, Copy)]
enum ChromiumPermissionOverrideReset {
    ResetExisting,
    KeepExisting,
}

/// Resets stale permission overrides and applies the policy to every open Chromium HTTP(S) origin.
///
/// # Errors
/// Returns lookup sentinels, remote-IP guard incidents, or CDP permission
/// reset/override failures.
pub(crate) async fn chromium_apply_session_permissions(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    permissions: SessionPermissionsInternal,
) -> Result<(), String> {
    chromium_apply_open_origin_permissions(
        runtime,
        session_id,
        permissions,
        ChromiumPermissionOverrideReset::ResetExisting,
    )
    .await
}

async fn chromium_apply_current_session_permissions(
    runtime: &BrowserRuntimeState,
    session_id: &str,
) -> Result<(), String> {
    let permissions = {
        let sessions = runtime.sessions.lock().await;
        let Some(session) = sessions.get(session_id) else {
            return Err("session_not_found".to_owned());
        };
        session.permissions.clone()
    };
    chromium_apply_open_origin_permissions(
        runtime,
        session_id,
        permissions,
        ChromiumPermissionOverrideReset::KeepExisting,
    )
    .await
}

async fn chromium_apply_open_origin_permissions(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    permissions: SessionPermissionsInternal,
    reset_existing_overrides: ChromiumPermissionOverrideReset,
) -> Result<(), String> {
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    let (command_tab, tabs) = {
        let chromium_sessions = runtime.chromium_sessions.lock().await;
        let Some(chromium_session) = chromium_sessions.get(session_id) else {
            return Err("chromium_session_not_found".to_owned());
        };
        let tabs = chromium_session.tabs.values().cloned().collect::<Vec<_>>();
        let Some(command_tab) = tabs.first().cloned() else {
            return Err("chromium_session_has_no_tabs".to_owned());
        };
        (command_tab, tabs)
    };
    run_chromium_blocking("chromium apply session permissions", move || {
        if matches!(reset_existing_overrides, ChromiumPermissionOverrideReset::ResetExisting) {
            // Browser.setPermission is origin-scoped; reset first so closed or
            // inactive origins cannot retain stale grants after a session change.
            command_tab
                .call_method(chromium_permission_reset_request())
                .map_err(|error| format!("failed to reset Chromium page permissions: {error}"))?;
        }
        let tab_urls = tabs.iter().map(|tab| tab.get_url()).collect::<Vec<_>>();
        for origin in chromium_permission_origins_for_urls(tab_urls.as_slice()) {
            for request in chromium_permission_set_requests(origin.as_str(), &permissions) {
                command_tab.call_method(request).map_err(|error| {
                    format!("failed to set Chromium page permission for {origin}: {error}")
                })?;
            }
        }
        Ok(())
    })
    .await?;
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    Ok(())
}

fn chromium_permission_origin(raw_url: &str) -> Result<String, String> {
    let url = Url::parse(raw_url).map_err(|error| {
        format!("failed to parse active tab URL for permission override: {error}")
    })?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(format!(
            "active tab URL scheme '{}' does not support browser permission overrides",
            url.scheme()
        ));
    }
    let host = url
        .host_str()
        .ok_or_else(|| "active tab URL has no host for permission override".to_owned())?;
    let mut origin = format!("{}://{}", url.scheme(), host);
    if let Some(port) = url.port() {
        origin.push(':');
        origin.push_str(port.to_string().as_str());
    }
    Ok(origin)
}

fn chromium_permission_origins_for_urls(raw_urls: &[String]) -> Vec<String> {
    raw_urls
        .iter()
        .filter_map(|raw_url| chromium_permission_origin(raw_url.as_str()).ok())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn chromium_permission_reset_request() -> Browser::ResetPermissions {
    Browser::ResetPermissions { browser_context_id: None }
}

#[cfg(test)]
fn chromium_sync_tab_url_is_trackable(raw_url: &str) -> bool {
    Url::parse(raw_url).map(|url| chromium_sync_tab_url_scheme_is_trackable(&url)).unwrap_or(false)
}

fn chromium_sync_tab_url_is_allowed(
    raw_url: &str,
    private_target_policy: &ChromiumPrivateTargetPolicy,
) -> Result<bool, String> {
    let Ok(url) = Url::parse(raw_url) else {
        return Ok(false);
    };
    if !chromium_sync_tab_url_scheme_is_trackable(&url) {
        return Ok(false);
    }
    validate_target_url_blocking(raw_url, private_target_policy.allows_url(raw_url))?;
    Ok(true)
}

fn chromium_sync_tab_url_scheme_is_trackable(url: &Url) -> bool {
    matches!(url.scheme(), "http" | "https" | "file")
}

fn chromium_permission_set_requests(
    origin: &str,
    permissions: &SessionPermissionsInternal,
) -> Vec<Browser::SetPermission> {
    [
        ("camera", permissions.camera),
        ("microphone", permissions.microphone),
        ("geolocation", permissions.location),
    ]
    .into_iter()
    .map(|(name, setting)| Browser::SetPermission {
        permission: Browser::PermissionDescriptor {
            name: name.to_owned(),
            sysex: None,
            user_visible_only: None,
            allow_without_sanitization: None,
            allow_without_gesture: None,
            pan_tilt_zoom: None,
        },
        setting: chromium_permission_setting(setting),
        origin: Some(origin.to_owned()),
        embedding_origin: None,
        browser_context_id: None,
    })
    .collect()
}

fn chromium_permission_setting(setting: PermissionSettingInternal) -> Browser::PermissionSetting {
    match setting {
        PermissionSettingInternal::Deny => Browser::PermissionSetting::Denied,
        PermissionSettingInternal::Allow => Browser::PermissionSetting::Granted,
    }
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

/// Drains page-captured client-side downloads for a tab.
///
/// # Errors
/// Returns lookup sentinels, remote-IP guard incidents, or script evaluation
/// failures.
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
    Ok(parse_chromium_client_download_entries(value))
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

fn decode_chromium_bounded_json_script_value(
    value: serde_json::Value,
    max_json_bytes: usize,
) -> serde_json::Value {
    match value {
        serde_json::Value::String(raw) if raw.len() <= max_json_bytes => {
            serde_json::from_str::<serde_json::Value>(raw.as_str())
                .unwrap_or(serde_json::Value::Null)
        }
        _ => serde_json::Value::Null,
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

const CHROMIUM_CLEAR_ACTIVE_ORIGIN_STORAGE_SCRIPT: &str = r#"
(() => {
  try {
    const origin = String((window.location && window.location.origin) || "");
    if (!origin || origin === "null") {
      return JSON.stringify({ ok: true, origin: "", entries_cleared: 0 });
    }
    const local = window.localStorage;
    const session = window.sessionStorage;
    const localEntries = local ? Number(local.length || 0) : 0;
    const sessionEntries = session ? Number(session.length || 0) : 0;
    if (local) {
      local.clear();
    }
    if (session) {
      session.clear();
    }
    return JSON.stringify({
      ok: true,
      origin,
      entries_cleared: Math.max(0, localEntries) + Math.max(0, sessionEntries)
    });
  } catch (error) {
    return JSON.stringify({
      ok: false,
      origin: "",
      entries_cleared: 0,
      error: String((error && (error.message || error)) || "")
    });
  }
})()
"#;

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

fn parse_chromium_clear_storage_status(value: serde_json::Value) -> Result<u32, String> {
    let object =
        value.as_object().ok_or_else(|| "storage clear returned non-object payload".to_owned())?;
    if !object.get("ok").and_then(serde_json::Value::as_bool).unwrap_or(false) {
        let error = object
            .get("error")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown storage clear failure");
        return Err(format!("storage clear failed: {error}"));
    }
    Ok(object
        .get("entries_cleared")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .unwrap_or(0))
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
    let visual = chromium_viewport_metric_pair(&value, "visual_width", "visual_height");
    let layout = chromium_viewport_metric_pair(&value, "width", "height");
    let (actual_width, actual_height) =
        select_chromium_viewport_metric_pair(requested_width, requested_height, visual, layout)
            .unwrap_or((requested_width, requested_height));
    let actual_device_scale_factor = value
        .get("device_scale_factor")
        .and_then(serde_json::Value::as_f64)
        .filter(|value| value.is_finite() && *value > 0.0)
        .unwrap_or(requested_device_scale_factor);
    (actual_width, actual_height, actual_device_scale_factor)
}

fn chromium_viewport_metric_pair(
    value: &serde_json::Value,
    width_field: &str,
    height_field: &str,
) -> Option<(u32, u32)> {
    Some((
        chromium_u32_metric_option(value, width_field)?,
        chromium_u32_metric_option(value, height_field)?,
    ))
}

fn select_chromium_viewport_metric_pair(
    requested_width: u32,
    requested_height: u32,
    visual: Option<(u32, u32)>,
    layout: Option<(u32, u32)>,
) -> Option<(u32, u32)> {
    [visual, layout]
        .iter()
        .flatten()
        .copied()
        .find(|candidate| {
            chromium_viewport_dimensions_match(requested_width, requested_height, *candidate)
        })
        .or(visual)
        .or(layout)
}

fn chromium_u32_metric_option(value: &serde_json::Value, field: &str) -> Option<u32> {
    value
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .filter(|value| *value > 0)
}

fn chromium_positive_f64_metric(value: f64) -> u32 {
    if !value.is_finite() || value <= 0.0 {
        return 0;
    }
    value.trunc().min(f64::from(u32::MAX)) as u32
}

fn chromium_layout_metrics_from_cdp(
    layout_viewport_width: u32,
    layout_viewport_height: u32,
    visual_viewport_width: f64,
    visual_viewport_height: f64,
    content_width: f64,
    content_height: f64,
    device_scale_factor: f64,
) -> ChromiumLayoutMetrics {
    let viewport_width = if layout_viewport_width > 0 {
        layout_viewport_width
    } else {
        chromium_positive_f64_metric(visual_viewport_width)
    };
    let viewport_height = if layout_viewport_height > 0 {
        layout_viewport_height
    } else {
        chromium_positive_f64_metric(visual_viewport_height)
    };
    let document_scroll_width = chromium_positive_f64_metric(content_width).max(viewport_width);
    let document_scroll_height = chromium_positive_f64_metric(content_height).max(viewport_height);
    ChromiumLayoutMetrics {
        viewport_width,
        viewport_height,
        device_scale_factor: if device_scale_factor.is_finite() && device_scale_factor > 0.0 {
            device_scale_factor
        } else {
            1.0
        },
        document_scroll_width,
        document_scroll_height,
        document_client_width: viewport_width,
        document_client_height: viewport_height,
        horizontal_overflow: document_scroll_width > viewport_width.saturating_add(1),
        vertical_overflow: document_scroll_height > viewport_height.saturating_add(1),
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

/// Refreshes the persisted tab snapshot from the live page: body, title, URL,
/// console log, localStorage, document cookies, and network logs from both the
/// CDP buffers and the page hooks.
///
/// Diagnostics reads degrade to empty data on failure; only the core snapshot
/// and session/tab lookups are fatal.
///
/// # Errors
/// Returns lookup sentinels, remote-IP guard incidents, or the observe
/// snapshot failure.
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

/// Refreshes only the active URL while preserving a pending native dialog.
///
/// # Errors
/// Returns lookup sentinels or remote-IP guard incidents.
pub(crate) async fn chromium_refresh_tab_url(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
) -> Result<(), String> {
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    let tab = chromium_tab_for_session(runtime, session_id, tab_id).await?;
    let page_url =
        run_chromium_blocking("chromium refresh tab URL", move || Ok(tab.get_url())).await?;
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;

    let mut sessions = runtime.sessions.lock().await;
    let Some(session) = sessions.get_mut(session_id) else {
        return Err("session_not_found".to_owned());
    };
    let Some(tab) = session.tabs.get_mut(tab_id) else {
        return Err("tab_not_found".to_owned());
    };
    tab.last_url = Some(page_url);
    Ok(())
}

/// Reads the live page title of a tab.
///
/// # Errors
/// Returns lookup sentinels, remote-IP guard incidents, or a CDP read failure.
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

/// Captures a PNG screenshot of the active tab's full surface.
///
/// # Errors
/// Returns lookup sentinels, remote-IP guard incidents, or a capture failure.
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

/// Reads layout/visual viewport metrics and overflow flags from the active tab.
///
/// # Errors
/// Returns lookup sentinels, remote-IP guard incidents, or a CDP transport
/// failure.
pub(crate) async fn chromium_layout_metrics(
    runtime: &BrowserRuntimeState,
    session_id: &str,
) -> Result<ChromiumLayoutMetrics, String> {
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    let (_tab_id, tab) = chromium_active_tab_for_session(runtime, session_id).await?;
    let device_scale_factor = runtime
        .chromium_sessions
        .lock()
        .await
        .get(session_id)
        .map(|session| session.device_scale_factor)
        .ok_or_else(|| "chromium_session_not_found".to_owned())?;
    let value = run_chromium_blocking("chromium layout metrics", move || {
        let metrics = tab
            .call_method(Page::GetLayoutMetrics(None))
            .map_err(|error| format!("failed to read Chromium layout metrics: {error}"))?;
        Ok(chromium_layout_metrics_from_cdp(
            metrics.css_layout_viewport.client_width,
            metrics.css_layout_viewport.client_height,
            metrics.css_visual_viewport.client_width,
            metrics.css_visual_viewport.client_height,
            metrics.css_content_size.width,
            metrics.css_content_size.height,
            device_scale_factor,
        ))
    })
    .await?;
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    Ok(value)
}

/// Navigates the session's active tab; see [`navigate_tab_with_chromium`].
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

/// Drives a guarded navigation on a specific tab; failures are reported via
/// the outcome's `success`/`error` fields rather than `Err`.
///
/// `body_bytes` reports the pre-truncation body size, so it can exceed the
/// length of the (possibly truncated) `page_body`.
pub(crate) async fn navigate_tab_with_chromium(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
    params: &ChromiumNavigateParams,
) -> NavigateOutcome {
    // The guarded HTTP fetch validates the target first (redirect policy,
    // private-target checks, response budget); Chromium then navigates to the
    // vetted final URL only when that pre-flight succeeded.
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
    if let Err(error) =
        chromium_cleanup_dialog_before_navigation(runtime, session_id, tab_id, &tab).await
    {
        outcome.success = false;
        outcome.error = format!("chromium dialog navigation cleanup failed: {error}");
        return outcome;
    }
    let tab_target_id = tab.get_target_id().to_string();
    let _scoped_private_target = if params.allow_private_targets {
        match private_target_policy
            .scoped_url_allowance(tab_target_id.as_str(), outcome.final_url.as_str())
        {
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
        let mut warnings = Vec::new();
        if let Err(error) = tab.wait_until_navigated() {
            let page_url = tab.get_url();
            if !chromium_timeout_snapshot_url_is_usable(page_url.as_str(), target_url.as_str()) {
                return Err(format!("Chromium navigation timeout or failure: {error}"));
            }
            warnings.push(format!(
                "Chromium navigation wait timed out after page URL reached {page_url}: {error}"
            ));
        }
        // Best-effort probe that the pre-registered diagnostics hook survived
        // the navigation; failures are non-fatal because the hooks are
        // reinstalled via chromium_install_page_diagnostics afterwards.
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
                // Restored storage is only visible to scripts that run after
                // the write, so restore then reload (see
                // restore_chromium_tab_live_state for the same dance).
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
                if let Err(error) = tab.wait_until_navigated() {
                    let reloaded_url = tab.get_url();
                    if !chromium_timeout_snapshot_url_is_usable(
                        reloaded_url.as_str(),
                        page_url.as_str(),
                    ) {
                        return Err(format!(
                            "Chromium reload after localStorage restore timed out: {error}"
                        ));
                    }
                    warnings.push(format!(
                        "Chromium reload wait timed out after page URL reached {reloaded_url}: {error}"
                    ));
                }
                page_url = tab.get_url();
            }
        }
        let page_body = tab.get_content().map_err(|error| {
            format!("failed to read Chromium page HTML after navigation: {error}")
        })?;
        let title = tab.get_title().unwrap_or_default();
        Ok((ChromiumObserveSnapshot { page_body, title, page_url }, warnings))
    })
    .await;
    let (snapshot, navigation_warnings) = match chromium_snapshot {
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
        outcome.success = false;
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
    if outcome.error.is_empty() && !navigation_warnings.is_empty() {
        outcome.error = navigation_warnings.join("; ");
    }
    if let Err(error) = chromium_apply_current_session_permissions(runtime, session_id).await {
        outcome.success = false;
        outcome.error =
            format!("failed to apply Chromium page permissions after navigation: {error}");
        return outcome;
    }
    let _ = chromium_install_page_diagnostics(runtime, session_id, tab_id).await;
    let _ = chromium_refresh_tab_snapshot(runtime, session_id, tab_id).await;
    outcome
}

fn chromium_timeout_snapshot_url_is_usable(page_url: &str, target_url: &str) -> bool {
    if page_url == target_url {
        return true;
    }
    page_url.strip_prefix(target_url).is_some_and(|suffix| suffix.starts_with('#'))
}

/// Clicks the first element matching `selector` on the active tab.
///
/// Retries only `not_found` results until `timeout_ms`/`max_attempts` runs
/// out; download-like anchors are blocked unless `allow_downloads` is set.
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
        Disabled,
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
            let Some(tag) =
                find_matching_html_tag(selector_for_attempt.as_str(), page_body.as_str())
            else {
                return Ok(ClickAttempt::NotFound);
            };
            let download_like = is_download_like_tag(tag.as_str());
            if download_like && !allow_downloads {
                return Ok(ClickAttempt::DownloadBlocked);
            }
            let tag_lower = tag.to_ascii_lowercase();
            if tag_lower.contains(" disabled")
                || tag_lower.contains(" aria-disabled=\"true\"")
                || tag_lower.contains(" aria-disabled='true'")
            {
                return Ok(ClickAttempt::Disabled);
            }
            if allow_downloads {
                tab_for_attempt
                    .evaluate(CHROMIUM_BEGIN_CLIENT_DOWNLOAD_CAPTURE_SCRIPT, false)
                    .map_err(|error| {
                        format!("failed to initialize Chromium download capture: {error}")
                    })?;
            }
            let element =
                tab_for_attempt.find_element(selector_for_attempt.as_str()).map_err(|error| {
                    format!(
                        "failed to resolve selector '{}' on Chromium page: {error}",
                        selector_for_attempt
                    )
                })?;
            element.click().map_err(|error| {
                format!(
                    "failed to click selector '{}' through Chromium input dispatch: {error}",
                    selector_for_attempt
                )
            })?;
            Ok(ClickAttempt::Clicked { download_like })
        })
        .await;

        match attempt {
            Ok(ClickAttempt::Clicked { download_like }) => {
                let new_tab_count = match chromium_sync_session_tabs_after_click(
                    runtime, session_id, false, started, timeout_ms,
                )
                .await
                {
                    Ok(value) => value,
                    Err(error) => {
                        return ChromiumActionOutcome {
                            success: false,
                            outcome: "new_tab_sync_failed".to_owned(),
                            error,
                            attempts,
                        };
                    }
                };
                let settle_ms = DEFAULT_ACTION_RETRY_INTERVAL_MS.min(timeout_ms.max(1));
                tokio::time::sleep(Duration::from_millis(settle_ms)).await;
                let _ = chromium_refresh_tab_snapshot(runtime, session_id, tab_id.as_str()).await;
                return ChromiumActionOutcome {
                    success: true,
                    outcome: if download_like {
                        "download_allowed".to_owned()
                    } else if new_tab_count > 0 {
                        "clicked_new_tab".to_owned()
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
            Ok(ClickAttempt::Disabled) => {
                return ChromiumActionOutcome {
                    success: false,
                    outcome: "selector_disabled".to_owned(),
                    error: format!("selector '{selector}' is disabled"),
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

async fn chromium_sync_session_tabs_after_click(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    opened_window: bool,
    started: Instant,
    timeout_ms: u64,
) -> Result<u32, String> {
    let first_sync_count = chromium_sync_session_tabs(runtime, session_id).await?;
    if first_sync_count > 0 || !opened_window {
        return Ok(first_sync_count);
    }

    let click_timeout = Duration::from_millis(timeout_ms);
    let popup_sync_max_wait_ms = u64::try_from(CHROMIUM_NEW_TAB_MAX_ATTEMPTS)
        .unwrap_or(u64::MAX)
        .saturating_mul(CHROMIUM_NEW_TAB_RETRY_DELAY_MS);
    let popup_sync_timeout = Duration::from_millis(timeout_ms.min(popup_sync_max_wait_ms));
    let popup_sync_started = Instant::now();
    loop {
        // `window.open` can return before Chromium exposes the new CDP target,
        // especially on slower Windows runners.
        if started.elapsed() >= click_timeout || popup_sync_started.elapsed() >= popup_sync_timeout
        {
            return Ok(0);
        }
        let click_remaining_ms = timeout_ms.saturating_sub(started.elapsed().as_millis() as u64);
        let popup_remaining_ms = popup_sync_timeout
            .saturating_sub(popup_sync_started.elapsed())
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        let remaining_ms = click_remaining_ms.min(popup_remaining_ms);
        let sleep_ms = CHROMIUM_NEW_TAB_RETRY_DELAY_MS.min(remaining_ms.max(1));
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;

        let new_tab_count = chromium_sync_session_tabs(runtime, session_id).await?;
        if new_tab_count > 0 {
            return Ok(new_tab_count);
        }
    }
}

/// Types text into an input-like or content-editable element on the active
/// tab, retrying `not_found` results until `timeout_ms` runs out.
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

/// Stages `file_bytes` inside the session profile dir and attaches the staged
/// file to a file input on the active tab, retrying `not_found` results until
/// `timeout_ms` runs out.
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
    let mut staged_upload =
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
    let upload_path_text = staged_upload.path().to_string_lossy().to_string();
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
                staged_upload.retain();
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

struct ChromiumStagedUpload {
    path: PathBuf,
    reserved_bytes: u64,
    session_bytes: Arc<AtomicU64>,
    retained: bool,
}

impl ChromiumStagedUpload {
    fn path(&self) -> &Path {
        self.path.as_path()
    }

    fn retain(&mut self) {
        self.retained = true;
    }
}

impl Drop for ChromiumStagedUpload {
    fn drop(&mut self) {
        if self.retained {
            return;
        }
        let cleanup_path = self.path.parent().unwrap_or(self.path.as_path());
        match fs::remove_dir_all(cleanup_path) {
            Ok(()) => {
                self.session_bytes.fetch_sub(self.reserved_bytes, Ordering::AcqRel);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                self.session_bytes.fetch_sub(self.reserved_bytes, Ordering::AcqRel);
            }
            Err(error) => {
                warn!(
                    path = %cleanup_path.display(),
                    error = %error,
                    "failed to remove unsuccessful Chromium upload staging directory"
                );
            }
        }
    }
}

async fn write_chromium_upload_file(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    file_name: &str,
    file_bytes: &[u8],
) -> Result<ChromiumStagedUpload, String> {
    let (upload_dir, session_bytes) = {
        let chromium_sessions = runtime.chromium_sessions.lock().await;
        let Some(chromium_session) = chromium_sessions.get(session_id) else {
            return Err("chromium_session_not_found".to_owned());
        };
        (
            chromium_session._profile_dir.path().join(UPLOADS_DIR),
            Arc::clone(&chromium_session.staged_upload_bytes),
        )
    };
    let reserved_bytes = u64::try_from(file_bytes.len()).unwrap_or(u64::MAX);
    let path = chromium_upload_staging_path(upload_dir.as_path(), file_name)?;
    reserve_chromium_upload_bytes(session_bytes.as_ref(), reserved_bytes)?;
    let staged_upload =
        ChromiumStagedUpload { path, reserved_bytes, session_bytes, retained: false };
    fs::create_dir_all(upload_dir.as_path()).map_err(|error| {
        format!(
            "failed to initialize Chromium upload directory '{}': {error}",
            upload_dir.display()
        )
    })?;
    if let Some(parent) = staged_upload.path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            format!(
                "failed to initialize Chromium upload staging directory '{}': {error}",
                parent.display()
            )
        })?;
    }
    fs::write(staged_upload.path.as_path(), file_bytes).map_err(|error| {
        format!("failed to stage uploaded browser file '{}': {error}", staged_upload.path.display())
    })?;
    Ok(staged_upload)
}

fn reserve_chromium_upload_bytes(
    session_bytes: &AtomicU64,
    requested_bytes: u64,
) -> Result<(), String> {
    if requested_bytes > UPLOAD_MAX_FILE_BYTES {
        return Err(format!(
            "upload file exceeds max_file_bytes ({requested_bytes} > {UPLOAD_MAX_FILE_BYTES})"
        ));
    }
    let mut current = session_bytes.load(Ordering::Acquire);
    loop {
        let projected = current.checked_add(requested_bytes).ok_or_else(|| {
            "upload session byte accounting overflowed; refusing staging".to_owned()
        })?;
        if projected > UPLOAD_MAX_TOTAL_BYTES_PER_SESSION {
            return Err(format!(
                "upload session exceeds max_total_bytes ({projected} > {UPLOAD_MAX_TOTAL_BYTES_PER_SESSION})"
            ));
        }
        match session_bytes.compare_exchange_weak(
            current,
            projected,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return Ok(()),
            Err(observed) => current = observed,
        }
    }
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

/// Parses a key spec such as `Ctrl+Shift+K` into the terminal key and modifiers.
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

/// Presses a key (with optional modifiers) on the active tab.
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

fn chromium_select_script(selector_json: &str, value_json: &str) -> String {
    format!(
        r#"
(() => {{
  const selector = {selector_json};
  const value = {value_json};
  const element = document.querySelector(selector);
  if (!element) {{
    return {status_not_found};
  }}
  if ((element.tagName || "").toLowerCase() !== "select") {{
    return {status_not_select};
  }}
  if (element.disabled) {{
    return {status_disabled};
  }}
  const option = Array.from(element.options || []).find((candidate) => candidate.value === value);
  if (!option) {{
    return {status_value_not_found};
  }}
  element.value = value;
  element.dispatchEvent(new Event("input", {{ bubbles: true }}));
  element.dispatchEvent(new Event("change", {{ bubbles: true }}));
  return {status_selected};
}})()
"#,
        status_not_found = CHROMIUM_SELECT_STATUS_NOT_FOUND,
        status_not_select = CHROMIUM_SELECT_STATUS_NOT_SELECT,
        status_disabled = CHROMIUM_SELECT_STATUS_DISABLED,
        status_value_not_found = CHROMIUM_SELECT_STATUS_VALUE_NOT_FOUND,
        status_selected = CHROMIUM_SELECT_STATUS_SELECTED,
    )
}

/// Selects an option by value on a `<select>` element of the active tab.
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
    let script = chromium_select_script(selector_json.as_str(), value_json.as_str());
    let result = run_chromium_blocking("chromium select", move || {
        tab.set_default_timeout(Duration::from_millis(timeout_ms.max(1)));
        tab.evaluate(script.as_str(), true)
            .map_err(|error| format!("failed to execute Chromium select script: {error}"))?
            .value
            .and_then(|value| value.as_u64())
            .ok_or_else(|| "Chromium select script returned an invalid primitive status".to_owned())
    })
    .await;
    match result {
        Ok(status) => match status {
            CHROMIUM_SELECT_STATUS_SELECTED => {
                let _ = chromium_refresh_tab_snapshot(runtime, session_id, tab_id.as_str()).await;
                ChromiumActionOutcome {
                    success: true,
                    outcome: "selected".to_owned(),
                    error: String::new(),
                    attempts: 1,
                }
            }
            CHROMIUM_SELECT_STATUS_DISABLED => ChromiumActionOutcome {
                success: false,
                outcome: "selector_disabled".to_owned(),
                error: format!("selector '{selector}' is disabled"),
                attempts: 1,
            },
            CHROMIUM_SELECT_STATUS_NOT_SELECT => ChromiumActionOutcome {
                success: false,
                outcome: "selector_not_select".to_owned(),
                error: format!("selector '{selector}' does not target a <select> element"),
                attempts: 1,
            },
            CHROMIUM_SELECT_STATUS_VALUE_NOT_FOUND => ChromiumActionOutcome {
                success: false,
                outcome: "value_not_found".to_owned(),
                error: format!("value '{value}' was not found for selector '{selector}'"),
                attempts: 1,
            },
            CHROMIUM_SELECT_STATUS_NOT_FOUND => ChromiumActionOutcome {
                success: false,
                outcome: "selector_not_found".to_owned(),
                error: format!("selector '{selector}' was not found"),
                attempts: 1,
            },
            _ => ChromiumActionOutcome {
                success: false,
                outcome: "select_failed".to_owned(),
                error: "Chromium select script returned an unknown primitive status".to_owned(),
                attempts: 1,
            },
        },
        Err(error) => ChromiumActionOutcome {
            success: false,
            outcome: "select_failed".to_owned(),
            error,
            attempts: 1,
        },
    }
}

/// Draws a temporary overlay around the first element matching `selector` on
/// the active tab; the overlay removes itself after `duration_ms` (clamped).
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
  const respond = (payload) => JSON.stringify(payload);
  const element = document.querySelector(selector);
  if (!element) {{
    return respond({{ status: "not_found" }});
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
  return respond({{ status: "highlighted" }});
}})()
"#
    );
    let result = run_chromium_blocking("chromium highlight", move || {
        tab.set_default_timeout(Duration::from_millis(timeout_ms.max(1)));
        let value = tab
            .evaluate(script.as_str(), true)
            .map_err(|error| format!("failed to execute Chromium highlight script: {error}"))?
            .value
            .unwrap_or(serde_json::Value::Null);
        Ok(decode_chromium_json_script_value(value))
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

/// Exports the active tab as a PDF using Chromium print defaults.
///
/// # Errors
/// Returns lookup sentinels, remote-IP guard incidents, or a print failure.
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

/// Applies device-metrics and touch emulation to the active tab and verifies
/// the page-visible viewport against the requested dimensions.
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
            metric_mismatch: false,
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
                metric_mismatch: false,
                error,
            }
        }
    };
    let result = run_chromium_blocking("chromium set viewport", move || {
        // Window bounds (and SetVisibleSize below, deprecated in CDP) are
        // best-effort; the authoritative sizing is SetDeviceMetricsOverride.
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
                    metric_mismatch: false,
                    error,
                };
            }
            if let Some(session) = runtime.chromium_sessions.lock().await.get_mut(session_id) {
                session.device_scale_factor = device_scale_factor;
            }
            ChromiumViewportOutcome {
                success: true,
                width: actual_width,
                height: actual_height,
                device_scale_factor: actual_device_scale_factor,
                mobile,
                metric_mismatch: chromium_viewport_metrics_mismatch(
                    width,
                    height,
                    actual_width,
                    actual_height,
                ),
                error: String::new(),
            }
        }
        Err(error) => ChromiumViewportOutcome {
            success: false,
            width: 0,
            height: 0,
            device_scale_factor: 0.0,
            mobile,
            metric_mismatch: false,
            error,
        },
    }
}

fn chromium_viewport_metrics_mismatch(
    requested_width: u32,
    requested_height: u32,
    actual_width: u32,
    actual_height: u32,
) -> bool {
    !chromium_viewport_dimensions_match(
        requested_width,
        requested_height,
        (actual_width, actual_height),
    )
}

fn chromium_viewport_dimensions_match(
    requested_width: u32,
    requested_height: u32,
    actual: (u32, u32),
) -> bool {
    let (actual_width, actual_height) = actual;
    if actual_width != requested_width {
        return false;
    }
    if actual_height == requested_height {
        return true;
    }
    actual_height < requested_height
        && requested_height.saturating_sub(actual_height) <= CHROMIUM_VIEWPORT_HEIGHT_TOLERANCE_PX
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::{
        chromium_cookie_delete_requests, chromium_element_capture_script,
        chromium_layout_metrics_from_cdp, chromium_network_log_headers,
        chromium_observe_state_script, chromium_permission_origin,
        chromium_permission_origins_for_urls, chromium_permission_reset_request,
        chromium_permission_set_requests, chromium_read_document_cookies_script,
        chromium_read_local_storage_script, chromium_restore_local_storage_script,
        chromium_select_script, chromium_sync_tab_url_is_allowed,
        chromium_sync_tab_url_is_trackable, chromium_timeout_snapshot_url_is_usable,
        chromium_touch_emulation_max_touch_points, chromium_transport_idle_timeout,
        chromium_upload_staging_path, chromium_viewport_metrics_mismatch, clamp_chromium_snapshot,
        decode_chromium_bounded_json_script_value, decode_chromium_console_entries_value,
        decode_chromium_json_script_value, decode_chromium_network_entries_value,
        decode_chromium_observe_state_value, page_body_with_chromium_observe_state,
        parse_chromium_clear_storage_status, parse_chromium_client_download_entries,
        parse_chromium_console_entries, parse_chromium_document_cookie_snapshot,
        parse_chromium_element_captures, parse_chromium_local_storage_restore_status,
        parse_chromium_local_storage_snapshot, parse_chromium_page_network_entries,
        parse_chromium_viewport_metrics, parse_key_press_spec, reserve_chromium_upload_bytes,
        selector_not_found_error_from_cached_snapshot, ChromiumLayoutMetrics,
        ChromiumObserveSnapshot, ChromiumPrivateTargetPolicy, ChromiumStagedUpload,
        CHROMIUM_CLEAR_ACTIVE_ORIGIN_STORAGE_SCRIPT, CHROMIUM_CLEAR_NETWORK_LOG_SCRIPT,
        CHROMIUM_DRAIN_NETWORK_LOG_SCRIPT, CHROMIUM_PAGE_DIAGNOSTICS_SCRIPT,
        CHROMIUM_READ_CONSOLE_LOG_SCRIPT, CHROMIUM_SELECT_STATUS_DISABLED,
        CHROMIUM_SELECT_STATUS_NOT_FOUND, CHROMIUM_SELECT_STATUS_NOT_SELECT,
        CHROMIUM_SELECT_STATUS_SELECTED, CHROMIUM_SELECT_STATUS_VALUE_NOT_FOUND,
        MAX_CHROMIUM_CONSOLE_JSON_BYTES, MAX_CHROMIUM_DOCUMENT_COOKIE_JSON_BYTES,
        MAX_CHROMIUM_LOCAL_STORAGE_JSON_BYTES, MAX_CHROMIUM_NETWORK_JSON_BYTES,
    };
    use crate::{
        PermissionSettingInternal, SessionPermissionsInternal, DEFAULT_SESSION_IDLE_TTL_MS,
        MAX_CONSOLE_MESSAGE_BYTES, MAX_CONSOLE_SOURCE_BYTES, MAX_CONSOLE_STACK_BYTES,
        MAX_NETWORK_LOG_URL_BYTES, UPLOAD_MAX_FILE_BYTES, UPLOAD_MAX_TOTAL_BYTES_PER_SESSION,
    };
    use base64::Engine as _;
    use std::collections::HashMap;
    use std::fs;
    use std::path::Path;
    use std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    };
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
    fn chromium_observe_state_summary_withholds_form_and_storage_values() {
        let raw = serde_json::json!({
            "html": "<html><body><input id=\"owner\" name=\"owner\" value=\"<redacted>\"></body></html>",
            "origin": "http://127.0.0.1:8786",
            "form_controls": [{
                "tag": "input",
                "type": "email",
                "id": "owner",
                "name": "owner",
                "selector": "#owner",
                "value": "owner@example.test",
                "checked": null,
                "selected_options": ["owner@example.test"]
            }],
            "local_storage": {
                "ok": true,
                "origin": "http://127.0.0.1:8786",
                "entries": {"wizard": "{\"access_token\":\"eyJhbGciOiJIUzI1NiJ9.payload.signature\"}"}
            },
            "session_storage": {
                "ok": true,
                "origin": "http://127.0.0.1:8786",
                "entries": {"step": "secret session note"}
            }
        });

        let payload =
            decode_chromium_observe_state_value(serde_json::Value::String(raw.to_string()))
                .expect("observe state should parse");
        let page_body = page_body_with_chromium_observe_state(payload);

        assert!(page_body.contains("browser_form_control"), "{page_body}");
        assert!(page_body.contains("selected_options_count=1"), "{page_body}");
        assert!(page_body.contains("localStorage") && page_body.contains("sessionStorage"));
        assert!(page_body.contains("key=\"wizard\""), "{page_body}");
        assert!(page_body.contains("value=\"&lt;redacted&gt;\""), "{page_body}");
        assert!(
            !page_body.contains("owner@example.test")
                && !page_body.contains("eyJhbGci")
                && !page_body.contains("secret session note"),
            "observe state summary must not leak form or storage values: {page_body}"
        );
    }

    #[test]
    fn chromium_observe_state_script_does_not_read_storage_values() {
        let script = chromium_observe_state_script();

        assert!(script.contains("storage.key(index)"), "{script}");
        assert!(
            !script.contains("storage.getItem(rawKey)"),
            "observe must enumerate storage keys without reading values: {script}"
        );
    }

    #[test]
    fn chromium_observe_state_summary_exposes_hidden_state_elements() {
        let raw = serde_json::json!({
            "html": "<html><body><section id=\"stepTwo\" hidden>Step two</section></body></html>",
            "origin": "http://127.0.0.1:8786",
            "form_controls": [],
            "state_elements": [{
                "tag": "section",
                "id": "stepTwo",
                "selector": "#stepTwo",
                "hidden": true,
                "visible": false,
                "reason": "hidden_attribute"
            }],
            "local_storage": {"ok": true, "origin": "http://127.0.0.1:8786", "entries": {}},
            "session_storage": {"ok": true, "origin": "http://127.0.0.1:8786", "entries": {}}
        });

        let payload =
            decode_chromium_observe_state_value(serde_json::Value::String(raw.to_string()))
                .expect("observe state should parse");
        let page_body = page_body_with_chromium_observe_state(payload);

        assert!(page_body.contains("browser_state_element"), "{page_body}");
        assert!(page_body.contains("selector=\"#stepTwo\""), "{page_body}");
        assert!(page_body.contains("hidden=true"), "{page_body}");
        assert!(page_body.contains("visible=false"), "{page_body}");
        assert!(page_body.contains("reason=\"hidden_attribute\""), "{page_body}");
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
        assert!(error.contains("verify visibility/actionability"), "{error}");
    }

    #[test]
    fn selector_not_found_error_mentions_hidden_cached_match() {
        let error = selector_not_found_error_from_cached_snapshot(
            "#stepTwo",
            "highlight",
            "tab-active",
            r#"<html><body><section id="stepTwo" hidden>Step two</section></body></html>"#,
            "http://127.0.0.1:8790/",
        );

        assert!(error.contains("cached element appeared hidden or aria-hidden"), "{error}");
        assert!(error.contains("present and actionable"), "{error}");
    }

    #[test]
    fn parse_chromium_element_captures_preserves_geometry_and_styles() {
        let raw = serde_json::json!([{
            "selector": "#hero",
            "found": true,
            "rect": {
                "x": 10.5,
                "y": 20.0,
                "width": 320.0,
                "height": 64.0,
                "top": 20.0,
                "right": 330.5,
                "bottom": 84.0,
                "left": 10.5
            },
            "visible": true,
            "tag_name": "section",
            "id": "hero",
            "class_name": "landing hero",
            "text": "Primary CTA",
            "text_truncated": false,
            "computed_styles": [
                {"name": "display", "value": "flex"},
                {"name": "position", "value": "relative"}
            ],
            "error": ""
        }]);

        let captures =
            parse_chromium_element_captures(raw, 64).expect("element captures should parse");

        assert_eq!(captures.len(), 1);
        let capture = &captures[0];
        assert_eq!(capture.selector, "#hero");
        assert!(capture.found);
        assert!(capture.visible);
        assert_eq!(capture.text, "Primary CTA");
        let rect = capture.bounding_rect.as_ref().expect("rect should be present");
        assert_eq!(rect.width, 320.0);
        assert_eq!(rect.height, 64.0);
        assert_eq!(capture.computed_styles[0].name, "display");
        assert_eq!(capture.computed_styles[0].value, "flex");
    }

    #[test]
    fn chromium_element_capture_script_uses_visible_inner_text_only() {
        let script = chromium_element_capture_script(
            &["script".to_owned(), "[hidden]".to_owned()],
            &["display".to_owned()],
            512,
        )
        .expect("element capture script should encode selector input");

        assert!(script.contains("sensitiveTags"));
        assert!(script.contains("aria-hidden"));
        assert!(script.contains("getAttribute(\"type\") || \"\").toLowerCase() === \"hidden\""));
        assert!(script.contains("const raw = element.innerText || \"\""));
        assert!(!script.contains("textContent"));
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
    fn chromium_upload_reservation_enforces_session_byte_cap() {
        let session_bytes = AtomicU64::new(0);
        let full_file_reservations =
            UPLOAD_MAX_TOTAL_BYTES_PER_SESSION.div_euclid(UPLOAD_MAX_FILE_BYTES);
        for _ in 0..full_file_reservations {
            reserve_chromium_upload_bytes(&session_bytes, UPLOAD_MAX_FILE_BYTES)
                .expect("uploads through the session cap should reserve");
        }
        let remainder = UPLOAD_MAX_TOTAL_BYTES_PER_SESSION.rem_euclid(UPLOAD_MAX_FILE_BYTES);
        if remainder > 0 {
            reserve_chromium_upload_bytes(&session_bytes, remainder)
                .expect("the final partial upload should reach the session cap");
        }

        let error = reserve_chromium_upload_bytes(&session_bytes, 1)
            .expect_err("upload beyond the session cap must fail");

        assert!(error.contains("max_total_bytes"), "{error}");
        assert_eq!(session_bytes.load(Ordering::Acquire), UPLOAD_MAX_TOTAL_BYTES_PER_SESSION);
    }

    #[test]
    fn unsuccessful_chromium_upload_releases_disk_and_reservation() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let staging_dir = tempdir.path().join("staged");
        let staged_path = staging_dir.join("upload.txt");
        fs::create_dir_all(staging_dir.as_path()).expect("staging directory should exist");
        fs::write(staged_path.as_path(), b"payload").expect("staged upload should exist");
        let session_bytes = Arc::new(AtomicU64::new(7));

        drop(ChromiumStagedUpload {
            path: staged_path,
            reserved_bytes: 7,
            session_bytes: Arc::clone(&session_bytes),
            retained: false,
        });

        assert!(!staging_dir.exists(), "failed upload staging must be removed");
        assert_eq!(session_bytes.load(Ordering::Acquire), 0);
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
    fn parse_chromium_console_entries_preserves_structured_object_preview() {
        let raw = serde_json::json!([{
            "severity": "info",
            "kind": "console",
            "message": "wizard-ready {\"restoredFromStorage\":true,\"step\":\"confirm\"}",
            "captured_at_unix_ms": 42_u64,
            "source": "console.info",
            "stack_trace": "",
            "page_url": "http://127.0.0.1/",
        }]);

        let entries = parse_chromium_console_entries(raw);

        assert_eq!(entries.len(), 1);
        assert!(entries[0].message.contains("\"restoredFromStorage\":true"));
        assert!(entries[0].message.contains("\"step\":\"confirm\""));
        assert!(!entries[0].message.contains("[object Object]"));
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
            CHROMIUM_CLEAR_NETWORK_LOG_SCRIPT.contains("network_entries")
                && CHROMIUM_CLEAR_NETWORK_LOG_SCRIPT.contains("length = 0"),
            "network reset should clear page-side diagnostics without exporting old entries"
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
        assert!(
            !CHROMIUM_PAGE_DIAGNOSTICS_SCRIPT.contains("JSON.stringify(value")
                && !CHROMIUM_PAGE_DIAGNOSTICS_SCRIPT
                    .contains("Object.prototype.toString.call(value)")
                && CHROMIUM_PAGE_DIAGNOSTICS_SCRIPT.contains("\"[Object]\"")
                && CHROMIUM_PAGE_DIAGNOSTICS_SCRIPT.contains("\"[Array]\""),
            "console hook must represent objects without traversing page-controlled values"
        );
        let blob_size_guard = CHROMIUM_PAGE_DIAGNOSTICS_SCRIPT
            .find("if (sizeBytes > MAX_CLIENT_DOWNLOAD_BYTES)")
            .expect("client-side download script should check Blob size");
        let blob_materialization = CHROMIUM_PAGE_DIAGNOSTICS_SCRIPT
            .find("const buffer = await blobArrayBuffer.call(blob)")
            .expect("client-side download script should use the captured Blob intrinsic");
        assert!(
            blob_size_guard < blob_materialization,
            "Blob size must be rejected before arrayBuffer materializes client download bytes"
        );
        assert!(
            !CHROMIUM_PAGE_DIAGNOSTICS_SCRIPT.contains("await blob.arrayBuffer()"),
            "page-overridable Blob methods must not control download materialization"
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
    fn chromium_select_script_returns_only_host_defined_primitive_statuses() {
        let script = chromium_select_script(r##""#country""##, r#""cz""#);

        assert!(!script.contains("JSON.stringify"));
        for status in [
            CHROMIUM_SELECT_STATUS_NOT_FOUND,
            CHROMIUM_SELECT_STATUS_NOT_SELECT,
            CHROMIUM_SELECT_STATUS_DISABLED,
            CHROMIUM_SELECT_STATUS_VALUE_NOT_FOUND,
            CHROMIUM_SELECT_STATUS_SELECTED,
        ] {
            assert!(script.contains(format!("return {status};").as_str()));
        }
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

        let (origin, entries) = parse_chromium_local_storage_snapshot(
            decode_chromium_bounded_json_script_value(raw, MAX_CHROMIUM_LOCAL_STORAGE_JSON_BYTES),
        )
        .expect("snapshot payload should parse")
        .expect("origin should be present");

        assert_eq!(origin, "http://127.0.0.1:49152");
        assert_eq!(entries.get("cart").map(String::as_str), Some("1"));
        assert_eq!(entries.get("theme").map(String::as_str), Some("dark"));
    }

    #[test]
    fn local_storage_decoder_rejects_oversized_or_non_string_payloads() {
        let oversized = serde_json::Value::String(format!(
            r#"{{"padding":"{}"}}"#,
            "x".repeat(MAX_CHROMIUM_LOCAL_STORAGE_JSON_BYTES)
        ));

        assert_eq!(
            decode_chromium_bounded_json_script_value(
                oversized,
                MAX_CHROMIUM_LOCAL_STORAGE_JSON_BYTES,
            ),
            serde_json::Value::Null
        );
        assert_eq!(
            decode_chromium_bounded_json_script_value(
                serde_json::json!({"entries": {"forged": "value"}}),
                MAX_CHROMIUM_LOCAL_STORAGE_JSON_BYTES,
            ),
            serde_json::Value::Null
        );
    }

    #[test]
    fn chromium_clear_active_origin_storage_script_clears_local_and_session_storage() {
        assert!(CHROMIUM_CLEAR_ACTIVE_ORIGIN_STORAGE_SCRIPT.contains("window.localStorage"));
        assert!(CHROMIUM_CLEAR_ACTIVE_ORIGIN_STORAGE_SCRIPT.contains("window.sessionStorage"));
        assert!(CHROMIUM_CLEAR_ACTIVE_ORIGIN_STORAGE_SCRIPT.contains("local.clear()"));
        assert!(CHROMIUM_CLEAR_ACTIVE_ORIGIN_STORAGE_SCRIPT.contains("session.clear()"));
    }

    #[test]
    fn parse_chromium_clear_storage_status_accepts_entry_count() {
        let raw = serde_json::json!({
            "ok": true,
            "origin": "http://127.0.0.1:8765",
            "entries_cleared": 2
        });

        let entries_cleared =
            parse_chromium_clear_storage_status(raw).expect("storage clear status should parse");

        assert_eq!(entries_cleared, 2);
    }

    #[test]
    fn parse_chromium_clear_storage_status_rejects_page_failure() {
        let raw = serde_json::json!({
            "ok": false,
            "error": "localStorage unavailable"
        });

        let error = parse_chromium_clear_storage_status(raw)
            .expect_err("page-side storage clear failure must be surfaced");

        assert!(error.contains("localStorage unavailable"));
    }

    #[test]
    fn chromium_cookie_delete_requests_preserve_match_fields_without_values() {
        let cookie: crate::Network::Cookie = serde_json::from_value(serde_json::json!({
            "name": "session",
            "value": "secret-cookie-value",
            "domain": ".example.test",
            "path": "/app",
            "expires": -1,
            "size": 26,
            "httpOnly": true,
            "secure": true,
            "session": true,
            "priority": "Medium",
            "sameParty": false,
            "sourceScheme": "Secure",
            "sourcePort": 443,
            "partitionKey": {
                "topLevelSite": "https://example.test",
                "hasCrossSiteAncestor": false
            }
        }))
        .expect("test cookie should match CDP schema");

        let delete_requests = chromium_cookie_delete_requests(&[cookie]);

        assert_eq!(delete_requests.len(), 1);
        let request = &delete_requests[0];
        assert_eq!(request.name, "session");
        assert_eq!(request.domain.as_deref(), Some(".example.test"));
        assert_eq!(request.path.as_deref(), Some("/app"));
        assert!(request.url.is_none());
        assert!(request.partition_key.is_some());
        let encoded =
            serde_json::to_value(request).expect("delete request should serialize as CDP JSON");
        assert!(
            encoded.get("value").is_none(),
            "cookie values must not be sent in delete requests"
        );
    }

    #[test]
    fn chromium_permission_origin_requires_http_origin() {
        assert_eq!(
            chromium_permission_origin("http://127.0.0.1:5175/path?token=secret")
                .expect("http origin should parse"),
            "http://127.0.0.1:5175"
        );
        assert_eq!(
            chromium_permission_origin("https://example.test/app")
                .expect("https origin should parse"),
            "https://example.test"
        );
        assert!(
            chromium_permission_origin("about:blank").is_err(),
            "permission override must fail loud when there is no page origin"
        );
    }

    #[test]
    fn chromium_permission_origins_deduplicate_open_http_origins() {
        let origins = chromium_permission_origins_for_urls(&[
            "https://example.test/app".to_owned(),
            "https://example.test/other".to_owned(),
            "http://127.0.0.1:5175/".to_owned(),
            "file:///tmp/local.html".to_owned(),
            "about:blank".to_owned(),
        ]);

        assert_eq!(
            origins,
            vec!["http://127.0.0.1:5175".to_owned(), "https://example.test".to_owned()]
        );
    }

    #[test]
    fn chromium_sync_tab_url_filter_ignores_browser_internal_tabs() {
        assert!(chromium_sync_tab_url_is_trackable("http://127.0.0.1:5175/callback.html"));
        assert!(chromium_sync_tab_url_is_trackable("https://example.test/callback"));
        assert!(chromium_sync_tab_url_is_trackable("file:///tmp/callback.html"));
        assert!(!chromium_sync_tab_url_is_trackable("about:blank"));
        assert!(!chromium_sync_tab_url_is_trackable("chrome://newtab/"));
        assert!(!chromium_sync_tab_url_is_trackable(""));
    }

    #[test]
    fn chromium_timeout_snapshot_url_accepts_reached_target_or_hash_route() {
        assert!(chromium_timeout_snapshot_url_is_usable(
            "http://127.0.0.1:8765/index.html?v=2#/settings",
            "http://127.0.0.1:8765/index.html?v=2#/settings",
        ));
        assert!(chromium_timeout_snapshot_url_is_usable(
            "http://127.0.0.1:8765/index.html?v=1#/login",
            "http://127.0.0.1:8765/index.html?v=1",
        ));
    }

    #[test]
    fn chromium_timeout_snapshot_url_rejects_unrelated_or_partial_targets() {
        assert!(!chromium_timeout_snapshot_url_is_usable(
            "http://127.0.0.1:8765/other.html",
            "http://127.0.0.1:8765/index.html",
        ));
        assert!(!chromium_timeout_snapshot_url_is_usable(
            "http://127.0.0.1:8765/index.html-extra",
            "http://127.0.0.1:8765/index.html",
        ));
    }

    #[test]
    fn chromium_sync_tab_url_validation_ignores_tab_scoped_private_file_allowance() {
        let policy = std::sync::Arc::new(ChromiumPrivateTargetPolicy::new(false));
        let fixture = tempfile::NamedTempFile::new().expect("file fixture should be created");
        let file_url =
            reqwest::Url::from_file_path(fixture.path()).expect("file URL should be built");

        let error = chromium_sync_tab_url_is_allowed(file_url.as_str(), policy.as_ref())
            .expect_err("unapproved file popup candidate must fail validation");
        assert!(
            error.contains("local file navigation requires allow_private_targets=true"),
            "file popup validation should fail through the normal local-file gate: {error}"
        );

        let scoped = policy
            .scoped_url_allowance("tab-a", file_url.as_str())
            .expect("scoped file allowance should parse")
            .expect("file URL should create scoped allowance");

        assert!(
            policy.allows_tab_url("tab-a", file_url.as_str()),
            "owning tab should be allowed for its scoped file URL"
        );
        let error = chromium_sync_tab_url_is_allowed(file_url.as_str(), policy.as_ref())
            .expect_err("popup sync must not reuse another tab's scoped file allowance");
        assert!(
            error.contains("local file navigation requires allow_private_targets=true"),
            "file popup validation should fail despite the tab-scoped guard: {error}"
        );
        assert!(
            !chromium_sync_tab_url_is_allowed("about:blank", policy.as_ref())
                .expect("browser-internal tabs should be ignored"),
            "browser-internal tabs are not popup sync candidates"
        );
        drop(scoped);

        let error = chromium_sync_tab_url_is_allowed(file_url.as_str(), policy.as_ref())
            .expect_err("dropped file popup allowance must fail validation again");
        assert!(
            error.contains("local file navigation requires allow_private_targets=true"),
            "file popup validation should fail after the scoped guard drops: {error}"
        );

        let session_allow_policy = ChromiumPrivateTargetPolicy::new(true);
        assert!(
            chromium_sync_tab_url_is_allowed(file_url.as_str(), &session_allow_policy)
                .expect("session private-target allow should validate private file popup"),
            "session-wide private-target allow should still permit file popup sync"
        );
    }

    #[test]
    fn chromium_permission_set_requests_match_session_policy() {
        let requests = chromium_permission_set_requests(
            "http://127.0.0.1:5175",
            &SessionPermissionsInternal {
                camera: PermissionSettingInternal::Deny,
                microphone: PermissionSettingInternal::Deny,
                location: PermissionSettingInternal::Allow,
            },
        );

        assert_eq!(requests.len(), 3);
        let encoded = serde_json::to_value(&requests).expect("permission requests should encode");
        assert_eq!(encoded[0]["permission"]["name"], "camera");
        assert_eq!(encoded[0]["setting"], "denied");
        assert_eq!(encoded[1]["permission"]["name"], "microphone");
        assert_eq!(encoded[1]["setting"], "denied");
        assert_eq!(encoded[2]["permission"]["name"], "geolocation");
        assert_eq!(encoded[2]["setting"], "granted");
        assert_eq!(encoded[2]["origin"], "http://127.0.0.1:5175");
    }

    #[test]
    fn chromium_permission_reset_request_targets_default_browser_context() {
        let encoded =
            serde_json::to_value(chromium_permission_reset_request()).expect("request encodes");

        assert!(
            encoded.get("browserContextId").is_none(),
            "session permission reset should clear the default Chromium browser context"
        );
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
    fn parse_chromium_viewport_metrics_prefers_visual_viewport_size() {
        let raw = serde_json::json!({
            "visual_width": 390,
            "visual_height": 844,
            "width": 980,
            "height": 2121,
            "device_scale_factor": 2.0
        });

        let (width, height, device_scale_factor) =
            parse_chromium_viewport_metrics(raw, 390, 844, 1.0);

        assert_eq!(width, 390);
        assert_eq!(height, 844);
        assert_eq!(device_scale_factor, 2.0);
    }

    #[test]
    fn parse_chromium_viewport_metrics_uses_matching_layout_when_visual_is_scaled_noise() {
        let raw = serde_json::json!({
            "visual_width": 1208,
            "visual_height": 2148,
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
    fn chromium_viewport_mismatch_allows_exact_css_viewport() {
        assert!(!chromium_viewport_metrics_mismatch(375, 812, 375, 812));
    }

    #[test]
    fn chromium_viewport_mismatch_allows_small_visible_height_delta() {
        assert!(!chromium_viewport_metrics_mismatch(375, 667, 375, 652));
    }

    #[test]
    fn chromium_viewport_mismatch_flags_actual_css_viewport() {
        assert!(chromium_viewport_metrics_mismatch(375, 812, 1040, 2252));
    }

    #[test]
    fn desktop_touch_emulation_omits_invalid_zero_touch_points() {
        assert_eq!(chromium_touch_emulation_max_touch_points(false), None);
        assert_eq!(chromium_touch_emulation_max_touch_points(true), Some(1));
    }

    #[test]
    fn chromium_layout_metrics_from_cdp_reports_overflow() {
        let metrics = chromium_layout_metrics_from_cdp(390, 844, 390.0, 844.0, 980.0, 1200.0, 2.0);

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
    fn chromium_layout_metrics_from_cdp_keeps_layout_viewport_for_overflow() {
        let metrics = chromium_layout_metrics_from_cdp(375, 667, 531.0, 944.0, 531.0, 1200.0, 2.0);

        assert_eq!(metrics.viewport_width, 375);
        assert_eq!(metrics.viewport_height, 667);
        assert_eq!(metrics.document_client_width, 375);
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

/// Scrolls the active tab by the given deltas and records the resulting
/// scroll position on the persisted tab record.
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

/// Polls the active tab until every requested condition is satisfied,
/// refreshing the tab snapshot on success.
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
    let selector_required = !selector_owned.is_empty();
    let text_required = !text_owned.trim().is_empty();
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
                if (!selector_required || selector_hit) && (!text_required || text_hit) {
                    let _ =
                        chromium_refresh_tab_snapshot(runtime, session_id, tab_id.as_str()).await;
                    return ChromiumWaitOutcome {
                        success: true,
                        matched_selector: if selector_hit {
                            selector_owned.clone()
                        } else {
                            String::new()
                        },
                        matched_text: if text_hit { text_owned.clone() } else { String::new() },
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
