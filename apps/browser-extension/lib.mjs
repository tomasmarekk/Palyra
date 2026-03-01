export const DEFAULT_RELAY_BASE_URL = "http://127.0.0.1:7142";
export const DEFAULT_OPEN_TAB_ALLOWLIST = Object.freeze([
  "https://",
  "http://127.0.0.1",
  "http://localhost",
]);
export const MAX_EXTENSION_ID_BYTES = 128;
export const MAX_SESSION_ID_BYTES = 128;
export const MAX_RELAY_TOKEN_BYTES = 2_048;
export const MAX_DOM_SNAPSHOT_BYTES_DEFAULT = 16 * 1024;
export const MAX_VISIBLE_TEXT_BYTES_DEFAULT = 8 * 1024;
export const MAX_CAPTURED_SCREENSHOT_BYTES_DEFAULT = 256 * 1024;

const LOOPBACK_HOSTS = new Set(["127.0.0.1", "localhost", "::1"]);
const EXTENSION_ID_PATTERN = /^[A-Za-z0-9._-]+$/;

function trimmed(value) {
  if (typeof value !== "string") {
    return "";
  }
  return value.trim();
}

export function utf8ByteLength(value) {
  return new TextEncoder().encode(value).length;
}

export function clampUtf8Bytes(value, maxBytes) {
  if (!Number.isFinite(maxBytes) || maxBytes <= 0) {
    return { value: "", truncated: value.length > 0, originalBytes: utf8ByteLength(value) };
  }
  if (utf8ByteLength(value) <= maxBytes) {
    return { value, truncated: false, originalBytes: utf8ByteLength(value) };
  }
  let low = 0;
  let high = value.length;
  let best = "";
  while (low <= high) {
    const mid = Math.floor((low + high) / 2);
    const candidate = value.slice(0, mid);
    const bytes = utf8ByteLength(candidate);
    if (bytes <= maxBytes) {
      best = candidate;
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  }
  return { value: best, truncated: true, originalBytes: utf8ByteLength(value) };
}

export function parseAllowlistPrefixes(rawValue) {
  const raw = trimmed(rawValue);
  if (!raw) {
    return [...DEFAULT_OPEN_TAB_ALLOWLIST];
  }
  const entries = raw
    .split(/[,\n]/)
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);
  if (entries.length === 0) {
    return [...DEFAULT_OPEN_TAB_ALLOWLIST];
  }
  return [...new Set(entries)];
}

export function normalizeRelayBaseUrl(rawValue) {
  const value = trimmed(rawValue);
  if (!value) {
    throw new Error("Relay base URL cannot be empty.");
  }
  let parsed;
  try {
    parsed = new URL(value);
  } catch (error) {
    throw new Error(`Relay base URL must be a valid absolute URL (${error.message}).`);
  }
  if (!["http:", "https:"].includes(parsed.protocol)) {
    throw new Error("Relay base URL must use http or https.");
  }
  if (!LOOPBACK_HOSTS.has(parsed.hostname.toLowerCase())) {
    throw new Error("Relay base URL must target loopback host (127.0.0.1, localhost, or ::1).");
  }
  parsed.pathname = parsed.pathname.replace(/\/+$/, "");
  parsed.hash = "";
  parsed.search = "";
  return parsed.toString().replace(/\/$/, "");
}

export function normalizeExtensionId(rawValue) {
  const value = trimmed(rawValue);
  if (!value) {
    throw new Error("Extension ID cannot be empty.");
  }
  if (utf8ByteLength(value) > MAX_EXTENSION_ID_BYTES) {
    throw new Error(`Extension ID exceeds ${MAX_EXTENSION_ID_BYTES} bytes.`);
  }
  if (!EXTENSION_ID_PATTERN.test(value)) {
    throw new Error("Extension ID contains unsupported characters.");
  }
  return value;
}

export function normalizeSessionId(rawValue) {
  const value = trimmed(rawValue);
  if (!value) {
    throw new Error("Session ID cannot be empty.");
  }
  if (utf8ByteLength(value) > MAX_SESSION_ID_BYTES) {
    throw new Error(`Session ID exceeds ${MAX_SESSION_ID_BYTES} bytes.`);
  }
  return value;
}

export function normalizeRelayToken(rawValue) {
  const value = trimmed(rawValue);
  if (!value) {
    throw new Error("Relay token cannot be empty.");
  }
  if (utf8ByteLength(value) > MAX_RELAY_TOKEN_BYTES) {
    throw new Error(`Relay token exceeds ${MAX_RELAY_TOKEN_BYTES} bytes.`);
  }
  return value;
}

export function validateOpenTabUrl(urlRaw, allowlistPrefixes) {
  const url = trimmed(urlRaw);
  if (!url) {
    throw new Error("Open-tab URL cannot be empty.");
  }
  let parsed;
  try {
    parsed = new URL(url);
  } catch (error) {
    throw new Error(`Open-tab URL must be an absolute URL (${error.message}).`);
  }
  if (!["http:", "https:"].includes(parsed.protocol)) {
    throw new Error("Open-tab URL must use http or https.");
  }
  const matched = allowlistPrefixes.some((prefix) => url.startsWith(prefix));
  if (!matched) {
    throw new Error("Open-tab URL is not allowed by extension allowlist.");
  }
  return parsed.toString();
}

export function decodeDataUrlByteLength(dataUrl) {
  const marker = ";base64,";
  const markerIndex = dataUrl.indexOf(marker);
  if (markerIndex < 0) {
    return utf8ByteLength(dataUrl);
  }
  const encoded = dataUrl.slice(markerIndex + marker.length).replace(/\s+/g, "");
  const padding = encoded.endsWith("==") ? 2 : encoded.endsWith("=") ? 1 : 0;
  return Math.max(0, Math.floor((encoded.length * 3) / 4) - padding);
}
