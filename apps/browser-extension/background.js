import {
  DEFAULT_OPEN_TAB_ALLOWLIST,
  DEFAULT_RELAY_BASE_URL,
  MAX_CAPTURED_SCREENSHOT_BYTES_DEFAULT,
  MAX_DOM_SNAPSHOT_BYTES_DEFAULT,
  MAX_VISIBLE_TEXT_BYTES_DEFAULT,
  clampUtf8Bytes,
  decodeDataUrlByteLength,
  normalizeExtensionId,
  normalizeRelayBaseUrl,
  normalizeRelayToken,
  normalizeSessionId,
  parseAllowlistPrefixes,
  validateOpenTabUrl,
} from "./lib.mjs";

const STORAGE_KEY = "palyraRelayConfig";

function defaultConfig() {
  return {
    relayBaseUrl: DEFAULT_RELAY_BASE_URL,
    sessionId: "",
    relayToken: "",
    extensionId: chrome.runtime.id,
    openTabAllowlistRaw: DEFAULT_OPEN_TAB_ALLOWLIST.join(","),
    maxDomSnapshotBytes: MAX_DOM_SNAPSHOT_BYTES_DEFAULT,
    maxVisibleTextBytes: MAX_VISIBLE_TEXT_BYTES_DEFAULT,
    maxScreenshotBytes: MAX_CAPTURED_SCREENSHOT_BYTES_DEFAULT,
  };
}

function toSafeNumber(value, fallback) {
  if (!Number.isFinite(value) || value <= 0) {
    return fallback;
  }
  return Math.trunc(value);
}

async function loadConfig() {
  const state = await chrome.storage.local.get(STORAGE_KEY);
  const stored = state[STORAGE_KEY] || {};
  const defaults = defaultConfig();
  return {
    relayBaseUrl: typeof stored.relayBaseUrl === "string" ? stored.relayBaseUrl : defaults.relayBaseUrl,
    sessionId: typeof stored.sessionId === "string" ? stored.sessionId : defaults.sessionId,
    relayToken: typeof stored.relayToken === "string" ? stored.relayToken : defaults.relayToken,
    extensionId: typeof stored.extensionId === "string" ? stored.extensionId : defaults.extensionId,
    openTabAllowlistRaw:
      typeof stored.openTabAllowlistRaw === "string"
        ? stored.openTabAllowlistRaw
        : defaults.openTabAllowlistRaw,
    maxDomSnapshotBytes: toSafeNumber(stored.maxDomSnapshotBytes, defaults.maxDomSnapshotBytes),
    maxVisibleTextBytes: toSafeNumber(
      stored.maxVisibleTextBytes,
      defaults.maxVisibleTextBytes,
    ),
    maxScreenshotBytes: toSafeNumber(stored.maxScreenshotBytes, defaults.maxScreenshotBytes),
  };
}

function normalizeConfig(rawConfig, options = {}) {
  const requireRelayAuth = Boolean(options.requireRelayAuth);
  const config = {
    relayBaseUrl: normalizeRelayBaseUrl(rawConfig.relayBaseUrl || DEFAULT_RELAY_BASE_URL),
    sessionId: "",
    relayToken: "",
    extensionId: normalizeExtensionId(rawConfig.extensionId || chrome.runtime.id),
    openTabAllowlistRaw:
      typeof rawConfig.openTabAllowlistRaw === "string"
        ? rawConfig.openTabAllowlistRaw
        : DEFAULT_OPEN_TAB_ALLOWLIST.join(","),
    maxDomSnapshotBytes: toSafeNumber(
      Number(rawConfig.maxDomSnapshotBytes),
      MAX_DOM_SNAPSHOT_BYTES_DEFAULT,
    ),
    maxVisibleTextBytes: toSafeNumber(
      Number(rawConfig.maxVisibleTextBytes),
      MAX_VISIBLE_TEXT_BYTES_DEFAULT,
    ),
    maxScreenshotBytes: toSafeNumber(
      Number(rawConfig.maxScreenshotBytes),
      MAX_CAPTURED_SCREENSHOT_BYTES_DEFAULT,
    ),
  };
  parseAllowlistPrefixes(config.openTabAllowlistRaw);
  if (requireRelayAuth) {
    config.sessionId = normalizeSessionId(rawConfig.sessionId || "");
    config.relayToken = normalizeRelayToken(rawConfig.relayToken || "");
  } else {
    config.sessionId = (rawConfig.sessionId || "").trim();
    config.relayToken = (rawConfig.relayToken || "").trim();
  }
  return config;
}

async function saveConfig(rawConfig) {
  const normalized = normalizeConfig(rawConfig);
  await chrome.storage.local.set({ [STORAGE_KEY]: normalized });
  await setBadgeState(normalized.relayToken.length > 0);
  return normalized;
}

async function setBadgeState(enabled) {
  const text = enabled ? "ON" : "";
  await chrome.action.setBadgeText({ text });
  if (enabled) {
    await chrome.action.setBadgeBackgroundColor({ color: "#0f7f3c" });
  }
}

async function getActiveTab() {
  const tabs = await chrome.tabs.query({ active: true, currentWindow: true });
  if (!tabs || tabs.length === 0 || !tabs[0].id) {
    throw new Error("No active tab is available.");
  }
  return tabs[0];
}

async function captureCurrentTabContext(config) {
  const tab = await getActiveTab();
  const currentUrl = tab.url || "";
  await chrome.scripting.executeScript({
    target: { tabId: tab.id },
    files: ["content_script.js"],
  });
  const response = await chrome.tabs.sendMessage(tab.id, {
    type: "palyra.collect_snapshot",
    maxDomBytes: config.maxDomSnapshotBytes,
    maxVisibleTextBytes: config.maxVisibleTextBytes,
  });
  if (!response || response.ok !== true) {
    throw new Error("Failed to collect page snapshot from active tab.");
  }
  const titleText = clampUtf8Bytes(response.title || "", 2_048).value;
  return {
    tabId: tab.id,
    pageUrl: currentUrl,
    title: titleText,
    domSnapshot: response.dom_snapshot || "",
    visibleText: response.visible_text || "",
    domTruncated: Boolean(response.dom_truncated),
    visibleTextTruncated: Boolean(response.visible_text_truncated),
  };
}

async function captureScreenshot(config) {
  const tab = await getActiveTab();
  const imageDataUrl = await chrome.tabs.captureVisibleTab(tab.windowId, { format: "png" });
  const payloadBytes = decodeDataUrlByteLength(imageDataUrl);
  const exceeded = payloadBytes > config.maxScreenshotBytes;
  return {
    tabId: tab.id,
    pageUrl: tab.url || "",
    payloadBytes,
    maxBytes: config.maxScreenshotBytes,
    truncated: exceeded,
    imageDataUrl: exceeded ? "" : imageDataUrl,
  };
}

async function dispatchRelayAction(config, actionBody) {
  const endpoint = `${config.relayBaseUrl}/console/v1/browser/relay/actions`;
  const payload = {
    session_id: config.sessionId,
    extension_id: config.extensionId,
    ...actionBody,
  };
  const response = await fetch(endpoint, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${config.relayToken}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  const text = await response.text();
  let data;
  try {
    data = text ? JSON.parse(text) : {};
  } catch (_error) {
    data = { raw: text };
  }
  if (!response.ok) {
    const message = data?.error || data?.message || `Relay request failed (HTTP ${response.status}).`;
    throw new Error(message);
  }
  return data;
}

async function runRelayOpenTab(config, openTabUrl) {
  const allowlist = parseAllowlistPrefixes(config.openTabAllowlistRaw);
  const normalizedUrl = validateOpenTabUrl(openTabUrl, allowlist);
  return dispatchRelayAction(config, {
    action: "open_tab",
    open_tab: {
      url: normalizedUrl,
      activate: true,
      timeout_ms: 5_000,
    },
  });
}

async function runRelayCaptureSelection(config, selector) {
  const trimmed = (selector || "").trim();
  if (!trimmed) {
    throw new Error("Selector cannot be empty.");
  }
  return dispatchRelayAction(config, {
    action: "capture_selection",
    capture_selection: {
      selector: trimmed,
      max_selection_bytes: 8 * 1024,
    },
  });
}

async function runRelaySendPageSnapshot(config) {
  return dispatchRelayAction(config, {
    action: "send_page_snapshot",
    page_snapshot: {
      include_dom_snapshot: true,
      include_visible_text: true,
      max_dom_snapshot_bytes: config.maxDomSnapshotBytes,
      max_visible_text_bytes: config.maxVisibleTextBytes,
    },
  });
}

async function handleMessage(message) {
  const type = message?.type;
  if (!type) {
    throw new Error("Message type is required.");
  }

  if (type === "palyra.get_config") {
    const config = await loadConfig();
    return { ok: true, config };
  }

  if (type === "palyra.save_config") {
    const config = await saveConfig(message.config || {});
    return { ok: true, config };
  }

  const rawConfig = await loadConfig();
  const config = normalizeConfig(rawConfig);
  await setBadgeState(config.relayToken.length > 0);

  if (type === "palyra.capture_context") {
    const snapshot = await captureCurrentTabContext(config);
    return { ok: true, snapshot };
  }

  if (type === "palyra.capture_screenshot") {
    const screenshot = await captureScreenshot(config);
    return { ok: true, screenshot };
  }

  if (type === "palyra.relay_open_tab") {
    const relayConfig = normalizeConfig(rawConfig, { requireRelayAuth: true });
    const result = await runRelayOpenTab(relayConfig, message.openTabUrl || "");
    return { ok: true, result };
  }

  if (type === "palyra.relay_capture_selection") {
    const relayConfig = normalizeConfig(rawConfig, { requireRelayAuth: true });
    const result = await runRelayCaptureSelection(relayConfig, message.selector || "");
    return { ok: true, result };
  }

  if (type === "palyra.relay_send_page_snapshot") {
    const relayConfig = normalizeConfig(rawConfig, { requireRelayAuth: true });
    const result = await runRelaySendPageSnapshot(relayConfig);
    return { ok: true, result };
  }

  throw new Error(`Unsupported message type '${type}'.`);
}

chrome.runtime.onInstalled.addListener(async () => {
  const config = await loadConfig();
  if (config.relayToken.trim().length > 0) {
    await setBadgeState(true);
  }
});

chrome.runtime.onMessage.addListener((message, _sender, sendResponse) => {
  handleMessage(message)
    .then((result) => sendResponse(result))
    .catch((error) => sendResponse({ ok: false, error: String(error?.message || error) }));
  return true;
});
