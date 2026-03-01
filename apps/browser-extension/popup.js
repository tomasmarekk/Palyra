const relayBaseUrlInput = document.getElementById("relayBaseUrl");
const sessionIdInput = document.getElementById("sessionId");
const relayTokenInput = document.getElementById("relayToken");
const extensionIdInput = document.getElementById("extensionId");
const openTabAllowlistInput = document.getElementById("openTabAllowlist");
const selectorInput = document.getElementById("selector");
const openTabUrlInput = document.getElementById("openTabUrl");
const saveConfigButton = document.getElementById("saveConfig");
const refreshConfigButton = document.getElementById("refreshConfig");
const captureContextButton = document.getElementById("captureContext");
const captureScreenshotButton = document.getElementById("captureScreenshot");
const relaySnapshotButton = document.getElementById("relaySnapshot");
const relaySelectionButton = document.getElementById("relaySelection");
const relayOpenTabButton = document.getElementById("relayOpenTab");
const statusElement = document.getElementById("status");
const outputElement = document.getElementById("output");

function renderStatus(message, ok) {
  statusElement.textContent = message;
  statusElement.className = ok ? "ok" : "error";
}

function renderOutput(payload) {
  outputElement.textContent = JSON.stringify(payload, null, 2);
}

async function sendMessage(payload) {
  const response = await chrome.runtime.sendMessage(payload);
  if (!response?.ok) {
    throw new Error(response?.error || "Unknown extension runtime error.");
  }
  return response;
}

function setConfigForm(config) {
  relayBaseUrlInput.value = config.relayBaseUrl || "";
  sessionIdInput.value = config.sessionId || "";
  relayTokenInput.value = config.relayToken || "";
  extensionIdInput.value = config.extensionId || "";
  openTabAllowlistInput.value = config.openTabAllowlistRaw || "";
}

function readConfigForm() {
  return {
    relayBaseUrl: relayBaseUrlInput.value,
    sessionId: sessionIdInput.value,
    relayToken: relayTokenInput.value,
    extensionId: extensionIdInput.value,
    openTabAllowlistRaw: openTabAllowlistInput.value,
  };
}

async function refreshConfig() {
  const response = await sendMessage({ type: "palyra.get_config" });
  setConfigForm(response.config);
  renderStatus("Config loaded.", true);
  renderOutput(response.config);
}

async function saveConfig() {
  const response = await sendMessage({ type: "palyra.save_config", config: readConfigForm() });
  setConfigForm(response.config);
  renderStatus("Config saved.", true);
  renderOutput(response.config);
}

async function captureContext() {
  const response = await sendMessage({ type: "palyra.capture_context" });
  renderStatus("Captured URL + DOM snapshot from active tab.", true);
  renderOutput(response.snapshot);
}

async function captureScreenshot() {
  const response = await sendMessage({ type: "palyra.capture_screenshot" });
  if (response.screenshot.truncated) {
    renderStatus(
      `Screenshot exceeds limit (${response.screenshot.payloadBytes} > ${response.screenshot.maxBytes}); binary payload omitted.`,
      false,
    );
  } else {
    renderStatus("Screenshot captured.", true);
  }
  renderOutput(response.screenshot);
}

async function relaySendPageSnapshot() {
  const response = await sendMessage({ type: "palyra.relay_send_page_snapshot" });
  renderStatus("Relay send_page_snapshot dispatched.", true);
  renderOutput(response.result);
}

async function relayCaptureSelection() {
  const response = await sendMessage({
    type: "palyra.relay_capture_selection",
    selector: selectorInput.value,
  });
  renderStatus("Relay capture_selection dispatched.", true);
  renderOutput(response.result);
}

async function relayOpenTab() {
  const response = await sendMessage({
    type: "palyra.relay_open_tab",
    openTabUrl: openTabUrlInput.value,
  });
  renderStatus("Relay open_tab dispatched.", true);
  renderOutput(response.result);
}

async function withUiErrorBoundary(work) {
  try {
    await work();
  } catch (error) {
    renderStatus(error?.message || String(error), false);
  }
}

saveConfigButton.addEventListener("click", () => withUiErrorBoundary(saveConfig));
refreshConfigButton.addEventListener("click", () => withUiErrorBoundary(refreshConfig));
captureContextButton.addEventListener("click", () => withUiErrorBoundary(captureContext));
captureScreenshotButton.addEventListener("click", () => withUiErrorBoundary(captureScreenshot));
relaySnapshotButton.addEventListener("click", () => withUiErrorBoundary(relaySendPageSnapshot));
relaySelectionButton.addEventListener("click", () => withUiErrorBoundary(relayCaptureSelection));
relayOpenTabButton.addEventListener("click", () => withUiErrorBoundary(relayOpenTab));

withUiErrorBoundary(refreshConfig);
