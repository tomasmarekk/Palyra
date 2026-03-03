const invoke = window.__TAURI__?.core?.invoke;

if (typeof invoke !== "function") {
  throw new Error("Tauri invoke API is unavailable. Run this UI inside the desktop host.");
}

const ui = {
  overallStatus: document.getElementById("overallStatus"),
  statusTimestamp: document.getElementById("statusTimestamp"),
  actionMessage: document.getElementById("actionMessage"),
  diagnosticsList: document.getElementById("diagnosticsList"),
  logOutput: document.getElementById("logOutput"),
  browserEnabledToggle: document.getElementById("browserEnabledToggle"),
  startBtn: document.getElementById("startBtn"),
  stopBtn: document.getElementById("stopBtn"),
  restartBtn: document.getElementById("restartBtn"),
  dashboardBtn: document.getElementById("dashboardBtn"),
  exportBundleBtn: document.getElementById("exportBundleBtn"),
  refreshBtn: document.getElementById("refreshBtn"),
  applySettingsBtn: document.getElementById("applySettingsBtn"),
  factGatewayVersion: document.getElementById("factGatewayVersion"),
  factGatewayHash: document.getElementById("factGatewayHash"),
  factGatewayUptime: document.getElementById("factGatewayUptime"),
  factDashboardUrl: document.getElementById("factDashboardUrl"),
  factDashboardAccessMode: document.getElementById("factDashboardAccessMode"),
  discordConnectorId: document.getElementById("discordConnectorId"),
  discordEnabled: document.getElementById("discordEnabled"),
  discordAuthenticated: document.getElementById("discordAuthenticated"),
  discordReadiness: document.getElementById("discordReadiness"),
  discordLastError: document.getElementById("discordLastError"),
  gatewayProcessSummary: document.getElementById("gatewayProcessSummary"),
  gatewayPorts: document.getElementById("gatewayPorts"),
  browserProcessSummary: document.getElementById("browserProcessSummary"),
  browserPorts: document.getElementById("browserPorts")
};

let pollHandle = null;
let latestSnapshot = null;

function setActionMessage(message, isError = false) {
  ui.actionMessage.textContent = message;
  ui.actionMessage.style.color = isError ? "#b43b2f" : "var(--muted)";
}

function formatUnixMs(unixMs) {
  if (typeof unixMs !== "number") {
    return "-";
  }
  return new Date(unixMs).toLocaleString();
}

function formatDurationSeconds(seconds) {
  if (typeof seconds !== "number") {
    return "-";
  }
  const total = Math.max(0, Math.floor(seconds));
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const s = total % 60;
  if (h > 0) {
    return `${h}h ${m}m ${s}s`;
  }
  if (m > 0) {
    return `${m}m ${s}s`;
  }
  return `${s}s`;
}

function renderStatusPill(overallStatus) {
  const normalized = typeof overallStatus === "string" ? overallStatus : "unknown";
  ui.overallStatus.className = "status-pill";
  ui.overallStatus.classList.add(`status-${normalized}`);
  if (!["healthy", "degraded", "down"].includes(normalized)) {
    ui.overallStatus.classList.add("status-unknown");
  }
  ui.overallStatus.textContent = normalized;
}

function renderDiagnostics(errors) {
  ui.diagnosticsList.innerHTML = "";
  if (!Array.isArray(errors) || errors.length === 0) {
    const li = document.createElement("li");
    li.textContent = "No diagnostics errors reported.";
    ui.diagnosticsList.appendChild(li);
    return;
  }

  for (const error of errors) {
    const li = document.createElement("li");
    li.textContent = String(error);
    ui.diagnosticsList.appendChild(li);
  }
}

function renderLogs(logs) {
  if (!Array.isArray(logs) || logs.length === 0) {
    ui.logOutput.textContent = "No sidecar logs captured yet.";
    return;
  }
  const lines = logs.map((entry) => {
    const time = formatUnixMs(entry.unix_ms);
    return `[${time}] [${entry.service}/${entry.stream}] ${entry.line}`;
  });
  ui.logOutput.textContent = lines.join("\n");
}

function renderProcess(serviceSnapshot, summaryNode, portsNode) {
  if (!serviceSnapshot || typeof serviceSnapshot !== "object") {
    summaryNode.textContent = "No process data";
    portsNode.textContent = "-";
    return;
  }

  const pid = serviceSnapshot.pid ? `pid=${serviceSnapshot.pid}` : "pid=n/a";
  const desired = serviceSnapshot.desired_running ? "desired=running" : "desired=stopped";
  const liveness = `liveness=${serviceSnapshot.liveness}`;
  const startTime = serviceSnapshot.last_start_unix_ms
    ? `started=${formatUnixMs(serviceSnapshot.last_start_unix_ms)}`
    : "started=n/a";
  const exit = serviceSnapshot.last_exit ? `last_exit=${serviceSnapshot.last_exit}` : "last_exit=none";

  summaryNode.textContent = `${liveness}, ${desired}, ${pid}, ${startTime}, ${exit}`;

  const ports = Array.isArray(serviceSnapshot.bound_ports)
    ? serviceSnapshot.bound_ports.join(", ")
    : "-";
  portsNode.textContent = `Bound ports: ${ports}`;
}

function renderSnapshot(snapshot) {
  latestSnapshot = snapshot;
  renderStatusPill(snapshot.overall_status);
  ui.statusTimestamp.textContent = `Updated ${formatUnixMs(snapshot.generated_at_unix_ms)}`;

  const facts = snapshot.quick_facts ?? {};
  ui.factGatewayVersion.textContent = facts.gateway_version ?? "-";
  ui.factGatewayHash.textContent = facts.gateway_git_hash ?? "-";
  ui.factGatewayUptime.textContent = formatDurationSeconds(facts.gateway_uptime_seconds);
  ui.factDashboardUrl.textContent = facts.dashboard_url ?? "-";
  ui.factDashboardAccessMode.textContent = facts.dashboard_access_mode ?? "-";

  const discord = facts.discord ?? {};
  ui.discordConnectorId.textContent = discord.connector_id ?? "-";
  ui.discordEnabled.textContent = String(discord.enabled ?? false);
  ui.discordAuthenticated.textContent = String(discord.authenticated ?? false);
  ui.discordReadiness.textContent = `${discord.readiness ?? "unknown"} / ${discord.liveness ?? "unknown"}`;
  ui.discordLastError.textContent = discord.last_error ?? "None";

  renderProcess(snapshot.gateway_process, ui.gatewayProcessSummary, ui.gatewayPorts);
  renderProcess(snapshot.browserd_process, ui.browserProcessSummary, ui.browserPorts);

  renderDiagnostics(snapshot.diagnostics?.errors ?? []);
  renderLogs(snapshot.logs ?? []);

  const warnings = Array.isArray(snapshot.warnings) ? snapshot.warnings : [];
  if (warnings.length > 0) {
    setActionMessage(`Warnings: ${warnings.join(" | ")}`, true);
  }
}

async function refreshSnapshot() {
  try {
    const snapshot = await invoke("get_snapshot");
    renderSnapshot(snapshot);
  } catch (error) {
    setActionMessage(`Snapshot refresh failed: ${String(error)}`, true);
  }
}

async function loadSettings() {
  try {
    const settings = await invoke("get_settings");
    ui.browserEnabledToggle.checked = Boolean(settings.browser_service_enabled);
  } catch (error) {
    setActionMessage(`Failed to load settings: ${String(error)}`, true);
  }
}

async function invokeAction(commandName, payload = undefined) {
  try {
    const response = payload === undefined ? await invoke(commandName) : await invoke(commandName, payload);
    setActionMessage(response?.message ?? "Action completed.");
  } catch (error) {
    setActionMessage(`${commandName} failed: ${String(error)}`, true);
  }
  await refreshSnapshot();
}

function wireEvents() {
  ui.startBtn.addEventListener("click", () => invokeAction("start_palyra"));
  ui.stopBtn.addEventListener("click", () => invokeAction("stop_palyra"));
  ui.restartBtn.addEventListener("click", () => invokeAction("restart_palyra"));
  ui.dashboardBtn.addEventListener("click", () => invokeAction("open_dashboard"));
  ui.refreshBtn.addEventListener("click", refreshSnapshot);
  ui.applySettingsBtn.addEventListener("click", () =>
    invokeAction("set_browser_service_enabled", { enabled: ui.browserEnabledToggle.checked })
  );

  ui.exportBundleBtn.addEventListener("click", async () => {
    ui.exportBundleBtn.disabled = true;
    try {
      const response = await invoke("export_support_bundle");
      const outputPath = response?.output_path ?? "(unknown path)";
      setActionMessage(`Support bundle exported to ${outputPath}`);
    } catch (error) {
      setActionMessage(`export_support_bundle failed: ${String(error)}`, true);
    } finally {
      ui.exportBundleBtn.disabled = false;
      await refreshSnapshot();
    }
  });
}

async function bootstrap() {
  wireEvents();
  await loadSettings();
  await refreshSnapshot();
  pollHandle = window.setInterval(refreshSnapshot, 4000);
}

bootstrap().catch((error) => {
  setActionMessage(`Desktop control center failed to initialize: ${String(error)}`, true);
});

window.addEventListener("beforeunload", () => {
  if (pollHandle !== null) {
    window.clearInterval(pollHandle);
  }
});
