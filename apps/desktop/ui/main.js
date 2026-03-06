const invoke = window.__TAURI__?.core?.invoke;

if (typeof invoke !== "function") {
  throw new Error("Tauri invoke API is unavailable. Run this UI inside the desktop host.");
}

const ui = {
  overallStatus: byId("overallStatus"),
  statusTimestamp: byId("statusTimestamp"),
  onboardingState: byId("onboardingState"),
  runtimeSummaryBadge: byId("runtimeSummaryBadge"),
  gatewayRuntimeBadge: byId("gatewayRuntimeBadge"),
  browserRuntimeBadge: byId("browserRuntimeBadge"),
  dashboardModeBadge: byId("dashboardModeBadge"),
  openAiStatus: byId("openAiStatus"),
  discordStatusBadge: byId("discordStatusBadge"),
  supportStateBadge: byId("supportStateBadge"),
  actionMessage: byId("actionMessage"),
  diagnosticsList: byId("diagnosticsList"),
  warningList: byId("warningList"),
  logOutput: byId("logOutput"),
  welcomeChecklist: byId("welcomeChecklist"),
  discordChecklist: byId("discordChecklist"),
  browserEnabledToggle: byId("browserEnabledToggle"),
  startBtn: byId("startBtn"),
  stopBtn: byId("stopBtn"),
  restartBtn: byId("restartBtn"),
  dashboardBtn: byId("dashboardBtn"),
  exportBundleBtn: byId("exportBundleBtn"),
  refreshBtn: byId("refreshBtn"),
  applySettingsBtn: byId("applySettingsBtn"),
  factGatewayVersion: byId("factGatewayVersion"),
  factGatewayHash: byId("factGatewayHash"),
  factGatewayUptime: byId("factGatewayUptime"),
  factDashboardUrl: byId("factDashboardUrl"),
  factDashboardAccessMode: byId("factDashboardAccessMode"),
  dashboardAccessHint: byId("dashboardAccessHint"),
  openAiDetail: byId("openAiDetail"),
  diagnosticErrorCount: byId("diagnosticErrorCount"),
  droppedDiagnostics: byId("droppedDiagnostics"),
  discordConnectorId: byId("discordConnectorId"),
  discordEnabled: byId("discordEnabled"),
  discordAuthenticated: byId("discordAuthenticated"),
  discordReadiness: byId("discordReadiness"),
  discordLastError: byId("discordLastError"),
  gatewayProcessSummary: byId("gatewayProcessSummary"),
  gatewayPorts: byId("gatewayPorts"),
  browserProcessSummary: byId("browserProcessSummary"),
  browserPorts: byId("browserPorts")
};

const commandButtons = {
  open_dashboard: Array.from(document.querySelectorAll('[data-command="open_dashboard"]')),
  export_support_bundle: Array.from(document.querySelectorAll('[data-command="export_support_bundle"]')),
  refresh_snapshot: Array.from(document.querySelectorAll('[data-command="refresh_snapshot"]'))
};

let pollHandle = null;

function byId(id) {
  const element = document.getElementById(id);
  if (element === null) {
    throw new Error(`Missing required desktop UI node #${id}.`);
  }
  return element;
}

function setActionMessage(message, isError = false) {
  ui.actionMessage.textContent = message;
  ui.actionMessage.style.color = isError ? "#8f3024" : "var(--muted)";
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
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const secs = total % 60;
  if (hours > 0) {
    return `${hours}h ${minutes}m ${secs}s`;
  }
  if (minutes > 0) {
    return `${minutes}m ${secs}s`;
  }
  return `${secs}s`;
}

function setStatusPill(node, status) {
  const normalized = normalizeStatus(status);
  node.className = node.classList.contains("mini-pill") ? "mini-pill" : "status-pill";
  node.classList.add(statusClassName(node, normalized));
  node.textContent = normalized;
}

function statusClassName(node, normalized) {
  if (node.classList.contains("mini-pill")) {
    return `mini-pill--${normalized === "unknown" ? "muted" : normalized}`;
  }
  return `status-${normalized}`;
}

function normalizeStatus(status) {
  if (status === "healthy" || status === "degraded" || status === "down") {
    return status;
  }
  return "unknown";
}

function renderList(node, items, emptyMessage) {
  node.innerHTML = "";
  if (!Array.isArray(items) || items.length === 0) {
    const li = document.createElement("li");
    li.textContent = emptyMessage;
    node.appendChild(li);
    return;
  }
  for (const item of items) {
    const li = document.createElement("li");
    li.textContent = String(item);
    node.appendChild(li);
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

function renderProcess(serviceSnapshot, summaryNode, portsNode, badgeNode) {
  if (!serviceSnapshot || typeof serviceSnapshot !== "object") {
    summaryNode.textContent = "No process data";
    portsNode.textContent = "Bound ports: -";
    setStatusPill(badgeNode, "unknown");
    return;
  }

  const liveness = asString(serviceSnapshot.liveness, "unknown");
  const desired = serviceSnapshot.desired_running ? "desired=running" : "desired=stopped";
  const pid = asNumber(serviceSnapshot.pid) ? `pid=${serviceSnapshot.pid}` : "pid=n/a";
  const startTime = asNumber(serviceSnapshot.last_start_unix_ms)
    ? `started=${formatUnixMs(serviceSnapshot.last_start_unix_ms)}`
    : "started=n/a";
  const exit = asString(serviceSnapshot.last_exit, "none");

  summaryNode.textContent = `liveness=${liveness}, ${desired}, ${pid}, ${startTime}, last_exit=${exit}`;
  const ports = Array.isArray(serviceSnapshot.bound_ports) ? serviceSnapshot.bound_ports.join(", ") : "-";
  portsNode.textContent = `Bound ports: ${ports}`;
  setStatusPill(
    badgeNode,
    liveness === "running" ? "healthy" : serviceSnapshot.desired_running ? "degraded" : "unknown"
  );
}

function renderWelcomeChecklist(snapshot) {
  const facts = snapshot.quick_facts ?? {};
  const discord = facts.discord ?? {};
  const dashboardReady = typeof facts.dashboard_url === "string" && facts.dashboard_url.length > 0;
  const gatewayReady = typeof facts.gateway_version === "string" && facts.gateway_version.length > 0;
  const discordReady = discord.enabled === true && discord.authenticated === true;

  const items = [
    `${gatewayReady ? "Complete" : "Pending"}: start the local runtime and confirm gateway health.`,
    `${dashboardReady ? "Complete" : "Pending"}: open the dashboard for full operator setup.`,
    `${discordReady ? "Complete" : "Pending"}: finish provider and connector setup in the dashboard.`
  ];
  renderList(ui.welcomeChecklist, items, "No onboarding steps are currently available.");
  setStatusPill(ui.onboardingState, gatewayReady && dashboardReady ? "healthy" : "degraded");
}

function renderDiscordChecklist(discord) {
  const items = [];
  if (discord.enabled !== true) {
    items.push("Enable the Discord connector from the dashboard before verification.");
  }
  if (discord.authenticated !== true) {
    items.push("Authenticate the Discord connector in the dashboard.");
  }
  if (asString(discord.readiness, "unknown") !== "ready") {
    items.push(`Resolve readiness state: ${asString(discord.readiness, "unknown")}.`);
  }
  if (asString(discord.liveness, "unknown") !== "running") {
    items.push(`Connector runtime is ${asString(discord.liveness, "unknown")}.`);
  }
  if (items.length === 0) {
    items.push("Discord connector looks ready for verification from the dashboard.");
  }
  renderList(ui.discordChecklist, items, "Discord status will appear after the next snapshot.");
}

function renderSnapshot(snapshot, options = {}) {
  setStatusPill(ui.overallStatus, snapshot.overall_status);
  ui.statusTimestamp.textContent = `Updated ${formatUnixMs(snapshot.generated_at_unix_ms)}`;

  const facts = snapshot.quick_facts ?? {};
  const discord = facts.discord ?? {};
  const browserService = facts.browser_service ?? {};
  const diagnostics = snapshot.diagnostics ?? {};
  const warnings = Array.isArray(snapshot.warnings) ? snapshot.warnings : [];

  ui.factGatewayVersion.textContent = asString(facts.gateway_version, "Unavailable");
  ui.factGatewayHash.textContent = asString(facts.gateway_git_hash, "Unavailable");
  ui.factGatewayUptime.textContent = formatDurationSeconds(facts.gateway_uptime_seconds);
  ui.factDashboardUrl.textContent = asString(facts.dashboard_url, "Unavailable");
  ui.factDashboardAccessMode.textContent = asString(facts.dashboard_access_mode, "unknown");
  ui.dashboardAccessHint.textContent =
    facts.dashboard_access_mode === "remote"
      ? "Remote dashboard access is configured. Desktop remains the local lifecycle shell."
      : "Dashboard access currently resolves to a local address.";

  ui.openAiDetail.textContent =
    "OpenAI sign-in and credential management stay in the dashboard. Desktop keeps provider credentials out of app-local state while surfacing runtime health and access handoff.";

  ui.discordConnectorId.textContent = asString(discord.connector_id, "Unavailable");
  ui.discordEnabled.textContent = String(Boolean(discord.enabled));
  ui.discordAuthenticated.textContent = String(Boolean(discord.authenticated));
  ui.discordReadiness.textContent = `${asString(discord.readiness, "unknown")} / ${asString(discord.liveness, "unknown")}`;
  ui.discordLastError.textContent = asString(discord.last_error, "None");

  renderProcess(snapshot.gateway_process, ui.gatewayProcessSummary, ui.gatewayPorts, ui.gatewayRuntimeBadge);
  renderProcess(snapshot.browserd_process, ui.browserProcessSummary, ui.browserPorts, ui.browserRuntimeBadge);

  setStatusPill(ui.runtimeSummaryBadge, snapshot.overall_status);
  setStatusPill(
    ui.dashboardModeBadge,
    facts.dashboard_access_mode === "remote" || facts.dashboard_access_mode === "local" ? "healthy" : "unknown"
  );
  setStatusPill(ui.openAiStatus, facts.gateway_version ? "healthy" : "degraded");
  setStatusPill(
    ui.discordStatusBadge,
    discord.enabled === true && discord.authenticated === true ? "healthy" : discord.enabled === true ? "degraded" : "unknown"
  );
  setStatusPill(
    ui.supportStateBadge,
    Array.isArray(diagnostics.errors) && diagnostics.errors.length > 0 ? "degraded" : "healthy"
  );

  const browserSummary = Boolean(browserService.healthy)
    ? "Browser sidecar available for dashboard workflows."
    : "Browser sidecar can be enabled here when dashboard workflows need browser automation.";
  ui.browserProcessSummary.textContent = `${ui.browserProcessSummary.textContent} ${browserSummary}`;

  ui.diagnosticErrorCount.textContent = Array.isArray(diagnostics.errors)
    ? String(diagnostics.errors.length)
    : "0";
  ui.droppedDiagnostics.textContent = typeof diagnostics.dropped_log_events_total === "number"
    ? String(diagnostics.dropped_log_events_total)
    : "0";

  renderWelcomeChecklist(snapshot);
  renderDiscordChecklist(discord);
  renderList(ui.warningList, warnings, "No warnings reported.");
  renderList(ui.diagnosticsList, diagnostics.errors, "No diagnostics errors reported.");
  renderLogs(snapshot.logs);

  if (warnings.length > 0) {
    setActionMessage(`Warnings: ${warnings.join(" | ")}`, true);
    return;
  }

  if (options.preserveMessage !== true) {
    setActionMessage("Desktop snapshot refreshed.");
  }
}

function asString(value, fallback) {
  return typeof value === "string" && value.length > 0 ? value : fallback;
}

function asNumber(value) {
  return typeof value === "number" && Number.isFinite(value);
}

async function refreshSnapshot(options = {}) {
  try {
    const snapshot = await invoke("get_snapshot");
    renderSnapshot(snapshot, options);
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
  let successMessage = "Action completed.";
  try {
    const response = payload === undefined ? await invoke(commandName) : await invoke(commandName, payload);
    if (commandName === "export_support_bundle") {
      const outputPath = asString(response?.output_path, "(unknown path)");
      successMessage = `Support bundle exported to ${outputPath}`;
    } else {
      successMessage = asString(response?.message, "Action completed.");
    }
  } catch (error) {
    setActionMessage(`${commandName} failed: ${String(error)}`, true);
    return;
  }
  await refreshSnapshot({ preserveMessage: true });
  setActionMessage(successMessage);
}

function wireEvents() {
  ui.startBtn.addEventListener("click", () => invokeAction("start_palyra"));
  ui.stopBtn.addEventListener("click", () => invokeAction("stop_palyra"));
  ui.restartBtn.addEventListener("click", () => invokeAction("restart_palyra"));
  ui.refreshBtn.addEventListener("click", refreshSnapshot);
  ui.applySettingsBtn.addEventListener("click", () =>
    invokeAction("set_browser_service_enabled", { enabled: ui.browserEnabledToggle.checked })
  );

  for (const button of commandButtons.open_dashboard) {
    button.addEventListener("click", () => invokeAction("open_dashboard"));
  }

  for (const button of commandButtons.refresh_snapshot) {
    button.addEventListener("click", refreshSnapshot);
  }

  for (const button of commandButtons.export_support_bundle) {
    button.addEventListener("click", async () => {
      button.disabled = true;
      ui.exportBundleBtn.disabled = true;
      try {
        await invokeAction("export_support_bundle");
      } finally {
        button.disabled = false;
        ui.exportBundleBtn.disabled = false;
      }
    });
  }
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
