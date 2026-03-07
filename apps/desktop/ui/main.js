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
  openAiProviderState: byId("openAiProviderState"),
  openAiDefaultProfile: byId("openAiDefaultProfile"),
  openAiProfileCount: byId("openAiProfileCount"),
  openAiRefreshMetrics: byId("openAiRefreshMetrics"),
  openAiAttemptState: byId("openAiAttemptState"),
  openAiFormMode: byId("openAiFormMode"),
  openAiAttemptDetail: byId("openAiAttemptDetail"),
  openAiProfileNameInput: byId("openAiProfileNameInput"),
  openAiScopeKindSelect: byId("openAiScopeKindSelect"),
  openAiAgentIdField: byId("openAiAgentIdField"),
  openAiAgentIdInput: byId("openAiAgentIdInput"),
  openAiSetDefaultToggle: byId("openAiSetDefaultToggle"),
  openAiApiKeyInput: byId("openAiApiKeyInput"),
  openAiApiKeySubmitBtn: byId("openAiApiKeySubmitBtn"),
  openAiEditorResetBtn: byId("openAiEditorResetBtn"),
  openAiApiKeyHint: byId("openAiApiKeyHint"),
  openAiOauthClientIdInput: byId("openAiOauthClientIdInput"),
  openAiOauthClientSecretInput: byId("openAiOauthClientSecretInput"),
  openAiOauthScopesInput: byId("openAiOauthScopesInput"),
  openAiOauthSubmitBtn: byId("openAiOauthSubmitBtn"),
  openAiOpenPendingBrowserBtn: byId("openAiOpenPendingBrowserBtn"),
  openAiOauthHint: byId("openAiOauthHint"),
  openAiProfilesList: byId("openAiProfilesList"),
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

const OPENAI_DEFAULT_SCOPES = "openid, profile, email, offline_access";

const openAiState = {
  targetProfileId: null,
  targetProfileName: null,
  pendingAttempt: null,
  pollHandle: null,
  status: null
};

const desktopState = {
  lastSnapshot: null
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
  if (typeof unixMs !== "number" || !Number.isFinite(unixMs) || unixMs <= 0) {
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
  setLabeledStatus(node, normalized, normalized);
}

function setLabeledStatus(node, label, status) {
  const normalized = normalizeStatus(status);
  node.className = node.classList.contains("mini-pill") ? "mini-pill" : "status-pill";
  node.classList.add(statusClassName(node, normalized));
  node.textContent = label;
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
  const openAiReady = openAiState.status?.available === true && (openAiState.status.summary?.total ?? 0) > 0;

  const items = [
    `${gatewayReady ? "Complete" : "Pending"}: start the local runtime and confirm gateway health.`,
    `${dashboardReady ? "Complete" : "Pending"}: open the dashboard for full operator setup.`,
    `${openAiReady ? "Complete" : "Pending"}: connect OpenAI with API key or OAuth from desktop or dashboard.`,
    `${discordReady ? "Complete" : "Pending"}: finish connector onboarding and verification in the dashboard.`
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
  desktopState.lastSnapshot = snapshot;
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

function renderOpenAiStatus(status) {
  openAiState.status = status;

  if (!status || status.available !== true) {
    setLabeledStatus(ui.openAiStatus, "unavailable", "unknown");
    ui.openAiProviderState.textContent = "unavailable";
    ui.openAiDefaultProfile.textContent = "None";
    ui.openAiProfileCount.textContent = "0";
    ui.openAiRefreshMetrics.textContent = "Unavailable";
    ui.openAiDetail.textContent =
      asString(status?.note, "Start the local runtime before opening the desktop OpenAI auth shell.");
    renderOpenAiProfilesList([]);
    return;
  }

  setLabeledStatus(ui.openAiStatus, asString(status.provider_state, "unknown"), status.badge_status);
  ui.openAiProviderState.textContent = asString(status.provider_state, "unknown");
  ui.openAiDefaultProfile.textContent = asString(status.default_profile_id, "None");
  ui.openAiProfileCount.textContent = formatOpenAiProfileCount(status.summary);
  ui.openAiRefreshMetrics.textContent = formatOpenAiRefreshMetrics(status.refresh_metrics);
  ui.openAiDetail.textContent = asString(
    status.note,
    "Desktop keeps OpenAI connect and quick recovery local while leaving the full operator surface to the dashboard."
  );

  const profiles = Array.isArray(status.profiles) ? status.profiles : [];
  renderOpenAiProfilesList(profiles);
  reconcileOpenAiEditorSelection(profiles);
  refreshOpenAiEditorMode();
  if (desktopState.lastSnapshot) {
    renderWelcomeChecklist(desktopState.lastSnapshot);
  }
}

function formatOpenAiProfileCount(summary) {
  if (!summary || typeof summary.total !== "number") {
    return "0";
  }
  return `${summary.total} total, ${summary.ok ?? 0} ok, ${summary.static_count ?? 0} static, ${summary.expiring ?? 0} expiring, ${summary.expired ?? 0} expired`;
}

function formatOpenAiRefreshMetrics(metrics) {
  if (!metrics || typeof metrics.attempts !== "number") {
    return "-";
  }
  return `${metrics.attempts} attempts, ${metrics.successes ?? 0} success, ${metrics.failures ?? 0} failed`;
}

function renderOpenAiProfilesList(profiles) {
  ui.openAiProfilesList.innerHTML = "";

  if (!Array.isArray(profiles) || profiles.length === 0) {
    const empty = document.createElement("div");
    empty.className = "openai-profile-empty";
    empty.textContent =
      "No OpenAI profiles are connected yet. Use API key or OAuth above, or open the dashboard for broader auth management.";
    ui.openAiProfilesList.appendChild(empty);
    return;
  }

  for (const profile of profiles) {
    ui.openAiProfilesList.appendChild(buildOpenAiProfileCard(profile));
  }
}

function buildOpenAiProfileCard(profile) {
  const card = document.createElement("article");
  card.className = "openai-profile-card";

  const top = document.createElement("div");
  top.className = "openai-profile-top";

  const titleWrap = document.createElement("div");
  const title = document.createElement("h3");
  title.textContent = profile.profile_name || profile.profile_id;
  const meta = document.createElement("div");
  meta.className = "openai-profile-meta";
  const metaLines = [
    `profile_id=${asString(profile.profile_id, "unknown")}`,
    `scope=${asString(profile.scope_label, "unknown")}`,
    `credential=${asString(profile.credential_type, "unknown")}`,
    `updated=${formatUnixMs(profile.updated_at_unix_ms)}`,
    profile.is_default === true ? "default profile" : null,
    profile.expires_at_unix_ms ? `expires=${formatUnixMs(profile.expires_at_unix_ms)}` : null,
    asString(profile.health_reason, "")
  ].filter(Boolean);
  for (const line of metaLines) {
    const lineNode = document.createElement("p");
    lineNode.textContent = line;
    meta.appendChild(lineNode);
  }
  titleWrap.appendChild(title);
  titleWrap.appendChild(meta);

  const badge = document.createElement("span");
  badge.className = "mini-pill";
  setLabeledStatus(badge, asString(profile.health_state, "unknown"), healthStateToBadge(profile.health_state));

  top.appendChild(titleWrap);
  top.appendChild(badge);
  card.appendChild(top);

  if (profile.credential_type === "oauth") {
    const refreshSummary = document.createElement("p");
    refreshSummary.className = "hint";
    refreshSummary.textContent = formatOpenAiRefreshState(profile.refresh_state, profile.scopes, profile.client_id);
    card.appendChild(refreshSummary);
  }

  const actions = document.createElement("div");
  actions.className = "openai-profile-actions";

  if (profile.can_rotate_api_key) {
    actions.appendChild(
      createProfileActionButton("Rotate API Key", async (button) => {
        button.disabled = true;
        try {
          primeOpenAiEditorForProfile(profile);
          setActionMessage(`API key editor primed for ${profile.profile_name}.`);
        } finally {
          button.disabled = false;
        }
      })
    );
  }

  if (profile.can_reconnect) {
    actions.appendChild(
      createProfileActionButton("Reconnect OAuth", async (button) => {
        await launchReconnect(button, profile.profile_id);
      })
    );
  }

  if (profile.can_refresh) {
    actions.appendChild(
      createProfileActionButton("Refresh", async (button) => {
        await runOpenAiProfileAction(button, "refresh_openai_profile_command", profile.profile_id);
      })
    );
  }

  if (profile.can_set_default) {
    actions.appendChild(
      createProfileActionButton("Set Default", async (button) => {
        await runOpenAiProfileAction(button, "set_openai_default_profile_command", profile.profile_id);
      })
    );
  }

  if (profile.can_revoke) {
    actions.appendChild(
      createProfileActionButton("Revoke", async (button) => {
        const confirmed = window.confirm(
          `Revoke OpenAI profile "${profile.profile_name || profile.profile_id}"? This removes the current credential.`
        );
        if (!confirmed) {
          return;
        }
        await runOpenAiProfileAction(button, "revoke_openai_profile_command", profile.profile_id);
      })
    );
  }

  if (actions.childElementCount > 0) {
    card.appendChild(actions);
  }

  return card;
}

function createProfileActionButton(label, onClick) {
  const button = document.createElement("button");
  button.type = "button";
  button.className = "secondary";
  button.textContent = label;
  button.addEventListener("click", () => onClick(button));
  return button;
}

function healthStateToBadge(healthState) {
  switch (asString(healthState, "unknown")) {
    case "ok":
    case "static":
      return "healthy";
    case "expiring":
    case "expired":
    case "missing":
      return "degraded";
    default:
      return "unknown";
  }
}

function formatOpenAiRefreshState(refreshState, scopes, clientId) {
  const fragments = [];
  if (clientId) {
    fragments.push(`client_id=${clientId}`);
  }
  if (Array.isArray(scopes) && scopes.length > 0) {
    fragments.push(`scopes=${scopes.join(", ")}`);
  }
  if (refreshState && typeof refreshState === "object") {
    fragments.push(`refresh_failures=${Number(refreshState.failure_count ?? 0)}`);
    if (refreshState.last_attempt_unix_ms) {
      fragments.push(`last_attempt=${formatUnixMs(refreshState.last_attempt_unix_ms)}`);
    }
    if (refreshState.last_success_unix_ms) {
      fragments.push(`last_success=${formatUnixMs(refreshState.last_success_unix_ms)}`);
    }
    if (refreshState.next_allowed_refresh_unix_ms) {
      fragments.push(`next_refresh=${formatUnixMs(refreshState.next_allowed_refresh_unix_ms)}`);
    }
    if (refreshState.last_error) {
      fragments.push(`last_error=${refreshState.last_error}`);
    }
  }
  return fragments.length > 0 ? fragments.join(" | ") : "OAuth profile is connected.";
}

function reconcileOpenAiEditorSelection(profiles) {
  if (!openAiState.targetProfileId) {
    return;
  }
  const selected = profiles.find((profile) => profile.profile_id === openAiState.targetProfileId);
  if (!selected || selected.credential_type !== "api_key") {
    resetOpenAiEditor({ preserveMessage: true });
  }
}

function primeOpenAiEditorForProfile(profile) {
  openAiState.targetProfileId = profile.profile_id;
  openAiState.targetProfileName = profile.profile_name || profile.profile_id;
  ui.openAiProfileNameInput.value = profile.profile_name || "";
  ui.openAiScopeKindSelect.value = asString(profile.scope_kind, "global");
  ui.openAiAgentIdInput.value = asString(profile.agent_id, "");
  ui.openAiSetDefaultToggle.checked = profile.is_default === true;
  applyOpenAiScopeVisibility();
  refreshOpenAiEditorMode();
}

function resetOpenAiEditor(options = {}) {
  openAiState.targetProfileId = null;
  openAiState.targetProfileName = null;
  ui.openAiProfileNameInput.value = "";
  ui.openAiScopeKindSelect.value = "global";
  ui.openAiAgentIdInput.value = "";
  ui.openAiSetDefaultToggle.checked = false;
  ui.openAiApiKeyInput.value = "";
  applyOpenAiScopeVisibility();
  refreshOpenAiEditorMode();
  if (options.preserveMessage !== true) {
    setActionMessage("OpenAI editor reset to new-profile mode.");
  }
}

function refreshOpenAiEditorMode() {
  if (openAiState.targetProfileId) {
    ui.openAiFormMode.textContent =
      `Rotating API key for ${openAiState.targetProfileName} (${openAiState.targetProfileId}). OAuth reconnect stays on each profile card.`;
    ui.openAiApiKeySubmitBtn.textContent = "Rotate API Key";
    ui.openAiApiKeyHint.textContent =
      "Submit a replacement API key for the selected profile. The desktop shell does not persist this key locally.";
  } else {
    ui.openAiFormMode.textContent =
      "Create a new OpenAI profile with API key or OAuth. Desktop sends credentials to the local control plane and does not persist them in app-local state.";
    ui.openAiApiKeySubmitBtn.textContent = "Connect API Key";
    ui.openAiApiKeyHint.textContent =
      "Use this flow for first connect or for rotating an existing API key profile.";
  }
}

function updateOpenAiAttemptState(label, status, detail) {
  setLabeledStatus(ui.openAiAttemptState, label, status);
  if (detail) {
    ui.openAiAttemptDetail.textContent = detail;
  }
}

function clearOpenAiAttemptPolling() {
  if (openAiState.pollHandle !== null) {
    window.clearInterval(openAiState.pollHandle);
    openAiState.pollHandle = null;
  }
}

function setPendingAttempt(result) {
  clearOpenAiAttemptPolling();
  openAiState.pendingAttempt = {
    attemptId: result.attempt_id,
    authorizationUrl: result.authorization_url,
    expiresAtUnixMs: result.expires_at_unix_ms,
    profileId: result.profile_id ?? null
  };
  ui.openAiOpenPendingBrowserBtn.disabled = false;
  updateOpenAiAttemptState(
    "pending",
    "unknown",
    `OAuth callback is pending for attempt ${result.attempt_id}. Expires ${formatUnixMs(result.expires_at_unix_ms)}.`
  );
  openAiState.pollHandle = window.setInterval(pollOpenAiAttemptState, 2500);
}

function clearPendingAttempt(detail) {
  clearOpenAiAttemptPolling();
  openAiState.pendingAttempt = null;
  ui.openAiOpenPendingBrowserBtn.disabled = true;
  updateOpenAiAttemptState("idle", "unknown", detail);
}

async function pollOpenAiAttemptState() {
  if (!openAiState.pendingAttempt?.attemptId) {
    return;
  }

  try {
    const response = await invoke("get_openai_oauth_callback_state_command", {
      payload: { attemptId: openAiState.pendingAttempt.attemptId }
    });
    const state = asString(response.state, "unknown");
    if (state === "pending") {
      updateOpenAiAttemptState(
        "pending",
        "unknown",
        `${response.message} Expires ${formatUnixMs(response.expires_at_unix_ms)}.`
      );
      return;
    }

    const status = state === "succeeded" ? "healthy" : "degraded";
    updateOpenAiAttemptState(state, status, response.message);
    clearOpenAiAttemptPolling();
    ui.openAiOpenPendingBrowserBtn.disabled = state === "succeeded";
    await refreshOpenAiAuthStatus();
    setActionMessage(response.message, state !== "succeeded");
  } catch (error) {
    clearOpenAiAttemptPolling();
    updateOpenAiAttemptState("error", "degraded", `OAuth callback polling failed: ${String(error)}`);
    setActionMessage(`OAuth callback polling failed: ${String(error)}`, true);
  }
}

function collectOpenAiScopePayload() {
  const kind = ui.openAiScopeKindSelect.value === "agent" ? "agent" : "global";
  const payload = { kind };
  if (kind === "agent") {
    payload.agentId = ui.openAiAgentIdInput.value.trim();
  }
  return payload;
}

function applyOpenAiScopeVisibility() {
  const showAgentId = ui.openAiScopeKindSelect.value === "agent";
  ui.openAiAgentIdField.classList.toggle("field--hidden", !showAgentId);
}

async function submitOpenAiApiKey() {
  const apiKey = ui.openAiApiKeyInput.value;
  if (apiKey.trim().length === 0) {
    setActionMessage("OpenAI API key is required.", true);
    ui.openAiApiKeyInput.focus();
    return;
  }

  ui.openAiApiKeySubmitBtn.disabled = true;
  try {
    const payload = {
      profileId: openAiState.targetProfileId,
      profileName: ui.openAiProfileNameInput.value.trim() || "OpenAI",
      scope: collectOpenAiScopePayload(),
      apiKey,
      setDefault: ui.openAiSetDefaultToggle.checked
    };
    const response = await invoke("connect_openai_api_key_command", { payload });
    ui.openAiApiKeyInput.value = "";
    await refreshOpenAiAuthStatus();
    resetOpenAiEditor({ preserveMessage: true });
    setActionMessage(asString(response?.message, "OpenAI API key connected."));
  } catch (error) {
    setActionMessage(`OpenAI API key connect failed: ${String(error)}`, true);
  } finally {
    ui.openAiApiKeySubmitBtn.disabled = false;
  }
}

async function submitOpenAiOAuth() {
  ui.openAiOauthSubmitBtn.disabled = true;
  try {
    const payload = {
      profileName: normalizeEmptyToNull(ui.openAiProfileNameInput.value),
      scope: collectOpenAiScopePayload(),
      clientId: normalizeEmptyToNull(ui.openAiOauthClientIdInput.value),
      clientSecret: normalizeEmptyToNull(ui.openAiOauthClientSecretInput.value),
      scopesText: ui.openAiOauthScopesInput.value.trim(),
      setDefault: ui.openAiSetDefaultToggle.checked
    };
    const response = await invoke("start_openai_oauth_bootstrap_command", { payload });
    ui.openAiOauthClientSecretInput.value = "";
    setPendingAttempt(response);
    setActionMessage(asString(response.message, "OpenAI OAuth browser handoff started."));
  } catch (error) {
    setActionMessage(`OpenAI OAuth bootstrap failed: ${String(error)}`, true);
  } finally {
    ui.openAiOauthSubmitBtn.disabled = false;
  }
}

async function launchReconnect(button, profileId) {
  button.disabled = true;
  try {
    const response = await invoke("reconnect_openai_oauth_command", {
      payload: { profileId }
    });
    setPendingAttempt(response);
    setActionMessage(asString(response.message, "OpenAI OAuth reconnect started."));
  } catch (error) {
    setActionMessage(`OpenAI OAuth reconnect failed: ${String(error)}`, true);
  } finally {
    button.disabled = false;
  }
}

async function runOpenAiProfileAction(button, commandName, profileId) {
  button.disabled = true;
  try {
    const response = await invoke(commandName, { payload: { profileId } });
    await refreshOpenAiAuthStatus();
    setActionMessage(asString(response?.message, "OpenAI profile action completed."));
  } catch (error) {
    setActionMessage(`${commandName} failed: ${String(error)}`, true);
  } finally {
    button.disabled = false;
  }
}

async function reopenPendingBrowser() {
  if (!openAiState.pendingAttempt?.authorizationUrl) {
    return;
  }
  ui.openAiOpenPendingBrowserBtn.disabled = true;
  try {
    await invoke("open_external_url_command", { url: openAiState.pendingAttempt.authorizationUrl });
    setActionMessage("Pending OpenAI browser handoff opened.");
  } catch (error) {
    setActionMessage(`Failed to open pending browser handoff: ${String(error)}`, true);
  } finally {
    ui.openAiOpenPendingBrowserBtn.disabled = false;
  }
}

function normalizeEmptyToNull(value) {
  const trimmed = String(value ?? "").trim();
  return trimmed.length > 0 ? trimmed : null;
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

async function refreshOpenAiAuthStatus() {
  try {
    const status = await invoke("get_openai_auth_status");
    renderOpenAiStatus(status);
  } catch (error) {
    renderOpenAiStatus({
      available: false,
      note: `OpenAI auth status refresh failed: ${String(error)}`
    });
  }
}

async function refreshAllData(options = {}) {
  await refreshSnapshot(options);
  await refreshOpenAiAuthStatus();
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
  await refreshAllData({ preserveMessage: true });
  setActionMessage(successMessage);
}

function wireEvents() {
  ui.startBtn.addEventListener("click", () => invokeAction("start_palyra"));
  ui.stopBtn.addEventListener("click", () => invokeAction("stop_palyra"));
  ui.restartBtn.addEventListener("click", () => invokeAction("restart_palyra"));
  ui.refreshBtn.addEventListener("click", () => refreshAllData());
  ui.applySettingsBtn.addEventListener("click", () =>
    invokeAction("set_browser_service_enabled", { enabled: ui.browserEnabledToggle.checked })
  );

  ui.openAiScopeKindSelect.addEventListener("change", applyOpenAiScopeVisibility);
  ui.openAiApiKeySubmitBtn.addEventListener("click", submitOpenAiApiKey);
  ui.openAiEditorResetBtn.addEventListener("click", () => resetOpenAiEditor());
  ui.openAiOauthSubmitBtn.addEventListener("click", submitOpenAiOAuth);
  ui.openAiOpenPendingBrowserBtn.addEventListener("click", reopenPendingBrowser);

  for (const button of commandButtons.open_dashboard) {
    button.addEventListener("click", () => invokeAction("open_dashboard"));
  }

  for (const button of commandButtons.refresh_snapshot) {
    button.addEventListener("click", () => refreshAllData());
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
  applyOpenAiScopeVisibility();
  refreshOpenAiEditorMode();
  ui.openAiOauthScopesInput.value = OPENAI_DEFAULT_SCOPES;
  await loadSettings();
  await refreshAllData();
  pollHandle = window.setInterval(() => {
    refreshAllData({ preserveMessage: true }).catch((error) => {
      setActionMessage(`Desktop refresh loop failed: ${String(error)}`, true);
    });
  }, 4000);
}

bootstrap().catch((error) => {
  setActionMessage(`Desktop control center failed to initialize: ${String(error)}`, true);
});

window.addEventListener("beforeunload", () => {
  if (pollHandle !== null) {
    window.clearInterval(pollHandle);
  }
  clearOpenAiAttemptPolling();
});
