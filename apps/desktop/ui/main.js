import { createDiscordOnboardingFeature } from "./features/onboarding/connectors/discord/feature.js";
import { createDiscordOnboardingState } from "./features/onboarding/connectors/discord/state.js";

const invoke = window.__TAURI__?.core?.invoke;

if (typeof invoke !== "function") {
  const actionMessage = document.getElementById("actionMessage");
  if (actionMessage) {
    actionMessage.textContent =
      "Desktop control center failed to connect to the Tauri invoke bridge.";
    actionMessage.style.color = "#8f3024";
  }
  throw new Error("Tauri invoke API is unavailable. Run this UI inside the desktop host.");
}

const ui = {
  bootOverlay: byId("bootOverlay"),
  bootMessage: byId("bootMessage"),
  overallStatus: byId("overallStatus"),
  statusTimestamp: byId("statusTimestamp"),
  onboardingState: byId("onboardingState"),
  onboardingPhaseBadge: byId("onboardingPhaseBadge"),
  onboardingStepTitle: byId("onboardingStepTitle"),
  onboardingStepDetail: byId("onboardingStepDetail"),
  onboardingFlowId: byId("onboardingFlowId"),
  onboardingBlockedCount: byId("onboardingBlockedCount"),
  onboardingWarningCount: byId("onboardingWarningCount"),
  onboardingPreflightList: byId("onboardingPreflightList"),
  stateRootStatus: byId("stateRootStatus"),
  stateRootInput: byId("stateRootInput"),
  stateRootHint: byId("stateRootHint"),
  startOnboardingBtn: byId("startOnboardingBtn"),
  confirmStateRootBtn: byId("confirmStateRootBtn"),
  useDefaultStateRootBtn: byId("useDefaultStateRootBtn"),
  onboardingHomePanel: byId("onboardingHomePanel"),
  onboardingHomeSummary: byId("onboardingHomeSummary"),
  onboardingRecoveryPanel: byId("onboardingRecoveryPanel"),
  onboardingRecoveryMessage: byId("onboardingRecoveryMessage"),
  onboardingRecoveryActions: byId("onboardingRecoveryActions"),
  recoveryRestartBtn: byId("recoveryRestartBtn"),
  onboardingEventList: byId("onboardingEventList"),
  onboardingFailureSteps: byId("onboardingFailureSteps"),
  onboardingSupportBundleStats: byId("onboardingSupportBundleStats"),
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
  supportProviderAuthSummary: byId("supportProviderAuthSummary"),
  supportConnectorSummary: byId("supportConnectorSummary"),
  supportDashboardSummary: byId("supportDashboardSummary"),
  supportBrowserSummary: byId("supportBrowserSummary"),
  supportBundleSummary: byId("supportBundleSummary"),
  supportRecentFailures: byId("supportRecentFailures"),
  supportFailureClasses: byId("supportFailureClasses"),
  discordConnectorId: byId("discordConnectorId"),
  discordEnabled: byId("discordEnabled"),
  discordAuthenticated: byId("discordAuthenticated"),
  discordReadiness: byId("discordReadiness"),
  discordLastError: byId("discordLastError"),
  discordActionDetail: byId("discordActionDetail"),
  discordVerifyStatus: byId("discordVerifyStatus"),
  discordTokenInput: byId("discordTokenInput"),
  discordFormAccountId: byId("discordFormAccountId"),
  discordFormMode: byId("discordFormMode"),
  discordFormScope: byId("discordFormScope"),
  discordFormVerifyChannelId: byId("discordFormVerifyChannelId"),
  discordFormConcurrency: byId("discordFormConcurrency"),
  discordFormBroadcast: byId("discordFormBroadcast"),
  discordFormRequireMention: byId("discordFormRequireMention"),
  discordFormConfirmOpen: byId("discordFormConfirmOpen"),
  discordFormAllowFrom: byId("discordFormAllowFrom"),
  discordFormDenyFrom: byId("discordFormDenyFrom"),
  discordVerifyTarget: byId("discordVerifyTarget"),
  discordVerifyText: byId("discordVerifyText"),
  discordPreflightBtn: byId("discordPreflightBtn"),
  discordApplyBtn: byId("discordApplyBtn"),
  discordVerifyBtn: byId("discordVerifyBtn"),
  discordWizardWarnings: byId("discordWizardWarnings"),
  discordPreflightBadge: byId("discordPreflightBadge"),
  discordPreflightResults: byId("discordPreflightResults"),
  discordApplyBadge: byId("discordApplyBadge"),
  discordApplyResults: byId("discordApplyResults"),
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
  lastSnapshot: null,
  lastOnboarding: null,
  stateRootDirty: false
};

const discordWizardState = createDiscordOnboardingState();

let pollHandle = null;
const ACTIVE_REFRESH_INTERVAL_MS = 4000;
const IDLE_REFRESH_INTERVAL_MS = 12000;
let mainWindowShown = false;

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

function setBootMessage(message) {
  ui.bootMessage.textContent = message;
}

function hideBootOverlay() {
  ui.bootOverlay.classList.add("boot-overlay--hidden");
}

async function showMainWindow() {
  if (mainWindowShown) {
    return;
  }
  await invoke("show_main_window");
  mainWindowShown = true;
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

function formatBasisPointsPercent(value) {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "-";
  }
  return `${(value / 100).toFixed(2)}%`;
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

function toggleHidden(node, hidden) {
  node.classList.toggle("section-hidden", hidden);
}

function renderOnboardingProgress(status) {
  const steps = Array.isArray(status?.steps) ? status.steps : [];
  ui.welcomeChecklist.innerHTML = "";

  if (steps.length === 0) {
    renderList(ui.welcomeChecklist, [], "No onboarding steps are currently available.");
    return;
  }

  for (const step of steps) {
    const li = document.createElement("li");
    const state = String(step.status ?? "pending").toUpperCase();
    const title = asString(step.title, "Step");
    const detail = asString(step.detail, "");
    const strong = document.createElement("strong");
    strong.textContent = state;
    li.appendChild(strong);
    li.append(document.createTextNode(`${title}${detail ? ` - ${detail}` : ""}`));
    ui.welcomeChecklist.appendChild(li);
  }
}

function renderPreflightChecks(status) {
  const checks = Array.isArray(status?.preflight?.checks) ? status.preflight.checks : [];
  ui.onboardingBlockedCount.textContent = `${Number(status?.preflight?.blocked_count ?? 0)} blockers`;
  ui.onboardingWarningCount.textContent = `${Number(status?.preflight?.warning_count ?? 0)} warnings`;
  ui.onboardingPreflightList.innerHTML = "";

  if (checks.length === 0) {
    renderList(ui.onboardingPreflightList, [], "Preflight checks will appear after the first refresh.");
    return;
  }

  for (const check of checks) {
    const li = document.createElement("li");
    li.textContent = `[${String(check.status ?? "unknown").toUpperCase()}] ${asString(check.label, check.key)}: ${asString(check.detail, "No detail")}`;
    ui.onboardingPreflightList.appendChild(li);
  }
}

function renderOnboardingEvents(status) {
  const events = Array.isArray(status?.recent_events) ? [...status.recent_events].reverse() : [];
  ui.onboardingEventList.innerHTML = "";
  if (events.length === 0) {
    renderList(ui.onboardingEventList, [], "No onboarding events recorded yet.");
    return;
  }

  for (const event of events.slice(0, 10)) {
    const li = document.createElement("li");
    const time = formatUnixMs(event.recorded_at_unix_ms);
    const detail = asString(event.detail, "");
    li.textContent = `${time} | ${asString(event.kind, "event")}${detail ? ` | ${detail}` : ""}`;
    ui.onboardingEventList.appendChild(li);
  }
}

function renderOnboardingSupportability(status) {
  const failureSteps = Array.isArray(status?.failure_step_counts) ? status.failure_step_counts : [];
  if (failureSteps.length === 0) {
    ui.onboardingFailureSteps.textContent = "No onboarding failure hotspots recorded.";
  } else {
    const summary = [...failureSteps]
      .sort((left, right) => Number(right.failures ?? 0) - Number(left.failures ?? 0))
      .slice(0, 3)
      .map((entry) => `${asString(entry.step, "unknown")}=${Number(entry.failures ?? 0)}`)
      .join(", ");
    ui.onboardingFailureSteps.textContent = summary;
  }

  const bundleMetrics = status?.support_bundle_exports ?? {};
  const attempts = Number(bundleMetrics.attempts ?? 0);
  const successes = Number(bundleMetrics.successes ?? 0);
  const failures = Number(bundleMetrics.failures ?? 0);
  if (attempts <= 0) {
    ui.onboardingSupportBundleStats.textContent = "No support bundle exports attempted yet.";
    return;
  }
  ui.onboardingSupportBundleStats.textContent =
    `${successes}/${attempts} succeeded (${formatBasisPointsPercent(Number(bundleMetrics.success_rate_bps ?? 0))}), failures=${failures}`;
}

function renderOnboardingRecovery(status) {
  const recovery = status?.recovery;
  if (!recovery || typeof recovery !== "object") {
    toggleHidden(ui.onboardingRecoveryPanel, true);
    return;
  }

  toggleHidden(ui.onboardingRecoveryPanel, false);
  ui.onboardingRecoveryMessage.textContent = asString(
    recovery.message,
    "Desktop onboarding detected a failure and is waiting for recovery."
  );
  renderList(
    ui.onboardingRecoveryActions,
    Array.isArray(recovery.suggested_actions) ? recovery.suggested_actions : [],
    "No recovery guidance available."
  );
}

function renderOnboardingHome(status) {
  const isHome = status?.phase === "home";
  toggleHidden(ui.onboardingHomePanel, !isHome);
  if (!isHome) {
    return;
  }

  const items = [
    `${status.openai_ready ? "Complete" : "Pending"}: OpenAI default profile ${asString(status.openai_default_profile_id, "is not set")}.`,
    `${status.discord_verified ? "Complete" : "Pending"}: Discord verification target ${asString(status.discord_last_verified_target, "has not been confirmed yet")}.`,
    `${status.dashboard_handoff_completed ? "Complete" : "Pending"}: dashboard handoff ${status.dashboard_handoff_completed ? "has been recorded" : "is still waiting"}.`,
    `${status.dashboard_reachable ? "Complete" : "Pending"}: dashboard URL ${asString(status.dashboard_url, "is unavailable")}.`
  ];
  renderList(ui.onboardingHomeSummary, items, "Home summary will appear here after onboarding completes.");
}

function primeStateRootInput(status) {
  if (desktopState.stateRootDirty) {
    return;
  }
  const preferred = asString(status?.state_root_path, "");
  if (preferred.length > 0) {
    ui.stateRootInput.value = preferred;
  }
}

function renderOnboardingStatus(status) {
  desktopState.lastOnboarding = status;
  setLabeledStatus(
    ui.onboardingState,
    status?.phase === "home"
      ? "home"
      : `${Number(status?.progress_completed ?? 0)}/${Number(status?.progress_total ?? 0)}`,
    status?.phase === "home" ? "healthy" : status?.recovery ? "degraded" : "unknown"
  );
  setLabeledStatus(
    ui.onboardingPhaseBadge,
    status?.phase === "home" ? "home" : "onboarding",
    status?.phase === "home" ? "healthy" : "unknown"
  );
  ui.onboardingStepTitle.textContent = asString(status?.current_step_title, "Local onboarding checklist");
  ui.onboardingStepDetail.textContent = asString(
    status?.current_step_detail,
    "Desktop will guide local runtime validation, OpenAI connect, Discord verification, and dashboard handoff."
  );
  ui.onboardingFlowId.textContent = `Flow ID: ${asString(status?.flow_id, "unavailable")}`;

  primeStateRootInput(status);
  setLabeledStatus(
    ui.stateRootStatus,
    status?.state_root_confirmed ? "confirmed" : "pending",
    status?.state_root_confirmed ? "healthy" : "unknown"
  );
  ui.stateRootHint.textContent = status?.state_root_overridden
    ? `Desktop is using a custom runtime root. Default: ${asString(status?.default_state_root_path, "-")}`
    : `Default runtime root: ${asString(status?.default_state_root_path, "-")}`;
  ui.startOnboardingBtn.textContent =
    status?.phase === "home"
      ? "Setup Complete"
      : Number(status?.progress_completed ?? 0) > 0
        ? "Resume Guided Setup"
        : "Start Guided Setup";
  ui.startOnboardingBtn.disabled = status?.phase === "home";
  if (status?.phase !== "home" && !openAiState.targetProfileId && (openAiState.status?.summary?.total ?? 0) === 0) {
    ui.openAiSetDefaultToggle.checked = true;
  }

  renderOnboardingProgress(status);
  renderPreflightChecks(status);
  renderOnboardingRecovery(status);
  renderOnboardingHome(status);
  renderOnboardingEvents(status);
  renderOnboardingSupportability(status);
  applyDiscordDefaultsFromOnboarding(status);
  renderDiscordResultCards(status);
}

function renderWelcomeChecklist() {
  if (desktopState.lastOnboarding) {
    renderOnboardingProgress(desktopState.lastOnboarding);
  }
}

function renderSnapshot(snapshot, options = {}) {
  desktopState.lastSnapshot = snapshot;
  setStatusPill(ui.overallStatus, snapshot.overall_status);
  ui.statusTimestamp.textContent = `Updated ${formatUnixMs(snapshot.generated_at_unix_ms)}`;

  const facts = snapshot.quick_facts ?? {};
  const discord = facts.discord ?? {};
  const browserService = facts.browser_service ?? {};
  const diagnostics = snapshot.diagnostics ?? {};
  const observability = diagnostics.observability ?? {};
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
  ui.discordReadiness.textContent = buildDiscordReadinessSummary(discord);
  ui.discordLastError.textContent = buildDiscordLastErrorSummary(discord);

  renderProcess(snapshot.gateway_process, ui.gatewayProcessSummary, ui.gatewayPorts, ui.gatewayRuntimeBadge);
  renderProcess(snapshot.browserd_process, ui.browserProcessSummary, ui.browserPorts, ui.browserRuntimeBadge);

  setStatusPill(ui.runtimeSummaryBadge, snapshot.overall_status);
  setStatusPill(
    ui.dashboardModeBadge,
    facts.dashboard_access_mode === "remote" || facts.dashboard_access_mode === "local" ? "healthy" : "unknown"
  );
  setStatusPill(
    ui.discordStatusBadge,
    resolveDiscordBadgeStatus(discord)
  );
  const connectorObservability = observability.connector ?? {};
  const providerAuthObservability = observability.provider_auth ?? {};
  const dashboardObservability = observability.dashboard ?? {};
  const browserObservability = observability.browser ?? {};
  const supportBundleObservability = observability.support_bundle ?? {};
  const hasSupportDegradation =
    (Array.isArray(diagnostics.errors) && diagnostics.errors.length > 0) ||
    Number(observability.recent_failure_count ?? 0) > 0 ||
    Number(connectorObservability.dead_letters ?? 0) > 0 ||
    Number(providerAuthObservability.failures ?? 0) > 0 ||
    Number(dashboardObservability.failures ?? 0) > 0 ||
    Number(browserObservability.relay_failures ?? 0) > 0;
  setStatusPill(
    ui.supportStateBadge,
    hasSupportDegradation ? "degraded" : "healthy"
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
  ui.supportProviderAuthSummary.textContent =
    `${asString(providerAuthObservability.state, "unknown")}; failures=${Number(providerAuthObservability.failures ?? 0)}/${Number(providerAuthObservability.attempts ?? 0)}, refresh=${Number(providerAuthObservability.refresh_failures ?? 0)}`;
  ui.supportConnectorSummary.textContent =
    `queue=${Number(connectorObservability.queue_depth ?? 0)}, dead=${Number(connectorObservability.dead_letters ?? 0)}, degraded=${Number(connectorObservability.degraded_connectors ?? 0)}, uploads=${Number(connectorObservability.upload_failures ?? 0)}`;
  ui.supportDashboardSummary.textContent =
    `failures=${Number(dashboardObservability.failures ?? 0)}/${Number(dashboardObservability.attempts ?? 0)} (${formatBasisPointsPercent(Number(dashboardObservability.failure_rate_bps ?? 0))})`;
  ui.supportBrowserSummary.textContent =
    `relay failures=${Number(browserObservability.relay_failures ?? 0)}/${Number(browserObservability.relay_attempts ?? 0)} (${formatBasisPointsPercent(Number(browserObservability.relay_failure_rate_bps ?? 0))})`;
  ui.supportBundleSummary.textContent =
    `${Number(supportBundleObservability.successes ?? 0)}/${Number(supportBundleObservability.attempts ?? 0)} succeeded (${formatBasisPointsPercent(Number(supportBundleObservability.success_rate_bps ?? 0))})`;
  ui.supportRecentFailures.textContent = `${Number(observability.recent_failure_count ?? 0)} redacted failures in the current snapshot`;
  const failureClasses = observability.failure_classes ?? {};
  ui.supportFailureClasses.textContent =
    `config=${Number(failureClasses.config_failure ?? 0)}, upstream=${Number(failureClasses.upstream_provider_failure ?? 0)}, product=${Number(failureClasses.product_failure ?? 0)}`;

  renderWelcomeChecklist();
  renderDiscordChecklist(discord);
  renderList(ui.warningList, warnings, "No warnings reported.");
  renderList(ui.diagnosticsList, diagnostics.errors, "No diagnostics errors reported.");
  renderLogs(snapshot.logs);

  if (warnings.length > 0) {
    if (options.preserveMessage !== true) {
      setActionMessage(`Warnings: ${warnings.join(" | ")}`, true);
    }
    return;
  }

  if (options.preserveMessage !== true) {
    setActionMessage("Desktop snapshot refreshed.");
  }
}

function applyOpenAiCapabilities(status) {
  const available = status?.available === true;
  const bootstrapSupported = available && status?.bootstrap_supported === true;
  const defaultSelectionSupported = available && status?.default_selection_supported === true;

  ui.openAiApiKeySubmitBtn.disabled = !available;
  ui.openAiOauthSubmitBtn.disabled = !bootstrapSupported;
  ui.openAiSetDefaultToggle.disabled = !defaultSelectionSupported;

  if (!available) {
    ui.openAiOauthHint.textContent =
      "Start the local runtime before using OAuth browser handoff from the desktop shell.";
    return;
  }

  ui.openAiOauthHint.textContent = bootstrapSupported
    ? "Desktop can bootstrap the OAuth attempt, open the browser and poll callback completion."
    : "OAuth bootstrap is not exposed on this install. Use API key connect here or continue in the dashboard.";
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
    applyOpenAiCapabilities(status);
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
  applyOpenAiCapabilities(status);
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

function shouldRequestOpenAiDefaultSelection() {
  return ui.openAiSetDefaultToggle.disabled !== true && ui.openAiSetDefaultToggle.checked === true;
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
      setDefault: shouldRequestOpenAiDefaultSelection()
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
      setDefault: shouldRequestOpenAiDefaultSelection()
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

function asPositiveNumber(value) {
  return asNumber(value) && value > 0 ? value : null;
}

function hasText(value) {
  return typeof value === "string" && value.trim().length > 0;
}

const discordOnboardingFeature = createDiscordOnboardingFeature({
  ui,
  invoke,
  desktopState,
  discordWizardState,
  setActionMessage,
  renderList,
  setLabeledStatus,
  asString,
  asPositiveNumber,
  formatUnixMs,
  normalizeEmptyToNull,
  refreshOnboardingStatus,
  refreshAllData
});
const {
  renderDiscordChecklist,
  buildDiscordReadinessSummary,
  buildDiscordLastErrorSummary,
  resolveDiscordBadgeStatus,
  applyDiscordDefaultsFromOnboarding,
  setDiscordWizardState,
  renderDiscordResultCards,
  bindDiscordInputs
} = discordOnboardingFeature;

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

async function refreshOnboardingStatus() {
  try {
    const status = await invoke("get_onboarding_status");
    renderOnboardingStatus(status);
  } catch (error) {
    setActionMessage(`Onboarding status refresh failed: ${String(error)}`, true);
  }
}

async function refreshAllData(options = {}) {
  try {
    const payload = await invoke("get_desktop_refresh_payload");
    renderDesktopRefreshPayload(payload, options);
    return payload;
  } catch (error) {
    setActionMessage(`Desktop refresh failed: ${String(error)}`, true);
    return null;
  }
}

function renderDesktopRefreshPayload(payload, options = {}) {
  renderSnapshot(payload?.snapshot, options);
  renderOpenAiStatus(payload?.openai_status);
  renderOnboardingStatus(payload?.onboarding_status);
}

function resolveRefreshIntervalMs(snapshot = desktopState.lastSnapshot, onboarding = desktopState.lastOnboarding) {
  if (
    snapshot?.overall_status === "healthy" &&
    onboarding?.phase === "home" &&
    onboarding?.recovery == null
  ) {
    return IDLE_REFRESH_INTERVAL_MS;
  }
  return ACTIVE_REFRESH_INTERVAL_MS;
}

function clearRefreshLoop() {
  if (pollHandle !== null) {
    window.clearTimeout(pollHandle);
    pollHandle = null;
  }
}

function scheduleNextRefresh(payload = null) {
  clearRefreshLoop();
  pollHandle = window.setTimeout(() => {
    runRefreshLoopOnce({ preserveMessage: true }).catch((error) => {
      setActionMessage(`Desktop refresh loop failed: ${String(error)}`, true);
    });
  }, resolveRefreshIntervalMs(payload?.snapshot, payload?.onboarding_status));
}

async function runRefreshLoopOnce(options = {}) {
  const payload = await refreshAllData(options);
  scheduleNextRefresh(payload);
  return payload;
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
  await runRefreshLoopOnce({ preserveMessage: true });
  setActionMessage(successMessage);
}

function wireEvents() {
  ui.startBtn.addEventListener("click", () => invokeAction("start_palyra"));
  ui.stopBtn.addEventListener("click", () => invokeAction("stop_palyra"));
  ui.restartBtn.addEventListener("click", () => invokeAction("restart_palyra"));
  ui.refreshBtn.addEventListener("click", () => runRefreshLoopOnce());
  ui.startOnboardingBtn.addEventListener("click", async () => {
    try {
      await invoke("acknowledge_onboarding_welcome");
      await runRefreshLoopOnce({ preserveMessage: true });
      setActionMessage("Desktop onboarding started.");
    } catch (error) {
      setActionMessage(`Failed to start onboarding: ${String(error)}`, true);
    }
  });
  ui.stateRootInput.addEventListener("input", () => {
    desktopState.stateRootDirty = true;
  });
  ui.confirmStateRootBtn.addEventListener("click", async () => {
    ui.confirmStateRootBtn.disabled = true;
    try {
      await invoke("set_onboarding_state_root_command", {
        payload: {
          path: normalizeEmptyToNull(ui.stateRootInput.value),
          confirmSelection: true
        }
      });
      desktopState.stateRootDirty = false;
      await runRefreshLoopOnce({ preserveMessage: true });
      setActionMessage("Desktop runtime state root confirmed.");
    } catch (error) {
      setActionMessage(`Failed to confirm runtime state root: ${String(error)}`, true);
    } finally {
      ui.confirmStateRootBtn.disabled = false;
    }
  });
  ui.useDefaultStateRootBtn.addEventListener("click", async () => {
    ui.useDefaultStateRootBtn.disabled = true;
    try {
      await invoke("set_onboarding_state_root_command", {
        payload: {
          path: null,
          confirmSelection: true
        }
      });
      desktopState.stateRootDirty = false;
      ui.stateRootInput.value = "";
      await runRefreshLoopOnce({ preserveMessage: true });
      setActionMessage("Desktop runtime state root reset to the default path.");
    } catch (error) {
      setActionMessage(`Failed to reset runtime state root: ${String(error)}`, true);
    } finally {
      ui.useDefaultStateRootBtn.disabled = false;
    }
  });
  ui.recoveryRestartBtn.addEventListener("click", () => invokeAction("restart_palyra"));
  ui.applySettingsBtn.addEventListener("click", () =>
    invokeAction("set_browser_service_enabled", { enabled: ui.browserEnabledToggle.checked })
  );

  ui.openAiScopeKindSelect.addEventListener("change", applyOpenAiScopeVisibility);
  ui.openAiApiKeySubmitBtn.addEventListener("click", submitOpenAiApiKey);
  ui.openAiEditorResetBtn.addEventListener("click", () => resetOpenAiEditor());
  ui.openAiOauthSubmitBtn.addEventListener("click", submitOpenAiOAuth);
  ui.openAiOpenPendingBrowserBtn.addEventListener("click", reopenPendingBrowser);
  bindDiscordInputs();

  for (const button of commandButtons.open_dashboard) {
    button.addEventListener("click", () => invokeAction("open_dashboard"));
  }

  for (const button of commandButtons.refresh_snapshot) {
    button.addEventListener("click", () => runRefreshLoopOnce());
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
  setBootMessage("Preparing the desktop shell.");
  wireEvents();
  applyOpenAiScopeVisibility();
  refreshOpenAiEditorMode();
  applyOpenAiCapabilities(null);
  ui.openAiOauthScopesInput.value = OPENAI_DEFAULT_SCOPES;
  ui.openAiSetDefaultToggle.checked = true;
  setDiscordWizardState(
    "idle",
    "unknown",
    "Run preflight, apply the connector, then send a verification message."
  );
  renderDiscordResultCards();
  await showMainWindow();
  setBootMessage("Loading desktop settings.");
  await loadSettings();
  setBootMessage("Refreshing local runtime status and recovery details.");
  await runRefreshLoopOnce();
  hideBootOverlay();
}

bootstrap().catch(async (error) => {
  try {
    await showMainWindow();
  } catch (_showError) {
    // Preserve the original bootstrap failure when the host window is already unavailable.
  }
  setBootMessage("Desktop initialization failed. Review the recovery details below.");
  hideBootOverlay();
  setActionMessage(`Desktop control center failed to initialize: ${String(error)}`, true);
});

window.addEventListener("beforeunload", () => {
  clearRefreshLoop();
  clearOpenAiAttemptPolling();
});
