
import {
  type Dispatch,
  type FormEvent,
  type SetStateAction,
  useEffect,
  useMemo,
  useState
} from "react";

import { ConsoleApiClient, type ConsoleSession, type JsonValue } from "../consoleApi";
import { useAuthDomain } from "./hooks/useAuthDomain";
import { useConfigDomain } from "./hooks/useConfigDomain";
import { useOverviewDomain } from "./hooks/useOverviewDomain";
import { useSupportDomain } from "./hooks/useSupportDomain";
import { DEFAULT_CRON_FORM, type CronForm, type LoginForm } from "./stateTypes";
import {
  emptyToUndefined,
  isJsonObject,
  isVisibleChannelConnector,
  parseInteger,
  readString,
  skillMetadata,
  toErrorMessage,
  toJsonObjectArray,
  toStringArray,
  type JsonObject
} from "./shared";

export type Section =
  | "overview"
  | "chat"
  | "auth"
  | "approvals"
  | "cron"
  | "channels"
  | "memory"
  | "skills"
  | "browser"
  | "config"
  | "audit"
  | "diagnostics"
  | "support";
export type ThemeMode = "light" | "dark";

export function useConsoleAppState() {
  const api = useMemo(() => new ConsoleApiClient(""), []);

  const [booting, setBooting] = useState(true);
  const [session, setSession] = useState<ConsoleSession | null>(null);
  const [section, setSection] = useState<Section>("approvals");
  const [theme, setTheme] = useState<ThemeMode>(() => {
    if (typeof window === "undefined") {
      return "light";
    }
    const stored = window.localStorage.getItem("palyra.console.theme");
    if (stored === "light" || stored === "dark") {
      return stored;
    }
    if (window.matchMedia !== undefined && window.matchMedia("(prefers-color-scheme: dark)").matches) {
      return "dark";
    }
    return "light";
  });
  const [revealSensitiveValues, setRevealSensitiveValues] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const [loginBusy, setLoginBusy] = useState(false);
  const [logoutBusy, setLogoutBusy] = useState(false);
  const [loginFormState, setLoginFormState] = useState<LoginForm>({
    adminToken: "",
    principal: "admin:web-console",
    deviceId: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
    channel: "web"
  });
  const loginForm: LoginForm = loginFormState;
  const setLoginForm: Dispatch<SetStateAction<LoginForm>> = setLoginFormState;

  const [approvalsBusy, setApprovalsBusy] = useState(false);
  const [approvals, setApprovals] = useState<JsonObject[]>([]);
  const [approvalId, setApprovalId] = useState("");
  const [approvalReason, setApprovalReason] = useState("");
  const [approvalScope, setApprovalScope] = useState("once");

  const [cronBusy, setCronBusy] = useState(false);
  const [cronJobs, setCronJobs] = useState<JsonObject[]>([]);
  const [cronRuns, setCronRuns] = useState<JsonObject[]>([]);
  const [cronJobId, setCronJobId] = useState("");
  const [cronForm, setCronForm] = useState<CronForm>(DEFAULT_CRON_FORM);

  const [channelsBusy, setChannelsBusy] = useState(false);
  const [channelsConnectors, setChannelsConnectors] = useState<JsonObject[]>([]);
  const [channelsSelectedConnectorId, setChannelsSelectedConnectorId] = useState("");
  const [channelsSelectedStatus, setChannelsSelectedStatus] = useState<JsonObject | null>(null);
  const [channelsEvents, setChannelsEvents] = useState<JsonObject[]>([]);
  const [channelsDeadLetters, setChannelsDeadLetters] = useState<JsonObject[]>([]);
  const [channelsLogsLimit, setChannelsLogsLimit] = useState("25");
  const [channelsTestText, setChannelsTestText] = useState("hello from web console");
  const [channelsTestConversationId, setChannelsTestConversationId] = useState("test:conversation");
  const [channelsTestSenderId, setChannelsTestSenderId] = useState("test-user");
  const [channelsTestSenderDisplay, setChannelsTestSenderDisplay] = useState("");
  const [channelsTestCrashOnce, setChannelsTestCrashOnce] = useState(false);
  const [channelsTestDirectMessage, setChannelsTestDirectMessage] = useState(true);
  const [channelsTestBroadcast, setChannelsTestBroadcast] = useState(false);
  const [channelsDiscordTarget, setChannelsDiscordTarget] = useState("channel:");
  const [channelsDiscordText, setChannelsDiscordText] = useState("palyra discord test message");
  const [channelsDiscordAutoReaction, setChannelsDiscordAutoReaction] = useState("");
  const [channelsDiscordThreadId, setChannelsDiscordThreadId] = useState("");
  const [channelsDiscordConfirm, setChannelsDiscordConfirm] = useState(false);
  const [channelRouterRules, setChannelRouterRules] = useState<JsonObject | null>(null);
  const [channelRouterConfigHash, setChannelRouterConfigHash] = useState("");
  const [channelRouterWarnings, setChannelRouterWarnings] = useState<string[]>([]);
  const [channelRouterPreviewChannel, setChannelRouterPreviewChannel] = useState("");
  const [channelRouterPreviewText, setChannelRouterPreviewText] = useState("pair 000000");
  const [channelRouterPreviewConversationId, setChannelRouterPreviewConversationId] = useState("");
  const [channelRouterPreviewSenderIdentity, setChannelRouterPreviewSenderIdentity] = useState("");
  const [channelRouterPreviewSenderDisplay, setChannelRouterPreviewSenderDisplay] = useState("");
  const [channelRouterPreviewSenderVerified, setChannelRouterPreviewSenderVerified] = useState(true);
  const [channelRouterPreviewIsDirectMessage, setChannelRouterPreviewIsDirectMessage] = useState(true);
  const [channelRouterPreviewRequestedBroadcast, setChannelRouterPreviewRequestedBroadcast] = useState(false);
  const [channelRouterPreviewMaxPayloadBytes, setChannelRouterPreviewMaxPayloadBytes] = useState("2048");
  const [channelRouterPreviewResult, setChannelRouterPreviewResult] = useState<JsonObject | null>(null);
  const [channelRouterPairingsFilterChannel, setChannelRouterPairingsFilterChannel] = useState("");
  const [channelRouterPairings, setChannelRouterPairings] = useState<JsonObject[]>([]);
  const [channelRouterMintChannel, setChannelRouterMintChannel] = useState("");
  const [channelRouterMintIssuedBy, setChannelRouterMintIssuedBy] = useState("");
  const [channelRouterMintTtlMs, setChannelRouterMintTtlMs] = useState("600000");
  const [channelRouterMintResult, setChannelRouterMintResult] = useState<JsonObject | null>(null);
  const [discordWizardBusy, setDiscordWizardBusy] = useState(false);
  const [discordWizardAccountId, setDiscordWizardAccountId] = useState("default");
  const [discordWizardMode, setDiscordWizardMode] = useState<"local" | "remote_vps">("local");
  const [discordWizardToken, setDiscordWizardToken] = useState("");
  const [discordWizardScope, setDiscordWizardScope] = useState<
    "dm_only" | "allowlisted_guild_channels" | "open_guild_channels"
  >("dm_only");
  const [discordWizardAllowFrom, setDiscordWizardAllowFrom] = useState("");
  const [discordWizardDenyFrom, setDiscordWizardDenyFrom] = useState("");
  const [discordWizardRequireMention, setDiscordWizardRequireMention] = useState(true);
  const [discordWizardBroadcast, setDiscordWizardBroadcast] = useState<"deny" | "mention_only" | "allow">("deny");
  const [discordWizardConcurrency, setDiscordWizardConcurrency] = useState("2");
  const [discordWizardConfirmOpen, setDiscordWizardConfirmOpen] = useState(false);
  const [discordWizardVerifyChannelId, setDiscordWizardVerifyChannelId] = useState("");
  const [discordWizardPreflight, setDiscordWizardPreflight] = useState<JsonObject | null>(null);
  const [discordWizardApply, setDiscordWizardApply] = useState<JsonObject | null>(null);
  const [discordWizardVerifyTarget, setDiscordWizardVerifyTarget] = useState("channel:");
  const [discordWizardVerifyText, setDiscordWizardVerifyText] = useState("palyra discord test message");
  const [discordWizardVerifyConfirm, setDiscordWizardVerifyConfirm] = useState(false);

  const [memoryBusy, setMemoryBusy] = useState(false);
  const [memoryQuery, setMemoryQuery] = useState("");
  const [memoryChannel, setMemoryChannel] = useState("");
  const [memoryPurgeChannel, setMemoryPurgeChannel] = useState("");
  const [memoryPurgeSessionId, setMemoryPurgeSessionId] = useState("");
  const [memoryPurgeAll, setMemoryPurgeAll] = useState(false);
  const [memoryHits, setMemoryHits] = useState<JsonObject[]>([]);
  const [memoryStatusBusy, setMemoryStatusBusy] = useState(false);
  const [memoryStatus, setMemoryStatus] = useState<JsonObject | null>(null);

  const [skillsBusy, setSkillsBusy] = useState(false);
  const [skillsEntries, setSkillsEntries] = useState<JsonObject[]>([]);
  const [skillArtifactPath, setSkillArtifactPath] = useState("");
  const [skillAllowTofu, setSkillAllowTofu] = useState(true);
  const [skillAllowUntrusted, setSkillAllowUntrusted] = useState(false);
  const [skillReason, setSkillReason] = useState("");

  const [auditBusy, setAuditBusy] = useState(false);
  const [auditFilterContains, setAuditFilterContains] = useState("");
  const [auditFilterPrincipal, setAuditFilterPrincipal] = useState("");
  const [auditEvents, setAuditEvents] = useState<JsonObject[]>([]);
  const [diagnosticsBusy, setDiagnosticsBusy] = useState(false);
  const [diagnosticsSnapshot, setDiagnosticsSnapshot] = useState<JsonObject | null>(null);

  const [browserBusy, setBrowserBusy] = useState(false);
  const [browserPrincipal, setBrowserPrincipal] = useState("");
  const [browserProfiles, setBrowserProfiles] = useState<JsonObject[]>([]);
  const [browserActiveProfileId, setBrowserActiveProfileId] = useState("");
  const [browserProfileName, setBrowserProfileName] = useState("");
  const [browserProfileTheme, setBrowserProfileTheme] = useState("");
  const [browserProfilePersistence, setBrowserProfilePersistence] = useState(true);
  const [browserProfilePrivate, setBrowserProfilePrivate] = useState(false);
  const [browserRenameProfileId, setBrowserRenameProfileId] = useState("");
  const [browserRenameName, setBrowserRenameName] = useState("");
  const [browserRelaySessionId, setBrowserRelaySessionId] = useState("");
  const [browserRelayExtensionId, setBrowserRelayExtensionId] = useState("com.palyra.extension");
  const [browserRelayTtlMs, setBrowserRelayTtlMs] = useState("300000");
  const [browserRelayToken, setBrowserRelayToken] = useState("");
  const [browserRelayTokenExpiry, setBrowserRelayTokenExpiry] = useState<number | null>(null);
  const [browserRelayAction, setBrowserRelayAction] = useState<"open_tab" | "capture_selection" | "send_page_snapshot">("capture_selection");
  const [browserRelayOpenTabUrl, setBrowserRelayOpenTabUrl] = useState("");
  const [browserRelaySelector, setBrowserRelaySelector] = useState("body");
  const [browserRelayResult, setBrowserRelayResult] = useState<JsonValue | null>(null);
  const [browserDownloadsSessionId, setBrowserDownloadsSessionId] = useState("");
  const [browserDownloadsQuarantinedOnly, setBrowserDownloadsQuarantinedOnly] = useState(false);
  const [browserDownloads, setBrowserDownloads] = useState<JsonObject[]>([]);

  const overviewDomain = useOverviewDomain({ api, setError });
  const authDomain = useAuthDomain({ api, setError, setNotice });
  const configDomain = useConfigDomain({ api, setError, setNotice });
  const supportDomain = useSupportDomain({ api, setError, setNotice });
  const {
    overviewBusy,
    overviewCatalog,
    overviewDeployment,
    overviewSupportJobs,
    refreshOverview,
    resetOverviewDomain
  } = overviewDomain;
  const {
    authBusy,
    authProfiles,
    authHealth,
    authProviderState,
    authDefaultProfileId,
    setAuthDefaultProfileId,
    authBootstrapProfileId,
    setAuthBootstrapProfileId,
    refreshAuth,
    executeOpenAiAction,
    resetAuthDomain
  } = authDomain;
  const {
    configBusy,
    configInspectPath,
    setConfigInspectPath,
    configInspectSnapshot,
    configMutationKey,
    setConfigMutationKey,
    configMutationValue,
    setConfigMutationValue,
    configValidation,
    configSecretsScope,
    setConfigSecretsScope,
    configSecrets,
    configSecretKey,
    setConfigSecretKey,
    configSecretValue,
    setConfigSecretValue,
    configSecretReveal,
    refreshConfigSurface,
    inspectConfigSurface,
    validateConfigSurface,
    mutateConfigSurface,
    refreshSecrets,
    setSecretValue,
    revealSecretValue,
    deleteSecretValue,
    resetConfigDomain
  } = configDomain;
  const {
    supportBusy,
    supportPairingSummary,
    supportPairingChannel,
    setSupportPairingChannel,
    supportPairingIssuedBy,
    setSupportPairingIssuedBy,
    supportPairingTtlMs,
    setSupportPairingTtlMs,
    supportBundleJobs,
    refreshSupport,
    mintSupportPairingCode,
    createSupportBundle,
    resetSupportDomain
  } = supportDomain;

  useEffect(() => {
    let cancelled = false;
    const bootstrap = async () => {
      setBooting(true);
      try {
        const current = await api.getSession();
        if (cancelled) {
          return;
        }
        setSession(current);
        setLoginForm((previous: LoginForm) => ({
          ...previous,
          principal: current.principal,
          deviceId: current.device_id,
          channel: current.channel ?? previous.channel
        }));
        setBrowserPrincipal((previous) => (previous.trim().length === 0 ? current.principal : previous));
      } catch {
        if (!cancelled) {
          setSession(null);
        }
      } finally {
        if (!cancelled) {
          setBooting(false);
        }
      }
    };
    void bootstrap();
    return () => {
      cancelled = true;
    };
  }, [api]);

  useEffect(() => {
    document.documentElement.setAttribute("data-theme", theme);
    if (typeof window !== "undefined") {
      window.localStorage.setItem("palyra.console.theme", theme);
    }
  }, [theme]);

  useEffect(() => {
    if (session === null) {
      return;
    }
    if (section === "overview") {
      void refreshOverview();
    }
    if (section === "auth") {
      void refreshAuth();
    }
    if (section === "approvals") {
      void refreshApprovals();
    }
    if (section === "cron") {
      void refreshCron();
    }
    if (section === "channels") {
      void refreshChannels();
    }
    if (section === "memory") {
      void refreshMemoryStatus();
    }
    if (section === "skills") {
      void refreshSkills();
    }
    if (section === "browser") {
      void refreshBrowserProfiles();
    }
    if (section === "config") {
      void refreshConfigSurface();
    }
    if (section === "audit") {
      void refreshAudit();
    }
    if (section === "diagnostics") {
      void refreshDiagnostics();
    }
    if (section === "support") {
      void refreshSupport();
    }
  }, [section, session]);

  function resetOperatorScopedState(): void {
    setSection("approvals");
    setRevealSensitiveValues(false);
    resetOverviewDomain();
    resetAuthDomain();

    setApprovalsBusy(false);
    setApprovals([]);
    setApprovalId("");
    setApprovalReason("");
    setApprovalScope("once");

    setCronBusy(false);
    setCronJobs([]);
    setCronRuns([]);
    setCronJobId("");
    setCronForm(DEFAULT_CRON_FORM);

    setChannelsBusy(false);
    setChannelsConnectors([]);
    setChannelsSelectedConnectorId("");
    setChannelsSelectedStatus(null);
    setChannelsEvents([]);
    setChannelsDeadLetters([]);
    setChannelsLogsLimit("25");
    setChannelsTestText("hello from web console");
    setChannelsTestConversationId("test:conversation");
    setChannelsTestSenderId("test-user");
    setChannelsTestSenderDisplay("");
    setChannelsTestCrashOnce(false);
    setChannelsTestDirectMessage(true);
    setChannelsTestBroadcast(false);
    setChannelsDiscordTarget("channel:");
    setChannelsDiscordText("palyra discord test message");
    setChannelsDiscordAutoReaction("");
    setChannelsDiscordThreadId("");
    setChannelsDiscordConfirm(false);
    setChannelRouterRules(null);
    setChannelRouterConfigHash("");
    setChannelRouterWarnings([]);
    setChannelRouterPreviewChannel("");
    setChannelRouterPreviewText("pair 000000");
    setChannelRouterPreviewConversationId("");
    setChannelRouterPreviewSenderIdentity("");
    setChannelRouterPreviewSenderDisplay("");
    setChannelRouterPreviewSenderVerified(true);
    setChannelRouterPreviewIsDirectMessage(true);
    setChannelRouterPreviewRequestedBroadcast(false);
    setChannelRouterPreviewMaxPayloadBytes("2048");
    setChannelRouterPreviewResult(null);
    setChannelRouterPairingsFilterChannel("");
    setChannelRouterPairings([]);
    setChannelRouterMintChannel("");
    setChannelRouterMintIssuedBy("");
    setChannelRouterMintTtlMs("600000");
    setChannelRouterMintResult(null);
    setDiscordWizardBusy(false);
    setDiscordWizardAccountId("default");
    setDiscordWizardMode("local");
    setDiscordWizardToken("");
    setDiscordWizardScope("dm_only");
    setDiscordWizardAllowFrom("");
    setDiscordWizardDenyFrom("");
    setDiscordWizardRequireMention(true);
    setDiscordWizardBroadcast("deny");
    setDiscordWizardConcurrency("2");
    setDiscordWizardConfirmOpen(false);
    setDiscordWizardVerifyChannelId("");
    setDiscordWizardPreflight(null);
    setDiscordWizardApply(null);
    setDiscordWizardVerifyTarget("channel:");
    setDiscordWizardVerifyText("palyra discord test message");
    setDiscordWizardVerifyConfirm(false);

    setMemoryBusy(false);
    setMemoryQuery("");
    setMemoryChannel("");
    setMemoryPurgeChannel("");
    setMemoryPurgeSessionId("");
    setMemoryPurgeAll(false);
    setMemoryHits([]);
    setMemoryStatusBusy(false);
    setMemoryStatus(null);

    setSkillsBusy(false);
    setSkillsEntries([]);
    setSkillArtifactPath("");
    setSkillAllowTofu(true);
    setSkillAllowUntrusted(false);
    setSkillReason("");

    setAuditBusy(false);
    setAuditFilterContains("");
    setAuditFilterPrincipal("");
    setAuditEvents([]);
    setDiagnosticsBusy(false);
    setDiagnosticsSnapshot(null);

    setBrowserBusy(false);
    setBrowserPrincipal("");
    setBrowserProfiles([]);
    setBrowserActiveProfileId("");
    setBrowserProfileName("");
    setBrowserProfileTheme("");
    setBrowserProfilePersistence(true);
    setBrowserProfilePrivate(false);
    setBrowserRenameProfileId("");
    setBrowserRenameName("");
    setBrowserRelaySessionId("");
    setBrowserRelayExtensionId("com.palyra.extension");
    setBrowserRelayTtlMs("300000");
    setBrowserRelayToken("");
    setBrowserRelayTokenExpiry(null);
    setBrowserRelayAction("capture_selection");
    setBrowserRelayOpenTabUrl("");
    setBrowserRelaySelector("body");
    setBrowserRelayResult(null);
    setBrowserDownloadsSessionId("");
    setBrowserDownloadsQuarantinedOnly(false);
    setBrowserDownloads([]);

    resetConfigDomain();
    resetSupportDomain();

    setLoginForm((previous: LoginForm) => ({ ...previous, adminToken: "" }));
  }

  async function signIn(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    setError(null);
    setNotice(null);
    setLoginBusy(true);
    try {
      const next = await api.login({
        admin_token: loginForm.adminToken,
        principal: loginForm.principal.trim(),
        device_id: loginForm.deviceId.trim(),
        channel: emptyToUndefined(loginForm.channel)
      });
      resetOperatorScopedState();
      setSession(next);
      setBrowserPrincipal(next.principal);
      setLoginForm((previous: LoginForm) => ({
        ...previous,
        adminToken: "",
        principal: next.principal,
        deviceId: next.device_id,
        channel: next.channel ?? previous.channel
      }));
      setNotice("Signed in.");
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setLoginBusy(false);
    }
  }

  async function signOut(): Promise<void> {
    setError(null);
    setNotice(null);
    setLogoutBusy(true);
    try {
      await api.logout();
      resetOperatorScopedState();
      setSession(null);
      setNotice("Signed out.");
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setLogoutBusy(false);
    }
  }

  async function refreshApprovals(): Promise<void> {
    setApprovalsBusy(true);
    setError(null);
    try {
      const response = await api.listApprovals();
      setApprovals(toJsonObjectArray(response.approvals));
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setApprovalsBusy(false);
    }
  }

  async function decideApproval(approved: boolean): Promise<void> {
    if (approvalId.trim().length === 0) {
      setError("Select an approval first.");
      return;
    }
    setApprovalsBusy(true);
    setError(null);
    try {
      await api.decideApproval(approvalId.trim(), {
        approved,
        reason: emptyToUndefined(approvalReason),
        decision_scope: approvalScope === "session" || approvalScope === "timeboxed" ? approvalScope : "once"
      });
      setNotice(approved ? "Approval allowed." : "Approval denied.");
      await refreshApprovals();
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setApprovalsBusy(false);
    }
  }

  async function refreshCron(): Promise<void> {
    setCronBusy(true);
    setError(null);
    try {
      const response = await api.listCronJobs();
      setCronJobs(toJsonObjectArray(response.jobs));
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setCronBusy(false);
    }
  }

  async function createCronJob(event: React.FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    setCronBusy(true);
    setError(null);
    try {
      await api.createCronJob({
        name: cronForm.name.trim(),
        prompt: cronForm.prompt.trim(),
        schedule_type: cronForm.scheduleType,
        cron_expression: cronForm.scheduleType === "cron" ? emptyToUndefined(cronForm.cronExpression) : undefined,
        every_interval_ms: cronForm.scheduleType === "every" ? parseInteger(cronForm.everyIntervalMs) ?? undefined : undefined,
        at_timestamp_rfc3339: cronForm.scheduleType === "at" ? emptyToUndefined(cronForm.atTimestampRfc3339) : undefined,
        enabled: cronForm.enabled,
        channel: emptyToUndefined(cronForm.channel)
      });
      setCronForm(DEFAULT_CRON_FORM);
      setNotice("Cron job created.");
      await refreshCron();
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setCronBusy(false);
    }
  }
  async function setCronEnabled(job: JsonObject, enabled: boolean): Promise<void> {
    const jobId = readString(job, "job_id");
    if (jobId === null) {
      setError("Job payload missing job_id.");
      return;
    }
    setCronBusy(true);
    setError(null);
    try {
      await api.setCronJobEnabled(jobId, enabled);
      setNotice(`Cron job ${enabled ? "enabled" : "disabled"}.`);
      await refreshCron();
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setCronBusy(false);
    }
  }

  async function runCronNow(job: JsonObject): Promise<void> {
    const jobId = readString(job, "job_id");
    if (jobId === null) {
      setError("Job payload missing job_id.");
      return;
    }
    setCronBusy(true);
    setError(null);
    try {
      await api.runCronJobNow(jobId);
      setCronJobId(jobId);
      const runs = await api.listCronRuns(jobId);
      setCronRuns(toJsonObjectArray(runs.runs));
      setNotice("Run-now dispatched.");
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setCronBusy(false);
    }
  }

  async function refreshCronRuns(): Promise<void> {
    if (cronJobId.trim().length === 0) {
      setError("Select a cron job before loading runs.");
      return;
    }
    setCronBusy(true);
    setError(null);
    try {
      const response = await api.listCronRuns(cronJobId.trim());
      setCronRuns(toJsonObjectArray(response.runs));
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setCronBusy(false);
    }
  }

  async function refreshChannelLogs(connectorId: string): Promise<void> {
    const params = new URLSearchParams();
    const parsedLimit = parseInteger(channelsLogsLimit);
    if (parsedLimit !== null && parsedLimit > 0) {
      params.set("limit", String(parsedLimit));
    }
    const response = await api.listChannelLogs(connectorId, params.size > 0 ? params : undefined);
    setChannelsEvents(toJsonObjectArray(response.events));
    setChannelsDeadLetters(toJsonObjectArray(response.dead_letters));
  }

  async function refreshChannelRouter(pairingsFilterOverride?: string): Promise<void> {
    const pairingsChannel = (pairingsFilterOverride ?? channelRouterPairingsFilterChannel).trim();
    const pairingsParams = new URLSearchParams();
    if (pairingsChannel.length > 0) {
      pairingsParams.set("channel", pairingsChannel);
    }

    const [rulesResponse, warningsResponse, pairingsResponse] = await Promise.all([
      api.getChannelRouterRules(),
      api.getChannelRouterWarnings(),
      api.listChannelRouterPairings(pairingsParams.size > 0 ? pairingsParams : undefined)
    ]);

    setChannelRouterRules(isJsonObject(rulesResponse.config) ? rulesResponse.config : null);
    setChannelRouterConfigHash(
      typeof rulesResponse.config_hash === "string" && rulesResponse.config_hash.trim().length > 0
        ? rulesResponse.config_hash
        : (typeof warningsResponse.config_hash === "string" ? warningsResponse.config_hash : "")
    );
    setChannelRouterWarnings(toStringArray(warningsResponse.warnings));
    setChannelRouterPairings(toJsonObjectArray(pairingsResponse.pairings));
  }

  async function refreshChannels(preferredConnectorId?: string): Promise<void> {
    setChannelsBusy(true);
    setError(null);
    try {
      const response = await api.listChannels();
      const connectors = toJsonObjectArray(response.connectors).filter(isVisibleChannelConnector);
      setChannelsConnectors(connectors);

      const requested = preferredConnectorId ?? channelsSelectedConnectorId;
      const requestedTrimmed = requested.trim();
      const connectorIds = connectors
        .map((entry) => readString(entry, "connector_id"))
        .filter((value): value is string => value !== null);
      const nextConnectorId = requestedTrimmed.length > 0 && connectorIds.includes(requestedTrimmed)
        ? requestedTrimmed
        : (connectorIds[0] ?? "");

      setChannelsSelectedConnectorId(nextConnectorId);
      if (nextConnectorId.length === 0) {
        setChannelsSelectedStatus(null);
        setChannelsEvents([]);
        setChannelsDeadLetters([]);
        setChannelRouterRules(null);
        setChannelRouterConfigHash("");
        setChannelRouterWarnings([]);
        setChannelRouterPairings([]);
        return;
      }

      const statusResponse = await api.getChannelStatus(nextConnectorId);
      setChannelsSelectedStatus(isJsonObject(statusResponse.connector) ? statusResponse.connector : null);
      setChannelRouterPreviewChannel((previous) =>
        previous.trim().length > 0 ? previous : nextConnectorId
      );
      setChannelRouterMintChannel((previous) =>
        previous.trim().length > 0 ? previous : nextConnectorId
      );
      const pairingsFilter =
        channelRouterPairingsFilterChannel.trim().length > 0
          ? channelRouterPairingsFilterChannel.trim()
          : nextConnectorId;
      if (channelRouterPairingsFilterChannel.trim().length === 0) {
        setChannelRouterPairingsFilterChannel(nextConnectorId);
      }
      await refreshChannelLogs(nextConnectorId);
      await refreshChannelRouter(pairingsFilter);
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setChannelsBusy(false);
    }
  }

  async function loadChannel(connectorId: string): Promise<void> {
    if (connectorId.trim().length === 0) {
      setError("Select a connector first.");
      return;
    }
    setChannelsBusy(true);
    setError(null);
    try {
      const normalizedConnectorId = connectorId.trim();
      setChannelsSelectedConnectorId(normalizedConnectorId);
      const statusResponse = await api.getChannelStatus(normalizedConnectorId);
      setChannelsSelectedStatus(isJsonObject(statusResponse.connector) ? statusResponse.connector : null);
      setChannelRouterPreviewChannel(normalizedConnectorId);
      setChannelRouterMintChannel(normalizedConnectorId);
      const pairingsFilter =
        channelRouterPairingsFilterChannel.trim().length > 0
          ? channelRouterPairingsFilterChannel.trim()
          : normalizedConnectorId;
      if (channelRouterPairingsFilterChannel.trim().length === 0) {
        setChannelRouterPairingsFilterChannel(normalizedConnectorId);
      }
      await refreshChannelLogs(normalizedConnectorId);
      await refreshChannelRouter(pairingsFilter);
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setChannelsBusy(false);
    }
  }

  async function setChannelEnabled(entry: JsonObject, enabled: boolean): Promise<void> {
    const connectorId = readString(entry, "connector_id");
    if (connectorId === null) {
      setError("Connector payload missing connector_id.");
      return;
    }
    setChannelsBusy(true);
    setError(null);
    try {
      const response = await api.setChannelEnabled(connectorId, enabled);
      if (isJsonObject(response.connector)) {
        setChannelsSelectedStatus(response.connector);
      }
      setNotice(`Connector ${enabled ? "enabled" : "disabled"}.`);
      await refreshChannels(connectorId);
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setChannelsBusy(false);
    }
  }

  async function submitChannelTestMessage(event: React.FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    if (channelsSelectedConnectorId.trim().length === 0) {
      setError("Select a connector before sending a test message.");
      return;
    }
    if (channelsTestText.trim().length === 0) {
      setError("Test message text cannot be empty.");
      return;
    }
    setChannelsBusy(true);
    setError(null);
    try {
      const response = await api.sendChannelTestMessage(channelsSelectedConnectorId.trim(), {
        text: channelsTestText.trim(),
        conversation_id: emptyToUndefined(channelsTestConversationId),
        sender_id: emptyToUndefined(channelsTestSenderId),
        sender_display: emptyToUndefined(channelsTestSenderDisplay),
        simulate_crash_once: channelsTestCrashOnce,
        is_direct_message: channelsTestDirectMessage,
        requested_broadcast: channelsTestBroadcast
      });
      if (isJsonObject(response.status)) {
        setChannelsSelectedStatus(response.status);
      }
      if (isJsonObject(response.ingest)) {
        const accepted = response.ingest.accepted === true ? "true" : "false";
        const immediateDeliveryValue = response.ingest.immediate_delivery;
        const immediateDelivery =
          typeof immediateDeliveryValue === "number" || typeof immediateDeliveryValue === "string"
            ? String(immediateDeliveryValue)
            : "0";
        setNotice(
          `Channel test dispatched (accepted=${accepted}, immediate_delivery=${immediateDelivery}).`
        );
      } else {
        setNotice("Channel test dispatched.");
      }
      setChannelsTestCrashOnce(false);
      await refreshChannelLogs(channelsSelectedConnectorId.trim());
      await refreshChannels(channelsSelectedConnectorId.trim());
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setChannelsBusy(false);
    }
  }

  async function submitChannelDiscordTestSend(
    event: React.FormEvent<HTMLFormElement>
  ): Promise<void> {
    event.preventDefault();
    if (channelsSelectedConnectorId.trim().length === 0) {
      setError("Select a connector before dispatching Discord test send.");
      return;
    }
    if (!channelsSelectedConnectorId.trim().startsWith("discord:")) {
      setError("Discord test send is available only for Discord connectors.");
      return;
    }
    if (channelsDiscordTarget.trim().length === 0) {
      setError("Discord test target cannot be empty.");
      return;
    }
    if (!channelsDiscordConfirm) {
      setError("Discord test send requires explicit confirmation.");
      return;
    }

    setChannelsBusy(true);
    setError(null);
    try {
      const response = await api.sendChannelDiscordTestSend(channelsSelectedConnectorId.trim(), {
        target: channelsDiscordTarget.trim(),
        text: emptyToUndefined(channelsDiscordText),
        confirm: true,
        auto_reaction: emptyToUndefined(channelsDiscordAutoReaction),
        thread_id: emptyToUndefined(channelsDiscordThreadId)
      });
      if (isJsonObject(response.status)) {
        setChannelsSelectedStatus(response.status);
      }
      setNotice("Discord test send dispatched.");
      setChannelsDiscordConfirm(false);
      await refreshChannelLogs(channelsSelectedConnectorId.trim());
      await refreshChannels(channelsSelectedConnectorId.trim());
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setChannelsBusy(false);
    }
  }

  async function refreshChannelRouterPairings(): Promise<void> {
    setChannelsBusy(true);
    setError(null);
    try {
      const filterChannel = channelRouterPairingsFilterChannel.trim();
      await refreshChannelRouter(filterChannel.length > 0 ? filterChannel : undefined);
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setChannelsBusy(false);
    }
  }

  async function submitChannelRouterPreview(
    event: React.FormEvent<HTMLFormElement>
  ): Promise<void> {
    event.preventDefault();
    const routeChannel = channelRouterPreviewChannel.trim();
    const text = channelRouterPreviewText.trim();
    if (routeChannel.length === 0) {
      setError("Router preview channel cannot be empty.");
      return;
    }
    if (text.length === 0) {
      setError("Router preview text cannot be empty.");
      return;
    }

    setChannelsBusy(true);
    setError(null);
    try {
      const maxPayloadBytes = parseInteger(channelRouterPreviewMaxPayloadBytes);
      const response = await api.previewChannelRoute({
        channel: routeChannel,
        text,
        conversation_id: emptyToUndefined(channelRouterPreviewConversationId),
        sender_identity: emptyToUndefined(channelRouterPreviewSenderIdentity),
        sender_display: emptyToUndefined(channelRouterPreviewSenderDisplay),
        sender_verified: channelRouterPreviewSenderVerified,
        is_direct_message: channelRouterPreviewIsDirectMessage,
        requested_broadcast: channelRouterPreviewRequestedBroadcast,
        max_payload_bytes: maxPayloadBytes !== null && maxPayloadBytes > 0 ? maxPayloadBytes : undefined
      });
      setChannelRouterPreviewResult(isJsonObject(response.preview) ? response.preview : null);
      if (isJsonObject(response.preview)) {
        const accepted = response.preview.accepted === true ? "accepted" : "rejected";
        const reason = readString(response.preview, "reason") ?? "unknown";
        setNotice(`Route preview ${accepted}: ${reason}.`);
      } else {
        setNotice("Route preview completed.");
      }
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setChannelsBusy(false);
    }
  }

  async function mintChannelRouterPairingCode(
    event: React.FormEvent<HTMLFormElement>
  ): Promise<void> {
    event.preventDefault();
    const routeChannel = channelRouterMintChannel.trim();
    if (routeChannel.length === 0) {
      setError("Pairing code channel cannot be empty.");
      return;
    }

    const parsedTtl = parseInteger(channelRouterMintTtlMs);
    if (parsedTtl !== null && parsedTtl <= 0) {
      setError("Pairing code TTL must be a positive integer.");
      return;
    }

    setChannelsBusy(true);
    setError(null);
    try {
      const response = await api.mintChannelRouterPairingCode({
        channel: routeChannel,
        issued_by: emptyToUndefined(channelRouterMintIssuedBy),
        ttl_ms: parsedTtl !== null ? parsedTtl : undefined
      });
      setChannelRouterMintResult(isJsonObject(response.code) ? response.code : null);
      await refreshChannelRouter(
        channelRouterPairingsFilterChannel.trim().length > 0
          ? channelRouterPairingsFilterChannel.trim()
          : routeChannel
      );
      if (isJsonObject(response.code)) {
        const code = readString(response.code, "code") ?? "(missing)";
        setNotice(`Pairing code minted: ${code}.`);
      } else {
        setNotice("Pairing code minted.");
      }
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setChannelsBusy(false);
    }
  }

  function discordWizardConnectorId(): string | null {
    const normalized = discordWizardAccountId.trim().toLowerCase();
    if (normalized.length === 0) {
      return "discord:default";
    }
    if (!/^[a-z0-9._-]+$/.test(normalized)) {
      return null;
    }
    return `discord:${normalized}`;
  }

  function parseDiscordWizardSenderList(raw: string): string[] {
    const entries: string[] = [];
    for (const candidate of raw.split(",")) {
      const normalized = candidate.trim().toLowerCase();
      if (normalized.length === 0) {
        continue;
      }
      if (!entries.includes(normalized)) {
        entries.push(normalized);
      }
    }
    return entries;
  }

  function parsedDiscordWizardConcurrency(): number {
    const parsed = parseInteger(discordWizardConcurrency);
    if (parsed === null || parsed <= 0) {
      return 2;
    }
    return Math.min(Math.max(parsed, 1), 32);
  }

  function parseDiscordWizardVerifyChannelId(): { value?: string; error?: string } {
    const normalized = discordWizardVerifyChannelId.trim();
    if (normalized.length === 0) {
      return {};
    }
    if (!/^[0-9]+$/.test(normalized)) {
      return { error: "Verify channel ID must contain decimal digits only." };
    }
    if (normalized.length < 16 || normalized.length > 24) {
      return { error: "Verify channel ID must be a canonical Discord snowflake (16-24 digits)." };
    }
    return { value: normalized };
  }

  function buildDiscordWizardPayload(verifyChannelId?: string): {
    account_id?: string;
    token: string;
    mode: "local" | "remote_vps";
    inbound_scope: "dm_only" | "allowlisted_guild_channels" | "open_guild_channels";
    allow_from: string[];
    deny_from: string[];
    require_mention: boolean;
    concurrency_limit: number;
    broadcast_strategy: "deny" | "mention_only" | "allow";
    confirm_open_guild_channels: boolean;
    verify_channel_id?: string;
  } {
    const normalized = discordWizardAccountId.trim().toLowerCase();
    return {
      account_id: normalized.length > 0 ? normalized : undefined,
      token: discordWizardToken.trim(),
      mode: discordWizardMode,
      inbound_scope: discordWizardScope,
      allow_from: parseDiscordWizardSenderList(discordWizardAllowFrom),
      deny_from: parseDiscordWizardSenderList(discordWizardDenyFrom),
      require_mention: discordWizardRequireMention,
      concurrency_limit: parsedDiscordWizardConcurrency(),
      broadcast_strategy: discordWizardBroadcast,
      confirm_open_guild_channels: discordWizardConfirmOpen,
      verify_channel_id: verifyChannelId
    };
  }

  async function runDiscordOnboardingProbe(): Promise<void> {
    if (discordWizardToken.trim().length === 0) {
      setError("Discord onboarding token cannot be empty.");
      return;
    }
    const verifyChannel = parseDiscordWizardVerifyChannelId();
    if (verifyChannel.error !== undefined) {
      setError(verifyChannel.error);
      return;
    }
    const connectorId = discordWizardConnectorId();
    if (connectorId === null) {
      setError("Discord account ID contains unsupported characters.");
      return;
    }
    setDiscordWizardBusy(true);
    setError(null);
    try {
      const response = await api.probeDiscordOnboarding(buildDiscordWizardPayload(verifyChannel.value));
      setDiscordWizardPreflight(isJsonObject(response) ? response : null);
      const botId = isJsonObject(response.bot) ? readString(response.bot, "id") : null;
      const botUsername = isJsonObject(response.bot) ? readString(response.bot, "username") : null;
      setNotice(
        botId !== null && botUsername !== null
          ? `Discord preflight OK for ${botUsername} (${botId}).`
          : "Discord preflight completed."
      );
      await refreshChannels(connectorId);
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setDiscordWizardBusy(false);
    }
  }

  async function applyDiscordOnboarding(): Promise<void> {
    if (discordWizardToken.trim().length === 0) {
      setError("Discord onboarding token cannot be empty.");
      return;
    }
    const verifyChannel = parseDiscordWizardVerifyChannelId();
    if (verifyChannel.error !== undefined) {
      setError(verifyChannel.error);
      return;
    }
    const connectorId = discordWizardConnectorId();
    if (connectorId === null) {
      setError("Discord account ID contains unsupported characters.");
      return;
    }
    if (discordWizardScope === "open_guild_channels" && !discordWizardConfirmOpen) {
      setError("Open guild channels require explicit confirmation.");
      return;
    }

    setDiscordWizardBusy(true);
    setError(null);
    try {
      const response = await api.applyDiscordOnboarding(buildDiscordWizardPayload(verifyChannel.value));
      setDiscordWizardApply(isJsonObject(response) ? response : null);
      const preflight = isJsonObject(response.preflight) ? response.preflight : null;
      const bot = preflight !== null && isJsonObject(preflight.bot) ? preflight.bot : null;
      const botId = bot !== null ? readString(bot, "id") : null;
      const botUsername = bot !== null ? readString(bot, "username") : null;
      setNotice(
        botId !== null && botUsername !== null
          ? `Discord onboarding applied for ${botUsername} (${botId}).`
          : "Discord onboarding applied."
      );
      setDiscordWizardToken("");
      await refreshChannels(connectorId);
      await loadChannel(connectorId);
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setDiscordWizardBusy(false);
    }
  }

  async function verifyDiscordOnboardingTarget(): Promise<void> {
    const connectorId = discordWizardConnectorId();
    if (connectorId === null) {
      setError("Discord account ID contains unsupported characters.");
      return;
    }
    if (discordWizardVerifyTarget.trim().length === 0) {
      setError("Verification target cannot be empty.");
      return;
    }
    if (!discordWizardVerifyConfirm) {
      setError("Verification send requires explicit confirmation.");
      return;
    }
    setDiscordWizardBusy(true);
    setError(null);
    try {
      const response = await api.sendChannelDiscordTestSend(connectorId, {
        target: discordWizardVerifyTarget.trim(),
        text: emptyToUndefined(discordWizardVerifyText),
        confirm: true
      });
      const dispatch = isJsonObject(response.dispatch) ? response.dispatch : null;
      const delivered = dispatch !== null ? readString(dispatch, "delivered") : null;
      setNotice(
        delivered !== null
          ? `Discord verification dispatched (delivered=${delivered}).`
          : "Discord verification dispatched."
      );
      setDiscordWizardVerifyConfirm(false);
      await refreshChannels(connectorId);
      await refreshChannelLogs(connectorId);
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setDiscordWizardBusy(false);
    }
  }

  async function searchMemory(event: React.FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    if (memoryQuery.trim().length === 0) {
      setError("Memory query cannot be empty.");
      return;
    }
    setMemoryBusy(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      params.set("query", memoryQuery.trim());
      if (memoryChannel.trim().length > 0) {
        params.set("channel", memoryChannel.trim());
      }
      const response = await api.searchMemory(params);
      setMemoryHits(toJsonObjectArray(response.hits));
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setMemoryBusy(false);
    }
  }

  async function refreshMemoryStatus(): Promise<void> {
    setMemoryStatusBusy(true);
    setError(null);
    try {
      const response = await api.getMemoryStatus();
      setMemoryStatus({
        usage: response.usage,
        retention: response.retention,
        maintenance: response.maintenance
      });
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setMemoryStatusBusy(false);
    }
  }

  async function purgeMemory(): Promise<void> {
    setMemoryBusy(true);
    setError(null);
    try {
      const response = await api.purgeMemory({
        channel: emptyToUndefined(memoryPurgeChannel),
        session_id: emptyToUndefined(memoryPurgeSessionId),
        purge_all_principal: memoryPurgeAll
      });
      setNotice(`Purged ${response.deleted_count} memory item(s).`);
      setMemoryHits([]);
      await refreshMemoryStatus();
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setMemoryBusy(false);
    }
  }

  async function refreshSkills(): Promise<void> {
    setSkillsBusy(true);
    setError(null);
    try {
      const response = await api.listSkills();
      setSkillsEntries(toJsonObjectArray(response.entries));
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setSkillsBusy(false);
    }
  }

  async function installSkill(event: React.FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    if (skillArtifactPath.trim().length === 0) {
      setError("Artifact path cannot be empty.");
      return;
    }
    setSkillsBusy(true);
    setError(null);
    try {
      await api.installSkill({
        artifact_path: skillArtifactPath.trim(),
        allow_tofu: skillAllowTofu,
        allow_untrusted: skillAllowUntrusted
      });
      setSkillArtifactPath("");
      setNotice("Skill installed.");
      await refreshSkills();
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setSkillsBusy(false);
    }
  }

  async function executeSkillAction(entry: JsonObject, action: "verify" | "audit" | "quarantine" | "enable"): Promise<void> {
    const metadata = skillMetadata(entry);
    if (metadata === null) {
      setError("Skill entry is missing record metadata.");
      return;
    }

    setSkillsBusy(true);
    setError(null);
    try {
      if (action === "verify") {
        await api.verifySkill(metadata.skillId, { version: metadata.version, allow_tofu: false });
      }
      if (action === "audit") {
        await api.auditSkill(metadata.skillId, {
          version: metadata.version,
          allow_tofu: false,
          quarantine_on_fail: true
        });
      }
      if (action === "quarantine") {
        await api.quarantineSkill({
          skill_id: metadata.skillId,
          version: metadata.version,
          reason: emptyToUndefined(skillReason)
        });
      }
      if (action === "enable") {
        await api.enableSkill({
          skill_id: metadata.skillId,
          version: metadata.version,
          reason: emptyToUndefined(skillReason)
        });
      }
      setNotice(`Skill action '${action}' completed.`);
      await refreshSkills();
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setSkillsBusy(false);
    }
  }

  async function refreshBrowserProfiles(): Promise<void> {
    setBrowserBusy(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      if (browserPrincipal.trim().length > 0) {
        params.set("principal", browserPrincipal.trim());
      }
      const response = await api.listBrowserProfiles(params);
      setBrowserProfiles(toJsonObjectArray(response.profiles));
      setBrowserActiveProfileId(response.active_profile_id ?? "");
      setBrowserPrincipal((previous) => {
        if (previous.trim().length > 0) {
          return previous;
        }
        return response.principal;
      });
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setBrowserBusy(false);
    }
  }

  async function createBrowserProfile(event: React.FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    if (browserProfileName.trim().length === 0) {
      setError("Profile name cannot be empty.");
      return;
    }
    setBrowserBusy(true);
    setError(null);
    try {
      await api.createBrowserProfile({
        principal: emptyToUndefined(browserPrincipal),
        name: browserProfileName.trim(),
        theme_color: emptyToUndefined(browserProfileTheme),
        persistence_enabled: browserProfilePersistence,
        private_profile: browserProfilePrivate
      });
      setBrowserProfileName("");
      setBrowserProfileTheme("");
      setBrowserProfilePrivate(false);
      setNotice("Browser profile created.");
      await refreshBrowserProfiles();
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setBrowserBusy(false);
    }
  }

  async function renameBrowserProfile(): Promise<void> {
    if (browserRenameProfileId.trim().length === 0) {
      setError("Select a browser profile to rename.");
      return;
    }
    if (browserRenameName.trim().length === 0) {
      setError("New profile name cannot be empty.");
      return;
    }
    setBrowserBusy(true);
    setError(null);
    try {
      await api.renameBrowserProfile(browserRenameProfileId.trim(), {
        principal: emptyToUndefined(browserPrincipal),
        name: browserRenameName.trim()
      });
      setNotice("Browser profile renamed.");
      setBrowserRenameName("");
      await refreshBrowserProfiles();
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setBrowserBusy(false);
    }
  }

  async function activateBrowserProfile(profile: JsonObject): Promise<void> {
    const profileId = readString(profile, "profile_id");
    if (profileId === null) {
      setError("Profile payload missing profile_id.");
      return;
    }
    setBrowserBusy(true);
    setError(null);
    try {
      await api.activateBrowserProfile(profileId, {
        principal: emptyToUndefined(browserPrincipal)
      });
      setNotice("Browser profile activated.");
      await refreshBrowserProfiles();
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setBrowserBusy(false);
    }
  }

  async function deleteBrowserProfile(profile: JsonObject): Promise<void> {
    const profileId = readString(profile, "profile_id");
    if (profileId === null) {
      setError("Profile payload missing profile_id.");
      return;
    }
    setBrowserBusy(true);
    setError(null);
    try {
      await api.deleteBrowserProfile(profileId, {
        principal: emptyToUndefined(browserPrincipal)
      });
      setNotice("Browser profile deleted.");
      await refreshBrowserProfiles();
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setBrowserBusy(false);
    }
  }

  async function mintBrowserRelayToken(): Promise<void> {
    if (browserRelaySessionId.trim().length === 0) {
      setError("Relay token issuance requires session_id.");
      return;
    }
    if (browserRelayExtensionId.trim().length === 0) {
      setError("Relay token issuance requires extension_id.");
      return;
    }
    setBrowserBusy(true);
    setError(null);
    try {
      const response = await api.mintBrowserRelayToken({
        session_id: browserRelaySessionId.trim(),
        extension_id: browserRelayExtensionId.trim(),
        ttl_ms: parseInteger(browserRelayTtlMs) ?? undefined
      });
      setBrowserRelayToken(response.relay_token);
      setBrowserRelayTokenExpiry(response.expires_at_unix_ms);
      setNotice("Browser relay token minted. Keep it private and short-lived.");
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setBrowserBusy(false);
    }
  }

  async function dispatchBrowserRelayAction(): Promise<void> {
    if (browserRelaySessionId.trim().length === 0) {
      setError("Relay action requires session_id.");
      return;
    }
    if (browserRelayExtensionId.trim().length === 0) {
      setError("Relay action requires extension_id.");
      return;
    }
    if (browserRelayAction === "open_tab" && browserRelayOpenTabUrl.trim().length === 0) {
      setError("Open tab relay action requires URL.");
      return;
    }
    if (browserRelayAction === "capture_selection" && browserRelaySelector.trim().length === 0) {
      setError("Capture selection relay action requires selector.");
      return;
    }
    setBrowserBusy(true);
    setError(null);
    try {
      const payload: {
        relay_token?: string;
        session_id: string;
        extension_id: string;
        action: "open_tab" | "capture_selection" | "send_page_snapshot";
        open_tab?: { url: string; activate?: boolean; timeout_ms?: number };
        capture_selection?: { selector: string; max_selection_bytes?: number };
        page_snapshot?: {
          include_dom_snapshot?: boolean;
          include_visible_text?: boolean;
          max_dom_snapshot_bytes?: number;
          max_visible_text_bytes?: number;
        };
        max_payload_bytes?: number;
      } = {
        relay_token: emptyToUndefined(browserRelayToken),
        session_id: browserRelaySessionId.trim(),
        extension_id: browserRelayExtensionId.trim(),
        action: browserRelayAction,
        max_payload_bytes: 16384
      };

      if (browserRelayAction === "open_tab") {
        payload.open_tab = {
          url: browserRelayOpenTabUrl.trim(),
          activate: true,
          timeout_ms: 6000
        };
      }
      if (browserRelayAction === "capture_selection") {
        payload.capture_selection = {
          selector: browserRelaySelector.trim(),
          max_selection_bytes: 2048
        };
      }
      if (browserRelayAction === "send_page_snapshot") {
        payload.page_snapshot = {
          include_dom_snapshot: true,
          include_visible_text: true,
          max_dom_snapshot_bytes: 4096,
          max_visible_text_bytes: 4096
        };
      }
      const response = await api.relayBrowserAction(payload, browserRelayToken);
      setBrowserRelayResult(response as JsonObject);
      if (response.success) {
        setNotice(`Relay action '${response.action}' completed.`);
      } else {
        setError(response.error.length > 0 ? response.error : "Relay action failed.");
      }
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setBrowserBusy(false);
    }
  }

  async function refreshBrowserDownloads(): Promise<void> {
    if (browserDownloadsSessionId.trim().length === 0) {
      setError("Downloads query requires session_id.");
      return;
    }
    setBrowserBusy(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      params.set("session_id", browserDownloadsSessionId.trim());
      params.set("limit", "50");
      if (browserDownloadsQuarantinedOnly) {
        params.set("quarantined_only", "true");
      }
      const response = await api.listBrowserDownloads(params);
      setBrowserDownloads(toJsonObjectArray(response.artifacts));
      if (response.error.length > 0) {
        setNotice(`Downloads listed with note: ${response.error}`);
      } else {
        setNotice("Downloads refreshed.");
      }
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setBrowserBusy(false);
    }
  }

  async function refreshAudit(): Promise<void> {
    setAuditBusy(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      if (auditFilterContains.trim().length > 0) {
        params.set("contains", auditFilterContains.trim());
      }
      if (auditFilterPrincipal.trim().length > 0) {
        params.set("principal", auditFilterPrincipal.trim());
      }
      const response = await api.listAuditEvents(params);
      setAuditEvents(toJsonObjectArray(response.events));
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setAuditBusy(false);
    }
  }

  async function refreshDiagnostics(): Promise<void> {
    setDiagnosticsBusy(true);
    setError(null);
    try {
      const response = await api.getDiagnostics();
      setDiagnosticsSnapshot(response as unknown as JsonObject);
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setDiagnosticsBusy(false);
    }
  }

  return {
    api,
    booting,
    session,
    section,
    setSection,
    theme,
    setTheme,
    revealSensitiveValues,
    setRevealSensitiveValues,
    error,
    setError,
    notice,
    setNotice,
    loginBusy,
    logoutBusy,
    loginForm,
    setLoginForm,
    signIn,
    signOut,
    approvalsBusy,
    approvals,
    approvalId,
    setApprovalId,
    approvalReason,
    setApprovalReason,
    approvalScope,
    setApprovalScope,
    refreshApprovals,
    decideApproval,
    cronBusy,
    cronJobs,
    cronRuns,
    cronJobId,
    setCronJobId,
    cronForm,
    setCronForm,
    refreshCron,
    createCron: createCronJob,
    toggleCron: setCronEnabled,
    runCronNow,
    refreshCronRuns,
    channelsBusy,
    channelsConnectors,
    channelsSelectedConnectorId,
    setChannelsSelectedConnectorId,
    channelsSelectedStatus,
    channelsEvents,
    channelsDeadLetters,
    channelsLogsLimit,
    setChannelsLogsLimit,
    channelsTestText,
    setChannelsTestText,
    channelsTestConversationId,
    setChannelsTestConversationId,
    channelsTestSenderId,
    setChannelsTestSenderId,
    channelsTestSenderDisplay,
    setChannelsTestSenderDisplay,
    channelsTestCrashOnce,
    setChannelsTestCrashOnce,
    channelsTestDirectMessage,
    setChannelsTestDirectMessage,
    channelsTestBroadcast,
    setChannelsTestBroadcast,
    channelsDiscordTarget,
    setChannelsDiscordTarget,
    channelsDiscordText,
    setChannelsDiscordText,
    channelsDiscordAutoReaction,
    setChannelsDiscordAutoReaction,
    channelsDiscordThreadId,
    setChannelsDiscordThreadId,
    channelsDiscordConfirm,
    setChannelsDiscordConfirm,
    channelRouterRules,
    channelRouterConfigHash,
    channelRouterWarnings,
    channelRouterPreviewChannel,
    setChannelRouterPreviewChannel,
    channelRouterPreviewText,
    setChannelRouterPreviewText,
    channelRouterPreviewConversationId,
    setChannelRouterPreviewConversationId,
    channelRouterPreviewSenderIdentity,
    setChannelRouterPreviewSenderIdentity,
    channelRouterPreviewSenderDisplay,
    setChannelRouterPreviewSenderDisplay,
    channelRouterPreviewSenderVerified,
    setChannelRouterPreviewSenderVerified,
    channelRouterPreviewIsDirectMessage,
    setChannelRouterPreviewIsDirectMessage,
    channelRouterPreviewRequestedBroadcast,
    setChannelRouterPreviewRequestedBroadcast,
    channelRouterPreviewMaxPayloadBytes,
    setChannelRouterPreviewMaxPayloadBytes,
    channelRouterPreviewResult,
    channelRouterPairingsFilterChannel,
    setChannelRouterPairingsFilterChannel,
    channelRouterPairings,
    channelRouterMintChannel,
    setChannelRouterMintChannel,
    channelRouterMintIssuedBy,
    setChannelRouterMintIssuedBy,
    channelRouterMintTtlMs,
    setChannelRouterMintTtlMs,
    channelRouterMintResult,
    discordWizardBusy,
    discordWizardAccountId,
    setDiscordWizardAccountId,
    discordWizardMode,
    setDiscordWizardMode,
    discordWizardToken,
    setDiscordWizardToken,
    discordWizardScope,
    setDiscordWizardScope,
    discordWizardAllowFrom,
    setDiscordWizardAllowFrom,
    discordWizardDenyFrom,
    setDiscordWizardDenyFrom,
    discordWizardRequireMention,
    setDiscordWizardRequireMention,
    discordWizardBroadcast,
    setDiscordWizardBroadcast,
    discordWizardConcurrency,
    setDiscordWizardConcurrency,
    discordWizardConfirmOpen,
    setDiscordWizardConfirmOpen,
    discordWizardVerifyChannelId,
    setDiscordWizardVerifyChannelId,
    discordWizardPreflight,
    discordWizardApply,
    discordWizardVerifyTarget,
    setDiscordWizardVerifyTarget,
    discordWizardVerifyText,
    setDiscordWizardVerifyText,
    discordWizardVerifyConfirm,
    setDiscordWizardVerifyConfirm,
    refreshChannels,
    selectChannelConnector: loadChannel,
    toggleConnector: setChannelEnabled,
    previewChannelRouter: submitChannelRouterPreview,
    refreshChannelRouterPairings,
    mintChannelRouterPairingCode,
    sendChannelTest: submitChannelTestMessage,
    sendDiscordTest: submitChannelDiscordTestSend,
    runDiscordPreflight: runDiscordOnboardingProbe,
    applyDiscordOnboarding,
    runDiscordVerification: verifyDiscordOnboardingTarget,
    memoryBusy,
    memoryQuery,
    setMemoryQuery,
    memoryChannel,
    setMemoryChannel,
    memoryPurgeChannel,
    setMemoryPurgeChannel,
    memoryPurgeSessionId,
    setMemoryPurgeSessionId,
    memoryPurgeAll,
    setMemoryPurgeAll,
    memoryHits,
    memoryStatusBusy,
    memoryStatus,
    refreshMemoryStatus,
    searchMemory,
    purgeMemory,
    skillsBusy,
    skillsEntries,
    skillArtifactPath,
    setSkillArtifactPath,
    skillAllowTofu,
    setSkillAllowTofu,
    skillAllowUntrusted,
    setSkillAllowUntrusted,
    skillReason,
    setSkillReason,
    refreshSkills,
    installSkill,
    executeSkillAction,
    browserBusy,
    browserPrincipal,
    setBrowserPrincipal,
    browserProfiles,
    browserActiveProfileId,
    browserProfileName,
    setBrowserProfileName,
    browserProfileTheme,
    setBrowserProfileTheme,
    browserProfilePersistence,
    setBrowserProfilePersistence,
    browserProfilePrivate,
    setBrowserProfilePrivate,
    browserRenameProfileId,
    setBrowserRenameProfileId,
    browserRenameName,
    setBrowserRenameName,
    browserRelaySessionId,
    setBrowserRelaySessionId,
    browserRelayExtensionId,
    setBrowserRelayExtensionId,
    browserRelayTtlMs,
    setBrowserRelayTtlMs,
    browserRelayToken,
    browserRelayTokenExpiry,
    browserRelayAction,
    setBrowserRelayAction,
    browserRelayOpenTabUrl,
    setBrowserRelayOpenTabUrl,
    browserRelaySelector,
    setBrowserRelaySelector,
    browserRelayResult,
    browserDownloadsSessionId,
    setBrowserDownloadsSessionId,
    browserDownloadsQuarantinedOnly,
    setBrowserDownloadsQuarantinedOnly,
    browserDownloads,
    refreshBrowserProfiles,
    createBrowserProfile,
    activateBrowserProfile,
    deleteBrowserProfile,
    renameBrowserProfile,
    mintBrowserRelayToken,
    dispatchBrowserRelayAction,
    refreshBrowserDownloads,
    auditBusy,
    auditFilterContains,
    setAuditFilterContains,
    auditFilterPrincipal,
    setAuditFilterPrincipal,
    auditEvents,
    refreshAudit,
    diagnosticsBusy,
    diagnosticsSnapshot,
    refreshDiagnostics,
    overviewBusy,
    overviewCatalog,
    overviewDeployment,
    overviewSupportJobs,
    refreshOverview,
    authBusy,
    authProfiles,
    authHealth,
    authProviderState,
    authDefaultProfileId,
    setAuthDefaultProfileId,
    authBootstrapProfileId,
    setAuthBootstrapProfileId,
    refreshAuth,
    executeOpenAiAction,
    configBusy,
    configInspectPath,
    setConfigInspectPath,
    configInspectSnapshot,
    configMutationKey,
    setConfigMutationKey,
    configMutationValue,
    setConfigMutationValue,
    configValidation,
    configSecretsScope,
    setConfigSecretsScope,
    configSecrets,
    configSecretKey,
    setConfigSecretKey,
    configSecretValue,
    setConfigSecretValue,
    configSecretReveal,
    refreshConfigSurface,
    inspectConfigSurface,
    validateConfigSurface,
    mutateConfigSurface,
    refreshSecrets,
    setSecretValue,
    revealSecretValue,
    deleteSecretValue,
    supportBusy,
    supportPairingSummary,
    supportPairingChannel,
    setSupportPairingChannel,
    supportPairingIssuedBy,
    setSupportPairingIssuedBy,
    supportPairingTtlMs,
    setSupportPairingTtlMs,
    supportBundleJobs,
    refreshSupport,
    mintSupportPairingCode,
    createSupportBundle
  };
}

export type ConsoleAppState = ReturnType<typeof useConsoleAppState>;
