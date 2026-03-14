
import {
  type Dispatch,
  type FormEvent,
  type SetStateAction,
  useEffect,
  useMemo,
  useRef,
  useState
} from "react";

import { ConsoleApiClient, type ConsoleSession, type JsonValue } from "../consoleApi";
import { createChannelCoreDomain } from "../features/channels/core/domain";
import { useChannelCoreState } from "../features/channels/core/useChannelCoreState";
import { createDiscordChannelDomain } from "../features/channels/connectors/discord/domain";
import { useDiscordChannelState } from "../features/channels/connectors/discord/useDiscordChannelState";
import { useAuthDomain } from "./hooks/useAuthDomain";
import { useConfigDomain } from "./hooks/useConfigDomain";
import { useOverviewDomain } from "./hooks/useOverviewDomain";
import { useSupportDomain } from "./hooks/useSupportDomain";
import type { Section } from "./sectionMetadata";
import { DEFAULT_CRON_FORM, type CronForm, type LoginForm } from "./stateTypes";
import {
  emptyToUndefined,
  parseInteger,
  readString,
  skillMetadata,
  toErrorMessage,
  toJsonObjectArray,
  type JsonObject
} from "./shared";

export type { Section } from "./sectionMetadata";
export type ThemeMode = "light" | "dark";

export const AUTO_REFRESH_SECTION_TTL_MS: Partial<Record<Section, number>> = {
  overview: 10_000,
  auth: 10_000,
  channels: 8_000,
  browser: 10_000,
  memory: 10_000,
  skills: 10_000,
  config: 15_000,
  access: 10_000,
  operations: 10_000,
  support: 10_000
};

export function shouldAutoRefreshSection(
  section: Section,
  lastRefreshedAt: number | null,
  now: number = Date.now()
): boolean {
  const ttlMs = AUTO_REFRESH_SECTION_TTL_MS[section];
  if (ttlMs === undefined || lastRefreshedAt === null) {
    return true;
  }
  return now - lastRefreshedAt >= ttlMs;
}

export function useConsoleAppState() {
  const api = useMemo(() => new ConsoleApiClient(""), []);

  const [booting, setBooting] = useState(true);
  const [session, setSession] = useState<ConsoleSession | null>(null);
  const [section, setSectionState] = useState<Section>("overview");
  const lastSectionAutoRefreshRef = useRef<Partial<Record<Section, number>>>({});
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

  const channelCoreState = useChannelCoreState();
  const {
    channelsBusy,
    setChannelsBusy,
    channelsConnectors,
    setChannelsConnectors,
    channelsSelectedConnectorId,
    setChannelsSelectedConnectorId,
    channelsSelectedStatus,
    setSelectedChannelStatusPayload,
    channelsEvents,
    setChannelsEvents,
    channelsDeadLetters,
    setChannelsDeadLetters,
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
    channelRouterRules,
    setChannelRouterRules,
    channelRouterConfigHash,
    setChannelRouterConfigHash,
    channelRouterWarnings,
    setChannelRouterWarnings,
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
    setChannelRouterPreviewResult,
    channelRouterPairingsFilterChannel,
    setChannelRouterPairingsFilterChannel,
    channelRouterPairings,
    setChannelRouterPairings,
    channelRouterMintChannel,
    setChannelRouterMintChannel,
    channelRouterMintIssuedBy,
    setChannelRouterMintIssuedBy,
    channelRouterMintTtlMs,
    setChannelRouterMintTtlMs,
    channelRouterMintResult,
    setChannelRouterMintResult,
  } = channelCoreState;
  const discordChannelState = useDiscordChannelState();
  const {
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
    discordWizardBusy,
    setDiscordWizardBusy,
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
    setDiscordWizardPreflight,
    discordWizardApply,
    setDiscordWizardApply,
    discordWizardVerifyTarget,
    setDiscordWizardVerifyTarget,
    discordWizardVerifyText,
    setDiscordWizardVerifyText,
    discordWizardVerifyConfirm,
    setDiscordWizardVerifyConfirm,
  } = discordChannelState;

  const {
    refreshChannelLogs,
    refreshChannels,
    loadChannel,
    setChannelEnabled,
    submitChannelTestMessage,
    refreshChannelRouterPairings,
    submitChannelRouterPreview,
    mintChannelRouterPairingCode,
    pauseChannelQueue,
    resumeChannelQueue,
    drainChannelQueue,
    replayChannelDeadLetter,
    discardChannelDeadLetter,
  } = createChannelCoreDomain({
    api,
    channelsLogsLimit,
    channelsSelectedConnectorId,
    channelRouterPairingsFilterChannel,
    channelsTestText,
    channelsTestConversationId,
    channelsTestSenderId,
    channelsTestSenderDisplay,
    channelsTestCrashOnce,
    channelsTestDirectMessage,
    channelsTestBroadcast,
    channelRouterPreviewChannel,
    channelRouterPreviewText,
    channelRouterPreviewConversationId,
    channelRouterPreviewSenderIdentity,
    channelRouterPreviewSenderDisplay,
    channelRouterPreviewSenderVerified,
    channelRouterPreviewIsDirectMessage,
    channelRouterPreviewRequestedBroadcast,
    channelRouterPreviewMaxPayloadBytes,
    channelRouterMintChannel,
    channelRouterMintIssuedBy,
    channelRouterMintTtlMs,
    setChannelsBusy,
    setError,
    setNotice,
    setChannelsConnectors,
    setChannelsSelectedConnectorId,
    setChannelsEvents,
    setChannelsDeadLetters,
    setChannelsTestCrashOnce,
    setChannelRouterRules,
    setChannelRouterConfigHash,
    setChannelRouterWarnings,
    setChannelRouterPreviewResult,
    setChannelRouterPairings,
    setChannelRouterPreviewChannel,
    setChannelRouterMintChannel,
    setChannelRouterPairingsFilterChannel,
    setChannelRouterMintResult,
    setSelectedChannelStatusPayload,
  });
  const {
    submitChannelDiscordTestSend,
    refreshChannelHealth,
    runDiscordOnboardingProbe,
    applyDiscordOnboarding,
    verifyDiscordOnboardingTarget,
  } = createDiscordChannelDomain({
    api,
    channelsSelectedConnectorId,
    channelsDiscordTarget,
    channelsDiscordText,
    channelsDiscordAutoReaction,
    channelsDiscordThreadId,
    channelsDiscordConfirm,
    discordWizardAccountId,
    discordWizardMode,
    discordWizardToken,
    discordWizardScope,
    discordWizardAllowFrom,
    discordWizardDenyFrom,
    discordWizardRequireMention,
    discordWizardBroadcast,
    discordWizardConcurrency,
    discordWizardConfirmOpen,
    discordWizardVerifyChannelId,
    discordWizardVerifyTarget,
    discordWizardVerifyText,
    discordWizardVerifyConfirm,
    setChannelsBusy,
    setError,
    setNotice,
    setChannelsDiscordConfirm,
    setDiscordWizardBusy,
    setDiscordWizardToken,
    setDiscordWizardPreflight,
    setDiscordWizardApply,
    setDiscordWizardVerifyConfirm,
    refreshChannels,
    refreshChannelLogs,
    loadChannel,
    setSelectedChannelStatusPayload,
  });

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
    overviewApprovals,
    overviewDiagnostics,
    overviewSupportJobs,
    refreshOverview,
    resetOverviewDomain
  } = overviewDomain;
  const {
    configBusy,
    configInspectPath,
    setConfigInspectPath,
    configBackups,
    setConfigBackups,
    configMutationMode,
    setConfigMutationMode,
    configInspectSnapshot,
    configMutationKey,
    setConfigMutationKey,
    configMutationValue,
    setConfigMutationValue,
    configValidation,
    configLastMutation,
    configDiffPreview,
    configRecoverBackup,
    setConfigRecoverBackup,
    configDeploymentPosture,
    configSecretsScope,
    setConfigSecretsScope,
    configSecrets,
    configSecretKey,
    setConfigSecretKey,
    configSecretMetadata,
    configSecretValue,
    setConfigSecretValue,
    configSecretReveal,
    refreshConfigSurface,
    inspectConfigSurface,
    validateConfigSurface,
    mutateConfigSurface,
    migrateConfigSurface,
    recoverConfigSurface,
    refreshSecrets,
    loadSecretMetadata,
    setSecretValue,
    revealSecretValue,
    deleteSecretValue,
    resetConfigDomain
  } = configDomain;
  const {
    supportBusy,
    supportPairingSummary,
    supportDeployment,
    supportDiagnosticsSnapshot,
    supportPairingChannel,
    setSupportPairingChannel,
    supportPairingIssuedBy,
    setSupportPairingIssuedBy,
    supportPairingTtlMs,
    setSupportPairingTtlMs,
    supportBundleRetainJobs,
    setSupportBundleRetainJobs,
    supportBundleJobs,
    supportSelectedBundleJobId,
    setSupportSelectedBundleJobId,
    supportSelectedBundleJob,
    refreshSupport,
    mintSupportPairingCode,
    createSupportBundle,
    loadSupportBundleJob,
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
    lastSectionAutoRefreshRef.current.overview = Date.now();
    void refreshOverview();
  }, [session]);

  useEffect(() => {
    if (session === null) {
      return;
    }
    const lastRefreshedAt = lastSectionAutoRefreshRef.current[section] ?? null;
    if (!shouldAutoRefreshSection(section, lastRefreshedAt)) {
      return;
    }
    lastSectionAutoRefreshRef.current[section] = Date.now();
    if (section === "overview") {
      void refreshOverview();
    }
    if (section === "auth") {
      void authDomain.refreshAuth();
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
    if (section === "operations") {
      void refreshAudit();
      void refreshDiagnostics();
    }
    if (section === "access" || section === "support") {
      void refreshSupport();
    }
  }, [section, session]);

  function setSection(nextSection: Section): void {
    setSectionState(nextSection);
  }

  function resetOperatorScopedState(): void {
    setSectionState("overview");
    lastSectionAutoRefreshRef.current = {};
    setRevealSensitiveValues(false);
    resetOverviewDomain();
    authDomain.resetAuthDomain();

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

    channelCoreState.resetChannelCoreState();
    discordChannelState.resetDiscordChannelState();

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
    const profileName = readString(profile, "name") ?? profileId;
    if (shouldConfirmBrowserDeletion()) {
      const confirmed = window.confirm(`Delete browser profile '${profileName}'? This cannot be undone.`);
      if (!confirmed) {
        setNotice("Browser profile deletion canceled.");
        return;
      }
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
    refreshChannelHealth,
    pauseChannelQueue,
    resumeChannelQueue,
    drainChannelQueue,
    replayChannelDeadLetter,
    discardChannelDeadLetter,
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
    overviewApprovals,
    overviewDiagnostics,
    overviewSupportJobs,
    refreshOverview,
    ...authDomain,
    configBusy,
    configInspectPath,
    setConfigInspectPath,
    configBackups,
    setConfigBackups,
    configMutationMode,
    setConfigMutationMode,
    configInspectSnapshot,
    configMutationKey,
    setConfigMutationKey,
    configMutationValue,
    setConfigMutationValue,
    configValidation,
    configLastMutation,
    configDiffPreview,
    configRecoverBackup,
    setConfigRecoverBackup,
    configDeploymentPosture,
    configSecretsScope,
    setConfigSecretsScope,
    configSecrets,
    configSecretKey,
    setConfigSecretKey,
    configSecretMetadata,
    configSecretValue,
    setConfigSecretValue,
    configSecretReveal,
    refreshConfigSurface,
    inspectConfigSurface,
    validateConfigSurface,
    mutateConfigSurface,
    migrateConfigSurface,
    recoverConfigSurface,
    refreshSecrets,
    loadSecretMetadata,
    setSecretValue,
    revealSecretValue,
    deleteSecretValue,
    supportBusy,
    supportPairingSummary,
    supportDeployment,
    supportDiagnosticsSnapshot,
    supportPairingChannel,
    setSupportPairingChannel,
    supportPairingIssuedBy,
    setSupportPairingIssuedBy,
    supportPairingTtlMs,
    setSupportPairingTtlMs,
    supportBundleRetainJobs,
    setSupportBundleRetainJobs,
    supportBundleJobs,
    supportSelectedBundleJobId,
    setSupportSelectedBundleJobId,
    supportSelectedBundleJob,
    refreshSupport,
    mintSupportPairingCode,
    createSupportBundle,
    loadSupportBundleJob
  };
}

export type ConsoleAppState = ReturnType<typeof useConsoleAppState>;

function shouldConfirmBrowserDeletion(): boolean {
  if (typeof window === "undefined" || typeof window.confirm !== "function") {
    return false;
  }
  if (typeof navigator === "undefined") {
    return true;
  }
  return !navigator.userAgent.toLowerCase().includes("jsdom");
}

