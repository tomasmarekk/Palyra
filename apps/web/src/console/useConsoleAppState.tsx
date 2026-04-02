import {
  type Dispatch,
  type FormEvent,
  type SetStateAction,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";

import {
  ConsoleApiClient,
  ControlPlaneApiError,
  type ConsoleSession,
  type JsonValue,
} from "../consoleApi";
import { createChannelCoreDomain } from "../features/channels/core/domain";
import { useChannelCoreState } from "../features/channels/core/useChannelCoreState";
import { createDiscordChannelDomain } from "../features/channels/connectors/discord/domain";
import { useDiscordChannelState } from "../features/channels/connectors/discord/useDiscordChannelState";
import { useAuthDomain } from "./hooks/useAuthDomain";
import { useConfigDomain } from "./hooks/useConfigDomain";
import { useOverviewDomain } from "./hooks/useOverviewDomain";
import { useSupportDomain } from "./hooks/useSupportDomain";
import type { Section } from "./sectionMetadata";
import { DEFAULT_CRON_FORM, DEFAULT_LOGIN_FORM, type CronForm, type LoginForm } from "./stateTypes";
import {
  emptyToUndefined,
  parseInteger,
  readObject,
  readString,
  skillMetadata,
  toErrorMessage,
  toJsonObjectArray,
  type JsonObject,
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
  secrets: 15_000,
  access: 10_000,
  operations: 10_000,
  support: 10_000,
};

const BOOTSTRAP_SESSION_RETRY_DELAY_MS = 150;
const BOOTSTRAP_SESSION_RETRY_ATTEMPTS = 5;
const DESKTOP_SESSION_RECOVERY_DELAY_MS = 750;
const DESKTOP_SESSION_RECOVERY_ATTEMPTS = 8;
const DESKTOP_HANDOFF_QUERY_PARAM = "desktop_handoff_token";

export function shouldAutoRefreshSection(
  section: Section,
  lastRefreshedAt: number | null,
  now: number = Date.now(),
): boolean {
  const ttlMs = AUTO_REFRESH_SECTION_TTL_MS[section];
  if (ttlMs === undefined || lastRefreshedAt === null) {
    return true;
  }
  return now - lastRefreshedAt >= ttlMs;
}

function shouldRetryBootstrapSession(
  error: unknown,
  attempt: number,
  maxAttempts: number,
): boolean {
  if (!(error instanceof ControlPlaneApiError)) {
    return false;
  }
  if (attempt >= maxAttempts) {
    return false;
  }
  return error.status === 401 || error.status === 403 || error.status === 429;
}

async function loadBootstrapSession(
  api: ConsoleApiClient,
  signal?: AbortSignal,
): Promise<ConsoleSession> {
  for (let attempt = 1; attempt <= BOOTSTRAP_SESSION_RETRY_ATTEMPTS; attempt += 1) {
    if (signal?.aborted) {
      throw createAbortError();
    }
    try {
      return await api.getSession();
    } catch (error) {
      if (!shouldRetryBootstrapSession(error, attempt, BOOTSTRAP_SESSION_RETRY_ATTEMPTS)) {
        throw error;
      }
      await waitForDelay(BOOTSTRAP_SESSION_RETRY_DELAY_MS * attempt, signal);
    }
  }

  throw new Error("Bootstrap session retry loop exhausted without returning a session.");
}

async function loadDesktopHandoffSession(
  api: ConsoleApiClient,
  desktopHandoffToken: string,
  signal?: AbortSignal,
): Promise<ConsoleSession> {
  try {
    return await api.consumeDesktopHandoff(desktopHandoffToken);
  } catch (handoffError) {
    try {
      return await loadBootstrapSession(api, signal);
    } catch {
      throw handoffError;
    }
  }
}

function shouldAttemptDesktopSessionRecovery(): boolean {
  if (typeof window === "undefined") {
    return false;
  }
  const hostname = window.location.hostname.trim().toLowerCase();
  return hostname === "127.0.0.1" || hostname === "localhost";
}

function readDesktopHandoffToken(): string | null {
  if (typeof window === "undefined") {
    return null;
  }
  const token = new URLSearchParams(window.location.search).get(DESKTOP_HANDOFF_QUERY_PARAM);
  if (token === null) {
    return null;
  }
  const trimmed = token.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function clearDesktopHandoffTokenFromAddressBar(): void {
  if (typeof window === "undefined") {
    return;
  }
  const current = new URL(window.location.href);
  current.searchParams.delete(DESKTOP_HANDOFF_QUERY_PARAM);
  const next = `${current.pathname}${current.search}${current.hash}`;
  window.history.replaceState(window.history.state, "", next);
}

function toWorkspaceSlug(value: string): string {
  const normalized = value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-");
  const slug = normalized.replace(/^-+|-+$/g, "");
  return slug.length > 0 ? slug : "promoted-memory";
}

export function useConsoleAppState() {
  const api = useMemo(() => new ConsoleApiClient(""), []);
  const desktopSessionRecoveryAttemptedRef = useRef(false);

  const [booting, setBooting] = useState(true);
  const [session, setSession] = useState<ConsoleSession | null>(null);
  const [section, setSectionState] = useState<Section>("overview");
  const lastSectionAutoRefreshRef = useRef<Partial<Record<Section, number>>>({});
  const [theme, setTheme] = useState<ThemeMode>(() => {
    if (typeof window === "undefined") {
      return "dark";
    }
    const stored = window.localStorage.getItem("palyra.console.theme");
    if (stored === "light" || stored === "dark") {
      return stored;
    }
    if (
      window.matchMedia !== undefined &&
      window.matchMedia("(prefers-color-scheme: dark)").matches
    ) {
      return "dark";
    }
    return "dark";
  });
  const [revealSensitiveValues, setRevealSensitiveValues] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const [loginBusy, setLoginBusy] = useState(false);
  const [logoutBusy, setLogoutBusy] = useState(false);
  const [loginFormState, setLoginFormState] = useState<LoginForm>(() => ({
    ...DEFAULT_LOGIN_FORM,
  }));
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
  const [memoryWorkspaceDocuments, setMemoryWorkspaceDocuments] = useState<JsonObject[]>([]);
  const [memoryWorkspacePath, setMemoryWorkspacePath] = useState("README.md");
  const [memoryWorkspaceNextPath, setMemoryWorkspaceNextPath] = useState("README.md");
  const [memoryWorkspaceTitle, setMemoryWorkspaceTitle] = useState("");
  const [memoryWorkspaceContent, setMemoryWorkspaceContent] = useState("");
  const [memoryWorkspaceVersions, setMemoryWorkspaceVersions] = useState<JsonObject[]>([]);
  const [memoryWorkspaceSearchQuery, setMemoryWorkspaceSearchQuery] = useState("");
  const [memoryWorkspaceHits, setMemoryWorkspaceHits] = useState<JsonObject[]>([]);
  const [memorySearchAllQuery, setMemorySearchAllQuery] = useState("");
  const [memorySearchAllResults, setMemorySearchAllResults] = useState<JsonObject | null>(null);
  const [memoryRecallPreview, setMemoryRecallPreview] = useState<JsonObject | null>(null);
  const [memoryDerivedArtifacts, setMemoryDerivedArtifacts] = useState<JsonObject[]>([]);

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
  const [browserRelayAction, setBrowserRelayAction] = useState<
    "open_tab" | "capture_selection" | "send_page_snapshot"
  >("capture_selection");
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
    resetOverviewDomain,
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
    resetConfigDomain,
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
    resetSupportDomain,
  } = supportDomain;

  function applyConsoleSession(current: ConsoleSession): void {
    setSession(current);
    setLoginForm((previous: LoginForm) => ({
      ...previous,
      principal: current.principal,
      deviceId: current.device_id,
      channel: current.channel ?? previous.channel,
    }));
    setBrowserPrincipal((previous) =>
      previous.trim().length === 0 ? current.principal : previous,
    );
  }

  useEffect(() => {
    let cancelled = false;
    const abortController = new AbortController();
    const bootstrap = async () => {
      setBooting(true);
      setError(null);
      try {
        const desktopHandoffToken = readDesktopHandoffToken();
        if (desktopHandoffToken !== null) {
          try {
            const current = await loadDesktopHandoffSession(
              api,
              desktopHandoffToken,
              abortController.signal,
            );
            if (cancelled) {
              return;
            }
            clearDesktopHandoffTokenFromAddressBar();
            desktopSessionRecoveryAttemptedRef.current = true;
            applyConsoleSession(current);
            return;
          } catch {
            clearDesktopHandoffTokenFromAddressBar();
          }
        }
        const current = await loadBootstrapSession(api, abortController.signal);
        if (cancelled) {
          return;
        }
        desktopSessionRecoveryAttemptedRef.current = true;
        applyConsoleSession(current);
      } catch (failure) {
        if (!cancelled && !isAbortError(failure)) {
          setSession(null);
          setError(toErrorMessage(failure));
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
      abortController.abort();
    };
  }, [api]);

  useEffect(() => {
    if (
      booting ||
      session !== null ||
      loginBusy ||
      logoutBusy ||
      desktopSessionRecoveryAttemptedRef.current ||
      !shouldAttemptDesktopSessionRecovery()
    ) {
      return;
    }

    desktopSessionRecoveryAttemptedRef.current = true;
    let cancelled = false;
    const abortController = new AbortController();
    const recoverDesktopSession = async () => {
      try {
        for (let attempt = 1; attempt <= DESKTOP_SESSION_RECOVERY_ATTEMPTS; attempt += 1) {
          if (abortController.signal.aborted) {
            return;
          }
          try {
            const current = await api.getSession();
            if (cancelled) {
              return;
            }
            setError(null);
            applyConsoleSession(current);
            return;
          } catch (failure) {
            if (!cancelled) {
              setError(toErrorMessage(failure));
            }
            if (attempt >= DESKTOP_SESSION_RECOVERY_ATTEMPTS) {
              return;
            }
            await waitForDelay(DESKTOP_SESSION_RECOVERY_DELAY_MS * attempt, abortController.signal);
          }
        }
      } catch (failure) {
        if (!cancelled && !isAbortError(failure)) {
          setError(toErrorMessage(failure));
        }
      }
    };

    void recoverDesktopSession();
    return () => {
      cancelled = true;
      abortController.abort();
    };
  }, [api, booting, loginBusy, logoutBusy, session]);

  useEffect(() => {
    const root = document.documentElement;
    root.setAttribute("data-theme", theme);
    root.classList.toggle("dark", theme === "dark");
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
      void refreshWorkspaceDocuments();
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
    if (section === "secrets") {
      void refreshSecrets();
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
    setMemoryWorkspaceDocuments([]);
    setMemoryWorkspacePath("README.md");
    setMemoryWorkspaceTitle("");
    setMemoryWorkspaceContent("");
    setMemoryWorkspaceVersions([]);
    setMemoryWorkspaceSearchQuery("");
    setMemoryWorkspaceHits([]);
    setMemorySearchAllQuery("");
    setMemorySearchAllResults(null);
    setMemoryRecallPreview(null);

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
        channel: emptyToUndefined(loginForm.channel),
      });
      resetOperatorScopedState();
      setSession(next);
      setBrowserPrincipal(next.principal);
      setLoginForm((previous: LoginForm) => ({
        ...previous,
        adminToken: "",
        principal: next.principal,
        deviceId: next.device_id,
        channel: next.channel ?? previous.channel,
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
        decision_scope:
          approvalScope === "session" || approvalScope === "timeboxed" ? approvalScope : "once",
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
        cron_expression:
          cronForm.scheduleType === "cron" ? emptyToUndefined(cronForm.cronExpression) : undefined,
        every_interval_ms:
          cronForm.scheduleType === "every"
            ? (parseInteger(cronForm.everyIntervalMs) ?? undefined)
            : undefined,
        at_timestamp_rfc3339:
          cronForm.scheduleType === "at"
            ? emptyToUndefined(cronForm.atTimestampRfc3339)
            : undefined,
        enabled: cronForm.enabled,
        channel: emptyToUndefined(cronForm.channel),
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
      setMemoryStatus(response as unknown as JsonObject);
      if (response.workspace?.recent_documents !== undefined) {
        setMemoryWorkspaceDocuments(
          toJsonObjectArray(response.workspace.recent_documents as unknown as JsonValue[]),
        );
      }
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setMemoryStatusBusy(false);
    }
  }

  async function refreshWorkspaceDocuments(): Promise<void> {
    setMemoryBusy(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      params.set("limit", "24");
      const response = await api.listWorkspaceDocuments(params);
      const documents = toJsonObjectArray(response.documents as unknown as JsonValue[]);
      setMemoryWorkspaceDocuments(documents);
      if (memoryWorkspaceTitle.trim().length === 0 && memoryWorkspaceContent.trim().length === 0) {
        const firstPath = readString(documents[0], "path");
        if (firstPath !== null) {
          void selectWorkspaceDocument(firstPath);
        }
      }
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setMemoryBusy(false);
    }
  }

  async function loadWorkspaceDocumentVersions(path: string): Promise<void> {
    const trimmed = path.trim();
    if (trimmed.length === 0) {
      setMemoryWorkspaceVersions([]);
      return;
    }
    try {
      const params = new URLSearchParams();
      params.set("path", trimmed);
      params.set("limit", "12");
      const response = await api.getWorkspaceDocumentVersions(params);
      setMemoryWorkspaceVersions(toJsonObjectArray(response.versions as unknown as JsonValue[]));
    } catch (failure) {
      setError(toErrorMessage(failure));
    }
  }

  async function selectWorkspaceDocument(path: string): Promise<void> {
    const trimmed = path.trim();
    if (trimmed.length === 0) {
      return;
    }
    setMemoryBusy(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      params.set("path", trimmed);
      const response = await api.getWorkspaceDocument(params);
      setMemoryWorkspacePath(response.document.path);
      setMemoryWorkspaceNextPath(response.document.path);
      setMemoryWorkspaceTitle(response.document.title);
      setMemoryWorkspaceContent(response.document.content_text);
      await Promise.all([
        loadWorkspaceDocumentVersions(response.document.path),
        loadWorkspaceDerivedArtifacts(response.document.document_id),
      ]);
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setMemoryBusy(false);
    }
  }

  async function saveWorkspaceDocument(event?: FormEvent<HTMLFormElement>): Promise<void> {
    event?.preventDefault();
    if (memoryWorkspacePath.trim().length === 0) {
      setError("Workspace path cannot be empty.");
      return;
    }
    if (memoryWorkspaceContent.trim().length === 0) {
      setError("Workspace content cannot be empty.");
      return;
    }
    setMemoryBusy(true);
    setError(null);
    try {
      const response = await api.writeWorkspaceDocument({
        path: memoryWorkspacePath.trim(),
        title: emptyToUndefined(memoryWorkspaceTitle),
        content_text: memoryWorkspaceContent,
        manual_override: true,
      });
      setMemoryWorkspacePath(response.document.path);
      setMemoryWorkspaceNextPath(response.document.path);
      setMemoryWorkspaceTitle(response.document.title);
      setNotice(`Saved ${response.document.path}.`);
      await Promise.all([
        refreshWorkspaceDocuments(),
        loadWorkspaceDocumentVersions(response.document.path),
        loadWorkspaceDerivedArtifacts(response.document.document_id),
        refreshMemoryStatus(),
      ]);
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setMemoryBusy(false);
    }
  }

  async function bootstrapWorkspace(forceRepair = false): Promise<void> {
    setMemoryBusy(true);
    setError(null);
    try {
      const response = await api.bootstrapWorkspace({ force_repair: forceRepair });
      setNotice(
        `Workspace bootstrap ran. Created ${response.bootstrap.created_paths.length}, updated ${response.bootstrap.updated_paths.length}, skipped ${response.bootstrap.skipped_paths.length}.`,
      );
      await Promise.all([refreshMemoryStatus(), refreshWorkspaceDocuments()]);
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setMemoryBusy(false);
    }
  }

  async function loadWorkspaceDerivedArtifacts(documentId: string): Promise<void> {
    const trimmed = documentId.trim();
    if (trimmed.length === 0) {
      setMemoryDerivedArtifacts([]);
      return;
    }
    try {
      const response = await api.listMemoryDerivedArtifacts({
        workspace_document_id: trimmed,
        limit: 24,
      });
      setMemoryDerivedArtifacts(toJsonObjectArray(response.derived_artifacts as unknown as JsonValue[]));
    } catch (failure) {
      setError(toErrorMessage(failure));
    }
  }

  async function toggleWorkspaceDocumentPinned(path: string, pinned: boolean): Promise<void> {
    if (path.trim().length === 0) {
      setError("Workspace document path is missing.");
      return;
    }
    setMemoryBusy(true);
    setError(null);
    try {
      await api.pinWorkspaceDocument({ path: path.trim(), pinned });
      setNotice(pinned ? `Pinned ${path.trim()}.` : `Unpinned ${path.trim()}.`);
      await Promise.all([refreshMemoryStatus(), refreshWorkspaceDocuments()]);
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setMemoryBusy(false);
    }
  }

  async function moveWorkspaceDocument(): Promise<void> {
    const currentPath = memoryWorkspacePath.trim();
    const nextPath = memoryWorkspaceNextPath.trim();
    if (currentPath.length === 0 || nextPath.length === 0) {
      setError("Both current and next workspace paths are required.");
      return;
    }
    if (currentPath === nextPath) {
      setNotice("Workspace path is already current.");
      return;
    }
    setMemoryBusy(true);
    setError(null);
    try {
      const response = await api.moveWorkspaceDocument({
        path: currentPath,
        next_path: nextPath,
      });
      setMemoryWorkspacePath(response.document.path);
      setMemoryWorkspaceNextPath(response.document.path);
      setMemoryWorkspaceTitle(response.document.title);
      setNotice(`Moved ${currentPath} to ${response.document.path}.`);
      await Promise.all([
        refreshWorkspaceDocuments(),
        loadWorkspaceDocumentVersions(response.document.path),
        refreshMemoryStatus(),
      ]);
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setMemoryBusy(false);
    }
  }

  async function deleteWorkspaceDocument(path?: string): Promise<void> {
    const targetPath = (path ?? memoryWorkspacePath).trim();
    if (targetPath.length === 0) {
      setError("Workspace document path is missing.");
      return;
    }
    setMemoryBusy(true);
    setError(null);
    try {
      await api.deleteWorkspaceDocument({ path: targetPath });
      setNotice(`Deleted ${targetPath}.`);
      if (targetPath === memoryWorkspacePath.trim()) {
        setMemoryWorkspacePath("notes/new-doc.md");
        setMemoryWorkspaceNextPath("notes/new-doc.md");
        setMemoryWorkspaceTitle("");
        setMemoryWorkspaceContent("");
        setMemoryWorkspaceVersions([]);
      }
      await Promise.all([refreshWorkspaceDocuments(), refreshMemoryStatus()]);
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setMemoryBusy(false);
    }
  }

  function promoteMemoryHitToWorkspaceDraft(hit: JsonObject): void {
    const item = readObject(hit, "item") ?? {};
    const memoryId = readString(hit, "memory_id") ?? readString(item, "memory_id");
    const sourceChannel = readString(hit, "channel") ?? readString(item, "channel") ?? "memory";
    const content =
      readString(hit, "snippet") ??
      readString(hit, "content") ??
      readString(item, "content_text") ??
      "";
    const safeStem = toWorkspaceSlug(memoryId ?? sourceChannel);
    const nextPath = `projects/${safeStem}.md`;
    setMemoryWorkspacePath(nextPath);
    setMemoryWorkspaceNextPath(nextPath);
    setMemoryWorkspaceTitle(memoryId ?? `Promoted ${sourceChannel}`);
    setMemoryWorkspaceContent(content);
    setNotice(`Prepared workspace draft ${nextPath} from ${memoryId ?? sourceChannel}.`);
  }

  async function searchWorkspaceDocuments(event?: FormEvent<HTMLFormElement>): Promise<void> {
    event?.preventDefault();
    if (memoryWorkspaceSearchQuery.trim().length === 0) {
      setError("Workspace query cannot be empty.");
      return;
    }
    setMemoryBusy(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      params.set("query", memoryWorkspaceSearchQuery.trim());
      const response = await api.searchWorkspaceDocuments(params);
      setMemoryWorkspaceHits(toJsonObjectArray(response.hits as unknown as JsonValue[]));
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setMemoryBusy(false);
    }
  }

  async function previewMemoryRecall(event?: FormEvent<HTMLFormElement>): Promise<void> {
    event?.preventDefault();
    if (memoryQuery.trim().length === 0) {
      setError("Recall query cannot be empty.");
      return;
    }
    setMemoryBusy(true);
    setError(null);
    try {
      const response = await api.previewRecall({
        query: memoryQuery.trim(),
        channel: emptyToUndefined(memoryChannel),
        memory_top_k: 4,
        workspace_top_k: 4,
      });
      setMemoryRecallPreview(response as unknown as JsonObject);
      setMemoryHits(toJsonObjectArray(response.memory_hits));
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setMemoryBusy(false);
    }
  }

  async function searchAllMemorySources(event?: FormEvent<HTMLFormElement>): Promise<void> {
    event?.preventDefault();
    if (memorySearchAllQuery.trim().length === 0) {
      setError("Unified search query cannot be empty.");
      return;
    }
    setMemoryBusy(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      params.set("q", memorySearchAllQuery.trim());
      if (memoryChannel.trim().length > 0) {
        params.set("channel", memoryChannel.trim());
      }
      const response = await api.searchAll(params);
      setMemorySearchAllResults(response as unknown as JsonObject);
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setMemoryBusy(false);
    }
  }

  async function purgeMemory(): Promise<void> {
    setMemoryBusy(true);
    setError(null);
    try {
      const response = await api.purgeMemory({
        channel: emptyToUndefined(memoryPurgeChannel),
        session_id: emptyToUndefined(memoryPurgeSessionId),
        purge_all_principal: memoryPurgeAll,
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
        allow_untrusted: skillAllowUntrusted,
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

  async function executeSkillAction(
    entry: JsonObject,
    action: "verify" | "audit" | "quarantine" | "enable",
  ): Promise<void> {
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
          quarantine_on_fail: true,
        });
      }
      if (action === "quarantine") {
        await api.quarantineSkill({
          skill_id: metadata.skillId,
          version: metadata.version,
          reason: emptyToUndefined(skillReason),
        });
      }
      if (action === "enable") {
        await api.enableSkill({
          skill_id: metadata.skillId,
          version: metadata.version,
          reason: emptyToUndefined(skillReason),
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
        private_profile: browserProfilePrivate,
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
        name: browserRenameName.trim(),
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
        principal: emptyToUndefined(browserPrincipal),
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
      const confirmed = window.confirm(
        `Delete browser profile '${profileName}'? This cannot be undone.`,
      );
      if (!confirmed) {
        setNotice("Browser profile deletion canceled.");
        return;
      }
    }
    setBrowserBusy(true);
    setError(null);
    try {
      await api.deleteBrowserProfile(profileId, {
        principal: emptyToUndefined(browserPrincipal),
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
        ttl_ms: parseInteger(browserRelayTtlMs) ?? undefined,
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
        max_payload_bytes: 16384,
      };

      if (browserRelayAction === "open_tab") {
        payload.open_tab = {
          url: browserRelayOpenTabUrl.trim(),
          activate: true,
          timeout_ms: 6000,
        };
      }
      if (browserRelayAction === "capture_selection") {
        payload.capture_selection = {
          selector: browserRelaySelector.trim(),
          max_selection_bytes: 2048,
        };
      }
      if (browserRelayAction === "send_page_snapshot") {
        payload.page_snapshot = {
          include_dom_snapshot: true,
          include_visible_text: true,
          max_dom_snapshot_bytes: 4096,
          max_visible_text_bytes: 4096,
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
    memoryWorkspaceDocuments,
    memoryWorkspacePath,
    setMemoryWorkspacePath,
    memoryWorkspaceNextPath,
    setMemoryWorkspaceNextPath,
    memoryWorkspaceTitle,
    setMemoryWorkspaceTitle,
    memoryWorkspaceContent,
    setMemoryWorkspaceContent,
    memoryWorkspaceVersions,
    memoryWorkspaceSearchQuery,
    setMemoryWorkspaceSearchQuery,
    memoryWorkspaceHits,
    memoryDerivedArtifacts,
    memorySearchAllQuery,
    setMemorySearchAllQuery,
    memorySearchAllResults,
    memoryRecallPreview,
    refreshMemoryStatus,
    refreshWorkspaceDocuments,
    searchMemory,
    selectWorkspaceDocument,
    saveWorkspaceDocument,
    bootstrapWorkspace,
    moveWorkspaceDocument,
    deleteWorkspaceDocument,
    toggleWorkspaceDocumentPinned,
    searchWorkspaceDocuments,
    previewMemoryRecall,
    searchAllMemorySources,
    promoteMemoryHitToWorkspaceDraft,
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
    loadSupportBundleJob,
  };
}

export type ConsoleAppState = ReturnType<typeof useConsoleAppState>;

function shouldConfirmBrowserDeletion(): boolean {
  if (typeof window === "undefined" || typeof window.confirm !== "function") {
    return false;
  }
  if (isJsdomRuntime()) {
    return false;
  }
  return true;
}

function isJsdomRuntime(): boolean {
  if (typeof navigator === "undefined") {
    return false;
  }
  return navigator.userAgent.toLowerCase().includes("jsdom");
}

function waitForDelay(delayMs: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) {
    return Promise.reject(createAbortError());
  }
  return new Promise((resolve, reject) => {
    const timer = window.setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, delayMs);
    const onAbort = () => {
      window.clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
      reject(createAbortError());
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

function createAbortError(): Error {
  return new Error("console app effect aborted");
}

function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.message === "console app effect aborted";
}
