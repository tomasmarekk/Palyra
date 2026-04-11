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
import { useBrowserDomain } from "./hooks/useBrowserDomain";
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
    channelMessageConversationId,
    setChannelMessageConversationId,
    channelMessageThreadId,
    setChannelMessageThreadId,
    channelMessageReadMessageId,
    setChannelMessageReadMessageId,
    channelMessageReadBeforeMessageId,
    setChannelMessageReadBeforeMessageId,
    channelMessageReadAfterMessageId,
    setChannelMessageReadAfterMessageId,
    channelMessageReadAroundMessageId,
    setChannelMessageReadAroundMessageId,
    channelMessageReadLimit,
    setChannelMessageReadLimit,
    channelMessageSearchQuery,
    setChannelMessageSearchQuery,
    channelMessageSearchAuthorId,
    setChannelMessageSearchAuthorId,
    channelMessageSearchHasAttachments,
    setChannelMessageSearchHasAttachments,
    channelMessageSearchBeforeMessageId,
    setChannelMessageSearchBeforeMessageId,
    channelMessageSearchLimit,
    setChannelMessageSearchLimit,
    channelMessageMutationMessageId,
    setChannelMessageMutationMessageId,
    channelMessageMutationApprovalId,
    setChannelMessageMutationApprovalId,
    channelMessageMutationBody,
    setChannelMessageMutationBody,
    channelMessageMutationDeleteReason,
    setChannelMessageMutationDeleteReason,
    channelMessageMutationEmoji,
    setChannelMessageMutationEmoji,
    channelMessageReadResult,
    setChannelMessageReadResultPayload,
    channelMessageSearchResult,
    setChannelMessageSearchResultPayload,
    channelMessageMutationResult,
    setChannelMessageMutationResultPayload,
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
    readChannelMessages,
    searchChannelMessages,
    editChannelMessage,
    deleteChannelMessage,
    addChannelMessageReaction,
    removeChannelMessageReaction,
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
    channelMessageConversationId,
    channelMessageThreadId,
    channelMessageReadMessageId,
    channelMessageReadBeforeMessageId,
    channelMessageReadAfterMessageId,
    channelMessageReadAroundMessageId,
    channelMessageReadLimit,
    channelMessageSearchQuery,
    channelMessageSearchAuthorId,
    channelMessageSearchHasAttachments,
    channelMessageSearchBeforeMessageId,
    channelMessageSearchLimit,
    channelMessageMutationMessageId,
    channelMessageMutationApprovalId,
    channelMessageMutationBody,
    channelMessageMutationDeleteReason,
    channelMessageMutationEmoji,
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
    setChannelMessageConversationId,
    setChannelMessageThreadId,
    setChannelMessageMutationMessageId,
    setChannelMessageMutationApprovalId,
    setChannelMessageReadResultPayload,
    setChannelMessageSearchResultPayload,
    setChannelMessageMutationResultPayload,
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
  const [memoryLearningBusy, setMemoryLearningBusy] = useState(false);
  const [memoryLearningCandidates, setMemoryLearningCandidates] = useState<JsonObject[]>([]);
  const [memoryLearningHistory, setMemoryLearningHistory] = useState<JsonObject[]>([]);
  const [memoryLearningPreferences, setMemoryLearningPreferences] = useState<JsonObject[]>([]);
  const [memoryLearningCandidateId, setMemoryLearningCandidateId] = useState("");
  const [memoryLearningCandidateKindFilter, setMemoryLearningCandidateKindFilter] = useState("");
  const [memoryLearningStatusFilter, setMemoryLearningStatusFilter] = useState("");
  const [memoryLearningRiskFilter, setMemoryLearningRiskFilter] = useState("");
  const [memoryLearningMinConfidenceFilter, setMemoryLearningMinConfidenceFilter] = useState("");
  const [memoryLearningMaxConfidenceFilter, setMemoryLearningMaxConfidenceFilter] = useState("");

  const [skillsBusy, setSkillsBusy] = useState(false);
  const [skillsEntries, setSkillsEntries] = useState<JsonObject[]>([]);
  const [skillProcedureCandidates, setSkillProcedureCandidates] = useState<JsonObject[]>([]);
  const [skillBuilderCandidates, setSkillBuilderCandidates] = useState<JsonObject[]>([]);
  const [lastSkillPromotion, setLastSkillPromotion] = useState<JsonObject | null>(null);
  const [skillArtifactPath, setSkillArtifactPath] = useState("");
  const [skillAllowTofu, setSkillAllowTofu] = useState(true);
  const [skillAllowUntrusted, setSkillAllowUntrusted] = useState(false);
  const [skillReason, setSkillReason] = useState("");
  const [skillBuilderPrompt, setSkillBuilderPrompt] = useState("");
  const [skillBuilderName, setSkillBuilderName] = useState("");

  const [auditBusy, setAuditBusy] = useState(false);
  const [auditFilterContains, setAuditFilterContains] = useState("");
  const [auditFilterPrincipal, setAuditFilterPrincipal] = useState("");
  const [auditEvents, setAuditEvents] = useState<JsonObject[]>([]);
  const [diagnosticsBusy, setDiagnosticsBusy] = useState(false);
  const [diagnosticsSnapshot, setDiagnosticsSnapshot] = useState<JsonObject | null>(null);

  const overviewDomain = useOverviewDomain({ api, setError });
  const authDomain = useAuthDomain({ api, setError, setNotice });
  const configDomain = useConfigDomain({ api, setError, setNotice });
  const supportDomain = useSupportDomain({ api, setError, setNotice });
  const { resetBrowserDomain, ...browserDomain } = useBrowserDomain({
    api,
    setError,
    setNotice,
    setSection,
  });
  const {
    overviewBusy,
    overviewCatalog,
    overviewDeployment,
    overviewApprovals,
    overviewDiagnostics,
    overviewUsageInsights,
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
    supportNodePairingMethod,
    setSupportNodePairingMethod,
    supportPairingChannel,
    setSupportPairingChannel,
    supportPairingIssuedBy,
    setSupportPairingIssuedBy,
    supportPairingTtlMs,
    setSupportPairingTtlMs,
    supportNodePairingCodes,
    supportNodePairingRequests,
    supportPairingDecisionReason,
    setSupportPairingDecisionReason,
    supportBundleRetainJobs,
    setSupportBundleRetainJobs,
    supportBundleJobs,
    supportSelectedBundleJobId,
    setSupportSelectedBundleJobId,
    supportSelectedBundleJob,
    supportDoctorRetainJobs,
    setSupportDoctorRetainJobs,
    supportDoctorOnly,
    setSupportDoctorOnly,
    supportDoctorSkip,
    setSupportDoctorSkip,
    supportDoctorRollbackRunId,
    setSupportDoctorRollbackRunId,
    supportDoctorForce,
    setSupportDoctorForce,
    supportDoctorJobs,
    supportSelectedDoctorJobId,
    setSupportSelectedDoctorJobId,
    supportSelectedDoctorJob,
    refreshSupport,
    mintSupportPairingCode,
    approveSupportPairingRequest,
    rejectSupportPairingRequest,
    createSupportBundle,
    loadSupportBundleJob,
    queueDoctorRecoveryPreview,
    queueDoctorRecoveryApply,
    queueDoctorRollbackPreview,
    queueDoctorRollbackApply,
    loadDoctorRecoveryJob,
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
    browserDomain.setBrowserPrincipal((previous) =>
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
      void refreshLearningQueue();
    }
    if (section === "skills") {
      void refreshSkills();
    }
    if (section === "browser") {
      void browserDomain.refreshBrowserProfiles();
      void browserDomain.refreshBrowserSessions();
      if (browserDomain.browserSessionId.trim().length > 0) {
        void browserDomain.inspectBrowserSessionWorkspace();
      }
    }
    if (section === "config") {
      void refreshConfigSurface();
    }
    if (
      section === "auth" ||
      section === "config" ||
      section === "usage" ||
      section === "operations"
    ) {
      void refreshDiagnostics();
    }
    if (section === "usage" || section === "operations") {
      void refreshMemoryStatus();
    }
    if (section === "secrets") {
      void refreshSecrets();
    }
    if (section === "operations") {
      void refreshAudit();
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
    setMemoryDerivedArtifacts([]);
    setMemoryLearningBusy(false);
    setMemoryLearningCandidates([]);
    setMemoryLearningHistory([]);
    setMemoryLearningPreferences([]);
    setMemoryLearningCandidateId("");
    setMemoryLearningCandidateKindFilter("");
    setMemoryLearningStatusFilter("");
    setMemoryLearningRiskFilter("");
    setMemoryLearningMinConfidenceFilter("");
    setMemoryLearningMaxConfidenceFilter("");

    setSkillsBusy(false);
    setSkillsEntries([]);
    setSkillProcedureCandidates([]);
    setSkillBuilderCandidates([]);
    setLastSkillPromotion(null);
    setSkillArtifactPath("");
    setSkillAllowTofu(true);
    setSkillAllowUntrusted(false);
    setSkillReason("");
    setSkillBuilderPrompt("");
    setSkillBuilderName("");

    setAuditBusy(false);
    setAuditFilterContains("");
    setAuditFilterPrincipal("");
    setAuditEvents([]);
    setDiagnosticsBusy(false);
    setDiagnosticsSnapshot(null);

    resetBrowserDomain();

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
      browserDomain.setBrowserPrincipal(next.principal);
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
      setNotice("Routine saved.");
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
      setNotice(`Routine ${enabled ? "enabled" : "disabled"}.`);
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
      setNotice("Routine run-now dispatched.");
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setCronBusy(false);
    }
  }

  async function refreshCronRuns(): Promise<void> {
    if (cronJobId.trim().length === 0) {
      setError("Select a routine before loading runs.");
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

  async function refreshLearningQueue(): Promise<void> {
    setMemoryLearningBusy(true);
    setError(null);
    try {
      const candidateParams = new URLSearchParams({ limit: "64" });
      if (memoryLearningCandidateKindFilter.trim().length > 0) {
        candidateParams.set("candidate_kind", memoryLearningCandidateKindFilter.trim());
      }
      if (memoryLearningStatusFilter.trim().length > 0) {
        candidateParams.set("status", memoryLearningStatusFilter.trim());
      }
      if (memoryLearningRiskFilter.trim().length > 0) {
        candidateParams.set("risk_level", memoryLearningRiskFilter.trim());
      }
      if (memoryLearningMinConfidenceFilter.trim().length > 0) {
        candidateParams.set("min_confidence", memoryLearningMinConfidenceFilter.trim());
      }
      if (memoryLearningMaxConfidenceFilter.trim().length > 0) {
        candidateParams.set("max_confidence", memoryLearningMaxConfidenceFilter.trim());
      }
      const [candidatesResponse, preferencesResponse] = await Promise.all([
        api.listLearningCandidates(candidateParams),
        api.listLearningPreferences(new URLSearchParams({ limit: "64", status: "active" })),
      ]);
      const candidates = toJsonObjectArray(candidatesResponse.candidates as unknown as JsonValue[]);
      setMemoryLearningCandidates(candidates);
      setMemoryLearningPreferences(
        toJsonObjectArray(preferencesResponse.preferences as unknown as JsonValue[]),
      );
      const nextCandidateId =
        memoryLearningCandidateId.trim().length > 0
          ? memoryLearningCandidateId.trim()
          : (readString(candidates[0], "candidate_id") ?? "");
      setMemoryLearningCandidateId(nextCandidateId);
      if (nextCandidateId.length > 0) {
        const historyResponse = await api.getLearningCandidateHistory(nextCandidateId);
        setMemoryLearningHistory(
          toJsonObjectArray(historyResponse.history as unknown as JsonValue[]),
        );
      } else {
        setMemoryLearningHistory([]);
      }
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setMemoryLearningBusy(false);
    }
  }

  async function selectLearningCandidate(candidateId: string): Promise<void> {
    const trimmed = candidateId.trim();
    setMemoryLearningCandidateId(trimmed);
    if (trimmed.length === 0) {
      setMemoryLearningHistory([]);
      return;
    }
    setMemoryLearningBusy(true);
    setError(null);
    try {
      const response = await api.getLearningCandidateHistory(trimmed);
      setMemoryLearningHistory(toJsonObjectArray(response.history as unknown as JsonValue[]));
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setMemoryLearningBusy(false);
    }
  }

  async function reviewLearningCandidate(
    candidateId: string,
    status: string,
    applyPreference = false,
  ): Promise<void> {
    const trimmed = candidateId.trim();
    if (trimmed.length === 0) {
      setError("Learning candidate ID is missing.");
      return;
    }
    setMemoryLearningBusy(true);
    setError(null);
    try {
      const response = await api.reviewLearningCandidate(trimmed, {
        status,
        apply_preference: applyPreference,
      });
      setNotice(
        `Learning candidate ${response.candidate.title} marked as ${response.candidate.status}.`,
      );
      await refreshLearningQueue();
      await selectLearningCandidate(trimmed);
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setMemoryLearningBusy(false);
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
      setMemoryDerivedArtifacts(
        toJsonObjectArray(response.derived_artifacts as unknown as JsonValue[]),
      );
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
      const [response, candidateResponse, builderResponse] = await Promise.all([
        api.listSkills(),
        api.listLearningCandidates(
          new URLSearchParams([
            ["candidate_kind", "procedure"],
            ["limit", "24"],
          ]),
        ),
        api.listSkillBuilderCandidates(),
      ]);
      setSkillsEntries(toJsonObjectArray(response.entries));
      setSkillProcedureCandidates(
        toJsonObjectArray(candidateResponse.candidates as unknown as JsonValue[]).filter(
          (candidate) => readString(candidate, "candidate_kind") === "procedure",
        ),
      );
      setSkillBuilderCandidates(toJsonObjectArray(builderResponse.entries as unknown as JsonValue[]));
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

  async function promoteProcedureCandidate(candidateId: string): Promise<void> {
    setSkillsBusy(true);
    setError(null);
    try {
      const response = await api.promoteProcedureCandidate(candidateId);
      setLastSkillPromotion(response.skill as unknown as JsonObject);
      setNotice("Procedure candidate promoted into a quarantined skill scaffold.");
      await refreshSkills();
      await refreshLearningQueue();
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setSkillsBusy(false);
    }
  }

  async function createSkillBuilderCandidate(event?: React.FormEvent<HTMLFormElement>): Promise<void> {
    event?.preventDefault();
    if (skillBuilderPrompt.trim().length === 0) {
      setError("Builder prompt cannot be empty.");
      return;
    }
    setSkillsBusy(true);
    setError(null);
    try {
      const response = await api.createSkillBuilderCandidate({
        prompt: skillBuilderPrompt.trim(),
        name: emptyToUndefined(skillBuilderName),
        review_notes: emptyToUndefined(skillReason),
      });
      setSkillBuilderPrompt("");
      setSkillBuilderName("");
      setLastSkillPromotion(response.skill as unknown as JsonObject);
      setNotice("Builder candidate created in quarantine.");
      await refreshSkills();
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setSkillsBusy(false);
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
    channelMessageConversationId,
    setChannelMessageConversationId,
    channelMessageThreadId,
    setChannelMessageThreadId,
    channelMessageReadMessageId,
    setChannelMessageReadMessageId,
    channelMessageReadBeforeMessageId,
    setChannelMessageReadBeforeMessageId,
    channelMessageReadAfterMessageId,
    setChannelMessageReadAfterMessageId,
    channelMessageReadAroundMessageId,
    setChannelMessageReadAroundMessageId,
    channelMessageReadLimit,
    setChannelMessageReadLimit,
    channelMessageSearchQuery,
    setChannelMessageSearchQuery,
    channelMessageSearchAuthorId,
    setChannelMessageSearchAuthorId,
    channelMessageSearchHasAttachments,
    setChannelMessageSearchHasAttachments,
    channelMessageSearchBeforeMessageId,
    setChannelMessageSearchBeforeMessageId,
    channelMessageSearchLimit,
    setChannelMessageSearchLimit,
    channelMessageMutationMessageId,
    setChannelMessageMutationMessageId,
    channelMessageMutationApprovalId,
    setChannelMessageMutationApprovalId,
    channelMessageMutationBody,
    setChannelMessageMutationBody,
    channelMessageMutationDeleteReason,
    setChannelMessageMutationDeleteReason,
    channelMessageMutationEmoji,
    setChannelMessageMutationEmoji,
    channelMessageReadResult,
    channelMessageSearchResult,
    channelMessageMutationResult,
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
    readChannelMessages,
    searchChannelMessages,
    editChannelMessage,
    deleteChannelMessage,
    addChannelMessageReaction,
    removeChannelMessageReaction,
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
    memoryLearningBusy,
    memoryLearningCandidates,
    memoryLearningHistory,
    memoryLearningPreferences,
    memoryLearningCandidateId,
    memoryLearningCandidateKindFilter,
    setMemoryLearningCandidateKindFilter,
    memoryLearningStatusFilter,
    setMemoryLearningStatusFilter,
    memoryLearningRiskFilter,
    setMemoryLearningRiskFilter,
    memoryLearningMinConfidenceFilter,
    setMemoryLearningMinConfidenceFilter,
    memoryLearningMaxConfidenceFilter,
    setMemoryLearningMaxConfidenceFilter,
    setMemoryLearningCandidateId,
    memorySearchAllQuery,
    setMemorySearchAllQuery,
    memorySearchAllResults,
    memoryRecallPreview,
    refreshMemoryStatus,
    refreshLearningQueue,
    refreshWorkspaceDocuments,
    searchMemory,
    selectLearningCandidate,
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
    reviewLearningCandidate,
    skillsBusy,
    skillsEntries,
    skillProcedureCandidates,
    skillBuilderCandidates,
    lastSkillPromotion,
    skillArtifactPath,
    setSkillArtifactPath,
    skillAllowTofu,
    setSkillAllowTofu,
    skillAllowUntrusted,
    setSkillAllowUntrusted,
    skillReason,
    setSkillReason,
    skillBuilderPrompt,
    setSkillBuilderPrompt,
    skillBuilderName,
    setSkillBuilderName,
    refreshSkills,
    installSkill,
    executeSkillAction,
    promoteProcedureCandidate,
    createSkillBuilderCandidate,
    ...browserDomain,
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
    overviewUsageInsights,
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
    supportNodePairingMethod,
    setSupportNodePairingMethod,
    supportPairingChannel,
    setSupportPairingChannel,
    supportPairingIssuedBy,
    setSupportPairingIssuedBy,
    supportPairingTtlMs,
    setSupportPairingTtlMs,
    supportNodePairingCodes,
    supportNodePairingRequests,
    supportPairingDecisionReason,
    setSupportPairingDecisionReason,
    supportBundleRetainJobs,
    setSupportBundleRetainJobs,
    supportBundleJobs,
    supportSelectedBundleJobId,
    setSupportSelectedBundleJobId,
    supportSelectedBundleJob,
    supportDoctorRetainJobs,
    setSupportDoctorRetainJobs,
    supportDoctorOnly,
    setSupportDoctorOnly,
    supportDoctorSkip,
    setSupportDoctorSkip,
    supportDoctorRollbackRunId,
    setSupportDoctorRollbackRunId,
    supportDoctorForce,
    setSupportDoctorForce,
    supportDoctorJobs,
    supportSelectedDoctorJobId,
    setSupportSelectedDoctorJobId,
    supportSelectedDoctorJob,
    refreshSupport,
    mintSupportPairingCode,
    approveSupportPairingRequest,
    rejectSupportPairingRequest,
    createSupportBundle,
    loadSupportBundleJob,
    queueDoctorRecoveryPreview,
    queueDoctorRecoveryApply,
    queueDoctorRollbackPreview,
    queueDoctorRollbackApply,
    loadDoctorRecoveryJob,
  };
}

export type ConsoleAppState = ReturnType<typeof useConsoleAppState>;

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
