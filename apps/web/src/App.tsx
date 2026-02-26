
import { useEffect, useMemo, useState } from "react";

import { ConsoleApiClient, type ConsoleSession, type JsonValue } from "./consoleApi";
import { ChatConsolePanel } from "./chat/ChatConsolePanel";

type Section = "chat" | "approvals" | "cron" | "memory" | "skills" | "browser" | "audit" | "diagnostics";
type ThemeMode = "light" | "dark";
type JsonObject = { [key: string]: JsonValue };
type CronScheduleType = "cron" | "every" | "at";

type LoginForm = {
  adminToken: string;
  principal: string;
  deviceId: string;
  channel: string;
};

type CronForm = {
  name: string;
  prompt: string;
  scheduleType: CronScheduleType;
  cronExpression: string;
  everyIntervalMs: string;
  atTimestampRfc3339: string;
  enabled: boolean;
  channel: string;
};

const SENSITIVE_KEY_PATTERN =
  /(secret|token|password|cookie|authorization|credential|api[-_]?key|private[-_]?key|vault[-_]?ref)/i;
const SENSITIVE_VALUE_PATTERN =
  /^(Bearer\s+|sk-[a-z0-9]|ghp_[A-Za-z0-9]|xox[baprs]-|AIza[0-9A-Za-z\-_]{20,})/i;

const DEFAULT_LOGIN_FORM: LoginForm = {
  adminToken: "",
  principal: "admin:web-console",
  deviceId: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
  channel: "web"
};

const DEFAULT_CRON_FORM: CronForm = {
  name: "",
  prompt: "",
  scheduleType: "every",
  cronExpression: "",
  everyIntervalMs: "60000",
  atTimestampRfc3339: "",
  enabled: true,
  channel: ""
};

export function App() {
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
  const [loginForm, setLoginForm] = useState<LoginForm>(DEFAULT_LOGIN_FORM);

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

  const [memoryBusy, setMemoryBusy] = useState(false);
  const [memoryQuery, setMemoryQuery] = useState("");
  const [memoryChannel, setMemoryChannel] = useState("");
  const [memoryPurgeChannel, setMemoryPurgeChannel] = useState("");
  const [memoryPurgeSessionId, setMemoryPurgeSessionId] = useState("");
  const [memoryPurgeAll, setMemoryPurgeAll] = useState(false);
  const [memoryHits, setMemoryHits] = useState<JsonObject[]>([]);

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
        setLoginForm((previous) => ({
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
    if (section === "approvals") {
      void refreshApprovals();
    }
    if (section === "cron") {
      void refreshCron();
    }
    if (section === "skills") {
      void refreshSkills();
    }
    if (section === "browser") {
      void refreshBrowserProfiles();
    }
    if (section === "audit") {
      void refreshAudit();
    }
    if (section === "diagnostics") {
      void refreshDiagnostics();
    }
  }, [section, session]);

  async function signIn(event: React.FormEvent<HTMLFormElement>): Promise<void> {
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
      setSession(next);
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

  if (booting) {
    return (
      <div className="console-root">
        <main className="console-card console-card--center">
          <p className="console-label">Palyra / M39</p>
          <h1>Web Console</h1>
          <p>Checking existing session...</p>
        </main>
      </div>
    );
  }

  if (session === null) {
    return (
      <div className="console-root">
        <main className="console-card console-card--auth">
          <p className="console-label">Palyra / M39</p>
          <h1>Operator Console</h1>
          <p className="console-copy">
            Sign in with an `admin:*` principal. Session cookie + CSRF are required for privileged actions.
          </p>
          <form
            className="console-form"
            onSubmit={(event) => {
              void signIn(event);
            }}
          >
            <label>
              Admin token
              <input
                type="password"
                value={loginForm.adminToken}
                onChange={(event) =>
                  setLoginForm((previous) => ({ ...previous, adminToken: event.target.value }))
                }
              />
            </label>
            <label>
              Principal
              <input
                value={loginForm.principal}
                onChange={(event) =>
                  setLoginForm((previous) => ({ ...previous, principal: event.target.value }))
                }
                required
              />
            </label>
            <label>
              Device ID
              <input
                value={loginForm.deviceId}
                onChange={(event) =>
                  setLoginForm((previous) => ({ ...previous, deviceId: event.target.value }))
                }
                required
              />
            </label>
            <label>
              Channel
              <input
                value={loginForm.channel}
                onChange={(event) =>
                  setLoginForm((previous) => ({ ...previous, channel: event.target.value }))
                }
              />
            </label>
            <button type="submit" disabled={loginBusy}>{loginBusy ? "Signing in..." : "Sign in"}</button>
          </form>
          {error !== null && <p className="console-banner console-banner--error">{error}</p>}
        </main>
      </div>
    );
  }
  return (
    <div className="console-root">
      <header className="console-topbar">
        <div>
          <p className="console-label">Palyra / M39</p>
          <h1>Web Console v1</h1>
          <p className="console-copy">
            Chat streaming, approvals, cron, memory, skills, browser relay controls, and audit workflows without using CLI.
          </p>
        </div>
        <div className="console-session-box">
          <p><strong>Principal:</strong> {session.principal}</p>
          <p><strong>Device:</strong> {session.device_id}</p>
          <p><strong>Channel:</strong> {session.channel ?? "-"}</p>
          <p><strong>Expires:</strong> {new Date(session.expires_at_unix_ms).toLocaleString()}</p>
          <button
            type="button"
            onClick={() => setTheme((current) => (current === "light" ? "dark" : "light"))}
          >
            Theme: {theme}
          </button>
          <button type="button" onClick={() => void signOut()} disabled={logoutBusy}>
            {logoutBusy ? "Signing out..." : "Sign out"}
          </button>
        </div>
      </header>

      <nav className="console-nav" aria-label="Console sections">
        <button type="button" className={section === "chat" ? "is-active" : ""} onClick={() => setSection("chat")}>Chat</button>
        <button type="button" className={section === "approvals" ? "is-active" : ""} onClick={() => setSection("approvals")}>Approvals</button>
        <button type="button" className={section === "cron" ? "is-active" : ""} onClick={() => setSection("cron")}>Cron</button>
        <button type="button" className={section === "memory" ? "is-active" : ""} onClick={() => setSection("memory")}>Memory</button>
        <button type="button" className={section === "skills" ? "is-active" : ""} onClick={() => setSection("skills")}>Skills</button>
        <button type="button" className={section === "browser" ? "is-active" : ""} onClick={() => setSection("browser")}>Browser</button>
        <button type="button" className={section === "audit" ? "is-active" : ""} onClick={() => setSection("audit")}>Audit</button>
        <button type="button" className={section === "diagnostics" ? "is-active" : ""} onClick={() => setSection("diagnostics")}>Diagnostics</button>
      </nav>

      {(error !== null || notice !== null) && (
        <section className="console-banner-row" aria-live="polite">
          {error !== null && <p className="console-banner console-banner--error">{error}</p>}
          {notice !== null && <p className="console-banner console-banner--notice">{notice}</p>}
        </section>
      )}

      <section className="console-guardrail">
        <label className="console-checkbox-inline">
          <input
            type="checkbox"
            checked={revealSensitiveValues}
            onChange={(event) => setRevealSensitiveValues(event.target.checked)}
          />
          Reveal sensitive fields (default is redacted)
        </label>
      </section>

      {section === "chat" && (
        <ChatConsolePanel
          api={api}
          revealSensitiveValues={revealSensitiveValues}
          setError={setError}
          setNotice={setNotice}
        />
      )}

      {section === "approvals" && (
        <main className="console-card">
          <header className="console-card__header">
            <h2>Approvals Inbox</h2>
            <button type="button" onClick={() => void refreshApprovals()} disabled={approvalsBusy}>
              {approvalsBusy ? "Refreshing..." : "Refresh"}
            </button>
          </header>
          <div className="console-table-wrap">
            <table className="console-table">
              <thead>
                <tr>
                  <th>Approval ID</th>
                  <th>Subject</th>
                  <th>Decision</th>
                  <th>Open</th>
                </tr>
              </thead>
              <tbody>
                {approvals.length === 0 && (
                  <tr><td colSpan={4}>No approvals found.</td></tr>
                )}
                {approvals.map((approval) => {
                  const itemId = readString(approval, "approval_id") ?? readString(approval, "id") ?? "(missing)";
                  return (
                    <tr key={itemId}>
                      <td>{itemId}</td>
                      <td>{readString(approval, "subject_type") ?? "-"}</td>
                      <td>{readString(approval, "decision") ?? "pending"}</td>
                      <td>
                        <button type="button" onClick={() => setApprovalId(itemId)}>Select</button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          <section className="console-subpanel">
            <h3>Decision</h3>
            <div className="console-grid-3">
              <label>
                Approval ID
                <input value={approvalId} onChange={(event) => setApprovalId(event.target.value)} />
              </label>
              <label>
                Scope
                <select value={approvalScope} onChange={(event) => setApprovalScope(event.target.value)}>
                  <option value="once">once</option>
                  <option value="session">session</option>
                  <option value="timeboxed">timeboxed</option>
                </select>
              </label>
              <label>
                Reason
                <input value={approvalReason} onChange={(event) => setApprovalReason(event.target.value)} />
              </label>
            </div>
            <div className="console-inline-actions">
              <button type="button" onClick={() => void decideApproval(true)} disabled={approvalsBusy}>Approve</button>
              <button type="button" className="button--warn" onClick={() => void decideApproval(false)} disabled={approvalsBusy}>Deny</button>
            </div>
          </section>
        </main>
      )}

      {section === "cron" && (
        <main className="console-card">
          <header className="console-card__header">
            <h2>Cron Jobs</h2>
            <button type="button" onClick={() => void refreshCron()} disabled={cronBusy}>{cronBusy ? "Refreshing..." : "Refresh"}</button>
          </header>
          <form
            className="console-form"
            onSubmit={(event) => {
              void createCronJob(event);
            }}
          >
            <div className="console-grid-2">
              <label>
                Name
                <input value={cronForm.name} onChange={(event) => setCronForm((previous) => ({ ...previous, name: event.target.value }))} required />
              </label>
              <label>
                Channel
                <input value={cronForm.channel} onChange={(event) => setCronForm((previous) => ({ ...previous, channel: event.target.value }))} />
              </label>
            </div>
            <label>
              Prompt
              <textarea rows={3} value={cronForm.prompt} onChange={(event) => setCronForm((previous) => ({ ...previous, prompt: event.target.value }))} required />
            </label>
            <div className="console-grid-4">
              <label>
                Schedule
                <select value={cronForm.scheduleType} onChange={(event) => setCronForm((previous) => ({ ...previous, scheduleType: event.target.value as CronScheduleType }))}>
                  <option value="every">every</option>
                  <option value="cron">cron</option>
                  <option value="at">at</option>
                </select>
              </label>
              <label>
                Every ms
                <input value={cronForm.everyIntervalMs} onChange={(event) => setCronForm((previous) => ({ ...previous, everyIntervalMs: event.target.value }))} disabled={cronForm.scheduleType !== "every"} />
              </label>
              <label>
                Cron expr
                <input value={cronForm.cronExpression} onChange={(event) => setCronForm((previous) => ({ ...previous, cronExpression: event.target.value }))} disabled={cronForm.scheduleType !== "cron"} />
              </label>
              <label>
                At RFC3339
                <input value={cronForm.atTimestampRfc3339} onChange={(event) => setCronForm((previous) => ({ ...previous, atTimestampRfc3339: event.target.value }))} disabled={cronForm.scheduleType !== "at"} />
              </label>
            </div>
            <button type="submit" disabled={cronBusy}>{cronBusy ? "Creating..." : "Create job"}</button>
          </form>
          <div className="console-table-wrap">
            <table className="console-table">
              <thead>
                <tr>
                  <th>Job ID</th><th>Name</th><th>Enabled</th><th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {cronJobs.length === 0 && <tr><td colSpan={4}>No cron jobs.</td></tr>}
                {cronJobs.map((job) => {
                  const itemId = readString(job, "job_id") ?? "(missing)";
                  const enabled = readBool(job, "enabled");
                  return (
                    <tr key={itemId}>
                      <td>{itemId}</td>
                      <td>{readString(job, "name") ?? "-"}</td>
                      <td>{enabled ? "yes" : "no"}</td>
                      <td className="console-action-cell">
                        <button type="button" onClick={() => void setCronEnabled(job, !enabled)}>{enabled ? "Disable" : "Enable"}</button>
                        <button type="button" onClick={() => void runCronNow(job)}>Run now</button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          <section className="console-subpanel">
            <h3>Run logs {cronJobId.length > 0 ? `for ${cronJobId}` : ""}</h3>
            {cronRuns.length === 0 ? (
              <p>No run logs loaded.</p>
            ) : (
              <pre>{toPrettyJson(cronRuns, revealSensitiveValues)}</pre>
            )}
          </section>
        </main>
      )}
      {section === "memory" && (
        <main className="console-card">
          <header className="console-card__header">
            <h2>Memory</h2>
          </header>
          <form
            className="console-form"
            onSubmit={(event) => {
              void searchMemory(event);
            }}
          >
            <div className="console-grid-2">
              <label>
                Query
                <input value={memoryQuery} onChange={(event) => setMemoryQuery(event.target.value)} required />
              </label>
              <label>
                Channel
                <input value={memoryChannel} onChange={(event) => setMemoryChannel(event.target.value)} />
              </label>
            </div>
            <button type="submit" disabled={memoryBusy}>{memoryBusy ? "Searching..." : "Search"}</button>
          </form>
          <section className="console-subpanel">
            <h3>Results</h3>
            {memoryHits.length === 0 ? <p>No memory hits.</p> : <pre>{toPrettyJson(memoryHits, revealSensitiveValues)}</pre>}
          </section>
          <section className="console-subpanel">
            <h3>Purge</h3>
            <div className="console-grid-3">
              <label>
                Channel
                <input value={memoryPurgeChannel} onChange={(event) => setMemoryPurgeChannel(event.target.value)} />
              </label>
              <label>
                Session ID
                <input value={memoryPurgeSessionId} onChange={(event) => setMemoryPurgeSessionId(event.target.value)} />
              </label>
              <label className="console-checkbox-inline">
                <input type="checkbox" checked={memoryPurgeAll} onChange={(event) => setMemoryPurgeAll(event.target.checked)} />
                Purge all principal memory
              </label>
            </div>
            <button type="button" className="button--warn" onClick={() => void purgeMemory()} disabled={memoryBusy}>
              {memoryBusy ? "Purging..." : "Purge"}
            </button>
          </section>
        </main>
      )}

      {section === "skills" && (
        <main className="console-card">
          <header className="console-card__header">
            <h2>Skills</h2>
            <button type="button" onClick={() => void refreshSkills()} disabled={skillsBusy}>{skillsBusy ? "Refreshing..." : "Refresh"}</button>
          </header>
          <form
            className="console-form"
            onSubmit={(event) => {
              void installSkill(event);
            }}
          >
            <label>
              Artifact path
              <input value={skillArtifactPath} onChange={(event) => setSkillArtifactPath(event.target.value)} placeholder="C:\\skills\\example.palyra-skill" required />
            </label>
            <div className="console-grid-3">
              <label className="console-checkbox-inline">
                <input type="checkbox" checked={skillAllowTofu} onChange={(event) => setSkillAllowTofu(event.target.checked)} />
                Allow TOFU
              </label>
              <label className="console-checkbox-inline">
                <input type="checkbox" checked={skillAllowUntrusted} onChange={(event) => setSkillAllowUntrusted(event.target.checked)} />
                Allow untrusted override
              </label>
              <label>
                Override reason
                <input value={skillReason} onChange={(event) => setSkillReason(event.target.value)} />
              </label>
            </div>
            <button type="submit" disabled={skillsBusy}>{skillsBusy ? "Installing..." : "Install"}</button>
          </form>
          {skillsEntries.length === 0 ? (
            <p>No installed skill records.</p>
          ) : (
            <div className="console-stack">
              {skillsEntries.map((entry, index) => {
                const meta = skillMetadata(entry);
                const label = meta === null ? `entry-${index}` : `${meta.skillId}@${meta.version}`;
                return (
                  <article key={label} className="console-item-card">
                    <h3>{label}</h3>
                    <div className="console-inline-actions">
                      <button type="button" onClick={() => void executeSkillAction(entry, "verify")}>Verify</button>
                      <button type="button" onClick={() => void executeSkillAction(entry, "audit")}>Audit</button>
                      <button type="button" className="button--warn" onClick={() => void executeSkillAction(entry, "quarantine")}>Quarantine</button>
                      <button type="button" onClick={() => void executeSkillAction(entry, "enable")}>Enable</button>
                    </div>
                    <pre>{toPrettyJson(entry, revealSensitiveValues)}</pre>
                  </article>
                );
              })}
            </div>
          )}
        </main>
      )}

      {section === "browser" && (
        <main className="console-card">
          <header className="console-card__header">
            <h2>Browser Profiles + Relay</h2>
            <button type="button" onClick={() => void refreshBrowserProfiles()} disabled={browserBusy}>
              {browserBusy ? "Refreshing..." : "Refresh"}
            </button>
          </header>

          <form
            className="console-form"
            onSubmit={(event) => {
              void createBrowserProfile(event);
            }}
          >
            <div className="console-grid-3">
              <label>
                Principal
                <input value={browserPrincipal} onChange={(event) => setBrowserPrincipal(event.target.value)} />
              </label>
              <label>
                Profile name
                <input value={browserProfileName} onChange={(event) => setBrowserProfileName(event.target.value)} required />
              </label>
              <label>
                Theme color
                <input value={browserProfileTheme} onChange={(event) => setBrowserProfileTheme(event.target.value)} placeholder="#4f46e5" />
              </label>
            </div>
            <div className="console-grid-3">
              <label className="console-checkbox-inline">
                <input type="checkbox" checked={browserProfilePersistence} onChange={(event) => setBrowserProfilePersistence(event.target.checked)} />
                Persistence enabled
              </label>
              <label className="console-checkbox-inline">
                <input type="checkbox" checked={browserProfilePrivate} onChange={(event) => setBrowserProfilePrivate(event.target.checked)} />
                Private profile (never persists)
              </label>
              <button type="submit" disabled={browserBusy}>{browserBusy ? "Creating..." : "Create profile"}</button>
            </div>
          </form>

          <div className="console-table-wrap">
            <table className="console-table">
              <thead>
                <tr>
                  <th>Profile ID</th>
                  <th>Name</th>
                  <th>Theme</th>
                  <th>Persistence</th>
                  <th>Private</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {browserProfiles.length === 0 && (
                  <tr><td colSpan={6}>No browser profiles available.</td></tr>
                )}
                {browserProfiles.map((profile) => {
                  const profileId = readString(profile, "profile_id") ?? "(missing)";
                  const active = profileId === browserActiveProfileId || readBool(profile, "active");
                  const persistence = readBool(profile, "persistence_enabled");
                  const privateProfile = readBool(profile, "private_profile");
                  return (
                    <tr key={profileId}>
                      <td>{profileId}</td>
                      <td>{readString(profile, "name") ?? "-"}</td>
                      <td>{readString(profile, "theme_color") ?? "-"}</td>
                      <td>{persistence ? "yes" : "no"}</td>
                      <td>{privateProfile ? "yes" : "no"}</td>
                      <td className="console-action-cell">
                        <button type="button" onClick={() => void activateBrowserProfile(profile)} disabled={browserBusy}>
                          {active ? "Active" : "Set active"}
                        </button>
                        <button
                          type="button"
                          onClick={() => {
                            setBrowserRenameProfileId(profileId);
                            setBrowserRenameName(readString(profile, "name") ?? "");
                          }}
                        >
                          Rename
                        </button>
                        <button type="button" className="button--warn" onClick={() => void deleteBrowserProfile(profile)} disabled={browserBusy}>
                          Delete
                        </button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>

          <section className="console-subpanel">
            <h3>Rename profile</h3>
            <div className="console-grid-3">
              <label>
                Profile ID
                <input value={browserRenameProfileId} onChange={(event) => setBrowserRenameProfileId(event.target.value)} />
              </label>
              <label>
                New name
                <input value={browserRenameName} onChange={(event) => setBrowserRenameName(event.target.value)} />
              </label>
              <button type="button" onClick={() => void renameBrowserProfile()} disabled={browserBusy}>
                {browserBusy ? "Renaming..." : "Rename profile"}
              </button>
            </div>
          </section>

          <section className="console-subpanel">
            <h3>Extension relay token</h3>
            <p>
              Relay token is short-lived and scoped to one browser session + extension ID. Treat token as secret.
            </p>
            <div className="console-grid-3">
              <label>
                Session ID
                <input value={browserRelaySessionId} onChange={(event) => setBrowserRelaySessionId(event.target.value)} placeholder="01ARZ3NDEKTSV4RRFFQ69G5FAV" />
              </label>
              <label>
                Extension ID
                <input value={browserRelayExtensionId} onChange={(event) => setBrowserRelayExtensionId(event.target.value)} />
              </label>
              <label>
                TTL ms
                <input value={browserRelayTtlMs} onChange={(event) => setBrowserRelayTtlMs(event.target.value)} />
              </label>
            </div>
            <div className="console-inline-actions">
              <button type="button" onClick={() => void mintBrowserRelayToken()} disabled={browserBusy}>
                {browserBusy ? "Minting..." : "Mint relay token"}
              </button>
            </div>
            {browserRelayToken.length > 0 && (
              <div className="console-stack">
                <p><strong>Token expires:</strong> {browserRelayTokenExpiry === null ? "-" : new Date(browserRelayTokenExpiry).toLocaleString()}</p>
                <pre>{toPrettyJson({ relay_token: browserRelayToken }, revealSensitiveValues)}</pre>
              </div>
            )}
          </section>

          <section className="console-subpanel">
            <h3>Relay action dispatch</h3>
            <div className="console-grid-3">
              <label>
                Action
                <select value={browserRelayAction} onChange={(event) => setBrowserRelayAction(event.target.value as "open_tab" | "capture_selection" | "send_page_snapshot")}>
                  <option value="capture_selection">capture_selection</option>
                  <option value="open_tab">open_tab</option>
                  <option value="send_page_snapshot">send_page_snapshot</option>
                </select>
              </label>
              <label>
                Open tab URL
                <input value={browserRelayOpenTabUrl} onChange={(event) => setBrowserRelayOpenTabUrl(event.target.value)} disabled={browserRelayAction !== "open_tab"} />
              </label>
              <label>
                Selector
                <input value={browserRelaySelector} onChange={(event) => setBrowserRelaySelector(event.target.value)} disabled={browserRelayAction !== "capture_selection"} />
              </label>
            </div>
            <div className="console-inline-actions">
              <button type="button" onClick={() => void dispatchBrowserRelayAction()} disabled={browserBusy}>
                {browserBusy ? "Dispatching..." : "Dispatch relay action"}
              </button>
            </div>
            {browserRelayResult !== null && (
              <pre>{toPrettyJson(browserRelayResult, revealSensitiveValues)}</pre>
            )}
          </section>

          <section className="console-subpanel">
            <h3>Download artifacts</h3>
            <div className="console-grid-3">
              <label>
                Session ID
                <input value={browserDownloadsSessionId} onChange={(event) => setBrowserDownloadsSessionId(event.target.value)} />
              </label>
              <label className="console-checkbox-inline">
                <input type="checkbox" checked={browserDownloadsQuarantinedOnly} onChange={(event) => setBrowserDownloadsQuarantinedOnly(event.target.checked)} />
                Quarantined only
              </label>
              <button type="button" onClick={() => void refreshBrowserDownloads()} disabled={browserBusy}>
                {browserBusy ? "Loading..." : "Load downloads"}
              </button>
            </div>
            {browserDownloads.length === 0 ? (
              <p>No download artifacts loaded.</p>
            ) : (
              <pre>{toPrettyJson(browserDownloads, revealSensitiveValues)}</pre>
            )}
          </section>
        </main>
      )}

      {section === "audit" && (
        <main className="console-card">
          <header className="console-card__header">
            <h2>Audit Explorer</h2>
            <button type="button" onClick={() => void refreshAudit()} disabled={auditBusy}>{auditBusy ? "Refreshing..." : "Refresh"}</button>
          </header>
          <div className="console-grid-2">
            <label>
              Principal filter
              <input value={auditFilterPrincipal} onChange={(event) => setAuditFilterPrincipal(event.target.value)} />
            </label>
            <label>
              Payload contains
              <input value={auditFilterContains} onChange={(event) => setAuditFilterContains(event.target.value)} />
            </label>
          </div>
          {auditEvents.length === 0 ? <p>No audit events loaded.</p> : <pre>{toPrettyJson(auditEvents, revealSensitiveValues)}</pre>}
        </main>
      )}

      {section === "diagnostics" && (
        <main className="console-card">
          <header className="console-card__header">
            <h2>Diagnostics</h2>
            <button type="button" onClick={() => void refreshDiagnostics()} disabled={diagnosticsBusy}>
              {diagnosticsBusy ? "Refreshing..." : "Refresh"}
            </button>
          </header>
          {diagnosticsSnapshot === null ? (
            <p>No diagnostics loaded.</p>
          ) : (
            <>
              <section className="console-subpanel">
                <h3>Model Provider + Rate Limits</h3>
                <pre>
                  {toPrettyJson(
                    {
                      model_provider: diagnosticsSnapshot["model_provider"] ?? null,
                      rate_limits: diagnosticsSnapshot["rate_limits"] ?? null
                    },
                    revealSensitiveValues
                  )}
                </pre>
              </section>
              <section className="console-subpanel">
                <h3>Auth Profile Health</h3>
                <pre>{toPrettyJson(diagnosticsSnapshot["auth_profiles"] ?? null, revealSensitiveValues)}</pre>
              </section>
              <section className="console-subpanel">
                <h3>Browserd Status</h3>
                <pre>{toPrettyJson(diagnosticsSnapshot["browserd"] ?? null, revealSensitiveValues)}</pre>
              </section>
            </>
          )}
        </main>
      )}
    </div>
  );
}

function toErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return "Unexpected failure.";
}

function isJsonObject(value: JsonValue): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function toJsonObjectArray(values: JsonValue[]): JsonObject[] {
  const rows: JsonObject[] = [];
  for (const value of values) {
    if (isJsonObject(value)) {
      rows.push(value);
    }
  }
  return rows;
}

function readString(record: JsonObject, key: string): string | null {
  const value = record[key];
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  return null;
}

function readBool(record: JsonObject, key: string): boolean {
  return record[key] === true;
}

function parseInteger(raw: string): number | null {
  const trimmed = raw.trim();
  if (trimmed.length === 0) {
    return null;
  }
  const parsed = Number.parseInt(trimmed, 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function emptyToUndefined(raw: string): string | undefined {
  const trimmed = raw.trim();
  return trimmed.length === 0 ? undefined : trimmed;
}

function skillMetadata(entry: JsonObject): { skillId: string; version: string } | null {
  const record = entry.record;
  if (!isJsonObject(record)) {
    return null;
  }
  const skillId = readString(record, "skill_id");
  const version = readString(record, "version");
  if (skillId === null || version === null) {
    return null;
  }
  return { skillId, version };
}

function redactValue(value: JsonValue, revealSensitive: boolean): JsonValue {
  if (revealSensitive) {
    return value;
  }
  if (typeof value === "string") {
    return SENSITIVE_VALUE_PATTERN.test(value) ? "[redacted]" : value;
  }
  if (Array.isArray(value)) {
    return value.map((entry) => redactValue(entry, false));
  }
  if (isJsonObject(value)) {
    const sanitized: JsonObject = {};
    for (const [key, item] of Object.entries(value)) {
      sanitized[key] = SENSITIVE_KEY_PATTERN.test(key) ? "[redacted]" : redactValue(item, false);
    }
    return sanitized;
  }
  return value;
}

function toPrettyJson(value: JsonValue, revealSensitive: boolean): string {
  return JSON.stringify(redactValue(value, revealSensitive), null, 2);
}
