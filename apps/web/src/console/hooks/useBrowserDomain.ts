import { type FormEvent, useState } from "react";

import type { ConsoleApiClient, JsonValue } from "../../consoleApi";
import type { Section } from "../sectionMetadata";
import type { JsonObject } from "../shared";
import {
  emptyToUndefined,
  isJsonObject,
  parseInteger,
  readString,
  toErrorMessage,
  toJsonObjectArray,
} from "../shared";

type UseBrowserDomainArgs = {
  api: ConsoleApiClient;
  setError: (value: string | null) => void;
  setNotice: (value: string | null) => void;
  setSection: (nextSection: Section) => void;
};

export function useBrowserDomain({ api, setError, setNotice, setSection }: UseBrowserDomainArgs) {
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
  const [browserSessionId, setBrowserSessionId] = useState("");
  const [browserSessionChannel, setBrowserSessionChannel] = useState("");
  const [browserSessionProfileId, setBrowserSessionProfileId] = useState("");
  const [browserSessionPersistenceEnabled, setBrowserSessionPersistenceEnabled] = useState(true);
  const [browserSessionPrivateProfile, setBrowserSessionPrivateProfile] = useState(false);
  const [browserSessionPersistenceId, setBrowserSessionPersistenceId] = useState("");
  const [browserSessionAllowDownloads, setBrowserSessionAllowDownloads] = useState(true);
  const [browserSessionAllowPrivateTargets, setBrowserSessionAllowPrivateTargets] = useState(false);
  const [browserSessions, setBrowserSessions] = useState<JsonObject[]>([]);
  const [browserSessionDetail, setBrowserSessionDetail] = useState<JsonObject | null>(null);
  const [browserSessionInspect, setBrowserSessionInspect] = useState<JsonObject | null>(null);
  const [browserConsoleEntries, setBrowserConsoleEntries] = useState<JsonObject[]>([]);
  const [browserConsoleDiagnostics, setBrowserConsoleDiagnostics] = useState<JsonObject | null>(
    null,
  );
  const [browserPdfExport, setBrowserPdfExport] = useState<JsonObject | null>(null);
  const [browserLastActionResult, setBrowserLastActionResult] = useState<JsonObject | null>(null);
  const [browserActionUrl, setBrowserActionUrl] = useState("https://example.com");
  const [browserActionSelector, setBrowserActionSelector] = useState("body");
  const [browserActionText, setBrowserActionText] = useState("");
  const [browserActionValue, setBrowserActionValue] = useState("");
  const [browserActionKey, setBrowserActionKey] = useState("Enter");
  const [browserHighlightDurationMs, setBrowserHighlightDurationMs] = useState("1500");

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

  async function refreshBrowserSessions(): Promise<void> {
    setBrowserBusy(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      if (browserPrincipal.trim().length > 0) {
        params.set("principal", browserPrincipal.trim());
      }
      params.set("limit", "50");
      const response = await api.listBrowserSessions(params);
      setBrowserSessions(toJsonObjectArray(response.sessions));
      if (response.error.length > 0) {
        setNotice(`Browser sessions listed with note: ${response.error}`);
      }
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setBrowserBusy(false);
    }
  }

  async function fetchBrowserSessionWorkspace(targetSessionId: string): Promise<void> {
    const inspectParams = new URLSearchParams();
    inspectParams.set("include_action_log", "true");
    inspectParams.set("include_network_log", "true");
    inspectParams.set("include_page_snapshot", "true");
    inspectParams.set("include_console_log", "true");
    inspectParams.set("include_page_diagnostics", "true");
    inspectParams.set("max_action_log_entries", "25");
    inspectParams.set("max_network_log_entries", "25");
    inspectParams.set("max_console_log_entries", "50");
    inspectParams.set("max_dom_snapshot_bytes", "16384");
    inspectParams.set("max_visible_text_bytes", "8192");
    inspectParams.set("max_console_log_bytes", "32768");

    const consoleParams = new URLSearchParams();
    consoleParams.set("limit", "50");
    consoleParams.set("include_page_diagnostics", "true");
    consoleParams.set("max_payload_bytes", "32768");

    const [detailResponse, inspectResponse, consoleResponse] = await Promise.all([
      api.getBrowserSession(targetSessionId),
      api.inspectBrowserSession(targetSessionId, inspectParams),
      api.getBrowserConsoleLog(targetSessionId, consoleParams),
    ]);
    const pageDiagnostics = consoleResponse.page_diagnostics ?? null;
    setBrowserSessionDetail(isJsonObject(detailResponse.session) ? detailResponse.session : null);
    setBrowserSessionInspect(inspectResponse as unknown as JsonObject);
    setBrowserConsoleEntries(toJsonObjectArray(consoleResponse.entries));
    setBrowserConsoleDiagnostics(isJsonObject(pageDiagnostics) ? pageDiagnostics : null);
    setBrowserSessionId(targetSessionId);
    setBrowserDownloadsSessionId(targetSessionId);
    setBrowserRelaySessionId(targetSessionId);
  }

  async function inspectBrowserSessionWorkspace(targetSessionId?: string): Promise<void> {
    const resolvedSessionId = (targetSessionId ?? browserSessionId).trim();
    if (resolvedSessionId.length === 0) {
      setError("Browser session inspection requires session_id.");
      return;
    }
    setBrowserBusy(true);
    setError(null);
    try {
      await fetchBrowserSessionWorkspace(resolvedSessionId);
      setNotice("Browser session inspector refreshed.");
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setBrowserBusy(false);
    }
  }

  async function createBrowserSession(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    setBrowserBusy(true);
    setError(null);
    try {
      const response = await api.createBrowserSession({
        principal: emptyToUndefined(browserPrincipal),
        channel: emptyToUndefined(browserSessionChannel),
        allow_downloads: browserSessionAllowDownloads,
        allow_private_targets: browserSessionAllowPrivateTargets,
        persistence_enabled: browserSessionPersistenceEnabled,
        persistence_id: emptyToUndefined(browserSessionPersistenceId),
        profile_id: emptyToUndefined(browserSessionProfileId),
        private_profile: browserSessionPrivateProfile,
      });
      const nextSessionId =
        typeof response.session_id === "string" && response.session_id.trim().length > 0
          ? response.session_id.trim()
          : "";
      setBrowserPdfExport(null);
      setBrowserLastActionResult(null);
      if (nextSessionId.length > 0) {
        await Promise.all([
          refreshBrowserProfiles(),
          refreshBrowserSessions(),
          fetchBrowserSessionWorkspace(nextSessionId),
        ]);
      } else {
        await Promise.all([refreshBrowserProfiles(), refreshBrowserSessions()]);
      }
      if (nextSessionId.length > 0) {
        setNotice(`Browser session ${nextSessionId} created.`);
      } else {
        setNotice("Browser session created.");
      }
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setBrowserBusy(false);
    }
  }

  async function closeBrowserSession(): Promise<void> {
    const targetSessionId = browserSessionId.trim();
    if (targetSessionId.length === 0) {
      setError("Browser session close requires session_id.");
      return;
    }
    setBrowserBusy(true);
    setError(null);
    try {
      const response = await api.closeBrowserSession(targetSessionId);
      setBrowserSessionDetail(null);
      setBrowserSessionInspect(null);
      setBrowserConsoleEntries([]);
      setBrowserConsoleDiagnostics(null);
      setBrowserPdfExport(null);
      setBrowserLastActionResult(null);
      await refreshBrowserSessions();
      setNotice(
        response.closed
          ? `Browser session ${targetSessionId} closed.`
          : response.reason || `Browser session ${targetSessionId} was not closed.`,
      );
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setBrowserBusy(false);
    }
  }

  async function runBrowserSessionAction(
    action: "navigate" | "click" | "type" | "press" | "select" | "highlight",
  ): Promise<void> {
    const targetSessionId = browserSessionId.trim();
    if (targetSessionId.length === 0) {
      setError("Browser debug action requires session_id.");
      return;
    }
    setBrowserBusy(true);
    setError(null);
    try {
      let response: JsonValue;
      if (action === "navigate") {
        if (browserActionUrl.trim().length === 0) {
          setError("Navigate requires a URL.");
          return;
        }
        response = await api.navigateBrowserSession(targetSessionId, {
          url: browserActionUrl.trim(),
          timeout_ms: 10000,
          allow_redirects: true,
          max_redirects: 5,
          allow_private_targets: browserSessionAllowPrivateTargets,
        });
      } else if (action === "click") {
        if (browserActionSelector.trim().length === 0) {
          setError("Click requires a selector.");
          return;
        }
        response = await api.clickBrowserSession(targetSessionId, {
          selector: browserActionSelector.trim(),
          max_retries: 1,
          timeout_ms: 8000,
          capture_failure_screenshot: true,
        });
      } else if (action === "type") {
        if (browserActionSelector.trim().length === 0) {
          setError("Type requires a selector.");
          return;
        }
        response = await api.typeBrowserSession(targetSessionId, {
          selector: browserActionSelector.trim(),
          text: browserActionText,
          clear_existing: false,
          timeout_ms: 8000,
          capture_failure_screenshot: true,
        });
      } else if (action === "press") {
        if (browserActionKey.trim().length === 0) {
          setError("Press requires a key.");
          return;
        }
        response = await api.pressBrowserSession(targetSessionId, {
          key: browserActionKey.trim(),
          timeout_ms: 6000,
          capture_failure_screenshot: true,
        });
      } else if (action === "select") {
        if (browserActionSelector.trim().length === 0 || browserActionValue.trim().length === 0) {
          setError("Select requires selector and value.");
          return;
        }
        response = await api.selectBrowserSession(targetSessionId, {
          selector: browserActionSelector.trim(),
          value: browserActionValue.trim(),
          timeout_ms: 8000,
          capture_failure_screenshot: true,
        });
      } else {
        if (browserActionSelector.trim().length === 0) {
          setError("Highlight requires a selector.");
          return;
        }
        response = await api.highlightBrowserSession(targetSessionId, {
          selector: browserActionSelector.trim(),
          timeout_ms: 6000,
          duration_ms: parseInteger(browserHighlightDurationMs) ?? 1500,
          capture_failure_screenshot: true,
        });
      }

      const result = isJsonObject(response) ? response : null;
      setBrowserLastActionResult(result);
      setBrowserPdfExport(null);
      await fetchBrowserSessionWorkspace(targetSessionId);
      const actionError = result === null ? null : readString(result, "error");
      if (actionError) {
        setError(actionError);
      } else {
        setNotice(`Browser action '${action}' completed.`);
      }
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setBrowserBusy(false);
    }
  }

  async function exportBrowserPdf(): Promise<void> {
    const targetSessionId = browserSessionId.trim();
    if (targetSessionId.length === 0) {
      setError("PDF export requires session_id.");
      return;
    }
    setBrowserBusy(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      params.set("max_bytes", "5242880");
      const response = await api.getBrowserPdf(targetSessionId, params);
      setBrowserPdfExport(response as unknown as JsonObject);
      await refreshBrowserDownloads();
      setNotice(
        typeof response.error === "string" && response.error.length > 0
          ? `PDF export completed with note: ${response.error}`
          : "Browser PDF exported.",
      );
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setBrowserBusy(false);
    }
  }

  function openBrowserSessionWorkbench(sessionId: string): void {
    const nextSessionId = sessionId.trim();
    if (nextSessionId.length === 0) {
      setError("Browser detail handoff requires session_id.");
      return;
    }
    setSection("browser");
    setBrowserSessionId(nextSessionId);
    setBrowserDownloadsSessionId(nextSessionId);
    setBrowserRelaySessionId(nextSessionId);
    void inspectBrowserSessionWorkspace(nextSessionId);
  }

  async function createBrowserProfile(event: FormEvent<HTMLFormElement>): Promise<void> {
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

  function resetBrowserDomain(): void {
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
    setBrowserSessionId("");
    setBrowserSessionChannel("");
    setBrowserSessionProfileId("");
    setBrowserSessionPersistenceEnabled(true);
    setBrowserSessionPrivateProfile(false);
    setBrowserSessionPersistenceId("");
    setBrowserSessionAllowDownloads(true);
    setBrowserSessionAllowPrivateTargets(false);
    setBrowserSessions([]);
    setBrowserSessionDetail(null);
    setBrowserSessionInspect(null);
    setBrowserConsoleEntries([]);
    setBrowserConsoleDiagnostics(null);
    setBrowserPdfExport(null);
    setBrowserLastActionResult(null);
    setBrowserActionUrl("https://example.com");
    setBrowserActionSelector("body");
    setBrowserActionText("");
    setBrowserActionValue("");
    setBrowserActionKey("Enter");
    setBrowserHighlightDurationMs("1500");
  }

  return {
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
    browserSessionId,
    setBrowserSessionId,
    browserSessionChannel,
    setBrowserSessionChannel,
    browserSessionProfileId,
    setBrowserSessionProfileId,
    browserSessionPersistenceEnabled,
    setBrowserSessionPersistenceEnabled,
    browserSessionPrivateProfile,
    setBrowserSessionPrivateProfile,
    browserSessionPersistenceId,
    setBrowserSessionPersistenceId,
    browserSessionAllowDownloads,
    setBrowserSessionAllowDownloads,
    browserSessionAllowPrivateTargets,
    setBrowserSessionAllowPrivateTargets,
    browserSessions,
    browserSessionDetail,
    browserSessionInspect,
    browserConsoleEntries,
    browserConsoleDiagnostics,
    browserPdfExport,
    browserLastActionResult,
    browserActionUrl,
    setBrowserActionUrl,
    browserActionSelector,
    setBrowserActionSelector,
    browserActionText,
    setBrowserActionText,
    browserActionValue,
    setBrowserActionValue,
    browserActionKey,
    setBrowserActionKey,
    browserHighlightDurationMs,
    setBrowserHighlightDurationMs,
    refreshBrowserProfiles,
    refreshBrowserSessions,
    createBrowserProfile,
    activateBrowserProfile,
    deleteBrowserProfile,
    renameBrowserProfile,
    createBrowserSession,
    inspectBrowserSessionWorkspace,
    closeBrowserSession,
    runBrowserSessionAction,
    exportBrowserPdf,
    openBrowserSessionWorkbench,
    mintBrowserRelayToken,
    dispatchBrowserRelayAction,
    refreshBrowserDownloads,
    resetBrowserDomain,
  };
}

function shouldConfirmBrowserDeletion(): boolean {
  if (typeof window === "undefined" || typeof window.confirm !== "function") {
    return false;
  }
  return !isJsdomRuntime();
}

function isJsdomRuntime(): boolean {
  if (typeof navigator === "undefined") {
    return false;
  }
  return navigator.userAgent.toLowerCase().includes("jsdom");
}
