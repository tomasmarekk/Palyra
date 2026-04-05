import { invoke } from "@tauri-apps/api/core";

export type RuntimeStatus = "healthy" | "degraded" | "down";
export type JsonValue =
  | string
  | number
  | boolean
  | null
  | { [key: string]: JsonValue }
  | JsonValue[];

export type ActionResult = {
  ok: boolean;
  message: string;
};

export type DesktopCompanionSection = "home" | "chat" | "approvals" | "access" | "onboarding";

export type DesktopCompanionNotification = {
  notification_id: string;
  kind: "approval" | "connection" | "run" | "draft" | "trust";
  title: string;
  detail: string;
  created_at_unix_ms: number;
  read: boolean;
};

export type DesktopCompanionOfflineDraft = {
  draft_id: string;
  session_id?: string;
  text: string;
  reason: string;
  created_at_unix_ms: number;
};

export type DesktopCompanionRollout = {
  companion_shell_enabled: boolean;
  desktop_notifications_enabled: boolean;
  offline_drafts_enabled: boolean;
  release_channel: string;
};

export type DesktopCompanionPreferences = {
  active_section: DesktopCompanionSection;
  active_session_id?: string;
  active_device_id?: string;
  last_run_id?: string;
};

export type ChatSessionRecord = {
  session_id: string;
  session_key: string;
  session_label?: string;
  principal: string;
  device_id: string;
  channel?: string;
  created_at_unix_ms: number;
  updated_at_unix_ms: number;
  last_run_id?: string;
};

export type SessionCatalogRecord = ChatSessionRecord & {
  title: string;
  title_source: string;
  preview?: string;
  preview_state: string;
  last_intent?: string;
  last_intent_state: string;
  last_summary?: string;
  last_summary_state: string;
  branch_state: string;
  parent_session_id?: string;
  last_run_state?: string;
  last_run_started_at_unix_ms?: number;
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
  archived: boolean;
  archived_at_unix_ms?: number;
  pending_approvals: number;
};

export type SessionCatalogSummary = {
  active_sessions: number;
  archived_sessions: number;
  sessions_with_pending_approvals: number;
  sessions_with_active_runs: number;
};

export type DesktopTranscriptRecord = {
  session_id: string;
  run_id: string;
  seq: number;
  event_type: string;
  payload_json: string;
  created_at_unix_ms: number;
  origin_kind: string;
  origin_run_id?: string;
};

export type DesktopQueuedInputRecord = {
  queued_input_id: string;
  run_id: string;
  session_id: string;
  state: string;
  text: string;
  created_at_unix_ms: number;
  updated_at_unix_ms: number;
  origin_run_id?: string;
};

export type DesktopSessionTranscriptEnvelope = {
  session: SessionCatalogRecord;
  records: DesktopTranscriptRecord[];
  queued_inputs: DesktopQueuedInputRecord[];
  runs: JsonValue[];
  background_tasks: JsonValue[];
};

export type DesktopCompanionSendMessageResult = {
  queued_offline: boolean;
  queued_draft_id?: string;
  run_id?: string;
  status?: string;
  message: string;
};

export type InventoryCapabilityRecord = {
  name: string;
  available: boolean;
  summary?: string;
};

export type InventoryCapabilitySummary = {
  total: number;
  available: number;
  unavailable: number;
};

export type InventoryActionAvailability = {
  can_rotate: boolean;
  can_revoke: boolean;
  can_remove: boolean;
  can_invoke: boolean;
};

export type InventoryDeviceRecord = {
  device_id: string;
  client_kind: string;
  device_status: string;
  trust_state: string;
  presence_state: string;
  paired_at_unix_ms: number;
  updated_at_unix_ms: number;
  registered_at_unix_ms?: number;
  last_seen_at_unix_ms?: number;
  heartbeat_age_ms?: number;
  latest_session_id?: string;
  pending_pairings: number;
  issued_by: string;
  approval_id: string;
  identity_fingerprint: string;
  transcript_hash_hex: string;
  current_certificate_fingerprint?: string;
  certificate_fingerprint_history: string[];
  platform?: string;
  capabilities: InventoryCapabilityRecord[];
  capability_summary: InventoryCapabilitySummary;
  last_event_name?: string;
  last_event_at_unix_ms?: number;
  current_certificate_expires_at_unix_ms?: number;
  revoked_reason?: string;
  warnings: string[];
  actions: InventoryActionAvailability;
};

export type InventorySummary = {
  devices: number;
  trusted_devices: number;
  pending_pairings: number;
  ok_devices: number;
  stale_devices: number;
  degraded_devices: number;
  offline_devices: number;
  ok_instances: number;
  stale_instances: number;
  degraded_instances: number;
  offline_instances: number;
};

export type InventoryListEnvelope = {
  generated_at_unix_ms: number;
  summary: InventorySummary;
  devices: InventoryDeviceRecord[];
};

export type OnboardingStatusSnapshot = {
  phase: string;
  current_step_title: string;
  current_step_detail: string;
  progress_completed: number;
  progress_total: number;
  dashboard_handoff_completed: boolean;
  completion_unix_ms?: number;
  recovery?: {
    message: string;
  } | null;
};

export type OpenAiAuthStatusSnapshot = {
  ready: boolean;
  state?: string;
  note?: string;
  default_profile_id?: string;
};

export type DesktopCompanionMetrics = {
  unread_notifications: number;
  pending_approvals: number;
  queued_offline_drafts: number;
  active_sessions: number;
  sessions_with_active_runs: number;
  trusted_devices: number;
  stale_devices: number;
};

export type DesktopCompanionSnapshot = {
  generated_at_unix_ms: number;
  control_center: ControlCenterSnapshot;
  onboarding: OnboardingStatusSnapshot;
  openai_status: OpenAiAuthStatusSnapshot;
  connection_state: "connected" | "reconnecting" | "offline" | string;
  rollout: DesktopCompanionRollout;
  preferences: DesktopCompanionPreferences;
  notifications: DesktopCompanionNotification[];
  offline_drafts: DesktopCompanionOfflineDraft[];
  session_catalog: SessionCatalogRecord[];
  session_summary?: SessionCatalogSummary;
  approvals: JsonValue[];
  inventory?: InventoryListEnvelope;
  warnings: string[];
  metrics: DesktopCompanionMetrics;
};

export type BrowserServiceSnapshot = {
  enabled: boolean;
  healthy: boolean;
  status: string;
  uptime_seconds: number | null;
  last_error: string | null;
};

export type QuickFactsSnapshot = {
  dashboard_url: string;
  dashboard_access_mode: string;
  gateway_version: string | null;
  gateway_git_hash: string | null;
  gateway_uptime_seconds: number | null;
  browser_service: BrowserServiceSnapshot;
};

export type DiagnosticsSnapshot = {
  generated_at_unix_ms: number | null;
  errors: string[];
  dropped_log_events_total: number;
};

export type ServiceProcessSnapshot = {
  service: string;
  desired_running: boolean;
  running: boolean;
  liveness: string;
  pid: number | null;
  last_start_unix_ms: number | null;
  last_exit: string | null;
  restart_attempt: number;
  next_restart_unix_ms: number | null;
  bound_ports: number[];
};

export type ControlCenterSnapshot = {
  generated_at_unix_ms: number;
  overall_status: RuntimeStatus;
  quick_facts: QuickFactsSnapshot;
  diagnostics: DiagnosticsSnapshot;
  gateway_process: ServiceProcessSnapshot;
  browserd_process: ServiceProcessSnapshot;
  warnings: string[];
};

type DesktopGlobal = typeof globalThis & {
  __TAURI__?: unknown;
  __TAURI_INTERNALS__?: unknown;
};

export const DESKTOP_PREVIEW_SNAPSHOT: ControlCenterSnapshot = {
  generated_at_unix_ms: Date.UTC(2026, 2, 13, 12, 0, 0),
  overall_status: "healthy",
  quick_facts: {
    dashboard_url: "http://127.0.0.1:7142/",
    dashboard_access_mode: "local",
    gateway_version: "preview",
    gateway_git_hash: "preview",
    gateway_uptime_seconds: 142,
    browser_service: {
      enabled: true,
      healthy: true,
      status: "running",
      uptime_seconds: 141,
      last_error: null,
    },
  },
  diagnostics: {
    generated_at_unix_ms: Date.UTC(2026, 2, 13, 12, 0, 0),
    errors: [],
    dropped_log_events_total: 0,
  },
  gateway_process: {
    service: "gateway",
    desired_running: true,
    running: true,
    liveness: "running",
    pid: 7142,
    last_start_unix_ms: Date.UTC(2026, 2, 13, 11, 58, 0),
    last_exit: null,
    restart_attempt: 0,
    next_restart_unix_ms: null,
    bound_ports: [7142, 7152],
  },
  browserd_process: {
    service: "browserd",
    desired_running: true,
    running: true,
    liveness: "running",
    pid: 7242,
    last_start_unix_ms: Date.UTC(2026, 2, 13, 11, 58, 4),
    last_exit: null,
    restart_attempt: 0,
    next_restart_unix_ms: null,
    bound_ports: [9222],
  },
  warnings: [],
};

export const DESKTOP_PREVIEW_COMPANION_SNAPSHOT: DesktopCompanionSnapshot = {
  generated_at_unix_ms: DESKTOP_PREVIEW_SNAPSHOT.generated_at_unix_ms,
  control_center: DESKTOP_PREVIEW_SNAPSHOT,
  onboarding: {
    phase: "home",
    current_step_title: "Desktop companion preview",
    current_step_detail:
      "Preview mode is active, so companion data is coming from the local preview snapshot.",
    progress_completed: 5,
    progress_total: 5,
    dashboard_handoff_completed: true,
    completion_unix_ms: DESKTOP_PREVIEW_SNAPSHOT.generated_at_unix_ms,
    recovery: null,
  },
  openai_status: {
    ready: true,
    state: "connected",
    note: "Preview profile is connected.",
    default_profile_id: "preview",
  },
  connection_state: "connected",
  rollout: {
    companion_shell_enabled: true,
    desktop_notifications_enabled: true,
    offline_drafts_enabled: true,
    release_channel: "preview",
  },
  preferences: {
    active_section: "home",
    active_session_id: "preview-session",
    active_device_id: "preview-device",
    last_run_id: "preview-run",
  },
  notifications: [
    {
      notification_id: "preview-note",
      kind: "run",
      title: "Preview run completed",
      detail: "Desktop companion preview data keeps the notifications rail populated.",
      created_at_unix_ms: DESKTOP_PREVIEW_SNAPSHOT.generated_at_unix_ms,
      read: false,
    },
  ],
  offline_drafts: [],
  session_catalog: [
    {
      session_id: "preview-session",
      session_key: "preview",
      session_label: "Preview conversation",
      principal: "admin:desktop-control-center",
      device_id: "preview-device",
      channel: "desktop",
      created_at_unix_ms: DESKTOP_PREVIEW_SNAPSHOT.generated_at_unix_ms - 30_000,
      updated_at_unix_ms: DESKTOP_PREVIEW_SNAPSHOT.generated_at_unix_ms,
      last_run_id: "preview-run",
      title: "Preview conversation",
      title_source: "label",
      preview: "Companion preview keeps session context and approvals in one desktop shell.",
      preview_state: "ready",
      last_intent: "daily companion flow",
      last_intent_state: "ready",
      last_summary: "Preview state is healthy.",
      last_summary_state: "ready",
      branch_state: "root",
      last_run_state: "completed",
      last_run_started_at_unix_ms: DESKTOP_PREVIEW_SNAPSHOT.generated_at_unix_ms - 10_000,
      prompt_tokens: 120,
      completion_tokens: 340,
      total_tokens: 460,
      archived: false,
      pending_approvals: 1,
    },
  ],
  session_summary: {
    active_sessions: 1,
    archived_sessions: 0,
    sessions_with_pending_approvals: 1,
    sessions_with_active_runs: 0,
  },
  approvals: [
    {
      approval_id: "preview-approval",
      request_summary: "Rotate desktop companion trust material",
      subject_type: "device.rotate",
      subject_id: "preview-device",
      principal: "admin:desktop-control-center",
      requested_at_unix_ms: DESKTOP_PREVIEW_SNAPSHOT.generated_at_unix_ms - 20_000,
      session_id: "preview-session",
      decision: null,
    },
  ],
  inventory: {
    generated_at_unix_ms: DESKTOP_PREVIEW_SNAPSHOT.generated_at_unix_ms,
    summary: {
      devices: 1,
      trusted_devices: 1,
      pending_pairings: 1,
      ok_devices: 1,
      stale_devices: 0,
      degraded_devices: 0,
      offline_devices: 0,
      ok_instances: 1,
      stale_instances: 0,
      degraded_instances: 0,
      offline_instances: 0,
    },
    devices: [
      {
        device_id: "preview-device",
        client_kind: "desktop_companion",
        device_status: "trusted",
        trust_state: "trusted",
        presence_state: "ok",
        paired_at_unix_ms: DESKTOP_PREVIEW_SNAPSHOT.generated_at_unix_ms - 60_000,
        updated_at_unix_ms: DESKTOP_PREVIEW_SNAPSHOT.generated_at_unix_ms,
        last_seen_at_unix_ms: DESKTOP_PREVIEW_SNAPSHOT.generated_at_unix_ms,
        heartbeat_age_ms: 4_000,
        latest_session_id: "preview-session",
        pending_pairings: 1,
        issued_by: "preview",
        approval_id: "preview-approval",
        identity_fingerprint: "SHA256:preview",
        transcript_hash_hex: "preview-hash",
        current_certificate_fingerprint: "SHA256:cert-preview",
        certificate_fingerprint_history: ["SHA256:cert-preview"],
        platform: "windows",
        capabilities: [
          { name: "presence", available: true, summary: "Publishes desktop presence." },
          { name: "dashboard_handoff", available: true, summary: "Opens scoped browser handoff." },
          { name: "local_notifications", available: true, summary: "Raises desktop notifications." },
        ],
        capability_summary: { total: 3, available: 3, unavailable: 0 },
        last_event_name: "desktop.presence",
        last_event_at_unix_ms: DESKTOP_PREVIEW_SNAPSHOT.generated_at_unix_ms - 5_000,
        current_certificate_expires_at_unix_ms:
          DESKTOP_PREVIEW_SNAPSHOT.generated_at_unix_ms + 86_400_000,
        warnings: [],
        actions: {
          can_rotate: true,
          can_revoke: true,
          can_remove: false,
          can_invoke: false,
        },
      },
    ],
  },
  warnings: [],
  metrics: {
    unread_notifications: 1,
    pending_approvals: 1,
    queued_offline_drafts: 0,
    active_sessions: 1,
    sessions_with_active_runs: 0,
    trusted_devices: 1,
    stale_devices: 0,
  },
};

function desktopGlobal(): DesktopGlobal {
  return globalThis as DesktopGlobal;
}

export function isDesktopHostAvailable(): boolean {
  const host = desktopGlobal();
  return typeof host.__TAURI_INTERNALS__ !== "undefined" || typeof host.__TAURI__ !== "undefined";
}

export async function showMainWindow(): Promise<void> {
  return invoke<void>("show_main_window");
}

export async function getSnapshot(): Promise<ControlCenterSnapshot> {
  return invoke<ControlCenterSnapshot>("get_snapshot");
}

export async function getDesktopCompanionSnapshot(): Promise<DesktopCompanionSnapshot> {
  return invoke<DesktopCompanionSnapshot>("get_desktop_companion_snapshot");
}

export async function updateDesktopCompanionPreferences(payload: {
  activeSection?: DesktopCompanionSection;
  activeSessionId?: string;
  activeDeviceId?: string;
  lastRunId?: string;
}): Promise<ActionResult> {
  return invoke<ActionResult>("update_desktop_companion_preferences", {
    payload,
  });
}

export async function updateDesktopCompanionRollout(payload: {
  companionShellEnabled?: boolean;
  desktopNotificationsEnabled?: boolean;
  offlineDraftsEnabled?: boolean;
  releaseChannel?: string;
}): Promise<ActionResult> {
  return invoke<ActionResult>("update_desktop_companion_rollout", {
    payload,
  });
}

export async function markDesktopCompanionNotificationsRead(ids?: string[]): Promise<ActionResult> {
  return invoke<ActionResult>("mark_desktop_companion_notifications_read", {
    payload: { ids },
  });
}

export async function removeDesktopCompanionOfflineDraft(draftId: string): Promise<ActionResult> {
  return invoke<ActionResult>("remove_desktop_companion_offline_draft", {
    draftId,
  });
}

export async function resolveDesktopCompanionChatSession(payload: {
  sessionId?: string;
  sessionKey?: string;
  sessionLabel?: string;
  requireExisting?: boolean;
  resetSession?: boolean;
}): Promise<ChatSessionRecord> {
  return invoke<ChatSessionRecord>("resolve_desktop_companion_chat_session", { payload });
}

export async function getDesktopCompanionSessionTranscript(
  sessionId: string,
): Promise<DesktopSessionTranscriptEnvelope> {
  return invoke<DesktopSessionTranscriptEnvelope>("get_desktop_companion_session_transcript", {
    sessionId,
  });
}

export async function sendDesktopCompanionChatMessage(payload: {
  sessionId: string;
  text: string;
  sessionLabel?: string;
  allowSensitiveTools?: boolean;
  queueOnFailure?: boolean;
  draftId?: string;
}): Promise<DesktopCompanionSendMessageResult> {
  return invoke<DesktopCompanionSendMessageResult>("send_desktop_companion_chat_message", {
    payload,
  });
}

export async function decideDesktopCompanionApproval(payload: {
  approvalId: string;
  approved: boolean;
  reason?: string;
  scope?: string;
}): Promise<JsonValue> {
  return invoke<JsonValue>("decide_desktop_companion_approval", { payload });
}

export async function openDesktopCompanionHandoff(payload: {
  section?: string;
  sessionId?: string;
  deviceId?: string;
  runId?: string;
}): Promise<ActionResult> {
  return invoke<ActionResult>("open_desktop_companion_handoff", { payload });
}

export async function startPalyra(): Promise<ActionResult> {
  return invoke<ActionResult>("start_palyra");
}

export async function stopPalyra(): Promise<ActionResult> {
  return invoke<ActionResult>("stop_palyra");
}

export async function restartPalyra(): Promise<ActionResult> {
  return invoke<ActionResult>("restart_palyra");
}

export async function openDashboard(): Promise<ActionResult> {
  return invoke<ActionResult>("open_dashboard");
}
