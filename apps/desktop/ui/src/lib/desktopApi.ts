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
  ambient_companion_enabled: boolean;
  desktop_notifications_enabled: boolean;
  offline_drafts_enabled: boolean;
  voice_capture_enabled: boolean;
  voice_overlay_enabled: boolean;
  voice_silence_detection_enabled: boolean;
  tts_playback_enabled: boolean;
  release_channel: string;
};

export type DesktopCompanionSurfaceMode = "main" | "quick_panel" | "voice_overlay";

export type DesktopVoiceLifecycleState =
  | "idle"
  | "recording"
  | "transcribing"
  | "review"
  | "sending"
  | "speaking"
  | "error"
  | "cancelled";

export type DesktopCompanionPreferences = {
  active_section: DesktopCompanionSection;
  active_session_id?: string;
  active_device_id?: string;
  last_run_id?: string;
};

export type DesktopCompanionAmbient = {
  start_on_login_enabled: boolean;
  global_hotkey_enabled: boolean;
  global_hotkey: string;
  hotkey_registration_error?: string;
  last_surface: DesktopCompanionSurfaceMode;
};

export type DesktopCompanionVoiceAuditEntry = {
  audit_id: string;
  kind: string;
  detail: string;
  created_at_unix_ms: number;
  session_id?: string;
  remote_processing: boolean;
  tts_playback: boolean;
  input_device_label?: string;
  output_voice_label?: string;
};

export type DesktopCompanionVoice = {
  lifecycle_state: DesktopVoiceLifecycleState;
  capture_consent_granted_at_unix_ms?: number;
  tts_consent_granted_at_unix_ms?: number;
  microphone_permission_state: string;
  microphone_device_id?: string;
  microphone_device_label?: string;
  tts_voice_uri?: string;
  tts_voice_label?: string;
  tts_muted: boolean;
  silence_detection_enabled: boolean;
  silence_timeout_ms: number;
  draft_session_id?: string;
  draft_text?: string;
  draft_summary?: string;
  draft_language?: string;
  draft_duration_ms?: number;
  last_error?: string;
  audit_log: DesktopCompanionVoiceAuditEntry[];
};

export type DesktopCompanionActiveRun = {
  session_id: string;
  session_title: string;
  run_id: string;
  status: string;
  started_at_unix_ms?: number;
  pending_approvals: number;
  preview?: string;
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

export type SessionCatalogFamilyRelative = {
  session_id: string;
  title: string;
  branch_state: string;
  relation: string;
};

export type SessionCatalogFamilyRecord = {
  root_title: string;
  sequence: number;
  family_size: number;
  parent_session_id?: string;
  parent_title?: string;
  relatives: SessionCatalogFamilyRelative[];
};

export type SessionCatalogArtifactRecord = {
  artifact_id: string;
  kind: string;
  label: string;
};

export type SessionProjectContextFocusRecord = {
  path: string;
  reason: string;
};

export type SessionProjectContextEntryRecord = {
  entry_id: string;
  order: number;
  path: string;
  source_kind: string;
  source_label: string;
  precedence_label: string;
  depth: number;
  root: boolean;
  active: boolean;
  disabled: boolean;
  approved: boolean;
  status: string;
  content_hash: string;
  loaded_at_unix_ms: number;
  modified_at_unix_ms?: number;
  estimated_tokens: number;
  discovery_reasons: string[];
  warnings: string[];
  preview_text: string;
};

export type SessionProjectContextRecord = {
  generated_at_unix_ms: number;
  active_entries: number;
  blocked_entries: number;
  approval_required_entries: number;
  disabled_entries: number;
  active_estimated_tokens: number;
  warnings: string[];
  focus_paths: SessionProjectContextFocusRecord[];
  entries: SessionProjectContextEntryRecord[];
};

export type SessionCatalogRecapRecord = {
  touched_files: string[];
  active_context_files: string[];
  project_context?: SessionProjectContextRecord;
  recent_artifacts: SessionCatalogArtifactRecord[];
  ctas: string[];
};

export type SessionCatalogQuickControlRecord = {
  value?: string;
  display_value: string;
  source: string;
  inherited_value?: string;
  override_active: boolean;
};

export type SessionCatalogToggleControlRecord = {
  value: boolean;
  source: string;
  inherited_value: boolean;
  override_active: boolean;
};

export type SessionCatalogQuickControlsRecord = {
  agent: SessionCatalogQuickControlRecord;
  model: SessionCatalogQuickControlRecord;
  thinking: SessionCatalogToggleControlRecord;
  trace: SessionCatalogToggleControlRecord;
  verbose: SessionCatalogToggleControlRecord;
  reset_to_default_available: boolean;
};

export type SessionCatalogRecord = ChatSessionRecord & {
  title: string;
  title_source: string;
  title_generation_state: string;
  manual_title_locked: boolean;
  auto_title_updated_at_unix_ms?: number;
  manual_title_updated_at_unix_ms?: number;
  preview?: string;
  preview_state: string;
  last_intent?: string;
  last_intent_state: string;
  last_summary?: string;
  last_summary_state: string;
  branch_state: string;
  parent_session_id?: string;
  branch_origin_run_id?: string;
  last_run_state?: string;
  last_run_started_at_unix_ms?: number;
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
  archived: boolean;
  archived_at_unix_ms?: number;
  pending_approvals: number;
  has_context_files: boolean;
  last_context_file?: string;
  agent_id?: string;
  model_profile?: string;
  artifact_count: number;
  family: SessionCatalogFamilyRecord;
  recap: SessionCatalogRecapRecord;
  quick_controls: SessionCatalogQuickControlsRecord;
};

export type SessionCatalogSummary = {
  active_sessions: number;
  archived_sessions: number;
  sessions_with_pending_approvals: number;
  sessions_with_active_runs: number;
  sessions_with_context_files: number;
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

export type DesktopCompanionAudioTranscriptionResult = {
  attachment_id: string;
  artifact_id: string;
  transcript_text: string;
  transcript_summary?: string;
  transcript_language?: string;
  transcript_duration_ms?: number;
  transcript_processing_ms?: number;
  derived_artifact_id?: string;
  privacy_note: string;
  warnings: string[];
};

export type InventoryCapabilityRecord = {
  name: string;
  available: boolean;
  execution_mode?: string;
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

export type OnboardingFlow = "quick_start" | "advanced_setup";
export type OnboardingPostureState =
  | "not_started"
  | "in_progress"
  | "blocked"
  | "ready"
  | "complete";
export type OnboardingStepStatus = "todo" | "in_progress" | "blocked" | "done" | "skipped";
export type OnboardingActionKind =
  | "open_console_path"
  | "run_cli_command"
  | "open_desktop_section"
  | "read_docs";

export type OnboardingStepAction = {
  label: string;
  kind: OnboardingActionKind;
  surface: string;
  target: string;
};

export type OnboardingBlockedReason = {
  code: string;
  detail: string;
  repair_hint: string;
};

export type OnboardingStepView = {
  step_id: string;
  title: string;
  summary: string;
  status: OnboardingStepStatus;
  optional?: boolean;
  verification_state?: string;
  blocked?: OnboardingBlockedReason;
  action?: OnboardingStepAction;
};

export type OnboardingStepCounts = {
  todo: number;
  in_progress: number;
  blocked: number;
  done: number;
  skipped: number;
};

export type OnboardingPostureEnvelope = {
  contract: { contract_version: string };
  flow: OnboardingFlow;
  flow_variant: string;
  status: OnboardingPostureState;
  config_path: string;
  resume_supported: boolean;
  ready_for_first_success: boolean;
  recommended_step_id?: string;
  first_success_hint?: string;
  counts: OnboardingStepCounts;
  available_flows: OnboardingFlow[];
  steps: OnboardingStepView[];
};

export type OpenAiAuthStatusSnapshot = {
  ready: boolean;
  state?: string;
  note?: string;
  default_profile_id?: string;
};

export type ConsoleProfileContext = {
  name: string;
  label: string;
  environment: string;
  color: string;
  risk_level: string;
  strict_mode: boolean;
  mode: string;
};

export type ConsoleSession = {
  principal: string;
  device_id: string;
  channel?: string;
  profile?: ConsoleProfileContext;
  csrf_token: string;
  issued_at_unix_ms: number;
  expires_at_unix_ms: number;
};

export type DesktopCompanionMetrics = {
  unread_notifications: number;
  pending_approvals: number;
  queued_offline_drafts: number;
  active_runs: number;
  active_sessions: number;
  sessions_with_active_runs: number;
  trusted_devices: number;
  stale_devices: number;
};

export type DesktopCompanionProfileRecord = {
  context: ConsoleProfileContext;
  implicit: boolean;
  recent: boolean;
  last_used_at_unix_ms?: number;
  active: boolean;
};

export type DesktopCompanionSnapshot = {
  generated_at_unix_ms: number;
  control_center: ControlCenterSnapshot;
  onboarding: OnboardingStatusSnapshot;
  shared_onboarding?: OnboardingPostureEnvelope;
  openai_status: OpenAiAuthStatusSnapshot;
  active_profile: DesktopCompanionProfileRecord;
  profiles: DesktopCompanionProfileRecord[];
  recent_profiles: string[];
  console_session?: ConsoleSession;
  connection_state: "connected" | "reconnecting" | "offline";
  rollout: DesktopCompanionRollout;
  preferences: DesktopCompanionPreferences;
  ambient: DesktopCompanionAmbient;
  voice: DesktopCompanionVoice;
  notifications: DesktopCompanionNotification[];
  offline_drafts: DesktopCompanionOfflineDraft[];
  active_runs: DesktopCompanionActiveRun[];
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

export type NodeHostStatusSnapshot = {
  installed: boolean;
  paired: boolean;
  running: boolean;
  device_id?: string | null;
  grpc_url?: string | null;
  cert_expires_at_unix_ms?: number | null;
  detail: string;
};

export type QuickFactsSnapshot = {
  dashboard_url: string;
  dashboard_access_mode: string;
  dashboard_remote_trust_state: string;
  dashboard_remote_verification_mode?: string | null;
  gateway_version: string | null;
  gateway_git_hash: string | null;
  gateway_uptime_seconds: number | null;
  browser_service: BrowserServiceSnapshot;
  node_host: NodeHostStatusSnapshot;
};

export type DiagnosticsSnapshot = {
  generated_at_unix_ms: number | null;
  errors: string[];
  dropped_log_events_total: number;
  experiments: DesktopExperimentGovernanceSnapshot;
};

export type DesktopExperimentGovernanceSnapshot = {
  structured_contract: string;
  fail_closed: boolean;
  requires_console_diagnostics: boolean;
  native_canvas: DesktopExperimentTrackSnapshot;
};

export type DesktopExperimentTrackSnapshot = {
  track_id: string;
  enabled: boolean;
  feature_flag: string;
  rollout_stage: string;
  ambient_mode: string;
  consent_required: boolean;
  support_summary: string;
  security_review: string[];
  exit_criteria: string[];
  limits: DesktopExperimentLimitsSnapshot;
};

export type DesktopExperimentLimitsSnapshot = {
  max_state_bytes: number;
  max_bundle_bytes: number;
  max_assets_per_bundle: number;
  max_updates_per_minute: number;
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
  node_host_process: ServiceProcessSnapshot;
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
    dashboard_remote_trust_state: "local",
    dashboard_remote_verification_mode: null,
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
    node_host: {
      installed: true,
      paired: true,
      running: true,
      device_id: "01ARZ3NDEKTSV4RRFFQ69NODE0",
      grpc_url: "https://127.0.0.1:7444",
      cert_expires_at_unix_ms: Date.UTC(2026, 2, 20, 12, 0, 0),
      detail: "Desktop node host is enrolled and attached to the local gateway.",
    },
  },
  diagnostics: {
    generated_at_unix_ms: Date.UTC(2026, 2, 13, 12, 0, 0),
    errors: [],
    dropped_log_events_total: 0,
    experiments: {
      structured_contract: "a2ui.v1",
      fail_closed: true,
      requires_console_diagnostics: true,
      native_canvas: {
        track_id: "native-canvas-preview",
        enabled: false,
        feature_flag: "canvas_host.enabled",
        rollout_stage: "disabled",
        ambient_mode: "disabled",
        consent_required: false,
        support_summary:
          "Native canvas stays behind the bounded canvas host and keeps A2UI as the only structured render contract.",
        security_review: [
          "Preserve CSP, frame-ancestor allowlists, and token-scoped access.",
          "Keep state and bundle limits fail-closed in diagnostics and support flows.",
        ],
        exit_criteria: [
          "Disable immediately if diagnostics or replay fidelity regress.",
          "Retire the track if it cannot justify operator value beyond the browser surface.",
        ],
        limits: {
          max_state_bytes: 8192,
          max_bundle_bytes: 65536,
          max_assets_per_bundle: 8,
          max_updates_per_minute: 30,
        },
      },
    },
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
  node_host_process: {
    service: "node_host",
    desired_running: true,
    running: true,
    liveness: "running",
    pid: 7342,
    last_start_unix_ms: Date.UTC(2026, 2, 13, 11, 58, 6),
    last_exit: null,
    restart_attempt: 0,
    next_restart_unix_ms: null,
    bound_ports: [],
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
  shared_onboarding: {
    contract: { contract_version: "control-plane.v1" },
    flow: "quick_start",
    flow_variant: "quickstart",
    status: "ready",
    config_path: "./palyra.toml",
    resume_supported: true,
    ready_for_first_success: true,
    recommended_step_id: "first_success",
    first_success_hint: "Open chat and send the first real operator request.",
    counts: { todo: 0, in_progress: 0, blocked: 0, done: 4, skipped: 1 },
    available_flows: ["quick_start", "advanced_setup"],
    steps: [
      {
        step_id: "runtime",
        title: "Start local runtime",
        summary: "Gateway and desktop sidecars are healthy.",
        status: "done",
        action: {
          label: "Open overview",
          kind: "open_console_path",
          surface: "web",
          target: "/#/control/overview",
        },
      },
      {
        step_id: "provider",
        title: "Connect provider",
        summary: "OpenAI default profile is ready for the first run.",
        status: "done",
        action: {
          label: "Open profiles",
          kind: "open_console_path",
          surface: "web",
          target: "/#/settings/profiles",
        },
      },
      {
        step_id: "channels",
        title: "Optional channel setup",
        summary: "Configure browser relay or channels later if needed.",
        status: "skipped",
        optional: true,
      },
      {
        step_id: "first_success",
        title: "Run the first operator task",
        summary: "Use a starter prompt in chat and inspect the result.",
        status: "done",
        action: {
          label: "Open chat",
          kind: "open_desktop_section",
          surface: "desktop",
          target: "chat",
        },
      },
    ],
  },
  openai_status: {
    ready: true,
    state: "connected",
    note: "Preview profile is connected.",
    default_profile_id: "preview",
  },
  active_profile: {
    context: {
      name: "preview",
      label: "Preview",
      environment: "staging",
      color: "amber",
      risk_level: "elevated",
      strict_mode: true,
      mode: "remote",
    },
    implicit: false,
    recent: true,
    last_used_at_unix_ms: DESKTOP_PREVIEW_SNAPSHOT.generated_at_unix_ms - 120_000,
    active: true,
  },
  profiles: [
    {
      context: {
        name: "preview",
        label: "Preview",
        environment: "staging",
        color: "amber",
        risk_level: "elevated",
        strict_mode: true,
        mode: "remote",
      },
      implicit: false,
      recent: true,
      last_used_at_unix_ms: DESKTOP_PREVIEW_SNAPSHOT.generated_at_unix_ms - 120_000,
      active: true,
    },
    {
      context: {
        name: "desktop-local",
        label: "Desktop local",
        environment: "local",
        color: "emerald",
        risk_level: "low",
        strict_mode: false,
        mode: "local",
      },
      implicit: true,
      recent: true,
      last_used_at_unix_ms: DESKTOP_PREVIEW_SNAPSHOT.generated_at_unix_ms - 420_000,
      active: false,
    },
  ],
  recent_profiles: ["preview", "desktop-local"],
  console_session: {
    principal: "admin:desktop-control-center",
    device_id: "preview-device",
    channel: "desktop",
    profile: {
      name: "preview",
      label: "Preview",
      environment: "staging",
      color: "amber",
      risk_level: "elevated",
      strict_mode: true,
      mode: "remote",
    },
    csrf_token: "preview-csrf",
    issued_at_unix_ms: DESKTOP_PREVIEW_SNAPSHOT.generated_at_unix_ms - 30_000,
    expires_at_unix_ms: DESKTOP_PREVIEW_SNAPSHOT.generated_at_unix_ms + 3_600_000,
  },
  connection_state: "connected",
  rollout: {
    companion_shell_enabled: true,
    ambient_companion_enabled: true,
    desktop_notifications_enabled: true,
    offline_drafts_enabled: true,
    voice_capture_enabled: true,
    voice_overlay_enabled: true,
    voice_silence_detection_enabled: true,
    tts_playback_enabled: true,
    release_channel: "preview",
  },
  preferences: {
    active_section: "home",
    active_session_id: "preview-session",
    active_device_id: "preview-device",
    last_run_id: "preview-run",
  },
  ambient: {
    start_on_login_enabled: false,
    global_hotkey_enabled: true,
    global_hotkey: "CommandOrControl+Shift+Space",
    last_surface: "quick_panel",
  },
  voice: {
    lifecycle_state: "review",
    capture_consent_granted_at_unix_ms: DESKTOP_PREVIEW_SNAPSHOT.generated_at_unix_ms - 600_000,
    tts_consent_granted_at_unix_ms: DESKTOP_PREVIEW_SNAPSHOT.generated_at_unix_ms - 420_000,
    microphone_permission_state: "granted",
    microphone_device_id: "default-mic",
    microphone_device_label: "Built-in microphone",
    tts_voice_uri: "preview-voice",
    tts_voice_label: "Preview voice",
    tts_muted: false,
    silence_detection_enabled: true,
    silence_timeout_ms: 1800,
    draft_session_id: "preview-session",
    draft_text: "Summarize the active run and tell me whether any approval is blocking it.",
    draft_summary: "Preview transcript queued for review",
    draft_language: "en",
    draft_duration_ms: 5200,
    last_error: undefined,
    audit_log: [
      {
        audit_id: "voice-audit-1",
        kind: "capture_started",
        detail: "Push-to-talk recording started from the quick panel.",
        created_at_unix_ms: DESKTOP_PREVIEW_SNAPSHOT.generated_at_unix_ms - 24_000,
        session_id: "preview-session",
        remote_processing: false,
        tts_playback: false,
        input_device_label: "Built-in microphone",
      },
      {
        audit_id: "voice-audit-2",
        kind: "transcript_ready",
        detail: "Transcript is ready for explicit review before sending.",
        created_at_unix_ms: DESKTOP_PREVIEW_SNAPSHOT.generated_at_unix_ms - 8_000,
        session_id: "preview-session",
        remote_processing: true,
        tts_playback: false,
        input_device_label: "Built-in microphone",
        output_voice_label: "Preview voice",
      },
    ],
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
  active_runs: [
    {
      session_id: "preview-session",
      session_title: "Preview conversation",
      run_id: "preview-run",
      status: "running",
      started_at_unix_ms: DESKTOP_PREVIEW_SNAPSHOT.generated_at_unix_ms - 15_000,
      pending_approvals: 1,
      preview:
        "Gathering desktop companion rollout status before handing off to the full dashboard.",
    },
  ],
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
      title_generation_state: "ready",
      manual_title_locked: true,
      auto_title_updated_at_unix_ms: DESKTOP_PREVIEW_SNAPSHOT.generated_at_unix_ms - 20_000,
      manual_title_updated_at_unix_ms: DESKTOP_PREVIEW_SNAPSHOT.generated_at_unix_ms - 10_000,
      preview: "Companion preview keeps session context and approvals in one desktop shell.",
      preview_state: "ready",
      last_intent: "daily companion flow",
      last_intent_state: "ready",
      last_summary: "Preview state is healthy.",
      last_summary_state: "ready",
      branch_state: "root",
      branch_origin_run_id: "preview-run",
      last_run_state: "running",
      last_run_started_at_unix_ms: DESKTOP_PREVIEW_SNAPSHOT.generated_at_unix_ms - 10_000,
      prompt_tokens: 120,
      completion_tokens: 340,
      total_tokens: 460,
      archived: false,
      pending_approvals: 1,
      has_context_files: true,
      last_context_file: "docs/desktop/companion-preview.md",
      agent_id: "desktop-companion",
      model_profile: "gpt-5.4",
      artifact_count: 2,
      family: {
        root_title: "Preview conversation",
        sequence: 1,
        family_size: 1,
        relatives: [],
      },
      recap: {
        touched_files: ["apps/desktop/ui/src/App.tsx", "docs/desktop/companion-preview.md"],
        active_context_files: ["docs/desktop/companion-preview.md"],
        recent_artifacts: [
          {
            artifact_id: "preview-artifact-1",
            kind: "summary",
            label: "Preview summary",
          },
          {
            artifact_id: "preview-artifact-2",
            kind: "note",
            label: "Companion note",
          },
        ],
        ctas: ["Resume chat", "Open diagnostics"],
      },
      quick_controls: {
        agent: {
          value: "desktop-companion",
          display_value: "desktop-companion",
          source: "session",
          inherited_value: "default",
          override_active: true,
        },
        model: {
          value: "gpt-5.4",
          display_value: "gpt-5.4",
          source: "session",
          inherited_value: "gpt-5.4-mini",
          override_active: true,
        },
        thinking: {
          value: true,
          source: "inherited",
          inherited_value: true,
          override_active: false,
        },
        trace: {
          value: true,
          source: "session",
          inherited_value: false,
          override_active: true,
        },
        verbose: {
          value: false,
          source: "session",
          inherited_value: true,
          override_active: true,
        },
        reset_to_default_available: true,
      },
    },
  ],
  session_summary: {
    active_sessions: 1,
    archived_sessions: 0,
    sessions_with_pending_approvals: 1,
    sessions_with_active_runs: 1,
    sessions_with_context_files: 1,
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
          {
            name: "presence",
            available: true,
            execution_mode: "automatic",
            summary: "Publishes desktop presence.",
          },
          {
            name: "dashboard_handoff",
            available: true,
            execution_mode: "local_mediation",
            summary: "Opens scoped browser handoff.",
          },
          {
            name: "local_notifications",
            available: true,
            execution_mode: "local_mediation",
            summary: "Raises desktop notifications.",
          },
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
    active_runs: 1,
    active_sessions: 1,
    sessions_with_active_runs: 1,
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

export async function showDesktopCompanionWindow(
  surface?: DesktopCompanionSurfaceMode,
): Promise<ActionResult> {
  return invoke<ActionResult>("show_desktop_companion_window", {
    payload: { surface },
  });
}

export async function hideDesktopCompanionWindow(
  surface?: DesktopCompanionSurfaceMode,
): Promise<ActionResult> {
  return invoke<ActionResult>("hide_desktop_companion_window", {
    payload: { surface },
  });
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
  ambientCompanionEnabled?: boolean;
  desktopNotificationsEnabled?: boolean;
  offlineDraftsEnabled?: boolean;
  voiceCaptureEnabled?: boolean;
  voiceOverlayEnabled?: boolean;
  voiceSilenceDetectionEnabled?: boolean;
  ttsPlaybackEnabled?: boolean;
  releaseChannel?: string;
}): Promise<ActionResult> {
  return invoke<ActionResult>("update_desktop_companion_rollout", {
    payload,
  });
}

export async function updateDesktopCompanionAmbient(payload: {
  startOnLoginEnabled?: boolean;
  globalHotkeyEnabled?: boolean;
  globalHotkey?: string;
  clearHotkeyRegistrationError?: boolean;
  lastSurface?: DesktopCompanionSurfaceMode;
}): Promise<ActionResult> {
  return invoke<ActionResult>("update_desktop_companion_ambient", {
    payload,
  });
}

export async function updateDesktopCompanionVoiceState(payload: {
  captureConsentGranted?: boolean;
  ttsConsentGranted?: boolean;
  microphonePermissionState?: string;
  microphoneDeviceId?: string;
  microphoneDeviceLabel?: string;
  ttsVoiceUri?: string;
  ttsVoiceLabel?: string;
  ttsMuted?: boolean;
  silenceDetectionEnabled?: boolean;
  silenceTimeoutMs?: number;
  lifecycleState?: DesktopVoiceLifecycleState;
  draftSessionId?: string;
  draftText?: string;
  draftSummary?: string;
  draftLanguage?: string;
  draftDurationMs?: number;
  lastError?: string;
  clearDraft?: boolean;
  clearError?: boolean;
  auditKind?: string;
  auditDetail?: string;
  auditSessionId?: string;
  auditRemoteProcessing?: boolean;
  auditTtsPlayback?: boolean;
}): Promise<ActionResult> {
  return invoke<ActionResult>("update_desktop_companion_voice_state", {
    payload,
  });
}

export async function switchDesktopCompanionProfile(payload: {
  profileName: string;
  allowStrictSwitch?: boolean;
}): Promise<ActionResult> {
  return invoke<ActionResult>("switch_desktop_companion_profile", {
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

export async function transcribeDesktopCompanionAudio(payload: {
  sessionId: string;
  filename: string;
  contentType: string;
  bytesBase64: string;
  consentAcknowledged: boolean;
}): Promise<DesktopCompanionAudioTranscriptionResult> {
  return invoke<DesktopCompanionAudioTranscriptionResult>("transcribe_desktop_companion_audio", {
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
  runId?: string;
  deviceId?: string;
  objectiveId?: string;
  canvasId?: string;
  intent?: string;
  source?: string;
}): Promise<ActionResult> {
  return invoke<ActionResult>("open_desktop_companion_handoff", { payload });
}

export async function emitDesktopCompanionUxEvent(payload: {
  name: string;
  summary?: string;
  section?: string;
  outcome?: string;
  step?: string;
  toolName?: string;
  sessionId?: string;
  runId?: string;
  deviceId?: string;
  objectiveId?: string;
  canvasId?: string;
  intent?: string;
  source?: string;
  locale?: string;
}): Promise<ActionResult> {
  return invoke<ActionResult>("emit_desktop_companion_ux_event", { payload });
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

export async function enrollDesktopNode(): Promise<ActionResult> {
  return invoke<ActionResult>("enroll_desktop_node");
}

export async function repairDesktopNode(): Promise<ActionResult> {
  return invoke<ActionResult>("repair_desktop_node");
}

export async function resetDesktopNode(): Promise<ActionResult> {
  return invoke<ActionResult>("reset_desktop_node");
}

export async function openDashboard(): Promise<ActionResult> {
  return invoke<ActionResult>("open_dashboard");
}

export async function openExternalUrl(url: string): Promise<ActionResult> {
  return invoke<ActionResult>("open_external_url_command", { url });
}
