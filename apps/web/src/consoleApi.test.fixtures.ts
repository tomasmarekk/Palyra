import type { MobileBootstrapEnvelope } from "./consoleApi";

export const MOBILE_BOOTSTRAP_RESPONSE: MobileBootstrapEnvelope = {
  contract: { contract_version: "control-plane.v1" },
  release_scope: {
    approvals_inbox: true,
    polling_notifications: true,
    recent_sessions: true,
    safe_url_open: true,
    voice_note: true,
  },
  notifications: {
    delivery_mode: "polling",
    quiet_hours_supported: true,
    grouping_supported: true,
    priority_supported: true,
    default_poll_interval_ms: 45000,
    max_alerts_per_poll: 24,
  },
  pairing: {
    auth_flow: "login",
    trust_model: "shared",
    revoke_supported: true,
    recovery_supported: true,
    offline_state_visible: true,
  },
  handoff: {
    contract: "cross_surface_handoff.v1",
    safe_url_open_requires_mediation: true,
    heavy_surface_handoff_supported: true,
    browser_automation_exposed: false,
  },
  store: {
    approvals_cache_key: "a",
    sessions_cache_key: "s",
    inbox_cache_key: "i",
    outbox_queue_key: "o",
    revoke_marker_key: "r",
  },
  rollout: {
    mobile_companion_enabled: true,
    approvals_enabled: true,
    notifications_enabled: true,
    recent_sessions_enabled: true,
    safe_url_open_enabled: true,
    voice_notes_enabled: true,
  },
  locales: ["en", "qps-ploc"],
  default_locale: "en",
};
