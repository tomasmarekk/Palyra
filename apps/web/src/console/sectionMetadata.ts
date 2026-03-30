export const CONSOLE_SECTIONS = [
  { id: "overview", label: "Overview", detail: "Operational posture, risks, and next actions" },
  { id: "chat", label: "Chat", detail: "Primary operator workspace" },
  { id: "sessions", label: "Sessions", detail: "Catalog, lifecycle, and run posture" },
  { id: "approvals", label: "Approvals", detail: "Sensitive action gate" },
  { id: "cron", label: "Automation", detail: "Scheduled jobs, runs, and history" },
  { id: "channels", label: "Channels", detail: "Connectors, router, and Discord setup" },
  { id: "browser", label: "Browser", detail: "Profiles, relay, and downloads" },
  { id: "agents", label: "Agents", detail: "Registry, defaults, and workspace setup" },
  { id: "memory", label: "Memory", detail: "Retention, search, and purge" },
  { id: "skills", label: "Skills", detail: "Install and runtime trust posture" },
  { id: "auth", label: "Profiles", detail: "Provider auth profiles and health posture" },
  { id: "config", label: "Config", detail: "Inspect, validate, mutate, and recover config" },
  { id: "secrets", label: "Secrets", detail: "Vault-backed secret metadata and reveal flows" },
  { id: "access", label: "Access", detail: "Pairing lifecycle and remote access posture" },
  { id: "operations", label: "Diagnostics", detail: "Runtime snapshots, audit, and CLI handoffs" },
  { id: "support", label: "Support", detail: "Bundles, recovery, and operator handoff" },
] as const;

export type Section = (typeof CONSOLE_SECTIONS)[number]["id"];
