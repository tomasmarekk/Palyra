export const CONSOLE_SECTIONS = [
  { id: "overview", label: "Overview", detail: "Operational posture, risks, and next actions" },
  { id: "chat", label: "Chat", detail: "Primary operator workspace" },
  { id: "approvals", label: "Approvals", detail: "Sensitive action gate" },
  { id: "cron", label: "Automation", detail: "Scheduled jobs, runs, and history" },
  { id: "channels", label: "Channels", detail: "Connectors, router, and Discord setup" },
  { id: "browser", label: "Browser", detail: "Profiles, relay, and downloads" },
  { id: "memory", label: "Memory", detail: "Retention, search, and purge" },
  { id: "skills", label: "Skills", detail: "Install and runtime trust posture" },
  { id: "auth", label: "OpenAI and Auth Profiles", detail: "OpenAI and auth profile state" },
  { id: "config", label: "Config and Secrets", detail: "Config lifecycle and secrets controls" },
  { id: "access", label: "Pairing and Gateway Access", detail: "Pairing lifecycle and remote access posture" },
  { id: "operations", label: "Diagnostics and Audit", detail: "Runtime snapshots, audit, and CLI handoffs" },
  { id: "support", label: "Support", detail: "Bundles, recovery, and operator handoff" }
] as const;

export type Section = (typeof CONSOLE_SECTIONS)[number]["id"];
