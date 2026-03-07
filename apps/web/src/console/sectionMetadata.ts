export const CONSOLE_SECTIONS = [
  { id: "overview", label: "Overview", detail: "Product posture and capability exposure map" },
  { id: "chat", label: "Chat and Sessions", detail: "Streaming operator workspace" },
  { id: "approvals", label: "Approvals", detail: "Sensitive action gate" },
  { id: "cron", label: "Cron", detail: "Scheduled prompts, runs, and logs" },
  { id: "channels", label: "Channels and Router", detail: "Discord operations and routing policy" },
  { id: "browser", label: "Browser", detail: "Profiles, relay, and downloads" },
  { id: "memory", label: "Memory", detail: "Retention, search, and purge" },
  { id: "skills", label: "Skills", detail: "Install and runtime trust posture" },
  { id: "auth", label: "OpenAI and Auth Profiles", detail: "Provider state and credential health" },
  { id: "config", label: "Config and Secrets", detail: "Config lifecycle and vault metadata" },
  { id: "access", label: "Pairing and Gateway Access", detail: "Pairing lifecycle and remote access posture" },
  { id: "operations", label: "Diagnostics and Audit", detail: "Runtime snapshots, audit, and CLI handoffs" },
  { id: "support", label: "Support and Recovery", detail: "Bundle export and recovery shortcuts" }
] as const;

export type Section = (typeof CONSOLE_SECTIONS)[number]["id"];
