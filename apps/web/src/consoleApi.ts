export type JsonValue =
  | string
  | number
  | boolean
  | null
  | { [key: string]: JsonValue }
  | JsonValue[];

export interface ConsoleSession {
  principal: string;
  device_id: string;
  channel?: string;
  csrf_token: string;
  issued_at_unix_ms: number;
  expires_at_unix_ms: number;
}

interface ErrorEnvelope {
  error?: string;
}

interface RequestOptions {
  csrf?: boolean;
}

function buildPathWithQuery(path: string, params?: URLSearchParams): string {
  if (params === undefined || params.size === 0) {
    return path;
  }
  return `${path}?${params.toString()}`;
}

export class ConsoleApiClient {
  private csrfToken: string | null = null;

  constructor(
    private readonly basePath = "",
    private readonly fetcher: typeof fetch = fetch
  ) {}

  async getSession(): Promise<ConsoleSession> {
    const session = await this.request<ConsoleSession>("/console/v1/auth/session");
    this.csrfToken = session.csrf_token;
    return session;
  }

  async login(payload: {
    admin_token: string;
    principal: string;
    device_id: string;
    channel?: string;
  }): Promise<ConsoleSession> {
    const session = await this.request<ConsoleSession>(
      "/console/v1/auth/login",
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: false }
    );
    this.csrfToken = session.csrf_token;
    return session;
  }

  async logout(): Promise<void> {
    await this.request<{ signed_out: boolean }>(
      "/console/v1/auth/logout",
      { method: "POST" },
      { csrf: true }
    );
    this.csrfToken = null;
  }

  async listApprovals(params?: URLSearchParams): Promise<{ approvals: JsonValue[] }> {
    return this.request(buildPathWithQuery("/console/v1/approvals", params));
  }

  async getApproval(approvalId: string): Promise<{ approval: JsonValue }> {
    return this.request(`/console/v1/approvals/${encodeURIComponent(approvalId)}`);
  }

  async decideApproval(
    approvalId: string,
    payload: {
      approved: boolean;
      reason?: string;
      decision_scope?: "once" | "session" | "timeboxed";
      decision_scope_ttl_ms?: number;
    }
  ): Promise<{ approval: JsonValue }> {
    return this.request(
      `/console/v1/approvals/${encodeURIComponent(approvalId)}/decision`,
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async listCronJobs(params?: URLSearchParams): Promise<{ jobs: JsonValue[] }> {
    return this.request(buildPathWithQuery("/console/v1/cron/jobs", params));
  }

  async createCronJob(payload: {
    name: string;
    prompt: string;
    schedule_type: "cron" | "every" | "at";
    cron_expression?: string;
    every_interval_ms?: number;
    at_timestamp_rfc3339?: string;
    enabled?: boolean;
    channel?: string;
  }): Promise<{ job: JsonValue }> {
    return this.request(
      "/console/v1/cron/jobs",
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async setCronJobEnabled(jobId: string, enabled: boolean): Promise<{ job: JsonValue }> {
    return this.request(
      `/console/v1/cron/jobs/${encodeURIComponent(jobId)}/enabled`,
      {
        method: "POST",
        body: JSON.stringify({ enabled })
      },
      { csrf: true }
    );
  }

  async runCronJobNow(jobId: string): Promise<{ run_id?: string; status: string; message: string }> {
    return this.request(
      `/console/v1/cron/jobs/${encodeURIComponent(jobId)}/run-now`,
      { method: "POST" },
      { csrf: true }
    );
  }

  async listCronRuns(jobId: string, params?: URLSearchParams): Promise<{ runs: JsonValue[] }> {
    return this.request(buildPathWithQuery(`/console/v1/cron/jobs/${encodeURIComponent(jobId)}/runs`, params));
  }

  async searchMemory(params?: URLSearchParams): Promise<{ hits: JsonValue[] }> {
    return this.request(buildPathWithQuery("/console/v1/memory/search", params));
  }

  async purgeMemory(payload: {
    channel?: string;
    session_id?: string;
    purge_all_principal?: boolean;
  }): Promise<{ deleted_count: number }> {
    return this.request(
      "/console/v1/memory/purge",
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async listSkills(params?: URLSearchParams): Promise<{ entries: JsonValue[] }> {
    return this.request(buildPathWithQuery("/console/v1/skills", params));
  }

  async installSkill(payload: {
    artifact_path: string;
    allow_tofu?: boolean;
    allow_untrusted?: boolean;
  }): Promise<{ record: JsonValue }> {
    return this.request(
      "/console/v1/skills/install",
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async verifySkill(
    skillId: string,
    payload: { version?: string; allow_tofu?: boolean }
  ): Promise<{ report: JsonValue }> {
    return this.request(
      `/console/v1/skills/${encodeURIComponent(skillId)}/verify`,
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async auditSkill(
    skillId: string,
    payload: { version?: string; allow_tofu?: boolean; quarantine_on_fail?: boolean }
  ): Promise<{ report: JsonValue; quarantined: boolean }> {
    return this.request(
      `/console/v1/skills/${encodeURIComponent(skillId)}/audit`,
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async quarantineSkill(payload: {
    skill_id: string;
    version: string;
    reason?: string;
  }): Promise<JsonValue> {
    return this.request(
      `/console/v1/skills/${encodeURIComponent(payload.skill_id)}/quarantine`,
      {
        method: "POST",
        body: JSON.stringify({
          version: payload.version,
          reason: payload.reason
        })
      },
      { csrf: true }
    );
  }

  async enableSkill(payload: {
    skill_id: string;
    version: string;
    reason?: string;
  }): Promise<JsonValue> {
    return this.request(
      `/console/v1/skills/${encodeURIComponent(payload.skill_id)}/enable`,
      {
        method: "POST",
        body: JSON.stringify({
          version: payload.version,
          reason: payload.reason,
          override: true
        })
      },
      { csrf: true }
    );
  }

  async listAuditEvents(params?: URLSearchParams): Promise<{ events: JsonValue[] }> {
    return this.request(buildPathWithQuery("/console/v1/audit/events", params));
  }

  async listBrowserProfiles(params?: URLSearchParams): Promise<{
    principal: string;
    active_profile_id?: string;
    profiles: JsonValue[];
  }> {
    return this.request(buildPathWithQuery("/console/v1/browser/profiles", params));
  }

  async createBrowserProfile(payload: {
    principal?: string;
    name: string;
    theme_color?: string;
    persistence_enabled?: boolean;
    private_profile?: boolean;
  }): Promise<{ profile: JsonValue }> {
    return this.request(
      "/console/v1/browser/profiles/create",
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async renameBrowserProfile(
    profileId: string,
    payload: { principal?: string; name: string }
  ): Promise<{ profile: JsonValue }> {
    return this.request(
      `/console/v1/browser/profiles/${encodeURIComponent(profileId)}/rename`,
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async deleteBrowserProfile(
    profileId: string,
    payload: { principal?: string } = {}
  ): Promise<{ deleted: boolean; active_profile_id?: string }> {
    return this.request(
      `/console/v1/browser/profiles/${encodeURIComponent(profileId)}/delete`,
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async activateBrowserProfile(
    profileId: string,
    payload: { principal?: string } = {}
  ): Promise<{ profile: JsonValue }> {
    return this.request(
      `/console/v1/browser/profiles/${encodeURIComponent(profileId)}/activate`,
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async listBrowserDownloads(params?: URLSearchParams): Promise<{
    artifacts: JsonValue[];
    truncated: boolean;
    error: string;
  }> {
    return this.request(buildPathWithQuery("/console/v1/browser/downloads", params));
  }

  async mintBrowserRelayToken(payload: {
    session_id: string;
    extension_id: string;
    ttl_ms?: number;
  }): Promise<{
    relay_token: string;
    session_id: string;
    extension_id: string;
    issued_at_unix_ms: number;
    expires_at_unix_ms: number;
    token_ttl_ms: number;
    warning: string;
  }> {
    return this.request(
      "/console/v1/browser/relay/tokens",
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async relayBrowserAction(
    payload: {
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
    },
    relayToken?: string
  ): Promise<{
    success: boolean;
    action: string;
    error: string;
    result: JsonValue;
  }> {
    const headers = relayToken !== undefined && relayToken.trim().length > 0
      ? { authorization: `Bearer ${relayToken.trim()}` }
      : undefined;
    return this.request(
      "/console/v1/browser/relay/actions",
      {
        method: "POST",
        headers,
        body: JSON.stringify(payload)
      },
      { csrf: false }
    );
  }

  private async request<T>(
    path: string,
    init: RequestInit = {},
    options: RequestOptions = {}
  ): Promise<T> {
    const headers = new Headers(init.headers);
    if (init.body !== undefined && !headers.has("Content-Type")) {
      headers.set("Content-Type", "application/json");
    }

    const method = (init.method ?? "GET").toUpperCase();
    const requiresCsrf = options.csrf ?? (method !== "GET");
    if (requiresCsrf) {
      if (this.csrfToken === null) {
        throw new Error("Missing CSRF token. Please sign in again.");
      }
      headers.set("x-palyra-csrf-token", this.csrfToken);
    }

    const response = await this.fetcher(`${this.basePath}${path}`, {
      ...init,
      headers,
      credentials: "include"
    });

    const contentType = response.headers.get("content-type") ?? "";
    const isJson = contentType.includes("application/json");
    const payload = isJson
      ? ((await response.json()) as JsonValue)
      : ((await response.text()) as unknown as JsonValue);

    if (!response.ok) {
      throw new Error(extractErrorMessage(payload, response.status));
    }
    return payload as T;
  }
}

function extractErrorMessage(payload: JsonValue, status: number): string {
  if (payload !== null && typeof payload === "object" && !Array.isArray(payload)) {
    const envelope = payload as ErrorEnvelope;
    if (typeof envelope.error === "string" && envelope.error.trim().length > 0) {
      return envelope.error;
    }
  }
  return `Request failed with HTTP ${status}.`;
}
