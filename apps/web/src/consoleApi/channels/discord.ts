import type { ChannelStatusEnvelope, JsonValue } from "../../consoleApi";

type RequestOptions = {
  csrf?: boolean;
  timeoutMs?: number;
};

type RequestFn = <T>(path: string, init?: RequestInit, options?: RequestOptions) => Promise<T>;

export function sendChannelDiscordTestSend(
  request: RequestFn,
  connectorId: string,
  payload: {
    target: string;
    text?: string;
    confirm: boolean;
    auto_reaction?: string;
    thread_id?: string;
  },
): Promise<{ dispatch: JsonValue; status: JsonValue; runtime?: JsonValue }> {
  return request(
    `/console/v1/channels/${encodeURIComponent(connectorId)}/test-send`,
    {
      method: "POST",
      body: JSON.stringify(payload),
    },
    { csrf: true },
  );
}

export function refreshChannelHealth(
  request: RequestFn,
  connectorId: string,
  payload: { verify_channel_id?: string },
): Promise<ChannelStatusEnvelope> {
  return request(
    `/console/v1/channels/${encodeURIComponent(connectorId)}/operations/health-refresh`,
    {
      method: "POST",
      body: JSON.stringify(payload),
    },
    { csrf: true },
  );
}

export function probeDiscordOnboarding(
  request: RequestFn,
  payload: {
    account_id?: string;
    token: string;
    mode?: "local" | "remote_vps";
    inbound_scope?: "dm_only" | "allowlisted_guild_channels" | "open_guild_channels";
    allow_from?: string[];
    deny_from?: string[];
    require_mention?: boolean;
    mention_patterns?: string[];
    concurrency_limit?: number;
    broadcast_strategy?: "deny" | "mention_only" | "allow";
    confirm_open_guild_channels?: boolean;
    verify_channel_id?: string;
  },
): Promise<{ [key: string]: JsonValue }> {
  return request(
    "/console/v1/channels/discord/onboarding/probe",
    {
      method: "POST",
      body: JSON.stringify(payload),
    },
    { csrf: true },
  );
}

export function applyDiscordOnboarding(
  request: RequestFn,
  payload: {
    account_id?: string;
    token: string;
    mode?: "local" | "remote_vps";
    inbound_scope?: "dm_only" | "allowlisted_guild_channels" | "open_guild_channels";
    allow_from?: string[];
    deny_from?: string[];
    require_mention?: boolean;
    mention_patterns?: string[];
    concurrency_limit?: number;
    broadcast_strategy?: "deny" | "mention_only" | "allow";
    confirm_open_guild_channels?: boolean;
    verify_channel_id?: string;
  },
): Promise<{ [key: string]: JsonValue }> {
  return request(
    "/console/v1/channels/discord/onboarding/apply",
    {
      method: "POST",
      body: JSON.stringify(payload),
    },
    { csrf: true },
  );
}
