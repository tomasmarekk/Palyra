import type { ChannelStatusEnvelope, JsonValue } from "../../consoleApi";

type RequestOptions = {
  csrf?: boolean;
  timeoutMs?: number;
};

type RequestFn = <T>(path: string, init?: RequestInit, options?: RequestOptions) => Promise<T>;

type BuildPathWithQueryFn = (path: string, params?: URLSearchParams) => string;

export function listChannels(request: RequestFn): Promise<{ connectors: JsonValue[] }> {
  return request("/console/v1/channels");
}

export function getChannelStatus(
  request: RequestFn,
  connectorId: string,
): Promise<ChannelStatusEnvelope> {
  return request(`/console/v1/channels/${encodeURIComponent(connectorId)}`);
}

export function setChannelEnabled(
  request: RequestFn,
  connectorId: string,
  enabled: boolean,
): Promise<{ connector: JsonValue }> {
  return request(
    `/console/v1/channels/${encodeURIComponent(connectorId)}/enabled`,
    {
      method: "POST",
      body: JSON.stringify({ enabled }),
    },
    { csrf: true },
  );
}

export function listChannelLogs(
  request: RequestFn,
  buildPathWithQuery: BuildPathWithQueryFn,
  connectorId: string,
  params?: URLSearchParams,
): Promise<{ events: JsonValue[]; dead_letters: JsonValue[] }> {
  return request(
    buildPathWithQuery(`/console/v1/channels/${encodeURIComponent(connectorId)}/logs`, params),
  );
}

export function sendChannelTestMessage(
  request: RequestFn,
  connectorId: string,
  payload: {
    text: string;
    conversation_id?: string;
    sender_id?: string;
    sender_display?: string;
    simulate_crash_once?: boolean;
    is_direct_message?: boolean;
    requested_broadcast?: boolean;
  },
): Promise<{ ingest: JsonValue; status: JsonValue }> {
  return request(
    `/console/v1/channels/${encodeURIComponent(connectorId)}/test`,
    {
      method: "POST",
      body: JSON.stringify(payload),
    },
    { csrf: true },
  );
}

export function readChannelMessages(
  request: RequestFn,
  connectorId: string,
  payload: {
    request: {
      conversation_id: string;
      thread_id?: string;
      message_id?: string;
      before_message_id?: string;
      after_message_id?: string;
      around_message_id?: string;
      limit: number;
    };
  },
): Promise<{ result: JsonValue }> {
  return request(
    `/console/v1/channels/${encodeURIComponent(connectorId)}/messages/read`,
    {
      method: "POST",
      body: JSON.stringify(payload),
    },
    { csrf: true },
  );
}

export function searchChannelMessages(
  request: RequestFn,
  connectorId: string,
  payload: {
    request: {
      conversation_id: string;
      thread_id?: string;
      query?: string;
      author_id?: string;
      has_attachments?: boolean;
      before_message_id?: string;
      limit: number;
    };
  },
): Promise<{ result: JsonValue }> {
  return request(
    `/console/v1/channels/${encodeURIComponent(connectorId)}/messages/search`,
    {
      method: "POST",
      body: JSON.stringify(payload),
    },
    { csrf: true },
  );
}

export function editChannelMessage(
  request: RequestFn,
  connectorId: string,
  payload: {
    request: {
      locator: {
        conversation_id: string;
        thread_id?: string;
        message_id: string;
      };
      body: string;
    };
    approval_id?: string;
  },
): Promise<{ result?: JsonValue; approval_required?: boolean; approval?: JsonValue }> {
  return request(
    `/console/v1/channels/${encodeURIComponent(connectorId)}/messages/edit`,
    {
      method: "POST",
      body: JSON.stringify(payload),
    },
    { csrf: true },
  );
}

export function deleteChannelMessage(
  request: RequestFn,
  connectorId: string,
  payload: {
    request: {
      locator: {
        conversation_id: string;
        thread_id?: string;
        message_id: string;
      };
      reason?: string;
    };
    approval_id?: string;
  },
): Promise<{ result?: JsonValue; approval_required?: boolean; approval?: JsonValue }> {
  return request(
    `/console/v1/channels/${encodeURIComponent(connectorId)}/messages/delete`,
    {
      method: "POST",
      body: JSON.stringify(payload),
    },
    { csrf: true },
  );
}

export function addChannelMessageReaction(
  request: RequestFn,
  connectorId: string,
  payload: {
    request: {
      locator: {
        conversation_id: string;
        thread_id?: string;
        message_id: string;
      };
      emoji: string;
    };
    approval_id?: string;
  },
): Promise<{ result?: JsonValue; approval_required?: boolean; approval?: JsonValue }> {
  return request(
    `/console/v1/channels/${encodeURIComponent(connectorId)}/messages/react-add`,
    {
      method: "POST",
      body: JSON.stringify(payload),
    },
    { csrf: true },
  );
}

export function removeChannelMessageReaction(
  request: RequestFn,
  connectorId: string,
  payload: {
    request: {
      locator: {
        conversation_id: string;
        thread_id?: string;
        message_id: string;
      };
      emoji: string;
    };
    approval_id?: string;
  },
): Promise<{ result?: JsonValue; approval_required?: boolean; approval?: JsonValue }> {
  return request(
    `/console/v1/channels/${encodeURIComponent(connectorId)}/messages/react-remove`,
    {
      method: "POST",
      body: JSON.stringify(payload),
    },
    { csrf: true },
  );
}

export function pauseChannelQueue(
  request: RequestFn,
  connectorId: string,
): Promise<ChannelStatusEnvelope> {
  return request(
    `/console/v1/channels/${encodeURIComponent(connectorId)}/operations/queue/pause`,
    { method: "POST" },
    { csrf: true },
  );
}

export function resumeChannelQueue(
  request: RequestFn,
  connectorId: string,
): Promise<ChannelStatusEnvelope> {
  return request(
    `/console/v1/channels/${encodeURIComponent(connectorId)}/operations/queue/resume`,
    { method: "POST" },
    { csrf: true },
  );
}

export function drainChannelQueue(
  request: RequestFn,
  connectorId: string,
): Promise<ChannelStatusEnvelope> {
  return request(
    `/console/v1/channels/${encodeURIComponent(connectorId)}/operations/queue/drain`,
    { method: "POST" },
    { csrf: true },
  );
}

export function replayChannelDeadLetter(
  request: RequestFn,
  connectorId: string,
  deadLetterId: number,
): Promise<ChannelStatusEnvelope> {
  return request(
    `/console/v1/channels/${encodeURIComponent(connectorId)}/operations/dead-letters/${deadLetterId}/replay`,
    { method: "POST" },
    { csrf: true },
  );
}

export function discardChannelDeadLetter(
  request: RequestFn,
  connectorId: string,
  deadLetterId: number,
): Promise<ChannelStatusEnvelope> {
  return request(
    `/console/v1/channels/${encodeURIComponent(connectorId)}/operations/dead-letters/${deadLetterId}/discard`,
    { method: "POST" },
    { csrf: true },
  );
}

export function getChannelRouterRules(
  request: RequestFn,
): Promise<{ config: JsonValue; config_hash: string }> {
  return request("/console/v1/channels/router/rules");
}

export function getChannelRouterWarnings(
  request: RequestFn,
): Promise<{ warnings: JsonValue[]; config_hash: string }> {
  return request("/console/v1/channels/router/warnings");
}

export function previewChannelRoute(
  request: RequestFn,
  payload: {
    channel: string;
    text: string;
    conversation_id?: string;
    sender_identity?: string;
    sender_display?: string;
    sender_verified?: boolean;
    is_direct_message?: boolean;
    requested_broadcast?: boolean;
    adapter_message_id?: string;
    adapter_thread_id?: string;
    max_payload_bytes?: number;
  },
): Promise<{ preview: JsonValue }> {
  return request(
    "/console/v1/channels/router/preview",
    {
      method: "POST",
      body: JSON.stringify(payload),
    },
    { csrf: true },
  );
}

export function listChannelRouterPairings(
  request: RequestFn,
  buildPathWithQuery: BuildPathWithQueryFn,
  params?: URLSearchParams,
): Promise<{ pairings: JsonValue[]; config_hash: string }> {
  return request(buildPathWithQuery("/console/v1/channels/router/pairings", params));
}

export function mintChannelRouterPairingCode(
  request: RequestFn,
  payload: {
    channel: string;
    issued_by?: string;
    ttl_ms?: number;
  },
): Promise<{ code: JsonValue; config_hash: string }> {
  return request(
    "/console/v1/channels/router/pairing-codes",
    {
      method: "POST",
      body: JSON.stringify(payload),
    },
    { csrf: true },
  );
}
