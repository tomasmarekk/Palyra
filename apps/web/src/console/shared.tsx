import type { JsonValue } from "../consoleApi";

export type JsonObject = { [key: string]: JsonValue };

const SENSITIVE_KEY_PATTERN =
  /(secret|token|password|cookie|authorization|credential|api[-_]?key|private[-_]?key|vault[-_]?ref)/i;
const SENSITIVE_VALUE_PATTERN =
  /^(Bearer\s+|sk-[a-z0-9]|ghp_[A-Za-z0-9]|xox[baprs]-|AIza[0-9A-Za-z\-_]{20,})/i;

type DiscordOnboardingHighlightsProps = {
  title: string;
  payload: JsonObject;
};

export function DiscordOnboardingHighlights({ title, payload }: DiscordOnboardingHighlightsProps) {
  const preflight = resolveDiscordOnboardingPreflight(payload);
  const inviteUrl = readString(preflight, "invite_url_template");
  const requiredPermissions = readStringList(preflight, "required_permissions");
  const egressAllowlist = readStringList(preflight, "egress_allowlist");
  const securityDefaults = readStringList(preflight, "security_defaults");
  const channelPermissionCheck = isJsonObject(preflight["channel_permission_check"])
    ? preflight["channel_permission_check"]
    : null;
  const permissionFlags: Array<[string, boolean]> = channelPermissionCheck === null
    ? []
    : [
      ["View Channels", readBool(channelPermissionCheck, "can_view_channel")],
      ["Send Messages", readBool(channelPermissionCheck, "can_send_messages")],
      ["Read Message History", readBool(channelPermissionCheck, "can_read_message_history")],
      ["Embed Links", readBool(channelPermissionCheck, "can_embed_links")],
      ["Attach Files", readBool(channelPermissionCheck, "can_attach_files")],
      ["Send Messages in Threads", readBool(channelPermissionCheck, "can_send_messages_in_threads")]
    ];

  return (
    <section className="console-subpanel">
      <h4>{title}</h4>
      {inviteUrl !== null && <p><strong>Invite URL template:</strong> {inviteUrl}</p>}
      <p><strong>Required permissions:</strong> {requiredPermissions.length > 0 ? requiredPermissions.join(", ") : "-"}</p>
      <p><strong>Egress allowlist:</strong></p>
      {egressAllowlist.length > 0 ? (
        <ul>
          {egressAllowlist.map((entry) => (
            <li key={entry}>{entry}</li>
          ))}
        </ul>
      ) : (
        <p>-</p>
      )}
      <p><strong>Security defaults:</strong></p>
      {securityDefaults.length > 0 ? (
        <ul>
          {securityDefaults.map((entry) => (
            <li key={entry}>{entry}</li>
          ))}
        </ul>
      ) : (
        <p>-</p>
      )}
      {channelPermissionCheck !== null && (
        <>
          <p>
            <strong>Channel permission check:</strong>{" "}
            channel_id={readString(channelPermissionCheck, "channel_id") ?? "unknown"} status=
            {readString(channelPermissionCheck, "status") ?? "unknown"}
          </p>
          <ul>
            {permissionFlags.map(([label, enabled]) => (
              <li key={label}>{label}: {enabled ? "yes" : "no"}</li>
            ))}
          </ul>
        </>
      )}
    </section>
  );
}

function resolveDiscordOnboardingPreflight(payload: JsonObject): JsonObject {
  const preflight = payload["preflight"];
  if (isJsonObject(preflight)) {
    return preflight;
  }
  return payload;
}

export function toErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return "Unexpected failure.";
}

export function isJsonObject(value: JsonValue): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function toJsonObjectArray(values: JsonValue[]): JsonObject[] {
  const rows: JsonObject[] = [];
  for (const value of values) {
    if (isJsonObject(value)) {
      rows.push(value);
    }
  }
  return rows;
}

export function channelConnectorAvailability(record: JsonObject): string {
  return readString(record, "availability") ?? "supported";
}

export function isVisibleChannelConnector(record: JsonObject): boolean {
  return channelConnectorAvailability(record) !== "deferred";
}

export function toStringArray(values: JsonValue[]): string[] {
  const rows: string[] = [];
  for (const value of values) {
    if (typeof value === "string" && value.trim().length > 0) {
      rows.push(value);
    }
  }
  return rows;
}

export function readStringList(record: JsonObject, key: string): string[] {
  const value = record[key];
  if (!Array.isArray(value)) {
    return [];
  }
  return toStringArray(value);
}

export function readString(record: JsonObject, key: string): string | null {
  const value = record[key];
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  return null;
}

export function readBool(record: JsonObject, key: string): boolean {
  return record[key] === true;
}

export function parseInteger(raw: string): number | null {
  const trimmed = raw.trim();
  if (trimmed.length === 0) {
    return null;
  }
  const parsed = Number.parseInt(trimmed, 10);
  return Number.isFinite(parsed) ? parsed : null;
}

export function emptyToUndefined(raw: string): string | undefined {
  const trimmed = raw.trim();
  return trimmed.length === 0 ? undefined : trimmed;
}

export function encodeBase64(value: string): string {
  if (typeof window !== "undefined" && typeof window.btoa === "function") {
    return window.btoa(unescape(encodeURIComponent(value)));
  }
  throw new Error("Base64 encoding is unavailable in this environment.");
}

export function skillMetadata(entry: JsonObject): { skillId: string; version: string } | null {
  const record = entry.record;
  if (!isJsonObject(record)) {
    return null;
  }
  const skillId = readString(record, "skill_id");
  const version = readString(record, "version");
  if (skillId === null || version === null) {
    return null;
  }
  return { skillId, version };
}

function redactValue(value: JsonValue, revealSensitive: boolean): JsonValue {
  if (revealSensitive) {
    return value;
  }
  if (typeof value === "string") {
    return SENSITIVE_VALUE_PATTERN.test(value) ? "[redacted]" : value;
  }
  if (Array.isArray(value)) {
    return value.map((entry) => redactValue(entry, false));
  }
  if (isJsonObject(value)) {
    const sanitized: JsonObject = {};
    for (const [key, item] of Object.entries(value)) {
      sanitized[key] = SENSITIVE_KEY_PATTERN.test(key) ? "[redacted]" : redactValue(item, false);
    }
    return sanitized;
  }
  return value;
}

export function toPrettyJson(value: JsonValue, revealSensitive: boolean): string {
  return JSON.stringify(redactValue(value, revealSensitive), null, 2);
}
