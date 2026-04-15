import type {
  AuthProfileView,
  ChatCheckpointRecord,
  JsonValue,
  SessionCatalogRecord,
} from "../consoleApi";
import { isJsonObject, readString, type JsonObject } from "../console/shared";
import type { ChatDelegationCatalog } from "../consoleApi";
import type { SlashCommandDefinition } from "./chatCommandRegistry";
import {
  findChatSlashCommand,
  resolveChatSlashCommandName,
  type SlashCommandSurface,
} from "./chatCommandRegistry";

export interface BrowserProfileSuggestionRecord {
  readonly profile_id: string;
  readonly name: string;
  readonly persistence_enabled: boolean;
  readonly private_profile: boolean;
}

export interface BrowserSessionSuggestionRecord {
  readonly session_id: string;
  readonly title: string;
}

export interface ChatSlashSuggestionContext {
  readonly surface: SlashCommandSurface;
  readonly input: string;
  readonly commands: readonly SlashCommandDefinition[];
  readonly sessions: readonly SessionCatalogRecord[];
  readonly objectives: readonly JsonObject[];
  readonly authProfiles: readonly AuthProfileView[];
  readonly browserProfiles: readonly BrowserProfileSuggestionRecord[];
  readonly browserSessions: readonly BrowserSessionSuggestionRecord[];
  readonly checkpoints: readonly ChatCheckpointRecord[];
  readonly delegationCatalog: ChatDelegationCatalog | null;
  readonly streaming: boolean;
}

export interface ChatSlashSuggestion {
  readonly id: string;
  readonly kind: "command" | "entity";
  readonly commandName: string;
  readonly title: string;
  readonly subtitle: string;
  readonly detail: string;
  readonly example: string;
  readonly replacement: string;
  readonly badge: string;
}

export interface ChatSlashSuggestionResult {
  readonly activeCommand: SlashCommandDefinition | null;
  readonly activeToken: string;
  readonly suggestions: readonly ChatSlashSuggestion[];
}

export function buildSlashSuggestions(
  context: ChatSlashSuggestionContext,
): ChatSlashSuggestionResult {
  const trimmed = context.input.trimStart();
  if (!trimmed.startsWith("/")) {
    return { activeCommand: null, activeToken: "", suggestions: [] };
  }

  const body = trimmed.slice(1);
  const hasTrailingWhitespace = /\s$/.test(trimmed);
  const [rawCommandToken = "", ...rawRest] = body.split(/\s+/);
  const normalizedCommandToken = rawCommandToken.trim().toLowerCase();
  const normalizedCommandName =
    resolveChatSlashCommandName(normalizedCommandToken, context.surface) ?? normalizedCommandToken;
  const activeCommand =
    normalizedCommandName.length > 0
      ? findChatSlashCommand(normalizedCommandName, context.surface)
      : null;

  if (rawCommandToken.trim().length === 0 || activeCommand === null) {
    const query = normalizedCommandToken;
    return {
      activeCommand,
      activeToken: query,
      suggestions: buildCommandNameSuggestions(context.commands, query),
    };
  }

  const normalizedRest = rawRest.join(" ").trim();
  const activeToken =
    hasTrailingWhitespace || normalizedRest.length === 0
      ? ""
      : (normalizedRest.split(/\s+/).at(-1)?.toLowerCase() ?? "");
  return {
    activeCommand,
    activeToken,
    suggestions: buildEntitySuggestions(context, activeCommand, normalizedRest, activeToken),
  };
}

export function selectUndoCheckpoint(
  checkpoints: readonly ChatCheckpointRecord[],
): ChatCheckpointRecord | null {
  const undoTagged = checkpoints
    .filter((checkpoint) => checkpointHasTag(checkpoint, "undo_safe"))
    .sort((left, right) => right.created_at_unix_ms - left.created_at_unix_ms);
  if (undoTagged.length > 0) {
    return undoTagged[0] ?? null;
  }
  const sorted = [...checkpoints].sort(
    (left, right) => right.created_at_unix_ms - left.created_at_unix_ms,
  );
  return sorted[0] ?? null;
}

export function checkpointHasTag(checkpoint: ChatCheckpointRecord, tag: string): boolean {
  const normalizedTag = tag.trim().toLowerCase();
  if (normalizedTag.length === 0) {
    return false;
  }
  try {
    const parsed: JsonValue = JSON.parse(checkpoint.tags_json);
    return Array.isArray(parsed)
      ? parsed.some(
          (value) => typeof value === "string" && value.trim().toLowerCase() === normalizedTag,
        )
      : false;
  } catch {
    return false;
  }
}

function buildCommandNameSuggestions(
  commands: readonly SlashCommandDefinition[],
  query: string,
): readonly ChatSlashSuggestion[] {
  const normalizedQuery = query.trim().toLowerCase();
  const matches = commands
    .filter((command) => {
      if (normalizedQuery.length === 0) {
        return true;
      }
      return (
        command.name.includes(normalizedQuery) ||
        command.aliases.some((alias) => alias.includes(normalizedQuery)) ||
        command.keywords.some((keyword) => keyword.includes(normalizedQuery))
      );
    })
    .slice(0, 8);
  return matches.map((command) => ({
    id: `command:${command.name}`,
    kind: "command",
    commandName: command.name,
    title: command.synopsis,
    subtitle: command.description,
    detail: command.example,
    example: command.example,
    replacement: command.example,
    badge: command.category,
  }));
}

function buildEntitySuggestions(
  context: ChatSlashSuggestionContext,
  command: SlashCommandDefinition,
  normalizedRest: string,
  activeToken: string,
): readonly ChatSlashSuggestion[] {
  switch (command.name) {
    case "resume":
    case "history":
      return buildSessionSuggestions(command, context.sessions, activeToken);
    case "objective":
      return buildObjectiveSuggestions(command, context.objectives, activeToken);
    case "profile":
      return buildProfileSuggestions(command, context.authProfiles, activeToken);
    case "browser":
      return [
        ...buildBrowserProfileSuggestions(command, context.browserProfiles, activeToken),
        ...buildBrowserSessionSuggestions(command, context.browserSessions, activeToken),
      ].slice(0, 8);
    case "delegate":
      return buildDelegationSuggestions(command, context.delegationCatalog, activeToken);
    case "checkpoint":
      return buildCheckpointSuggestions(command, context.checkpoints, normalizedRest, activeToken);
    case "undo":
      return buildUndoSuggestions(command, context.checkpoints, activeToken);
    case "interrupt":
      return buildInterruptSuggestions(command, activeToken, context.streaming);
    case "doctor":
      return buildDoctorSuggestions(command, activeToken);
    case "compact":
      return buildStaticSuggestions(command, ["preview", "apply", "history"], activeToken);
    case "export":
      return buildStaticSuggestions(command, ["json", "markdown"], activeToken);
    default:
      return [];
  }
}

function buildSessionSuggestions(
  command: SlashCommandDefinition,
  sessions: readonly SessionCatalogRecord[],
  activeToken: string,
): readonly ChatSlashSuggestion[] {
  const query = activeToken.trim().toLowerCase();
  return sessions
    .filter((session) => {
      if (query.length === 0) {
        return true;
      }
      return (
        session.session_id.toLowerCase().includes(query) ||
        session.title.toLowerCase().includes(query) ||
        (session.session_key ?? "").toLowerCase().includes(query) ||
        session.family.root_title.toLowerCase().includes(query) ||
        (session.last_summary ?? "").toLowerCase().includes(query) ||
        (session.agent_id ?? "").toLowerCase().includes(query) ||
        (session.model_profile ?? "").toLowerCase().includes(query) ||
        session.family.relatives.some((relative) => relative.title.toLowerCase().includes(query)) ||
        session.recap.touched_files.some((file) => file.toLowerCase().includes(query)) ||
        session.recap.active_context_files.some((file) => file.toLowerCase().includes(query)) ||
        session.recap.recent_artifacts.some((artifact) =>
          artifact.label.toLowerCase().includes(query),
        )
      );
    })
    .slice(0, 6)
    .map((session) => ({
      id: `session:${session.session_id}`,
      kind: "entity",
      commandName: command.name,
      title: session.title,
      subtitle:
        session.session_key && session.session_key.length > 0
          ? session.family.family_size > 1
            ? `${session.session_key} · family ${session.family.sequence}/${session.family.family_size}`
            : session.session_key
          : session.session_id,
      detail:
        session.preview ??
        session.last_summary ??
        (session.family.root_title !== session.title
          ? `Family root: ${session.family.root_title}`
          : "Resume this session context."),
      example: `/${command.name} ${session.session_id}`,
      replacement: `/${command.name} ${command.name === "history" ? session.title : session.session_id}`,
      badge: session.archived
        ? "archived"
        : session.branch_state === "active_branch"
          ? "branch"
          : "session",
    }));
}

function buildObjectiveSuggestions(
  command: SlashCommandDefinition,
  objectives: readonly JsonObject[],
  activeToken: string,
): readonly ChatSlashSuggestion[] {
  const query = activeToken.trim().toLowerCase();
  return objectives
    .filter((objective) => {
      const objectiveId = readString(objective, "objective_id")?.toLowerCase() ?? "";
      const name = readString(objective, "name")?.toLowerCase() ?? "";
      const kind = readString(objective, "kind")?.toLowerCase() ?? "";
      return (
        query.length === 0 ||
        objectiveId.includes(query) ||
        name.includes(query) ||
        kind.includes(query)
      );
    })
    .slice(0, 6)
    .map((objective) => {
      const objectiveId = readString(objective, "objective_id") ?? "unknown";
      const name = readString(objective, "name") ?? "Objective";
      const kind = readString(objective, "kind") ?? "objective";
      const focus = readString(objective, "current_focus") ?? "No focus recorded.";
      return {
        id: `objective:${objectiveId}`,
        kind: "entity" as const,
        commandName: command.name,
        title: `${kind.replaceAll("_", " ")} · ${name}`,
        subtitle: objectiveId,
        detail: focus,
        example: `/${command.name} ${objectiveId}`,
        replacement: `/${command.name} ${objectiveId}`,
        badge: kind,
      };
    });
}

function buildProfileSuggestions(
  command: SlashCommandDefinition,
  profiles: readonly AuthProfileView[],
  activeToken: string,
): readonly ChatSlashSuggestion[] {
  const query = activeToken.trim().toLowerCase();
  return profiles
    .filter((profile) => {
      if (query.length === 0) {
        return true;
      }
      return (
        profile.profile_id.toLowerCase().includes(query) ||
        profile.profile_name.toLowerCase().includes(query) ||
        profile.provider.kind.toLowerCase().includes(query)
      );
    })
    .slice(0, 6)
    .map((profile) => ({
      id: `profile:${profile.profile_id}`,
      kind: "entity",
      commandName: command.name,
      title: profile.profile_name,
      subtitle: `${profile.provider.kind} · ${profile.scope.kind}`,
      detail: profile.profile_id,
      example: `/${command.name} ${profile.profile_id}`,
      replacement: `/${command.name} ${profile.profile_id}`,
      badge: "profile",
    }));
}

function buildBrowserProfileSuggestions(
  command: SlashCommandDefinition,
  profiles: readonly BrowserProfileSuggestionRecord[],
  activeToken: string,
): readonly ChatSlashSuggestion[] {
  const query = activeToken.trim().toLowerCase();
  return profiles
    .filter((profile) => {
      if (query.length === 0) {
        return true;
      }
      return (
        profile.profile_id.toLowerCase().includes(query) ||
        profile.name.toLowerCase().includes(query)
      );
    })
    .slice(0, 4)
    .map((profile) => ({
      id: `browser-profile:${profile.profile_id}`,
      kind: "entity",
      commandName: command.name,
      title: profile.name,
      subtitle: profile.profile_id,
      detail: `${profile.persistence_enabled ? "persistent" : "ephemeral"} · ${profile.private_profile ? "private" : "shared"}`,
      example: `/${command.name} ${profile.profile_id}`,
      replacement: `/${command.name} ${profile.profile_id}`,
      badge: "browser profile",
    }));
}

function buildBrowserSessionSuggestions(
  command: SlashCommandDefinition,
  sessions: readonly BrowserSessionSuggestionRecord[],
  activeToken: string,
): readonly ChatSlashSuggestion[] {
  const query = activeToken.trim().toLowerCase();
  return sessions
    .filter((session) => {
      if (query.length === 0) {
        return true;
      }
      return (
        session.session_id.toLowerCase().includes(query) ||
        session.title.toLowerCase().includes(query)
      );
    })
    .slice(0, 4)
    .map((session) => ({
      id: `browser-session:${session.session_id}`,
      kind: "entity",
      commandName: command.name,
      title: session.title,
      subtitle: session.session_id,
      detail: "Open browser workbench",
      example: `/${command.name} ${session.session_id}`,
      replacement: `/${command.name} ${session.session_id}`,
      badge: "browser session",
    }));
}

function buildDelegationSuggestions(
  command: SlashCommandDefinition,
  catalog: ChatDelegationCatalog | null,
  activeToken: string,
): readonly ChatSlashSuggestion[] {
  if (catalog === null) {
    return [];
  }
  const query = activeToken.trim().toLowerCase();
  const items = [
    ...catalog.templates.map((template) => ({
      id: template.template_id,
      title: template.display_name,
      detail: template.template_id,
      badge: "template",
    })),
    ...catalog.profiles.map((profile) => ({
      id: profile.profile_id,
      title: profile.display_name,
      detail: profile.profile_id,
      badge: "profile",
    })),
  ];
  return items
    .filter((item) => {
      if (query.length === 0) {
        return true;
      }
      return item.id.toLowerCase().includes(query) || item.title.toLowerCase().includes(query);
    })
    .slice(0, 8)
    .map((item) => ({
      id: `delegate:${item.id}`,
      kind: "entity",
      commandName: command.name,
      title: item.title,
      subtitle: item.detail,
      detail: "Complete the command with a delegated task prompt.",
      example: `/${command.name} ${item.id} Summarize the latest operator findings.`,
      replacement: `/${command.name} ${item.id} `,
      badge: item.badge,
    }));
}

function buildCheckpointSuggestions(
  command: SlashCommandDefinition,
  checkpoints: readonly ChatCheckpointRecord[],
  normalizedRest: string,
  activeToken: string,
): readonly ChatSlashSuggestion[] {
  if (!normalizedRest.startsWith("restore")) {
    return buildStaticSuggestions(command, ["list", "restore", "save"], activeToken);
  }
  const query = activeToken.trim().toLowerCase();
  return checkpoints
    .filter((checkpoint) => {
      if (query.length === 0) {
        return true;
      }
      return (
        checkpoint.checkpoint_id.toLowerCase().includes(query) ||
        checkpoint.name.toLowerCase().includes(query)
      );
    })
    .slice(0, 6)
    .map((checkpoint) => ({
      id: `checkpoint:${checkpoint.checkpoint_id}`,
      kind: "entity",
      commandName: command.name,
      title: checkpoint.name,
      subtitle: checkpoint.checkpoint_id,
      detail: checkpoint.note ?? "Restore checkpoint into a new branch.",
      example: `/${command.name} restore ${checkpoint.checkpoint_id}`,
      replacement: `/${command.name} restore ${checkpoint.checkpoint_id}`,
      badge: checkpointHasTag(checkpoint, "undo_safe") ? "undo-safe" : "checkpoint",
    }));
}

function buildUndoSuggestions(
  command: SlashCommandDefinition,
  checkpoints: readonly ChatCheckpointRecord[],
  activeToken: string,
): readonly ChatSlashSuggestion[] {
  const latest = selectUndoCheckpoint(checkpoints);
  const suggestions: ChatSlashSuggestion[] = [];
  if (latest !== null && activeToken.trim().length === 0) {
    suggestions.push({
      id: `undo:latest:${latest.checkpoint_id}`,
      kind: "entity",
      commandName: command.name,
      title: "Undo last turn",
      subtitle: latest.name,
      detail: latest.note ?? "Restore the latest safe checkpoint.",
      example: `/${command.name}`,
      replacement: `/${command.name}`,
      badge: checkpointHasTag(latest, "undo_safe") ? "undo-safe" : "checkpoint",
    });
  }
  const query = activeToken.trim().toLowerCase();
  suggestions.push(
    ...checkpoints
      .filter((checkpoint) => {
        if (query.length === 0) {
          return true;
        }
        return (
          checkpoint.checkpoint_id.toLowerCase().includes(query) ||
          checkpoint.name.toLowerCase().includes(query)
        );
      })
      .slice(0, 6)
      .map((checkpoint) => ({
        id: `undo:${checkpoint.checkpoint_id}`,
        kind: "entity" as const,
        commandName: command.name,
        title: checkpoint.name,
        subtitle: checkpoint.checkpoint_id,
        detail: checkpoint.note ?? "Restore this checkpoint as the new active branch.",
        example: `/${command.name} ${checkpoint.checkpoint_id}`,
        replacement: `/${command.name} ${checkpoint.checkpoint_id}`,
        badge: checkpointHasTag(checkpoint, "undo_safe") ? "undo-safe" : "checkpoint",
      })),
  );
  return suggestions.slice(0, 8);
}

function buildInterruptSuggestions(
  command: SlashCommandDefinition,
  activeToken: string,
  streaming: boolean,
): readonly ChatSlashSuggestion[] {
  return ["soft", "force"]
    .filter((candidate) => activeToken.length === 0 || candidate.includes(activeToken))
    .map((candidate) => ({
      id: `interrupt:${candidate}`,
      kind: "entity",
      commandName: command.name,
      title: candidate === "soft" ? "Soft interrupt" : "Force interrupt",
      subtitle: streaming ? "Current run is active" : "Prepare redirect before the next send",
      detail:
        candidate === "soft"
          ? "Wait for the runtime to honor a normal cancellation request."
          : "Escalate wording only when a normal interrupt already failed.",
      example: `/${command.name} ${candidate} Summarize the failures instead.`,
      replacement: `/${command.name} ${candidate} `,
      badge: candidate,
    }));
}

function buildDoctorSuggestions(
  command: SlashCommandDefinition,
  activeToken: string,
): readonly ChatSlashSuggestion[] {
  return [
    {
      id: "doctor:jobs",
      title: "Recent jobs",
      detail: "List the latest doctor recovery jobs.",
    },
    {
      id: "doctor:run",
      title: "Dry-run doctor",
      detail: "Queue a non-mutating doctor recovery pass.",
    },
    {
      id: "doctor:repair",
      title: "Repair doctor",
      detail: "Queue a repair-enabled doctor recovery pass.",
    },
  ]
    .filter((candidate) => {
      if (activeToken.length === 0) {
        return true;
      }
      return (
        candidate.id.includes(activeToken) || candidate.title.toLowerCase().includes(activeToken)
      );
    })
    .map((candidate) => {
      const keyword = candidate.id.split(":")[1] ?? "jobs";
      return {
        id: candidate.id,
        kind: "entity" as const,
        commandName: command.name,
        title: candidate.title,
        subtitle: keyword,
        detail: candidate.detail,
        example: `/${command.name} ${keyword}`,
        replacement: `/${command.name} ${keyword}`,
        badge: "doctor",
      };
    });
}

function buildStaticSuggestions(
  command: SlashCommandDefinition,
  values: readonly string[],
  activeToken: string,
): readonly ChatSlashSuggestion[] {
  return values
    .filter((value) => activeToken.length === 0 || value.includes(activeToken))
    .map((value) => ({
      id: `${command.name}:${value}`,
      kind: "entity",
      commandName: command.name,
      title: `${command.name} ${value}`,
      subtitle: command.synopsis,
      detail: command.description,
      example: `/${command.name} ${value}`,
      replacement: `/${command.name} ${value}`,
      badge: command.category,
    }));
}

export function toBrowserProfileSuggestionRecords(
  values: readonly JsonValue[],
): BrowserProfileSuggestionRecord[] {
  return values
    .filter(isJsonObject)
    .map((record) => ({
      profile_id: readString(record, "profile_id") ?? "",
      name: readString(record, "name") ?? readString(record, "profile_name") ?? "Browser profile",
      persistence_enabled: record["persistence_enabled"] === true || record["persistence"] === true,
      private_profile: record["private_profile"] === true,
    }))
    .filter((record) => record.profile_id.length > 0);
}

export function toBrowserSessionSuggestionRecords(
  values: readonly JsonValue[],
): BrowserSessionSuggestionRecord[] {
  return values
    .filter(isJsonObject)
    .map((record) => ({
      session_id: readString(record, "session_id") ?? "",
      title:
        readString(record, "page_title") ??
        readString(record, "target_url") ??
        readString(record, "channel") ??
        "Browser session",
    }))
    .filter((record) => record.session_id.length > 0);
}
