import registryData from "./chatCommandRegistry.json";

export type SlashCommandSurface = "web" | "tui";
export type SlashCommandExecution = "local" | "server" | "local_capability";

export interface SlashCommandDefinition {
  readonly name: string;
  readonly synopsis: string;
  readonly description: string;
  readonly example: string;
  readonly category: string;
  readonly execution: SlashCommandExecution;
  readonly surfaces: readonly SlashCommandSurface[];
  readonly aliases: readonly string[];
  readonly capability_tags: readonly string[];
  readonly entity_targets: readonly string[];
  readonly keywords: readonly string[];
}

const ALL_COMMANDS = Object.freeze(
  (registryData as SlashCommandDefinition[]).map((command) =>
    Object.freeze({
      ...command,
      surfaces: Object.freeze([...command.surfaces]),
      aliases: Object.freeze([...(command.aliases ?? [])]),
      capability_tags: Object.freeze([...command.capability_tags]),
      entity_targets: Object.freeze([...command.entity_targets]),
      keywords: Object.freeze([...command.keywords]),
    }),
  ),
);

export const ALL_CHAT_SLASH_COMMANDS: readonly SlashCommandDefinition[] = ALL_COMMANDS;
export const CHAT_SLASH_COMMANDS: readonly SlashCommandDefinition[] = getChatSlashCommands("web");
export const TUI_CHAT_SLASH_COMMANDS: readonly SlashCommandDefinition[] = getChatSlashCommands(
  "tui",
);

export function getChatSlashCommands(
  surface: SlashCommandSurface,
): readonly SlashCommandDefinition[] {
  return ALL_CHAT_SLASH_COMMANDS.filter((command) => command.surfaces.includes(surface));
}

export function findChatSlashCommand(
  name: string,
  surface: SlashCommandSurface,
): SlashCommandDefinition | null {
  const normalized = name.trim().toLowerCase();
  if (normalized.length === 0) {
    return null;
  }
  return (
    ALL_CHAT_SLASH_COMMANDS.find(
      (command) =>
        (command.name === normalized || command.aliases.includes(normalized)) &&
        command.surfaces.includes(surface),
    ) ?? null
  );
}
