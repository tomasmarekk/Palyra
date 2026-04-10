use std::{collections::BTreeSet, sync::OnceLock};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SharedChatCommandSurface {
    Web,
    Tui,
}

impl SharedChatCommandSurface {
    pub fn as_label(self) -> &'static str {
        match self {
            Self::Web => "web",
            Self::Tui => "tui",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SharedChatCommandExecution {
    Local,
    Server,
    LocalCapability,
}

impl SharedChatCommandExecution {
    pub fn as_label(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Server => "server",
            Self::LocalCapability => "local_capability",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedChatCommandDefinition {
    pub name: String,
    pub synopsis: String,
    pub description: String,
    pub example: String,
    pub category: String,
    pub execution: SharedChatCommandExecution,
    pub surfaces: Vec<SharedChatCommandSurface>,
    #[serde(default)]
    pub aliases: Vec<String>,
    #[serde(default)]
    pub capability_tags: Vec<String>,
    #[serde(default)]
    pub entity_targets: Vec<String>,
    #[serde(default)]
    pub keywords: Vec<String>,
}

static SHARED_CHAT_COMMANDS: OnceLock<Vec<SharedChatCommandDefinition>> = OnceLock::new();

pub fn shared_chat_commands() -> &'static [SharedChatCommandDefinition] {
    SHARED_CHAT_COMMANDS.get_or_init(load_shared_chat_commands).as_slice()
}

pub fn shared_chat_commands_for_surface(
    surface: SharedChatCommandSurface,
) -> Vec<&'static SharedChatCommandDefinition> {
    shared_chat_commands().iter().filter(|command| command.surfaces.contains(&surface)).collect()
}

pub fn find_shared_chat_command(
    name: &str,
    surface: SharedChatCommandSurface,
) -> Option<&'static SharedChatCommandDefinition> {
    let normalized = name.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return None;
    }

    shared_chat_commands().iter().find(|command| {
        (command.name == normalized || command.aliases.iter().any(|alias| alias == &normalized))
            && command.surfaces.contains(&surface)
    })
}

pub fn render_shared_chat_command_synopsis_lines(
    surface: SharedChatCommandSurface,
    max_width: usize,
) -> Vec<String> {
    let mut lines = Vec::new();
    let mut current = String::from("  ");

    for command in shared_chat_commands_for_surface(surface) {
        let synopsis = command.synopsis.as_str();
        let separator = if current.trim().is_empty() { "" } else { "  " };
        if current.len() + separator.len() + synopsis.len() > max_width
            && !current.trim().is_empty()
        {
            lines.push(current);
            current = format!("  {synopsis}");
            continue;
        }
        if !current.trim().is_empty() {
            current.push_str(separator);
        }
        current.push_str(synopsis);
    }

    if !current.trim().is_empty() {
        lines.push(current);
    }

    lines
}

fn load_shared_chat_commands() -> Vec<SharedChatCommandDefinition> {
    let raw = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../apps/web/src/chat/chatCommandRegistry.json"
    ));
    let commands: Vec<SharedChatCommandDefinition> =
        serde_json::from_str(raw).expect("shared chat command registry must be valid JSON");
    validate_shared_chat_commands(commands)
}

fn validate_shared_chat_commands(
    commands: Vec<SharedChatCommandDefinition>,
) -> Vec<SharedChatCommandDefinition> {
    let mut seen_names = BTreeSet::new();
    for command in &commands {
        assert!(
            !command.name.trim().is_empty(),
            "shared chat command registry contains an empty name"
        );
        assert!(
            seen_names.insert(command.name.clone()),
            "shared chat command registry contains duplicate command {}",
            command.name
        );
        let mut seen_aliases = BTreeSet::new();
        for alias in &command.aliases {
            assert!(
                seen_aliases.insert(alias.clone()),
                "shared chat command registry contains duplicate alias {} for {}",
                alias,
                command.name
            );
        }
        assert!(
            command.synopsis.starts_with(format!("/{}", command.name).as_str()),
            "shared chat command registry synopsis must start with /{}",
            command.name
        );
        assert!(
            !command.description.trim().is_empty(),
            "shared chat command registry description cannot be empty for {}",
            command.name
        );
        assert!(
            !command.example.trim().is_empty(),
            "shared chat command registry example cannot be empty for {}",
            command.name
        );
        assert!(
            !command.category.trim().is_empty(),
            "shared chat command registry category cannot be empty for {}",
            command.name
        );
        assert!(
            !command.surfaces.is_empty(),
            "shared chat command registry surfaces cannot be empty for {}",
            command.name
        );
    }
    commands
}

#[cfg(test)]
mod tests {
    use super::{
        find_shared_chat_command, render_shared_chat_command_synopsis_lines, shared_chat_commands,
        SharedChatCommandSurface,
    };

    #[test]
    fn shared_chat_command_registry_loads_without_duplicates() {
        let commands = shared_chat_commands();
        assert!(!commands.is_empty(), "registry should not be empty");
        assert!(commands.iter().any(|command| command.name == "help"));
        assert!(commands.iter().any(|command| command.name == "compact"));
    }

    #[test]
    fn shared_chat_command_registry_filters_by_surface() {
        assert!(find_shared_chat_command("help", SharedChatCommandSurface::Web).is_some());
        assert!(find_shared_chat_command("help", SharedChatCommandSurface::Tui).is_some());
        assert!(find_shared_chat_command("status", SharedChatCommandSurface::Web).is_none());
        assert!(find_shared_chat_command("status", SharedChatCommandSurface::Tui).is_some());
        assert!(find_shared_chat_command("quit", SharedChatCommandSurface::Tui).is_some());
    }

    #[test]
    fn shared_chat_command_registry_renders_surface_help_lines() {
        let lines = render_shared_chat_command_synopsis_lines(SharedChatCommandSurface::Web, 84);
        assert!(!lines.is_empty(), "web help lines should not be empty");
        assert!(lines.iter().any(|line| line.contains("/help")));
        assert!(lines.iter().any(|line| line.contains("/compact")));
        assert!(lines.iter().all(|line| !line.contains("/status")));
    }
}
