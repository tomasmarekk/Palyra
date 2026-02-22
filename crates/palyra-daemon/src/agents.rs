use std::{
    collections::HashSet,
    env, fs,
    path::{Component, Path, PathBuf},
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use palyra_common::{default_state_root, validate_canonical_id};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const REGISTRY_VERSION: u32 = 1;
const REGISTRY_FILE: &str = "agents.toml";
const ENV_STATE_ROOT: &str = "PALYRA_STATE_ROOT";
const ENV_REGISTRY_PATH: &str = "PALYRA_AGENTS_REGISTRY_PATH";
const DEFAULT_MODEL_PROFILE: &str = "gpt-4o-mini";
const MAX_AGENT_COUNT: usize = 1024;
const MAX_WORKSPACE_ROOTS: usize = 32;
const MAX_SESSION_BINDINGS: usize = 10_000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentRecord {
    pub agent_id: String,
    pub display_name: String,
    pub agent_dir: String,
    pub workspace_roots: Vec<String>,
    pub default_model_profile: String,
    pub default_tool_allowlist: Vec<String>,
    pub default_skill_allowlist: Vec<String>,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionAgentBinding {
    pub principal: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    pub session_id: String,
    pub agent_id: String,
    pub updated_at_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct OpenClawImportCompat {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentCreateRequest {
    pub agent_id: String,
    pub display_name: String,
    pub agent_dir: Option<String>,
    pub workspace_roots: Vec<String>,
    pub default_model_profile: Option<String>,
    pub default_tool_allowlist: Vec<String>,
    pub default_skill_allowlist: Vec<String>,
    pub set_default: bool,
    pub allow_absolute_paths: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentCreateOutcome {
    pub agent: AgentRecord,
    pub previous_default_agent_id: Option<String>,
    pub default_agent_id: Option<String>,
    pub default_changed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentSetDefaultOutcome {
    pub previous_default_agent_id: Option<String>,
    pub default_agent_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentResolveRequest {
    pub principal: String,
    pub channel: Option<String>,
    pub session_id: Option<String>,
    pub preferred_agent_id: Option<String>,
    pub persist_session_binding: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentResolutionSource {
    SessionBinding,
    Default,
    Fallback,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentResolveOutcome {
    pub agent: AgentRecord,
    pub source: AgentResolutionSource,
    pub binding_created: bool,
    pub is_default: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentStatusSnapshot {
    pub default_agent_id: Option<String>,
    pub agent_count: usize,
    pub session_bindings: Vec<SessionAgentBinding>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentListPage {
    pub agents: Vec<AgentRecord>,
    pub default_agent_id: Option<String>,
    pub next_after_agent_id: Option<String>,
}

#[derive(Debug)]
pub struct AgentRegistry {
    registry_path: PathBuf,
    state_root: PathBuf,
    state: Mutex<RegistryDocument>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RegistryDocument {
    version: u32,
    #[serde(default)]
    default_agent_id: Option<String>,
    #[serde(default)]
    agents: Vec<AgentRecord>,
    #[serde(default)]
    session_bindings: Vec<SessionAgentBinding>,
    #[serde(default)]
    openclaw_import: OpenClawImportCompat,
}

impl Default for RegistryDocument {
    fn default() -> Self {
        Self {
            version: REGISTRY_VERSION,
            default_agent_id: None,
            agents: Vec::new(),
            session_bindings: Vec::new(),
            openclaw_import: OpenClawImportCompat::default(),
        }
    }
}

#[derive(Debug, Error)]
pub enum AgentRegistryError {
    #[error("agent registry lock poisoned")]
    LockPoisoned,
    #[error("invalid path in {field}: {message}")]
    InvalidPath { field: &'static str, message: String },
    #[error("failed to read agent registry {path}: {source}")]
    ReadRegistry {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse agent registry {path}: {source}")]
    ParseRegistry {
        path: PathBuf,
        #[source]
        source: Box<toml::de::Error>,
    },
    #[error("failed to write agent registry {path}: {source}")]
    WriteRegistry {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to serialize agent registry: {0}")]
    SerializeRegistry(#[from] toml::ser::Error),
    #[error("unsupported registry version {0}")]
    UnsupportedVersion(u32),
    #[error("agent not found: {0}")]
    AgentNotFound(String),
    #[error("agent already exists: {0}")]
    DuplicateAgentId(String),
    #[error("agent directory overlaps with existing agent {0}")]
    AgentDirCollision(String),
    #[error("workspace root escapes agent dir: {0}")]
    WorkspaceRootEscape(String),
    #[error("workspace root duplicated: {0}")]
    DuplicateWorkspaceRoot(String),
    #[error("default agent is not configured")]
    DefaultAgentNotConfigured,
    #[error("invalid canonical session id: {0}")]
    InvalidSessionId(String),
    #[error("too many entries in registry")]
    RegistryLimitExceeded,
    #[error("system time before unix epoch: {0}")]
    InvalidSystemTime(#[from] std::time::SystemTimeError),
}

impl AgentRegistry {
    pub fn open(identity_store_root: &Path) -> Result<Self, AgentRegistryError> {
        let state_root = resolve_state_root(identity_store_root)?;
        let registry_path = resolve_registry_path(state_root.as_path())?;
        if let Some(parent) = registry_path.parent() {
            fs::create_dir_all(parent).map_err(|source| AgentRegistryError::WriteRegistry {
                path: parent.to_path_buf(),
                source,
            })?;
        }

        let mut document = if registry_path.exists() {
            let raw = fs::read_to_string(&registry_path).map_err(|source| {
                AgentRegistryError::ReadRegistry { path: registry_path.clone(), source }
            })?;
            toml::from_str::<RegistryDocument>(&raw).map_err(|source| {
                AgentRegistryError::ParseRegistry {
                    path: registry_path.clone(),
                    source: Box::new(source),
                }
            })?
        } else {
            RegistryDocument::default()
        };
        normalize_document(&mut document, state_root.as_path())?;
        persist_registry(registry_path.as_path(), &document)?;

        Ok(Self { registry_path, state_root, state: Mutex::new(document) })
    }

    pub fn list_agents(
        &self,
        after_agent_id: Option<&str>,
        limit: Option<usize>,
    ) -> Result<AgentListPage, AgentRegistryError> {
        let guard = self.state.lock().map_err(|_| AgentRegistryError::LockPoisoned)?;
        let limit = limit.unwrap_or(100).clamp(1, 500);
        let start = after_agent_id
            .and_then(|after| guard.agents.iter().position(|a| a.agent_id == after))
            .map_or(0, |index| index.saturating_add(1));
        let mut page = guard
            .agents
            .iter()
            .skip(start)
            .take(limit.saturating_add(1))
            .cloned()
            .collect::<Vec<_>>();
        let has_more = page.len() > limit;
        if has_more {
            page.truncate(limit);
        }
        Ok(AgentListPage {
            next_after_agent_id: if has_more {
                page.last().map(|a| a.agent_id.clone())
            } else {
                None
            },
            agents: page,
            default_agent_id: guard.default_agent_id.clone(),
        })
    }

    pub fn get_agent(&self, agent_id: &str) -> Result<(AgentRecord, bool), AgentRegistryError> {
        let agent_id = normalize_agent_id(agent_id)?;
        let guard = self.state.lock().map_err(|_| AgentRegistryError::LockPoisoned)?;
        let agent = guard
            .agents
            .iter()
            .find(|candidate| candidate.agent_id == agent_id)
            .cloned()
            .ok_or_else(|| AgentRegistryError::AgentNotFound(agent_id.clone()))?;
        Ok((agent, guard.default_agent_id.as_deref() == Some(agent_id.as_str())))
    }

    pub fn create_agent(
        &self,
        request: AgentCreateRequest,
    ) -> Result<AgentCreateOutcome, AgentRegistryError> {
        let mut guard = self.state.lock().map_err(|_| AgentRegistryError::LockPoisoned)?;
        if guard.agents.len() >= MAX_AGENT_COUNT {
            return Err(AgentRegistryError::RegistryLimitExceeded);
        }
        let agent_id = normalize_agent_id(request.agent_id.as_str())?;
        if guard.agents.iter().any(|agent| agent.agent_id == agent_id) {
            return Err(AgentRegistryError::DuplicateAgentId(agent_id));
        }
        let display_name = normalize_required_text(request.display_name.as_str(), "display_name")?;
        let default_model_profile = normalize_required_text(
            request.default_model_profile.as_deref().unwrap_or(DEFAULT_MODEL_PROFILE),
            "default_model_profile",
        )?;
        let agent_dir = resolve_agent_dir(
            request.agent_dir.as_deref(),
            agent_id.as_str(),
            self.state_root.as_path(),
            request.allow_absolute_paths,
        )?;
        let agent_dir_key = canonical_path_key(agent_dir.as_path());
        for existing in &guard.agents {
            if canonical_path_key(Path::new(existing.agent_dir.as_str())) == agent_dir_key {
                return Err(AgentRegistryError::AgentDirCollision(existing.agent_id.clone()));
            }
        }
        let workspace_roots = resolve_workspace_roots(
            request.workspace_roots.as_slice(),
            agent_dir.as_path(),
            request.allow_absolute_paths,
        )?;
        let now = current_unix_ms()?;
        let record = AgentRecord {
            agent_id: agent_id.clone(),
            display_name,
            agent_dir: agent_dir.to_string_lossy().into_owned(),
            workspace_roots: workspace_roots
                .iter()
                .map(|path| path.to_string_lossy().into_owned())
                .collect(),
            default_model_profile,
            default_tool_allowlist: normalize_allowlist(request.default_tool_allowlist),
            default_skill_allowlist: normalize_allowlist(request.default_skill_allowlist),
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        };
        let previous_default_agent_id = guard.default_agent_id.clone();
        guard.agents.push(record.clone());
        guard.agents.sort_by(|left, right| left.agent_id.cmp(&right.agent_id));

        let mut default_changed = false;
        if guard.default_agent_id.is_none() || request.set_default {
            guard.default_agent_id = Some(agent_id);
            default_changed = previous_default_agent_id != guard.default_agent_id;
        }
        persist_registry(self.registry_path.as_path(), &guard)?;
        Ok(AgentCreateOutcome {
            previous_default_agent_id,
            default_agent_id: guard.default_agent_id.clone(),
            default_changed,
            agent: record,
        })
    }

    pub fn set_default_agent(
        &self,
        agent_id: &str,
    ) -> Result<AgentSetDefaultOutcome, AgentRegistryError> {
        let agent_id = normalize_agent_id(agent_id)?;
        let mut guard = self.state.lock().map_err(|_| AgentRegistryError::LockPoisoned)?;
        if !guard.agents.iter().any(|agent| agent.agent_id == agent_id) {
            return Err(AgentRegistryError::AgentNotFound(agent_id));
        }
        let previous_default_agent_id = guard.default_agent_id.clone();
        guard.default_agent_id = Some(agent_id.clone());
        if previous_default_agent_id != guard.default_agent_id {
            persist_registry(self.registry_path.as_path(), &guard)?;
        }
        Ok(AgentSetDefaultOutcome { previous_default_agent_id, default_agent_id: agent_id })
    }

    pub fn resolve_agent_for_context(
        &self,
        request: AgentResolveRequest,
    ) -> Result<AgentResolveOutcome, AgentRegistryError> {
        let principal = normalize_required_text(request.principal.as_str(), "principal")?;
        let channel = normalize_optional_text(request.channel.as_deref());
        let session_id = if let Some(value) = request.session_id {
            validate_canonical_id(value.as_str())
                .map_err(|_| AgentRegistryError::InvalidSessionId(value.clone()))?;
            Some(value)
        } else {
            None
        };

        let mut guard = self.state.lock().map_err(|_| AgentRegistryError::LockPoisoned)?;
        if guard.agents.is_empty() {
            return Err(AgentRegistryError::DefaultAgentNotConfigured);
        }

        let preferred_agent_id =
            request.preferred_agent_id.as_deref().map(normalize_agent_id).transpose()?;
        let mut source = AgentResolutionSource::Fallback;
        let resolved_agent_id = if let Some(preferred) = preferred_agent_id {
            if !guard.agents.iter().any(|agent| agent.agent_id == preferred) {
                return Err(AgentRegistryError::AgentNotFound(preferred));
            }
            preferred
        } else if let Some(session_id_value) = session_id.as_deref() {
            if let Some(binding) = guard.session_bindings.iter().find(|binding| {
                binding.principal == principal
                    && binding.channel == channel
                    && binding.session_id == session_id_value
            }) {
                source = AgentResolutionSource::SessionBinding;
                binding.agent_id.clone()
            } else if let Some(default_agent_id) = guard.default_agent_id.clone() {
                source = AgentResolutionSource::Default;
                default_agent_id
            } else {
                guard
                    .agents
                    .first()
                    .map(|agent| agent.agent_id.clone())
                    .ok_or(AgentRegistryError::DefaultAgentNotConfigured)?
            }
        } else if let Some(default_agent_id) = guard.default_agent_id.clone() {
            source = AgentResolutionSource::Default;
            default_agent_id
        } else {
            guard
                .agents
                .first()
                .map(|agent| agent.agent_id.clone())
                .ok_or(AgentRegistryError::DefaultAgentNotConfigured)?
        };

        let mut binding_created = false;
        if request.persist_session_binding {
            if let Some(session_id_value) = session_id {
                let now = current_unix_ms()?;
                if let Some(binding) = guard.session_bindings.iter_mut().find(|binding| {
                    binding.principal == principal
                        && binding.channel == channel
                        && binding.session_id == session_id_value
                }) {
                    if binding.agent_id != resolved_agent_id {
                        binding.agent_id = resolved_agent_id.clone();
                        binding.updated_at_unix_ms = now;
                        binding_created = true;
                    }
                } else {
                    guard.session_bindings.push(SessionAgentBinding {
                        principal: principal.clone(),
                        channel: channel.clone(),
                        session_id: session_id_value,
                        agent_id: resolved_agent_id.clone(),
                        updated_at_unix_ms: now,
                    });
                    binding_created = true;
                }
                if guard.session_bindings.len() > MAX_SESSION_BINDINGS {
                    guard.session_bindings.sort_by(|left, right| {
                        right.updated_at_unix_ms.cmp(&left.updated_at_unix_ms)
                    });
                    guard.session_bindings.truncate(MAX_SESSION_BINDINGS);
                }
            }
        }

        let agent = guard
            .agents
            .iter()
            .find(|candidate| candidate.agent_id == resolved_agent_id)
            .cloned()
            .ok_or_else(|| AgentRegistryError::AgentNotFound(resolved_agent_id.clone()))?;
        if binding_created {
            persist_registry(self.registry_path.as_path(), &guard)?;
        }
        Ok(AgentResolveOutcome {
            is_default: guard.default_agent_id.as_deref() == Some(resolved_agent_id.as_str()),
            agent,
            source,
            binding_created,
        })
    }

    pub fn status_snapshot(&self) -> Result<AgentStatusSnapshot, AgentRegistryError> {
        let guard = self.state.lock().map_err(|_| AgentRegistryError::LockPoisoned)?;
        Ok(AgentStatusSnapshot {
            default_agent_id: guard.default_agent_id.clone(),
            agent_count: guard.agents.len(),
            session_bindings: guard.session_bindings.clone(),
        })
    }
}

fn resolve_state_root(identity_store_root: &Path) -> Result<PathBuf, AgentRegistryError> {
    if let Ok(raw) = env::var(ENV_STATE_ROOT) {
        let raw = parse_path_literal(raw.as_str(), "state_root")?;
        return ensure_canonical_dir(raw.as_path(), "state_root");
    }
    if let Some(parent) = identity_store_root.parent() {
        return ensure_canonical_dir(parent, "state_root");
    }
    let fallback = default_state_root().map_err(|error| AgentRegistryError::InvalidPath {
        field: "state_root",
        message: error.to_string(),
    })?;
    ensure_canonical_dir(fallback.as_path(), "state_root")
}

fn resolve_registry_path(state_root: &Path) -> Result<PathBuf, AgentRegistryError> {
    if let Ok(raw) = env::var(ENV_REGISTRY_PATH) {
        let parsed = parse_path_literal(raw.as_str(), "registry_path")?;
        if parsed.is_absolute() {
            return Ok(parsed);
        }
        return Ok(state_root.join(parsed));
    }
    Ok(state_root.join(REGISTRY_FILE))
}

fn normalize_document(
    document: &mut RegistryDocument,
    state_root: &Path,
) -> Result<(), AgentRegistryError> {
    if document.version == 0 {
        document.version = REGISTRY_VERSION;
    }
    if document.version != REGISTRY_VERSION {
        return Err(AgentRegistryError::UnsupportedVersion(document.version));
    }
    if document.agents.len() > MAX_AGENT_COUNT {
        return Err(AgentRegistryError::RegistryLimitExceeded);
    }

    let mut seen_dirs = HashSet::new();
    for agent in &mut document.agents {
        agent.agent_id = normalize_agent_id(agent.agent_id.as_str())?;
        agent.display_name = normalize_required_text(agent.display_name.as_str(), "display_name")?;
        agent.default_model_profile =
            normalize_required_text(agent.default_model_profile.as_str(), "default_model_profile")?;
        agent.default_tool_allowlist = normalize_allowlist(agent.default_tool_allowlist.clone());
        agent.default_skill_allowlist = normalize_allowlist(agent.default_skill_allowlist.clone());

        let parsed_agent_dir = parse_path_literal(agent.agent_dir.as_str(), "agent_dir")?;
        let candidate = if parsed_agent_dir.is_absolute() {
            parsed_agent_dir
        } else {
            state_root.join(parsed_agent_dir)
        };
        let canonical_agent_dir = ensure_canonical_dir(candidate.as_path(), "agent_dir")?;
        let key = canonical_path_key(canonical_agent_dir.as_path());
        if !seen_dirs.insert(key) {
            return Err(AgentRegistryError::AgentDirCollision(agent.agent_id.clone()));
        }
        agent.agent_dir = canonical_agent_dir.to_string_lossy().into_owned();

        let roots = if agent.workspace_roots.is_empty() {
            vec![canonical_agent_dir.join("workspace")]
        } else {
            let mut resolved = Vec::new();
            for root in &agent.workspace_roots {
                let parsed = parse_path_literal(root.as_str(), "workspace_root")?;
                let candidate =
                    if parsed.is_absolute() { parsed } else { canonical_agent_dir.join(parsed) };
                let canonical_workspace =
                    ensure_canonical_dir(candidate.as_path(), "workspace_root")?;
                if !canonical_workspace.starts_with(canonical_agent_dir.as_path()) {
                    return Err(AgentRegistryError::WorkspaceRootEscape(
                        canonical_workspace.to_string_lossy().into_owned(),
                    ));
                }
                resolved.push(canonical_workspace);
            }
            resolved
        };
        agent.workspace_roots =
            roots.iter().map(|root| root.to_string_lossy().into_owned()).collect();
    }
    document.agents.sort_by(|left, right| left.agent_id.cmp(&right.agent_id));

    document.session_bindings.retain(|binding| {
        if validate_canonical_id(binding.session_id.as_str()).is_err() {
            return false;
        }
        document.agents.iter().any(|agent| agent.agent_id == binding.agent_id)
    });
    if document.session_bindings.len() > MAX_SESSION_BINDINGS {
        document.session_bindings.truncate(MAX_SESSION_BINDINGS);
    }
    if let Some(default_agent_id) = document.default_agent_id.as_deref() {
        let normalized = normalize_agent_id(default_agent_id)?;
        if document.agents.iter().any(|agent| agent.agent_id == normalized) {
            document.default_agent_id = Some(normalized);
        } else {
            document.default_agent_id = None;
        }
    }
    Ok(())
}

fn persist_registry(path: &Path, document: &RegistryDocument) -> Result<(), AgentRegistryError> {
    let payload = toml::to_string_pretty(document)?;
    fs::write(path, payload)
        .map_err(|source| AgentRegistryError::WriteRegistry { path: path.to_path_buf(), source })
}

fn resolve_agent_dir(
    raw_agent_dir: Option<&str>,
    agent_id: &str,
    state_root: &Path,
    allow_absolute_paths: bool,
) -> Result<PathBuf, AgentRegistryError> {
    let candidate = if let Some(raw) = raw_agent_dir {
        let parsed = parse_path_literal(raw, "agent_dir")?;
        if parsed.is_absolute() {
            if !allow_absolute_paths {
                return Err(AgentRegistryError::InvalidPath {
                    field: "agent_dir",
                    message: "absolute paths require allow_absolute_paths=true".to_owned(),
                });
            }
            parsed
        } else {
            state_root.join(parsed)
        }
    } else {
        state_root.join("agents").join(agent_id)
    };
    ensure_canonical_dir(candidate.as_path(), "agent_dir")
}

fn resolve_workspace_roots(
    raw_workspace_roots: &[String],
    agent_dir: &Path,
    allow_absolute_paths: bool,
) -> Result<Vec<PathBuf>, AgentRegistryError> {
    let raw_values = if raw_workspace_roots.is_empty() {
        vec!["workspace".to_owned()]
    } else {
        raw_workspace_roots.to_vec()
    };
    if raw_values.len() > MAX_WORKSPACE_ROOTS {
        return Err(AgentRegistryError::RegistryLimitExceeded);
    }

    let mut roots = Vec::new();
    let mut seen = HashSet::new();
    for raw in raw_values {
        let parsed = parse_path_literal(raw.as_str(), "workspace_root")?;
        let candidate = if parsed.is_absolute() {
            if !allow_absolute_paths {
                return Err(AgentRegistryError::InvalidPath {
                    field: "workspace_root",
                    message: "absolute paths require allow_absolute_paths=true".to_owned(),
                });
            }
            parsed
        } else {
            agent_dir.join(parsed)
        };
        let canonical = ensure_canonical_dir(candidate.as_path(), "workspace_root")?;
        if !canonical.starts_with(agent_dir) {
            return Err(AgentRegistryError::WorkspaceRootEscape(
                canonical.to_string_lossy().into_owned(),
            ));
        }
        let key = canonical_path_key(canonical.as_path());
        if !seen.insert(key) {
            return Err(AgentRegistryError::DuplicateWorkspaceRoot(
                canonical.to_string_lossy().into_owned(),
            ));
        }
        roots.push(canonical);
    }
    Ok(roots)
}

fn parse_path_literal(raw: &str, field: &'static str) -> Result<PathBuf, AgentRegistryError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(AgentRegistryError::InvalidPath {
            field,
            message: "cannot be empty".to_owned(),
        });
    }
    if trimmed.contains('\0') {
        return Err(AgentRegistryError::InvalidPath {
            field,
            message: "contains embedded NUL byte".to_owned(),
        });
    }
    let path = PathBuf::from(trimmed);
    if path.components().any(|component| matches!(component, Component::ParentDir)) {
        return Err(AgentRegistryError::InvalidPath {
            field,
            message: "cannot contain parent traversal ('..')".to_owned(),
        });
    }
    Ok(path)
}

fn ensure_canonical_dir(path: &Path, field: &'static str) -> Result<PathBuf, AgentRegistryError> {
    fs::create_dir_all(path)
        .map_err(|source| AgentRegistryError::WriteRegistry { path: path.to_path_buf(), source })?;
    fs::canonicalize(path).map_err(|source| AgentRegistryError::InvalidPath {
        field,
        message: format!("failed to canonicalize path '{}': {source}", path.display()),
    })
}

fn normalize_agent_id(raw: &str) -> Result<String, AgentRegistryError> {
    let value = raw.trim();
    if value.is_empty() {
        return Err(AgentRegistryError::InvalidPath {
            field: "agent_id",
            message: "cannot be empty".to_owned(),
        });
    }
    if value.len() > 64 {
        return Err(AgentRegistryError::InvalidPath {
            field: "agent_id",
            message: "cannot exceed 64 bytes".to_owned(),
        });
    }
    for character in value.chars() {
        if !(character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.')) {
            return Err(AgentRegistryError::InvalidPath {
                field: "agent_id",
                message: format!("contains unsupported character '{character}'"),
            });
        }
    }
    Ok(value.to_ascii_lowercase())
}

fn normalize_required_text(raw: &str, field: &'static str) -> Result<String, AgentRegistryError> {
    let value = raw.trim();
    if value.is_empty() {
        return Err(AgentRegistryError::InvalidPath {
            field,
            message: "cannot be empty".to_owned(),
        });
    }
    Ok(value.to_owned())
}

fn normalize_optional_text(raw: Option<&str>) -> Option<String> {
    let value = raw?.trim();
    if value.is_empty() {
        return None;
    }
    Some(value.to_owned())
}

fn normalize_allowlist(values: Vec<String>) -> Vec<String> {
    let mut normalized = values
        .into_iter()
        .filter_map(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_ascii_lowercase())
            }
        })
        .collect::<Vec<_>>();
    normalized.sort();
    normalized.dedup();
    normalized
}

fn canonical_path_key(path: &Path) -> String {
    let normalized = path
        .components()
        .map(|component| component.as_os_str().to_string_lossy().replace('\\', "/"))
        .collect::<Vec<_>>()
        .join("/");
    #[cfg(windows)]
    {
        normalized.to_ascii_lowercase()
    }
    #[cfg(not(windows))]
    {
        normalized
    }
}

fn current_unix_ms() -> Result<i64, AgentRegistryError> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{
        AgentCreateRequest, AgentRegistry, AgentRegistryError, AgentResolutionSource,
        AgentResolveRequest,
    };

    #[test]
    fn create_agent_rejects_canonicalized_agent_dir_collision() {
        let temp = tempdir().expect("tempdir should be created");
        let registry = AgentRegistry::open(temp.path().join("identity").as_path())
            .expect("registry should initialize");

        registry
            .create_agent(AgentCreateRequest {
                agent_id: "main".to_owned(),
                display_name: "Main".to_owned(),
                agent_dir: Some("agents/main".to_owned()),
                workspace_roots: vec!["workspace".to_owned()],
                default_model_profile: Some("gpt-4o-mini".to_owned()),
                default_tool_allowlist: Vec::new(),
                default_skill_allowlist: Vec::new(),
                set_default: true,
                allow_absolute_paths: false,
            })
            .expect("first create should succeed");

        let duplicate = registry.create_agent(AgentCreateRequest {
            agent_id: "review".to_owned(),
            display_name: "Review".to_owned(),
            agent_dir: Some("agents/./main".to_owned()),
            workspace_roots: vec!["workspace".to_owned()],
            default_model_profile: Some("gpt-4o-mini".to_owned()),
            default_tool_allowlist: Vec::new(),
            default_skill_allowlist: Vec::new(),
            set_default: false,
            allow_absolute_paths: false,
        });
        assert!(matches!(duplicate, Err(AgentRegistryError::AgentDirCollision(_))));
    }

    #[test]
    fn resolve_agent_for_context_persists_session_binding() {
        let temp = tempdir().expect("tempdir should be created");
        let registry = AgentRegistry::open(temp.path().join("identity").as_path())
            .expect("registry should initialize");
        registry
            .create_agent(AgentCreateRequest {
                agent_id: "main".to_owned(),
                display_name: "Main".to_owned(),
                agent_dir: None,
                workspace_roots: Vec::new(),
                default_model_profile: None,
                default_tool_allowlist: Vec::new(),
                default_skill_allowlist: Vec::new(),
                set_default: true,
                allow_absolute_paths: false,
            })
            .expect("create should succeed");

        let first = registry
            .resolve_agent_for_context(AgentResolveRequest {
                principal: "admin:ops".to_owned(),
                channel: Some("cli".to_owned()),
                session_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned()),
                preferred_agent_id: None,
                persist_session_binding: true,
            })
            .expect("first resolve should succeed");
        assert_eq!(first.source, AgentResolutionSource::Default);
        assert!(first.binding_created);

        let second = registry
            .resolve_agent_for_context(AgentResolveRequest {
                principal: "admin:ops".to_owned(),
                channel: Some("cli".to_owned()),
                session_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned()),
                preferred_agent_id: None,
                persist_session_binding: true,
            })
            .expect("second resolve should succeed");
        assert_eq!(second.source, AgentResolutionSource::SessionBinding);
        assert!(!second.binding_created);
    }

    #[test]
    fn registry_reopen_preserves_agents_default_and_session_binding() {
        let temp = tempdir().expect("tempdir should be created");
        let identity_root = temp.path().join("state").join("identity");
        let registry =
            AgentRegistry::open(identity_root.as_path()).expect("registry should initialize");
        registry
            .create_agent(AgentCreateRequest {
                agent_id: "main".to_owned(),
                display_name: "Main".to_owned(),
                agent_dir: Some("agents/main".to_owned()),
                workspace_roots: vec!["workspace".to_owned()],
                default_model_profile: Some("gpt-4o-mini".to_owned()),
                default_tool_allowlist: Vec::new(),
                default_skill_allowlist: Vec::new(),
                set_default: true,
                allow_absolute_paths: false,
            })
            .expect("main agent should be created");
        registry
            .create_agent(AgentCreateRequest {
                agent_id: "review".to_owned(),
                display_name: "Review".to_owned(),
                agent_dir: Some("agents/review".to_owned()),
                workspace_roots: vec!["workspace-review".to_owned()],
                default_model_profile: Some("gpt-4o-mini".to_owned()),
                default_tool_allowlist: Vec::new(),
                default_skill_allowlist: Vec::new(),
                set_default: false,
                allow_absolute_paths: false,
            })
            .expect("review agent should be created");
        registry.set_default_agent("review").expect("set default should succeed");
        let first = registry
            .resolve_agent_for_context(AgentResolveRequest {
                principal: "admin:ops".to_owned(),
                channel: Some("cli".to_owned()),
                session_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned()),
                preferred_agent_id: Some("main".to_owned()),
                persist_session_binding: true,
            })
            .expect("first resolve should succeed");
        assert_eq!(first.agent.agent_id, "main");
        assert!(first.binding_created);
        drop(registry);

        let reopened =
            AgentRegistry::open(identity_root.as_path()).expect("registry should reopen");
        let page = reopened.list_agents(None, Some(10)).expect("list should succeed");
        assert_eq!(page.agents.len(), 2);
        assert_eq!(page.default_agent_id.as_deref(), Some("review"));
        let second = reopened
            .resolve_agent_for_context(AgentResolveRequest {
                principal: "admin:ops".to_owned(),
                channel: Some("cli".to_owned()),
                session_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned()),
                preferred_agent_id: None,
                persist_session_binding: true,
            })
            .expect("second resolve should succeed");
        assert_eq!(second.agent.agent_id, "main");
        assert_eq!(second.source, AgentResolutionSource::SessionBinding);
    }

    #[cfg(unix)]
    #[test]
    fn create_agent_rejects_workspace_symlink_escape() {
        use std::os::unix::fs::symlink;

        let temp = tempdir().expect("tempdir should be created");
        let outside = temp.path().join("outside");
        std::fs::create_dir_all(&outside).expect("outside should exist");

        let registry = AgentRegistry::open(temp.path().join("identity").as_path())
            .expect("registry should initialize");
        let agent_dir = temp.path().join("agents").join("main");
        std::fs::create_dir_all(&agent_dir).expect("agent dir should exist");
        symlink(&outside, agent_dir.join("escape")).expect("symlink should be created");

        let result = registry.create_agent(AgentCreateRequest {
            agent_id: "main".to_owned(),
            display_name: "Main".to_owned(),
            agent_dir: Some(agent_dir.to_string_lossy().into_owned()),
            workspace_roots: vec!["escape".to_owned()],
            default_model_profile: None,
            default_tool_allowlist: Vec::new(),
            default_skill_allowlist: Vec::new(),
            set_default: false,
            allow_absolute_paths: true,
        });
        assert!(matches!(result, Err(AgentRegistryError::WorkspaceRootEscape(_))));
    }
}
