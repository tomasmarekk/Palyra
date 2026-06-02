use std::{
    collections::HashSet,
    env, fs,
    path::{Component, Path, PathBuf},
    sync::Mutex,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::execution_backends::ExecutionBackendPreference;
use palyra_common::{default_state_root, validate_canonical_id};
use palyra_vault::{ensure_owner_only_dir, ensure_owner_only_file};
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
const REGISTRY_LOCK_MAX_ATTEMPTS: u32 = 40;
const REGISTRY_LOCK_RETRY_DELAY_MS: u64 = 25;
const REGISTRY_LOCK_STALE_AFTER_SECS: u64 = 30;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentRecord {
    pub agent_id: String,
    pub display_name: String,
    pub agent_dir: String,
    pub workspace_roots: Vec<String>,
    pub default_model_profile: String,
    #[serde(default)]
    pub execution_backend_preference: ExecutionBackendPreference,
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
    pub execution_backend_preference: Option<ExecutionBackendPreference>,
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
pub struct AgentDeleteOutcome {
    pub deleted_agent_id: String,
    pub deleted: bool,
    pub removed_bindings_count: usize,
    pub previous_default_agent_id: Option<String>,
    pub default_agent_id: Option<String>,
    pub agent_dir: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentBindingRequest {
    pub agent_id: String,
    pub principal: String,
    pub channel: Option<String>,
    pub session_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentBindingOutcome {
    pub binding: SessionAgentBinding,
    pub created: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentBindingQuery {
    pub agent_id: Option<String>,
    pub principal: Option<String>,
    pub channel: Option<String>,
    pub session_id: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentUnbindRequest {
    pub principal: String,
    pub channel: Option<String>,
    pub session_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentUnbindOutcome {
    pub removed: bool,
    pub removed_agent_id: Option<String>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AgentDefaultEnsureOutcome {
    AlreadyConfigured { agent_id: String },
    Created { agent_id: String },
    SelectedExisting { agent_id: String },
    Updated { agent_id: String },
    SkippedMultipleAgents { observed_agent_count: usize },
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
        let mut next = guard.clone();
        if next.agents.len() >= MAX_AGENT_COUNT {
            return Err(AgentRegistryError::RegistryLimitExceeded);
        }
        let agent_id = normalize_agent_id(request.agent_id.as_str())?;
        if next.agents.iter().any(|agent| agent.agent_id == agent_id) {
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
        for existing in &next.agents {
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
            execution_backend_preference: request.execution_backend_preference.unwrap_or_default(),
            default_tool_allowlist: normalize_allowlist(request.default_tool_allowlist),
            default_skill_allowlist: normalize_allowlist(request.default_skill_allowlist),
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        };
        let previous_default_agent_id = next.default_agent_id.clone();
        next.agents.push(record.clone());
        next.agents.sort_by(|left, right| left.agent_id.cmp(&right.agent_id));

        let mut default_changed = false;
        if next.default_agent_id.is_none() || request.set_default {
            next.default_agent_id = Some(agent_id);
            default_changed = previous_default_agent_id != next.default_agent_id;
        }
        persist_registry(self.registry_path.as_path(), &next)?;
        let default_agent_id = next.default_agent_id.clone();
        *guard = next;
        Ok(AgentCreateOutcome {
            previous_default_agent_id,
            default_agent_id,
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
        let mut next = guard.clone();
        if !next.agents.iter().any(|agent| agent.agent_id == agent_id) {
            return Err(AgentRegistryError::AgentNotFound(agent_id));
        }
        let previous_default_agent_id = next.default_agent_id.clone();
        next.default_agent_id = Some(agent_id.clone());
        if previous_default_agent_id != next.default_agent_id {
            persist_registry(self.registry_path.as_path(), &next)?;
            *guard = next;
        }
        Ok(AgentSetDefaultOutcome { previous_default_agent_id, default_agent_id: agent_id })
    }

    pub(crate) fn ensure_local_default_agent(
        &self,
        workspace_root: &Path,
        default_model_profile: Option<String>,
    ) -> Result<AgentDefaultEnsureOutcome, AgentRegistryError> {
        let page = self.list_agents(None, Some(2))?;
        if let Some(agent_id) = page.default_agent_id {
            if agent_id == "local-default"
                && self
                    .sync_local_default_agent_workspace_root(agent_id.as_str(), workspace_root)?
            {
                return Ok(AgentDefaultEnsureOutcome::Updated { agent_id });
            }
            return Ok(AgentDefaultEnsureOutcome::AlreadyConfigured { agent_id });
        }

        match page.agents.as_slice() {
            [] => {
                let outcome = self.create_agent(AgentCreateRequest {
                    agent_id: "local-default".to_owned(),
                    display_name: "LocalDefaultAgent".to_owned(),
                    agent_dir: None,
                    workspace_roots: vec![workspace_root.to_string_lossy().into_owned()],
                    default_model_profile,
                    execution_backend_preference: None,
                    default_tool_allowlist: Vec::new(),
                    default_skill_allowlist: Vec::new(),
                    set_default: true,
                    allow_absolute_paths: true,
                })?;
                Ok(AgentDefaultEnsureOutcome::Created { agent_id: outcome.agent.agent_id })
            }
            [agent] => {
                let outcome = self.set_default_agent(agent.agent_id.as_str())?;
                if outcome.default_agent_id == "local-default"
                    && self.sync_local_default_agent_workspace_root(
                        outcome.default_agent_id.as_str(),
                        workspace_root,
                    )?
                {
                    return Ok(AgentDefaultEnsureOutcome::Updated {
                        agent_id: outcome.default_agent_id,
                    });
                }
                Ok(AgentDefaultEnsureOutcome::SelectedExisting {
                    agent_id: outcome.default_agent_id,
                })
            }
            _ => Ok(AgentDefaultEnsureOutcome::SkippedMultipleAgents {
                observed_agent_count: page.agents.len(),
            }),
        }
    }

    fn sync_local_default_agent_workspace_root(
        &self,
        agent_id: &str,
        workspace_root: &Path,
    ) -> Result<bool, AgentRegistryError> {
        let mut guard = self.state.lock().map_err(|_| AgentRegistryError::LockPoisoned)?;
        let mut next = guard.clone();
        let agent = next
            .agents
            .iter_mut()
            .find(|candidate| candidate.agent_id == agent_id)
            .ok_or_else(|| AgentRegistryError::AgentNotFound(agent_id.to_owned()))?;
        if agent.agent_id != "local-default" {
            return Ok(false);
        }

        let agent_dir = PathBuf::from(agent.agent_dir.as_str());
        let workspace_roots = resolve_workspace_roots(
            &[workspace_root.to_string_lossy().into_owned()],
            agent_dir.as_path(),
            true,
        )?;
        let workspace_roots = workspace_roots
            .iter()
            .map(|path| path.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        if agent.workspace_roots == workspace_roots {
            return Ok(false);
        }

        agent.workspace_roots = workspace_roots;
        agent.updated_at_unix_ms = current_unix_ms()?;
        persist_registry(self.registry_path.as_path(), &next)?;
        *guard = next;
        Ok(true)
    }

    pub fn delete_agent(&self, agent_id: &str) -> Result<AgentDeleteOutcome, AgentRegistryError> {
        let agent_id = normalize_agent_id(agent_id)?;
        let mut guard = self.state.lock().map_err(|_| AgentRegistryError::LockPoisoned)?;
        let mut next = guard.clone();
        let index = next
            .agents
            .iter()
            .position(|agent| agent.agent_id == agent_id)
            .ok_or_else(|| AgentRegistryError::AgentNotFound(agent_id.clone()))?;
        let removed_agent = next.agents.remove(index);
        let previous_default_agent_id = next.default_agent_id.clone();
        let removed_bindings_count =
            next.session_bindings.iter().filter(|binding| binding.agent_id == agent_id).count();
        next.session_bindings.retain(|binding| binding.agent_id != agent_id);
        if previous_default_agent_id.as_deref() == Some(agent_id.as_str()) {
            next.default_agent_id = next.agents.first().map(|agent| agent.agent_id.clone());
        }
        persist_registry(self.registry_path.as_path(), &next)?;
        let default_agent_id = next.default_agent_id.clone();
        *guard = next;
        Ok(AgentDeleteOutcome {
            deleted_agent_id: agent_id,
            deleted: true,
            removed_bindings_count,
            previous_default_agent_id,
            default_agent_id,
            agent_dir: removed_agent.agent_dir,
        })
    }

    pub fn list_bindings(
        &self,
        query: AgentBindingQuery,
    ) -> Result<Vec<SessionAgentBinding>, AgentRegistryError> {
        let agent_id = query.agent_id.as_deref().map(normalize_agent_id).transpose()?;
        let principal = query
            .principal
            .as_deref()
            .map(|value| normalize_required_text(value, "principal"))
            .transpose()?;
        let channel = normalize_optional_text(query.channel.as_deref());
        let session_id = if let Some(value) = query.session_id {
            validate_canonical_id(value.as_str())
                .map_err(|_| AgentRegistryError::InvalidSessionId(value.clone()))?;
            Some(value)
        } else {
            None
        };
        let limit = query.limit.unwrap_or(500).clamp(1, 5_000);
        let guard = self.state.lock().map_err(|_| AgentRegistryError::LockPoisoned)?;
        let mut bindings = guard
            .session_bindings
            .iter()
            .filter(|binding| {
                agent_id.as_deref().is_none_or(|value| binding.agent_id == value)
                    && principal.as_deref().is_none_or(|value| binding.principal == value)
                    && channel
                        .as_deref()
                        .is_none_or(|value| binding.channel.as_deref() == Some(value))
                    && session_id.as_deref().is_none_or(|value| binding.session_id == value)
            })
            .cloned()
            .collect::<Vec<_>>();
        bindings.sort_by(|left, right| {
            right
                .updated_at_unix_ms
                .cmp(&left.updated_at_unix_ms)
                .then_with(|| left.agent_id.cmp(&right.agent_id))
                .then_with(|| left.session_id.cmp(&right.session_id))
        });
        bindings.truncate(limit);
        Ok(bindings)
    }

    pub fn bind_agent_for_context(
        &self,
        request: AgentBindingRequest,
    ) -> Result<AgentBindingOutcome, AgentRegistryError> {
        let agent_id = normalize_agent_id(request.agent_id.as_str())?;
        let principal = normalize_required_text(request.principal.as_str(), "principal")?;
        let channel = normalize_optional_text(request.channel.as_deref());
        validate_canonical_id(request.session_id.as_str())
            .map_err(|_| AgentRegistryError::InvalidSessionId(request.session_id.clone()))?;

        let mut guard = self.state.lock().map_err(|_| AgentRegistryError::LockPoisoned)?;
        let mut next = guard.clone();
        if !next.agents.iter().any(|agent| agent.agent_id == agent_id) {
            return Err(AgentRegistryError::AgentNotFound(agent_id));
        }
        let now = current_unix_ms()?;
        let mut created = false;
        let binding = if let Some(existing) = next.session_bindings.iter_mut().find(|binding| {
            binding.principal == principal
                && binding.channel == channel
                && binding.session_id == request.session_id
        }) {
            if existing.agent_id != agent_id {
                existing.agent_id = agent_id.clone();
            }
            existing.updated_at_unix_ms = now;
            existing.clone()
        } else {
            created = true;
            let binding = SessionAgentBinding {
                principal: principal.clone(),
                channel: channel.clone(),
                session_id: request.session_id.clone(),
                agent_id: agent_id.clone(),
                updated_at_unix_ms: now,
            };
            next.session_bindings.push(binding.clone());
            if next.session_bindings.len() > MAX_SESSION_BINDINGS {
                next.session_bindings
                    .sort_by_key(|binding| std::cmp::Reverse(binding.updated_at_unix_ms));
                next.session_bindings.truncate(MAX_SESSION_BINDINGS);
            }
            binding
        };
        persist_registry(self.registry_path.as_path(), &next)?;
        *guard = next;
        Ok(AgentBindingOutcome { binding, created })
    }

    pub fn unbind_agent_for_context(
        &self,
        request: AgentUnbindRequest,
    ) -> Result<AgentUnbindOutcome, AgentRegistryError> {
        let principal = normalize_required_text(request.principal.as_str(), "principal")?;
        let channel = normalize_optional_text(request.channel.as_deref());
        validate_canonical_id(request.session_id.as_str())
            .map_err(|_| AgentRegistryError::InvalidSessionId(request.session_id.clone()))?;

        let mut guard = self.state.lock().map_err(|_| AgentRegistryError::LockPoisoned)?;
        let mut next = guard.clone();
        let Some(index) = next.session_bindings.iter().position(|binding| {
            binding.principal == principal
                && binding.channel == channel
                && binding.session_id == request.session_id
        }) else {
            return Ok(AgentUnbindOutcome { removed: false, removed_agent_id: None });
        };
        let removed_agent_id = Some(next.session_bindings.remove(index).agent_id);
        persist_registry(self.registry_path.as_path(), &next)?;
        *guard = next;
        Ok(AgentUnbindOutcome { removed: true, removed_agent_id })
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
        let mut next = guard.clone();

        let preferred_agent_id =
            request.preferred_agent_id.as_deref().map(normalize_agent_id).transpose()?;
        let mut source = AgentResolutionSource::Fallback;
        let resolved_agent_id = if let Some(preferred) = preferred_agent_id {
            if !next.agents.iter().any(|agent| agent.agent_id == preferred) {
                return Err(AgentRegistryError::AgentNotFound(preferred));
            }
            preferred
        } else if let Some(session_id_value) = session_id.as_deref() {
            if let Some(binding) = next.session_bindings.iter().find(|binding| {
                binding.principal == principal
                    && binding.channel == channel
                    && binding.session_id == session_id_value
            }) {
                source = AgentResolutionSource::SessionBinding;
                binding.agent_id.clone()
            } else if let Some(default_agent_id) = next.default_agent_id.clone() {
                source = AgentResolutionSource::Default;
                default_agent_id
            } else {
                next.agents
                    .first()
                    .map(|agent| agent.agent_id.clone())
                    .ok_or(AgentRegistryError::DefaultAgentNotConfigured)?
            }
        } else if let Some(default_agent_id) = next.default_agent_id.clone() {
            source = AgentResolutionSource::Default;
            default_agent_id
        } else {
            next.agents
                .first()
                .map(|agent| agent.agent_id.clone())
                .ok_or(AgentRegistryError::DefaultAgentNotConfigured)?
        };

        let mut binding_created = false;
        if request.persist_session_binding {
            if let Some(session_id_value) = session_id {
                let now = current_unix_ms()?;
                if let Some(binding) = next.session_bindings.iter_mut().find(|binding| {
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
                    next.session_bindings.push(SessionAgentBinding {
                        principal: principal.clone(),
                        channel: channel.clone(),
                        session_id: session_id_value,
                        agent_id: resolved_agent_id.clone(),
                        updated_at_unix_ms: now,
                    });
                    binding_created = true;
                }
                if next.session_bindings.len() > MAX_SESSION_BINDINGS {
                    next.session_bindings.sort_by(|left, right| {
                        right.updated_at_unix_ms.cmp(&left.updated_at_unix_ms)
                    });
                    next.session_bindings.truncate(MAX_SESSION_BINDINGS);
                }
            }
        }

        let agent = next
            .agents
            .iter()
            .find(|candidate| candidate.agent_id == resolved_agent_id)
            .cloned()
            .ok_or_else(|| AgentRegistryError::AgentNotFound(resolved_agent_id.clone()))?;
        let is_default = next.default_agent_id.as_deref() == Some(resolved_agent_id.as_str());
        if binding_created {
            persist_registry(self.registry_path.as_path(), &next)?;
            *guard = next;
        }
        Ok(AgentResolveOutcome { is_default, agent, source, binding_created })
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
                let parsed_absolute = parsed.is_absolute();
                let candidate =
                    if parsed_absolute { parsed } else { canonical_agent_dir.join(parsed) };
                let canonical_workspace =
                    ensure_canonical_dir(candidate.as_path(), "workspace_root")?;
                if !parsed_absolute
                    && !canonical_workspace.starts_with(canonical_agent_dir.as_path())
                {
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
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| AgentRegistryError::WriteRegistry {
            path: parent.to_path_buf(),
            source,
        })?;
        harden_registry_dir_permissions(parent).map_err(|source| {
            AgentRegistryError::WriteRegistry { path: parent.to_path_buf(), source }
        })?;
    }
    let _lock = acquire_registry_lock(path)
        .map_err(|source| AgentRegistryError::WriteRegistry { path: path.to_path_buf(), source })?;
    let payload = toml::to_string_pretty(document)?;
    write_registry_atomically(path, payload.as_str())
        .map_err(|source| AgentRegistryError::WriteRegistry { path: path.to_path_buf(), source })
}

struct RegistryLock {
    lock_path: PathBuf,
}

impl Drop for RegistryLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.lock_path);
    }
}

fn acquire_registry_lock(path: &Path) -> Result<RegistryLock, std::io::Error> {
    let lock_path = registry_lock_path(path);
    let stale_after = Duration::from_secs(REGISTRY_LOCK_STALE_AFTER_SECS);
    for attempt in 0..=REGISTRY_LOCK_MAX_ATTEMPTS {
        match fs::OpenOptions::new().create_new(true).write(true).open(&lock_path) {
            Ok(_) => return Ok(RegistryLock { lock_path }),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if reclaim_stale_registry_lock(lock_path.as_path(), stale_after) {
                    continue;
                }
                if attempt == REGISTRY_LOCK_MAX_ATTEMPTS {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        format!(
                            "timed out waiting for agent registry lock at {}",
                            lock_path.display()
                        ),
                    ));
                }
                std::thread::sleep(Duration::from_millis(REGISTRY_LOCK_RETRY_DELAY_MS));
            }
            Err(error) => return Err(error),
        }
    }
    Err(std::io::Error::other("agent registry lock acquisition exhausted retry budget"))
}

fn registry_lock_path(path: &Path) -> PathBuf {
    let mut lock_name = path.as_os_str().to_os_string();
    lock_name.push(".lock");
    PathBuf::from(lock_name)
}

fn reclaim_stale_registry_lock(lock_path: &Path, stale_after: Duration) -> bool {
    let metadata = match fs::metadata(lock_path) {
        Ok(value) => value,
        Err(_) => return false,
    };
    let modified = match metadata.modified() {
        Ok(value) => value,
        Err(_) => return false,
    };
    let is_stale = SystemTime::now().duration_since(modified).unwrap_or_default() >= stale_after;
    if !is_stale {
        return false;
    }
    fs::remove_file(lock_path).is_ok()
}

fn write_registry_atomically(path: &Path, payload: &str) -> Result<(), std::io::Error> {
    let timestamp_ns = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
    let mut temporary_name = path.as_os_str().to_os_string();
    temporary_name.push(format!(".tmp.{}.{}", std::process::id(), timestamp_ns));
    let temporary_path = PathBuf::from(temporary_name);

    fs::write(&temporary_path, payload)?;
    harden_registry_file_permissions(temporary_path.as_path())?;
    if let Err(rename_error) = fs::rename(&temporary_path, path) {
        if !path.exists() || !path.is_file() {
            let _ = fs::remove_file(&temporary_path);
            return Err(rename_error);
        }
        let mut rollback_name = path.as_os_str().to_os_string();
        rollback_name.push(format!(".swap.{}.{}", std::process::id(), timestamp_ns));
        let rollback_path = PathBuf::from(rollback_name);
        fs::rename(path, &rollback_path)?;
        if let Err(install_error) = fs::rename(&temporary_path, path) {
            let _ = fs::rename(&rollback_path, path);
            let _ = fs::remove_file(&temporary_path);
            return Err(install_error);
        }
        let _ = fs::remove_file(&rollback_path);
    }
    harden_registry_file_permissions(path)?;
    Ok(())
}

fn harden_registry_dir_permissions(path: &Path) -> Result<(), std::io::Error> {
    ensure_owner_only_dir(path).map_err(|error| {
        std::io::Error::other(format!(
            "failed to enforce owner-only directory permissions on {}: {error}",
            path.display()
        ))
    })
}

fn harden_registry_file_permissions(path: &Path) -> Result<(), std::io::Error> {
    ensure_owner_only_file(path).map_err(|error| {
        std::io::Error::other(format!(
            "failed to enforce owner-only file permissions on {}: {error}",
            path.display()
        ))
    })
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
        let parsed_absolute = parsed.is_absolute();
        let candidate = if parsed_absolute {
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
        if !parsed_absolute && !canonical.starts_with(agent_dir) {
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
    harden_registry_dir_permissions(path)
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
    use std::fs;
    use std::path::{Path, PathBuf};

    use tempfile::tempdir;

    use super::{
        AgentCreateRequest, AgentDefaultEnsureOutcome, AgentRegistry, AgentRegistryError,
        AgentResolutionSource, AgentResolveRequest,
    };

    fn agent_registry_lock_path(identity_root: &Path) -> PathBuf {
        let state_root = identity_root.parent().unwrap_or(identity_root);
        state_root.join("agents.toml.lock")
    }

    #[cfg(unix)]
    #[test]
    fn open_hardens_registry_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir().expect("tempdir should be created");
        let identity_root = temp.path().join("state").join("identity");
        let _registry =
            AgentRegistry::open(identity_root.as_path()).expect("registry should initialize");

        let state_root = temp.path().join("state");
        let registry_path = state_root.join("agents.toml");
        let state_mode = fs::metadata(&state_root)
            .expect("state root metadata should be readable")
            .permissions()
            .mode()
            & 0o777;
        let registry_mode = fs::metadata(&registry_path)
            .expect("registry metadata should be readable")
            .permissions()
            .mode()
            & 0o777;

        assert_eq!(state_mode, 0o700, "state root must be owner-only");
        assert_eq!(registry_mode, 0o600, "agent registry file must be owner-only");
    }

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
                execution_backend_preference: None,
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
            execution_backend_preference: None,
            default_tool_allowlist: Vec::new(),
            default_skill_allowlist: Vec::new(),
            set_default: false,
            allow_absolute_paths: false,
        });
        assert!(matches!(duplicate, Err(AgentRegistryError::AgentDirCollision(_))));
    }

    #[test]
    fn create_agent_allows_explicit_absolute_workspace_root_outside_agent_dir() {
        let temp = tempdir().expect("tempdir should be created");
        let identity_root = temp.path().join("state").join("identity");
        let registry =
            AgentRegistry::open(identity_root.as_path()).expect("registry should initialize");
        let workspace = temp.path().join("checkout");
        fs::create_dir_all(workspace.as_path()).expect("workspace should be created");

        let outcome = registry
            .create_agent(AgentCreateRequest {
                agent_id: "main".to_owned(),
                display_name: "Main".to_owned(),
                agent_dir: None,
                workspace_roots: vec![workspace.to_string_lossy().into_owned()],
                default_model_profile: None,
                execution_backend_preference: None,
                default_tool_allowlist: Vec::new(),
                default_skill_allowlist: Vec::new(),
                set_default: true,
                allow_absolute_paths: true,
            })
            .expect("explicit absolute workspace should be accepted");
        let canonical_workspace =
            fs::canonicalize(workspace.as_path()).expect("workspace should canonicalize");
        let canonical_workspace = canonical_workspace.to_string_lossy().into_owned();

        assert_eq!(outcome.agent.workspace_roots, vec![canonical_workspace.clone()]);

        drop(registry);
        let reopened =
            AgentRegistry::open(identity_root.as_path()).expect("registry should reopen");
        let page = reopened.list_agents(None, Some(10)).expect("list should succeed");
        assert_eq!(page.agents[0].workspace_roots, vec![canonical_workspace]);
    }

    #[test]
    fn create_agent_rejects_absolute_workspace_root_without_flag() {
        let temp = tempdir().expect("tempdir should be created");
        let registry = AgentRegistry::open(temp.path().join("identity").as_path())
            .expect("registry should initialize");
        let workspace = temp.path().join("checkout");

        let result = registry.create_agent(AgentCreateRequest {
            agent_id: "main".to_owned(),
            display_name: "Main".to_owned(),
            agent_dir: None,
            workspace_roots: vec![workspace.to_string_lossy().into_owned()],
            default_model_profile: None,
            execution_backend_preference: None,
            default_tool_allowlist: Vec::new(),
            default_skill_allowlist: Vec::new(),
            set_default: true,
            allow_absolute_paths: false,
        });

        assert!(matches!(
            result,
            Err(AgentRegistryError::InvalidPath { field: "workspace_root", .. })
        ));
    }

    #[test]
    fn ensure_local_default_agent_bootstraps_empty_registry() {
        let temp = tempdir().expect("tempdir should be created");
        let identity_root = temp.path().join("state").join("identity");
        let workspace = temp.path().join("workspace");
        let registry =
            AgentRegistry::open(identity_root.as_path()).expect("registry should initialize");

        let outcome = registry
            .ensure_local_default_agent(workspace.as_path(), Some("MiniMax-M2.7".to_owned()))
            .expect("default local agent should be created");

        assert_eq!(
            outcome,
            AgentDefaultEnsureOutcome::Created { agent_id: "local-default".to_owned() }
        );
        let page = registry.list_agents(None, Some(10)).expect("agents should list");
        assert_eq!(page.default_agent_id.as_deref(), Some("local-default"));
        assert_eq!(page.agents.len(), 1);
        assert_eq!(page.agents[0].display_name, "LocalDefaultAgent");
        assert_eq!(page.agents[0].default_model_profile, "MiniMax-M2.7");
        let canonical_workspace =
            fs::canonicalize(workspace.as_path()).expect("workspace should canonicalize");
        assert_eq!(
            page.agents[0].workspace_roots,
            vec![canonical_workspace.to_string_lossy().into_owned()]
        );
    }

    #[test]
    fn ensure_local_default_agent_updates_stale_local_default_workspace_root() {
        let temp = tempdir().expect("tempdir should be created");
        let identity_root = temp.path().join("state").join("identity");
        let old_workspace = temp.path().join("state").join("workspace");
        let new_workspace = temp.path().join("workspace");
        fs::create_dir_all(old_workspace.as_path()).expect("old workspace should exist");
        fs::create_dir_all(new_workspace.as_path()).expect("new workspace should exist");
        let registry =
            AgentRegistry::open(identity_root.as_path()).expect("registry should initialize");

        registry
            .create_agent(AgentCreateRequest {
                agent_id: "local-default".to_owned(),
                display_name: "LocalDefaultAgent".to_owned(),
                agent_dir: None,
                workspace_roots: vec![old_workspace.to_string_lossy().into_owned()],
                default_model_profile: None,
                execution_backend_preference: None,
                default_tool_allowlist: Vec::new(),
                default_skill_allowlist: Vec::new(),
                set_default: true,
                allow_absolute_paths: true,
            })
            .expect("stale local default agent should be created");

        let outcome = registry
            .ensure_local_default_agent(new_workspace.as_path(), Some("MiniMax-M2.7".to_owned()))
            .expect("local default agent should synchronize");

        assert_eq!(
            outcome,
            AgentDefaultEnsureOutcome::Updated { agent_id: "local-default".to_owned() }
        );
        let page = registry.list_agents(None, Some(10)).expect("agents should list");
        let canonical_workspace =
            fs::canonicalize(new_workspace.as_path()).expect("new workspace should canonicalize");
        assert_eq!(
            page.agents[0].workspace_roots,
            vec![canonical_workspace.to_string_lossy().into_owned()]
        );
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
                execution_backend_preference: None,
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
    fn create_agent_keeps_in_memory_state_when_registry_write_fails() {
        let temp = tempdir().expect("tempdir should be created");
        let identity_root = temp.path().join("identity");
        let registry =
            AgentRegistry::open(identity_root.as_path()).expect("registry should initialize");
        registry
            .create_agent(AgentCreateRequest {
                agent_id: "main".to_owned(),
                display_name: "Main".to_owned(),
                agent_dir: Some("agents/main".to_owned()),
                workspace_roots: vec!["workspace".to_owned()],
                default_model_profile: Some("gpt-4o-mini".to_owned()),
                execution_backend_preference: None,
                default_tool_allowlist: Vec::new(),
                default_skill_allowlist: Vec::new(),
                set_default: true,
                allow_absolute_paths: false,
            })
            .expect("first create should succeed");

        let lock_path = agent_registry_lock_path(identity_root.as_path());
        fs::write(&lock_path, "busy").expect("lock file should be created");
        let error = registry
            .create_agent(AgentCreateRequest {
                agent_id: "review".to_owned(),
                display_name: "Review".to_owned(),
                agent_dir: Some("agents/review".to_owned()),
                workspace_roots: vec!["workspace".to_owned()],
                default_model_profile: Some("gpt-4o-mini".to_owned()),
                execution_backend_preference: None,
                default_tool_allowlist: Vec::new(),
                default_skill_allowlist: Vec::new(),
                set_default: false,
                allow_absolute_paths: false,
            })
            .expect_err("persist lock should force write failure");
        assert!(
            matches!(error, AgentRegistryError::WriteRegistry { .. }),
            "create_agent should fail with write-registry error when lock is held"
        );
        fs::remove_file(&lock_path).expect("lock file should be removed");

        let page = registry.list_agents(None, Some(10)).expect("list should succeed");
        assert_eq!(
            page.agents.len(),
            1,
            "in-memory registry must not include agent when persistence failed"
        );
        assert_eq!(page.agents[0].agent_id, "main");
        assert_eq!(page.default_agent_id.as_deref(), Some("main"));
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
                execution_backend_preference: None,
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
                execution_backend_preference: None,
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
            execution_backend_preference: None,
            default_tool_allowlist: Vec::new(),
            default_skill_allowlist: Vec::new(),
            set_default: false,
            allow_absolute_paths: true,
        });
        assert!(matches!(result, Err(AgentRegistryError::WorkspaceRootEscape(_))));
    }
}
