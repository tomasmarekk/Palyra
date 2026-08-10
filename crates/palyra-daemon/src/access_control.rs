//! File-backed access-control registry for the daemon's external API surface.
//!
//! [`AccessRegistry`] persists staged-rollout feature flags, scoped API
//! tokens, teams/workspaces/memberships, invitations, resource shares, and a
//! bounded feature-telemetry tape in one JSON document under the daemon state
//! root. Gating is deny-by-default: unknown feature flags evaluate as
//! disabled and every workspace permission requires an explicit membership
//! role.
//!
//! Token and invitation secrets are returned to the caller exactly once at
//! mint time; only SHA-256 digests and short display prefixes are persisted.
//! Consumed by the console/compat HTTP surfaces and the operator CLI
//! `auth access` commands.

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use rand::Rng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use ulid::Ulid;

const ACCESS_REGISTRY_FILE_NAME: &str = "access_registry.json";
const ACCESS_REGISTRY_VERSION: u32 = 1;
/// Retention cap for the persisted telemetry tape; oldest events drop first.
const TELEMETRY_EVENT_LIMIT: usize = 256;
const DEFAULT_TOKEN_RATE_LIMIT_PER_MINUTE: u32 = 120;

/// Feature flag gating the OpenAI-compatible HTTP facade.
pub(crate) const FEATURE_COMPAT_API: &str = "compat_api";
/// Feature flag gating `POST /v1/embeddings` on the compat facade.
pub(crate) const FEATURE_COMPAT_EMBEDDINGS_API: &str = "compat_embeddings_api";
/// Feature flag gating the conservative `/v1/tools/invoke` compat skeleton.
pub(crate) const FEATURE_COMPAT_TOOLS_INVOKE: &str = "compat_tools_invoke";
/// Feature flag gating issuance and use of scoped external API tokens.
pub(crate) const FEATURE_API_TOKENS: &str = "api_tokens";
/// Feature flag gating shared teams, workspaces, and invitations.
pub(crate) const FEATURE_TEAM_MODE: &str = "team_mode";
/// Feature flag gating role-based access checks and explicit resource shares.
pub(crate) const FEATURE_RBAC: &str = "rbac";
/// Feature flag gating staged rollout controls and feature telemetry.
pub(crate) const FEATURE_STAGED_ROLLOUT: &str = "staged_rollout";
/// Feature flag gating unattended routine scheduler automation.
pub(crate) const FEATURE_ROUTINES_AUTOMATION: &str = "routines_automation";

// Permission identifiers. Roles expand to permission sets via
// `WorkspaceRole::permissions`; API-token scopes and workspace authorization
// checks are matched against these exact strings.
pub(crate) const PERMISSION_COMPAT_MODELS_READ: &str = "compat.models.read";
pub(crate) const PERMISSION_COMPAT_CHAT_CREATE: &str = "compat.chat.create";
pub(crate) const PERMISSION_COMPAT_EMBEDDINGS_CREATE: &str = "compat.embeddings.create";
pub(crate) const PERMISSION_COMPAT_RESPONSES_CREATE: &str = "compat.responses.create";
pub(crate) const PERMISSION_COMPAT_RUNS_CONTROL: &str = "compat.runs.control";
pub(crate) const PERMISSION_COMPAT_RUNS_APPROVE: &str = "compat.runs.approve";
pub(crate) const PERMISSION_COMPAT_TOOLS_INVOKE: &str = "compat.tools.invoke";
pub(crate) const PERMISSION_API_TOKENS_MANAGE: &str = "api_tokens.manage";
pub(crate) const PERMISSION_WORKSPACE_MANAGE: &str = "workspace.manage";
pub(crate) const PERMISSION_MEMBERSHIP_MANAGE: &str = "workspace.memberships.manage";
pub(crate) const PERMISSION_SHARING_MANAGE: &str = "workspace.sharing.manage";
pub(crate) const PERMISSION_OBSERVABILITY_READ: &str = "observability.read";
pub(crate) const PERMISSION_TRUST_OPERATE: &str = "trust.operate";
pub(crate) const PERMISSION_SESSION_USE: &str = "sessions.use";
pub(crate) const PERMISSION_MEMORY_USE: &str = "memory.use";
pub(crate) const PERMISSION_ROUTINES_USE: &str = "routines.use";
pub(crate) const PERMISSION_ROLLOUT_MANAGE: &str = "rollout.manage";

/// Failure modes of [`AccessRegistry`] persistence, validation, and gating.
///
/// `AccessDenied`, `FeatureDisabled`, `InvalidApiToken`, and `MissingScope`
/// are authorization outcomes; the remaining variants signal registry IO or
/// input validation problems.
#[derive(Debug, thiserror::Error)]
pub(crate) enum AccessRegistryError {
    #[error("failed to read access registry {path}: {error}")]
    ReadRegistry { path: String, error: String },
    #[error("failed to write access registry {path}: {error}")]
    WriteRegistry { path: String, error: String },
    #[error("failed to parse access registry {path}: {error}")]
    ParseRegistry { path: String, error: String },
    #[error("failed to serialize access registry: {0}")]
    SerializeRegistry(String),
    #[error("{field} is invalid: {message}")]
    InvalidField { field: &'static str, message: String },
    #[error("feature flag not found: {0}")]
    FeatureFlagNotFound(String),
    #[error("API token not found: {0}")]
    ApiTokenNotFound(String),
    #[error("team not found: {0}")]
    TeamNotFound(String),
    #[error("workspace not found: {0}")]
    WorkspaceNotFound(String),
    #[error("invitation not found")]
    InvitationNotFound,
    #[error("invitation has expired")]
    InvitationExpired,
    #[error("invitation has already been accepted")]
    InvitationAlreadyAccepted,
    #[error("access denied: {0}")]
    AccessDenied(String),
    #[error("feature flag '{0}' is disabled")]
    FeatureDisabled(String),
    #[error("API token is expired or revoked")]
    InvalidApiToken,
    #[error("API token is missing required scope '{0}'")]
    MissingScope(String),
}

/// Membership role inside a workspace, ordered from widest to narrowest
/// authority: `Owner` > `Admin` > `Operator`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkspaceRole {
    Owner,
    Admin,
    Operator,
}

impl WorkspaceRole {
    /// Parses a role name case-insensitively.
    ///
    /// # Errors
    /// Returns [`AccessRegistryError::InvalidField`] for unsupported names.
    pub(crate) fn parse(value: &str) -> Result<Self, AccessRegistryError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "owner" => Ok(Self::Owner),
            "admin" => Ok(Self::Admin),
            "operator" => Ok(Self::Operator),
            other => Err(AccessRegistryError::InvalidField {
                field: "role",
                message: format!("unsupported workspace role '{other}'"),
            }),
        }
    }

    /// Returns the canonical lowercase role name.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Owner => "owner",
            Self::Admin => "admin",
            Self::Operator => "operator",
        }
    }

    /// Expands the role into its full permission set.
    ///
    /// Every role carries the baseline compat/session permissions; `Admin`
    /// adds token, membership, and sharing management, and `Owner` adds
    /// workspace, trust, and rollout management on top.
    pub(crate) fn permissions(self) -> Vec<String> {
        let mut permissions = vec![
            PERMISSION_COMPAT_MODELS_READ.to_owned(),
            PERMISSION_COMPAT_CHAT_CREATE.to_owned(),
            PERMISSION_COMPAT_EMBEDDINGS_CREATE.to_owned(),
            PERMISSION_COMPAT_RESPONSES_CREATE.to_owned(),
            PERMISSION_COMPAT_RUNS_CONTROL.to_owned(),
            PERMISSION_COMPAT_RUNS_APPROVE.to_owned(),
            PERMISSION_COMPAT_TOOLS_INVOKE.to_owned(),
            PERMISSION_SESSION_USE.to_owned(),
            PERMISSION_MEMORY_USE.to_owned(),
            PERMISSION_ROUTINES_USE.to_owned(),
            PERMISSION_OBSERVABILITY_READ.to_owned(),
        ];
        match self {
            Self::Owner => {
                permissions.extend([
                    PERMISSION_API_TOKENS_MANAGE.to_owned(),
                    PERMISSION_WORKSPACE_MANAGE.to_owned(),
                    PERMISSION_MEMBERSHIP_MANAGE.to_owned(),
                    PERMISSION_SHARING_MANAGE.to_owned(),
                    PERMISSION_TRUST_OPERATE.to_owned(),
                    PERMISSION_ROLLOUT_MANAGE.to_owned(),
                ]);
            }
            Self::Admin => {
                permissions.extend([
                    PERMISSION_API_TOKENS_MANAGE.to_owned(),
                    PERMISSION_MEMBERSHIP_MANAGE.to_owned(),
                    PERMISSION_SHARING_MANAGE.to_owned(),
                ]);
            }
            Self::Operator => {}
        }
        permissions
    }
}

/// Persisted state of one staged-rollout feature flag.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FeatureFlagRecord {
    pub(crate) key: String,
    pub(crate) label: String,
    pub(crate) description: String,
    pub(crate) enabled: bool,
    pub(crate) stage: String,
    #[serde(default)]
    pub(crate) depends_on: Vec<String>,
    pub(crate) updated_at_unix_ms: i64,
    pub(crate) updated_by_principal: String,
}

/// Persisted API token metadata; stores only the SHA-256 digest and a short
/// display prefix of the secret, never the secret itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ApiTokenRecord {
    pub(crate) token_id: String,
    pub(crate) label: String,
    pub(crate) token_prefix: String,
    pub(crate) token_hash_sha256: String,
    #[serde(default)]
    pub(crate) scopes: Vec<String>,
    pub(crate) principal: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) workspace_id: Option<String>,
    pub(crate) role: String,
    pub(crate) rate_limit_per_minute: u32,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) updated_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) expires_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) revoked_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) last_used_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) rotated_from_token_id: Option<String>,
}

/// Persisted team record; teams group workspaces for display purposes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TeamRecord {
    pub(crate) team_id: String,
    pub(crate) slug: String,
    pub(crate) display_name: String,
    pub(crate) created_by_principal: String,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) updated_at_unix_ms: i64,
}

/// Persisted workspace record, including the derived runtime principal and
/// device identity that workspace-scoped traffic executes under.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WorkspaceRecord {
    pub(crate) workspace_id: String,
    pub(crate) team_id: String,
    pub(crate) slug: String,
    pub(crate) display_name: String,
    pub(crate) runtime_principal: String,
    pub(crate) runtime_device_id: String,
    pub(crate) created_by_principal: String,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) updated_at_unix_ms: i64,
}

/// Persisted binding of a principal to a workspace with a single role.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MembershipRecord {
    pub(crate) membership_id: String,
    pub(crate) workspace_id: String,
    pub(crate) principal: String,
    pub(crate) role: WorkspaceRole,
    pub(crate) created_by_principal: String,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) updated_at_unix_ms: i64,
}

/// Persisted workspace invitation; the acceptance secret is stored only as a
/// SHA-256 digest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct InvitationRecord {
    pub(crate) invitation_id: String,
    pub(crate) workspace_id: String,
    pub(crate) invited_identity: String,
    pub(crate) role: WorkspaceRole,
    pub(crate) token_hash_sha256: String,
    pub(crate) issued_by_principal: String,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) expires_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) accepted_by_principal: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) accepted_at_unix_ms: Option<i64>,
}

/// Persisted grant exposing one resource (by kind and id) to a workspace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ResourceShareRecord {
    pub(crate) share_id: String,
    pub(crate) resource_kind: String,
    pub(crate) resource_id: String,
    pub(crate) workspace_id: String,
    pub(crate) access_level: String,
    pub(crate) created_by_principal: String,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) updated_at_unix_ms: i64,
}

/// One privacy-aware feature telemetry event on the bounded registry tape.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TelemetryEventRecord {
    pub(crate) event_id: String,
    pub(crate) feature_key: String,
    pub(crate) category: String,
    pub(crate) outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) principal: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) workspace_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) token_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) detail: Option<String>,
    pub(crate) recorded_at_unix_ms: i64,
}

/// On-disk shape of the access registry document.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct AccessRegistryFile {
    version: u32,
    #[serde(default)]
    last_backfill_at_unix_ms: Option<i64>,
    #[serde(default)]
    feature_flags: Vec<FeatureFlagRecord>,
    #[serde(default)]
    api_tokens: Vec<ApiTokenRecord>,
    #[serde(default)]
    teams: Vec<TeamRecord>,
    #[serde(default)]
    workspaces: Vec<WorkspaceRecord>,
    #[serde(default)]
    memberships: Vec<MembershipRecord>,
    #[serde(default)]
    invitations: Vec<InvitationRecord>,
    #[serde(default)]
    shares: Vec<ResourceShareRecord>,
    #[serde(default)]
    telemetry: Vec<TelemetryEventRecord>,
}

impl Default for AccessRegistryFile {
    fn default() -> Self {
        Self {
            version: ACCESS_REGISTRY_VERSION,
            last_backfill_at_unix_ms: None,
            feature_flags: default_feature_flags(),
            api_tokens: Vec::new(),
            teams: Vec::new(),
            workspaces: Vec::new(),
            memberships: Vec::new(),
            invitations: Vec::new(),
            shares: Vec::new(),
            telemetry: Vec::new(),
        }
    }
}

/// Membership joined with its workspace/team metadata and expanded
/// role permissions, for console and CLI display.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct WorkspaceMembershipView {
    pub(crate) membership_id: String,
    pub(crate) workspace_id: String,
    pub(crate) workspace_name: String,
    pub(crate) workspace_slug: String,
    pub(crate) team_id: String,
    pub(crate) team_name: String,
    pub(crate) principal: String,
    pub(crate) role: String,
    pub(crate) permissions: Vec<String>,
    pub(crate) runtime_principal: String,
    pub(crate) runtime_device_id: String,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) updated_at_unix_ms: i64,
}

/// Secret-free projection of an [`ApiTokenRecord`] with a derived
/// `state` of `active`, `expired`, or `revoked`.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct ApiTokenView {
    pub(crate) token_id: String,
    pub(crate) label: String,
    pub(crate) token_prefix: String,
    pub(crate) scopes: Vec<String>,
    pub(crate) principal: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) workspace_id: Option<String>,
    pub(crate) role: String,
    pub(crate) rate_limit_per_minute: u32,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) updated_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) expires_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) revoked_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) last_used_at_unix_ms: Option<i64>,
    pub(crate) state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) rotated_from_token_id: Option<String>,
}

/// One-time response envelope carrying the freshly minted token secret;
/// the secret is never persisted and cannot be retrieved again.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct ApiTokenSecretEnvelope {
    pub(crate) token: String,
    pub(crate) token_record: ApiTokenView,
}

/// Result of creating a workspace: the new team, workspace, and the
/// creator's owner membership.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct WorkspaceCreateEnvelope {
    pub(crate) team: TeamRecord,
    pub(crate) workspace: WorkspaceRecord,
    pub(crate) membership: WorkspaceMembershipView,
}

/// One-time response envelope carrying the invitation acceptance secret;
/// only its SHA-256 digest is persisted.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct InvitationSecretEnvelope {
    pub(crate) invitation_token: String,
    pub(crate) invitation: InvitationRecord,
}

/// Per-feature aggregation of the telemetry tape for operator dashboards.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct FeatureTelemetrySummary {
    pub(crate) feature_key: String,
    pub(crate) total_events: usize,
    pub(crate) success_events: usize,
    pub(crate) error_events: usize,
    pub(crate) latest_at_unix_ms: Option<i64>,
}

/// One migration health check with `state` of `ready`, `warning`,
/// `backfill_required`, or `blocked`, plus a remediation hint.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct AccessMigrationCheck {
    pub(crate) key: String,
    pub(crate) state: String,
    pub(crate) detail: String,
    pub(crate) remediation: String,
}

/// Aggregated migration posture of the registry, derived from
/// [`AccessMigrationCheck`] states.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct AccessMigrationStatus {
    pub(crate) registry_path: String,
    pub(crate) version: u32,
    pub(crate) backfill_required: bool,
    pub(crate) blocking_issues: usize,
    pub(crate) warning_issues: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) last_backfill_at_unix_ms: Option<i64>,
    pub(crate) checks: Vec<AccessMigrationCheck>,
}

/// Outcome of a registry repair pass; all counters refer to records
/// actually changed (zero on an already-clean registry).
#[derive(Debug, Clone, Serialize)]
pub(crate) struct AccessBackfillReport {
    pub(crate) dry_run: bool,
    pub(crate) changed_records: usize,
    pub(crate) feature_flags_added: usize,
    pub(crate) teams_repaired: usize,
    pub(crate) workspaces_repaired: usize,
    pub(crate) api_tokens_repaired: usize,
    pub(crate) memberships_repaired: usize,
    pub(crate) telemetry_trimmed: usize,
    pub(crate) notes: Vec<String>,
}

/// Rollout posture of a single feature flag, including unmet dependencies
/// and the CLI kill-switch command operators can run.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct AccessRolloutPackageStatus {
    pub(crate) feature_key: String,
    pub(crate) label: String,
    pub(crate) enabled: bool,
    pub(crate) stage: String,
    pub(crate) depends_on: Vec<String>,
    pub(crate) dependency_blockers: Vec<String>,
    pub(crate) safe_mode_when_disabled: bool,
    pub(crate) kill_switch_command: String,
}

/// Overall rollout posture; the `*_safe_mode` flags report whether the
/// external API and team mode are still gated off.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct AccessRolloutStatus {
    pub(crate) staged_rollout_enabled: bool,
    pub(crate) external_api_safe_mode: bool,
    pub(crate) team_mode_safe_mode: bool,
    pub(crate) telemetry_events_retained: usize,
    pub(crate) packages: Vec<AccessRolloutPackageStatus>,
    pub(crate) operator_notes: Vec<String>,
}

/// Principal-scoped view of the whole registry: only workspaces, teams,
/// tokens, invitations, and shares visible to the requesting principal.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct AccessRegistrySnapshot {
    pub(crate) version: u32,
    pub(crate) feature_flags: Vec<FeatureFlagRecord>,
    pub(crate) api_tokens: Vec<ApiTokenView>,
    pub(crate) teams: Vec<TeamRecord>,
    pub(crate) workspaces: Vec<WorkspaceRecord>,
    pub(crate) memberships: Vec<WorkspaceMembershipView>,
    pub(crate) invitations: Vec<InvitationRecord>,
    pub(crate) shares: Vec<ResourceShareRecord>,
    pub(crate) telemetry: Vec<FeatureTelemetrySummary>,
    pub(crate) migration: AccessMigrationStatus,
    pub(crate) rollout: AccessRolloutStatus,
}

/// Identity material of a successfully authenticated API token, handed to
/// request handlers for rate limiting and workspace resolution.
#[derive(Debug, Clone)]
pub(crate) struct AuthenticatedApiToken {
    pub(crate) token_id: String,
    pub(crate) label: String,
    pub(crate) principal: String,
    pub(crate) workspace_id: Option<String>,
    pub(crate) rate_limit_per_minute: u32,
}

/// Successful workspace authorization: the runtime identity traffic should
/// execute under, plus the granting role and an auditable reason string.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct WorkspaceAccessResolution {
    pub(crate) workspace_id: String,
    pub(crate) runtime_principal: String,
    pub(crate) runtime_device_id: String,
    pub(crate) actor_principal: String,
    pub(crate) role: String,
    pub(crate) permissions: Vec<String>,
    pub(crate) reason: String,
}

/// In-memory access registry bound to its JSON backing file.
///
/// Mutating methods authorize the acting principal first, then persist the
/// full document back to disk before returning, so the file always reflects
/// the last successful mutation.
#[derive(Debug, Clone)]
pub(crate) struct AccessRegistry {
    path: PathBuf,
    data: AccessRegistryFile,
}

/// Parameters for [`AccessRegistry::create_api_token`].
#[derive(Debug, Clone)]
pub(crate) struct ApiTokenCreateRequest {
    pub(crate) label: String,
    pub(crate) scopes: Vec<String>,
    pub(crate) principal: String,
    pub(crate) workspace_id: Option<String>,
    pub(crate) role: String,
    pub(crate) expires_at_unix_ms: Option<i64>,
    pub(crate) rate_limit_per_minute: Option<u32>,
}

/// Parameters for [`AccessRegistry::create_workspace_bundle`].
#[derive(Debug, Clone)]
pub(crate) struct WorkspaceCreateRequest {
    pub(crate) team_name: String,
    pub(crate) workspace_name: String,
}

/// Parameters for [`AccessRegistry::create_invitation`].
#[derive(Debug, Clone)]
pub(crate) struct InvitationCreateRequest {
    pub(crate) workspace_id: String,
    pub(crate) invited_identity: String,
    pub(crate) role: String,
    pub(crate) expires_at_unix_ms: i64,
}

/// Parameters for [`AccessRegistry::upsert_resource_share`].
#[derive(Debug, Clone)]
pub(crate) struct ResourceShareUpsertRequest {
    pub(crate) resource_kind: String,
    pub(crate) resource_id: String,
    pub(crate) workspace_id: String,
    pub(crate) access_level: String,
}

impl AccessRegistry {
    /// Opens (or initializes) the registry under `state_root`, applying
    /// non-destructive schema repairs and persisting the result.
    ///
    /// # Errors
    /// Returns a read/parse/write variant on IO or JSON failure, and
    /// [`AccessRegistryError::InvalidField`] for an unsupported on-disk
    /// schema version.
    pub(crate) fn open(state_root: &Path) -> Result<Self, AccessRegistryError> {
        fs::create_dir_all(state_root).map_err(|error| AccessRegistryError::WriteRegistry {
            path: state_root.display().to_string(),
            error: error.to_string(),
        })?;
        let path = state_root.join(ACCESS_REGISTRY_FILE_NAME);
        if !path.exists() {
            let registry = Self { path, data: AccessRegistryFile::default() };
            registry.persist()?;
            return Ok(registry);
        }

        let raw = fs::read_to_string(&path).map_err(|error| AccessRegistryError::ReadRegistry {
            path: path.display().to_string(),
            error: error.to_string(),
        })?;
        let parsed = serde_json::from_str::<AccessRegistryFile>(raw.as_str()).map_err(|error| {
            AccessRegistryError::ParseRegistry {
                path: path.display().to_string(),
                error: error.to_string(),
            }
        })?;
        let mut registry = Self { path, data: parsed };
        registry.apply_migrations()?;
        registry.persist()?;
        Ok(registry)
    }

    /// Builds the registry view visible to `principal`: their memberships,
    /// the workspaces/teams/shares those memberships reach, tokens they own
    /// or can manage, and invitations addressed to or manageable by them.
    pub(crate) fn snapshot(&self, principal: &str) -> AccessRegistrySnapshot {
        let memberships = self.list_visible_workspace_memberships(principal);
        let visible_workspace_ids = memberships
            .iter()
            .map(|membership| membership.workspace_id.clone())
            .collect::<BTreeSet<_>>();
        let workspaces = self
            .data
            .workspaces
            .iter()
            .filter(|workspace| visible_workspace_ids.contains(workspace.workspace_id.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        let visible_team_ids =
            workspaces.iter().map(|workspace| workspace.team_id.clone()).collect::<BTreeSet<_>>();
        AccessRegistrySnapshot {
            version: self.data.version,
            feature_flags: self.data.feature_flags.clone(),
            api_tokens: self.list_api_tokens(principal),
            teams: self
                .data
                .teams
                .iter()
                .filter(|team| visible_team_ids.contains(team.team_id.as_str()))
                .cloned()
                .collect(),
            workspaces,
            memberships,
            invitations: self
                .data
                .invitations
                .iter()
                .filter(|invitation| {
                    invitation.invited_identity.eq_ignore_ascii_case(principal)
                        || self
                            .workspace_role_for_principal(
                                principal,
                                invitation.workspace_id.as_str(),
                            )
                            .is_ok()
                })
                .cloned()
                .collect(),
            shares: self
                .data
                .shares
                .iter()
                .filter(|share| visible_workspace_ids.contains(share.workspace_id.as_str()))
                .cloned()
                .collect(),
            telemetry: self.telemetry_summary(),
            migration: self.migration_status(),
            rollout: self.rollout_status(),
        }
    }

    /// Evaluates the registry against the current schema contract and
    /// reports per-check readiness plus aggregate blocking/warning counts.
    pub(crate) fn migration_status(&self) -> AccessMigrationStatus {
        let missing_feature_flags = missing_feature_flag_keys(&self.data.feature_flags);
        let workspaces_missing_runtime = self
            .data
            .workspaces
            .iter()
            .filter(|workspace| {
                workspace.runtime_principal.trim().is_empty()
                    || workspace.runtime_device_id.trim().is_empty()
            })
            .count();
        let api_tokens_missing_scopes =
            self.data.api_tokens.iter().filter(|token| token.scopes.is_empty()).count();
        let orphaned_memberships = self
            .data
            .memberships
            .iter()
            .filter(|membership| {
                !self
                    .data
                    .workspaces
                    .iter()
                    .any(|workspace| workspace.workspace_id == membership.workspace_id)
            })
            .count();
        let orphaned_invitations = self
            .data
            .invitations
            .iter()
            .filter(|invitation| {
                !self
                    .data
                    .workspaces
                    .iter()
                    .any(|workspace| workspace.workspace_id == invitation.workspace_id)
            })
            .count();
        let unsatisfied_dependencies = self
            .data
            .feature_flags
            .iter()
            .filter(|flag| {
                flag.enabled
                    && flag.depends_on.iter().any(|dependency| !self.is_feature_enabled(dependency))
            })
            .count();

        let checks = vec![
            AccessMigrationCheck {
                key: "feature_flag_inventory".to_owned(),
                state: if missing_feature_flags.is_empty() {
                    "ready".to_owned()
                } else {
                    "backfill_required".to_owned()
                },
                detail: if missing_feature_flags.is_empty() {
                    "All rollout package flags are present in the registry.".to_owned()
                } else {
                    format!("Missing feature flags: {}.", missing_feature_flags.join(", "))
                },
                remediation:
                    "Run `palyra auth access backfill` to restore the default rollout package set."
                        .to_owned(),
            },
            AccessMigrationCheck {
                key: "workspace_runtime_binding".to_owned(),
                state: if workspaces_missing_runtime == 0 {
                    "ready".to_owned()
                } else {
                    "backfill_required".to_owned()
                },
                detail: if workspaces_missing_runtime == 0 {
                    "All workspaces expose runtime principal and runtime device bindings."
                        .to_owned()
                } else {
                    format!(
                        "{workspaces_missing_runtime} workspaces are missing runtime binding metadata."
                    )
                },
                remediation:
                    "Run `palyra auth access backfill` before enabling shared workspace traffic."
                        .to_owned(),
            },
            AccessMigrationCheck {
                key: "api_token_scope_inventory".to_owned(),
                state: if api_tokens_missing_scopes == 0 {
                    "ready".to_owned()
                } else {
                    "backfill_required".to_owned()
                },
                detail: if api_tokens_missing_scopes == 0 {
                    "All API tokens carry explicit scope inventories.".to_owned()
                } else {
                    format!(
                        "{api_tokens_missing_scopes} API tokens are missing scopes and need role-derived defaults."
                    )
                },
                remediation:
                    "Run `palyra auth access backfill` before exposing compat API tokens to operators."
                        .to_owned(),
            },
            AccessMigrationCheck {
                key: "membership_integrity".to_owned(),
                state: if orphaned_memberships == 0 && orphaned_invitations == 0 {
                    "ready".to_owned()
                } else {
                    "blocked".to_owned()
                },
                detail: if orphaned_memberships == 0 && orphaned_invitations == 0 {
                    "Memberships and invitations resolve to existing workspaces.".to_owned()
                } else {
                    format!(
                        "Detected orphaned records: memberships={orphaned_memberships}, invitations={orphaned_invitations}."
                    )
                },
                remediation:
                    "Safe-stop team mode rollout and repair or remove orphaned records before continuing."
                        .to_owned(),
            },
            AccessMigrationCheck {
                key: "flag_dependency_consistency".to_owned(),
                state: if unsatisfied_dependencies == 0 {
                    "ready".to_owned()
                } else {
                    "warning".to_owned()
                },
                detail: if unsatisfied_dependencies == 0 {
                    "Enabled rollout packages respect declared dependencies.".to_owned()
                } else {
                    format!(
                        "{unsatisfied_dependencies} enabled rollout packages have disabled prerequisites."
                    )
                },
                remediation:
                    "Toggle rollout packages in dependency order or disable the dependent package via its kill switch."
                        .to_owned(),
            },
        ];

        AccessMigrationStatus {
            registry_path: self.path.display().to_string(),
            version: self.data.version,
            backfill_required: checks
                .iter()
                .any(|check| check.state == "backfill_required" || check.state == "blocked"),
            blocking_issues: checks.iter().filter(|check| check.state == "blocked").count(),
            warning_issues: checks
                .iter()
                .filter(|check| check.state == "warning" || check.state == "backfill_required")
                .count(),
            last_backfill_at_unix_ms: self.data.last_backfill_at_unix_ms,
            checks,
        }
    }

    /// Runs the registry repair pass; with `dry_run` the staged changes are
    /// discarded and only the would-be report is returned.
    ///
    /// # Errors
    /// Returns [`AccessRegistryError::WriteRegistry`] or
    /// [`AccessRegistryError::SerializeRegistry`] if persisting fails.
    pub(crate) fn run_backfill(
        &mut self,
        actor_principal: &str,
        dry_run: bool,
        now: i64,
    ) -> Result<AccessBackfillReport, AccessRegistryError> {
        let mut staged = self.data.clone();
        let report = repair_access_registry_data(&mut staged);
        if dry_run {
            return Ok(AccessBackfillReport { dry_run: true, ..report });
        }

        self.data = staged;
        self.data.last_backfill_at_unix_ms = Some(now);
        self.record_telemetry(
            FEATURE_STAGED_ROLLOUT,
            "backfill",
            if report.changed_records > 0 { "repaired" } else { "no_change" },
            Some(actor_principal),
            None,
            None,
            Some("access_registry_backfill"),
            now,
        );
        self.persist()?;
        Ok(AccessBackfillReport { dry_run: false, ..report })
    }

    /// Summarizes rollout posture: per-package dependency blockers, safe-mode
    /// flags, and operator guidance notes.
    pub(crate) fn rollout_status(&self) -> AccessRolloutStatus {
        let compat_api_enabled = self.is_feature_enabled(FEATURE_COMPAT_API);
        let api_tokens_enabled = self.is_feature_enabled(FEATURE_API_TOKENS);
        let team_mode_enabled = self.is_feature_enabled(FEATURE_TEAM_MODE);
        let rbac_enabled = self.is_feature_enabled(FEATURE_RBAC);
        let staged_rollout_enabled = self.is_feature_enabled(FEATURE_STAGED_ROLLOUT);

        let mut packages = self
            .data
            .feature_flags
            .iter()
            .map(|flag| AccessRolloutPackageStatus {
                feature_key: flag.key.clone(),
                label: flag.label.clone(),
                enabled: flag.enabled,
                stage: flag.stage.clone(),
                depends_on: flag.depends_on.clone(),
                dependency_blockers: flag
                    .depends_on
                    .iter()
                    .filter(|dependency| !self.is_feature_enabled(dependency))
                    .cloned()
                    .collect(),
                safe_mode_when_disabled: true,
                kill_switch_command: format!("palyra auth access feature {} false", flag.key),
            })
            .collect::<Vec<_>>();
        packages.sort_by(|left, right| left.feature_key.cmp(&right.feature_key));

        let mut operator_notes = Vec::new();
        if !compat_api_enabled || !api_tokens_enabled {
            operator_notes.push(
                "External API remains in safe mode until both `compat_api` and `api_tokens` are enabled."
                    .to_owned(),
            );
        }
        if !team_mode_enabled {
            operator_notes.push(
                "Team/workspace sharing remains isolated until `team_mode` is enabled.".to_owned(),
            );
        } else if !rbac_enabled {
            operator_notes.push(
                "Workspace collaboration is enabled without explicit share metadata because `rbac` is still disabled."
                    .to_owned(),
            );
        }
        if !staged_rollout_enabled {
            operator_notes.push(
                "Staged rollout controls are disabled; widen exposure only after enabling the rollout package."
                    .to_owned(),
            );
        }

        AccessRolloutStatus {
            staged_rollout_enabled,
            external_api_safe_mode: !compat_api_enabled || !api_tokens_enabled,
            team_mode_safe_mode: !team_mode_enabled || !rbac_enabled,
            telemetry_events_retained: self.data.telemetry.len(),
            packages,
            operator_notes,
        }
    }

    /// Reports whether `feature_key` is enabled.
    ///
    /// Deny-by-default: a flag missing from the registry is treated as
    /// disabled rather than an error.
    pub(crate) fn is_feature_enabled(&self, feature_key: &str) -> bool {
        self.data
            .feature_flags
            .iter()
            .find(|flag| flag.key == feature_key)
            .is_some_and(|flag| flag.enabled)
    }

    /// Gate helper returning `Ok(())` only when `feature_key` is enabled.
    ///
    /// # Errors
    /// Returns [`AccessRegistryError::FeatureDisabled`] when the flag is
    /// disabled or unknown.
    pub(crate) fn require_feature_enabled(
        &self,
        feature_key: &str,
    ) -> Result<(), AccessRegistryError> {
        if self.is_feature_enabled(feature_key) {
            Ok(())
        } else {
            Err(AccessRegistryError::FeatureDisabled(feature_key.to_owned()))
        }
    }

    /// Enables or disables `feature_key`, optionally moving its rollout
    /// stage, and persists the change.
    ///
    /// The acting principal is recorded on the flag for audit; authorizing
    /// the actor is the caller's responsibility.
    ///
    /// # Errors
    /// Returns [`AccessRegistryError::FeatureFlagNotFound`] for unknown keys
    /// and a persistence variant if writing the registry fails.
    pub(crate) fn set_feature_flag(
        &mut self,
        feature_key: &str,
        enabled: bool,
        stage: Option<String>,
        actor_principal: &str,
        now: i64,
    ) -> Result<FeatureFlagRecord, AccessRegistryError> {
        let updated_flag = {
            let flag = self
                .data
                .feature_flags
                .iter_mut()
                .find(|flag| flag.key == feature_key)
                .ok_or_else(|| AccessRegistryError::FeatureFlagNotFound(feature_key.to_owned()))?;
            flag.enabled = enabled;
            if let Some(stage) = stage.and_then(trim_to_option) {
                flag.stage = stage;
            }
            flag.updated_at_unix_ms = now;
            flag.updated_by_principal = actor_principal.to_owned();
            flag.clone()
        };
        self.persist()?;
        Ok(updated_flag)
    }

    /// Mints a scoped API token and returns the secret exactly once.
    ///
    /// Workspace-bound tokens require the actor to hold
    /// [`PERMISSION_API_TOKENS_MANAGE`] in that workspace; personal tokens
    /// can only be minted for the actor itself. An empty scope list defaults
    /// to the requested role's full permission set.
    ///
    /// # Errors
    /// Returns [`AccessRegistryError::FeatureDisabled`] when token issuance
    /// is gated off, [`AccessRegistryError::AccessDenied`] on authorization
    /// failure, [`AccessRegistryError::InvalidField`] for malformed fields,
    /// and a persistence variant if writing fails.
    pub(crate) fn create_api_token(
        &mut self,
        actor_principal: &str,
        request: ApiTokenCreateRequest,
        now: i64,
    ) -> Result<ApiTokenSecretEnvelope, AccessRegistryError> {
        self.require_feature_enabled(FEATURE_API_TOKENS)?;
        let label = normalize_required_text(request.label.as_str(), "label")?;
        let principal = normalize_required_text(request.principal.as_str(), "principal")?;
        let role = WorkspaceRole::parse(request.role.as_str())?;
        let workspace_id = request.workspace_id.and_then(trim_to_option);
        if let Some(workspace_id) = workspace_id.as_deref() {
            self.authorize_workspace_permission(
                actor_principal,
                workspace_id,
                PERMISSION_API_TOKENS_MANAGE,
            )?;
        } else if principal != actor_principal {
            return Err(AccessRegistryError::AccessDenied(
                "personal API tokens can only be created for the current principal".to_owned(),
            ));
        }
        let scopes = normalize_scopes(request.scopes, role.permissions());
        let token = mint_access_token_secret();
        let token_id = Ulid::new().to_string();
        let record = ApiTokenRecord {
            token_id: token_id.clone(),
            label,
            token_prefix: token_prefix(token.as_str()),
            token_hash_sha256: sha256_hex(token.as_bytes()),
            scopes,
            principal,
            workspace_id,
            role: role.as_str().to_owned(),
            rate_limit_per_minute: request
                .rate_limit_per_minute
                .unwrap_or(DEFAULT_TOKEN_RATE_LIMIT_PER_MINUTE)
                .clamp(10, 10_000),
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
            expires_at_unix_ms: request.expires_at_unix_ms,
            revoked_at_unix_ms: None,
            last_used_at_unix_ms: None,
            rotated_from_token_id: None,
        };
        self.data.api_tokens.push(record.clone());
        self.record_telemetry(
            FEATURE_API_TOKENS,
            "mutation",
            "created",
            Some(actor_principal),
            record.workspace_id.as_deref(),
            Some(record.token_id.as_str()),
            Some(record.label.as_str()),
            now,
        );
        self.persist()?;
        Ok(ApiTokenSecretEnvelope { token, token_record: api_token_view(&record, now) })
    }

    /// Lists tokens visible to `principal` (their personal tokens plus
    /// tokens of workspaces they belong to), most recently updated first.
    pub(crate) fn list_api_tokens(&self, principal: &str) -> Vec<ApiTokenView> {
        let mut visible = self
            .data
            .api_tokens
            .iter()
            .filter(|record| {
                record.principal == principal
                    || record.workspace_id.as_deref().is_some_and(|workspace_id| {
                        self.workspace_role_for_principal(principal, workspace_id).is_ok()
                    })
            })
            // now = 0 disables expiry-state derivation, so list views report
            // only "active"/"revoked" without needing a clock.
            .map(|record| api_token_view(record, 0))
            .collect::<Vec<_>>();
        visible.sort_by_key(|entry| std::cmp::Reverse(entry.updated_at_unix_ms));
        visible
    }

    /// Revokes `token_id` and mints a replacement with identical label,
    /// scopes, principal, workspace, role, expiry, and rate limit.
    ///
    /// The new secret is returned exactly once and the replacement is linked
    /// back to the old token via `rotated_from_token_id`.
    ///
    /// # Errors
    /// Returns [`AccessRegistryError::FeatureDisabled`] when token issuance
    /// is gated off, [`AccessRegistryError::ApiTokenNotFound`] for unknown
    /// ids, [`AccessRegistryError::AccessDenied`] on authorization failure,
    /// and a persistence variant if writing fails.
    pub(crate) fn rotate_api_token(
        &mut self,
        actor_principal: &str,
        token_id: &str,
        now: i64,
    ) -> Result<ApiTokenSecretEnvelope, AccessRegistryError> {
        self.require_feature_enabled(FEATURE_API_TOKENS)?;
        let existing = self
            .data
            .api_tokens
            .iter()
            .find(|record| record.token_id == token_id)
            .cloned()
            .ok_or_else(|| AccessRegistryError::ApiTokenNotFound(token_id.to_owned()))?;
        if let Some(workspace_id) = existing.workspace_id.as_deref() {
            self.authorize_workspace_permission(
                actor_principal,
                workspace_id,
                PERMISSION_API_TOKENS_MANAGE,
            )?;
        } else if existing.principal != actor_principal {
            return Err(AccessRegistryError::AccessDenied(
                "personal API token rotation requires the owning principal".to_owned(),
            ));
        }
        // Revoke in memory first: create_api_token persists on success, so
        // the revocation and the replacement commit to disk together and the
        // old secret never outlives the new one.
        if let Some(record) =
            self.data.api_tokens.iter_mut().find(|record| record.token_id == token_id)
        {
            record.revoked_at_unix_ms = Some(now);
            record.updated_at_unix_ms = now;
        }
        let request = ApiTokenCreateRequest {
            label: existing.label.clone(),
            scopes: existing.scopes.clone(),
            principal: existing.principal.clone(),
            workspace_id: existing.workspace_id.clone(),
            role: existing.role.clone(),
            expires_at_unix_ms: existing.expires_at_unix_ms,
            rate_limit_per_minute: Some(existing.rate_limit_per_minute),
        };
        let mut rotated = self.create_api_token(actor_principal, request, now)?;
        if let Some(record) = self
            .data
            .api_tokens
            .iter_mut()
            .find(|record| record.token_id == rotated.token_record.token_id)
        {
            record.rotated_from_token_id = Some(token_id.to_owned());
            rotated.token_record.rotated_from_token_id = Some(token_id.to_owned());
        }
        self.record_telemetry(
            FEATURE_API_TOKENS,
            "mutation",
            "rotated",
            Some(actor_principal),
            rotated.token_record.workspace_id.as_deref(),
            Some(rotated.token_record.token_id.as_str()),
            Some(token_id),
            now,
        );
        self.persist()?;
        Ok(rotated)
    }

    /// Permanently revokes `token_id`; revocation cannot be undone, only
    /// replaced by minting a new token.
    ///
    /// # Errors
    /// Returns [`AccessRegistryError::FeatureDisabled`] when the token
    /// feature is gated off, [`AccessRegistryError::ApiTokenNotFound`] for
    /// unknown ids, [`AccessRegistryError::AccessDenied`] on authorization
    /// failure, and a persistence variant if writing fails.
    pub(crate) fn revoke_api_token(
        &mut self,
        actor_principal: &str,
        token_id: &str,
        now: i64,
    ) -> Result<ApiTokenView, AccessRegistryError> {
        self.require_feature_enabled(FEATURE_API_TOKENS)?;
        let existing = self
            .data
            .api_tokens
            .iter()
            .find(|record| record.token_id == token_id)
            .cloned()
            .ok_or_else(|| AccessRegistryError::ApiTokenNotFound(token_id.to_owned()))?;
        if let Some(workspace_id) = existing.workspace_id.as_deref() {
            self.authorize_workspace_permission(
                actor_principal,
                workspace_id,
                PERMISSION_API_TOKENS_MANAGE,
            )?;
        } else if existing.principal != actor_principal {
            return Err(AccessRegistryError::AccessDenied(
                "personal API token revocation requires the owning principal".to_owned(),
            ));
        }
        let revoked_view = {
            let record = self
                .data
                .api_tokens
                .iter_mut()
                .find(|record| record.token_id == token_id)
                .ok_or_else(|| AccessRegistryError::ApiTokenNotFound(token_id.to_owned()))?;
            record.revoked_at_unix_ms = Some(now);
            record.updated_at_unix_ms = now;
            api_token_view(record, now)
        };
        self.record_telemetry(
            FEATURE_API_TOKENS,
            "mutation",
            "revoked",
            Some(actor_principal),
            revoked_view.workspace_id.as_deref(),
            Some(revoked_view.token_id.as_str()),
            Some(revoked_view.label.as_str()),
            now,
        );
        self.persist()?;
        Ok(revoked_view)
    }

    /// Validates a presented token secret and checks that it grants
    /// `required_scope`.
    ///
    /// Lookup is by SHA-256 digest of the secret. Unknown, revoked, and
    /// expired tokens all fail with the same opaque error so callers cannot
    /// probe token state.
    ///
    /// # Errors
    /// Returns [`AccessRegistryError::InvalidApiToken`] for unknown,
    /// revoked, or expired tokens and [`AccessRegistryError::MissingScope`]
    /// when the token authenticates but lacks the required scope.
    pub(crate) fn authenticate_api_token(
        &self,
        raw_token: &str,
        required_scope: &str,
        now: i64,
    ) -> Result<AuthenticatedApiToken, AccessRegistryError> {
        let raw_token =
            trim_to_option(raw_token.to_owned()).ok_or(AccessRegistryError::InvalidApiToken)?;
        let hash = sha256_hex(raw_token.as_bytes());
        let record = self
            .data
            .api_tokens
            .iter()
            .find(|record| record.token_hash_sha256 == hash)
            .ok_or(AccessRegistryError::InvalidApiToken)?;
        if record.revoked_at_unix_ms.is_some() {
            return Err(AccessRegistryError::InvalidApiToken);
        }
        if record.expires_at_unix_ms.is_some_and(|expires_at| expires_at <= now) {
            return Err(AccessRegistryError::InvalidApiToken);
        }
        if !record.scopes.iter().any(|scope| scope == required_scope) {
            return Err(AccessRegistryError::MissingScope(required_scope.to_owned()));
        }
        Ok(AuthenticatedApiToken {
            token_id: record.token_id.clone(),
            label: record.label.clone(),
            principal: record.principal.clone(),
            workspace_id: record.workspace_id.clone(),
            rate_limit_per_minute: record.rate_limit_per_minute,
        })
    }

    /// Records a token usage event: bumps `last_used_at` and appends a
    /// telemetry event attributed to the token's principal and workspace.
    ///
    /// # Errors
    /// Returns [`AccessRegistryError::ApiTokenNotFound`] for unknown ids
    /// and a persistence variant if writing fails.
    pub(crate) fn touch_api_token(
        &mut self,
        token_id: &str,
        feature_key: &str,
        category: &str,
        outcome: &str,
        detail: Option<&str>,
        now: i64,
    ) -> Result<(), AccessRegistryError> {
        let (principal, workspace_id, activity_at_unix_ms) = {
            let record = self
                .data
                .api_tokens
                .iter_mut()
                .find(|record| record.token_id == token_id)
                .ok_or_else(|| AccessRegistryError::ApiTokenNotFound(token_id.to_owned()))?;
            let activity_at_unix_ms =
                record.last_used_at_unix_ms.map_or(now, |last_used| last_used.max(now));
            record.last_used_at_unix_ms = Some(activity_at_unix_ms);
            record.updated_at_unix_ms = record.updated_at_unix_ms.max(activity_at_unix_ms);
            (record.principal.clone(), record.workspace_id.clone(), activity_at_unix_ms)
        };
        self.record_telemetry(
            feature_key,
            category,
            outcome,
            Some(principal.as_str()),
            workspace_id.as_deref(),
            Some(token_id),
            detail,
            activity_at_unix_ms,
        );
        self.persist()?;
        Ok(())
    }

    /// Creates a team, a workspace inside it, and an owner membership for
    /// the acting principal in one persisted step.
    ///
    /// # Errors
    /// Returns [`AccessRegistryError::FeatureDisabled`] when team mode is
    /// gated off, [`AccessRegistryError::InvalidField`] for empty names,
    /// and a persistence variant if writing fails.
    pub(crate) fn create_workspace_bundle(
        &mut self,
        actor_principal: &str,
        request: WorkspaceCreateRequest,
        now: i64,
    ) -> Result<WorkspaceCreateEnvelope, AccessRegistryError> {
        self.require_feature_enabled(FEATURE_TEAM_MODE)?;
        let team_name = normalize_required_text(request.team_name.as_str(), "team_name")?;
        let workspace_name =
            normalize_required_text(request.workspace_name.as_str(), "workspace_name")?;
        let team_id = Ulid::new().to_string();
        let workspace_id = Ulid::new().to_string();
        let team = TeamRecord {
            team_id: team_id.clone(),
            slug: slugify(team_name.as_str()),
            display_name: team_name,
            created_by_principal: actor_principal.to_owned(),
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        };
        let workspace = WorkspaceRecord {
            workspace_id: workspace_id.clone(),
            team_id: team_id.clone(),
            slug: slugify(workspace_name.as_str()),
            display_name: workspace_name,
            runtime_principal: workspace_runtime_principal(workspace_id.as_str()),
            runtime_device_id: workspace_runtime_device_id(workspace_id.as_str()),
            created_by_principal: actor_principal.to_owned(),
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        };
        let membership = MembershipRecord {
            membership_id: Ulid::new().to_string(),
            workspace_id: workspace_id.clone(),
            principal: actor_principal.to_owned(),
            role: WorkspaceRole::Owner,
            created_by_principal: actor_principal.to_owned(),
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        };
        self.data.teams.push(team.clone());
        self.data.workspaces.push(workspace.clone());
        self.data.memberships.push(membership.clone());
        self.record_telemetry(
            FEATURE_TEAM_MODE,
            "mutation",
            "workspace_created",
            Some(actor_principal),
            Some(workspace_id.as_str()),
            None,
            Some(workspace.display_name.as_str()),
            now,
        );
        self.persist()?;
        let membership_view = workspace_membership_view(&team, &workspace, &membership);
        Ok(WorkspaceCreateEnvelope { team, workspace, membership: membership_view })
    }

    /// Lists every membership of every workspace `principal` belongs to
    /// (not just the principal's own rows), sorted by workspace name and
    /// then principal for stable display.
    pub(crate) fn list_visible_workspace_memberships(
        &self,
        principal: &str,
    ) -> Vec<WorkspaceMembershipView> {
        let visible_workspace_ids = self
            .data
            .memberships
            .iter()
            .filter(|membership| membership.principal == principal)
            .map(|membership| membership.workspace_id.clone())
            .collect::<BTreeSet<_>>();
        let mut memberships = Vec::new();
        for membership in self
            .data
            .memberships
            .iter()
            .filter(|record| visible_workspace_ids.contains(record.workspace_id.as_str()))
        {
            let Some(workspace) = self
                .data
                .workspaces
                .iter()
                .find(|workspace| workspace.workspace_id == membership.workspace_id)
            else {
                continue;
            };
            let Some(team) = self.data.teams.iter().find(|team| team.team_id == workspace.team_id)
            else {
                continue;
            };
            memberships.push(workspace_membership_view(team, workspace, membership));
        }
        memberships.sort_by(|left, right| {
            left.workspace_name
                .cmp(&right.workspace_name)
                .then(left.principal.cmp(&right.principal))
        });
        memberships
    }

    /// Issues a workspace invitation and returns the acceptance secret
    /// exactly once; only its SHA-256 digest is persisted.
    ///
    /// # Errors
    /// Returns [`AccessRegistryError::FeatureDisabled`] when team mode is
    /// gated off, [`AccessRegistryError::AccessDenied`] when the actor
    /// cannot manage memberships, [`AccessRegistryError::InvalidField`] for
    /// malformed input or a non-future expiry, and a persistence variant if
    /// writing fails.
    pub(crate) fn create_invitation(
        &mut self,
        actor_principal: &str,
        request: InvitationCreateRequest,
        now: i64,
    ) -> Result<InvitationSecretEnvelope, AccessRegistryError> {
        self.require_feature_enabled(FEATURE_TEAM_MODE)?;
        self.authorize_workspace_permission(
            actor_principal,
            request.workspace_id.as_str(),
            PERMISSION_MEMBERSHIP_MANAGE,
        )?;
        let invited_identity =
            normalize_required_text(request.invited_identity.as_str(), "invited_identity")?;
        let role = WorkspaceRole::parse(request.role.as_str())?;
        if request.expires_at_unix_ms <= now {
            return Err(AccessRegistryError::InvalidField {
                field: "expires_at_unix_ms",
                message: "invitation expiry must be in the future".to_owned(),
            });
        }
        let token = mint_access_token_secret();
        let invitation = InvitationRecord {
            invitation_id: Ulid::new().to_string(),
            workspace_id: request.workspace_id,
            invited_identity,
            role,
            token_hash_sha256: sha256_hex(token.as_bytes()),
            issued_by_principal: actor_principal.to_owned(),
            created_at_unix_ms: now,
            expires_at_unix_ms: request.expires_at_unix_ms,
            accepted_by_principal: None,
            accepted_at_unix_ms: None,
        };
        self.data.invitations.push(invitation.clone());
        self.record_telemetry(
            FEATURE_TEAM_MODE,
            "mutation",
            "invitation_created",
            Some(actor_principal),
            Some(invitation.workspace_id.as_str()),
            None,
            Some(invitation.invited_identity.as_str()),
            now,
        );
        self.persist()?;
        Ok(InvitationSecretEnvelope { invitation_token: token, invitation })
    }

    /// Redeems an invitation secret and creates the promised membership.
    ///
    /// Invitations are single-use, expire at their deadline, and may only
    /// be accepted by the invited identity (compared ASCII
    /// case-insensitively).
    ///
    /// # Errors
    /// Returns [`AccessRegistryError::FeatureDisabled`] when team mode is
    /// gated off, [`AccessRegistryError::InvitationNotFound`] for unknown
    /// secrets, [`AccessRegistryError::InvitationAlreadyAccepted`] or
    /// [`AccessRegistryError::InvitationExpired`] for spent or stale
    /// invitations, [`AccessRegistryError::AccessDenied`] when the acceptor
    /// does not match the invited identity, and a persistence variant if
    /// writing fails.
    pub(crate) fn accept_invitation(
        &mut self,
        actor_principal: &str,
        invitation_token: &str,
        now: i64,
    ) -> Result<WorkspaceMembershipView, AccessRegistryError> {
        self.require_feature_enabled(FEATURE_TEAM_MODE)?;
        let hash =
            sha256_hex(normalize_required_text(invitation_token, "invitation_token")?.as_bytes());
        let invitation = self
            .data
            .invitations
            .iter()
            .find(|record| record.token_hash_sha256 == hash)
            .cloned()
            .ok_or(AccessRegistryError::InvitationNotFound)?;
        if invitation.accepted_at_unix_ms.is_some() {
            return Err(AccessRegistryError::InvitationAlreadyAccepted);
        }
        if invitation.expires_at_unix_ms <= now {
            return Err(AccessRegistryError::InvitationExpired);
        }
        if !invitation.invited_identity.eq_ignore_ascii_case(actor_principal) {
            return Err(AccessRegistryError::AccessDenied(
                "invitation was issued for a different principal".to_owned(),
            ));
        }
        if let Some(record) =
            self.data.invitations.iter_mut().find(|record| record.token_hash_sha256 == hash)
        {
            record.accepted_at_unix_ms = Some(now);
            record.accepted_by_principal = Some(actor_principal.to_owned());
        }
        let membership = MembershipRecord {
            membership_id: Ulid::new().to_string(),
            workspace_id: invitation.workspace_id.clone(),
            principal: actor_principal.to_owned(),
            role: invitation.role,
            created_by_principal: invitation.issued_by_principal.clone(),
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        };
        self.data.memberships.push(membership.clone());
        let workspace = self
            .data
            .workspaces
            .iter()
            .find(|workspace| workspace.workspace_id == membership.workspace_id)
            .cloned()
            .ok_or_else(|| {
                AccessRegistryError::WorkspaceNotFound(membership.workspace_id.clone())
            })?;
        let team = self
            .data
            .teams
            .iter()
            .find(|team| team.team_id == workspace.team_id)
            .cloned()
            .ok_or_else(|| AccessRegistryError::TeamNotFound(workspace.team_id.clone()))?;
        self.record_telemetry(
            FEATURE_TEAM_MODE,
            "mutation",
            "invitation_accepted",
            Some(actor_principal),
            Some(workspace.workspace_id.as_str()),
            None,
            Some(workspace.display_name.as_str()),
            now,
        );
        self.persist()?;
        Ok(workspace_membership_view(&team, &workspace, &membership))
    }

    /// Changes a member's role while enforcing the owner boundary: only
    /// owners may grant or revoke the owner role, members cannot
    /// self-promote to owner, and the last owner cannot be demoted.
    ///
    /// # Errors
    /// Returns [`AccessRegistryError::AccessDenied`] on any authorization
    /// or owner-boundary violation, [`AccessRegistryError::InvalidField`]
    /// for unknown role names, not-found variants for dangling workspace or
    /// team references, and a persistence variant if writing fails.
    pub(crate) fn update_membership_role(
        &mut self,
        actor_principal: &str,
        workspace_id: &str,
        member_principal: &str,
        role: &str,
        now: i64,
    ) -> Result<WorkspaceMembershipView, AccessRegistryError> {
        self.authorize_workspace_permission(
            actor_principal,
            workspace_id,
            PERMISSION_MEMBERSHIP_MANAGE,
        )?;
        let actor_role = self.workspace_role_for_principal(actor_principal, workspace_id)?;
        let role = WorkspaceRole::parse(role)?;
        let target_role = self.workspace_role_for_principal(member_principal, workspace_id)?;
        if (role == WorkspaceRole::Owner || target_role == WorkspaceRole::Owner)
            && actor_role != WorkspaceRole::Owner
        {
            return Err(AccessRegistryError::AccessDenied(
                "only workspace owners can grant or revoke owner role".to_owned(),
            ));
        }
        if actor_principal == member_principal
            && target_role != WorkspaceRole::Owner
            && role == WorkspaceRole::Owner
        {
            return Err(AccessRegistryError::AccessDenied(
                "members cannot self-promote to owner".to_owned(),
            ));
        }
        if target_role == WorkspaceRole::Owner
            && role != WorkspaceRole::Owner
            && self.workspace_owner_count(workspace_id) <= 1
        {
            return Err(AccessRegistryError::AccessDenied(
                "cannot remove or demote the last workspace owner".to_owned(),
            ));
        }
        {
            let membership = self
                .data
                .memberships
                .iter_mut()
                .find(|membership| {
                    membership.workspace_id == workspace_id
                        && membership.principal == member_principal
                })
                .ok_or_else(|| {
                    AccessRegistryError::AccessDenied(format!(
                        "membership for principal '{member_principal}' was not found"
                    ))
                })?;
            membership.role = role;
            membership.updated_at_unix_ms = now;
        }
        let workspace = self
            .data
            .workspaces
            .iter()
            .find(|workspace| workspace.workspace_id == workspace_id)
            .ok_or_else(|| AccessRegistryError::WorkspaceNotFound(workspace_id.to_owned()))?;
        let team = self
            .data
            .teams
            .iter()
            .find(|team| team.team_id == workspace.team_id)
            .ok_or_else(|| AccessRegistryError::TeamNotFound(workspace.team_id.clone()))?;
        let membership = self
            .data
            .memberships
            .iter()
            .find(|membership| {
                membership.workspace_id == workspace_id && membership.principal == member_principal
            })
            .ok_or_else(|| {
                AccessRegistryError::AccessDenied(format!(
                    "membership for principal '{member_principal}' was not found"
                ))
            })?;
        self.persist()?;
        Ok(workspace_membership_view(team, workspace, membership))
    }

    /// Removes a member from a workspace; only owners may remove owner
    /// memberships and the last owner can never be removed.
    ///
    /// # Errors
    /// Returns [`AccessRegistryError::AccessDenied`] on authorization or
    /// owner-boundary violations or when no matching membership exists, and
    /// a persistence variant if writing fails.
    pub(crate) fn remove_membership(
        &mut self,
        actor_principal: &str,
        workspace_id: &str,
        member_principal: &str,
        now: i64,
    ) -> Result<(), AccessRegistryError> {
        self.authorize_workspace_permission(
            actor_principal,
            workspace_id,
            PERMISSION_MEMBERSHIP_MANAGE,
        )?;
        let actor_role = self.workspace_role_for_principal(actor_principal, workspace_id)?;
        let target_role = self.workspace_role_for_principal(member_principal, workspace_id)?;
        if target_role == WorkspaceRole::Owner && actor_role != WorkspaceRole::Owner {
            return Err(AccessRegistryError::AccessDenied(
                "only workspace owners can remove owner memberships".to_owned(),
            ));
        }
        if target_role == WorkspaceRole::Owner && self.workspace_owner_count(workspace_id) <= 1 {
            return Err(AccessRegistryError::AccessDenied(
                "cannot remove or demote the last workspace owner".to_owned(),
            ));
        }
        let previous_len = self.data.memberships.len();
        self.data.memberships.retain(|membership| {
            !(membership.workspace_id == workspace_id && membership.principal == member_principal)
        });
        if self.data.memberships.len() == previous_len {
            return Err(AccessRegistryError::AccessDenied(format!(
                "membership for principal '{member_principal}' was not found"
            )));
        }
        self.record_telemetry(
            FEATURE_TEAM_MODE,
            "mutation",
            "membership_removed",
            Some(actor_principal),
            Some(workspace_id),
            None,
            Some(member_principal),
            now,
        );
        self.persist()?;
        Ok(())
    }

    /// Creates or updates the share exposing one resource to a workspace,
    /// keyed by `(workspace_id, resource_kind, resource_id)`.
    ///
    /// # Errors
    /// Returns [`AccessRegistryError::FeatureDisabled`] when RBAC is gated
    /// off, [`AccessRegistryError::AccessDenied`] when the actor cannot
    /// manage sharing, [`AccessRegistryError::InvalidField`] for empty
    /// fields, and a persistence variant if writing fails.
    pub(crate) fn upsert_resource_share(
        &mut self,
        actor_principal: &str,
        request: ResourceShareUpsertRequest,
        now: i64,
    ) -> Result<ResourceShareRecord, AccessRegistryError> {
        self.require_feature_enabled(FEATURE_RBAC)?;
        self.authorize_workspace_permission(
            actor_principal,
            request.workspace_id.as_str(),
            PERMISSION_SHARING_MANAGE,
        )?;
        let resource_kind =
            normalize_required_text(request.resource_kind.as_str(), "resource_kind")?;
        let resource_id = normalize_required_text(request.resource_id.as_str(), "resource_id")?;
        let access_level = normalize_required_text(request.access_level.as_str(), "access_level")?;
        if let Some(existing) = self.data.shares.iter_mut().find(|share| {
            share.workspace_id == request.workspace_id
                && share.resource_kind == resource_kind
                && share.resource_id == resource_id
        }) {
            existing.access_level = access_level;
            existing.updated_at_unix_ms = now;
            let updated = existing.clone();
            self.persist()?;
            return Ok(updated);
        }
        let share = ResourceShareRecord {
            share_id: Ulid::new().to_string(),
            resource_kind,
            resource_id,
            workspace_id: request.workspace_id,
            access_level,
            created_by_principal: actor_principal.to_owned(),
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        };
        self.data.shares.push(share.clone());
        self.persist()?;
        Ok(share)
    }

    /// Authorizes `principal` to exercise `permission` inside
    /// `workspace_id` and resolves the runtime identity to execute under.
    ///
    /// Deny-by-default: a missing membership and a role that does not
    /// expand to the permission both fail closed.
    ///
    /// # Errors
    /// Returns [`AccessRegistryError::AccessDenied`] when membership or
    /// permission is missing and
    /// [`AccessRegistryError::WorkspaceNotFound`] for a dangling workspace
    /// reference.
    pub(crate) fn authorize_workspace_permission(
        &self,
        principal: &str,
        workspace_id: &str,
        permission: &str,
    ) -> Result<WorkspaceAccessResolution, AccessRegistryError> {
        let membership = self
            .data
            .memberships
            .iter()
            .find(|membership| {
                membership.workspace_id == workspace_id && membership.principal == principal
            })
            .ok_or_else(|| {
                AccessRegistryError::AccessDenied(format!(
                    "principal '{principal}' is not a member of workspace '{workspace_id}'"
                ))
            })?;
        let permissions = membership.role.permissions();
        if !permissions.iter().any(|candidate| candidate == permission) {
            return Err(AccessRegistryError::AccessDenied(format!(
                "role '{}' does not grant permission '{}'",
                membership.role.as_str(),
                permission
            )));
        }
        let workspace = self
            .data
            .workspaces
            .iter()
            .find(|workspace| workspace.workspace_id == workspace_id)
            .ok_or_else(|| AccessRegistryError::WorkspaceNotFound(workspace_id.to_owned()))?;
        Ok(WorkspaceAccessResolution {
            workspace_id: workspace_id.to_owned(),
            runtime_principal: workspace.runtime_principal.clone(),
            runtime_device_id: workspace.runtime_device_id.clone(),
            actor_principal: principal.to_owned(),
            role: membership.role.as_str().to_owned(),
            permissions,
            reason: format!(
                "workspace '{}' grants '{}' role to '{}'",
                workspace.display_name,
                membership.role.as_str(),
                principal
            ),
        })
    }

    /// Resolves the workspace runtime identity for an authenticated token.
    ///
    /// Personal tokens (no workspace binding) resolve to `Ok(None)` and run
    /// under the caller's own identity; workspace-bound tokens must pass
    /// the full membership and permission check.
    ///
    /// # Errors
    /// Propagates [`Self::authorize_workspace_permission`] failures for
    /// workspace-bound tokens.
    pub(crate) fn resolve_workspace_access_for_token(
        &self,
        token: &AuthenticatedApiToken,
        required_permission: &str,
    ) -> Result<Option<WorkspaceAccessResolution>, AccessRegistryError> {
        let Some(workspace_id) = token.workspace_id.as_deref() else {
            return Ok(None);
        };
        self.authorize_workspace_permission(
            token.principal.as_str(),
            workspace_id,
            required_permission,
        )
        .map(Some)
    }

    fn workspace_role_for_principal(
        &self,
        principal: &str,
        workspace_id: &str,
    ) -> Result<WorkspaceRole, AccessRegistryError> {
        self.data
            .memberships
            .iter()
            .find(|membership| {
                membership.workspace_id == workspace_id && membership.principal == principal
            })
            .map(|membership| membership.role)
            .ok_or_else(|| {
                AccessRegistryError::AccessDenied(format!(
                    "principal '{principal}' is not a member of workspace '{workspace_id}'"
                ))
            })
    }

    fn workspace_owner_count(&self, workspace_id: &str) -> usize {
        self.data
            .memberships
            .iter()
            .filter(|membership| {
                membership.workspace_id == workspace_id && membership.role == WorkspaceRole::Owner
            })
            .count()
    }

    fn telemetry_summary(&self) -> Vec<FeatureTelemetrySummary> {
        let mut keys = BTreeSet::new();
        for event in &self.data.telemetry {
            keys.insert(event.feature_key.clone());
        }
        keys.into_iter()
            .map(|feature_key| {
                let events = self
                    .data
                    .telemetry
                    .iter()
                    .filter(|event| event.feature_key == feature_key)
                    .collect::<Vec<_>>();
                FeatureTelemetrySummary {
                    feature_key,
                    total_events: events.len(),
                    success_events: events
                        .iter()
                        .filter(|event| event.outcome == "ok" || event.outcome == "created")
                        .count(),
                    error_events: events
                        .iter()
                        .filter(|event| {
                            event.outcome.contains("error") || event.outcome == "denied"
                        })
                        .count(),
                    latest_at_unix_ms: events.iter().map(|event| event.recorded_at_unix_ms).max(),
                }
            })
            .collect()
    }

    /// Appends a telemetry event in memory, dropping the oldest events
    /// beyond [`TELEMETRY_EVENT_LIMIT`]; callers persist afterwards.
    #[allow(clippy::too_many_arguments)]
    fn record_telemetry(
        &mut self,
        feature_key: &str,
        category: &str,
        outcome: &str,
        principal: Option<&str>,
        workspace_id: Option<&str>,
        token_id: Option<&str>,
        detail: Option<&str>,
        now: i64,
    ) {
        self.data.telemetry.push(TelemetryEventRecord {
            event_id: Ulid::new().to_string(),
            feature_key: feature_key.to_owned(),
            category: category.to_owned(),
            outcome: outcome.to_owned(),
            principal: principal.map(ToOwned::to_owned),
            workspace_id: workspace_id.map(ToOwned::to_owned),
            token_id: token_id.map(ToOwned::to_owned),
            detail: detail.and_then(|value| trim_to_option(value.to_owned())),
            recorded_at_unix_ms: now,
        });
        if self.data.telemetry.len() > TELEMETRY_EVENT_LIMIT {
            let overflow = self.data.telemetry.len() - TELEMETRY_EVENT_LIMIT;
            self.data.telemetry.drain(0..overflow);
        }
    }

    fn apply_migrations(&mut self) -> Result<(), AccessRegistryError> {
        match self.data.version {
            ACCESS_REGISTRY_VERSION => {
                let _ = repair_access_registry_data(&mut self.data);
                Ok(())
            }
            other => Err(AccessRegistryError::InvalidField {
                field: "version",
                message: format!(
                    "unsupported access registry version {other}; expected {ACCESS_REGISTRY_VERSION}"
                ),
            }),
        }
    }

    fn persist(&self) -> Result<(), AccessRegistryError> {
        let encoded = serde_json::to_string_pretty(&self.data)
            .map_err(|error| AccessRegistryError::SerializeRegistry(error.to_string()))?;
        fs::write(&self.path, encoded).map_err(|error| AccessRegistryError::WriteRegistry {
            path: self.path.display().to_string(),
            error: error.to_string(),
        })
    }
}

/// Authoritative flag inventory; backfill restores any record missing from
/// disk with these defaults (everything off except routine automation).
fn default_feature_flags() -> Vec<FeatureFlagRecord> {
    vec![
        FeatureFlagRecord {
            key: FEATURE_COMPAT_API.to_owned(),
            label: "OpenAI-compatible API".to_owned(),
            description:
                "Expose the minimal OpenAI-compatible facade for models, chat completions, and responses."
                    .to_owned(),
            enabled: false,
            stage: "admin_only".to_owned(),
            depends_on: Vec::new(),
            updated_at_unix_ms: 0,
            updated_by_principal: "system".to_owned(),
        },
        FeatureFlagRecord {
            key: FEATURE_COMPAT_EMBEDDINGS_API.to_owned(),
            label: "Compat embeddings API".to_owned(),
            description:
                "Expose POST /v1/embeddings on the compat facade when a production embeddings path is available."
                    .to_owned(),
            enabled: false,
            stage: "admin_only".to_owned(),
            depends_on: vec![FEATURE_COMPAT_API.to_owned()],
            updated_at_unix_ms: 0,
            updated_by_principal: "system".to_owned(),
        },
        FeatureFlagRecord {
            key: FEATURE_COMPAT_TOOLS_INVOKE.to_owned(),
            label: "Compat tools invoke".to_owned(),
            description:
                "Allow the conservative /v1/tools/invoke compat skeleton for explicit operator testing."
                    .to_owned(),
            enabled: false,
            stage: "lab".to_owned(),
            depends_on: vec![FEATURE_COMPAT_API.to_owned()],
            updated_at_unix_ms: 0,
            updated_by_principal: "system".to_owned(),
        },
        FeatureFlagRecord {
            key: FEATURE_API_TOKENS.to_owned(),
            label: "External API tokens".to_owned(),
            description:
                "Allow operators to issue scoped external API tokens with rotation and lifecycle control."
                    .to_owned(),
            enabled: false,
            stage: "admin_only".to_owned(),
            depends_on: vec![FEATURE_COMPAT_API.to_owned()],
            updated_at_unix_ms: 0,
            updated_by_principal: "system".to_owned(),
        },
        FeatureFlagRecord {
            key: FEATURE_TEAM_MODE.to_owned(),
            label: "Team and workspace mode".to_owned(),
            description:
                "Enable shared workspaces, invitations, and workspace-bound runtime principals."
                    .to_owned(),
            enabled: false,
            stage: "pilot".to_owned(),
            depends_on: Vec::new(),
            updated_at_unix_ms: 0,
            updated_by_principal: "system".to_owned(),
        },
        FeatureFlagRecord {
            key: FEATURE_RBAC.to_owned(),
            label: "RBAC and sharing".to_owned(),
            description:
                "Enforce role-based access checks and explicit workspace sharing metadata."
                    .to_owned(),
            enabled: false,
            stage: "pilot".to_owned(),
            depends_on: vec![FEATURE_TEAM_MODE.to_owned()],
            updated_at_unix_ms: 0,
            updated_by_principal: "system".to_owned(),
        },
        FeatureFlagRecord {
            key: FEATURE_STAGED_ROLLOUT.to_owned(),
            label: "Staged rollout controls".to_owned(),
            description:
                "Expose rollout stages, kill switches, and privacy-aware feature telemetry."
                    .to_owned(),
            enabled: false,
            stage: "internal".to_owned(),
            depends_on: Vec::new(),
            updated_at_unix_ms: 0,
            updated_by_principal: "system".to_owned(),
        },
        FeatureFlagRecord {
            key: FEATURE_ROUTINES_AUTOMATION.to_owned(),
            label: "Routine automation controls".to_owned(),
            description:
                "Enable unattended routine scheduler controls with command kill switches and operator review fallback."
                    .to_owned(),
            enabled: true,
            stage: "pilot".to_owned(),
            depends_on: vec![FEATURE_STAGED_ROLLOUT.to_owned()],
            updated_at_unix_ms: 0,
            updated_by_principal: "system".to_owned(),
        },
    ]
}

fn backfill_missing_feature_flags(feature_flags: &mut Vec<FeatureFlagRecord>) {
    for expected in default_feature_flags() {
        if feature_flags.iter().any(|flag| flag.key == expected.key) {
            continue;
        }
        feature_flags.push(expected);
    }
}

fn missing_feature_flag_keys(feature_flags: &[FeatureFlagRecord]) -> Vec<String> {
    default_feature_flags()
        .into_iter()
        .filter(|expected| !feature_flags.iter().any(|flag| flag.key == expected.key))
        .map(|flag| flag.key)
        .collect()
}

/// Non-destructive schema repair shared by `open` and `run_backfill`:
/// restores missing flags, regenerates empty slugs/runtime bindings,
/// normalizes token scopes and limits, and trims telemetry to budget.
/// Idempotent; the returned report's `dry_run` is overridden by callers.
fn repair_access_registry_data(data: &mut AccessRegistryFile) -> AccessBackfillReport {
    let feature_flags_before = data.feature_flags.len();
    backfill_missing_feature_flags(&mut data.feature_flags);
    let feature_flags_added = data.feature_flags.len().saturating_sub(feature_flags_before);

    let mut teams_repaired = 0_usize;
    for team in &mut data.teams {
        if team.slug.trim().is_empty() {
            team.slug = slugify(team.display_name.as_str());
            teams_repaired += 1;
        }
    }

    let mut workspaces_repaired = 0_usize;
    for workspace in &mut data.workspaces {
        let mut changed = false;
        if workspace.slug.trim().is_empty() {
            workspace.slug = slugify(workspace.display_name.as_str());
            changed = true;
        }
        if workspace.runtime_principal.trim().is_empty() {
            workspace.runtime_principal =
                workspace_runtime_principal(workspace.workspace_id.as_str());
            changed = true;
        }
        if workspace.runtime_device_id.trim().is_empty() {
            workspace.runtime_device_id =
                workspace_runtime_device_id(workspace.workspace_id.as_str());
            changed = true;
        }
        if changed {
            workspaces_repaired += 1;
        }
    }

    let mut api_tokens_repaired = 0_usize;
    for token in &mut data.api_tokens {
        let mut changed = false;
        if token.scopes.is_empty() {
            let defaults = WorkspaceRole::parse(token.role.as_str())
                .map(|role| normalize_scopes(Vec::new(), role.permissions()))
                .unwrap_or_default();
            if !defaults.is_empty() {
                token.scopes = defaults;
                changed = true;
            }
        }
        let clamped = token.rate_limit_per_minute.clamp(10, 10_000);
        if clamped != token.rate_limit_per_minute {
            token.rate_limit_per_minute = clamped;
            changed = true;
        }
        if changed {
            api_tokens_repaired += 1;
        }
    }

    let mut memberships_repaired = 0_usize;
    for membership in &mut data.memberships {
        if membership.updated_at_unix_ms < membership.created_at_unix_ms {
            membership.updated_at_unix_ms = membership.created_at_unix_ms;
            memberships_repaired += 1;
        }
    }

    let telemetry_trimmed = data.telemetry.len().saturating_sub(TELEMETRY_EVENT_LIMIT);
    if telemetry_trimmed > 0 {
        data.telemetry.drain(0..telemetry_trimmed);
    }

    let changed_records = feature_flags_added
        + teams_repaired
        + workspaces_repaired
        + api_tokens_repaired
        + memberships_repaired
        + telemetry_trimmed;

    let mut notes = Vec::new();
    if feature_flags_added > 0 {
        notes.push(format!("added {feature_flags_added} missing feature flag records"));
    }
    if workspaces_repaired > 0 {
        notes.push(format!("repaired {workspaces_repaired} workspace runtime bindings"));
    }
    if api_tokens_repaired > 0 {
        notes.push(format!("repaired {api_tokens_repaired} API token scope or limit records"));
    }
    if telemetry_trimmed > 0 {
        notes.push(format!("trimmed {telemetry_trimmed} telemetry events to retention budget"));
    }
    if notes.is_empty() {
        notes.push("registry already matched the current access schema contract".to_owned());
    }

    AccessBackfillReport {
        dry_run: false,
        changed_records,
        feature_flags_added,
        teams_repaired,
        workspaces_repaired,
        api_tokens_repaired,
        memberships_repaired,
        telemetry_trimmed,
        notes,
    }
}

fn normalize_required_text(raw: &str, field: &'static str) -> Result<String, AccessRegistryError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(AccessRegistryError::InvalidField {
            field,
            message: "value cannot be empty".to_owned(),
        });
    }
    Ok(trimmed.to_owned())
}

fn trim_to_option(raw: String) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

/// Trims, lowercases, sorts, and dedups scope names; an empty request
/// falls back to `defaults` (the role's permission set).
fn normalize_scopes(requested: Vec<String>, defaults: Vec<String>) -> Vec<String> {
    let mut scopes = if requested.is_empty() { defaults } else { requested };
    scopes = scopes
        .into_iter()
        .filter_map(trim_to_option)
        .map(|scope| scope.to_ascii_lowercase())
        .collect();
    scopes.sort();
    scopes.dedup();
    scopes
}

/// Mints a fresh secret: 24 bytes from the OS-seeded CSPRNG, URL-safe
/// base64, behind a fixed product prefix that identifies the token kind.
fn mint_access_token_secret() -> String {
    let mut bytes = [0_u8; 24];
    rand::rng().fill_bytes(&mut bytes);
    format!("palyra_{encoded}", encoded = URL_SAFE_NO_PAD.encode(bytes))
}

fn token_prefix(token: &str) -> String {
    token.chars().take(16).collect()
}

fn slugify(raw: &str) -> String {
    let mut slug = String::with_capacity(raw.len());
    let mut previous_was_dash = false;
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch.to_ascii_lowercase());
            previous_was_dash = false;
        } else if !previous_was_dash {
            slug.push('-');
            previous_was_dash = true;
        }
    }
    let normalized = slug.trim_matches('-').to_owned();
    if normalized.is_empty() {
        Ulid::new().to_string().to_ascii_lowercase()
    } else {
        normalized
    }
}

fn workspace_runtime_principal(workspace_id: &str) -> String {
    format!("workspace:{workspace_id}")
}

fn workspace_runtime_device_id(workspace_id: &str) -> String {
    workspace_id.to_owned()
}

/// Projects a token record into its secret-free view, deriving `state`;
/// `now <= 0` skips expiry derivation for clockless list views.
fn api_token_view(record: &ApiTokenRecord, now: i64) -> ApiTokenView {
    let state = if record.revoked_at_unix_ms.is_some() {
        "revoked"
    } else if now > 0 && record.expires_at_unix_ms.is_some_and(|expires_at| expires_at <= now) {
        "expired"
    } else {
        "active"
    };
    ApiTokenView {
        token_id: record.token_id.clone(),
        label: record.label.clone(),
        token_prefix: record.token_prefix.clone(),
        scopes: record.scopes.clone(),
        principal: record.principal.clone(),
        workspace_id: record.workspace_id.clone(),
        role: record.role.clone(),
        rate_limit_per_minute: record.rate_limit_per_minute,
        created_at_unix_ms: record.created_at_unix_ms,
        updated_at_unix_ms: record.updated_at_unix_ms,
        expires_at_unix_ms: record.expires_at_unix_ms,
        revoked_at_unix_ms: record.revoked_at_unix_ms,
        last_used_at_unix_ms: record.last_used_at_unix_ms,
        state: state.to_owned(),
        rotated_from_token_id: record.rotated_from_token_id.clone(),
    }
}

fn workspace_membership_view(
    team: &TeamRecord,
    workspace: &WorkspaceRecord,
    membership: &MembershipRecord,
) -> WorkspaceMembershipView {
    WorkspaceMembershipView {
        membership_id: membership.membership_id.clone(),
        workspace_id: workspace.workspace_id.clone(),
        workspace_name: workspace.display_name.clone(),
        workspace_slug: workspace.slug.clone(),
        team_id: team.team_id.clone(),
        team_name: team.display_name.clone(),
        principal: membership.principal.clone(),
        role: membership.role.as_str().to_owned(),
        permissions: membership.role.permissions(),
        runtime_principal: workspace.runtime_principal.clone(),
        runtime_device_id: workspace.runtime_device_id.clone(),
        created_at_unix_ms: membership.created_at_unix_ms,
        updated_at_unix_ms: membership.updated_at_unix_ms,
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn normalize_scopes_falls_back_to_role_permissions() {
        let scopes = normalize_scopes(
            vec![" compat.chat.create ".to_owned(), "compat.chat.create".to_owned()],
            WorkspaceRole::Operator.permissions(),
        );
        assert_eq!(scopes, vec!["compat.chat.create".to_owned()]);

        let defaults = normalize_scopes(Vec::new(), WorkspaceRole::Admin.permissions());
        assert!(defaults.iter().any(|scope| scope == PERMISSION_API_TOKENS_MANAGE));
    }

    #[test]
    fn workspace_bundle_and_invitation_flow_is_audit_safe() {
        let temp = tempdir().expect("tempdir should exist");
        let mut registry = AccessRegistry::open(temp.path()).expect("registry should open");
        registry
            .set_feature_flag(FEATURE_TEAM_MODE, true, Some("pilot".to_owned()), "user:owner", 10)
            .expect("team mode should enable");
        let created = registry
            .create_workspace_bundle(
                "user:owner",
                WorkspaceCreateRequest {
                    team_name: "Core Team".to_owned(),
                    workspace_name: "Incident Ops".to_owned(),
                },
                20,
            )
            .expect("workspace should be created");
        let invitation = registry
            .create_invitation(
                "user:owner",
                InvitationCreateRequest {
                    workspace_id: created.workspace.workspace_id.clone(),
                    invited_identity: "user:operator".to_owned(),
                    role: "operator".to_owned(),
                    expires_at_unix_ms: 10_000,
                },
                30,
            )
            .expect("invitation should be created");
        let accepted = registry
            .accept_invitation("user:operator", invitation.invitation_token.as_str(), 40)
            .expect("invitation should be accepted");

        assert_eq!(accepted.workspace_id, created.workspace.workspace_id);
        assert_eq!(accepted.role, "operator");
        assert_eq!(accepted.runtime_device_id, created.workspace.runtime_device_id);
    }

    #[test]
    fn membership_owner_transitions_enforce_owner_boundary() {
        let temp = tempdir().expect("tempdir should exist");
        let mut registry = AccessRegistry::open(temp.path()).expect("registry should open");
        registry
            .set_feature_flag(FEATURE_TEAM_MODE, true, Some("pilot".to_owned()), "user:owner", 1)
            .expect("team mode should enable");
        let created = registry
            .create_workspace_bundle(
                "user:owner",
                WorkspaceCreateRequest {
                    team_name: "Core Team".to_owned(),
                    workspace_name: "Incident Ops".to_owned(),
                },
                2,
            )
            .expect("workspace should be created");
        let workspace_id = created.workspace.workspace_id.clone();
        let admin_invitation = registry
            .create_invitation(
                "user:owner",
                InvitationCreateRequest {
                    workspace_id: workspace_id.clone(),
                    invited_identity: "user:admin".to_owned(),
                    role: "admin".to_owned(),
                    expires_at_unix_ms: 10_000,
                },
                3,
            )
            .expect("admin invitation should be created");
        let operator_invitation = registry
            .create_invitation(
                "user:owner",
                InvitationCreateRequest {
                    workspace_id: workspace_id.clone(),
                    invited_identity: "user:operator".to_owned(),
                    role: "operator".to_owned(),
                    expires_at_unix_ms: 10_000,
                },
                4,
            )
            .expect("operator invitation should be created");
        registry
            .accept_invitation("user:admin", admin_invitation.invitation_token.as_str(), 5)
            .expect("admin invitation should be accepted");
        registry
            .accept_invitation("user:operator", operator_invitation.invitation_token.as_str(), 6)
            .expect("operator invitation should be accepted");

        assert!(
            registry
                .update_membership_role(
                    "user:admin",
                    workspace_id.as_str(),
                    "user:admin",
                    "owner",
                    7,
                )
                .is_err(),
            "admins must not be able to self-promote to owner"
        );
        assert!(
            registry
                .update_membership_role(
                    "user:owner",
                    workspace_id.as_str(),
                    "user:owner",
                    "admin",
                    8,
                )
                .is_err(),
            "the last workspace owner must not be demotable"
        );
        assert!(
            registry
                .remove_membership("user:admin", workspace_id.as_str(), "user:owner", 9)
                .is_err(),
            "admins must not be able to remove owner memberships"
        );
        registry
            .update_membership_role(
                "user:owner",
                workspace_id.as_str(),
                "user:operator",
                "owner",
                10,
            )
            .expect("owner should be able to promote another member to owner");
        registry
            .update_membership_role("user:owner", workspace_id.as_str(), "user:owner", "admin", 11)
            .expect("owner should be demotable once another owner exists");
    }

    #[test]
    fn api_tokens_require_scope_and_rotation_revokes_previous_secret() {
        let temp = tempdir().expect("tempdir should exist");
        let mut registry = AccessRegistry::open(temp.path()).expect("registry should open");
        registry
            .set_feature_flag(
                FEATURE_COMPAT_API,
                true,
                Some("admin_only".to_owned()),
                "user:ops",
                1,
            )
            .expect("compat api should enable");
        registry
            .set_feature_flag(
                FEATURE_API_TOKENS,
                true,
                Some("admin_only".to_owned()),
                "user:ops",
                2,
            )
            .expect("api tokens should enable");
        let created = registry
            .create_api_token(
                "user:ops",
                ApiTokenCreateRequest {
                    label: "Compat".to_owned(),
                    scopes: vec![PERMISSION_COMPAT_CHAT_CREATE.to_owned()],
                    principal: "user:ops".to_owned(),
                    workspace_id: None,
                    role: "operator".to_owned(),
                    expires_at_unix_ms: None,
                    rate_limit_per_minute: Some(50),
                },
                3,
            )
            .expect("token should be created");

        registry
            .authenticate_api_token(created.token.as_str(), PERMISSION_COMPAT_CHAT_CREATE, 4)
            .expect("created token should authenticate");
        let rotated = registry
            .rotate_api_token("user:ops", created.token_record.token_id.as_str(), 5)
            .expect("token should rotate");
        assert!(
            registry
                .authenticate_api_token(created.token.as_str(), PERMISSION_COMPAT_CHAT_CREATE, 6)
                .is_err(),
            "previous token secret must be invalid after rotation"
        );
        assert_eq!(
            rotated.token_record.rotated_from_token_id.as_deref(),
            Some(created.token_record.token_id.as_str())
        );
    }

    #[test]
    fn api_token_activity_timestamps_never_move_backwards() {
        let temp = tempdir().expect("tempdir should exist");
        let mut registry = AccessRegistry::open(temp.path()).expect("registry should open");
        registry
            .set_feature_flag(
                FEATURE_COMPAT_API,
                true,
                Some("admin_only".to_owned()),
                "user:ops",
                1,
            )
            .expect("compat api should enable");
        registry
            .set_feature_flag(
                FEATURE_API_TOKENS,
                true,
                Some("admin_only".to_owned()),
                "user:ops",
                2,
            )
            .expect("api tokens should enable");
        let created = registry
            .create_api_token(
                "user:ops",
                ApiTokenCreateRequest {
                    label: "Compat".to_owned(),
                    scopes: vec![PERMISSION_COMPAT_RESPONSES_CREATE.to_owned()],
                    principal: "user:ops".to_owned(),
                    workspace_id: None,
                    role: "operator".to_owned(),
                    expires_at_unix_ms: None,
                    rate_limit_per_minute: Some(50),
                },
                10,
            )
            .expect("token should be created");
        let token_id = created.token_record.token_id;

        registry
            .touch_api_token(
                token_id.as_str(),
                FEATURE_COMPAT_API,
                "runs_wait",
                "run_wait_completed",
                None,
                30,
            )
            .expect("newer activity should persist");
        registry
            .touch_api_token(
                token_id.as_str(),
                FEATURE_COMPAT_API,
                "runs_wait",
                "run_wait_completed",
                None,
                20,
            )
            .expect("late stale activity should be recorded without backdating");

        let token = registry
            .data
            .api_tokens
            .iter()
            .find(|record| record.token_id == token_id)
            .expect("token should remain registered");
        assert_eq!(token.last_used_at_unix_ms, Some(30));
        assert_eq!(token.updated_at_unix_ms, 30);
        let latest_activity = registry
            .data
            .telemetry
            .iter()
            .rev()
            .find(|event| event.token_id.as_deref() == Some(token_id.as_str()))
            .expect("token activity telemetry should be recorded");
        assert_eq!(latest_activity.recorded_at_unix_ms, 30);
    }

    #[test]
    fn response_creation_scope_cannot_control_or_approve_compat_runs() {
        let temp = tempdir().expect("tempdir should exist");
        let mut registry = AccessRegistry::open(temp.path()).expect("registry should open");
        registry
            .set_feature_flag(
                FEATURE_COMPAT_API,
                true,
                Some("admin_only".to_owned()),
                "user:ops",
                1,
            )
            .expect("compat api should enable");
        registry
            .set_feature_flag(
                FEATURE_API_TOKENS,
                true,
                Some("admin_only".to_owned()),
                "user:ops",
                2,
            )
            .expect("api tokens should enable");
        let created = registry
            .create_api_token(
                "user:ops",
                ApiTokenCreateRequest {
                    label: "Compat responses".to_owned(),
                    scopes: vec![PERMISSION_COMPAT_RESPONSES_CREATE.to_owned()],
                    principal: "user:ops".to_owned(),
                    workspace_id: None,
                    role: "operator".to_owned(),
                    expires_at_unix_ms: None,
                    rate_limit_per_minute: Some(50),
                },
                3,
            )
            .expect("token should be created");

        registry
            .authenticate_api_token(created.token.as_str(), PERMISSION_COMPAT_RESPONSES_CREATE, 4)
            .expect("response creation scope should remain valid");
        assert!(matches!(
            registry.authenticate_api_token(
                created.token.as_str(),
                PERMISSION_COMPAT_RUNS_CONTROL,
                4,
            ),
            Err(AccessRegistryError::MissingScope(scope))
                if scope == PERMISSION_COMPAT_RUNS_CONTROL
        ));
        assert!(matches!(
            registry.authenticate_api_token(
                created.token.as_str(),
                PERMISSION_COMPAT_RUNS_APPROVE,
                4,
            ),
            Err(AccessRegistryError::MissingScope(scope))
                if scope == PERMISSION_COMPAT_RUNS_APPROVE
        ));
    }

    #[test]
    fn backfill_repairs_access_registry_records_idempotently() {
        let temp = tempdir().expect("tempdir should exist");
        let registry_path = temp.path().join(ACCESS_REGISTRY_FILE_NAME);
        let raw = serde_json::json!({
            "version": ACCESS_REGISTRY_VERSION,
            "feature_flags": [{
                "key": FEATURE_COMPAT_API,
                "label": "OpenAI-compatible API",
                "description": "compat",
                "enabled": false,
                "stage": "admin_only",
                "depends_on": [],
                "updated_at_unix_ms": 0,
                "updated_by_principal": "system"
            }],
            "api_tokens": [{
                "token_id": "01TESTTOKEN",
                "label": "Compat",
                "token_prefix": "palyra_test",
                "token_hash_sha256": "abcd",
                "scopes": [],
                "principal": "user:ops",
                "workspace_id": null,
                "role": "operator",
                "rate_limit_per_minute": 1,
                "created_at_unix_ms": 1,
                "updated_at_unix_ms": 1,
                "expires_at_unix_ms": null,
                "revoked_at_unix_ms": null,
                "last_used_at_unix_ms": null,
                "rotated_from_token_id": null
            }],
            "teams": [{
                "team_id": "01TEAM",
                "slug": "",
                "display_name": "Core Team",
                "created_by_principal": "user:ops",
                "created_at_unix_ms": 1,
                "updated_at_unix_ms": 1
            }],
            "workspaces": [{
                "workspace_id": "01WORKSPACE",
                "team_id": "01TEAM",
                "slug": "",
                "display_name": "Incident Ops",
                "runtime_principal": "",
                "runtime_device_id": "",
                "created_by_principal": "user:ops",
                "created_at_unix_ms": 1,
                "updated_at_unix_ms": 1
            }],
            "memberships": [{
                "membership_id": "01MEMBERSHIP",
                "workspace_id": "01WORKSPACE",
                "principal": "user:ops",
                "role": "owner",
                "created_by_principal": "user:ops",
                "created_at_unix_ms": 20,
                "updated_at_unix_ms": 10
            }],
            "invitations": [],
            "shares": [],
            "telemetry": []
        });
        fs::write(
            registry_path,
            serde_json::to_string_pretty(&raw).expect("fixture should serialize"),
        )
        .expect("fixture should persist");

        let mut registry = AccessRegistry::open(temp.path()).expect("registry should open");
        let before = registry.migration_status();
        assert!(
            !before.backfill_required,
            "open() should already run non-destructive schema backfills"
        );
        let report =
            registry.run_backfill("user:ops", false, 100).expect("backfill should succeed");
        assert_eq!(report.changed_records, 0, "second repair pass should be idempotent");
        let snapshot = registry.snapshot("user:ops");
        assert!(snapshot.rollout.external_api_safe_mode);
        assert!(
            snapshot.feature_flags.iter().any(|flag| flag.key == FEATURE_STAGED_ROLLOUT),
            "backfill should restore missing rollout package flags"
        );
        let routines_flag = snapshot
            .feature_flags
            .iter()
            .find(|flag| flag.key == FEATURE_ROUTINES_AUTOMATION)
            .expect("backfill should restore routine automation rollout flag");
        assert!(routines_flag.enabled);
        assert_eq!(routines_flag.depends_on, vec![FEATURE_STAGED_ROLLOUT.to_owned()]);
        assert_eq!(snapshot.api_tokens[0].rate_limit_per_minute, 10);
        assert!(snapshot.api_tokens[0]
            .scopes
            .iter()
            .any(|scope| scope == PERMISSION_COMPAT_CHAT_CREATE));
    }
}
