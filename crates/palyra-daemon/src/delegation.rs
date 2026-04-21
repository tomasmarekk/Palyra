use serde::{Deserialize, Serialize};
use tonic::Status;

const DEFAULT_MODEL_PROFILE: &str = "gpt-4o-mini";
const DEFAULT_MAX_ATTEMPTS: u64 = 3;
const DEFAULT_MAX_CONCURRENT_CHILDREN: u64 = 2;
const DEFAULT_MAX_CHILDREN_PER_PARENT: u64 = 8;
const DEFAULT_MAX_PARALLEL_GROUPS: u64 = 2;
const DEFAULT_CHILD_TIMEOUT_MS: u64 = 10 * 60 * 1_000;
const MAX_DELEGATION_BUDGET_TOKENS: u64 = 32_768;
const MAX_DELEGATION_ATTEMPTS: u64 = 16;
const MAX_DELEGATION_CONCURRENT_CHILDREN: u64 = 16;
const MAX_DELEGATION_CHILDREN_PER_PARENT: u64 = 64;
const MAX_DELEGATION_PARALLEL_GROUPS: u64 = 16;
const MAX_DELEGATION_CHILD_TIMEOUT_MS: u64 = 6 * 60 * 60 * 1_000;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DelegationRole {
    Research,
    Synthesis,
    Review,
    Patching,
    Triage,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DelegationExecutionMode {
    Serial,
    Parallel,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DelegationMemoryScopeKind {
    None,
    ParentSession,
    ParentSessionAndWorkspace,
    WorkspaceOnly,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DelegationMergeStrategy {
    Summarize,
    Compare,
    PatchReview,
    Triage,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegationMergeContract {
    pub strategy: DelegationMergeStrategy,
    pub approval_required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegationRuntimeLimits {
    pub max_concurrent_children: u64,
    pub max_children_per_parent: u64,
    pub max_parallel_groups: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub child_budget_override: Option<u64>,
    pub child_timeout_ms: u64,
}

impl Default for DelegationRuntimeLimits {
    fn default() -> Self {
        Self {
            max_concurrent_children: DEFAULT_MAX_CONCURRENT_CHILDREN,
            max_children_per_parent: DEFAULT_MAX_CHILDREN_PER_PARENT,
            max_parallel_groups: DEFAULT_MAX_PARALLEL_GROUPS,
            child_budget_override: None,
            child_timeout_ms: DEFAULT_CHILD_TIMEOUT_MS,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegationSnapshot {
    pub profile_id: String,
    pub display_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub template_id: Option<String>,
    pub role: DelegationRole,
    pub execution_mode: DelegationExecutionMode,
    pub group_id: String,
    pub model_profile: String,
    pub tool_allowlist: Vec<String>,
    pub skill_allowlist: Vec<String>,
    pub memory_scope: DelegationMemoryScopeKind,
    pub budget_tokens: u64,
    pub max_attempts: u64,
    pub merge_contract: DelegationMergeContract,
    #[serde(default)]
    pub runtime_limits: DelegationRuntimeLimits,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegationMergeProvenanceRecord {
    pub child_run_id: String,
    pub kind: String,
    pub label: String,
    pub excerpt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    pub requires_approval: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DelegationMergeFailureCategory {
    Model,
    Tool,
    Approval,
    Budget,
    Cancellation,
    Transport,
    Unknown,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegationMergeUsageSummary {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegationMergeApprovalSummary {
    pub approval_required: bool,
    pub approval_events: u64,
    pub approval_pending: bool,
    pub approval_denied: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegationMergeArtifactReference {
    pub artifact_id: String,
    pub artifact_kind: String,
    pub label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegationToolTraceSummary {
    pub child_run_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proposal_id: Option<String>,
    pub tool_name: String,
    pub status: String,
    pub excerpt: String,
    pub requires_approval: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegationMergeResult {
    pub status: String,
    pub strategy: DelegationMergeStrategy,
    pub summary_text: String,
    pub warnings: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_category: Option<DelegationMergeFailureCategory>,
    pub approval_required: bool,
    #[serde(default)]
    pub approval_summary: DelegationMergeApprovalSummary,
    #[serde(default)]
    pub usage_summary: DelegationMergeUsageSummary,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifact_references: Vec<DelegationMergeArtifactReference>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_trace_summary: Vec<DelegationToolTraceSummary>,
    pub provenance: Vec<DelegationMergeProvenanceRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub merged_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegationProfileDefinition {
    pub profile_id: String,
    pub display_name: String,
    pub description: String,
    pub role: DelegationRole,
    pub model_profile: String,
    pub tool_allowlist: Vec<String>,
    pub skill_allowlist: Vec<String>,
    pub memory_scope: DelegationMemoryScopeKind,
    pub budget_tokens: u64,
    pub max_attempts: u64,
    pub execution_mode: DelegationExecutionMode,
    pub merge_contract: DelegationMergeContract,
    #[serde(default)]
    pub runtime_limits: DelegationRuntimeLimits,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegationTemplateDefinition {
    pub template_id: String,
    pub display_name: String,
    pub description: String,
    pub primary_profile_id: String,
    pub recommended_profiles: Vec<String>,
    pub execution_mode: DelegationExecutionMode,
    pub merge_strategy: DelegationMergeStrategy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_limits: Option<DelegationRuntimeLimits>,
    pub examples: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegationCatalog {
    pub profiles: Vec<DelegationProfileDefinition>,
    pub templates: Vec<DelegationTemplateDefinition>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Default)]
pub struct DelegationRequestInput {
    #[serde(default)]
    pub profile_id: Option<String>,
    #[serde(default)]
    pub template_id: Option<String>,
    #[serde(default)]
    pub group_id: Option<String>,
    #[serde(default)]
    pub execution_mode: Option<DelegationExecutionMode>,
    #[serde(default)]
    pub manifest: Option<DelegationManifestInput>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Default)]
pub struct DelegationManifestInput {
    #[serde(default)]
    pub profile_id: Option<String>,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub role: Option<DelegationRole>,
    #[serde(default)]
    pub model_profile: Option<String>,
    #[serde(default)]
    pub tool_allowlist: Vec<String>,
    #[serde(default)]
    pub skill_allowlist: Vec<String>,
    #[serde(default)]
    pub memory_scope: Option<DelegationMemoryScopeKind>,
    #[serde(default)]
    pub budget_tokens: Option<u64>,
    #[serde(default)]
    pub max_attempts: Option<u64>,
    #[serde(default)]
    pub merge_strategy: Option<DelegationMergeStrategy>,
    #[serde(default)]
    pub approval_required: Option<bool>,
    #[serde(default)]
    pub max_concurrent_children: Option<u64>,
    #[serde(default)]
    pub max_children_per_parent: Option<u64>,
    #[serde(default)]
    pub max_parallel_groups: Option<u64>,
    #[serde(default)]
    pub child_budget_override: Option<u64>,
    #[serde(default)]
    pub child_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DelegationParentContext {
    pub parent_run_id: Option<String>,
    pub agent_id: Option<String>,
    pub parent_model_profile: Option<String>,
    pub parent_tool_allowlist: Vec<String>,
    pub parent_skill_allowlist: Vec<String>,
    pub parent_budget_tokens: Option<u64>,
}

#[allow(clippy::too_many_arguments)]
fn profile_definition(
    profile_id: &str,
    display_name: &str,
    description: &str,
    role: DelegationRole,
    tool_allowlist: &[&str],
    memory_scope: DelegationMemoryScopeKind,
    budget_tokens: u64,
    execution_mode: DelegationExecutionMode,
    merge_strategy: DelegationMergeStrategy,
    approval_required: bool,
) -> DelegationProfileDefinition {
    DelegationProfileDefinition {
        profile_id: profile_id.to_owned(),
        display_name: display_name.to_owned(),
        description: description.to_owned(),
        role,
        model_profile: DEFAULT_MODEL_PROFILE.to_owned(),
        tool_allowlist: normalize_allowlist(
            tool_allowlist.iter().map(ToString::to_string).collect(),
        ),
        skill_allowlist: Vec::new(),
        memory_scope,
        budget_tokens,
        max_attempts: DEFAULT_MAX_ATTEMPTS,
        execution_mode,
        merge_contract: DelegationMergeContract { strategy: merge_strategy, approval_required },
        runtime_limits: default_runtime_limits_for_execution_mode(execution_mode),
    }
}

fn default_runtime_limits_for_execution_mode(
    execution_mode: DelegationExecutionMode,
) -> DelegationRuntimeLimits {
    let mut limits = DelegationRuntimeLimits::default();
    if execution_mode == DelegationExecutionMode::Serial {
        limits.max_concurrent_children = 1;
        limits.max_parallel_groups = 1;
    }
    limits
}

pub fn built_in_delegation_catalog() -> DelegationCatalog {
    let profiles = vec![
        profile_definition(
            "research",
            "Research",
            "Collect evidence and return a concise summary with source-aware recommendations.",
            DelegationRole::Research,
            &["palyra.http.fetch"],
            DelegationMemoryScopeKind::ParentSessionAndWorkspace,
            1_800,
            DelegationExecutionMode::Parallel,
            DelegationMergeStrategy::Summarize,
            false,
        ),
        profile_definition(
            "synthesis",
            "Synthesis",
            "Condense existing findings into a decision-ready answer for the parent run.",
            DelegationRole::Synthesis,
            &[],
            DelegationMemoryScopeKind::ParentSession,
            1_600,
            DelegationExecutionMode::Serial,
            DelegationMergeStrategy::Summarize,
            false,
        ),
        profile_definition(
            "review",
            "Review",
            "Inspect evidence or code paths and highlight risks, regressions, and tradeoffs.",
            DelegationRole::Review,
            &["palyra.http.fetch"],
            DelegationMemoryScopeKind::ParentSessionAndWorkspace,
            2_200,
            DelegationExecutionMode::Parallel,
            DelegationMergeStrategy::Triage,
            false,
        ),
        profile_definition(
            "patching",
            "Patching",
            "Prepare patch-oriented output with strong provenance and approval-aware merge rules.",
            DelegationRole::Patching,
            &["palyra.fs.apply_patch", "palyra.http.fetch"],
            DelegationMemoryScopeKind::ParentSessionAndWorkspace,
            2_600,
            DelegationExecutionMode::Serial,
            DelegationMergeStrategy::PatchReview,
            true,
        ),
        profile_definition(
            "triage",
            "Triage",
            "Summarize multiple signals into a prioritized issue list for parent review.",
            DelegationRole::Triage,
            &["palyra.http.fetch"],
            DelegationMemoryScopeKind::ParentSession,
            1_400,
            DelegationExecutionMode::Parallel,
            DelegationMergeStrategy::Triage,
            false,
        ),
    ];
    let templates = vec![
        DelegationTemplateDefinition {
            template_id: "compare_variants".to_owned(),
            display_name: "Compare Variants".to_owned(),
            description: "Run parallel variant checks and merge the output into a side-by-side comparison."
                .to_owned(),
            primary_profile_id: "research".to_owned(),
            recommended_profiles: vec!["research".to_owned(), "synthesis".to_owned()],
            execution_mode: DelegationExecutionMode::Parallel,
            merge_strategy: DelegationMergeStrategy::Compare,
            runtime_limits: Some(DelegationRuntimeLimits {
                max_concurrent_children: 2,
                max_children_per_parent: 8,
                max_parallel_groups: 2,
                child_budget_override: None,
                child_timeout_ms: DEFAULT_CHILD_TIMEOUT_MS,
            }),
            examples: vec![
                "/delegate compare_variants Compare the current branch with the release branch for migration risk."
                    .to_owned(),
            ],
        },
        DelegationTemplateDefinition {
            template_id: "research_then_synthesize".to_owned(),
            display_name: "Research Then Synthesize".to_owned(),
            description: "Collect evidence first, then hand it off for a condensed answer.".to_owned(),
            primary_profile_id: "research".to_owned(),
            recommended_profiles: vec!["research".to_owned(), "synthesis".to_owned()],
            execution_mode: DelegationExecutionMode::Serial,
            merge_strategy: DelegationMergeStrategy::Summarize,
            runtime_limits: Some(DelegationRuntimeLimits {
                max_concurrent_children: 1,
                max_children_per_parent: 6,
                max_parallel_groups: 1,
                child_budget_override: None,
                child_timeout_ms: DEFAULT_CHILD_TIMEOUT_MS,
            }),
            examples: vec![
                "/delegate research_then_synthesize Read the recent daemon changes and summarize deployment impact."
                    .to_owned(),
            ],
        },
        DelegationTemplateDefinition {
            template_id: "review_and_patch".to_owned(),
            display_name: "Review And Patch".to_owned(),
            description: "Audit a change first and keep patch-oriented output approval-aware.".to_owned(),
            primary_profile_id: "patching".to_owned(),
            recommended_profiles: vec!["review".to_owned(), "patching".to_owned()],
            execution_mode: DelegationExecutionMode::Serial,
            merge_strategy: DelegationMergeStrategy::PatchReview,
            runtime_limits: Some(DelegationRuntimeLimits {
                max_concurrent_children: 1,
                max_children_per_parent: 4,
                max_parallel_groups: 1,
                child_budget_override: None,
                child_timeout_ms: DEFAULT_CHILD_TIMEOUT_MS,
            }),
            examples: vec![
                "/delegate review_and_patch Investigate the failing web lint output and propose the minimal fix."
                    .to_owned(),
            ],
        },
        DelegationTemplateDefinition {
            template_id: "multi_source_triage".to_owned(),
            display_name: "Multi-Source Triage".to_owned(),
            description: "Blend transcript, references, and external evidence into a prioritized triage summary."
                .to_owned(),
            primary_profile_id: "triage".to_owned(),
            recommended_profiles: vec!["triage".to_owned(), "research".to_owned()],
            execution_mode: DelegationExecutionMode::Parallel,
            merge_strategy: DelegationMergeStrategy::Triage,
            runtime_limits: Some(DelegationRuntimeLimits {
                max_concurrent_children: 3,
                max_children_per_parent: 10,
                max_parallel_groups: 3,
                child_budget_override: None,
                child_timeout_ms: DEFAULT_CHILD_TIMEOUT_MS,
            }),
            examples: vec![
                "/delegate multi_source_triage Triage the failing workflow signals and summarize the probable root causes."
                    .to_owned(),
            ],
        },
    ];

    DelegationCatalog { profiles, templates }
}

fn normalize_optional_text(value: Option<&str>) -> Option<String> {
    let trimmed = value?.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.to_owned())
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

fn find_profile<'a>(
    catalog: &'a DelegationCatalog,
    profile_id: &str,
) -> Option<&'a DelegationProfileDefinition> {
    catalog.profiles.iter().find(|profile| profile.profile_id == profile_id)
}

fn find_template<'a>(
    catalog: &'a DelegationCatalog,
    template_id: &str,
) -> Option<&'a DelegationTemplateDefinition> {
    catalog.templates.iter().find(|template| template.template_id == template_id)
}

fn ensure_identifier(raw: &str, field: &str) -> Result<String, Status> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(Status::invalid_argument(format!("{field} cannot be empty")));
    }
    if trimmed.len() > 64 {
        return Err(Status::invalid_argument(format!("{field} cannot exceed 64 characters")));
    }
    if trimmed.chars().any(|character| {
        !(character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.'))
    }) {
        return Err(Status::invalid_argument(format!("{field} contains unsupported characters")));
    }
    Ok(trimmed.to_ascii_lowercase())
}

fn ensure_budget_tokens(value: u64, field: &str) -> Result<u64, Status> {
    if value == 0 {
        return Err(Status::invalid_argument(format!("{field} must be greater than zero")));
    }
    if value > MAX_DELEGATION_BUDGET_TOKENS {
        return Err(Status::invalid_argument(format!(
            "{field} exceeds the maximum supported delegation budget"
        )));
    }
    Ok(value)
}

fn ensure_attempts(value: u64, field: &str) -> Result<u64, Status> {
    if value == 0 {
        return Err(Status::invalid_argument(format!("{field} must be greater than zero")));
    }
    if value > MAX_DELEGATION_ATTEMPTS {
        return Err(Status::invalid_argument(format!(
            "{field} exceeds the maximum supported attempt count"
        )));
    }
    Ok(value)
}

fn ensure_count_limit(value: u64, field: &str, maximum: u64) -> Result<u64, Status> {
    if value == 0 {
        return Err(Status::invalid_argument(format!("{field} must be greater than zero")));
    }
    if value > maximum {
        return Err(Status::invalid_argument(format!("{field} exceeds the supported maximum")));
    }
    Ok(value)
}

fn ensure_child_timeout_ms(value: u64, field: &str) -> Result<u64, Status> {
    if value == 0 {
        return Err(Status::invalid_argument(format!("{field} must be greater than zero")));
    }
    if value > MAX_DELEGATION_CHILD_TIMEOUT_MS {
        return Err(Status::invalid_argument(format!(
            "{field} exceeds the maximum supported child timeout"
        )));
    }
    Ok(value)
}

fn ensure_runtime_limits(
    limits: DelegationRuntimeLimits,
    field: &str,
) -> Result<DelegationRuntimeLimits, Status> {
    let child_budget_override = limits
        .child_budget_override
        .map(|value| ensure_budget_tokens(value, &format!("{field}.child_budget_override")))
        .transpose()?;
    Ok(DelegationRuntimeLimits {
        max_concurrent_children: ensure_count_limit(
            limits.max_concurrent_children,
            &format!("{field}.max_concurrent_children"),
            MAX_DELEGATION_CONCURRENT_CHILDREN,
        )?,
        max_children_per_parent: ensure_count_limit(
            limits.max_children_per_parent,
            &format!("{field}.max_children_per_parent"),
            MAX_DELEGATION_CHILDREN_PER_PARENT,
        )?,
        max_parallel_groups: ensure_count_limit(
            limits.max_parallel_groups,
            &format!("{field}.max_parallel_groups"),
            MAX_DELEGATION_PARALLEL_GROUPS,
        )?,
        child_budget_override,
        child_timeout_ms: ensure_child_timeout_ms(
            limits.child_timeout_ms,
            &format!("{field}.child_timeout_ms"),
        )?,
    })
}

fn apply_manifest_runtime_overrides(
    limits: &mut DelegationRuntimeLimits,
    manifest: &DelegationManifestInput,
) -> Result<(), Status> {
    if let Some(value) = manifest.max_concurrent_children {
        limits.max_concurrent_children = ensure_count_limit(
            value,
            "delegation.manifest.max_concurrent_children",
            MAX_DELEGATION_CONCURRENT_CHILDREN,
        )?;
    }
    if let Some(value) = manifest.max_children_per_parent {
        limits.max_children_per_parent = ensure_count_limit(
            value,
            "delegation.manifest.max_children_per_parent",
            MAX_DELEGATION_CHILDREN_PER_PARENT,
        )?;
    }
    if let Some(value) = manifest.max_parallel_groups {
        limits.max_parallel_groups = ensure_count_limit(
            value,
            "delegation.manifest.max_parallel_groups",
            MAX_DELEGATION_PARALLEL_GROUPS,
        )?;
    }
    if let Some(value) = manifest.child_budget_override {
        limits.child_budget_override =
            Some(ensure_budget_tokens(value, "delegation.manifest.child_budget_override")?);
    }
    if let Some(value) = manifest.child_timeout_ms {
        limits.child_timeout_ms =
            ensure_child_timeout_ms(value, "delegation.manifest.child_timeout_ms")?;
    }
    Ok(())
}

fn validate_allowlist_subset(
    field: &str,
    requested: &[String],
    parent_allowlist: &[String],
) -> Result<(), Status> {
    if parent_allowlist.is_empty() {
        return Ok(());
    }
    if let Some(disallowed) = requested
        .iter()
        .find(|candidate| !parent_allowlist.iter().any(|parent| parent == *candidate))
    {
        return Err(Status::invalid_argument(format!(
            "{field} entry '{disallowed}' exceeds the parent allowlist"
        )));
    }
    Ok(())
}

pub fn resolve_delegation_request(
    request: &DelegationRequestInput,
    parent: &DelegationParentContext,
) -> Result<DelegationSnapshot, Status> {
    let catalog = built_in_delegation_catalog();
    if request.profile_id.is_some() && request.template_id.is_some() {
        return Err(Status::invalid_argument(
            "delegation cannot specify both profile_id and template_id",
        ));
    }

    let requested_profile_id = normalize_optional_text(request.profile_id.as_deref())
        .map(|value| value.to_ascii_lowercase());
    let requested_template_id = normalize_optional_text(request.template_id.as_deref())
        .map(|value| value.to_ascii_lowercase());

    let mut base_profile = if let Some(profile_id) = requested_profile_id.as_deref() {
        find_profile(&catalog, profile_id).cloned().ok_or_else(|| {
            Status::not_found(format!("delegation profile not found: {profile_id}"))
        })?
    } else {
        find_profile(&catalog, "research").cloned().expect("default research profile must exist")
    };
    let mut template_id = None;
    if let Some(template_key) = requested_template_id.as_deref() {
        let template = find_template(&catalog, template_key).ok_or_else(|| {
            Status::not_found(format!("delegation template not found: {template_key}"))
        })?;
        base_profile = find_profile(&catalog, template.primary_profile_id.as_str())
            .cloned()
            .ok_or_else(|| {
                Status::internal(format!(
                    "delegation template '{}' references an unknown primary profile",
                    template.template_id
                ))
            })?;
        base_profile.execution_mode = template.execution_mode;
        base_profile.merge_contract.strategy = template.merge_strategy;
        if let Some(runtime_limits) = template.runtime_limits.clone() {
            base_profile.runtime_limits = runtime_limits;
        }
        template_id = Some(template.template_id.clone());
    }

    if let Some(manifest) = request.manifest.as_ref() {
        if let Some(profile_id) = manifest.profile_id.as_deref() {
            let profile_key = ensure_identifier(profile_id, "delegation.manifest.profile_id")?;
            base_profile =
                find_profile(&catalog, profile_key.as_str()).cloned().ok_or_else(|| {
                    Status::not_found(format!(
                        "delegation manifest profile not found: {profile_key}"
                    ))
                })?;
        }
        if let Some(display_name) = normalize_optional_text(manifest.display_name.as_deref()) {
            base_profile.display_name = display_name;
        }
        if let Some(description) = normalize_optional_text(manifest.description.as_deref()) {
            base_profile.description = description;
        }
        if let Some(role) = manifest.role {
            base_profile.role = role;
        }
        if let Some(model_profile) = normalize_optional_text(manifest.model_profile.as_deref()) {
            base_profile.model_profile = model_profile;
        }
        let manifest_tool_allowlist = normalize_allowlist(manifest.tool_allowlist.clone());
        if !manifest_tool_allowlist.is_empty() {
            base_profile.tool_allowlist = manifest_tool_allowlist;
        }
        let manifest_skill_allowlist = normalize_allowlist(manifest.skill_allowlist.clone());
        if !manifest_skill_allowlist.is_empty() {
            base_profile.skill_allowlist = manifest_skill_allowlist;
        }
        if let Some(memory_scope) = manifest.memory_scope {
            base_profile.memory_scope = memory_scope;
        }
        if let Some(budget_tokens) = manifest.budget_tokens {
            base_profile.budget_tokens =
                ensure_budget_tokens(budget_tokens, "delegation.manifest.budget_tokens")?;
        }
        if let Some(max_attempts) = manifest.max_attempts {
            base_profile.max_attempts =
                ensure_attempts(max_attempts, "delegation.manifest.max_attempts")?;
        }
        if let Some(merge_strategy) = manifest.merge_strategy {
            base_profile.merge_contract.strategy = merge_strategy;
        }
        if let Some(approval_required) = manifest.approval_required {
            base_profile.merge_contract.approval_required = approval_required;
        }
        apply_manifest_runtime_overrides(&mut base_profile.runtime_limits, manifest)?;
    }

    let execution_mode = request.execution_mode.unwrap_or(base_profile.execution_mode);
    if execution_mode == DelegationExecutionMode::Serial {
        base_profile.runtime_limits.max_concurrent_children = 1;
        base_profile.runtime_limits.max_parallel_groups = 1;
    }
    let runtime_limits =
        ensure_runtime_limits(base_profile.runtime_limits, "delegation.runtime_limits")?;
    let budget_tokens = ensure_budget_tokens(
        runtime_limits.child_budget_override.unwrap_or(base_profile.budget_tokens),
        "delegation.budget_tokens",
    )?;
    let max_attempts = ensure_attempts(base_profile.max_attempts, "delegation.max_attempts")?;
    if let Some(parent_budget_tokens) = parent.parent_budget_tokens {
        if budget_tokens > parent_budget_tokens {
            return Err(Status::invalid_argument(
                "delegation budget_tokens exceeds the parent budget ceiling",
            ));
        }
    }

    let tool_allowlist = normalize_allowlist(base_profile.tool_allowlist.clone());
    validate_allowlist_subset(
        "delegation.tool_allowlist",
        tool_allowlist.as_slice(),
        parent.parent_tool_allowlist.as_slice(),
    )?;
    let skill_allowlist = normalize_allowlist(base_profile.skill_allowlist.clone());
    validate_allowlist_subset(
        "delegation.skill_allowlist",
        skill_allowlist.as_slice(),
        parent.parent_skill_allowlist.as_slice(),
    )?;

    let group_id = if let Some(group_id) = request.group_id.as_deref() {
        ensure_identifier(group_id, "delegation.group_id")?
    } else if execution_mode == DelegationExecutionMode::Serial {
        format!(
            "serial-{}-{}",
            parent
                .parent_run_id
                .as_deref()
                .and_then(|value| normalize_optional_text(Some(value)))
                .unwrap_or_else(|| "root".to_owned()),
            template_id.as_deref().unwrap_or(base_profile.profile_id.as_str()).replace('.', "-")
        )
    } else {
        format!(
            "parallel-{}",
            parent
                .parent_run_id
                .as_deref()
                .and_then(|value| normalize_optional_text(Some(value)))
                .unwrap_or_else(|| "root".to_owned())
        )
    };

    let model_profile = normalize_optional_text(Some(base_profile.model_profile.as_str()))
        .or(parent.parent_model_profile.clone())
        .unwrap_or_else(|| DEFAULT_MODEL_PROFILE.to_owned());

    Ok(DelegationSnapshot {
        profile_id: base_profile.profile_id,
        display_name: base_profile.display_name,
        description: Some(base_profile.description),
        template_id,
        role: base_profile.role,
        execution_mode,
        group_id,
        model_profile,
        tool_allowlist,
        skill_allowlist,
        memory_scope: base_profile.memory_scope,
        budget_tokens,
        max_attempts,
        merge_contract: base_profile.merge_contract,
        runtime_limits,
        agent_id: parent.agent_id.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::{
        built_in_delegation_catalog, resolve_delegation_request, DelegationExecutionMode,
        DelegationManifestInput, DelegationMemoryScopeKind, DelegationParentContext,
        DelegationRequestInput, DelegationRole,
    };

    fn parent_context() -> DelegationParentContext {
        DelegationParentContext {
            parent_run_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned()),
            agent_id: Some("main".to_owned()),
            parent_model_profile: Some("gpt-4o-mini".to_owned()),
            parent_tool_allowlist: vec!["palyra.http.fetch".to_owned()],
            parent_skill_allowlist: vec!["repo.read".to_owned()],
            parent_budget_tokens: Some(2_400),
        }
    }

    #[test]
    fn built_in_catalog_contains_expected_templates() {
        let catalog = built_in_delegation_catalog();
        assert!(
            catalog.templates.iter().any(|template| template.template_id == "review_and_patch"),
            "template pack should expose the review_and_patch pattern"
        );
        assert!(
            catalog.profiles.iter().any(|profile| profile.profile_id == "research"),
            "catalog should expose a research profile"
        );
    }

    #[test]
    fn resolve_delegation_request_caps_against_parent_context() {
        let error = resolve_delegation_request(
            &DelegationRequestInput {
                manifest: Some(DelegationManifestInput {
                    display_name: Some("Wide patcher".to_owned()),
                    role: Some(DelegationRole::Patching),
                    tool_allowlist: vec!["palyra.fs.apply_patch".to_owned()],
                    budget_tokens: Some(2_000),
                    ..Default::default()
                }),
                ..Default::default()
            },
            &parent_context(),
        )
        .expect_err("wider manifest than parent should fail");
        assert!(
            error
                .message()
                .contains("delegation tool_allowlist entry 'palyra.fs.apply_patch' exceeds")
                || error.message().contains("delegation.tool_allowlist entry"),
            "validation should explain the allowlist conflict"
        );
    }

    #[test]
    fn resolve_delegation_request_accepts_template_override() {
        let snapshot = resolve_delegation_request(
            &DelegationRequestInput {
                template_id: Some("research_then_synthesize".to_owned()),
                execution_mode: Some(DelegationExecutionMode::Serial),
                manifest: Some(DelegationManifestInput {
                    display_name: Some("Focused research".to_owned()),
                    memory_scope: Some(DelegationMemoryScopeKind::ParentSession),
                    budget_tokens: Some(1_200),
                    ..Default::default()
                }),
                ..Default::default()
            },
            &parent_context(),
        )
        .expect("template override should resolve");

        assert_eq!(snapshot.display_name, "Focused research");
        assert_eq!(snapshot.execution_mode, DelegationExecutionMode::Serial);
        assert_eq!(snapshot.memory_scope, DelegationMemoryScopeKind::ParentSession);
        assert_eq!(snapshot.budget_tokens, 1_200);
        assert_eq!(snapshot.template_id.as_deref(), Some("research_then_synthesize"));
    }

    #[test]
    fn resolve_delegation_request_applies_runtime_overrides_and_budget_ceiling() {
        let snapshot = resolve_delegation_request(
            &DelegationRequestInput {
                profile_id: Some("research".to_owned()),
                manifest: Some(DelegationManifestInput {
                    max_concurrent_children: Some(3),
                    max_children_per_parent: Some(9),
                    max_parallel_groups: Some(2),
                    child_budget_override: Some(1_100),
                    child_timeout_ms: Some(45_000),
                    ..Default::default()
                }),
                ..Default::default()
            },
            &parent_context(),
        )
        .expect("safe runtime overrides should resolve");

        assert_eq!(snapshot.budget_tokens, 1_100);
        assert_eq!(snapshot.runtime_limits.max_concurrent_children, 3);
        assert_eq!(snapshot.runtime_limits.max_children_per_parent, 9);
        assert_eq!(snapshot.runtime_limits.max_parallel_groups, 2);
        assert_eq!(snapshot.runtime_limits.child_budget_override, Some(1_100));
        assert_eq!(snapshot.runtime_limits.child_timeout_ms, 45_000);

        let error = resolve_delegation_request(
            &DelegationRequestInput {
                manifest: Some(DelegationManifestInput {
                    child_budget_override: Some(9_999),
                    ..Default::default()
                }),
                ..Default::default()
            },
            &parent_context(),
        )
        .expect_err("child budget override above parent ceiling should fail");
        assert!(error.message().contains("delegation budget_tokens exceeds"));
    }

    #[test]
    fn resolve_delegation_request_forces_serial_runtime_limits() {
        let snapshot = resolve_delegation_request(
            &DelegationRequestInput {
                profile_id: Some("research".to_owned()),
                execution_mode: Some(DelegationExecutionMode::Serial),
                manifest: Some(DelegationManifestInput {
                    max_concurrent_children: Some(4),
                    max_parallel_groups: Some(3),
                    ..Default::default()
                }),
                ..Default::default()
            },
            &parent_context(),
        )
        .expect("serial override should resolve");

        assert_eq!(snapshot.execution_mode, DelegationExecutionMode::Serial);
        assert_eq!(snapshot.runtime_limits.max_concurrent_children, 1);
        assert_eq!(snapshot.runtime_limits.max_parallel_groups, 1);
    }
}
