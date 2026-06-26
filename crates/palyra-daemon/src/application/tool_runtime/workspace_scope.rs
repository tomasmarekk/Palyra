//! Session and run-launch scoping for workspace tool roots.
//!
//! Workspace tools operate on an ordered list of workspace roots. This module
//! derives that list from two dynamic sources layered over the agent's
//! configured roots: the run-launch CLI context (launch cwd, extra roots, exact
//! file grants, and allowlisted path env keys carried in the run's parameter
//! delta) and the session's project focus (the directory the operator is
//! currently working in, resolved from stored focus paths).
//!
//! Every dynamic root is canonicalized and validated before use: launch roots
//! must be existing absolute directories outside the OS deny-list in
//! `protected_launch_workspace_root` and must remain inside configured agent
//! roots, while focus directories must resolve (symlinks included) to a strict
//! descendant of a configured root. The containment decisions made here feed
//! the security checks in `workspace_file` and `workspace_patch`; treat any
//! semantic change as a security change.

use std::{
    collections::BTreeMap,
    fs,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use serde::Deserialize;

use crate::agents::AgentResolutionSource;
use crate::gateway::GatewayRuntimeState;

/// Session-focused directory selected as the preferred workspace root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ActiveWorkspaceRoot {
    /// Canonicalized focus directory.
    pub(crate) root: PathBuf,
    /// Normalized `/`-separated path of [`Self::root`] relative to the agent
    /// workspace root it lives in; never `"."`.
    pub(crate) relative_path: String,
}

#[derive(Debug, Deserialize)]
struct RunLaunchParameterDelta {
    cli_context: Option<RunLaunchCliContext>,
}

#[derive(Debug, Deserialize)]
struct RunLaunchCliContext {
    launch_cwd: Option<String>,
    workspace_roots: Option<Vec<String>>,
    workspace_file_grants: Option<Vec<String>>,
    env: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Default)]
struct RunLaunchWorkspaceRoots {
    launch_cwd: Option<PathBuf>,
    extra_roots: Vec<PathBuf>,
}

/// Launch-context env keys whose values may contribute filesystem roots.
/// Strict allowlist: every other key (including credential-bearing ones) is
/// dropped before its value is ever interpreted as a path.
const RUN_LAUNCH_SAFE_PATH_ENV_KEYS: &[&str] = &["PALYRA_E2E_HOME", "PALYRA_E2E_OS_ROOT"];

/// Resolves the session's active workspace root from its stored project
/// focus paths, if any focus resolves inside `workspace_roots`.
///
/// # Errors
/// Returns an error when the session project-context state cannot be loaded
/// from the runtime.
pub(crate) async fn session_active_workspace_root(
    runtime_state: &Arc<GatewayRuntimeState>,
    session_id: &str,
    workspace_roots: &[PathBuf],
) -> Result<Option<ActiveWorkspaceRoot>, String> {
    let state = runtime_state.session_project_context_state(session_id.to_owned()).await.map_err(
        |status| format!("failed to load session project workspace focus: {}", status.message()),
    )?;
    let Some(state) = state else {
        return Ok(None);
    };
    Ok(active_workspace_root_from_focus_paths(workspace_roots, state.focus_paths.as_slice()))
}

/// Merges validated run-launch roots (launch cwd first, then explicit extra
/// roots) ahead of `workspace_roots`, dropping duplicates.
pub(crate) async fn workspace_roots_with_run_launch_context(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    workspace_roots: &[PathBuf],
) -> Vec<PathBuf> {
    let launch_roots = run_launch_context_workspace_roots(runtime_state, run_id).await;
    merge_launch_workspace_roots(workspace_roots, launch_roots)
}

/// [`workspace_roots_with_run_launch_context`] variant that also receives how
/// the agent was resolved; the resolution source is currently ignored.
pub(crate) async fn workspace_roots_with_run_launch_context_for_agent_source(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    workspace_roots: &[PathBuf],
    _source: AgentResolutionSource,
) -> Vec<PathBuf> {
    let launch_roots = run_launch_context_workspace_roots(runtime_state, run_id).await;
    merge_launch_workspace_roots(workspace_roots, launch_roots)
}

/// Extracts allowlisted path-valued env entries from the run's launch
/// context, validated and canonicalized like launch workspace roots.
pub(crate) async fn run_launch_context_path_env(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
) -> BTreeMap<String, PathBuf> {
    let Some(parameter_delta_json) = run_launch_parameter_delta_json(runtime_state, run_id).await
    else {
        return BTreeMap::new();
    };
    let Ok(parameter_delta) =
        serde_json::from_str::<RunLaunchParameterDelta>(parameter_delta_json.as_str())
    else {
        return BTreeMap::new();
    };
    parameter_delta.cli_context.map(launch_path_env_from_context).unwrap_or_default()
}

/// Extracts exact read-file grants from the run's launch context.
pub(crate) async fn run_launch_context_read_file_grants(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    workspace_roots: &[PathBuf],
) -> Vec<PathBuf> {
    let Some(parameter_delta_json) = run_launch_parameter_delta_json(runtime_state, run_id).await
    else {
        return Vec::new();
    };
    let Ok(parameter_delta) =
        serde_json::from_str::<RunLaunchParameterDelta>(parameter_delta_json.as_str())
    else {
        return Vec::new();
    };
    let grants = parameter_delta
        .cli_context
        .map(launch_workspace_file_grants_from_context)
        .unwrap_or_default();
    filter_launch_file_grants_by_workspace_roots(grants, workspace_roots)
}

/// Returns the launch-context root that represents generic `/workspace`
/// process execution for this run, if the run supplied one.
pub(crate) async fn run_launch_context_primary_workspace_root(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    workspace_roots: &[PathBuf],
) -> Option<PathBuf> {
    let launch_roots = run_launch_context_workspace_roots(runtime_state, run_id).await;
    let launch_roots = filter_launch_workspace_roots(launch_roots, workspace_roots);
    launch_roots.launch_cwd.or_else(|| launch_roots.extra_roots.into_iter().next())
}

async fn run_launch_context_workspace_roots(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
) -> RunLaunchWorkspaceRoots {
    // Launch context is optional and best-effort: a missing run, missing
    // parameter delta, or unparseable JSON simply contributes no extra roots.
    let Some(parameter_delta_json) = run_launch_parameter_delta_json(runtime_state, run_id).await
    else {
        return RunLaunchWorkspaceRoots::default();
    };
    let Ok(parameter_delta) =
        serde_json::from_str::<RunLaunchParameterDelta>(parameter_delta_json.as_str())
    else {
        return RunLaunchWorkspaceRoots::default();
    };
    parameter_delta.cli_context.map(launch_workspace_roots_from_context).unwrap_or_default()
}

async fn run_launch_parameter_delta_json(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
) -> Option<String> {
    if let Some(parameter_delta_json) = runtime_state.cached_run_parameter_delta_json(run_id) {
        return Some(parameter_delta_json);
    }
    runtime_state
        .orchestrator_run_status_snapshot(run_id.to_owned())
        .await
        .ok()
        .flatten()?
        .parameter_delta_json
}

fn launch_path_env_from_context(context: RunLaunchCliContext) -> BTreeMap<String, PathBuf> {
    let mut env = BTreeMap::new();
    for (key, value) in context.env.unwrap_or_default() {
        if !RUN_LAUNCH_SAFE_PATH_ENV_KEYS.iter().any(|allowed| *allowed == key) {
            continue;
        }
        let Some(root) = canonical_launch_workspace_root(value.as_str()) else {
            continue;
        };
        env.insert(key, root);
    }
    env
}

fn launch_workspace_roots_from_context(context: RunLaunchCliContext) -> RunLaunchWorkspaceRoots {
    let mut roots = RunLaunchWorkspaceRoots::default();
    for raw_root in context.workspace_roots.unwrap_or_default() {
        push_launch_workspace_root(&mut roots.extra_roots, raw_root.as_str());
    }
    roots.launch_cwd =
        context.launch_cwd.and_then(|raw_cwd| canonical_launch_workspace_root(raw_cwd.as_str()));
    roots
}

fn launch_workspace_file_grants_from_context(context: RunLaunchCliContext) -> Vec<PathBuf> {
    let mut grants = Vec::new();
    for raw_file in context.workspace_file_grants.unwrap_or_default() {
        push_launch_workspace_file_grant(&mut grants, raw_file.as_str());
    }
    grants
}

fn push_launch_workspace_root(roots: &mut Vec<PathBuf>, raw_root: &str) {
    let Some(root) = canonical_launch_workspace_root(raw_root) else {
        return;
    };
    if !roots.iter().any(|existing| same_workspace_root(existing.as_path(), root.as_path())) {
        roots.push(root);
    }
}

fn push_launch_workspace_file_grant(grants: &mut Vec<PathBuf>, raw_file: &str) {
    let Some(file) = canonical_launch_workspace_file_grant(raw_file) else {
        return;
    };
    if !grants.iter().any(|existing| same_workspace_root(existing.as_path(), file.as_path())) {
        grants.push(file);
    }
}

/// Validates one raw launch-context path: control-character free, absolute,
/// canonicalizable to an existing directory, and not a protected OS location.
///
/// Launch roots are advisory, so invalid entries are skipped (`None`) rather
/// than failing the run.
fn canonical_launch_workspace_root(raw_root: &str) -> Option<PathBuf> {
    let raw_root = raw_root.trim();
    if raw_root.is_empty() || raw_root.chars().any(char::is_control) {
        return None;
    }
    let requested = Path::new(raw_root);
    if !requested.is_absolute() {
        return None;
    }
    let Ok(canonical) = fs::canonicalize(requested) else {
        return None;
    };
    let Ok(metadata) = fs::metadata(canonical.as_path()) else {
        return None;
    };
    if !metadata.is_dir() || protected_launch_workspace_root(canonical.as_path()) {
        return None;
    }
    Some(canonical)
}

/// Validates one raw exact file grant from launch context.
fn canonical_launch_workspace_file_grant(raw_file: &str) -> Option<PathBuf> {
    let raw_file = raw_file.trim();
    if raw_file.is_empty() || raw_file.chars().any(char::is_control) {
        return None;
    }
    let requested = Path::new(raw_file);
    if !requested.is_absolute() {
        return None;
    }
    let Ok(canonical) = fs::canonicalize(requested) else {
        return None;
    };
    let Ok(metadata) = fs::metadata(canonical.as_path()) else {
        return None;
    };
    if !metadata.is_file() {
        return None;
    }
    let parent = canonical.parent()?;
    if protected_launch_workspace_root(parent) {
        return None;
    }
    Some(canonical)
}

/// Orders roots as launch cwd, then explicit launch roots, then agent roots,
/// deduplicated; root index 0 is the default target for relative paths, so
/// this precedence is pinned by tests.
fn merge_launch_workspace_roots(
    workspace_roots: &[PathBuf],
    launch_roots: RunLaunchWorkspaceRoots,
) -> Vec<PathBuf> {
    let launch_roots = filter_launch_workspace_roots(launch_roots, workspace_roots);
    if launch_roots.extra_roots.is_empty() && launch_roots.launch_cwd.is_none() {
        return workspace_roots.to_vec();
    }
    let mut merged: Vec<PathBuf> = Vec::with_capacity(
        workspace_roots.len().saturating_add(launch_roots.extra_roots.len() + 1),
    );
    if let Some(launch_cwd) = launch_roots.launch_cwd {
        push_unique_workspace_root(&mut merged, launch_cwd);
    }
    push_unique_workspace_roots(&mut merged, launch_roots.extra_roots);
    push_unique_workspace_roots(&mut merged, workspace_roots.iter().cloned());
    merged
}

fn filter_launch_workspace_roots(
    launch_roots: RunLaunchWorkspaceRoots,
    workspace_roots: &[PathBuf],
) -> RunLaunchWorkspaceRoots {
    let canonical_workspace_roots = canonicalize_workspace_roots(workspace_roots);
    if canonical_workspace_roots.is_empty() {
        return RunLaunchWorkspaceRoots::default();
    }
    RunLaunchWorkspaceRoots {
        launch_cwd: launch_roots
            .launch_cwd
            .filter(|root| launch_path_is_within_workspace_roots(root, &canonical_workspace_roots)),
        extra_roots: launch_roots
            .extra_roots
            .into_iter()
            .filter(|root| launch_path_is_within_workspace_roots(root, &canonical_workspace_roots))
            .collect(),
    }
}

fn filter_launch_file_grants_by_workspace_roots(
    grants: Vec<PathBuf>,
    workspace_roots: &[PathBuf],
) -> Vec<PathBuf> {
    let canonical_workspace_roots = canonicalize_workspace_roots(workspace_roots);
    if canonical_workspace_roots.is_empty() {
        return Vec::new();
    }
    grants
        .into_iter()
        .filter(|grant| launch_path_is_within_workspace_roots(grant, &canonical_workspace_roots))
        .collect()
}

fn launch_path_is_within_workspace_roots(
    path: &Path,
    canonical_workspace_roots: &[PathBuf],
) -> bool {
    let Ok(canonical_path) = fs::canonicalize(path) else {
        return false;
    };
    canonical_workspace_roots
        .iter()
        .any(|root| canonical_path == *root || canonical_path.starts_with(root))
}

fn push_unique_workspace_roots(
    roots: &mut Vec<PathBuf>,
    candidates: impl IntoIterator<Item = PathBuf>,
) {
    for candidate in candidates {
        push_unique_workspace_root(roots, candidate);
    }
}

fn push_unique_workspace_root(roots: &mut Vec<PathBuf>, candidate: PathBuf) {
    if roots.iter().any(|existing| same_workspace_root(existing.as_path(), candidate.as_path())) {
        return;
    }
    roots.push(candidate);
}

/// Root equality for deduplication: canonicalization makes symlinked
/// duplicates equal; on Windows a case-insensitive, separator-normalized
/// comparison additionally matches aliases of roots that cannot be
/// canonicalized (e.g. not-yet-created directories).
fn same_workspace_root(left: &Path, right: &Path) -> bool {
    let left = fs::canonicalize(left).unwrap_or_else(|_| left.to_path_buf());
    let right = fs::canonicalize(right).unwrap_or_else(|_| right.to_path_buf());
    if left == right {
        return true;
    }
    #[cfg(windows)]
    {
        let left = left.to_string_lossy().replace('\\', "/").to_ascii_lowercase();
        let right = right.to_string_lossy().replace('\\', "/").to_ascii_lowercase();
        left == right
    }
    #[cfg(not(windows))]
    {
        false
    }
}

/// Deny-list of OS locations that may never become workspace roots, even when
/// a launch context asks for them: drive/filesystem roots and core system
/// directories. Last line of defense before a launch-supplied path would
/// grant workspace-tool access.
fn protected_launch_workspace_root(path: &Path) -> bool {
    #[cfg(windows)]
    {
        let normalized = path.to_string_lossy().replace('\\', "/").to_ascii_lowercase();
        normalized.ends_with(":/")
            || normalized.contains(":/windows")
            || normalized.contains(":/program files")
            || normalized.contains(":/program files (x86)")
            || normalized.contains(":/system volume information")
    }
    #[cfg(not(windows))]
    {
        let normalized = path.to_string_lossy().replace('\\', "/");
        if normalized == "/" {
            return true;
        }
        for prefix in ["/etc", "/bin", "/sbin", "/usr", "/lib", "/lib64", "/System", "/Library"] {
            if normalized == prefix || normalized.starts_with(format!("{prefix}/").as_str()) {
                return true;
            }
        }
        false
    }
}

/// Picks the first focus path that resolves to an existing directory strictly
/// inside one of `workspace_roots`.
///
/// The candidate (or its nearest existing ancestor, for file-like focus
/// paths) is canonicalized before the containment check, so a focus path that
/// traverses a symlink out of the workspace is rejected rather than followed.
pub(crate) fn active_workspace_root_from_focus_paths(
    workspace_roots: &[PathBuf],
    focus_paths: &[String],
) -> Option<ActiveWorkspaceRoot> {
    let canonical_roots = canonicalize_workspace_roots(workspace_roots);
    if canonical_roots.is_empty() {
        return None;
    }

    for focus_path in focus_paths {
        let Some(focus_path) = normalize_relative_workspace_path(focus_path) else {
            continue;
        };
        if focus_path == "." {
            continue;
        }
        if workspace_focus_path_is_runtime_internal(focus_path.as_str()) {
            continue;
        }
        for root in &canonical_roots {
            let candidate = root.join(focus_path.as_str());
            let Some(directory) = nearest_existing_directory(candidate.as_path(), root) else {
                continue;
            };
            let Ok(directory) = fs::canonicalize(directory) else {
                continue;
            };
            // Containment is checked on the canonicalized directory so a
            // symlinked focus cannot escape the root; the root itself is not
            // a narrowing focus, so it is skipped.
            if directory == *root || !directory.starts_with(root) {
                continue;
            }
            let relative_path = directory
                .strip_prefix(root)
                .ok()
                .and_then(|relative| normalize_relative_workspace_path(&relative.to_string_lossy()))
                .unwrap_or_else(|| ".".to_owned());
            if relative_path == "." {
                continue;
            }
            return Some(ActiveWorkspaceRoot { root: directory, relative_path });
        }
    }
    None
}

/// Returns true when `path` is already expressed relative to the agent root
/// and points at or below the active focus directory.
pub(crate) fn relative_path_already_targets_active_root(
    path: &str,
    active: &ActiveWorkspaceRoot,
) -> bool {
    let Some(path) = normalize_relative_workspace_path(path) else {
        return false;
    };
    path == active.relative_path || path.starts_with(format!("{}/", active.relative_path).as_str())
}

/// Heuristic deciding whether a bare relative path should be re-rooted under
/// the active focus directory.
///
/// A path qualifies only when its parent directory already exists inside the
/// (canonicalized) active root. Paths that already target the focus keep
/// their original meaning, and top-level workspace paths whose parent exists
/// only at the outer root are never silently nested under the focus.
pub(crate) fn relative_path_should_use_active_root(
    path: &str,
    active: &ActiveWorkspaceRoot,
) -> bool {
    let Some(path) = normalize_relative_workspace_path(path) else {
        return false;
    };
    if path == "." || relative_path_already_targets_active_root(path.as_str(), active) {
        return false;
    }

    let parsed = Path::new(path.as_str());
    let parent = parsed.parent().filter(|path| !path.as_os_str().is_empty());
    let candidate_parent =
        parent.map_or_else(|| active.root.clone(), |parent| active.root.join(parent));
    let Ok(canonical_active_root) = fs::canonicalize(active.root.as_path()) else {
        return false;
    };
    let Ok(canonical_parent) = fs::canonicalize(candidate_parent.as_path()) else {
        return false;
    };
    canonical_parent.is_dir() && canonical_parent.starts_with(canonical_active_root.as_path())
}

/// Returns true when an explicit `workspace_root` override refers to the
/// active focus directory itself.
///
/// Accepted spellings: the canonicalized absolute path of the focus or its
/// full root-relative path.
pub(crate) fn workspace_root_override_targets_active_root(
    workspace_root: &str,
    active: &ActiveWorkspaceRoot,
) -> bool {
    let workspace_root = workspace_root.trim();
    if workspace_root.is_empty() {
        return false;
    }

    let requested = Path::new(workspace_root);
    if requested.is_absolute() {
        return fs::canonicalize(requested).is_ok_and(|candidate| candidate == active.root);
    }

    let Some(normalized) = normalize_relative_workspace_path(workspace_root) else {
        return false;
    };
    if normalized == "." {
        return false;
    }
    normalized == active.relative_path
}

pub(crate) fn workspace_focus_path_is_runtime_internal(path: &str) -> bool {
    let Some(normalized) = normalize_relative_workspace_path(path) else {
        return false;
    };
    normalized.split('/').any(workspace_focus_segment_is_runtime_internal)
}

fn workspace_focus_segment_is_runtime_internal(segment: &str) -> bool {
    let normalized = segment.to_ascii_lowercase();
    matches!(
        normalized.as_str(),
        ".pnpm" | ".venv" | ".yarn" | "__pycache__" | "node_modules" | "site-packages" | "venv"
    )
}

fn canonicalize_workspace_roots(workspace_roots: &[PathBuf]) -> Vec<PathBuf> {
    workspace_roots
        .iter()
        .filter_map(|root| fs::canonicalize(root).ok().filter(|path| path.is_dir()))
        .collect()
}

/// Walks up from `candidate` to the closest existing directory (a file maps
/// to its parent), giving up rather than climbing past `workspace_root`.
fn nearest_existing_directory(candidate: &Path, workspace_root: &Path) -> Option<PathBuf> {
    let mut cursor = candidate.to_path_buf();
    loop {
        if cursor.exists() {
            if cursor.is_dir() {
                return Some(cursor);
            }
            return cursor.parent().map(Path::to_path_buf);
        }
        if cursor == workspace_root || !cursor.pop() {
            return None;
        }
    }
}

/// Normalizes a workspace-relative path to `/`-separated form (or `"."`),
/// stripping the `workspace/` alias prefix models commonly emit.
///
/// Returns `None` for absolute paths and for any path containing `..`, a root,
/// or a drive prefix, so callers can join the result onto a workspace root
/// without re-checking traversal.
fn normalize_relative_workspace_path(path: &str) -> Option<String> {
    let normalized = path.trim().replace('\\', "/");
    let without_workspace_alias = normalized
        .strip_prefix("/workspace/")
        .or_else(|| normalized.strip_prefix("workspace/"))
        .unwrap_or(normalized.as_str());
    let trimmed = without_workspace_alias.trim_start_matches("./").trim_matches('/');
    if trimmed.is_empty() || trimmed == "." {
        return Some(".".to_owned());
    }

    let parsed = Path::new(trimmed);
    if parsed.is_absolute() {
        return None;
    }
    let mut components = Vec::new();
    for component in parsed.components() {
        match component {
            Component::Normal(value) => components.push(value.to_string_lossy().into_owned()),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }
    if components.is_empty() {
        Some(".".to_owned())
    } else {
        Some(components.join("/"))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        active_workspace_root_from_focus_paths, canonical_launch_workspace_root,
        filter_launch_file_grants_by_workspace_roots, launch_path_env_from_context,
        launch_workspace_file_grants_from_context, launch_workspace_roots_from_context,
        merge_launch_workspace_roots, relative_path_already_targets_active_root,
        relative_path_should_use_active_root, same_workspace_root,
        workspace_focus_path_is_runtime_internal, workspace_root_override_targets_active_root,
        ActiveWorkspaceRoot, RunLaunchCliContext,
    };
    use std::{collections::BTreeMap, fs};

    #[test]
    fn active_workspace_root_uses_existing_session_focus_directory() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let project = tempdir.path().join("notes-api");
        fs::create_dir_all(project.as_path()).expect("project directory should exist");

        let active = active_workspace_root_from_focus_paths(
            &[tempdir.path().to_path_buf()],
            &["notes-api".to_owned()],
        )
        .expect("active workspace root should resolve");

        assert_eq!(active.root, fs::canonicalize(project).expect("project should canonicalize"));
        assert_eq!(active.relative_path, "notes-api");
        assert!(relative_path_already_targets_active_root("notes-api/package.json", &active));
        assert!(!relative_path_already_targets_active_root("package.json", &active));
    }

    #[test]
    fn active_workspace_root_only_handles_paths_with_existing_active_parent() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let workspace = tempdir.path().join("workspace");
        let reports = workspace.join("reports");
        let scoped_parent = reports.join("daily");
        let top_level_fixture = workspace.join("audit-fixture");
        fs::create_dir_all(scoped_parent.as_path()).expect("scoped parent should exist");
        fs::create_dir_all(top_level_fixture.as_path()).expect("top-level fixture should exist");
        let active = ActiveWorkspaceRoot {
            root: fs::canonicalize(reports.as_path()).expect("reports should canonicalize"),
            relative_path: "reports".to_owned(),
        };

        assert!(relative_path_should_use_active_root("summary.md", &active));
        assert!(relative_path_should_use_active_root("daily/report.md", &active));
        assert!(!relative_path_should_use_active_root("audit-fixture/alpha.txt", &active));
        assert!(!relative_path_should_use_active_root("reports/journal-replay.md", &active));
    }

    #[cfg(unix)]
    #[test]
    fn active_workspace_root_rejects_symlink_focus_outside_workspace() {
        use std::os::unix::fs::symlink;

        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let workspace = tempdir.path().join("workspace");
        let outside = tempdir.path().join("outside");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should exist");
        fs::create_dir_all(outside.as_path()).expect("outside directory should exist");
        symlink(outside.as_path(), workspace.join("link").as_path())
            .expect("symlink should be created");

        let active =
            active_workspace_root_from_focus_paths(&[workspace], &["link/secret.txt".to_owned()]);

        assert_eq!(active, None);
    }

    #[test]
    fn active_workspace_root_uses_nearest_existing_parent_for_file_focus() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let project = tempdir.path().join("routine-workspace");
        fs::create_dir_all(project.as_path()).expect("project directory should exist");

        let active = active_workspace_root_from_focus_paths(
            &[tempdir.path().to_path_buf()],
            &["routine-workspace/reports/cron-edit.log".to_owned()],
        )
        .expect("active workspace root should resolve to nearest existing parent");

        assert_eq!(active.root, fs::canonicalize(project).expect("project should canonicalize"));
        assert_eq!(active.relative_path, "routine-workspace");
    }

    #[test]
    fn active_workspace_root_ignores_runtime_internal_focus_paths() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let package = tempdir.path().join("repo").join("node_modules").join("vitest");
        fs::create_dir_all(package.as_path()).expect("dependency package should exist");

        let active = active_workspace_root_from_focus_paths(
            &[tempdir.path().to_path_buf()],
            &["repo/node_modules/vitest".to_owned()],
        );

        assert_eq!(active, None);
        assert!(workspace_focus_path_is_runtime_internal("repo/node_modules/vitest"));
        assert!(!workspace_focus_path_is_runtime_internal("repo/src"));
    }

    #[test]
    fn workspace_root_override_accepts_only_exact_active_root_paths() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let active_dir = tempdir.path().join("agent-workspaces").join("harness-smoke-20260525");
        fs::create_dir_all(active_dir.as_path()).expect("active directory should exist");
        let active = ActiveWorkspaceRoot {
            root: fs::canonicalize(active_dir.as_path()).expect("active dir should canonicalize"),
            relative_path: "agent-workspaces/harness-smoke-20260525".to_owned(),
        };

        assert!(workspace_root_override_targets_active_root(
            "agent-workspaces/harness-smoke-20260525",
            &active
        ));
        assert!(workspace_root_override_targets_active_root(
            active.root.to_string_lossy().as_ref(),
            &active
        ));
        assert!(
            !workspace_root_override_targets_active_root("harness-smoke-20260525", &active),
            "bare basenames are ambiguous when another workspace subdirectory has the same name"
        );
        assert!(!workspace_root_override_targets_active_root(
            "other/harness-smoke-20260525",
            &active
        ));
        assert!(!workspace_root_override_targets_active_root(".", &active));
    }

    #[test]
    fn launch_cwd_workspace_root_requires_existing_absolute_directory() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let canonical = fs::canonicalize(tempdir.path()).expect("tempdir should canonicalize");

        assert_eq!(
            canonical_launch_workspace_root(tempdir.path().to_string_lossy().as_ref()),
            Some(canonical)
        );
        assert_eq!(canonical_launch_workspace_root("relative/project"), None);
        assert_eq!(canonical_launch_workspace_root("bad\u{0000}path"), None);
        assert_eq!(
            canonical_launch_workspace_root(
                tempdir.path().join("missing").to_string_lossy().as_ref()
            ),
            None
        );
    }

    #[test]
    fn default_agent_launch_cwd_precedes_agent_roots_without_duplicates() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let default_root = tempdir.path().join("default");
        let launch_root = tempdir.path().join("launch");
        fs::create_dir_all(default_root.as_path()).expect("default root should exist");
        fs::create_dir_all(launch_root.as_path()).expect("launch root should exist");
        let canonical_launch =
            fs::canonicalize(launch_root.as_path()).expect("launch root should canonicalize");

        let launch_roots = launch_workspace_roots_from_context(RunLaunchCliContext {
            launch_cwd: Some(canonical_launch.to_string_lossy().into_owned()),
            workspace_roots: None,
            workspace_file_grants: None,
            env: None,
        });
        let roots = merge_launch_workspace_roots(
            &[default_root.clone(), launch_root.clone()],
            launch_roots,
        );

        assert_eq!(roots.len(), 2);
        assert_eq!(roots.first(), Some(&canonical_launch));
        assert_eq!(roots.get(1), Some(&default_root));
        assert!(same_workspace_root(roots[0].as_path(), launch_root.as_path()));
    }

    #[test]
    fn launch_cwd_precedes_extra_roots_and_agent_roots() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let default_root = tempdir.path().join("default");
        let explicit_root = default_root.join("explicit");
        let launch_cwd = default_root.join("cwd");
        fs::create_dir_all(explicit_root.as_path()).expect("explicit root should exist");
        fs::create_dir_all(launch_cwd.as_path()).expect("launch cwd should exist");
        fs::create_dir_all(default_root.as_path()).expect("default root should exist");
        let default_root =
            fs::canonicalize(default_root.as_path()).expect("default root should canonicalize");
        let explicit_root =
            fs::canonicalize(explicit_root.as_path()).expect("explicit root should canonicalize");
        let launch_cwd =
            fs::canonicalize(launch_cwd.as_path()).expect("launch cwd should canonicalize");

        let launch_roots = launch_workspace_roots_from_context(RunLaunchCliContext {
            launch_cwd: Some(launch_cwd.to_string_lossy().into_owned()),
            workspace_roots: Some(vec![
                explicit_root.to_string_lossy().into_owned(),
                launch_cwd.to_string_lossy().into_owned(),
            ]),
            workspace_file_grants: None,
            env: None,
        });
        let roots = merge_launch_workspace_roots(std::slice::from_ref(&default_root), launch_roots);

        assert_eq!(roots.len(), 3);
        assert_eq!(roots.first(), Some(&launch_cwd));
        assert_eq!(roots.get(1), Some(&explicit_root));
        assert_eq!(roots.get(2), Some(&default_root));
    }

    #[test]
    fn session_bound_launch_cwd_precedes_agent_roots_without_extra_roots() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let bound_root = tempdir.path().join("bound-agent");
        let launch_cwd = bound_root.join("cwd");
        fs::create_dir_all(bound_root.as_path()).expect("bound root should exist");
        fs::create_dir_all(launch_cwd.as_path()).expect("launch cwd should exist");
        let bound_root = fs::canonicalize(bound_root.as_path()).expect("bound root canonical");
        let launch_cwd = fs::canonicalize(launch_cwd.as_path()).expect("launch cwd canonical");

        let launch_roots = launch_workspace_roots_from_context(RunLaunchCliContext {
            launch_cwd: Some(launch_cwd.to_string_lossy().into_owned()),
            workspace_roots: None,
            workspace_file_grants: None,
            env: None,
        });
        let roots = merge_launch_workspace_roots(std::slice::from_ref(&bound_root), launch_roots);

        assert_eq!(roots.len(), 2);
        assert_eq!(roots.first(), Some(&launch_cwd));
        assert_eq!(roots.get(1), Some(&bound_root));
    }

    #[test]
    fn launch_roots_outside_agent_workspace_roots_are_ignored() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let agent_root = tempdir.path().join("agent");
        let outside_root = tempdir.path().join("outside");
        fs::create_dir_all(agent_root.as_path()).expect("agent root should exist");
        fs::create_dir_all(outside_root.as_path()).expect("outside root should exist");
        let agent_root = fs::canonicalize(agent_root).expect("agent root should canonicalize");
        let outside_root =
            fs::canonicalize(outside_root).expect("outside root should canonicalize");

        let launch_roots = launch_workspace_roots_from_context(RunLaunchCliContext {
            launch_cwd: Some(outside_root.to_string_lossy().into_owned()),
            workspace_roots: Some(vec![outside_root.to_string_lossy().into_owned()]),
            workspace_file_grants: None,
            env: None,
        });
        let roots = merge_launch_workspace_roots(std::slice::from_ref(&agent_root), launch_roots);

        assert_eq!(roots, vec![agent_root]);
    }

    #[test]
    fn launch_file_grants_accept_existing_absolute_files_only() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let watched = tempdir.path().join("watched.md");
        fs::write(watched.as_path(), "ready\n").expect("watched file should exist");
        let watched =
            fs::canonicalize(watched.as_path()).expect("watched file should canonicalize");

        let grants = launch_workspace_file_grants_from_context(RunLaunchCliContext {
            launch_cwd: None,
            workspace_roots: None,
            workspace_file_grants: Some(vec![
                watched.to_string_lossy().into_owned(),
                watched.to_string_lossy().into_owned(),
                tempdir.path().to_string_lossy().into_owned(),
                tempdir.path().join("missing.md").to_string_lossy().into_owned(),
                "relative.md".to_owned(),
            ]),
            env: None,
        });

        assert_eq!(grants, vec![watched]);
    }

    #[test]
    fn launch_file_grants_are_limited_to_agent_workspace_roots() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let agent_root = tempdir.path().join("agent");
        let outside_root = tempdir.path().join("outside");
        fs::create_dir_all(agent_root.as_path()).expect("agent root should exist");
        fs::create_dir_all(outside_root.as_path()).expect("outside root should exist");
        let allowed = agent_root.join("allowed.md");
        let outside = outside_root.join("secret.txt");
        fs::write(allowed.as_path(), "ok\n").expect("allowed file should exist");
        fs::write(outside.as_path(), "secret\n").expect("outside file should exist");
        let allowed = fs::canonicalize(allowed).expect("allowed file should canonicalize");
        let outside = fs::canonicalize(outside).expect("outside file should canonicalize");
        let agent_root = fs::canonicalize(agent_root).expect("agent root should canonicalize");

        let grants = filter_launch_file_grants_by_workspace_roots(
            vec![outside, allowed.clone()],
            std::slice::from_ref(&agent_root),
        );

        assert_eq!(grants, vec![allowed]);
    }

    #[test]
    fn launch_path_env_accepts_safe_existing_user_roots_only() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let home = tempdir.path().join("home");
        let os_root = tempdir.path().join("os-root");
        fs::create_dir_all(home.as_path()).expect("home root should exist");
        fs::create_dir_all(os_root.as_path()).expect("OS root should exist");
        let env = launch_path_env_from_context(RunLaunchCliContext {
            launch_cwd: None,
            workspace_roots: None,
            workspace_file_grants: None,
            env: Some(BTreeMap::from([
                ("PALYRA_E2E_HOME".to_owned(), home.to_string_lossy().into_owned()),
                ("PALYRA_E2E_OS_ROOT".to_owned(), os_root.to_string_lossy().into_owned()),
                ("PALYRA_ADMIN_TOKEN".to_owned(), "secret".to_owned()),
                (
                    "PALYRA_E2E_MISSING".to_owned(),
                    tempdir.path().join("missing").to_string_lossy().into_owned(),
                ),
            ])),
        });

        assert_eq!(env.get("PALYRA_E2E_HOME"), Some(&fs::canonicalize(home).unwrap()));
        assert_eq!(env.get("PALYRA_E2E_OS_ROOT"), Some(&fs::canonicalize(os_root).unwrap()));
        assert!(!env.contains_key("PALYRA_ADMIN_TOKEN"));
        assert!(!env.contains_key("PALYRA_E2E_MISSING"));
    }
}
