use std::{
    collections::BTreeMap,
    fs,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use serde::Deserialize;

use crate::agents::AgentResolutionSource;
use crate::gateway::GatewayRuntimeState;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ActiveWorkspaceRoot {
    pub(crate) root: PathBuf,
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
    env: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Default)]
struct RunLaunchWorkspaceRoots {
    prompt_roots: Vec<PathBuf>,
    launch_cwd: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LaunchCwdPrecedence {
    BeforeAgentRoots,
    AfterAgentRoots,
}

const RUN_LAUNCH_SAFE_PATH_ENV_KEYS: &[&str] = &["PALYRA_E2E_HOME", "PALYRA_E2E_OS_ROOT"];

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

pub(crate) async fn workspace_roots_with_run_launch_context(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    workspace_roots: &[PathBuf],
) -> Vec<PathBuf> {
    let launch_roots = run_launch_context_workspace_roots(runtime_state, run_id).await;
    merge_launch_workspace_roots(
        workspace_roots,
        launch_roots,
        LaunchCwdPrecedence::BeforeAgentRoots,
    )
}

pub(crate) async fn workspace_roots_with_run_launch_context_for_agent_source(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    workspace_roots: &[PathBuf],
    source: AgentResolutionSource,
) -> Vec<PathBuf> {
    let launch_roots = run_launch_context_workspace_roots(runtime_state, run_id).await;
    let launch_cwd_precedence = match source {
        AgentResolutionSource::SessionBinding => LaunchCwdPrecedence::AfterAgentRoots,
        AgentResolutionSource::Default | AgentResolutionSource::Fallback => {
            LaunchCwdPrecedence::BeforeAgentRoots
        }
    };
    merge_launch_workspace_roots(workspace_roots, launch_roots, launch_cwd_precedence)
}

pub(crate) async fn run_launch_context_path_env(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
) -> BTreeMap<String, PathBuf> {
    let Some(run) =
        runtime_state.orchestrator_run_status_snapshot(run_id.to_owned()).await.ok().flatten()
    else {
        return BTreeMap::new();
    };
    let Some(parameter_delta_json) = run.parameter_delta_json.as_deref() else {
        return BTreeMap::new();
    };
    let Ok(parameter_delta) = serde_json::from_str::<RunLaunchParameterDelta>(parameter_delta_json)
    else {
        return BTreeMap::new();
    };
    parameter_delta.cli_context.map(launch_path_env_from_context).unwrap_or_default()
}

async fn run_launch_context_workspace_roots(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
) -> RunLaunchWorkspaceRoots {
    let Some(run) =
        runtime_state.orchestrator_run_status_snapshot(run_id.to_owned()).await.ok().flatten()
    else {
        return RunLaunchWorkspaceRoots::default();
    };
    let Some(parameter_delta_json) = run.parameter_delta_json.as_deref() else {
        return RunLaunchWorkspaceRoots::default();
    };
    let Ok(parameter_delta) = serde_json::from_str::<RunLaunchParameterDelta>(parameter_delta_json)
    else {
        return RunLaunchWorkspaceRoots::default();
    };
    parameter_delta.cli_context.map(launch_workspace_roots_from_context).unwrap_or_default()
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
        push_launch_workspace_root(&mut roots.prompt_roots, raw_root.as_str());
    }
    roots.launch_cwd =
        context.launch_cwd.and_then(|raw_cwd| canonical_launch_workspace_root(raw_cwd.as_str()));
    if roots.launch_cwd.as_ref().is_some_and(|launch_cwd| {
        roots
            .prompt_roots
            .iter()
            .any(|root| same_workspace_root(root.as_path(), launch_cwd.as_path()))
    }) {
        roots.launch_cwd = None;
    }
    roots
}

fn push_launch_workspace_root(roots: &mut Vec<PathBuf>, raw_root: &str) {
    let Some(root) = canonical_launch_workspace_root(raw_root) else {
        return;
    };
    if !roots.iter().any(|existing| same_workspace_root(existing.as_path(), root.as_path())) {
        roots.push(root);
    }
}

fn canonical_launch_workspace_root(raw_root: &str) -> Option<PathBuf> {
    let raw_root = raw_root.trim();
    if raw_root.is_empty() || raw_root.chars().any(char::is_control) {
        return None;
    }
    let requested = Path::new(raw_root);
    if !requested.is_absolute() {
        return None;
    }
    let canonical = match fs::canonicalize(requested) {
        Ok(path) => path,
        Err(_) => return None,
    };
    let Ok(metadata) = fs::metadata(canonical.as_path()) else {
        return None;
    };
    if !metadata.is_dir() || protected_launch_workspace_root(canonical.as_path()) {
        return None;
    }
    Some(canonical)
}

fn merge_launch_workspace_roots(
    workspace_roots: &[PathBuf],
    launch_roots: RunLaunchWorkspaceRoots,
    launch_cwd_precedence: LaunchCwdPrecedence,
) -> Vec<PathBuf> {
    if launch_roots.prompt_roots.is_empty() && launch_roots.launch_cwd.is_none() {
        return workspace_roots.to_vec();
    }
    let mut merged: Vec<PathBuf> = Vec::with_capacity(
        workspace_roots.len().saturating_add(launch_roots.prompt_roots.len() + 1),
    );
    push_unique_workspace_roots(&mut merged, launch_roots.prompt_roots);
    if launch_cwd_precedence == LaunchCwdPrecedence::BeforeAgentRoots {
        if let Some(launch_cwd) = launch_roots.launch_cwd.clone() {
            push_unique_workspace_root(&mut merged, launch_cwd);
        }
    }
    push_unique_workspace_roots(&mut merged, workspace_roots.iter().cloned());
    if launch_cwd_precedence == LaunchCwdPrecedence::AfterAgentRoots {
        if let Some(launch_cwd) = launch_roots.launch_cwd {
            push_unique_workspace_root(&mut merged, launch_cwd);
        }
    }
    merged
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
        for root in &canonical_roots {
            let candidate = root.join(focus_path.as_str());
            let Some(directory) = nearest_existing_directory(candidate.as_path(), root) else {
                continue;
            };
            let Ok(directory) = fs::canonicalize(directory) else {
                continue;
            };
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

pub(crate) fn relative_path_already_targets_active_root(
    path: &str,
    active: &ActiveWorkspaceRoot,
) -> bool {
    let Some(path) = normalize_relative_workspace_path(path) else {
        return false;
    };
    path == active.relative_path || path.starts_with(format!("{}/", active.relative_path).as_str())
}

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
    if normalized == active.relative_path {
        return true;
    }
    Path::new(active.relative_path.as_str())
        .file_name()
        .and_then(|value| value.to_str())
        .is_some_and(|basename| normalized == basename)
}

fn canonicalize_workspace_roots(workspace_roots: &[PathBuf]) -> Vec<PathBuf> {
    workspace_roots
        .iter()
        .filter_map(|root| fs::canonicalize(root).ok().filter(|path| path.is_dir()))
        .collect()
}

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
        launch_path_env_from_context, launch_workspace_roots_from_context,
        merge_launch_workspace_roots, relative_path_already_targets_active_root,
        relative_path_should_use_active_root, same_workspace_root,
        workspace_root_override_targets_active_root, ActiveWorkspaceRoot, LaunchCwdPrecedence,
        RunLaunchCliContext,
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
    fn workspace_root_override_accepts_active_root_relative_path_or_basename() {
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
        assert!(workspace_root_override_targets_active_root("harness-smoke-20260525", &active));
        assert!(workspace_root_override_targets_active_root(
            active.root.to_string_lossy().as_ref(),
            &active
        ));
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
            env: None,
        });
        let roots = merge_launch_workspace_roots(
            &[default_root.clone(), launch_root.clone()],
            launch_roots,
            LaunchCwdPrecedence::BeforeAgentRoots,
        );

        assert_eq!(roots.len(), 2);
        assert_eq!(roots.first(), Some(&canonical_launch));
        assert_eq!(roots.get(1), Some(&default_root));
        assert!(same_workspace_root(roots[0].as_path(), launch_root.as_path()));
    }

    #[test]
    fn prompt_workspace_roots_precede_launch_cwd_and_agent_roots() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let explicit_root = tempdir.path().join("explicit");
        let launch_cwd = tempdir.path().join("cwd");
        let default_root = tempdir.path().join("default");
        fs::create_dir_all(explicit_root.as_path()).expect("explicit root should exist");
        fs::create_dir_all(launch_cwd.as_path()).expect("launch cwd should exist");
        fs::create_dir_all(default_root.as_path()).expect("default root should exist");
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
            env: None,
        });
        let roots = merge_launch_workspace_roots(
            std::slice::from_ref(&default_root),
            launch_roots,
            LaunchCwdPrecedence::BeforeAgentRoots,
        );

        assert_eq!(roots.len(), 3);
        assert_eq!(roots.first(), Some(&explicit_root));
        assert_eq!(roots.get(1), Some(&launch_cwd));
        assert_eq!(roots.get(2), Some(&default_root));
    }

    #[test]
    fn session_bound_agent_roots_precede_launch_cwd_without_prompt_roots() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let bound_root = tempdir.path().join("bound-agent");
        let launch_cwd = tempdir.path().join("cwd");
        fs::create_dir_all(bound_root.as_path()).expect("bound root should exist");
        fs::create_dir_all(launch_cwd.as_path()).expect("launch cwd should exist");
        let bound_root = fs::canonicalize(bound_root.as_path()).expect("bound root canonical");
        let launch_cwd = fs::canonicalize(launch_cwd.as_path()).expect("launch cwd canonical");

        let launch_roots = launch_workspace_roots_from_context(RunLaunchCliContext {
            launch_cwd: Some(launch_cwd.to_string_lossy().into_owned()),
            workspace_roots: None,
            env: None,
        });
        let roots = merge_launch_workspace_roots(
            std::slice::from_ref(&bound_root),
            launch_roots,
            LaunchCwdPrecedence::AfterAgentRoots,
        );

        assert_eq!(roots.len(), 2);
        assert_eq!(roots.first(), Some(&bound_root));
        assert_eq!(roots.get(1), Some(&launch_cwd));
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
