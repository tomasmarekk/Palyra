use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Context, Result};
use serde::Serialize;

use crate::{
    app::{self, CliConnectionProfile, CliProfilesDocument},
    args::{ProfileCommand, ProfileModeArg, ProfileRiskLevelArg},
    output,
};

#[derive(Debug, Clone, Serialize)]
struct ProfileListRecord {
    name: String,
    active: bool,
    is_default: bool,
    label: Option<String>,
    environment: Option<String>,
    color: Option<String>,
    risk_level: Option<String>,
    strict_mode: bool,
    mode: Option<String>,
    config_path: Option<String>,
    state_root: Option<String>,
    daemon_url: Option<String>,
    grpc_url: Option<String>,
    principal: Option<String>,
    device_id: Option<String>,
    channel: Option<String>,
    admin_token_env: Option<String>,
    updated_at_unix_ms: Option<i64>,
    last_used_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
struct ProfileListPayload {
    registry_path: String,
    active_profile: Option<String>,
    default_profile: Option<String>,
    profiles: Vec<ProfileListRecord>,
}

#[derive(Debug, Clone, Serialize)]
struct ProfileMutationPayload {
    action: &'static str,
    registry_path: String,
    active_profile: Option<String>,
    default_profile: Option<String>,
    profile: Option<ProfileListRecord>,
    removed_profile: Option<String>,
    removed_state_root: Option<String>,
    warnings: Vec<String>,
}

pub(crate) fn run_profile(command: ProfileCommand) -> Result<()> {
    match command {
        ProfileCommand::List { json, ndjson } => run_profile_list(json, ndjson),
        ProfileCommand::Show { name, json } => run_profile_show(name, json),
        ProfileCommand::Create {
            name,
            mode,
            label,
            environment,
            color,
            risk_level,
            strict_mode,
            config_path,
            state_root,
            daemon_url,
            grpc_url,
            admin_token_env,
            principal,
            device_id,
            channel,
            set_default,
            force,
            json,
        } => run_profile_create(ProfileCreateRequest {
            name,
            mode,
            label,
            environment,
            color,
            risk_level,
            strict_mode,
            config_path,
            state_root,
            daemon_url,
            grpc_url,
            admin_token_env,
            principal,
            device_id,
            channel,
            set_default,
            force,
            json,
        }),
        ProfileCommand::Use { name, json } => run_profile_use(name, json),
        ProfileCommand::Rename { name, new_name, json } => run_profile_rename(name, new_name, json),
        ProfileCommand::Delete { name, yes, delete_state_root, json } => {
            run_profile_delete(name, yes, delete_state_root, json)
        }
    }
}

fn run_profile_list(json: bool, ndjson: bool) -> Result<()> {
    let (path, document) = app::load_cli_profiles_registry()?;
    let active_profile = app::current_root_context()
        .and_then(|context| context.profile_name().map(ToOwned::to_owned));
    let payload = ProfileListPayload {
        registry_path: path.display().to_string(),
        active_profile: active_profile.clone(),
        default_profile: document.default_profile.clone(),
        profiles: document
            .profiles
            .iter()
            .map(|(name, profile)| {
                profile_record(
                    name.as_str(),
                    profile,
                    active_profile.as_deref(),
                    document.default_profile.as_deref(),
                )
            })
            .collect(),
    };
    if output::preferred_json(json) {
        return output::print_json_pretty(&payload, "failed to encode profile list output as JSON");
    }
    if output::preferred_ndjson(json, ndjson) {
        for record in payload.profiles.as_slice() {
            output::print_json_line(record, "failed to encode profile list output as NDJSON")?;
        }
        return Ok(());
    }
    println!(
        "profile.list registry_path={} active_profile={} default_profile={}",
        payload.registry_path,
        payload.active_profile.as_deref().unwrap_or("none"),
        payload.default_profile.as_deref().unwrap_or("none")
    );
    if payload.profiles.is_empty() {
        println!("profile.list.empty=true");
    }
    for record in payload.profiles.as_slice() {
        println!(
            "profile name={} active={} default={} environment={} risk_level={} strict_mode={} state_root={} config_path={}",
            record.name,
            record.active,
            record.is_default,
            record.environment.as_deref().unwrap_or("none"),
            record.risk_level.as_deref().unwrap_or("none"),
            record.strict_mode,
            record.state_root.as_deref().unwrap_or("none"),
            record.config_path.as_deref().unwrap_or("none"),
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn run_profile_show(name: Option<String>, json: bool) -> Result<()> {
    let (path, document) = app::load_cli_profiles_registry()?;
    let active_profile = app::current_root_context()
        .and_then(|context| context.profile_name().map(ToOwned::to_owned));
    let name = resolve_requested_profile_name(name, active_profile.as_deref(), &document)?;
    let profile = document
        .profiles
        .get(name.as_str())
        .ok_or_else(|| anyhow!("CLI profile not found: {name}"))?;
    let payload = ProfileMutationPayload {
        action: "show",
        registry_path: path.display().to_string(),
        active_profile: active_profile.clone(),
        default_profile: document.default_profile.clone(),
        profile: Some(profile_record(
            name.as_str(),
            profile,
            active_profile.as_deref(),
            document.default_profile.as_deref(),
        )),
        removed_profile: None,
        removed_state_root: None,
        warnings: Vec::new(),
    };
    emit_mutation_payload(&payload, json)
}

#[derive(Debug)]
struct ProfileCreateRequest {
    name: String,
    mode: ProfileModeArg,
    label: Option<String>,
    environment: Option<String>,
    color: Option<String>,
    risk_level: Option<ProfileRiskLevelArg>,
    strict_mode: bool,
    config_path: Option<String>,
    state_root: Option<String>,
    daemon_url: Option<String>,
    grpc_url: Option<String>,
    admin_token_env: Option<String>,
    principal: Option<String>,
    device_id: Option<String>,
    channel: Option<String>,
    set_default: bool,
    force: bool,
    json: bool,
}

fn run_profile_create(request: ProfileCreateRequest) -> Result<()> {
    let name = app::validate_profile_name(request.name.as_str())?;
    let (path, mut document) = app::load_cli_profiles_registry()?;
    if document.profiles.contains_key(name.as_str()) && !request.force {
        anyhow::bail!("CLI profile already exists: {name} (pass --force to replace it)");
    }
    let state_root = resolve_profile_state_root(name.as_str(), request.state_root.as_deref())?;
    let profile = CliConnectionProfile {
        config_path: normalize_optional_path(request.config_path.as_deref())?,
        state_root: Some(state_root.display().to_string()),
        daemon_url: app::normalized_profile_text(request.daemon_url.as_deref()),
        grpc_url: app::normalized_profile_text(request.grpc_url.as_deref()),
        admin_token: None,
        admin_token_env: app::normalized_profile_text(request.admin_token_env.as_deref()),
        principal: app::normalized_profile_text(request.principal.as_deref()),
        device_id: app::normalized_profile_text(request.device_id.as_deref()),
        channel: app::normalized_profile_text(request.channel.as_deref()),
        label: app::normalized_profile_text(request.label.as_deref()),
        environment: Some(
            app::normalized_profile_text(request.environment.as_deref())
                .unwrap_or_else(|| default_environment(request.mode).to_owned()),
        ),
        color: app::normalized_profile_text(request.color.as_deref()),
        risk_level: Some(
            request
                .risk_level
                .map(profile_risk_level_label)
                .unwrap_or_else(|| default_risk_level(request.mode).to_owned()),
        ),
        strict_mode: request.strict_mode || matches!(request.mode, ProfileModeArg::Remote),
        mode: Some(profile_mode_label(request.mode).to_owned()),
        created_at_unix_ms: Some(now_unix_ms()?),
        updated_at_unix_ms: Some(now_unix_ms()?),
        last_used_at_unix_ms: None,
    };
    document.profiles.insert(name.clone(), profile.clone());
    if request.set_default || document.default_profile.is_none() {
        document.default_profile = Some(name.clone());
    }
    app::persist_cli_profiles_registry(path.as_path(), &document)?;
    let active_profile = if request.set_default {
        Some(name.clone())
    } else {
        app::current_root_context()
            .and_then(|context| context.profile_name().map(ToOwned::to_owned))
    };
    let payload = ProfileMutationPayload {
        action: "create",
        registry_path: path.display().to_string(),
        active_profile: active_profile.clone(),
        default_profile: document.default_profile.clone(),
        profile: Some(profile_record(
            name.as_str(),
            &profile,
            active_profile.as_deref(),
            document.default_profile.as_deref(),
        )),
        removed_profile: None,
        removed_state_root: None,
        warnings: create_profile_warnings(&profile),
    };
    emit_mutation_payload(&payload, request.json)
}

fn run_profile_use(name: String, json: bool) -> Result<()> {
    let name = app::validate_profile_name(name.as_str())?;
    let (path, mut document) = app::load_cli_profiles_registry()?;
    let profile = document
        .profiles
        .get_mut(name.as_str())
        .ok_or_else(|| anyhow!("CLI profile not found: {name}"))?;
    let now = now_unix_ms()?;
    profile.last_used_at_unix_ms = Some(now);
    profile.updated_at_unix_ms = Some(now);
    document.default_profile = Some(name.clone());
    let profile_snapshot = profile.clone();
    app::persist_cli_profiles_registry(path.as_path(), &document)?;
    let payload = ProfileMutationPayload {
        action: "use",
        registry_path: path.display().to_string(),
        active_profile: Some(name.clone()),
        default_profile: document.default_profile.clone(),
        profile: Some(profile_record(
            name.as_str(),
            &profile_snapshot,
            Some(name.as_str()),
            document.default_profile.as_deref(),
        )),
        removed_profile: None,
        removed_state_root: None,
        warnings: Vec::new(),
    };
    emit_mutation_payload(&payload, json)
}

fn run_profile_rename(name: String, new_name: String, json: bool) -> Result<()> {
    let name = app::validate_profile_name(name.as_str())?;
    let new_name = app::validate_profile_name(new_name.as_str())?;
    let (path, mut document) = app::load_cli_profiles_registry()?;
    if document.profiles.contains_key(new_name.as_str()) {
        anyhow::bail!("CLI profile already exists: {new_name}");
    }
    let mut profile = document
        .profiles
        .remove(name.as_str())
        .ok_or_else(|| anyhow!("CLI profile not found: {name}"))?;
    if let Some(state_root) =
        profile.state_root.as_deref().and_then(|value| app::normalized_profile_text(Some(value)))
    {
        let expected_old = app::default_profile_state_root(name.as_str())?;
        let actual = PathBuf::from(state_root.as_str());
        if paths_equivalent(actual.as_path(), expected_old.as_path()) {
            profile.state_root =
                Some(app::default_profile_state_root(new_name.as_str())?.display().to_string());
        }
    }
    let now = now_unix_ms()?;
    profile.updated_at_unix_ms = Some(now);
    document.profiles.insert(new_name.clone(), profile.clone());
    if document.default_profile.as_deref() == Some(name.as_str()) {
        document.default_profile = Some(new_name.clone());
    }
    app::persist_cli_profiles_registry(path.as_path(), &document)?;
    let active_profile = app::current_root_context()
        .and_then(|context| context.profile_name().map(ToOwned::to_owned))
        .map(|value| if value == name { new_name.clone() } else { value });
    let payload = ProfileMutationPayload {
        action: "rename",
        registry_path: path.display().to_string(),
        active_profile: active_profile.clone(),
        default_profile: document.default_profile.clone(),
        profile: Some(profile_record(
            new_name.as_str(),
            &profile,
            active_profile.as_deref(),
            document.default_profile.as_deref(),
        )),
        removed_profile: None,
        removed_state_root: None,
        warnings: Vec::new(),
    };
    emit_mutation_payload(&payload, json)
}

fn run_profile_delete(name: String, yes: bool, delete_state_root: bool, json: bool) -> Result<()> {
    let name = app::validate_profile_name(name.as_str())?;
    let active_profile = app::current_root_context()
        .and_then(|context| context.profile_name().map(ToOwned::to_owned));
    if active_profile.as_deref() == Some(name.as_str()) && !yes {
        anyhow::bail!("refusing to delete the active profile without --yes: {name}");
    }
    let (path, mut document) = app::load_cli_profiles_registry()?;
    let profile = document
        .profiles
        .remove(name.as_str())
        .ok_or_else(|| anyhow!("CLI profile not found: {name}"))?;
    let mut warnings = Vec::new();
    let mut removed_state_root = None;
    if document.default_profile.as_deref() == Some(name.as_str()) {
        document.default_profile = document.profiles.keys().next().cloned();
    }
    if delete_state_root {
        let Some(state_root_raw) = profile.state_root.as_deref() else {
            warnings.push("profile did not define a state root to delete".to_owned());
            app::persist_cli_profiles_registry(path.as_path(), &document)?;
            let payload = ProfileMutationPayload {
                action: "delete",
                registry_path: path.display().to_string(),
                active_profile,
                default_profile: document.default_profile.clone(),
                profile: None,
                removed_profile: Some(name),
                removed_state_root,
                warnings,
            };
            return emit_mutation_payload(&payload, json);
        };
        let state_root = PathBuf::from(state_root_raw);
        if state_root.exists() {
            if !yes {
                anyhow::bail!(
                    "profile delete would remove state root {}; pass --yes to confirm",
                    state_root.display()
                );
            }
            ensure_safe_profile_state_root_removal(state_root.as_path())?;
            fs::remove_dir_all(state_root.as_path()).with_context(|| {
                format!("failed to remove profile state root {}", state_root.display())
            })?;
            removed_state_root = Some(state_root.display().to_string());
        } else {
            warnings.push(format!(
                "profile state root did not exist at delete time: {}",
                state_root.display()
            ));
        }
    } else if profile.state_root.is_some() {
        warnings.push(
            "state root was preserved; pass --delete-state-root --yes to remove it explicitly"
                .to_owned(),
        );
    }
    app::persist_cli_profiles_registry(path.as_path(), &document)?;
    let payload = ProfileMutationPayload {
        action: "delete",
        registry_path: path.display().to_string(),
        active_profile,
        default_profile: document.default_profile.clone(),
        profile: None,
        removed_profile: Some(name),
        removed_state_root,
        warnings,
    };
    emit_mutation_payload(&payload, json)
}

fn emit_mutation_payload(payload: &ProfileMutationPayload, json: bool) -> Result<()> {
    if output::preferred_json(json) {
        return output::print_json_pretty(payload, "failed to encode profile output as JSON");
    }
    println!(
        "profile.{} registry_path={} active_profile={} default_profile={}",
        payload.action,
        payload.registry_path,
        payload.active_profile.as_deref().unwrap_or("none"),
        payload.default_profile.as_deref().unwrap_or("none"),
    );
    if let Some(profile) = payload.profile.as_ref() {
        println!(
            "profile.name={} environment={} risk_level={} strict_mode={} state_root={} config_path={}",
            profile.name,
            profile.environment.as_deref().unwrap_or("none"),
            profile.risk_level.as_deref().unwrap_or("none"),
            profile.strict_mode,
            profile.state_root.as_deref().unwrap_or("none"),
            profile.config_path.as_deref().unwrap_or("none"),
        );
    }
    if let Some(removed_profile) = payload.removed_profile.as_deref() {
        println!("profile.removed={removed_profile}");
    }
    if let Some(removed_state_root) = payload.removed_state_root.as_deref() {
        println!("profile.removed_state_root={removed_state_root}");
    }
    for warning in payload.warnings.as_slice() {
        println!("profile.warning={warning}");
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn resolve_requested_profile_name(
    name: Option<String>,
    active_profile: Option<&str>,
    document: &CliProfilesDocument,
) -> Result<String> {
    if let Some(name) = name {
        return app::validate_profile_name(name.as_str());
    }
    if let Some(active_profile) = active_profile {
        return Ok(active_profile.to_owned());
    }
    if let Some(default_profile) = document.default_profile.as_deref() {
        return Ok(default_profile.to_owned());
    }
    anyhow::bail!("no active or default profile is configured; pass a profile name explicitly")
}

fn profile_record(
    name: &str,
    profile: &CliConnectionProfile,
    active_profile: Option<&str>,
    default_profile: Option<&str>,
) -> ProfileListRecord {
    ProfileListRecord {
        name: name.to_owned(),
        active: active_profile == Some(name),
        is_default: default_profile == Some(name),
        label: profile.label.clone(),
        environment: profile.environment.clone(),
        color: profile.color.clone(),
        risk_level: profile.risk_level.clone(),
        strict_mode: profile.strict_mode,
        mode: profile.mode.clone(),
        config_path: profile.config_path.clone(),
        state_root: profile.state_root.clone(),
        daemon_url: profile.daemon_url.clone(),
        grpc_url: profile.grpc_url.clone(),
        principal: profile.principal.clone(),
        device_id: profile.device_id.clone(),
        channel: profile.channel.clone(),
        admin_token_env: profile.admin_token_env.clone(),
        updated_at_unix_ms: profile.updated_at_unix_ms,
        last_used_at_unix_ms: profile.last_used_at_unix_ms,
    }
}

fn create_profile_warnings(profile: &CliConnectionProfile) -> Vec<String> {
    let mut warnings = Vec::new();
    if profile.config_path.is_none() {
        warnings.push(
            "config_path is unset; run `palyra setup --profile <name>` or `palyra configure --profile <name>` to attach a config file"
                .to_owned(),
        );
    }
    if profile.admin_token_env.is_none() && profile.daemon_url.is_some() {
        warnings.push(
            "remote-oriented profile has no admin token environment override configured".to_owned(),
        );
    }
    warnings
}

fn resolve_profile_state_root(profile_name: &str, explicit: Option<&str>) -> Result<PathBuf> {
    if let Some(explicit) = explicit {
        let normalized = explicit.trim();
        if normalized.is_empty() {
            anyhow::bail!("profile state_root cannot be empty when provided");
        }
        return parse_profile_path(normalized, "profile state_root");
    }
    let state_root = app::default_profile_state_root(profile_name)?;
    if let Some(parent) = state_root.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!("failed to create profile state root parent {}", parent.display())
        })?;
    }
    Ok(state_root)
}

fn normalize_optional_path(raw: Option<&str>) -> Result<Option<String>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let normalized = raw.trim();
    if normalized.is_empty() {
        return Ok(None);
    }
    parse_profile_path(normalized, "profile config_path")
        .map(|path| Some(path.display().to_string()))
}

fn parse_profile_path(raw: &str, label: &str) -> Result<PathBuf> {
    palyra_common::parse_config_path(raw).with_context(|| format!("{label} is invalid: {raw}"))
}

fn ensure_safe_profile_state_root_removal(path: &Path) -> Result<()> {
    crate::support::lifecycle::ensure_safe_removal_target(path, "profile state root")?;
    let canonical = crate::support::lifecycle::canonicalize_lossy(path)?;
    let default_root = app::resolve_cli_state_root(None)?;
    if !crate::support::lifecycle::path_starts_with(canonical.as_path(), default_root.as_path()) {
        anyhow::bail!(
            "refusing to remove state root outside the CLI state root namespace: {}",
            canonical.display()
        );
    }
    Ok(())
}

fn profile_mode_label(mode: ProfileModeArg) -> &'static str {
    match mode {
        ProfileModeArg::Local => "local",
        ProfileModeArg::Remote => "remote",
        ProfileModeArg::Custom => "custom",
    }
}

fn default_environment(mode: ProfileModeArg) -> &'static str {
    match mode {
        ProfileModeArg::Local => "local",
        ProfileModeArg::Remote => "remote",
        ProfileModeArg::Custom => "custom",
    }
}

fn default_risk_level(mode: ProfileModeArg) -> &'static str {
    match mode {
        ProfileModeArg::Local => "low",
        ProfileModeArg::Remote => "high",
        ProfileModeArg::Custom => "elevated",
    }
}

fn profile_risk_level_label(level: ProfileRiskLevelArg) -> String {
    match level {
        ProfileRiskLevelArg::Low => "low",
        ProfileRiskLevelArg::Elevated => "elevated",
        ProfileRiskLevelArg::High => "high",
        ProfileRiskLevelArg::Critical => "critical",
    }
    .to_owned()
}

fn now_unix_ms() -> Result<i64> {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock is set before UNIX_EPOCH")?;
    i64::try_from(duration.as_millis()).context("system clock exceeds supported timestamp range")
}

fn paths_equivalent(left: &Path, right: &Path) -> bool {
    let left =
        crate::support::lifecycle::canonicalize_lossy(left).unwrap_or_else(|_| left.to_path_buf());
    let right = crate::support::lifecycle::canonicalize_lossy(right)
        .unwrap_or_else(|_| right.to_path_buf());
    #[cfg(windows)]
    {
        left.to_string_lossy()
            .replace('/', "\\")
            .eq_ignore_ascii_case(right.to_string_lossy().replace('/', "\\").as_str())
    }
    #[cfg(not(windows))]
    {
        left == right
    }
}

#[cfg(test)]
mod tests {
    use super::{default_environment, default_risk_level, profile_mode_label};
    use crate::args::ProfileModeArg;

    #[test]
    fn profile_mode_defaults_match_environment_story() {
        assert_eq!(profile_mode_label(ProfileModeArg::Local), "local");
        assert_eq!(default_environment(ProfileModeArg::Remote), "remote");
        assert_eq!(default_risk_level(ProfileModeArg::Custom), "elevated");
    }
}
