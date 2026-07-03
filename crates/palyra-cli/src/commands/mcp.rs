//! `palyra mcp`: external MCP registry management plus a stdio MCP server
//! facade over Palyra sessions, transcripts, memory, and approvals.
//!
//! Registry subcommands only edit local config. `serve` exposes Palyra to
//! external MCP clients and never imports external MCP tools into agent runs.
//! Mutating facade tools are gated by `--read-only`.

use std::{
    collections::HashSet,
    io::{self, BufRead, BufReader, Write},
    path::Path,
};

use anyhow::{anyhow, Context, Result};
use palyra_vault::VaultRef;
use serde_json::{json, Map, Value};
use tonic::Request;

use crate::cli::{
    AcpConnectionArgs, AcpSessionDefaultsArgs, McpCommand, McpEgressPolicyArg,
    McpRegistryMutateArgs, McpRegistryToggleArgs, McpStatusArgs, McpSubcommand, McpTransportArg,
};
use crate::*;

const MCP_PROTOCOL_VERSION: &str = "2024-11-05";
const JSONRPC_VERSION: &str = "2.0";

const TOOL_SESSIONS_LIST: &str = "sessions_list";
const TOOL_SESSION_TRANSCRIPT_READ: &str = "session_transcript_read";
const TOOL_SESSION_EXPORT: &str = "session_export";
const TOOL_MEMORY_SEARCH: &str = "memory_search";
const TOOL_APPROVALS_LIST: &str = "approvals_list";
const TOOL_SESSION_CREATE: &str = "session_create";
const TOOL_SESSION_PROMPT: &str = "session_prompt";
const TOOL_APPROVAL_DECIDE: &str = "approval_decide";
const MCP_MEMORY_SEARCH_HITS_PRESENT_CLAIM_BOUNDARY: &str =
    "durable memory hits were returned; cite them as stored memory evidence";
const MCP_MEMORY_SEARCH_HITS_ABSENT_CLAIM_BOUNDARY: &str =
    "no durable memory hits were returned by this memory search; this does not search prior session transcripts";
const MCP_SERVER_SCOPE_NOTE: &str = "`palyra mcp serve` exposes Palyra as an MCP server facade for external MCP clients. It does not import external MCP servers or register external MCP client tools into Palyra agent runs.";
const REGISTERED_MCP_TOOLS: &[&str] = &[
    TOOL_SESSIONS_LIST,
    TOOL_SESSION_TRANSCRIPT_READ,
    TOOL_SESSION_EXPORT,
    TOOL_MEMORY_SEARCH,
    TOOL_APPROVALS_LIST,
    TOOL_SESSION_CREATE,
    TOOL_SESSION_PROMPT,
    TOOL_APPROVAL_DECIDE,
];

/// Runs a `palyra mcp` subcommand; `serve` blocks on stdio until the client disconnects.
///
/// # Errors
/// Returns an error when connection resolution fails or the stdio message loop
/// hits an IO/framing error.
pub(crate) fn run_mcp(command: McpCommand) -> Result<()> {
    match command.subcommand {
        McpSubcommand::Serve { connection, session_defaults, read_only, allow_sensitive_tools } => {
            run_mcp_serve(connection, session_defaults, read_only, allow_sensitive_tools)
        }
        McpSubcommand::Status(args) => run_mcp_runtime_status(args),
        McpSubcommand::List { path, json } => run_mcp_registry_list(path, json),
        McpSubcommand::Show { id, path, json } => run_mcp_registry_show(path, &id, json),
        McpSubcommand::Add(args) => run_mcp_registry_add(args),
        McpSubcommand::Set(args) => run_mcp_registry_set(args),
        McpSubcommand::Enable(args) => run_mcp_registry_toggle(args, true),
        McpSubcommand::Disable(args) => run_mcp_registry_toggle(args, false),
        McpSubcommand::Remove(args) => run_mcp_registry_remove(args),
    }
}

fn run_mcp_runtime_status(args: McpStatusArgs) -> Result<()> {
    let overrides = app::ConnectionOverrides {
        daemon_url: args.url,
        grpc_url: None,
        token: args.token,
        principal: args.principal,
        device_id: args.device_id,
        channel: args.channel,
    };
    let runtime = build_runtime()?;
    let mcp_payload = runtime.block_on(async {
        let context = client::control_plane::connect_admin_console(overrides).await?;
        let diagnostics = context.client.get_diagnostics().await?;
        diagnostics
            .get("mcp")
            .cloned()
            .ok_or_else(|| anyhow!("daemon diagnostics did not include MCP runtime status"))
    })?;

    if output::preferred_json(args.json) {
        return output::print_json_pretty(
            &mcp_payload,
            "failed to encode MCP runtime status as JSON",
        );
    }
    println!(
        "mcp.status mode={} total={} enabled={} healthy={} degraded={} backoff={} quarantined={} disabled={}",
        json_string(&mcp_payload, "mode").unwrap_or("unknown"),
        json_u64(&mcp_payload, "total_servers"),
        json_u64(&mcp_payload, "enabled_servers"),
        json_u64(&mcp_payload, "healthy_servers"),
        json_u64(&mcp_payload, "degraded_servers"),
        json_u64(&mcp_payload, "backoff_servers"),
        json_u64(&mcp_payload, "quarantined_servers"),
        json_u64(&mcp_payload, "disabled_servers")
    );
    for server in mcp_payload.get("servers").and_then(Value::as_array).into_iter().flatten() {
        println!(
            "mcp.server id={} namespace={} transport={} enabled={} state={} failures={} restarts={} next_retry_at_unix_ms={} last_error_code={}",
            json_string(server, "id").unwrap_or("unknown"),
            json_string(server, "namespace").unwrap_or("unknown"),
            json_string(server, "transport").unwrap_or("unknown"),
            json_bool(server, "enabled").unwrap_or(false),
            json_string(server, "state").unwrap_or("unknown"),
            json_u64(server, "total_failures"),
            json_u64(server, "restart_count"),
            json_i64(server, "next_retry_at_unix_ms")
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_owned()),
            json_string(server, "last_error_code").unwrap_or("-"),
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn run_mcp_registry_list(path: Option<String>, json: bool) -> Result<()> {
    let path = resolve_config_path(path, true)?;
    let (mut document, _) = load_document_from_existing_path(Path::new(&path))
        .with_context(|| format!("failed to parse {path}"))?;
    canonicalize_mcp_registry_section(&mut document)?;
    let servers = read_mcp_server_entries(&document)?;
    if output::preferred_json(json) {
        return output::print_json_pretty(
            &json!({
                "source": path,
                "servers": servers,
            }),
            "failed to encode MCP registry list as JSON",
        );
    }
    println!("mcp.list source={} count={}", path, servers.len());
    for server in servers {
        println!(
            "mcp.server id={} enabled={} namespace={} transport={}",
            json_string(&server, "id").unwrap_or("unknown"),
            json_bool(&server, "enabled").unwrap_or(false),
            json_string(&server, "namespace").unwrap_or("unknown"),
            json_string(&server, "transport").unwrap_or("unknown")
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn run_mcp_registry_show(path: Option<String>, id: &str, json: bool) -> Result<()> {
    let path = resolve_config_path(path, true)?;
    let (mut document, _) = load_document_from_existing_path(Path::new(&path))
        .with_context(|| format!("failed to parse {path}"))?;
    canonicalize_mcp_registry_section(&mut document)?;
    let server = find_mcp_server_entry(&document, id)?
        .ok_or_else(|| anyhow!("MCP server `{id}` is not configured"))?;
    if output::preferred_json(json) {
        return output::print_json_pretty(
            &json!({
                "source": path,
                "server": server,
            }),
            "failed to encode MCP registry server as JSON",
        );
    }
    println!(
        "mcp.show source={} id={} enabled={} namespace={} transport={}",
        path,
        json_string(&server, "id").unwrap_or("unknown"),
        json_bool(&server, "enabled").unwrap_or(false),
        json_string(&server, "namespace").unwrap_or("unknown"),
        json_string(&server, "transport").unwrap_or("unknown")
    );
    std::io::stdout().flush().context("stdout flush failed")
}

fn run_mcp_registry_add(args: McpRegistryMutateArgs) -> Result<()> {
    mutate_mcp_registry(args, McpRegistryMutation::Add)
}

fn run_mcp_registry_set(args: McpRegistryMutateArgs) -> Result<()> {
    mutate_mcp_registry(args, McpRegistryMutation::Set)
}

fn run_mcp_registry_toggle(args: McpRegistryToggleArgs, enabled: bool) -> Result<()> {
    let path = resolve_config_path(args.path, true)?;
    let path_ref = Path::new(&path);
    let (mut document, _) = load_document_from_existing_path(path_ref)
        .with_context(|| format!("failed to parse {}", path_ref.display()))?;
    canonicalize_mcp_registry_section(&mut document)?;
    let server = mcp_server_entry_mut(&mut document, &args.id)?
        .ok_or_else(|| anyhow!("MCP server `{}` is not configured", args.id))?;
    let table = server
        .as_table_mut()
        .ok_or_else(|| anyhow!("MCP server `{}` is not a TOML table", args.id))?;
    table.insert("enabled".to_owned(), toml::Value::Boolean(enabled));
    persist_mcp_registry_document(path_ref, &document, args.backups)?;
    let payload = json!({
        "source": path_ref.display().to_string(),
        "id": args.id,
        "enabled": enabled,
        "backups": args.backups,
    });
    if output::preferred_json(args.json) {
        output::print_json_pretty(&payload, "failed to encode MCP registry toggle as JSON")
    } else {
        println!(
            "mcp.{} id={} source={} backups={}",
            if enabled { "enable" } else { "disable" },
            payload["id"].as_str().unwrap_or("unknown"),
            path_ref.display(),
            args.backups
        );
        std::io::stdout().flush().context("stdout flush failed")
    }
}

fn run_mcp_registry_remove(args: McpRegistryToggleArgs) -> Result<()> {
    let path = resolve_config_path(args.path, true)?;
    let path_ref = Path::new(&path);
    let (mut document, _) = load_document_from_existing_path(path_ref)
        .with_context(|| format!("failed to parse {}", path_ref.display()))?;
    canonicalize_mcp_registry_section(&mut document)?;
    let removed = remove_mcp_server_entry(&mut document, &args.id)?;
    if !removed {
        anyhow::bail!("MCP server `{}` is not configured", args.id);
    }
    persist_mcp_registry_document(path_ref, &document, args.backups)?;
    let payload = json!({
        "source": path_ref.display().to_string(),
        "id": args.id,
        "removed": true,
        "backups": args.backups,
    });
    if output::preferred_json(args.json) {
        output::print_json_pretty(&payload, "failed to encode MCP registry remove as JSON")
    } else {
        println!(
            "mcp.remove id={} source={} backups={}",
            payload["id"].as_str().unwrap_or("unknown"),
            path_ref.display(),
            args.backups
        );
        std::io::stdout().flush().context("stdout flush failed")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum McpRegistryMutation {
    Add,
    Set,
}

fn mutate_mcp_registry(args: McpRegistryMutateArgs, mutation: McpRegistryMutation) -> Result<()> {
    let normalized_id = normalize_mcp_config_identifier(args.id.as_str(), "id")?;
    let path = resolve_config_path(args.path.clone(), false)?;
    let path_ref = Path::new(&path);
    let (mut document, migration) = load_document_for_mutation(path_ref)
        .with_context(|| format!("failed to parse {}", path_ref.display()))?;
    canonicalize_mcp_registry_section(&mut document)?;
    let server = mcp_server_table(&args)?;
    let servers = mcp_servers_array_mut(&mut document)?;
    let existing = servers
        .iter()
        .position(|entry| toml_table_string(entry, "id") == Some(normalized_id.as_str()));
    match (mutation, existing) {
        (McpRegistryMutation::Add, Some(_)) => {
            anyhow::bail!("MCP server `{}` is already configured; use `palyra mcp set`", args.id);
        }
        (McpRegistryMutation::Add, None) => servers.push(server),
        (McpRegistryMutation::Set, Some(index)) => servers[index] = server,
        (McpRegistryMutation::Set, None) => {
            anyhow::bail!("MCP server `{}` is not configured; use `palyra mcp add`", args.id);
        }
    }
    persist_mcp_registry_document(path_ref, &document, args.backups)?;
    let action = match mutation {
        McpRegistryMutation::Add => "add",
        McpRegistryMutation::Set => "set",
    };
    let payload = json!({
        "source": path_ref.display().to_string(),
        "id": normalized_id,
        "action": action,
        "enabled": args.enabled,
        "backups": args.backups,
        "migrated": migration.migrated,
    });
    if output::preferred_json(args.json) {
        output::print_json_pretty(&payload, "failed to encode MCP registry mutation as JSON")
    } else {
        println!(
            "mcp.{action} id={} source={} enabled={} backups={} migrated={}",
            payload["id"].as_str().unwrap_or("unknown"),
            path_ref.display(),
            args.enabled,
            args.backups,
            migration.migrated
        );
        std::io::stdout().flush().context("stdout flush failed")
    }
}

fn persist_mcp_registry_document(
    path: &Path,
    document: &toml::Value,
    backups: usize,
) -> Result<()> {
    validate_mcp_registry_document(document)
        .with_context(|| format!("mutated MCP registry in {} is invalid", path.display()))?;
    validate_daemon_compatible_document(document).with_context(|| {
        format!("mutated config {} does not match daemon schema", path.display())
    })?;
    write_document_with_backups(path, document, backups)
        .with_context(|| format!("failed to persist config {}", path.display()))
}

fn mcp_server_table(args: &McpRegistryMutateArgs) -> Result<toml::Value> {
    let id = normalize_mcp_config_identifier(args.id.as_str(), "id")?;
    let namespace =
        normalize_mcp_namespace(args.namespace.as_deref().unwrap_or(id.as_str()), "namespace")?;
    validate_mcp_transport_args(args)?;
    let mut table = toml::map::Map::new();
    table.insert("id".to_owned(), toml::Value::String(id));
    table.insert("enabled".to_owned(), toml::Value::Boolean(args.enabled));
    table.insert("namespace".to_owned(), toml::Value::String(namespace));
    table.insert("transport".to_owned(), toml::Value::String(args.transport.as_str().to_owned()));
    match args.transport {
        McpTransportArg::Stdio => {
            let command = args.command.as_deref().expect("stdio command validated as present");
            table.insert("command".to_owned(), toml::Value::String(command.to_owned()));
            if !args.args.is_empty() {
                table.insert("args".to_owned(), string_array_toml(args.args.as_slice()));
            }
            if !args.env_vault_refs.is_empty() {
                table.insert(
                    "env_vault_refs".to_owned(),
                    toml::Value::Array(parse_env_vault_ref_tables(args.env_vault_refs.as_slice())?),
                );
            }
        }
        McpTransportArg::Http | McpTransportArg::Sse => {
            let url = args.url.as_deref().expect("HTTP/SSE URL validated as present");
            table.insert("url".to_owned(), toml::Value::String(url.to_owned()));
        }
    }
    table.insert(
        "trust_level".to_owned(),
        toml::Value::String(args.trust_level.as_str().to_owned()),
    );
    table.insert(
        "approval_profile".to_owned(),
        toml::Value::String(args.approval_profile.as_str().to_owned()),
    );
    table.insert(
        "egress_policy".to_owned(),
        toml::Value::String(args.egress_policy.as_str().to_owned()),
    );
    if !args.egress_allowlist.is_empty() {
        table.insert(
            "egress_allowlist".to_owned(),
            string_array_toml(args.egress_allowlist.as_slice()),
        );
    }
    if !args.tool_allowlist.is_empty() {
        table
            .insert("tool_allowlist".to_owned(), string_array_toml(args.tool_allowlist.as_slice()));
    }
    if !args.tool_denylist.is_empty() {
        table.insert("tool_denylist".to_owned(), string_array_toml(args.tool_denylist.as_slice()));
    }
    Ok(toml::Value::Table(table))
}

fn validate_mcp_registry_document(document: &toml::Value) -> Result<()> {
    let Some(value) = get_value_at_path(document, "mcp.servers")
        .context("invalid MCP registry path mcp.servers")?
    else {
        return Ok(());
    };
    let servers = value.as_array().ok_or_else(|| anyhow!("mcp.servers must be a TOML array"))?;
    let mut ids = HashSet::new();
    let mut namespaces = HashSet::new();
    for (index, server) in servers.iter().enumerate() {
        let source = format!("mcp.servers[{index}]");
        let table = server.as_table().ok_or_else(|| anyhow!("{source} must be a TOML table"))?;
        if table.contains_key("env") {
            anyhow::bail!(
                "{source}.env is not supported; use env_vault_refs with NAME=scope/key bindings"
            );
        }
        let id_source = format!("{source}.id");
        let id = table
            .get("id")
            .and_then(toml::Value::as_str)
            .ok_or_else(|| anyhow!("{id_source} is required"))?;
        let normalized_id = normalize_mcp_config_identifier(id, id_source.as_str())?;
        if !ids.insert(normalized_id.clone()) {
            anyhow::bail!("{id_source} duplicates `{normalized_id}`");
        }

        let namespace_source = format!("{source}.namespace");
        let namespace = match table.get("namespace").and_then(toml::Value::as_str) {
            Some(raw) => normalize_mcp_namespace(raw, namespace_source.as_str())?,
            None => normalized_id,
        };
        if !namespaces.insert(namespace.clone()) {
            anyhow::bail!("{namespace_source} duplicates `{namespace}`");
        }
    }
    Ok(())
}

fn validate_mcp_transport_args(args: &McpRegistryMutateArgs) -> Result<()> {
    match args.transport {
        McpTransportArg::Stdio => {
            if args.command.as_deref().is_none_or(|value| value.trim().is_empty()) {
                anyhow::bail!("--command is required when --transport stdio");
            }
            if args.url.is_some() {
                anyhow::bail!("--url must be omitted when --transport stdio");
            }
        }
        McpTransportArg::Http | McpTransportArg::Sse => {
            if args.url.as_deref().is_none_or(|value| value.trim().is_empty()) {
                anyhow::bail!("--url is required when --transport {}", args.transport.as_str());
            }
            if args.command.is_some() || !args.args.is_empty() || !args.env_vault_refs.is_empty() {
                anyhow::bail!(
                    "--command, --arg, and --env-vault-ref are only valid when --transport stdio"
                );
            }
        }
    }
    if matches!(args.egress_policy, McpEgressPolicyArg::Allowlist)
        && args.egress_allowlist.is_empty()
    {
        anyhow::bail!("--egress-host is required when --egress-policy allowlist");
    }
    Ok(())
}

fn canonicalize_mcp_registry_section(document: &mut toml::Value) -> Result<()> {
    let root = document
        .as_table_mut()
        .ok_or_else(|| anyhow!("config document root must be a TOML table"))?;
    if root.contains_key("mcp") && root.contains_key("mcp_servers") {
        anyhow::bail!(
            "mcp and mcp_servers cannot both be set; use canonical [mcp] / [[mcp.servers]]"
        );
    }
    if !root.contains_key("mcp") {
        if let Some(legacy) = root.remove("mcp_servers") {
            root.insert("mcp".to_owned(), legacy);
        }
    }
    Ok(())
}

fn mcp_servers_array_mut(document: &mut toml::Value) -> Result<&mut Vec<toml::Value>> {
    let root = document
        .as_table_mut()
        .ok_or_else(|| anyhow!("config document root must be a TOML table"))?;
    let section = root
        .entry("mcp".to_owned())
        .or_insert_with(|| toml::Value::Table(toml::map::Map::new()))
        .as_table_mut()
        .ok_or_else(|| anyhow!("mcp must be a TOML table"))?;
    section
        .entry("mode".to_owned())
        .or_insert_with(|| toml::Value::String("preview_only".to_owned()));
    section
        .entry("servers".to_owned())
        .or_insert_with(|| toml::Value::Array(Vec::new()))
        .as_array_mut()
        .ok_or_else(|| anyhow!("mcp.servers must be a TOML array"))
}

fn read_mcp_server_entries(document: &toml::Value) -> Result<Vec<Value>> {
    let Some(value) = get_value_at_path(document, "mcp.servers")
        .context("invalid MCP registry path mcp.servers")?
    else {
        return Ok(Vec::new());
    };
    let servers = value.as_array().ok_or_else(|| anyhow!("mcp.servers must be a TOML array"))?;
    servers.iter().map(toml_server_to_json).collect::<Result<Vec<_>>>()
}

fn find_mcp_server_entry(document: &toml::Value, id: &str) -> Result<Option<Value>> {
    let id = normalize_mcp_config_identifier(id, "id")?;
    Ok(read_mcp_server_entries(document)?
        .into_iter()
        .find(|server| json_string(server, "id") == Some(id.as_str())))
}

fn mcp_server_entry_mut<'a>(
    document: &'a mut toml::Value,
    id: &str,
) -> Result<Option<&'a mut toml::Value>> {
    let id = normalize_mcp_config_identifier(id, "id")?;
    Ok(mcp_servers_array_mut(document)?
        .iter_mut()
        .find(|server| toml_table_string(server, "id") == Some(id.as_str())))
}

fn remove_mcp_server_entry(document: &mut toml::Value, id: &str) -> Result<bool> {
    let id = normalize_mcp_config_identifier(id, "id")?;
    let servers = mcp_servers_array_mut(document)?;
    let Some(index) =
        servers.iter().position(|server| toml_table_string(server, "id") == Some(id.as_str()))
    else {
        return Ok(false);
    };
    servers.remove(index);
    Ok(true)
}

fn parse_env_vault_ref_tables(raw_refs: &[String]) -> Result<Vec<toml::Value>> {
    raw_refs
        .iter()
        .map(|raw| {
            let (name, vault_ref) = raw.split_once('=').ok_or_else(|| {
                anyhow!("--env-vault-ref must use NAME=scope/key syntax, got `{raw}`")
            })?;
            let name = validate_mcp_env_name(name)?;
            let vault_ref = validate_mcp_vault_ref(vault_ref)?;
            let mut table = toml::map::Map::new();
            table.insert("name".to_owned(), toml::Value::String(name));
            table.insert("vault_ref".to_owned(), toml::Value::String(vault_ref));
            Ok(toml::Value::Table(table))
        })
        .collect()
}

fn string_array_toml(values: &[String]) -> toml::Value {
    toml::Value::Array(values.iter().cloned().map(toml::Value::String).collect())
}

fn toml_server_to_json(value: &toml::Value) -> Result<Value> {
    serde_json::to_value(value).context("failed to encode MCP registry entry as JSON")
}

fn toml_table_string<'a>(value: &'a toml::Value, key: &str) -> Option<&'a str> {
    value.as_table()?.get(key)?.as_str()
}

fn json_string<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key)?.as_str()
}

fn json_bool(value: &Value, key: &str) -> Option<bool> {
    value.get(key)?.as_bool()
}

fn json_u64(value: &Value, key: &str) -> u64 {
    value.get(key).and_then(Value::as_u64).unwrap_or_default()
}

fn json_i64(value: &Value, key: &str) -> Option<i64> {
    value.get(key)?.as_i64()
}

fn normalize_mcp_config_identifier(raw: &str, field_name: &str) -> Result<String> {
    let normalized = raw.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        anyhow::bail!("{field_name} cannot be empty");
    }
    if normalized.len() > 128 {
        anyhow::bail!("{field_name} exceeds maximum bytes ({} > 128)", normalized.len());
    }
    if !normalized
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | ':'))
    {
        anyhow::bail!("{field_name} contains invalid identifier `{raw}`");
    }
    Ok(normalized)
}

fn normalize_mcp_namespace(raw: &str, field_name: &str) -> Result<String> {
    let namespace = normalize_mcp_config_identifier(raw, field_name)?;
    let first_segment = namespace.split(['.', ':']).next().unwrap_or(namespace.as_str());
    if matches!(
        first_segment,
        "palyra" | "builtin" | "skill" | "skills" | "plugin" | "plugins" | "mcp"
    ) {
        anyhow::bail!(
            "{field_name} uses reserved namespace `{first_segment}`; choose an external server namespace"
        );
    }
    Ok(namespace)
}

fn validate_mcp_env_name(raw: &str) -> Result<String> {
    let name = raw.trim();
    if name.is_empty() {
        anyhow::bail!("env vault ref name cannot be empty");
    }
    let mut chars = name.chars();
    let first = chars.next().expect("non-empty env name should have a first char");
    if !(first == '_' || first.is_ascii_uppercase()) {
        anyhow::bail!("env vault ref name must start with '_' or an uppercase ASCII letter");
    }
    if !chars.all(|ch| ch == '_' || ch.is_ascii_uppercase() || ch.is_ascii_digit()) {
        anyhow::bail!(
            "env vault ref name must contain only uppercase ASCII letters, digits, or '_'"
        );
    }
    Ok(name.to_owned())
}

fn validate_mcp_vault_ref(raw: &str) -> Result<String> {
    let vault_ref = raw.trim();
    if vault_ref.is_empty() {
        anyhow::bail!("env vault ref value cannot be empty");
    }
    VaultRef::parse(vault_ref)
        .map_err(|error| anyhow!("env vault ref `{vault_ref}` is invalid: {error}"))?;
    Ok(vault_ref.to_owned())
}

fn run_mcp_serve(
    connection: AcpConnectionArgs,
    session_defaults: AcpSessionDefaultsArgs,
    read_only: bool,
    allow_sensitive_tools: bool,
) -> Result<()> {
    let root_context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for MCP command"))?;
    let overrides = app::ConnectionOverrides {
        grpc_url: connection.grpc_url,
        token: connection.token,
        principal: connection.principal,
        device_id: connection.device_id,
        channel: connection.channel,
        daemon_url: None,
    };
    let user_connection =
        root_context.resolve_grpc_connection(overrides.clone(), app::ConnectionDefaults::USER)?;
    let admin_connection =
        root_context.resolve_grpc_connection(overrides.clone(), app::ConnectionDefaults::ADMIN)?;
    let mut backend = LiveMcpBackend {
        runtime: build_runtime()?,
        user_connection,
        admin_connection,
        control_plane_overrides: overrides,
        session_defaults: acp_bridge::AcpSessionDefaults {
            session_key: session_defaults.session_key,
            session_label: session_defaults.session_label,
            require_existing: session_defaults.require_existing,
            reset_session: session_defaults.reset_session,
        },
        read_only,
        allow_sensitive_tools,
    };
    let mut reader = BufReader::new(io::stdin().lock());
    let mut writer = io::stdout().lock();
    while let Some(request) = read_mcp_message(&mut reader)? {
        if let Some(response) = handle_mcp_request(&mut backend, request)? {
            write_mcp_message(&mut writer, &response)?;
        }
    }
    Ok(())
}

/// Tool-execution seam between JSON-RPC request handling and the live daemon
/// clients, so `handle_mcp_request` can be tested with fake backends.
trait McpBackend {
    fn read_only(&self) -> bool;

    fn call_tool(&mut self, name: &str, arguments: &Value) -> Result<Value>;
}

struct LiveMcpBackend {
    runtime: tokio::runtime::Runtime,
    user_connection: AgentConnection,
    admin_connection: AgentConnection,
    control_plane_overrides: app::ConnectionOverrides,
    session_defaults: acp_bridge::AcpSessionDefaults,
    read_only: bool,
    allow_sensitive_tools: bool,
}

impl McpBackend for LiveMcpBackend {
    fn read_only(&self) -> bool {
        self.read_only
    }

    fn call_tool(&mut self, name: &str, arguments: &Value) -> Result<Value> {
        match name {
            TOOL_SESSIONS_LIST => self.sessions_list(arguments),
            TOOL_SESSION_TRANSCRIPT_READ => self.session_transcript_read(arguments),
            TOOL_SESSION_EXPORT => self.session_export(arguments),
            TOOL_MEMORY_SEARCH => self.memory_search(arguments),
            TOOL_APPROVALS_LIST => self.approvals_list(arguments),
            TOOL_SESSION_CREATE => self.session_create(arguments),
            TOOL_SESSION_PROMPT => self.session_prompt(arguments),
            TOOL_APPROVAL_DECIDE => self.approval_decide(arguments),
            other => anyhow::bail!("unknown MCP tool `{other}`"),
        }
    }
}

impl LiveMcpBackend {
    fn operator_runtime(&self) -> client::operator::OperatorRuntime {
        client::operator::OperatorRuntime::new(self.user_connection.clone())
    }

    fn sessions_list(&mut self, arguments: &Value) -> Result<Value> {
        let args = expect_arguments_object(arguments, TOOL_SESSIONS_LIST)?;
        let after_session_key = opt_string_arg(args, "after_session_key")?;
        let include_archived = opt_bool_arg(args, "include_archived")?.unwrap_or(false);
        let limit = opt_u32_arg(args, "limit")?;
        let query = opt_string_arg(args, "query")?;
        let response = self.runtime.block_on(async {
            self.operator_runtime()
                .list_sessions(after_session_key, include_archived, limit, query)
                .await
        })?;
        Ok(json!({
            "sessions": response
                .sessions
                .iter()
                .map(session_summary_to_json)
                .collect::<Vec<Value>>(),
            "next_after_session_key": normalize_optional_text(response.next_after_session_key.as_str()),
            "include_archived": include_archived,
        }))
    }

    fn session_transcript_read(&mut self, arguments: &Value) -> Result<Value> {
        let args = expect_arguments_object(arguments, TOOL_SESSION_TRANSCRIPT_READ)?;
        let session_id =
            self.resolve_read_session_id_from_args(TOOL_SESSION_TRANSCRIPT_READ, args)?;
        let path = format!(
            "console/v1/chat/sessions/{}/transcript",
            percent_encode_component(session_id.as_str())
        );
        self.runtime.block_on(async {
            let context =
                client::control_plane::connect_admin_console(self.control_plane_overrides.clone())
                    .await?;
            context.client.get_json_value(path.as_str()).await.map_err(Into::into)
        })
    }

    fn session_export(&mut self, arguments: &Value) -> Result<Value> {
        let args = expect_arguments_object(arguments, TOOL_SESSION_EXPORT)?;
        let format = opt_string_arg(args, "format")?.unwrap_or_else(|| "json".to_owned());
        if !format.eq_ignore_ascii_case("json") && !format.eq_ignore_ascii_case("markdown") {
            anyhow::bail!("session_export format must be one of: json, markdown");
        }
        let session_id = self.resolve_read_session_id_from_args(TOOL_SESSION_EXPORT, args)?;
        let path = format!(
            "console/v1/chat/sessions/{}/export?format={}",
            percent_encode_component(session_id.as_str()),
            percent_encode_component(format.as_str())
        );
        self.runtime.block_on(async {
            let context =
                client::control_plane::connect_admin_console(self.control_plane_overrides.clone())
                    .await?;
            context.client.get_json_value(path.as_str()).await.map_err(Into::into)
        })
    }

    fn memory_search(&mut self, arguments: &Value) -> Result<Value> {
        let args = expect_arguments_object(arguments, TOOL_MEMORY_SEARCH)?;
        let query = required_string_arg(args, "query")?;
        if query.trim().is_empty() {
            anyhow::bail!("memory_search query cannot be empty");
        }
        let scope = opt_string_arg(args, "scope")?.unwrap_or_else(|| "principal".to_owned());
        let top_k = opt_u32_arg(args, "top_k")?.unwrap_or(5);
        let min_score = opt_f64_arg(args, "min_score")?.unwrap_or(0.0);
        if !min_score.is_finite() || !(0.0..=1.0).contains(&min_score) {
            anyhow::bail!("memory_search.min_score must be in range 0.0..=1.0");
        }
        let include_score_breakdown =
            opt_bool_arg(args, "include_score_breakdown")?.unwrap_or(false);
        let tags = opt_string_vec_arg(args, "tags")?;
        let sources = opt_string_vec_arg(args, "sources")?;
        let channel_arg = opt_string_arg(args, "channel")?;
        let session_arg = opt_string_arg(args, "session_id")?;
        let (channel, session_id) = resolve_memory_scope_for_mcp(
            scope.as_str(),
            channel_arg,
            session_arg,
            &self.user_connection,
        )?;
        let source_values = sources
            .into_iter()
            .map(|value| parse_memory_source_arg(value.as_str()).map(memory_source_to_proto))
            .collect::<Result<Vec<i32>>>()?;
        let mut request = Request::new(memory_v1::SearchMemoryRequest {
            v: CANONICAL_PROTOCOL_MAJOR,
            query,
            channel: channel.unwrap_or_default(),
            session_id: session_id.map(|ulid| common_v1::CanonicalId { ulid }),
            top_k,
            min_score,
            tags,
            sources: source_values,
            include_score_breakdown,
        });
        inject_run_stream_metadata(request.metadata_mut(), &self.user_connection)?;
        let grpc_url = self.user_connection.grpc_url.clone();
        let response = self.runtime.block_on(async move {
            let mut client =
                memory_v1::memory_service_client::MemoryServiceClient::connect(grpc_url.clone())
                    .await
                    .with_context(|| {
                        format!("failed to connect gateway gRPC endpoint {grpc_url}")
                    })?;
            client
                .search_memory(request)
                .await
                .context("failed to call memory SearchMemory")
                .map(|value| value.into_inner())
        })?;
        Ok(json!({
            "memory_store_kind": "durable_memory",
            "hit_count": response.hits.len(),
            "claim_boundary": mcp_memory_search_claim_boundary(response.hits.len()),
            "hits": response.hits.iter().map(memory_search_hit_to_json).collect::<Vec<Value>>(),
        }))
    }

    fn approvals_list(&mut self, arguments: &Value) -> Result<Value> {
        let args = expect_arguments_object(arguments, TOOL_APPROVALS_LIST)?;
        let after = opt_string_arg(args, "after_approval_id")?;
        if let Some(value) = after.as_deref() {
            validate_canonical_id(value)
                .context("approvals_list.after_approval_id must be a canonical ULID")?;
        }
        let limit = opt_u32_arg(args, "limit")?.unwrap_or(50);
        let since = opt_i64_arg(args, "since_unix_ms")?.unwrap_or_default();
        let until = opt_i64_arg(args, "until_unix_ms")?.unwrap_or_default();
        let subject = opt_string_arg(args, "subject_id")?;
        let principal = opt_string_arg(args, "principal")?;
        let decision = approval_decision_filter_arg(args, "decision")?;
        let subject_type = approval_subject_type_filter_arg(args, "subject_type")?;
        let mut request = Request::new(gateway_v1::ListApprovalsRequest {
            v: CANONICAL_PROTOCOL_MAJOR,
            after_approval_ulid: after.unwrap_or_default(),
            limit,
            since_unix_ms: since,
            until_unix_ms: until,
            subject_id: subject.unwrap_or_default(),
            principal: principal.unwrap_or_default(),
            decision,
            subject_type,
        });
        inject_run_stream_metadata(request.metadata_mut(), &self.admin_connection)?;
        let grpc_url = self.admin_connection.grpc_url.clone();
        let response = self.runtime.block_on(async move {
            let mut client = gateway_v1::approvals_service_client::ApprovalsServiceClient::connect(
                grpc_url.clone(),
            )
            .await
            .with_context(|| format!("failed to connect gateway gRPC endpoint {grpc_url}"))?;
            client
                .list_approvals(request)
                .await
                .context("failed to call approvals ListApprovals")
                .map(|value| value.into_inner())
        })?;
        Ok(json!({
            "approvals": response
                .approvals
                .iter()
                .map(approval_record_to_json)
                .collect::<Vec<Value>>(),
            "next_after_approval_id": normalize_optional_text(response.next_after_approval_ulid.as_str()),
        }))
    }

    fn session_create(&mut self, arguments: &Value) -> Result<Value> {
        let args = expect_arguments_object(arguments, TOOL_SESSION_CREATE)?;
        let response = self.resolve_session_with_defaults(args, false)?;
        let session =
            response.session.as_ref().context("ResolveSession returned empty session payload")?;
        Ok(json!({
            "session": session_summary_to_json(session),
            "created": response.created,
            "reset_applied": response.reset_applied,
        }))
    }

    fn session_prompt(&mut self, arguments: &Value) -> Result<Value> {
        let args = expect_arguments_object(arguments, TOOL_SESSION_PROMPT)?;
        let prompt = required_string_arg(args, "prompt")?;
        if prompt.trim().is_empty() {
            anyhow::bail!("session_prompt prompt cannot be empty");
        }
        // Per-call escalation is honored only when the server itself was started
        // with --allow-sensitive-tools; an MCP client cannot widen that grant.
        let allow_sensitive_tools = self.allow_sensitive_tools
            && opt_bool_arg(args, "allow_sensitive_tools")?.unwrap_or(false);
        let resolved = self.resolve_session_with_defaults(args, false)?;
        let session = resolved
            .session
            .as_ref()
            .context("ResolveSession returned empty session payload")?
            .clone();
        let run_input = build_agent_run_input(AgentRunInputArgs {
            session_id: session.session_id.clone(),
            session_key: None,
            session_label: None,
            require_existing: true,
            reset_session: false,
            run_id: None,
            prompt,
            allow_sensitive_tools,
            interrupt_active_run: false,
            approval_mode: AgentApprovalMode::Prompt,
            origin_kind: Some("mcp_stdio".to_owned()),
            origin_run_id: None,
            parameter_delta_json: None,
        })?;
        self.runtime.block_on(async {
            let runtime = self.operator_runtime();
            let mut stream = runtime.start_run_stream(run_input).await?;
            collect_mcp_run_stream(&session, &resolved, &mut stream).await
        })
    }

    fn approval_decide(&mut self, arguments: &Value) -> Result<Value> {
        let args = expect_arguments_object(arguments, TOOL_APPROVAL_DECIDE)?;
        let approval_id = required_string_arg(args, "approval_id")?;
        validate_canonical_id(approval_id.as_str())
            .context("approval_decide.approval_id must be a canonical ULID")?;
        let approved = required_bool_arg(args, "approved")?;
        let decision_scope =
            opt_string_arg(args, "decision_scope")?.unwrap_or_else(|| "once".to_owned());
        if !matches!(decision_scope.as_str(), "once" | "session" | "timeboxed") {
            anyhow::bail!(
                "approval_decide.decision_scope must be one of: once, session, timeboxed"
            );
        }
        let ttl_ms = opt_i64_arg(args, "decision_scope_ttl_ms")?;
        validate_mcp_approval_scope(decision_scope.as_str(), ttl_ms)?;
        let reason = opt_string_arg(args, "reason")?;
        let payload = self.runtime.block_on(async {
            self.operator_runtime()
                .decide_approval(approval_id, approved, decision_scope, ttl_ms, reason)
                .await
        })?;
        Ok(json!({
            "approval": payload.approval,
            "dm_pairing": payload.dm_pairing,
        }))
    }

    fn resolve_session_with_defaults(
        &mut self,
        args: &Map<String, Value>,
        require_existing_default: bool,
    ) -> Result<gateway_v1::ResolveSessionResponse> {
        let session_id = opt_string_arg(args, "session_id")?;
        let session_key = opt_string_arg(args, "session_key")?
            .or_else(|| self.session_defaults.session_key.clone())
            .unwrap_or_default();
        let session_label = opt_string_arg(args, "session_label")?
            .or_else(|| self.session_defaults.session_label.clone())
            .unwrap_or_default();
        let require_existing = opt_bool_arg(args, "require_existing")?
            .unwrap_or(require_existing_default || self.session_defaults.require_existing);
        let reset_session =
            opt_bool_arg(args, "reset_session")?.unwrap_or(self.session_defaults.reset_session);
        let request = SessionResolveInput {
            session_id: resolve_optional_canonical_id(session_id)?,
            session_key,
            session_label,
            require_existing,
            reset_session,
        };
        self.runtime.block_on(async { self.operator_runtime().resolve_session(request).await })
    }

    fn resolve_read_session_id_from_args(
        &mut self,
        tool_name: &str,
        args: &Map<String, Value>,
    ) -> Result<String> {
        reject_read_session_mutation_args(tool_name, args)?;
        let session_id = opt_string_arg(args, "session_id")?;
        if let Some(session_id) = resolve_optional_canonical_id(session_id)? {
            return Ok(session_id.ulid);
        }
        let session_key = opt_string_arg(args, "session_key")?
            .or_else(|| self.session_defaults.session_key.clone());
        if let Some(session_key) = session_key {
            return self.lookup_read_session_id(SessionReadSelector::Key(session_key));
        }
        let session_label = opt_string_arg(args, "session_label")?
            .or_else(|| self.session_defaults.session_label.clone());
        if let Some(session_label) = session_label {
            return self.lookup_read_session_id(SessionReadSelector::Label(session_label));
        }
        anyhow::bail!("{tool_name} requires session_id, session_key, or session_label")
    }

    fn lookup_read_session_id(&mut self, selector: SessionReadSelector) -> Result<String> {
        let mut after_session_key = None::<String>;
        loop {
            let response = self.runtime.block_on(async {
                self.operator_runtime()
                    .list_sessions(after_session_key.clone(), true, Some(200), None)
                    .await
            })?;
            if let Some(session_id) = response
                .sessions
                .iter()
                .find(|session| selector.matches(session))
                .and_then(|session| session.session_id.as_ref())
                .map(|session_id| session_id.ulid.clone())
            {
                return Ok(session_id);
            }
            let next_after_session_key = next_distinct_session_page_cursor(
                &after_session_key,
                response.next_after_session_key.as_str(),
            );
            if next_after_session_key.is_none() {
                break;
            }
            after_session_key = next_after_session_key;
        }
        anyhow::bail!("session not found for {}", selector.description())
    }
}

enum SessionReadSelector {
    Key(String),
    Label(String),
}

impl SessionReadSelector {
    fn matches(&self, session: &gateway_v1::SessionSummary) -> bool {
        match self {
            Self::Key(expected) => session.session_key == *expected,
            // Labels are not unique across a session's archived predecessors, so
            // label lookup only matches live (unarchived) sessions.
            Self::Label(expected) => {
                session.session_label == *expected && session.archived_at_unix_ms == 0
            }
        }
    }

    fn description(&self) -> String {
        match self {
            Self::Key(value) => format!("session_key `{value}`"),
            Self::Label(value) => format!("session_label `{value}`"),
        }
    }
}

/// Drains a run stream into a single MCP tool result.
///
/// A stdio tool call cannot block on interactive approval, so the stream is cut at
/// the first tool-approval request and reported as `approval_required`; the client
/// is expected to decide via the `approval_decide` tool and re-prompt.
async fn collect_mcp_run_stream(
    session: &gateway_v1::SessionSummary,
    resolved: &gateway_v1::ResolveSessionResponse,
    stream: &mut client::operator::ManagedRunStream,
) -> Result<Value> {
    let mut events = Vec::new();
    let mut assistant_text = String::new();
    let mut terminal_status = None::<String>;
    let mut approval_request = None::<Value>;
    while let Some(event) = stream.next_event().await? {
        events.push(mcp_run_stream_event_to_json(&event));
        match event.body.as_ref() {
            Some(common_v1::run_stream_event::Body::ModelToken(token))
                if !token.token.is_empty() =>
            {
                assistant_text.push_str(token.token.as_str());
            }
            Some(common_v1::run_stream_event::Body::ToolApprovalRequest(request)) => {
                approval_request = Some(tool_approval_request_to_json(request));
                break;
            }
            Some(common_v1::run_stream_event::Body::Status(status)) => {
                terminal_status = Some(stream_status_kind_to_text(status.kind).to_owned());
                if is_terminal_stream_status(status.kind) {
                    break;
                }
            }
            _ => {}
        }
    }

    let status = if approval_request.is_some() {
        "approval_required"
    } else if terminal_status.as_deref() == Some("failed") {
        "failed"
    } else {
        "completed"
    };

    Ok(json!({
        "status": status,
        "run_id": stream.run_id(),
        "session": session_summary_to_json(session),
        "created": resolved.created,
        "reset_applied": resolved.reset_applied,
        "assistant_text": if assistant_text.is_empty() { None::<String> } else { Some(assistant_text) },
        "approval_request": approval_request,
        "events": events,
    }))
}

/// Handles one MCP JSON-RPC request; `Ok(None)` means no response is owed
/// (notifications). Tool failures become in-band `isError` results, not RPC errors.
fn handle_mcp_request(backend: &mut dyn McpBackend, request: Value) -> Result<Option<Value>> {
    let Some(method) = request.get("method").and_then(Value::as_str) else {
        return Ok(request
            .get("id")
            .cloned()
            .map(|id| rpc_error(id, -32600, "invalid_request", "request is missing method")));
    };
    let id = request.get("id").cloned();
    match method {
        "initialize" => Ok(id.map(|request_id| {
            json!({
                "jsonrpc": JSONRPC_VERSION,
                "id": request_id,
                "result": {
                    "protocolVersion": MCP_PROTOCOL_VERSION,
                    "capabilities": {
                        "tools": {
                            "listChanged": false,
                        }
                    },
                    "serverInfo": {
                        "name": "palyra-cli",
                        "version": env!("CARGO_PKG_VERSION"),
                    },
                    "instructions": mcp_server_instructions(backend.read_only()),
                }
            })
        })),
        "notifications/initialized" => Ok(None),
        "ping" => Ok(id.map(|request_id| {
            json!({
                "jsonrpc": JSONRPC_VERSION,
                "id": request_id,
                "result": {}
            })
        })),
        "tools/list" => Ok(id.map(|request_id| {
            json!({
                "jsonrpc": JSONRPC_VERSION,
                "id": request_id,
                "result": {
                    "tools": registered_tools(backend.read_only()),
                }
            })
        })),
        "tools/call" => {
            // A tools/call without an id is a JSON-RPC notification; the spec
            // forbids responding to it.
            let Some(request_id) = id else {
                return Ok(None);
            };
            let params = request.get("params").and_then(Value::as_object);
            let Some(params) = params else {
                return Ok(Some(rpc_error(
                    request_id,
                    -32602,
                    "invalid_params",
                    "tools/call requires params object",
                )));
            };
            let Some(name) = params.get("name").and_then(Value::as_str) else {
                return Ok(Some(rpc_error(
                    request_id,
                    -32602,
                    "invalid_params",
                    "tools/call params.name must be a string",
                )));
            };
            let arguments = params.get("arguments").cloned().unwrap_or_else(|| json!({}));
            if !is_registered_mcp_tool(name) {
                return Ok(Some(json!({
                    "jsonrpc": JSONRPC_VERSION,
                    "id": request_id,
                    "result": tool_error_payload(unregistered_mcp_tool_message(name)),
                })));
            }
            if backend.read_only() && is_mutating_tool(name) {
                return Ok(Some(json!({
                    "jsonrpc": JSONRPC_VERSION,
                    "id": request_id,
                    "result": tool_error_payload(format!(
                        "tool `{name}` is unavailable because the MCP server is running in --read-only mode"
                    )),
                })));
            }
            let tool_result = match backend.call_tool(name, &arguments) {
                Ok(value) => tool_success_payload(value),
                Err(error) => tool_error_payload(format_mcp_tool_error(&error)),
            };
            Ok(Some(json!({
                "jsonrpc": JSONRPC_VERSION,
                "id": request_id,
                "result": tool_result,
            })))
        }
        _ => Ok(id.map(|request_id| {
            rpc_error(
                request_id,
                -32601,
                "method_not_found",
                format!("unsupported MCP method `{method}`"),
            )
        })),
    }
}

/// Reads one Content-Length framed JSON-RPC message; returns `Ok(None)` on clean EOF.
///
/// # Errors
/// Returns a validation error for malformed headers, a missing Content-Length,
/// a truncated body, or an unparsable JSON payload.
fn read_mcp_message(reader: &mut dyn BufRead) -> Result<Option<Value>> {
    let mut content_length = None::<usize>;
    loop {
        let mut line = String::new();
        let bytes_read = reader.read_line(&mut line).context("failed to read MCP header line")?;
        if bytes_read == 0 {
            return Ok(None);
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            break;
        }
        let Some((name, value)) = trimmed.split_once(':') else {
            anyhow::bail!("invalid MCP header line `{trimmed}`");
        };
        if name.eq_ignore_ascii_case("content-length") {
            content_length = Some(value.trim().parse::<usize>().with_context(|| {
                format!("invalid MCP input: invalid Content-Length value `{}`", value.trim())
            })?);
        }
    }
    let content_length =
        content_length.context("invalid MCP input: missing Content-Length header")?;
    let mut payload = vec![0_u8; content_length];
    reader
        .read_exact(payload.as_mut_slice())
        .context("invalid MCP input: failed to read framed MCP payload")?;
    serde_json::from_slice::<Value>(payload.as_slice())
        .context("invalid MCP input: failed to parse MCP JSON payload")
        .map(Some)
}

/// Writes one Content-Length framed JSON-RPC message and flushes the writer.
fn write_mcp_message(writer: &mut dyn Write, payload: &Value) -> Result<()> {
    let encoded =
        serde_json::to_vec(payload).context("failed to serialize MCP response payload")?;
    write!(writer, "Content-Length: {}\r\n\r\n", encoded.len())
        .context("failed to write MCP response header")?;
    writer.write_all(encoded.as_slice()).context("failed to write MCP response body")?;
    writer.flush().context("failed to flush MCP response")
}

fn registered_tools(read_only: bool) -> Vec<Value> {
    REGISTERED_MCP_TOOLS
        .iter()
        .copied()
        .filter(|name| !read_only || !is_mutating_tool(name))
        .map(tool_definition)
        .collect()
}

fn is_registered_mcp_tool(name: &str) -> bool {
    REGISTERED_MCP_TOOLS.contains(&name)
}

fn mcp_server_instructions(read_only: bool) -> String {
    let mode = if read_only {
        "Read-only MCP facade over Palyra sessions, transcripts, approvals, and memory."
    } else {
        "MCP facade over Palyra sessions, approvals, memory, and approval-aware mutations."
    };
    format!("{mode} {MCP_SERVER_SCOPE_NOTE}")
}

fn unregistered_mcp_tool_message(name: &str) -> String {
    format!("tool `{name}` is not registered by this MCP server. {MCP_SERVER_SCOPE_NOTE}")
}

fn tool_definition(name: &str) -> Value {
    match name {
        TOOL_SESSIONS_LIST => json!({
            "name": TOOL_SESSIONS_LIST,
            "title": "List sessions",
            "description": "List visible Palyra sessions for the current principal and channel scope.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "after_session_key": { "type": "string" },
                    "include_archived": { "type": "boolean" },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 200 },
                    "query": { "type": "string" },
                },
                "additionalProperties": false,
            },
        }),
        TOOL_SESSION_TRANSCRIPT_READ => json!({
            "name": TOOL_SESSION_TRANSCRIPT_READ,
            "title": "Read session transcript",
            "description": "Read the transcript payload for a resolved session.",
            "inputSchema": read_session_locator_schema(),
        }),
        TOOL_SESSION_EXPORT => json!({
            "name": TOOL_SESSION_EXPORT,
            "title": "Export session",
            "description": "Export a resolved session as JSON or Markdown.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "session_id": { "type": "string" },
                    "session_key": { "type": "string" },
                    "session_label": { "type": "string" },
                    "format": {
                        "type": "string",
                        "enum": ["json", "markdown"],
                    },
                },
                "additionalProperties": false,
            },
        }),
        TOOL_MEMORY_SEARCH => json!({
            "name": TOOL_MEMORY_SEARCH,
            "title": "Search memory",
            "description": "Search durable Palyra memory with the same access controls used by the CLI. This does not search prior session transcripts.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": { "type": "string" },
                    "scope": { "type": "string", "enum": ["principal", "channel", "session"] },
                    "channel": { "type": "string" },
                    "session_id": { "type": "string" },
                    "top_k": { "type": "integer", "minimum": 1, "maximum": 100 },
                    "min_score": { "type": "number", "minimum": 0, "maximum": 1 },
                    "include_score_breakdown": { "type": "boolean" },
                    "tags": { "type": "array", "items": { "type": "string" } },
                    "sources": { "type": "array", "items": { "type": "string" } },
                },
                "required": ["query"],
                "additionalProperties": false,
            },
        }),
        TOOL_APPROVALS_LIST => json!({
            "name": TOOL_APPROVALS_LIST,
            "title": "List approvals",
            "description": "List approval records visible to the current admin-capable connection.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "after_approval_id": { "type": "string" },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 200 },
                    "since_unix_ms": { "type": "integer" },
                    "until_unix_ms": { "type": "integer" },
                    "subject_id": { "type": "string" },
                    "principal": { "type": "string" },
                    "decision": {
                        "type": "string",
                        "enum": ["allow", "deny", "timeout", "error"],
                    },
                    "subject_type": {
                        "type": "string",
                        "enum": [
                            "tool",
                            "channel_send",
                            "secret_access",
                            "browser_action",
                            "node_capability",
                            "device_pairing"
                        ],
                    },
                },
                "additionalProperties": false,
            },
        }),
        TOOL_SESSION_CREATE => json!({
            "name": TOOL_SESSION_CREATE,
            "title": "Create or resolve session",
            "description": "Resolve a Palyra session using the same defaults and binding rules as ACP.",
            "inputSchema": session_locator_schema(),
        }),
        TOOL_SESSION_PROMPT => json!({
            "name": TOOL_SESSION_PROMPT,
            "title": "Send prompt",
            "description": "Send a prompt into a resolved session and stream until completion or approval wait.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "session_id": { "type": "string" },
                    "session_key": { "type": "string" },
                    "session_label": { "type": "string" },
                    "require_existing": { "type": "boolean" },
                    "reset_session": { "type": "boolean" },
                    "prompt": { "type": "string" },
                    "allow_sensitive_tools": { "type": "boolean" },
                },
                "required": ["prompt"],
                "additionalProperties": false,
            },
        }),
        TOOL_APPROVAL_DECIDE => json!({
            "name": TOOL_APPROVAL_DECIDE,
            "title": "Resolve approval",
            "description": "Approve or deny a pending approval using the existing Palyra approval model.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "approval_id": { "type": "string" },
                    "approved": { "type": "boolean" },
                    "decision_scope": {
                        "type": "string",
                        "enum": ["once", "session", "timeboxed"],
                    },
                    "decision_scope_ttl_ms": { "type": "integer", "minimum": 1 },
                    "reason": { "type": "string" },
                },
                "required": ["approval_id", "approved"],
                "additionalProperties": false,
            },
        }),
        other => json!({
            "name": other,
            "title": other,
            "description": "Undocumented MCP tool",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "additionalProperties": false,
            },
        }),
    }
}

fn mcp_memory_search_claim_boundary(hit_count: usize) -> &'static str {
    if hit_count == 0 {
        MCP_MEMORY_SEARCH_HITS_ABSENT_CLAIM_BOUNDARY
    } else {
        MCP_MEMORY_SEARCH_HITS_PRESENT_CLAIM_BOUNDARY
    }
}

fn session_locator_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "session_id": { "type": "string" },
            "session_key": { "type": "string" },
            "session_label": { "type": "string" },
            "require_existing": { "type": "boolean" },
            "reset_session": { "type": "boolean" },
        },
        "additionalProperties": false,
    })
}

fn read_session_locator_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "session_id": { "type": "string" },
            "session_key": { "type": "string" },
            "session_label": { "type": "string" },
        },
        "additionalProperties": false,
    })
}

fn rpc_error(id: Value, code: i64, kind: &str, message: impl Into<String>) -> Value {
    let message = message.into();
    json!({
        "jsonrpc": JSONRPC_VERSION,
        "id": id,
        "error": {
            "code": code,
            "message": message,
            "data": {
                "kind": kind,
            },
        },
    })
}

fn tool_success_payload(value: Value) -> Value {
    let text = serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string());
    json!({
        "content": [{
            "type": "text",
            "text": text,
        }],
        "structuredContent": value,
        "isError": false,
    })
}

fn tool_error_payload(message: impl Into<String>) -> Value {
    let message = message.into();
    json!({
        "content": [{
            "type": "text",
            "text": message,
        }],
        "isError": true,
    })
}

fn format_mcp_tool_error(error: &anyhow::Error) -> String {
    sanitize_diagnostic_error(format!("{error:#}").as_str())
}

fn is_mutating_tool(name: &str) -> bool {
    matches!(name, TOOL_SESSION_CREATE | TOOL_SESSION_PROMPT | TOOL_APPROVAL_DECIDE)
}

fn expect_arguments_object<'a>(
    value: &'a Value,
    tool_name: &str,
) -> Result<&'a Map<String, Value>> {
    value.as_object().ok_or_else(|| anyhow!("tool `{tool_name}` requires an arguments object"))
}

fn reject_read_session_mutation_args(tool_name: &str, args: &Map<String, Value>) -> Result<()> {
    for key in ["require_existing", "reset_session"] {
        if args.contains_key(key) {
            anyhow::bail!(
                "{tool_name} does not accept `{key}` because read tools must not mutate session state"
            );
        }
    }
    Ok(())
}

fn required_string_arg(args: &Map<String, Value>, key: &str) -> Result<String> {
    opt_string_arg(args, key)?.ok_or_else(|| anyhow!("missing required string argument `{key}`"))
}

fn opt_string_arg(args: &Map<String, Value>, key: &str) -> Result<Option<String>> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(normalize_optional_text(value.as_str())),
        Some(_) => anyhow::bail!("argument `{key}` must be a string"),
    }
}

fn required_bool_arg(args: &Map<String, Value>, key: &str) -> Result<bool> {
    opt_bool_arg(args, key)?.ok_or_else(|| anyhow!("missing required boolean argument `{key}`"))
}

fn opt_bool_arg(args: &Map<String, Value>, key: &str) -> Result<Option<bool>> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(value)) => Ok(Some(*value)),
        Some(_) => anyhow::bail!("argument `{key}` must be a boolean"),
    }
}

fn opt_u32_arg(args: &Map<String, Value>, key: &str) -> Result<Option<u32>> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => {
            let value = value
                .as_u64()
                .ok_or_else(|| anyhow!("argument `{key}` must be an unsigned integer"))?;
            u32::try_from(value)
                .map(Some)
                .with_context(|| format!("argument `{key}` exceeds u32 range"))
        }
        Some(_) => anyhow::bail!("argument `{key}` must be an unsigned integer"),
    }
}

fn opt_i64_arg(args: &Map<String, Value>, key: &str) -> Result<Option<i64>> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => value
            .as_i64()
            .map(Some)
            .ok_or_else(|| anyhow!("argument `{key}` must be a signed integer")),
        Some(_) => anyhow::bail!("argument `{key}` must be a signed integer"),
    }
}

fn opt_f64_arg(args: &Map<String, Value>, key: &str) -> Result<Option<f64>> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => {
            value.as_f64().map(Some).ok_or_else(|| anyhow!("argument `{key}` must be a number"))
        }
        Some(_) => anyhow::bail!("argument `{key}` must be a number"),
    }
}

fn opt_string_vec_arg(args: &Map<String, Value>, key: &str) -> Result<Vec<String>> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(values)) => values
            .iter()
            .enumerate()
            .map(|(index, value)| match value {
                Value::String(value) => {
                    Ok(normalize_optional_text(value.as_str()).unwrap_or_default())
                }
                _ => anyhow::bail!("argument `{key}[{index}]` must be a string"),
            })
            // Drop entries normalized to empty, but keep Err entries so collect()
            // still surfaces type errors instead of silently filtering them away.
            .filter(|entry| match entry {
                Ok(value) => !value.is_empty(),
                Err(_) => true,
            })
            .collect(),
        Some(_) => anyhow::bail!("argument `{key}` must be an array of strings"),
    }
}

fn resolve_memory_scope_for_mcp(
    scope: &str,
    channel: Option<String>,
    session_id: Option<String>,
    connection: &AgentConnection,
) -> Result<(Option<String>, Option<String>)> {
    let scope = scope.trim().to_ascii_lowercase();
    let channel = normalize_optional_owned_text(channel);
    let session_id = normalize_optional_owned_text(session_id);
    match scope.as_str() {
        "principal" | "" => Ok((channel, None)),
        "channel" => {
            let channel = channel.or_else(|| Some(connection.channel.clone()));
            Ok((channel, None))
        }
        "session" => {
            let session_id = session_id
                .ok_or_else(|| anyhow!("memory_search scope=session requires session_id"))?;
            validate_canonical_id(session_id.as_str())
                .context("memory_search.session_id must be a canonical ULID")?;
            Ok((channel.or_else(|| Some(connection.channel.clone())), Some(session_id)))
        }
        other => anyhow::bail!(
            "memory_search scope must be one of: principal, channel, session (got `{other}`)"
        ),
    }
}

fn parse_memory_source_arg(value: &str) -> Result<MemorySourceArg> {
    match value.trim().to_ascii_lowercase().as_str() {
        "tapeusermessage" | "tape_user_message" | "tape:user_message" => {
            Ok(MemorySourceArg::TapeUserMessage)
        }
        "tapetoolresult" | "tape_tool_result" | "tape:tool_result" => {
            Ok(MemorySourceArg::TapeToolResult)
        }
        "summary" => Ok(MemorySourceArg::Summary),
        "manual" => Ok(MemorySourceArg::Manual),
        "import" => Ok(MemorySourceArg::Import),
        other => anyhow::bail!(
            "unsupported memory source `{other}`; expected tape_user_message, tape_tool_result, summary, manual, or import"
        ),
    }
}

fn approval_decision_filter_arg(args: &Map<String, Value>, key: &str) -> Result<i32> {
    let value = opt_string_arg(args, key)?;
    Ok(match value.as_deref().map(str::to_ascii_lowercase).as_deref() {
        None => gateway_v1::ApprovalDecision::Unspecified as i32,
        Some("allow") => gateway_v1::ApprovalDecision::Allow as i32,
        Some("deny") => gateway_v1::ApprovalDecision::Deny as i32,
        Some("timeout") => gateway_v1::ApprovalDecision::Timeout as i32,
        Some("error") => gateway_v1::ApprovalDecision::Error as i32,
        Some(other) => anyhow::bail!(
            "unsupported approval decision `{other}`; expected allow, deny, timeout, or error"
        ),
    })
}

fn approval_subject_type_filter_arg(args: &Map<String, Value>, key: &str) -> Result<i32> {
    let value = opt_string_arg(args, key)?;
    Ok(match value
        .as_deref()
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        None => gateway_v1::ApprovalSubjectType::Unspecified as i32,
        Some("tool") => gateway_v1::ApprovalSubjectType::Tool as i32,
        Some("channelsend") | Some("channel_send") => {
            gateway_v1::ApprovalSubjectType::ChannelSend as i32
        }
        Some("secretaccess") | Some("secret_access") => {
            gateway_v1::ApprovalSubjectType::SecretAccess as i32
        }
        Some("browseraction") | Some("browser_action") => {
            gateway_v1::ApprovalSubjectType::BrowserAction as i32
        }
        Some("nodecapability") | Some("node_capability") => {
            gateway_v1::ApprovalSubjectType::NodeCapability as i32
        }
        Some("devicepairing") | Some("device_pairing") => {
            gateway_v1::ApprovalSubjectType::DevicePairing as i32
        }
        Some(other) => anyhow::bail!(
            "unsupported approval subject type `{other}`; expected tool, channel_send, secret_access, browser_action, node_capability, or device_pairing"
        ),
    })
}

fn validate_mcp_approval_scope(scope: &str, ttl_ms: Option<i64>) -> Result<()> {
    match scope {
        "once" | "session" => {
            if ttl_ms.is_some() {
                anyhow::bail!(
                    "approval_decide.decision_scope_ttl_ms is only valid when decision_scope=timeboxed"
                );
            }
        }
        "timeboxed" => {
            let ttl_ms = ttl_ms.ok_or_else(|| {
                anyhow!("approval_decide.decision_scope_ttl_ms is required for timeboxed decisions")
            })?;
            if ttl_ms <= 0 {
                anyhow::bail!("approval_decide.decision_scope_ttl_ms must be greater than zero");
            }
        }
        other => anyhow::bail!(
            "approval_decide.decision_scope must be one of: once, session, timeboxed (got `{other}`)"
        ),
    }
    Ok(())
}

fn session_summary_to_json(session: &gateway_v1::SessionSummary) -> Value {
    json!({
        "session_id": session.session_id.as_ref().map(|value| value.ulid.clone()),
        "session_key": normalize_optional_text(session.session_key.as_str()),
        "session_label": normalize_optional_text(session.session_label.as_str()),
        "title": normalize_optional_text(session.title.as_str()),
        "title_source": normalize_optional_text(session.title_source.as_str()),
        "title_generator_version": normalize_optional_text(session.title_generator_version.as_str()),
        "preview": normalize_optional_text(session.preview.as_str()),
        "preview_state": normalize_optional_text(session.preview_state.as_str()),
        "last_intent": normalize_optional_text(session.last_intent.as_str()),
        "last_summary": normalize_optional_text(session.last_summary.as_str()),
        "match_snippet": normalize_optional_text(session.match_snippet.as_str()),
        "branch_state": normalize_optional_text(session.branch_state.as_str()),
        "parent_session_id": session.parent_session_id.as_ref().map(|value| value.ulid.clone()),
        "last_run_state": normalize_optional_text(session.last_run_state.as_str()),
        "created_at_unix_ms": session.created_at_unix_ms,
        "updated_at_unix_ms": session.updated_at_unix_ms,
        "last_run_id": session.last_run_id.as_ref().map(|value| value.ulid.clone()),
        "archived_at_unix_ms": optional_unix_ms_json_value(session.archived_at_unix_ms),
    })
}

fn optional_unix_ms_json_value(value: i64) -> Value {
    if value <= 0 {
        Value::Null
    } else {
        json!(value)
    }
}

fn tool_approval_request_to_json(request: &common_v1::ToolApprovalRequest) -> Value {
    let prompt = request.prompt.as_ref().map(|prompt| {
        let details_json = if prompt.details_json.is_empty() {
            json!({})
        } else {
            serde_json::from_slice::<Value>(prompt.details_json.as_slice()).unwrap_or_else(|_| {
                json!({
                    "raw": String::from_utf8_lossy(prompt.details_json.as_slice()).to_string(),
                })
            })
        };
        json!({
            "title": normalize_optional_text(prompt.title.as_str()),
            "risk_level": approval_risk_to_text(prompt.risk_level),
            "subject_id": normalize_optional_text(prompt.subject_id.as_str()),
            "summary": normalize_optional_text(prompt.summary.as_str()),
            "timeout_seconds": prompt.timeout_seconds,
            "policy_explanation": normalize_optional_text(prompt.policy_explanation.as_str()),
            "options": prompt.options.iter().map(|option| json!({
                "option_id": normalize_optional_text(option.option_id.as_str()),
                "label": normalize_optional_text(option.label.as_str()),
                "description": normalize_optional_text(option.description.as_str()),
                "default_selected": option.default_selected,
                "decision_scope": approval_scope_to_text(option.decision_scope),
                "timebox_ttl_ms": option.timebox_ttl_ms,
            })).collect::<Vec<Value>>(),
            "details_json": details_json,
        })
    });
    json!({
        "proposal_id": request.proposal_id.as_ref().map(|value| value.ulid.clone()),
        "approval_id": request.approval_id.as_ref().map(|value| value.ulid.clone()),
        "tool_name": normalize_optional_text(request.tool_name.as_str()),
        "request_summary": normalize_optional_text(request.request_summary.as_str()),
        "approval_required": request.approval_required,
        "prompt": prompt,
    })
}

fn mcp_run_stream_event_to_json(event: &common_v1::RunStreamEvent) -> Value {
    let run_id = event.run_id.as_ref().map(|value| value.ulid.clone());
    match event.body.as_ref() {
        Some(common_v1::run_stream_event::Body::ModelToken(token)) => json!({
            "type": "model_token",
            "run_id": run_id,
            "token": token.token,
            "is_final": token.is_final,
        }),
        Some(common_v1::run_stream_event::Body::Status(status)) => json!({
            "type": "status",
            "run_id": run_id,
            "kind": stream_status_kind_to_text(status.kind),
            "message": normalize_optional_text(status.message.as_str()),
        }),
        Some(common_v1::run_stream_event::Body::ToolProposal(proposal)) => json!({
            "type": "tool_proposal",
            "run_id": run_id,
            "proposal_id": proposal.proposal_id.as_ref().map(|value| value.ulid.clone()),
            "tool_name": normalize_optional_text(proposal.tool_name.as_str()),
            "approval_required": proposal.approval_required,
        }),
        Some(common_v1::run_stream_event::Body::ToolDecision(decision)) => json!({
            "type": "tool_decision",
            "run_id": run_id,
            "proposal_id": decision.proposal_id.as_ref().map(|value| value.ulid.clone()),
            "kind": tool_decision_kind_to_text(decision.kind),
            "reason": normalize_optional_text(decision.reason.as_str()),
            "approval_required": decision.approval_required,
            "policy_enforced": decision.policy_enforced,
        }),
        Some(common_v1::run_stream_event::Body::ToolApprovalRequest(request)) => json!({
            "type": "tool_approval_request",
            "run_id": run_id,
            "request": tool_approval_request_to_json(request),
        }),
        Some(common_v1::run_stream_event::Body::ToolApprovalResponse(response)) => json!({
            "type": "tool_approval_response",
            "run_id": run_id,
            "proposal_id": response.proposal_id.as_ref().map(|value| value.ulid.clone()),
            "approval_id": response.approval_id.as_ref().map(|value| value.ulid.clone()),
            "approved": response.approved,
            "decision_scope": approval_scope_to_text(response.decision_scope),
            "decision_scope_ttl_ms": response.decision_scope_ttl_ms,
            "reason": normalize_optional_text(response.reason.as_str()),
        }),
        Some(common_v1::run_stream_event::Body::ToolResult(result)) => json!({
            "type": "tool_result",
            "run_id": run_id,
            "proposal_id": result.proposal_id.as_ref().map(|value| value.ulid.clone()),
            "success": result.success,
            "error": normalize_optional_text(result.error.as_str()),
        }),
        Some(common_v1::run_stream_event::Body::ToolAttestation(attestation)) => json!({
            "type": "tool_attestation",
            "run_id": run_id,
            "proposal_id": attestation.proposal_id.as_ref().map(|value| value.ulid.clone()),
            "attestation_id": attestation.attestation_id.as_ref().map(|value| value.ulid.clone()),
            "timed_out": attestation.timed_out,
            "executor": normalize_optional_text(attestation.executor.as_str()),
        }),
        Some(common_v1::run_stream_event::Body::A2uiUpdate(update)) => json!({
            "type": "a2ui_update",
            "run_id": run_id,
            "surface": normalize_optional_text(update.surface.as_str()),
            "version": update.v,
        }),
        Some(common_v1::run_stream_event::Body::JournalEvent(event)) => json!({
            "type": "journal_event",
            "run_id": run_id,
            "event_id": event.event_id.as_ref().map(|value| value.ulid.clone()),
            "kind": event.kind,
            "actor": event.actor,
        }),
        None => json!({
            "type": "unknown",
            "run_id": run_id,
        }),
    }
}

// Percent-encodes everything outside the RFC 3986 unreserved set so caller-supplied
// ids embed safely into console URL paths.
fn percent_encode_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push_str(format!("{byte:02X}").as_str());
        }
    }
    encoded
}

fn normalize_optional_text(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_owned())
    }
}

fn next_distinct_session_page_cursor(
    current_after_session_key: &Option<String>,
    next_after_session_key: &str,
) -> Option<String> {
    let next_after_session_key = normalize_optional_text(next_after_session_key);
    if next_after_session_key == *current_after_session_key {
        None
    } else {
        next_after_session_key
    }
}

#[cfg(test)]
mod tests {
    use crate::cli::{McpApprovalProfileArg, McpTrustLevelArg};
    use crate::output::{classify_error, CliExitCode};

    use super::*;

    struct TestBackend {
        read_only: bool,
        last_call: Option<(String, Value)>,
        response: Value,
    }

    impl McpBackend for TestBackend {
        fn read_only(&self) -> bool {
            self.read_only
        }

        fn call_tool(&mut self, name: &str, arguments: &Value) -> Result<Value> {
            self.last_call = Some((name.to_owned(), arguments.clone()));
            Ok(self.response.clone())
        }
    }

    struct ErrorBackend;

    impl McpBackend for ErrorBackend {
        fn read_only(&self) -> bool {
            false
        }

        fn call_tool(&mut self, _name: &str, _arguments: &Value) -> Result<Value> {
            Err(anyhow!("status: permission denied: authorization token is invalid")
                .context("failed to call ListSessions"))
        }
    }

    fn stdio_registry_args() -> McpRegistryMutateArgs {
        McpRegistryMutateArgs {
            id: "Docs".to_owned(),
            path: None,
            transport: McpTransportArg::Stdio,
            namespace: Some("docs".to_owned()),
            command: Some("mcp-docs".to_owned()),
            args: vec!["--root".to_owned(), "docs".to_owned()],
            url: None,
            env_vault_refs: vec!["DOCS_TOKEN=global/docs-token".to_owned()],
            trust_level: McpTrustLevelArg::Workspace,
            approval_profile: McpApprovalProfileArg::RequireApproval,
            egress_policy: McpEgressPolicyArg::DenyAll,
            egress_allowlist: Vec::new(),
            tool_allowlist: vec!["search".to_owned()],
            tool_denylist: Vec::new(),
            enabled: false,
            backups: 5,
            json: false,
        }
    }

    #[test]
    fn mcp_registry_table_uses_vault_refs_and_canonical_namespace() {
        let args = stdio_registry_args();
        let server = mcp_server_table(&args).expect("valid MCP registry args should encode");
        assert_eq!(toml_table_string(&server, "id"), Some("docs"));
        assert_eq!(toml_table_string(&server, "namespace"), Some("docs"));
        assert_eq!(toml_table_string(&server, "transport"), Some("stdio"));
        assert_eq!(toml_table_string(&server, "command"), Some("mcp-docs"));

        let mut document = toml::Value::Table(toml::map::Map::new());
        mcp_servers_array_mut(&mut document).expect("mcp section should be created").push(server);
        validate_daemon_compatible_document(&document)
            .expect("generated MCP registry document should match daemon schema");
    }

    #[test]
    fn mcp_registry_rejects_plain_env_secret_and_reserved_namespace() {
        let mut args = stdio_registry_args();
        args.env_vault_refs = vec!["DOCS_TOKEN=plain-secret".to_owned()];
        let error = mcp_server_table(&args).expect_err("plain env secret should be rejected");
        assert!(error.to_string().contains("env vault ref"));

        let mut args = stdio_registry_args();
        args.namespace = Some("palyra.memory".to_owned());
        let error = mcp_server_table(&args).expect_err("reserved namespace should be rejected");
        assert!(error.to_string().contains("reserved namespace"));
    }

    #[test]
    fn mcp_registry_document_rejects_duplicate_namespace_and_env_table() {
        let first = mcp_server_table(&stdio_registry_args())
            .expect("valid MCP registry args should encode");
        let mut second_args = stdio_registry_args();
        second_args.id = "wiki".to_owned();
        second_args.namespace = Some("docs".to_owned());
        let second =
            mcp_server_table(&second_args).expect("valid duplicate namespace server should encode");

        let mut document = toml::Value::Table(toml::map::Map::new());
        let servers = mcp_servers_array_mut(&mut document).expect("mcp section should be created");
        servers.push(first);
        servers.push(second);
        let error = validate_mcp_registry_document(&document)
            .expect_err("duplicate namespace should be rejected before persist");
        assert!(error.to_string().contains("namespace"));
        assert!(error.to_string().contains("duplicates"));

        let mut server = mcp_server_table(&stdio_registry_args())
            .expect("valid MCP registry args should encode");
        server
            .as_table_mut()
            .expect("server should be a table")
            .insert("env".to_owned(), toml::Value::Table(toml::map::Map::new()));
        let mut document = toml::Value::Table(toml::map::Map::new());
        mcp_servers_array_mut(&mut document).expect("mcp section should be created").push(server);
        let error = validate_mcp_registry_document(&document)
            .expect_err("inline env table should be rejected before persist");
        assert!(error.to_string().contains("env_vault_refs"));
    }

    #[test]
    fn tools_list_hides_mutations_in_read_only_mode() {
        let tools = registered_tools(true);
        let names = tools
            .iter()
            .filter_map(|tool| tool.get("name").and_then(Value::as_str))
            .collect::<Vec<_>>();
        assert!(names.contains(&TOOL_SESSIONS_LIST));
        assert!(!names.contains(&TOOL_SESSION_PROMPT));
        assert!(!names.contains(&TOOL_APPROVAL_DECIDE));
    }

    #[test]
    fn read_session_tool_schemas_exclude_mutating_resolution_controls() {
        for tool_name in [TOOL_SESSION_TRANSCRIPT_READ, TOOL_SESSION_EXPORT] {
            let tool = tool_definition(tool_name);
            let properties = tool
                .pointer("/inputSchema/properties")
                .and_then(Value::as_object)
                .expect("tool should expose object schema properties");
            assert!(properties.contains_key("session_id"));
            assert!(properties.contains_key("session_key"));
            assert!(properties.contains_key("session_label"));
            assert!(!properties.contains_key("require_existing"));
            assert!(!properties.contains_key("reset_session"));
        }
    }

    #[test]
    fn read_session_tools_reject_mutating_resolution_arguments() {
        let mut args = Map::new();
        args.insert("session_key".to_owned(), json!("ops:triage"));
        args.insert("require_existing".to_owned(), json!(false));
        let error = reject_read_session_mutation_args(TOOL_SESSION_EXPORT, &args)
            .expect_err("read tool should reject require_existing");
        assert!(error.to_string().contains("require_existing"));

        let mut args = Map::new();
        args.insert("session_key".to_owned(), json!("ops:triage"));
        args.insert("reset_session".to_owned(), json!(true));
        let error = reject_read_session_mutation_args(TOOL_SESSION_TRANSCRIPT_READ, &args)
            .expect_err("read tool should reject reset_session");
        assert!(error.to_string().contains("reset_session"));
    }

    #[test]
    fn read_session_pagination_guard_stops_on_repeated_cursor() {
        assert_eq!(
            next_distinct_session_page_cursor(&None, "session-a"),
            Some("session-a".to_owned())
        );
        assert_eq!(
            next_distinct_session_page_cursor(&Some("session-a".to_owned()), "session-b"),
            Some("session-b".to_owned())
        );
        assert_eq!(
            next_distinct_session_page_cursor(&Some("session-a".to_owned()), "session-a"),
            None
        );
        assert_eq!(next_distinct_session_page_cursor(&Some("session-a".to_owned()), "  "), None);
    }

    #[test]
    fn tools_call_rejects_mutation_when_read_only() {
        let mut backend =
            TestBackend { read_only: true, last_call: None, response: json!({"ok": true}) };
        let response = handle_mcp_request(
            &mut backend,
            json!({
                "jsonrpc": JSONRPC_VERSION,
                "id": 7,
                "method": "tools/call",
                "params": {
                    "name": TOOL_SESSION_PROMPT,
                    "arguments": { "prompt": "hi" }
                }
            }),
        )
        .expect("request should succeed")
        .expect("response should be present");
        let message = response["result"]["content"][0]["text"]
            .as_str()
            .expect("tool error text should be a string");

        assert_eq!(backend.last_call, None);
        assert_eq!(response["result"]["isError"], Value::Bool(true));
        assert!(message.contains("--read-only mode"), "{message}");
    }

    #[test]
    fn initialize_instructions_explain_mcp_server_scope() {
        let mut backend =
            TestBackend { read_only: false, last_call: None, response: json!({"ok": true}) };
        let response = handle_mcp_request(
            &mut backend,
            json!({
                "jsonrpc": JSONRPC_VERSION,
                "id": 6,
                "method": "initialize"
            }),
        )
        .expect("request should succeed")
        .expect("response should be present");
        let instructions =
            response["result"]["instructions"].as_str().expect("instructions should be a string");

        assert!(instructions.contains("MCP server facade"), "{instructions}");
        assert!(instructions.contains("does not import external MCP servers"), "{instructions}");
        assert!(instructions.contains("Palyra agent runs"), "{instructions}");
    }

    #[test]
    fn tools_call_rejects_unregistered_external_tool_with_scope_note() {
        let mut backend =
            TestBackend { read_only: false, last_call: None, response: json!({"ok": true}) };
        let response = handle_mcp_request(
            &mut backend,
            json!({
                "jsonrpc": JSONRPC_VERSION,
                "id": 9,
                "method": "tools/call",
                "params": {
                    "name": "ticket.read",
                    "arguments": { "ticket_id": "TICKET-085" }
                }
            }),
        )
        .expect("request should succeed")
        .expect("response should be present");
        let message = response["result"]["content"][0]["text"]
            .as_str()
            .expect("tool error text should be a string");

        assert_eq!(backend.last_call, None);
        assert_eq!(response["result"]["isError"], Value::Bool(true));
        assert!(message.contains("ticket.read"), "{message}");
        assert!(message.contains("does not import external MCP servers"), "{message}");
        assert!(message.contains("Palyra agent runs"), "{message}");
    }

    #[test]
    fn tools_call_error_includes_sanitized_error_chain() {
        let mut backend = ErrorBackend;
        let response = handle_mcp_request(
            &mut backend,
            json!({
                "jsonrpc": JSONRPC_VERSION,
                "id": 8,
                "method": "tools/call",
                "params": {
                    "name": TOOL_SESSIONS_LIST,
                    "arguments": { "limit": 1 }
                }
            }),
        )
        .expect("request should succeed")
        .expect("response should be present");
        let message = response["result"]["content"][0]["text"]
            .as_str()
            .expect("tool error text should be a string");

        assert_eq!(response["result"]["isError"], Value::Bool(true));
        assert!(message.contains("failed to call ListSessions"), "{message}");
        assert!(message.contains("authorization token is invalid"), "{message}");
    }

    #[test]
    fn framing_round_trip_parses_single_message() {
        let body = json!({
            "jsonrpc": JSONRPC_VERSION,
            "id": 1,
            "method": "ping"
        });
        let encoded = serde_json::to_vec(&body).expect("serialize");
        let frame = format!("Content-Length: {}\r\n\r\n", encoded.len());
        let mut bytes = frame.into_bytes();
        bytes.extend_from_slice(encoded.as_slice());
        let mut cursor = std::io::Cursor::new(bytes);
        let parsed = read_mcp_message(&mut cursor)
            .expect("frame should parse")
            .expect("message should exist");
        assert_eq!(parsed, body);
    }

    #[test]
    fn blank_frame_without_content_length_is_validation_error() {
        let mut cursor = std::io::Cursor::new(b"\r\n".to_vec());
        let error =
            read_mcp_message(&mut cursor).expect_err("blank frame should require Content-Length");
        let message = error.to_string();

        assert_eq!(classify_error(&error), CliExitCode::Validation);
        assert!(message.contains("invalid MCP input"));
        assert!(message.contains("missing Content-Length header"));
    }
}
