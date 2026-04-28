use serde_json::{json, Map, Value};

use crate::tool_protocol::{tool_metadata, tool_requires_approval};

use super::hashing::stable_hash_value;
use super::types::{
    ToolApprovalPosture, ToolExposureSurface, ToolParallelismPolicy, ToolRegistryEntry,
    ToolResultProjectionPolicy, TOOL_REGISTRY_ENTRY_VERSION,
};

#[must_use]
pub(crate) fn registry_entries() -> Vec<ToolRegistryEntry> {
    let mut entries = vec![
        entry(
            "palyra.echo",
            "Echo safe text for connectivity and tool-flow checks.",
            object_schema(
                &["text"],
                vec![("text", json!({"type":"string","maxLength":4096}))],
                false,
            ),
            ToolParallelismPolicy::ReadOnly,
            ToolResultProjectionPolicy::InlineUnlessLarge,
        ),
        entry(
            "palyra.sleep",
            "Wait for a bounded number of milliseconds.",
            object_schema(
                &["duration_ms"],
                vec![("duration_ms", json!({"type":"integer","minimum":0,"maximum":5000}))],
                false,
            ),
            ToolParallelismPolicy::Idempotent,
            ToolResultProjectionPolicy::InlineUnlessLarge,
        ),
        entry(
            "palyra.memory.search",
            "Search scoped Palyra memory and return redacted hits.",
            object_schema(
                &["query"],
                vec![
                    ("query", json!({"type":"string","maxLength":8192})),
                    ("scope", json!({"type":"string","enum":["session","channel","principal"]})),
                    ("top_k", json!({"type":"integer","minimum":1,"maximum":20})),
                    ("min_score", json!({"type":"number","minimum":0.0,"maximum":1.0})),
                    ("tags", json!({"type":"array","items":{"type":"string"},"maxItems":16})),
                    ("sources", json!({"type":"array","items":{"type":"string"},"maxItems":16})),
                ],
                false,
            ),
            ToolParallelismPolicy::ReadOnly,
            ToolResultProjectionPolicy::InlineUnlessLarge,
        ),
        entry(
            "palyra.memory.recall",
            "Build a scoped recall preview from memory, workspace and run evidence.",
            object_schema(
                &["query"],
                vec![
                    ("query", json!({"type":"string","maxLength":8192})),
                    ("channel", json!({"type":"string"})),
                    ("session_id", json!({"type":"string"})),
                    ("agent_id", json!({"type":"string"})),
                    ("memory_top_k", json!({"type":"integer","minimum":0,"maximum":16})),
                    ("workspace_top_k", json!({"type":"integer","minimum":0,"maximum":16})),
                    ("min_score", json!({"type":"number","minimum":0.0,"maximum":1.0})),
                    ("max_candidates", json!({"type":"integer","minimum":0,"maximum":12})),
                    (
                        "prompt_budget_tokens",
                        json!({"type":"integer","minimum":512,"maximum":4096}),
                    ),
                    ("workspace_prefix", json!({"type":"string"})),
                    ("include_workspace_historical", json!({"type":"boolean"})),
                    ("include_workspace_quarantined", json!({"type":"boolean"})),
                ],
                false,
            ),
            ToolParallelismPolicy::ReadOnly,
            ToolResultProjectionPolicy::InlineUnlessLarge,
        ),
        entry(
            "palyra.memory.retain",
            "Write a reviewable scoped memory item with provenance.",
            object_schema(
                &["content_text"],
                vec![
                    ("content_text", json!({"type":"string","maxLength":8192})),
                    ("scope", json!({"type":"string","enum":["session","principal","workspace"]})),
                    ("source", json!({"type":"string"})),
                    ("tags", json!({"type":"array","items":{"type":"string"},"maxItems":16})),
                    ("confidence", json!({"type":"number","minimum":0.0,"maximum":1.0})),
                    ("ttl_ms", json!({"type":"integer","minimum":0})),
                    ("ttl_unix_ms", json!({"type":"integer","minimum":0})),
                    (
                        "provenance",
                        json!({"type":"object","properties":{},"additionalProperties":true}),
                    ),
                ],
                false,
            ),
            ToolParallelismPolicy::Exclusive,
            ToolResultProjectionPolicy::InlineUnlessLarge,
        ),
        entry(
            "palyra.memory.reflect",
            "Extract bounded memory reflection candidates from run context.",
            object_schema(
                &["content_text"],
                vec![
                    ("content_text", json!({"type":"string","maxLength":8192})),
                    (
                        "category",
                        json!({"type":"string","enum":["durable_fact","preference","procedure"]}),
                    ),
                    ("confidence", json!({"type":"number","minimum":0.0,"maximum":1.0})),
                ],
                false,
            ),
            ToolParallelismPolicy::ReadOnly,
            ToolResultProjectionPolicy::InlineUnlessLarge,
        ),
        entry(
            "palyra.routines.query",
            "Inspect routine definitions, runs and schedule previews.",
            object_schema(
                &[],
                vec![(
                    "operation",
                    json!({"type":"string","enum":["list","get","list_runs","schedule_preview"]}),
                )],
                true,
            ),
            ToolParallelismPolicy::ReadOnly,
            ToolResultProjectionPolicy::SummarizeAndArtifact,
        ),
        entry(
            "palyra.routines.control",
            "Mutate or dispatch routines through the approval-aware runtime.",
            object_schema(
                &["operation"],
                vec![(
                    "operation",
                    json!({"type":"string","enum":["upsert","pause","resume","run_now","test_run"]}),
                )],
                true,
            ),
            ToolParallelismPolicy::Exclusive,
            ToolResultProjectionPolicy::RedactedPreviewAndArtifact,
        ),
        entry(
            "palyra.artifact.read",
            "Read a bounded scoped chunk from a tool-result artifact.",
            object_schema(
                &["artifact_id"],
                vec![
                    ("artifact_id", json!({"type":"string"})),
                    ("expected_digest_sha256", json!({"type":"string"})),
                    ("offset_bytes", json!({"type":"integer","minimum":0})),
                    ("max_bytes", json!({"type":"integer","minimum":1})),
                    ("text_preview", json!({"type":"boolean"})),
                ],
                false,
            ),
            ToolParallelismPolicy::ReadOnly,
            ToolResultProjectionPolicy::InlineUnlessLarge,
        ),
        entry(
            "palyra.http.fetch",
            "Fetch an HTTP(S) URL through Palyra SSRF, header and content-type guardrails.",
            object_schema(
                &["url"],
                vec![
                    ("url", json!({"type":"string"})),
                    ("method", json!({"type":"string","enum":["GET","HEAD","POST"]})),
                    ("body", json!({"type":"string"})),
                    (
                        "headers",
                        json!({"type":"object","properties":{},"additionalProperties":{"type":"string"}}),
                    ),
                    ("allow_redirects", json!({"type":"boolean"})),
                    ("max_redirects", json!({"type":"integer","minimum":1,"maximum":20})),
                    ("allow_private_targets", json!({"type":"boolean"})),
                    ("max_response_bytes", json!({"type":"integer","minimum":1})),
                    ("cache", json!({"type":"boolean"})),
                    ("cache_ttl_ms", json!({"type":"integer","minimum":1})),
                    (
                        "allowed_content_types",
                        json!({"type":"array","items":{"type":"string"},"maxItems":32}),
                    ),
                    (
                        "credential_bindings",
                        json!({"type":"array","items":{"type":"object","properties":{},"additionalProperties":true}}),
                    ),
                ],
                false,
            ),
            ToolParallelismPolicy::Idempotent,
            ToolResultProjectionPolicy::RedactedPreviewAndArtifact,
        ),
        entry(
            "palyra.process.run",
            "Run an allowlisted executable inside the configured process sandbox.",
            object_schema(
                &["command"],
                vec![
                    ("command", json!({"type":"string","maxLength":128})),
                    ("args", json!({"type":"array","items":{"type":"string"},"maxItems":64})),
                    ("cwd", json!({"type":"string"})),
                    (
                        "requested_egress_hosts",
                        json!({"type":"array","items":{"type":"string"},"maxItems":64}),
                    ),
                    ("timeout_ms", json!({"type":"integer","minimum":1})),
                ],
                false,
            ),
            ToolParallelismPolicy::Exclusive,
            ToolResultProjectionPolicy::RedactedPreviewAndArtifact,
        ),
        entry(
            "palyra.tool_program.run",
            "Execute a bounded ToolProgram DAG through nested tool policy gates.",
            object_schema(
                &["schema_version", "program_id", "granted_tools", "steps"],
                vec![
                    ("schema_version", json!({"type":"integer","enum":[1]})),
                    ("program_id", json!({"type":"string","maxLength":128})),
                    (
                        "granted_tools",
                        json!({"type":"array","items":{"type":"string","maxLength":256},"minItems":1,"maxItems":64}),
                    ),
                    (
                        "budgets",
                        json!({"type":"object","properties":{},"additionalProperties":true}),
                    ),
                    (
                        "safety_policy",
                        json!({"type":"object","properties":{},"additionalProperties":true}),
                    ),
                    (
                        "steps",
                        json!({"type":"array","items":{"type":"object","properties":{},"additionalProperties":true},"maxItems":32}),
                    ),
                ],
                false,
            ),
            ToolParallelismPolicy::Exclusive,
            ToolResultProjectionPolicy::RedactedPreviewAndArtifact,
        ),
        entry(
            "palyra.fs.apply_patch",
            "Apply a strict workspace-confined patch with attestation.",
            object_schema(
                &["patch"],
                vec![
                    ("patch", json!({"type":"string"})),
                    ("workspace_root", json!({"type":"string"})),
                    ("dry_run", json!({"type":"boolean"})),
                ],
                false,
            ),
            ToolParallelismPolicy::Exclusive,
            ToolResultProjectionPolicy::RedactedPreviewAndArtifact,
        ),
        entry(
            "palyra.plugin.run",
            "Run a verified Palyra skill or bounded inline WASM module.",
            object_schema(
                &[],
                vec![
                    ("skill_id", json!({"type":"string"})),
                    ("skill_version", json!({"type":"string"})),
                    ("module_path", json!({"type":"string"})),
                    ("tool_id", json!({"type":"string"})),
                    ("module_wat", json!({"type":"string"})),
                    ("module_base64", json!({"type":"string"})),
                    ("entrypoint", json!({"type":"string"})),
                    (
                        "capabilities",
                        json!({"type":"object","properties":{},"additionalProperties":true}),
                    ),
                ],
                false,
            ),
            ToolParallelismPolicy::Exclusive,
            ToolResultProjectionPolicy::RedactedPreviewAndArtifact,
        ),
    ];

    for browser_tool in browser_tool_names() {
        entries.push(entry(
            browser_tool,
            browser_tool_description(browser_tool),
            browser_tool_schema(browser_tool),
            ToolParallelismPolicy::Exclusive,
            ToolResultProjectionPolicy::RedactedPreviewAndArtifact,
        ));
    }

    entries.sort_by(|left, right| left.name.cmp(&right.name));
    entries
}

pub(crate) fn registry_entry(tool_name: &str) -> Option<ToolRegistryEntry> {
    registry_entries().into_iter().find(|entry| entry.name == tool_name)
}

fn entry(
    name: &str,
    description: &str,
    input_schema: Value,
    parallelism_policy: ToolParallelismPolicy,
    projection_policy: ToolResultProjectionPolicy,
) -> ToolRegistryEntry {
    let capabilities = tool_metadata(name)
        .map(|metadata| {
            metadata
                .capabilities
                .iter()
                .map(|capability| capability.policy_name().to_owned())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    ToolRegistryEntry {
        name: name.to_owned(),
        description: description.to_owned(),
        version: TOOL_REGISTRY_ENTRY_VERSION,
        provenance: "builtin:palyra-daemon".to_owned(),
        schema_hash: stable_hash_value(&input_schema),
        input_schema,
        capabilities,
        approval_posture: if tool_requires_approval(name) {
            ToolApprovalPosture::ApprovalRequired
        } else {
            ToolApprovalPosture::Safe
        },
        projection_policy,
        parallelism_policy,
        target_surfaces: vec![ToolExposureSurface::RunStream, ToolExposureSurface::RouteMessage],
    }
}

fn object_schema(required: &[&str], properties: Vec<(&str, Value)>, additional: bool) -> Value {
    let mut property_map = Map::new();
    for (name, schema) in properties {
        property_map.insert(name.to_owned(), schema);
    }
    json!({
        "type": "object",
        "properties": property_map,
        "required": required,
        "additionalProperties": additional,
    })
}

fn browser_tool_names() -> &'static [&'static str] {
    &[
        "palyra.browser.session.create",
        "palyra.browser.session.close",
        "palyra.browser.navigate",
        "palyra.browser.click",
        "palyra.browser.type",
        "palyra.browser.press",
        "palyra.browser.select",
        "palyra.browser.highlight",
        "palyra.browser.scroll",
        "palyra.browser.wait_for",
        "palyra.browser.title",
        "palyra.browser.screenshot",
        "palyra.browser.pdf",
        "palyra.browser.observe",
        "palyra.browser.network_log",
        "palyra.browser.console_log",
        "palyra.browser.reset_state",
        "palyra.browser.tabs.list",
        "palyra.browser.tabs.open",
        "palyra.browser.tabs.switch",
        "palyra.browser.tabs.close",
        "palyra.browser.permissions.get",
        "palyra.browser.permissions.set",
    ]
}

fn browser_tool_description(tool_name: &str) -> &'static str {
    match tool_name {
        "palyra.browser.session.create" => "Create a brokered browser session.",
        "palyra.browser.session.close" => "Close a brokered browser session.",
        "palyra.browser.navigate" => "Navigate a brokered browser session to a URL.",
        "palyra.browser.click" => "Click an element in a brokered browser session.",
        "palyra.browser.type" => "Type text in a brokered browser session.",
        "palyra.browser.press" => "Press a key in a brokered browser session.",
        "palyra.browser.select" => "Select an option in a brokered browser session.",
        "palyra.browser.highlight" => "Highlight an element in a brokered browser session.",
        "palyra.browser.scroll" => "Scroll a brokered browser session.",
        "palyra.browser.wait_for" => "Wait for a browser condition.",
        "palyra.browser.title" => "Read the current browser title.",
        "palyra.browser.screenshot" => "Capture a bounded browser screenshot.",
        "palyra.browser.pdf" => "Capture a bounded browser PDF.",
        "palyra.browser.observe" => "Observe visible browser state.",
        "palyra.browser.network_log" => "Read bounded browser network logs.",
        "palyra.browser.console_log" => "Read bounded browser console logs.",
        "palyra.browser.reset_state" => "Reset browser session state.",
        "palyra.browser.tabs.list" => "List browser tabs.",
        "palyra.browser.tabs.open" => "Open a browser tab.",
        "palyra.browser.tabs.switch" => "Switch the active browser tab.",
        "palyra.browser.tabs.close" => "Close a browser tab.",
        "palyra.browser.permissions.get" => "Read browser permission state.",
        "palyra.browser.permissions.set" => "Update browser permission state.",
        _ => "Operate a brokered browser session.",
    }
}

fn browser_tool_schema(tool_name: &str) -> Value {
    let mut properties = vec![
        ("session_id", json!({"type":"string"})),
        ("timeout_ms", json!({"type":"integer","minimum":1})),
    ];
    match tool_name {
        "palyra.browser.navigate" | "palyra.browser.tabs.open" => {
            properties.push(("url", json!({"type":"string"})));
        }
        "palyra.browser.click"
        | "palyra.browser.type"
        | "palyra.browser.press"
        | "palyra.browser.select"
        | "palyra.browser.highlight" => {
            properties.push(("selector", json!({"type":"string"})));
            properties.push(("text", json!({"type":"string"})));
            properties.push(("key", json!({"type":"string"})));
            properties.push(("value", json!({"type":"string"})));
        }
        "palyra.browser.scroll" => {
            properties.push(("delta_x", json!({"type":"integer"})));
            properties.push(("delta_y", json!({"type":"integer"})));
        }
        "palyra.browser.session.create" => {
            properties.push(("profile_id", json!({"type":"string"})));
            properties.push(("private_profile", json!({"type":"boolean"})));
            properties.push(("allow_private_targets", json!({"type":"boolean"})));
            properties.push(("allow_downloads", json!({"type":"boolean"})));
            properties.push((
                "budget",
                json!({"type":"object","properties":{},"additionalProperties":true}),
            ));
        }
        _ => {}
    }
    object_schema(&[], properties, true)
}
