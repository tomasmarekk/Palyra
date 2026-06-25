//! Static builtin tool registry: names, model-facing descriptions, input
//! schemas, and scheduling/projection policies for every daemon tool.
//!
//! Descriptions and schema strings here are model-visible prompt surface and
//! are pinned by unit tests and replay fixtures; treat any wording change as
//! a contract change, not a copy edit.

use serde_json::{json, Map, Value};

use crate::tool_protocol::{tool_metadata, tool_requires_approval};

use super::hashing::stable_hash_value;
use super::types::{
    ToolApprovalPosture, ToolExposureSurface, ToolParallelismPolicy, ToolRegistryEntry,
    ToolResultProjectionPolicy, TOOL_REGISTRY_ENTRY_VERSION,
};

/// Returns every builtin tool registry entry, sorted by name.
///
/// Entries are rebuilt on each call; they are small and call sites operate
/// per proposal/turn, so no caching is layered on top.
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
                vec![("duration_ms", json!({"type":"integer","minimum":0,"maximum":30000}))],
                false,
            ),
            ToolParallelismPolicy::Idempotent,
            ToolResultProjectionPolicy::InlineUnlessLarge,
        ),
        entry(
            "palyra.memory.status",
            "Inspect read-only memory usage, retention limits, maintenance timing, and capacity_state; use this before deciding whether memory is full or needs consolidation.",
            object_schema(&[], Vec::new(), false),
            ToolParallelismPolicy::ReadOnly,
            ToolResultProjectionPolicy::InlineUnlessLarge,
        ),
        entry(
            "palyra.memory.search",
            "Search Palyra lifecycle memory and workspace/project documents by default, or narrow to one explicit scope, and return redacted hits.",
            object_schema(
                &["query"],
                vec![
                    ("query", json!({"type":"string","maxLength":8192})),
                    (
                        "scope",
                        json!({"type":"string","enum":["all","principal","session","channel","workspace","project"],"description":"Defaults to all, which searches durable principal memory plus indexed workspace/project documents. Use principal, session, channel, workspace, or project to narrow the search."}),
                    ),
                    (
                        "channel",
                        json!({"type":"string","description":"Only valid with scope=channel. Omit or use current/default sentinels for the authenticated run channel. To verify that a marker is absent in a different channel, set isolation_probe=true; the result returns only hit_count/isolated metadata and never memory content."}),
                    ),
                    (
                        "isolation_probe",
                        json!({"type":"boolean","description":"Only valid with scope=channel. When true, permits a read-only negative probe against channel and redacts all hit content from the output."}),
                    ),
                    ("top_k", json!({"type":"integer","minimum":1,"maximum":20})),
                    ("min_score", json!({"type":"number","minimum":0.0,"maximum":1.0})),
                    (
                        "workspace_prefix",
                        json!({"type":"string","description":"Optional workspace/project document path prefix used with scope=workspace or scope=project. When omitted, scope=workspace and scope=project bind to the active launch workspace project memory; without an active root, workspace searches MEMORY.md and project searches projects/default."}),
                    ),
                    (
                        "prefix",
                        json!({"type":"string","description":"Alias for workspace_prefix used with scope=workspace or scope=project. When omitted, scope=workspace and scope=project bind to the active launch workspace project memory; without an active root, workspace searches MEMORY.md and project searches projects/default."}),
                    ),
                    ("include_workspace_historical", json!({"type":"boolean"})),
                    ("include_workspace_quarantined", json!({"type":"boolean"})),
                    ("tags", json!({"type":"array","items":{"type":"string"},"maxItems":16})),
                    (
                        "sources",
                        json!({"type":"array","items":{"type":"string","enum":["manual","summary","import","tape:user_message","tape:tool_result"]},"maxItems":16}),
                    ),
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
                    ("channel", json!({"type":"string","description":"Optional authenticated runtime channel. Omit to use the current run channel; do not guess default/current/commentary/final/analysis sentinel values."})),
                    (
                        "session_id",
                        json!({"type":"string","description":"Optional exact session id. Do not ask users for this for 'previous session' or 'last time'; omit it for principal cross-session recall."}),
                    ),
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
            "palyra.memory.session_search",
            "Search prior session transcripts for facts that were not stored as durable memory. Results expose stable prior_session/prior_run labels, not raw internal session_id or run_id values.",
            object_schema(
                &["query"],
                vec![
                    ("query", json!({"type":"string","maxLength":8192,"description":"Search text for prior-session transcript recall. Use this for previous session, last time, earlier conversation, or temporary facts explicitly not saved as memory."})),
                    ("channel", json!({"type":"string","description":"Optional authenticated runtime channel. Omit to use the current run channel; do not guess default/current/commentary/final/analysis sentinel values."})),
                    ("top_k", json!({"type":"integer","minimum":1,"maximum":24})),
                    ("min_score", json!({"type":"number","minimum":0.0,"maximum":1.0})),
                    ("window_before", json!({"type":"integer","minimum":0,"maximum":8})),
                    ("window_after", json!({"type":"integer","minimum":0,"maximum":8})),
                    (
                        "max_windows_per_session",
                        json!({"type":"integer","minimum":1,"maximum":8}),
                    ),
                    ("include_current_session", json!({"type":"boolean","description":"Defaults to false so prior-session searches are not dominated by the current prompt. Set true only when the user explicitly asks to search this active session."})),
                    ("include_archived", json!({"type":"boolean"})),
                ],
                false,
            ),
            ToolParallelismPolicy::ReadOnly,
            ToolResultProjectionPolicy::InlineUnlessLarge,
        ),
        entry(
            "palyra.session_search",
            "Compatibility alias for palyra.memory.session_search; search prior session transcripts for facts that were not stored as durable memory. Results expose stable prior_session/prior_run labels, not raw internal session_id or run_id values.",
            object_schema(
                &["query"],
                vec![
                    ("query", json!({"type":"string","maxLength":8192,"description":"Search text for prior-session transcript recall. Use this for previous session, last time, earlier conversation, or temporary facts explicitly not saved as memory."})),
                    ("channel", json!({"type":"string"})),
                    ("top_k", json!({"type":"integer","minimum":1,"maximum":24})),
                    ("min_score", json!({"type":"number","minimum":0.0,"maximum":1.0})),
                    ("window_before", json!({"type":"integer","minimum":0,"maximum":8})),
                    ("window_after", json!({"type":"integer","minimum":0,"maximum":8})),
                    (
                        "max_windows_per_session",
                        json!({"type":"integer","minimum":1,"maximum":8}),
                    ),
                    ("include_current_session", json!({"type":"boolean","description":"Defaults to false so prior-session searches are not dominated by the current prompt. Set true only when the user explicitly asks to search this active session."})),
                    ("include_archived", json!({"type":"boolean"})),
                ],
                false,
            ),
            ToolParallelismPolicy::ReadOnly,
            ToolResultProjectionPolicy::InlineUnlessLarge,
        ),
        entry(
            "palyra.memory.retain",
            "Write a reviewable scoped memory item or workspace/project memory document with provenance. Defaults to scope=principal for preferences, facts, or corrections that must affect future sessions; use scope=workspace or scope=project for durable project context.",
            object_schema(
                &["content_text"],
                vec![
                    ("content_text", json!({"type":"string","maxLength":8192,"description":"Memory content to retain. For corrections, include the corrected durable statement only and provide the obsolete values in replaces_terms."})),
                    (
                        "category",
                        json!({"type":"string","enum":["fact","preference","procedure","constraint","decision","correction","transient_runtime_fact"],"description":"Structured lifecycle category. Set this explicitly instead of relying on natural-language wording."}),
                    ),
                    (
                        "replaces_terms",
                        json!({"type":"array","items":{"type":"string"},"maxItems":32,"description":"Language-neutral obsolete values and context terms that identify existing memory to replace. Use with category=correction when possible; workspace/project memory applies these terms whenever provided."}),
                    ),
                    ("scope", json!({"type":"string","enum":["principal","session","channel","workspace","project"],"description":"Defaults to principal. Use session only for current-session scratch memory, channel for authenticated channel memory, and workspace or project to write indexed workspace/project memory documents."})),
                    (
                        "workspace_path",
                        json!({"type":"string","description":"Exact workspace document path for scope=workspace or scope=project. Defaults to the active launch workspace's project memory for workspace/project scope; without an active root, workspace falls back to MEMORY.md and project falls back to projects/default/MEMORY.md. Bare project/workspace names and absolute workspace roots map to projects/<name>/MEMORY.md."}),
                    ),
                    (
                        "workspace_prefix",
                        json!({"type":"string","description":"Workspace/project directory or document path for scope=workspace or scope=project. Directory prefixes write to MEMORY.md below that prefix; bare names and absolute workspace roots map under projects/."}),
                    ),
                    (
                        "prefix",
                        json!({"type":"string","description":"Alias for workspace_prefix used with scope=workspace or scope=project."}),
                    ),
                    (
                        "source",
                        json!({"type":"string","enum":["manual","summary","import","tape:user_message","tape:tool_result"]}),
                    ),
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
            "palyra.retain",
            "Compatibility alias for palyra.memory.retain; write a reviewable scoped memory item or workspace/project memory document with provenance.",
            object_schema(
                &["content_text"],
                vec![
                    ("content_text", json!({"type":"string","maxLength":8192,"description":"Memory content to retain. For corrections, include the corrected durable statement only and provide the obsolete values in replaces_terms."})),
                    (
                        "category",
                        json!({"type":"string","enum":["fact","preference","procedure","constraint","decision","correction","transient_runtime_fact"],"description":"Structured lifecycle category. Set this explicitly instead of relying on natural-language wording."}),
                    ),
                    (
                        "replaces_terms",
                        json!({"type":"array","items":{"type":"string"},"maxItems":32,"description":"Language-neutral obsolete values and context terms that identify existing memory to replace. Use with category=correction when possible; workspace/project memory applies these terms whenever provided."}),
                    ),
                    ("scope", json!({"type":"string","enum":["principal","session","channel","workspace","project"],"description":"Defaults to principal. Use session only for current-session scratch memory, channel for authenticated channel memory, and workspace or project to write indexed workspace/project memory documents."})),
                    (
                        "workspace_path",
                        json!({"type":"string","description":"Exact workspace document path for scope=workspace or scope=project. Defaults to the active launch workspace's project memory for workspace/project scope; without an active root, workspace falls back to MEMORY.md and project falls back to projects/default/MEMORY.md. Bare project/workspace names and absolute workspace roots map to projects/<name>/MEMORY.md."}),
                    ),
                    (
                        "workspace_prefix",
                        json!({"type":"string","description":"Workspace/project directory or document path for scope=workspace or scope=project. Directory prefixes write to MEMORY.md below that prefix; bare names and absolute workspace roots map under projects/."}),
                    ),
                    (
                        "prefix",
                        json!({"type":"string","description":"Alias for workspace_prefix used with scope=workspace or scope=project."}),
                    ),
                    (
                        "source",
                        json!({"type":"string","enum":["manual","summary","import","tape:user_message","tape:tool_result"]}),
                    ),
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
            "palyra.memory.delete",
            "Delete a scoped memory item or workspace memory document by the id returned from memory search after the user asks to forget or remove it.",
            object_schema(
                &["memory_id"],
                vec![(
                    "memory_id",
                    json!({"type":"string","description":"Canonical memory_id or workspace document_id returned by palyra.memory.search or palyra.memory.recall."}),
                )],
                false,
            ),
            ToolParallelismPolicy::Exclusive,
            ToolResultProjectionPolicy::InlineUnlessLarge,
        ),
        entry(
            "palyra.memory.replace",
            "Replace the content of an existing scoped memory item by memory_id when the user corrects an obsolete preference or fact.",
            object_schema(
                &["memory_id", "content_text"],
                vec![
                    (
                        "memory_id",
                        json!({"type":"string","description":"Canonical memory_id returned by palyra.memory.search or palyra.memory.recall."}),
                    ),
                    (
                        "content_text",
                        json!({"type":"string","maxLength":8192,"description":"Correct replacement memory content. Include the new durable preference/fact, not both stale and corrected values."}),
                    ),
                    ("tags", json!({"type":"array","items":{"type":"string"},"maxItems":16})),
                    ("confidence", json!({"type":"number","minimum":0.0,"maximum":1.0})),
                    (
                        "ttl_ms",
                        json!({"type":"integer","minimum":0,"description":"Optional relative TTL in milliseconds. Omit both TTL fields to keep existing/default retention; do not set this with ttl_unix_ms."}),
                    ),
                    (
                        "ttl_unix_ms",
                        json!({"type":"integer","minimum":0,"description":"Optional absolute expiry time in Unix milliseconds. Omit both TTL fields to keep existing/default retention; do not set this with ttl_ms."}),
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
                        "categories",
                        json!({"type":"array","items":{"type":"string","enum":["facts","preferences","workflow_rules","risks","temporary_state"]},"maxItems":5,"description":"Optional structured reflection categories to apply to the supplied observations."}),
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
            "Inspect routine definitions, run history, schedule previews, and bounded scheduler completion. Use operation=schedule_preview with phrase such as 'every 40 seconds' or a timezone such as Europe/Prague before creating scheduled monitors. Use operation=wait_terminal with routine_id and expected_successful_runs for capped recurring routines instead of polling list_runs across many model turns.",
            object_schema(
                &[],
                vec![
                    (
                        "operation",
                        json!({"type":"string","enum":["list","get","list_runs","wait_terminal","schedule_preview"]}),
                    ),
                    ("routine_id", json!({"type":"string"})),
                    ("phrase", json!({"type":"string"})),
                    (
                        "timezone",
                        json!({"type":"string","description":"local, utc, or an IANA timezone such as Europe/Prague."}),
                    ),
                    (
                        "limit",
                        json!({"type":"integer","minimum":1,"maximum":500,"description":"Page size for list/list_runs, or requested preview run count for operation=schedule_preview (runtime caps previews to a safe bounded maximum)."}),
                    ),
                    (
                        "start_date",
                        json!({"type":"string","description":"Optional YYYY-MM-DD local date for operation=schedule_preview. The date is interpreted in timezone and included in the preview window."}),
                    ),
                    (
                        "end_date",
                        json!({"type":"string","description":"Optional YYYY-MM-DD local date for operation=schedule_preview. The date is interpreted in timezone and included through the end of that local day."}),
                    ),
                    (
                        "expected_successful_runs",
                        json!({"type":"integer","minimum":0,"description":"Optional success count to wait for when operation=wait_terminal, for example max_runs from a capped recurring schedule."}),
                    ),
                    (
                        "timeout_ms",
                        json!({"type":"integer","minimum":1,"maximum":900000,"description":"Maximum wait for operation=wait_terminal. Prefer enough time for capped scheduler intervals plus one safety poll."}),
                    ),
                    (
                        "poll_interval_ms",
                        json!({"type":"integer","minimum":250,"maximum":30000,"description":"Polling cadence for operation=wait_terminal inside one tool call."}),
                    ),
                ],
                true,
            ),
            ToolParallelismPolicy::ReadOnly,
            ToolResultProjectionPolicy::SummarizeAndArtifact,
        ),
        entry(
            "palyra.routines.control",
            "Create, update, pause, resume, delete, or manually dispatch routines through the approval-aware runtime. For new reminders and monitors, omit routine_id and use operation=upsert with trigger_kind=schedule, name, prompt, and structured schedule fields (schedule_type plus every_interval_ms, cron_expression, at_timestamp_rfc3339, or delay_ms) when the requested timing is clear. For one-shot relative requests like 'in 30 seconds', prefer schedule_type=at with delay_ms so the daemon computes a future timestamp at execution time. Use operation=delete with routine_id when the user asks to remove a routine or clean up a temporary routine after a test. Pass timezone=local, utc, or an IANA timezone such as Europe/Prague when the user gives local wall-clock timing. natural_language_schedule accepts a small English convenience grammar such as 'every 30 minutes' or 'every 40 seconds'. Set max_runs when the user asks for an explicit run cap. For standing orders tied to an absolute user-owned OS file path, use trigger_kind=file_watch with trigger_payload.path and optional trigger_payload.poll_interval_ms. Use workdir for a scheduled project root that future runs should treat as their cwd and output base. Scheduled routines with workdir default to sensitive-tools posture for unattended file/process work. File-watch routines default to fresh sessions and sensitive-tools posture because follow-up work often needs audited OS file tools. Set approval_mode=before_enable or before_first_run only when the user wants an approval gate.",
            object_schema(
                &["operation"],
                vec![
                    (
                        "operation",
                        json!({"type":"string","enum":["upsert","pause","resume","delete","run_now","test_run"]}),
                    ),
                    (
                        "routine_id",
                        json!({"type":"string","description":"Canonical ULID returned by a previous successful routine operation. Omit this when creating a new routine; do not put human slugs here."}),
                    ),
                    ("name", json!({"type":"string"})),
                    ("prompt", json!({"type":"string"})),
                    (
                        "workdir",
                        json!({"type":"string","description":"Optional explicit project/root directory for scheduled runs; the run log and scheduled prompt metadata expose this value."}),
                    ),
                    (
                        "trigger_kind",
                        json!({"type":"string","enum":["schedule","hook","webhook","system_event","file_watch","manual"]}),
                    ),
                    (
                        "trigger_payload",
                        json!({"type":"object","description":"Trigger-specific payload. For trigger_kind=file_watch, provide path as an absolute user-owned OS path and optional poll_interval_ms >= 30000 plus fire_on_start.","properties":{},"additionalProperties":true}),
                    ),
                    ("natural_language_schedule", json!({"type":"string"})),
                    ("schedule_type", json!({"type":"string","enum":["cron","every","at"]})),
                    (
                        "every_interval_ms",
                        json!({"type":"integer","minimum":30000,"description":"Minimum 30000 ms for durable routines; use palyra.sleep for shorter bounded in-session polling."}),
                    ),
                    (
                        "max_runs",
                        json!({"type":"integer","minimum":1,"description":"Optional maximum number of scheduled runs before the scheduler stops dispatching this routine."}),
                    ),
                    ("cron_expression", json!({"type":"string"})),
                    (
                        "timezone",
                        json!({"type":"string","description":"local, utc, or an IANA timezone such as Europe/Prague for schedule_type=cron or natural_language_schedule."}),
                    ),
                    ("at_timestamp_rfc3339", json!({"type":"string"})),
                    (
                        "delay_ms",
                        json!({"type":"integer","minimum":1,"description":"Relative one-shot delay for schedule_type=at, computed by the daemon at tool execution time. Prefer this for requests like 'in 30 seconds' instead of precomputing at_timestamp_rfc3339 in the model."}),
                    ),
                    (
                        "delivery_mode",
                        json!({"type":"string","enum":["same_channel","specific_channel","local_only","logs_only"]}),
                    ),
                    (
                        "success_visibility",
                        json!({"type":"string","enum":["announce","artifact_only","audit_only"],"description":"Language-neutral successful-output intent. Use announce for reminders, monitors, and other routines whose success should be visible to the user; artifact_only when successful output is written to an explicit artifact or file; audit_only only when the user explicitly wants no success announcement."}),
                    ),
                    (
                        "execution_posture",
                        json!({"type":"string","enum":["standard","sensitive_tools"],"description":"Use sensitive_tools when a routine should be allowed to use audited sensitive tools during its scheduled or manual runs."}),
                    ),
                    (
                        "approval_mode",
                        json!({"type":"string","enum":["none","before_enable","before_first_run"],"description":"Optional approval gate for routines that should wait before enabling or before the first run."}),
                    ),
                    ("enabled", json!({"type":"boolean"})),
                ],
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
                    (
                        "text_preview",
                        json!({"type":"boolean","default":true,"description":"Defaults to true so textual tool-result artifacts return a bounded redacted text preview instead of gated full bytes."}),
                    ),
                ],
                false,
            ),
            ToolParallelismPolicy::ReadOnly,
            ToolResultProjectionPolicy::InlineUnlessLarge,
        ),
        entry(
            "palyra.image.observe",
            "Observe an image path or artifact without returning base64; returns OCR/vision metadata when available, otherwise a structured unsupported capability error that forbids verifier-oracle workarounds.",
            object_schema(
                &[],
                vec![
                    (
                        "path",
                        json!({"type":"string","description":"Workspace-relative image path or /workspace/path alias. Provide exactly one of path or artifact_id."}),
                    ),
                    (
                        "artifact_id",
                        json!({"type":"string","description":"Image artifact id returned by another tool. Provide exactly one of path or artifact_id."}),
                    ),
                    ("expected_digest_sha256", json!({"type":"string"})),
                ],
                false,
            ),
            ToolParallelismPolicy::ReadOnly,
            ToolResultProjectionPolicy::InlineUnlessLarge,
        ),
        entry(
            "palyra.fs.read_file",
            "Read a bounded chunk from a file inside the current agent workspace root. Accepts relative paths and virtual workspace aliases such as /workspace/file.txt.",
            object_schema(
                &["path"],
                vec![
                    ("path", json!({"type":"string"})),
                    (
                        "workspace_root",
                        json!({"type":"string","description":"Optional existing workspace subdirectory to treat as the read root. Use this when a prior apply_patch call used workspace_root for the same project."}),
                    ),
                    ("offset_bytes", json!({"type":"integer","minimum":0})),
                    ("max_bytes", json!({"type":"integer","minimum":1})),
                    (
                        "line_start",
                        json!({"type":"integer","minimum":1,"description":"Optional 1-based line number to start reading from. Use this to follow up on palyra.fs.search line hits."}),
                    ),
                    (
                        "line_count",
                        json!({"type":"integer","minimum":1,"description":"Optional number of lines to return when line_start is set."}),
                    ),
                ],
                false,
            ),
            ToolParallelismPolicy::ReadOnly,
            ToolResultProjectionPolicy::InlineUnlessLarge,
        ),
        entry(
            "palyra.fs.list_dir",
            "List entries in a directory inside the current agent workspace root. Use this for discovery instead of process.run find, grep, cat, or shell commands.",
            object_schema(
                &[],
                vec![
                    (
                        "path",
                        json!({"type":"string","description":"Directory path relative to the workspace root. Omit for the workspace root. /workspace and workspace/ are accepted virtual workspace aliases."}),
                    ),
                    (
                        "workspace_root",
                        json!({"type":"string","description":"Optional existing workspace subdirectory to treat as the listing root. Use this when the task is scoped to a nested project directory."}),
                    ),
                    ("max_entries", json!({"type":"integer","minimum":1,"maximum":512})),
                ],
                false,
            ),
            ToolParallelismPolicy::ReadOnly,
            ToolResultProjectionPolicy::InlineUnlessLarge,
        ),
        entry(
            "palyra.fs.search",
            "Search text files inside the current agent workspace root for a literal string. Use this before and after refactors or public API renames to find implementation, test, docs, and example references without relying on shell grep.",
            object_schema(
                &["query"],
                vec![
                    ("query", json!({"type":"string","maxLength":512,"description":"Literal text to search for, such as an old identifier during a rename. This is not a regular expression."})),
                    (
                        "path",
                        json!({"type":"string","description":"Optional file or directory path relative to the workspace root. Omit for the active workspace root. /workspace and workspace/ are accepted virtual workspace aliases."}),
                    ),
                    (
                        "workspace_root",
                        json!({"type":"string","description":"Optional existing workspace subdirectory to treat as the search root. Use this when the task is scoped to a nested project directory."}),
                    ),
                    ("case_sensitive", json!({"type":"boolean","default":true})),
                    ("max_matches", json!({"type":"integer","minimum":1,"maximum":200})),
                ],
                false,
            ),
            ToolParallelismPolicy::ReadOnly,
            ToolResultProjectionPolicy::InlineUnlessLarge,
        ),
        entry(
            "palyra.fs.os_file",
            "Perform an audited file operation on an absolute user-owned OS path. Use this for requested files outside the workspace, such as Downloads, user config files, configured local test harness OS roots, or user-cache cleanup. Protected system paths are denied, and paths are limited to workspace roots plus the runtime-approved OS roots. For small edits to files inside a workspace, prefer palyra.fs.apply_patch; os_file write is a full-file replacement surface.",
            object_schema(
                &["operation", "path"],
                vec![
                    (
                        "operation",
                        json!({"type":"string","enum":["stat","read","write","copy","move","delete_file","delete_empty_dir","mkdir","list_dir","search"],"description":"Operation to perform. Prefer list_dir/search/read/write for ordinary OS-level files; use copy/move/delete_file/delete_empty_dir/mkdir only when explicitly requested. Use delete_empty_dir for cleanup of an existing empty directory instead of shell builtins such as rmdir. For privacy cleanup, use search to find exact matching cache files before delete_file."}),
                    ),
                    (
                        "path",
                        json!({"type":"string","description":"Absolute OS path to inspect or modify. A leading environment prefix such as $PALYRA_E2E_OS_ROOT, ${PALYRA_E2E_OS_ROOT}, or %PALYRA_E2E_OS_ROOT% is expanded before validation. Use this only for user-owned paths such as profile, temp, Downloads, or configured harness OS roots; protected system paths are denied."}),
                    ),
                    (
                        "target_path",
                        json!({"type":"string","description":"Destination path for copy or move operations. Use an absolute OS path, optionally with a leading environment prefix such as %PALYRA_E2E_OS_ROOT%, for OS-to-OS moves. Use a workspace-relative path such as data/imported/file.csv or /workspace/data/imported/file.csv to import an allowed OS file into the active workspace without guessing the workspace's absolute root."}),
                    ),
                    (
                        "content_text",
                        json!({"type":"string","maxLength":262144,"description":"UTF-8 file content for write. Do not include raw secrets; write vault references or redacted-safe config values when possible."}),
                    ),
                    (
                        "bytes_base64",
                        json!({"type":"string","maxLength":349528,"description":"Base64 file content for binary writes. Provide either content_text or bytes_base64, not both."}),
                    ),
                    (
                        "create_parent_dirs",
                        json!({"type":"boolean","description":"Defaults to true for write/copy/move so requested user-owned destination directories can be created."}),
                    ),
                    (
                        "overwrite",
                        json!({"type":"boolean","description":"Defaults to true for write/copy/move. For operation=write, overwrite=true replaces the entire file; use palyra.fs.apply_patch for scoped edits."}),
                    ),
                    (
                        "full_replace",
                        json!({"type":"boolean","description":"Set true only when operation=write intentionally replaces the whole existing file. Without this flag, writes to an existing file are rejected because append/partial-edit semantics are unsupported; read the original content first and provide the full replacement body when intentional."}),
                    ),
                    ("dry_run", json!({"type":"boolean"})),
                    ("offset_bytes", json!({"type":"integer","minimum":0})),
                    ("max_bytes", json!({"type":"integer","minimum":1,"maximum":131072})),
                    (
                        "query",
                        json!({"type":"string","maxLength":512,"description":"Literal filename/path/content text to search for when operation=search."}),
                    ),
                    ("case_sensitive", json!({"type":"boolean","default":false})),
                    ("max_entries", json!({"type":"integer","minimum":1,"maximum":200})),
                    ("max_matches", json!({"type":"integer","minimum":1,"maximum":100})),
                ],
                false,
            ),
            ToolParallelismPolicy::Exclusive,
            ToolResultProjectionPolicy::RedactedPreviewAndArtifact,
        ),
        entry(
            "palyra.delegation.query",
            "Inspect delegated child tasks, child run status and merge previews in the current scope.",
            object_schema(
                &["operation"],
                vec![
                    (
                        "operation",
                        json!({"type":"string","enum":["list","status","merge_preview"]}),
                    ),
                    ("session_id", json!({"type":"string"})),
                    ("parent_run_id", json!({"type":"string"})),
                    ("task_id", json!({"type":"string"})),
                    ("run_id", json!({"type":"string"})),
                    ("include_completed", json!({"type":"boolean"})),
                ],
                false,
            ),
            ToolParallelismPolicy::ReadOnly,
            ToolResultProjectionPolicy::InlineUnlessLarge,
        ),
        entry(
            "palyra.delegation.control",
            "Create or interrupt bounded delegated child runs through the journaled runtime.",
            object_schema(
                &["operation"],
                vec![
                    ("operation", json!({"type":"string","enum":["delegate","interrupt"]})),
                    ("objective", json!({"type":"string","maxLength":8192})),
                    ("profile_id", json!({"type":"string"})),
                    ("template_id", json!({"type":"string"})),
                    ("task_id", json!({"type":"string"})),
                    ("run_id", json!({"type":"string"})),
                    ("reason", json!({"type":"string","maxLength":2048})),
                    ("priority", json!({"type":"integer","minimum":-10,"maximum":10})),
                    ("budget_tokens", json!({"type":"integer","minimum":1})),
                    ("max_attempts", json!({"type":"integer","minimum":1,"maximum":16})),
                    ("execution_mode", json!({"type":"string","enum":["serial","parallel"]})),
                    ("group_id", json!({"type":"string"})),
                    ("model_profile", json!({"type":"string"})),
                    ("memory_scope", json!({"type":"string","enum":["none","parent_session","parent_session_and_workspace","workspace_only"]})),
                    ("tool_allowlist", json!({"type":"array","items":{"type":"string"},"maxItems":64})),
                    ("skill_allowlist", json!({"type":"array","items":{"type":"string"},"maxItems":64})),
                    ("approval_required", json!({"type":"boolean"})),
                    ("max_concurrent_children", json!({"type":"integer","minimum":1})),
                    ("max_children_per_parent", json!({"type":"integer","minimum":1})),
                    ("max_total_children", json!({"type":"integer","minimum":1})),
                    ("max_parallel_groups", json!({"type":"integer","minimum":1})),
                    ("max_depth", json!({"type":"integer","minimum":1})),
                    ("max_budget_share_bps", json!({"type":"integer","minimum":1,"maximum":10000})),
                    ("child_timeout_ms", json!({"type":"integer","minimum":1000})),
                ],
                false,
            ),
            ToolParallelismPolicy::Exclusive,
            ToolResultProjectionPolicy::RedactedPreviewAndArtifact,
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
                        json!({
                            "type":"array",
                            "maxItems":8,
                            "items":{
                                "type":"object",
                                "required":["header_name","secret_ref"],
                                "additionalProperties":false,
                                "properties":{
                                    "header_name":{
                                        "type":"string",
                                        "description":"Credential header to inject. Runtime allows authorization, x-*, *-token, *-api-key, and cookie."
                                    },
                                    "secret_ref":{
                                        "type":"object",
                                        "required":["kind","vault_ref"],
                                        "additionalProperties":false,
                                        "properties":{
                                            "kind":{
                                                "type":"string",
                                                "enum":["vault"],
                                                "description":"Only configured vault-backed secret refs are accepted for HTTP fetch."
                                            },
                                            "vault_ref":{
                                                "type":"string",
                                                "description":"Vault ref that must exactly match tool_call.http_fetch.allowed_credential_vault_refs."
                                            },
                                            "required":{"type":"boolean"},
                                            "refresh_policy":{"type":"string","enum":["on_startup","on_reload","per_run","per_use"]},
                                            "snapshot_policy":{"type":"string","enum":["freeze_until_reload","refresh_per_run","refresh_per_use"]},
                                            "max_bytes":{"type":"integer","minimum":1},
                                            "redaction_label":{"type":"string","maxLength":128},
                                            "display_name":{"type":"string","maxLength":128}
                                        }
                                    },
                                    "required":{"type":"boolean"}
                                }
                            }
                        }),
                    ),
                ],
                false,
            ),
            ToolParallelismPolicy::Idempotent,
            ToolResultProjectionPolicy::RedactedPreviewAndArtifact,
        ),
        entry(
            "palyra.process.run",
            "Run a local process using the configured process posture. Local desktop defaults may allow host-wide execution; restrictive deployments can use executable allowlists, egress controls, and workspace scoping. Use palyra.fs.apply_patch, not this tool, for file writes. On Windows, prefer Palyra file tools or PowerShell script-file execution over Unix-only discovery commands.",
            object_schema(
                &["command"],
                vec![
                    (
                        "command",
                        json!({
                            "type":"string",
                            "maxLength":128,
                            "description":"Prefer a bare executable or portable builtin name. In host-access process runner mode, use an exact existing executable path when PATH lookup is insufficient, including Windows Program Files paths with spaces. When process_runner.allowed_executables contains '*', any PATH-resolvable executable is permitted by policy. Do not include arguments, shell syntax, or repeat this value in args; put executable arguments in args and do not split executable paths at spaces."
                        }),
                    ),
                    (
                        "args",
                        json!({
                            "type":"array",
                            "items":{"type":"string"},
                            "maxItems":64,
                            "description":"Command arguments only. For `echo hello`, use command='echo' and args=['hello'], not args=['echo hello']. For `node script.js`, use command='node' and args=['script.js'], not args=['node','script.js']; the runtime normalizes a duplicated leading command token and leading --cwd PATH into the cwd field, but callers should not rely on that. For npm scripts, use command='npm' with args=['run','script'] and cwd set to the package directory, or args=['--prefix','project','run','script'] if cwd cannot be set; never run npm through node and never put --prefix on node. Portable workspace-scoped builtins include pwd, ls/dir, cat/type, and mkdir. Portable background lifecycle builtins include command='palyra.process.stop' and command='palyra.process.status' with args=[pid] for PIDs returned by a background process result. On Windows, Unix grep/find/xargs/sed/awk are not portable; Windows find is a text search command, not directory traversal, so use palyra.fs.list_dir/read_file/search for workspace discovery. Inline shell-eval flags such as -c, /c, and -Command are blocked; to verify generated Windows scripts, write a workspace .ps1 file with palyra.fs.apply_patch and run command='pwsh' or command='powershell' with args=['-NoProfile','-File','scripts/check.ps1']. Do not use mkdir, touch, echo redirection, or interpreter eval for file writes; use palyra.fs.apply_patch first, then use this tool only to verify."
                        }),
                    ),
                    (
                        "cwd",
                        json!({"type":"string","description":"Working directory. In sandbox mode, omit for the workspace root or use /workspace and /workspace/subdir as virtual workspace aliases. In host-access E2E mode, launch-context path prefixes are accepted only when they were explicitly provided by the CLI/tool context; do not invent $PALYRA_E2E_OS_ROOT or similar env aliases when the prompt already contains an absolute path."}),
                    ),
                    (
                        "env",
                        json!({
                            "type":"object",
                            "additionalProperties":{"type":"string"},
                            "maxProperties":32,
                            "description":"Optional environment variables for this child process, for fixture and local app settings such as PALYRA_E2E_HOME. Values are applied without shell syntax, after the sandbox base environment. Reserved runtime/path keys such as PATH, PATHEXT, PALYRA_CONFIG, PALYRA_STATE_ROOT, PALYRA_HOME, PALYRA_CLI_PROFILE, PALYRA_CLI_PROFILES_PATH, PALYRA_VAULT_DIR, LD_PRELOAD, LD_LIBRARY_PATH, DYLD_INSERT_LIBRARIES, and DYLD_LIBRARY_PATH are rejected."
                        }),
                    ),
                    (
                        "prepend_path",
                        json!({
                            "type":"array",
                            "items":{"type":"string"},
                            "maxItems":16,
                            "description":"Optional directories to prepend to the child PATH after runtime validation. Use this instead of env.PATH when a task requires a specific local toolchain such as bundled Node or pnpm. Each entry must be an existing directory inside the workspace, or inside approved host-access roots in host-access mode; entries also affect bare command resolution on Windows."
                        }),
                    ),
                    (
                        "background",
                        json!({
                            "type":"boolean",
                            "description":"Start an allowlisted local background process and return immediately. Use this instead of shell background syntax or nohup for temporary dev servers. The runtime returns bounded startup stdout/stderr snapshots, which may include a server URL or selected dynamic port. If the direct launcher exits during startup with code 0, the call succeeds with launcher_completed_successfully=true and no run-owned PID after terminating the launcher process tree; do not assume an external service remains alive. Non-zero startup exits still fail fast. Long-running child lifetimes default to run-owned: Palyra automatically stops them when the agent run reaches a terminal state, and may also stop them at the operator-configured tool execution timeout/runtime hard cap. For services that must survive the final answer or a hidden verifier, set lifetime_mode='detached' or keep_running_after_run=true; detached processes are not registered for terminal run cleanup, but still auto-kill after the returned cleanup.auto_kill_after_ms and must be handed off with cleanup.portable_stop_command. For local browser verification, bind to 127.0.0.1 with an explicit port and omit timeout_ms unless a specific long lifetime is needed; short background timeout_ms values are raised to the safe minimum when the execution cap permits."
                        }),
                    ),
                    (
                        "lifetime_mode",
                        json!({
                            "type":"string",
                            "enum":["run_owned","detached","until_verifier"],
                            "description":"Optional background lifecycle. Defaults to run_owned. Use detached or until_verifier only with background=true when the user or verifier explicitly needs the service after the agent final answer; the result includes durable_handoff=true, PID, inferred ports, safe start command preview, status command, stop command, and cleanup handle. Detached lifetimes are approval-free out of the box, but remain bounded by timeout_ms or the runtime maximum and surface advisory risk metadata when relevant."
                        }),
                    ),
                    (
                        "keep_running_after_run",
                        json!({
                            "type":"boolean",
                            "description":"Compatibility alias for lifetime_mode='detached'. Use only with background=true when a service must survive terminal run cleanup for user or verifier handoff."
                        }),
                    ),
                    (
                        "requested_egress_hosts",
                        json!({"type":"array","items":{"type":"string"},"maxItems":64}),
                    ),
                    (
                        "timeout_ms",
                        json!({"type":"integer","minimum":1,"description":"Foreground process timeout in milliseconds. For background=true this is the requested process lifetime, not just startup timeout; omit it for the default long-lived local-server window, or set a value of at least 120000 for browser/app verification loops. Shorter background values are raised to the safe minimum when the operator execution cap permits."}),
                    ),
                ],
                false,
            ),
            ToolParallelismPolicy::Exclusive,
            ToolResultProjectionPolicy::RedactedPreviewAndArtifact,
        ),
        entry(
            "palyra.process.stop",
            "Stop a run-owned background process by PID returned from palyra.process.run.",
            object_schema(
                &["pid"],
                vec![(
                    "pid",
                    json!({"type":"integer","minimum":1,"description":"PID returned by a background palyra.process.run result for this run."}),
                )],
                false,
            ),
            ToolParallelismPolicy::Exclusive,
            ToolResultProjectionPolicy::InlineUnlessLarge,
        ),
        entry(
            "palyra.process.status",
            "Check whether a background process PID returned from palyra.process.run is still alive.",
            object_schema(
                &["pid"],
                vec![(
                    "pid",
                    json!({"type":"integer","minimum":1,"description":"PID returned by a background palyra.process.run result for this run."}),
                )],
                false,
            ),
            ToolParallelismPolicy::ReadOnly,
            ToolResultProjectionPolicy::InlineUnlessLarge,
        ),
        entry(
            "palyra.process.list",
            "List background process PIDs currently tracked for cleanup in this run.",
            object_schema(&[], Vec::new(), false),
            ToolParallelismPolicy::ReadOnly,
            ToolResultProjectionPolicy::InlineUnlessLarge,
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
            "Create, update, or delete workspace files by applying a strict workspace-confined Palyra patch document with attestation. This is the primary file-write tool.",
            object_schema(
                &["patch"],
                vec![
                    (
                        "patch",
                        json!({
                            "type":"string",
                            "description":"A complete Palyra patch document. It must start with '*** Begin Patch', contain one or more '*** Add File:', '*** Replace File:', '*** Replace Line:', '*** Update File:', or '*** Delete File:' operations, and end with '*** End Patch'. The final non-whitespace line must be exactly '*** End Patch'; never send a partial or truncated patch. For large file creation or multi-file changes, split work into multiple smaller complete apply_patch calls. Add-file and replace-file body lines may start with '+'. For Add File, missing parent directories are created automatically. Replace Line requires the target to exist and accepts exactly one '-' old line followed by one '+' new line; use it after search/read_file confirms a unique exact target line. Replace File requires the target to exist and is the deterministic fallback after reading a file when update hunk context cannot be matched. Update-file operations require '@@' hunks; hunk lines should start with ' ', '+', or '-', and a bare empty hunk line is accepted as blank context. Never write redaction placeholders such as [REDACTED], [REDACTED_SECRET], or <redacted> into secret-bearing files like .env; preserve existing secret lines or update example/template files instead. Use forward-slash relative paths only, such as reports/report.md; never use host absolute paths."
                        }),
                    ),
                    (
                        "workspace_root",
                        json!({
                            "type":"string",
                            "description":"Optional workspace subdirectory to treat as the patch root. Use /workspace for the current agent workspace root, /workspace/path or workspace/path for a subdirectory alias, or a plain relative subdirectory. For write calls, a missing relative subdirectory is created inside the active agent workspace root. Omit for the agent workspace root."
                        }),
                    ),
                    ("dry_run", json!({"type":"boolean"})),
                ],
                false,
            ),
            ToolParallelismPolicy::Exclusive,
            ToolResultProjectionPolicy::RedactedPreviewAndArtifact,
        ),
        entry(
            "palyra.plugin.run",
            "Run a verified installed Palyra skill module or a bounded inline WASM module. Use skill_id/tool_id for installed skill modules; use module_wat or module_base64 for inline modules when runtime policy permits them.",
            object_schema(
                &[],
                vec![
                    (
                        "skill_id",
                        json!({"type":"string","description":"Installed skill identifier to resolve when no inline module payload is supplied."}),
                    ),
                    (
                        "skill_version",
                        json!({"type":"string","description":"Optional installed skill version; omit to use the current installed version."}),
                    ),
                    (
                        "module_path",
                        json!({"type":"string","description":"Optional modules/*.wasm path inside the installed skill artifact."}),
                    ),
                    (
                        "tool_id",
                        json!({"type":"string","description":"Optional tool identifier from the installed skill manifest, used to resolve the target module."}),
                    ),
                    (
                        "module_wat",
                        json!({"type":"string","description":"Inline WebAssembly text module for bounded local execution when inline modules are enabled."}),
                    ),
                    (
                        "module_base64",
                        json!({"type":"string","description":"Base64-encoded inline WebAssembly binary for bounded local execution when inline modules are enabled."}),
                    ),
                    (
                        "entrypoint",
                        json!({"type":"string","description":"Exported WebAssembly function to call; defaults to run."}),
                    ),
                    (
                        "capabilities",
                        json!({"type":"object","description":"Optional requested host capabilities; each requested capability must be allowed by runtime policy.","properties":{},"additionalProperties":true}),
                    ),
                ],
                false,
            ),
            ToolParallelismPolicy::Exclusive,
            ToolResultProjectionPolicy::RedactedPreviewAndArtifact,
        ),
    ];

    for browser_tool in browser_tool_names() {
        // Session lifecycle results are tiny and safe to inline; every other
        // browser tool can return page-derived content, so results go through
        // the redacted-preview/artifact projection.
        let projection_policy = if matches!(
            *browser_tool,
            "palyra.browser.session.create" | "palyra.browser.session.close"
        ) {
            ToolResultProjectionPolicy::InlineUnlessLarge
        } else {
            ToolResultProjectionPolicy::RedactedPreviewAndArtifact
        };
        entries.push(entry(
            browser_tool,
            browser_tool_description(browser_tool),
            browser_tool_schema(browser_tool),
            ToolParallelismPolicy::Exclusive,
            projection_policy,
        ));
    }

    // Name-sorted so catalog hashing never depends on declaration order.
    entries.sort_by(|left, right| left.name.cmp(&right.name));
    entries
}

/// Looks up one builtin registry entry by exact tool name.
pub(crate) fn registry_entry(tool_name: &str) -> Option<ToolRegistryEntry> {
    registry_entries().into_iter().find(|entry| entry.name == tool_name)
}

/// Assembles one registry entry, sourcing capabilities and approval posture
/// from `tool_protocol` metadata so the registry stays consistent with the
/// policy layer instead of duplicating those decisions.
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

/// Builds a `type: object` schema from `(name, schema)` property pairs.
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

/// Every brokered browser tool registered alongside the core tools.
fn browser_tool_names() -> &'static [&'static str] {
    &[
        "palyra.browser.session.create",
        "palyra.browser.session.close",
        "palyra.browser.navigate",
        "palyra.browser.reload",
        "palyra.browser.click",
        "palyra.browser.type",
        "palyra.browser.fill",
        "palyra.browser.upload",
        "palyra.browser.press",
        "palyra.browser.select",
        "palyra.browser.viewport",
        "palyra.browser.highlight",
        "palyra.browser.scroll",
        "palyra.browser.wait_for",
        "palyra.browser.title",
        "palyra.browser.screenshot",
        "palyra.browser.pdf",
        "palyra.browser.observe",
        "palyra.browser.storage",
        "palyra.browser.network_log",
        "palyra.browser.console_log",
        "palyra.browser.reset_state",
        "palyra.browser.tabs.list",
        "palyra.browser.tabs.open",
        "palyra.browser.tabs.switch",
        "palyra.browser.tabs.close",
        "palyra.browser.permissions.get",
        "palyra.browser.permissions.set",
        "palyra.browser.downloads.list",
        "palyra.browser.downloads.get",
    ]
}

/// Model-facing description for one browser tool.
fn browser_tool_description(tool_name: &str) -> &'static str {
    match tool_name {
        "palyra.browser.session.create" => "Create a brokered browser session.",
        "palyra.browser.session.close" => "Close a brokered browser session.",
        "palyra.browser.navigate" => "Navigate a brokered browser session to a URL.",
        "palyra.browser.reload" => "Reload the active tab in a brokered browser session.",
        "palyra.browser.click" => "Click an element in a brokered browser session.",
        "palyra.browser.type" => "Type text in a brokered browser session.",
        "palyra.browser.fill" => "Replace an element value in a brokered browser session.",
        "palyra.browser.upload" => "Set a file input from an audited local file path.",
        "palyra.browser.press" => "Press a key in a brokered browser session.",
        "palyra.browser.select" => "Select an option in a brokered browser session.",
        "palyra.browser.viewport" => "Set the active browser viewport dimensions.",
        "palyra.browser.highlight" => "Highlight an element in a brokered browser session.",
        "palyra.browser.scroll" => "Scroll a brokered browser session.",
        "palyra.browser.wait_for" => "Wait for a browser condition.",
        "palyra.browser.title" => "Read the current browser title.",
        "palyra.browser.screenshot" => {
            "Capture a bounded browser screenshot and optionally save it directly to a workspace or approved user-owned output_path; do not use it alone as visible text evidence."
        }
        "palyra.browser.pdf" => {
            "Capture a bounded browser PDF and optionally save it directly to a workspace or approved user-owned output_path."
        }
        "palyra.browser.observe" => {
            "Observe visible browser state, bounded DOM/accessibility visible text evidence, safe current form/storage state, and optional read-only selector geometry/computed-style captures for layout assertions."
        }
        "palyra.browser.storage" => "Inspect bounded visible cookies and localStorage for diagnostics.",
        "palyra.browser.network_log" => {
            "Read bounded browser network logs. Entries include entry_id, phase=response, captured_at_unix_ms, status, latency, and request_url; use since_unix_ms after an action boundary to ignore stale entries."
        }
        "palyra.browser.console_log" => "Read bounded browser console logs.",
        "palyra.browser.reset_state" => "Reset browser session state.",
        "palyra.browser.tabs.list" => "List browser tabs.",
        "palyra.browser.tabs.open" => "Open a browser tab.",
        "palyra.browser.tabs.switch" => "Switch the active browser tab.",
        "palyra.browser.tabs.close" => "Close a browser tab.",
        "palyra.browser.permissions.get" => "Read browser permission state.",
        "palyra.browser.permissions.set" => "Update browser permission state.",
        "palyra.browser.downloads.list" => "List browser download artifacts.",
        "palyra.browser.downloads.get" => {
            "Return bounded browser download artifact metadata plus a first-byte content preview. The response marks content_truncated=true when the artifact is larger than max_bytes; output_path writes are allowed only for complete content."
        }
        _ => "Operate a brokered browser session.",
    }
}

/// Builds the input schema for one browser tool from the shared
/// session/timeout base plus tool-specific fields; `session.create` is the
/// only tool that does not require `session_id`.
fn browser_tool_schema(tool_name: &str) -> Value {
    let mut properties = vec![
        (
            "session_id",
            json!({
                "type":"string",
                "description":"Required for every browser tool except palyra.browser.session.create. Copy the exact 26-character session_id returned by palyra.browser.session.create; do not invent, shorten, or reuse a URL/label as the session_id."
            }),
        ),
        ("timeout_ms", json!({"type":"integer","minimum":1})),
    ];
    let mut required = if tool_name == "palyra.browser.session.create" {
        Vec::<&str>::new()
    } else {
        vec!["session_id"]
    };
    match tool_name {
        "palyra.browser.navigate" | "palyra.browser.tabs.open" => {
            properties.push((
                "url",
                json!({
                    "type":"string",
                    "description":"Target URL. file:// URLs are supported only for regular files inside the active agent workspace roots; after opening one, use palyra.browser.observe for DOM/text evidence instead of treating a filesystem read as browser validation."
                }),
            ));
            properties.push((
                "allow_private_targets",
                json!({
                    "type":"boolean",
                    "description":"Optional private-target access override for requests that are explicitly authorized by runtime policy."
                }),
            ));
            required.push("url");
        }
        "palyra.browser.tabs.switch" => {
            properties.push((
                "tab_id",
                json!({
                    "type":"string",
                    "description":"Required tab_id returned by palyra.browser.tabs.list or palyra.browser.tabs.open. Copy the exact tab_id value; do not use a tab index, URL, title, or session_id."
                }),
            ));
            required.push("tab_id");
        }
        "palyra.browser.reload" => {
            properties.push((
                "allow_private_targets",
                json!({
                    "type":"boolean",
                    "description":"Optional private-target access override for reloading a current URL that is explicitly authorized by runtime policy."
                }),
            ));
            properties.push((
                "allow_redirects",
                json!({"type":"boolean","description":"Whether reload navigation may follow redirects. Defaults to true."}),
            ));
            properties.push((
                "max_redirects",
                json!({"type":"integer","minimum":0,"description":"Maximum redirects for reload navigation. Defaults to 3."}),
            ));
        }
        "palyra.browser.click" | "palyra.browser.highlight" => {
            properties.push((
                "selector",
                json!({"type":"string","description":"CSS selector grounded in a prior palyra.browser.observe result. If this selector is not found, call observe and retry once with an observed id, label, role, name, or visible-text-adjacent selector."}),
            ));
            required.push("selector");
        }
        "palyra.browser.type" | "palyra.browser.fill" => {
            properties.push((
                "selector",
                json!({"type":"string","description":"CSS selector grounded in a prior palyra.browser.observe result. Prefer stable input id/name/label evidence; if not found, call observe before retrying."}),
            ));
            properties.push(("text", json!({"type":"string"})));
            if tool_name == "palyra.browser.type" {
                properties.push((
                    "clear_existing",
                    json!({"type":"boolean","description":"Set true to replace the current input/textarea/contenteditable value before typing. Prefer palyra.browser.fill for form value replacement instead of click + Control+A + type."}),
                ));
            }
            required.extend(["selector", "text"]);
        }
        "palyra.browser.upload" => {
            properties.push((
                "selector",
                json!({"type":"string","description":"CSS selector for an <input type=file> grounded in a prior palyra.browser.observe result."}),
            ));
            properties.push((
                "file_path",
                json!({"type":"string","description":"Absolute user-owned OS path or workspace path to upload. The daemon resolves and audits the path; protected system paths are denied."}),
            ));
            properties
                .push(("capture_failure_screenshot", json!({"type":"boolean","default":true})));
            properties
                .push(("max_failure_screenshot_bytes", json!({"type":"integer","minimum":0})));
            required.extend(["selector", "file_path"]);
        }
        "palyra.browser.press" => {
            properties.push((
                "key",
                json!({"type":"string","description":"Keyboard key or chord. Common names include Enter, Escape, Tab, Space, Spacebar, Ctrl+Space, and literal space."}),
            ));
            required.push("key");
        }
        "palyra.browser.select" => {
            properties.push((
                "selector",
                json!({"type":"string","description":"CSS selector for a select/input control grounded in a prior palyra.browser.observe result. If the selector is not found, inspect the current DOM/accessibility state before retrying."}),
            ));
            properties.push(("value", json!({"type":"string"})));
            required.extend(["selector", "value"]);
        }
        "palyra.browser.viewport" => {
            properties.push((
                "width",
                json!({"type":"integer","minimum":50,"maximum":10000,"description":"Viewport width in CSS pixels. Use values like 375 for mobile, 768 for tablet, or 1440 for desktop verification; oversized width*height or scaled pixel areas are rejected by the browser service."}),
            ));
            properties.push((
                "height",
                json!({"type":"integer","minimum":50,"maximum":10000,"description":"Viewport height in CSS pixels. Pair with width before screenshot or observe when verifying responsive layouts; keep total area bounded because oversized width*height or scaled pixel areas are rejected."}),
            ));
            properties.push((
                "device_scale_factor",
                json!({"type":"number","exclusiveMinimum":0,"maximum":8,"default":1,"description":"Device pixel ratio multiplier. Prefer 1 or 2; high values are rejected when width*height*device_scale_factor^2 exceeds browser safety limits."}),
            ));
            properties.push((
                "mobile",
                json!({"type":"boolean","description":"Enable mobile device metrics and touch emulation for responsive/mobile layout checks."}),
            ));
            required.extend(["width", "height"]);
        }
        "palyra.browser.scroll" => {
            properties.push(("delta_x", json!({"type":"integer"})));
            properties.push(("delta_y", json!({"type":"integer"})));
        }
        "palyra.browser.wait_for" => {
            properties.push((
                "selector",
                json!({"type":"string","description":"Optional CSS selector to wait for. Provide selector or text; if unsure, call palyra.browser.observe first."}),
            ));
            properties.push((
                "text",
                json!({"type":"string","description":"Optional visible text snippet to wait for. Provide selector or text; do not call wait_for with both empty."}),
            ));
            properties.push(("poll_interval_ms", json!({"type":"integer","minimum":1})));
        }
        "palyra.browser.screenshot" => {
            properties.push(("max_bytes", json!({"type":"integer","minimum":1})));
            properties.push(("format", json!({"type":"string","enum":["png"],"default":"png"})));
            properties.push((
                "output_path",
                json!({"type":"string","description":"Optional workspace-relative path, or approved user-owned absolute OS path, where the daemon should write the PNG bytes. Use this when the user asks to save a screenshot or when visual/OCR follow-up should use palyra.image.observe; screenshot output omits model-visible image_base64."}),
            ));
        }
        "palyra.browser.pdf" => {
            properties.push(("max_bytes", json!({"type":"integer","minimum":1})));
            properties.push((
                "output_path",
                json!({"type":"string","description":"Optional workspace-relative path, or approved user-owned absolute OS path, where the daemon should write the PDF bytes. Use this when the user asks to save a PDF; do not try to write pdf_base64 with file patch tools."}),
            ));
        }
        "palyra.browser.session.create" => {
            properties.push((
                "profile_id",
                json!({
                    "type":"string",
                    "description":"Optional existing browser profile_id returned by a profile list/create flow. Omit it for ordinary one-off sessions; do not invent labels or reuse scenario names as profile_id."
                }),
            ));
            properties.push((
                "private_profile",
                json!({"type":"boolean","description":"Set true only when the user asks for a non-persistent private browser session. Ordinary sessions persist state within the current agent session by default."}),
            ));
            properties.push((
                "persistence_enabled",
                json!({"type":"boolean","description":"Defaults to true for ordinary sessions so close/recreate recovery preserves browser state. Set false only for explicit ephemeral sessions."}),
            ));
            properties.push((
                "persistence_id",
                json!({"type":"string","description":"Optional advanced stable persistence id. Omit for ordinary sessions; the runtime uses the current agent session id."}),
            ));
            properties.push(("allow_private_targets", json!({"type":"boolean"})));
            properties.push(("allow_downloads", json!({"type":"boolean"})));
            properties.push((
                "budget",
                json!({"type":"object","properties":{},"additionalProperties":true}),
            ));
        }
        "palyra.browser.downloads.list" => {
            properties.push(("limit", json!({"type":"integer","minimum":1})));
            properties.push(("quarantined_only", json!({"type":"boolean"})));
        }
        "palyra.browser.downloads.get" => {
            properties.push((
                "artifact_id",
                json!({"type":"string","description":"Optional artifact id returned by palyra.browser.downloads.list. If omitted, the latest non-quarantined artifact is fetched."}),
            ));
            properties.push((
                "max_bytes",
                json!({"type":"integer","minimum":1,"description":"Maximum bytes to return from the start of the artifact for inspection. Larger artifacts return content_truncated=true with metadata instead of failing."}),
            ));
            properties.push((
                "output_path",
                json!({"type":"string","description":"Optional workspace-relative path, or approved user-owned absolute OS path, where the daemon should write artifact bytes. This requires an untruncated response; omit output_path when inspecting a large artifact preview."}),
            ));
        }
        "palyra.browser.storage" => {
            properties.push(("max_cookie_bytes", json!({"type":"integer","minimum":1})));
            properties.push(("max_storage_bytes", json!({"type":"integer","minimum":1})));
        }
        "palyra.browser.network_log" => {
            properties.push(("limit", json!({"type":"integer","minimum":1})));
            properties.push(("include_headers", json!({"type":"boolean","default":false})));
            properties.push(("max_payload_bytes", json!({"type":"integer","minimum":1})));
            properties.push((
                "since_unix_ms",
                json!({"type":"integer","minimum":0,"description":"Only return entries captured at or after this Unix millisecond timestamp. Use it after a click/reload/action boundary to avoid mixing stale network entries from earlier page states."}),
            ));
        }
        "palyra.browser.observe" => {
            properties.push((
                "include_dom_snapshot",
                json!({"type":"boolean","default":true,"description":"Include bounded DOM evidence with safe current form values when available."}),
            ));
            properties.push((
                "include_accessibility_tree",
                json!({"type":"boolean","default":true,"description":"Include bounded accessibility roles, names, and grounded selectors."}),
            ));
            properties.push((
                "include_visible_text",
                json!({"type":"boolean","default":true,"description":"Include visible text plus safe browser state summaries, including current form and local/session storage state when available."}),
            ));
            properties.push(("max_dom_snapshot_bytes", json!({"type":"integer","minimum":0})));
            properties
                .push(("max_accessibility_tree_bytes", json!({"type":"integer","minimum":0})));
            properties.push(("max_visible_text_bytes", json!({"type":"integer","minimum":0})));
            properties.push((
                "capture_selectors",
                json!({"type":"array","items":{"type":"string"},"maxItems":8,"description":"Optional CSS selectors to inspect without mutating page code. Returns element_captures with bounding_rect, visible, visible-safe text preview for rendered non-hidden elements, and computed_styles; hidden/script/style/template text is omitted. Use this for responsive layout, overlap, visibility, and computed-style assertions instead of adding measurement code or console logs to the app."}),
            ));
            properties.push((
                "computed_style_properties",
                json!({"type":"array","items":{"type":"string"},"maxItems":16,"description":"Optional CSS property names to include for capture_selectors. Defaults include display, visibility, opacity, position, z-index, overflow, pointer-events, font-size, line-height, margin, and padding."}),
            ));
            properties.push((
                "max_capture_text_bytes",
                json!({"type":"integer","minimum":0,"description":"Maximum text preview bytes per captured selector. Defaults to a small bounded preview."}),
            ));
        }
        _ => {}
    }
    object_schema(required.as_slice(), properties, true)
}

#[cfg(test)]
mod tests {
    use super::registry_entry;

    #[test]
    fn browser_tabs_switch_registry_exposes_required_tab_id() {
        let entry = registry_entry("palyra.browser.tabs.switch").expect("tabs switch entry exists");
        let required = entry
            .input_schema
            .pointer("/required")
            .and_then(serde_json::Value::as_array)
            .expect("required fields should be visible");
        assert!(required.iter().any(|value| value.as_str() == Some("session_id")));
        assert!(required.iter().any(|value| value.as_str() == Some("tab_id")));

        let tab_id = entry
            .input_schema
            .pointer("/properties/tab_id")
            .expect("tab_id property should be visible to models");
        assert_eq!(tab_id.get("type").and_then(serde_json::Value::as_str), Some("string"));
        let description = tab_id
            .get("description")
            .and_then(serde_json::Value::as_str)
            .expect("tab_id should explain where to get the value");
        assert!(description.contains("palyra.browser.tabs.list"));
        assert!(description.contains("exact tab_id"));
    }

    #[test]
    fn browser_network_log_registry_exposes_since_filter() {
        let entry = registry_entry("palyra.browser.network_log").expect("network_log entry exists");
        let since = entry
            .input_schema
            .pointer("/properties/since_unix_ms")
            .expect("since_unix_ms property should be visible");

        assert_eq!(since.get("type").and_then(serde_json::Value::as_str), Some("integer"));
        let description = since
            .get("description")
            .and_then(serde_json::Value::as_str)
            .expect("since_unix_ms should explain its boundary usage");
        assert!(description.contains("click/reload/action boundary"));
    }

    #[test]
    fn process_runner_registry_steers_file_writes_to_patch_tool() {
        let entry = registry_entry("palyra.process.run").expect("process runner entry exists");
        assert!(entry.description.contains("not this tool"));
        assert!(entry.description.contains("PowerShell script-file execution"));

        let args_description = entry
            .input_schema
            .pointer("/properties/args/description")
            .and_then(serde_json::Value::as_str)
            .expect("args description should be visible to models");
        let command_description = entry
            .input_schema
            .pointer("/properties/command/description")
            .and_then(serde_json::Value::as_str)
            .expect("command description should be visible to models");
        assert!(command_description.contains("exact existing executable path"));
        assert!(command_description.contains("do not split executable paths at spaces"));
        assert!(args_description.contains("Do not use mkdir, touch"));
        assert!(args_description.contains("palyra.fs.apply_patch first"));
        assert!(args_description.contains("not args=['node','script.js']"));
        assert!(args_description.contains("leading --cwd PATH"));
        assert!(args_description.contains("command='npm'"));
        assert!(args_description.contains("never run npm through node"));
        assert!(args_description.contains("Windows find is a text search command"));
        assert!(args_description.contains("palyra.fs.list_dir/read_file/search"));
        assert!(args_description.contains("cat/type"));
        assert!(args_description.contains("command='pwsh'"));
        assert!(args_description.contains("'-File','scripts/check.ps1'"));
        assert!(args_description.contains("Inline shell-eval flags"));

        let background_description = entry
            .input_schema
            .pointer("/properties/background/description")
            .and_then(serde_json::Value::as_str)
            .expect("background description should be visible to models");
        assert!(background_description.contains("omit timeout_ms"));
        assert!(background_description.contains("safe minimum"));
        assert!(background_description.contains("terminal state"));

        let lifetime_mode_description = entry
            .input_schema
            .pointer("/properties/lifetime_mode/description")
            .and_then(serde_json::Value::as_str)
            .expect("lifetime_mode description should be visible to models");
        assert!(lifetime_mode_description.contains("approval-free out of the box"));
        assert!(lifetime_mode_description.contains("advisory risk metadata"));

        let timeout_description = entry
            .input_schema
            .pointer("/properties/timeout_ms/description")
            .and_then(serde_json::Value::as_str)
            .expect("timeout_ms description should be visible to models");
        assert!(timeout_description.contains("background=true"));
        assert!(timeout_description.contains("120000"));
        assert!(timeout_description.contains("safe minimum"));

        let cwd_description = entry
            .input_schema
            .pointer("/properties/cwd/description")
            .and_then(serde_json::Value::as_str)
            .expect("cwd description should be visible to models");
        assert!(cwd_description.contains("/workspace/subdir"));
        assert!(cwd_description.contains("do not invent $PALYRA_E2E_OS_ROOT"));

        let env_description = entry
            .input_schema
            .pointer("/properties/env/description")
            .and_then(serde_json::Value::as_str)
            .expect("env description should be visible to models");
        assert!(env_description.contains("PALYRA_E2E_HOME"));
        assert!(env_description.contains("without shell syntax"));
        assert!(env_description.contains("PATH"));

        let prepend_path_description = entry
            .input_schema
            .pointer("/properties/prepend_path/description")
            .and_then(serde_json::Value::as_str)
            .expect("prepend_path description should be visible to models");
        assert!(prepend_path_description.contains("instead of env.PATH"));
        assert!(prepend_path_description.contains("bundled Node"));
        assert!(prepend_path_description.contains("bare command resolution"));

        let background_description = entry
            .input_schema
            .pointer("/properties/background/description")
            .and_then(serde_json::Value::as_str)
            .expect("background description should be visible to models");
        assert!(background_description.contains("fail fast"));
        assert!(background_description.contains("startup stdout/stderr snapshots"));
        assert!(background_description.contains("selected dynamic port"));
        assert!(background_description.contains("operator-configured tool execution timeout"));

        let stop = registry_entry("palyra.process.stop").expect("process stop entry exists");
        assert_eq!(
            stop.input_schema.pointer("/required/0").and_then(serde_json::Value::as_str),
            Some("pid")
        );
        let status = registry_entry("palyra.process.status").expect("process status entry exists");
        assert_eq!(
            status.input_schema.pointer("/required/0").and_then(serde_json::Value::as_str),
            Some("pid")
        );
        let list = registry_entry("palyra.process.list").expect("process list entry exists");
        assert!(list.description.contains("tracked for cleanup"));
    }

    #[test]
    fn apply_patch_registry_explains_workspace_report_file_creation() {
        let entry = registry_entry("palyra.fs.apply_patch").expect("patch entry exists");
        assert!(entry.description.contains("primary file-write tool"));

        let patch_description = entry
            .input_schema
            .pointer("/properties/patch/description")
            .and_then(serde_json::Value::as_str)
            .expect("patch description should be visible to models");
        assert!(patch_description.contains("*** Replace File:"));
        assert!(patch_description.contains("*** Replace Line:"));
        assert!(patch_description.contains("one '-' old line"));
        assert!(patch_description.contains("unique exact target line"));
        assert!(patch_description.contains("final non-whitespace line"));
        assert!(patch_description.contains("multiple smaller complete apply_patch calls"));
        assert!(patch_description.contains("context cannot be matched"));
        assert!(patch_description.contains("bare empty hunk line is accepted as blank context"));
        assert!(patch_description.contains("missing parent directories"));
        assert!(patch_description.contains("Never write redaction placeholders"));
        assert!(patch_description.contains("[REDACTED_SECRET]"));
        assert!(patch_description.contains("reports/report.md"));
        assert!(patch_description.contains("never use host absolute paths"));
    }

    #[test]
    fn os_file_registry_exposes_cache_discovery_operations() {
        let entry = registry_entry("palyra.fs.os_file").expect("os_file entry exists");
        let operation_values = entry
            .input_schema
            .pointer("/properties/operation/enum")
            .and_then(serde_json::Value::as_array)
            .expect("os_file operation enum should be visible to models");
        assert!(operation_values.iter().any(|value| value.as_str() == Some("list_dir")));
        assert!(operation_values.iter().any(|value| value.as_str() == Some("search")));
        assert!(operation_values.iter().any(|value| value.as_str() == Some("delete_empty_dir")));
        let operation_description = entry
            .input_schema
            .pointer("/properties/operation/description")
            .and_then(serde_json::Value::as_str)
            .expect("os_file operation description should be visible to models");
        assert!(operation_description.contains("delete_empty_dir"));
        assert!(operation_description.contains("rmdir"));

        let path_description = entry
            .input_schema
            .pointer("/properties/path/description")
            .and_then(serde_json::Value::as_str)
            .expect("os_file path description should be visible to models");
        assert!(path_description.contains("$PALYRA_E2E_OS_ROOT"));
        assert!(path_description.contains("%PALYRA_E2E_OS_ROOT%"));

        let target_description = entry
            .input_schema
            .pointer("/properties/target_path/description")
            .and_then(serde_json::Value::as_str)
            .expect("os_file target_path description should be visible to models");
        assert!(target_description.contains("workspace-relative"));
        assert!(target_description.contains("%PALYRA_E2E_OS_ROOT%"));
        assert!(target_description.contains("/workspace/data/imported/file.csv"));

        let query_description = entry
            .input_schema
            .pointer("/properties/query/description")
            .and_then(serde_json::Value::as_str)
            .expect("os_file query description should be visible to models");
        assert!(query_description.contains("filename/path/content"));
        let full_replace_description = entry
            .input_schema
            .pointer("/properties/full_replace/description")
            .and_then(serde_json::Value::as_str)
            .expect("os_file full_replace description should be visible to models");
        assert!(full_replace_description.contains("replaces the whole existing file"));
        assert!(full_replace_description.contains("append/partial-edit semantics are unsupported"));
        assert!(entry.input_schema.pointer("/properties/max_entries").is_some());
        assert!(entry.input_schema.pointer("/properties/max_matches").is_some());
    }

    #[test]
    fn routines_control_registry_exposes_run_cap() {
        let entry =
            registry_entry("palyra.routines.control").expect("routines control entry exists");
        let description = entry.description.as_str();
        assert!(description.contains("Set max_runs"));
        let max_runs = entry
            .input_schema
            .pointer("/properties/max_runs")
            .expect("max_runs should be visible to models");
        assert_eq!(max_runs.pointer("/minimum").and_then(serde_json::Value::as_i64), Some(1));
    }

    #[test]
    fn browser_wait_for_schema_exposes_required_condition_fields() {
        let entry = registry_entry("palyra.browser.wait_for").expect("wait_for entry exists");
        let selector_description = entry
            .input_schema
            .pointer("/properties/selector/description")
            .and_then(serde_json::Value::as_str)
            .expect("wait_for selector description should be visible to models");
        let text_description = entry
            .input_schema
            .pointer("/properties/text/description")
            .and_then(serde_json::Value::as_str)
            .expect("wait_for text description should be visible to models");

        assert!(selector_description.contains("Provide selector or text"));
        assert!(text_description.contains("do not call wait_for with both empty"));

        let type_entry = registry_entry("palyra.browser.type").expect("type entry exists");
        let type_selector_description = type_entry
            .input_schema
            .pointer("/properties/selector/description")
            .and_then(serde_json::Value::as_str)
            .expect("type selector description should be visible to models");
        assert!(type_selector_description.contains("palyra.browser.observe"));
        let clear_description = type_entry
            .input_schema
            .pointer("/properties/clear_existing/description")
            .and_then(serde_json::Value::as_str)
            .expect("type clear_existing description should be visible to models");
        assert!(clear_description.contains("palyra.browser.fill"));

        let fill_entry = registry_entry("palyra.browser.fill").expect("fill entry exists");
        assert!(fill_entry.description.contains("Replace"));
        assert_eq!(
            fill_entry.input_schema.pointer("/required/1").and_then(serde_json::Value::as_str),
            Some("selector")
        );
        assert_eq!(
            fill_entry.input_schema.pointer("/required/2").and_then(serde_json::Value::as_str),
            Some("text")
        );
    }

    #[test]
    fn browser_file_transfer_tools_are_visible_to_agents() {
        let upload = registry_entry("palyra.browser.upload").expect("upload entry exists");
        assert!(upload.description.contains("file input"));
        assert_eq!(
            upload.input_schema.pointer("/required/0").and_then(serde_json::Value::as_str),
            Some("session_id")
        );
        assert_eq!(
            upload.input_schema.pointer("/required/1").and_then(serde_json::Value::as_str),
            Some("selector")
        );
        assert_eq!(
            upload.input_schema.pointer("/required/2").and_then(serde_json::Value::as_str),
            Some("file_path")
        );
        let file_path_description = upload
            .input_schema
            .pointer("/properties/file_path/description")
            .and_then(serde_json::Value::as_str)
            .expect("upload file_path description should be visible to models");
        assert!(file_path_description.contains("Absolute user-owned OS path"));
        assert!(file_path_description.contains("workspace path"));

        let downloads_list =
            registry_entry("palyra.browser.downloads.list").expect("downloads list entry exists");
        assert_eq!(
            downloads_list.input_schema.pointer("/required/0").and_then(serde_json::Value::as_str),
            Some("session_id")
        );
        assert!(downloads_list.input_schema.pointer("/properties/limit").is_some());

        let downloads_get =
            registry_entry("palyra.browser.downloads.get").expect("downloads get entry exists");
        assert_eq!(
            downloads_get.input_schema.pointer("/required/0").and_then(serde_json::Value::as_str),
            Some("session_id")
        );
        let artifact_id_description = downloads_get
            .input_schema
            .pointer("/properties/artifact_id/description")
            .and_then(serde_json::Value::as_str)
            .expect("downloads get artifact_id description should be visible to models");
        assert!(artifact_id_description.contains("If omitted"));
        let max_bytes_description = downloads_get
            .input_schema
            .pointer("/properties/max_bytes/description")
            .and_then(serde_json::Value::as_str)
            .expect("downloads get max_bytes description should be visible to models");
        assert!(max_bytes_description.contains("content_truncated"));
    }

    #[test]
    fn browser_registry_marks_observe_as_visible_text_evidence() {
        let observe = registry_entry("palyra.browser.observe").expect("observe entry exists");
        assert!(observe.description.contains("visible text"));
        assert!(observe.description.contains("selector geometry"));
        assert_eq!(
            observe.input_schema.pointer("/required/0").and_then(serde_json::Value::as_str),
            Some("session_id")
        );
        let capture_description = observe
            .input_schema
            .pointer("/properties/capture_selectors/description")
            .and_then(serde_json::Value::as_str)
            .expect("observe capture selectors description should be visible to models");
        assert!(capture_description.contains("bounding_rect"));
        assert!(capture_description.contains("instead of adding measurement code"));
        assert_eq!(observe.input_schema["properties"]["capture_selectors"]["maxItems"], 8);
        assert_eq!(observe.input_schema["properties"]["computed_style_properties"]["maxItems"], 16);
        let storage = registry_entry("palyra.browser.storage").expect("storage entry exists");
        assert!(storage.description.contains("cookies"));
        assert_eq!(
            storage.input_schema.pointer("/required/0").and_then(serde_json::Value::as_str),
            Some("session_id")
        );
        assert!(storage.input_schema.pointer("/properties/max_cookie_bytes").is_some());
        assert!(storage.input_schema.pointer("/properties/max_storage_bytes").is_some());

        let navigate = registry_entry("palyra.browser.navigate").expect("navigate entry exists");
        let required = navigate
            .input_schema
            .pointer("/required")
            .and_then(serde_json::Value::as_array)
            .expect("navigate required fields should be present");
        assert!(required.iter().any(|value| value.as_str() == Some("session_id")));
        assert!(required.iter().any(|value| value.as_str() == Some("url")));
        let session_description = navigate
            .input_schema
            .pointer("/properties/session_id/description")
            .and_then(serde_json::Value::as_str)
            .expect("browser session_id description should be visible to models");
        assert!(session_description.contains("26-character session_id"));
        let url_description = navigate
            .input_schema
            .pointer("/properties/url/description")
            .and_then(serde_json::Value::as_str)
            .expect("browser url description should be visible to models");
        assert!(!url_description.contains("allow_private_targets=true"));
        assert!(!url_description.contains("localhost"));
        assert!(url_description.contains("file:// URLs"));
        assert!(url_description.contains("active agent workspace roots"));
        let private_override_description = navigate
            .input_schema
            .pointer("/properties/allow_private_targets/description")
            .and_then(serde_json::Value::as_str)
            .expect("private-target override description should be visible to models");
        assert!(private_override_description.contains("explicitly authorized by runtime policy"));
        assert!(!private_override_description.contains("Required when"));

        let reload = registry_entry("palyra.browser.reload").expect("reload entry exists");
        assert_eq!(
            reload.input_schema.pointer("/required/0").and_then(serde_json::Value::as_str),
            Some("session_id")
        );
        assert!(reload.input_schema.pointer("/properties/url").is_none());
        assert!(reload.input_schema.pointer("/properties/allow_redirects").is_some());
        assert!(reload.input_schema.pointer("/properties/max_redirects").is_some());

        let screenshot =
            registry_entry("palyra.browser.screenshot").expect("screenshot entry exists");
        assert!(screenshot.description.contains("do not use it alone"));

        let viewport = registry_entry("palyra.browser.viewport").expect("viewport entry exists");
        assert_eq!(
            viewport.input_schema.pointer("/required/1").and_then(serde_json::Value::as_str),
            Some("width")
        );
        assert_eq!(
            viewport.input_schema.pointer("/required/2").and_then(serde_json::Value::as_str),
            Some("height")
        );
        let mobile_description = viewport
            .input_schema
            .pointer("/properties/mobile/description")
            .and_then(serde_json::Value::as_str)
            .expect("viewport mobile description should be visible to models");
        assert!(mobile_description.contains("mobile"));
        let scale_description = viewport
            .input_schema
            .pointer("/properties/device_scale_factor/description")
            .and_then(serde_json::Value::as_str)
            .expect("viewport device scale description should be visible to models");
        assert!(scale_description.contains("safety limits"));
    }
}
