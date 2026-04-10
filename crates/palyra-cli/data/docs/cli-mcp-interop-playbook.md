# CLI MCP Interop Playbook

Purpose: explain the shipped MCP facade, how it differs from ACP, and how to connect common
stdio-oriented tooling without weakening Palyra's policy or approval posture.

## Recommended rollout order

1. Start with read-only MCP:
   - `palyra mcp serve --read-only`
2. Validate session and transcript visibility:
   - `sessions_list`
   - `session_transcript_read`
   - `session_export`
   - `memory_search`
   - `approvals_list`
3. Only then allow controlled mutations:
   - `palyra mcp serve --allow-sensitive-tools`

## Tool surface

- Read-only tools:
  - `sessions_list`
  - `session_transcript_read`
  - `session_export`
  - `memory_search`
  - `approvals_list`
- Mutation tools:
  - `session_create`
  - `session_prompt`
  - `approval_decide`

`session_prompt` and `approval_decide` reuse the existing run-stream and approval machinery. If a
tool call needs human approval, the MCP result reports `status=approval_required` instead of
inventing a second approval model.

## ACP versus MCP

- Use ACP for editor-native session control, reconnect-heavy work, and inline permission prompts.
- Use MCP for tool-driven client integrations that expect `tools/list` and `tools/call` over stdio.
- Keep both on the same profile, token, and policy scope. MCP is not a bypass around ACP or the
  native CLI.

## Safe client patterns

- Prefer dedicated low-risk profiles for MCP rollout.
- Keep `--read-only` on until the client proves it only needs indexing and retrieval.
- Enable `--allow-sensitive-tools` only for clients that must trigger prompts or approvals.
- Pair remote dashboards with explicit `palyra dashboard --verify-remote --json` checks whenever
  trust material rotates.

## Generic stdio example

```json
{
  "command": "palyra",
  "args": ["mcp", "serve", "--read-only"],
  "env": {
    "PALYRA_CONFIG": "/path/to/palyra.toml"
  }
}
```

## Mutable stdio example

```json
{
  "command": "palyra",
  "args": ["mcp", "serve", "--allow-sensitive-tools"],
  "env": {
    "PALYRA_CONFIG": "/path/to/palyra.toml"
  }
}
```

## Troubleshooting

- If sessions are visible but prompts fail, verify the profile and admin/user token pairing first.
- If prompt runs return `approval_required`, resolve the approval through native surfaces or the
  `approval_decide` MCP tool using the existing approval scope semantics.
- If remote dashboard trust changes, rerun:
  - `palyra gateway discover --verify-remote --json`
  - `palyra dashboard --verify-remote --json`
- If interop behavior is still unclear, export:
  - `palyra support-bundle export --output ./artifacts/palyra-support-bundle.zip`
