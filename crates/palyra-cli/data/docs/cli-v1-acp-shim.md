# CLI v1 + ACP Bridge

Purpose: capture the current CLI v1 surface and the ACP bridge entry points used for IDE-style
agent integration and release-ready operator workflows.

## Top-level command families

- `palyra acp ...`
- `palyra agent ...`
- `palyra agents ...`
- `palyra approvals ...`
- `palyra auth ...`
- `palyra browser ...`
- `palyra channels ...`
- `palyra completion ...`
- `palyra config ...`
- `palyra configure ...`
- `palyra cron ...`
- `palyra docs ...`
- `palyra gateway ...`
- `palyra memory ...`
- `palyra node ...`
- `palyra nodes ...`
- `palyra onboarding wizard ...`
- `palyra patch apply ...`
- `palyra secrets ...`
- `palyra sessions ...`
- `palyra setup ...`
- `palyra skills ...`
- `palyra support-bundle ...`
- `palyra tunnel ...`
- `palyra update ...`
- `palyra uninstall ...`

Compatibility aliases remain available where implemented, including `init`, `daemon`, and
`onboard`.

## ACP bridge posture

- `palyra acp ...` is the preferred ACP bridge family.
- ACP commands stay discoverable in CLI help and operator docs so IDE integrations can target a
  stable bridge surface.
- ACP bridge behavior follows the same auth, routing, and policy posture as the rest of the CLI
  gateway-facing surfaces.
- ACP binding now tracks ACP session id, resolved session key, and gateway session id separately so
  reconnect and list flows stay stable after session reuse or metadata overrides.
- Compat/OpenAI-style responses expose `_palyra.run_id` and `_palyra.session_id` so support can
  correlate interop traffic with transcript and audit surfaces.

## ACP versus MCP

- ACP is the stateful editor bridge. Prefer it when the client expects session lifecycle,
  reconnect-friendly prompts, and inline approval mediation.
- MCP is the narrower stdio facade. Prefer `palyra mcp serve --read-only` for tool-oriented
  indexing, transcript reads, memory search, and approval inspection.
- MCP mutation tools (`session_create`, `session_prompt`, `approval_decide`) stay mapped onto the
  same approval and policy model as native CLI and ACP flows; they do not create a parallel
  governance path.

## Diagnostics and support surface

- `palyra gateway discover --verify-remote --json` is the canonical first-pass handshake and
  trust-state report for remote dashboard access.
- `palyra dashboard --verify-remote --json` confirms the currently configured server-cert or
  gateway-CA pin before opening the remote dashboard.
- `palyra support-bundle export --output ./artifacts/palyra-support-bundle.zip` remains the
  escalation path for handshake drift, trust rotation, or ACP/MCP interoperability failures.

## Release-ready operator guidance

- Prefer `palyra setup` over `palyra init` for bootstrap and packaged install examples.
- Prefer `palyra gateway` over `palyra daemon` for runtime and admin examples.
- Prefer `palyra onboarding wizard` over `palyra onboard` when clarity matters in docs and smoke
  coverage.
- Keep `palyra docs` available in portable installs so the ACP bridge, migration notes, and release
  checklist remain available offline.

## Browser and service lifecycle notes

- `palyra browser` is a real operator surface over `palyra-browserd`, not a pure lifecycle stub.
- `palyra node` and `palyra nodes` remain packaged and covered by release smoke because service and
  fleet workflows are part of the shipped operator contract.
- `palyra update --archive <zip> --dry-run` and `palyra uninstall --dry-run` are part of the
  package lifecycle contract and stay discoverable in help and docs.
