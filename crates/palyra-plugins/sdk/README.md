# Palyra Plugin SDK Contracts

The SDK crate publishes stable typed contract descriptors for host-mediated
plugins. The JSON snapshot in `tests/golden/plugin_sdk_contract_snapshot.json`
is the review boundary for public contract changes.

## Agent Harness Contract

Agent harness plugins implement `agent_harness@1` and are mediated by the host.
The host owns attempt selection, budgets, tool grants, resource grants,
callbacks, and cleanup. Plugins never receive raw vault material; they receive
only scoped handles and redacted attempt metadata.

The current agent harness operations are:

- `supports_agent_attempt`: report whether the harness can handle a prepared
  attempt under the host-provided selection mode and resource manifest.
- `claim_agent_attempt`: claim a prepared attempt after host selection.
- `run_agent_attempt`: execute the claimed attempt using only granted handles,
  redacted transcript views, and callback services.
- `dispose_agent_harness`: release handles, temporary resources, and callback
  subscriptions after completion, cancellation, timeout, or denial.

Host callback and cleanup services are exposed as capability-scoped descriptors:

- `agent_harness_callback`: emits bounded callback payloads through the host.
- `agent_harness_dispose_cleanup`: reports cleanup evidence and released
  resource references.

Both services redact payload/resource fields in audit output and do not return
raw secret material.

## Capability Manifest Requirements

Conforming plugins declare least-privilege needs in their capability manifest:

- `operator.plugin.sensitivity`
- `operator.plugin.tools_posture`
- `operator.plugin.resource_needs`

Harness plugins must also pass the built-in conformance fixture ids
`agent_harness_fake_host`, `agent_harness_dispose_cleanup`, and
`no_raw_vault_access`. These fixtures verify support/run/dispose flow, callback
redaction, resource cleanup, and the absence of raw vault access.
