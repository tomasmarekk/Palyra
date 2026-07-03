# Run Replay Diff

- Status: `failed`
- Events: `12`
- Diffs: `2`

| Path | Expected | Actual | Context |
| --- | --- | --- | --- |
| `$.expected.event_count` | `1` | `12` | trajectory event count |
| `$.expected.tool_proposals` | `["palyra.safe.read"]` | `["palyra.shell"]` | tool proposal stream changed |
