# Run Replay Diff

- Status: `failed`
- Events: `10`
- Diffs: `2`

| Path | Expected | Actual | Context |
| --- | --- | --- | --- |
| `$.expected.event_count` | `1` | `10` | trajectory event count |
| `$.expected.tool_proposals` | `["palyra.safe.read"]` | `["palyra.shell"]` | tool proposal stream changed |
