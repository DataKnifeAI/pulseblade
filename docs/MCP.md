# MCP

Agents use the [Model Context Protocol](https://modelcontextprotocol.io) tools below. Measured response sizes are in [ARCHITECTURE.md](ARCHITECTURE.md#token-efficiency).

The cheap agent loop is: summary snapshot, remember `as_of_seq`, poll `changes_since` or `if_changed_since`, drill down only on what changed.

## Tools

| Tool | Purpose |
|------|---------|
| `state_snapshot` | `summary` by default: counts by kind, collector health, host key metrics, unhealthy resources, `as_of_seq`. `detail=brief\|full` for lists; `if_changed_since` for near-free polling |
| `resource_explain` | One resource: labels, attributes, parent/children, latest metrics, recent changes, notes, open findings, prior actions |
| `metrics_query` | Bounded, downsampled series as a compact `values` array (`range`, `step`, `agg`) |
| `checkpoint_create` | Name the current point in the change journal |
| `changes_since` | Changes since a checkpoint, sequence number, timestamp, or relative duration |
| `export_bulk` | JSONL dump of resources and changes for offline analysis |
| `findings_list` | Open findings by default. Summaries only; `detail=true` adds causal context and error lines |
| `finding_ack` | Acknowledge a finding. An optional note is stored on its resource |
| `memory_note` / `memory_search` | Attach a short note, or search notes by substring |
| `logs_query` | Recent error lines for one resource (default 1h, capped) |
| `actions_list` | Built-in remediations with risk tier, parameter schema, preconditions, and rate limit |
| `remedies_list` | Resolved finding and action pairs, matched by rule and resource kind. The pilot reads this before a model call |
| `action_plan` | Plan a run. Optional `finding_id` ties it to a finding. Returns `plan_id` (same as `run_id`) and the tier. Does not execute |
| `action_execute` | `auto` runs now; `approve` waits; `deny` is rejected. A finished run is not repeated |
| `action_status` | One run, including output when it has finished, and whether its finding is resolved, still open, or the approval is still waiting |

MCP resources `pulseblade://state` and `pulseblade://findings` notify subscribers when the journal or the open-findings set changes.

The same queries are on the [HTTP API](HTTP.md). Action parameters and risk tiers are in [CONFIG.md](CONFIG.md).

## Clients

Any MCP client can connect: Cursor, Claude, agent runtimes such as Hermes Agent, local agents on Ollama, HolmesGPT, or a custom script. External agent runtimes such as Hermes Agent use MCP directly and do not need the pilot. Pilot model endpoints are in [ARCHITECTURE.md](ARCHITECTURE.md#pilot).

### HTTP

Against a running node (`pulseblade node`), Cursor (`~/.cursor/mcp.json`):

```json
{
  "mcpServers": {
    "pulseblade": { "url": "http://127.0.0.1:7171/mcp" }
  }
}
```

### stdio

No daemon required. The README quick start uses this block:

```json
{
  "mcpServers": {
    "pulseblade": { "command": "pulseblade", "args": ["mcp"] }
  }
}
```

`pulseblade mcp --dashboard` (args `["mcp", "--dashboard"]`) serves the HTTP dashboard and API on the listen address alongside stdio. `--listen 127.0.0.1:7171` implies `--dashboard` and overrides the config `listen` address. `dashboard = true` in the config file turns the same server on with no flag. The stdio MCP process can serve the dashboard when it is the collector, so a second `pulseblade node` is unnecessary. If that listen address is already in use, stdio MCP keeps running and the dashboard is skipped.

Use an absolute path (e.g. `~/.cargo/bin/pulseblade`, expanded) if `~/.cargo/bin` is not on the PATH your editor launches with.

## Other commands

```bash
# Serve an existing database without collecting (offline inspection, copied databases, demos)
pulseblade --db ./copy.db node --no-collect

pulseblade ctl status                  # plain-text summary: self-health, counts, unhealthy, recent changes
pulseblade ctl snapshot --detail brief # JSON, same shapes as the MCP tools
pulseblade ctl checkpoint before-deploy
pulseblade ctl changes --since before-deploy
pulseblade ctl export --since 1h > dump.jsonl
pulseblade ctl findings --detail
pulseblade ctl approve <run_id>
pulseblade ctl runs
pulseblade pilot --once

# Fleet hub. Nodes connect out; set [upstream] on each node and [[hub.nodes]] on the hub.
pulseblade hub
```

Hub enrollment, the reverse proxy, and deploy manifests: [DEPLOY.md](DEPLOY.md).
