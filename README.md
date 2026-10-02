# Pulseblade

![Pulseblade — an amber pulse sweeping out from a central hub across a fleet of infrastructure nodes, revealing healthy and degraded hosts](docs/assets/pulseblade-hero.jpg)

**Pulseblade** — a pulse that reveals what is moving across your infrastructure.

Pulseblade is a self-hosted infrastructure monitor built for **AI agents as the primary consumer**. A lightweight per-host **node** collects metrics and state, keeps a change journal, and exposes everything through the [Model Context Protocol](https://modelcontextprotocol.io) (MCP). Agents ask "what changed since checkpoint X?" instead of drowning in alert storms; dashboards are a later, degraded view of the same data.

It scales in two shapes with the same MCP contract:

- **Single box** — one node, with an optional **pilot** (a local-LLM reasoning loop) watching the OS and user session.
- **Fleet** — many nodes feeding a **hub** (control node); pilots and other agents talk to the hub, nodes enforce their own action policy.

## Why

Existing tools are either human-first monitoring stacks with an agent bolted on, or agent-observability tools that watch LLMs rather than infrastructure. Pulseblade is designed the other way around:

- **MCP first** — structured, schema'd tools are the primary interface.
- **Standing, queryable state** — snapshots, checkpoints, and `changes_since` instead of human-cognitive alert floods.
- **Semantic labels and causal context** baked into telemetry.
- **Context-window friendly** — bounded, downsampled responses and bulk JSONL export.
- **Remediation as first-class tools** (planned) — idempotent, audited, and gated by risk tier.
- **Memory** (planned) — notes and prior actions attached to resources.
- **No per-seat anything** — one binary, embedded SQLite, runs anywhere.

## Status

Milestone **M1** (single-host node) — see [docs/PLAN.md](docs/PLAN.md).

| Works today | Planned |
|-------------|---------|
| Host, CPU, memory, disk, network, systemd service collectors | Logs, processes, user units; findings and memory (M2) |
| Change journal, checkpoints, `changes_since` | Gated remediation and audit chain (M3) |
| Metrics with query-time downsampling | Pilot: local-LLM reasoning loop (M4) |
| MCP over stdio and streamable HTTP | Docker, Proxmox (M5), hub (M6), Kubernetes (M7) |

## Quick start

```bash
make install            # cargo install --path crates/pulseblade

# Run this host's node: collectors + MCP over HTTP at http://127.0.0.1:7171/mcp
pulseblade node

# Or serve MCP over stdio (collects in-process unless a node already owns the database)
pulseblade mcp
```

### Connect an MCP client

Cursor (`~/.cursor/mcp.json`), HTTP against a running node:

```json
{
  "mcpServers": {
    "pulseblade": { "url": "http://127.0.0.1:7171/mcp" }
  }
}
```

Or stdio, with no daemon required:

```json
{
  "mcpServers": {
    "pulseblade": { "command": "pulseblade", "args": ["mcp"] }
  }
}
```

Use an absolute path (e.g. `~/.cargo/bin/pulseblade`, expanded) if `~/.cargo/bin` is not on the PATH your editor launches with.

### Debug from the shell

```bash
pulseblade ctl snapshot
pulseblade ctl checkpoint before-deploy
pulseblade ctl changes --since before-deploy
pulseblade ctl export --since 1h > dump.jsonl
```

## MCP tools (M1)

| Tool | Purpose |
|------|---------|
| `state_snapshot` | Compact current state: counts by kind, unhealthy resources first, key metrics, `as_of_seq` |
| `resource_explain` | One resource: labels, attributes, parent/children, latest metrics, recent changes |
| `metrics_query` | Bounded, downsampled time series (`range`, `step`, `agg`) |
| `checkpoint_create` | Name the current point in the change journal |
| `changes_since` | Changes since a checkpoint, sequence number, timestamp, or relative duration |
| `export_bulk` | JSONL dump of resources and changes for offline analysis |

## Configuration

Optional TOML at `~/.config/pulseblade/pulseblade.toml` (or `--config`):

```toml
interval_secs = 15
retention_hours = 24

# Semantic labels attached to matching resource ids (`*` wildcard).
[[labels]]
match = "unit:*:sshd.service"
set = { criticality = "high", role = "access" }

[[labels]]
match = "disk:*:/"
set = { criticality = "high" }
```

State lives in `~/.local/state/pulseblade/pulseblade.db` by default (`--db` to override).

## Integrates with

- Any MCP client: Cursor, Claude, local agents on Ollama, HolmesGPT, custom scripts
- Ollama or any OpenAI-compatible endpoint, as the pilot's model (planned)
- systemd (today); Docker, Proxmox VE, Kubernetes, Prometheus (planned)

## Development

```bash
make help   # list targets
make ci     # fmt-check + clippy -D warnings + tests
```

Architecture: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## License

Apache-2.0 — see [LICENSE](LICENSE).
