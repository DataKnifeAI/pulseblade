# Pulseblade

![Pulseblade — a person walking a lane of infrastructure nodes as an amber pulse sweeps ahead, revealing healthy and degraded hosts](docs/assets/pulseblade-hero.jpg)

**Pulseblade** — a pulse that reveals what is moving across your infrastructure.

Pulseblade is a self-hosted infrastructure monitor built for **AI agents as the primary consumer**. A lightweight per-host **node** collects metrics and state, keeps a change journal, and exposes everything through the [Model Context Protocol](https://modelcontextprotocol.io) (MCP). Agents ask "what changed since checkpoint X?" instead of drowning in alert storms. Humans get a degraded view of the same data with no LLM involved: a built-in dashboard, a JSON API, Prometheus metrics, and `pulseblade ctl status`.

It scales in two shapes with the same MCP contract:

- **Single box** — one node, with an optional **pilot** (a local-LLM reasoning loop) watching the OS and user session.
- **Fleet** — many nodes feeding a **hub** (control node); pilots and other agents talk to the hub, nodes enforce their own action policy.

## Status

Today: host, CPU, memory, disk, network, systemd, journald, processes, and user session; Docker, Proxmox, and Kubernetes; change journal, checkpoints, findings, notes, and gated actions; a pilot on any OpenAI-compatible endpoint; Prometheus scrape targets and `/metrics`; a hub with per-node cursors, `fleet_overview`, and routed actions; dashboard, REST API, and `ctl status`; MCP over stdio and streamable HTTP. Milestones M1 through M7 are done. Later work is in [docs/PLAN.md](docs/PLAN.md).

## Quick start

```bash
make install            # cargo install --path crates/pulseblade --locked

# Collectors, MCP at http://127.0.0.1:7171/mcp, dashboard at http://127.0.0.1:7171/
pulseblade node
```

Dashboard: <http://127.0.0.1:7171/>.

Stdio, with no daemon required (Cursor `~/.cursor/mcp.json`):

```json
{
  "mcpServers": {
    "pulseblade": { "command": "pulseblade", "args": ["mcp"] }
  }
}
```

Use an absolute path (e.g. `~/.cargo/bin/pulseblade`, expanded) if `~/.cargo/bin` is not on the PATH your editor launches with. HTTP MCP, dashboard flags, and the shell commands: [docs/MCP.md](docs/MCP.md).

## Docs

- [MCP tools and clients](docs/MCP.md)
- [Configuration](docs/CONFIG.md)
- [HTTP API](docs/HTTP.md)
- [Dashboard](docs/DASHBOARD.md)
- [Deploy, Prometheus, and Homepage](docs/DEPLOY.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Plan](docs/PLAN.md)

## Development

```bash
make help   # list targets
make ci     # fmt-check + clippy -D warnings + tests
```

## License

Apache-2.0 — see [LICENSE](LICENSE).
