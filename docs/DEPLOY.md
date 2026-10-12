# Deploy

Loopback is the default. This page is the reverse proxy, Prometheus, Homepage, and the hub manifests. Config keys are in [CONFIG.md](CONFIG.md). Hub protocol is in [ARCHITECTURE.md](ARCHITECTURE.md#hub).

## Remote access

Everything listens on loopback by default and has no authentication of its own. Requests are only accepted when their `Host` header is loopback, the listen address, or listed in `allowed_hosts` (DNS-rebinding protection for `/mcp`, the API, and the dashboard alike). For access from other machines, put a reverse proxy with TLS and authentication in front and allow its hostname:

```toml
# pulseblade.toml
allowed_hosts = ["pulseblade.example.com"]
```

```caddyfile
pulseblade.example.com {
    basic_auth {
        admin <bcrypt hash from `caddy hash-password`>
    }
    reverse_proxy 127.0.0.1:7171
}
```

(nginx: `proxy_pass http://127.0.0.1:7171; proxy_set_header Host $host;`.) If you instead set `listen = "0.0.0.0:7171"`, add every name or `ip:port` that clients use to `allowed_hosts`, and firewall the port.

## Prometheus and Grafana

```yaml
# prometheus.yml
scrape_configs:
  - job_name: pulseblade
    scrape_interval: 15s
    static_configs:
      - targets: ["127.0.0.1:7171"]   # remote targets: add "web1:7171" to that node's allowed_hosts
```

| Series | Meaning |
|--------|---------|
| `pulseblade_up`, `pulseblade_build_info{version}`, `pulseblade_journal_seq` | Node liveness, version, journal position |
| `pulseblade_open_findings`, `pulseblade_actions_awaiting` | Open findings, and runs waiting for approval |
| `pulseblade_collector_up{collector}` | 1 when the last pass succeeded and is recent |
| `pulseblade_collector_last_run_timestamp_seconds`, `..._last_success_timestamp_seconds`, `..._last_duration_seconds`, `..._consecutive_failures`, `..._resources` | Per-collector detail |
| `pulseblade_resource_health{id,kind,host,...}` | **0 ok, 1 unknown, 2 degraded, 3 failed** |
| `pulseblade_<metric>{id,kind,host,...}` | Latest value of every metric, name sanitized: `cpu.used_pct` becomes `pulseblade_cpu_used_pct`, `load.1m` becomes `pulseblade_load_1m` |

Resource series also carry your semantic labels (e.g. `criticality`, `role`) when their names are valid Prometheus labels. In Grafana, add the Prometheus data source and start from queries such as:

```promql
pulseblade_resource_health >= 2                                   # degraded or failed resources
pulseblade_cpu_used_pct{kind="host"}                              # CPU per host
max by (id) (pulseblade_disk_used_pct)                            # disk fill
time() - pulseblade_collector_last_success_timestamp_seconds > 60 # stale collectors
```

`[[prometheus]]` in config scrapes other text endpoints into the node. An empty `url` is skipped.

## Homepage

A [Homepage](https://gethomepage.dev) `customapi` widget against the status endpoint (Homepage fetches server-side, so the hostname it uses must reach the node and be in `allowed_hosts`):

```yaml
# services.yaml
- Infrastructure:
    - Pulseblade:
        icon: mdi-heart-pulse
        href: https://pulseblade.example.com/
        widget:
          type: customapi
          url: https://pulseblade.example.com/api/v1/status
          refreshInterval: 15000
          mappings:
            - field: { resources: total }
              label: Resources
              format: number
            - field: { resources: failed }
              label: Failed
              format: number
            - field: { resources: degraded }
              label: Degraded
              format: number
            - field: samples
              label: Samples
              format: number
```

`/api/v1/snapshot` (summary detail) works too: its `total` is the number of unhealthy resources and `as_of_seq` moves whenever anything changes.

## Hub

```bash
# Nodes connect out; set [upstream] on each node and [[hub.nodes]] on the hub.
pulseblade hub
```

`pulseblade hub` serves the same MCP tools for every node that pushes to it, plus `fleet_overview`. Point the pilot at the hub with `mcp_url`; the pilot does not change. An action the hub routes still runs only when the target node's own tier allows it.

Manifests:

- Image and units: `deploy/Dockerfile`, `deploy/pulseblade-hub.service`, `deploy/pulseblade-node.service`
- Per-host DaemonSet: `deploy/k8s/daemonset.yaml`
- Helm chart: `deploy/helm/pulseblade`
