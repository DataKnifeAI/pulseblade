# Configuration

Optional TOML at `~/.config/pulseblade/pulseblade.toml` (or `--config`). State lives in `~/.local/state/pulseblade/pulseblade.db` by default (`--db` to override).

```toml
interval_secs = 15
retention_hours = 24
listen = "127.0.0.1:7171"
# Set `dashboard = true` so `pulseblade mcp` serves the HTTP dashboard and API
# when that process is the collector.
dashboard = false
allowed_hosts = []        # extra Host header values, e.g. a reverse proxy's hostname

# Semantic labels attached to matching resource ids (`*` wildcard).
[[labels]]
match = "unit:*:sshd.service"
set = { criticality = "high", role = "access" }

[[labels]]
match = "disk:*:/"
set = { criticality = "high" }

# Detection. Missing keys keep the defaults (EWMA on, no threshold rules, no webhook).
[detect]
webhook = ""          # POST a small JSON body when new findings open; empty disables it

[[detect.rules]]
match = "disk:*:*"
metric = "disk.used_pct"
op = ">="
value = 95
severity = "critical"

# Remediation policy is node-local. Omit a tier to keep the built-in default.
[actions]
approval_ttl = "1h"   # 30s, 15m, 1h, or 1d; how long an approve-tier run waits
approve_token = ""    # when set, the approval webhook requires Authorization: Bearer

[actions.systemd_restart]   # default tier: approve
tier = "approve"
rate_per_hour = 12

[actions.docker_restart]    # default tier: approve
tier = "approve"

[actions.disk_prune]        # default tier: approve; only these directories can be pruned
tier = "approve"
allow = []

[actions.script_run]        # default tier: deny; only these absolute paths can run
tier = "deny"
allow = []

[actions.proxmox_start]     # default tier: approve
tier = "approve"

[actions.proxmox_stop]
tier = "approve"

[actions.proxmox_migrate]
tier = "approve"

[actions.k8s_rollout_restart]   # default tier: approve
tier = "approve"

[actions.k8s_scale]
tier = "approve"

[collect]
docker = true             # false skips the docker collector even when the daemon is up

# Proxmox collector and actions. An empty api_url leaves both off.
# token_secret_env is the name of an environment variable, never the secret.
[proxmox]
api_url = ""
token_id = ""
token_secret_env = "PVE_TOKEN_SECRET"
verify_tls = true

# Pilot. Missing keys keep the defaults (disabled, no model calls).
# `pulseblade pilot` talks to MCP at mcp_url, or http://<listen>/mcp.
[pilot]
enabled = false
local_only = false        # when true, only loopback base_url values are called
wake_on = ["findings", "changes"]
heartbeat_secs = 900
poll_secs = 30            # MCP poll interval; this is not a model call
execute_auto = false      # auto-tier plans run only when this is true; approve and deny never do
mcp_url = ""

[pilot.budget]
max_tool_calls = 12
max_tokens = 20000
max_wall_secs = 120

[[pilot.tiers]]
name = "triage"
[[pilot.tiers.endpoints]]
kind = "openai_compatible"
base_url = "http://127.0.0.1:11434/v1"
model = "local"

# Hub. Empty nodes means `pulseblade node` ignores this section.
# token_env is the name of an environment variable, never the token.
[hub]
listen = "127.0.0.1:7170"
action_wait_secs = 20
# Names of environment variables, never the values. Empty database_url_env
# keeps this hub on its SQLite file. Nodes ignore this section.
database_url_env = ""
database_user_env = ""
database_password_env = ""
database_ca_env = ""

[[hub.nodes]]
id = "web1"
token_env = "PULSEBLADE_NODE_WEB1"

[hub.tls]
cert = ""
key = ""
client_ca = ""

# Node push. An empty url leaves the node standalone.
# https:// verifies the hub certificate and sends the bearer token.
# Set ca, cert, and key together only when the hub requires mutual TLS.
[upstream]
url = ""
node_id = ""
token_env = "PULSEBLADE_HUB_TOKEN"
poll_secs = 15
ca = ""
cert = ""
key = ""

# Kubernetes collector and actions. An empty api_url leaves both off.
# token_env is the name of an environment variable, never the secret.
[k8s]
api_url = ""
token_env = "K8S_TOKEN"
cluster = "demo"
verify_tls = true
ca = ""

# Prometheus text endpoints. An empty url is skipped.
[[prometheus]]
name = "app"
url = ""
```

`systemd.restart` takes `unit` (a bare name gets `.service`) and optional `user: true` for `systemctl --user`. `docker.restart` takes `container` and shells out to `docker`. `disk.prune_path` deletes regular files in an allow-listed directory and refuses `/`, `/home`, `/etc`, `/usr`, `/var`, `/boot`, and the other system roots. `script.run` runs an allow-listed program with an `args` array and no shell. `proxmox.start` and `proxmox.stop` take `node`, `vmid`, and optional `guest` (`qemu` or `lxc`). `proxmox.migrate` takes `node`, `vmid`, and `target`. `k8s.rollout_restart` takes `namespace`, `name`, and optional `kind` (`deployment`, `statefulset`, or `daemonset`). `k8s.scale` takes `namespace`, `name`, `replicas`, and optional `kind` (`deployment` or `statefulset`). Those wait for approval unless the tier is changed. `action_plan` with `dry_run: true` records the run without invoking the command. Approve a waiting run with `pulseblade ctl approve <run_id>` or `POST /api/v1/actions/{id}/approve`.

`pulseblade pilot` polls the MCP server and calls the model only when a new finding, a journal change, or the heartbeat says so. A quiet `--once` prints `{"investigated": false}` and does not call the model. Conclusions are `memory_note`s; proposed remediations are `action_plan`s. The node still applies its own tier. `pulseblade ctl runs` lists recorded investigations. Provider routing and a two-tier example: [ARCHITECTURE.md](ARCHITECTURE.md#example-configuration).

Hub push, enrollment, and manifest paths: [DEPLOY.md](DEPLOY.md).
