use std::path::{Path, PathBuf};

use anyhow::Context;
use pulseblade_act::ActionsConfig;
use pulseblade_collect::{CollectorOpts, ProxmoxTarget};
use pulseblade_core::LabelRule;
use pulseblade_detect::DetectConfig;
use pulseblade_pilot::PilotConfig;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Seconds between collector passes.
    pub interval_secs: u64,
    /// How long raw samples are kept.
    pub retention_hours: u64,
    /// How long change journal entries are kept.
    pub change_retention_days: u64,
    /// Address for the node's HTTP MCP endpoint.
    pub listen: String,
    /// When true, `pulseblade mcp` also serves the HTTP dashboard and API.
    #[serde(default)]
    pub dashboard: bool,
    /// Extra `Host` header values accepted by the HTTP endpoint (loopback is always allowed).
    pub allowed_hosts: Vec<String>,
    /// Semantic labels applied to matching resource ids.
    pub labels: Vec<LabelRule>,
    /// Threshold rules, anomaly detection, and an optional findings webhook.
    #[serde(default)]
    pub detect: DetectConfig,
    /// Node-local action policy. Missing keys keep the built-in tiers and allow lists.
    #[serde(default)]
    pub actions: ActionsConfig,
    /// Optional collectors. Docker is attempted when enabled; Proxmox stays off
    /// until `[proxmox]` has an API URL.
    #[serde(default)]
    pub collect: CollectConfig,
    /// Proxmox API used by the collector and by proxmox actions. The token secret
    /// is an environment variable name, never the secret itself.
    #[serde(default)]
    pub proxmox: ProxmoxFile,
    /// Optional event-driven investigator. The node does not start it.
    #[serde(default)]
    pub pilot: PilotConfig,
    /// Fleet control node. Ignored by `pulseblade node`.
    #[serde(default)]
    pub hub: HubConfig,
    /// When `url` is set, this node pushes its journal to a hub and long-polls for actions.
    #[serde(default)]
    pub upstream: UpstreamConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CollectConfig {
    pub docker: bool,
}

impl Default for CollectConfig {
    fn default() -> Self {
        Self { docker: true }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProxmoxFile {
    pub api_url: String,
    pub token_id: String,
    pub token_secret_env: String,
    pub verify_tls: bool,
}

impl Default for ProxmoxFile {
    fn default() -> Self {
        Self {
            api_url: String::new(),
            token_id: String::new(),
            token_secret_env: String::new(),
            verify_tls: true,
        }
    }
}

impl ProxmoxFile {
    pub fn endpoint(&self) -> pulseblade_act::ProxmoxEndpoint {
        pulseblade_act::ProxmoxEndpoint {
            api_url: self.api_url.clone(),
            token_id: self.token_id.clone(),
            token_secret_env: self.token_secret_env.clone(),
            verify_tls: self.verify_tls,
        }
    }

    pub fn target(&self) -> Option<ProxmoxTarget> {
        if self.api_url.trim().is_empty() {
            None
        } else {
            Some(ProxmoxTarget {
                api_url: self.api_url.clone(),
                token_id: self.token_id.clone(),
                token_secret_env: self.token_secret_env.clone(),
                verify_tls: self.verify_tls,
            })
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HubConfig {
    pub listen: String,
    pub nodes: Vec<HubNode>,
    pub tls: HubTls,
    /// How long a routed action call waits for the node.
    pub action_wait_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HubNode {
    pub id: String,
    /// Environment variable that holds this node's bearer token. Not the token.
    pub token_env: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HubTls {
    pub cert: String,
    pub key: String,
    pub client_ca: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct UpstreamConfig {
    pub url: String,
    /// Empty uses this machine's hostname.
    pub node_id: String,
    pub token_env: String,
    pub poll_secs: u64,
    /// CA that signed the hub certificate. Required with `cert` and `key`.
    pub ca: String,
    pub cert: String,
    pub key: String,
}

impl Default for HubConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:7170".into(),
            nodes: Vec::new(),
            tls: HubTls::default(),
            action_wait_secs: 20,
        }
    }
}

impl HubTls {
    pub fn enabled(&self) -> bool {
        !self.cert.is_empty() || !self.key.is_empty() || !self.client_ca.is_empty()
    }
}

impl Default for UpstreamConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            node_id: String::new(),
            token_env: "PULSEBLADE_HUB_TOKEN".into(),
            poll_secs: 15,
            ca: String::new(),
            cert: String::new(),
            key: String::new(),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            interval_secs: 15,
            retention_hours: 24,
            change_retention_days: 30,
            listen: "127.0.0.1:7171".to_string(),
            dashboard: false,
            allowed_hosts: Vec::new(),
            labels: Vec::new(),
            detect: DetectConfig::default(),
            actions: ActionsConfig::default(),
            collect: CollectConfig::default(),
            proxmox: ProxmoxFile::default(),
            pilot: PilotConfig::default(),
            hub: HubConfig::default(),
            upstream: UpstreamConfig::default(),
        }
    }
}

impl Config {
    pub fn collector_opts(&self) -> CollectorOpts {
        CollectorOpts {
            docker: self.collect.docker,
            proxmox: self.proxmox.target(),
        }
    }

    /// Load `path`, or the default location if it exists, or built-in defaults.
    pub fn load(path: Option<&Path>) -> anyhow::Result<Self> {
        let (path, required) = match path {
            Some(p) => (p.to_path_buf(), true),
            None => (default_config_path(), false),
        };
        if !path.exists() {
            anyhow::ensure!(!required, "config file {} not found", path.display());
            return Ok(Self::default());
        }
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("reading {}", path.display()))?;
        toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))
    }
}

fn home() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

fn xdg(var: &str, fallback: &str) -> PathBuf {
    std::env::var_os(var)
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
        .unwrap_or_else(|| home().join(fallback))
}

pub fn default_config_path() -> PathBuf {
    xdg("XDG_CONFIG_HOME", ".config").join("pulseblade/pulseblade.toml")
}

pub fn default_db_path() -> PathBuf {
    xdg("XDG_STATE_HOME", ".local/state").join("pulseblade/pulseblade.db")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_readme_example() {
        let cfg: Config = toml::from_str(
            r#"
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

            [detect]
            webhook = ""

            [[detect.rules]]
            match = "disk:*:*"
            metric = "disk.used_pct"
            op = ">="
            value = 95
            severity = "critical"

            [actions]
            approval_ttl = "1h"
            approve_token = ""

            [actions.systemd_restart]
            tier = "approve"
            rate_per_hour = 12

            [actions.docker_restart]
            tier = "approve"

            [actions.disk_prune]
            tier = "approve"
            allow = []

            [actions.script_run]
            tier = "deny"
            allow = []

            [actions.proxmox_start]
            tier = "approve"

            [actions.proxmox_stop]
            tier = "approve"

            [actions.proxmox_migrate]
            tier = "approve"

            [collect]
            docker = true

            [proxmox]
            api_url = ""
            token_id = ""
            token_secret_env = "PVE_TOKEN_SECRET"
            verify_tls = true

            [pilot]
            enabled = false
            local_only = false
            wake_on = ["findings", "changes"]
            heartbeat_secs = 900
            poll_secs = 30
            execute_auto = false
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

            [hub]
            listen = "127.0.0.1:7170"
            action_wait_secs = 20

            [[hub.nodes]]
            id = "web1"
            token_env = "PULSEBLADE_NODE_WEB1"

            [hub.tls]
            cert = ""
            key = ""
            client_ca = ""

            [upstream]
            url = ""
            node_id = ""
            token_env = "PULSEBLADE_HUB_TOKEN"
            poll_secs = 15
            ca = ""
            cert = ""
            key = ""
            "#,
        )
        .unwrap();
        assert_eq!(cfg.labels.len(), 2);
        assert_eq!(cfg.labels[0].set["role"], "access");
        assert_eq!(cfg.listen, "127.0.0.1:7171");
        assert!(!cfg.dashboard);
        assert_eq!(cfg.detect.rules.len(), 1);
        assert_eq!(cfg.actions.approval_ttl, "1h");
        assert!(cfg.actions.disk_prune.allow.is_empty());
        assert_eq!(cfg.actions.script_run.rate_per_hour, 4);
        assert!(cfg.collect.docker);
        assert!(cfg.proxmox.api_url.is_empty());
        assert!(!cfg.pilot.execute_auto);
        assert_eq!(cfg.pilot.tiers.len(), 1);
        assert_eq!(cfg.pilot.budget.max_tool_calls, 12);
        assert_eq!(cfg.hub.listen, "127.0.0.1:7170");
        assert_eq!(cfg.hub.nodes[0].id, "web1");
        assert!(cfg.upstream.url.is_empty());
    }

    #[test]
    fn dashboard_defaults_false_and_parses_true() {
        assert!(!Config::default().dashboard);
        let omitted: Config = toml::from_str("interval_secs = 30").unwrap();
        assert!(!omitted.dashboard);
        let enabled: Config = toml::from_str("dashboard = true").unwrap();
        assert!(enabled.dashboard);
    }

    #[test]
    fn rejects_unknown_keys() {
        assert!(toml::from_str::<Config>("intervl_secs = 5").is_err());
        assert!(toml::from_str::<Config>("dashbord = true").is_err());
    }
}
