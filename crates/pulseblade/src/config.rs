use std::path::{Path, PathBuf};

use anyhow::Context;
use pulseblade_act::ActionsConfig;
use pulseblade_collect::{CollectorOpts, K8sTarget, PromTarget, ProxmoxTarget};
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
    /// Days to keep 5-minute metric buckets and the failure/restart/error index.
    #[serde(default = "default_history_days")]
    pub history_days: u64,
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
    /// Kubernetes API used by the collector and by k8s actions. The token is an
    /// environment variable name, never the secret itself.
    #[serde(default)]
    pub k8s: K8sFile,
    /// Prometheus text endpoints to scrape into samples. An empty URL is skipped.
    #[serde(default)]
    pub prometheus: Vec<PromFile>,
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
    /// Environment variable holding a `postgres://` URL. Empty keeps the hub on SQLite.
    #[serde(default)]
    pub database_url_env: String,
    /// Environment variable holding the database user. Empty when the URL already has one.
    #[serde(default)]
    pub database_user_env: String,
    /// Environment variable holding the database password.
    #[serde(default)]
    pub database_password_env: String,
    /// Environment variable holding the server CA PEM. Empty uses a non-TLS connection.
    #[serde(default)]
    pub database_ca_env: String,
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
    /// Mutual TLS, only when `ca`, `cert`, and `key` are all set.
    /// Empty means an `https://` url uses ordinary TLS and the bearer token.
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
            database_url_env: String::new(),
            database_user_env: String::new(),
            database_password_env: String::new(),
            database_ca_env: String::new(),
        }
    }
}

/// Fill `user` and `password` into a URL that has no userinfo yet.
/// A URL that already contains `@` is returned unchanged.
pub(crate) fn inject_database_user(
    url: &str,
    user_env: &str,
    password_env: &str,
) -> anyhow::Result<String> {
    if user_env.is_empty() && password_env.is_empty() {
        return Ok(url.to_string());
    }
    if user_env.is_empty() || password_env.is_empty() {
        anyhow::bail!("database_user_env and database_password_env must both be set");
    }
    let user = std::env::var(user_env).with_context(|| format!("reading {user_env}"))?;
    let password =
        std::env::var(password_env).with_context(|| format!("reading {password_env}"))?;
    format_database_url(url, &user, &password)
}

fn format_database_url(url: &str, user: &str, password: &str) -> anyhow::Result<String> {
    let rest = url
        .strip_prefix("postgres://")
        .or_else(|| url.strip_prefix("postgresql://"))
        .context("database url must start with postgres://")?;
    if rest.contains('@') {
        return Ok(url.to_string());
    }
    let scheme = if url.starts_with("postgresql://") {
        "postgresql"
    } else {
        "postgres"
    };
    Ok(format!(
        "{scheme}://{}:{}@{rest}",
        encode_userinfo(user),
        encode_userinfo(password)
    ))
}

pub(crate) fn optional_env(name: &str) -> anyhow::Result<Option<String>> {
    if name.is_empty() {
        return Ok(None);
    }
    let value = std::env::var(name).with_context(|| format!("reading {name}"))?;
    Ok(Some(value))
}

fn encode_userinfo(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for b in value.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
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

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct K8sFile {
    pub api_url: String,
    pub token_env: String,
    pub cluster: String,
    pub verify_tls: bool,
    /// PEM file for the API server CA. Empty uses the default trust store.
    pub ca: String,
}

impl Default for K8sFile {
    fn default() -> Self {
        Self {
            api_url: String::new(),
            token_env: "K8S_TOKEN".into(),
            cluster: "cluster".into(),
            verify_tls: true,
            ca: String::new(),
        }
    }
}

impl K8sFile {
    pub fn endpoint(&self) -> pulseblade_act::K8sEndpoint {
        pulseblade_act::K8sEndpoint {
            api_url: self.api_url.clone(),
            token_env: self.token_env.clone(),
            cluster: self.cluster.clone(),
            verify_tls: self.verify_tls,
            ca: self.ca.clone(),
        }
    }

    pub fn target(&self) -> Option<K8sTarget> {
        if self.api_url.trim().is_empty() {
            None
        } else {
            Some(K8sTarget {
                api_url: self.api_url.clone(),
                token_env: self.token_env.clone(),
                cluster: self.cluster.clone(),
                verify_tls: self.verify_tls,
                ca: self.ca.clone(),
            })
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PromFile {
    pub name: String,
    pub url: String,
}

impl PromFile {
    pub fn target(&self) -> Option<PromTarget> {
        if self.url.trim().is_empty() || self.name.trim().is_empty() {
            None
        } else {
            Some(PromTarget {
                name: self.name.clone(),
                url: self.url.clone(),
            })
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            interval_secs: 15,
            retention_hours: 24,
            change_retention_days: 30,
            history_days: default_history_days(),
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
            k8s: K8sFile::default(),
            prometheus: Vec::new(),
        }
    }
}

impl Config {
    pub fn collector_opts(&self) -> CollectorOpts {
        CollectorOpts {
            docker: self.collect.docker,
            proxmox: self.proxmox.target(),
            k8s: self.k8s.target(),
            prometheus: self.prometheus.iter().filter_map(|p| p.target()).collect(),
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

fn default_history_days() -> u64 {
    90
}

pub fn default_db_path() -> PathBuf {
    xdg("XDG_STATE_HOME", ".local/state").join("pulseblade/pulseblade.db")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn database_userinfo_is_percent_encoded() {
        let url = format_database_url(
            "postgres://pulseblade-postgres-rw.pulseblade.svc:5432/pulseblade?sslmode=require",
            "pulseblade",
            "a/b c",
        )
        .unwrap();
        assert_eq!(
            url,
            "postgres://pulseblade:a%2Fb%20c@pulseblade-postgres-rw.pulseblade.svc:5432/pulseblade?sslmode=require"
        );
        let existing = "postgres://already:set@db/pulseblade";
        assert_eq!(
            format_database_url(existing, "other", "x").unwrap(),
            existing
        );
    }

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

            [actions.k8s_rollout_restart]
            tier = "approve"

            [actions.k8s_scale]
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

            [k8s]
            api_url = ""
            token_env = "K8S_TOKEN"
            cluster = "demo"
            verify_tls = true
            ca = ""

            [[prometheus]]
            name = "app"
            url = ""
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
        assert!(cfg.k8s.api_url.is_empty());
        assert_eq!(cfg.k8s.cluster, "demo");
        assert_eq!(cfg.prometheus.len(), 1);
        assert!(cfg.prometheus[0].url.is_empty());
    }

    #[test]
    fn dashboard_defaults_false_and_parses_true() {
        assert!(!Config::default().dashboard);
        let omitted: Config = toml::from_str("interval_secs = 30").unwrap();
        assert!(!omitted.dashboard);
        assert_eq!(omitted.history_days, 90);
        let enabled: Config = toml::from_str("dashboard = true").unwrap();
        assert!(enabled.dashboard);
    }

    #[test]
    fn rejects_unknown_keys() {
        assert!(toml::from_str::<Config>("intervl_secs = 5").is_err());
        assert!(toml::from_str::<Config>("dashbord = true").is_err());
    }
}
