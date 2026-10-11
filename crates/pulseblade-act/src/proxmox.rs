//! Proxmox start, stop, and migrate. Default tier is approve, so planning and
//! execute do not call the API; the node calls it only after a human approves.

use std::sync::Arc;

use crate::ActError;

/// API coordinates. The token secret is the name of an environment variable.
#[derive(Debug, Clone)]
pub struct ProxmoxEndpoint {
    pub api_url: String,
    pub token_id: String,
    pub token_secret_env: String,
    pub verify_tls: bool,
}

impl Default for ProxmoxEndpoint {
    fn default() -> Self {
        Self {
            api_url: String::new(),
            token_id: String::new(),
            token_secret_env: String::new(),
            verify_tls: true,
        }
    }
}

pub trait HttpClient: Send + Sync {
    fn post_form(
        &self,
        url: &str,
        authorization: &str,
        fields: &[(&str, &str)],
    ) -> Result<String, String>;
}

pub trait SecretLookup: Send + Sync {
    fn get(&self, name: &str) -> Option<String>;
}

pub struct ProcessEnv;

impl SecretLookup for ProcessEnv {
    fn get(&self, name: &str) -> Option<String> {
        std::env::var(name).ok().filter(|s| !s.is_empty())
    }
}

pub struct UreqClient {
    pub verify_tls: bool,
}

impl HttpClient for UreqClient {
    fn post_form(
        &self,
        url: &str,
        authorization: &str,
        fields: &[(&str, &str)],
    ) -> Result<String, String> {
        let agent = agent(self.verify_tls)?;
        let resp = agent
            .post(url)
            .set("Authorization", authorization)
            .send_form(fields)
            .map_err(|e| clip(&e.to_string()))?;
        resp.into_string()
            .map_err(|e| e.to_string())
            .map(|s| clip(&s))
    }
}

fn agent(verify_tls: bool) -> Result<ureq::Agent, String> {
    if verify_tls {
        return Ok(ureq::Agent::new());
    }
    let tls = native_tls::TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .danger_accept_invalid_hostnames(true)
        .build()
        .map_err(|e| e.to_string())?;
    Ok(ureq::AgentBuilder::new()
        .tls_connector(Arc::new(tls))
        .build())
}

fn clip(s: &str) -> String {
    if s.chars().count() <= 500 {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(500).collect();
        out.push('…');
        out
    }
}

pub fn is_proxmox(name: &str) -> bool {
    matches!(name, "proxmox.start" | "proxmox.stop" | "proxmox.migrate")
}

pub fn description(name: &str) -> &'static str {
    match name {
        "proxmox.start" => "Start a Proxmox QEMU guest or LXC. Waits for approval by default.",
        "proxmox.stop" => "Stop a Proxmox QEMU guest or LXC. Waits for approval by default.",
        "proxmox.migrate" => "Migrate a QEMU guest to another node. Waits for approval by default.",
        _ => "",
    }
}

pub fn schema(name: &str) -> serde_json::Value {
    let guest = serde_json::json!({
        "type": "string",
        "enum": ["qemu", "lxc"]
    });
    match name {
        "proxmox.start" | "proxmox.stop" => serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["node", "vmid"],
            "properties": {
                "node": {"type": "string", "minLength": 1, "maxLength": 64},
                "vmid": {"type": "integer", "minimum": 1, "maximum": 999999999},
                "guest": guest
            }
        }),
        "proxmox.migrate" => serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["node", "vmid", "target"],
            "properties": {
                "node": {"type": "string", "minLength": 1, "maxLength": 64},
                "vmid": {"type": "integer", "minimum": 1, "maximum": 999999999},
                "target": {"type": "string", "minLength": 1, "maxLength": 64}
            }
        }),
        _ => serde_json::json!({"type": "object"}),
    }
}

pub fn preconditions(name: &str) -> &'static [&'static str] {
    match name {
        "proxmox.start" | "proxmox.stop" => &[
            "node and vmid name a guest on this node's Proxmox API",
            "guest is qemu (default) or lxc",
            "the API token is configured on the node",
        ],
        "proxmox.migrate" => &[
            "node, vmid, and target name a QEMU guest and a destination node",
            "the API token is configured on the node",
        ],
        _ => &[],
    }
}

/// Resource id the run is filed under, after the params are known to be safe.
pub fn resource_id(name: &str, params: &serde_json::Value) -> Result<String, ActError> {
    let node = node_name(params.get("node").and_then(|v| v.as_str()).unwrap_or(""))?;
    let vmid = vmid(params.get("vmid"))?;
    let guest = guest_kind(params.get("guest").and_then(|v| v.as_str()))?;
    if name == "proxmox.migrate" {
        let _target = node_name(params.get("target").and_then(|v| v.as_str()).unwrap_or(""))?;
        if guest != "qemu" {
            return Err(ActError::Invalid(
                "proxmox.migrate only moves QEMU guests".into(),
            ));
        }
    }
    let prefix = if guest == "lxc" { "lxc" } else { "vm" };
    Ok(format!("{prefix}:{node}:{vmid}"))
}

pub fn ensure_configured(endpoint: &ProxmoxEndpoint) -> Result<(), ActError> {
    if endpoint.api_url.trim().is_empty()
        || endpoint.token_id.trim().is_empty()
        || endpoint.token_secret_env.trim().is_empty()
    {
        return Err(ActError::Invalid(
            "proxmox API is not configured (api_url, token_id, token_secret_env)".into(),
        ));
    }
    Ok(())
}

pub fn perform(
    name: &str,
    params: &serde_json::Value,
    endpoint: &ProxmoxEndpoint,
    http: &dyn HttpClient,
    secrets: &dyn SecretLookup,
) -> Result<String, String> {
    ensure_configured(endpoint).map_err(|e| e.to_string())?;
    let secret = secrets.get(&endpoint.token_secret_env).ok_or_else(|| {
        format!(
            "environment variable {} is not set",
            endpoint.token_secret_env
        )
    })?;
    let node = node_name(params.get("node").and_then(|v| v.as_str()).unwrap_or(""))
        .map_err(|e| e.to_string())?;
    let vmid = vmid(params.get("vmid")).map_err(|e| e.to_string())?;
    let guest =
        guest_kind(params.get("guest").and_then(|v| v.as_str())).map_err(|e| e.to_string())?;
    let path = match name {
        "proxmox.start" => format!("/nodes/{node}/{guest}/{vmid}/status/start"),
        "proxmox.stop" => format!("/nodes/{node}/{guest}/{vmid}/status/stop"),
        "proxmox.migrate" => {
            if guest != "qemu" {
                return Err("proxmox.migrate only moves QEMU guests".into());
            }
            format!("/nodes/{node}/qemu/{vmid}/migrate")
        }
        other => return Err(format!("unknown action `{other}`")),
    };
    let url = format!("{}/api2/json{path}", endpoint.api_url.trim_end_matches('/'));
    let auth = format!("PVEAPIToken={}={}", endpoint.token_id, secret);
    let fields: Vec<(&str, String)> = if name == "proxmox.migrate" {
        let target = node_name(params.get("target").and_then(|v| v.as_str()).unwrap_or(""))
            .map_err(|e| e.to_string())?;
        vec![("target", target)]
    } else {
        Vec::new()
    };
    let borrowed: Vec<(&str, &str)> = fields.iter().map(|(k, v)| (*k, v.as_str())).collect();
    http.post_form(&url, &auth, &borrowed)
}

fn node_name(raw: &str) -> Result<String, ActError> {
    let ok = !raw.is_empty()
        && raw.len() <= 64
        && raw
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_'))
        && raw
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphanumeric());
    if ok {
        Ok(raw.to_string())
    } else {
        Err(ActError::Invalid(format!(
            "node name `{raw}` is not allowed"
        )))
    }
}

fn vmid(v: Option<&serde_json::Value>) -> Result<u64, ActError> {
    let n = v
        .and_then(|v| {
            v.as_u64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .filter(|n| (1..=999_999_999).contains(n));
    n.ok_or_else(|| ActError::Invalid("vmid must be an integer from 1 to 999999999".into()))
}

fn guest_kind(raw: Option<&str>) -> Result<&'static str, ActError> {
    match raw.unwrap_or("qemu") {
        "qemu" => Ok("qemu"),
        "lxc" => Ok("lxc"),
        other => Err(ActError::Invalid(format!(
            "guest `{other}` must be qemu or lxc"
        ))),
    }
}
