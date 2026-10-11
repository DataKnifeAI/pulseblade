//! Proxmox VE nodes, QEMU guests, LXC, and storage.
//!
//! Ids: `node:<name>`, `vm:<node>:<vmid>`, `lxc:<node>:<vmid>`,
//! `storage:<node>:<name>`. Guests and storage are parented to their node.
//! The REST client is compiled with the `proxmox` feature. An empty API URL
//! leaves the collector off; a configured API that is down fails the pass.

use pulseblade_core::{Health, Observation, Resource, ResourceKind, Sample};
use serde_json::Value;

use crate::{CollectError, Collector};

#[derive(Debug, Clone, PartialEq)]
pub struct PveNode {
    pub name: String,
    pub status: String,
    pub cpu: f64,
    pub mem: f64,
    pub maxmem: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuestKind {
    Qemu,
    Lxc,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PveGuest {
    pub node: String,
    pub vmid: u64,
    pub name: String,
    pub status: String,
    pub kind: GuestKind,
    pub cpu: f64,
    pub mem: f64,
    pub maxmem: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PveStorage {
    pub node: String,
    pub name: String,
    pub kind: String,
    pub active: bool,
    pub used: f64,
    pub total: f64,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct PveView {
    pub nodes: Vec<PveNode>,
    pub guests: Vec<PveGuest>,
    pub storage: Vec<PveStorage>,
}

pub trait ProxmoxSource: Send {
    fn snapshot(&mut self) -> Result<PveView, CollectError>;
}

pub struct ProxmoxCollector<S> {
    source: S,
}

impl<S> ProxmoxCollector<S> {
    pub fn new(source: S) -> Self {
        Self { source }
    }
}

impl<S: ProxmoxSource> Collector for ProxmoxCollector<S> {
    fn name(&self) -> &'static str {
        "proxmox"
    }

    fn collect(&mut self) -> Result<Observation, CollectError> {
        let view = self.source.snapshot()?;
        Ok(observe_proxmox(&view))
    }
}

pub fn node_health(status: &str) -> Health {
    match status.to_ascii_lowercase().as_str() {
        "online" => Health::Ok,
        "offline" | "unknown" => Health::Failed,
        _ => Health::Degraded,
    }
}

pub fn guest_health(status: &str) -> Health {
    match status.to_ascii_lowercase().as_str() {
        "running" => Health::Ok,
        "stopped" => Health::Unknown,
        "paused" => Health::Degraded,
        _ => Health::Degraded,
    }
}

pub fn storage_health(active: bool, used: f64, total: f64) -> Health {
    if !active {
        return Health::Failed;
    }
    if total <= 0.0 {
        return Health::Unknown;
    }
    let pct = used / total;
    if pct >= 0.95 {
        Health::Failed
    } else if pct >= 0.85 {
        Health::Degraded
    } else {
        Health::Ok
    }
}

pub fn observe_proxmox(view: &PveView) -> Observation {
    let mut obs = Observation::default();
    for node in &view.nodes {
        let Some(name) = id_part(&node.name) else {
            continue;
        };
        let id = format!("node:{name}");
        let mem_pct = if node.maxmem > 0.0 {
            node.mem / node.maxmem * 100.0
        } else {
            0.0
        };
        obs.resources.push(
            Resource::new(&id, ResourceKind::Node, &name)
                .health(node_health(&node.status))
                .attr("status", node.status.clone()),
        );
        obs.samples
            .push(Sample::new(&id, "node.cpu_pct", round2(node.cpu * 100.0)));
        obs.samples
            .push(Sample::new(&id, "node.mem_pct", round2(mem_pct)));
    }
    for guest in &view.guests {
        let (prefix, kind, metric_cpu, metric_mem) = match guest.kind {
            GuestKind::Qemu => ("vm", ResourceKind::Vm, "vm.cpu_pct", "vm.mem_bytes"),
            GuestKind::Lxc => (
                "lxc",
                ResourceKind::Container,
                "lxc.cpu_pct",
                "lxc.mem_bytes",
            ),
        };
        let Some(node) = id_part(&guest.node) else {
            continue;
        };
        let id = format!("{prefix}:{node}:{}", guest.vmid);
        let name = if guest.name.is_empty() {
            guest.vmid.to_string()
        } else {
            guest.name.clone()
        };
        obs.resources.push(
            Resource::new(&id, kind, name)
                .parent(format!("node:{node}"))
                .health(guest_health(&guest.status))
                .label("node", &node)
                .attr("vmid", guest.vmid)
                .attr("status", guest.status.clone()),
        );
        obs.samples
            .push(Sample::new(&id, metric_cpu, round2(guest.cpu * 100.0)));
        obs.samples.push(Sample::new(&id, metric_mem, guest.mem));
    }
    for vol in &view.storage {
        let Some(node) = id_part(&vol.node) else {
            continue;
        };
        let Some(name) = id_part(&vol.name) else {
            continue;
        };
        let id = format!("storage:{node}:{name}");
        let pct = if vol.total > 0.0 {
            vol.used / vol.total * 100.0
        } else {
            0.0
        };
        obs.resources.push(
            Resource::new(&id, ResourceKind::Storage, &name)
                .parent(format!("node:{node}"))
                .health(storage_health(vol.active, vol.used, vol.total))
                .label("node", &node)
                .attr("kind", vol.kind.clone())
                .attr("active", vol.active),
        );
        obs.samples
            .push(Sample::new(&id, "storage.used_pct", round2(pct)));
    }
    obs
}

/// Parse the four Proxmox list payloads. `qemu`, `lxc`, and `storage` are keyed
/// by node name and are the raw `/nodes/{node}/...` bodies.
#[cfg(any(test, feature = "proxmox"))]
pub fn parse_inventory(
    nodes_body: &Value,
    guests: &[(String, GuestKind, Value)],
    storage: &[(String, Value)],
) -> Result<PveView, CollectError> {
    let mut view = PveView::default();
    for row in data_rows(nodes_body)? {
        let Some(name) = row.get("node").and_then(|v| v.as_str()) else {
            continue;
        };
        view.nodes.push(PveNode {
            name: name.to_string(),
            status: row
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string(),
            cpu: num(row.get("cpu")),
            mem: num(row.get("mem")),
            maxmem: num(row.get("maxmem")),
        });
    }
    for (node, kind, body) in guests {
        for row in data_rows(body)? {
            let Some(vmid) = row.get("vmid").and_then(|v| v.as_u64()) else {
                continue;
            };
            view.guests.push(PveGuest {
                node: node.clone(),
                vmid,
                name: row
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                status: row
                    .get("status")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string(),
                kind: *kind,
                cpu: num(row.get("cpu")),
                mem: num(row.get("mem")),
                maxmem: num(row.get("maxmem")),
            });
        }
    }
    for (node, body) in storage {
        for row in data_rows(body)? {
            let Some(name) = row
                .get("storage")
                .and_then(|v| v.as_str())
                .or_else(|| row.get("name").and_then(|v| v.as_str()))
            else {
                continue;
            };
            view.storage.push(PveStorage {
                node: node.clone(),
                name: name.to_string(),
                kind: row
                    .get("type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                active: row
                    .get("active")
                    .and_then(|v| v.as_bool().or_else(|| v.as_i64().map(|n| n != 0)))
                    .unwrap_or(false),
                used: num(row.get("used")),
                total: num(row.get("total")),
            });
        }
    }
    Ok(view)
}

#[cfg(any(test, feature = "proxmox"))]
fn data_rows(body: &Value) -> Result<Vec<Value>, CollectError> {
    let data = body.get("data").unwrap_or(body);
    data.as_array()
        .cloned()
        .ok_or_else(|| CollectError::Unavailable("proxmox response has no data array".into()))
}

#[cfg(any(test, feature = "proxmox"))]
fn num(v: Option<&Value>) -> f64 {
    v.and_then(|n| n.as_f64().or_else(|| n.as_u64().map(|u| u as f64)))
        .unwrap_or(0.0)
}

fn id_part(raw: &str) -> Option<String> {
    let s = raw.trim();
    if s.is_empty() || s.chars().any(|c| c.is_control() || c == ' ' || c == '/') {
        None
    } else {
        Some(s.to_string())
    }
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

/// Where the collector and the action runner send API calls.
#[derive(Debug, Clone)]
pub struct ProxmoxTarget {
    pub api_url: String,
    pub token_id: String,
    pub token_secret_env: String,
    pub verify_tls: bool,
}

#[cfg(feature = "proxmox")]
mod live {
    use super::{parse_inventory, GuestKind, ProxmoxSource, ProxmoxTarget, PveView};
    use crate::CollectError;
    use serde_json::Value;

    pub struct LiveProxmox {
        target: ProxmoxTarget,
    }

    impl LiveProxmox {
        pub fn new(target: ProxmoxTarget) -> Self {
            Self { target }
        }
    }

    impl ProxmoxSource for LiveProxmox {
        fn snapshot(&mut self) -> Result<PveView, CollectError> {
            let secret = std::env::var(&self.target.token_secret_env).map_err(|_| {
                CollectError::Unavailable(format!(
                    "environment variable {} is not set",
                    self.target.token_secret_env
                ))
            })?;
            if secret.is_empty() {
                return Err(CollectError::Unavailable(format!(
                    "environment variable {} is empty",
                    self.target.token_secret_env
                )));
            }
            let auth = format!("PVEAPIToken={}={}", self.target.token_id, secret);
            let agent =
                crate::http_agent(self.target.verify_tls).map_err(CollectError::Unavailable)?;
            let base = self.target.api_url.trim_end_matches('/');
            let nodes_body = get_json(&agent, &format!("{base}/api2/json/nodes"), &auth)?;
            let mut guests = Vec::new();
            let mut storage = Vec::new();
            for row in nodes_body
                .get("data")
                .and_then(|d| d.as_array())
                .cloned()
                .unwrap_or_default()
            {
                let Some(name) = row.get("node").and_then(|v| v.as_str()) else {
                    continue;
                };
                if name.contains('/') || name.contains("..") {
                    continue;
                }
                let qemu = get_json(
                    &agent,
                    &format!("{base}/api2/json/nodes/{name}/qemu"),
                    &auth,
                )?;
                let lxc = get_json(&agent, &format!("{base}/api2/json/nodes/{name}/lxc"), &auth)?;
                let vols = get_json(
                    &agent,
                    &format!("{base}/api2/json/nodes/{name}/storage"),
                    &auth,
                )?;
                guests.push((name.to_string(), GuestKind::Qemu, qemu));
                guests.push((name.to_string(), GuestKind::Lxc, lxc));
                storage.push((name.to_string(), vols));
            }
            parse_inventory(&nodes_body, &guests, &storage)
        }
    }

    fn get_json(
        agent: &ureq::Agent,
        url: &str,
        authorization: &str,
    ) -> Result<Value, CollectError> {
        let resp = agent
            .get(url)
            .set("Authorization", authorization)
            .call()
            .map_err(|e| CollectError::Unavailable(format!("proxmox: {e}")))?;
        resp.into_json()
            .map_err(|e| CollectError::Unavailable(format!("proxmox json: {e}")))
    }
}

#[cfg(feature = "proxmox")]
pub use live::LiveProxmox;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct Fixed(Result<PveView, String>);

    impl ProxmoxSource for Fixed {
        fn snapshot(&mut self) -> Result<PveView, CollectError> {
            self.0.clone().map_err(CollectError::Unavailable)
        }
    }

    #[test]
    fn inventory_ids_parent_guests_to_the_node() {
        let body_nodes = json!({"data": [{
            "node": "pve1", "status": "online", "cpu": 0.25, "mem": 50.0, "maxmem": 200.0
        }]});
        let qemu = json!({"data": [{
            "vmid": 100, "name": "web", "status": "running", "cpu": 0.5, "mem": 10.0, "maxmem": 40.0
        }]});
        let lxc = json!({"data": [{
            "vmid": 101, "name": "db", "status": "stopped", "cpu": 0.0, "mem": 0.0, "maxmem": 20.0
        }]});
        let storage = json!({"data": [{
            "storage": "local", "type": "dir", "active": 1, "used": 90.0, "total": 100.0
        }]});
        let view = parse_inventory(
            &body_nodes,
            &[
                ("pve1".into(), GuestKind::Qemu, qemu),
                ("pve1".into(), GuestKind::Lxc, lxc),
            ],
            &[("pve1".into(), storage)],
        )
        .unwrap();
        let obs = observe_proxmox(&view);
        let node = obs
            .resources
            .iter()
            .find(|r| r.kind == ResourceKind::Node)
            .unwrap();
        assert_eq!(node.id, "node:pve1");
        assert_eq!(node.health, Health::Ok);
        let vm = obs
            .resources
            .iter()
            .find(|r| r.id == "vm:pve1:100")
            .unwrap();
        assert_eq!(vm.kind, ResourceKind::Vm);
        assert_eq!(vm.parent.as_deref(), Some("node:pve1"));
        assert_eq!(vm.health, Health::Ok);
        let lxc = obs
            .resources
            .iter()
            .find(|r| r.id == "lxc:pve1:101")
            .unwrap();
        assert_eq!(lxc.kind, ResourceKind::Container);
        assert_eq!(lxc.health, Health::Unknown);
        let vol = obs
            .resources
            .iter()
            .find(|r| r.id == "storage:pve1:local")
            .unwrap();
        assert_eq!(vol.kind, ResourceKind::Storage);
        assert_eq!(vol.health, Health::Degraded);
        assert!(obs
            .samples
            .iter()
            .any(|s| s.resource_id == "vm:pve1:100" && s.metric == "vm.cpu_pct"));
        assert!(obs
            .samples
            .iter()
            .any(|s| s.metric == "node.cpu_pct" && s.value == 25.0));
    }

    #[test]
    fn offline_node_and_full_storage_fail() {
        assert_eq!(node_health("offline"), Health::Failed);
        assert_eq!(storage_health(true, 96.0, 100.0), Health::Failed);
        assert_eq!(storage_health(false, 1.0, 100.0), Health::Failed);
        assert_eq!(guest_health("paused"), Health::Degraded);
    }

    #[test]
    fn api_error_fails_the_pass() {
        let mut c = ProxmoxCollector::new(Fixed(Err("connection refused".into())));
        let err = c.collect().unwrap_err();
        assert!(err.to_string().contains("connection refused"));
    }
}
