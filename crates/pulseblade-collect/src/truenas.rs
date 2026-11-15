//! TrueNAS pools, disks, replication jobs, and alerts.
//!
//! The live client speaks JSON-RPC 2.0 over `wss://<host>/api/current`.
//! Nothing is installed on the NAS. The API key is an environment variable.
//! Ids: `host:<name>`, `pool:<name>:<pool>`, `disk:<name>:<dev>`.

use chrono::{DateTime, Utc};
use pulseblade_core::{Health, LogLine, Observation, Resource, ResourceKind, Sample};
use serde_json::Value;

use crate::hw::{ident, num, round2, worse, HwLimits, HwTarget};
use crate::{CollectError, Collector};

#[derive(Debug, Clone, PartialEq)]
pub struct NasPool {
    pub name: String,
    pub status: String,
    pub healthy: bool,
    pub used: f64,
    pub available: f64,
    pub scrub_errors: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NasDisk {
    pub name: String,
    pub pool: String,
    pub temp_c: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NasAlert {
    pub level: String,
    pub text: String,
    pub dismissed: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NasJob {
    pub name: String,
    pub state: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NasView {
    pub name: String,
    pub version: String,
    pub pools: Vec<NasPool>,
    pub disks: Vec<NasDisk>,
    pub alerts: Vec<NasAlert>,
    pub jobs: Vec<NasJob>,
    pub limits: HwLimits,
}

pub trait TrueNasSource: Send {
    fn snapshot(&mut self) -> Result<NasView, CollectError>;
}

pub struct TrueNasCollector<S> {
    source: S,
}

impl<S> TrueNasCollector<S> {
    pub fn new(source: S) -> Self {
        Self { source }
    }
}

impl<S: TrueNasSource> Collector for TrueNasCollector<S> {
    fn name(&self) -> &'static str {
        "truenas"
    }

    fn collect(&mut self) -> Result<Observation, CollectError> {
        let view = self.source.snapshot()?;
        Ok(observe_truenas(&view, Utc::now()))
    }
}

pub fn observe_truenas(view: &NasView, now: DateTime<Utc>) -> Observation {
    let Some(name) = ident(&view.name) else {
        return Observation::default();
    };
    let host_id = format!("host:{name}");
    let mut obs = Observation::default();
    let mut host_health = Health::Ok;
    let mut faults: Vec<&str> = Vec::new();

    for pool in &view.pools {
        let Some(pool_name) = ident(&pool.name) else {
            continue;
        };
        let id = format!("pool:{name}:{pool_name}");
        let (health, fault) = pool_health(pool, &view.limits);
        host_health = worse(host_health, health);
        if health.is_unhealthy() {
            faults.push("pool");
        }
        let mut resource = Resource::new(&id, ResourceKind::Pool, pool_name)
            .parent(host_id.clone())
            .health(health)
            .label("host", name)
            .attr("status", pool.status.clone());
        if let Some(fault) = fault {
            resource = resource.attr("fault", fault);
        }
        obs.resources.push(resource);
        let total = pool.used + pool.available;
        if total > 0.0 {
            obs.samples.push(Sample::new(
                &id,
                "pool.used_pct",
                round2(pool.used / total * 100.0),
            ));
        }
    }

    for disk in &view.disks {
        let Some(dev) = ident(&disk.name) else {
            continue;
        };
        let id = format!("disk:{name}:{dev}");
        let (health, fault) = disk_health(disk.temp_c, &view.limits);
        host_health = worse(host_health, health);
        let parent = ident(&disk.pool)
            .map(|pool| format!("pool:{name}:{pool}"))
            .unwrap_or_else(|| host_id.clone());
        let mut resource = Resource::new(&id, ResourceKind::Disk, dev)
            .parent(parent)
            .health(health)
            .label("host", name);
        if !disk.pool.is_empty() {
            resource = resource.attr("pool", disk.pool.clone());
        }
        if let Some(fault) = fault {
            resource = resource.attr("fault", fault);
        }
        obs.resources.push(resource);
        if let Some(temp) = disk.temp_c {
            obs.samples
                .push(Sample::new(&id, "disk.temp_c", round2(temp)));
        }
    }

    let mut replication_error = false;
    for job in &view.jobs {
        if job.state.eq_ignore_ascii_case("error") {
            replication_error = true;
        }
    }
    if replication_error {
        host_health = worse(host_health, Health::Degraded);
        faults.push("replication error");
    }

    for alert in &view.alerts {
        if alert.dismissed || alert.text.is_empty() {
            continue;
        }
        let level = alert.level.to_ascii_uppercase();
        if level == "CRITICAL" {
            host_health = Health::Failed;
            faults.push("critical alert");
        } else if level == "WARNING" {
            host_health = worse(host_health, Health::Degraded);
            faults.push("warning alert");
        } else {
            continue;
        }
        obs.logs.push(LogLine {
            ts: now,
            resource_id: host_id.clone(),
            message: clip(&format!("{level}: {}", alert.text), 400),
        });
    }

    let mut host = Resource::new(&host_id, ResourceKind::Host, name)
        .health(host_health)
        .label("role", "storage")
        .attr("role", "truenas");
    if !view.version.is_empty() {
        host = host.attr("version", view.version.clone());
    }
    if replication_error {
        host = host.attr("replication", "error");
    } else if !view.jobs.is_empty() {
        host = host.attr("replication", "ok");
    }
    if host_health.is_unhealthy() {
        let fault = unique_join(&faults);
        if !fault.is_empty() {
            host = host.attr("fault", fault);
        }
    }
    obs.resources.insert(0, host);
    obs
}

fn pool_health(pool: &NasPool, limits: &HwLimits) -> (Health, Option<String>) {
    let status = pool.status.to_ascii_uppercase();
    let mut health = match status.as_str() {
        "ONLINE" => {
            if pool.healthy {
                Health::Ok
            } else {
                Health::Degraded
            }
        }
        "DEGRADED" => Health::Degraded,
        "FAULTED" | "UNAVAIL" | "OFFLINE" | "REMOVED" => Health::Failed,
        _ => Health::Degraded,
    };
    let mut why: Vec<String> = Vec::new();
    if health != Health::Ok {
        why.push(format!("pool {status}"));
    }
    let total = pool.used + pool.available;
    if total > 0.0 {
        let pct = pool.used / total * 100.0;
        if pct >= limits.pool_used_failed {
            health = Health::Failed;
            why.push("pool over capacity".into());
        } else if pct >= limits.pool_used_degraded {
            health = worse(health, Health::Degraded);
            why.push("pool filling".into());
        }
    }
    if pool.scrub_errors > 0 {
        health = worse(health, Health::Degraded);
        why.push("scrub errors".into());
    }
    let fault = if why.is_empty() {
        None
    } else {
        Some(why.join("; "))
    };
    (health, fault)
}

fn disk_health(temp_c: Option<f64>, limits: &HwLimits) -> (Health, Option<String>) {
    let Some(temp) = temp_c else {
        return (Health::Unknown, None);
    };
    if temp >= limits.disk_temp_failed {
        (Health::Failed, Some("disk hot".into()))
    } else if temp >= limits.disk_temp_degraded {
        (Health::Degraded, Some("disk warm".into()))
    } else {
        (Health::Ok, None)
    }
}

fn unique_join(parts: &[&str]) -> String {
    let mut out = Vec::new();
    for part in parts {
        if !out.contains(part) {
            out.push(*part);
        }
    }
    out.join("; ")
}

fn clip(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(max).collect();
        out.push('…');
        out
    }
}

/// Build a view from the JSON-RPC results. Missing objects are skipped.
#[allow(clippy::too_many_arguments)]
pub fn parse_truenas(
    name: &str,
    info: &Value,
    pools: &Value,
    datasets: &Value,
    disks: &Value,
    alerts: &Value,
    jobs: &Value,
    temperatures: &Value,
    limits: HwLimits,
) -> NasView {
    let version = info
        .get("version")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let mut view = NasView {
        name: name.to_string(),
        version,
        pools: Vec::new(),
        disks: Vec::new(),
        alerts: Vec::new(),
        jobs: Vec::new(),
        limits,
    };
    let dataset_rows = rows(datasets);
    let membership = pool_membership(pools);
    for row in rows(pools) {
        let Some(pool_name) = row.get("name").and_then(|v| v.as_str()) else {
            continue;
        };
        let (used, available) = dataset_usage(&dataset_rows, pool_name);
        let scan = row.get("scan").unwrap_or(&Value::Null);
        view.pools.push(NasPool {
            name: pool_name.to_string(),
            status: row
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("UNKNOWN")
                .to_string(),
            healthy: row.get("healthy").and_then(|v| v.as_bool()).unwrap_or(true),
            used,
            available,
            scrub_errors: num(scan.get("errors")).unwrap_or(0.0) as u64,
        });
    }
    for row in rows(disks) {
        let dev = row
            .get("devname")
            .and_then(|v| v.as_str())
            .or_else(|| row.get("name").and_then(|v| v.as_str()))
            .unwrap_or("");
        if dev.is_empty() {
            continue;
        }
        let pool = row
            .get("pool")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .or_else(|| membership.get(dev).cloned())
            .unwrap_or_default();
        let temp_c = num(row.get("temperature")).or_else(|| num(temperatures.get(dev)));
        view.disks.push(NasDisk {
            name: dev.to_string(),
            pool,
            temp_c,
        });
    }
    for row in rows(alerts) {
        let text = row
            .get("text")
            .and_then(|v| v.as_str())
            .or_else(|| row.get("formatted").and_then(|v| v.as_str()))
            .unwrap_or("")
            .to_string();
        view.alerts.push(NasAlert {
            level: row
                .get("level")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            text,
            dismissed: row
                .get("dismissed")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
        });
    }
    for row in rows(jobs) {
        let state = match row.get("state") {
            Some(Value::String(s)) => s.clone(),
            Some(obj) => obj
                .get("state")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            _ => String::new(),
        };
        let job_name = row
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if !job_name.is_empty() {
            view.jobs.push(NasJob {
                name: job_name,
                state,
            });
        }
    }
    view
}

fn pool_membership(pools: &Value) -> std::collections::BTreeMap<String, String> {
    let mut out = std::collections::BTreeMap::new();
    for pool in rows(pools) {
        let Some(name) = pool.get("name").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(topo) = pool.get("topology").and_then(|v| v.as_object()) else {
            continue;
        };
        for group in topo.values() {
            walk_vdev(group, name, &mut out);
        }
    }
    out
}

fn walk_vdev(node: &Value, pool: &str, out: &mut std::collections::BTreeMap<String, String>) {
    if let Some(rows) = node.as_array() {
        for child in rows {
            walk_vdev(child, pool, out);
        }
        return;
    }
    let Some(obj) = node.as_object() else {
        return;
    };
    if let Some(disk) = obj.get("disk").and_then(|v| v.as_str()) {
        if !disk.is_empty() {
            out.insert(disk.to_string(), pool.to_string());
        }
    }
    if let Some(children) = obj.get("children") {
        walk_vdev(children, pool, out);
    }
}

fn dataset_usage(rows: &[&Value], pool: &str) -> (f64, f64) {
    let Some(row) = rows.iter().copied().find(|row| {
        let id = row.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let name = row.get("name").and_then(|v| v.as_str()).unwrap_or("");
        id == pool || name == pool
    }) else {
        return (0.0, 0.0);
    };
    (
        parsed_bytes(row.get("used")),
        parsed_bytes(row.get("available")),
    )
}

fn parsed_bytes(v: Option<&Value>) -> f64 {
    let Some(v) = v else {
        return 0.0;
    };
    num(v.get("parsed")).or_else(|| num(Some(v))).unwrap_or(0.0)
}

fn rows(v: &Value) -> Vec<&Value> {
    v.as_array().map(|a| a.iter().collect()).unwrap_or_default()
}

#[cfg(feature = "hw")]
pub struct LiveTrueNas {
    target: HwTarget,
}

#[cfg(feature = "hw")]
impl LiveTrueNas {
    pub fn new(target: HwTarget) -> Self {
        Self { target }
    }
}

#[cfg(feature = "hw")]
impl TrueNasSource for LiveTrueNas {
    fn snapshot(&mut self) -> Result<NasView, CollectError> {
        let key = std::env::var(&self.target.secret_env)
            .ok()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                CollectError::Unavailable(format!(
                    "environment variable {} is unset",
                    self.target.secret_env
                ))
            })?;
        let (host, ws_url, port) = ws_parts(&self.target.url)?;
        let mut socket = connect_ws(&host, port, &ws_url, self.target.verify_tls)?;
        let authed = rpc(
            &mut socket,
            1,
            "auth.login_with_api_key",
            serde_json::json!([key]),
        )?;
        if authed.as_bool() != Some(true) {
            return Err(CollectError::Unavailable(
                "TrueNAS API key was rejected".into(),
            ));
        }
        let info = rpc(&mut socket, 2, "system.info", serde_json::json!([]))?;
        let pools = rpc(&mut socket, 3, "pool.query", serde_json::json!([]))?;
        let datasets = rpc(&mut socket, 4, "pool.dataset.query", serde_json::json!([]))?;
        let disks = rpc(&mut socket, 5, "disk.query", serde_json::json!([]))?;
        let temperatures =
            rpc(&mut socket, 6, "disk.temperatures", serde_json::json!([])).unwrap_or(Value::Null);
        let alerts = rpc(&mut socket, 7, "alert.list", serde_json::json!([]))?;
        let jobs =
            rpc(&mut socket, 8, "replication.query", serde_json::json!([])).unwrap_or(Value::Null);
        let _ = socket.close(None);
        Ok(parse_truenas(
            &self.target.name,
            &info,
            &pools,
            &datasets,
            &disks,
            &alerts,
            &jobs,
            &temperatures,
            self.target.limits,
        ))
    }
}

#[cfg(feature = "hw")]
fn ws_parts(url: &str) -> Result<(String, String, u16), CollectError> {
    let url = url.trim().trim_end_matches('/');
    let rest = url.strip_prefix("https://").ok_or_else(|| {
        CollectError::Unavailable(
            "TrueNAS URL must be https; an API key sent over http is revoked".into(),
        )
    })?;
    let (hostport, path) = rest.split_once('/').unwrap_or((rest, ""));
    if hostport.is_empty() {
        return Err(CollectError::Unavailable("TrueNAS URL has no host".into()));
    }
    let (host, port) = match hostport.split_once(':') {
        Some((host, port)) => {
            let port = port
                .parse::<u16>()
                .map_err(|_| CollectError::Unavailable("TrueNAS URL has a bad port".into()))?;
            (host.to_string(), port)
        }
        None => (hostport.to_string(), 443),
    };
    let path = if path.is_empty() {
        "/api/current".to_string()
    } else {
        format!("/{path}")
    };
    Ok((host.clone(), format!("wss://{host}:{port}{path}"), port))
}

#[cfg(feature = "hw")]
fn connect_ws(
    host: &str,
    port: u16,
    ws_url: &str,
    verify_tls: bool,
) -> Result<tungstenite::WebSocket<native_tls::TlsStream<std::net::TcpStream>>, CollectError> {
    use tungstenite::client::IntoClientRequest;
    let tcp = std::net::TcpStream::connect((host, port))
        .map_err(|e| CollectError::Unavailable(format!("TrueNAS connection failed: {e}")))?;
    tcp.set_read_timeout(Some(std::time::Duration::from_secs(20)))
        .ok();
    tcp.set_write_timeout(Some(std::time::Duration::from_secs(20)))
        .ok();
    let mut builder = native_tls::TlsConnector::builder();
    if !verify_tls {
        builder.danger_accept_invalid_certs(true);
        builder.danger_accept_invalid_hostnames(true);
    }
    let connector = builder
        .build()
        .map_err(|e| CollectError::Unavailable(format!("TLS setup failed: {e}")))?;
    let tls = connector
        .connect(host, tcp)
        .map_err(|e| CollectError::Unavailable(format!("TrueNAS TLS failed: {e}")))?;
    let request = ws_url
        .into_client_request()
        .map_err(|e| CollectError::Unavailable(format!("TrueNAS URL rejected: {e}")))?;
    let (socket, _) = tungstenite::client(request, tls)
        .map_err(|e| CollectError::Unavailable(format!("TrueNAS websocket failed: {e}")))?;
    Ok(socket)
}

#[cfg(feature = "hw")]
fn rpc(
    socket: &mut tungstenite::WebSocket<native_tls::TlsStream<std::net::TcpStream>>,
    id: u64,
    method: &str,
    params: Value,
) -> Result<Value, CollectError> {
    use tungstenite::Message;
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
        "params": params,
    });
    socket
        .send(Message::Text(body.to_string().into()))
        .map_err(|e| CollectError::Unavailable(format!("{method} send failed: {e}")))?;
    for _ in 0..32 {
        let message = socket
            .read()
            .map_err(|e| CollectError::Unavailable(format!("{method} read failed: {e}")))?;
        match message {
            Message::Text(text) => {
                let value: Value = serde_json::from_str(&text).map_err(|e| {
                    CollectError::Unavailable(format!("{method} returned invalid JSON: {e}"))
                })?;
                if value.get("id").and_then(|v| v.as_u64()) != Some(id) {
                    continue;
                }
                if let Some(err) = value.get("error") {
                    return Err(CollectError::Unavailable(format!(
                        "{method} failed: {}",
                        clip(&err.to_string(), 180)
                    )));
                }
                return Ok(value.get("result").cloned().unwrap_or(Value::Null));
            }
            Message::Ping(payload) => {
                socket.send(Message::Pong(payload)).ok();
            }
            Message::Close(_) => {
                return Err(CollectError::Unavailable(format!(
                    "{method} failed: websocket closed"
                )));
            }
            _ => {}
        }
    }
    Err(CollectError::Unavailable(format!(
        "{method} failed: no response"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn limits() -> HwLimits {
        HwLimits::default()
    }

    #[test]
    fn a_degraded_pool_and_a_hot_disk_fail_the_nas() {
        let view = parse_truenas(
            "tn",
            &serde_json::json!({"version": "25.10"}),
            &serde_json::json!([
                {"name": "tank", "status": "DEGRADED", "healthy": false, "scan": {"errors": 2}}
            ]),
            &serde_json::json!([
                {"id": "tank", "used": {"parsed": 96.0}, "available": {"parsed": 4.0}},
                {"id": "tank/home", "used": {"parsed": 1.0}, "available": {"parsed": 1.0}}
            ]),
            &serde_json::json!([
                {"devname": "sda", "pool": "tank", "temperature": 61}
            ]),
            &serde_json::json!([
                {"level": "CRITICAL", "text": "Pool tank is DEGRADED", "dismissed": false},
                {"level": "WARNING", "text": "old", "dismissed": true}
            ]),
            &serde_json::json!([
                {"name": "offsite", "state": {"state": "ERROR"}}
            ]),
            &serde_json::json!({}),
            limits(),
        );
        let now = Utc.with_ymd_and_hms(2026, 11, 15, 0, 0, 0).unwrap();
        let obs = observe_truenas(&view, now);
        let host = obs.resources.iter().find(|r| r.id == "host:tn").unwrap();
        assert_eq!(host.health, Health::Failed);
        assert_eq!(host.attrs.get("role").unwrap(), "truenas");
        assert_eq!(host.attrs.get("replication").unwrap(), "error");
        let pool = obs
            .resources
            .iter()
            .find(|r| r.id == "pool:tn:tank")
            .unwrap();
        assert_eq!(pool.health, Health::Failed);
        assert!(pool.attrs["fault"].as_str().unwrap().contains("DEGRADED"));
        assert!(pool.attrs["fault"].as_str().unwrap().contains("capacity"));
        let disk = obs
            .resources
            .iter()
            .find(|r| r.id == "disk:tn:sda")
            .unwrap();
        assert_eq!(disk.health, Health::Failed);
        assert_eq!(disk.parent.as_deref(), Some("pool:tn:tank"));
        assert_eq!(obs.logs.len(), 1);
        assert!(obs.logs[0].message.contains("Pool tank"));
        assert!(!obs.logs[0].message.contains("old"));
    }

    #[test]
    fn an_online_pool_under_the_cutoff_stays_ok() {
        let view = parse_truenas(
            "tn",
            &serde_json::json!({}),
            &serde_json::json!([{"name": "tank", "status": "ONLINE", "healthy": true}]),
            &serde_json::json!([{"id": "tank", "used": {"parsed": 10}, "available": {"parsed": 90}}]),
            &serde_json::json!([]),
            &serde_json::json!([]),
            &serde_json::json!([]),
            &serde_json::json!({}),
            limits(),
        );
        let obs = observe_truenas(&view, Utc::now());
        assert_eq!(obs.resources[0].health, Health::Ok);
        assert_eq!(obs.resources[1].health, Health::Ok);
        assert!(!obs.resources[0].attrs.contains_key("fault"));
    }

    #[test]
    fn temperature_and_topology_fill_a_disk_the_query_left_blank() {
        let view = parse_truenas(
            "tn",
            &serde_json::json!({}),
            &serde_json::json!([{
                "name": "tank",
                "status": "ONLINE",
                "healthy": true,
                "topology": {"data": [{"type": "RAIDZ", "children": [{"disk": "sda", "type": "DISK"}]}]}
            }]),
            &serde_json::json!([{"id": "tank", "used": {"parsed": 10}, "available": {"parsed": 90}}]),
            &serde_json::json!([{"devname": "sda"}]),
            &serde_json::json!([]),
            &serde_json::json!([]),
            &serde_json::json!({"sda": 33}),
            limits(),
        );
        let disk = view.disks.iter().find(|d| d.name == "sda").unwrap();
        assert_eq!(disk.pool, "tank");
        assert_eq!(disk.temp_c, Some(33.0));
        let obs = observe_truenas(&view, Utc::now());
        let stored = obs
            .resources
            .iter()
            .find(|r| r.id == "disk:tn:sda")
            .unwrap();
        assert_eq!(stored.health, Health::Ok);
        assert_eq!(stored.parent.as_deref(), Some("pool:tn:tank"));
    }
}
