//! Kubernetes nodes, workloads, pods, and warning events.
//!
//! Ids: `knode:<cluster>:<name>`, `workload:<cluster>:<ns>:<kind>:<name>`,
//! `pod:<cluster>:<ns>:<name>`. The live client is `kube`. An empty API URL
//! leaves the collector off; a configured API that is down fails the pass.

#[cfg(all(test, feature = "k8s"))]
use std::io::{Read, Write};
#[cfg(all(test, feature = "k8s"))]
use std::net::TcpListener;

use std::sync::Arc;

use chrono::{DateTime, SecondsFormat, Utc};
use pulseblade_core::{
    Health, HealthThresholds, LogLine, Observation, Resource, ResourceKind, Sample,
};
#[cfg(test)]
use serde_json::Value;

use crate::{CollectError, Collector, HealthGate};

const MAX_NODES: usize = 40;
const MAX_WORKLOADS: usize = 60;
const MAX_PODS: usize = 80;
const MAX_EVENTS: usize = 40;

#[derive(Debug, Clone, PartialEq)]
pub struct K8sNode {
    pub name: String,
    pub ready: bool,
    pub cpu: f64,
    pub mem: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct K8sWorkload {
    pub kind: String,
    pub namespace: String,
    pub name: String,
    pub replicas: u64,
    pub ready: u64,
    /// `readiness_port` when the HTTP probe targets a port the container does not declare.
    pub fault: String,
    pub container: String,
    pub probe_port: u16,
    pub container_port: u16,
}

struct ProbeMismatch {
    container: String,
    probe_port: u16,
    container_port: u16,
}

fn fault_fields(mismatch: Option<ProbeMismatch>) -> (String, String, u16, u16) {
    match mismatch {
        Some(mismatch) => (
            "readiness_port".into(),
            mismatch.container,
            mismatch.probe_port,
            mismatch.container_port,
        ),
        None => (String::new(), String::new(), 0, 0),
    }
}

/// HTTP readiness port that is not one of the container's declared ports.
fn mismatch_from_ports(
    container: &str,
    probe_port: u16,
    declared: &[u16],
) -> Option<ProbeMismatch> {
    if container.is_empty() || probe_port == 0 || declared.is_empty() {
        return None;
    }
    if declared.contains(&probe_port) {
        return None;
    }
    Some(ProbeMismatch {
        container: container.to_string(),
        probe_port,
        container_port: declared[0],
    })
}

#[derive(Debug, Clone, PartialEq)]
pub struct K8sPod {
    pub namespace: String,
    pub name: String,
    pub phase: String,
    pub node: String,
    pub restarts: u64,
    /// Newest container restart, when one could be read.
    pub restarted_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct K8sEvent {
    pub namespace: String,
    pub kind: String,
    pub name: String,
    pub reason: String,
    pub message: String,
    pub warning: bool,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct K8sView {
    pub nodes: Vec<K8sNode>,
    pub workloads: Vec<K8sWorkload>,
    pub pods: Vec<K8sPod>,
    pub events: Vec<K8sEvent>,
}

#[derive(Debug, Clone)]
pub struct K8sTarget {
    pub api_url: String,
    pub token_env: String,
    pub cluster: String,
    pub verify_tls: bool,
    /// PEM CA bundle. Empty uses the default trust store.
    pub ca: String,
}

pub trait K8sSource: Send {
    fn snapshot(&mut self) -> Result<K8sView, CollectError>;
}

pub struct K8sCollector<S> {
    cluster: String,
    source: S,
    health: Arc<HealthGate>,
}

impl<S> K8sCollector<S> {
    pub fn new(cluster: impl Into<String>, source: S) -> Self {
        Self::with_health(
            cluster,
            source,
            Arc::new(HealthGate::new(HealthThresholds::default())),
        )
    }

    pub fn with_health(cluster: impl Into<String>, source: S, health: Arc<HealthGate>) -> Self {
        Self {
            cluster: cluster.into(),
            source,
            health,
        }
    }
}

impl<S: K8sSource> Collector for K8sCollector<S> {
    fn name(&self) -> &'static str {
        "k8s"
    }

    fn collect(&mut self) -> Result<Observation, CollectError> {
        let view = self.source.snapshot()?;
        let limits = self.health.get();
        Ok(observe_k8s(&self.cluster, &view, &limits, Utc::now()))
    }
}

pub fn observe_k8s(
    cluster: &str,
    view: &K8sView,
    limits: &HealthThresholds,
    now: DateTime<Utc>,
) -> Observation {
    let mut obs = Observation::default();
    for node in view.nodes.iter().take(MAX_NODES) {
        let Some(name) = id_part(&node.name) else {
            continue;
        };
        let id = format!("knode:{cluster}:{name}");
        let health = if node.ready {
            Health::Ok
        } else {
            Health::Failed
        };
        obs.resources.push(
            Resource::new(&id, ResourceKind::Node, name)
                .health(health)
                .label("cluster", cluster)
                .label("host", name)
                .attr("status", if node.ready { "Ready" } else { "NotReady" }),
        );
        if node.cpu.is_finite() {
            obs.samples.push(Sample::new(&id, "k8s.cpu", node.cpu));
        }
        if node.mem.is_finite() {
            obs.samples
                .push(Sample::new(&id, "k8s.mem_bytes", node.mem));
        }
    }
    for wl in view.workloads.iter().take(MAX_WORKLOADS) {
        let Some(name) = id_part(&wl.name) else {
            continue;
        };
        let Some(ns) = id_part(&wl.namespace) else {
            continue;
        };
        let kind = wl.kind.to_ascii_lowercase();
        let id = format!("workload:{cluster}:{ns}:{kind}:{name}");
        let health = if wl.replicas > 0 && wl.ready == 0 {
            Health::Failed
        } else if wl.ready < wl.replicas {
            Health::Degraded
        } else {
            Health::Ok
        };
        let mut resource = Resource::new(&id, ResourceKind::Workload, name)
            .health(health)
            .label("cluster", cluster)
            .label("namespace", ns)
            .attr("kind", kind.clone())
            .attr("ready", format!("{}/{}", wl.ready, wl.replicas));
        if wl.fault == "readiness_port" {
            resource = resource
                .attr("fault", "readiness_port")
                .attr("probe_port", wl.probe_port)
                .attr("container_port", wl.container_port)
                .attr("container", wl.container.clone())
                .attr(
                    "unready",
                    format!(
                        "readiness probe port {}, container {} listens on {}",
                        wl.probe_port, wl.container, wl.container_port
                    ),
                );
        }
        obs.resources.push(resource);
        obs.samples
            .push(Sample::new(&id, "k8s.replicas", wl.replicas as f64));
        obs.samples
            .push(Sample::new(&id, "k8s.ready", wl.ready as f64));
    }
    for pod in view.pods.iter().take(MAX_PODS) {
        let Some(name) = id_part(&pod.name) else {
            continue;
        };
        let Some(ns) = id_part(&pod.namespace) else {
            continue;
        };
        let id = format!("pod:{cluster}:{ns}:{name}");
        let health = pod_health(&pod.phase, pod.restarts, pod.restarted_at, now, limits);
        let mut resource = Resource::new(&id, ResourceKind::Pod, name)
            .health(health)
            .label("cluster", cluster)
            .label("namespace", ns)
            .attr("phase", pod.phase.clone())
            .attr("namespace", ns);
        if let Some(at) = pod.restarted_at {
            resource = resource.attr(
                "restarted_at",
                at.to_rfc3339_opts(SecondsFormat::Secs, true),
            );
        }
        if let Some(node) = id_part(&pod.node) {
            resource = resource.parent(format!("knode:{cluster}:{node}"));
        }
        obs.resources.push(resource);
        obs.samples
            .push(Sample::new(&id, "k8s.restarts", pod.restarts as f64));
    }
    for event in view.events.iter().filter(|e| e.warning).take(MAX_EVENTS) {
        let Some(name) = id_part(&event.name) else {
            continue;
        };
        let Some(ns) = id_part(&event.namespace) else {
            continue;
        };
        let id = match event.kind.to_ascii_lowercase().as_str() {
            "pod" => format!("pod:{cluster}:{ns}:{name}"),
            "node" => format!("knode:{cluster}:{name}"),
            other => format!("workload:{cluster}:{ns}:{other}:{name}"),
        };
        let message = format!("{}: {}", event.reason, event.message);
        obs.logs.push(LogLine {
            ts: chrono::Utc::now(),
            resource_id: id,
            message,
        });
    }
    obs
}

fn id_part(raw: &str) -> Option<&str> {
    let s = raw.trim();
    if s.is_empty() || s.contains(':') || s.contains('/') {
        None
    } else {
        Some(s)
    }
}

/// One container's restart counter and the two timestamps the rule can use.
struct ContainerRestart {
    restarts: u64,
    running_started_at: Option<DateTime<Utc>>,
    waiting_reason: String,
    last_finished_at: Option<DateTime<Utc>>,
}

/// Newest restart across containers, and the lifetime sum.
///
/// A container with `restartCount > 0` contributes `state.running.startedAt`.
/// A container waiting in backoff contributes `lastState.terminated.finishedAt`.
fn fold_restarts(
    containers: impl IntoIterator<Item = ContainerRestart>,
) -> (u64, Option<DateTime<Utc>>) {
    let mut sum = 0u64;
    let mut newest = None;
    for c in containers {
        sum = sum.saturating_add(c.restarts);
        if let Some(at) = container_restart_at(&c) {
            if newest.is_none_or(|have| at > have) {
                newest = Some(at);
            }
        }
    }
    (sum, newest)
}

fn container_restart_at(c: &ContainerRestart) -> Option<DateTime<Utc>> {
    let mut newest = None;
    let consider = |newest: &mut Option<DateTime<Utc>>, at: Option<DateTime<Utc>>| {
        let Some(at) = at else {
            return;
        };
        if newest.is_none_or(|have| at > have) {
            *newest = Some(at);
        }
    };
    if c.restarts > 0 {
        consider(&mut newest, c.running_started_at);
    }
    if c.waiting_reason.to_ascii_lowercase().contains("backoff") {
        consider(&mut newest, c.last_finished_at);
    }
    newest
}

/// Phase first. Restart age and the lifetime sum apply only to Running and Succeeded.
pub fn pod_health(
    phase: &str,
    restarts: u64,
    restarted_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
    limits: &HealthThresholds,
) -> Health {
    let phase = phase.to_ascii_lowercase();
    if phase == "failed" || phase == "unknown" {
        return Health::Failed;
    }
    if phase == "pending" {
        return Health::Degraded;
    }
    if phase != "running" && phase != "succeeded" {
        return Health::Unknown;
    }
    let window = chrono::Duration::minutes(limits.pod_restart_window_mins as i64);
    let recent = restarted_at.is_some_and(|at| now.signed_duration_since(at) <= window);
    if recent || restarts >= limits.pod_restart_lifetime {
        Health::Degraded
    } else {
        Health::Ok
    }
}

#[cfg(test)]
fn parse_ts(raw: Option<&str>) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw?)
        .ok()
        .map(|d| d.with_timezone(&Utc))
}

#[cfg(test)]
fn restart_from_status(c: &Value) -> ContainerRestart {
    ContainerRestart {
        restarts: c.get("restartCount").and_then(|n| n.as_u64()).unwrap_or(0),
        running_started_at: parse_ts(
            c.pointer("/state/running/startedAt")
                .and_then(|v| v.as_str()),
        ),
        waiting_reason: c
            .pointer("/state/waiting/reason")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        last_finished_at: parse_ts(
            c.pointer("/lastState/terminated/finishedAt")
                .and_then(|v| v.as_str()),
        ),
    }
}

/// Parse list JSON. The live client uses typed objects; this is for tests.
#[cfg(test)]
pub fn parse_lists(
    nodes: &Value,
    pods: &Value,
    deployments: &Value,
    statefulsets: &Value,
    daemonsets: &Value,
    events: &Value,
) -> K8sView {
    let mut view = K8sView::default();
    for item in items(nodes) {
        let name = str_at(item, &["metadata", "name"]);
        if name.is_empty() {
            continue;
        }
        let ready = item
            .pointer("/status/conditions")
            .and_then(|c| c.as_array())
            .is_some_and(|conds| {
                conds.iter().any(|c| {
                    c.get("type").and_then(|t| t.as_str()) == Some("Ready")
                        && c.get("status").and_then(|s| s.as_str()) == Some("True")
                })
            });
        view.nodes.push(K8sNode {
            name,
            ready,
            cpu: num_at(item, &["usage", "cpu"]),
            mem: num_at(item, &["usage", "memory"]),
        });
    }
    for (kind, list) in [
        ("Deployment", deployments),
        ("StatefulSet", statefulsets),
        ("DaemonSet", daemonsets),
    ] {
        for item in items(list) {
            let name = str_at(item, &["metadata", "name"]);
            let namespace = str_at(item, &["metadata", "namespace"]);
            if name.is_empty() {
                continue;
            }
            let (fault, container, probe_port, container_port) =
                fault_fields(probe_from_spec(item));
            view.workloads.push(K8sWorkload {
                kind: kind.into(),
                namespace: if namespace.is_empty() {
                    "default".into()
                } else {
                    namespace
                },
                name,
                replicas: num_at(item, &["spec", "replicas"]) as u64,
                ready: num_at(item, &["status", "readyReplicas"]) as u64,
                fault,
                container,
                probe_port,
                container_port,
            });
        }
    }
    for item in items(pods) {
        let name = str_at(item, &["metadata", "name"]);
        let namespace = str_at(item, &["metadata", "namespace"]);
        if name.is_empty() {
            continue;
        }
        let (restarts, restarted_at) = item
            .pointer("/status/containerStatuses")
            .and_then(|v| v.as_array())
            .map(|cs| fold_restarts(cs.iter().map(restart_from_status)))
            .unwrap_or((0, None));
        view.pods.push(K8sPod {
            namespace: if namespace.is_empty() {
                "default".into()
            } else {
                namespace
            },
            name,
            phase: str_at(item, &["status", "phase"]),
            node: str_at(item, &["spec", "nodeName"]),
            restarts,
            restarted_at,
        });
    }
    for item in items(events) {
        let warning = str_at(item, &["type"]).eq_ignore_ascii_case("warning");
        view.events.push(K8sEvent {
            namespace: {
                let ns = str_at(item, &["involvedObject", "namespace"]);
                if ns.is_empty() {
                    str_at(item, &["metadata", "namespace"])
                } else {
                    ns
                }
            },
            kind: str_at(item, &["involvedObject", "kind"]),
            name: str_at(item, &["involvedObject", "name"]),
            reason: str_at(item, &["reason"]),
            message: str_at(item, &["message"]),
            warning,
        });
    }
    view
}

#[cfg(test)]
fn probe_from_spec(item: &Value) -> Option<ProbeMismatch> {
    let containers = item.pointer("/spec/template/spec/containers")?.as_array()?;
    for container in containers {
        let name = container.get("name").and_then(|v| v.as_str()).unwrap_or("");
        let Some(probe_port) = container
            .pointer("/readinessProbe/httpGet/port")
            .and_then(|v| v.as_u64())
            .filter(|port| *port <= u64::from(u16::MAX))
            .map(|port| port as u16)
        else {
            continue;
        };
        let declared = container
            .get("ports")
            .and_then(|v| v.as_array())
            .map(|ports| {
                ports
                    .iter()
                    .filter_map(|port| port.get("containerPort").and_then(|v| v.as_u64()))
                    .filter(|port| *port <= u64::from(u16::MAX))
                    .map(|port| port as u16)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if let Some(mismatch) = mismatch_from_ports(name, probe_port, &declared) {
            return Some(mismatch);
        }
    }
    None
}

#[cfg(test)]
fn items(v: &Value) -> &[Value] {
    v.get("items")
        .and_then(|i| i.as_array())
        .map(|a| a.as_slice())
        .unwrap_or(&[])
}

#[cfg(test)]
fn str_at(v: &Value, path: &[&str]) -> String {
    let mut cur = v;
    for p in path {
        cur = match cur.get(*p) {
            Some(next) => next,
            None => return String::new(),
        };
    }
    cur.as_str().unwrap_or("").to_string()
}

#[cfg(test)]
fn num_at(v: &Value, path: &[&str]) -> f64 {
    let mut cur = v;
    for p in path {
        cur = match cur.get(*p) {
            Some(next) => next,
            None => return 0.0,
        };
    }
    cur.as_f64()
        .or_else(|| cur.as_i64().map(|n| n as f64))
        .unwrap_or(0.0)
}

#[cfg(feature = "k8s")]
pub struct LiveK8s {
    cluster: String,
    client: kube::Client,
    rt: tokio::runtime::Runtime,
}

#[cfg(feature = "k8s")]
impl LiveK8s {
    pub fn connect(target: &K8sTarget) -> Result<Self, CollectError> {
        let token = std::env::var(&target.token_env).map_err(|_| {
            CollectError::Unavailable(format!(
                "environment variable {} is not set",
                target.token_env
            ))
        })?;
        if token.is_empty() {
            return Err(CollectError::Unavailable(format!(
                "environment variable {} is empty",
                target.token_env
            )));
        }
        let uri: http::Uri = target
            .api_url
            .parse()
            .map_err(|e| CollectError::Unavailable(format!("kubernetes API url: {e}")))?;
        let _ = rustls::crypto::ring::default_provider().install_default();
        let mut cfg = kube::Config::new(uri);
        cfg.accept_invalid_certs = !target.verify_tls;
        cfg.auth_info.token = Some(token.into());
        if !target.ca.trim().is_empty() {
            cfg.root_cert_file = Some(std::path::PathBuf::from(target.ca.trim()));
        }
        cfg.connect_timeout = Some(std::time::Duration::from_secs(5));
        cfg.read_timeout = Some(std::time::Duration::from_secs(10));
        cfg.default_retry = false;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| CollectError::Unavailable(e.to_string()))?;
        let client = rt
            .block_on(async move { kube::Client::try_from(cfg) })
            .map_err(|e| CollectError::Unavailable(format!("kubernetes client: {e}")))?;
        let cluster = if target.cluster.trim().is_empty() {
            "cluster".into()
        } else {
            target.cluster.clone()
        };
        Ok(Self {
            cluster,
            client,
            rt,
        })
    }

    pub fn cluster(&self) -> &str {
        &self.cluster
    }
}

#[cfg(feature = "k8s")]
impl K8sSource for LiveK8s {
    fn snapshot(&mut self) -> Result<K8sView, CollectError> {
        let client = self.client.clone();
        self.rt.block_on(async move { fetch_view(client).await })
    }
}

#[cfg(feature = "k8s")]
fn k8s_time(t: &k8s_openapi::apimachinery::pkg::apis::meta::v1::Time) -> Option<DateTime<Utc>> {
    let ts = t.clone().0;
    let nanos = ts.subsec_nanosecond();
    let nanos = if nanos < 0 { 0 } else { nanos as u32 };
    DateTime::<Utc>::from_timestamp(ts.as_second(), nanos)
}

#[cfg(feature = "k8s")]
fn container_restart_of(cs: &k8s_openapi::api::core::v1::ContainerStatus) -> ContainerRestart {
    ContainerRestart {
        restarts: cs.restart_count.max(0) as u64,
        running_started_at: cs
            .state
            .as_ref()
            .and_then(|s| s.running.as_ref())
            .and_then(|r| r.started_at.as_ref())
            .and_then(k8s_time),
        waiting_reason: cs
            .state
            .as_ref()
            .and_then(|s| s.waiting.as_ref())
            .and_then(|w| w.reason.clone())
            .unwrap_or_default(),
        last_finished_at: cs
            .last_state
            .as_ref()
            .and_then(|s| s.terminated.as_ref())
            .and_then(|t| t.finished_at.as_ref())
            .and_then(k8s_time),
    }
}

#[cfg(feature = "k8s")]
async fn fetch_view(client: kube::Client) -> Result<K8sView, CollectError> {
    use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, StatefulSet};
    use k8s_openapi::api::core::v1::{Event, Node, Pod};
    use kube::api::{Api, ListParams};

    let lp = ListParams::default();
    let nodes = Api::<Node>::all(client.clone())
        .list(&lp)
        .await
        .map_err(api_err)?;
    let pods = Api::<Pod>::all(client.clone())
        .list(&lp)
        .await
        .map_err(api_err)?;
    let deployments = Api::<Deployment>::all(client.clone())
        .list(&lp)
        .await
        .map_err(api_err)?;
    let statefulsets = Api::<StatefulSet>::all(client.clone())
        .list(&lp)
        .await
        .map_err(api_err)?;
    let daemonsets = Api::<DaemonSet>::all(client.clone())
        .list(&lp)
        .await
        .map_err(api_err)?;
    let events = Api::<Event>::all(client).list(&lp).await.map_err(api_err)?;

    let mut view = K8sView::default();
    for node in nodes {
        let Some(name) = node.metadata.name.clone() else {
            continue;
        };
        let ready = node
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .is_some_and(|conds| {
                conds
                    .iter()
                    .any(|c| c.type_ == "Ready" && c.status == "True")
            });
        view.nodes.push(K8sNode {
            name,
            ready,
            cpu: 0.0,
            mem: 0.0,
        });
    }
    for item in deployments {
        let mismatch = item.spec.as_ref().and_then(|spec| {
            spec.template
                .spec
                .as_ref()
                .and_then(|pod| probe_mismatch_containers(&pod.containers))
        });
        if let Some(wl) = workload_of(
            "Deployment",
            item.metadata.name,
            item.metadata.namespace,
            item.spec.and_then(|s| s.replicas),
            item.status.and_then(|s| s.ready_replicas),
            mismatch,
        ) {
            view.workloads.push(wl);
        }
    }
    for item in statefulsets {
        let mismatch = item.spec.as_ref().and_then(|spec| {
            spec.template
                .spec
                .as_ref()
                .and_then(|pod| probe_mismatch_containers(&pod.containers))
        });
        if let Some(wl) = workload_of(
            "StatefulSet",
            item.metadata.name,
            item.metadata.namespace,
            item.spec.and_then(|s| s.replicas),
            item.status.and_then(|s| s.ready_replicas),
            mismatch,
        ) {
            view.workloads.push(wl);
        }
    }
    for item in daemonsets {
        let (replicas, ready) = match item.status {
            Some(status) => (
                status.desired_number_scheduled.max(0) as u64,
                status.number_ready.max(0) as u64,
            ),
            None => (0, 0),
        };
        let Some(name) = item.metadata.name else {
            continue;
        };
        let mismatch = item
            .spec
            .as_ref()
            .and_then(|spec| spec.template.spec.as_ref())
            .and_then(|pod| probe_mismatch_containers(&pod.containers));
        let (fault, container, probe_port, container_port) = fault_fields(mismatch);
        view.workloads.push(K8sWorkload {
            kind: "DaemonSet".into(),
            namespace: item.metadata.namespace.unwrap_or_else(|| "default".into()),
            name,
            replicas,
            ready,
            fault,
            container,
            probe_port,
            container_port,
        });
    }
    for pod in pods {
        let Some(name) = pod.metadata.name else {
            continue;
        };
        let (restarts, restarted_at) = pod
            .status
            .as_ref()
            .and_then(|s| s.container_statuses.as_ref())
            .map(|cs| fold_restarts(cs.iter().map(container_restart_of)))
            .unwrap_or((0, None));
        view.pods.push(K8sPod {
            namespace: pod.metadata.namespace.unwrap_or_else(|| "default".into()),
            name,
            phase: pod
                .status
                .as_ref()
                .and_then(|s| s.phase.clone())
                .unwrap_or_default(),
            node: pod
                .spec
                .as_ref()
                .and_then(|s| s.node_name.clone())
                .unwrap_or_default(),
            restarts,
            restarted_at,
        });
    }
    for event in events {
        let warning = event
            .type_
            .as_deref()
            .is_some_and(|t| t.eq_ignore_ascii_case("warning"));
        view.events.push(K8sEvent {
            namespace: event
                .involved_object
                .namespace
                .or(event.metadata.namespace)
                .unwrap_or_default(),
            kind: event.involved_object.kind.unwrap_or_default(),
            name: event.involved_object.name.unwrap_or_default(),
            reason: event.reason.unwrap_or_default(),
            message: event.message.unwrap_or_default(),
            warning,
        });
    }
    Ok(view)
}

#[cfg(feature = "k8s")]
fn workload_of(
    kind: &str,
    name: Option<String>,
    namespace: Option<String>,
    replicas: Option<i32>,
    ready: Option<i32>,
    mismatch: Option<ProbeMismatch>,
) -> Option<K8sWorkload> {
    let name = name?;
    let (fault, container, probe_port, container_port) = fault_fields(mismatch);
    Some(K8sWorkload {
        kind: kind.into(),
        namespace: namespace.unwrap_or_else(|| "default".into()),
        name,
        replicas: replicas.unwrap_or(0).max(0) as u64,
        ready: ready.unwrap_or(0).max(0) as u64,
        fault,
        container,
        probe_port,
        container_port,
    })
}

#[cfg(feature = "k8s")]
fn probe_mismatch_containers(
    containers: &[k8s_openapi::api::core::v1::Container],
) -> Option<ProbeMismatch> {
    use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
    for container in containers {
        let Some(port) = container
            .readiness_probe
            .as_ref()
            .and_then(|probe| probe.http_get.as_ref())
            .map(|http| &http.port)
        else {
            continue;
        };
        let IntOrString::Int(probe_port) = port else {
            continue;
        };
        if *probe_port <= 0 || *probe_port > i32::from(u16::MAX) {
            continue;
        }
        let declared = container
            .ports
            .as_ref()
            .map(|ports| {
                ports
                    .iter()
                    .map(|port| port.container_port)
                    .filter(|port| *port > 0 && *port <= i32::from(u16::MAX))
                    .map(|port| port as u16)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if let Some(mismatch) = mismatch_from_ports(&container.name, *probe_port as u16, &declared)
        {
            return Some(mismatch);
        }
    }
    None
}

#[cfg(feature = "k8s")]
fn api_err(e: kube::Error) -> CollectError {
    CollectError::Unavailable(format!("kubernetes API: {e}"))
}

/// List fixture for tests. Query strings are ignored. The thread exits after
/// a handful of requests; callers do not join it.
#[cfg(all(test, feature = "k8s"))]
pub fn serve_fixture(lists: std::collections::HashMap<String, Value>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for _ in 0..16 {
            let Ok((mut sock, _)) = listener.accept() else {
                break;
            };
            let _ = sock.set_read_timeout(Some(std::time::Duration::from_secs(2)));
            let mut buf = Vec::new();
            let mut tmp = [0u8; 2048];
            loop {
                match sock.read(&mut tmp) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        buf.extend_from_slice(&tmp[..n]);
                        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                            break;
                        }
                    }
                }
            }
            let req = String::from_utf8_lossy(&buf);
            let path = req
                .split_whitespace()
                .nth(1)
                .unwrap_or("/")
                .split('?')
                .next()
                .unwrap_or("/");
            let body = lists
                .get(path)
                .cloned()
                .unwrap_or_else(|| serde_json::json!({"items": []}));
            let bytes = body.to_string();
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{bytes}",
                bytes.len()
            );
            let _ = sock.write_all(resp.as_bytes());
        }
    });
    format!("http://{addr}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fixture() -> K8sView {
        parse_lists(
            &json!({"items": [
                {"metadata": {"name": "n1"}, "status": {"conditions": [{"type": "Ready", "status": "True"}]}, "usage": {"cpu": 0.2, "memory": 100.0}},
                {"metadata": {"name": "n2"}, "status": {"conditions": [{"type": "Ready", "status": "False"}]}}
            ]}),
            &json!({"items": [
                {"metadata": {"name": "api", "namespace": "demo"}, "spec": {"nodeName": "n1"}, "status": {"phase": "Running", "containerStatuses": [{"restartCount": 1}]}},
                {"metadata": {"name": "bad", "namespace": "demo"}, "spec": {"nodeName": "n2"}, "status": {"phase": "Failed", "containerStatuses": [{"restartCount": 0}]}}
            ]}),
            &json!({"items": [
                {"metadata": {"name": "api", "namespace": "demo"}, "spec": {"replicas": 2}, "status": {"readyReplicas": 1}}
            ]}),
            &json!({"items": []}),
            &json!({"items": []}),
            &json!({"items": [
                {"type": "Warning", "reason": "BackOff", "message": "back-off restarting", "involvedObject": {"kind": "Pod", "name": "bad", "namespace": "demo"}}
            ]}),
        )
    }

    #[test]
    fn inventory_ids_and_health() {
        let obs = observe_k8s("demo", &fixture(), &HealthThresholds::default(), Utc::now());
        let n1 = obs
            .resources
            .iter()
            .find(|r| r.id == "knode:demo:n1")
            .unwrap();
        assert_eq!(n1.health, Health::Ok);
        assert_eq!(n1.kind, ResourceKind::Node);
        let n2 = obs
            .resources
            .iter()
            .find(|r| r.id == "knode:demo:n2")
            .unwrap();
        assert_eq!(n2.health, Health::Failed);
        let wl = obs
            .resources
            .iter()
            .find(|r| r.id == "workload:demo:demo:deployment:api")
            .unwrap();
        assert_eq!(wl.kind, ResourceKind::Workload);
        assert_eq!(wl.health, Health::Degraded);
        let pod = obs
            .resources
            .iter()
            .find(|r| r.id == "pod:demo:demo:api")
            .unwrap();
        assert_eq!(pod.parent.as_deref(), Some("knode:demo:n1"));
        assert_eq!(pod.health, Health::Ok);
        let bad = obs
            .resources
            .iter()
            .find(|r| r.id == "pod:demo:demo:bad")
            .unwrap();
        assert_eq!(bad.health, Health::Failed);
        assert!(obs
            .logs
            .iter()
            .any(|l| l.resource_id == "pod:demo:demo:bad" && l.message.contains("BackOff")));
    }

    #[test]
    fn readiness_probe_on_the_wrong_port_is_a_fault() {
        let view = parse_lists(
            &json!({"items": []}),
            &json!({"items": []}),
            &json!({"items": [{
                "metadata": {"name": "pb-fault-loop", "namespace": "pb-fault"},
                "spec": {
                    "replicas": 1,
                    "template": {"spec": {"containers": [{
                        "name": "nginx",
                        "ports": [{"containerPort": 80}],
                        "readinessProbe": {"httpGet": {"path": "/", "port": 81}}
                    }]}}
                },
                "status": {"readyReplicas": 0}
            }]}),
            &json!({"items": []}),
            &json!({"items": []}),
            &json!({"items": []}),
        );
        let obs = observe_k8s("poc-apps", &view, &HealthThresholds::default(), Utc::now());
        let wl = obs
            .resources
            .iter()
            .find(|r| r.id == "workload:poc-apps:pb-fault:deployment:pb-fault-loop")
            .unwrap();
        assert_eq!(
            wl.attrs.get("fault").and_then(|v| v.as_str()),
            Some("readiness_port")
        );
        assert_eq!(
            wl.attrs.get("probe_port").and_then(|v| v.as_u64()),
            Some(81)
        );
        assert_eq!(
            wl.attrs.get("container_port").and_then(|v| v.as_u64()),
            Some(80)
        );
        assert_eq!(
            wl.attrs.get("container").and_then(|v| v.as_str()),
            Some("nginx")
        );
    }

    fn pod_at(name: &str, phase: &str, statuses: Value) -> Value {
        json!({
            "metadata": {"name": name, "namespace": "demo"},
            "spec": {"nodeName": "n1"},
            "status": {"phase": phase, "containerStatuses": statuses}
        })
    }

    fn started(restarts: u64, at: &str) -> Value {
        json!({
            "restartCount": restarts,
            "state": {"running": {"startedAt": at}}
        })
    }

    #[test]
    fn running_pod_restart_window_and_lifetime() {
        let now = DateTime::parse_from_rfc3339("2026-10-26T18:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let days = (now - chrono::Duration::days(3)).to_rfc3339_opts(SecondsFormat::Secs, true);
        let recent =
            (now - chrono::Duration::minutes(2)).to_rfc3339_opts(SecondsFormat::Secs, true);
        let limits = HealthThresholds::default();
        assert_eq!(limits.pod_restart_window_mins, 15);
        assert_eq!(limits.pod_restart_lifetime, 100);
        let view = parse_lists(
            &json!({"items": []}),
            &json!({"items": [
                pod_at("old", "Running", json!([started(29, &days)])),
                pod_at("fresh", "Running", json!([started(29, &recent)])),
                pod_at("many", "Running", json!([started(100, &days)])),
                pod_at("dead", "Failed", json!([started(0, &recent)])),
                pod_at("wait", "Pending", json!([started(0, &days)])),
                pod_at("loop", "Running", json!([{
                    "restartCount": 4,
                    "state": {"waiting": {"reason": "CrashLoopBackOff"}},
                    "lastState": {"terminated": {"finishedAt": recent}}
                }]))
            ]}),
            &json!({"items": []}),
            &json!({"items": []}),
            &json!({"items": []}),
            &json!({"items": []}),
        );
        let obs = observe_k8s("c", &view, &limits, now);
        let health = |name: &str| {
            obs.resources
                .iter()
                .find(|r| r.id == format!("pod:c:demo:{name}"))
                .unwrap()
                .health
        };
        let attr = |name: &str| {
            obs.resources
                .iter()
                .find(|r| r.id == format!("pod:c:demo:{name}"))
                .unwrap()
                .attrs
                .get("restarted_at")
                .and_then(|v| v.as_str())
                .map(str::to_string)
        };
        assert_eq!(health("old"), Health::Ok);
        assert_eq!(attr("old").as_deref(), Some(days.as_str()));
        assert_eq!(health("fresh"), Health::Degraded);
        assert_eq!(attr("fresh").as_deref(), Some(recent.as_str()));
        assert_eq!(health("many"), Health::Degraded);
        assert_eq!(health("dead"), Health::Failed);
        assert_eq!(health("wait"), Health::Degraded);
        assert_eq!(health("loop"), Health::Degraded);
        assert_eq!(attr("loop").as_deref(), Some(recent.as_str()));
        assert!(obs.samples.iter().any(|s| {
            s.resource_id == "pod:c:demo:old" && s.metric == "k8s.restarts" && s.value == 29.0
        }));
        let mut wider = limits.clone();
        wider.pod_restart_window_mins = 1;
        let narrowed = observe_k8s("c", &view, &wider, now);
        assert_eq!(
            narrowed
                .resources
                .iter()
                .find(|r| r.id == "pod:c:demo:fresh")
                .unwrap()
                .health,
            Health::Ok
        );
        assert_eq!(
            narrowed
                .resources
                .iter()
                .find(|r| r.id == "pod:c:demo:many")
                .unwrap()
                .health,
            Health::Degraded
        );
    }

    #[cfg(feature = "k8s")]
    #[test]
    fn fixture_api_is_collected() {
        let view = fixture();
        let lists = std::collections::HashMap::from([
            ("/api/v1/nodes".into(), nodes_json(&view)),
            ("/api/v1/pods".into(), pods_json(&view)),
            (
                "/apis/apps/v1/deployments".into(),
                workloads_json(&view, "Deployment"),
            ),
            ("/apis/apps/v1/statefulsets".into(), json!({"items": []})),
            ("/apis/apps/v1/daemonsets".into(), json!({"items": []})),
            (
                "/api/v1/events".into(),
                json!({"items": [{
                    "metadata": {"name": "e1", "namespace": "demo"},
                    "involvedObject": {"kind": "Pod", "name": "bad", "namespace": "demo"},
                    "type": "Warning",
                    "reason": "BackOff",
                    "message": "back-off restarting"
                }]}),
            ),
        ]);
        let url = serve_fixture(lists);
        std::env::set_var("PULSEBLADE_TEST_K8S_TOKEN", "fixture-token");
        let live = LiveK8s::connect(&K8sTarget {
            api_url: url,
            token_env: "PULSEBLADE_TEST_K8S_TOKEN".into(),
            cluster: "demo".into(),
            verify_tls: true,
            ca: String::new(),
        })
        .unwrap();
        let mut collector = K8sCollector::new("demo", live);
        let obs = collector.collect().unwrap();
        assert!(obs.resources.iter().any(|r| r.id == "knode:demo:n1"));
        assert!(obs.resources.iter().any(|r| r.id == "pod:demo:demo:bad"));
        assert!(obs
            .resources
            .iter()
            .any(|r| r.id == "workload:demo:demo:deployment:api"));
        assert!(obs
            .logs
            .iter()
            .any(|l| l.resource_id == "pod:demo:demo:bad"));
        let store = pulseblade_store::Store::open_in_memory().unwrap();
        store.apply("k8s", chrono::Utc::now(), &obs).unwrap();
        let bad = store.resource("pod:demo:demo:bad").unwrap().unwrap();
        assert_eq!(bad.resource.health, Health::Failed);
        let wl = store
            .resource("workload:demo:demo:deployment:api")
            .unwrap()
            .unwrap();
        assert_eq!(wl.resource.kind, ResourceKind::Workload);
    }

    #[cfg(feature = "k8s")]
    fn nodes_json(view: &K8sView) -> Value {
        json!({
            "items": view.nodes.iter().map(|n| json!({
                "metadata": {"name": n.name},
                "status": {"conditions": [{"type": "Ready", "status": if n.ready { "True" } else { "False" }}]},
                "usage": {"cpu": n.cpu, "memory": n.mem}
            })).collect::<Vec<_>>()
        })
    }

    #[cfg(feature = "k8s")]
    fn pods_json(view: &K8sView) -> Value {
        json!({
            "items": view.pods.iter().map(|p| json!({
                "metadata": {"name": p.name, "namespace": p.namespace},
                "spec": {"nodeName": p.node},
                "status": {"phase": p.phase, "containerStatuses": [{"restartCount": p.restarts}]}
            })).collect::<Vec<_>>()
        })
    }

    #[cfg(feature = "k8s")]
    fn workloads_json(view: &K8sView, kind: &str) -> Value {
        json!({
            "items": view.workloads.iter().filter(|w| w.kind == kind).map(|w| json!({
                "metadata": {"name": w.name, "namespace": w.namespace},
                "spec": {"replicas": w.replicas},
                "status": {"readyReplicas": w.ready}
            })).collect::<Vec<_>>()
        })
    }
}
