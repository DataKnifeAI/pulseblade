//! Kubernetes nodes, workloads, pods, and warning events.
//!
//! Ids: `knode:<cluster>:<name>`, `workload:<cluster>:<ns>:<kind>:<name>`,
//! `pod:<cluster>:<ns>:<name>`. The live client is `kube`. An empty API URL
//! leaves the collector off; a configured API that is down fails the pass.

#[cfg(all(test, feature = "k8s"))]
use std::io::{Read, Write};
#[cfg(all(test, feature = "k8s"))]
use std::net::TcpListener;

use pulseblade_core::{Health, LogLine, Observation, Resource, ResourceKind, Sample};
#[cfg(test)]
use serde_json::Value;

use crate::{CollectError, Collector};

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
}

#[derive(Debug, Clone, PartialEq)]
pub struct K8sPod {
    pub namespace: String,
    pub name: String,
    pub phase: String,
    pub node: String,
    pub restarts: u64,
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
}

impl<S> K8sCollector<S> {
    pub fn new(cluster: impl Into<String>, source: S) -> Self {
        Self {
            cluster: cluster.into(),
            source,
        }
    }
}

impl<S: K8sSource> Collector for K8sCollector<S> {
    fn name(&self) -> &'static str {
        "k8s"
    }

    fn collect(&mut self) -> Result<Observation, CollectError> {
        let view = self.source.snapshot()?;
        Ok(observe_k8s(&self.cluster, &view))
    }
}

pub fn observe_k8s(cluster: &str, view: &K8sView) -> Observation {
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
        obs.resources.push(
            Resource::new(&id, ResourceKind::Workload, name)
                .health(health)
                .label("cluster", cluster)
                .label("namespace", ns)
                .attr("kind", kind.clone())
                .attr("ready", format!("{}/{}", wl.ready, wl.replicas)),
        );
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
        let phase = pod.phase.to_ascii_lowercase();
        let health = if phase == "failed" || phase == "unknown" {
            Health::Failed
        } else if phase == "pending" || pod.restarts >= 5 {
            Health::Degraded
        } else if phase == "running" || phase == "succeeded" {
            Health::Ok
        } else {
            Health::Unknown
        };
        let mut resource = Resource::new(&id, ResourceKind::Pod, name)
            .health(health)
            .label("cluster", cluster)
            .label("namespace", ns)
            .attr("phase", pod.phase.clone())
            .attr("namespace", ns);
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
            });
        }
    }
    for item in items(pods) {
        let name = str_at(item, &["metadata", "name"]);
        let namespace = str_at(item, &["metadata", "namespace"]);
        if name.is_empty() {
            continue;
        }
        let restarts = item
            .pointer("/status/containerStatuses")
            .and_then(|v| v.as_array())
            .map(|cs| {
                cs.iter()
                    .map(|c| c.get("restartCount").and_then(|n| n.as_u64()).unwrap_or(0))
                    .sum()
            })
            .unwrap_or(0);
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
        if let Some(wl) = workload_of(
            "Deployment",
            item.metadata.name,
            item.metadata.namespace,
            item.spec.and_then(|s| s.replicas),
            item.status.and_then(|s| s.ready_replicas),
        ) {
            view.workloads.push(wl);
        }
    }
    for item in statefulsets {
        if let Some(wl) = workload_of(
            "StatefulSet",
            item.metadata.name,
            item.metadata.namespace,
            item.spec.and_then(|s| s.replicas),
            item.status.and_then(|s| s.ready_replicas),
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
        view.workloads.push(K8sWorkload {
            kind: "DaemonSet".into(),
            namespace: item.metadata.namespace.unwrap_or_else(|| "default".into()),
            name,
            replicas,
            ready,
        });
    }
    for pod in pods {
        let Some(name) = pod.metadata.name else {
            continue;
        };
        let restarts = pod
            .status
            .as_ref()
            .and_then(|s| s.container_statuses.as_ref())
            .map(|cs| cs.iter().map(|c| c.restart_count.max(0) as u64).sum())
            .unwrap_or(0);
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
) -> Option<K8sWorkload> {
    let name = name?;
    Some(K8sWorkload {
        kind: kind.into(),
        namespace: namespace.unwrap_or_else(|| "default".into()),
        name,
        replicas: replicas.unwrap_or(0).max(0) as u64,
        ready: ready.unwrap_or(0).max(0) as u64,
    })
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
        let obs = observe_k8s("demo", &fixture());
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
