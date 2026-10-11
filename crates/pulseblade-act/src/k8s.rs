//! Kubernetes rollout restart and scale. Default tier is approve, so planning
//! and execute do not call the API; the node calls it only after a human approves.

use crate::ActError;

/// API coordinates. The token secret is the name of an environment variable.
#[derive(Debug, Clone)]
pub struct K8sEndpoint {
    pub api_url: String,
    pub token_env: String,
    pub cluster: String,
    pub verify_tls: bool,
    /// PEM CA bundle. Empty uses the default trust store.
    pub ca: String,
}

impl Default for K8sEndpoint {
    fn default() -> Self {
        Self {
            api_url: String::new(),
            token_env: String::new(),
            cluster: String::new(),
            verify_tls: true,
            ca: String::new(),
        }
    }
}

pub trait K8sApi: Send + Sync {
    fn rollout_restart(&self, kind: &str, namespace: &str, name: &str) -> Result<(), String>;
    fn scale(&self, kind: &str, namespace: &str, name: &str, replicas: u32) -> Result<(), String>;
}

/// Used when `[k8s]` is empty or the client could not be built.
pub struct IdleK8s {
    pub message: String,
}

impl K8sApi for IdleK8s {
    fn rollout_restart(&self, _: &str, _: &str, _: &str) -> Result<(), String> {
        Err(self.message.clone())
    }

    fn scale(&self, _: &str, _: &str, _: &str, _: u32) -> Result<(), String> {
        Err(self.message.clone())
    }
}

pub fn is_k8s(name: &str) -> bool {
    matches!(name, "k8s.rollout_restart" | "k8s.scale")
}

pub fn description(name: &str) -> &'static str {
    match name {
        "k8s.rollout_restart" => {
            "Restart a Deployment, StatefulSet, or DaemonSet. Waits for approval by default."
        }
        "k8s.scale" => {
            "Set replicas on a Deployment or StatefulSet. Waits for approval by default."
        }
        _ => "",
    }
}

pub fn schema(name: &str) -> serde_json::Value {
    let kind = serde_json::json!({
        "type": "string",
        "enum": ["deployment", "statefulset", "daemonset"]
    });
    match name {
        "k8s.rollout_restart" => serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["namespace", "name"],
            "properties": {
                "namespace": {"type": "string", "minLength": 1, "maxLength": 63},
                "name": {"type": "string", "minLength": 1, "maxLength": 253},
                "kind": kind
            }
        }),
        "k8s.scale" => serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["namespace", "name", "replicas"],
            "properties": {
                "namespace": {"type": "string", "minLength": 1, "maxLength": 63},
                "name": {"type": "string", "minLength": 1, "maxLength": 253},
                "kind": {"type": "string", "enum": ["deployment", "statefulset"]},
                "replicas": {"type": "integer", "minimum": 0, "maximum": 1000}
            }
        }),
        _ => serde_json::json!({"type": "object"}),
    }
}

pub fn preconditions(name: &str) -> &'static [&'static str] {
    match name {
        "k8s.rollout_restart" => &[
            "namespace and name are single DNS labels (no slashes)",
            "kind is deployment, statefulset, or daemonset",
            "the node calls the API only after approval",
        ],
        "k8s.scale" => &[
            "namespace and name are single DNS labels (no slashes)",
            "kind is deployment or statefulset",
            "replicas is between 0 and 1000",
            "the node calls the API only after approval",
        ],
        _ => &[],
    }
}

pub fn ensure_configured(endpoint: &K8sEndpoint) -> Result<(), ActError> {
    if endpoint.api_url.trim().is_empty() || endpoint.token_env.trim().is_empty() {
        return Err(ActError::Invalid(
            "kubernetes API is not configured (api_url, token_env)".into(),
        ));
    }
    Ok(())
}

pub fn resource_id(
    name: &str,
    params: &serde_json::Value,
    cluster: &str,
) -> Result<String, ActError> {
    let namespace = label(
        params
            .get("namespace")
            .and_then(|v| v.as_str())
            .unwrap_or(""),
    )?;
    let workload = label(params.get("name").and_then(|v| v.as_str()).unwrap_or(""))?;
    let kind = kind_of(name, params.get("kind").and_then(|v| v.as_str()))?;
    if name == "k8s.scale" && kind == "daemonset" {
        return Err(ActError::Invalid(
            "k8s.scale does not apply to a daemonset".into(),
        ));
    }
    let cluster = if cluster.trim().is_empty() {
        "cluster"
    } else {
        cluster.trim()
    };
    if cluster.contains(':') || cluster.contains('/') {
        return Err(ActError::Invalid(
            "k8s cluster name cannot contain ':' or '/'".into(),
        ));
    }
    Ok(format!("workload:{cluster}:{namespace}:{kind}:{workload}"))
}

pub fn perform(name: &str, params: &serde_json::Value, api: &dyn K8sApi) -> Result<String, String> {
    let namespace = label(
        params
            .get("namespace")
            .and_then(|v| v.as_str())
            .unwrap_or(""),
    )
    .map_err(|e| e.to_string())?;
    let workload = label(params.get("name").and_then(|v| v.as_str()).unwrap_or(""))
        .map_err(|e| e.to_string())?;
    let kind =
        kind_of(name, params.get("kind").and_then(|v| v.as_str())).map_err(|e| e.to_string())?;
    match name {
        "k8s.rollout_restart" => {
            api.rollout_restart(&kind, &namespace, &workload)?;
            Ok(format!("restarted {kind}/{namespace}/{workload}"))
        }
        "k8s.scale" => {
            let replicas = params
                .get("replicas")
                .and_then(|v| v.as_u64())
                .ok_or("replicas is required")? as u32;
            api.scale(&kind, &namespace, &workload, replicas)?;
            Ok(format!(
                "scaled {kind}/{namespace}/{workload} to {replicas}"
            ))
        }
        other => Err(format!("unknown action `{other}`")),
    }
}

fn label(raw: &str) -> Result<String, ActError> {
    let s = raw.trim();
    if s.is_empty()
        || s.len() > 253
        || s.contains(':')
        || s.contains('/')
        || s.contains(' ')
        || !s
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '.')
    {
        return Err(ActError::Invalid(
            "kubernetes name must be a DNS label without ':' or '/'".into(),
        ));
    }
    Ok(s.to_string())
}

fn kind_of(action: &str, raw: Option<&str>) -> Result<String, ActError> {
    let kind = raw.unwrap_or("deployment").to_ascii_lowercase();
    let ok = match action {
        "k8s.scale" => matches!(kind.as_str(), "deployment" | "statefulset"),
        _ => matches!(kind.as_str(), "deployment" | "statefulset" | "daemonset"),
    };
    if ok {
        Ok(kind)
    } else {
        Err(ActError::Invalid(format!(
            "unsupported kubernetes kind `{kind}`"
        )))
    }
}

#[cfg(feature = "k8s")]
pub struct KubeApi {
    rt: tokio::runtime::Runtime,
    client: kube::Client,
}

#[cfg(feature = "k8s")]
impl KubeApi {
    pub fn connect(endpoint: &K8sEndpoint) -> Result<Self, String> {
        let token = std::env::var(&endpoint.token_env)
            .map_err(|_| format!("environment variable {} is not set", endpoint.token_env))?;
        if token.is_empty() {
            return Err(format!(
                "environment variable {} is empty",
                endpoint.token_env
            ));
        }
        let uri: http::Uri = endpoint
            .api_url
            .parse()
            .map_err(|e| format!("kubernetes API url: {e}"))?;
        let _ = rustls::crypto::ring::default_provider().install_default();
        let mut cfg = kube::Config::new(uri);
        cfg.accept_invalid_certs = !endpoint.verify_tls;
        cfg.auth_info.token = Some(token.into());
        if !endpoint.ca.trim().is_empty() {
            cfg.root_cert_file = Some(std::path::PathBuf::from(endpoint.ca.trim()));
        }
        cfg.connect_timeout = Some(std::time::Duration::from_secs(5));
        cfg.read_timeout = Some(std::time::Duration::from_secs(10));
        cfg.default_retry = false;
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        let client = rt
            .block_on(async move { kube::Client::try_from(cfg) })
            .map_err(|e| format!("kubernetes client: {e}"))?;
        Ok(Self { rt, client })
    }

    fn call<T: Send + 'static>(
        &self,
        fut: impl std::future::Future<Output = T> + Send + 'static,
    ) -> Result<T, String> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.rt.spawn(async move {
            let _ = tx.send(fut.await);
        });
        rx.recv().map_err(|e| e.to_string())
    }
}

#[cfg(feature = "k8s")]
impl K8sApi for KubeApi {
    fn rollout_restart(&self, kind: &str, namespace: &str, name: &str) -> Result<(), String> {
        let client = self.client.clone();
        let kind = kind.to_string();
        let namespace = namespace.to_string();
        let name = name.to_string();
        self.call(async move {
            let ts = chrono::Utc::now().to_rfc3339();
            let body = serde_json::json!({
                "spec": {"template": {"metadata": {"annotations": {"pulseblade/restartedAt": ts}}}}
            });
            patch(&client, &kind, &namespace, &name, body).await
        })?
    }

    fn scale(&self, kind: &str, namespace: &str, name: &str, replicas: u32) -> Result<(), String> {
        let client = self.client.clone();
        let kind = kind.to_string();
        let namespace = namespace.to_string();
        let name = name.to_string();
        self.call(async move {
            let body = serde_json::json!({"spec": {"replicas": replicas}});
            patch(&client, &kind, &namespace, &name, body).await
        })?
    }
}

#[cfg(feature = "k8s")]
async fn patch(
    client: &kube::Client,
    kind: &str,
    namespace: &str,
    name: &str,
    body: serde_json::Value,
) -> Result<(), String> {
    use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, StatefulSet};
    use kube::api::{Api, Patch, PatchParams};

    let params = PatchParams::default();
    let patch = Patch::Strategic(&body);
    let result = match kind {
        "deployment" => Api::<Deployment>::namespaced(client.clone(), namespace)
            .patch(name, &params, &patch)
            .await
            .map(|_| ()),
        "statefulset" => Api::<StatefulSet>::namespaced(client.clone(), namespace)
            .patch(name, &params, &patch)
            .await
            .map(|_| ()),
        "daemonset" => Api::<DaemonSet>::namespaced(client.clone(), namespace)
            .patch(name, &params, &patch)
            .await
            .map(|_| ()),
        other => return Err(format!("unsupported kubernetes kind `{other}`")),
    };
    result.map_err(|e| clip(&e.to_string()))
}

#[cfg(feature = "k8s")]
fn clip(s: &str) -> String {
    if s.chars().count() <= 500 {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(500).collect();
        out.push('…');
        out
    }
}

#[cfg(all(test, feature = "k8s"))]
pub fn serve_patch(hits: std::sync::Arc<std::sync::Mutex<Vec<String>>>) -> String {
    use std::io::{Read, Write};
    use std::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for _ in 0..8 {
            let Ok((mut sock, _)) = listener.accept() else {
                break;
            };
            let _ = sock.set_read_timeout(Some(std::time::Duration::from_secs(2)));
            let mut buf = Vec::new();
            let mut tmp = [0u8; 4096];
            let header_end = loop {
                match sock.read(&mut tmp) {
                    Ok(0) | Err(_) => break None,
                    Ok(n) => {
                        buf.extend_from_slice(&tmp[..n]);
                        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break Some(i);
                        }
                    }
                }
            };
            let Some(header_end) = header_end else {
                continue;
            };
            let header = String::from_utf8_lossy(&buf[..header_end]).to_string();
            let path = header
                .split_whitespace()
                .nth(1)
                .unwrap_or("/")
                .split('?')
                .next()
                .unwrap_or("/")
                .to_string();
            hits.lock().unwrap().push(path);
            let body = r#"{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"api","namespace":"demo"}}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = sock.write_all(resp.as_bytes());
        }
    });
    format!("http://{addr}")
}
