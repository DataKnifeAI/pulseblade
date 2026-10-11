//! Collectors that observe infrastructure and report resources and samples.
//!
//! Each collector returns the complete set of resources it owns on every pass;
//! the store turns successive passes into a change journal.

mod docker;
mod host;
mod journal;
mod k8s;
mod processes;
mod prometheus;
mod proxmox;
mod systemd;
mod user;

pub use docker::{observe_docker, DockerCollector, DockerSource, DockerView};
pub use k8s::{observe_k8s, K8sCollector, K8sSource, K8sTarget, K8sView};
pub use prometheus::{observe_prom, PromCollector, PromSource, PromTarget};
pub use proxmox::{observe_proxmox, ProxmoxCollector, ProxmoxSource, ProxmoxTarget, PveView};

pub use host::HostCollector;
pub use journal::JournaldCollector;
pub use processes::ProcessCollector;
pub use systemd::SystemdCollector;
pub use user::UserCollector;

use std::process::Command;

use pulseblade_core::Observation;

#[derive(Debug, thiserror::Error)]
pub enum CollectError {
    #[error("{0}: {1}")]
    Command(&'static str, String),
    #[error("{0}")]
    Unavailable(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

pub(crate) fn run_command(bin: &'static str, args: &[&str]) -> Result<Vec<u8>, CollectError> {
    let out = Command::new(bin).args(args).output()?;
    if !out.status.success() {
        let err = String::from_utf8_lossy(&out.stderr).trim().to_string();
        let err = if err.is_empty() {
            format!("exit {}", out.status)
        } else {
            err
        };
        return Err(CollectError::Command(bin, err));
    }
    Ok(out.stdout)
}

pub trait Collector: Send {
    /// Stable name; also the `source` that owns this collector's resources.
    fn name(&self) -> &'static str;

    fn collect(&mut self) -> Result<Observation, CollectError>;
}

/// The local hostname, used to namespace resource ids.
pub fn hostname() -> String {
    sysinfo::System::host_name().unwrap_or_else(|| "localhost".to_string())
}

/// Which optional collectors to attempt. A missing daemon or API is skipped
/// or recorded as a failed pass; it does not stop the others.
#[derive(Debug, Clone)]
pub struct CollectorOpts {
    pub docker: bool,
    pub proxmox: Option<ProxmoxTarget>,
    pub k8s: Option<K8sTarget>,
    pub prometheus: Vec<PromTarget>,
}

impl Default for CollectorOpts {
    fn default() -> Self {
        Self {
            docker: true,
            proxmox: None,
            k8s: None,
            prometheus: Vec::new(),
        }
    }
}

/// Collectors that make sense on this machine.
pub fn default_collectors(opts: &CollectorOpts) -> Vec<Box<dyn Collector>> {
    let host = hostname();
    let mut out: Vec<Box<dyn Collector>> = vec![Box::new(HostCollector::new(host.clone()))];
    if SystemdCollector::available() {
        out.push(Box::new(SystemdCollector::new(host.clone())));
    } else {
        tracing::info!("systemctl not found; systemd collector disabled");
    }
    if UserCollector::available() {
        out.push(Box::new(UserCollector::new(host.clone())));
    } else {
        tracing::info!("user systemd bus unavailable; user collector disabled");
    }
    if JournaldCollector::available() {
        out.push(Box::new(JournaldCollector::new(host.clone())));
    } else {
        tracing::info!("journalctl not found; journald collector disabled");
    }
    out.push(Box::new(ProcessCollector::new(host.clone())));
    push_docker(&mut out, &host, opts.docker);
    push_proxmox(&mut out, opts.proxmox.as_ref());
    push_k8s(&mut out, opts.k8s.as_ref());
    push_prometheus(&mut out, &host, &opts.prometheus);
    out
}

fn push_docker(out: &mut Vec<Box<dyn Collector>>, host: &str, enabled: bool) {
    if !enabled {
        return;
    }
    #[cfg(feature = "docker")]
    {
        match docker::LiveDocker::connect() {
            Ok(src) => out.push(Box::new(DockerCollector::new(host, src))),
            Err(e) => {
                tracing::info!(error = %e, "docker daemon unavailable; docker collector disabled")
            }
        }
    }
    #[cfg(not(feature = "docker"))]
    {
        let _ = (out, host);
        tracing::info!("docker collector not compiled");
    }
}

fn push_proxmox(out: &mut Vec<Box<dyn Collector>>, target: Option<&ProxmoxTarget>) {
    let Some(target) = target else {
        return;
    };
    if target.api_url.trim().is_empty() {
        return;
    }
    #[cfg(feature = "proxmox")]
    {
        out.push(Box::new(ProxmoxCollector::new(proxmox::LiveProxmox::new(
            target.clone(),
        ))));
    }
    #[cfg(not(feature = "proxmox"))]
    {
        let _ = out;
        tracing::info!("proxmox collector not compiled");
    }
}

fn push_k8s(out: &mut Vec<Box<dyn Collector>>, target: Option<&K8sTarget>) {
    let Some(target) = target else {
        return;
    };
    if target.api_url.trim().is_empty() {
        return;
    }
    #[cfg(feature = "k8s")]
    {
        match k8s::LiveK8s::connect(target) {
            Ok(src) => {
                let cluster = src.cluster().to_string();
                out.push(Box::new(K8sCollector::new(cluster, src)));
            }
            Err(e) => {
                tracing::info!(error = %e, "kubernetes API unavailable; k8s collector disabled")
            }
        }
    }
    #[cfg(not(feature = "k8s"))]
    {
        let _ = out;
        tracing::info!("k8s collector not compiled");
    }
}

fn push_prometheus(out: &mut Vec<Box<dyn Collector>>, host: &str, targets: &[PromTarget]) {
    let targets: Vec<PromTarget> = targets
        .iter()
        .filter(|t| !t.url.trim().is_empty() && !t.name.trim().is_empty())
        .cloned()
        .collect();
    if targets.is_empty() {
        return;
    }
    #[cfg(any(feature = "k8s", feature = "proxmox"))]
    {
        out.push(Box::new(PromCollector::new(
            host,
            targets,
            prometheus::LiveProm::new(),
        )));
    }
    #[cfg(not(any(feature = "k8s", feature = "proxmox")))]
    {
        let _ = (out, host, targets);
        tracing::info!("prometheus scrape not compiled");
    }
}

#[cfg(feature = "proxmox")]
pub(crate) fn http_agent(verify_tls: bool) -> Result<ureq::Agent, String> {
    if verify_tls {
        return Ok(ureq::Agent::new());
    }
    let tls = native_tls::TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .danger_accept_invalid_hostnames(true)
        .build()
        .map_err(|e| e.to_string())?;
    Ok(ureq::AgentBuilder::new()
        .tls_connector(std::sync::Arc::new(tls))
        .build())
}
