//! Collectors that observe infrastructure and report resources and samples.
//!
//! Each collector returns the complete set of resources it owns on every pass;
//! the store turns successive passes into a change journal.

mod host;
mod systemd;

pub use host::HostCollector;
pub use systemd::SystemdCollector;

use pulseblade_core::Observation;

#[derive(Debug, thiserror::Error)]
pub enum CollectError {
    #[error("{0}: {1}")]
    Command(&'static str, String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
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

/// Collectors that make sense on this machine.
pub fn default_collectors() -> Vec<Box<dyn Collector>> {
    let host = hostname();
    let mut out: Vec<Box<dyn Collector>> = vec![Box::new(HostCollector::new(host.clone()))];
    if SystemdCollector::available() {
        out.push(Box::new(SystemdCollector::new(host)));
    } else {
        tracing::info!("systemctl not found; systemd collector disabled");
    }
    out
}
