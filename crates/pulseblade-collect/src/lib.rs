//! Collectors that observe infrastructure and report resources and samples.
//!
//! Each collector returns the complete set of resources it owns on every pass;
//! the store turns successive passes into a change journal.

mod host;
mod journal;
mod processes;
mod systemd;
mod user;

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

/// Collectors that make sense on this machine.
pub fn default_collectors() -> Vec<Box<dyn Collector>> {
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
    out.push(Box::new(ProcessCollector::new(host)));
    out
}
