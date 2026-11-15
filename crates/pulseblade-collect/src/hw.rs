//! Shared limits for the hardware collectors.
//!
//! Percents are of capacity. Disk and chassis temperatures are Celsius.
//! A degraded cutoff must sit below the failed cutoff.

use pulseblade_core::Health;

/// Cutoffs applied while mapping a hardware snapshot into health.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HwLimits {
    pub pool_used_degraded: f64,
    pub pool_used_failed: f64,
    pub disk_temp_degraded: f64,
    pub disk_temp_failed: f64,
    pub pdu_load_degraded: f64,
    pub pdu_load_failed: f64,
}

impl Default for HwLimits {
    fn default() -> Self {
        Self {
            pool_used_degraded: 85.0,
            pool_used_failed: 95.0,
            disk_temp_degraded: 50.0,
            disk_temp_failed: 60.0,
            pdu_load_degraded: 80.0,
            pdu_load_failed: 90.0,
        }
    }
}

/// One configured appliance. `secret_env` is an environment variable name.
#[derive(Debug, Clone, PartialEq)]
pub struct HwTarget {
    pub name: String,
    pub url: String,
    pub user_env: String,
    pub secret_env: String,
    pub verify_tls: bool,
    /// PDU rated watts. Zero means the load percent is not computed.
    pub rated_watts: f64,
    pub limits: HwLimits,
}

pub(crate) fn ident(raw: &str) -> Option<&str> {
    let s = raw.trim();
    if s.is_empty() || s.len() > 64 {
        return None;
    }
    if s.contains(['/', ' ', ':', '\\', '\n', '\0']) {
        return None;
    }
    Some(s)
}

pub(crate) fn worse(a: Health, b: Health) -> Health {
    match (a, b) {
        (Health::Failed, _) | (_, Health::Failed) => Health::Failed,
        (Health::Degraded, _) | (_, Health::Degraded) => Health::Degraded,
        _ => Health::Ok,
    }
}

pub(crate) fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

pub(crate) fn num(v: Option<&serde_json::Value>) -> Option<f64> {
    let v = v?;
    v.as_f64()
        .or_else(|| v.as_i64().map(|n| n as f64))
        .or_else(|| v.as_u64().map(|n| n as f64))
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}
