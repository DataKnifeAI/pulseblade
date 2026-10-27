//! Health cutoffs shared by collectors, config, and the portal.
//!
//! Missing keys keep these defaults, so an older config file still loads.
//! Unknown keys are rejected.

use serde::{Deserialize, Serialize};

fn default_window() -> u64 {
    15
}

fn default_lifetime() -> u64 {
    100
}

fn default_cpu() -> f64 {
    90.0
}

fn default_mem_degraded() -> f64 {
    25.0
}

fn default_mem_failed() -> f64 {
    50.0
}

/// IANA zone for dashboard clocks. October is PDT; winter is PST.
pub fn default_timezone() -> String {
    "America/Los_Angeles".into()
}

/// A timezone string the dashboard can pass to `Intl`. Not a full zone database.
pub fn valid_timezone(tz: &str) -> bool {
    !tz.is_empty()
        && tz.len() <= 64
        && !tz.starts_with('/')
        && !tz.ends_with('/')
        && !tz.contains("//")
        && tz
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '/' | '+' | '-'))
}

/// Dashboard clock format. `12h` is 12-hour with AM/PM; `24h` is 24-hour.
pub fn default_time_format() -> String {
    "12h".into()
}

/// `12h` or `24h`. Any other string is rejected when the config is loaded.
pub fn valid_time_format(fmt: &str) -> bool {
    matches!(fmt, "12h" | "24h")
}

/// Cutoffs the node uses when it scores a pod or a process.
///
/// The portal may edit this document. It cannot carry action tiers, allow-lists,
/// or `script.run`: those fields are not on this struct, and unknown keys fail.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct HealthThresholds {
    /// Minutes. A Running or Succeeded pod is degraded when its newest restart
    /// is inside this window.
    #[serde(default = "default_window")]
    pub pod_restart_window_mins: u64,
    /// Lifetime container restart sum at or above this degrades a Running or
    /// Succeeded pod, even when the newest restart is older than the window.
    #[serde(default = "default_lifetime")]
    pub pod_restart_lifetime: u64,
    /// Host CPU share at or above this fails a process. 100 is the whole machine.
    #[serde(default = "default_cpu")]
    pub proc_cpu_share_failed: f64,
    /// Process memory, as a percent of host RAM, that degrades the process.
    #[serde(default = "default_mem_degraded")]
    pub proc_mem_degraded: f64,
    /// Process memory, as a percent of host RAM, that fails the process.
    #[serde(default = "default_mem_failed")]
    pub proc_mem_failed: f64,
}

impl Default for HealthThresholds {
    fn default() -> Self {
        Self {
            pod_restart_window_mins: default_window(),
            pod_restart_lifetime: default_lifetime(),
            proc_cpu_share_failed: default_cpu(),
            proc_mem_degraded: default_mem_degraded(),
            proc_mem_failed: default_mem_failed(),
        }
    }
}

impl HealthThresholds {
    /// Parse a portal or hub document. Omitted keys are the compiled defaults.
    pub fn from_value(value: &serde_json::Value) -> Result<Self, String> {
        let doc: Self = serde_json::from_value(value.clone()).map_err(|e| e.to_string())?;
        doc.validate()?;
        Ok(doc)
    }

    pub fn validate(&self) -> Result<(), String> {
        if !(1..=10_080).contains(&self.pod_restart_window_mins) {
            return Err("pod_restart_window_mins must be from 1 to 10080".into());
        }
        if !(1..=1_000_000).contains(&self.pod_restart_lifetime) {
            return Err("pod_restart_lifetime must be from 1 to 1000000".into());
        }
        if !self.proc_cpu_share_failed.is_finite()
            || self.proc_cpu_share_failed <= 0.0
            || self.proc_cpu_share_failed > 100.0
        {
            return Err("proc_cpu_share_failed must be greater than 0 and at most 100".into());
        }
        if !self.proc_mem_degraded.is_finite()
            || !self.proc_mem_failed.is_finite()
            || self.proc_mem_degraded <= 0.0
            || self.proc_mem_failed > 100.0
            || self.proc_mem_degraded >= self.proc_mem_failed
        {
            return Err(
                "proc_mem_degraded must be greater than 0 and less than proc_mem_failed, which is at most 100"
                    .into(),
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn defaults_match_the_shipped_cutoffs() {
        let d = HealthThresholds::default();
        assert_eq!(d.pod_restart_window_mins, 15);
        assert_eq!(d.pod_restart_lifetime, 100);
        assert_eq!(d.proc_cpu_share_failed, 90.0);
        assert_eq!(d.proc_mem_degraded, 25.0);
        assert_eq!(d.proc_mem_failed, 50.0);
        assert_eq!(default_timezone(), "America/Los_Angeles");
        assert!(valid_timezone(&default_timezone()));
        assert_eq!(default_time_format(), "12h");
        assert!(valid_time_format(&default_time_format()));
    }

    #[test]
    fn time_format_accepts_only_12h_and_24h() {
        assert!(valid_time_format("12h"));
        assert!(valid_time_format("24h"));
        assert!(!valid_time_format("12H"));
        assert!(!valid_time_format("24H"));
        assert!(!valid_time_format(""));
        assert!(!valid_time_format("hours"));
    }

    #[test]
    fn omitted_keys_keep_defaults_and_policy_keys_are_rejected() {
        let doc = HealthThresholds::from_value(&json!({"pod_restart_window_mins": 30})).unwrap();
        assert_eq!(doc.pod_restart_window_mins, 30);
        assert_eq!(doc.pod_restart_lifetime, 100);
        assert!(HealthThresholds::from_value(&json!({"tier": "auto"})).is_err());
        assert!(
            HealthThresholds::from_value(&json!({"script_run": {"allow": ["/bin/true"]}})).is_err()
        );
        assert!(HealthThresholds::from_value(
            &json!({"actions": {"script_run": {"tier": "auto"}}})
        )
        .is_err());
    }
}
