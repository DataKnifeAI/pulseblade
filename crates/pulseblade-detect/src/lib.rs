//! Detection: threshold rules and anomaly detection producing findings (M2).
//!
//! Health findings cover a failed unit, a full disk, or a runaway process.
//! Configured thresholds and an EWMA z-score add the rest. Each open finding
//! carries recent changes and error lines for the resource, its parent, and
//! its dependencies.

mod config;

use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Utc};
use pulseblade_core::{glob_match, Change, Finding, Health, LogLine, Resource, Severity};
use pulseblade_store::{FindingDraft, ResourceFilter, Store};

pub use config::{Compare, DetectConfig, ThresholdRule};

/// Exponentially weighted moving average and variance for streaming anomaly scoring.
#[derive(Debug, Clone, Copy)]
pub struct Ewma {
    alpha: f64,
    mean: Option<f64>,
    var: f64,
}

impl Ewma {
    /// `alpha` in `(0, 1]`; higher reacts faster to recent samples.
    pub fn new(alpha: f64) -> Self {
        Self {
            alpha: alpha.clamp(f64::EPSILON, 1.0),
            mean: None,
            var: 0.0,
        }
    }

    /// Feed a sample and return its z-score against the state before the update.
    pub fn observe(&mut self, x: f64) -> f64 {
        let Some(mean) = self.mean else {
            self.mean = Some(x);
            return 0.0;
        };
        let std = self.var.sqrt();
        let z = if std > f64::EPSILON {
            (x - mean) / std
        } else {
            0.0
        };
        let diff = x - mean;
        let incr = self.alpha * diff;
        self.mean = Some(mean + incr);
        self.var = (1.0 - self.alpha) * (self.var + diff * incr);
        z
    }

    pub fn mean(&self) -> Option<f64> {
        self.mean
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DetectError {
    #[error(transparent)]
    Store(#[from] pulseblade_store::StoreError),
}

const CONTEXT_CHANGES: usize = 8;
const CONTEXT_LOGS: usize = 5;
const EWMA_POINTS: usize = 120;

/// One condition that should be an open finding, before causal context is attached.
#[derive(Debug, Clone)]
struct Incident {
    rule: String,
    resource_id: String,
    severity: Severity,
    summary: String,
}

/// Compare the store to `cfg` and persist findings. Newly opened findings are returned.
///
/// A disabled config is a no-op: existing findings are not resolved.
pub fn sync(
    store: &Store,
    cfg: &DetectConfig,
    now: DateTime<Utc>,
) -> Result<Vec<Finding>, DetectError> {
    if !cfg.enabled {
        return Ok(Vec::new());
    }
    let resources = store.resources(&ResourceFilter::default())?;
    let latest = store.latest_all()?;
    let series = ewma_series(store, cfg, &latest)?;
    let incidents = incidents(cfg, &resources, &latest, &series);
    let mut drafts = Vec::with_capacity(incidents.len());
    for incident in incidents {
        let resource = resources
            .iter()
            .find(|r| r.resource.id == incident.resource_id);
        let ids = resource
            .map(|r| related_ids(&r.resource))
            .unwrap_or_else(|| vec![incident.resource_id.clone()]);
        let context = gather_changes(store, &ids)?;
        let logs = gather_logs(store, &ids)?;
        drafts.push(FindingDraft {
            dedupe_key: format!("{}\u{0}{}", incident.rule, incident.resource_id),
            resource_id: incident.resource_id,
            rule: incident.rule,
            severity: incident.severity,
            summary: incident.summary,
            context,
            logs,
        });
    }
    Ok(store.reconcile_findings(&drafts, now)?)
}

fn ewma_series(
    store: &Store,
    cfg: &DetectConfig,
    latest: &HashMap<String, BTreeMap<String, f64>>,
) -> Result<HashMap<(String, String), Vec<f64>>, DetectError> {
    let mut out = HashMap::new();
    if !cfg.ewma || cfg.ewma_metrics.is_empty() || cfg.ewma_z <= 0.0 {
        return Ok(out);
    }
    for (id, metrics) in latest {
        for metric in &cfg.ewma_metrics {
            if !metrics.contains_key(metric) {
                continue;
            }
            let values = store.recent_samples(id, metric, EWMA_POINTS)?;
            if values.len() >= cfg.ewma_min_points.max(2) {
                out.insert((id.clone(), metric.clone()), values);
            }
        }
    }
    Ok(out)
}

fn incidents(
    cfg: &DetectConfig,
    resources: &[pulseblade_core::StoredResource],
    latest: &HashMap<String, BTreeMap<String, f64>>,
    series: &HashMap<(String, String), Vec<f64>>,
) -> Vec<Incident> {
    let mut out = Vec::new();
    for stored in resources {
        let resource = &stored.resource;
        if resource.health == Health::Failed {
            out.push(health_incident(resource, Severity::Critical));
        } else if resource.health == Health::Degraded {
            out.push(health_incident(resource, Severity::Warning));
        }
        for rule in &cfg.rules {
            if !glob_match(&rule.pattern, &resource.id) {
                continue;
            }
            let Some(value) = latest
                .get(&resource.id)
                .and_then(|m| m.get(&rule.metric))
                .copied()
            else {
                continue;
            };
            if !rule.op.holds(value, rule.value) {
                continue;
            }
            out.push(Incident {
                rule: rule.rule_name(),
                resource_id: resource.id.clone(),
                severity: rule.severity,
                summary: format!(
                    "{} {} is {:.1} ({} {})",
                    resource.name,
                    rule.metric,
                    value,
                    rule.op.as_str(),
                    rule.value
                ),
            });
        }
    }
    if cfg.ewma && cfg.ewma_z > 0.0 {
        for ((id, metric), values) in series {
            let Some(z) = z_score(cfg.ewma_alpha, values) else {
                continue;
            };
            let value = *values.last().unwrap_or(&0.0);
            if z < cfg.ewma_z || value < ewma_floor(metric) {
                continue;
            }
            let severity = if z >= cfg.ewma_z * 1.5 {
                Severity::Critical
            } else {
                Severity::Warning
            };
            out.push(Incident {
                rule: format!("ewma:{metric}"),
                resource_id: id.clone(),
                severity,
                summary: format!("{metric} spiked to {value:.1} (z={z:.1})"),
            });
        }
    }
    out
}

fn health_incident(resource: &Resource, severity: Severity) -> Incident {
    let summary = match resource
        .attrs
        .get("unready")
        .and_then(|value| value.as_str())
        .filter(|why| !why.is_empty())
    {
        Some(why) => format!("{} is {}: {}", resource.name, resource.health.as_str(), why),
        None => format!("{} is {}", resource.name, resource.health.as_str()),
    };
    Incident {
        rule: "health".into(),
        resource_id: resource.id.clone(),
        severity,
        summary,
    }
}

/// Absolute floor so a tiny, low-variance series does not page on jitter.
/// `proc.cpu_pct` is not a default anomaly metric; the floor still applies
/// when a config opts that one-core sample back in.
fn ewma_floor(metric: &str) -> f64 {
    match metric {
        "cpu.used_pct" | "proc.cpu_pct" => 70.0,
        "mem.used_pct" | "disk.used_pct" => 80.0,
        "journal.errors" => 3.0,
        _ => 0.0,
    }
}

fn z_score(alpha: f64, values: &[f64]) -> Option<f64> {
    if values.len() < 2 {
        return None;
    }
    let mut ewma = Ewma::new(alpha);
    let mut z = 0.0;
    for value in values {
        if !value.is_finite() {
            return None;
        }
        z = ewma.observe(*value);
    }
    Some(z)
}

/// The resource, then its parent, then `requires` dependency ids.
pub fn related_ids(resource: &Resource) -> Vec<String> {
    let mut ids = vec![resource.id.clone()];
    if let Some(parent) = &resource.parent {
        if !ids.contains(parent) {
            ids.push(parent.clone());
        }
    }
    if let Some(serde_json::Value::Array(deps)) = resource.attrs.get("requires") {
        for dep in deps.iter().filter_map(|v| v.as_str()).take(8) {
            if !ids.iter().any(|id| id == dep) {
                ids.push(dep.to_string());
            }
        }
    }
    ids
}

fn gather_changes(store: &Store, ids: &[String]) -> Result<Vec<Change>, DetectError> {
    let mut out = Vec::new();
    for id in ids {
        if out.len() >= CONTEXT_CHANGES {
            break;
        }
        let more = store.recent_changes(std::slice::from_ref(id), CONTEXT_CHANGES - out.len())?;
        out.extend(more);
    }
    Ok(out)
}

fn gather_logs(store: &Store, ids: &[String]) -> Result<Vec<LogLine>, DetectError> {
    let mut out = Vec::new();
    for id in ids {
        if out.len() >= CONTEXT_LOGS {
            break;
        }
        let more = store.recent_logs(std::slice::from_ref(id), CONTEXT_LOGS - out.len())?;
        out.extend(more);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulseblade_core::{ChangeKind, Observation, ResourceKind, Sample};
    use pulseblade_store::Store;
    use serde_json::json;

    fn unit(name: &str, health: Health) -> Resource {
        Resource::new(format!("unit:h:{name}"), ResourceKind::Service, name)
            .parent("host:h")
            .health(health)
            .attr(
                "active",
                if health == Health::Failed {
                    "failed"
                } else {
                    "active"
                },
            )
            .attr("requires", json!(["unit:h:dep.service"]))
    }

    #[test]
    fn spike_scores_high_after_stable_series() {
        let mut e = Ewma::new(0.3);
        for i in 0..50 {
            e.observe(10.0 + (i % 2) as f64 * 0.5);
        }
        assert!(e.observe(30.0) > 5.0);
    }

    #[test]
    fn failed_unit_carries_the_preceding_change_and_error_lines() {
        let store = Store::open_in_memory().unwrap();
        let t0 = Utc::now() - chrono::Duration::minutes(5);
        store
            .apply(
                "systemd",
                t0,
                &Observation {
                    resources: vec![
                        unit("a.service", Health::Ok),
                        unit("dep.service", Health::Ok),
                        Resource::new("host:h", ResourceKind::Host, "h").health(Health::Ok),
                    ],
                    samples: vec![],
                    logs: vec![],
                },
            )
            .unwrap();
        let t1 = Utc::now();
        store
            .apply(
                "systemd",
                t1,
                &Observation {
                    resources: vec![
                        unit("a.service", Health::Failed),
                        unit("dep.service", Health::Ok),
                        Resource::new("host:h", ResourceKind::Host, "h").health(Health::Ok),
                    ],
                    samples: vec![],
                    logs: vec![LogLine {
                        ts: t1,
                        resource_id: "unit:h:a.service".into(),
                        message: "Failed to start OpenSSH".into(),
                    }],
                },
            )
            .unwrap();
        // The host resource above is owned by systemd in this fixture; that is enough
        // for parent context. User units use the same health rule.
        store
            .apply(
                "user-systemd",
                t1,
                &Observation {
                    resources: vec![Resource::new(
                        "unit:h:user:app.service",
                        ResourceKind::Service,
                        "app.service",
                    )
                    .parent("session:h")
                    .health(Health::Failed)],
                    samples: vec![],
                    logs: vec![LogLine {
                        ts: t1,
                        resource_id: "unit:h:user:app.service".into(),
                        message: "user unit exited".into(),
                    }],
                },
            )
            .unwrap();

        let opened = sync(&store, &DetectConfig::default(), Utc::now()).unwrap();
        let system = opened
            .iter()
            .find(|f| f.resource_id == "unit:h:a.service")
            .unwrap();
        assert_eq!(system.rule, "health");
        assert_eq!(system.severity, Severity::Critical);
        assert!(system.context.iter().any(|c| {
            c.resource_id == "unit:h:a.service"
                && c.field.as_deref() == Some("health")
                && c.kind == ChangeKind::Changed
        }));
        assert!(system.logs.iter().any(|l| l.message.contains("OpenSSH")));

        let user = opened
            .iter()
            .find(|f| f.resource_id == "unit:h:user:app.service")
            .unwrap();
        assert!(user.logs.iter().any(|l| l.message.contains("user unit")));

        let again = sync(&store, &DetectConfig::default(), Utc::now()).unwrap();
        assert!(again.is_empty());
    }

    #[test]
    fn full_disk_and_runaway_process_open_findings() {
        let store = Store::open_in_memory().unwrap();
        store
            .apply(
                "host",
                Utc::now(),
                &Observation {
                    resources: vec![Resource::new("disk:h:/", ResourceKind::Disk, "/")
                        .parent("host:h")
                        .health(Health::Failed)],
                    samples: vec![Sample::new("disk:h:/", "disk.used_pct", 99.0)],
                    logs: vec![],
                },
            )
            .unwrap();
        store
            .apply(
                "processes",
                Utc::now(),
                &Observation {
                    resources: vec![Resource::new("proc:h:9", ResourceKind::Process, "runaway")
                        .parent("host:h")
                        .health(Health::Failed)],
                    samples: vec![Sample::new("proc:h:9", "proc.cpu_pct", 99.0)],
                    logs: vec![],
                },
            )
            .unwrap();
        let mut cfg = DetectConfig {
            ewma: false,
            ..DetectConfig::default()
        };
        cfg.rules.push(ThresholdRule {
            name: "disk-full".into(),
            pattern: "disk:*".into(),
            metric: "disk.used_pct".into(),
            op: Compare::Gte,
            value: 90.0,
            severity: Severity::Critical,
        });
        let opened = sync(&store, &cfg, Utc::now()).unwrap();
        assert!(opened
            .iter()
            .any(|f| f.resource_id == "disk:h:/" && f.rule == "health"));
        assert!(opened
            .iter()
            .any(|f| f.resource_id == "disk:h:/" && f.rule == "disk-full"));
        assert!(opened
            .iter()
            .any(|f| f.resource_id == "proc:h:9" && f.rule == "health"));
    }

    #[test]
    fn ewma_ignores_low_jitter_and_flags_a_high_spike() {
        let store = Store::open_in_memory().unwrap();
        let start = Utc::now() - chrono::Duration::minutes(30);
        for i in 0..25 {
            let value = if i == 24 {
                95.0
            } else {
                10.0 + (i % 2) as f64 * 0.2
            };
            store
                .apply(
                    "host",
                    start + chrono::Duration::seconds(i * 15),
                    &Observation {
                        resources: vec![
                            Resource::new("host:h", ResourceKind::Host, "h").health(Health::Ok)
                        ],
                        samples: vec![Sample::new("host:h", "cpu.used_pct", value)],
                        logs: vec![],
                    },
                )
                .unwrap();
        }
        let cfg = DetectConfig {
            ewma_min_points: 20,
            ..DetectConfig::default()
        };
        let opened = sync(&store, &cfg, Utc::now()).unwrap();
        assert!(opened.iter().any(|f| f.rule == "ewma:cpu.used_pct"));

        let quiet = Store::open_in_memory().unwrap();
        for i in 0..25 {
            quiet
                .apply(
                    "host",
                    start + chrono::Duration::seconds(i * 15),
                    &Observation {
                        resources: vec![
                            Resource::new("host:h", ResourceKind::Host, "h").health(Health::Ok)
                        ],
                        samples: vec![Sample::new("host:h", "cpu.used_pct", 12.0)],
                        logs: vec![],
                    },
                )
                .unwrap();
        }
        assert!(sync(&quiet, &cfg, Utc::now()).unwrap().is_empty());
    }

    #[test]
    fn default_anomalies_ignore_one_core_process_cpu() {
        assert!(!DetectConfig::default()
            .ewma_metrics
            .iter()
            .any(|m| m == "proc.cpu_pct"));
        assert!(DetectConfig::default()
            .ewma_metrics
            .iter()
            .any(|m| m == "cpu.used_pct"));

        let store = Store::open_in_memory().unwrap();
        let start = Utc::now() - chrono::Duration::minutes(30);
        for i in 0..25 {
            let value = if i == 24 {
                400.0
            } else {
                10.0 + (i % 2) as f64 * 0.2
            };
            store
                .apply(
                    "processes",
                    start + chrono::Duration::seconds(i * 15),
                    &Observation {
                        resources: vec![Resource::new("proc:h:9", ResourceKind::Process, "game")
                            .parent("host:h")
                            .health(Health::Ok)],
                        samples: vec![Sample::new("proc:h:9", "proc.cpu_pct", value)],
                        logs: vec![],
                    },
                )
                .unwrap();
        }
        assert!(sync(&store, &DetectConfig::default(), Utc::now())
            .unwrap()
            .is_empty());

        let opted = DetectConfig {
            ewma_metrics: vec!["proc.cpu_pct".into()],
            ewma_min_points: 20,
            ..DetectConfig::default()
        };
        let flagged = sync(&store, &opted, Utc::now()).unwrap();
        assert!(flagged.iter().any(|f| f.rule == "ewma:proc.cpu_pct"));
    }

    #[test]
    fn disabled_config_does_not_resolve() {
        let store = Store::open_in_memory().unwrap();
        store
            .apply(
                "host",
                Utc::now(),
                &Observation {
                    resources: vec![
                        Resource::new("disk:h:/", ResourceKind::Disk, "/").health(Health::Failed)
                    ],
                    samples: vec![],
                    logs: vec![],
                },
            )
            .unwrap();
        let opened = sync(&store, &DetectConfig::default(), Utc::now()).unwrap();
        assert_eq!(opened.len(), 1);
        let off = DetectConfig {
            enabled: false,
            ..DetectConfig::default()
        };
        assert!(sync(&store, &off, Utc::now()).unwrap().is_empty());
        assert_eq!(store.open_finding_count().unwrap(), 1);
    }
}
