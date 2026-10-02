//! Prometheus text exposition (format 0.0.4) of collector health, resource
//! health, and the latest value of every metric.
//!
//! `pulseblade_resource_health` encodes health as 0 ok, 1 unknown, 2 degraded, 3 failed.

use std::collections::HashMap;
use std::fmt::Write as _;

use pulseblade_core::{Health, StoredResource};
use pulseblade_store::{ResourceFilter, Store};

use crate::query;

pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Resource labels that the exposition sets itself; semantic labels never override them.
const RESERVED_LABELS: &[&str] = &["id", "kind", "host", "collector", "version"];

pub fn health_code(h: Health) -> u8 {
    match h {
        Health::Ok => 0,
        Health::Unknown => 1,
        Health::Degraded => 2,
        Health::Failed => 3,
    }
}

/// `cpu.used_pct` -> `pulseblade_cpu_used_pct`.
pub fn metric_name(raw: &str) -> String {
    let mut out = String::from("pulseblade_");
    out.extend(raw.chars().map(|c| {
        if c.is_ascii_alphanumeric() || c == '_' || c == ':' {
            c
        } else {
            '_'
        }
    }));
    out
}

/// A valid label name, or `None` if `raw` cannot be one (empty or reserved `__` prefix).
pub fn label_name(raw: &str) -> Option<String> {
    let mut out: String = raw
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if out.starts_with(|c: char| c.is_ascii_digit()) {
        out.insert(0, '_');
    }
    (!out.is_empty() && !out.starts_with("__")).then_some(out)
}

/// Escape a label value: backslash, double quote, and newline.
pub fn escape_label_value(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for c in v.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
    out
}

fn escape_help(v: &str) -> String {
    v.replace('\\', "\\\\").replace('\n', "\\n")
}

fn format_value(v: f64) -> String {
    if v.is_nan() {
        "NaN".into()
    } else if v.is_infinite() {
        if v > 0.0 { "+Inf" } else { "-Inf" }.into()
    } else {
        v.to_string()
    }
}

/// Gauge families in first-seen order, each with its samples kept contiguous.
#[derive(Default)]
struct Exposition {
    order: Vec<String>,
    families: HashMap<String, (String, Vec<String>)>,
}

impl Exposition {
    fn gauge(&mut self, name: &str, help: &str, labels: &[(String, String)], value: f64) {
        let family = self.families.entry(name.to_string()).or_insert_with(|| {
            self.order.push(name.to_string());
            (help.to_string(), Vec::new())
        });
        let mut line = name.to_string();
        if !labels.is_empty() {
            line.push('{');
            for (i, (k, v)) in labels.iter().enumerate() {
                if i > 0 {
                    line.push(',');
                }
                let _ = write!(line, "{k}=\"{}\"", escape_label_value(v));
            }
            line.push('}');
        }
        let _ = write!(line, " {}", format_value(value));
        family.1.push(line);
    }

    fn finish(self) -> String {
        let mut out = String::new();
        for name in &self.order {
            let (help, lines) = &self.families[name];
            let _ = writeln!(out, "# HELP {name} {}", escape_help(help));
            let _ = writeln!(out, "# TYPE {name} gauge");
            for l in lines {
                out.push_str(l);
                out.push('\n');
            }
        }
        out
    }
}

fn l(k: &str, v: impl Into<String>) -> (String, String) {
    (k.to_string(), v.into())
}

fn resource_labels(r: &StoredResource) -> Vec<(String, String)> {
    let res = &r.resource;
    let host = res
        .labels
        .get("host")
        .cloned()
        .or_else(|| res.id.split(':').nth(1).map(str::to_string))
        .unwrap_or_default();
    let mut labels = vec![
        l("id", &res.id),
        l("kind", res.kind.as_str()),
        l("host", host),
    ];
    for (k, v) in &res.labels {
        if let Some(name) = label_name(k) {
            if !RESERVED_LABELS.contains(&name.as_str()) && !labels.iter().any(|(n, _)| *n == name)
            {
                labels.push((name, v.clone()));
            }
        }
    }
    labels
}

pub fn render(store: &Store) -> query::Result<String> {
    let mut x = Exposition::default();
    x.gauge(
        "pulseblade_up",
        "1 when the Pulseblade node is serving.",
        &[],
        1.0,
    );
    x.gauge(
        "pulseblade_build_info",
        "Pulseblade build information.",
        &[l("version", env!("CARGO_PKG_VERSION"))],
        1.0,
    );
    x.gauge(
        "pulseblade_journal_seq",
        "Current end of the change journal.",
        &[],
        store.current_seq()? as f64,
    );

    for c in query::collectors(store)? {
        let labels = [l("collector", &c.source)];
        x.gauge(
            "pulseblade_collector_up",
            "1 when the collector's last pass succeeded and is recent.",
            &labels,
            if c.ok { 1.0 } else { 0.0 },
        );
        x.gauge(
            "pulseblade_collector_last_run_timestamp_seconds",
            "Unix time of the collector's last pass.",
            &labels,
            c.last_run.timestamp_millis() as f64 / 1000.0,
        );
        if let Some(t) = c.last_success {
            x.gauge(
                "pulseblade_collector_last_success_timestamp_seconds",
                "Unix time of the collector's last successful pass.",
                &labels,
                t.timestamp_millis() as f64 / 1000.0,
            );
        }
        x.gauge(
            "pulseblade_collector_last_duration_seconds",
            "Duration of the collector's last pass.",
            &labels,
            c.duration_ms as f64 / 1000.0,
        );
        x.gauge(
            "pulseblade_collector_consecutive_failures",
            "Failed passes since the collector's last success.",
            &labels,
            c.consecutive_failures as f64,
        );
        x.gauge(
            "pulseblade_collector_resources",
            "Resources reported by the collector's last pass.",
            &labels,
            c.resource_count as f64,
        );
    }

    let resources = store.resources(&ResourceFilter::default())?;
    let latest = store.latest_all()?;
    for r in &resources {
        let labels = resource_labels(r);
        x.gauge(
            "pulseblade_resource_health",
            "Resource health: 0 ok, 1 unknown, 2 degraded, 3 failed.",
            &labels,
            health_code(r.resource.health) as f64,
        );
    }
    // Second pass so each metric family stays contiguous across resources.
    for r in &resources {
        let Some(metrics) = latest.get(&r.resource.id) else {
            continue;
        };
        let labels = resource_labels(r);
        for (metric, value) in metrics {
            x.gauge(
                &metric_name(metric),
                &format!("Latest `{metric}` sample."),
                &labels,
                *value,
            );
        }
    }
    Ok(x.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use pulseblade_core::{Observation, Resource, ResourceKind, Sample};

    #[test]
    fn names_are_sanitized() {
        assert_eq!(metric_name("cpu.used_pct"), "pulseblade_cpu_used_pct");
        assert_eq!(metric_name("load.1m"), "pulseblade_load_1m");
        assert_eq!(metric_name("net.rx-bytes/s"), "pulseblade_net_rx_bytes_s");
        assert_eq!(label_name("criticality").as_deref(), Some("criticality"));
        assert_eq!(label_name("team.name").as_deref(), Some("team_name"));
        assert_eq!(label_name("9lives").as_deref(), Some("_9lives"));
        assert_eq!(label_name("__meta"), None);
        assert_eq!(label_name(""), None);
    }

    #[test]
    fn label_values_are_escaped() {
        assert_eq!(escape_label_value(r#"a"b\c"#), r#"a\"b\\c"#);
        assert_eq!(escape_label_value("x\ny"), r"x\ny");
        assert_eq!(escape_label_value("disk:h:/"), "disk:h:/");
    }

    #[test]
    fn renders_families_once_with_labels() {
        let store = Store::open_in_memory().unwrap();
        store
            .apply(
                "host",
                Utc::now(),
                &Observation {
                    resources: vec![
                        Resource::new("host:h", ResourceKind::Host, "h")
                            .health(pulseblade_core::Health::Ok)
                            .label("host", "h"),
                        Resource::new("disk:h:/", ResourceKind::Disk, "/")
                            .parent("host:h")
                            .health(pulseblade_core::Health::Failed)
                            .label("host", "h")
                            .label("criticality", "high")
                            .label("odd\"label", "v\"q"),
                    ],
                    samples: vec![
                        Sample::new("host:h", "cpu.used_pct", 12.5),
                        Sample::new("disk:h:/", "disk.used_pct", 99.0),
                    ],
                },
            )
            .unwrap();
        let text = render(&store).unwrap();
        assert!(text.contains("pulseblade_up 1\n"));
        assert!(text.contains("pulseblade_build_info{version=\""));
        assert!(text.contains(
            "pulseblade_resource_health{id=\"disk:h:/\",kind=\"disk\",host=\"h\",criticality=\"high\",odd_label=\"v\\\"q\"} 3\n"
        ));
        assert!(
            text.contains("pulseblade_cpu_used_pct{id=\"host:h\",kind=\"host\",host=\"h\"} 12.5\n")
        );
        assert_eq!(
            text.matches("# TYPE pulseblade_resource_health gauge")
                .count(),
            1
        );
        for line in text.lines().filter(|l| !l.starts_with('#')) {
            let (series, value) = line.rsplit_once(' ').unwrap();
            assert!(value.parse::<f64>().is_ok(), "{line}");
            assert!(series.starts_with("pulseblade_"), "{line}");
        }
    }
}
