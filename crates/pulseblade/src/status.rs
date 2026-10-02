//! `pulseblade ctl status`: a plain-text basic view of the local store.

use std::fmt::Write as _;

use chrono::{DateTime, Local, Utc};
use pulseblade_mcp::query::{self, ChangeEntry, Detail};
use pulseblade_store::Store;
use serde_json::Value;

const KEY_METRICS: &[(&str, &str)] = &[
    ("cpu.used_pct", "cpu"),
    ("mem.used_pct", "mem"),
    ("load.1m", "load"),
    ("swap.used_pct", "swap"),
];

pub fn render(store: &Store, recent: usize) -> anyhow::Result<String> {
    let st = query::status(store, None)?;
    let snap = query::snapshot(
        store,
        query::SnapshotParams {
            detail: Some(Detail::Summary),
            limit: Some(50),
            ..Default::default()
        },
    )?;
    let mut o = String::new();

    writeln!(
        o,
        "Pulseblade {} · journal seq {} · {}",
        st.version,
        st.as_of_seq,
        local(st.generated_at)
    )?;
    writeln!(
        o,
        "Database   {} ({}, {} samples, {} changes, {} checkpoints)",
        st.db.path.as_deref().unwrap_or("(in memory)"),
        st.db.size_bytes.map_or("size unknown".into(), bytes),
        st.samples,
        st.changes,
        st.checkpoints
    )?;

    writeln!(o, "\nCollectors")?;
    if st.collectors.is_empty() {
        writeln!(o, "  none recorded yet (is `pulseblade node` running?)")?;
    }
    let w = st
        .collectors
        .iter()
        .map(|c| c.source.len())
        .max()
        .unwrap_or(0);
    for c in &st.collectors {
        let state = if c.stale {
            "STALE"
        } else if !c.ok {
            "FAILED"
        } else {
            "ok"
        };
        write!(
            o,
            "  {:<w$}  {:<6}  last {:>7}  {:>5} ms  {:>4} resources  {:>4} samples",
            c.source,
            state,
            ago(c.age_secs),
            c.duration_ms,
            c.resource_count,
            c.sample_count,
        )?;
        if let Some(e) = &c.error {
            write!(o, "  error: {e} ({} consecutive)", c.consecutive_failures)?;
        }
        writeln!(o)?;
    }

    if !snap.hosts.is_empty() {
        writeln!(o, "\nHosts")?;
        for h in &snap.hosts {
            let metrics: Vec<String> = KEY_METRICS
                .iter()
                .filter_map(|(k, label)| {
                    h.metrics.get(*k).map(|v| {
                        if k.ends_with("_pct") {
                            format!("{label} {v:.1}%")
                        } else {
                            format!("{label} {v:.2}")
                        }
                    })
                })
                .collect();
            writeln!(
                o,
                "  {}  {}  {}",
                h.id,
                h.health.as_str(),
                metrics.join("  ")
            )?;
        }
    }

    let r = &st.resources;
    writeln!(
        o,
        "\nResources: {} total, {} failed, {} degraded",
        r.total, r.failed, r.degraded
    )?;
    let w = r.by_kind.keys().map(String::len).max().unwrap_or(0);
    for (kind, c) in &r.by_kind {
        write!(o, "  {kind:<w$}  {:>5}", c.total)?;
        if c.failed > 0 {
            write!(o, "  {} failed", c.failed)?;
        }
        if c.degraded > 0 {
            write!(o, "  {} degraded", c.degraded)?;
        }
        writeln!(o)?;
    }

    writeln!(o, "\nUnhealthy ({})", snap.total.unwrap_or(0))?;
    if snap.resources.is_empty() {
        writeln!(o, "  none")?;
    }
    let w = snap.resources.iter().map(|r| r.id.len()).max().unwrap_or(0);
    for res in &snap.resources {
        let attrs = res
            .attrs
            .as_ref()
            .map(|a| {
                a.iter()
                    .filter(|(k, _)| *k != "description")
                    .take(4)
                    .map(|(k, v)| format!("{k}={}", short(v)))
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .unwrap_or_default();
        writeln!(o, "  {:<8}  {:<w$}  {attrs}", res.health.as_str(), res.id)?;
    }
    if snap.truncated {
        writeln!(o, "  … more; see `pulseblade ctl snapshot --unhealthy`")?;
    }

    if recent > 0 {
        let cs = query::changes(
            store,
            query::ChangesParams {
                since: format!("seq:{}", (st.as_of_seq - recent as i64).max(0)),
                kind: None,
                resource_prefix: None,
                limit: Some(recent),
                compact: Some(false),
            },
        )?;
        writeln!(o, "\nRecent changes (last {})", cs.changes.len())?;
        if cs.changes.is_empty() {
            writeln!(o, "  none")?;
        }
        let w = cs
            .changes
            .iter()
            .map(|c| c.resource_id.len())
            .max()
            .unwrap_or(0);
        for c in cs.changes.iter().rev() {
            writeln!(
                o,
                "  {}  #{:<6} {:<w$}  {}",
                c.ts.map(|t| t.with_timezone(&Local).format("%H:%M:%S").to_string())
                    .unwrap_or_default(),
                c.seq,
                c.resource_id,
                describe(c)
            )?;
        }
    }
    Ok(o)
}

fn local(t: DateTime<Utc>) -> String {
    t.with_timezone(&Local)
        .format("%Y-%m-%d %H:%M:%S")
        .to_string()
}

fn ago(secs: i64) -> String {
    match secs {
        s if s < 60 => format!("{s}s ago"),
        s if s < 3600 => format!("{}m ago", s / 60),
        s if s < 86_400 => format!("{}h ago", s / 3600),
        s => format!("{}d ago", s / 86_400),
    }
}

fn bytes(n: u64) -> String {
    let units = ["B", "KB", "MB", "GB", "TB"];
    let mut v = n as f64;
    let mut i = 0;
    while v >= 1024.0 && i < units.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{n} B")
    } else {
        format!("{v:.1} {}", units[i])
    }
}

fn short(v: &Value) -> String {
    let s = match v {
        Value::String(s) => s.clone(),
        Value::Object(m) if m.contains_key("health") => format!(
            "{} {}",
            m.get("kind").and_then(Value::as_str).unwrap_or(""),
            m["health"].as_str().unwrap_or("")
        ),
        other => other.to_string(),
    };
    if s.chars().count() > 60 {
        format!("{}…", s.chars().take(59).collect::<String>())
    } else {
        s
    }
}

fn describe(c: &ChangeEntry) -> String {
    let v = |x: &Option<Value>| x.as_ref().map_or("∅".to_string(), short);
    match c.kind {
        pulseblade_core::ChangeKind::Changed => format!(
            "{}: {} -> {}",
            c.field.as_deref().unwrap_or("?"),
            v(&c.before),
            v(&c.after)
        ),
        pulseblade_core::ChangeKind::Appeared => format!("appeared ({})", v(&c.after)),
        pulseblade_core::ChangeKind::Disappeared => format!("disappeared (was {})", v(&c.before)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulseblade_core::{CollectorRun, Health, Observation, Resource, ResourceKind, Sample};

    #[test]
    fn renders_plain_text_summary() {
        let store = Store::open_in_memory().unwrap();
        let now = Utc::now();
        store
            .apply(
                "host",
                now,
                &Observation {
                    resources: vec![
                        Resource::new("host:h", ResourceKind::Host, "h").health(Health::Ok),
                        Resource::new("disk:h:/", ResourceKind::Disk, "/")
                            .parent("host:h")
                            .health(Health::Failed)
                            .attr("fs", "ext4"),
                    ],
                    samples: vec![Sample::new("host:h", "cpu.used_pct", 7.25)],
                },
            )
            .unwrap();
        store
            .record_run(&CollectorRun {
                source: "host".into(),
                ts: now,
                duration_ms: 9,
                ok: true,
                error: None,
                resource_count: 2,
                sample_count: 1,
                change_count: 2,
                interval_secs: Some(15),
            })
            .unwrap();
        let text = render(&store, 5).unwrap();
        assert!(text.contains("journal seq 2"), "{text}");
        assert!(text.contains("host:h  ok  cpu 7.2%"), "{text}");
        assert!(
            text.contains("Resources: 2 total, 1 failed, 0 degraded"),
            "{text}"
        );
        assert!(text.contains("failed    disk:h:/  fs=ext4"), "{text}");
        assert!(text.contains("Recent changes (last 2)"), "{text}");
        assert!(text.contains("appeared (disk failed)"), "{text}");
        assert!(!text.contains('{'), "plain text, not JSON:\n{text}");
    }
}
