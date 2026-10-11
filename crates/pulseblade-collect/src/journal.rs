//! Per-unit error lines and a recent error count from `journalctl`.
//!
//! The collector owns no resources. It attaches samples and log lines to unit
//! ids the systemd collectors already report, and degrades to an empty pass
//! when journald cannot be read.

use std::collections::{BTreeMap, BTreeSet};
use std::process::Command;

use chrono::{DateTime, Utc};
use pulseblade_core::{LogLine, Observation, Sample};
use serde_json::Value;

use crate::{run_command, CollectError, Collector};

const WINDOW: &str = "2 min ago";
const MAX_LINES: usize = 200;

pub struct JournaldCollector {
    host: String,
    /// Units that have reported errors, so a quiet window can record a zero.
    seen: BTreeSet<String>,
}

impl JournaldCollector {
    pub fn new(host: String) -> Self {
        Self {
            host,
            seen: BTreeSet::new(),
        }
    }

    pub fn available() -> bool {
        Command::new("journalctl")
            .arg("--version")
            .output()
            .is_ok_and(|o| o.status.success())
    }
}

impl Collector for JournaldCollector {
    fn name(&self) -> &'static str {
        "journald"
    }

    fn collect(&mut self) -> Result<Observation, CollectError> {
        let bytes = run_command(
            "journalctl",
            &[
                "-p",
                "err",
                "-o",
                "json",
                "-n",
                "200",
                "--since",
                WINDOW,
                "--no-pager",
            ],
        )?;
        Ok(parse_journal(&self.host, &bytes, &mut self.seen))
    }
}

/// Group journal JSON lines (one object per line) into log lines and error counts.
pub(crate) fn parse_journal(host: &str, bytes: &[u8], seen: &mut BTreeSet<String>) -> Observation {
    let mut counts: BTreeMap<String, f64> = BTreeMap::new();
    let mut logs = Vec::new();
    for line in bytes.split(|b| *b == b'\n') {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let Ok(value) = serde_json::from_slice::<Value>(line) else {
            continue;
        };
        let Some(message) = message_of(&value) else {
            continue;
        };
        if message.is_empty() {
            continue;
        }
        let resource_id = resource_of(host, &value);
        *counts.entry(resource_id.clone()).or_default() += 1.0;
        if logs.len() < MAX_LINES {
            logs.push(LogLine {
                ts: timestamp_of(&value).unwrap_or_else(Utc::now),
                resource_id,
                message,
            });
        }
    }
    for id in counts.keys() {
        seen.insert(id.clone());
    }
    let mut samples = Vec::new();
    for id in seen.iter() {
        samples.push(Sample::new(
            id,
            "journal.errors",
            counts.get(id).copied().unwrap_or(0.0),
        ));
    }
    Observation {
        resources: Vec::new(),
        samples,
        logs,
    }
}

fn resource_of(host: &str, value: &Value) -> String {
    if let Some(unit) = text(value, "_SYSTEMD_USER_UNIT") {
        return format!("unit:{host}:user:{unit}");
    }
    if let Some(unit) = text(value, "_SYSTEMD_UNIT") {
        return format!("unit:{host}:{unit}");
    }
    format!("host:{host}")
}

fn text<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key)?.as_str().filter(|s| !s.is_empty())
}

fn message_of(value: &Value) -> Option<String> {
    let raw = match value.get("MESSAGE")? {
        Value::String(s) => s.clone(),
        Value::Array(bytes) => {
            let raw: Vec<u8> = bytes
                .iter()
                .filter_map(|n| n.as_u64().map(|n| n as u8))
                .collect();
            String::from_utf8_lossy(&raw).into_owned()
        }
        _ => return None,
    };
    let flat = raw.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() <= 400 {
        Some(flat)
    } else {
        let mut out: String = flat.chars().take(399).collect();
        out.push('…');
        Some(out)
    }
}

fn timestamp_of(value: &Value) -> Option<DateTime<Utc>> {
    let raw = value.get("__REALTIME_TIMESTAMP")?.as_str()?;
    let micros: i64 = raw.parse().ok()?;
    DateTime::from_timestamp_micros(micros)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn groups_system_and_user_units() {
        let raw = r#"
{"__REALTIME_TIMESTAMP":"1700000000000000","PRIORITY":"3","_SYSTEMD_UNIT":"sshd.service","MESSAGE":"error: bad config"}
{"__REALTIME_TIMESTAMP":"1700000001000000","PRIORITY":"3","_SYSTEMD_USER_UNIT":"app.service","MESSAGE":"panic"}
{"__REALTIME_TIMESTAMP":"1700000002000000","PRIORITY":"3","MESSAGE":"kernel oops"}
"#;
        let mut seen = BTreeSet::new();
        let obs = parse_journal("h", raw.as_bytes(), &mut seen);
        assert_eq!(obs.logs.len(), 3);
        assert!(obs
            .samples
            .iter()
            .any(|s| s.resource_id == "unit:h:sshd.service" && s.value == 1.0));
        assert!(obs
            .samples
            .iter()
            .any(|s| s.resource_id == "unit:h:user:app.service" && s.value == 1.0));
        assert!(obs
            .samples
            .iter()
            .any(|s| s.resource_id == "host:h" && s.value == 1.0));

        let quiet = parse_journal("h", b"", &mut seen);
        assert!(quiet.logs.is_empty());
        assert!(quiet.samples.iter().all(|s| s.value == 0.0));
        assert_eq!(quiet.samples.len(), 3);
    }

    #[test]
    fn skips_garbage_lines() {
        let mut seen = BTreeSet::new();
        let obs = parse_journal(
            "h",
            b"not json\n{\"MESSAGE\":\"ok\",\"_SYSTEMD_UNIT\":\"a.service\"}\n",
            &mut seen,
        );
        assert_eq!(obs.logs.len(), 1);
        assert_eq!(obs.logs[0].message, "ok");
    }
}
