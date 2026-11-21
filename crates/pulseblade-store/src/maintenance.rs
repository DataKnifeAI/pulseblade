//! Planned quiet time, and hosts whose silence is normal.
//!
//! A maintenance window is a bounded interval: one shot, or one weekday clock
//! range of at most six hours. `offline_ok` is a standing set of node ids.
//! Neither one is a cron expression. The hub reads both when it decides
//! `host offline`. An empty set means every missing host can still open that
//! finding.

use std::collections::BTreeSet;

use chrono::{DateTime, Datelike, Duration, Timelike, Utc};
use chrono_tz::Tz;

use crate::sql::{SqlExt, SqlVal};
use crate::{Store, StoreError};

/// Longest quiet interval. A row past this is rejected, not clipped.
pub const MAX_MAINTENANCE: Duration = Duration::hours(6);
const MAX_REASON: usize = 160;

/// One weekday clock range from hub config. `clock` is `utc` or `local`.
/// `local` uses `tz` (the dashboard timezone). The span is at most six hours
/// and does not cross midnight.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WeeklyMaintenance {
    /// Monday is 0, Sunday is 6.
    pub weekday: u8,
    pub start_min: u16,
    pub end_min: u16,
    pub clock: String,
    pub tz: String,
    pub nodes: Vec<String>,
    pub reason: String,
}

/// A one-shot window with absolute UTC bounds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceOnce {
    pub starts_at: DateTime<Utc>,
    pub ends_at: DateTime<Utc>,
    pub nodes: Vec<String>,
    pub reason: String,
}

/// A window that is open at the instant it was computed.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ActiveMaintenance {
    pub id: String,
    pub starts_at: DateTime<Utc>,
    pub ends_at: DateTime<Utc>,
    pub nodes: Vec<String>,
    pub reason: String,
}

impl ActiveMaintenance {
    pub fn covers(&self, node: &str) -> bool {
        self.nodes.is_empty() || self.nodes.iter().any(|id| id == node)
    }
}

impl WeeklyMaintenance {
    /// Parse one config row. `tz` is the IANA name used when `clock` is `local`.
    pub fn parse(
        weekday: &str,
        start: &str,
        end: &str,
        clock: &str,
        tz: &str,
        nodes: &[String],
        reason: &str,
    ) -> Result<Self, String> {
        let weekday = parse_weekday(weekday)?;
        let start_min = parse_hhmm(start)?;
        let end_min = parse_hhmm(end)?;
        if end_min <= start_min {
            return Err("maintenance end must be later the same day".into());
        }
        let span = u32::from(end_min - start_min) * 60 * 1000;
        if span > MAX_MAINTENANCE.num_milliseconds() as u32 {
            return Err("maintenance window longer than 6 hours".into());
        }
        let clock = match clock.trim() {
            "" | "utc" => "utc".to_string(),
            "local" => "local".to_string(),
            other => {
                return Err(format!(
                    "maintenance clock {other:?} must be \"utc\" or \"local\""
                ))
            }
        };
        if clock == "local" {
            let _: Tz = tz
                .parse()
                .map_err(|_| format!("timezone {tz:?} is not a known IANA name"))?;
        }
        Ok(Self {
            weekday,
            start_min,
            end_min,
            clock: clock.clone(),
            tz: if clock == "local" {
                tz.to_string()
            } else {
                String::new()
            },
            nodes: normalize_nodes(nodes)?,
            reason: normalize_reason(reason)?,
        })
    }
}

/// Nodes the hub should not treat as an outage when they stop pushing.
pub(crate) struct SilencePolicy {
    offline_ok: BTreeSet<String>,
    windows: Vec<ActiveMaintenance>,
}

impl SilencePolicy {
    pub(crate) fn allows(&self, node: &str) -> bool {
        self.offline_ok.contains(node) || self.windows.iter().any(|w| w.covers(node))
    }
}

fn parse_weekday(text: &str) -> Result<u8, String> {
    match text.trim().to_ascii_lowercase().as_str() {
        "mon" | "monday" => Ok(0),
        "tue" | "tuesday" => Ok(1),
        "wed" | "wednesday" => Ok(2),
        "thu" | "thursday" => Ok(3),
        "fri" | "friday" => Ok(4),
        "sat" | "saturday" => Ok(5),
        "sun" | "sunday" => Ok(6),
        _ => Err("weekday must be mon, tue, wed, thu, fri, sat, or sun".into()),
    }
}

fn parse_hhmm(text: &str) -> Result<u16, String> {
    let (hour, minute) = text
        .trim()
        .split_once(':')
        .ok_or_else(|| format!("clock {text:?} must be HH:MM"))?;
    if minute.len() != 2 || hour.is_empty() || hour.len() > 2 {
        return Err(format!("clock {text:?} must be HH:MM"));
    }
    let hour: u16 = hour
        .parse()
        .map_err(|_| format!("clock {text:?} must be HH:MM"))?;
    let minute: u16 = minute
        .parse()
        .map_err(|_| format!("clock {text:?} must be HH:MM"))?;
    if hour > 23 || minute > 59 {
        return Err(format!("clock {text:?} must be HH:MM"));
    }
    Ok(hour * 60 + minute)
}

fn normalize_reason(reason: &str) -> Result<String, String> {
    let reason = reason.trim();
    if reason.is_empty()
        || reason.chars().count() > MAX_REASON
        || reason.chars().any(char::is_control)
    {
        return Err("maintenance reason must be 1 to 160 characters on one line".into());
    }
    Ok(reason.to_string())
}

fn normalize_nodes(nodes: &[String]) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    for node in nodes {
        let node = node.trim();
        if node.is_empty() {
            continue;
        }
        valid_node(node)?;
        if !out.iter().any(|have: &String| have == node) {
            out.push(node.to_string());
        }
    }
    Ok(out)
}

fn valid_node(node: &str) -> Result<(), String> {
    if node.is_empty()
        || node.contains(':')
        || node.contains(',')
        || node.chars().any(char::is_whitespace)
    {
        return Err(format!(
            "node id {node:?} must be a non-empty name without ':'"
        ));
    }
    Ok(())
}

fn zone_of(clock: &str, tz: &str) -> Option<Tz> {
    if clock == "local" {
        tz.parse().ok()
    } else {
        Some(chrono_tz::UTC)
    }
}

fn weekly_occurrence(
    row: &WeeklyMaintenance,
    now: DateTime<Utc>,
) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    let tz = zone_of(&row.clock, &row.tz)?;
    let local = now.with_timezone(&tz);
    if local.weekday().num_days_from_monday() != u32::from(row.weekday) {
        return None;
    }
    let sod = local.hour() * 3600 + local.minute() * 60 + local.second();
    let start_s = u32::from(row.start_min) * 60;
    let end_s = u32::from(row.end_min) * 60;
    if sod < start_s || sod >= end_s {
        return None;
    }
    let date = local.date_naive();
    let start_naive = date.and_hms_opt(
        u32::from(row.start_min / 60),
        u32::from(row.start_min % 60),
        0,
    )?;
    let end_naive =
        date.and_hms_opt(u32::from(row.end_min / 60), u32::from(row.end_min % 60), 0)?;
    let start = start_naive.and_local_timezone(tz).earliest()?;
    let end = end_naive.and_local_timezone(tz).earliest()?;
    Some((start.with_timezone(&Utc), end.with_timezone(&Utc)))
}

fn nodes_json(nodes: &[String]) -> Result<String, StoreError> {
    Ok(serde_json::to_string(nodes)?)
}

fn nodes_from(text: &str) -> Vec<String> {
    serde_json::from_str::<Vec<String>>(text).unwrap_or_default()
}

struct StoredWindow {
    id: String,
    kind: String,
    start_ms: i64,
    end_ms: i64,
    weekday: i64,
    clock: String,
    tz: String,
    nodes: String,
    reason: String,
}

impl Store {
    pub(crate) fn silence_policy(&self, now: DateTime<Utc>) -> crate::Result<SilencePolicy> {
        Ok(SilencePolicy {
            offline_ok: self.offline_ok_nodes()?.into_iter().collect(),
            windows: self.active_maintenance(now)?,
        })
    }

    /// Node ids whose silence is not an incident. Config rows and runtime marks.
    pub fn offline_ok_nodes(&self) -> crate::Result<Vec<String>> {
        self.with_sql(|sql| {
            let rows = sql.query(
                "SELECT DISTINCT node_id FROM offline_ok ORDER BY node_id",
                &[],
                |row| row.text(0),
            )?;
            Ok(rows)
        })
    }

    /// Config-file rows only. A runtime clear does not remove these.
    pub fn config_offline_ok(&self) -> crate::Result<Vec<String>> {
        self.with_sql(|sql| {
            sql.query(
                "SELECT node_id FROM offline_ok WHERE origin = 'config' ORDER BY node_id",
                &[],
                |row| row.text(0),
            )
        })
    }

    /// Runtime marks only, so ctl can tell them from the config set.
    pub fn runtime_offline_ok(&self) -> crate::Result<Vec<String>> {
        self.with_sql(|sql| {
            sql.query(
                "SELECT node_id FROM offline_ok WHERE origin = 'runtime' ORDER BY node_id",
                &[],
                |row| row.text(0),
            )
        })
    }

    /// Replace the config-owned half of the set. Runtime marks stay.
    pub fn replace_config_offline_ok(&self, nodes: &[String]) -> crate::Result<()> {
        let nodes = normalize_nodes(nodes).map_err(StoreError::Invalid)?;
        self.transaction(|sql| {
            sql.execute("DELETE FROM offline_ok WHERE origin = 'config'", &[])?;
            for node in &nodes {
                sql.execute(
                    "INSERT INTO offline_ok (node_id, origin) VALUES (?1, 'config')
                     ON CONFLICT(node_id, origin) DO NOTHING",
                    &[SqlVal::text(node)],
                )?;
            }
            Ok(())
        })
    }

    /// Mark or clear one node without editing the hub file.
    /// Clearing removes the runtime mark. A node still listed in config stays allowed.
    pub fn set_runtime_offline_ok(&self, node: &str, enabled: bool) -> crate::Result<()> {
        valid_node(node.trim()).map_err(StoreError::Invalid)?;
        let node = node.trim();
        self.with_sql(|sql| {
            if enabled {
                sql.execute(
                    "INSERT INTO offline_ok (node_id, origin) VALUES (?1, 'runtime')
                     ON CONFLICT(node_id, origin) DO NOTHING",
                    &[SqlVal::text(node)],
                )?;
            } else {
                sql.execute(
                    "DELETE FROM offline_ok WHERE node_id = ?1 AND origin = 'runtime'",
                    &[SqlVal::text(node)],
                )?;
            }
            Ok(())
        })
    }

    /// Open a one-shot window. The span must be at most [`MAX_MAINTENANCE`].
    pub fn open_maintenance(&self, window: MaintenanceOnce) -> crate::Result<ActiveMaintenance> {
        let nodes = normalize_nodes(&window.nodes).map_err(StoreError::Invalid)?;
        let reason = normalize_reason(&window.reason).map_err(StoreError::Invalid)?;
        let span = window.ends_at.signed_duration_since(window.starts_at);
        if span <= Duration::zero() {
            return Err(StoreError::Invalid(
                "maintenance end must be after the start".into(),
            ));
        }
        if span > MAX_MAINTENANCE {
            return Err(StoreError::Invalid(
                "maintenance window longer than 6 hours".into(),
            ));
        }
        if window.ends_at <= Utc::now() {
            return Err(StoreError::Invalid(
                "maintenance window has already ended".into(),
            ));
        }
        let id = format!("m{}", Utc::now().timestamp_nanos_opt().unwrap_or(0));
        let body = nodes_json(&nodes)?;
        self.with_sql(|sql| {
            sql.execute(
                "INSERT INTO maintenance
                    (id, kind, start_ms, end_ms, weekday, clock, tz, nodes, reason)
                 VALUES (?1, 'once', ?2, ?3, -1, 'utc', '', ?4, ?5)",
                &[
                    SqlVal::text(&id),
                    SqlVal::Int(window.starts_at.timestamp_millis()),
                    SqlVal::Int(window.ends_at.timestamp_millis()),
                    SqlVal::text(&body),
                    SqlVal::text(&reason),
                ],
            )?;
            Ok(())
        })?;
        Ok(ActiveMaintenance {
            id,
            starts_at: window.starts_at,
            ends_at: window.ends_at,
            nodes,
            reason,
        })
    }

    /// Drop one-shot windows. Weekly rows from config stay.
    pub fn clear_maintenance_once(&self) -> crate::Result<usize> {
        self.with_sql(|sql| sql.execute("DELETE FROM maintenance WHERE kind = 'once'", &[]))
    }

    /// Replace weekly rows when the config schedule changed. One-shots stay.
    pub fn replace_weekly_maintenance(&self, rows: &[WeeklyMaintenance]) -> crate::Result<bool> {
        let current = self.weekly_rows()?;
        if current == rows {
            return Ok(false);
        }
        self.transaction(|sql| {
            sql.execute("DELETE FROM maintenance WHERE kind = 'weekly'", &[])?;
            for (i, row) in rows.iter().enumerate() {
                let id = format!("w{i:04}");
                sql.execute(
                    "INSERT INTO maintenance
                        (id, kind, start_ms, end_ms, weekday, clock, tz, nodes, reason)
                     VALUES (?1, 'weekly', ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    &[
                        SqlVal::text(&id),
                        SqlVal::Int(i64::from(row.start_min)),
                        SqlVal::Int(i64::from(row.end_min)),
                        SqlVal::Int(i64::from(row.weekday)),
                        SqlVal::text(&row.clock),
                        SqlVal::text(&row.tz),
                        SqlVal::text(&nodes_json(&row.nodes)?),
                        SqlVal::text(&row.reason),
                    ],
                )?;
            }
            Ok(())
        })?;
        Ok(true)
    }

    /// Windows open at `now`, one-shot and weekly.
    pub fn active_maintenance(&self, now: DateTime<Utc>) -> crate::Result<Vec<ActiveMaintenance>> {
        let rows = self.maintenance_rows()?;
        let mut out = Vec::new();
        let now_ms = now.timestamp_millis();
        for row in rows {
            if row.kind == "once" {
                if row.start_ms <= now_ms && now_ms < row.end_ms {
                    out.push(ActiveMaintenance {
                        id: row.id,
                        starts_at: crate::from_ms(row.start_ms),
                        ends_at: crate::from_ms(row.end_ms),
                        nodes: nodes_from(&row.nodes),
                        reason: row.reason,
                    });
                }
                continue;
            }
            if row.kind != "weekly" {
                continue;
            }
            let weekly = WeeklyMaintenance {
                weekday: u8::try_from(row.weekday).unwrap_or(255),
                start_min: u16::try_from(row.start_ms).unwrap_or(0),
                end_min: u16::try_from(row.end_ms).unwrap_or(0),
                clock: row.clock,
                tz: row.tz,
                nodes: nodes_from(&row.nodes),
                reason: row.reason,
            };
            if let Some((starts_at, ends_at)) = weekly_occurrence(&weekly, now) {
                out.push(ActiveMaintenance {
                    id: row.id,
                    starts_at,
                    ends_at,
                    nodes: weekly.nodes,
                    reason: weekly.reason,
                });
            }
        }
        Ok(out)
    }

    fn weekly_rows(&self) -> crate::Result<Vec<WeeklyMaintenance>> {
        let rows = self.maintenance_rows()?;
        let mut out = Vec::new();
        for row in rows {
            if row.kind != "weekly" {
                continue;
            }
            out.push(WeeklyMaintenance {
                weekday: u8::try_from(row.weekday).unwrap_or(255),
                start_min: u16::try_from(row.start_ms).unwrap_or(0),
                end_min: u16::try_from(row.end_ms).unwrap_or(0),
                clock: row.clock,
                tz: row.tz,
                nodes: nodes_from(&row.nodes),
                reason: row.reason,
            });
        }
        Ok(out)
    }

    fn maintenance_rows(&self) -> crate::Result<Vec<StoredWindow>> {
        self.with_sql(|sql| {
            sql.query(
                "SELECT id, kind, start_ms, end_ms, weekday, clock, tz, nodes, reason
                 FROM maintenance ORDER BY id",
                &[],
                |row| {
                    Ok(StoredWindow {
                        id: row.text(0)?,
                        kind: row.text(1)?,
                        start_ms: row.int(2)?,
                        end_ms: row.int(3)?,
                        weekday: row.int(4)?,
                        clock: row.text(5)?,
                        tz: row.text(6)?,
                        nodes: row.text(7)?,
                        reason: row.text(8)?,
                    })
                },
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hub::NodeBatch;
    use crate::FindingFilter;
    use chrono::Duration;
    use pulseblade_core::{Change, ChangeKind, FindingStatus, Health, Resource, ResourceKind};

    fn at(text: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(text)
            .unwrap()
            .with_timezone(&Utc)
    }

    fn silence(store: &Store, node: &str, now: DateTime<Utc>) {
        store
            .ingest_node(
                node,
                &NodeBatch {
                    changes: vec![Change {
                        seq: 1,
                        ts: now,
                        resource_id: format!("host:{node}"),
                        kind: ChangeKind::Appeared,
                        field: None,
                        before: None,
                        after: None,
                    }],
                    resources: vec![Resource::new(
                        format!("host:{node}"),
                        ResourceKind::Host,
                        node,
                    )
                    .health(Health::Ok)],
                    interval_secs: 15,
                    ..NodeBatch::default()
                },
            )
            .unwrap();
        store
            .set_node_last_push(node, now.timestamp_millis() - 120_000)
            .unwrap();
    }

    fn offline_ids(store: &Store) -> Vec<String> {
        let (rows, _) = store
            .findings(
                &FindingFilter {
                    status: Some(FindingStatus::Open),
                    ..Default::default()
                },
                20,
            )
            .unwrap();
        rows.into_iter()
            .filter(|f| f.rule == crate::hub::OFFLINE_RULE)
            .map(|f| f.resource_id)
            .collect()
    }

    #[test]
    fn weekly_clock_is_utc_or_local_and_six_hours_is_the_cap() {
        let utc = WeeklyMaintenance::parse(
            "sun",
            "02:00",
            "04:00",
            "utc",
            "America/Los_Angeles",
            &[],
            "patch",
        )
        .unwrap();
        let local = WeeklyMaintenance::parse(
            "sun",
            "02:00",
            "04:00",
            "local",
            "America/Los_Angeles",
            &[],
            "patch",
        )
        .unwrap();
        let sunday_utc = at("2026-11-22T03:00:00Z");
        let sunday_pacific = at("2026-11-22T11:00:00Z");
        assert!(weekly_occurrence(&utc, sunday_utc).is_some());
        assert!(weekly_occurrence(&utc, sunday_pacific).is_none());
        assert!(weekly_occurrence(&local, sunday_pacific).is_some());
        assert!(weekly_occurrence(&local, sunday_utc).is_none());
        assert!(weekly_occurrence(&utc, at("2026-11-21T03:00:00Z")).is_none());
        assert!(WeeklyMaintenance::parse("sun", "02:00", "08:00", "utc", "UTC", &[], "x").is_ok());
        let err =
            WeeklyMaintenance::parse("sun", "02:00", "08:01", "utc", "UTC", &[], "x").unwrap_err();
        assert!(err.contains("6 hours"), "{err}");
        assert!(WeeklyMaintenance::parse("sun", "22:00", "01:00", "utc", "UTC", &[], "x").is_err());
    }

    #[test]
    fn a_window_quiets_the_fleet_until_it_ends_and_rejects_seven_hours() {
        let store = Store::open_in_memory().unwrap();
        let now = Utc::now();
        let err = store
            .open_maintenance(MaintenanceOnce {
                starts_at: now,
                ends_at: now + Duration::hours(6) + Duration::seconds(1),
                nodes: vec![],
                reason: "too long".into(),
            })
            .unwrap_err();
        assert!(err.to_string().contains("6 hours"), "{err}");
        store
            .open_maintenance(MaintenanceOnce {
                starts_at: now - Duration::minutes(5),
                ends_at: now + Duration::minutes(20),
                nodes: vec![],
                reason: "hub roll".into(),
            })
            .unwrap();
        silence(&store, "web1", now);
        assert!(store.sync_offline_hosts(now).unwrap().is_empty());
        assert!(offline_ids(&store).is_empty());
        let later = now + Duration::minutes(21);
        let opened = store.sync_offline_hosts(later).unwrap();
        assert_eq!(opened.len(), 1, "{opened:?}");
        assert_eq!(opened[0].resource_id, "host:web1");
        assert_eq!(opened[0].rule, crate::hub::OFFLINE_RULE);
    }

    #[test]
    fn a_node_outside_the_window_still_opens_host_offline() {
        let store = Store::open_in_memory().unwrap();
        let now = Utc::now();
        store
            .open_maintenance(MaintenanceOnce {
                starts_at: now - Duration::minutes(1),
                ends_at: now + Duration::minutes(30),
                nodes: vec!["other".into()],
                reason: "one node".into(),
            })
            .unwrap();
        silence(&store, "web1", now);
        silence(&store, "other", now);
        let ids = offline_ids(&store);
        assert!(ids.is_empty(), "sync has not run");
        store.sync_offline_hosts(now).unwrap();
        let ids = offline_ids(&store);
        assert_eq!(ids, vec!["host:web1".to_string()]);
    }

    #[test]
    fn offline_ok_skips_that_node_and_a_clear_lets_silence_count() {
        let store = Store::open_in_memory().unwrap();
        let now = Utc::now();
        store.set_runtime_offline_ok("work1", true).unwrap();
        silence(&store, "web1", now);
        store
            .ingest_node(
                "work1",
                &NodeBatch {
                    changes: vec![Change {
                        seq: 1,
                        ts: now,
                        resource_id: "host:work1".into(),
                        kind: ChangeKind::Appeared,
                        field: None,
                        before: None,
                        after: None,
                    }],
                    resources: vec![
                        Resource::new("host:work1", ResourceKind::Host, "work1").health(Health::Ok),
                        Resource::new("disk:work1:/", ResourceKind::Disk, "/")
                            .parent("host:work1")
                            .health(Health::Failed),
                    ],
                    interval_secs: 15,
                    ..NodeBatch::default()
                },
            )
            .unwrap();
        store
            .set_node_last_push("work1", now.timestamp_millis() - 120_000)
            .unwrap();
        store.sync_offline_hosts(now).unwrap();
        let ids = offline_ids(&store);
        assert_eq!(ids, vec!["host:web1".to_string()], "{ids:?}");
        let disk = store.resource("disk:work1:/").unwrap().unwrap();
        assert_eq!(disk.resource.health, Health::Failed);
        store.set_runtime_offline_ok("work1", false).unwrap();
        store.sync_offline_hosts(now).unwrap();
        let mut ids = offline_ids(&store);
        ids.sort();
        assert_eq!(ids, vec!["host:web1".to_string(), "host:work1".to_string()]);
    }

    #[test]
    fn config_offline_ok_is_replaced_without_dropping_a_runtime_mark() {
        let store = Store::open_in_memory().unwrap();
        store.replace_config_offline_ok(&["work1".into()]).unwrap();
        store.set_runtime_offline_ok("spare", true).unwrap();
        store.replace_config_offline_ok(&["other".into()]).unwrap();
        let mut ids = store.offline_ok_nodes().unwrap();
        ids.sort();
        assert_eq!(ids, vec!["other".to_string(), "spare".to_string()]);
        assert_eq!(
            store.runtime_offline_ok().unwrap(),
            vec!["spare".to_string()]
        );
    }

    #[test]
    fn an_empty_offline_ok_set_still_opens_host_offline() {
        let store = Store::open_in_memory().unwrap();
        let now = Utc::now();
        silence(&store, "web1", now);
        assert_eq!(store.sync_offline_hosts(now).unwrap().len(), 1);
    }
}
