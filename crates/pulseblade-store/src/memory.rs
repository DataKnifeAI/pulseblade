//! Findings, notes, and journal lines persisted beside resource state.

use crate::sql::{Sql, SqlExt, SqlVal};
use crate::{from_ms, Store, StoreError};
use chrono::{DateTime, Utc};
use pulseblade_core::{Change, Finding, FindingStatus, LogLine, Note, Severity};

/// A finding the detector wants open, before the store assigns an id.
#[derive(Debug, Clone)]
pub struct FindingDraft {
    /// Stable identity of the condition, e.g. `health\u{0}unit:h:a.service`.
    pub dedupe_key: String,
    pub resource_id: String,
    pub rule: String,
    pub severity: Severity,
    pub summary: String,
    pub context: Vec<Change>,
    pub logs: Vec<LogLine>,
}

/// Filter for [`Store::findings`]. Empty fields match everything.
#[derive(Debug, Clone, Default)]
pub struct FindingFilter {
    pub status: Option<FindingStatus>,
    pub severity: Option<Severity>,
    pub resource_id: Option<String>,
}

struct FindingRow {
    finding: Finding,
    dedupe_key: String,
}

impl Store {
    /// Raw samples for one series, oldest first, at most `limit` (the newest ones).
    pub fn recent_samples(&self, id: &str, metric: &str, limit: usize) -> crate::Result<Vec<f64>> {
        let mut values = self.with_sql(|sql| {
            sql.query(
                "SELECT value FROM samples WHERE resource_id = ?1 AND metric = ?2
                 ORDER BY ts DESC LIMIT ?3",
                &[
                    SqlVal::text(id),
                    SqlVal::text(metric),
                    SqlVal::Int(limit as i64),
                ],
                |row| row.float(0),
            )
        })?;
        values.reverse();
        Ok(values)
    }

    /// Error lines for `ids`, newest first.
    pub fn recent_logs(&self, ids: &[String], limit: usize) -> crate::Result<Vec<LogLine>> {
        Ok(self.query_logs(ids, 0, limit)?.0)
    }

    /// Error lines for one resource at or after `since`, newest first, plus the total.
    pub fn logs_since(
        &self,
        id: &str,
        since: DateTime<Utc>,
        limit: usize,
    ) -> crate::Result<(Vec<LogLine>, usize)> {
        self.query_logs(&[id.to_string()], since.timestamp_millis(), limit)
    }

    fn query_logs(
        &self,
        ids: &[String],
        since_ms: i64,
        limit: usize,
    ) -> crate::Result<(Vec<LogLine>, usize)> {
        if ids.is_empty() || limit == 0 {
            return Ok((Vec::new(), 0));
        }
        let in_list = (0..ids.len())
            .map(|i| format!("?{}", i + 2))
            .collect::<Vec<_>>()
            .join(",");
        let limit_slot = ids.len() + 2;
        let mut args = Vec::with_capacity(ids.len() + 1);
        args.push(SqlVal::Int(since_ms));
        for id in ids {
            args.push(SqlVal::text(id));
        }
        self.with_sql(|sql| {
            let total = sql.query_one(
                &format!("SELECT COUNT(*) FROM logs WHERE ts >= ?1 AND resource_id IN ({in_list})"),
                &args,
                |row| row.int(0),
            )?;
            args.push(SqlVal::Int(limit as i64));
            let lines = sql.query(
                &format!(
                    "SELECT ts, resource_id, message FROM logs
                     WHERE ts >= ?1 AND resource_id IN ({in_list})
                     ORDER BY ts DESC LIMIT ?{limit_slot}"
                ),
                &args,
                |row| {
                    Ok(LogLine {
                        ts: from_ms(row.int(0)?),
                        resource_id: row.text(1)?,
                        message: row.text(2)?,
                    })
                },
            )?;
            Ok((lines, total as usize))
        })
    }

    pub fn open_finding_count(&self) -> crate::Result<u64> {
        self.with_sql(|sql| {
            Ok(sql.query_one(
                "SELECT COUNT(*) FROM findings WHERE status = 'open'",
                &[],
                |row| row.int(0),
            )? as u64)
        })
    }

    /// Milliseconds of the newest finding write, or 0. Moves on open, ack, and resolve.
    pub fn findings_token(&self) -> crate::Result<i64> {
        self.with_sql(|sql| {
            sql.query_one(
                "SELECT COALESCE(MAX(updated_at), 0) FROM findings",
                &[],
                |row| row.int(0),
            )
        })
    }

    pub fn findings(
        &self,
        filter: &FindingFilter,
        limit: usize,
    ) -> crate::Result<(Vec<Finding>, usize)> {
        let args = [
            SqlVal::opt_text(filter.status.map(|s| s.as_str().to_string())),
            SqlVal::opt_text(filter.severity.map(|s| s.as_str().to_string())),
            SqlVal::opt_text(filter.resource_id.clone()),
        ];
        self.with_sql(|sql| {
            let total = sql.query_one(
                "SELECT COUNT(*) FROM findings
                 WHERE (CAST(?1 AS TEXT) IS NULL OR status = ?1)
                   AND (CAST(?2 AS TEXT) IS NULL OR severity = ?2)
                   AND (CAST(?3 AS TEXT) IS NULL OR resource_id = ?3)",
                &args,
                |row| row.int(0),
            )?;
            let mut listed = args.to_vec();
            listed.push(SqlVal::Int(limit as i64));
            let rows = sql.query(
                "SELECT id, dedupe_key, resource_id, rule, severity, status, summary,
                        opened_at, context, logs
                 FROM findings
                 WHERE (CAST(?1 AS TEXT) IS NULL OR status = ?1)
                   AND (CAST(?2 AS TEXT) IS NULL OR severity = ?2)
                   AND (CAST(?3 AS TEXT) IS NULL OR resource_id = ?3)
                 ORDER BY CASE severity WHEN 'critical' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END,
                          opened_at DESC
                 LIMIT ?4",
                &listed,
                map_finding,
            )?;
            Ok((
                rows.into_iter().map(|row| row.finding).collect(),
                total as usize,
            ))
        })
    }

    pub fn finding(&self, id: &str) -> crate::Result<Option<Finding>> {
        self.with_sql(|sql| {
            Ok(sql
                .query_opt(
                    "SELECT id, dedupe_key, resource_id, rule, severity, status, summary,
                            opened_at, context, logs
                     FROM findings WHERE id = ?1",
                    &[SqlVal::text(id)],
                    map_finding,
                )?
                .map(|row| row.finding))
        })
    }

    /// Open `drafts` and resolve any open or acked finding whose condition cleared.
    /// Returns findings that were newly opened on this pass.
    pub fn reconcile_findings(
        &self,
        drafts: &[FindingDraft],
        now: DateTime<Utc>,
    ) -> crate::Result<Vec<Finding>> {
        self.reconcile_filtered(drafts, now, None)
    }

    /// Same as [`reconcile_findings`](Self::reconcile_findings) for one rule.
    /// Findings with any other rule stay as they are.
    pub fn reconcile_rule(
        &self,
        rule: &str,
        drafts: &[FindingDraft],
        now: DateTime<Utc>,
    ) -> crate::Result<Vec<Finding>> {
        self.reconcile_filtered(drafts, now, Some(rule))
    }

    fn reconcile_filtered(
        &self,
        drafts: &[FindingDraft],
        now: DateTime<Utc>,
        rule: Option<&str>,
    ) -> crate::Result<Vec<Finding>> {
        let ms = now.timestamp_millis();
        let rule_arg = SqlVal::opt_text(rule.map(|s| s.to_string()));
        self.transaction(|sql| {
            // CAST: a bare `$1 IS NULL` is untyped, and Postgres rejects the
            // parse with SQLSTATE 42P08 before the value is bound.
            let mut open = sql.query(
                "SELECT id, dedupe_key, resource_id, rule, severity, status, summary,
                        opened_at, context, logs
                 FROM findings WHERE status IN ('open', 'acked')
                   AND (CAST(?1 AS TEXT) IS NULL OR rule = ?1)",
                &[rule_arg],
                map_finding,
            )?;
            let mut created = Vec::new();
            for draft in drafts {
                if rule.is_some_and(|rule| draft.rule != rule) {
                    continue;
                }
                let context = serde_json::to_string(&draft.context)?;
                let logs = serde_json::to_string(&draft.logs)?;
                if let Some(idx) = open.iter().position(|r| r.dedupe_key == draft.dedupe_key) {
                    let row = open.remove(idx);
                    sql.execute(
                        "UPDATE findings
                         SET severity = ?1, summary = ?2, context = ?3, logs = ?4, updated_at = ?5
                         WHERE id = ?6",
                        &[
                            SqlVal::text(draft.severity.as_str()),
                            SqlVal::text(&draft.summary),
                            SqlVal::text(context),
                            SqlVal::text(logs),
                            SqlVal::Int(ms),
                            SqlVal::text(&row.finding.id),
                        ],
                    )?;
                } else {
                    let id = fresh_finding_id(sql, &draft.dedupe_key, ms)?;
                    sql.execute(
                        "INSERT INTO findings
                            (id, dedupe_key, resource_id, rule, severity, status, summary,
                             opened_at, updated_at, context, logs)
                         VALUES (?1, ?2, ?3, ?4, ?5, 'open', ?6, ?7, ?7, ?8, ?9)",
                        &[
                            SqlVal::text(&id),
                            SqlVal::text(&draft.dedupe_key),
                            SqlVal::text(&draft.resource_id),
                            SqlVal::text(&draft.rule),
                            SqlVal::text(draft.severity.as_str()),
                            SqlVal::text(&draft.summary),
                            SqlVal::Int(ms),
                            SqlVal::text(context),
                            SqlVal::text(logs),
                        ],
                    )?;
                    created.push(Finding {
                        id,
                        resource_id: draft.resource_id.clone(),
                        rule: draft.rule.clone(),
                        severity: draft.severity,
                        status: FindingStatus::Open,
                        summary: draft.summary.clone(),
                        opened_at: from_ms(ms),
                        context: draft.context.clone(),
                        logs: draft.logs.clone(),
                    });
                }
            }
            for row in open {
                sql.execute(
                    "UPDATE findings SET status = 'resolved', updated_at = ?1 WHERE id = ?2",
                    &[SqlVal::Int(ms), SqlVal::text(row.finding.id)],
                )?;
            }
            Ok(created)
        })
    }

    /// Mark an open or already-acked finding acked. Resolved findings are rejected.
    pub fn ack_finding(&self, id: &str, now: DateTime<Utc>) -> crate::Result<Finding> {
        let Some(mut finding) = self.finding(id)? else {
            return Err(StoreError::UnknownFinding(id.to_string()));
        };
        if finding.status == FindingStatus::Resolved {
            return Err(StoreError::Invalid(format!("finding `{id}` is resolved")));
        }
        self.with_sql(|sql| {
            sql.execute(
                "UPDATE findings SET status = 'acked', updated_at = ?1 WHERE id = ?2",
                &[SqlVal::Int(now.timestamp_millis()), SqlVal::text(id)],
            )?;
            Ok(())
        })?;
        finding.status = FindingStatus::Acked;
        Ok(finding)
    }

    pub fn insert_note(
        &self,
        resource_id: &str,
        author: &str,
        text: &str,
        now: DateTime<Utc>,
    ) -> crate::Result<Note> {
        let id = self.with_sql(|sql| {
            sql.execute(
                "INSERT INTO notes (resource_id, author, text, created_at) VALUES (?1, ?2, ?3, ?4)",
                &[
                    SqlVal::text(resource_id),
                    SqlVal::text(author),
                    SqlVal::text(text),
                    SqlVal::Int(now.timestamp_millis()),
                ],
            )?;
            sql.last_id()
        })?;
        Ok(Note {
            id: format!("n{id}"),
            resource_id: resource_id.to_string(),
            author: author.to_string(),
            text: text.to_string(),
            created_at: from_ms(now.timestamp_millis()),
        })
    }

    pub fn notes_for(&self, resource_id: &str, limit: usize) -> crate::Result<Vec<Note>> {
        self.with_sql(|sql| {
            sql.query(
                "SELECT id, resource_id, author, text, created_at FROM notes
                 WHERE resource_id = ?1 ORDER BY created_at DESC LIMIT ?2",
                &[SqlVal::text(resource_id), SqlVal::Int(limit as i64)],
                map_note,
            )
        })
    }

    /// Case-insensitive substring match on note text or resource id, newest first.
    pub fn search_notes(&self, query: &str, limit: usize) -> crate::Result<(Vec<Note>, usize)> {
        let pattern = like_contains(query);
        self.with_sql(|sql| {
            let total = sql.query_one(
                "SELECT COUNT(*) FROM notes
                 WHERE text LIKE ?1 ESCAPE '\\' OR resource_id LIKE ?1 ESCAPE '\\'",
                &[SqlVal::text(&pattern)],
                |row| row.int(0),
            )?;
            let notes = sql.query(
                "SELECT id, resource_id, author, text, created_at FROM notes
                 WHERE text LIKE ?1 ESCAPE '\\' OR resource_id LIKE ?1 ESCAPE '\\'
                 ORDER BY created_at DESC LIMIT ?2",
                &[SqlVal::text(pattern), SqlVal::Int(limit as i64)],
                map_note,
            )?;
            Ok((notes, total as usize))
        })
    }
}

/// Ids include the millisecond, so a condition that clears and returns inside
/// the same millisecond needs a fresh suffix.
fn fresh_finding_id(sql: &mut dyn Sql, dedupe_key: &str, ms: i64) -> crate::Result<String> {
    for n in 0..64 {
        let id = format!("f{:08x}{ms:x}{n}", fnv(dedupe_key) as u32);
        let taken = sql.query_opt(
            "SELECT 1 FROM findings WHERE id = ?1",
            &[SqlVal::text(&id)],
            |row| row.int(0),
        )?;
        if taken.is_none() {
            return Ok(id);
        }
    }
    Err(StoreError::Invalid(
        "could not allocate a finding id".into(),
    ))
}

fn fnv(s: &str) -> u64 {
    let mut h = 0xcbf29ce484222325u64;
    for b in s.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

fn like_contains(query: &str) -> String {
    let mut out = String::from("%");
    for c in query.chars() {
        if c == '\\' || c == '%' || c == '_' {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('%');
    out
}

fn map_finding(row: &dyn crate::sql::SqlRow) -> crate::Result<FindingRow> {
    let severity = row.text(4)?;
    let status = row.text(5)?;
    let context = row.text(8)?;
    let logs = row.text(9)?;
    Ok(FindingRow {
        finding: Finding {
            id: row.text(0)?,
            resource_id: row.text(2)?,
            rule: row.text(3)?,
            severity: severity.parse().map_err(StoreError::Corrupt)?,
            status: status.parse().map_err(StoreError::Corrupt)?,
            summary: row.text(6)?,
            opened_at: from_ms(row.int(7)?),
            context: serde_json::from_str(&context)?,
            logs: serde_json::from_str(&logs)?,
        },
        dedupe_key: row.text(1)?,
    })
}

fn map_note(row: &dyn crate::sql::SqlRow) -> crate::Result<Note> {
    Ok(Note {
        id: format!("n{}", row.int(0)?),
        resource_id: row.text(1)?,
        author: row.text(2)?,
        text: row.text(3)?,
        created_at: from_ms(row.int(4)?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulseblade_core::{ChangeKind, Health, Resource, ResourceKind};
    use serde_json::json;

    fn draft(severity: Severity) -> FindingDraft {
        FindingDraft {
            dedupe_key: "health\0unit:h:a.service".into(),
            resource_id: "unit:h:a.service".into(),
            rule: "health".into(),
            severity,
            summary: "a.service is failed".into(),
            context: vec![Change {
                seq: 2,
                ts: Utc::now(),
                resource_id: "unit:h:a.service".into(),
                kind: ChangeKind::Changed,
                field: Some("health".into()),
                before: Some(json!("ok")),
                after: Some(json!("failed")),
            }],
            logs: vec![LogLine {
                ts: Utc::now(),
                resource_id: "unit:h:a.service".into(),
                message: "start request repeated too quickly".into(),
            }],
        }
    }

    #[test]
    fn reconcile_ack_and_resolve() {
        let store = Store::open_in_memory().unwrap();
        let now = Utc::now();
        let created = store
            .reconcile_findings(&[draft(Severity::Critical)], now)
            .unwrap();
        assert_eq!(created.len(), 1);
        assert_eq!(created[0].logs.len(), 1);
        assert!(created[0]
            .context
            .iter()
            .any(|c| c.field.as_deref() == Some("health")));

        let again = store
            .reconcile_findings(&[draft(Severity::Critical)], now)
            .unwrap();
        assert!(again.is_empty(), "an open finding is not duplicated");

        let acked = store.ack_finding(&created[0].id, now).unwrap();
        assert_eq!(acked.status, FindingStatus::Acked);
        let still = store
            .reconcile_findings(&[draft(Severity::Warning)], now)
            .unwrap();
        assert!(still.is_empty());
        assert_eq!(
            store.finding(&created[0].id).unwrap().unwrap().status,
            FindingStatus::Acked
        );

        assert!(store.reconcile_findings(&[], now).unwrap().is_empty());
        assert_eq!(
            store.finding(&created[0].id).unwrap().unwrap().status,
            FindingStatus::Resolved
        );
        assert!(store.ack_finding(&created[0].id, now).is_err());

        let reopened = store
            .reconcile_findings(&[draft(Severity::Critical)], now)
            .unwrap();
        assert_eq!(reopened.len(), 1);
        assert_ne!(reopened[0].id, created[0].id);
    }

    #[test]
    fn notes_round_trip_and_search() {
        let store = Store::open_in_memory().unwrap();
        store
            .apply(
                "host",
                Utc::now(),
                &crate::Observation {
                    resources: vec![
                        Resource::new("host:h", ResourceKind::Host, "h").health(Health::Ok)
                    ],
                    samples: vec![],
                    logs: vec![LogLine {
                        ts: Utc::now(),
                        resource_id: "host:h".into(),
                        message: "disk quota exceeded".into(),
                    }],
                },
            )
            .unwrap();
        let note = store
            .insert_note("host:h", "agent", "full after the backup", Utc::now())
            .unwrap();
        assert!(note.id.starts_with('n'));
        let (found, total) = store.search_notes("backup", 10).unwrap();
        assert_eq!(total, 1);
        assert_eq!(found[0].text, note.text);
        let (logs, n) = store
            .logs_since("host:h", Utc::now() - chrono::Duration::hours(1), 10)
            .unwrap();
        assert_eq!(n, 1);
        assert_eq!(logs[0].message, "disk quota exceeded");
        assert!(store.search_notes("100%", 10).unwrap().0.is_empty());
    }
}
