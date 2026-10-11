//! Findings, notes, and journal lines persisted beside resource state.

use chrono::{DateTime, Utc};
use pulseblade_core::{Change, Finding, FindingStatus, LogLine, Note, Severity};
use rusqlite::{params, OptionalExtension};

use crate::{from_ms, Store, StoreError};

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
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT value FROM samples WHERE resource_id = ?1 AND metric = ?2
             ORDER BY ts DESC LIMIT ?3",
        )?;
        let rows = stmt.query_map(params![id, metric, limit as i64], |r| r.get::<_, f64>(0))?;
        let mut values: Vec<f64> = rows.collect::<rusqlite::Result<_>>()?;
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
        let mut bound: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::with_capacity(ids.len() + 1);
        bound.push(Box::new(since_ms));
        for id in ids {
            bound.push(Box::new(id.clone()));
        }
        let conn = self.conn();
        let total: i64 = conn.query_row(
            &format!("SELECT COUNT(*) FROM logs WHERE ts >= ?1 AND resource_id IN ({in_list})"),
            rusqlite::params_from_iter(bound.iter().map(|b| b.as_ref())),
            |r| r.get(0),
        )?;
        bound.push(Box::new(limit as i64));
        let mut stmt = conn.prepare(&format!(
            "SELECT ts, resource_id, message FROM logs
             WHERE ts >= ?1 AND resource_id IN ({in_list})
             ORDER BY ts DESC LIMIT ?{limit_slot}"
        ))?;
        let rows = stmt.query_map(
            rusqlite::params_from_iter(bound.iter().map(|b| b.as_ref())),
            |r| {
                Ok(LogLine {
                    ts: from_ms(r.get(0)?),
                    resource_id: r.get(1)?,
                    message: r.get(2)?,
                })
            },
        )?;
        Ok((rows.collect::<rusqlite::Result<_>>()?, total as usize))
    }

    pub fn open_finding_count(&self) -> crate::Result<u64> {
        Ok(self.conn().query_row(
            "SELECT COUNT(*) FROM findings WHERE status = 'open'",
            [],
            |r| r.get::<_, i64>(0),
        )? as u64)
    }

    /// Milliseconds of the newest finding write, or 0. Moves on open, ack, and resolve.
    pub fn findings_token(&self) -> crate::Result<i64> {
        self.conn()
            .query_row(
                "SELECT COALESCE(MAX(updated_at), 0) FROM findings",
                [],
                |r| r.get(0),
            )
            .map_err(Into::into)
    }

    pub fn findings(
        &self,
        filter: &FindingFilter,
        limit: usize,
    ) -> crate::Result<(Vec<Finding>, usize)> {
        let status = filter.status.map(FindingStatus::as_str);
        let severity = filter.severity.map(Severity::as_str);
        let conn = self.conn();
        let total: i64 = conn.query_row(
            "SELECT COUNT(*) FROM findings
             WHERE (?1 IS NULL OR status = ?1)
               AND (?2 IS NULL OR severity = ?2)
               AND (?3 IS NULL OR resource_id = ?3)",
            params![status, severity, filter.resource_id],
            |r| r.get(0),
        )?;
        let mut stmt = conn.prepare(
            "SELECT id, dedupe_key, resource_id, rule, severity, status, summary,
                    opened_at, context, logs
             FROM findings
             WHERE (?1 IS NULL OR status = ?1)
               AND (?2 IS NULL OR severity = ?2)
               AND (?3 IS NULL OR resource_id = ?3)
             ORDER BY CASE severity WHEN 'critical' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END,
                      opened_at DESC
             LIMIT ?4",
        )?;
        let rows = stmt.query_map(
            params![status, severity, filter.resource_id, limit as i64],
            row_to_finding,
        )?;
        let mut findings = Vec::new();
        for row in rows {
            findings.push(row??.finding);
        }
        Ok((findings, total as usize))
    }

    pub fn finding(&self, id: &str) -> crate::Result<Option<Finding>> {
        Ok(self
            .conn()
            .query_row(
                "SELECT id, dedupe_key, resource_id, rule, severity, status, summary,
                        opened_at, context, logs
                 FROM findings WHERE id = ?1",
                [id],
                row_to_finding,
            )
            .optional()?
            .transpose()?
            .map(|row| row.finding))
    }

    /// Open `drafts` and resolve any open or acked finding whose condition cleared.
    /// Returns findings that were newly opened on this pass.
    pub fn reconcile_findings(
        &self,
        drafts: &[FindingDraft],
        now: DateTime<Utc>,
    ) -> crate::Result<Vec<Finding>> {
        let ms = now.timestamp_millis();
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let mut open: Vec<FindingRow> = {
            let mut stmt = tx.prepare(
                "SELECT id, dedupe_key, resource_id, rule, severity, status, summary,
                        opened_at, context, logs
                 FROM findings WHERE status IN ('open', 'acked')",
            )?;
            let rows = stmt.query_map([], row_to_finding)?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row??);
            }
            out
        };
        let mut created = Vec::new();
        for draft in drafts {
            let context = serde_json::to_string(&draft.context)?;
            let logs = serde_json::to_string(&draft.logs)?;
            if let Some(idx) = open.iter().position(|r| r.dedupe_key == draft.dedupe_key) {
                let row = open.remove(idx);
                tx.execute(
                    "UPDATE findings
                     SET severity = ?1, summary = ?2, context = ?3, logs = ?4, updated_at = ?5
                     WHERE id = ?6",
                    params![
                        draft.severity.as_str(),
                        draft.summary,
                        context,
                        logs,
                        ms,
                        row.finding.id
                    ],
                )?;
            } else {
                let id = fresh_finding_id(&tx, &draft.dedupe_key, ms)?;
                tx.execute(
                    "INSERT INTO findings
                        (id, dedupe_key, resource_id, rule, severity, status, summary,
                         opened_at, updated_at, context, logs)
                     VALUES (?1, ?2, ?3, ?4, ?5, 'open', ?6, ?7, ?7, ?8, ?9)",
                    params![
                        id,
                        draft.dedupe_key,
                        draft.resource_id,
                        draft.rule,
                        draft.severity.as_str(),
                        draft.summary,
                        ms,
                        context,
                        logs,
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
            tx.execute(
                "UPDATE findings SET status = 'resolved', updated_at = ?1 WHERE id = ?2",
                params![ms, row.finding.id],
            )?;
        }
        tx.commit()?;
        Ok(created)
    }

    /// Mark an open or already-acked finding acked. Resolved findings are rejected.
    pub fn ack_finding(&self, id: &str, now: DateTime<Utc>) -> crate::Result<Finding> {
        let Some(mut finding) = self.finding(id)? else {
            return Err(StoreError::UnknownFinding(id.to_string()));
        };
        if finding.status == FindingStatus::Resolved {
            return Err(StoreError::Invalid(format!("finding `{id}` is resolved")));
        }
        self.conn().execute(
            "UPDATE findings SET status = 'acked', updated_at = ?1 WHERE id = ?2",
            params![now.timestamp_millis(), id],
        )?;
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
        let conn = self.conn();
        conn.execute(
            "INSERT INTO notes (resource_id, author, text, created_at) VALUES (?1, ?2, ?3, ?4)",
            params![resource_id, author, text, now.timestamp_millis()],
        )?;
        let id = conn.last_insert_rowid();
        Ok(Note {
            id: format!("n{id}"),
            resource_id: resource_id.to_string(),
            author: author.to_string(),
            text: text.to_string(),
            created_at: from_ms(now.timestamp_millis()),
        })
    }

    pub fn notes_for(&self, resource_id: &str, limit: usize) -> crate::Result<Vec<Note>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT id, resource_id, author, text, created_at FROM notes
             WHERE resource_id = ?1 ORDER BY created_at DESC LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![resource_id, limit as i64], row_to_note)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    /// Case-insensitive substring match on note text or resource id, newest first.
    pub fn search_notes(&self, query: &str, limit: usize) -> crate::Result<(Vec<Note>, usize)> {
        let pattern = like_contains(query);
        let conn = self.conn();
        let total: i64 = conn.query_row(
            "SELECT COUNT(*) FROM notes
             WHERE text LIKE ?1 ESCAPE '\\' OR resource_id LIKE ?1 ESCAPE '\\'",
            [&pattern],
            |r| r.get(0),
        )?;
        let mut stmt = conn.prepare(
            "SELECT id, resource_id, author, text, created_at FROM notes
             WHERE text LIKE ?1 ESCAPE '\\' OR resource_id LIKE ?1 ESCAPE '\\'
             ORDER BY created_at DESC LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![pattern, limit as i64], row_to_note)?;
        Ok((rows.collect::<rusqlite::Result<_>>()?, total as usize))
    }
}

/// Ids include the millisecond, so a condition that clears and returns inside
/// the same millisecond needs a fresh suffix.
fn fresh_finding_id(
    conn: &rusqlite::Connection,
    dedupe_key: &str,
    ms: i64,
) -> crate::Result<String> {
    for n in 0..64 {
        let id = format!("f{:08x}{ms:x}{n}", fnv(dedupe_key) as u32);
        let taken: Option<i64> = conn
            .query_row("SELECT 1 FROM findings WHERE id = ?1", [&id], |r| r.get(0))
            .optional()?;
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

fn row_to_finding(row: &rusqlite::Row<'_>) -> rusqlite::Result<crate::Result<FindingRow>> {
    let id: String = row.get(0)?;
    let dedupe_key: String = row.get(1)?;
    let resource_id: String = row.get(2)?;
    let rule: String = row.get(3)?;
    let severity: String = row.get(4)?;
    let status: String = row.get(5)?;
    let summary: String = row.get(6)?;
    let opened_at: i64 = row.get(7)?;
    let context: String = row.get(8)?;
    let logs: String = row.get(9)?;
    Ok((|| {
        Ok(FindingRow {
            finding: Finding {
                id,
                resource_id,
                rule,
                severity: severity.parse().map_err(StoreError::Corrupt)?,
                status: status.parse().map_err(StoreError::Corrupt)?,
                summary,
                opened_at: from_ms(opened_at),
                context: serde_json::from_str(&context)?,
                logs: serde_json::from_str(&logs)?,
            },
            dedupe_key,
        })
    })())
}

fn row_to_note(row: &rusqlite::Row<'_>) -> rusqlite::Result<Note> {
    let id: i64 = row.get(0)?;
    Ok(Note {
        id: format!("n{id}"),
        resource_id: row.get(1)?,
        author: row.get(2)?,
        text: row.get(3)?,
        created_at: from_ms(row.get(4)?),
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
