//! Pilot investigation records. The pilot writes one row per wake; steady state
//! does not insert anything.

use chrono::{DateTime, Utc};
use rusqlite::{params, OptionalExtension};
use serde::Serialize;

use crate::sql::{SqlExt, SqlVal};
use crate::{from_ms, Store, StoreError};

/// One finished (or budget-stopped) pilot investigation.
pub struct NewPilotRun {
    pub trigger: String,
    pub tier: String,
    pub endpoint: String,
    pub tool_calls: u32,
    pub tokens: u32,
    pub conclusion: String,
    /// Action names proposed via `action_plan`.
    pub proposed: Vec<String>,
    pub status: String,
    /// Tool names the model called, in order.
    pub detail: String,
    /// Resource ids a model run looked at. Empty when no model was called.
    pub subjects: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PilotRunRow {
    pub id: String,
    pub ts: DateTime<Utc>,
    pub trigger: String,
    pub tier: String,
    pub endpoint: String,
    pub tool_calls: u32,
    pub tokens: u32,
    pub conclusion: String,
    pub proposed: Vec<String>,
    pub status: String,
    pub detail: String,
    pub subjects: Vec<String>,
}

/// A model run counted by the pilot's spend caps.
#[derive(Debug, Clone, PartialEq)]
pub struct PilotSpend {
    pub ts: DateTime<Utc>,
    pub tokens: u32,
    pub subjects: Vec<String>,
    pub status: String,
}

impl Store {
    pub fn insert_pilot_run(
        &self,
        draft: &NewPilotRun,
        now: DateTime<Utc>,
    ) -> crate::Result<String> {
        let ms = now.timestamp_millis();
        let proposed = serde_json::to_string(&draft.proposed)?;
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let id = fresh_pilot_id(&tx, ms)?;
        tx.execute(
            "INSERT INTO pilot_runs
             (id, ts, trigger, tier, endpoint, tool_calls, tokens, conclusion, proposed, status, detail, subjects)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                id,
                ms,
                draft.trigger,
                draft.tier,
                draft.endpoint,
                draft.tool_calls,
                draft.tokens,
                draft.conclusion,
                proposed,
                draft.status,
                draft.detail,
                draft.subjects.join("\n"),
            ],
        )?;
        tx.commit()?;
        Ok(id)
    }

    pub fn recent_pilot_runs(&self, limit: usize) -> crate::Result<Vec<PilotRunRow>> {
        let limit = limit.clamp(1, 50) as i64;
        self.with_sql(|sql| {
            sql.query(
                "SELECT id, ts, trigger, tier, endpoint, tool_calls, tokens, conclusion, proposed, status, detail, subjects
                 FROM pilot_runs ORDER BY ts DESC LIMIT ?1",
                &[SqlVal::Int(limit)],
                map_pilot,
            )
        })
    }

    /// Model runs since `since`, oldest first. Remedy filings and held wakes
    /// called no model and are left out.
    pub fn pilot_spend_since(&self, since: DateTime<Utc>) -> crate::Result<Vec<PilotSpend>> {
        self.with_sql(|sql| {
            sql.query(
                "SELECT ts, tokens, subjects, status FROM pilot_runs
                 WHERE ts >= ?1 AND status NOT IN ('remedy', 'capped')
                 ORDER BY ts",
                &[SqlVal::Int(since.timestamp_millis())],
                |row| {
                    Ok(PilotSpend {
                        ts: from_ms(row.int(0)?),
                        tokens: u32::try_from(row.int(1)?).unwrap_or(0),
                        subjects: split_subjects(&row.text(2)?),
                        status: row.text(3)?,
                    })
                },
            )
        })
    }
}

fn split_subjects(text: &str) -> Vec<String> {
    text.split('\n')
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

fn fresh_pilot_id(conn: &rusqlite::Connection, ms: i64) -> crate::Result<String> {
    for n in 0..64 {
        let id = format!("p{ms:x}{n}");
        let taken: Option<i64> = conn
            .query_row("SELECT 1 FROM pilot_runs WHERE id = ?1", [&id], |r| {
                r.get(0)
            })
            .optional()?;
        if taken.is_none() {
            return Ok(id);
        }
    }
    Err(StoreError::Invalid(
        "could not allocate a pilot run id".into(),
    ))
}

fn map_pilot(row: &dyn crate::sql::SqlRow) -> crate::Result<PilotRunRow> {
    let proposed: Vec<String> = serde_json::from_str(&row.text(8)?).unwrap_or_default();
    Ok(PilotRunRow {
        id: row.text(0)?,
        ts: from_ms(row.int(1)?),
        trigger: row.text(2)?,
        tier: row.text(3)?,
        endpoint: row.text(4)?,
        tool_calls: u32::try_from(row.int(5)?).unwrap_or(0),
        tokens: u32::try_from(row.int(6)?).unwrap_or(0),
        conclusion: row.text(7)?,
        proposed,
        status: row.text(9)?,
        detail: row.text(10)?,
        subjects: split_subjects(&row.text(11)?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn draft(status: &str, tokens: u32, subjects: &[&str]) -> NewPilotRun {
        NewPilotRun {
            trigger: "finding:f1".into(),
            tier: "triage".into(),
            endpoint: "http://127.0.0.1:9/v1".into(),
            tool_calls: 3,
            tokens,
            conclusion: "sshd failed; restart planned".into(),
            proposed: vec!["systemd.restart".into()],
            status: status.into(),
            detail: "resource_explain,memory_note,action_plan".into(),
            subjects: subjects.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn records_a_run_and_lists_newest_first() {
        let store = Store::open_in_memory().unwrap();
        let now = Utc::now();
        let id = store
            .insert_pilot_run(&draft("ok", 120, &["unit:web1:sshd.service"]), now)
            .unwrap();
        let rows = store.recent_pilot_runs(10).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, id);
        assert_eq!(rows[0].proposed, vec!["systemd.restart".to_string()]);
        assert_eq!(rows[0].status, "ok");
        assert_eq!(rows[0].subjects, vec!["unit:web1:sshd.service".to_string()]);
    }

    #[test]
    fn spend_counts_model_runs_only() {
        let store = Store::open_in_memory().unwrap();
        let now = Utc::now();
        let old = now - chrono::Duration::hours(30);
        store
            .insert_pilot_run(&draft("ok", 900, &["host:a"]), old)
            .unwrap();
        store
            .insert_pilot_run(&draft("ok", 500, &["host:a", "host:b"]), now)
            .unwrap();
        store
            .insert_pilot_run(&draft("error", 0, &["host:c"]), now)
            .unwrap();
        store
            .insert_pilot_run(&draft("remedy", 0, &[]), now)
            .unwrap();
        store
            .insert_pilot_run(&draft("capped", 0, &[]), now)
            .unwrap();
        let spend = store
            .pilot_spend_since(now - chrono::Duration::hours(24))
            .unwrap();
        assert_eq!(spend.len(), 2);
        assert_eq!(
            spend[0].subjects,
            vec!["host:a".to_string(), "host:b".to_string()]
        );
        assert_eq!(spend[1].status, "error");
    }
}
