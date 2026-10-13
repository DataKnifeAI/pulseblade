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
             (id, ts, trigger, tier, endpoint, tool_calls, tokens, conclusion, proposed, status, detail)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
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
            ],
        )?;
        tx.commit()?;
        Ok(id)
    }

    pub fn recent_pilot_runs(&self, limit: usize) -> crate::Result<Vec<PilotRunRow>> {
        let limit = limit.clamp(1, 50) as i64;
        self.with_sql(|sql| {
            sql.query(
                "SELECT id, ts, trigger, tier, endpoint, tool_calls, tokens, conclusion, proposed, status, detail
                 FROM pilot_runs ORDER BY ts DESC LIMIT ?1",
                &[SqlVal::Int(limit)],
                map_pilot,
            )
        })
    }
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
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_a_run_and_lists_newest_first() {
        let store = Store::open_in_memory().unwrap();
        let now = Utc::now();
        let id = store
            .insert_pilot_run(
                &NewPilotRun {
                    trigger: "finding:f1".into(),
                    tier: "triage".into(),
                    endpoint: "http://127.0.0.1:9/v1".into(),
                    tool_calls: 3,
                    tokens: 120,
                    conclusion: "sshd failed; restart planned".into(),
                    proposed: vec!["systemd.restart".into()],
                    status: "ok".into(),
                    detail: "resource_explain,memory_note,action_plan".into(),
                },
                now,
            )
            .unwrap();
        let rows = store.recent_pilot_runs(10).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, id);
        assert_eq!(rows[0].proposed, vec!["systemd.restart".to_string()]);
        assert_eq!(rows[0].status, "ok");
    }
}
