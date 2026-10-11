//! Action runs and the hash-chained audit log (M3).

use chrono::{DateTime, Utc};
use pulseblade_core::{ActionRun, ActionRunStatus, RiskTier};
use rusqlite::{params, OptionalExtension};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::{from_ms, Store, StoreError};

const GENESIS: &str = "0000000000000000000000000000000000000000000000000000000000000000";

/// Status change applied together with one audit event.
pub struct ActionTransition<'a> {
    pub from: ActionRunStatus,
    pub to: ActionRunStatus,
    pub output: Option<&'a str>,
    pub expires_at: Option<DateTime<Utc>>,
    pub event: &'a str,
}

/// A planned action before the store assigns an id.
pub struct NewAction {
    pub action: String,
    pub resource_id: String,
    pub params: Value,
    pub idempotency_key: String,
    pub tier: RiskTier,
    pub dry_run: bool,
}

impl Store {
    /// An in-flight run with this idempotency key, if one exists.
    pub fn open_action_by_key(&self, key: &str) -> crate::Result<Option<ActionRun>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT id, action, resource_id, params, idempotency_key, tier, status, dry_run,
                    output, created_at, updated_at, expires_at
             FROM action_runs
             WHERE idempotency_key = ?1 AND status IN ('planned', 'awaiting_approval', 'running')
             ORDER BY created_at DESC LIMIT 1",
        )?;
        let mut rows = stmt.query([key])?;
        match rows.next()? {
            Some(row) => Ok(Some(row_to_action(row)??)),
            None => Ok(None),
        }
    }

    pub fn action(&self, id: &str) -> crate::Result<Option<ActionRun>> {
        let conn = self.conn();
        conn.query_row(
            "SELECT id, action, resource_id, params, idempotency_key, tier, status, dry_run,
                    output, created_at, updated_at, expires_at
             FROM action_runs WHERE id = ?1",
            [id],
            row_to_action,
        )
        .optional()?
        .transpose()
    }

    /// Recent runs, newest first. `resource_id` limits the list to one resource.
    pub fn recent_actions(
        &self,
        resource_id: Option<&str>,
        limit: usize,
    ) -> crate::Result<Vec<ActionRun>> {
        let limit = limit.clamp(1, 100) as i64;
        let conn = self.conn();
        let mut out = Vec::new();
        if let Some(id) = resource_id {
            let mut stmt = conn.prepare(
                "SELECT id, action, resource_id, params, idempotency_key, tier, status, dry_run,
                        output, created_at, updated_at, expires_at
                 FROM action_runs WHERE resource_id = ?1
                 ORDER BY created_at DESC LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![id, limit], row_to_action)?;
            for row in rows {
                out.push(row??);
            }
        } else {
            let mut stmt = conn.prepare(
                "SELECT id, action, resource_id, params, idempotency_key, tier, status, dry_run,
                        output, created_at, updated_at, expires_at
                 FROM action_runs ORDER BY created_at DESC LIMIT ?1",
            )?;
            let rows = stmt.query_map([limit], row_to_action)?;
            for row in rows {
                out.push(row??);
            }
        }
        Ok(out)
    }

    pub fn count_actions_since(&self, action: &str, since: DateTime<Utc>) -> crate::Result<u32> {
        let conn = self.conn();
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM action_runs WHERE action = ?1 AND created_at >= ?2",
            params![action, since.timestamp_millis()],
            |r| r.get(0),
        )?;
        Ok(u32::try_from(n).unwrap_or(u32::MAX))
    }

    pub fn awaiting_action_count(&self) -> crate::Result<u64> {
        let conn = self.conn();
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM action_runs WHERE status = 'awaiting_approval'",
            [],
            |r| r.get(0),
        )?;
        Ok(u64::try_from(n).unwrap_or(0))
    }

    /// Insert a planned run and the matching audit event.
    pub fn insert_action(&self, draft: &NewAction, now: DateTime<Utc>) -> crate::Result<ActionRun> {
        let ms = now.timestamp_millis();
        let id = {
            let mut conn = self.conn();
            let tx = conn.transaction()?;
            let id = fresh_action_id(&tx, ms)?;
            let params_json = serde_json::to_string(&draft.params)?;
            tx.execute(
                "INSERT INTO action_runs
                (id, action, resource_id, params, idempotency_key, tier, status, dry_run,
                 output, created_at, updated_at, expires_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'planned', ?7, NULL, ?8, ?8, NULL)",
                params![
                    id,
                    draft.action,
                    draft.resource_id,
                    params_json,
                    draft.idempotency_key,
                    tier_str(draft.tier),
                    i64::from(draft.dry_run),
                    ms,
                ],
            )?;
            let payload = json!({
                "event": "plan",
                "run_id": id,
                "action": draft.action,
                "resource_id": draft.resource_id,
                "status": "planned",
                "tier": tier_str(draft.tier),
                "dry_run": draft.dry_run,
                "ts": ms,
            })
            .to_string();
            append_audit(&tx, ms, &id, "plan", &payload)?;
            tx.commit()?;
            id
        };
        self.action(&id)?
            .ok_or_else(|| StoreError::Corrupt(format!("missing action {id}")))
    }

    /// Move a run from `from` to `to` and append an audit event.
    /// Returns the current run unchanged when the status is no longer `from`.
    pub fn transition_action(
        &self,
        id: &str,
        step: ActionTransition<'_>,
    ) -> crate::Result<ActionRun> {
        let now = Utc::now();
        let ms = now.timestamp_millis();
        let exp = step.expires_at.map(|t| t.timestamp_millis());
        let ActionTransition {
            from,
            to,
            output,
            event,
            ..
        } = step;
        {
            let mut conn = self.conn();
            let tx = conn.transaction()?;
            let current = load_in(&tx, id)?;
            if current.status != from {
                return Ok(current);
            }
            let n = tx.execute(
                "UPDATE action_runs
             SET status = ?1, output = COALESCE(?2, output), expires_at = COALESCE(?3, expires_at),
                 updated_at = ?4
             WHERE id = ?5 AND status = ?6",
                params![to.as_str(), output, exp, ms, id, from.as_str()],
            )?;
            if n == 0 {
                return load_in(&tx, id);
            }
            let payload = json!({
                "event": event,
                "run_id": id,
                "action": current.action,
                "resource_id": current.resource_id,
                "status": to.as_str(),
                "tier": tier_str(current.tier),
                "dry_run": current.dry_run,
                "output": output.map(clip_audit),
                "ts": ms,
            })
            .to_string();
            append_audit(&tx, ms, id, event, &payload)?;
            tx.commit()?;
        };
        self.action(id)?
            .ok_or_else(|| StoreError::Corrupt(format!("missing action {id}")))
    }

    /// Mark awaiting runs whose approval window has closed. Returns how many expired.
    pub fn expire_actions(&self, now: DateTime<Utc>) -> crate::Result<usize> {
        let ms = now.timestamp_millis();
        let ids: Vec<String> = {
            let conn = self.conn();
            let mut stmt = conn.prepare(
                "SELECT id FROM action_runs
                 WHERE status = 'awaiting_approval' AND expires_at IS NOT NULL AND expires_at <= ?1",
            )?;
            let rows = stmt.query_map([ms], |r| r.get(0))?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        for id in &ids {
            self.transition_action(
                id,
                ActionTransition {
                    from: ActionRunStatus::AwaitingApproval,
                    to: ActionRunStatus::Expired,
                    output: Some("approval window elapsed"),
                    expires_at: None,
                    event: "expire",
                },
            )?;
        }
        Ok(ids.len())
    }

    /// Recompute every audit hash. Errors when a row was altered or reordered.
    pub fn verify_audit(&self) -> crate::Result<()> {
        let conn = self.conn();
        let mut stmt =
            conn.prepare("SELECT seq, prev_hash, payload, hash FROM audit ORDER BY seq ASC")?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
            ))
        })?;
        let mut prev = GENESIS.to_string();
        for row in rows {
            let (seq, prev_hash, payload, hash) = row?;
            if prev_hash != prev {
                return Err(StoreError::Invalid(format!(
                    "audit seq {seq} does not chain from the previous row"
                )));
            }
            let expect = chain_hash(&prev, &payload);
            if expect != hash {
                return Err(StoreError::Invalid(format!(
                    "audit seq {seq} hash mismatch"
                )));
            }
            prev = hash;
        }
        Ok(())
    }
}

fn load_in(conn: &rusqlite::Connection, id: &str) -> crate::Result<ActionRun> {
    conn.query_row(
        "SELECT id, action, resource_id, params, idempotency_key, tier, status, dry_run,
                output, created_at, updated_at, expires_at
         FROM action_runs WHERE id = ?1",
        [id],
        row_to_action,
    )
    .optional()?
    .transpose()?
    .ok_or_else(|| StoreError::UnknownAction(id.to_string()))
}

fn fresh_action_id(conn: &rusqlite::Connection, ms: i64) -> crate::Result<String> {
    for n in 0..64 {
        let id = format!("a{ms:x}{n}");
        let taken: Option<i64> = conn
            .query_row("SELECT 1 FROM action_runs WHERE id = ?1", [&id], |r| {
                r.get(0)
            })
            .optional()?;
        if taken.is_none() {
            return Ok(id);
        }
    }
    Err(StoreError::Invalid(
        "could not allocate an action id".into(),
    ))
}

fn append_audit(
    conn: &rusqlite::Connection,
    ts: i64,
    run_id: &str,
    event: &str,
    payload: &str,
) -> crate::Result<()> {
    let prev: String = conn
        .query_row(
            "SELECT hash FROM audit ORDER BY seq DESC LIMIT 1",
            [],
            |r| r.get(0),
        )
        .optional()?
        .unwrap_or_else(|| GENESIS.to_string());
    let hash = chain_hash(&prev, payload);
    conn.execute(
        "INSERT INTO audit (ts, run_id, event, payload, prev_hash, hash)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![ts, run_id, event, payload, prev, hash],
    )?;
    Ok(())
}

fn chain_hash(prev: &str, payload: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(prev.as_bytes());
    hasher.update(b"\n");
    hasher.update(payload.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn clip_audit(s: &str) -> String {
    const MAX: usize = 500;
    if s.chars().count() <= MAX {
        return s.to_string();
    }
    let mut out: String = s.chars().take(MAX).collect();
    out.push('…');
    out
}

fn tier_str(tier: RiskTier) -> &'static str {
    match tier {
        RiskTier::Auto => "auto",
        RiskTier::Approve => "approve",
        RiskTier::Deny => "deny",
    }
}

fn parse_tier(s: &str) -> crate::Result<RiskTier> {
    match s {
        "auto" => Ok(RiskTier::Auto),
        "approve" => Ok(RiskTier::Approve),
        "deny" => Ok(RiskTier::Deny),
        other => Err(StoreError::Corrupt(format!("unknown risk tier `{other}`"))),
    }
}

fn row_to_action(row: &rusqlite::Row<'_>) -> rusqlite::Result<crate::Result<ActionRun>> {
    let status: String = row.get(6)?;
    let tier: String = row.get(5)?;
    let params: String = row.get(3)?;
    let expires: Option<i64> = row.get(11)?;
    let created: i64 = row.get(9)?;
    Ok((|| {
        Ok(ActionRun {
            id: row.get(0)?,
            action: row.get(1)?,
            resource_id: row.get(2)?,
            params: serde_json::from_str(&params)?,
            idempotency_key: row.get(4)?,
            tier: parse_tier(&tier)?,
            status: status.parse().map_err(|e: String| StoreError::Corrupt(e))?,
            created_at: from_ms(created),
            dry_run: row.get::<_, i64>(7)? != 0,
            expires_at: expires.map(from_ms),
            output: row.get(8)?,
        })
    })())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn draft(key: &str) -> NewAction {
        NewAction {
            action: "script.run".into(),
            resource_id: "host:h".into(),
            params: json!({"script": "/usr/local/bin/hook"}),
            idempotency_key: key.into(),
            tier: RiskTier::Auto,
            dry_run: false,
        }
    }

    #[test]
    fn chain_detects_a_tampered_payload() {
        let store = Store::open_in_memory().unwrap();
        let now = Utc::now();
        let run = store.insert_action(&draft("k1"), now).unwrap();
        store
            .transition_action(
                &run.id,
                ActionTransition {
                    from: ActionRunStatus::Planned,
                    to: ActionRunStatus::Succeeded,
                    output: Some("ok"),
                    expires_at: None,
                    event: "finish",
                },
            )
            .unwrap();
        store.verify_audit().unwrap();

        store
            .conn()
            .execute(
                "UPDATE audit SET payload = '{\"event\":\"tampered\"}' WHERE seq = 1",
                [],
            )
            .unwrap();
        assert!(store.verify_audit().is_err());
    }
}
