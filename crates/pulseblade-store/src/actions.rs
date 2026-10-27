//! Action runs and the hash-chained audit log (M3).

use chrono::{DateTime, Utc};
use pulseblade_core::{
    ActionRun, ActionRunStatus, FindingOutcome, FindingStatus, Health, RiskTier,
};
use rusqlite::{params, OptionalExtension};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::sql::{SqlExt, SqlVal};
use crate::{from_ms, Store, StoreError};

const GENESIS: &str = "0000000000000000000000000000000000000000000000000000000000000000";

const ACTION_COLS: &str = "id, action, resource_id, params, idempotency_key, tier, status, dry_run,
                    output, created_at, updated_at, expires_at, finding_id";

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
    pub finding_id: Option<String>,
}

impl Store {
    /// An in-flight run with this idempotency key, if one exists.
    pub fn open_action_by_key(&self, key: &str) -> crate::Result<Option<ActionRun>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(&format!(
            "SELECT {ACTION_COLS}
             FROM action_runs
             WHERE idempotency_key = ?1 AND status IN ('planned', 'awaiting_approval', 'running')
             ORDER BY created_at DESC LIMIT 1"
        ))?;
        let mut rows = stmt.query([key])?;
        match rows.next()? {
            Some(row) => Ok(Some(row_to_action(row)??)),
            None => Ok(None),
        }
    }

    pub fn action(&self, id: &str) -> crate::Result<Option<ActionRun>> {
        let run = {
            let conn = self.conn();
            conn.query_row(
                &format!("SELECT {ACTION_COLS} FROM action_runs WHERE id = ?1"),
                [id],
                row_to_action,
            )
            .optional()?
            .transpose()?
        };
        run.map(|run| self.attach_finding_outcome(run)).transpose()
    }

    /// Recent runs, newest first. `resource_id` limits the list to one resource.
    pub fn recent_actions(
        &self,
        resource_id: Option<&str>,
        limit: usize,
    ) -> crate::Result<Vec<ActionRun>> {
        let limit = limit.clamp(1, 100) as i64;
        let (sql_text, args) = if let Some(id) = resource_id {
            (
                format!(
                    "SELECT {ACTION_COLS}
                 FROM action_runs WHERE resource_id = ?1
                 ORDER BY created_at DESC LIMIT ?2"
                ),
                vec![SqlVal::text(id), SqlVal::Int(limit)],
            )
        } else {
            (
                format!("SELECT {ACTION_COLS} FROM action_runs ORDER BY created_at DESC LIMIT ?1"),
                vec![SqlVal::Int(limit)],
            )
        };
        let runs = self.with_sql(|sql| sql.query(&sql_text, &args, map_action))?;
        runs.into_iter()
            .map(|run| self.attach_finding_outcome(run))
            .collect()
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

    /// Waiting runs a person can approve. A routed body is the node's copy and
    /// wins over a hub row with the same id.
    pub fn awaiting_action_count(&self) -> crate::Result<u64> {
        let mut waiting = std::collections::BTreeSet::new();
        let mut covered = std::collections::BTreeSet::new();
        for run in self.routed_runs()? {
            covered.insert(run.id.clone());
            if run.status == ActionRunStatus::AwaitingApproval {
                waiting.insert(run.id);
            }
        }
        let local = self.with_sql(|sql| {
            sql.query(
                "SELECT id FROM action_runs WHERE status = 'awaiting_approval'",
                &[],
                |row| row.text(0),
            )
        })?;
        for id in local {
            if covered.insert(id.clone()) {
                waiting.insert(id);
            }
        }
        Ok(waiting.len() as u64)
    }

    /// Portal list: routed node runs plus local runs. Waiting runs are kept
    /// even when they are older than the recent window. A routed body wins.
    pub fn portal_actions(&self, limit: usize) -> crate::Result<Vec<ActionRun>> {
        let limit = limit.clamp(1, 100);
        let mut by_id = std::collections::BTreeMap::new();
        for run in self.recent_actions(None, limit)? {
            by_id.insert(run.id.clone(), run);
        }
        for mut run in self.routed_runs()? {
            run.finding_outcome = None;
            let run = self.attach_finding_outcome(run)?;
            by_id.insert(run.id.clone(), run);
        }
        let mut waiting = Vec::new();
        let mut rest = Vec::new();
        for run in by_id.into_values() {
            if run.status == ActionRunStatus::AwaitingApproval {
                waiting.push(run);
            } else {
                rest.push(run);
            }
        }
        let by_time =
            |a: &ActionRun, b: &ActionRun| b.created_at.cmp(&a.created_at).then(b.id.cmp(&a.id));
        waiting.sort_by(by_time);
        rest.sort_by(by_time);
        rest.truncate(limit);
        waiting.append(&mut rest);
        Ok(waiting)
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
                 output, created_at, updated_at, expires_at, finding_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'planned', ?7, NULL, ?8, ?8, NULL, ?9)",
                params![
                    id,
                    draft.action,
                    draft.resource_id,
                    params_json,
                    draft.idempotency_key,
                    tier_str(draft.tier),
                    i64::from(draft.dry_run),
                    ms,
                    draft.finding_id,
                ],
            )?;
            let payload = json!({
                "event": "plan",
                "run_id": id,
                "action": draft.action,
                "resource_id": draft.resource_id,
                "finding_id": draft.finding_id,
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
        &format!("SELECT {ACTION_COLS} FROM action_runs WHERE id = ?1"),
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

pub(crate) fn tier_str(tier: RiskTier) -> &'static str {
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

fn map_action(row: &dyn crate::sql::SqlRow) -> crate::Result<ActionRun> {
    let status = row.text(6)?;
    let tier = row.text(5)?;
    let params = row.text(3)?;
    Ok(ActionRun {
        id: row.text(0)?,
        action: row.text(1)?,
        resource_id: row.text(2)?,
        params: serde_json::from_str(&params)?,
        idempotency_key: row.text(4)?,
        tier: parse_tier(&tier)?,
        status: status.parse().map_err(|e: String| StoreError::Corrupt(e))?,
        created_at: from_ms(row.int(9)?),
        dry_run: row.int(7)? != 0,
        expires_at: row.opt_int(11)?.map(from_ms),
        output: row.opt_text(8)?,
        finding_id: row.opt_text(12)?,
        finding_outcome: None,
    })
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
            finding_id: row.get(12)?,
            finding_outcome: None,
        })
    })())
}

impl Store {
    /// Recent runs on one resource, including plans the hub routed elsewhere.
    pub fn actions_for_resource(
        &self,
        resource_id: &str,
        limit: usize,
    ) -> crate::Result<Vec<ActionRun>> {
        let mut runs = self.recent_actions(Some(resource_id), limit)?;
        for mut run in self.routed_actions(resource_id)? {
            if runs.iter().any(|have| have.id == run.id) {
                continue;
            }
            // The cached body is from execute time. Recompute against current health.
            run.finding_outcome = None;
            runs.push(self.attach_finding_outcome(run)?);
        }
        runs.truncate(limit);
        Ok(runs)
    }

    /// Fill `finding_outcome` from the finding row, or from resource health when
    /// this store does not have that finding (a hub reading a node-opened id).
    pub fn attach_finding_outcome(&self, mut run: ActionRun) -> crate::Result<ActionRun> {
        let Some(fid) = run.finding_id.clone().filter(|id| !id.is_empty()) else {
            run.finding_outcome = None;
            return Ok(run);
        };
        run.finding_outcome = Some(self.outcome_for(&fid, &run.resource_id, run.status)?);
        self.learn_remedy(&run)?;
        Ok(run)
    }

    fn outcome_for(
        &self,
        finding_id: &str,
        resource_id: &str,
        status: ActionRunStatus,
    ) -> crate::Result<FindingOutcome> {
        if status == ActionRunStatus::AwaitingApproval {
            return Ok(FindingOutcome::Waiting);
        }
        if let Some(finding) = self.finding(finding_id)? {
            return Ok(match finding.status {
                FindingStatus::Resolved => FindingOutcome::Resolved,
                FindingStatus::Open | FindingStatus::Acked => FindingOutcome::StillOpen,
            });
        }
        let Some(row) = self.resource(resource_id)? else {
            return Ok(FindingOutcome::StillOpen);
        };
        if !row.present || row.resource.health == Health::Ok {
            Ok(FindingOutcome::Resolved)
        } else {
            Ok(FindingOutcome::StillOpen)
        }
    }
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
            finding_id: None,
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

    #[test]
    fn outcome_is_waiting_then_open_then_resolved() {
        use pulseblade_core::{Health, Observation, Resource, ResourceKind, Severity};

        let store = Store::open_in_memory().unwrap();
        let now = Utc::now();
        let opened = store
            .reconcile_findings(
                &[crate::FindingDraft {
                    dedupe_key: "health\u{0}unit:h:a.service".into(),
                    resource_id: "unit:h:a.service".into(),
                    rule: "health".into(),
                    severity: Severity::Critical,
                    summary: "a.service is failed".into(),
                    context: vec![],
                    logs: vec![],
                }],
                now,
            )
            .unwrap();
        let finding_id = opened[0].id.clone();
        let mut tied = draft("tied");
        tied.finding_id = Some(finding_id.clone());
        tied.resource_id = "unit:h:a.service".into();
        let run = store.insert_action(&tied, now).unwrap();
        assert_eq!(run.finding_id.as_deref(), Some(finding_id.as_str()));
        assert_eq!(run.finding_outcome, Some(FindingOutcome::StillOpen));

        let waiting = store
            .transition_action(
                &run.id,
                ActionTransition {
                    from: ActionRunStatus::Planned,
                    to: ActionRunStatus::AwaitingApproval,
                    output: None,
                    expires_at: Some(now + chrono::Duration::hours(1)),
                    event: "execute",
                },
            )
            .unwrap();
        assert_eq!(waiting.finding_outcome, Some(FindingOutcome::Waiting));

        let done = store
            .transition_action(
                &run.id,
                ActionTransition {
                    from: ActionRunStatus::AwaitingApproval,
                    to: ActionRunStatus::Succeeded,
                    output: Some("ok"),
                    expires_at: None,
                    event: "finish",
                },
            )
            .unwrap();
        assert_eq!(done.finding_outcome, Some(FindingOutcome::StillOpen));

        store.reconcile_findings(&[], now).unwrap();
        let later = store.action(&run.id).unwrap().unwrap();
        assert_eq!(later.finding_outcome, Some(FindingOutcome::Resolved));

        store
            .apply(
                "k8s",
                now,
                &Observation {
                    resources: vec![Resource::new(
                        "workload:demo:ns:deployment:api",
                        ResourceKind::Workload,
                        "api",
                    )
                    .health(Health::Failed)],
                    samples: vec![],
                    logs: vec![],
                },
            )
            .unwrap();
        let mut other = draft("health-only");
        other.finding_id = Some("f-missing".into());
        other.resource_id = "workload:demo:ns:deployment:api".into();
        other.idempotency_key = "health-only".into();
        let proxied = store.insert_action(&other, now).unwrap();
        assert_eq!(proxied.finding_outcome, Some(FindingOutcome::StillOpen));
        store
            .apply(
                "k8s",
                now,
                &Observation {
                    resources: vec![Resource::new(
                        "workload:demo:ns:deployment:api",
                        ResourceKind::Workload,
                        "api",
                    )
                    .health(Health::Ok)],
                    samples: vec![],
                    logs: vec![],
                },
            )
            .unwrap();
        let cleared = store.action(&proxied.id).unwrap().unwrap();
        assert_eq!(cleared.finding_outcome, Some(FindingOutcome::Resolved));
    }

    #[test]
    fn portal_lists_a_routed_run_the_hub_table_does_not_have() {
        let store = Store::open_in_memory().unwrap();
        let now = Utc::now();
        let run = ActionRun {
            id: "a1a17b4835310".into(),
            action: "k8s.rollout_restart".into(),
            resource_id: "workload:poc-apps:pb-fault:deployment:pb-fault-loop".into(),
            params: json!({
                "namespace": "pb-fault",
                "name": "pb-fault-loop",
                "kind": "deployment"
            }),
            idempotency_key: "k".into(),
            tier: RiskTier::Approve,
            status: ActionRunStatus::AwaitingApproval,
            created_at: now,
            dry_run: false,
            expires_at: Some(now + chrono::Duration::minutes(30)),
            output: None,
            finding_id: Some("fc13a29631a17b47555a0".into()),
            finding_outcome: None,
        };
        let body = serde_json::to_value(&run).unwrap();
        store
            .remember_plan(
                &run.id,
                "poc-apps-worker-2",
                run.finding_id.as_deref(),
                Some(run.resource_id.as_str()),
            )
            .unwrap();
        store
            .cache_run(&run.id, "poc-apps-worker-2", &body)
            .unwrap();
        assert!(store.recent_actions(None, 20).unwrap().is_empty());
        let listed = store.portal_actions(20).unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, run.id);
        assert_eq!(listed[0].status, ActionRunStatus::AwaitingApproval);
        assert_eq!(
            listed[0].finding_id.as_deref(),
            Some("fc13a29631a17b47555a0")
        );
        let on_resource = store.actions_for_resource(&run.resource_id, 8).unwrap();
        assert_eq!(on_resource.len(), 1);
        assert_eq!(on_resource[0].status, ActionRunStatus::AwaitingApproval);
        assert_eq!(store.awaiting_action_count().unwrap(), 1);

        let local = store.insert_action(&draft("local-wait"), now).unwrap();
        store
            .transition_action(
                &local.id,
                ActionTransition {
                    from: ActionRunStatus::Planned,
                    to: ActionRunStatus::AwaitingApproval,
                    output: None,
                    expires_at: None,
                    event: "execute",
                },
            )
            .unwrap();
        assert_eq!(store.awaiting_action_count().unwrap(), 2);
        let both = store.portal_actions(20).unwrap();
        assert!(both.iter().any(|r| r.id == run.id));
        assert!(both
            .iter()
            .any(|r| r.id == local.id && r.status == ActionRunStatus::AwaitingApproval));
    }
}
