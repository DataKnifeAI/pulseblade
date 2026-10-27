//! Remedy table. A row is written when an action's finding outcome becomes
//! resolved. The pilot reads it before calling a model.

use chrono::Utc;
use pulseblade_core::{
    generalize_params, resource_kind_of_id, ActionRun, ActionRunStatus, FindingOutcome,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::actions::tier_str;
use crate::sql::{SqlExt, SqlVal};
use crate::Store;

/// One remembered fix. `params` is a shape (`$namespace`), not a stored name.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RemedyRow {
    pub rule: String,
    pub kind: String,
    pub action: String,
    pub params: Value,
    pub tier: String,
    pub outcome: String,
}

/// A deny for one resource and one action. It is not a remedy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActionRejection {
    pub action: String,
    pub reason: String,
}

/// What the pilot asks before a model call.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RemedyPage {
    pub open_plan: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matched: Option<RemedyRow>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub closest: Vec<RemedyRow>,
    /// Denies for this resource id. They block that action name only.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rejections: Vec<ActionRejection>,
}

impl Store {
    /// Record a succeeded run whose finding is resolved. A deny, a wait, and
    /// `still_open` do not become rows.
    pub fn learn_remedy(&self, run: &ActionRun) -> crate::Result<()> {
        if run.dry_run || run.status != ActionRunStatus::Succeeded {
            return Ok(());
        }
        if run.finding_outcome != Some(FindingOutcome::Resolved) {
            return Ok(());
        }
        let Some(finding_id) = run.finding_id.as_deref().filter(|id| !id.is_empty()) else {
            return Ok(());
        };
        let Some(finding) = self.finding(finding_id)? else {
            return Ok(());
        };
        let kind = self
            .resource(&run.resource_id)?
            .map(|row| row.resource.kind.as_str().to_string())
            .or_else(|| resource_kind_of_id(&run.resource_id).map(str::to_string))
            .unwrap_or_default();
        if kind.is_empty() || finding.rule.is_empty() {
            return Ok(());
        }
        let params = generalize_params(&run.action, &run.params, &run.resource_id);
        self.upsert_remedy(&RemedyRow {
            rule: finding.rule,
            kind,
            action: run.action.clone(),
            params,
            tier: tier_str(run.tier).to_string(),
            outcome: "resolved".into(),
        })
    }

    pub fn remedy_page(
        &self,
        rule: &str,
        kind: &str,
        finding_id: Option<&str>,
        resource_id: Option<&str>,
    ) -> crate::Result<RemedyPage> {
        let open_plan = finding_id
            .filter(|id| !id.is_empty())
            .map(|id| self.finding_has_open_plan(id))
            .transpose()?
            .unwrap_or(false);
        let matched = if rule.is_empty() || kind.is_empty() {
            None
        } else {
            self.remedy_match(rule, kind)?
        };
        let closest = if matched.is_some() || (rule.is_empty() && kind.is_empty()) {
            Vec::new()
        } else {
            self.remedy_closest(rule, kind, 3)?
        };
        let rejections = match resource_id.filter(|id| !id.is_empty()) {
            Some(id) => self
                .rejections_for(id)?
                .into_iter()
                .map(|(action, reason)| ActionRejection { action, reason })
                .collect(),
            None => Vec::new(),
        };
        Ok(RemedyPage {
            open_plan,
            matched,
            closest,
            rejections,
        })
    }

    fn upsert_remedy(&self, row: &RemedyRow) -> crate::Result<()> {
        let params = serde_json::to_string(&row.params)?;
        let now = Utc::now().timestamp_millis();
        self.with_sql(|sql| {
            sql.execute(
                "INSERT INTO remedies (rule, kind, action, params, tier, outcome, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(rule, kind, action) DO UPDATE SET
                    params = excluded.params,
                    tier = excluded.tier,
                    outcome = excluded.outcome,
                    updated_at = excluded.updated_at",
                &[
                    SqlVal::text(&row.rule),
                    SqlVal::text(&row.kind),
                    SqlVal::text(&row.action),
                    SqlVal::text(params),
                    SqlVal::text(&row.tier),
                    SqlVal::text(&row.outcome),
                    SqlVal::Int(now),
                ],
            )?;
            Ok(())
        })
    }

    fn remedy_match(&self, rule: &str, kind: &str) -> crate::Result<Option<RemedyRow>> {
        self.with_sql(|sql| {
            sql.query_opt(
                "SELECT rule, kind, action, params, tier, outcome FROM remedies
                 WHERE rule = ?1 AND kind = ?2
                 ORDER BY updated_at DESC LIMIT 1",
                &[SqlVal::text(rule), SqlVal::text(kind)],
                map_remedy,
            )
        })
    }

    fn remedy_closest(
        &self,
        rule: &str,
        kind: &str,
        limit: usize,
    ) -> crate::Result<Vec<RemedyRow>> {
        self.with_sql(|sql| {
            sql.query(
                "SELECT rule, kind, action, params, tier, outcome FROM remedies
                 WHERE (rule = ?1 OR kind = ?2)
                   AND NOT (rule = ?1 AND kind = ?2)
                 ORDER BY updated_at DESC LIMIT ?3",
                &[
                    SqlVal::text(rule),
                    SqlVal::text(kind),
                    SqlVal::Int(limit as i64),
                ],
                map_remedy,
            )
        })
    }

    /// A plan or an approval already filed for this finding. A finished run is not open.
    pub fn finding_has_open_plan(&self, finding_id: &str) -> crate::Result<bool> {
        let local = self.with_sql(|sql| {
            sql.query_opt(
                "SELECT 1 FROM action_runs
                 WHERE finding_id = ?1 AND status IN ('planned', 'awaiting_approval')
                 LIMIT 1",
                &[SqlVal::text(finding_id)],
                |row| row.int(0),
            )
        })?;
        if local.is_some() {
            return Ok(true);
        }
        let routed = self.with_sql(|sql| {
            sql.query(
                "SELECT r.body FROM route_plans p
                 LEFT JOIN route_results r ON r.id = p.plan_id
                 WHERE p.finding_id = ?1",
                &[SqlVal::text(finding_id)],
                |row| row.opt_text(0),
            )
        })?;
        for body in routed {
            match body {
                None => return Ok(true),
                Some(body) => {
                    let run: ActionRun = serde_json::from_str(&body)?;
                    if matches!(
                        run.status,
                        ActionRunStatus::Planned | ActionRunStatus::AwaitingApproval
                    ) {
                        return Ok(true);
                    }
                }
            }
        }
        Ok(false)
    }
}

fn map_remedy(row: &dyn crate::sql::SqlRow) -> crate::Result<RemedyRow> {
    let params: Value = serde_json::from_str(&row.text(3)?)?;
    Ok(RemedyRow {
        rule: row.text(0)?,
        kind: row.text(1)?,
        action: row.text(2)?,
        params,
        tier: row.text(4)?,
        outcome: row.text(5)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ActionTransition, FindingDraft, NewAction};
    use chrono::Utc;
    use pulseblade_core::{RiskTier, Severity};
    use serde_json::json;

    fn workload_finding(store: &Store) -> String {
        let now = Utc::now();
        let opened = store
            .reconcile_findings(
                &[FindingDraft {
                    dedupe_key: "health\u{0}workload:poc-apps:pb-fault:deployment:pb-fault-loop"
                        .into(),
                    resource_id: "workload:poc-apps:pb-fault:deployment:pb-fault-loop".into(),
                    rule: "health".into(),
                    severity: Severity::Critical,
                    summary: "pb-fault-loop is failed".into(),
                    context: vec![],
                    logs: vec![],
                }],
                now,
            )
            .unwrap();
        opened[0].id.clone()
    }

    fn rollout(finding_id: &str) -> NewAction {
        NewAction {
            action: "k8s.rollout_restart".into(),
            resource_id: "workload:poc-apps:pb-fault:deployment:pb-fault-loop".into(),
            params: json!({"namespace": "pb-fault", "name": "pb-fault-loop", "kind": "deployment"}),
            idempotency_key: "rollout-1".into(),
            tier: RiskTier::Approve,
            dry_run: false,
            finding_id: Some(finding_id.into()),
        }
    }

    #[test]
    fn resolved_rollout_restart_becomes_a_remedy() {
        let store = Store::open_in_memory().unwrap();
        let finding_id = workload_finding(&store);
        let now = Utc::now();
        let run = store.insert_action(&rollout(&finding_id), now).unwrap();
        let open = store
            .transition_action(
                &run.id,
                ActionTransition {
                    from: ActionRunStatus::Planned,
                    to: ActionRunStatus::Succeeded,
                    output: Some("restarted deployment/pb-fault/pb-fault-loop"),
                    expires_at: None,
                    event: "finish",
                },
            )
            .unwrap();
        assert_eq!(open.finding_outcome, Some(FindingOutcome::StillOpen));
        assert!(store
            .remedy_page("health", "workload", None, None)
            .unwrap()
            .matched
            .is_none());

        store.reconcile_findings(&[], now).unwrap();
        let done = store.action(&run.id).unwrap().unwrap();
        assert_eq!(done.finding_outcome, Some(FindingOutcome::Resolved));
        let page = store.remedy_page("health", "workload", None, None).unwrap();
        let row = page.matched.expect("resolved rollout is a remedy");
        assert_eq!(row.action, "k8s.rollout_restart");
        assert_eq!(row.rule, "health");
        assert_eq!(row.kind, "workload");
        assert_eq!(row.tier, "approve");
        assert_eq!(row.outcome, "resolved");
        assert_eq!(row.params["namespace"], "$namespace");
        assert_eq!(row.params["name"], "$name");
        assert_eq!(row.params["kind"], "$kind");
        assert!(!row.params.to_string().contains("pb-fault-loop"));
    }

    #[test]
    fn deny_does_not_become_a_remedy() {
        let store = Store::open_in_memory().unwrap();
        let finding_id = workload_finding(&store);
        let now = Utc::now();
        let run = store.insert_action(&rollout(&finding_id), now).unwrap();
        store
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
        let denied = store
            .transition_action(
                &run.id,
                ActionTransition {
                    from: ActionRunStatus::AwaitingApproval,
                    to: ActionRunStatus::Rejected,
                    output: Some("denied: command not executed"),
                    expires_at: None,
                    event: "deny",
                },
            )
            .unwrap();
        assert_eq!(denied.status, ActionRunStatus::Rejected);
        store.reconcile_findings(&[], now).unwrap();
        let later = store.action(&run.id).unwrap().unwrap();
        assert_eq!(later.finding_outcome, Some(FindingOutcome::Resolved));
        assert!(store
            .remedy_page("health", "workload", Some(&finding_id), None)
            .unwrap()
            .matched
            .is_none());
    }
}
