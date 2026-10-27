//! One investigation: a stable system prompt, a Rust-built digest, then a tool loop
//! bounded by the configured budget.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::chat::{parse_completion, Chat};
use crate::config::{is_loopback, EndpointConfig, PilotConfig, TierConfig};
use crate::mcp::Tools;

const SYSTEM: &str = "\
You are the Pulseblade pilot. Investigate one incident using the tools. \
The digest already includes the finding, recent changes, prior notes, and known remedies. \
Prefer a known remedy from the digest when it fits this finding. \
Use state_snapshot with no arguments or detail=brief, short metrics_query ranges, \
and one resource_explain. Write a short conclusion with memory_note on the resource. \
Propose remediation with action_plan and pass that finding's id as finding_id. \
Call action_status to read a run. It says whether that finding is resolved, still open, \
or the approval is still waiting. After an auto execute, a later action_status shows that outcome. \
Never approve an action. \
Call action_execute only when a previous tool result says tier is auto and auto execution is enabled. \
Call escalate when a stronger model should take over. \
The node enforces risk tiers; you cannot override approve or deny.";

const OUTPUT_CAP: usize = 6_000;

#[derive(Clone)]
struct OpenFinding {
    id: String,
    resource_id: String,
    summary: String,
    severity: String,
    rule: String,
}

/// Record of one wake. Steady-state polls do not produce a run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PilotRun {
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

pub struct Session {
    config: PilotConfig,
    seq: i64,
    seen: BTreeSet<String>,
    heartbeat_at: Instant,
}

impl Session {
    pub fn new(config: PilotConfig, now: Instant) -> Self {
        Self {
            config,
            seq: 0,
            seen: BTreeSet::new(),
            heartbeat_at: now,
        }
    }

    /// Poll MCP. Returns `Ok(None)` when nothing needs a model.
    pub fn tick<T: Tools, C: Chat>(
        &mut self,
        tools: &mut T,
        chat: &mut C,
        now: Instant,
    ) -> Result<Option<PilotRun>, String> {
        let findings = tools.call(
            "findings_list",
            json!({"status": "open", "limit": 8, "detail": true}),
        )?;
        let changes = tools.call(
            "changes_since",
            json!({"since": format!("seq:{}", self.seq), "limit": 20}),
        )?;
        let seq = seq_of(&findings).max(seq_of(&changes)).max(self.seq);
        let open = open_findings(&findings);
        let change_list = changes
            .get("changes")
            .and_then(|c| c.as_array())
            .map(|list| {
                list.iter()
                    .filter(|c| wakes_pilot(c))
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let elapsed = now
            .checked_duration_since(self.heartbeat_at)
            .unwrap_or(Duration::ZERO);
        let heartbeat_due = elapsed >= Duration::from_secs(self.config.heartbeat_secs.max(1));
        if heartbeat_due {
            self.heartbeat_at = now;
        }
        let fresh: Vec<_> = open
            .iter()
            .filter(|f| !self.seen.contains(&f.id))
            .cloned()
            .collect();
        let wake_findings = self.config.wakes_on("findings") && !fresh.is_empty();
        let wake_changes = self.config.wakes_on("changes") && !change_list.is_empty();
        let wake_heart = heartbeat_due && (!open.is_empty() || !change_list.is_empty());
        let trigger = if wake_findings {
            format!("finding:{}", fresh[0].id)
        } else if wake_changes {
            "changes".into()
        } else if wake_heart {
            "heartbeat".into()
        } else {
            self.seq = seq;
            return Ok(None);
        };
        let focus = if fresh.is_empty() {
            open.clone()
        } else {
            fresh
        };
        let notes = prior_notes(tools, &focus);
        let mut model_focus = Vec::new();
        let mut filed = Vec::new();
        let mut closest = Vec::new();
        for finding in &focus {
            if finding.rule.is_empty() {
                model_focus.push(finding.clone());
                continue;
            }
            let kind = pulseblade_core::resource_kind_of_id(&finding.resource_id).unwrap_or("");
            let page = tools
                .call(
                    "remedies_list",
                    json!({
                        "rule": finding.rule,
                        "kind": kind,
                        "finding_id": finding.id,
                        "resource_id": finding.resource_id,
                    }),
                )
                .unwrap_or_else(|_| json!({}));
            if page.get("open_plan").and_then(|v| v.as_bool()) == Some(true) {
                continue;
            }
            if let Some(matched) = page.get("matched").filter(|row| {
                row.get("action")
                    .and_then(|action| action.as_str())
                    .is_some_and(|action| !action.is_empty())
            }) {
                if let Some(action) = file_remedy(tools, finding, matched) {
                    filed.push(action);
                    continue;
                }
            }
            if let Some(list) = page.get("closest").and_then(|rows| rows.as_array()) {
                for row in list.iter().take(3) {
                    if let Some(line) = remedy_line(row) {
                        closest.push(line);
                    }
                }
            }
            model_focus.push(finding.clone());
        }
        for finding in &open {
            self.seen.insert(finding.id.clone());
        }
        self.seq = seq;
        if model_focus.is_empty() && !filed.is_empty() {
            return Ok(Some(PilotRun {
                trigger,
                tier: String::new(),
                endpoint: String::new(),
                tool_calls: u32::try_from(filed.len()).unwrap_or(u32::MAX),
                tokens: 0,
                conclusion: format!("filed {}", filed.join(",")),
                proposed: filed,
                status: "remedy".into(),
                detail: "action_plan".into(),
            }));
        }
        // Every finding already has a plan. A change with no finding still investigates.
        if model_focus.is_empty() && !focus.is_empty() {
            return Ok(None);
        }
        let digest = digest(&model_focus, &change_list, &notes, &closest, seq);
        let run = investigate(&self.config, tools, chat, &trigger, &digest, &model_focus);
        Ok(Some(run))
    }
}

/// File the known plan for this resource. Does not execute it.
fn file_remedy<T: Tools>(tools: &mut T, finding: &OpenFinding, matched: &Value) -> Option<String> {
    let action = matched.get("action")?.as_str()?.to_string();
    let shape = matched.get("params").cloned().unwrap_or_else(|| json!({}));
    let params = pulseblade_core::bind_remedy_params(&shape, &finding.resource_id)?;
    let filed = tools
        .call(
            "action_plan",
            json!({
                "action": action,
                "params": params,
                "resource_id": finding.resource_id,
                "finding_id": finding.id,
            }),
        )
        .ok()?;
    if filed.get("error").is_some() {
        return None;
    }
    Some(action)
}

fn remedy_line(row: &Value) -> Option<String> {
    let action = row.get("action")?.as_str()?;
    let rule = row.get("rule").and_then(|v| v.as_str()).unwrap_or("");
    let kind = row.get("kind").and_then(|v| v.as_str()).unwrap_or("");
    let tier = row.get("tier").and_then(|v| v.as_str()).unwrap_or("");
    let params = row.get("params").cloned().unwrap_or_else(|| json!({}));
    Some(format!("remedy {rule} {kind} {action} {tier} {params}"))
}

fn investigate<T: Tools, C: Chat>(
    config: &PilotConfig,
    tools: &mut T,
    chat: &mut C,
    trigger: &str,
    digest: &str,
    focus: &[OpenFinding],
) -> PilotRun {
    let started = Instant::now();
    let wall = Duration::from_secs(config.budget.max_wall_secs);
    let max_calls = config.budget.max_tool_calls;
    let max_tokens = config.budget.max_tokens;
    let tiers = usable_tiers(config);
    let mut run = PilotRun {
        trigger: trigger.to_string(),
        tier: String::new(),
        endpoint: String::new(),
        tool_calls: 0,
        tokens: 0,
        conclusion: String::new(),
        proposed: Vec::new(),
        status: "error".into(),
        detail: String::new(),
    };
    if tiers.is_empty() {
        run.detail = "no usable endpoint".into();
        return run;
    }
    let mut messages = vec![
        json!({"role": "system", "content": SYSTEM}),
        json!({"role": "user", "content": digest}),
    ];
    let schema = tool_defs(config.execute_auto);
    let mut names = Vec::new();
    let mut plans: BTreeMap<String, String> = BTreeMap::new();
    let mut note = String::new();

    for (idx, tier) in tiers.iter().enumerate() {
        let mut tier_calls = 0u32;
        let mut ep_i = 0usize;
        let mut locked: Option<usize> = None;
        loop {
            if started.elapsed() >= wall || run.tokens >= max_tokens {
                run.status = "budget".into();
                run.conclusion = conclusion_of(&note, "");
                run.detail = names.join(",");
                return run;
            }
            let use_i = locked.unwrap_or(ep_i);
            if use_i >= tier.endpoints.len() {
                break;
            }
            let endpoint = &tier.endpoints[use_i];
            let body = json!({
                "model": endpoint.model,
                "messages": messages,
                "tools": schema,
            });
            let resp = match chat.complete(endpoint, &body) {
                Ok(v) => v,
                Err(_) => {
                    locked = None;
                    ep_i = use_i + 1;
                    continue;
                }
            };
            let completion = match parse_completion(&resp) {
                Ok(c) => c,
                Err(_) => {
                    locked = None;
                    ep_i = use_i + 1;
                    continue;
                }
            };
            locked = Some(use_i);
            run.endpoint = endpoint.base_url.clone();
            run.tier = tier.name.clone();
            run.tokens = run.tokens.saturating_add(completion.tokens);
            if completion.calls.is_empty() {
                run.status = "ok".into();
                run.conclusion = conclusion_of(&note, &completion.content);
                run.detail = names.join(",");
                return run;
            }
            if completion.calls.iter().any(|c| c.name == "escalate") {
                names.push("escalate".into());
                messages.push(json!({
                    "role": "user",
                    "content": "Escalating to the next tier."
                }));
                break;
            }
            let mut executed = Vec::new();
            let mut results = Vec::new();
            let mut spent = false;
            for call in &completion.calls {
                if tier_calls >= max_calls {
                    spent = true;
                    break;
                }
                tier_calls += 1;
                run.tool_calls += 1;
                names.push(call.name.clone());
                let mut state = ToolState {
                    execute_auto: config.execute_auto,
                    focus,
                    plans: &mut plans,
                    proposed: &mut run.proposed,
                    note: &mut note,
                };
                let result = apply_tool(tools, &call.name, &call.arguments, &mut state);
                executed.push(json!({
                    "id": call.id,
                    "type": "function",
                    "function": {
                        "name": call.name,
                        "arguments": call.arguments.to_string()
                    }
                }));
                results.push(json!({
                    "role": "tool",
                    "tool_call_id": call.id,
                    "content": truncate(&result.to_string())
                }));
            }
            if !executed.is_empty() {
                messages.push(json!({
                    "role": "assistant",
                    "content": empty_content(&completion.content),
                    "tool_calls": executed
                }));
                messages.extend(results);
            }
            if spent {
                if idx + 1 < tiers.len() {
                    messages.push(json!({
                        "role": "user",
                        "content": "Tool budget for this tier is spent. Continue on the next tier."
                    }));
                    break;
                }
                run.status = "budget".into();
                run.conclusion = conclusion_of(&note, &completion.content);
                run.detail = names.join(",");
                return run;
            }
        }
    }
    if run.conclusion.is_empty() {
        run.conclusion = note;
    }
    if run.detail.is_empty() {
        run.detail = if names.is_empty() {
            "no usable endpoint".into()
        } else {
            names.join(",")
        };
    }
    run
}

fn conclusion_of(note: &str, content: &str) -> String {
    if !content.is_empty() {
        content.to_string()
    } else {
        note.to_string()
    }
}

fn empty_content(content: &str) -> Value {
    if content.is_empty() {
        Value::Null
    } else {
        Value::String(content.to_string())
    }
}

struct ToolState<'a> {
    execute_auto: bool,
    focus: &'a [OpenFinding],
    plans: &'a mut BTreeMap<String, String>,
    proposed: &'a mut Vec<String>,
    note: &'a mut String,
}

fn apply_tool<T: Tools>(
    tools: &mut T,
    name: &str,
    args: &Value,
    state: &mut ToolState<'_>,
) -> Value {
    if name == "action_execute" {
        return guard_execute(tools, args, state.execute_auto, state.plans);
    }
    if !allowed(name) {
        return json!({"error": format!("tool `{name}` is not available to the pilot")});
    }
    let mut args = clamp(name, args.clone());
    if name == "memory_note" {
        if let Some(text) = args.get("text").and_then(|t| t.as_str()) {
            *state.note = text.to_string();
        }
    }
    if name == "action_plan" {
        args = bind_finding(args, state.focus);
        if let Some(action) = args.get("action").and_then(|a| a.as_str()) {
            state.proposed.push(action.to_string());
        }
    }
    match tools.call(name, args) {
        Ok(v) => {
            if name == "action_plan" {
                if let (Some(id), Some(tier)) = (
                    v.get("plan_id").and_then(|s| s.as_str()),
                    v.get("tier").and_then(|s| s.as_str()),
                ) {
                    state.plans.insert(id.to_string(), tier.to_string());
                }
            }
            v
        }
        Err(e) => json!({"error": e}),
    }
}

fn guard_execute<T: Tools>(
    tools: &mut T,
    args: &Value,
    execute_auto: bool,
    plans: &BTreeMap<String, String>,
) -> Value {
    if !execute_auto {
        return json!({"error": "auto execution is disabled"});
    }
    let id = args.get("plan_id").and_then(|s| s.as_str()).unwrap_or("");
    match plans.get(id).map(String::as_str) {
        Some("auto") => match tools.call("action_execute", args.clone()) {
            Ok(v) => v,
            Err(e) => json!({"error": e}),
        },
        Some("approve") => {
            json!({"error": "approve tier waits for a human; the pilot cannot approve"})
        }
        Some("deny") => json!({"error": "deny tier cannot run"}),
        _ => json!({"error": "unknown plan; call action_plan first"}),
    }
}

fn allowed(name: &str) -> bool {
    matches!(
        name,
        "state_snapshot"
            | "resource_explain"
            | "changes_since"
            | "metrics_query"
            | "logs_query"
            | "findings_list"
            | "memory_search"
            | "memory_note"
            | "actions_list"
            | "action_plan"
            | "action_status"
    )
}

/// The model can pass `finding_id`. When it does not, use the open finding on
/// that resource, or the only finding this investigation is about.
fn bind_finding(mut args: Value, focus: &[OpenFinding]) -> Value {
    let supplied = args
        .get("finding_id")
        .and_then(|v| v.as_str())
        .is_some_and(|id| !id.is_empty());
    if supplied {
        return args;
    }
    let resource = args.get("resource_id").and_then(|v| v.as_str());
    let matched = focus
        .iter()
        .find(|f| resource.is_some_and(|id| id == f.resource_id));
    let chosen = matched.or_else(|| (focus.len() == 1).then(|| &focus[0]));
    if let Some(finding) = chosen {
        args["finding_id"] = json!(finding.id);
    }
    args
}

fn clamp(name: &str, mut args: Value) -> Value {
    if !args.is_object() {
        args = json!({});
    }
    match name {
        "state_snapshot" => {
            if args.get("detail").and_then(|v| v.as_str()) == Some("full") {
                args["detail"] = json!("brief");
            }
            cap_limit(&mut args, 20);
        }
        "metrics_query" => {
            if args.get("range").is_none() {
                args["range"] = json!("15m");
            }
        }
        "findings_list" => cap_limit(&mut args, 10),
        "changes_since" | "logs_query" => cap_limit(&mut args, 20),
        "memory_search" => cap_limit(&mut args, 5),
        _ => {}
    }
    args
}

fn cap_limit(args: &mut Value, max: u64) {
    let current = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(max);
    args["limit"] = json!(current.min(max));
}

fn truncate(s: &str) -> String {
    if s.chars().count() <= OUTPUT_CAP {
        return s.to_string();
    }
    let mut out: String = s.chars().take(OUTPUT_CAP).collect();
    out.push('…');
    out
}

fn usable_tiers(config: &PilotConfig) -> Vec<TierConfig> {
    config
        .tiers
        .iter()
        .filter_map(|tier| {
            let endpoints: Vec<EndpointConfig> = tier
                .endpoints
                .iter()
                .filter(|ep| ep.kind == "openai_compatible")
                .filter(|ep| !config.local_only || is_loopback(&ep.base_url))
                .filter(|ep| !ep.base_url.is_empty() && !ep.model.is_empty())
                .cloned()
                .collect();
            if endpoints.is_empty() {
                None
            } else {
                Some(TierConfig {
                    name: tier.name.clone(),
                    endpoints,
                })
            }
        })
        .collect()
}

fn prior_notes<T: Tools>(tools: &mut T, focus: &[OpenFinding]) -> Vec<String> {
    let mut out = Vec::new();
    for finding in focus.iter().take(3) {
        let Ok(page) = tools.call(
            "memory_search",
            json!({"query": finding.resource_id, "limit": 3}),
        ) else {
            continue;
        };
        let Some(notes) = page.get("notes").and_then(|n| n.as_array()) else {
            continue;
        };
        for note in notes.iter().take(3) {
            if let Some(text) = note.get("text").and_then(|t| t.as_str()) {
                out.push(format!("{}: {text}", finding.resource_id));
            }
        }
    }
    out
}

fn digest(
    focus: &[OpenFinding],
    changes: &[Value],
    notes: &[String],
    remedies: &[String],
    seq: i64,
) -> String {
    let mut lines = vec![format!("as_of_seq {seq}")];
    for finding in focus.iter().take(5) {
        lines.push(format!(
            "finding {} {} {} {}",
            finding.id, finding.severity, finding.resource_id, finding.summary
        ));
    }
    if focus.len() > 5 {
        lines.push(format!("{} more findings omitted", focus.len() - 5));
    }
    for change in changes.iter().take(8) {
        let id = change
            .get("resource_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let kind = change.get("kind").and_then(|v| v.as_str()).unwrap_or("");
        let field = change.get("field").and_then(|v| v.as_str()).unwrap_or("");
        lines.push(format!("change {kind} {id} {field}"));
    }
    for note in notes.iter().take(5) {
        lines.push(format!("note {note}"));
    }
    for remedy in remedies.iter().take(5) {
        lines.push(remedy.clone());
    }
    let mut text = lines.join("\n");
    if text.chars().count() > 4000 {
        text = text.chars().take(4000).collect();
    }
    text
}

/// Process start/stop does not wake the pilot, and neither does any process
/// journal line other than a stored health change. The 50–90% host-share
/// band is display-only, so it never becomes a health line. Failed and
/// degraded (memory) still wake, and so does a finding.
fn wakes_pilot(change: &Value) -> bool {
    let id = change
        .get("resource_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if !id.starts_with("proc:") {
        return true;
    }
    let kind = change.get("kind").and_then(|v| v.as_str()).unwrap_or("");
    if kind == "appeared" || kind == "disappeared" {
        return false;
    }
    if change.get("field").and_then(|v| v.as_str()) != Some("health") {
        return false;
    }
    matches!(
        change.get("after").and_then(|v| v.as_str()),
        Some("failed" | "degraded")
    )
}

fn seq_of(v: &Value) -> i64 {
    v.get("as_of_seq").and_then(|n| n.as_i64()).unwrap_or(0)
}

fn open_findings(v: &Value) -> Vec<OpenFinding> {
    v.get("findings")
        .and_then(|f| f.as_array())
        .into_iter()
        .flatten()
        .filter_map(|f| {
            Some(OpenFinding {
                id: f.get("id")?.as_str()?.to_string(),
                resource_id: f.get("resource_id")?.as_str()?.to_string(),
                summary: f
                    .get("summary")
                    .and_then(|s| s.as_str())
                    .unwrap_or("")
                    .to_string(),
                severity: f
                    .get("severity")
                    .and_then(|s| s.as_str())
                    .unwrap_or("")
                    .to_string(),
                rule: f
                    .get("rule")
                    .and_then(|s| s.as_str())
                    .unwrap_or("")
                    .to_string(),
            })
        })
        .collect()
}

fn tool_defs(execute_auto: bool) -> Value {
    let mut tools = vec![
        tool(
            "state_snapshot",
            "Summary of current state.",
            json!({"type":"object","properties":{"detail":{"type":"string"},"if_changed_since":{"type":"integer"}}}),
        ),
        tool(
            "resource_explain",
            "One resource: attributes, metrics, changes, notes, findings.",
            json!({"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}),
        ),
        tool(
            "changes_since",
            "Changes after a cursor.",
            json!({"type":"object","properties":{"since":{"type":"string"},"limit":{"type":"integer"}},"required":["since"]}),
        ),
        tool(
            "metrics_query",
            "Short metric history.",
            json!({"type":"object","properties":{"id":{"type":"string"},"metric":{"type":"string"},"range":{"type":"string"}},"required":["id","metric"]}),
        ),
        tool(
            "logs_query",
            "Recent error lines for one resource.",
            json!({"type":"object","properties":{"resource":{"type":"string"},"limit":{"type":"integer"}},"required":["resource"]}),
        ),
        tool(
            "findings_list",
            "Open findings.",
            json!({"type":"object","properties":{"status":{"type":"string"},"detail":{"type":"boolean"},"limit":{"type":"integer"}}}),
        ),
        tool(
            "memory_search",
            "Notes matching a substring.",
            json!({"type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer"}},"required":["query"]}),
        ),
        tool(
            "memory_note",
            "Attach a conclusion to a resource.",
            json!({"type":"object","properties":{"resource":{"type":"string"},"text":{"type":"string"},"author":{"type":"string"}},"required":["resource","text"]}),
        ),
        tool(
            "actions_list",
            "Remediations and their risk tiers.",
            json!({"type":"object","properties":{}}),
        ),
        tool(
            "action_plan",
            "Plan a remediation. Does not run it. Pass finding_id from the digest.",
            json!({"type":"object","properties":{"action":{"type":"string"},"params":{"type":"object"},"resource_id":{"type":"string"},"finding_id":{"type":"string"}},"required":["action"]}),
        ),
        tool(
            "action_status",
            "Read one run. Says whether its finding is resolved, still open, or the approval is still waiting.",
            json!({"type":"object","properties":{"run_id":{"type":"string"}},"required":["run_id"]}),
        ),
        tool(
            "escalate",
            "Hand this incident to the next model tier.",
            json!({"type":"object","properties":{}}),
        ),
    ];
    if execute_auto {
        tools.push(tool(
            "action_execute",
            "Execute a plan whose tier is auto. Approve and deny never run.",
            json!({"type":"object","properties":{"plan_id":{"type":"string"}},"required":["plan_id"]}),
        ));
    }
    Value::Array(tools)
}

fn tool(name: &str, description: &str, parameters: Value) -> Value {
    json!({
        "type": "function",
        "function": {"name": name, "description": description, "parameters": parameters}
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chat::Chat;
    use crate::config::{EndpointConfig, PilotConfig, TierConfig};
    use crate::mcp::Tools;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::time::Duration;

    struct FakeTools {
        findings: Value,
        changes: Value,
        calls: Vec<String>,
        plan_tier: &'static str,
        plan_args: Vec<Value>,
        remedy: Value,
    }

    impl Tools for FakeTools {
        fn call(&mut self, name: &str, args: Value) -> Result<Value, String> {
            self.calls.push(name.to_string());
            match name {
                "findings_list" => Ok(self.findings.clone()),
                "changes_since" => Ok(self.changes.clone()),
                "memory_search" => Ok(json!({"notes": []})),
                "memory_note" => Ok(
                    json!({"id": "n1", "text": args.get("text").cloned().unwrap_or(Value::Null)}),
                ),
                "remedies_list" => Ok(self.remedy.clone()),
                "action_plan" => {
                    self.plan_args.push(args);
                    Ok(json!({
                        "plan_id": "p1",
                        "run_id": "p1",
                        "tier": self.plan_tier,
                        "status": "planned"
                    }))
                }
                "action_execute" => Ok(json!({"status": "succeeded", "id": "p1"})),
                "action_status" => Ok(json!({
                    "id": "p1",
                    "status": "succeeded",
                    "finding_id": "f1",
                    "finding_outcome": "still_open"
                })),
                "resource_explain" => {
                    Ok(json!({"id": "unit:web1:sshd.service", "health": "failed"}))
                }
                "logs_query" => Ok(json!({"lines": [{"message": "Failed to start sshd"}]})),
                other => Ok(json!({"tool": other})),
            }
        }
    }

    struct ScriptChat {
        rounds: Vec<Value>,
        n: usize,
        urls: Vec<String>,
    }

    impl Chat for ScriptChat {
        fn complete(&mut self, endpoint: &EndpointConfig, _body: &Value) -> Result<Value, String> {
            self.urls.push(endpoint.base_url.clone());
            if endpoint.base_url.contains("down.example") {
                return Err("endpoint down".into());
            }
            let i = self.n;
            self.n += 1;
            self.rounds
                .get(i)
                .cloned()
                .ok_or_else(|| "script exhausted".into())
        }
    }

    fn finding_page() -> Value {
        json!({
            "as_of_seq": 4,
            "findings": [{
                "id": "f1",
                "resource_id": "unit:web1:sshd.service",
                "severity": "critical",
                "summary": "sshd.service is failed",
                "rule": "health"
            }]
        })
    }

    fn empty_changes() -> Value {
        json!({"as_of_seq": 4, "changes": []})
    }

    fn local_config() -> PilotConfig {
        PilotConfig {
            enabled: true,
            tiers: vec![TierConfig {
                name: "triage".into(),
                endpoints: vec![EndpointConfig {
                    kind: "openai_compatible".into(),
                    base_url: "http://127.0.0.1:11434/v1".into(),
                    model: "local".into(),
                    api_key_env: String::new(),
                    headers: BTreeMap::new(),
                }],
            }],
            ..PilotConfig::default()
        }
    }

    fn completion(calls: Value, content: &str) -> Value {
        json!({
            "choices": [{"message": {"role": "assistant", "content": content, "tool_calls": calls}}],
            "usage": {"total_tokens": 40}
        })
    }

    fn call(id: &str, name: &str, arguments: Value) -> Value {
        json!({
            "id": id,
            "type": "function",
            "function": {"name": name, "arguments": arguments.to_string()}
        })
    }

    #[test]
    fn steady_state_does_not_call_the_model() {
        let mut session = Session::new(local_config(), Instant::now());
        let mut tools = FakeTools {
            findings: json!({"as_of_seq": 1, "findings": []}),
            changes: json!({"as_of_seq": 1, "changes": []}),
            calls: Vec::new(),
            plan_tier: "approve",
            plan_args: Vec::new(),
            remedy: json!({}),
        };
        let mut chat = ScriptChat {
            rounds: vec![],
            n: 0,
            urls: vec![],
        };
        let run = session.tick(&mut tools, &mut chat, Instant::now()).unwrap();
        assert!(run.is_none());
        assert!(chat.urls.is_empty());
    }

    #[test]
    fn failed_unit_notes_and_plans_a_restart_within_budget() {
        let mut session = Session::new(local_config(), Instant::now());
        let mut tools = FakeTools {
            findings: finding_page(),
            changes: empty_changes(),
            calls: Vec::new(),
            plan_tier: "approve",
            plan_args: Vec::new(),
            remedy: json!({}),
        };
        let mut chat = ScriptChat {
            rounds: vec![
                completion(
                    json!([
                        call(
                            "1",
                            "resource_explain",
                            json!({"id": "unit:web1:sshd.service"})
                        ),
                        call(
                            "2",
                            "logs_query",
                            json!({"resource": "unit:web1:sshd.service"})
                        )
                    ]),
                    "",
                ),
                completion(
                    json!([
                        call(
                            "3",
                            "memory_note",
                            json!({"resource": "unit:web1:sshd.service", "text": "sshd failed; restart planned"})
                        ),
                        call(
                            "4",
                            "action_plan",
                            json!({"action": "systemd.restart", "params": {"unit": "sshd.service"}, "resource_id": "unit:web1:sshd.service"})
                        )
                    ]),
                    "",
                ),
                completion(json!([]), "sshd failed; restart is waiting for approval"),
            ],
            n: 0,
            urls: vec![],
        };
        let now = Instant::now();
        let run = session.tick(&mut tools, &mut chat, now).unwrap().unwrap();
        assert_eq!(run.status, "ok");
        assert!(run.tool_calls <= 12);
        assert!(run.proposed.iter().any(|a| a == "systemd.restart"));
        assert!(run.conclusion.contains("restart"));
        assert!(!tools.calls.iter().any(|c| c == "action_execute"));
        assert!(tools.calls.iter().any(|c| c == "memory_note"));
        let again = session.tick(&mut tools, &mut chat, now).unwrap();
        assert!(
            again.is_none(),
            "the same open finding does not wake again until the heartbeat"
        );
    }

    #[test]
    fn approve_tier_is_not_executed_even_when_auto_is_enabled() {
        let mut config = local_config();
        config.execute_auto = true;
        let mut session = Session::new(config, Instant::now());
        let mut tools = FakeTools {
            findings: finding_page(),
            changes: empty_changes(),
            calls: Vec::new(),
            plan_tier: "approve",
            plan_args: Vec::new(),
            remedy: json!({}),
        };
        let mut chat = ScriptChat {
            rounds: vec![
                completion(
                    json!([
                        call(
                            "1",
                            "action_plan",
                            json!({"action": "systemd.restart", "params": {"unit": "sshd"}})
                        ),
                        call("2", "action_execute", json!({"plan_id": "p1"}))
                    ]),
                    "",
                ),
                completion(json!([]), "planned"),
            ],
            n: 0,
            urls: vec![],
        };
        let run = session
            .tick(&mut tools, &mut chat, Instant::now())
            .unwrap()
            .unwrap();
        assert!(run.proposed.iter().any(|a| a == "systemd.restart"));
        assert!(!tools.calls.iter().any(|c| c == "action_execute"));
        assert_eq!(run.status, "ok");
    }

    #[test]
    fn auto_tier_executes_only_when_enabled() {
        let mut config = local_config();
        config.execute_auto = true;
        let mut session = Session::new(config, Instant::now());
        let mut tools = FakeTools {
            findings: finding_page(),
            changes: empty_changes(),
            calls: Vec::new(),
            plan_tier: "auto",
            plan_args: Vec::new(),
            remedy: json!({}),
        };
        let mut chat = ScriptChat {
            rounds: vec![
                completion(
                    json!([
                        call(
                            "1",
                            "action_plan",
                            json!({"action": "script.run", "params": {"script": "/usr/local/lib/pulseblade/hook"}})
                        ),
                        call("2", "action_execute", json!({"plan_id": "p1"}))
                    ]),
                    "",
                ),
                completion(json!([]), "ran"),
            ],
            n: 0,
            urls: vec![],
        };
        let run = session
            .tick(&mut tools, &mut chat, Instant::now())
            .unwrap()
            .unwrap();
        assert!(tools.calls.iter().any(|c| c == "action_execute"));
        assert_eq!(run.status, "ok");
    }

    #[test]
    fn tool_budget_stops_the_loop() {
        let mut config = local_config();
        config.budget.max_tool_calls = 1;
        let mut session = Session::new(config, Instant::now());
        let mut tools = FakeTools {
            findings: finding_page(),
            changes: empty_changes(),
            calls: Vec::new(),
            plan_tier: "approve",
            plan_args: Vec::new(),
            remedy: json!({}),
        };
        let forever = completion(
            json!([call(
                "1",
                "resource_explain",
                json!({"id": "unit:web1:sshd.service"})
            )]),
            "",
        );
        let mut chat = ScriptChat {
            rounds: vec![forever.clone(), forever],
            n: 0,
            urls: vec![],
        };
        let run = session
            .tick(&mut tools, &mut chat, Instant::now())
            .unwrap()
            .unwrap();
        assert_eq!(run.status, "budget");
        assert_eq!(run.tool_calls, 1);
    }

    #[test]
    fn local_only_refuses_a_remote_endpoint() {
        let mut config = local_config();
        config.local_only = true;
        config.tiers[0].endpoints[0].base_url = "https://models.example/v1".into();
        let mut session = Session::new(config, Instant::now());
        let mut tools = FakeTools {
            findings: finding_page(),
            changes: empty_changes(),
            calls: Vec::new(),
            plan_tier: "approve",
            plan_args: Vec::new(),
            remedy: json!({}),
        };
        let mut chat = ScriptChat {
            rounds: vec![],
            n: 0,
            urls: vec![],
        };
        let run = session
            .tick(&mut tools, &mut chat, Instant::now())
            .unwrap()
            .unwrap();
        assert_eq!(run.status, "error");
        assert!(chat.urls.is_empty());
        assert_eq!(run.detail, "no usable endpoint");
    }

    #[test]
    fn fallback_skips_a_down_endpoint() {
        let mut config = local_config();
        config.tiers[0].endpoints.insert(
            0,
            EndpointConfig {
                kind: "openai_compatible".into(),
                base_url: "http://down.example/v1".into(),
                model: "x".into(),
                api_key_env: String::new(),
                headers: BTreeMap::new(),
            },
        );
        let mut session = Session::new(config, Instant::now());
        let mut tools = FakeTools {
            findings: finding_page(),
            changes: empty_changes(),
            calls: Vec::new(),
            plan_tier: "approve",
            plan_args: Vec::new(),
            remedy: json!({}),
        };
        let mut chat = ScriptChat {
            rounds: vec![completion(json!([]), "quiet")],
            n: 0,
            urls: vec![],
        };
        let run = session
            .tick(&mut tools, &mut chat, Instant::now())
            .unwrap()
            .unwrap();
        assert_eq!(run.status, "ok");
        assert!(chat.urls.iter().any(|u| u.contains("down.example")));
        assert!(chat.urls.iter().any(|u| u.contains("127.0.0.1")));
    }

    #[test]
    fn heartbeat_reinvestigates_an_open_finding() {
        let mut config = local_config();
        config.heartbeat_secs = 10;
        let start = Instant::now();
        let mut session = Session::new(config, start);
        let mut tools = FakeTools {
            findings: finding_page(),
            changes: empty_changes(),
            calls: Vec::new(),
            plan_tier: "approve",
            plan_args: Vec::new(),
            remedy: json!({}),
        };
        let round = completion(json!([]), "seen");
        let mut chat = ScriptChat {
            rounds: vec![round.clone(), round],
            n: 0,
            urls: vec![],
        };
        assert!(session
            .tick(&mut tools, &mut chat, start)
            .unwrap()
            .is_some());
        assert!(session
            .tick(&mut tools, &mut chat, start)
            .unwrap()
            .is_none());
        let later = start + Duration::from_secs(11);
        let run = session.tick(&mut tools, &mut chat, later).unwrap().unwrap();
        assert_eq!(run.trigger, "heartbeat");
    }

    #[test]
    fn follow_up_reads_the_outcome_after_auto_execute() {
        let mut config = local_config();
        config.execute_auto = true;
        config.heartbeat_secs = 10;
        let start = Instant::now();
        let mut session = Session::new(config, start);
        let mut tools = FakeTools {
            findings: finding_page(),
            changes: empty_changes(),
            calls: Vec::new(),
            plan_tier: "auto",
            plan_args: Vec::new(),
            remedy: json!({}),
        };
        let mut chat = ScriptChat {
            rounds: vec![
                completion(
                    json!([
                        call(
                            "1",
                            "action_plan",
                            json!({
                                "action": "script.run",
                                "params": {"script": "/usr/local/lib/pulseblade/hook"},
                                "resource_id": "unit:web1:sshd.service"
                            })
                        ),
                        call("2", "action_execute", json!({"plan_id": "p1"}))
                    ]),
                    "",
                ),
                completion(json!([]), "ran"),
                completion(
                    json!([call("3", "action_status", json!({"run_id": "p1"}))]),
                    "",
                ),
                completion(json!([]), "finding still open"),
            ],
            n: 0,
            urls: vec![],
        };
        let first = session.tick(&mut tools, &mut chat, start).unwrap().unwrap();
        assert_eq!(first.status, "ok");
        assert!(tools.calls.iter().any(|c| c == "action_execute"));
        assert_eq!(
            tools.plan_args[0]
                .get("finding_id")
                .and_then(|v| v.as_str()),
            Some("f1")
        );
        let later = start + Duration::from_secs(11);
        let second = session.tick(&mut tools, &mut chat, later).unwrap().unwrap();
        assert_eq!(second.trigger, "heartbeat");
        assert_eq!(second.status, "ok");
        assert!(tools.calls.iter().any(|c| c == "action_status"));
        assert_eq!(
            tools
                .calls
                .iter()
                .filter(|c| *c == "action_execute")
                .count(),
            1
        );
    }

    #[test]
    fn action_status_is_available_when_auto_execute_is_off() {
        let mut session = Session::new(local_config(), Instant::now());
        let mut tools = FakeTools {
            findings: finding_page(),
            changes: empty_changes(),
            calls: Vec::new(),
            plan_tier: "approve",
            plan_args: Vec::new(),
            remedy: json!({}),
        };
        let mut chat = ScriptChat {
            rounds: vec![
                completion(
                    json!([
                        call(
                            "1",
                            "action_plan",
                            json!({
                                "action": "systemd.restart",
                                "resource_id": "unit:web1:sshd.service",
                                "finding_id": "f-kept"
                            })
                        ),
                        call("2", "action_status", json!({"run_id": "p1"}))
                    ]),
                    "",
                ),
                completion(json!([]), "waiting"),
            ],
            n: 0,
            urls: vec![],
        };
        let run = session
            .tick(&mut tools, &mut chat, Instant::now())
            .unwrap()
            .unwrap();
        assert_eq!(run.status, "ok");
        assert!(tools.calls.iter().any(|c| c == "action_status"));
        assert!(!tools.calls.iter().any(|c| c == "action_execute"));
        assert_eq!(
            tools.plan_args[0]
                .get("finding_id")
                .and_then(|v| v.as_str()),
            Some("f-kept")
        );
    }

    fn quiet_tools(findings: Value, changes: Value) -> FakeTools {
        FakeTools {
            findings,
            changes,
            calls: Vec::new(),
            plan_tier: "approve",
            plan_args: Vec::new(),
            remedy: json!({}),
        }
    }

    #[test]
    fn process_churn_and_busy_band_do_not_wake() {
        assert!(!PilotConfig::default().execute_auto);
        let start = Instant::now();
        let mut config = local_config();
        config.heartbeat_secs = 10;
        assert!(!config.execute_auto);
        let mut session = Session::new(config, start);
        let changes = json!({
            "as_of_seq": 8,
            "changes": [
                {"resource_id": "proc:h:1", "kind": "appeared", "after": {"health": "ok"}},
                {"resource_id": "proc:h:2", "kind": "disappeared"},
                {"resource_id": "proc:h:3", "kind": "changed", "field": "health", "before": "ok", "after": "ok"},
                {"resource_id": "proc:h:4", "kind": "changed", "field": "attrs.cmd", "before": "a", "after": "game"}
            ]
        });
        let mut tools = quiet_tools(json!({"as_of_seq": 8, "findings": []}), changes);
        let mut chat = ScriptChat {
            rounds: vec![],
            n: 0,
            urls: vec![],
        };
        assert!(session
            .tick(&mut tools, &mut chat, start)
            .unwrap()
            .is_none());
        assert!(session
            .tick(&mut tools, &mut chat, start + Duration::from_secs(11))
            .unwrap()
            .is_none());
        assert!(chat.urls.is_empty());
        assert!(!tools.calls.iter().any(|c| c == "action_execute"));
    }

    #[test]
    fn process_health_failed_still_wakes_without_executing() {
        let mut session = Session::new(local_config(), Instant::now());
        let mut tools = quiet_tools(
            json!({"as_of_seq": 9, "findings": []}),
            json!({
                "as_of_seq": 9,
                "changes": [
                    {"resource_id": "proc:h:1", "kind": "appeared"},
                    {
                        "resource_id": "proc:h:9",
                        "kind": "changed",
                        "field": "health",
                        "before": "ok",
                        "after": "failed"
                    }
                ]
            }),
        );
        let mut chat = ScriptChat {
            rounds: vec![completion(json!([]), "process is using the host")],
            n: 0,
            urls: vec![],
        };
        let run = session
            .tick(&mut tools, &mut chat, Instant::now())
            .unwrap()
            .expect("failed process health should wake");
        assert_eq!(run.trigger, "changes");
        assert!(!tools.calls.iter().any(|c| c == "action_execute"));
        assert!(!chat.urls.is_empty());
    }

    #[test]
    fn process_memory_degraded_still_wakes() {
        let mut session = Session::new(local_config(), Instant::now());
        let mut tools = quiet_tools(
            json!({"as_of_seq": 3, "findings": []}),
            json!({
                "as_of_seq": 3,
                "changes": [{
                    "resource_id": "proc:h:4",
                    "kind": "changed",
                    "field": "health",
                    "before": "ok",
                    "after": "degraded"
                }]
            }),
        );
        let mut chat = ScriptChat {
            rounds: vec![completion(json!([]), "memory")],
            n: 0,
            urls: vec![],
        };
        let run = session
            .tick(&mut tools, &mut chat, Instant::now())
            .unwrap()
            .expect("degraded process memory should wake");
        assert_eq!(run.trigger, "changes");
        assert!(!tools.calls.iter().any(|c| c == "action_execute"));
    }

    #[test]
    fn finding_wakes_when_the_journal_is_only_process_churn() {
        let mut session = Session::new(local_config(), Instant::now());
        let mut tools = quiet_tools(
            finding_page(),
            json!({
                "as_of_seq": 4,
                "changes": [
                    {"resource_id": "proc:h:1", "kind": "appeared"},
                    {"resource_id": "proc:h:1", "kind": "disappeared"}
                ]
            }),
        );
        let mut chat = ScriptChat {
            rounds: vec![completion(json!([]), "finding")],
            n: 0,
            urls: vec![],
        };
        let run = session
            .tick(&mut tools, &mut chat, Instant::now())
            .unwrap()
            .expect("an open finding should wake");
        assert_eq!(run.trigger, "finding:f1");
        assert!(!tools.calls.iter().any(|c| c == "action_execute"));
    }

    fn workload_page() -> Value {
        json!({
            "as_of_seq": 9,
            "findings": [{
                "id": "f-repeat",
                "resource_id": "workload:poc-apps:other:deployment:api",
                "severity": "critical",
                "summary": "api is failed",
                "rule": "health"
            }]
        })
    }

    fn rollout_remedy() -> Value {
        json!({
            "matched": {
                "rule": "health",
                "kind": "workload",
                "action": "k8s.rollout_restart",
                "params": {"kind": "$kind", "namespace": "$namespace", "name": "$name"},
                "tier": "approve",
                "outcome": "resolved"
            }
        })
    }

    #[test]
    fn repeat_finding_files_the_known_plan_and_does_not_call_the_model() {
        let mut session = Session::new(local_config(), Instant::now());
        let mut tools = FakeTools {
            findings: workload_page(),
            changes: empty_changes(),
            calls: Vec::new(),
            plan_tier: "approve",
            plan_args: Vec::new(),
            remedy: rollout_remedy(),
        };
        let mut chat = ScriptChat {
            rounds: vec![],
            n: 0,
            urls: vec![],
        };
        let run = session
            .tick(&mut tools, &mut chat, Instant::now())
            .unwrap()
            .unwrap();
        assert_eq!(run.tokens, 0);
        assert_eq!(run.status, "remedy");
        assert_eq!(chat.n, 0);
        assert!(tools.calls.iter().any(|c| c == "action_plan"));
        assert!(!tools.calls.iter().any(|c| c == "action_execute"));
        let args = &tools.plan_args[0];
        assert_eq!(args["action"], "k8s.rollout_restart");
        assert_eq!(args["finding_id"], "f-repeat");
        assert_eq!(args["params"]["namespace"], "other");
        assert_eq!(args["params"]["name"], "api");
        assert_eq!(args["params"]["kind"], "deployment");
        assert!(!args.to_string().contains("pb-fault-loop"));
    }

    #[test]
    fn open_plan_suppresses_a_second_run() {
        let mut session = Session::new(local_config(), Instant::now());
        let mut tools = FakeTools {
            findings: workload_page(),
            changes: empty_changes(),
            calls: Vec::new(),
            plan_tier: "approve",
            plan_args: Vec::new(),
            remedy: json!({"open_plan": true}),
        };
        let mut chat = ScriptChat {
            rounds: vec![],
            n: 0,
            urls: vec![],
        };
        let run = session.tick(&mut tools, &mut chat, Instant::now()).unwrap();
        assert!(run.is_none());
        assert_eq!(chat.n, 0);
        assert!(!tools.calls.iter().any(|c| c == "action_plan"));
        assert!(!tools.calls.iter().any(|c| c == "action_execute"));
    }

    #[test]
    fn digest_includes_the_closest_remedies() {
        let finding = OpenFinding {
            id: "f1".into(),
            resource_id: "workload:c:ns:deployment:api".into(),
            summary: "api is failed".into(),
            severity: "critical".into(),
            rule: "health".into(),
        };
        let text = digest(
            &[finding],
            &[],
            &[],
            &["remedy health service systemd.restart approve {\"unit\":\"$unit\"}".into()],
            3,
        );
        assert!(text.contains("remedy health service systemd.restart"));
    }
}
