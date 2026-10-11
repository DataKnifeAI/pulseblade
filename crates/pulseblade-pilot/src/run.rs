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
The digest already includes the finding, recent changes, and prior notes. \
Use state_snapshot with no arguments or detail=brief, short metrics_query ranges, \
and one resource_explain. Write a short conclusion with memory_note on the resource. \
Propose remediation with action_plan. Never approve an action. \
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
            .cloned()
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
        let digest = digest(&focus, &change_list, &notes, seq);
        let run = investigate(&self.config, tools, chat, &trigger, &digest);
        for finding in &open {
            self.seen.insert(finding.id.clone());
        }
        self.seq = seq;
        Ok(Some(run))
    }
}

fn investigate<T: Tools, C: Chat>(
    config: &PilotConfig,
    tools: &mut T,
    chat: &mut C,
    trigger: &str,
    digest: &str,
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
                let result = apply_tool(
                    tools,
                    &call.name,
                    &call.arguments,
                    config.execute_auto,
                    &mut plans,
                    &mut run.proposed,
                    &mut note,
                );
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

fn apply_tool<T: Tools>(
    tools: &mut T,
    name: &str,
    args: &Value,
    execute_auto: bool,
    plans: &mut BTreeMap<String, String>,
    proposed: &mut Vec<String>,
    note: &mut String,
) -> Value {
    if name == "action_execute" {
        return guard_execute(tools, args, execute_auto, plans);
    }
    if !allowed(name) {
        return json!({"error": format!("tool `{name}` is not available to the pilot")});
    }
    let args = clamp(name, args.clone());
    if name == "memory_note" {
        if let Some(text) = args.get("text").and_then(|t| t.as_str()) {
            *note = text.to_string();
        }
    }
    if name == "action_plan" {
        if let Some(action) = args.get("action").and_then(|a| a.as_str()) {
            proposed.push(action.to_string());
        }
    }
    match tools.call(name, args) {
        Ok(v) => {
            if name == "action_plan" {
                if let (Some(id), Some(tier)) = (
                    v.get("plan_id").and_then(|s| s.as_str()),
                    v.get("tier").and_then(|s| s.as_str()),
                ) {
                    plans.insert(id.to_string(), tier.to_string());
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
    )
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

fn digest(focus: &[OpenFinding], changes: &[Value], notes: &[String], seq: i64) -> String {
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
    let mut text = lines.join("\n");
    if text.chars().count() > 4000 {
        text = text.chars().take(4000).collect();
    }
    text
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
            "Plan a remediation. Does not run it.",
            json!({"type":"object","properties":{"action":{"type":"string"},"params":{"type":"object"},"resource_id":{"type":"string"}},"required":["action"]}),
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
                "action_plan" => Ok(json!({
                    "plan_id": "p1",
                    "run_id": "p1",
                    "tier": self.plan_tier,
                    "status": "planned"
                })),
                "action_execute" => Ok(json!({"status": "succeeded", "id": "p1"})),
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
}
