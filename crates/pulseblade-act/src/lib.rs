//! Node-local remediation: a registry of gated actions and a hash-chained audit log.
//!
//! Policy lives on the node. Planning does not run anything. `auto` runs on
//! execute; `approve` waits until `approve` (CLI or HTTP); `deny` never runs.

mod config;
mod k8s;
mod proxmox;

use proxmox::{HttpClient, ProcessEnv, SecretLookup, UreqClient};

use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use pulseblade_core::{
    bind_remedy_params, resource_kind_of_id, ActionRun, ActionRunStatus, FindingStatus,
    ResourceKind, RiskTier,
};
use pulseblade_store::{Store, StoreError};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub use config::{parse_ttl, ActionsConfig, PathAction, TierLimit};
pub use k8s::{IdleK8s, K8sApi, K8sEndpoint};
pub use proxmox::ProxmoxEndpoint;

/// Runs a program without a shell. Tests substitute a fake.
pub trait CommandRunner: Send + Sync {
    fn run(&self, program: &str, args: &[String]) -> Result<String, String>;
}

/// `std::process::Command`. No shell interpolation.
pub struct SystemRunner;

impl CommandRunner for SystemRunner {
    fn run(&self, program: &str, args: &[String]) -> Result<String, String> {
        let out = std::process::Command::new(program)
            .args(args)
            .output()
            .map_err(|e| format!("{program}: {e}"))?;
        let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
        let err = String::from_utf8_lossy(&out.stderr);
        if !err.trim().is_empty() {
            if !text.is_empty() && !text.ends_with('\n') {
                text.push('\n');
            }
            text.push_str(err.trim());
        }
        let text = clip(&text, 2000);
        if out.status.success() {
            Ok(if text.is_empty() { "ok".into() } else { text })
        } else {
            Err(format!("exit {}: {text}", out.status))
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ActError {
    #[error("{0}")]
    Invalid(String),
    #[error("unknown action run `{0}`")]
    NotFound(String),
    #[error(transparent)]
    Store(StoreError),
}

impl From<StoreError> for ActError {
    fn from(e: StoreError) -> Self {
        match e {
            StoreError::UnknownAction(id) => Self::NotFound(id),
            other => Self::Store(other),
        }
    }
}

/// One built-in action as an agent should see it.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ActionView {
    pub name: String,
    pub description: String,
    pub tier: RiskTier,
    pub params_schema: Value,
    pub applies_to: Vec<ResourceKind>,
    pub preconditions: Vec<String>,
    pub rate_per_hour: u32,
    /// `action_plan` accepts `dry_run: true` and execute then skips the command.
    pub supports_dry_run: bool,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ActionCatalog {
    pub actions: Vec<ActionView>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct ActionsParams {}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PlanParams {
    /// `systemd.restart`, `docker.restart`, `disk.prune_path`, or `script.run`.
    pub action: String,
    /// Parameters matching the action's JSON schema. Defaults to `{}`.
    #[serde(default)]
    pub params: Value,
    /// Resource the run is filed under. Derived from params when omitted.
    #[serde(default)]
    pub resource_id: Option<String>,
    /// Repeat plans with the same key return the in-flight run.
    #[serde(default)]
    pub idempotency_key: Option<String>,
    /// Record the run without invoking the command.
    #[serde(default)]
    pub dry_run: bool,
    /// Finding this plan is for. The run stays tied to it.
    #[serde(default)]
    pub finding_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PlanResult {
    pub plan_id: String,
    /// Same id `action_execute` and `action_status` use.
    pub run_id: String,
    pub tier: RiskTier,
    pub status: ActionRunStatus,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub dry_run: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,
    /// Echo of the finding id the caller supplied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finding_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ExecuteParams {
    pub plan_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StatusParams {
    pub run_id: String,
}

pub struct Registry {
    config: ActionsConfig,
    ttl: chrono::Duration,
    runner: Arc<dyn CommandRunner>,
    http: Arc<dyn HttpClient>,
    secrets: Arc<dyn SecretLookup>,
    proxmox: ProxmoxEndpoint,
    k8s: K8sEndpoint,
    k8s_api: Arc<dyn K8sApi>,
}

impl Registry {
    pub fn new(config: ActionsConfig, runner: Arc<dyn CommandRunner>) -> Result<Self, ActError> {
        let ttl = parse_ttl(&config.approval_ttl).map_err(ActError::Invalid)?;
        Ok(Self {
            config,
            ttl,
            runner,
            http: Arc::new(UreqClient { verify_tls: true }),
            secrets: Arc::new(ProcessEnv),
            proxmox: ProxmoxEndpoint::default(),
            k8s: K8sEndpoint::default(),
            k8s_api: Arc::new(IdleK8s {
                message: "kubernetes API client is not connected".into(),
            }),
        })
    }

    /// API used by `k8s.rollout_restart` and `k8s.scale`.
    /// An empty URL leaves the actions unconfigured. A connect failure is returned
    /// and later performs fail with the same message.
    pub fn set_k8s(&mut self, endpoint: K8sEndpoint) -> Result<(), String> {
        self.k8s = endpoint.clone();
        if endpoint.api_url.trim().is_empty() {
            return Ok(());
        }
        #[cfg(feature = "k8s")]
        {
            match k8s::KubeApi::connect(&endpoint) {
                Ok(api) => {
                    self.k8s_api = Arc::new(api);
                    Ok(())
                }
                Err(e) => {
                    self.k8s_api = Arc::new(IdleK8s { message: e.clone() });
                    Err(e)
                }
            }
        }
        #[cfg(not(feature = "k8s"))]
        {
            let message = "k8s actions are not compiled in".to_string();
            self.k8s_api = Arc::new(IdleK8s {
                message: message.clone(),
            });
            Err(message)
        }
    }

    /// Install a client without dialing. Tests use this to prove the approve gate.
    pub fn with_k8s(mut self, endpoint: K8sEndpoint, api: Arc<dyn K8sApi>) -> Self {
        self.k8s = endpoint;
        self.k8s_api = api;
        self
    }

    /// API used by `proxmox.start`, `proxmox.stop`, and `proxmox.migrate`.
    pub fn set_proxmox(&mut self, endpoint: ProxmoxEndpoint) {
        let verify = endpoint.verify_tls;
        self.proxmox = endpoint;
        self.http = Arc::new(UreqClient { verify_tls: verify });
    }

    pub fn with_http(mut self, http: Arc<dyn HttpClient>) -> Self {
        self.http = http;
        self
    }

    pub fn with_secrets(mut self, secrets: Arc<dyn SecretLookup>) -> Self {
        self.secrets = secrets;
        self
    }

    pub fn builtin() -> Self {
        Self::new(ActionsConfig::default(), Arc::new(SystemRunner))
            .expect("default approval_ttl parses")
    }

    pub fn approve_token(&self) -> Option<&str> {
        let t = self.config.approve_token.trim();
        if t.is_empty() {
            None
        } else {
            Some(t)
        }
    }

    pub fn list(&self) -> ActionCatalog {
        ActionCatalog {
            actions: NAMES.iter().map(|n| self.view(n)).collect(),
        }
    }

    pub fn plan(&self, store: &Store, req: PlanParams) -> Result<PlanResult, ActError> {
        if !NAMES.contains(&req.action.as_str()) {
            return Err(ActError::Invalid(format!(
                "unknown action `{}`; known: {}",
                req.action,
                NAMES.join(", ")
            )));
        }
        let params = if req.params.is_null() {
            json!({})
        } else {
            req.params
        };
        if !params.is_object() {
            return Err(ActError::Invalid("params must be an object".into()));
        }
        validate_schema(&req.action, &params)?;
        let derived = check_params(&req.action, &params, &self.config, &self.proxmox, &self.k8s)?;
        let resource_id = match req.resource_id {
            Some(id) => clean_resource_id(&id)?,
            None => derived,
        };
        let finding_id = match req.finding_id {
            Some(id) => clean_finding_id(&id)?,
            None => None,
        };
        let key = match req.idempotency_key {
            Some(k) => clean_key(&k)?,
            None => default_key(&req.action, &params),
        };
        if let Some(existing) = store.open_action_by_key(&key)? {
            return Ok(plan_of(&existing));
        }
        if let Some(reason) = store.rejection(&resource_id, &req.action)? {
            return Err(ActError::Invalid(format!(
                "{} on {resource_id} was denied: {reason}",
                req.action
            )));
        }
        let since = Utc::now() - chrono::Duration::hours(1);
        let used = store.count_actions_since(&req.action, since)?;
        let rate = self.rate(&req.action);
        if used >= rate {
            return Err(ActError::Invalid(format!(
                "{} is limited to {rate} runs per hour ({used} already)",
                req.action
            )));
        }
        let tier = self.tier(&req.action);
        let run = store.insert_action(
            &pulseblade_store::NewAction {
                action: req.action,
                resource_id,
                params,
                idempotency_key: key,
                tier,
                dry_run: req.dry_run,
                finding_id,
            },
            Utc::now(),
        )?;
        Ok(plan_of(&run))
    }

    /// Start a planned run. Approve-tier runs wait; this call returns immediately.
    pub fn execute(&self, store: &Store, plan_id: &str) -> Result<ActionRun, ActError> {
        self.sweep(store)?;
        let run = self.load(store, plan_id)?;
        if run.status != ActionRunStatus::Planned {
            return Ok(run);
        }
        if run.dry_run {
            return self.finish(
                store,
                &run.id,
                ActionRunStatus::Planned,
                ActionRunStatus::Succeeded,
                Some("dry-run: command not executed"),
                "finish",
            );
        }
        match run.tier {
            RiskTier::Deny => self.finish(
                store,
                &run.id,
                ActionRunStatus::Planned,
                ActionRunStatus::Rejected,
                Some("risk tier deny does not run"),
                "reject",
            ),
            RiskTier::Approve => {
                let exp = Utc::now() + self.ttl;
                let waiting = store.transition_action(
                    &run.id,
                    pulseblade_store::ActionTransition {
                        from: ActionRunStatus::Planned,
                        to: ActionRunStatus::AwaitingApproval,
                        output: None,
                        expires_at: Some(exp),
                        event: "execute",
                    },
                )?;
                store.record_basis(&waiting.id)?;
                Ok(waiting)
            }
            RiskTier::Auto => self.invoke(store, &run),
        }
    }

    pub fn status(&self, store: &Store, run_id: &str) -> Result<ActionRun, ActError> {
        self.sweep(store)?;
        self.load(store, run_id)
    }

    /// Run an approve-tier action that still matches the open finding and resource.
    pub fn approve(&self, store: &Store, run_id: &str) -> Result<ActionRun, ActError> {
        self.sweep(store)?;
        let run = self.load(store, run_id)?;
        if run.status != ActionRunStatus::AwaitingApproval {
            return Err(ActError::Invalid(format!(
                "run {} is {}, not awaiting approval",
                run.id,
                run.status.as_str()
            )));
        }
        if let Some(why) = store.approval_block(&run)? {
            store.transition_action(
                &run.id,
                pulseblade_store::ActionTransition {
                    from: ActionRunStatus::AwaitingApproval,
                    to: ActionRunStatus::Expired,
                    output: Some(why),
                    expires_at: None,
                    event: "expire",
                },
            )?;
            return Err(ActError::Invalid(format!(
                "run {} was not approved ({why})",
                run.id
            )));
        }
        let claimed = store.transition_action(
            &run.id,
            pulseblade_store::ActionTransition {
                from: ActionRunStatus::AwaitingApproval,
                to: ActionRunStatus::Running,
                output: None,
                expires_at: None,
                event: "approve",
            },
        )?;
        if claimed.status != ActionRunStatus::Running {
            return Err(ActError::Invalid(format!(
                "run {} was not approved (status {})",
                claimed.id,
                claimed.status.as_str()
            )));
        }
        self.invoke_claimed(store, &claimed)
    }

    /// Cancel an approve-tier run that is still waiting. The command does not start.
    /// `reason` is stored on the run and blocks this action on this resource only.
    pub fn deny(&self, store: &Store, run_id: &str, reason: &str) -> Result<ActionRun, ActError> {
        let reason = reason.trim();
        if reason.is_empty() {
            return Err(ActError::Invalid("deny requires a reason".into()));
        }
        self.sweep(store)?;
        let run = self.load(store, run_id)?;
        if run.status != ActionRunStatus::AwaitingApproval {
            return Err(ActError::Invalid(format!(
                "run {} is {}, not awaiting approval",
                run.id,
                run.status.as_str()
            )));
        }
        let done = self.finish(
            store,
            &run.id,
            ActionRunStatus::AwaitingApproval,
            ActionRunStatus::Rejected,
            Some(reason),
            "deny",
        )?;
        store.record_rejection(&done.resource_id, &done.action, reason)?;
        Ok(done)
    }

    /// Close this request only. The finding stays open. No remedy and no rejection.
    pub fn dismiss(&self, store: &Store, run_id: &str) -> Result<ActionRun, ActError> {
        self.sweep(store)?;
        let run = self.load(store, run_id)?;
        if matches!(
            run.status,
            ActionRunStatus::Expired | ActionRunStatus::Rejected
        ) {
            return Ok(run);
        }
        if run.status != ActionRunStatus::AwaitingApproval {
            return Err(ActError::Invalid(format!(
                "run {} is {}, not awaiting approval",
                run.id,
                run.status.as_str()
            )));
        }
        self.finish(
            store,
            &run.id,
            ActionRunStatus::AwaitingApproval,
            ActionRunStatus::Expired,
            Some("dismissed"),
            "dismiss",
        )
    }

    /// Re-read the finding and resource. An unchanged failure keeps or restores
    /// the same waiting run. A change drops the plan. This does not call a model
    /// and does not start the command.
    pub fn recheck(&self, store: &Store, run_id: &str) -> Result<ActionRun, ActError> {
        self.sweep(store)?;
        let run = self.load(store, run_id)?;
        if !matches!(
            run.status,
            ActionRunStatus::AwaitingApproval | ActionRunStatus::Expired
        ) {
            return Err(ActError::Invalid(format!(
                "run {} is {}, not waiting",
                run.id,
                run.status.as_str()
            )));
        }
        if let Some(why) = store.approval_block(&run)? {
            if run.status == ActionRunStatus::AwaitingApproval {
                return self.finish(
                    store,
                    &run.id,
                    ActionRunStatus::AwaitingApproval,
                    ActionRunStatus::Expired,
                    Some(why),
                    "expire",
                );
            }
            if run.output.as_deref() != Some(why) {
                return Ok(store.transition_action(
                    &run.id,
                    pulseblade_store::ActionTransition {
                        from: ActionRunStatus::Expired,
                        to: ActionRunStatus::Expired,
                        output: Some(why),
                        expires_at: None,
                        event: "expire",
                    },
                )?);
            }
            return Ok(run);
        }
        if !store.finding_still_open(&run)? {
            return Ok(run);
        }
        let exp = Utc::now() + self.ttl;
        if run.status == ActionRunStatus::AwaitingApproval {
            return Ok(store.transition_action(
                &run.id,
                pulseblade_store::ActionTransition {
                    from: ActionRunStatus::AwaitingApproval,
                    to: ActionRunStatus::AwaitingApproval,
                    output: None,
                    expires_at: Some(exp),
                    event: "recheck",
                },
            )?);
        }
        Ok(store.reopen_approval(&run.id, exp)?)
    }

    fn sweep(&self, store: &Store) -> Result<(), ActError> {
        store.expire_actions(Utc::now(), self.ttl)?;
        Ok(())
    }

    fn invoke(&self, store: &Store, run: &ActionRun) -> Result<ActionRun, ActError> {
        let claimed = store.transition_action(
            &run.id,
            pulseblade_store::ActionTransition {
                from: ActionRunStatus::Planned,
                to: ActionRunStatus::Running,
                output: None,
                expires_at: None,
                event: "execute",
            },
        )?;
        if claimed.status != ActionRunStatus::Running {
            return Ok(claimed);
        }
        self.invoke_claimed(store, &claimed)
    }

    fn invoke_claimed(&self, store: &Store, run: &ActionRun) -> Result<ActionRun, ActError> {
        match perform(
            &run.action,
            &run.params,
            &self.config,
            self.runner.as_ref(),
            ExecEnv {
                http: self.http.as_ref(),
                secrets: self.secrets.as_ref(),
                proxmox: &self.proxmox,
                k8s: self.k8s_api.as_ref(),
            },
        ) {
            Ok(output) => self.finish(
                store,
                &run.id,
                ActionRunStatus::Running,
                ActionRunStatus::Succeeded,
                Some(&output),
                "finish",
            ),
            Err(err) => self.finish(
                store,
                &run.id,
                ActionRunStatus::Running,
                ActionRunStatus::Failed,
                Some(&err),
                "fail",
            ),
        }
    }

    fn finish(
        &self,
        store: &Store,
        id: &str,
        from: ActionRunStatus,
        to: ActionRunStatus,
        output: Option<&str>,
        event: &str,
    ) -> Result<ActionRun, ActError> {
        Ok(store.transition_action(
            id,
            pulseblade_store::ActionTransition {
                from,
                to,
                output,
                expires_at: None,
                event,
            },
        )?)
    }

    fn load(&self, store: &Store, id: &str) -> Result<ActionRun, ActError> {
        store
            .action(id)?
            .ok_or_else(|| ActError::NotFound(id.to_string()))
    }

    fn tier(&self, name: &str) -> RiskTier {
        let over = match name {
            "systemd.restart" => self.config.systemd_restart.tier,
            "docker.restart" => self.config.docker_restart.tier,
            "disk.prune_path" => self.config.disk_prune.tier,
            "script.run" => self.config.script_run.tier,
            "proxmox.start" => self.config.proxmox_start.tier,
            "proxmox.stop" => self.config.proxmox_stop.tier,
            "proxmox.migrate" => self.config.proxmox_migrate.tier,
            "k8s.rollout_restart" => self.config.k8s_rollout_restart.tier,
            "k8s.scale" => self.config.k8s_scale.tier,
            _ => None,
        };
        over.unwrap_or(match name {
            "script.run" => RiskTier::Deny,
            _ => RiskTier::Approve,
        })
    }

    fn rate(&self, name: &str) -> u32 {
        match name {
            "systemd.restart" => self.config.systemd_restart.rate_per_hour,
            "docker.restart" => self.config.docker_restart.rate_per_hour,
            "disk.prune_path" => self.config.disk_prune.rate_per_hour,
            "script.run" => self.config.script_run.rate_per_hour,
            "proxmox.start" => self.config.proxmox_start.rate_per_hour,
            "proxmox.stop" => self.config.proxmox_stop.rate_per_hour,
            "proxmox.migrate" => self.config.proxmox_migrate.rate_per_hour,
            "k8s.rollout_restart" => self.config.k8s_rollout_restart.rate_per_hour,
            "k8s.scale" => self.config.k8s_scale.rate_per_hour,
            _ => 0,
        }
    }

    fn view(&self, name: &str) -> ActionView {
        ActionView {
            name: name.into(),
            description: description(name).into(),
            tier: self.tier(name),
            params_schema: schema(name),
            applies_to: applies_to(name),
            preconditions: preconditions(name)
                .iter()
                .map(|s| (*s).to_string())
                .collect(),
            rate_per_hour: self.rate(name),
            supports_dry_run: true,
        }
    }
}

/// Plan params for a resolved remedy after the workload changed. `None` when
/// nothing matches or the action was denied for this resource. Does not call a model.
pub fn known_remedy_params(store: &Store, run: &ActionRun) -> Result<Option<PlanParams>, ActError> {
    if run.output.as_deref() != Some("resource changed") {
        return Ok(None);
    }
    let Some(finding_id) = run.finding_id.clone().filter(|id| !id.is_empty()) else {
        return Ok(None);
    };
    let Some(finding) = store.finding(&finding_id)? else {
        return Ok(None);
    };
    if finding.status == FindingStatus::Resolved {
        return Ok(None);
    }
    let kind = resource_kind_of_id(&run.resource_id).unwrap_or("");
    let page = store.remedy_page(
        &finding.rule,
        kind,
        Some(&finding_id),
        Some(&run.resource_id),
    )?;
    let Some(matched) = page.matched else {
        return Ok(None);
    };
    if page
        .rejections
        .iter()
        .any(|row| row.action == matched.action)
    {
        return Ok(None);
    }
    let params = bind_remedy_params(&matched.params, &run.resource_id)
        .ok_or_else(|| ActError::Invalid("remedy params do not bind to this resource".into()))?;
    Ok(Some(PlanParams {
        action: matched.action,
        params,
        resource_id: Some(run.resource_id.clone()),
        idempotency_key: None,
        dry_run: false,
        finding_id: Some(finding_id),
    }))
}

const NAMES: &[&str] = &[
    "systemd.restart",
    "docker.restart",
    "disk.prune_path",
    "script.run",
    "proxmox.start",
    "proxmox.stop",
    "proxmox.migrate",
    "k8s.rollout_restart",
    "k8s.scale",
];

fn plan_of(run: &ActionRun) -> PlanResult {
    PlanResult {
        plan_id: run.id.clone(),
        run_id: run.id.clone(),
        tier: run.tier,
        status: run.status,
        dry_run: run.dry_run,
        expires_at: run.expires_at,
        finding_id: run.finding_id.clone(),
    }
}

fn description(name: &str) -> &'static str {
    match name {
        "systemd.restart" => "Restart one systemd unit (system or --user).",
        "docker.restart" => "Restart one container via the docker CLI.",
        "disk.prune_path" => "Delete regular files in an allow-listed directory.",
        "script.run" => "Run an allow-listed program with no shell.",
        other if proxmox::is_proxmox(other) => proxmox::description(other),
        other if k8s::is_k8s(other) => k8s::description(other),
        _ => "",
    }
}

fn applies_to(name: &str) -> Vec<ResourceKind> {
    match name {
        "systemd.restart" => vec![ResourceKind::Service],
        "docker.restart" => vec![ResourceKind::Container],
        "disk.prune_path" => vec![ResourceKind::Disk],
        "proxmox.start" | "proxmox.stop" | "proxmox.migrate" => {
            vec![ResourceKind::Vm, ResourceKind::Container]
        }
        "k8s.rollout_restart" | "k8s.scale" => vec![ResourceKind::Workload],
        _ => vec![ResourceKind::Host],
    }
}

fn preconditions(name: &str) -> &'static [&'static str] {
    match name {
        "systemd.restart" => &[
            "unit name is a single systemd unit (no slashes)",
            "user=true restarts a user unit via systemctl --user",
        ],
        "docker.restart" => &["container name is a single docker name (no slashes)"],
        "disk.prune_path" => &[
            "path is an existing directory on the node allow list",
            "the directory is not /, /home, /etc, /usr, /var, /boot, or another system root",
            "only regular files in that directory are removed",
        ],
        "script.run" => &[
            "script is an absolute path on the node allow list",
            "arguments are passed without a shell",
        ],
        other if proxmox::is_proxmox(other) => proxmox::preconditions(other),
        other if k8s::is_k8s(other) => k8s::preconditions(other),
        _ => &[],
    }
}

fn schema(name: &str) -> Value {
    match name {
        "systemd.restart" => json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["unit"],
            "properties": {
                "unit": {"type": "string", "minLength": 1, "maxLength": 128},
                "user": {"type": "boolean"}
            }
        }),
        "docker.restart" => json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["container"],
            "properties": {
                "container": {"type": "string", "minLength": 1, "maxLength": 128}
            }
        }),
        "disk.prune_path" => json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["path"],
            "properties": {
                "path": {"type": "string", "minLength": 1, "maxLength": 512}
            }
        }),
        "script.run" => json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["script"],
            "properties": {
                "script": {"type": "string", "minLength": 1, "maxLength": 512},
                "args": {
                    "type": "array",
                    "maxItems": 16,
                    "items": {"type": "string", "maxLength": 200}
                }
            }
        }),
        other if proxmox::is_proxmox(other) => proxmox::schema(other),
        other if k8s::is_k8s(other) => k8s::schema(other),
        _ => json!({"type": "object"}),
    }
}

fn validate_schema(name: &str, params: &Value) -> Result<(), ActError> {
    let schema = schema(name);
    let validator = jsonschema::validator_for(&schema)
        .map_err(|e| ActError::Invalid(format!("action schema: {e}")))?;
    let errors: Vec<String> = validator
        .iter_errors(params)
        .map(|e| format!("{e}"))
        .take(3)
        .collect();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ActError::Invalid(format!(
            "params rejected: {}",
            errors.join("; ")
        )))
    }
}

fn check_params(
    name: &str,
    params: &Value,
    config: &ActionsConfig,
    proxmox_ep: &ProxmoxEndpoint,
    k8s_ep: &K8sEndpoint,
) -> Result<String, ActError> {
    match name {
        "systemd.restart" => {
            let unit = unit_name(params.get("unit").and_then(|v| v.as_str()).unwrap_or(""))?;
            let user = params
                .get("user")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            Ok(if user {
                format!("unit:local:user:{unit}")
            } else {
                format!("unit:local:{unit}")
            })
        }
        "docker.restart" => {
            let name = docker_name(
                params
                    .get("container")
                    .and_then(|v| v.as_str())
                    .unwrap_or(""),
            )?;
            Ok(format!("ctr:local:{name}"))
        }
        other if proxmox::is_proxmox(other) => {
            proxmox::ensure_configured(proxmox_ep)?;
            proxmox::resource_id(other, params)
        }
        other if k8s::is_k8s(other) => {
            k8s::ensure_configured(k8s_ep)?;
            k8s::resource_id(other, params, &k8s_ep.cluster)
        }
        "disk.prune_path" => {
            let path = params.get("path").and_then(|v| v.as_str()).unwrap_or("");
            let canon = resolve_allowed(path, &config.disk_prune.allow)?;
            Ok(format!("disk:local:{}", canon.display()))
        }
        "script.run" => {
            let script = params.get("script").and_then(|v| v.as_str()).unwrap_or("");
            let cleaned = clean_script(script)?;
            if !config
                .script_run
                .allow
                .iter()
                .any(|a| clean_script(a).ok().as_deref() == Some(cleaned.as_str()))
            {
                return Err(ActError::Invalid(
                    "script is not on the script.run allow list".into(),
                ));
            }
            if let Some(args) = params.get("args") {
                check_args(args)?;
            }
            Ok("host:local".into())
        }
        _ => Err(ActError::Invalid(format!("unknown action `{name}`"))),
    }
}

struct ExecEnv<'a> {
    http: &'a dyn HttpClient,
    secrets: &'a dyn SecretLookup,
    proxmox: &'a ProxmoxEndpoint,
    k8s: &'a dyn K8sApi,
}

fn perform(
    name: &str,
    params: &Value,
    config: &ActionsConfig,
    runner: &dyn CommandRunner,
    env: ExecEnv<'_>,
) -> Result<String, String> {
    match name {
        "systemd.restart" => {
            let unit = unit_name(params.get("unit").and_then(|v| v.as_str()).unwrap_or(""))
                .map_err(|e| e.to_string())?;
            let user = params
                .get("user")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let mut args = Vec::new();
            if user {
                args.push("--user".into());
            }
            args.push("restart".into());
            args.push(unit);
            runner.run("systemctl", &args)
        }
        "docker.restart" => {
            let name = docker_name(
                params
                    .get("container")
                    .and_then(|v| v.as_str())
                    .unwrap_or(""),
            )
            .map_err(|e| e.to_string())?;
            runner.run("docker", &["restart".into(), name])
        }
        "disk.prune_path" => {
            let path = params.get("path").and_then(|v| v.as_str()).unwrap_or("");
            prune_dir(path, &config.disk_prune.allow)
        }
        "script.run" => {
            let script = params.get("script").and_then(|v| v.as_str()).unwrap_or("");
            let cleaned = clean_script(script).map_err(|e| e.to_string())?;
            if !config
                .script_run
                .allow
                .iter()
                .any(|a| clean_script(a).ok().as_deref() == Some(cleaned.as_str()))
            {
                return Err("script is not on the script.run allow list".into());
            }
            let args = match params.get("args") {
                Some(v) => arg_list(v).map_err(|e| e.to_string())?,
                None => Vec::new(),
            };
            runner.run(&cleaned, &args)
        }
        other if proxmox::is_proxmox(other) => {
            proxmox::perform(other, params, env.proxmox, env.http, env.secrets)
        }
        other if k8s::is_k8s(other) => k8s::perform(other, params, env.k8s),
        other => Err(format!("unknown action `{other}`")),
    }
}

fn unit_name(raw: &str) -> Result<String, ActError> {
    let name = if raw.contains('.') {
        raw.to_string()
    } else if !raw.is_empty() {
        format!("{raw}.service")
    } else {
        return Err(ActError::Invalid("unit name is empty".into()));
    };
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, ':' | '_' | '.' | '@' | '-'))
        || name.starts_with('.')
        || name.contains("..")
    {
        return Err(ActError::Invalid(format!(
            "unit name `{raw}` is not allowed"
        )));
    }
    Ok(name)
}

fn docker_name(raw: &str) -> Result<String, ActError> {
    let ok = !raw.is_empty()
        && raw
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
        && raw
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphanumeric());
    if ok {
        Ok(raw.to_string())
    } else {
        Err(ActError::Invalid(format!(
            "container name `{raw}` is not allowed"
        )))
    }
}

fn clean_script(path: &str) -> Result<String, ActError> {
    if !path.starts_with('/') || path.contains('\0') || path.chars().any(|c| c.is_control()) {
        return Err(ActError::Invalid("script must be an absolute path".into()));
    }
    let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
    if parts.contains(&"..") {
        return Err(ActError::Invalid("script path must not contain ..".into()));
    }
    Ok(format!("/{}", parts.join("/")))
}

fn check_args(v: &Value) -> Result<(), ActError> {
    arg_list(v).map(|_| ())
}

fn arg_list(v: &Value) -> Result<Vec<String>, ActError> {
    let Some(arr) = v.as_array() else {
        return Err(ActError::Invalid("args must be an array of strings".into()));
    };
    if arr.len() > 16 {
        return Err(ActError::Invalid("at most 16 arguments".into()));
    }
    arr.iter()
        .map(|item| {
            let s = item
                .as_str()
                .ok_or_else(|| ActError::Invalid("args must be strings".into()))?;
            if s.chars().count() > 200 || s.contains('\0') {
                return Err(ActError::Invalid("argument is too long".into()));
            }
            Ok(s.to_string())
        })
        .collect()
}

const FORBIDDEN: &[&str] = &[
    "/", "/bin", "/boot", "/dev", "/etc", "/home", "/lib", "/lib64", "/opt", "/proc", "/root",
    "/sbin", "/srv", "/sys", "/usr", "/var",
];

fn is_forbidden(path: &Path) -> bool {
    FORBIDDEN.iter().any(|p| Path::new(p) == path)
}

fn resolve_allowed(path: &str, allow: &[String]) -> Result<PathBuf, ActError> {
    if path.contains('\0') || path.split('/').any(|p| p == "..") {
        return Err(ActError::Invalid("path must not contain ..".into()));
    }
    let raw = Path::new(path);
    if !raw.is_absolute() {
        return Err(ActError::Invalid("path must be absolute".into()));
    }
    if allow.is_empty() {
        return Err(ActError::Invalid(
            "disk.prune_path allow list is empty".into(),
        ));
    }
    let canon = raw
        .canonicalize()
        .map_err(|e| ActError::Invalid(format!("path: {e}")))?;
    if is_forbidden(&canon) || !canon.is_dir() {
        return Err(ActError::Invalid(format!(
            "{} is not a prunable directory",
            canon.display()
        )));
    }
    let allowed = allow.iter().any(|entry| {
        let candidate = Path::new(entry);
        if !candidate.is_absolute() || is_forbidden(candidate) {
            return false;
        }
        let Ok(allow_canon) = candidate.canonicalize() else {
            return false;
        };
        if is_forbidden(&allow_canon) {
            return false;
        }
        canon == allow_canon || canon.starts_with(&allow_canon)
    });
    if !allowed {
        return Err(ActError::Invalid(
            "path is not under the disk.prune allow list".into(),
        ));
    }
    Ok(canon)
}

fn prune_dir(path: &str, allow: &[String]) -> Result<String, String> {
    let canon = resolve_allowed(path, allow).map_err(|e| e.to_string())?;
    let mut removed = 0u32;
    for entry in std::fs::read_dir(&canon).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        let ft = entry.file_type().map_err(|e| e.to_string())?;
        if ft.is_symlink() || !ft.is_file() {
            continue;
        }
        std::fs::remove_file(entry.path()).map_err(|e| e.to_string())?;
        removed += 1;
        if removed >= 1000 {
            break;
        }
    }
    Ok(format!("removed {removed} files"))
}

fn clean_resource_id(id: &str) -> Result<String, ActError> {
    if id.is_empty() || id.chars().count() > 256 || id.chars().any(|c| c.is_control() || c == ' ') {
        return Err(ActError::Invalid("resource_id is empty or too long".into()));
    }
    Ok(id.to_string())
}

fn clean_finding_id(id: &str) -> Result<Option<String>, ActError> {
    let id = id.trim();
    if id.is_empty() {
        return Ok(None);
    }
    let ok = (1..=128).contains(&id.len())
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ':' | '-'));
    if ok {
        Ok(Some(id.to_string()))
    } else {
        Err(ActError::Invalid(
            "finding_id must be 1-128 characters of [A-Za-z0-9_.:-]".into(),
        ))
    }
}

fn clean_key(key: &str) -> Result<String, ActError> {
    let ok = (1..=128).contains(&key.len())
        && key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ':' | '-'));
    if ok {
        Ok(key.to_string())
    } else {
        Err(ActError::Invalid(
            "idempotency_key must be 1-128 characters of [A-Za-z0-9_.:-]".into(),
        ))
    }
}

fn default_key(action: &str, params: &Value) -> String {
    let body = serde_json::to_string(params).unwrap_or_else(|_| "{}".into());
    let mut h = 0xcbf29ce484222325u64;
    for b in format!("{action}\n{body}").bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x100000001b3);
    }
    format!("k{h:016x}")
}

fn clip(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max).collect();
    out.push('…');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct FakeRunner {
        calls: Mutex<Vec<(String, Vec<String>)>>,
        fail: bool,
    }

    impl FakeRunner {
        fn ok() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                fail: false,
            }
        }
    }

    impl CommandRunner for FakeRunner {
        fn run(&self, program: &str, args: &[String]) -> Result<String, String> {
            self.calls
                .lock()
                .unwrap()
                .push((program.to_string(), args.to_vec()));
            if self.fail {
                Err("boom".into())
            } else {
                Ok(format!("ran {program}"))
            }
        }
    }

    fn auto_script(runner: Arc<dyn CommandRunner>) -> Registry {
        let config = ActionsConfig {
            approval_ttl: "1h".into(),
            script_run: PathAction {
                tier: Some(RiskTier::Auto),
                allow: vec!["/usr/local/lib/pulseblade/hook".into()],
                ..PathAction::default()
            },
            systemd_restart: TierLimit {
                tier: Some(RiskTier::Approve),
                ..TierLimit::default()
            },
            ..ActionsConfig::default()
        };
        Registry::new(config, runner).unwrap()
    }

    fn plan_script(reg: &Registry, store: &Store, dry_run: bool) -> PlanResult {
        reg.plan(
            store,
            PlanParams {
                action: "script.run".into(),
                params: json!({"script": "/usr/local/lib/pulseblade/hook", "args": ["--once"]}),
                resource_id: Some("host:h".into()),
                idempotency_key: None,
                dry_run,
                finding_id: None,
            },
        )
        .unwrap()
    }

    #[test]
    fn auto_runs_and_approve_waits_for_a_human() {
        let runner = Arc::new(FakeRunner::ok());
        let reg = auto_script(runner.clone());
        let store = Store::open_in_memory().unwrap();

        let planned = plan_script(&reg, &store, false);
        assert_eq!(planned.tier, RiskTier::Auto);
        assert_eq!(planned.status, ActionRunStatus::Planned);
        let done = reg.execute(&store, &planned.plan_id).unwrap();
        assert_eq!(done.status, ActionRunStatus::Succeeded);
        assert_eq!(
            done.output.as_deref(),
            Some("ran /usr/local/lib/pulseblade/hook")
        );
        assert_eq!(runner.calls.lock().unwrap().len(), 1);
        let again = reg.execute(&store, &planned.plan_id).unwrap();
        assert_eq!(again.status, ActionRunStatus::Succeeded);
        assert_eq!(
            runner.calls.lock().unwrap().len(),
            1,
            "a finished run does not re-run"
        );

        let waiting = reg
            .plan(
                &store,
                PlanParams {
                    action: "systemd.restart".into(),
                    params: json!({"unit": "a.service"}),
                    resource_id: Some("unit:h:a.service".into()),
                    idempotency_key: Some("restart-a".into()),
                    dry_run: false,
                    finding_id: None,
                },
            )
            .unwrap();
        assert_eq!(waiting.tier, RiskTier::Approve);
        let held = reg.execute(&store, &waiting.plan_id).unwrap();
        assert_eq!(held.status, ActionRunStatus::AwaitingApproval);
        assert!(held.expires_at.is_some());
        assert_eq!(
            runner.calls.lock().unwrap().len(),
            1,
            "approve tier does not run yet"
        );
        let still = reg.status(&store, &held.id).unwrap();
        assert_eq!(still.status, ActionRunStatus::AwaitingApproval);

        let approved = reg.approve(&store, &held.id).unwrap();
        assert_eq!(approved.status, ActionRunStatus::Succeeded);
        let calls = runner.calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[1].0, "systemctl");
        assert_eq!(
            calls[1].1,
            vec!["restart".to_string(), "a.service".to_string()]
        );
        drop(calls);
        store.verify_audit().unwrap();

        let prior = store.recent_actions(Some("unit:h:a.service"), 8).unwrap();
        assert_eq!(prior.len(), 1);
        assert_eq!(prior[0].status, ActionRunStatus::Succeeded);
    }

    #[test]
    fn deny_and_dry_run_do_not_invoke() {
        let runner = Arc::new(FakeRunner::ok());
        let config = ActionsConfig {
            script_run: PathAction {
                allow: vec!["/usr/local/lib/pulseblade/hook".into()],
                ..PathAction::default()
            },
            ..ActionsConfig::default()
        };
        let reg = Registry::new(config, runner.clone()).unwrap();
        let store = Store::open_in_memory().unwrap();
        let planned = plan_script(&reg, &store, false);
        assert_eq!(planned.tier, RiskTier::Deny);
        let rejected = reg.execute(&store, &planned.plan_id).unwrap();
        assert_eq!(rejected.status, ActionRunStatus::Rejected);
        assert!(runner.calls.lock().unwrap().is_empty());

        let dry = plan_script(&reg, &store, true);
        let done = reg.execute(&store, &dry.plan_id).unwrap();
        assert_eq!(done.status, ActionRunStatus::Succeeded);
        assert!(done.dry_run);
        assert!(runner.calls.lock().unwrap().is_empty());
        store.verify_audit().unwrap();
    }

    #[test]
    fn expired_approval_does_not_run() {
        let runner = Arc::new(FakeRunner::ok());
        let config = ActionsConfig {
            approval_ttl: "0s".into(),
            systemd_restart: TierLimit {
                tier: Some(RiskTier::Approve),
                ..TierLimit::default()
            },
            ..ActionsConfig::default()
        };
        let reg = Registry::new(config, runner.clone()).unwrap();
        let store = Store::open_in_memory().unwrap();
        let planned = reg
            .plan(
                &store,
                PlanParams {
                    action: "systemd.restart".into(),
                    params: json!({"unit": "a"}),
                    resource_id: None,
                    idempotency_key: None,
                    dry_run: false,
                    finding_id: None,
                },
            )
            .unwrap();
        let held = reg.execute(&store, &planned.plan_id).unwrap();
        assert_eq!(held.status, ActionRunStatus::AwaitingApproval);
        let err = reg.approve(&store, &held.id).unwrap_err();
        assert!(err.to_string().contains("expired") || err.to_string().contains("not awaiting"));
        let status = reg.status(&store, &held.id).unwrap();
        assert_eq!(status.status, ActionRunStatus::Expired);
        assert!(runner.calls.lock().unwrap().is_empty());
        store.verify_audit().unwrap();
    }

    #[test]
    fn prune_stays_inside_the_allow_list() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("scratch");
        std::fs::create_dir(&target).unwrap();
        std::fs::write(target.join("old.log"), b"x").unwrap();
        std::fs::create_dir(target.join("keep")).unwrap();
        std::fs::write(target.join("keep").join("nested"), b"y").unwrap();

        let config = ActionsConfig {
            disk_prune: PathAction {
                tier: Some(RiskTier::Auto),
                allow: vec![root.path().display().to_string()],
                ..PathAction::default()
            },
            ..ActionsConfig::default()
        };
        let reg = Registry::new(config, Arc::new(FakeRunner::ok())).unwrap();
        let store = Store::open_in_memory().unwrap();
        let planned = reg
            .plan(
                &store,
                PlanParams {
                    action: "disk.prune_path".into(),
                    params: json!({"path": target.display().to_string()}),
                    resource_id: None,
                    idempotency_key: None,
                    dry_run: false,
                    finding_id: None,
                },
            )
            .unwrap();
        let done = reg.execute(&store, &planned.plan_id).unwrap();
        assert_eq!(done.status, ActionRunStatus::Succeeded);
        assert!(!target.join("old.log").exists());
        assert!(
            target.join("keep").join("nested").exists(),
            "prune is not recursive"
        );

        let outside = reg.plan(
            &store,
            PlanParams {
                action: "disk.prune_path".into(),
                params: json!({"path": "/etc"}),
                resource_id: None,
                idempotency_key: None,
                dry_run: false,
                finding_id: None,
            },
        );
        assert!(outside.is_err());
    }

    struct RecHttp {
        urls: Mutex<Vec<String>>,
    }

    impl HttpClient for RecHttp {
        fn post_form(
            &self,
            url: &str,
            _authorization: &str,
            _fields: &[(&str, &str)],
        ) -> Result<String, String> {
            self.urls.lock().unwrap().push(url.to_string());
            Ok("UPID:pve1:1".into())
        }
    }

    struct MapSecrets;

    impl SecretLookup for MapSecrets {
        fn get(&self, name: &str) -> Option<String> {
            (name == "PVE_TOKEN_SECRET").then(|| "secret-value".into())
        }
    }

    #[test]
    fn proxmox_start_waits_and_posts_only_after_approval() {
        let http = Arc::new(RecHttp {
            urls: Mutex::new(Vec::new()),
        });
        let mut reg = Registry::new(ActionsConfig::default(), Arc::new(FakeRunner::ok())).unwrap();
        reg.set_proxmox(ProxmoxEndpoint {
            api_url: "https://pve.example:8006".into(),
            token_id: "monitor@pve!pulseblade".into(),
            token_secret_env: "PVE_TOKEN_SECRET".into(),
            verify_tls: true,
        });
        let reg = reg
            .with_http(http.clone())
            .with_secrets(Arc::new(MapSecrets));
        let store = Store::open_in_memory().unwrap();
        let planned = reg
            .plan(
                &store,
                PlanParams {
                    action: "proxmox.start".into(),
                    params: json!({"node": "pve1", "vmid": 100}),
                    resource_id: None,
                    idempotency_key: None,
                    dry_run: false,
                    finding_id: None,
                },
            )
            .unwrap();
        assert_eq!(planned.tier, RiskTier::Approve);
        let held = reg.execute(&store, &planned.plan_id).unwrap();
        assert_eq!(held.status, ActionRunStatus::AwaitingApproval);
        assert!(http.urls.lock().unwrap().is_empty());
        let done = reg.approve(&store, &held.id).unwrap();
        assert_eq!(done.status, ActionRunStatus::Succeeded);
        let urls = http.urls.lock().unwrap();
        assert_eq!(
            urls.as_slice(),
            ["https://pve.example:8006/api2/json/nodes/pve1/qemu/100/status/start"]
        );
    }

    #[test]
    fn proxmox_without_api_config_does_not_plan() {
        let reg = Registry::new(ActionsConfig::default(), Arc::new(FakeRunner::ok())).unwrap();
        let store = Store::open_in_memory().unwrap();
        let err = reg
            .plan(
                &store,
                PlanParams {
                    action: "proxmox.stop".into(),
                    params: json!({"node": "pve1", "vmid": 100, "guest": "lxc"}),
                    resource_id: None,
                    idempotency_key: None,
                    dry_run: false,
                    finding_id: None,
                },
            )
            .unwrap_err();
        assert!(err.to_string().contains("not configured"));
    }

    struct RecK8s {
        calls: Mutex<Vec<String>>,
    }

    impl K8sApi for RecK8s {
        fn rollout_restart(&self, kind: &str, namespace: &str, name: &str) -> Result<(), String> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("restart {kind}/{namespace}/{name}"));
            Ok(())
        }

        fn scale(
            &self,
            kind: &str,
            namespace: &str,
            name: &str,
            replicas: u32,
        ) -> Result<(), String> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("scale {kind}/{namespace}/{name} {replicas}"));
            Ok(())
        }
    }

    fn k8s_endpoint() -> K8sEndpoint {
        K8sEndpoint {
            api_url: "https://k8s.example".into(),
            token_env: "K8S_TOKEN".into(),
            cluster: "demo".into(),
            verify_tls: true,
            ca: String::new(),
        }
    }

    #[test]
    fn k8s_rollout_waits_and_patches_only_after_approval() {
        let api = Arc::new(RecK8s {
            calls: Mutex::new(Vec::new()),
        });
        let reg = Registry::new(ActionsConfig::default(), Arc::new(FakeRunner::ok()))
            .unwrap()
            .with_k8s(k8s_endpoint(), api.clone());
        let store = Store::open_in_memory().unwrap();
        let planned = reg
            .plan(
                &store,
                PlanParams {
                    action: "k8s.rollout_restart".into(),
                    params: json!({"namespace": "demo", "name": "api", "kind": "deployment"}),
                    resource_id: None,
                    idempotency_key: None,
                    dry_run: false,
                    finding_id: None,
                },
            )
            .unwrap();
        assert_eq!(planned.tier, RiskTier::Approve);
        let held = reg.execute(&store, &planned.plan_id).unwrap();
        assert_eq!(held.status, ActionRunStatus::AwaitingApproval);
        assert!(api.calls.lock().unwrap().is_empty());
        let done = reg.approve(&store, &held.id).unwrap();
        assert_eq!(done.status, ActionRunStatus::Succeeded);
        assert_eq!(
            api.calls.lock().unwrap().as_slice(),
            ["restart deployment/demo/api"]
        );
        assert_eq!(done.resource_id, "workload:demo:demo:deployment:api");
    }

    #[test]
    fn k8s_without_api_config_does_not_plan() {
        let reg = Registry::new(ActionsConfig::default(), Arc::new(FakeRunner::ok())).unwrap();
        let store = Store::open_in_memory().unwrap();
        let err = reg
            .plan(
                &store,
                PlanParams {
                    action: "k8s.scale".into(),
                    params: json!({"namespace": "demo", "name": "api", "replicas": 2}),
                    resource_id: None,
                    idempotency_key: None,
                    dry_run: false,
                    finding_id: None,
                },
            )
            .unwrap_err();
        assert!(err.to_string().contains("not configured"));
    }

    #[cfg(feature = "k8s")]
    #[test]
    fn k8s_scale_patches_the_fixture_api_after_approval() {
        let hits = Arc::new(Mutex::new(Vec::new()));
        let url = k8s::serve_patch(hits.clone());
        std::env::set_var("PULSEBLADE_TEST_K8S_ACT", "fixture-token");
        let mut reg = Registry::new(ActionsConfig::default(), Arc::new(FakeRunner::ok())).unwrap();
        reg.set_k8s(K8sEndpoint {
            api_url: url,
            token_env: "PULSEBLADE_TEST_K8S_ACT".into(),
            cluster: "demo".into(),
            verify_tls: true,
            ca: String::new(),
        })
        .unwrap();
        let store = Store::open_in_memory().unwrap();
        let planned = reg
            .plan(
                &store,
                PlanParams {
                    action: "k8s.scale".into(),
                    params: json!({"namespace": "demo", "name": "api", "replicas": 3}),
                    resource_id: None,
                    idempotency_key: None,
                    dry_run: false,
                    finding_id: None,
                },
            )
            .unwrap();
        let held = reg.execute(&store, &planned.plan_id).unwrap();
        assert_eq!(held.status, ActionRunStatus::AwaitingApproval);
        assert!(hits.lock().unwrap().is_empty());
        let done = reg.approve(&store, &held.id).unwrap();
        assert_eq!(done.status, ActionRunStatus::Succeeded);
        assert_eq!(
            hits.lock().unwrap().as_slice(),
            ["/apis/apps/v1/namespaces/demo/deployments/api"]
        );
    }

    #[test]
    fn plan_keeps_the_finding_and_status_reports_it() {
        let reg = auto_script(Arc::new(FakeRunner::ok()));
        let store = Store::open_in_memory().unwrap();
        let planned = reg
            .plan(
                &store,
                PlanParams {
                    action: "script.run".into(),
                    params: json!({"script": "/usr/local/lib/pulseblade/hook"}),
                    resource_id: Some("host:h".into()),
                    idempotency_key: Some("tied-f1".into()),
                    dry_run: true,
                    finding_id: Some("f1".into()),
                },
            )
            .unwrap();
        assert_eq!(planned.finding_id.as_deref(), Some("f1"));
        let done = reg.execute(&store, &planned.plan_id).unwrap();
        assert_eq!(done.status, ActionRunStatus::Succeeded);
        assert_eq!(
            done.finding_outcome,
            Some(pulseblade_core::FindingOutcome::StillOpen)
        );
        let again = reg.status(&store, &done.id).unwrap();
        assert_eq!(again.finding_id.as_deref(), Some("f1"));
        assert_eq!(
            again.finding_outcome,
            Some(pulseblade_core::FindingOutcome::StillOpen)
        );

        let waiting = reg
            .plan(
                &store,
                PlanParams {
                    action: "systemd.restart".into(),
                    params: json!({"unit": "a.service"}),
                    resource_id: Some("unit:h:a.service".into()),
                    idempotency_key: Some("tied-wait".into()),
                    dry_run: false,
                    finding_id: Some("f1".into()),
                },
            )
            .unwrap();
        let held = reg.execute(&store, &waiting.plan_id).unwrap();
        assert_eq!(held.status, ActionRunStatus::AwaitingApproval);
        assert_eq!(
            held.finding_outcome,
            Some(pulseblade_core::FindingOutcome::Waiting)
        );

        let bad = reg.plan(
            &store,
            PlanParams {
                action: "script.run".into(),
                params: json!({"script": "/usr/local/lib/pulseblade/hook"}),
                resource_id: Some("host:h".into()),
                idempotency_key: Some("bad-finding".into()),
                dry_run: true,
                finding_id: Some("not a finding".into()),
            },
        );
        assert!(bad.is_err());
    }

    #[test]
    fn same_key_returns_the_open_run() {
        let reg = auto_script(Arc::new(FakeRunner::ok()));
        let store = Store::open_in_memory().unwrap();
        let first = plan_script(&reg, &store, false);
        let second = plan_script(&reg, &store, false);
        assert_eq!(first.plan_id, second.plan_id);
    }

    const WORKLOAD: &str = "workload:demo:pb-fault:deployment:pb-fault-loop";

    fn put_workload(store: &Store, health: pulseblade_core::Health, ready: &str, kind: &str) {
        use pulseblade_core::{Observation, Resource, ResourceKind};
        store
            .apply(
                "k8s",
                Utc::now(),
                &Observation {
                    resources: vec![Resource::new(
                        WORKLOAD,
                        ResourceKind::Workload,
                        "pb-fault-loop",
                    )
                    .health(health)
                    .label("namespace", "pb-fault")
                    .attr("kind", kind)
                    .attr("ready", ready)],
                    samples: vec![],
                    logs: vec![],
                },
            )
            .unwrap();
    }

    fn open_workload_finding(store: &Store) -> String {
        use pulseblade_core::Severity;
        use pulseblade_store::FindingDraft;
        let opened = store
            .reconcile_findings(
                &[FindingDraft {
                    dedupe_key: format!("health:{WORKLOAD}"),
                    resource_id: WORKLOAD.into(),
                    rule: "health".into(),
                    severity: Severity::Critical,
                    summary: "pb-fault-loop is failed".into(),
                    context: vec![],
                    logs: vec![],
                }],
                Utc::now(),
            )
            .unwrap();
        opened[0].id.clone()
    }

    fn k8s_reg(api: Arc<RecK8s>) -> Registry {
        Registry::new(ActionsConfig::default(), Arc::new(FakeRunner::ok()))
            .unwrap()
            .with_k8s(k8s_endpoint(), api)
    }

    fn waiting_rollout(reg: &Registry, store: &Store, finding_id: &str, key: &str) -> ActionRun {
        let planned = reg
            .plan(
                store,
                PlanParams {
                    action: "k8s.rollout_restart".into(),
                    params: json!({
                        "namespace": "pb-fault",
                        "name": "pb-fault-loop",
                        "kind": "deployment"
                    }),
                    resource_id: Some(WORKLOAD.into()),
                    idempotency_key: Some(key.into()),
                    dry_run: false,
                    finding_id: Some(finding_id.into()),
                },
            )
            .unwrap();
        let held = reg.execute(store, &planned.plan_id).unwrap();
        assert_eq!(held.status, ActionRunStatus::AwaitingApproval);
        held
    }

    #[test]
    fn still_failed_workload_is_not_expired_at_one_hour() {
        let api = Arc::new(RecK8s {
            calls: Mutex::new(Vec::new()),
        });
        let reg = k8s_reg(api.clone());
        let store = Store::open_in_memory().unwrap();
        put_workload(&store, pulseblade_core::Health::Failed, "0/1", "deployment");
        let finding = open_workload_finding(&store);
        let held = waiting_rollout(&reg, &store, &finding, "roll-hour");
        store
            .transition_action(
                &held.id,
                pulseblade_store::ActionTransition {
                    from: ActionRunStatus::AwaitingApproval,
                    to: ActionRunStatus::AwaitingApproval,
                    output: None,
                    expires_at: Some(held.created_at + chrono::Duration::hours(1)),
                    event: "execute",
                },
            )
            .unwrap();
        let hour = held.created_at + chrono::Duration::hours(1) + chrono::Duration::seconds(1);
        assert_eq!(
            store
                .expire_actions(hour, chrono::Duration::days(1))
                .unwrap(),
            0
        );
        let still = store.action(&held.id).unwrap().unwrap();
        assert_eq!(still.status, ActionRunStatus::AwaitingApproval);
        assert!(api.calls.lock().unwrap().is_empty());
    }

    #[test]
    fn waiting_run_expires_when_the_finding_resolves() {
        let api = Arc::new(RecK8s {
            calls: Mutex::new(Vec::new()),
        });
        let reg = k8s_reg(api.clone());
        let store = Store::open_in_memory().unwrap();
        put_workload(&store, pulseblade_core::Health::Failed, "0/1", "deployment");
        let finding = open_workload_finding(&store);
        let held = waiting_rollout(&reg, &store, &finding, "roll-clear");
        store.reconcile_findings(&[], Utc::now()).unwrap();
        store
            .expire_actions(Utc::now(), chrono::Duration::days(1))
            .unwrap();
        let gone = store.action(&held.id).unwrap().unwrap();
        assert_eq!(gone.status, ActionRunStatus::Expired);
        assert_eq!(gone.output.as_deref(), Some("finding resolved"));
        assert!(api.calls.lock().unwrap().is_empty());
    }

    #[test]
    fn waiting_run_expires_when_health_ready_or_params_change() {
        let cases = [
            (
                pulseblade_core::Health::Degraded,
                "0/1",
                "deployment",
                "health",
            ),
            (
                pulseblade_core::Health::Failed,
                "1/1",
                "deployment",
                "ready",
            ),
            (
                pulseblade_core::Health::Failed,
                "0/1",
                "statefulset",
                "params",
            ),
        ];
        for (i, (health, ready, kind, label)) in cases.into_iter().enumerate() {
            let api = Arc::new(RecK8s {
                calls: Mutex::new(Vec::new()),
            });
            let reg = k8s_reg(api.clone());
            let store = Store::open_in_memory().unwrap();
            put_workload(&store, pulseblade_core::Health::Failed, "0/1", "deployment");
            let finding = open_workload_finding(&store);
            let held = waiting_rollout(&reg, &store, &finding, &format!("roll-{i}"));
            put_workload(&store, health, ready, kind);
            store
                .expire_actions(Utc::now(), chrono::Duration::days(1))
                .unwrap();
            let gone = store.action(&held.id).unwrap().unwrap();
            assert_eq!(gone.status, ActionRunStatus::Expired, "{label}");
            assert_eq!(gone.output.as_deref(), Some("resource changed"), "{label}");
            assert!(api.calls.lock().unwrap().is_empty(), "{label}");
        }
    }

    #[test]
    fn one_day_backstop_expires_an_abandoned_run() {
        let api = Arc::new(RecK8s {
            calls: Mutex::new(Vec::new()),
        });
        let reg = k8s_reg(api.clone());
        let store = Store::open_in_memory().unwrap();
        put_workload(&store, pulseblade_core::Health::Failed, "0/1", "deployment");
        let finding = open_workload_finding(&store);
        let held = waiting_rollout(&reg, &store, &finding, "roll-day");
        let later = held.created_at + chrono::Duration::days(1) + chrono::Duration::seconds(2);
        store
            .expire_actions(later, chrono::Duration::days(1))
            .unwrap();
        let gone = store.action(&held.id).unwrap().unwrap();
        assert_eq!(gone.status, ActionRunStatus::Expired);
        assert_eq!(gone.output.as_deref(), Some("approval window elapsed"));
        assert!(api.calls.lock().unwrap().is_empty());
    }

    #[test]
    fn approve_refuses_a_mismatched_plan_and_does_not_run() {
        let api = Arc::new(RecK8s {
            calls: Mutex::new(Vec::new()),
        });
        let reg = k8s_reg(api.clone());
        let store = Store::open_in_memory().unwrap();
        put_workload(&store, pulseblade_core::Health::Failed, "0/1", "deployment");
        let finding = open_workload_finding(&store);
        let held = waiting_rollout(&reg, &store, &finding, "roll-approve");
        put_workload(
            &store,
            pulseblade_core::Health::Degraded,
            "1/1",
            "deployment",
        );
        let err = reg.approve(&store, &held.id).unwrap_err();
        assert!(err.to_string().contains("not awaiting") || err.to_string().contains("resource"));
        let gone = store.action(&held.id).unwrap().unwrap();
        assert_eq!(gone.status, ActionRunStatus::Expired);
        assert_eq!(gone.output.as_deref(), Some("resource changed"));
        assert!(api.calls.lock().unwrap().is_empty());
    }

    #[test]
    fn deny_without_a_reason_is_rejected() {
        let api = Arc::new(RecK8s {
            calls: Mutex::new(Vec::new()),
        });
        let reg = k8s_reg(api);
        let store = Store::open_in_memory().unwrap();
        put_workload(&store, pulseblade_core::Health::Failed, "0/1", "deployment");
        let finding = open_workload_finding(&store);
        let held = waiting_rollout(&reg, &store, &finding, "roll-empty");
        let err = reg.deny(&store, &held.id, "  ").unwrap_err();
        assert!(err.to_string().contains("reason"));
        assert_eq!(
            store.action(&held.id).unwrap().unwrap().status,
            ActionRunStatus::AwaitingApproval
        );
    }

    #[test]
    fn deny_with_a_reason_blocks_that_action_and_is_not_a_remedy() {
        let api = Arc::new(RecK8s {
            calls: Mutex::new(Vec::new()),
        });
        let reg = k8s_reg(api.clone());
        let store = Store::open_in_memory().unwrap();
        put_workload(&store, pulseblade_core::Health::Failed, "0/1", "deployment");
        let finding = open_workload_finding(&store);
        let held = waiting_rollout(&reg, &store, &finding, "roll-deny");
        let denied = reg.deny(&store, &held.id, "port is closed").unwrap();
        assert_eq!(denied.status, ActionRunStatus::Rejected);
        assert_eq!(denied.output.as_deref(), Some("port is closed"));
        assert_eq!(
            store
                .rejection(WORKLOAD, "k8s.rollout_restart")
                .unwrap()
                .as_deref(),
            Some("port is closed")
        );
        store.reconcile_findings(&[], Utc::now()).unwrap();
        let later = store.action(&held.id).unwrap().unwrap();
        assert_eq!(
            later.finding_outcome,
            Some(pulseblade_core::FindingOutcome::Resolved)
        );
        assert!(store
            .remedy_page("health", "workload", Some(&finding), Some(WORKLOAD))
            .unwrap()
            .matched
            .is_none());
        let again = reg.plan(
            &store,
            PlanParams {
                action: "k8s.rollout_restart".into(),
                params: json!({
                    "namespace": "pb-fault",
                    "name": "pb-fault-loop",
                    "kind": "deployment"
                }),
                resource_id: Some(WORKLOAD.into()),
                idempotency_key: Some("roll-again".into()),
                dry_run: false,
                finding_id: Some(finding),
            },
        );
        assert!(again.unwrap_err().to_string().contains("denied"));
        let other = reg
            .plan(
                &store,
                PlanParams {
                    action: "k8s.rollout_restart".into(),
                    params: json!({"namespace": "other", "name": "api", "kind": "deployment"}),
                    resource_id: Some("workload:demo:other:deployment:api".into()),
                    idempotency_key: Some("roll-other".into()),
                    dry_run: false,
                    finding_id: None,
                },
            )
            .unwrap();
        assert_eq!(other.status, ActionRunStatus::Planned);
        assert!(api.calls.lock().unwrap().is_empty());
    }

    #[test]
    fn dismiss_closes_the_run_without_a_remedy_or_rejection() {
        let api = Arc::new(RecK8s {
            calls: Mutex::new(Vec::new()),
        });
        let reg = k8s_reg(api.clone());
        let store = Store::open_in_memory().unwrap();
        put_workload(&store, pulseblade_core::Health::Failed, "0/1", "deployment");
        let finding = open_workload_finding(&store);
        let held = waiting_rollout(&reg, &store, &finding, "roll-dismiss");
        let closed = reg.dismiss(&store, &held.id).unwrap();
        assert_eq!(closed.status, ActionRunStatus::Expired);
        assert_eq!(closed.output.as_deref(), Some("dismissed"));
        let still = store.finding(&finding).unwrap().unwrap();
        assert_eq!(still.status, pulseblade_core::FindingStatus::Open);
        assert!(store
            .rejection(WORKLOAD, "k8s.rollout_restart")
            .unwrap()
            .is_none());
        assert!(store
            .remedy_page("health", "workload", Some(&finding), Some(WORKLOAD))
            .unwrap()
            .matched
            .is_none());
        assert!(api.calls.lock().unwrap().is_empty());
    }

    #[test]
    fn recheck_on_an_unchanged_failure_refreshes_the_waiting_run() {
        let api = Arc::new(RecK8s {
            calls: Mutex::new(Vec::new()),
        });
        let reg = k8s_reg(api.clone());
        let store = Store::open_in_memory().unwrap();
        put_workload(&store, pulseblade_core::Health::Failed, "0/1", "deployment");
        let finding = open_workload_finding(&store);
        let held = waiting_rollout(&reg, &store, &finding, "roll-recheck");
        let later = held.created_at + chrono::Duration::days(1) + chrono::Duration::seconds(2);
        store
            .expire_actions(later, chrono::Duration::days(1))
            .unwrap();
        assert_eq!(
            store.action(&held.id).unwrap().unwrap().status,
            ActionRunStatus::Expired
        );
        let again = reg.recheck(&store, &held.id).unwrap();
        assert_eq!(again.status, ActionRunStatus::AwaitingApproval);
        let fresh = again.expires_at.expect("fresh backstop");
        assert!(fresh > Utc::now() + chrono::Duration::hours(12));
        assert!(api.calls.lock().unwrap().is_empty());
    }

    #[test]
    fn recheck_on_a_changed_workload_files_a_known_remedy_without_a_model() {
        let api = Arc::new(RecK8s {
            calls: Mutex::new(Vec::new()),
        });
        let reg = k8s_reg(api.clone());
        let store = Store::open_in_memory().unwrap();
        put_workload(&store, pulseblade_core::Health::Failed, "0/1", "deployment");
        let first = open_workload_finding(&store);
        let learned = store
            .insert_action(
                &pulseblade_store::NewAction {
                    action: "k8s.rollout_restart".into(),
                    resource_id: WORKLOAD.into(),
                    params: json!({
                        "namespace": "pb-fault",
                        "name": "pb-fault-loop",
                        "kind": "deployment"
                    }),
                    idempotency_key: "learned".into(),
                    tier: RiskTier::Approve,
                    dry_run: false,
                    finding_id: Some(first.clone()),
                },
                Utc::now(),
            )
            .unwrap();
        store
            .transition_action(
                &learned.id,
                pulseblade_store::ActionTransition {
                    from: ActionRunStatus::Planned,
                    to: ActionRunStatus::Succeeded,
                    output: Some("restarted"),
                    expires_at: None,
                    event: "finish",
                },
            )
            .unwrap();
        store.reconcile_findings(&[], Utc::now()).unwrap();
        let _ = store.action(&learned.id).unwrap().unwrap();
        assert!(store
            .remedy_page("health", "workload", None, None)
            .unwrap()
            .matched
            .is_some());

        put_workload(&store, pulseblade_core::Health::Failed, "0/1", "deployment");
        let finding = open_workload_finding(&store);
        let held = waiting_rollout(&reg, &store, &finding, "roll-changed");
        put_workload(
            &store,
            pulseblade_core::Health::Degraded,
            "1/1",
            "deployment",
        );
        let dropped = reg.recheck(&store, &held.id).unwrap();
        assert_eq!(dropped.status, ActionRunStatus::Expired);
        assert_eq!(dropped.output.as_deref(), Some("resource changed"));
        let params = known_remedy_params(&store, &dropped).unwrap().unwrap();
        let planned = reg.plan(&store, params).unwrap();
        assert_eq!(planned.status, ActionRunStatus::Planned);
        assert_eq!(planned.finding_id.as_deref(), Some(finding.as_str()));
        assert!(api.calls.lock().unwrap().is_empty());
    }
}
