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
use pulseblade_core::{ActionRun, ActionRunStatus, ResourceKind, RiskTier};
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
        store.expire_actions(Utc::now())?;
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
                Ok(store.transition_action(
                    &run.id,
                    pulseblade_store::ActionTransition {
                        from: ActionRunStatus::Planned,
                        to: ActionRunStatus::AwaitingApproval,
                        output: None,
                        expires_at: Some(exp),
                        event: "execute",
                    },
                )?)
            }
            RiskTier::Auto => self.invoke(store, &run),
        }
    }

    pub fn status(&self, store: &Store, run_id: &str) -> Result<ActionRun, ActError> {
        store.expire_actions(Utc::now())?;
        self.load(store, run_id)
    }

    /// Run an approve-tier action that is still inside its approval window.
    pub fn approve(&self, store: &Store, run_id: &str) -> Result<ActionRun, ActError> {
        let now = Utc::now();
        store.expire_actions(now)?;
        let run = self.load(store, run_id)?;
        if run.status != ActionRunStatus::AwaitingApproval {
            return Err(ActError::Invalid(format!(
                "run {} is {}, not awaiting approval",
                run.id,
                run.status.as_str()
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
}
