//! Pilot settings. Product names (Ollama, vLLM, OpenRouter) are not config keys;
//! every endpoint is `openai_compatible`.

use std::collections::BTreeMap;

use serde::Deserialize;

fn openai_kind() -> String {
    "openai_compatible".into()
}

fn default_wake() -> Vec<String> {
    vec!["findings".into(), "changes".into()]
}

/// One OpenAI-compatible Chat Completions endpoint.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EndpointConfig {
    /// Must be `openai_compatible`.
    #[serde(default = "openai_kind")]
    pub kind: String,
    pub base_url: String,
    pub model: String,
    /// Environment variable that holds the API key. Empty means no key.
    #[serde(default)]
    pub api_key_env: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TierConfig {
    pub name: String,
    pub endpoints: Vec<EndpointConfig>,
}

/// Per-run limits, then spend caps across runs. The caps count the pilot's own
/// recorded runs, so a restart does not reset them. Zero blocks every model
/// call for that cap, except `failure_backoff_secs`, where zero means retry on
/// the next poll.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct BudgetConfig {
    pub max_tool_calls: u32,
    pub max_tokens: u32,
    pub max_wall_secs: u64,
    /// Tokens across every run in a rolling 24 hours.
    pub max_tokens_per_day: u32,
    /// Model runs in a rolling hour.
    pub max_runs_per_hour: u32,
    /// Model runs that include one resource in a rolling 24 hours.
    pub max_runs_per_resource_per_day: u32,
    /// Wait after a run where no endpoint answered. Doubles per failure, up to 6 hours.
    pub failure_backoff_secs: u64,
}

impl Default for BudgetConfig {
    fn default() -> Self {
        Self {
            max_tool_calls: 12,
            max_tokens: 20_000,
            max_wall_secs: 120,
            max_tokens_per_day: 200_000,
            max_runs_per_hour: 6,
            max_runs_per_resource_per_day: 2,
            failure_backoff_secs: 600,
        }
    }
}

/// Event-driven investigator. Missing keys keep the defaults, which do not call a model.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct PilotConfig {
    pub enabled: bool,
    pub local_only: bool,
    pub wake_on: Vec<String>,
    /// Accepted so existing config still parses. The poll timer does not call the model.
    pub heartbeat_secs: u64,
    /// Seconds between MCP polls. This is not an LLM call.
    pub poll_secs: u64,
    /// When true, the pilot may call `action_execute` for a plan whose tier is `auto`.
    /// Approve and deny are never executed here.
    pub execute_auto: bool,
    /// MCP streamable HTTP endpoint. Empty means the CLI uses `http://<listen>/mcp`.
    pub mcp_url: String,
    pub budget: BudgetConfig,
    pub tiers: Vec<TierConfig>,
}

impl Default for PilotConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            local_only: false,
            wake_on: default_wake(),
            heartbeat_secs: 900,
            poll_secs: 30,
            execute_auto: false,
            mcp_url: String::new(),
            budget: BudgetConfig::default(),
            tiers: Vec::new(),
        }
    }
}

impl PilotConfig {
    pub fn wakes_on(&self, kind: &str) -> bool {
        self.wake_on.iter().any(|w| w == kind)
    }
}

/// Loopback endpoints are the only ones `local_only` will call.
pub fn is_loopback(base_url: &str) -> bool {
    let rest = base_url.split("://").nth(1).unwrap_or(base_url);
    let hostport = rest.split('/').next().unwrap_or("");
    let hostport = hostport.rsplit('@').next().unwrap_or(hostport);
    let host = if let Some(h) = hostport.strip_prefix('[') {
        h.split(']').next().unwrap_or(h)
    } else {
        hostport.split(':').next().unwrap_or(hostport)
    };
    matches!(host, "localhost" | "127.0.0.1" | "::1")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_documented_example() {
        let cfg: PilotConfig = toml::from_str(
            r#"
            enabled = true
            local_only = false
            wake_on = ["findings", "changes"]
            heartbeat_secs = 900

            [budget]
            max_tool_calls = 12
            max_tokens = 20000
            max_wall_secs = 120
            max_tokens_per_day = 200000
            max_runs_per_hour = 6
            max_runs_per_resource_per_day = 2
            failure_backoff_secs = 600

            [[tiers]]
            name = "triage"
            [[tiers.endpoints]]
            kind = "openai_compatible"
            base_url = "http://127.0.0.1:11434/v1"
            model = "qwen3:8b"

            [[tiers]]
            name = "escalate"
            [[tiers.endpoints]]
            kind = "openai_compatible"
            base_url = "http://127.0.0.1:8000/v1"
            model = "example-70b"
            [[tiers.endpoints]]
            kind = "openai_compatible"
            base_url = "https://models.example/v1"
            model = "example-sonnet"
            api_key_env = "EXAMPLE_API_KEY"
            headers = { "X-Title" = "pulseblade" }
            "#,
        )
        .unwrap();
        assert!(cfg.enabled);
        assert_eq!(cfg.tiers.len(), 2);
        assert_eq!(cfg.tiers[1].endpoints[1].api_key_env, "EXAMPLE_API_KEY");
        assert_eq!(
            cfg.tiers[1].endpoints[1]
                .headers
                .get("X-Title")
                .map(String::as_str),
            Some("pulseblade")
        );
        assert!(is_loopback(&cfg.tiers[0].endpoints[0].base_url));
        assert!(!is_loopback(&cfg.tiers[1].endpoints[1].base_url));
    }

    #[test]
    fn rejects_unknown_keys() {
        assert!(toml::from_str::<PilotConfig>("ollama_url = \"http://127.0.0.1\"").is_err());
    }
}
