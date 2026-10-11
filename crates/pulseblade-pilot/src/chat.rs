//! OpenAI-compatible Chat Completions. Tests stand up a local HTTP server;
//! nothing here calls a real model.

use serde_json::Value;

use crate::config::EndpointConfig;

pub trait Chat {
    fn complete(&mut self, endpoint: &EndpointConfig, body: &Value) -> Result<Value, String>;
}

#[derive(Debug, Default)]
pub struct HttpChat;

impl Chat for HttpChat {
    fn complete(&mut self, endpoint: &EndpointConfig, body: &Value) -> Result<Value, String> {
        if endpoint.kind != "openai_compatible" {
            return Err(format!(
                "endpoint kind `{}` is not openai_compatible",
                endpoint.kind
            ));
        }
        let url = format!(
            "{}/chat/completions",
            endpoint.base_url.trim_end_matches('/')
        );
        let mut req = ureq::post(&url)
            .set("Content-Type", "application/json")
            .set("Accept", "application/json");
        if !endpoint.api_key_env.is_empty() {
            let key = std::env::var(&endpoint.api_key_env)
                .map_err(|_| format!("environment variable {} is not set", endpoint.api_key_env))?;
            if key.is_empty() {
                return Err(format!(
                    "environment variable {} is empty",
                    endpoint.api_key_env
                ));
            }
            req = req.set("Authorization", &format!("Bearer {key}"));
        }
        for (name, value) in &endpoint.headers {
            if name.eq_ignore_ascii_case("authorization") {
                continue;
            }
            req = req.set(name, value);
        }
        let resp = req
            .send_json(body)
            .map_err(|e| clip(&scrub(&e.to_string())))?;
        resp.into_json().map_err(|e| format!("chat response: {e}"))
    }
}

fn scrub(s: &str) -> String {
    s.lines()
        .map(|line| {
            if line.to_ascii_lowercase().contains("authorization") || line.contains("Bearer ") {
                "authorization: redacted"
            } else {
                line
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn clip(s: &str) -> String {
    if s.chars().count() <= 300 {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(300).collect();
        out.push('…');
        out
    }
}

/// A completion the engine understands. `arguments` is a JSON object.
#[derive(Debug, Clone)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: Value,
}

#[derive(Debug, Clone)]
pub struct Completion {
    pub content: String,
    pub calls: Vec<ToolCall>,
    pub tokens: u32,
}

pub fn parse_completion(v: &Value) -> Result<Completion, String> {
    if let Some(err) = v.get("error") {
        return Err(clip(&err.to_string()));
    }
    let message = v
        .pointer("/choices/0/message")
        .ok_or_else(|| "chat response has no choices".to_string())?;
    let content = match message.get("content") {
        Some(Value::String(s)) => s.clone(),
        _ => String::new(),
    };
    let mut calls = Vec::new();
    if let Some(arr) = message.get("tool_calls").and_then(|t| t.as_array()) {
        for (i, call) in arr.iter().enumerate() {
            let name = call
                .pointer("/function/name")
                .and_then(|s| s.as_str())
                .unwrap_or("")
                .to_string();
            if name.is_empty() {
                continue;
            }
            let id = call
                .get("id")
                .and_then(|s| s.as_str())
                .map(str::to_string)
                .unwrap_or_else(|| format!("call-{i}"));
            let arguments = call
                .pointer("/function/arguments")
                .cloned()
                .unwrap_or(Value::Null);
            let arguments = match arguments {
                Value::String(s) => serde_json::from_str(&s).unwrap_or(Value::String(s)),
                other => other,
            };
            calls.push(ToolCall {
                id,
                name,
                arguments,
            });
        }
    }
    let tokens = v
        .pointer("/usage/total_tokens")
        .and_then(|n| n.as_u64())
        .map(|n| u32::try_from(n).unwrap_or(u32::MAX))
        .unwrap_or_else(|| estimate_tokens(v));
    Ok(Completion {
        content,
        calls,
        tokens,
    })
}

fn estimate_tokens(v: &Value) -> u32 {
    let chars = v.to_string().chars().count();
    u32::try_from(chars / 4).unwrap_or(u32::MAX).max(1)
}
