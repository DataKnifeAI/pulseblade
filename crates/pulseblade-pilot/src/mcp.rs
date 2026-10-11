//! Streamable HTTP MCP client. The pilot talks to a node or a hub through this
//! URL only; it does not open the store.

use serde_json::{json, Value};

const PROTOCOL: &str = "2025-03-26";

pub trait Tools {
    fn call(&mut self, name: &str, args: Value) -> Result<Value, String>;
}

pub struct McpClient {
    url: String,
    session: Option<String>,
    next_id: i64,
}

impl McpClient {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            session: None,
            next_id: 1,
        }
    }

    fn ensure(&mut self) -> Result<(), String> {
        if self.session.is_some() {
            return Ok(());
        }
        let init = json!({
            "jsonrpc": "2.0",
            "id": self.alloc(),
            "method": "initialize",
            "params": {
                "protocolVersion": PROTOCOL,
                "capabilities": {},
                "clientInfo": {"name": "pulseblade-pilot", "version": env!("CARGO_PKG_VERSION")}
            }
        });
        let init_reply = post(&self.url, None, &init)?;
        if !(200..300).contains(&init_reply.status) {
            return Err(format!(
                "mcp initialize status {}: {}",
                init_reply.status,
                clip(&init_reply.body)
            ));
        }
        let session = init_reply
            .headers
            .into_iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("mcp-session-id"))
            .map(|(_, v)| v)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| "mcp initialize did not return a session".to_string())?;
        let note = json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized"
        });
        let noted = post(&self.url, Some(&session), &note)?;
        if noted.status != 202 && !(200..300).contains(&noted.status) {
            return Err(format!("mcp initialized status {}", noted.status));
        }
        self.session = Some(session);
        Ok(())
    }

    fn alloc(&mut self) -> i64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}

impl Tools for McpClient {
    fn call(&mut self, name: &str, args: Value) -> Result<Value, String> {
        self.ensure()?;
        let session = self.session.clone().unwrap_or_default();
        let req = json!({
            "jsonrpc": "2.0",
            "id": self.alloc(),
            "method": "tools/call",
            "params": {"name": name, "arguments": args}
        });
        let reply = post(&self.url, Some(&session), &req)?;
        if !(200..300).contains(&reply.status) {
            return Err(format!(
                "mcp {name} status {}: {}",
                reply.status,
                clip(&reply.body)
            ));
        }
        let v = parse_body(&reply.body)?;
        tool_result(&v)
    }
}

fn parse_body(body: &str) -> Result<Value, String> {
    let trimmed = body.trim();
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        return serde_json::from_str(trimmed).map_err(|e| format!("mcp json: {e}"));
    }
    let mut last = None;
    for line in body.lines() {
        let Some(data) = line.strip_prefix("data:") else {
            continue;
        };
        let data = data.trim();
        if data.is_empty() {
            continue;
        }
        if let Ok(v) = serde_json::from_str::<Value>(data) {
            last = Some(v);
        }
    }
    last.ok_or_else(|| format!("mcp body is not json: {}", clip(body)))
}

fn tool_result(v: &Value) -> Result<Value, String> {
    if let Some(err) = v.get("error") {
        return Err(clip(&err.to_string()));
    }
    let result = v.get("result").unwrap_or(v);
    if result
        .get("isError")
        .and_then(|b| b.as_bool())
        .unwrap_or(false)
    {
        return Err(text_of(result));
    }
    if let Some(structured) = result.get("structuredContent") {
        if !structured.is_null() {
            return Ok(structured.clone());
        }
    }
    if let Some(text) = result.pointer("/content/0/text").and_then(|t| t.as_str()) {
        if let Ok(parsed) = serde_json::from_str::<Value>(text) {
            return Ok(parsed);
        }
        return Ok(Value::String(text.to_string()));
    }
    Ok(result.clone())
}

fn text_of(result: &Value) -> String {
    result
        .pointer("/content/0/text")
        .and_then(|t| t.as_str())
        .map(clip)
        .unwrap_or_else(|| clip(&result.to_string()))
}

struct HttpReply {
    status: u16,
    headers: Vec<(String, String)>,
    body: String,
}

fn post(url: &str, session: Option<&str>, body: &Value) -> Result<HttpReply, String> {
    let mut req = ureq::post(url)
        .set("Content-Type", "application/json")
        .set("Accept", "application/json, text/event-stream")
        .set("MCP-Protocol-Version", PROTOCOL);
    if let Some(session) = session {
        req = req.set("Mcp-Session-Id", session);
    }
    match req.send_json(body) {
        Ok(resp) => Ok(read_resp(resp.status(), resp)),
        Err(ureq::Error::Status(code, resp)) => {
            let reply = read_resp(code, resp);
            if reply.status == 202 {
                Ok(reply)
            } else {
                Err(format!(
                    "mcp status {}: {}",
                    reply.status,
                    clip(&reply.body)
                ))
            }
        }
        Err(e) => Err(format!("mcp: {e}")),
    }
}

fn read_resp(status: u16, resp: ureq::Response) -> HttpReply {
    let headers = resp
        .headers_names()
        .into_iter()
        .filter_map(|name| resp.header(&name).map(|value| (name, value.to_string())))
        .collect();
    let body = resp.into_string().unwrap_or_default();
    HttpReply {
        status,
        headers,
        body,
    }
}

fn clip(s: &str) -> String {
    if s.chars().count() <= 400 {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(400).collect();
        out.push('…');
        out
    }
}
