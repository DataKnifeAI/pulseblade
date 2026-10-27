//! The pilot speaks MCP. Pointing `mcp_url` at a node or a future hub uses this
//! same client; the test stands in for the node and mocks the model HTTP API.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use pulseblade_core::{Health, Observation, Resource, ResourceKind, Severity};
use pulseblade_pilot::{HttpChat, McpClient, PilotConfig, Session, TierConfig};
use pulseblade_store::{FindingDraft, Store};
use serde_json::json;
use tokio_util::sync::CancellationToken;

#[test]
fn failed_unit_notes_and_plans_a_restart_over_mcp() {
    let store = Arc::new(Store::open_in_memory().unwrap());
    store
        .apply(
            "systemd",
            Utc::now(),
            &Observation {
                resources: vec![Resource::new(
                    "unit:web1:sshd.service",
                    ResourceKind::Service,
                    "sshd.service",
                )
                .parent("host:web1")
                .health(Health::Failed)],
                samples: vec![],
                logs: vec![],
            },
        )
        .unwrap();
    store
        .reconcile_findings(
            &[FindingDraft {
                dedupe_key: "health\0unit:web1:sshd.service".into(),
                resource_id: "unit:web1:sshd.service".into(),
                rule: "health".into(),
                severity: Severity::Critical,
                summary: "sshd.service is failed".into(),
                context: vec![],
                logs: vec![],
            }],
            Utc::now(),
        )
        .unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let listener =
        rt.block_on(async { tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap() });
    let addr = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let served = store.clone();
    rt.spawn(async move {
        let actions = Arc::new(pulseblade_act::Registry::builtin());
        let _ = pulseblade_mcp::serve_listener(
            listener,
            served,
            vec!["127.0.0.1".into()],
            stop,
            actions,
            pulseblade_mcp::PortalDefaults::default(),
        )
        .await;
    });

    let model = mock_chat(vec![
        chat_with_tools(json!([
            tool_call(
                "1",
                "resource_explain",
                json!({"id": "unit:web1:sshd.service"})
            ),
            tool_call(
                "2",
                "logs_query",
                json!({"resource": "unit:web1:sshd.service"})
            ),
            tool_call(
                "3",
                "memory_note",
                json!({
                    "resource": "unit:web1:sshd.service",
                    "text": "sshd failed; restart planned",
                    "author": "pilot"
                })
            ),
            tool_call(
                "4",
                "action_plan",
                json!({
                    "action": "systemd.restart",
                    "params": {"unit": "sshd.service"},
                    "resource_id": "unit:web1:sshd.service"
                })
            )
        ])),
        chat_text("sshd failed; restart is waiting for approval"),
    ]);

    let config = PilotConfig {
        enabled: true,
        execute_auto: false,
        tiers: vec![TierConfig {
            name: "triage".into(),
            endpoints: vec![pulseblade_pilot::EndpointConfig {
                kind: "openai_compatible".into(),
                base_url: model,
                model: "local".into(),
                api_key_env: String::new(),
                headers: Default::default(),
            }],
        }],
        ..PilotConfig::default()
    };
    let mut session = Session::new(config, Instant::now());
    let mut chat = HttpChat;
    let mut last = String::new();
    let run = (0..40).find_map(|_| {
        let mut tools = McpClient::new(format!("http://{addr}/mcp"));
        match session.tick(&mut tools, &mut chat, Instant::now()) {
            Ok(run) => Some(run),
            Err(e) => {
                last = e;
                std::thread::sleep(Duration::from_millis(50));
                None
            }
        }
    });
    let run = run.unwrap_or_else(|| panic!("pilot did not reach the node: {last}"));
    let run = run.expect("a failed unit should wake the pilot");
    assert_eq!(run.status, "ok");
    assert!(run.tool_calls <= 12);
    assert!(run.proposed.iter().any(|a| a == "systemd.restart"));
    assert!(run.conclusion.contains("restart"));

    let notes = store.notes_for("unit:web1:sshd.service", 5).unwrap();
    assert!(
        notes.iter().any(|n| n.text.contains("restart planned")),
        "{notes:?}"
    );
    let actions = store.recent_actions(None, 5).unwrap();
    assert_eq!(actions.len(), 1);
    assert_eq!(actions[0].action, "systemd.restart");
    assert_eq!(actions[0].status, pulseblade_core::ActionRunStatus::Planned);
    let id = store
        .insert_pilot_run(
            &pulseblade_store::NewPilotRun {
                trigger: run.trigger,
                tier: run.tier,
                endpoint: run.endpoint,
                tool_calls: run.tool_calls,
                tokens: run.tokens,
                conclusion: run.conclusion,
                proposed: run.proposed,
                status: run.status,
                detail: run.detail,
            },
            Utc::now(),
        )
        .unwrap();
    assert_eq!(store.recent_pilot_runs(5).unwrap()[0].id, id);
    shutdown.cancel();
}

fn tool_call(id: &str, name: &str, arguments: serde_json::Value) -> serde_json::Value {
    json!({
        "id": id,
        "type": "function",
        "function": {"name": name, "arguments": arguments.to_string()}
    })
}

fn chat_with_tools(calls: serde_json::Value) -> String {
    json!({
        "choices": [{"message": {"role": "assistant", "content": "", "tool_calls": calls}}],
        "usage": {"total_tokens": 30}
    })
    .to_string()
}

fn chat_text(text: &str) -> String {
    json!({
        "choices": [{"message": {"role": "assistant", "content": text}}],
        "usage": {"total_tokens": 12}
    })
    .to_string()
}

fn mock_chat(rounds: Vec<String>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for body in rounds {
            let (mut sock, _) = listener.accept().unwrap();
            let _ = read_request(&mut sock);
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = sock.write_all(resp.as_bytes());
        }
    });
    format!("http://{addr}/v1")
}

fn read_request(sock: &mut std::net::TcpStream) -> Vec<u8> {
    sock.set_read_timeout(Some(Duration::from_secs(5))).ok();
    let mut data = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        match sock.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                data.extend_from_slice(&buf[..n]);
                if let Some(end) = data.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&data[..end]);
                    let len = headers.lines().find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())
                            .flatten()
                    });
                    if data.len() >= end + 4 + len.unwrap_or(0) {
                        break;
                    }
                }
            }
        }
    }
    data
}
