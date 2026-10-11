//! Optional outbound POST when new findings open.

use std::time::Duration;

use pulseblade_core::Finding;
use serde_json::json;

pub fn post(url: &str, findings: &[Finding]) -> anyhow::Result<()> {
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        anyhow::bail!("webhook url must be http or https");
    }
    let body = json!({
        "source": "pulseblade",
        "findings": findings.iter().map(|f| json!({
            "id": f.id,
            "resource_id": f.resource_id,
            "rule": f.rule,
            "severity": f.severity,
            "summary": f.summary,
        })).collect::<Vec<_>>(),
    });
    let agent = ureq::AgentBuilder::new()
        .timeout(Duration::from_secs(5))
        .build();
    agent
        .post(url)
        .set("content-type", "application/json")
        .send_json(body)
        .map_err(|e| anyhow::anyhow!(e))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use pulseblade_core::{FindingStatus, Severity};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::Duration;

    #[test]
    fn posts_a_small_body() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            let (mut sock, _) = listener.accept().unwrap();
            sock.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
            let mut raw = Vec::new();
            let mut buf = [0u8; 4096];
            loop {
                match sock.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => {
                        raw.extend_from_slice(&buf[..n]);
                        if raw.windows(10).any(|w| w == b"pulseblade") {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            let req = String::from_utf8_lossy(&raw).into_owned();
            sock.write_all(
                b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            )
            .unwrap();
            req
        });
        let finding = Finding {
            id: "f1".into(),
            resource_id: "unit:h:a.service".into(),
            rule: "health".into(),
            severity: Severity::Critical,
            status: FindingStatus::Open,
            summary: "a.service is failed".into(),
            opened_at: Utc::now(),
            context: vec![],
            logs: vec![],
        };
        post(&format!("http://{addr}"), &[finding]).unwrap();
        let req = handle.join().unwrap();
        assert!(req.contains("POST / HTTP/1.1"));
        assert!(req.contains("\"source\":\"pulseblade\""));
        assert!(req.contains("unit:h:a.service"));
        assert!(!req.contains("failed to start"));
    }

    #[test]
    fn rejects_non_http_urls() {
        assert!(post("file:///tmp/hook", &[]).is_err());
    }
}
