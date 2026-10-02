//! Approximate token cost (chars / 4) of typical tool responses against a dataset
//! shaped like a real workstation. Run with `--nocapture` to see the table.

use chrono::{Duration, Utc};
use pulseblade_core::{CollectorRun, Health, Observation, Resource, ResourceKind, Sample};
use pulseblade_mcp::query::{self, *};
use pulseblade_store::Store;
use serde::Serialize;

const HOST: &str = "ws1";
const PASSES: i64 = 240; // one hour at 15s

fn host() -> Resource {
    Resource::new(format!("host:{HOST}"), ResourceKind::Host, HOST)
        .health(Health::Ok)
        .label("host", HOST)
        .attr("os", "Linux 7.2 CachyOS")
        .attr("kernel", "7.2.6-1-cachyos")
        .attr("arch", "x86_64")
        .attr("cpu_count", 32)
        .attr("memory_total_bytes", 67_108_864_000u64)
        .attr("swap_total_bytes", 17_179_869_184u64)
        .attr("boot_time", "2026-09-28T08:00:00+00:00")
}

fn disks() -> Vec<Resource> {
    [
        "/", "/home", "/boot", "/var", "/var/log", "/srv", "/tmp", "/data", "/backup", "/opt",
    ]
    .iter()
    .enumerate()
    .map(|(i, m)| {
        Resource::new(format!("disk:{HOST}:{m}"), ResourceKind::Disk, *m)
            .parent(format!("host:{HOST}"))
            .health(Health::Ok)
            .label("host", HOST)
            .attr("device", format!("/dev/nvme0n1p{}", i + 1))
            .attr("fs", "btrfs")
            .attr("kind", "SSD")
            .attr("read_only", false)
            .attr("removable", false)
            .attr("total_bytes", 1_000_204_886_016u64)
    })
    .collect()
}

fn nets() -> Vec<Resource> {
    [
        "enp5s0",
        "wlan0",
        "docker0",
        "br-1a2b3c",
        "tailscale0",
        "virbr0",
        "wg0",
    ]
    .iter()
    .enumerate()
    .map(|(i, n)| {
        Resource::new(format!("net:{HOST}:{n}"), ResourceKind::NetIface, *n)
            .parent(format!("host:{HOST}"))
            .health(Health::Ok)
            .label("host", HOST)
            .attr("mac", format!("aa:bb:cc:dd:ee:{i:02x}"))
            .attr(
                "addresses",
                vec![format!("10.0.{i}.2/24"), format!("fe80::{i}/64")],
            )
    })
    .collect()
}

fn services(pass: i64) -> Vec<Resource> {
    (0..130)
        .map(|i| {
            let name = format!("example-service-{i:03}.service");
            // A handful of oneshot units flip between active and inactive.
            let flipping = i % 13 == 0;
            let active = if flipping && (pass / 10 + i) % 2 == 0 {
                "inactive"
            } else {
                "active"
            };
            let (health, active) = if i == 42 && pass > PASSES / 2 {
                (Health::Failed, "failed")
            } else {
                (Health::Ok, active)
            };
            Resource::new(format!("unit:{HOST}:{name}"), ResourceKind::Service, &name)
                .parent(format!("host:{HOST}"))
                .health(health)
                .label("host", HOST)
                .attr("active", active)
                .attr(
                    "sub",
                    match active {
                        "active" => "running",
                        "failed" => "failed",
                        _ => "dead",
                    },
                )
                .attr(
                    "unit_file_state",
                    if i % 3 == 0 { "static" } else { "enabled" },
                )
                .attr(
                    "description",
                    format!("Example background service number {i}"),
                )
        })
        .collect()
}

fn host_samples(pass: i64) -> Vec<Sample> {
    let id = format!("host:{HOST}");
    let wobble = (pass as f64 / 7.0).sin();
    let mut s = vec![
        Sample::new(&id, "cpu.used_pct", 12.0 + 8.0 * wobble),
        Sample::new(&id, "load.1m", 2.1 + wobble),
        Sample::new(&id, "load.5m", 2.0),
        Sample::new(&id, "load.15m", 1.9),
        Sample::new(&id, "mem.used_pct", 41.3 + wobble),
        Sample::new(&id, "mem.available_bytes", 39_000_000_000.0 + 1e8 * wobble),
        Sample::new(&id, "swap.used_pct", 0.4),
    ];
    for d in disks() {
        s.push(Sample::new(&d.id, "disk.used_pct", 37.25));
        s.push(Sample::new(&d.id, "disk.available_bytes", 6.2e11));
    }
    for n in nets() {
        s.push(Sample::new(
            &n.id,
            "net.rx_bytes_per_s",
            1234.5 + 100.0 * wobble,
        ));
        s.push(Sample::new(&n.id, "net.tx_bytes_per_s", 987.6));
        s.push(Sample::new(&n.id, "net.rx_errors", 0.0));
        s.push(Sample::new(&n.id, "net.tx_errors", 0.0));
    }
    s
}

fn dataset() -> Store {
    let store = Store::open_in_memory().unwrap();
    let start = Utc::now() - Duration::seconds(PASSES * 15);
    for pass in 0..PASSES {
        let ts = start + Duration::seconds(pass * 15);
        let mut resources = vec![host()];
        resources.extend(disks());
        resources.extend(nets());
        store
            .apply(
                "host",
                ts,
                &Observation {
                    resources,
                    samples: host_samples(pass),
                },
            )
            .unwrap();
        store
            .apply(
                "systemd",
                ts,
                &Observation {
                    resources: services(pass),
                    samples: vec![],
                },
            )
            .unwrap();
        for (source, resource_count, sample_count) in [("host", 18, 53), ("systemd", 130, 0)] {
            store
                .record_run(&CollectorRun {
                    source: source.into(),
                    ts,
                    duration_ms: 40,
                    ok: true,
                    error: None,
                    resource_count,
                    sample_count,
                    change_count: 1,
                    interval_secs: Some(15),
                })
                .unwrap();
        }
    }
    store
}

fn tokens<T: Serialize>(v: &T) -> usize {
    serde_json::to_string(v).unwrap().len() / 4
}

#[test]
fn token_budget_report() {
    let store = dataset();
    let seq = store.current_seq().unwrap();
    let mut rows: Vec<(&str, usize)> = Vec::new();

    let snap = |detail: Option<Detail>| {
        query::snapshot(
            &store,
            SnapshotParams {
                detail,
                ..Default::default()
            },
        )
        .unwrap()
    };
    let summary_json = serde_json::to_string(&snap(None)).unwrap();
    println!("\nsummary snapshot: {summary_json}");
    let summary = summary_json.len() / 4;
    rows.push(("state_snapshot (default: summary)", summary));
    rows.push((
        "state_snapshot detail=brief",
        tokens(&snap(Some(Detail::Brief))),
    ));
    rows.push((
        "state_snapshot detail=full",
        tokens(&snap(Some(Detail::Full))),
    ));
    let unchanged = tokens(
        &query::snapshot(
            &store,
            SnapshotParams {
                if_changed_since: Some(seq),
                ..Default::default()
            },
        )
        .unwrap(),
    );
    rows.push(("state_snapshot if_changed_since (unchanged)", unchanged));
    let svc = query::snapshot(
        &store,
        SnapshotParams {
            kind: Some(ResourceKind::Service),
            limit: Some(500),
            ..Default::default()
        },
    )
    .unwrap();
    rows.push(("state_snapshot kind=service limit=500", tokens(&svc)));
    for id in [
        format!("host:{HOST}"),
        format!("unit:{HOST}:example-service-042.service"),
    ] {
        let e = query::explain(
            &store,
            ExplainParams {
                id,
                changes_limit: None,
            },
        )
        .unwrap();
        rows.push(("resource_explain", tokens(&e)));
    }
    let m = query::metrics(
        &store,
        MetricsParams {
            id: format!("host:{HOST}"),
            metric: "cpu.used_pct".into(),
            range: None,
            step: None,
            agg: None,
        },
    )
    .unwrap();
    let metrics_1h = tokens(&m);
    rows.push(("metrics_query 1h (default)", metrics_1h));
    let changes = |since: String, compact: bool| {
        query::changes(
            &store,
            ChangesParams {
                since,
                kind: None,
                resource_prefix: None,
                limit: None,
                compact: Some(compact),
            },
        )
        .unwrap()
    };
    let tail = format!("seq:{}", seq - 100);
    rows.push((
        "changes_since seq:0 (100 changes, compact)",
        tokens(&changes("seq:0".into(), true)),
    ));
    rows.push((
        "changes_since seq:0 (100 changes, compact=false)",
        tokens(&changes("seq:0".into(), false)),
    ));
    rows.push((
        "changes_since last 100 (compact)",
        tokens(&changes(tail.clone(), true)),
    ));
    rows.push((
        "changes_since last 100 (compact=false)",
        tokens(&changes(tail, false)),
    ));
    rows.push((
        "changes_since last 10 (compact)",
        tokens(&changes(format!("seq:{}", seq - 10), true)),
    ));
    let x = query::export(&store, ExportParams::default()).unwrap();
    rows.push(("export_bulk (resources)", x.len() / 4));

    println!("\njournal seq: {seq}");
    for (name, t) in &rows {
        println!("{name:<50} ~{t:>6} tokens");
    }

    assert!(summary < 600, "summary snapshot grew to ~{summary} tokens");
    assert!(
        unchanged < 20,
        "unchanged snapshot grew to ~{unchanged} tokens"
    );
    assert!(metrics_1h < 600, "1h series grew to ~{metrics_1h} tokens");
}
