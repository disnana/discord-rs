//! Process-level two-shard separation with the loopback Discord simulator.
#![cfg(unix)]

use serde_json::{Value, json};
use std::{
    future::Future,
    path::{Path, PathBuf},
    process::Stdio,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, tcp::OwnedReadHalf, tcp::OwnedWriteHalf},
    process::Child,
};

const WAIT: Duration = Duration::from_secs(40);
const SECRET: [u8; 32] = [83; 32];
const CONFIG: &str = r#"
[cluster]
id = "separate-cores-test"
mode = "standalone"
[node]
id = "local"
[[bots]]
name = "alpha"
bot_id = "101"
application_id = "101"
token_env = "TOKEN_ALPHA"
total_shards = 2
[[bots.shard_placement]]
shard_id = 0
primary = "local"
[[bots.shard_placement]]
shard_id = 1
primary = "local"
"#;

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(WAIT, future)
        .await
        .expect("separate cores deadline")
}

struct Gateway {
    ready: Value,
    input: OwnedWriteHalf,
    output: BufReader<OwnedReadHalf>,
}

impl Gateway {
    async fn receive(&mut self) -> Value {
        let mut bytes = Vec::new();
        bounded(
            (&mut self.output)
                .take(64 * 1024 + 1)
                .read_until(b'\n', &mut bytes),
        )
        .await
        .unwrap();
        assert!(!bytes.is_empty(), "Gateway connection closed");
        serde_json::from_slice(&bytes).expect("collector JSON")
    }

    async fn status(&mut self) -> Value {
        self.input
            .write_all(b"{\"type\":\"status\",\"id\":7}\n")
            .await
            .unwrap();
        self.receive().await
    }

    fn pid(&self) -> u32 {
        self.ready["pid"].as_u64().unwrap().try_into().unwrap()
    }
}

fn worker_pids(value: &Value) -> Vec<u32> {
    value["workers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|worker| worker["pid"].as_u64().unwrap().try_into().unwrap())
        .collect()
}

fn alive(pid: u32) -> bool {
    Path::new(&format!("/proc/{pid}")).exists()
        && !std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .map(|stat| stat.contains(") Z "))
            .unwrap_or(true)
}

fn signal(pid: u32, name: &str) {
    let status = std::process::Command::new("kill")
        .arg(format!("-{name}"))
        .arg(pid.to_string())
        .status()
        .unwrap();
    assert!(status.success());
}

struct Fixture {
    root: PathBuf,
    daemon: Child,
    collector: TcpListener,
    port: u16,
}

impl Fixture {
    async fn start() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        // Short: Unix socket paths are limited to about 100 bytes.
        let root = std::env::temp_dir().join(format!(
            "bscs{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        let config = root.join("placement.toml");
        std::fs::write(&config, CONFIG).unwrap();
        let collector = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_botd"));
        command
            .arg("--file")
            .arg(config)
            .arg("--runtime-root")
            .arg(&root)
            .arg("--gateway-program")
            .arg(env!("CARGO_BIN_EXE_src"))
            .args([
                "--bootstrap-stdin",
                "--stop-children-on-exit",
                "--core-mode",
                "separate",
                "--mock-discord",
            ])
            .arg(collector.local_addr().unwrap().to_string())
            .env("TOKEN_ALPHA", "separate-test-credential")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut daemon = command.spawn().unwrap();
        let mut bootstrap = serde_json::to_vec(&json!({"secret": SECRET})).unwrap();
        bootstrap.push(b'\n');
        bounded(daemon.stdin.take().unwrap().write_all(&bootstrap))
            .await
            .unwrap();
        let mut line = Vec::new();
        bounded(BufReader::new(daemon.stdout.take().unwrap()).read_until(b'\n', &mut line))
            .await
            .unwrap();
        if line.is_empty() {
            let mut diagnostic = Vec::new();
            if let Some(stderr) = daemon.stderr.take() {
                let _ = bounded(stderr.take(4096).read_to_end(&mut diagnostic)).await;
            }
            panic!(
                "botd exited before announcement: {}",
                String::from_utf8_lossy(&diagnostic)
            );
        }
        let announced: Value = serde_json::from_slice(&line).expect("botd announcement");
        let port = announced["port"].as_u64().unwrap().try_into().unwrap();
        Self {
            root,
            daemon,
            collector,
            port,
        }
    }

    async fn accept(&self) -> Gateway {
        let (stream, _) = bounded(self.collector.accept()).await.unwrap();
        let (output, input) = stream.into_split();
        let mut gateway = Gateway {
            ready: Value::Null,
            input,
            output: BufReader::new(output),
        };
        gateway.ready = gateway.receive().await;
        assert_eq!(gateway.ready["type"], "ready");
        assert_eq!(gateway.ready["workers"].as_array().unwrap().len(), 2);
        gateway
    }
}

fn walk(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(walk(&path));
            } else {
                out.push(path);
            }
        }
    }
    out
}

async fn control(port: u16, mut value: Value) -> Value {
    value["secret"] = json!(SECRET);
    let mut stream = bounded(tokio::net::TcpStream::connect(("127.0.0.1", port)))
        .await
        .unwrap();
    let mut bytes = serde_json::to_vec(&value).unwrap();
    bytes.push(b'\n');
    bounded(stream.write_all(&bytes)).await.unwrap();
    let mut reply = Vec::new();
    bounded(BufReader::new(stream).read_until(b'\n', &mut reply))
        .await
        .unwrap();
    serde_json::from_slice(&reply).expect("daemon response")
}

#[tokio::test]
async fn each_shard_has_its_own_cores_and_updates_independently() {
    let fixture = Fixture::start().await;
    let first = fixture.accept().await;
    let second = fixture.accept().await;
    let (shard0, mut shard1) = match (
        first.ready["shard_id"].as_u64(),
        second.ready["shard_id"].as_u64(),
    ) {
        (Some(0), Some(1)) => (first, second),
        (Some(1), Some(0)) => (second, first),
        ids => panic!("unexpected Gateway shards: {ids:?}"),
    };
    let old0 = shard0.pid();
    let old1 = shard1.pid();
    assert_ne!(old0, old1);
    let cores0 = worker_pids(&shard0.ready);
    let cores1 = worker_pids(&shard1.ready);
    let cores: Vec<u32> = cores0.iter().chain(&cores1).copied().collect();
    assert_eq!(
        cores
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>()
            .len(),
        4
    );
    assert!(cores.iter().all(|pid| alive(*pid)));
    assert!(alive(old0) && alive(old1));

    signal(old0, "KILL");
    let restarted = fixture.accept().await;
    assert_eq!(restarted.ready["shard_id"], 0);
    let restarted_pid = restarted.pid();
    assert_ne!(restarted_pid, old0);
    assert_eq!(worker_pids(&restarted.ready), cores0);
    let status1 = shard1.status().await;
    assert_eq!(status1["pid"], old1);
    assert_eq!(worker_pids(&status1), cores1);
    assert!(alive(old1) && cores.iter().all(|pid| alive(*pid)));

    let id = "0123456789abcdef0123456789abcdef";
    let admitted = control(
        fixture.port,
        json!({"command":"gateway-update","bot":"101","operation_id":id}),
    )
    .await;
    assert_eq!(admitted["ok"], true, "{admitted}");
    // Updates are sequential; accept each active replacement as it appears.
    let update_a = fixture.accept().await;
    let update_b = fixture.accept().await;
    let (updated0, updated1) = match (
        update_a.ready["shard_id"].as_u64(),
        update_b.ready["shard_id"].as_u64(),
    ) {
        (Some(0), Some(1)) => (update_a, update_b),
        (Some(1), Some(0)) => (update_b, update_a),
        ids => panic!("unexpected updated Gateway shards: {ids:?}"),
    };
    let finished = bounded(async {
        loop {
            let view = control(
                fixture.port,
                json!({"command":"operation","operation_id":id}),
            )
            .await;
            if view["operation"]["state"] != "running" {
                return view;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert_eq!(finished["operation"]["state"], "succeeded", "{finished}");
    let new0 = updated0.pid();
    let new1 = updated1.pid();
    assert_ne!(new0, restarted_pid);
    assert_ne!(new0, old0);
    assert_ne!(new1, old1);
    assert_ne!(new0, new1);
    assert_eq!(worker_pids(&updated0.ready), cores0);
    assert_eq!(worker_pids(&updated1.ready), cores1);
    assert!(alive(new0) && alive(new1) && cores.iter().all(|pid| alive(*pid)));
    bounded(async {
        while [old0, restarted_pid, old1].into_iter().any(alive) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    signal(fixture.daemon.id().unwrap(), "TERM");
    let mut daemon = fixture.daemon;
    bounded(daemon.wait()).await.unwrap();
    bounded(async {
        while [old0, restarted_pid, old1, new0, new1]
            .into_iter()
            .chain(cores.iter().copied())
            .any(alive)
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    let ipc: Vec<_> = walk(&fixture.root)
        .into_iter()
        .filter(|p| {
            p.extension().is_some_and(|e| e == "sock")
                || p.file_name().is_some_and(|n| n == "core.secret")
        })
        .collect();
    assert!(
        ipc.is_empty(),
        "sockets or core.secret left behind: {ipc:?}"
    );
    std::fs::remove_dir_all(&fixture.root).unwrap();
}
