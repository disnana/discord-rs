//! `botd --core-mode separate` with real botd/Gateway/Core executables (stage 1-C of the
//! Gateway/Core separation). Discord is the explicit loopback simulator; this establishes
//! process behaviour only, not live Discord.
//!
//! What it shows: the Cores are botd's children and outlive their Gateway; a restarted
//! Gateway reconnects to the same Core processes; a killed Core is restarted by botd while
//! the Gateway stays; stopping botd stops Gateway and Cores with nothing left behind.
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
total_shards = 1
[[bots.shard_placement]]
shard_id = 0
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
        Self::start_with(None).await
    }

    /// `stub`: a script used as the Gateway program instead of the real binary.
    async fn start_with(stub: Option<&dyn Fn(&Path) -> PathBuf>) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        // Short: Unix socket paths are limited to about 100 bytes.
        let root = std::env::temp_dir().join(format!(
            "bsc{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let config = root.join("placement.toml");
        std::fs::write(&config, CONFIG).unwrap();
        let program = match stub {
            Some(make) => make(&root),
            None => PathBuf::from(env!("CARGO_BIN_EXE_src")),
        };
        let collector = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_botd"));
        command
            .arg("--file")
            .arg(config)
            .arg("--runtime-root")
            .arg(&root)
            .arg("--gateway-program")
            .arg(&program)
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

#[tokio::test]
async fn cores_outlive_their_gateway_are_restarted_alone_and_stop_with_botd() {
    let fixture = Fixture::start().await;

    // 1. Gateway up, attached to two Cores that are not its children.
    let mut gateway = fixture.accept().await;
    let gateway_pid = gateway.pid();
    let cores = worker_pids(&gateway.ready);
    assert_eq!(cores.len(), 2);
    for core in &cores {
        assert_ne!(*core, gateway_pid);
        assert!(alive(*core));
    }

    // 2. The Gateway crashes (mock fault injection). botd restarts it; the Cores are the
    //    very same processes.
    gateway
        .input
        .write_all(b"{\"type\":\"crash\",\"id\":8}\n")
        .await
        .unwrap();
    let mut second = fixture.accept().await;
    assert_ne!(second.pid(), gateway_pid, "a new Gateway process");
    assert_eq!(worker_pids(&second.ready), cores, "the Cores were kept");
    for core in &cores {
        assert!(alive(*core), "a Core died with its Gateway");
    }

    // 3. One Core is killed. botd restarts it; the Gateway process stays, reconnects, and
    //    serves again with a new Core in that slot.
    signal(cores[0], "KILL");
    let replaced = bounded(async {
        loop {
            let status = second.status().await;
            let now = worker_pids(&status);
            if now.len() == 2 && now[0] != cores[0] && now[1] == cores[1] && alive(now[0]) {
                return now;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await;
    assert_eq!(second.status().await["pid"], second.pid());

    // 4. Stopping botd stops the Gateway and both Cores; nothing is left running.
    let botd_pid = fixture.daemon.id().unwrap();
    signal(botd_pid, "TERM");
    let mut daemon = fixture.daemon;
    bounded(daemon.wait()).await.unwrap();
    bounded(async {
        while [second.pid(), replaced[0], replaced[1]]
            .into_iter()
            .any(alive)
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    let ipc: Vec<_> = walk(&fixture.root)
        .into_iter()
        .filter(|p| p.extension().is_some_and(|e| e == "sock" || e == "secret"))
        .collect();
    assert!(ipc.is_empty(), "sockets or secret left behind: {ipc:?}");
    let _ = std::fs::remove_dir_all(&fixture.root);
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

/// A Gateway update through botd: a standby Gateway starts, becomes active, and the old one
/// exits, while the Core processes stay the very same ones. A Gateway botd restarts
/// afterwards attaches with the new epoch (it could not otherwise reach the Cores).
#[tokio::test]
async fn gateway_update_replaces_the_gateway_and_keeps_the_cores() {
    let fixture = Fixture::start().await;
    let first = fixture.accept().await;
    let cores = worker_pids(&first.ready);
    let old_pid = first.pid();

    let id = "0123456789abcdef0123456789abcdef";
    let admitted = control(
        fixture.port,
        json!({"command":"gateway-update","bot":"101","operation_id":id}),
    )
    .await;
    assert_eq!(admitted["ok"], true, "{admitted}");

    // The standby connects to the collector and announces itself only once active.
    let mut second = fixture.accept().await;
    assert_ne!(second.pid(), old_pid, "a new Gateway process");
    assert_eq!(worker_pids(&second.ready), cores, "the Cores were kept");
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

    // A restarted Core must inherit the activated Gateway epoch; its sibling is unchanged.
    signal(cores[0], "KILL");
    let cores = bounded(async {
        loop {
            let now = worker_pids(&second.status().await);
            if now.len() == 2 && now[0] != cores[0] && now[1] == cores[1] && alive(now[0]) {
                return now;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await;
    let args = |pid| {
        std::fs::read(format!("/proc/{pid}/cmdline"))
            .unwrap()
            .split(|byte| *byte == 0)
            .map(|arg| arg.to_vec())
            .collect::<Vec<_>>()
    };
    assert!(
        args(cores[0])
            .windows(2)
            .any(|pair| pair[0] == b"--epoch-floor" && pair[1] == b"1")
    );
    assert!(!args(cores[1]).iter().any(|arg| arg == b"--epoch-floor"));

    // The standby logs into the shard's one `gateway.log`; no separate candidate log appears.
    fn named(dir: &std::path::Path, name: &str, out: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.is_dir() {
                named(&path, name, out);
            } else if path.file_name().is_some_and(|n| n == name) {
                out.push(path);
            }
        }
    }
    let mut candidate_logs = Vec::new();
    named(&fixture.root, "gateway-candidate.log", &mut candidate_logs);
    assert!(
        candidate_logs.is_empty(),
        "a separate candidate log was created: {candidate_logs:?}"
    );
    let mut gateway_logs = Vec::new();
    named(&fixture.root, "gateway.log", &mut gateway_logs);
    assert_eq!(gateway_logs.len(), 1, "{gateway_logs:?}");

    // The request and its outcome are on record, with who asked and nothing sensitive.
    let audit = std::fs::read_to_string(fixture.root.join("gateway-update-audit.jsonl"))
        .expect("gateway-update audit");
    let events: Vec<Value> = audit
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let words: Vec<_> = events
        .iter()
        .map(|e| e["event"].as_str().unwrap())
        .collect();
    assert_eq!(words, ["requested", "succeeded"], "{audit}");
    assert_eq!(events[0]["operation_id"], id);
    assert_eq!(events[0]["subject"], "local-bootstrap");
    assert!(!audit.contains("secret"));

    // The old Gateway is told to stop and goes away; the Cores do not.
    bounded(async {
        while alive(old_pid) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    for core in &cores {
        assert!(alive(*core), "a Core died during the update");
    }
    assert_eq!(second.status().await["pid"], second.pid());

    // botd now supervises the new Gateway: when it dies, its replacement attaches to the
    // same Cores (with the raised epoch).
    let new_pid = second.pid();
    second
        .input
        .write_all(b"{\"type\":\"crash\",\"id\":9}\n")
        .await
        .unwrap();
    let third = fixture.accept().await;
    assert_ne!(third.pid(), new_pid);
    assert_eq!(worker_pids(&third.ready), cores);

    signal(fixture.daemon.id().unwrap(), "TERM");
    let mut daemon = fixture.daemon;
    bounded(daemon.wait()).await.unwrap();
    bounded(async {
        while [third.pid(), cores[0], cores[1]].into_iter().any(alive) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    let _ = std::fs::remove_dir_all(&fixture.root);
}

#[tokio::test]
async fn a_core_crash_during_a_gateway_update_leaves_a_working_system() {
    let fixture = Fixture::start().await;
    let mut first = Some(fixture.accept().await);
    let original_gateway = first.as_ref().unwrap().pid();
    let original_cores = worker_pids(&first.as_ref().unwrap().ready);
    let botd_pid = fixture.daemon.id().unwrap();
    let id = "fedcba9876543210fedcba9876543210";
    let admitted = control(
        fixture.port,
        json!({"command":"gateway-update","bot":"101","operation_id":id}),
    )
    .await;
    signal(original_cores[0], "KILL");
    assert_eq!(admitted["ok"], true, "{admitted}");

    bounded(async {
        loop {
            let view = control(
                fixture.port,
                json!({"command":"operation","operation_id":id}),
            )
            .await;
            if matches!(
                view["operation"]["state"].as_str(),
                Some("succeeded" | "failed")
            ) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;

    // Identify all fixture processes, including any that have lost their parent. Both
    // Gateway and Core command lines contain the fixture's private runtime path.
    let (gateway_pid, cores) = bounded(async {
        loop {
            let mut gateways = Vec::new();
            let mut cores = Vec::new();
            let mut all_children = true;
            for entry in std::fs::read_dir("/proc").unwrap().flatten() {
                let Some(pid) = entry
                    .file_name()
                    .to_str()
                    .and_then(|s| s.parse::<u32>().ok())
                else {
                    continue;
                };
                if pid == botd_pid {
                    continue;
                }
                let Ok(cmdline) = std::fs::read(entry.path().join("cmdline")) else {
                    continue;
                };
                let args: Vec<_> = cmdline.split(|b| *b == 0).collect();
                let root = fixture.root.to_string_lossy();
                if !args.iter().any(|arg| arg.starts_with(root.as_bytes())) || !alive(pid) {
                    continue;
                }
                let status =
                    std::fs::read_to_string(entry.path().join("status")).unwrap_or_default();
                let parent = status
                    .lines()
                    .find_map(|line| line.strip_prefix("PPid:")?.trim().parse::<u32>().ok());
                all_children &= parent == Some(botd_pid);
                if args.contains(&b"--core-listen".as_slice()) {
                    cores.push(pid);
                } else {
                    gateways.push(pid);
                }
            }
            if all_children
                && gateways.len() == 1
                && cores.len() == 2
                && !cores.contains(&original_cores[0])
                && cores.iter().all(|pid| alive(*pid))
            {
                break (gateways[0], cores);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;

    let mut gateway = if gateway_pid == original_gateway {
        first.take().unwrap()
    } else {
        bounded(async {
            loop {
                let candidate = fixture.accept().await;
                if candidate.pid() == gateway_pid {
                    break candidate;
                }
            }
        })
        .await
    };
    bounded(async {
        loop {
            let status = gateway.status().await;
            let workers = worker_pids(&status);
            if status["pid"] == gateway_pid
                && workers.len() == 2
                && workers.iter().all(|pid| cores.contains(pid) && alive(*pid))
                && workers[0] != workers[1]
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;

    signal(botd_pid, "TERM");
    let mut daemon = fixture.daemon;
    bounded(daemon.wait()).await.unwrap();
    bounded(async {
        while [gateway_pid, cores[0], cores[1]].into_iter().any(alive) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    let ipc: Vec<_> = walk(&fixture.root)
        .into_iter()
        .filter(|p| p.extension().is_some_and(|e| e == "sock" || e == "secret"))
        .collect();
    assert!(ipc.is_empty(), "sockets or secret left behind: {ipc:?}");
    let _ = std::fs::remove_dir_all(&fixture.root);
}

/// A Gateway program that behaves like the real one, except that a `--standby` launch logs
/// in (writes `standby-ready`) and then never confirms that it holds the Cores.
fn silent_standby_program(root: &Path) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let script = root.join("gw.sh");
    std::fs::write(
        &script,
        format!(
            r#"#!/bin/sh
case " $* " in
  *" --standby "*)
    state=""
    prev=""
    for arg in "$@"; do
      if [ "$prev" = "--state-path" ]; then state="$arg"; fi
      prev="$arg"
    done
    mkdir -p "$state"
    echo ready > "$state/standby-ready"
    IFS= read -r line
    echo got > "$state/got-activate"
    exec cat > /dev/null
    ;;
  *) exec {real} "$@" ;;
esac
"#,
            real = env!("CARGO_BIN_EXE_src")
        ),
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    script
}

/// If the activated standby never confirms that it holds the Cores, the update fails; the
/// running Gateway (already cut off from the Cores by the higher epoch) is killed and botd
/// brings a Gateway back, with the raised epoch, onto the very same Cores.
#[tokio::test]
async fn an_update_whose_standby_never_confirms_fails_and_the_gateway_recovers() {
    unconfirmed_update(false).await;
}

/// Same, but the old Gateway exits CLEANLY (code 0, as after a fatal Discord close) while the
/// update waits for the confirmation. Recovery reads a clean exit as "stopped on purpose"; the
/// failed update must still bring the Shard back.
#[tokio::test]
async fn a_gateway_that_exits_cleanly_during_an_update_is_brought_back_when_it_fails() {
    unconfirmed_update(true).await;
}

async fn unconfirmed_update(old_exits_cleanly: bool) {
    let fixture = Fixture::start_with(Some(&silent_standby_program)).await;
    let first = fixture.accept().await;
    let cores = worker_pids(&first.ready);
    let old_pid = first.pid();

    let id = "fedcba9876543210fedcba9876543210";
    let admitted = control(
        fixture.port,
        json!({"command":"gateway-update","bot":"101","operation_id":id}),
    )
    .await;
    assert_eq!(admitted["ok"], true, "{admitted}");

    if old_exits_cleanly {
        // Wait until the standby has been told to activate, then let the old Gateway leave
        // on its own with a clean exit (SIGINT is its cooperative shutdown).
        bounded(async {
            while !walk(&fixture.root)
                .iter()
                .any(|path| path.file_name().is_some_and(|n| n == "got-activate"))
            {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        signal(old_pid, "INT");
    }

    // The standby stub never confirms: the operation fails after the confirmation window.
    let finished = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let view = control(
                fixture.port,
                json!({"command":"operation","operation_id":id}),
            )
            .await;
            if view["operation"]["state"] != "running" {
                return view;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("the update ends");
    assert_eq!(finished["operation"]["state"], "failed", "{finished}");

    // The old Gateway is gone, a new one serves the same Cores, and the failure is on record.
    bounded(async {
        while alive(old_pid) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    let second = fixture.accept().await;
    assert_ne!(second.pid(), old_pid);
    assert_eq!(worker_pids(&second.ready), cores, "the Cores were kept");
    let audit = std::fs::read_to_string(fixture.root.join("gateway-update-audit.jsonl")).unwrap();
    let words: Vec<_> = audit
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap()["event"].clone())
        .collect();
    assert_eq!(words, [json!("requested"), json!("failed")], "{audit}");

    signal(fixture.daemon.id().unwrap(), "TERM");
    let mut daemon = fixture.daemon;
    bounded(daemon.wait()).await.unwrap();
    bounded(async {
        while [second.pid(), cores[0], cores[1]].into_iter().any(alive) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    let _ = std::fs::remove_dir_all(&fixture.root);
}
