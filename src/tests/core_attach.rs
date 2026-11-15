//! 実プロセスの Core を `--core-listen`（Unix ソケット）で起動し、接続して要求ループが
//! 動くことを確認する統合試験（Gateway/Core 分離の 1-B1）。Discord には接続しない。Unix のみ。
#![cfg(unix)]

use serde_json::Value;
use std::{
    future::Future,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Stdio,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    process::{Child, Command},
};

const SECRET: &str = "a3f1c2d4e5b60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";
const OTHER_SECRET: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(20), future)
        .await
        .expect("core attach integration deadline")
}

/// 0700 の一時ディレクトリ（Core はこの前提でしかソケットを作らない）。
fn private_dir() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "core-attach-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    dir
}

struct Core {
    child: Child,
    socket: PathBuf,
}

impl Core {
    /// Core を起動して、秘密を stdin で渡し、ソケットができるまで待つ。
    async fn start(socket: &Path, secret: &str) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_src"))
            .arg("--core-listen")
            .arg(socket)
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut stdin = child.stdin.take().unwrap();
        stdin
            .write_all(format!("{secret}\n").as_bytes())
            .await
            .unwrap();
        stdin.flush().await.unwrap();
        // stdin は閉じない（閉じても Core は動き続けるが、実運用の親と同じ形にする）。
        std::mem::forget(stdin);
        let core = Self {
            child,
            socket: socket.to_path_buf(),
        };
        // 「ファイルがある」ではなく「接続できる」まで待つ（古いソケットが残っている
        // 場合、ファイルは最初からあるが、まだ誰も listen していない）。接続は、秘密を
        // 送らずに閉じるので、試行として失敗扱いになるだけで、Core は次を受け付ける。
        bounded(async {
            while UnixStream::connect(&core.socket).await.is_err() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        core
    }
}

async fn attach(socket: &Path, secret: &str) -> UnixStream {
    let mut stream = UnixStream::connect(socket).await.unwrap();
    stream
        .write_all(format!("{secret}\n").as_bytes())
        .await
        .unwrap();
    stream
}

/// 1 行のフレームを送って、1 行の応答を JSON で受ける。
async fn exchange(stream: &mut UnixStream, request: &str) -> Option<Value> {
    stream
        .write_all(format!("{request}\n").as_bytes())
        .await
        .ok()?;
    let mut reader = BufReader::new(&mut *stream);
    let mut line = String::new();
    let read = reader.read_line(&mut line).await.ok()?;
    if read == 0 {
        return None;
    }
    serde_json::from_str(&line).ok()
}

#[tokio::test]
async fn core_serves_the_request_loop_over_the_socket_and_exits_when_the_connection_closes() {
    let dir = private_dir();
    let socket = dir.join("core.sock");
    let mut core = Core::start(&socket, SECRET).await;

    // ソケットは 0600。
    let mode = std::fs::metadata(&socket).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600);

    let mut stream = attach(&socket, SECRET).await;
    let response = bounded(exchange(&mut stream, r#"{"type":"health"}"#))
        .await
        .expect("health response");
    assert_eq!(response["type"], "ready", "{response}");
    assert_eq!(response["credential_free"], true, "{response}");

    // 接続を閉じると、1 接続の寿命の Core は終了する（1-B1 の契約）。
    drop(stream);
    let status = bounded(core.child.wait()).await.unwrap();
    let _ = status; // 終了コードは問わない。終了すること自体が契約。
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn a_wrong_secret_is_refused_and_a_later_correct_client_is_served() {
    let dir = private_dir();
    let socket = dir.join("core.sock");
    let mut core = Core::start(&socket, SECRET).await;

    // 誤った秘密: 応答はなく、接続は閉じられる。
    let mut intruder = attach(&socket, OTHER_SECRET).await;
    assert!(
        bounded(exchange(&mut intruder, r#"{"type":"health"}"#))
            .await
            .is_none(),
        "a wrong secret must not get a response"
    );

    // 形式が不正な最初の行も拒否される。
    let mut malformed = UnixStream::connect(&socket).await.unwrap();
    malformed
        .write_all(b"not-a-secret-not-a-secret-not-a-secret-not-a-secret-not-a-secret!!\n")
        .await
        .unwrap();
    let mut rest = Vec::new();
    let _ = bounded(malformed.read_to_end(&mut rest)).await;
    assert!(rest.is_empty());

    // その後の正しいクライアントは通常どおり処理される。
    let mut stream = attach(&socket, SECRET).await;
    let response = bounded(exchange(&mut stream, r#"{"type":"health"}"#))
        .await
        .expect("health response after refused clients");
    assert_eq!(response["type"], "ready");

    drop(stream);
    let _ = bounded(core.child.wait()).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn unsafe_socket_locations_are_refused_before_listening() {
    // 相対パス。
    let mut relative = Command::new(env!("CARGO_BIN_EXE_src"))
        .args(["--core-listen", "relative.sock"])
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut relative_stdin = relative.stdin.take().unwrap();
    relative_stdin
        .write_all(format!("{SECRET}\n").as_bytes())
        .await
        .unwrap();
    assert!(!bounded(relative.wait()).await.unwrap().success());

    // グループ/その他が入れる親ディレクトリ。
    let open = private_dir();
    std::fs::set_permissions(&open, std::fs::Permissions::from_mode(0o755)).unwrap();
    let socket = open.join("core.sock");
    let mut child = Command::new(env!("CARGO_BIN_EXE_src"))
        .arg("--core-listen")
        .arg(&socket)
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    stdin
        .write_all(format!("{SECRET}\n").as_bytes())
        .await
        .unwrap();
    let status = bounded(child.wait()).await.unwrap();
    assert!(!status.success());
    assert!(
        !socket.exists(),
        "no socket may be created in an open directory"
    );
    let _ = std::fs::remove_dir_all(&open);
}

#[tokio::test]
async fn a_stale_socket_is_replaced_but_a_regular_file_is_never_overwritten() {
    // 古いソケット（前の Core の残り）は置き換えられる。
    let dir = private_dir();
    let socket = dir.join("core.sock");
    {
        // ソケットファイルを作って、リスナーを閉じる（ファイルは残る）。
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        drop(listener);
    }
    assert!(socket.exists());
    let mut core = Core::start(&socket, SECRET).await;
    let mut stream = attach(&socket, SECRET).await;
    let response = bounded(exchange(&mut stream, r#"{"type":"health"}"#))
        .await
        .expect("health response on a replaced socket");
    assert_eq!(response["type"], "ready");
    drop(stream);
    let _ = bounded(core.child.wait()).await;
    let _ = std::fs::remove_dir_all(&dir);

    // 通常ファイルがあるパスには何もしない。
    let dir = private_dir();
    let path = dir.join("core.sock");
    std::fs::write(&path, b"precious").unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_src"))
        .arg("--core-listen")
        .arg(&path)
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    stdin
        .write_all(format!("{SECRET}\n").as_bytes())
        .await
        .unwrap();
    assert!(!bounded(child.wait()).await.unwrap().success());
    assert_eq!(std::fs::read(&path).unwrap(), b"precious");
    let _ = std::fs::remove_dir_all(&dir);
}
