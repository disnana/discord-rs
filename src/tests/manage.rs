use std::process::Stdio;
use tokio::io::AsyncWriteExt;

#[tokio::test]
async fn management_console_reports_status_and_missing_token() {
    let mut process = tokio::process::Command::new(env!("CARGO_BIN_EXE_manage"))
        .env_remove("DISCORD_TOKEN")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = process.stdin.take().unwrap();
    input
        .write_all(b"status\nstart\nstop\nquit\n")
        .await
        .unwrap();
    drop(input);
    let output = process.wait_with_output().await.unwrap();
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("管理中のBotは停止しています"));
    assert!(String::from_utf8_lossy(&output.stderr).contains("DISCORD_TOKEN が設定されていません"));
}
