use src::worker::Manager;
use std::{path::PathBuf, time::Duration};

#[tokio::test]
async fn reload_command_builds_and_switches_a_live_manager() {
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target-codex-verify")
        .join(format!("discord-reload-command-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let file = directory.join("worker-path.txt");
    let manager = Manager::start(PathBuf::from(env!("CARGO_BIN_EXE_src")))
        .await
        .unwrap();
    let before = manager.describe_workers().await;
    manager.dispatch("!job start".into()).await.unwrap();
    for _ in 0..4 {
        manager.tick_key(0).await.unwrap();
    }
    let watcher = tokio::spawn(manager.clone().watch(file.clone()));
    let output = tokio::time::timeout(
        Duration::from_secs(240),
        tokio::process::Command::new(env!("CARGO_BIN_EXE_reload"))
            .arg("general")
            .env("DISCORD_WORKER_FILE", &file)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let after = manager.describe_workers().await;
    let output_text = String::from_utf8_lossy(&output.stdout);
    let build = output_text
        .lines()
        .find_map(|line| line.strip_prefix("更新しました！ Workerのビルド: "))
        .unwrap();
    assert_ne!(before, after);
    assert_eq!(after.matches(build).count(), 2, "{after}");
    assert!(
        manager
            .dispatch("!version".into())
            .await
            .unwrap()
            .unwrap()
            .contains(build)
    );
    assert_eq!(
        manager
            .dispatch("!job status".into())
            .await
            .unwrap()
            .as_deref()
            .map(|s| s.lines().next().unwrap()),
        Some("仕事実行中: 4ステップ、合計 10")
    );
    manager.tick_key(0).await.unwrap();
    assert_eq!(
        manager
            .dispatch("!job status".into())
            .await
            .unwrap()
            .as_deref()
            .map(|s| s.lines().next().unwrap()),
        Some("仕事実行中: 5ステップ、合計 15")
    );
    assert_eq!(
        manager
            .dispatch("!ping".into())
            .await
            .unwrap()
            .as_deref()
            .map(|s| s.lines().next().unwrap()),
        Some("Pong! 🦀")
    );
    let worker = PathBuf::from(std::fs::read_to_string(&file).unwrap());
    watcher.abort();
    drop(manager);
    for _ in 0..40 {
        if std::fs::remove_file(&worker).is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    std::fs::remove_dir_all(directory).unwrap();
}
