use src::worker::Manager;
use std::{path::PathBuf, sync::Arc};

#[tokio::test]
async fn scheduler_advances_jobs_automatically() {
    let manager = Manager::start_pool(PathBuf::from(env!("CARGO_BIN_EXE_src")), 1)
        .await
        .unwrap();
    manager.start_jobs();
    manager.dispatch("!job start".into()).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1300)).await;
    let status = manager
        .dispatch("!job status".into())
        .await
        .unwrap()
        .unwrap();
    assert!(status.starts_with("仕事実行中: 1ステップ"), "{status}");
}

#[tokio::test]
async fn switches_real_workers_and_retains_old_worker_on_failure() {
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_src"));
    let directory =
        std::env::temp_dir().join(format!("discord-rs-update-test-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let second = directory.join("worker-v2.exe");
    std::fs::copy(&binary, &second).unwrap();
    let manager = Manager::start(binary).await.unwrap();
    assert_eq!(
        manager
            .dispatch("!ping".into())
            .await
            .unwrap()
            .as_deref()
            .map(|s| s.lines().next().unwrap()),
        Some("Pong! 🦀")
    );
    assert_eq!(
        manager
            .dispatch("!count".into())
            .await
            .unwrap()
            .as_deref()
            .map(|s| s.lines().next().unwrap()),
        Some("カウント: 1")
    );
    assert_eq!(
        manager
            .dispatch_key(1, "!count".into())
            .await
            .unwrap()
            .as_deref()
            .map(|s| s.lines().next().unwrap()),
        Some("カウント: 1")
    );
    manager.dispatch("!job start".into()).await.unwrap();
    manager.dispatch_key(1, "!job start".into()).await.unwrap();
    for _ in 0..3 {
        manager.tick_key(0).await.unwrap();
    }
    for _ in 0..7 {
        manager.tick_key(1).await.unwrap();
    }
    assert_eq!(
        manager
            .dispatch("!job status".into())
            .await
            .unwrap()
            .as_deref()
            .map(|s| s.lines().next().unwrap()),
        Some("仕事実行中: 3ステップ、合計 6")
    );
    assert_eq!(
        manager
            .dispatch_key(1, "!job status".into())
            .await
            .unwrap()
            .as_deref()
            .map(|s| s.lines().next().unwrap()),
        Some("仕事実行中: 7ステップ、合計 28")
    );

    let mut requests = Vec::new();
    for _ in 0..20 {
        let manager = Arc::clone(&manager);
        requests.push(tokio::spawn(async move {
            manager.dispatch("!count".into()).await.unwrap().unwrap()
        }));
    }
    manager.replace(second, false).await.unwrap();
    assert_eq!(
        manager
            .dispatch("!job status".into())
            .await
            .unwrap()
            .as_deref()
            .map(|s| s.lines().next().unwrap()),
        Some("仕事実行中: 3ステップ、合計 6")
    );
    assert_eq!(
        manager
            .dispatch_key(1, "!job status".into())
            .await
            .unwrap()
            .as_deref()
            .map(|s| s.lines().next().unwrap()),
        Some("仕事実行中: 7ステップ、合計 28")
    );
    for _ in 0..27 {
        manager.tick_key(0).await.unwrap();
    }
    for _ in 0..23 {
        manager.tick_key(1).await.unwrap();
    }
    manager.tick_key(0).await.unwrap();
    assert!(
        manager
            .dispatch("!job status".into())
            .await
            .unwrap()
            .unwrap()
            .starts_with("仕事実行中: 31ステップ、合計 496")
    );
    manager.dispatch("!job stop".into()).await.unwrap();
    manager.tick_key(0).await.unwrap();
    assert!(
        manager
            .dispatch("!job status".into())
            .await
            .unwrap()
            .unwrap()
            .starts_with("仕事停止中: 31ステップ、合計 496")
    );
    assert_eq!(
        manager
            .dispatch("!job status".into())
            .await
            .unwrap()
            .as_deref()
            .map(|s| s.lines().next().unwrap()),
        Some("仕事停止中: 31ステップ、合計 496")
    );
    assert_eq!(
        manager
            .dispatch_key(1, "!job status".into())
            .await
            .unwrap()
            .as_deref()
            .map(|s| s.lines().next().unwrap()),
        Some("仕事実行中: 30ステップ、合計 465")
    );
    let mut counts = Vec::new();
    for request in requests {
        counts.push(
            request
                .await
                .unwrap()
                .strip_prefix("カウント: ")
                .unwrap()
                .parse::<u64>()
                .unwrap(),
        );
    }
    counts.sort_unstable();
    assert_eq!(counts, (2..=21).collect::<Vec<_>>());
    assert_eq!(
        manager
            .dispatch("!count".into())
            .await
            .unwrap()
            .as_deref()
            .map(|s| s.lines().next().unwrap()),
        Some("カウント: 22")
    );
    assert_eq!(
        manager
            .dispatch_key(1, "!count".into())
            .await
            .unwrap()
            .as_deref()
            .map(|s| s.lines().next().unwrap()),
        Some("カウント: 2")
    );
    assert!(
        manager
            .replace(directory.join("missing.exe"), false)
            .await
            .is_err()
    );
    assert_eq!(
        manager
            .dispatch("!count".into())
            .await
            .unwrap()
            .as_deref()
            .map(|s| s.lines().next().unwrap()),
        Some("カウント: 23")
    );
    assert_eq!(
        manager
            .dispatch_key(1, "!count".into())
            .await
            .unwrap()
            .as_deref()
            .map(|s| s.lines().next().unwrap()),
        Some("カウント: 3")
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
    assert_eq!(manager.dispatch("hello".into()).await.unwrap(), None);
    drop(manager);
    // On Windows the process may take a moment to exit after kill_on_drop.
    for _ in 0..20 {
        if std::fs::remove_dir_all(&directory).is_ok() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("could not remove test directory");
}
