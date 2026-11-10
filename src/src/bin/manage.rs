use std::{
    env,
    error::Error,
    path::{Path, PathBuf},
    process::Stdio,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, Command},
};

type ErrorBox = Box<dyn Error + Send + Sync>;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

async fn build() -> Result<PathBuf, ErrorBox> {
    let root = root();
    let target = root.join("target").join("reload-build");
    println!("Botをビルドしています...");
    let status = Command::new(env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
        .current_dir(&root)
        .args(["build", "--release", "--bin", "src", "--target-dir"])
        .arg(&target)
        .env_remove("DISCORD_BUILD_ID")
        .status()
        .await?;
    if !status.success() {
        return Err("ビルドに失敗しました".into());
    }
    let folder = root.join("workers");
    tokio::fs::create_dir_all(&folder).await?;
    let id = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let copy = folder.join(format!(
        "parent-{}-{id}{}",
        std::process::id(),
        env::consts::EXE_SUFFIX
    ));
    tokio::fs::copy(
        target
            .join("release")
            .join(format!("src{}", env::consts::EXE_SUFFIX)),
        &copy,
    )
    .await?;
    Ok(copy)
}

fn start(path: &Path) -> Result<Child, ErrorBox> {
    if env::var_os("DISCORD_TOKEN").is_none_or(|value| value.is_empty()) {
        return Err("DISCORD_TOKEN が設定されていません".into());
    }
    let child = Command::new(path)
        .current_dir(root())
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    println!(
        "Botプロセスを起動しました (PID: {})。Discord接続の成否はBotのログを確認してください。",
        child.id().unwrap_or_default()
    );
    Ok(child)
}

async fn stop(child: &mut Option<Child>) -> Result<(), ErrorBox> {
    if let Some(mut process) = child.take() {
        if process.try_wait()?.is_none() {
            process.kill().await?;
        }
        let _ = process.wait().await;
        println!("Botを停止しました。メモリ内の状態は失われます。");
    } else {
        println!("管理中のBotは停止しています。");
    }
    Ok(())
}

fn help() {
    println!(
        "start: 起動 / status: 状態 / stop: 停止 / restart: 再起動 / update: Worker更新 / help: ヘルプ / quit: 停止して終了"
    );
}

#[tokio::main]
async fn main() -> Result<(), ErrorBox> {
    println!("Discord Bot 管理コンソール（このコンソールで起動したBotのみ管理します）");
    help();
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut child: Option<Child> = None;
    loop {
        print!("> ");
        tokio::io::stdout().flush().await?;
        let Some(line) = lines.next_line().await? else {
            break;
        };
        let command = line.trim();
        if let Some(process) = child.as_mut()
            && let Some(status) = process.try_wait()?
        {
            println!("Botプロセスは終了しています: {status}");
            child = None;
        }
        let result: Result<(), ErrorBox> = async {
            match command {
                "start" => {
                    if child.is_some() { println!("Botは既に起動しています。"); }
                    else if env::var_os("DISCORD_TOKEN").is_none_or(|value| value.is_empty()) { return Err("DISCORD_TOKEN が設定されていません".into()); }
                    else { let path = build().await?; child = Some(start(&path)?); }
                }
                "status" => match child.as_ref() {
                    Some(process) => println!("Botプロセス: 実行中 (PID: {})。Discord接続とWorkerはBotログまたはDiscordの !workers で確認してください。", process.id().unwrap_or_default()),
                    None => println!("管理中のBotは停止しています。"),
                },
                "stop" => stop(&mut child).await?,
                "restart" => {
                    if env::var_os("DISCORD_TOKEN").is_none_or(|value| value.is_empty()) { return Err("DISCORD_TOKEN が設定されていません".into()); }
                    let path = build().await?;
                    stop(&mut child).await?;
                    child = Some(start(&path)?);
                }
                "update" => {
                    if child.is_none() { return Err("管理中のBotを先に起動してください".into()); }
                    let status = Command::new(env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
                        .current_dir(root()).args(["run", "--bin", "reload", "--", "general"]).status().await?;
                    if !status.success() { return Err("更新を確認できませんでした。Botのログを確認してください".into()); }
                }
                "help" => help(),
                "quit" | "exit" => {}
                "" => {}
                _ => println!("不明なコマンドです。help を入力してください。"),
            }
            Ok(())
        }.await;
        if let Err(error) = result {
            eprintln!("エラー: {error}");
        }
        if matches!(command, "quit" | "exit") {
            break;
        }
    }
    stop(&mut child).await?;
    Ok(())
}
