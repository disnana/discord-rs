use src::{AppError, worker};
use std::{env, sync::Arc};
use tokio::sync::Semaphore;
use twilight_gateway::{Event, EventTypeFlags, Intents, Shard, ShardId, StreamExt as _};
use twilight_http::Client;

#[tokio::main]
async fn main() -> Result<(), AppError> {
    if env::args().nth(1).as_deref() == Some("--worker") {
        return worker::run().await;
    }
    let _ = rustls::crypto::ring::default_provider().install_default();
    let token = env::var("DISCORD_TOKEN")?;
    let manager = worker::Manager::start(env::current_exe()?).await?;
    manager.start_jobs();
    tokio::spawn(manager.clone().watch(worker::worker_file()));
    let mut shard = Shard::new(
        ShardId::ONE,
        token.clone(),
        Intents::GUILD_MESSAGES | Intents::MESSAGE_CONTENT,
    );
    let http = Arc::new(Client::new(token));
    let capacity = Arc::new(Semaphore::new(64));
    println!("Botを起動しています...");
    while let Some(result) = shard.next_event(EventTypeFlags::all()).await {
        match result {
            Ok(Event::Ready(ready)) => println!("ログイン成功: {}", ready.user.name),
            Ok(Event::MessageCreate(message)) if !message.author.bot => {
                let Ok(permit) = capacity.clone().try_acquire_owned() else {
                    eprintln!("Workerが混雑しているためメッセージを処理できませんでした");
                    continue;
                };
                let manager = manager.clone();
                let http = http.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    if message.content.trim() == "!workers" {
                        let content = manager.describe_workers().await;
                        if let Err(error) = http
                            .create_message(message.channel_id)
                            .content(&content)
                            .await
                        {
                            eprintln!("送信エラー: {error}");
                        }
                        return;
                    }
                    match manager
                        .dispatch_key(message.channel_id.get(), message.content.clone())
                        .await
                    {
                        Ok(Some(content)) => {
                            if let Err(error) = http
                                .create_message(message.channel_id)
                                .content(&content)
                                .await
                            {
                                eprintln!("送信エラー: {error}");
                            }
                        }
                        Ok(None) => {}
                        Err(error) => eprintln!("Worker error: {error}"),
                    }
                });
            }
            Err(error) => eprintln!("Gateway error: {error}"),
            _ => {}
        }
    }
    Ok(())
}
