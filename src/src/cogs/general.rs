use super::{Job, State};

pub const BUILD_ID: &str = match option_env!("DISCORD_BUILD_ID") {
    Some(value) => value,
    None => "dev",
};

pub fn dispatch(key: u64, content: &str, state: &mut State) -> Option<String> {
    match content.trim() {
        "!ping" => Some("Pong! 🦀".to_owned()),
        "!version" => Some(format!(
            "Workerビルド: {BUILD_ID} / PID: {}",
            std::process::id()
        )),
        "!count" => {
            let count = state.counts.entry(key).or_default();
            *count = count.saturating_add(1);
            Some(format!("カウント: {count}"))
        }
        "!job start" => {
            if state.jobs.get(&key).is_some_and(|job| job.running) {
                Some("このチャンネルでは仕事が実行中です。!job status で確認できます。".into())
            } else {
                state.jobs.insert(
                    key,
                    Job {
                        step: 0,
                        total: 0,
                        running: true,
                    },
                );
                Some("仕事を開始しました。!job status で進捗を確認できます。".into())
            }
        }
        "!job status" => Some(match state.jobs.get(&key) {
            Some(job) => format!(
                "仕事{}: {}ステップ、合計 {}\nWorker: {BUILD_ID} / PID: {}",
                if job.running {
                    "実行中"
                } else {
                    "停止中"
                },
                job.step,
                job.total,
                std::process::id()
            ),
            None => "仕事はありません。!job start で開始できます。".into(),
        }),
        "!job stop" => Some(match state.jobs.get_mut(&key) {
            Some(job) => {
                job.running = false;
                format!(
                    "仕事を停止しました: {}ステップ、合計 {}",
                    job.step, job.total
                )
            }
            None => "仕事はありません。".into(),
        }),
        _ => None,
    }
}
