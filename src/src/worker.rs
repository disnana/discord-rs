use crate::AppError;
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{Mutex, RwLock},
    time::timeout,
};

const PROTOCOL: u32 = 5;
const STATE_VERSION: u32 = 3;
const DEADLINE: Duration = Duration::from_secs(5);

pub fn worker_file() -> PathBuf {
    std::env::var_os("DISCORD_WORKER_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("worker-path.txt"))
}

pub fn status_file(file: &Path) -> PathBuf {
    let mut path = file.as_os_str().to_owned();
    path.push(".status");
    PathBuf::from(path)
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Request {
    Health,
    Dispatch {
        key: u64,
        content: String,
    },
    Tick,
    Snapshot,
    Restore {
        version: u32,
        state: serde_json::Value,
    },
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Response {
    Ready {
        protocol: u32,
        build: String,
        pid: u32,
    },
    Result {
        content: Option<String>,
        state: serde_json::Value,
    },
    Snapshot {
        version: u32,
        state: serde_json::Value,
    },
    Restored,
    Rejected {
        reason: String,
    },
}

struct Process {
    _child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    broken: bool,
    checkpoint: Option<serde_json::Value>,
    build: String,
    pid: u32,
}
impl Process {
    async fn start(path: &Path) -> Result<Self, AppError> {
        let mut child = Command::new(path)
            .arg("--worker")
            .env_remove("DISCORD_TOKEN")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;
        let input = child.stdin.take().ok_or("missing worker stdin")?;
        let output = BufReader::new(child.stdout.take().ok_or("missing worker stdout")?);
        let mut process = Self {
            _child: child,
            input,
            output,
            broken: false,
            checkpoint: None,
            build: String::new(),
            pid: 0,
        };
        match process.request(&Request::Health).await? {
            Response::Ready {
                protocol: PROTOCOL,
                build,
                pid,
            } => {
                process.build = build;
                process.pid = pid;
                Ok(process)
            }
            _ => Err("incompatible worker protocol".into()),
        }
    }

    async fn request(&mut self, request: &Request) -> Result<Response, AppError> {
        if self.broken {
            return Err("worker is unavailable".into());
        }
        let result = timeout(DEADLINE, async {
            let mut data = serde_json::to_vec(request)?;
            data.push(b'\n');
            self.input.write_all(&data).await?;
            self.input.flush().await?;
            let mut line = String::new();
            if self.output.read_line(&mut line).await? == 0 {
                return Err("worker exited".into());
            }
            Ok(serde_json::from_str(&line)?)
        })
        .await
        .map_err(|_| -> AppError { "worker timed out".into() })
        .and_then(|result| result);
        if result.is_err() {
            self.broken = true;
        }
        result
    }
}

struct Active {
    path: PathBuf,
    process: Arc<Mutex<Process>>,
}
pub struct Manager {
    workers: Vec<RwLock<Active>>,
    update: Mutex<()>,
}
impl Manager {
    pub async fn describe_workers(&self) -> String {
        let mut lines = vec!["稼働中のWorker:".to_string()];
        for (index, worker) in self.workers.iter().enumerate() {
            let active = worker.read().await;
            let process = active.process.lock().await;
            lines.push(format!(
                "{}: {} / PID: {}",
                index + 1,
                process.build,
                process.pid
            ));
        }
        lines.join("\n")
    }
    pub async fn start(path: PathBuf) -> Result<Arc<Self>, AppError> {
        Self::start_pool(path, 2).await
    }
    pub async fn start_pool(path: PathBuf, count: usize) -> Result<Arc<Self>, AppError> {
        if count == 0 {
            return Err("Worker数は1以上にしてください".into());
        }
        let mut workers = Vec::with_capacity(count);
        for _ in 0..count {
            let process = Process::start(&path).await?;
            workers.push(RwLock::new(Active {
                path: path.clone(),
                process: Arc::new(Mutex::new(process)),
            }));
        }
        Ok(Arc::new(Self {
            workers,
            update: Mutex::new(()),
        }))
    }
    pub async fn replace(&self, path: PathBuf, force: bool) -> Result<(), AppError> {
        let _guard = self.update.lock().await;
        for (index, worker) in self.workers.iter().enumerate() {
            if !force && worker.read().await.path == path {
                continue;
            }
            let mut process = Process::start(&path).await?;
            // Exclusive lock drains accepted requests and queues new ones during migration.
            let mut active = worker.write().await;
            let snapshot = active
                .process
                .lock()
                .await
                .request(&Request::Snapshot)
                .await?;
            let (version, state) = match snapshot {
                Response::Snapshot { version, state } => (version, state),
                _ => return Err("旧Workerから状態を取得できません".into()),
            };
            match process
                .request(&Request::Restore {
                    version,
                    state: state.clone(),
                })
                .await?
            {
                Response::Restored => {}
                Response::Rejected { reason } => {
                    return Err(format!("状態の引き継ぎを拒否しました: {reason}").into());
                }
                _ => return Err("新Workerが状態の引き継ぎを確認できません".into()),
            }
            process.checkpoint = Some(state);
            *active = Active {
                path: path.clone(),
                process: Arc::new(Mutex::new(process)),
            };
            println!(
                "Worker {}/{} を切り替えました: {}",
                index + 1,
                self.workers.len(),
                path.display()
            );
        }
        Ok(())
    }
    pub async fn dispatch(&self, content: String) -> Result<Option<String>, AppError> {
        self.dispatch_key(0, content).await
    }
    pub async fn dispatch_key(
        &self,
        key: u64,
        content: String,
    ) -> Result<Option<String>, AppError> {
        let index = ((key ^ (key >> 22)) % self.workers.len() as u64) as usize;
        self.execute(index, Request::Dispatch { key, content })
            .await
    }
    pub async fn tick_key(&self, key: u64) -> Result<(), AppError> {
        let index = ((key ^ (key >> 22)) % self.workers.len() as u64) as usize;
        self.execute(index, Request::Tick).await.map(|_| ())
    }
    pub fn start_jobs(self: &Arc<Self>) {
        for index in 0..self.workers.len() {
            let manager = self.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(1));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                interval.tick().await;
                loop {
                    interval.tick().await;
                    if let Err(error) = manager.execute(index, Request::Tick).await {
                        eprintln!("Worker {} の仕事を進められません: {error}", index + 1);
                    }
                }
            });
        }
    }
    async fn execute(&self, index: usize, request: Request) -> Result<Option<String>, AppError> {
        let worker = &self.workers[index];
        let active = worker.read().await;
        let process = active.process.clone();
        let mut guard = process.lock().await;
        let result = guard.request(&request).await;
        if let Ok(Response::Result { state, .. }) = &result {
            guard.checkpoint = Some(state.clone());
        }
        let checkpoint = guard.checkpoint.clone();
        drop(guard);
        drop(active);
        let result = match result {
            Ok(Response::Result { content, .. }) => Ok(content),
            Ok(_) => Err("unexpected worker response".into()),
            Err(error) => Err(error),
        };
        if result.is_err() {
            let _guard = self.update.lock().await;
            let active = worker.read().await;
            if Arc::ptr_eq(&active.process, &process) {
                let path = active.path.clone();
                drop(active);
                match Process::start(&path).await {
                    Ok(mut new_process) => {
                        if let Some(state) = checkpoint {
                            match new_process
                                .request(&Request::Restore {
                                    version: STATE_VERSION,
                                    state: state.clone(),
                                })
                                .await
                            {
                                Ok(Response::Restored) => new_process.checkpoint = Some(state),
                                _ => {
                                    eprintln!("Workerの状態を復旧できません");
                                    return result;
                                }
                            }
                        }
                        *worker.write().await = Active {
                            path,
                            process: Arc::new(Mutex::new(new_process)),
                        };
                        eprintln!("Workerを再起動しました");
                    }
                    Err(error) => eprintln!("Workerの再起動に失敗しました: {error}"),
                }
            }
        }
        result
    }
    pub async fn watch(self: Arc<Self>, file: PathBuf) {
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            match tokio::fs::read_to_string(&file).await {
                Ok(value) if !value.trim().is_empty() => {
                    let path = PathBuf::from(value.trim());
                    if !path.is_absolute() {
                        eprintln!("Workerのパスは絶対パスにしてください");
                        continue;
                    }
                    match self.replace(path.clone(), false).await {
                        Ok(()) => {
                            if let Err(error) = tokio::fs::write(
                                status_file(&file),
                                path.to_string_lossy().as_bytes(),
                            )
                            .await
                            {
                                eprintln!("Workerの状態を記録できません: {error}");
                            }
                        }
                        Err(error) => eprintln!("Workerの更新に失敗しました: {error}"),
                    }
                }
                Ok(_) => eprintln!("Worker指定ファイルが空です"),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => eprintln!("Worker指定ファイルを読めません: {error}"),
            }
        }
    }
}

pub async fn run() -> Result<(), AppError> {
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut output = tokio::io::stdout();
    let mut state = crate::cogs::State::default();
    while let Some(line) = lines.next_line().await? {
        let response = match serde_json::from_str(&line)? {
            Request::Health => Response::Ready {
                protocol: PROTOCOL,
                build: crate::cogs::build_id().to_owned(),
                pid: std::process::id(),
            },
            Request::Dispatch { key, content } => {
                let content = crate::cogs::dispatch(key, &content, &mut state);
                Response::Result {
                    content,
                    state: serde_json::to_value(&state)?,
                }
            }
            Request::Tick => {
                crate::cogs::tick(&mut state);
                Response::Result {
                    content: None,
                    state: serde_json::to_value(&state)?,
                }
            }
            Request::Snapshot => Response::Snapshot {
                version: STATE_VERSION,
                state: serde_json::to_value(&state)?,
            },
            Request::Restore {
                version,
                state: incoming,
            } => {
                if version != STATE_VERSION {
                    Response::Rejected {
                        reason: format!("unsupported state version: {version}"),
                    }
                } else {
                    match serde_json::from_value(incoming) {
                        Ok(restored) => {
                            state = restored;
                            Response::Restored
                        }
                        Err(error) => Response::Rejected {
                            reason: error.to_string(),
                        },
                    }
                }
            }
        };
        let mut data = serde_json::to_vec(&response)?;
        data.push(b'\n');
        output.write_all(&data).await?;
        output.flush().await?;
    }
    Ok(())
}
