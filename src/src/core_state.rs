//! Durable copy of the Gateway's confirmed snapshot-v3 (legacy count/job) state.
//!
//! Only the credential-free `cogs::State` values the Manager already holds as
//! confirmed checkpoints are written: no Bot token, no event journal and no
//! framework `StateStore` data. The file is bound to Bot id, Shard id, total
//! shards and the worker pool size; a file that is missing, unreadable, corrupt,
//! of another schema or bound to another Shard is ignored (empty state), and a
//! failed write is only a warning. This is independent of the framework's own
//! file-backed `StateStore`.
//!
//! Writes are atomic (private temporary file, fsync, rename, directory fsync)
//! and mode 0600. Symbolic links are never followed when reading, and the
//! temporary file is created with `create_new`.

use std::{
    fs,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::Mutex as StdMutex,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;

pub const FILE_NAME: &str = "core-state.json";
const TEMP_NAME: &str = ".core-state.json.tmp";
pub const SCHEMA: u32 = 1;
/// A larger encoded document is never written (warning only).
pub const MAX_FILE_BYTES: usize = 1024 * 1024;
/// Minimum spacing between periodic writes; shutdown always writes.
pub const MIN_WRITE_INTERVAL: Duration = Duration::from_secs(2);

/// Identity the file is bound to. `workers` is the size of the Manager pool,
/// because the key-to-worker routing depends on it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Binding {
    pub bot_id: u64,
    pub shard_id: u32,
    pub total_shards: u32,
    pub workers: usize,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    schema: u32,
    bot_id: u64,
    shard_id: u32,
    total_shards: u32,
    workers: usize,
    /// Ownership epoch of the Gateway that wrote this file (0 for a file from before
    /// Gateway updates existed). A Gateway with a lower epoch never replaces it.
    #[serde(default)]
    epoch: u64,
    states: Vec<Option<Value>>,
}

#[derive(Deserialize)]
struct StoredEpoch {
    #[serde(default)]
    epoch: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum EncodeError {
    TooLarge,
    Invalid,
}

fn valid_state(value: &Value) -> bool {
    serde_json::from_value::<crate::cogs::State>(value.clone()).is_ok()
}

/// Serialize `states` (one slot per worker). Pure; no I/O.
pub fn encode(binding: &Binding, states: &[Option<Value>]) -> Result<Vec<u8>, EncodeError> {
    encode_at(binding, states, 0)
}

/// Like [`encode`], stamping the writer ownership epoch into the document.
pub fn encode_at(
    binding: &Binding,
    states: &[Option<Value>],
    epoch: u64,
) -> Result<Vec<u8>, EncodeError> {
    if states.len() != binding.workers {
        return Err(EncodeError::Invalid);
    }
    let document = Document {
        schema: SCHEMA,
        bot_id: binding.bot_id,
        shard_id: binding.shard_id,
        total_shards: binding.total_shards,
        workers: binding.workers,
        epoch,
        states: states.to_vec(),
    };
    let mut bytes = serde_json::to_vec(&document).map_err(|_| EncodeError::Invalid)?;
    bytes.push(b'\n');
    if bytes.len() > MAX_FILE_BYTES {
        return Err(EncodeError::TooLarge);
    }
    Ok(bytes)
}

/// Parse and validate a document. Any defect yields `None`; no detail is kept.
pub fn decode(binding: &Binding, bytes: &[u8]) -> Option<Vec<Option<Value>>> {
    if bytes.len() > MAX_FILE_BYTES {
        return None;
    }
    let document: Document = serde_json::from_slice(bytes).ok()?;
    if document.schema != SCHEMA
        || document.bot_id != binding.bot_id
        || document.shard_id != binding.shard_id
        || document.total_shards != binding.total_shards
        || document.workers != binding.workers
        || document.states.len() != binding.workers
    {
        return None;
    }
    if document
        .states
        .iter()
        .flatten()
        .any(|state| !valid_state(state))
    {
        return None;
    }
    Some(document.states)
}

fn empty(binding: &Binding) -> Vec<Option<Value>> {
    vec![None; binding.workers]
}

/// Read the persisted states. Every failure returns an all-empty result.
pub fn load(dir: &Path, binding: &Binding) -> Vec<Option<Value>> {
    read_limited(&dir.join(FILE_NAME))
        .ok()
        .and_then(|bytes| decode(binding, &bytes))
        .unwrap_or_else(|| empty(binding))
}

fn read_limited(path: &Path) -> io::Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.len() > MAX_FILE_BYTES as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected persisted state object",
        ));
    }
    let mut bytes = Vec::new();
    fs::File::open(path)?
        .take(MAX_FILE_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_FILE_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "persisted state too large",
        ));
    }
    Ok(bytes)
}

/// Atomically publish `bytes` as `dir/name`: private temporary file, fsync,
/// rename, then fsync of the directory (best effort).
pub fn write_atomic(dir: &Path, name: &str, temp: &str, bytes: &[u8]) -> io::Result<()> {
    let temp_path = dir.join(temp);
    // A stale temporary (or a planted link) is removed, never followed.
    match fs::remove_file(&temp_path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let publish = |options: &fs::OpenOptions| -> io::Result<()> {
        let mut file = options.open(&temp_path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temp_path, dir.join(name))
    };
    let result = publish(&options);
    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
        return result;
    }
    if let Ok(handle) = fs::File::open(dir) {
        let _ = handle.sync_all();
    }
    Ok(())
}

/// Encode and atomically store. Oversize documents are rejected before I/O.
pub fn store(dir: &Path, binding: &Binding, states: &[Option<Value>]) -> io::Result<()> {
    store_at(dir, binding, states, 0)
}

/// The epoch recorded in the stored document (0 when absent or unreadable).
fn stored_epoch(dir: &Path) -> u64 {
    read_limited(&dir.join(FILE_NAME))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<StoredEpoch>(&bytes).ok())
        .map_or(0, |stored| stored.epoch)
}

/// Store as the Gateway of ownership epoch `epoch`. While two Gateways overlap during an
/// update, the older one may still flush at its shutdown; a file already written by a higher
/// epoch is left alone so a stale snapshot never replaces a newer one. The check and the
/// rename are not one atomic step across processes: this closes the common ordering, it is
/// not a lock. The temporary name carries the process id, so two writers never share it.
pub fn store_at(
    dir: &Path,
    binding: &Binding,
    states: &[Option<Value>],
    epoch: u64,
) -> io::Result<()> {
    let bytes = encode_at(binding, states, epoch).map_err(|error| match error {
        EncodeError::TooLarge => io::Error::new(io::ErrorKind::InvalidInput, "state too large"),
        EncodeError::Invalid => io::Error::new(io::ErrorKind::InvalidInput, "state invalid"),
    })?;
    if stored_epoch(dir) > epoch {
        return Ok(());
    }
    remove_dead_temporaries(dir);
    let temp = format!("{TEMP_NAME}.{}", std::process::id());
    write_atomic(dir, FILE_NAME, &temp, &bytes)
}

/// Temporaries of writers that no longer exist: the pre-epoch fixed name, and
/// `<name>.<pid>` for a pid that is gone (a crashed writer). A live writer's temporary is
/// never touched.
fn remove_dead_temporaries(dir: &Path) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let dead = if name == TEMP_NAME {
            true
        } else if let Some(pid) = name
            .strip_prefix(TEMP_NAME)
            .and_then(|rest| rest.strip_prefix('.'))
            .and_then(|pid| pid.parse::<u32>().ok())
        {
            pid != std::process::id() && !Path::new(&format!("/proc/{pid}")).exists()
        } else {
            false
        };
        if dead {
            let _ = fs::remove_file(entry.path());
        }
    }
}

struct Pending {
    latest: Vec<Option<Value>>,
    version: u64,
    saved: u64,
    last_write: Option<Instant>,
    oversize_warned: bool,
}

/// Change-driven, throttled, serialized writer of the confirmed states.
pub struct Persister {
    dir: PathBuf,
    binding: Binding,
    pending: StdMutex<Pending>,
    // Serializes writers within this process.
    writing: Mutex<()>,
    epoch: u64,
}

impl Persister {
    /// Load any valid persisted states (blocking read on a worker thread).
    pub async fn open(dir: PathBuf, binding: Binding) -> (Self, Vec<Option<Value>>) {
        let loaded = {
            let dir = dir.clone();
            tokio::task::spawn_blocking(move || load(&dir, &binding))
                .await
                .unwrap_or_else(|_| empty(&binding))
        };
        // The stamp this writer puts on its saves: the time it started, in nanoseconds, but
        // always above whatever the file already carries (a clock that stepped back, or a tie,
        // must not leave a new writer "older" than the file it is about to take over). A writer
        // that started later therefore always outranks the earlier one, whichever layout, botd
        // incarnation or ownership epoch either of them had.
        let stamp = {
            let dir = dir.clone();
            tokio::task::spawn_blocking(move || {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |since| {
                        u64::try_from(since.as_nanos()).unwrap_or(u64::MAX)
                    });
                now.max(stored_epoch(&dir).saturating_add(1))
            })
            .await
            .unwrap_or(1)
        };
        let persister = Self {
            dir,
            binding,
            pending: StdMutex::new(Pending {
                latest: loaded.clone(),
                version: 0,
                saved: 0,
                last_write: None,
                oversize_warned: false,
            }),
            writing: Mutex::new(()),
            epoch: stamp,
        };
        (persister, loaded)
    }

    /// Record a confirmed state. Only a real change marks the file dirty.
    pub fn observe(&self, index: usize, state: &Value) {
        let mut pending = self.pending.lock().unwrap();
        let Some(slot) = pending.latest.get_mut(index) else {
            return;
        };
        if slot.as_ref() == Some(state) {
            return;
        }
        *slot = Some(state.clone());
        pending.version += 1;
    }

    pub fn is_dirty(&self) -> bool {
        let pending = self.pending.lock().unwrap();
        pending.version != pending.saved
    }

    /// Write if dirty. Unless `force`, honor [`MIN_WRITE_INTERVAL`]. Failure is
    /// a warning; the dirty mark is kept so a later flush retries.
    pub async fn flush(&self, force: bool) {
        let _writing = self.writing.lock().await;
        let (states, version) = {
            let mut pending = self.pending.lock().unwrap();
            if pending.version == pending.saved {
                return;
            }
            if !force
                && pending
                    .last_write
                    .is_some_and(|at| at.elapsed() < MIN_WRITE_INTERVAL)
            {
                return;
            }
            pending.last_write = Some(Instant::now());
            (pending.latest.clone(), pending.version)
        };
        let dir = self.dir.clone();
        let binding = self.binding;
        let epoch = self.epoch;
        let result = tokio::task::spawn_blocking(move || store_at(&dir, &binding, &states, epoch))
            .await
            .unwrap_or_else(|_| Err(io::Error::other("persist task failed")));
        let mut pending = self.pending.lock().unwrap();
        match result {
            Ok(()) => {
                // A change observed during the write bumped `version` past
                // the captured one, so it stays pending.
                pending.saved = version;
                pending.oversize_warned = false;
            }
            Err(error) if error.kind() == io::ErrorKind::InvalidInput => {
                // Retrying an oversize document is pointless until it changes.
                pending.saved = version;
                if !pending.oversize_warned {
                    pending.oversize_warned = true;
                    eprintln!("警告: Coreの確認済み状態が大きすぎるため永続化しません");
                }
            }
            Err(error) => {
                eprintln!(
                    "警告: Coreの確認済み状態を保存できません (kind={:?})",
                    error.kind()
                );
            }
        }
    }
}
