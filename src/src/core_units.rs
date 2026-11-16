//! botd's supervision of the Core processes of one Shard in the separated layout (stage 1-C
//! of docs/gateway-core-separation-design.md).
//!
//! botd starts the Cores (`src --core-listen <socket> --core-persist`), hands each its
//! capability secret on stdin, restarts one that exits (bounded exponential backoff) and
//! stops them after the Gateway. A Core exits by itself when its stdin closes, so Cores
//! cannot outlive a botd that died: the pipe is the leash. The Gateway is not a parent of
//! the Cores and does not restart them; it only reconnects (see `Manager`).
//!
//! The Bot token is never given to a Core: its environment is cleared and holds only the
//! Core identity.

use crate::core_attach::{SLOTS, identity, prepare_dir, renew_secret, secret_path, socket_path};
use crate::protocol::{BotId, Shard};
use crate::redact::Secret;
use crate::secret_file::{SECRET_BYTES, load_secret_file, wipe};
use std::io::{Read, Seek, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::time::{Duration, Instant};

const BASE_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// A Core that stays up this long starts over with the shortest backoff.
const STABLE_AFTER: Duration = Duration::from_secs(60);
const LOG_MAX_BYTES: u64 = 4 * 1024 * 1024;
/// How often a running Core log is checked for size.
const LOG_CHECK_EVERY: Duration = Duration::from_secs(60);

/// Keeps a log that processes append to from growing without bound: when it is over
/// `max_bytes`, its content is copied to `<path>.1` (replacing an older copy) and the file is
/// emptied in place. The writers keep their descriptors (they append), so nothing is lost
/// except what was already older than one rotation. Returns whether it rotated. A symlink or
/// non-file at either path is left alone.
pub fn rotate_in_place(path: &Path, max_bytes: u64) -> std::io::Result<bool> {
    let meta = match std::fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    if !meta.file_type().is_file() || meta.len() <= max_bytes {
        return Ok(false);
    }
    let mut older = path.as_os_str().to_owned();
    older.push(".1");
    let older = PathBuf::from(older);
    match std::fs::symlink_metadata(&older) {
        Ok(meta) if meta.file_type().is_file() => std::fs::remove_file(&older)?,
        Ok(_) => return Ok(false),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    // One descriptor for both the copy and the truncation, and it must be the file that was
    // checked (same device and inode): a path swapped in since is never touched.
    let mut live = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?;
    let opened = live.metadata()?;
    if opened.dev() != meta.dev() || opened.ino() != meta.ino() || !opened.file_type().is_file() {
        return Ok(false);
    }
    let measured = opened.len();
    let mut copy = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&older)?;
    std::io::copy(&mut Read::by_ref(&mut live).take(measured), &mut copy)?;
    // Lines the writers appended while the copy ran: copy those too (a few rounds), then empty
    // the file. A writer that never pauses must not be able to starve the rotation, or the file
    // would grow without bound - so the last few bytes written between the final copy and the
    // truncation can be lost (a window of microseconds). Bounded size wins over completeness.
    for _ in 0..4 {
        let end = live.metadata()?.len();
        let position = live.stream_position()?;
        if end <= position {
            break;
        }
        // Copy exactly what is there now; a writer that keeps ahead cannot keep this loop.
        std::io::copy(&mut Read::by_ref(&mut live).take(end - position), &mut copy)?;
    }
    live.set_len(0)?;
    Ok(true)
}

/// Opens a private append-only file without following a replaced symlink.
pub fn open_append_private(path: &Path) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::PermissionsExt;
    unsafe extern "C" {
        fn geteuid() -> u32;
    }
    let mut options = std::fs::OpenOptions::new();
    options.read(true).append(true).mode(0o600);
    let (file, before) = match options.create_new(true).open(path) {
        Ok(file) => (file, None),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let before = std::fs::symlink_metadata(path)?;
            if !before.is_file() {
                return Err(std::io::Error::other("not a regular file"));
            }
            (options.create_new(false).open(path)?, Some(before))
        }
        Err(error) => return Err(error),
    };
    let meta = file.metadata()?;
    if !meta.is_file()
        || meta.uid() != unsafe { geteuid() }
        || meta.nlink() != 1
        || before
            .as_ref()
            .is_some_and(|old| old.dev() != meta.dev() || old.ino() != meta.ino())
    {
        return Err(std::io::Error::other("unsafe file"));
    }
    if meta.mode() & 0o7777 != 0o600 {
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(file)
}

struct Slot {
    child: Option<Child>,
    /// Held so the Core sees EOF (and exits) when botd closes or loses it.
    stdin: Option<ChildStdin>,
    started_at: Option<Instant>,
    failures: u32,
    next_start: Option<Instant>,
}

impl Slot {
    fn empty() -> Self {
        Self {
            child: None,
            stdin: None,
            started_at: None,
            failures: 0,
            next_start: None,
        }
    }
}

pub struct CoreUnits {
    dir: PathBuf,
    bot_id: BotId,
    shard: Shard,
    slots: Vec<Slot>,
    prepared: bool,
    log_checked: Option<Instant>,
    epoch_floor: u64,
}

/// What an operator may be told about a Core, never a path, secret or argument.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CoreStatus {
    pub running: bool,
    pub failures: u32,
}

impl CoreUnits {
    pub fn new(dir: PathBuf, bot_id: BotId, shard: Shard) -> Self {
        Self {
            dir,
            bot_id,
            shard,
            slots: (0..SLOTS).map(|_| Slot::empty()).collect(),
            prepared: false,
            log_checked: None,
            epoch_floor: 0,
        }
    }

    pub fn set_epoch_floor(&mut self, floor: u64) {
        self.epoch_floor = self.epoch_floor.max(floor);
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn status(&self) -> Vec<CoreStatus> {
        self.slots
            .iter()
            .map(|slot| CoreStatus {
                running: slot.child.is_some(),
                failures: slot.failures,
            })
            .collect()
    }

    /// Starts every Core that is not running. The first call also prepares the private
    /// directory and makes a new secret (a secret never outlives its Cores). Later calls
    /// start only the missing ones, with the secret already in place.
    pub fn ensure(&mut self, program: &Path) -> std::io::Result<()> {
        if !self.prepared {
            prepare_dir(&self.dir).map_err(|_| std::io::Error::other("core directory"))?;
            renew_secret(&self.dir).map_err(|_| std::io::Error::other("core secret"))?;
            self.prepared = true;
        }
        let now = Instant::now();
        for index in 0..self.slots.len() {
            if self.slots[index].child.is_none() {
                self.start_slot(index, program, now)?;
            }
        }
        Ok(())
    }

    fn start_slot(&mut self, index: usize, program: &Path, now: Instant) -> std::io::Result<()> {
        let secret = load_secret_file(&secret_path(&self.dir))
            .map_err(|_| std::io::Error::other("core secret"))?;
        let identity = serde_json::to_string(&identity(self.bot_id, self.shard))
            .map_err(|_| std::io::Error::other("core identity"))?;
        let log = open_log(&self.dir, index);
        let (out, err) = match log.and_then(|file| file.try_clone().ok().map(|c| (file, c))) {
            Some((a, b)) => (Stdio::from(a), Stdio::from(b)),
            None => (Stdio::null(), Stdio::null()),
        };
        let mut command = Command::new(program);
        command
            .arg("--core-listen")
            .arg(socket_path(&self.dir, index))
            .arg("--core-persist");
        if self.epoch_floor > 0 {
            command
                .arg("--epoch-floor")
                .arg(self.epoch_floor.to_string());
        }
        command
            .env_clear()
            .env("BOT_WORKER_IPC_IDENTITY", identity)
            .stdin(Stdio::piped())
            .stdout(out)
            .stderr(err)
            // Terminal Ctrl-C targets botd's foreground group; botd stops Cores itself.
            .process_group(0);
        let mut child = command.spawn()?;
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| std::io::Error::other("core stdin"))?;
        if write_secret(&mut stdin, &secret).is_err() {
            let _ = child.kill();
            let _ = child.wait();
            return Err(std::io::Error::other("core secret delivery"));
        }
        let slot = &mut self.slots[index];
        slot.child = Some(child);
        slot.stdin = Some(stdin);
        slot.started_at = Some(now);
        slot.next_start = None;
        Ok(())
    }

    /// Reaps exited Cores and restarts due ones. `wanted` false (the Shard is stopped
    /// on purpose) reaps but never restarts.
    pub fn poll(&mut self, program: &Path, wanted: bool) {
        let now = Instant::now();
        if self
            .log_checked
            .is_none_or(|at| now.duration_since(at) >= LOG_CHECK_EVERY)
        {
            self.log_checked = Some(now);
            for index in 0..self.slots.len() {
                let _ = rotate_in_place(&self.dir.join(format!("core-{index}.log")), LOG_MAX_BYTES);
            }
        }
        for index in 0..self.slots.len() {
            let exited = matches!(
                self.slots[index].child.as_mut().map(Child::try_wait),
                Some(Ok(Some(_)) | Err(_))
            );
            if exited {
                let slot = &mut self.slots[index];
                slot.child = None;
                slot.stdin = None;
                let lived = slot.started_at.map_or(Duration::ZERO, |at| now - at);
                slot.failures = if lived >= STABLE_AFTER {
                    1
                } else {
                    slot.failures.saturating_add(1)
                };
                let delay = BASE_BACKOFF
                    .saturating_mul(1u32 << slot.failures.saturating_sub(1).min(8))
                    .min(MAX_BACKOFF);
                slot.next_start = Some(now + delay);
                eprintln!("Coreが終了しました（slot {index}）。再起動を予定します");
            }
            let due = self.slots[index].child.is_none()
                && wanted
                && self.slots[index].next_start.is_some_and(|at| now >= at);
            if due && self.start_slot(index, program, now).is_err() {
                let slot = &mut self.slots[index];
                slot.failures = slot.failures.saturating_add(1);
                slot.next_start = Some(now + MAX_BACKOFF.min(BASE_BACKOFF * 4));
            }
        }
    }

    /// Asks every Core to stop by closing its stdin. Returns without waiting.
    pub fn begin_stop(&mut self) {
        for slot in &mut self.slots {
            slot.stdin = None;
            slot.next_start = None;
        }
    }

    /// True while any Core process is still alive (reaping the ones that exited).
    pub fn any_alive(&mut self) -> bool {
        let mut alive = false;
        for slot in &mut self.slots {
            if let Some(child) = slot.child.as_mut() {
                if matches!(child.try_wait(), Ok(None)) {
                    alive = true;
                } else {
                    slot.child = None;
                }
            }
        }
        alive
    }

    /// Last resort after the stop deadline. Kills only handles botd itself holds.
    pub fn kill(&mut self) {
        for slot in &mut self.slots {
            if let Some(child) = slot.child.as_mut() {
                let _ = child.kill();
                let _ = child.wait();
            }
            slot.child = None;
            slot.stdin = None;
        }
    }

    /// Removes the sockets and the secret once no Core is running.
    pub fn cleanup(&mut self) {
        if !self.any_alive() {
            crate::core_attach::cleanup(&self.dir);
            self.prepared = false;
        }
    }
}

/// 64 hex digits and a newline from a fixed buffer that is wiped right after the write.
fn write_secret(
    stdin: &mut ChildStdin,
    secret: &Secret<[u8; SECRET_BYTES]>,
) -> std::io::Result<()> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut line = [0u8; SECRET_BYTES * 2 + 1];
    for (index, byte) in secret.expose_secret().iter().enumerate() {
        line[index * 2] = HEX[usize::from(byte >> 4)];
        line[index * 2 + 1] = HEX[usize::from(byte & 0x0f)];
    }
    line[SECRET_BYTES * 2] = b'\n';
    let result = stdin.write_all(&line).and_then(|()| stdin.flush());
    wipe(&mut line);
    result
}

/// `core-<slot>.log` beside the sockets: append-only, 0600, restarted when oversized.
/// Falls back to no log (null) on anything unexpected rather than failing the start.
fn open_log(dir: &Path, index: usize) -> Option<std::fs::File> {
    let path = dir.join(format!("core-{index}.log"));
    match std::fs::symlink_metadata(&path) {
        Ok(meta) if !meta.file_type().is_file() => return None,
        Ok(meta) if meta.len() > LOG_MAX_BYTES => {
            let _ = std::fs::remove_file(&path);
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return None,
    }
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(&path)
        .ok()?;
    writeln!(file, "--- core slot {index} start ---").ok()?;
    Some(file)
}

#[cfg(test)]
mod private_append_tests {
    use super::open_append_private;
    use std::os::unix::fs::{PermissionsExt, symlink};

    #[test]
    fn refuses_symlink_and_restricts_existing_file() {
        let dir = std::env::temp_dir().join(format!("core-private-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let target = dir.join("target");
        let link = dir.join("link");
        std::fs::write(&target, b"").unwrap();
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o644)).unwrap();
        symlink(&target, &link).unwrap();
        assert!(open_append_private(&link).is_err());
        open_append_private(&target).unwrap();
        assert_eq!(
            std::fs::metadata(&target).unwrap().permissions().mode() & 0o777,
            0o600
        );
        std::fs::remove_file(link).unwrap();
        std::fs::remove_file(target).unwrap();
        std::fs::remove_dir(dir).unwrap();
    }
    #[test]
    fn refuses_hardlink_and_fifo() {
        let dir = std::env::temp_dir().join(format!(
            "core-private-extra-{}-{}",
            std::process::id(),
            getrandom::u64().unwrap()
        ));
        std::fs::create_dir(&dir).unwrap();
        let target = dir.join("target");
        let link = dir.join("hardlink");
        std::fs::write(&target, b"").unwrap();
        std::fs::hard_link(&target, &link).unwrap();
        assert!(open_append_private(&target).is_err());
        let fifo = dir.join("fifo");
        if std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .is_ok_and(|s| s.success())
        {
            assert!(open_append_private(&fifo).is_err());
        }
        std::fs::remove_dir_all(dir).unwrap();
    }
}
