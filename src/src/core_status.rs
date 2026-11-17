// Status files live on tmpfs: no file data is written to a disk-backed filesystem.
// Tmpfs pages can be swapped under memory pressure; noswap is not enforced. This
// provides zero file writeback in normal operation, not an absolute zero.
use std::time::{Duration, Instant};

pub const RECORD_LEN: usize = 64;

#[cfg(unix)]
pub fn try_boot_ms() -> Option<u64> {
    let mut time = std::mem::MaybeUninit::<libc::timespec>::uninit();
    // SAFETY: clock_gettime initializes the writable timespec on success.
    if unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, time.as_mut_ptr()) } != 0 {
        return None;
    }
    // SAFETY: a successful clock_gettime initialized time.
    let time = unsafe { time.assume_init() };
    Some(
        (time.tv_sec as u64)
            .saturating_mul(1000)
            .saturating_add(time.tv_nsec as u64 / 1_000_000),
    )
}

#[cfg(unix)]
pub fn boot_ms() -> u64 {
    try_boot_ms().unwrap_or(0)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StatusRecord {
    pub pending: u32,
    pub oldest_age_s: u32,
    pub warned: u32,
    pub threshold_s: u32,
    pub boot_ms: u64,
    pub pid: u32,
    pub generation: u64,
    pub seq: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum DecodeError {
    WrongLength,
    BadMagic,
    BadCrc,
}

impl StatusRecord {
    pub fn encode(&self) -> [u8; RECORD_LEN] {
        let mut bytes = [0; RECORD_LEN];
        bytes[..4].copy_from_slice(b"CSP2");
        bytes[4..8].copy_from_slice(&self.pending.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.oldest_age_s.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.warned.to_le_bytes());
        bytes[16..20].copy_from_slice(&self.threshold_s.to_le_bytes());
        bytes[20..28].copy_from_slice(&self.boot_ms.to_le_bytes());
        bytes[28..32].copy_from_slice(&self.pid.to_le_bytes());
        bytes[32..40].copy_from_slice(&self.generation.to_le_bytes());
        bytes[40..48].copy_from_slice(&self.seq.to_le_bytes());
        let crc = crc32(&bytes[..60]);
        bytes[60..].copy_from_slice(&crc.to_le_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        if bytes.len() != RECORD_LEN {
            return Err(DecodeError::WrongLength);
        }
        if &bytes[..4] != b"CSP2" {
            return Err(DecodeError::BadMagic);
        }
        if crc32(&bytes[..60]) != u32::from_le_bytes(bytes[60..64].try_into().unwrap()) {
            return Err(DecodeError::BadCrc);
        }
        Ok(Self {
            pending: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            oldest_age_s: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            warned: u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
            threshold_s: u32::from_le_bytes(bytes[16..20].try_into().unwrap()),
            boot_ms: u64::from_le_bytes(bytes[20..28].try_into().unwrap()),
            pid: u32::from_le_bytes(bytes[28..32].try_into().unwrap()),
            generation: u64::from_le_bytes(bytes[32..40].try_into().unwrap()),
            seq: u64::from_le_bytes(bytes[40..48].try_into().unwrap()),
        })
    }
}

pub fn crc32(data: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xEDB8_8320 & (0u32.wrapping_sub(crc & 1)));
        }
    }
    !crc
}

pub struct WritePolicy {
    check_every: Duration,
    heartbeat_every: Duration,
    last_written: Option<((u32, u32, u32, u32), Instant)>,
}

impl Default for WritePolicy {
    fn default() -> Self {
        Self::new(Duration::from_secs(5), Duration::from_secs(10))
    }
}

impl WritePolicy {
    pub fn new(check_every: Duration, heartbeat_every: Duration) -> Self {
        Self {
            check_every,
            heartbeat_every,
            last_written: None,
        }
    }

    pub fn should_write(&mut self, now: Instant, current: &StatusRecord) -> bool {
        let snapshot = (
            current.pending,
            current.oldest_age_s,
            current.warned,
            current.threshold_s,
        );
        let write = match self.last_written {
            None => true,
            Some((last, time)) => {
                let elapsed = now.checked_duration_since(time).unwrap_or_default();
                elapsed >= self.check_every
                    && if current.pending > 0 {
                        snapshot != last
                    } else {
                        elapsed >= self.heartbeat_every
                    }
            }
        };
        if write {
            self.last_written = Some((snapshot, now));
        }
        write
    }
}

#[cfg(unix)]
mod file_io {
    use super::{DecodeError, RECORD_LEN, StatusRecord};
    use std::{
        fs::{self, File, Metadata, OpenOptions},
        io::{self, Read, Seek, SeekFrom},
        os::fd::AsRawFd,
        os::unix::ffi::OsStrExt,
        os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt, PermissionsExt},
        path::Path,
    };

    unsafe extern "C" {
        fn geteuid() -> u32;
    }
    // Linux flags prevent symlink traversal and blocking on a substituted FIFO.
    const SAFE_FLAGS: i32 = libc::O_NOFOLLOW | libc::O_NONBLOCK;

    fn safe(meta: &Metadata) -> bool {
        meta.is_file() && meta.uid() == unsafe { geteuid() } && meta.nlink() == 1
    }
    fn same(a: &Metadata, b: &Metadata) -> bool {
        a.dev() == b.dev() && a.ino() == b.ino()
    }

    pub fn is_tmpfs(dir: &Path) -> bool {
        let Ok(path) = std::ffi::CString::new(dir.as_os_str().as_bytes()) else {
            return false;
        };
        let mut info = std::mem::MaybeUninit::<libc::statfs>::uninit();
        // SAFETY: path is NUL-terminated and info is writable; success initializes it.
        if unsafe { libc::statfs(path.as_ptr(), info.as_mut_ptr()) } != 0 {
            return false;
        }
        // SAFETY: statfs succeeded and initialized info.
        unsafe { info.assume_init() }.f_type as u64 == 0x0102_1994
    }

    pub fn file_is_tmpfs(file: &File) -> bool {
        let mut info = std::mem::MaybeUninit::<libc::statfs>::uninit();
        // SAFETY: the descriptor is valid and success initializes info.
        if unsafe { libc::fstatfs(file.as_raw_fd(), info.as_mut_ptr()) } != 0 {
            return false;
        }
        unsafe { info.assume_init() }.f_type as u64 == 0x0102_1994
    }

    pub struct StatusWriter {
        file: File,
        generation: u64,
        seq: u64,
    }
    impl StatusWriter {
        // The unchecked writer exists only for debug integration tests.
        #[cfg(debug_assertions)]
        pub fn open(path: &Path) -> Option<StatusWriter> {
            Self::open_checked(path, false)
        }

        pub fn open_checked(path: &Path, require_tmpfs: bool) -> Option<StatusWriter> {
            Self::open_checked_inner(path, require_tmpfs, true)
        }

        fn open_checked_inner(
            path: &Path,
            require_tmpfs: bool,
            check_parent: bool,
        ) -> Option<StatusWriter> {
            if require_tmpfs && check_parent && !is_tmpfs(path.parent()?) {
                return None;
            }
            let generation = loop {
                let value = getrandom::u64().ok()?;
                if value != 0 {
                    break value;
                }
            };
            let mut options = OpenOptions::new();
            options
                .read(true)
                .write(true)
                .mode(0o600)
                .custom_flags(SAFE_FLAGS);
            let (file, before) = match options.create_new(true).open(path) {
                Ok(file) => (file, None),
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                    let meta = fs::symlink_metadata(path).ok()?;
                    if !safe(&meta) {
                        return None;
                    }
                    (options.create_new(false).open(path).ok()?, Some(meta))
                }
                Err(_) => return None,
            };
            if require_tmpfs && !file_is_tmpfs(&file) {
                if before.is_none() {
                    // Remove only the inode we created, not a replacement at the path.
                    if let (Ok(opened), Ok(current)) = (file.metadata(), fs::symlink_metadata(path))
                        && same(&opened, &current)
                    {
                        let _ = fs::remove_file(path);
                    }
                }
                return None;
            }
            let meta = file.metadata().ok()?;
            if !safe(&meta) || before.as_ref().is_some_and(|old| !same(old, &meta)) {
                return None;
            }
            if meta.mode() & 0o7777 != 0o600 {
                file.set_permissions(fs::Permissions::from_mode(0o600))
                    .ok()?;
            }
            if meta.len() != RECORD_LEN as u64 {
                file.set_len(RECORD_LEN as u64).ok()?;
            }
            Some(Self {
                file,
                generation,
                seq: 0,
            })
        }

        pub fn write(&mut self, record: &StatusRecord) {
            self.seq = self.seq.wrapping_add(1);
            let mut record = *record;
            record.generation = self.generation;
            record.seq = self.seq;
            let _ = self.file.write_all_at(&record.encode(), 0);
        }
    }

    #[cfg(all(test, debug_assertions))]
    mod guard_tests {
        use super::*;

        #[test]
        fn descriptor_check_rejects_non_tmpfs_even_if_parent_check_is_bypassed() {
            let dir = std::env::temp_dir();
            if is_tmpfs(&dir) {
                return; // This test needs a disk-backed temporary directory.
            }
            let path = dir.join(format!(
                "core-status-guard-{}-{}",
                std::process::id(),
                getrandom::u64().unwrap()
            ));
            assert!(!path.exists());
            assert!(StatusWriter::open_checked_inner(&path, true, false).is_none());
            assert!(!path.exists());
        }

        #[test]
        fn safe_flags_are_platform_constants() {
            assert_eq!(SAFE_FLAGS, libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    pub enum StatusRead {
        Fresh {
            record: StatusRecord,
            status_age_s: u64,
        },
        Missing,
        Unreadable,
        Corrupt,
        ClockUnavailable,
    }

    pub fn read_status(path: &Path) -> StatusRead {
        read_status_at(path, super::try_boot_ms)
    }

    pub(super) fn read_status_at(path: &Path, now_boot_ms: impl Fn() -> Option<u64>) -> StatusRead {
        if !path.parent().is_some_and(is_tmpfs) {
            return StatusRead::Unreadable;
        }
        let before = match fs::symlink_metadata(path) {
            Ok(meta) => meta,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return StatusRead::Missing,
            Err(_) => return StatusRead::Unreadable,
        };
        if !safe(&before) || before.len() != RECORD_LEN as u64 {
            return StatusRead::Unreadable;
        }
        let mut file = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_RDONLY | SAFE_FLAGS)
            .open(path)
        {
            Ok(file) => file,
            Err(_) => return StatusRead::Unreadable,
        };
        if !file_is_tmpfs(&file) {
            return StatusRead::Unreadable;
        }
        let meta = match file.metadata() {
            Ok(meta) => meta,
            Err(_) => return StatusRead::Unreadable,
        };
        if !safe(&meta) || meta.len() != RECORD_LEN as u64 || !same(&before, &meta) {
            return StatusRead::Unreadable;
        }
        for attempt in 0..2 {
            let mut bytes = [0; RECORD_LEN];
            if file.seek(SeekFrom::Start(0)).is_err() || file.read_exact(&mut bytes).is_err() {
                return StatusRead::Unreadable;
            }
            match StatusRecord::decode(&bytes) {
                Ok(record) => {
                    let Some(now_ms) = now_boot_ms() else {
                        return StatusRead::ClockUnavailable;
                    };
                    if record.boot_ms > now_ms.saturating_add(1000) {
                        return StatusRead::Corrupt;
                    }
                    return StatusRead::Fresh {
                        status_age_s: now_ms.saturating_sub(record.boot_ms) / 1000,
                        record,
                    };
                }
                Err(DecodeError::BadCrc) if attempt == 0 => continue,
                Err(_) => return StatusRead::Corrupt,
            }
        }
        StatusRead::Corrupt
    }
}
#[cfg(unix)]
pub use file_io::{StatusRead, StatusWriter, file_is_tmpfs, is_tmpfs, read_status};

#[cfg(test)]
mod tests {
    use super::*;
    fn record() -> StatusRecord {
        StatusRecord {
            pending: 1,
            oldest_age_s: 2,
            warned: 3,
            threshold_s: 4,
            boot_ms: 5000,
            pid: 6,
            generation: 7,
            seq: 8,
        }
    }
    #[test]
    fn encoding_and_crc() {
        assert_eq!(crc32(b"123456789"), 0xCBF43926);
        assert_eq!(crc32(b""), 0);
        let bytes = record().encode();
        assert_eq!(StatusRecord::decode(&bytes), Ok(record()));
        assert_eq!(
            StatusRecord::decode(&bytes[..63]),
            Err(DecodeError::WrongLength)
        );
        let mut bad = bytes;
        bad[0] ^= 1;
        assert_eq!(StatusRecord::decode(&bad), Err(DecodeError::BadMagic));
        for i in 0..RECORD_LEN {
            let mut bad = bytes;
            bad[i] ^= 1;
            assert!(StatusRecord::decode(&bad).is_err(), "byte {i}");
        }
    }
    #[test]
    fn write_policy_ticks() {
        let base = Instant::now();
        let mut p = WritePolicy::default();
        let mut r = record();
        for (s, pending, age, expected) in [
            (0, 1, 2, true),
            (4, 1, 3, false),
            (5, 1, 3, true),
            (9, 1, 4, false),
            (10, 1, 4, true),
            (15, 1, 4, false),
            (19, 0, 0, false),
            (20, 0, 0, true),
            (25, 0, 0, false),
            (30, 0, 0, true),
        ] {
            r.pending = pending;
            r.oldest_age_s = age;
            assert_eq!(
                p.should_write(base + Duration::from_secs(s), &r),
                expected,
                "{s}"
            );
        }
        let mut p = WritePolicy::new(Duration::from_secs(5), Duration::from_secs(2));
        r.pending = 0;
        assert!(p.should_write(base, &r));
        assert!(!p.should_write(base + Duration::from_secs(2), &r));
        assert!(p.should_write(base + Duration::from_secs(5), &r));
    }
    #[cfg(unix)]
    mod files {
        use super::*;
        use std::{
            fs,
            os::unix::fs::{MetadataExt, symlink},
            path::PathBuf,
            sync::atomic::{AtomicU64, Ordering},
        };
        static NEXT: AtomicU64 = AtomicU64::new(0);
        struct Dir(PathBuf);
        impl Dir {
            fn new() -> Self {
                loop {
                    let p = PathBuf::from("/dev/shm").join(format!(
                        "core-status-{}-{}",
                        std::process::id(),
                        NEXT.fetch_add(1, Ordering::Relaxed)
                    ));
                    match fs::create_dir(&p) {
                        Ok(()) => return Self(p),
                        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                        Err(e) => panic!("{e}"),
                    }
                }
            }
        }
        impl Drop for Dir {
            fn drop(&mut self) {
                let _ = fs::remove_dir_all(&self.0);
            }
        }
        unsafe extern "C" {
            fn mkfifo(path: *const std::ffi::c_char, mode: u32) -> i32;
        }
        #[test]
        fn reader_rejects_non_tmpfs_parent() {
            let dir = std::env::temp_dir();
            if is_tmpfs(&dir) {
                return;
            }
            let path = dir.join(format!(
                "core-status-reader-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::write(&path, record().encode()).unwrap();
            assert_eq!(
                file_io::read_status_at(&path, || Some(9000)),
                StatusRead::Unreadable
            );
            fs::remove_file(path).unwrap();
        }
        #[test]
        fn writer_and_reader() {
            use std::os::unix::ffi::OsStrExt;
            let d = Dir::new();
            let path = d.0.join("status");
            assert_eq!(
                file_io::read_status_at(&path, || Some(9000)),
                StatusRead::Missing
            );
            let mut writer = StatusWriter::open(&path).unwrap();
            let before = fs::metadata(&path).unwrap();
            assert_eq!(before.len(), 64);
            assert_eq!(before.mode() & 0o777, 0o600);
            writer.write(&record());
            let first = match file_io::read_status_at(&path, || Some(9000)) {
                StatusRead::Fresh { record, .. } => record,
                other => panic!("unexpected status: {other:?}"),
            };
            assert_ne!(first.generation, 0);
            writer.write(&record());
            let second = match file_io::read_status_at(&path, || Some(9000)) {
                StatusRead::Fresh { record, .. } => record,
                other => panic!("unexpected status: {other:?}"),
            };
            assert_eq!(second.generation, first.generation);
            assert_eq!(second.seq, first.seq + 1);
            let after = fs::metadata(&path).unwrap();
            assert_eq!(
                (before.dev(), before.ino(), after.len()),
                (after.dev(), after.ino(), 64)
            );
            assert_eq!(
                file_io::read_status_at(&path, || Some(9000)),
                StatusRead::Fresh {
                    record: second,
                    status_age_s: 4
                }
            );
            assert_eq!(
                file_io::read_status_at(&path, || Some(0)),
                StatusRead::Corrupt
            );
            assert_eq!(
                file_io::read_status_at(&path, || None),
                StatusRead::ClockUnavailable
            );
            // Exactly 1000 ms in the future is accepted with age zero.
            assert_eq!(
                file_io::read_status_at(&path, || Some(4000)),
                StatusRead::Fresh {
                    record: second,
                    status_age_s: 0
                }
            );
            assert_eq!(
                file_io::read_status_at(&path, || Some(3999)),
                StatusRead::Corrupt
            );
            let mut bytes = fs::read(&path).unwrap();
            bytes[10] ^= 1;
            fs::write(&path, bytes).unwrap();
            assert_eq!(
                file_io::read_status_at(&path, || Some(9000)),
                StatusRead::Corrupt
            );
            let link = d.0.join("link");
            symlink(&path, &link).unwrap();
            assert!(StatusWriter::open(&link).is_none());
            assert_eq!(
                file_io::read_status_at(&link, || Some(0)),
                StatusRead::Unreadable
            );
            let fifo = d.0.join("fifo");
            let c = std::ffi::CString::new(fifo.as_os_str().as_bytes()).unwrap();
            assert_eq!(unsafe { mkfifo(c.as_ptr(), 0o600) }, 0);
            assert!(StatusWriter::open(&fifo).is_none());
            assert_eq!(
                file_io::read_status_at(&fifo, || Some(0)),
                StatusRead::Unreadable
            );
            let short = d.0.join("short");
            fs::write(&short, b"x").unwrap();
            assert_eq!(
                file_io::read_status_at(&short, || Some(0)),
                StatusRead::Unreadable
            );
            assert!(StatusWriter::open(&short).is_some());
            assert_eq!(fs::metadata(short).unwrap().len(), 64);
        }
    }
}
