//! Persistence of the confirmed snapshot-v3 (legacy count/job) state.
use serde_json::{Value, json};
use src::{
    core_state::{self, Binding, EncodeError, Persister},
    worker::{DeploymentIdentity, Manager},
};
use std::{path::PathBuf, time::Duration};
use tokio::time::timeout;

struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "core-state-{}-{}",
            std::process::id(),
            getrandom::u64().unwrap()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn file(&self) -> PathBuf {
        self.0.join(core_state::FILE_NAME)
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn binding() -> Binding {
    Binding {
        bot_id: 101,
        shard_id: 1,
        total_shards: 2,
        workers: 2,
    }
}

fn count(value: u64) -> Value {
    json!({"counts": {"0": value}, "jobs": {}})
}

fn job() -> Value {
    json!({"counts": {}, "jobs": {"7": {"step": 3, "total": 6, "running": true}}})
}

#[test]
fn round_trip_preserves_count_and_job_state() {
    let dir = Dir::new();
    let states = vec![Some(count(5)), Some(job())];
    core_state::store(&dir.0, &binding(), &states).unwrap();
    assert_eq!(core_state::load(&dir.0, &binding()), states);
    let partial = vec![None, Some(count(1))];
    core_state::store(&dir.0, &binding(), &partial).unwrap();
    assert_eq!(core_state::load(&dir.0, &binding()), partial);
}

#[test]
fn missing_corrupt_and_unreadable_files_load_as_empty() {
    let dir = Dir::new();
    let empty = vec![None, None];
    assert_eq!(core_state::load(&dir.0, &binding()), empty);
    for bytes in [
        &b""[..],
        b"not json",
        b"{\"schema\":1}",
        b"\xff\xfe\x00",
        b"[1,2,3]",
    ] {
        std::fs::write(dir.file(), bytes).unwrap();
        assert_eq!(core_state::load(&dir.0, &binding()), empty);
    }
    // A directory in place of the file is also just ignored.
    std::fs::remove_file(dir.file()).unwrap();
    std::fs::create_dir(dir.file()).unwrap();
    assert_eq!(core_state::load(&dir.0, &binding()), empty);
}

#[test]
fn schema_and_binding_mismatches_are_ignored() {
    let dir = Dir::new();
    let states = vec![Some(count(9)), None];
    core_state::store(&dir.0, &binding(), &states).unwrap();
    for other in [
        Binding {
            bot_id: 102,
            ..binding()
        },
        Binding {
            shard_id: 0,
            ..binding()
        },
        Binding {
            total_shards: 3,
            ..binding()
        },
        Binding {
            workers: 3,
            ..binding()
        },
    ] {
        assert!(core_state::load(&dir.0, &other).iter().all(Option::is_none));
    }
    let bytes = core_state::encode(&binding(), &states).unwrap();
    let mut document: Value = serde_json::from_slice(&bytes).unwrap();
    document["schema"] = json!(core_state::SCHEMA + 1);
    assert!(core_state::decode(&binding(), &serde_json::to_vec(&document).unwrap()).is_none());
    document["schema"] = json!(core_state::SCHEMA);
    document["unexpected"] = json!(true);
    assert!(core_state::decode(&binding(), &serde_json::to_vec(&document).unwrap()).is_none());
    document.as_object_mut().unwrap().remove("unexpected");
    document["states"][0] = json!({"counts": "not a map"});
    assert!(core_state::decode(&binding(), &serde_json::to_vec(&document).unwrap()).is_none());
}

#[test]
fn oversize_state_is_not_encoded_or_stored() {
    let dir = Dir::new();
    let counts: serde_json::Map<String, Value> = (0..100_000u64)
        .map(|index| (format!("{}", 1_000_000 + index), json!(1_000_000 + index)))
        .collect();
    let big = json!({"counts": counts, "jobs": {}});
    let states = vec![Some(big), None];
    assert_eq!(
        core_state::encode(&binding(), &states),
        Err(EncodeError::TooLarge)
    );
    assert!(core_state::store(&dir.0, &binding(), &states).is_err());
    assert!(!dir.file().exists());
    // An oversize file on disk is never read.
    std::fs::write(dir.file(), vec![b' '; core_state::MAX_FILE_BYTES + 1]).unwrap();
    assert!(
        core_state::load(&dir.0, &binding())
            .iter()
            .all(Option::is_none)
    );
}

#[test]
fn atomic_write_replaces_without_leaving_temporaries() {
    let dir = Dir::new();
    core_state::store(&dir.0, &binding(), &[Some(count(1)), None]).unwrap();
    // A stale temporary from a crashed writer does not block the next write.
    std::fs::write(dir.0.join(".core-state.json.tmp"), b"stale").unwrap();
    std::fs::write(dir.0.join(".core-state.json.tmp.4294967290"), b"stale").unwrap();
    core_state::store(&dir.0, &binding(), &[Some(count(2)), None]).unwrap();
    assert_eq!(
        core_state::load(&dir.0, &binding()),
        vec![Some(count(2)), None]
    );
    let names: Vec<_> = std::fs::read_dir(&dir.0)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert_eq!(names, vec![std::ffi::OsString::from(core_state::FILE_NAME)]);
}

#[cfg(unix)]
#[test]
fn file_is_private_and_symbolic_links_are_not_followed() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let dir = Dir::new();
    core_state::store(&dir.0, &binding(), &[Some(count(4)), None]).unwrap();
    let mode = std::fs::metadata(dir.file()).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o600);

    let elsewhere = Dir::new();
    let target = elsewhere.0.join("target.json");
    std::fs::rename(dir.file(), &target).unwrap();
    symlink(&target, dir.file()).unwrap();
    assert!(
        core_state::load(&dir.0, &binding())
            .iter()
            .all(Option::is_none)
    );

    // Writing replaces the link itself and never writes through it.
    let before = std::fs::read(&target).unwrap();
    core_state::store(&dir.0, &binding(), &[Some(count(8)), None]).unwrap();
    assert_eq!(std::fs::read(&target).unwrap(), before);
    assert_eq!(
        core_state::load(&dir.0, &binding()),
        vec![Some(count(8)), None]
    );

    // A planted temporary link is removed, not followed.
    let victim = elsewhere.0.join("victim");
    std::fs::write(&victim, b"keep").unwrap();
    symlink(&victim, dir.0.join(".core-state.json.tmp")).unwrap();
    core_state::store(&dir.0, &binding(), &[Some(count(9)), None]).unwrap();
    assert_eq!(std::fs::read(&victim).unwrap(), b"keep");
}

#[tokio::test]
async fn persister_writes_only_changes_and_throttles_until_forced() {
    let dir = Dir::new();
    let (persister, loaded) = Persister::open(dir.0.clone(), binding()).await;
    assert!(loaded.iter().all(Option::is_none));

    // Nothing observed: nothing written. An unknown index is ignored.
    persister.flush(true).await;
    assert!(!dir.file().exists());
    persister.observe(9, &count(1));
    assert!(!persister.is_dirty());

    persister.observe(0, &count(1));
    assert!(persister.is_dirty());
    persister.flush(false).await;
    assert!(!persister.is_dirty());
    assert_eq!(
        core_state::load(&dir.0, &binding()),
        vec![Some(count(1)), None]
    );

    // Re-observing the same value is not a change.
    persister.observe(0, &count(1));
    assert!(!persister.is_dirty());

    // A change inside the minimum interval is held back...
    persister.observe(0, &count(2));
    persister.flush(false).await;
    assert!(persister.is_dirty());
    assert_eq!(
        core_state::load(&dir.0, &binding()),
        vec![Some(count(1)), None]
    );
    // ...written once the interval has passed, and always when forced.
    tokio::time::sleep(core_state::MIN_WRITE_INTERVAL + Duration::from_millis(100)).await;
    persister.flush(false).await;
    assert_eq!(
        core_state::load(&dir.0, &binding()),
        vec![Some(count(2)), None]
    );
    persister.observe(1, &job());
    persister.flush(true).await;
    assert_eq!(
        core_state::load(&dir.0, &binding()),
        vec![Some(count(2)), Some(job())]
    );

    // A reopened persister starts from the stored values and sees no change.
    let (reopened, loaded) = Persister::open(dir.0.clone(), binding()).await;
    assert_eq!(loaded, vec![Some(count(2)), Some(job())]);
    reopened.observe(0, &count(2));
    assert!(!reopened.is_dirty());
}

#[tokio::test]
async fn persister_failure_and_oversize_only_warn() {
    let dir = Dir::new();
    let (persister, _) = Persister::open(dir.0.join("missing").join("deeper"), binding()).await;
    persister.observe(0, &count(1));
    // The directory does not exist: a warning, the change stays pending.
    persister.flush(true).await;
    assert!(persister.is_dirty());

    let (persister, _) = Persister::open(dir.0.clone(), binding()).await;
    let counts: serde_json::Map<String, Value> = (0..100_000u64)
        .map(|index| (format!("{}", 1_000_000 + index), json!(1_000_000 + index)))
        .collect();
    persister.observe(0, &json!({"counts": counts, "jobs": {}}));
    persister.flush(true).await;
    assert!(!dir.file().exists());
    assert!(!persister.is_dirty());
}

fn binary() -> PathBuf {
    env!("CARGO_BIN_EXE_src").into()
}

fn deployment(bot: &str, dir: &std::path::Path) -> DeploymentIdentity {
    let bot = bot.to_owned();
    DeploymentIdentity::parse(|name| {
        Ok(match name {
            "BOT_ID" => Some(bot.clone()),
            "SHARD_ID" => Some("1".into()),
            "TOTAL_SHARDS" => Some("2".into()),
            "GENERATION_ID" => Some("800".into()),
            _ => None,
        })
    })
    .unwrap()
    .with_state_dir(dir.to_owned())
}

async fn count_reply(manager: &Manager) -> String {
    manager
        .dispatch("!count".into())
        .await
        .unwrap()
        .unwrap()
        .lines()
        .next()
        .unwrap()
        .to_owned()
}

#[tokio::test]
async fn count_continues_after_the_manager_is_restarted_on_the_same_state_path() {
    timeout(Duration::from_secs(60), async {
        let dir = Dir::new();
        let manager = Manager::start_pool_isolated(binary(), 2, deployment("101", &dir.0))
            .await
            .unwrap();
        assert_eq!(count_reply(&manager).await, "カウント: 1");
        assert_eq!(count_reply(&manager).await, "カウント: 2");
        manager.shutdown().await.unwrap();
        assert!(dir.file().is_file());

        // Same Shard, same state path: the count continues.
        let manager = Manager::start_pool_isolated(binary(), 2, deployment("101", &dir.0))
            .await
            .unwrap();
        assert_eq!(count_reply(&manager).await, "カウント: 3");
        manager.shutdown().await.unwrap();

        // Another Bot never reads this Shard's file.
        let other = Manager::start_pool_isolated(binary(), 2, deployment("102", &dir.0))
            .await
            .unwrap();
        assert_eq!(count_reply(&other).await, "カウント: 1");
        other.shutdown().await.unwrap();

        // A corrupt file is ignored and does not fail startup.
        std::fs::write(dir.file(), b"corrupt").unwrap();
        let fresh = Manager::start_pool_isolated(binary(), 2, deployment("101", &dir.0))
            .await
            .unwrap();
        assert_eq!(count_reply(&fresh).await, "カウント: 1");
        fresh.shutdown().await.unwrap();
    })
    .await
    .expect("persistence integration deadline");
}

/// 更新中に 2 つの Gateway が重なっても、古い epoch の Gateway が、新しい epoch の状態を
/// 古い状態で上書きしない。同じ epoch・高い epoch は書ける。一時ファイル名は書き手ごとに
/// 別で、残らない。
#[test]
fn a_lower_epoch_never_replaces_a_state_written_by_a_higher_epoch() {
    let dir = Dir::new();
    let newer = [Some(count(9)), None];
    let stale = [Some(count(2)), None];
    core_state::store_at(&dir.0, &binding(), &newer, 5).unwrap();
    // The older Gateway flushing at its shutdown: accepted by the call, but not written.
    core_state::store_at(&dir.0, &binding(), &stale, 3).unwrap();
    assert_eq!(core_state::load(&dir.0, &binding()), newer);
    // The same epoch and a higher one still write.
    let same = [Some(count(10)), None];
    core_state::store_at(&dir.0, &binding(), &same, 5).unwrap();
    assert_eq!(core_state::load(&dir.0, &binding()), same);
    let higher = [Some(count(11)), None];
    core_state::store_at(&dir.0, &binding(), &higher, 7).unwrap();
    assert_eq!(core_state::load(&dir.0, &binding()), higher);
    // A file from before epochs existed (no field) counts as epoch 0.
    let legacy = core_state::encode(&binding(), &[Some(count(1)), None]).unwrap();
    std::fs::write(dir.file(), legacy).unwrap();
    core_state::store_at(&dir.0, &binding(), &newer, 1).unwrap();
    assert_eq!(core_state::load(&dir.0, &binding()), newer);
    let leftovers: Vec<_> = std::fs::read_dir(&dir.0)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains(".tmp"))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
}

/// Astra の再監査：保存の印は、Gateway が起動するたびに「ファイルの値より必ず大きい」ものを取る。
/// 時計が戻っても、モードを切り替えても、botd を再起動しても、新しい Gateway の保存は止まらず、
/// 先に起動していた古い Gateway の遅れた保存は新しい状態を上書きしない。
#[tokio::test]
async fn a_newer_writer_always_outranks_the_older_one_whatever_the_file_carries() {
    let dir = Dir::new();
    // The file is stamped far in the future (as if the clock had stepped back since).
    core_state::store_at(&dir.0, &binding(), &[Some(count(1)), None], u64::MAX / 2).unwrap();

    // A writer that starts now still writes: it ranks above whatever the file carries.
    let (first, loaded) = Persister::open(dir.0.clone(), binding()).await;
    assert_eq!(loaded, vec![Some(count(1)), None]);
    first.observe(0, &count(2));
    first.flush(true).await;
    assert_eq!(
        core_state::load(&dir.0, &binding()),
        vec![Some(count(2)), None],
        "a new writer is not locked out by a larger stamp already in the file"
    );

    // A second writer starts later (the replacement Gateway): it outranks the first.
    let (second, _) = Persister::open(dir.0.clone(), binding()).await;
    second.observe(0, &count(5));
    second.flush(true).await;
    // The first (old) writer flushes late with older content: it must not win.
    first.observe(0, &count(3));
    first.flush(true).await;
    assert_eq!(
        core_state::load(&dir.0, &binding()),
        vec![Some(count(5)), None],
        "an older writer never replaces what a newer one saved"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn writer_stamps_increase_before_either_writer_saves() {
    let dir = Dir::new();
    let path = dir.0.join("core-state.writer");
    let future = u64::MAX / 2;
    std::fs::write(&path, future.to_string()).unwrap();
    let (first, _) = Persister::open(dir.0.clone(), binding()).await;
    let stamp1: u64 = std::fs::read_to_string(&path).unwrap().parse().unwrap();
    let (second, _) = Persister::open(dir.0.clone(), binding()).await;
    let stamp2: u64 = std::fs::read_to_string(&path).unwrap().parse().unwrap();
    assert_eq!(stamp1, future + 1);
    assert_eq!(stamp2, stamp1 + 1);
    second.observe(0, &count(5));
    second.flush(true).await;
    first.observe(0, &count(3));
    first.flush(true).await;
    assert_eq!(
        core_state::load(&dir.0, &binding()),
        vec![Some(count(5)), None]
    );
}

#[cfg(unix)]
#[tokio::test]
async fn exhausted_writer_stamp_disables_writes_but_loads_state() {
    let dir = Dir::new();
    let states = vec![Some(count(9)), None];
    core_state::store(&dir.0, &binding(), &states).unwrap();
    std::fs::write(dir.0.join("core-state.writer"), u64::MAX.to_string()).unwrap();
    let (persister, loaded) = Persister::open(dir.0.clone(), binding()).await;
    assert_eq!(loaded, states);
    persister.observe(0, &count(10));
    persister.flush(true).await;
    persister.flush(true).await;
    assert_eq!(core_state::load(&dir.0, &binding()), states);
}

#[cfg(unix)]
#[tokio::test]
async fn writer_stamp_rejects_symlink_and_accepts_existing_public_mode() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let dir = Dir::new();
    let target = dir.0.join("target");
    std::fs::write(&target, "123").unwrap();
    let path = dir.0.join("core-state.writer");
    symlink(&target, &path).unwrap();
    let (disabled, _) = Persister::open(dir.0.clone(), binding()).await;
    disabled.observe(0, &count(1));
    disabled.flush(true).await;
    assert!(!dir.file().exists());
    assert_eq!(std::fs::read_to_string(&target).unwrap(), "123");
    std::fs::remove_file(&path).unwrap();
    std::fs::write(&path, "123").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    let (enabled, _) = Persister::open(dir.0.clone(), binding()).await;
    enabled.observe(0, &count(2));
    enabled.flush(true).await;
    assert_eq!(
        core_state::load(&dir.0, &binding()),
        vec![Some(count(2)), None]
    );
    assert_eq!(
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
        0o644
    );
}
