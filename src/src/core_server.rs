//! Connection-oriented Core: listens on a Unix domain socket instead of being the
//! stdin/stdout child of a Gateway (see docs/gateway-core-separation-design.md).
//!
//! This is stage 1-B1 of the Gateway/Core separation. It accepts connections, checks
//! that the peer is the same user and knows the capability secret, and then runs the
//! unchanged request loop (`worker::serve_connection`) on the accepted socket. With
//! `Lifetime::OneConnection` (what `run` uses today) the Core exits after one session, like a
//! stdin/stdout Core; with `UntilStopped` (stage 1-B3) the application state lives in a
//! `CoreLife` that outlasts connections. botd does not request that mode yet.
//!
//! Security model (Linux/Unix only): the socket lives in a directory that only the
//! current user can enter, is created 0600, and every connection must present a
//! 32-byte secret that botd handed to this process on its stdin. No secret is taken
//! from argv or the environment, and none is logged. A same-user attacker is out of
//! scope, exactly as for the embedded Core.

use crate::AppError;
use crate::redact::Secret;
use crate::secret_file::{SECRET_BYTES, effective_uid, parse_hex_secret};
use crate::worker::{CoreIo, CoreLife, Fence, failure, serve_connection};
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::time::Duration;
use subtle::ConstantTimeEq;
use tokio::io::AsyncReadExt;
use tokio::net::{UnixListener, UnixStream};

/// The first line a client sends: the secret as 64 hex digits and a newline.
const ATTACH_FRAME_BYTES: usize = SECRET_BYTES * 2 + 1;
/// A client that does not authenticate within this time is dropped.
const ATTACH_DEADLINE: Duration = Duration::from_secs(5);
/// Failed attempts tolerated before the Core gives up instead of being probed forever.
const MAX_FAILED_ATTACHES: u32 = 16;
/// The secret from botd must arrive promptly; a Core nobody configures must not linger.
const SECRET_DEADLINE: Duration = Duration::from_secs(10);

/// Why a socket path or an attach attempt was refused. Fixed text only: never the path
/// content, the secret or an OS error string.
#[derive(Debug, PartialEq, Eq)]
pub enum CoreServerError {
    InvalidPath,
    UnsafeDirectory,
    NotASocket,
    Bind,
    Secret,
    TooManyFailedAttaches,
}

impl std::fmt::Display for CoreServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidPath => "core socket path must be absolute and free of '..'",
            Self::UnsafeDirectory => "core socket directory must be private to the current user",
            Self::NotASocket => "core socket path exists and is not a socket",
            Self::Bind => "cannot create the core socket",
            Self::Secret => "core capability secret was not provided",
            Self::TooManyFailedAttaches => "too many failed attach attempts",
        })
    }
}
impl std::error::Error for CoreServerError {}

/// Absolute, no `..`: the same rule every other path argument follows.
fn validate_socket_path(path: &Path) -> Result<(), CoreServerError> {
    if !path.is_absolute()
        || path.file_name().is_none()
        || path.components().any(|part| part == Component::ParentDir)
    {
        return Err(CoreServerError::InvalidPath);
    }
    Ok(())
}

/// The parent must be a real directory owned by us with no group/other access, so only
/// this user can reach the socket regardless of the socket's own mode.
fn check_private_directory(dir: &Path) -> Result<(), CoreServerError> {
    let meta = std::fs::symlink_metadata(dir).map_err(|_| CoreServerError::UnsafeDirectory)?;
    if !meta.is_dir() || meta.uid() != effective_uid() || meta.mode() & 0o077 != 0 {
        return Err(CoreServerError::UnsafeDirectory);
    }
    Ok(())
}

/// A stale socket file from a previous Core is removed; anything else at the path is
/// left alone and refused.
fn remove_stale_socket(path: &Path) -> Result<(), CoreServerError> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_socket() => {
            std::fs::remove_file(path).map_err(|_| CoreServerError::Bind)
        }
        Ok(_) => Err(CoreServerError::NotASocket),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(CoreServerError::Bind),
    }
}

/// Binds the listening socket (0600) after the directory and path checks.
pub fn bind_socket(path: &Path) -> Result<UnixListener, CoreServerError> {
    validate_socket_path(path)?;
    let parent = path.parent().ok_or(CoreServerError::InvalidPath)?;
    check_private_directory(parent)?;
    remove_stale_socket(path)?;
    let listener = UnixListener::bind(path).map_err(|_| CoreServerError::Bind)?;
    // The directory is private, so the window before this call is not reachable by
    // other users; the mode makes the intent explicit and survives a later directory
    // mode change.
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .map_err(|_| CoreServerError::Bind)?;
    Ok(listener)
}

/// Longest attach line: 64 hex digits, a space, a u64 in decimal (20 digits).
const ATTACH_LINE_MAX: usize = SECRET_BYTES * 2 + 1 + 20;

/// Reads the attach line `<64 hex>[ <epoch>]\n` and compares the secret in constant time.
/// Reads byte by byte up to the newline, so nothing of the protocol that follows is
/// consumed. Returns the ownership epoch the client attached with (0 when absent).
async fn authenticate(
    stream: &mut UnixStream,
    expected: &Secret<[u8; SECRET_BYTES]>,
) -> Result<u64, CoreServerError> {
    let mut line = [0u8; ATTACH_LINE_MAX];
    let mut len = 0usize;
    let read = tokio::time::timeout(ATTACH_DEADLINE, async {
        loop {
            let byte = stream
                .read_u8()
                .await
                .map_err(|_| CoreServerError::Secret)?;
            if byte == b'\n' {
                return Ok(());
            }
            if len >= ATTACH_LINE_MAX {
                return Err(CoreServerError::Secret);
            }
            line[len] = byte;
            len += 1;
        }
    })
    .await
    .map_err(|_| CoreServerError::Secret)
    .and_then(|result| result);
    let result = read.and_then(|()| verify_attach_line(&line[..len], expected));
    crate::secret_file::wipe(&mut line);
    result
}

fn verify_attach_line(
    line: &[u8],
    expected: &Secret<[u8; SECRET_BYTES]>,
) -> Result<u64, CoreServerError> {
    let hex_len = SECRET_BYTES * 2;
    if line.len() < hex_len {
        return Err(CoreServerError::Secret);
    }
    let presented = parse_hex_secret(&line[..hex_len]).map_err(|_| CoreServerError::Secret)?;
    // The epoch is parsed only after the secret matched, so an unauthenticated peer gets no
    // information from how its suffix is judged.
    if !bool::from(presented.expose_secret().ct_eq(expected.expose_secret())) {
        return Err(CoreServerError::Secret);
    }
    match &line[hex_len..] {
        [] => Ok(0),
        [b' ', digits @ ..] if !digits.is_empty() && digits.iter().all(u8::is_ascii_digit) => {
            std::str::from_utf8(digits)
                .ok()
                .and_then(|text| text.parse::<u64>().ok())
                .ok_or(CoreServerError::Secret)
        }
        _ => Err(CoreServerError::Secret),
    }
}

/// Same user as this process (SO_PEERCRED). It cannot tell two processes of one user
/// apart; the capability secret is the actual gate.
fn peer_is_current_user(stream: &UnixStream) -> bool {
    stream
        .peer_cred()
        .is_ok_and(|credentials| credentials.uid() == effective_uid())
}

/// How long a Core serves. `OneConnection` is the embedded Core's lifetime; `UntilStopped`
/// keeps the application state across connections (a restarted Gateway reconnects), with
/// everything negotiated per connection starting fresh each time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lifetime {
    OneConnection,
    UntilStopped,
}

/// Accepts connections and serves each authenticated one with the shared request loop.
/// `OneConnection` returns when that session ends; `UntilStopped` keeps serving.
pub async fn serve(
    listener: UnixListener,
    secret: Secret<[u8; SECRET_BYTES]>,
    backend: Option<std::sync::Arc<dyn crate::state_store::StateStore>>,
    lifetime: Lifetime,
    initial_floor: u64,
) -> Result<(), AppError> {
    let mut life = CoreLife::new(backend)?;
    let failed = std::sync::atomic::AtomicU32::new(0);
    if lifetime == Lifetime::OneConnection {
        let (stream, _) = accept_authenticated(&listener, &secret, &failed).await?;
        return serve_connection(&mut life, CoreIo::socket(stream), None).await;
    }
    // UntilStopped. While one connection is served, further ones are still accepted and
    // authenticated, and ordered by the epoch they attach with:
    // - a higher epoch fences the one being served (it stops between requests) and is
    //   served next, so a Gateway that took over is never raced by the one it replaced;
    // - an equal epoch waits its turn (a restarted Gateway behind a dead connection);
    // - a lower epoch is refused at once.
    // The acceptor is one future kept across the whole loop: dropping it while a client is
    // half-way through authenticating would silently lose that client.
    let (fence_tx, _) = tokio::sync::watch::channel(initial_floor);
    let mut pending: Option<(UnixStream, u64)> = None;
    let mut acceptor = Box::pin(accept_authenticated(&listener, &secret, &failed));
    // The highest epoch this Core has ever served. It outlives the connection that set it:
    // once a Gateway has taken over at epoch N, no Gateway below N may attach again, even
    // when nothing is connected at that moment.
    let mut floor = initial_floor;
    loop {
        let (stream, epoch) = match pending.take() {
            Some(next) => next,
            None => {
                let next = acceptor.as_mut().await?;
                acceptor = Box::pin(accept_authenticated(&listener, &secret, &failed));
                if next.1 < floor {
                    drop(next.0);
                    continue;
                }
                next
            }
        };
        floor = floor.max(epoch);
        let fence = Fence {
            rx: fence_tx.subscribe(),
            epoch,
        };
        let serving = serve_connection(&mut life, CoreIo::socket(stream), Some(fence));
        tokio::pin!(serving);
        loop {
            tokio::select! {
                // A broken connection must not take the Core and its state down with it.
                _ = &mut serving => break,
                next = acceptor.as_mut() => {
                    let (stream, attached) = next?;
                    acceptor = Box::pin(accept_authenticated(&listener, &secret, &failed));
                    let highest = pending.as_ref().map_or(epoch, |(_, e)| (*e).max(epoch)).max(floor);
                    if attached < highest {
                        drop(stream);
                    } else {
                        if attached > epoch {
                            fence_tx.send_replace(attached);
                        }
                        pending = Some((stream, attached));
                    }
                }
            }
        }
    }
}

/// Accepts until one connection passes the peer check and the secret. Gives up after too
/// many failures in a row so a Core is not probed forever.
async fn accept_authenticated(
    listener: &UnixListener,
    secret: &Secret<[u8; SECRET_BYTES]>,
    failed: &std::sync::atomic::AtomicU32,
) -> Result<(UnixStream, u64), AppError> {
    loop {
        let (mut stream, _) = listener
            .accept()
            .await
            .map_err(|_| failure("core socket accept failed"))?;
        if peer_is_current_user(&stream)
            && let Ok(epoch) = authenticate(&mut stream, secret).await
        {
            failed.store(0, std::sync::atomic::Ordering::Relaxed);
            return Ok((stream, epoch));
        }
        let count = failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        if count >= MAX_FAILED_ATTACHES {
            return Err(Box::new(CoreServerError::TooManyFailedAttaches));
        }
    }
}

/// Reads the capability secret botd writes on this process's stdin: 64 hex digits and a
/// newline, nothing else.
async fn read_secret_from_stdin() -> Result<Secret<[u8; SECRET_BYTES]>, CoreServerError> {
    let mut frame = [0u8; ATTACH_FRAME_BYTES];
    let mut stdin = tokio::io::stdin();
    tokio::time::timeout(SECRET_DEADLINE, stdin.read_exact(&mut frame))
        .await
        .map_err(|_| CoreServerError::Secret)?
        .map_err(|_| CoreServerError::Secret)?;
    if frame[ATTACH_FRAME_BYTES - 1] != b'\n' {
        return Err(CoreServerError::Secret);
    }
    parse_hex_secret(&frame[..ATTACH_FRAME_BYTES - 1]).map_err(|_| CoreServerError::Secret)
}

/// Process entry for `src --core-listen <absolute socket path>`.
pub async fn run(socket: PathBuf, lifetime: Lifetime, floor: u64) -> Result<(), AppError> {
    let secret = read_secret_from_stdin().await?;
    let listener = bind_socket(&socket)?;
    if lifetime == Lifetime::UntilStopped {
        // A persistent Core is supervised by whoever holds the other end of its stdin
        // (botd). When that end closes, whether botd stopped it or died, the Core exits:
        // it must not linger as an unreachable orphan holding state.
        tokio::spawn(async {
            let mut stdin = tokio::io::stdin();
            let mut sink = [0u8; 64];
            while matches!(stdin.read(&mut sink).await, Ok(n) if n > 0) {}
            std::process::exit(0);
        });
    }
    let result = serve(listener, secret, None, lifetime, floor).await;
    if lifetime == Lifetime::UntilStopped {
        // The stdin watcher above is a blocking read that cannot be cancelled, so returning
        // would leave the runtime waiting on it: a live process with no listener, which botd
        // would never replace. Exit explicitly, so botd sees the exit and restarts the Core.
        if let Err(error) = &result {
            eprintln!("core: {}", crate::redact::error_diagnostic(error.as_ref()));
        }
        std::process::exit(i32::from(result.is_err()));
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn socket_path_must_be_absolute_and_free_of_parent_components() {
        for bad in ["relative.sock", "./core.sock", "/a/../core.sock", "/"] {
            assert_eq!(
                validate_socket_path(Path::new(bad)),
                Err(CoreServerError::InvalidPath),
                "{bad}"
            );
        }
        assert_eq!(
            validate_socket_path(Path::new("/run/discord-rs/core.sock")),
            Ok(())
        );
    }

    #[test]
    fn errors_name_no_path_and_no_secret() {
        for error in [
            CoreServerError::InvalidPath,
            CoreServerError::UnsafeDirectory,
            CoreServerError::NotASocket,
            CoreServerError::Bind,
            CoreServerError::Secret,
            CoreServerError::TooManyFailedAttaches,
        ] {
            let text = error.to_string();
            assert!(!text.contains('/') || text.contains("'..'"), "{text}");
        }
    }
}
