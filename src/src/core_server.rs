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
use crate::worker::{CoreIo, CoreLife, failure, serve_connection};
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

/// Reads exactly the attach line and compares it in constant time. Reads no more than
/// the frame, so nothing of the protocol that follows is consumed.
async fn authenticate(
    stream: &mut UnixStream,
    expected: &Secret<[u8; SECRET_BYTES]>,
) -> Result<(), CoreServerError> {
    let mut frame = [0u8; ATTACH_FRAME_BYTES];
    tokio::time::timeout(ATTACH_DEADLINE, stream.read_exact(&mut frame))
        .await
        .map_err(|_| CoreServerError::Secret)?
        .map_err(|_| CoreServerError::Secret)?;
    if frame[ATTACH_FRAME_BYTES - 1] != b'\n' {
        return Err(CoreServerError::Secret);
    }
    let presented =
        parse_hex_secret(&frame[..ATTACH_FRAME_BYTES - 1]).map_err(|_| CoreServerError::Secret)?;
    if bool::from(presented.expose_secret().ct_eq(expected.expose_secret())) {
        Ok(())
    } else {
        Err(CoreServerError::Secret)
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
) -> Result<(), AppError> {
    let mut life = CoreLife::new(backend)?;
    let mut failed = 0u32;
    loop {
        let (mut stream, _) = listener
            .accept()
            .await
            .map_err(|_| failure("core socket accept failed"))?;
        if !peer_is_current_user(&stream) || authenticate(&mut stream, &secret).await.is_err() {
            failed += 1;
            if failed >= MAX_FAILED_ATTACHES {
                return Err(Box::new(CoreServerError::TooManyFailedAttaches));
            }
            continue;
        }
        let outcome = serve_connection(&mut life, CoreIo::socket(stream)).await;
        match lifetime {
            Lifetime::OneConnection => return outcome,
            // A broken connection must not take the Core and its state down with it.
            Lifetime::UntilStopped => failed = 0,
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
pub async fn run(socket: PathBuf) -> Result<(), AppError> {
    let secret = read_secret_from_stdin().await?;
    let listener = bind_socket(&socket)?;
    serve(listener, secret, None, Lifetime::OneConnection).await
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
