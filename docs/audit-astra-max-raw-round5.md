# Astra Max 再監査 第 5 回の原文（a4f6001 まで）

Reviewed at **`a4f6001`**, including surrounding regression paths. No edits, builds, tests, or network access. Findings below are based on static analysis.

The epoch-floor wiring matches the candidate and recovery Gateway epochs, including failed activation. Reconnect checks look correct. Stamp reservation holds the lock through file/directory fsync and handles overflow without wrapping. Pending audit memory is bounded by the 1,024-operation limit; shutdown retries once.

1. **P2 — Identify cooldown breaks normal multi-shard updates.** [daemon.rs:3171](D:/data-backup/code/コード保存/discord-rs/src/src/daemon.rs:3171)  
   If the first shard activates within 30 seconds, the next shard’s probe returns `Pending`. This becomes `InvalidState`, and `advance_update` terminates the operation. Retrying starts with the first shard again.  
   **Minimal fix:** keep `Begin` pending until `retry_after`, subject to the operation deadline; distinguish cooldown from exhaustion. **Confidence: high.**

2. **P2 — Replay can retain unbounded completed tasks.** [main.rs:1170](D:/data-backup/code/コード保存/discord-rs/src/src/main.rs:1170), [main.rs:1192](D:/data-backup/code/コード保存/discord-rs/src/src/main.rs:1192)  
   If arrivals keep the replay backlog nonempty without overflowing it, `requests.join_next()` never runs. Completed tasks accumulate in `JoinSet`; the 64 permits and 512-event limit do not bound those allocations.  
   **Minimal fix:** reap completed tasks during replay, including before taking an immediately available permit. **Confidence: high.**

3. **P2 — Symlink protection uses the wrong flag on Oracle aarch64.** [core_units.rs:108](D:/data-backup/code/コード保存/discord-rs/src/src/core_units.rs:108), [core_state.rs:286](D:/data-backup/code/コード保存/discord-rs/src/src/core_state.rs:286)  
   Linux aarch64 uses `O_NOFOLLOW = 0o100000`; the hardcoded `0o400000` is the x86_64 value. A symlink substituted between the path check and open can therefore be followed. A same-owner regular target can pass the subsequent checks and receive audit/log writes or chmod.  
   **Minimal fix:** use target-specific constants such as `libc::O_NOFOLLOW`. The “シンボリックリンク拒否” claim in [progress.md:341](D:/data-backup/code/コード保存/discord-rs/docs/progress.md:341) currently overstates protection. **Confidence: high.**

4. **P2 — Audit retries can report recovery while leaving invalid JSONL.** [daemon.rs:983](D:/data-backup/code/コード保存/discord-rs/src/src/daemon.rs:983), [daemon.rs:987](D:/data-backup/code/コード保存/discord-rs/src/src/daemon.rs:987)  
   ENOSPC can leave a partial record without its newline. After space is freed, retry appends a complete record directly to that fragment. The queue clears and status reports `ok`, but the terminal record is unparseable. An uncertain fsync failure can also produce duplicates.  
   **Minimal fix:** serialize appends, repair incomplete tails before retry, and retain a stable serialized record/identity for retries. Qualify “1 行 1 JSON” in [known-limitations.md:37](D:/data-backup/code/コード保存/discord-rs/docs/known-limitations.md:37). **Confidence: high.**

5. **P2 — Writer-lock contention has no deadline.** [core_state.rs:298](D:/data-backup/code/コード保存/discord-rs/src/src/core_state.rs:298), [core_state.rs:368](D:/data-backup/code/コード保存/discord-rs/src/src/core_state.rs:368)  
   A live writer stopped with SIGSTOP while holding the lock blocks another Gateway’s construction indefinitely. `O_NONBLOCK` does not make `flock(LOCK_EX)` nonblocking, and cancelling the awaiting future does not cancel its blocking thread.  
   **Minimal fix:** use `try_lock` with a bounded retry inside the blocking task, then enter the intended persistence-disabled fallback. A dead holder’s stale file alone is harmless. **Confidence: high.**

6. **P2 — Replay overflow bypasses the response drain.** [main.rs:1204](D:/data-backup/code/コード保存/discord-rs/src/src/main.rs:1204)  
   Overflow or shard closure returns immediately, skipping the drain at line 1490. Dropping `JoinSet` aborts already-admitted requests, including HTTP work, after this Gateway has taken ownership.  
   **Minimal fix:** save the error, leave the event loop, run the common bounded drain, then return the error. **Confidence: high.**

7. **P3 — Persistence-disabled diagnostics are unreadable.** [core_state.rs:371](D:/data-backup/code/コード保存/discord-rs/src/src/core_state.rs:371)  
   Allocation failure permanently disables this writer’s persistence, but its only warning literally contains `??: ??????????????`. Operators cannot identify the lost capability or required recovery.  
   **Minimal fix:** emit a meaningful sanitized warning stating that checkpoint persistence is disabled until restart. Also qualify the approximately 2.5-second loss statement in [known-limitations.md:55](D:/data-backup/code/コード保存/discord-rs/docs/known-limitations.md:55): this mode can lose the writer’s entire lifetime of changes. **Confidence: high.**

ACTIONABLE ISSUES REMAIN: 7