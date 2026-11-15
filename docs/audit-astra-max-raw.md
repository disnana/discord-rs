# Astra Max 監査の原文（Codex `gpt-6-astra`、max、読み取り専用、2026-11-16 01:32）

生の出力。整理と対応状況は [audit-2026-11-16.md](audit-2026-11-16.md)。

Reviewed `0703cf1..8046326`. HEAD advanced during review only by a deployment helper and documentation commit. I made no file changes and ran no builds, tests, or network requests. These are static findings; failure scenarios were not reproduced.

**The highest-risk finding is silent checkpoint loss after restarting botd following a Gateway update.**

1. **P1 · B — Restarting botd can silently disable checkpoint writes. Confidence: high.**  
   [core_state.rs:229](D:/data-backup/code/コード保存/discord-rs/src/src/core_state.rs:229), [daemon.rs:2990](D:/data-backup/code/コード保存/discord-rs/src/src/daemon.rs:2990), [worker.rs:1801](D:/data-backup/code/コード保存/discord-rs/src/src/worker.rs:1801).  
   Update to epoch 1 and save state; restart botd, which resets the Gateway epoch to 0. Every subsequent save returns `Ok(())` without writing because the file contains epoch 1. The persister nevertheless marks it saved. Another restart loses all intervening changes, potentially hours.  
   **Minimal fix:** recover a durable, monotonic ownership epoch across daemon incarnations; distinguish rejected stale writes from successful persistence.

2. **P1 · B — Backlog replay stops driving Discord heartbeats. Confidence: high on code; disconnect outcome is a hypothesis.**  
   [main.rs:470](D:/data-backup/code/コード保存/discord-rs/src/src/main.rs:470), [main.rs:1144](D:/data-backup/code/コード保存/discord-rs/src/src/main.rs:1144), [main.rs:1124](D:/data-backup/code/コード保存/discord-rs/src/src/main.rs:1124).  
   Attaching and replaying bypass `shard.next_event`; replay can await occupied dispatch slots. Twilight 0.18 requires continued shard polling to send heartbeats. A backlog exceeding 64 requests plus slow dispatch/HTTP can therefore stall heartbeats and shutdown handling after botd has accepted activation.  
   **Minimal fix:** keep shard polling and shutdown responsive independently of bounded backlog dispatch.

3. **P1 · B/C — A fatal Core error can leave an unrecoverable, apparently live PID. Confidence: high; runtime-hang hypothesis.**  
   [core_server.rs:310](D:/data-backup/code/コード保存/discord-rs/src/src/core_server.rs:310), [main.rs:914](D:/data-backup/code/コード保存/discord-rs/src/src/main.rs:914), [core_units.rs:219](D:/data-backup/code/コード保存/discord-rs/src/src/core_units.rs:219).  
   After the EOF watcher blocks on Tokio stdin, 16 failed attaches make `serve` return. Runtime destruction waits for that uncancellable blocking read while botd keeps stdin open. The listener disappears, but `try_wait` sees a live process and never replaces it.  
   **Minimal fix:** use a dedicated ordinary thread for the EOF watcher, or bounded runtime teardown.  
   **Doc overclaim:** [operator-guide.md:117](D:/data-backup/code/コード保存/discord-rs/docs/operator-guide.md:117): “Core が連続して認証に失敗（その間に認証済みの接続がない）を 16 回重ねると、Core は終了し、botd が起動し直します。”

4. **P2 · B — The replacement gives up before the blocked-write deadline expires. Confidence: high.**  
   [worker.rs:856](D:/data-backup/code/コード保存/discord-rs/src/src/worker.rs:856), [worker.rs:1035](D:/data-backup/code/コード保存/discord-rs/src/src/worker.rs:1035), [worker.rs:3745](D:/data-backup/code/コード保存/discord-rs/src/src/worker.rs:3745).  
   A non-reading old peer can hold the Core for 10 seconds, but the replacement’s initial Health request expires after 5 seconds. If more than five seconds remain, the real standby fails instead of taking over; activation recovery kills the old Gateway too. The regression test uses a raw client willing to wait 40 seconds ([core_attach.rs:1013](D:/data-backup/code/コード保存/discord-rs/src/tests/core_attach.rs:1013)).  
   **Minimal fix:** provide a takeover handshake budget covering fencing/drain, consistently bounded by activation’s deadline.

5. **P2 · B — Repeated updates can exhaust the recovery budget and strand the shard. Confidence: high.**  
   [daemon.rs:3190](D:/data-backup/code/コード保存/discord-rs/src/src/daemon.rs:3190), [daemon.rs:2761](D:/data-backup/code/コード保存/discord-rs/src/src/daemon.rs:2761), [child_recovery.rs:778](D:/data-backup/code/コード保存/discord-rs/src/src/child_recovery.rs:778).  
   Standbys are recorded after spawning but never admitted against the budget. Once ten starts are recorded within an hour, an activation failure kills both Gateways; automatic recovery then receives `Unavailable` and becomes permanently Degraded until intervention. This is a consequence beyond the previously fixed missing accounting.  
   **Minimal fix:** gate candidate launches and reserve recovery capacity; treat temporary budget exhaustion as scheduled deferral where appropriate.

6. **P2 · A/B — Audit and rotation I/O can freeze the supervision loop. Confidence: high.**  
   [daemon.rs:963](D:/data-backup/code/コード保存/discord-rs/src/src/daemon.rs:963), [core_units.rs:71](D:/data-backup/code/コード保存/discord-rs/src/src/core_units.rs:71), [botd.rs:785](D:/data-backup/code/コード保存/discord-rs/src/src/bin/botd.rs:785).  
   Synchronous audit `open/write/sync_data` and whole-log copying run inside the owner loop. Stalled storage stops status handling, child supervision and activation deadlines. With an enabled systemd watchdog, it can restart the entire service; the shipped unit currently leaves that watchdog disabled.  
   **Minimal fix:** move disk work to a bounded worker and advance admission only after its durable acknowledgement.

7. **P2 · A — Successful operations can lack terminal audit records without exposing audit failure. Confidence: high.**  
   [daemon.rs:1184](D:/data-backup/code/コード保存/discord-rs/src/src/daemon.rs:1184), [daemon.rs:1202](D:/data-backup/code/コード保存/discord-rs/src/src/daemon.rs:1202), [daemon.rs:1395](D:/data-backup/code/コード保存/discord-rs/src/src/daemon.rs:1395).  
   Fill storage after `requested`: `finish` publishes Succeeded before attempting the terminal write, then only prints an error. There is no retry or Gateway-audit health field. Orderly SIGTERM also abandons in-flight updates without completing their operation audit.  
   **Minimal fix:** retain terminal outcomes pending durable audit, expose audit failure separately from execution success, and finalize/drain them during clean shutdown.

8. **P2 · A — Candidate-log and audit opens omit existing file-safety checks. Confidence: high; requires an unsafe pre-existing path.**  
   [daemon.rs:3158](D:/data-backup/code/コード保存/discord-rs/src/src/daemon.rs:3158), [daemon.rs:963](D:/data-backup/code/コード保存/discord-rs/src/src/daemon.rs:963).  
   Both opens follow symlinks; `.mode(0600)` only affects newly created files. A mistaken `gateway-candidate.log → ipc/core.secret` link lets startup logging corrupt the capability, breaking activation and subsequent reconnects. An existing 0644 audit also stays 0644. This is conditional filesystem corruption, not an unauthenticated remote exploit.  
   **Minimal fix:** reject symlinks and validate the opened descriptor’s type, ownership, link count and permissions.

9. **P2 · B/C — Continuous logging can prevent rotation indefinitely. Confidence: high.**  
   [core_units.rs:75](D:/data-backup/code/コード保存/discord-rs/src/src/core_units.rs:75).  
   If each copy overlaps even one append, the length check always skips truncation. Every later cycle copies a larger file; both the live log and `.1` grow until storage exhaustion. The test writes before and after rotation, not concurrently.  
   **Minimal fix:** use an owned rotating writer or a coordinated reopen protocol; withdraw the bounded-size claim meanwhile.  
   **Doc overclaim:** [operator-guide.md:158](D:/data-backup/code/コード保存/discord-rs/docs/operator-guide.md:158): “ログは稼働中も大きさに上限があります。”

10. **P2 · B/C — The advertised 180-second operation deadline is not global. Confidence: high.**  
    [daemon.rs:1467](D:/data-backup/code/コード保存/discord-rs/src/src/daemon.rs:1467), [daemon.rs:1489](D:/data-backup/code/コード保存/discord-rs/src/src/daemon.rs:1489).  
    The deadline is checked only when Warming returns Warming. A shard reaching Ready at 179 seconds can activate for another 19 seconds and succeed; subsequent shards can also begin after expiry. Restarts remain held meanwhile.  
    **Minimal fix:** enforce the absolute deadline in every phase, using fenced recovery once activation has begun.  
    **Doc overclaim:** [operator-guide.md:151](D:/data-backup/code/コード保存/discord-rs/docs/operator-guide.md:151): “操作全体は 180 秒で打ち切ります。”

11. **P2 · B — `!workers` bypasses overlap deduplication entirely. Confidence: high.**  
    [main.rs:1194](D:/data-backup/code/コード保存/discord-rs/src/src/main.rs:1194).  
    Send `!workers` while both sessions receive events: the old Gateway replies directly, then the standby replays it and replies again. This branch never submits the message ID to `SeenIds`; neither window size nor Core continuity helps.  
    **Minimal fix:** route Gateway-local commands through the same message-ID admission boundary.

12. **P2 · B — The soak report becomes blind to the active Gateway after an update. Confidence: high.**  
    [soak-report.sh:34](D:/data-backup/code/コード保存/discord-rs/deploy/dev/soak-report.sh:34), [daemon.rs:3151](D:/data-backup/code/コード保存/discord-rs/src/src/daemon.rs:3151).  
    Promotion preserves the candidate’s stdout/stderr descriptors, so the active Gateway continues writing `gateway-candidate.log`. The report counts disconnects, unavailable workers and unknown REST outcomes only in `gateway.log`, producing stale or falsely clean observations.  
    **Minimal fix:** include candidate logs and make counters rotation-aware.

For A, I found no new unauthenticated administration path, production Core token transfer, or home-made consensus implementation. Separate Core spawning clears the environment and sends only the capability through stdin ([core_units.rs:174](D:/data-backup/code/コード保存/discord-rs/src/src/core_units.rs:174)); control authentication and role/scope checks remain present. This does not establish comprehensive secret or filesystem isolation.

For **D**, on tomorrow’s Oracle aarch64 server, record the deployed binary hash and effective systemd settings first, then prioritize:

1. **Persistence across two daemon restarts.** Update once, confirm a checkpoint, restart botd, increment `!count`, confirm another checkpoint, restart again. **Proof:** file contents advance after the first restart and the second restart restores that value. A frozen file or rolled-back count confirms finding 1.

2. **Real overlap under backlog pressure.** Hold more than 64 commands before activation with controlled slow dispatch; include `!count`, `!workers` and slash commands. **Proof:** uninterrupted heartbeat send/ACK cadence, one effect/reply per input, unchanged Core PIDs and one surviving Gateway. Record Identify remaining-budget changes too.

3. **Crash recovery at the cutover boundaries.** SIGKILL botd during warming, activation and retirement; separately kill a Core during activation. **Proof:** within configured recovery windows, exactly one Gateway and two responsive Cores remain, old PIDs/zombies disappear, reservations settle, and counters match confirmed checkpoints. The existing separate-mode systemd test does not cover this matrix.

4. **Core liveness rather than PID existence.** Exercise a non-reading old socket and, separately, 16 invalid attaches while botd keeps stdin open. **Proof:** the actual Manager completes takeover, or the failed Core exits and is replaced; a live PID with a permanently unavailable socket disproves recovery.

5. **Storage-fault and audit behavior in an isolated quota-limited runtime.** Force ENOSPC before admission and after admission; maintain concurrent log writes across rotations. **Proof:** pre-admission failure starts nothing; terminal audit failure is visible and recoverable; status remains responsive; logs stabilize across rotation cycles; file modes and targets remain correct.

