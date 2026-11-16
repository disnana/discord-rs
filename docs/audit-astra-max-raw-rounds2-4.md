# Astra Max 再監査の原文（第 2〜4 回、同じセッション）

生の出力。整理と対応状況は [audit-2026-11-16.md](audit-2026-11-16.md)。

## 第 2 回

Reviewed through `3faf015`, read-only and offline. **Seven actionable issues remain.** Scenarios below are code-derived, not reproduced.

| Fix | Verdict |
|---|---|
| P1-1 persisted epoch | Incomplete: findings 1, 2 and 6. |
| P1-3 explicit Core exit | Correct for the reported `serve`-return hang; no new regression found statically. |
| P2-4 3-second writes | Fixes the specific deadline mismatch; does not bound total takeover time (#4). |
| P2-9 rotation | Incomplete: one copy can still run indefinitely (#3). |
| P2-10 update timeout | Correct for the revised admission deadline plus activation grace; checker timing needs adjustment (#7). |
| P2-12 candidate logs | Fixes normal candidate-log coverage; rotation still invalidates comparisons (#5). |

**mtime lifecycle:** [CoreUnits::ensure:152](D:/data-backup/code/コード保存/discord-rs/src/src/core_units.rs:152) renews the secret only while `prepared` is false. Core restarts and repeated `ensure` calls read the existing secret. Linux chmod changes ctime, not mtime. `cleanup` deletes the secret and resets `prepared`; a subsequent ensure recreates it. Thus routine Core restarts do **not** invalidate the stamp—the problems are its ordering and use across layouts.

1. **P1 — Returning to embedded mode silently disables persistence. Confidence: high.**  
   [worker.rs:1821](D:/data-backup/code/コード保存/discord-rs/src/src/worker.rs:1821), [core_state.rs:229](D:/data-backup/code/コード保存/discord-rs/src/src/core_state.rs:229).  
   Run separate mode, save state, then remove `--core-mode separate` while retaining the state directory. Embedded mode still stamps zero; every save is silently rejected against the timestamp-stamped file and marked successful. A later restart loses subsequent changes. The new patch makes this happen even without a Gateway update.  
   **Minimal fix:** use the same durable writer-generation scheme in both layouts, with a fenced transition between them.

2. **P1 — Secret mtime does not provide monotonic incarnation ordering. Confidence: high.**  
   [worker.rs:692](D:/data-backup/code/コード保存/discord-rs/src/src/worker.rs:692).  
   Save with secret creation time `T`, step the clock backward, then restart botd. The new stamp is smaller and persistence silently stops. Clock recovery alone cannot repair it because the secret’s timestamp remains fixed. Reusing the same second after a nonzero epoch also produces a smaller stamp; metadata errors silently substitute zero.  
   **Minimal fix:** durably allocate a monotonic incarnation under exclusive ownership, rather than deriving it from wall time; fail explicitly when allocation fails.  
   **Doc overclaim:** [progress.md:335](D:/data-backup/code/コード保存/discord-rs/docs/progress.md:335), “保存 epoch を botd の起動時刻と合成して世代をまたいで単調に”.

3. **P2 — Four rotation rounds do not bound the copying. Confidence: high.**  
   [core_units.rs:80](D:/data-backup/code/コード保存/discord-rs/src/src/core_units.rs:80).  
   Each tail copy calls unrestricted `io::copy`, which reads until EOF. A writer keeping ahead of the copier can prevent even the first round from completing, blocking botd’s supervision loop and growing both files. The new test has no completion deadline and checks neither a size bound nor synchronized concurrent writing.  
   **Minimal fix:** capture an end offset each round and copy only `end − position` bytes, with an overall work budget.  
   **Doc overclaim:** [operator-guide.md:158](D:/data-backup/code/コード保存/discord-rs/docs/operator-guide.md:158) still promises bounded logs. Its “ごく少量” loss description is also unbounded: descheduling between copying and truncation can lose substantial appended data.

4. **P2 — A stuck handler still prevents takeover and recovery indefinitely. Confidence: high on the path; fault scenario untested.**  
   [worker.rs:4192](D:/data-backup/code/コード保存/discord-rs/src/src/worker.rs:4192), [supervisor.rs:810](D:/data-backup/code/コード保存/discord-rs/src/src/supervisor.rs:810).  
   A handler awaiting an unresponsive dependency has no Core-side timeout. Fencing is checked only between requests; the 3-second write timer never starts. Replacement Health requests fail, while botd sees a live Core PID and does not replace it. This is distinct from the repaired blocked-write mismatch.  
   **Minimal fix:** add a bounded request/drain policy and explicit recovery for an unresponsive Core, preserving unknown-outcome semantics.  
   **Doc overclaim:** [known-limitations.md:33](D:/data-backup/code/コード保存/discord-rs/docs/known-limitations.md:33), “新しい epoch の引き継ぎは最大約 3 秒の遅れで済む”.

5. **P2 — Rotation can make soak comparisons falsely report `OK`. Confidence: high.**  
   [soak-report.sh:36](D:/data-backup/code/コード保存/discord-rs/deploy/dev/soak-report.sh:36), [soak-diff.sh:103](D:/data-backup/code/コード保存/discord-rs/deploy/dev/soak-diff.sh:103).  
   Suppose the first snapshot counts ten disconnects. More occur, rotation clears the log, and the next snapshot counts two. `soak-diff` labels `10 → 2` as `OK`, hiding the intervening failures. Candidate-only directories are also omitted because enumeration still requires `gateway.log`.  
   **Minimal fix:** enumerate both log names independently and use rotation-aware counters; at minimum classify decreases/missing coverage as unknown or `WATCH`.

6. **P3 — The 20-bit clamp merges distinct ownership epochs. Confidence: high; limited reachability.**  
   [worker.rs:696](D:/data-backup/code/コード保存/discord-rs/src/src/worker.rs:696), [main.rs:259](D:/data-backup/code/コード保存/discord-rs/src/src/main.rs:259).  
   Manually supplied/API epochs `1,048,575` and `1,048,576` receive identical persistence stamps. An older Gateway’s delayed flush can therefore overwrite the newer snapshot. Normal botctl operation cannot currently reach this threshold because its journal caps at 1,024 operations, but the CLI/API accepts larger `u64` epochs.  
   **Minimal fix:** reject out-of-range epochs before takeover or persist the full incarnation/epoch pair instead of saturating.

7. **P3 — `update-check.sh` rejects valid grace periods. Confidence: high.**  
   [update-check.sh:95](D:/data-backup/code/コード保存/discord-rs/deploy/dev/update-check.sh:95), [update-check.sh:104](D:/data-backup/code/コード保存/discord-rs/deploy/dev/update-check.sh:104).  
   It stops polling after 180 seconds although activation may now legitimately finish later. It also checks for exactly one Gateway after five seconds, before waiting for the old Gateway’s allowed 30-second retirement. A healthy update can therefore fail the checker, and its selected “new” PID may actually be the retiring old PID.  
   **Minimal fix:** accommodate activation grace, wait for retirement, then take a fresh snapshot and identify the replacement by excluding the old PID.

**ACTIONABLE ISSUES REMAIN: 7**
## 第 3 回

Reviewed through `e48d98c`, read-only and offline. No builds or tests run. Three actionable findings remain:

1. **P1 — Writer stamps can collide or reverse. Confidence: high.**  
   [core_state.rs:306](D:/data-backup/code/コード保存/discord-rs/src/src/core_state.rs:306), [core_state.rs:229](D:/data-backup/code/コード保存/discord-rs/src/src/core_state.rs:229).  
   With file stamp `F = u64::MAX/2`, writers A and B opened before either flushes both receive `F+1`. B saves newer state; A’s subsequent stale save overwrites it. Equal clock readings also permit collisions. A clock rollback while A’s stamp remains unpublished can give B a **smaller** stamp; A’s later flush then disables B’s persistence. At `u64::MAX`, saturation permanently prevents strict ordering.  
   **Minimal fix:** reserve writer IDs using a separate durable counter under a process-shared lock; reject overflow. The new test flushes A before opening B, missing this ordering. Consequently, 「後から起動した Gateway が先に起動していた Gateway に常に勝つ」 in [known-limitations.md:35](D:/data-backup/code/コード保存/discord-rs/docs/known-limitations.md:35), and the resolution claims in progress.md:336 and audit-2026-11-16.md:60, overclaim.

2. **P1 — Core restart can let a retiring Gateway restore stale state. Confidence: high, static interleaving.**  
   [core_server.rs:219](D:/data-backup/code/コード保存/discord-rs/src/src/core_server.rs:219), [worker.rs:2116](D:/data-backup/code/コード保存/discord-rs/src/src/worker.rs:2116), [main.rs:1427](D:/data-backup/code/コード保存/discord-rs/src/src/main.rs:1427).  
   The old Gateway can spend 30 seconds draining HTTP requests before `Manager::shutdown` disables reconnects. If a Core restarts meanwhile, its epoch floor resets to zero. The old Gateway can reconnect first and restore count 10; the replacement’s latest confirmed count is 11. When the replacement reconnects, it adopts the now-nonempty count 10 instead of restoring 11.  
   **Minimal fix:** initialize restarted Cores with botd’s current ownership floor; disable retiring Gateways’ reconnects before draining external responses.

3. **P2 — A temporary request timeout can permanently block reconnection. Confidence: high.**  
   [worker.rs:2103](D:/data-backup/code/コード保存/discord-rs/src/src/worker.rs:2103), [core_server.rs:251](D:/data-backup/code/コード保存/discord-rs/src/src/core_server.rs:251).  
   Pause a Core beyond the request timeout, then resume it. The Manager marks the connection broken but retains its socket. After completing its small response, the Core waits on that old connection. Every replacement connection has the same epoch, so it queues without fencing; its Health times out repeatedly. This persists after the handler has recovered.  
   **Minimal fix:** close the broken session’s write half before reattaching, retaining checkpoint and unknown-outcome bookkeeping.

Other checks: rotation’s captured-end copies and cursor handling look correct; the checker’s 240-second polling and retirement-before-snapshot ordering fix the reported timing error. Both layouts now use the same Persister, and standby allocation occurs at activation. No production literal-epoch caller or new crash-temporary regression was found. An old flush between opening and first saving is handled correctly **provided the writer stamps are strictly ordered**.

ACTIONABLE ISSUES REMAIN: 3

## 第 4 回

Reviewed only `e48d98c..0676122`. Static review; no builds, tests, network, or file changes.

1. **P1 — Disabling reconnects does not stop an attempt already running. Confidence: high.**  
   [worker.rs:2090](D:/data-backup/code/コード保存/discord-rs/src/src/worker.rs:2090), [worker.rs:2134](D:/data-backup/code/コード保存/discord-rs/src/src/worker.rs:2134).  
   A reconnect passes the entry check, awaits connection/preparation, then `stop_reconnecting()` runs. The attempt can still restore stale state, install the connection, and proceed to another Core: neither the restore nor installation checks `reconnect_disabled`. The original retirement race therefore remains.  
   **Minimal fix:** synchronize shutdown with reconnect/restore admission, cancel pending attempts, and check the flag before each attach/restore/install. Already-transmitted restores require Core-side epoch fencing. 「**修正**：停止要求を受けた時点で再接続を止める」 in [audit-2026-11-16.md:67](D:/data-backup/code/コード保存/discord-rs/docs/audit-2026-11-16.md:67) overclaims.

2. **P2 — Saturation makes the process-wide allocator repeatedly issue the same stamp. Confidence: high.**  
   [core_state.rs:309](D:/data-backup/code/コード保存/discord-rs/src/src/core_state.rs:309), [core_state.rs:312](D:/data-backup/code/コード保存/discord-rs/src/src/core_state.rs:312).  
   Opening a file stamped `u64::MAX` sets `LAST` to `MAX`. Every subsequent allocation—including for unrelated directories—successfully performs `compare_exchange(MAX, MAX)` and returns `MAX`. Equal-stamped writers can overwrite newer state.  
   **Minimal fix:** use checked increments for both stored stamp and `LAST`; propagate exhaustion as an error, without falling back to stamp `1`. The “always greater” and unconditional same-process monotonicity claims in [known-limitations.md:35](D:/data-backup/code/コード保存/discord-rs/docs/known-limitations.md:35) need qualification.

3. **P2 — Shutdown during backlog replay reaches the new call late. Confidence: high.**  
   [main.rs:1429](D:/data-backup/code/コード保存/discord-rs/src/src/main.rs:1429), [main.rs:1145](D:/data-backup/code/コード保存/discord-rs/src/src/main.rs:1145).  
   With more than 64 held events and occupied request slots, replay awaits a permit without polling shutdown. A promoted Gateway subsequently told to retire can therefore keep reconnecting before it reaches `stop_reconnecting()`. Both new calls correctly precede their explicit 30-second drains, but do not establish the documented shutdown-receipt boundary.  
   **Minimal fix:** observe shutdown during backlog replay and permit waits, disabling reconnects immediately before further admission.

The write-half fix looks correct: Tokio drops it with `SHUT_WR`; the Core reader sees EOF and closes its request channel after queued work. The drain task shares the same writer `Option`; no lock cycle or `None` panic path was found. Embedded recovery is unaffected. Below exhaustion, the CAS correctly orders allocations across blocking threads, though not concurrent `open` invocation/completion order; cross-process limitations remain documented.

ACTIONABLE ISSUES REMAIN: 3