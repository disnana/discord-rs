# `!workers` の重複応答を抑える設計

## 現状と範囲

- Gateway は非 Bot の `MESSAGE_CREATE` で `!workers` を特別扱いし、`manager.describe_workers()` の結果を直接 Discord に送り、そのまま戻る（`src/src/main.rs:1265`, `src/src/main.rs:1280`）。通常のメッセージ経路は channel ID で応答を予約し、message ID を `source_id` として Core に渡す（`src/src/main.rs:1295`, `src/src/main.rs:1308`）。
- Core の `SeenIds` は全接続で共有される Core 内メモリで、最初の ID を記録し、重複を応答なしにする。直近 4096 件を保持し、Core 再起動時は空になる（`src/src/worker.rs:3670`, `src/src/worker.rs:3685`, `src/src/worker.rs:3694`, `src/src/worker.rs:4177`）。
- `describe_workers` は Manager が各 Worker の情報を集める（`src/src/worker.rs:1652`）。したがって一覧の生成と Discord HTTP は Gateway 側に残す。
- 通常経路は応答ありなら HTTP 後に `finish_response`、応答なしなら `confirm` を使う（`src/src/main.rs:1317`, `src/src/main.rs:1330`, `src/src/main.rs:1341`）。Core は応答ありの操作を保留し、対応する `ApiResult` を受け付ける（`src/src/worker.rs:3952`, `src/src/worker.rs:4020`, `src/src/worker.rs:4254`）。

## 提案

1. `Request::WorkersDiagnostic { key, source_id }` を追加する。`key` は channel ID、`source_id` は必須の message ID。既存 Dispatch と同じ channel のルーティング、認証済み Bot/Shard 範囲と `SeenIds` を使い、文字列コマンドへの偽装はしない。現在の Manager は key から Worker を選ぶ（`src/src/worker.rs:2641`）。同じメッセージの両 Gateway が同じ Core 世代に到達することが効果の前提。
2. Gateway は `begin_response_delivery(key)` 後に型付き要求を `ResponseDispatch` で送り、Core は重複なら応答なし、初回なら専用の型付き「一覧を送る」結果を返す。既存の `ResponseDispatch` の許可対象、結果判定と保留登録をこの結果にも拡張する（現状 `src/src/worker.rs:3958`, `src/src/worker.rs:4254`）。重複は `guard.confirm()` で完了する。
3. 初回の Gateway だけが `describe_workers()` を呼び、既存の長さ制限・メンション抑止・HTTP タイムアウトを適用して送信する（現状 `src/src/main.rs:1278`, `src/src/main.rs:1281`, `src/src/main.rs:1318`）。成功・HTTP エラー・結果不明のいずれも `finish_response` に渡す。これは発行元世代に `ApiResult` を送り、操作 ID・結果・identity・PID の一致を確認する（`src/src/worker.rs:2787`, `src/src/worker.rs:2814`）。HTTP の成功と Core の ACK は別々に観測する。

## 障害時の扱い

- 最小案は既存 `SeenIds` の挿入時点を変えない。HTTP 失敗、不明、送信前の Gateway 障害でも同じ ID は窓内で抑止される。自動 HTTP 再試行はしない。利用者は新しい `!workers` メッセージを送れば新しい ID で再実行できる。これは元の要求の応答を保証しない代わりに、不明な HTTP の再送による二重投稿を避ける選択である。
- ID は無期限には保持されず、4096 件からの押し出しや Core 再起動で再受理され得る（`src/src/worker.rs:3685`, `src/src/worker.rs:3695`, `src/src/worker.rs:3711`）。再配信がなければ元の要求は自動回復しない。Discord 側で投稿済みか不明な場合、再受理後の重複もあり得る。exactly-once は保証しない。
- Core が結果を返した後に ACK が失われても、Gateway は新世代へ結果を振り替えず、HTTP を再試行しない既存方針を維持する（`src/src/worker.rs:2787`, `src/src/worker.rs:2840`）。Core 間の切替・再起動や異なる Core へのルーティングをまたぐ重複排除は対象外。

## IPC と段階的更新

- `workers-diagnostic-v1` を任意 capability として明示広告・Hello 交渉し、要求送信前に対象 Core の実装済み広告と交渉結果を確認する。既存の Health/Hello は古い peer による optional の反射を実装の証拠としない（`src/src/worker.rs:845`, `src/src/worker.rs:868`, `src/src/worker.rs:881`）。仕様も capability 交渉を要求する（`docs/architecture.md:345`, `docs/architecture.md:347`）。未知 variant を旧 Core に送らない。
- 新 Gateway / 旧 Core では `!workers` を送信せず、互換性不足をログ・管理状態に表示する。直接送信へのフォールバックはしない。全対象 Core の capability を確認してから新 Gateway を有効にする。
- 旧 Gateway はこの制限を理解せず直接応答する。旧 Gateway と新 Gateway の重複期間には重複を防げないため、その組合せでは Gateway の重複起動を避ける停止切替を運用条件とする。旧 Gateway の退役後に通常の rolling update を許す。旧 Gateway の動作を新 Core の交渉だけで変更できるとは仮定しない。
- 実装時は新旧 Core・Gateway の組合せ、全 Worker の確認、切替時の再接続・世代変更を明示的に検証する。認証、Bot/Shard 分離、token の Gateway 限定を維持し、新たな遠隔管理経路や独自コンセンサスは導入しない。

## テストと実装順序

1. capability 広告・交渉、旧 Core の拒否、未知 variant を送らないことを単体テストする。送信先世代が変わる場合も確認する。
2. 型付き要求と既存 `SeenIds` の統合を追加し、同一 ID・異なる ID・4096 件の押し出し・再起動、別 Bot/Shard、重複時の応答なしを単体テストする。
3. ガードと結果の追跡を拡張し、成功、確定 HTTP エラー、タイムアウト、不明、ACK 消失、送信前障害での抑止と新規メッセージによる再試行を検証する。
4. separate mode の実プロセスと mock Discord で、二つの Gateway から同じ `source_id` を同一 Core に同時投入し、HTTP 投稿が 1 件、結果が発行元 Core に届くことを確認する。異なる ID、旧 Core、新旧 Gateway 混在、Core 再起動と障害注入も別ケースで調べる。
5. Ubuntu で対象テストを実行し、条件と結果・残るリスクを `docs/progress.md` に記録してから導入する。

## 今回の進捗・未検証

設計のみ。実装・テスト・cargo 実行・Discord 実機検証は行っていない。二つの Gateway が同一 Core 世代へ到達すること、切替時の実際の重複率、提案のプロトコル互換性と障害時挙動は未検証。上記の停止切替を実運用で実行できるかも未確認。
