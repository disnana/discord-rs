# Gateway と Core の分離（本番経路）設計書

状態: 設計案（2026-11-15）。実装は未着手。この文書は読み取り調査だけで書いた。**cargo は一切実行しておらず、コードも変更していない。** したがって「確認済み」と書くのは、コードや既存文書の記述を読んで確かめた事実と、既存文書が実機・CI で確認済みと記録している事実に限る。それ以外は「推測」「要確認」と明記する。

仕様の基準は [architecture.md](architecture.md)（§2 の ShardRuntime、§6 Core の無停止更新、§7 Gateway の並行更新、§8 IPC）。関連: [shard-runtime-design.md](shard-runtime-design.md)、[ipc-protocol.md](ipc-protocol.md)、[local-response-management.md](local-response-management.md)、[known-limitations.md](known-limitations.md)、[progress.md](progress.md)（末尾の 2026-11-15 の実機結果）。

根拠の表記: `パス:行` は 2026-11-15 時点の `main`（477992f）の内容。

---

## 0. 要約（最重要の発見）

1. **試験用モードでも Core は botd の子ではなく、Gateway の子である。** `--allow-test-gateway-switch` で「分離した形」に見えるのは、Core の起動引数と identity の契約（`CoreLaunch`）が使われ、Core が 1 本になり、`credential_free` を検査する、という点だけである。起動するのは今も Gateway の `Manager`（`worker.rs:683-705`、`worker.rs:1545-1548`）で、通信は匿名パイプ（stdin/stdout）のままである。`shard_core_launch` を「昇格」させても、それだけでは「botd が起動した既存の Core に Gateway が接続する」形にならない。
2. **IPC の媒体そのものを替える必要がある。** Core の入出力は子プロセスの stdin/stdout で、親が先に終わると EOF で Core も終わる（`ipc-protocol.md:51`、`shard-runtime-design.md:76`）。`--ipc-path`（`ipc` という名前の予約パス）は引数として渡され検証されるだけで、listen も connect もされない（`worker.rs:3164-3172`、`main.rs:700` は表示のみ）。「既存の Core に接続」には、Core 側の listener、接続断で終了しない契約、接続ごとの状態初期化が要る。**これは起動経路の移設ではなく IPC の改修である。**
3. **分離で追加で得られるものは限定的で、すでに得られている性質は多い。** Core だけが落ちても Gateway が切れないこと、Core のホットスワップ、Gateway 再起動後の `!count`・`!job` の継続は、現在の経路ですでに実機で確認済みである（`progress.md:262`）。分離で追加されるのは主に次の 3 点である。(a) Gateway だけが落ちても Core が生きていて、**snapshot-v3 に載らないメモリ内の状態**（Framework の StateMap、Cog、実行中のハンドラー）が残ること。(b) 新旧の Gateway が**同じ Core を共有できる**こと（Gateway の無停止更新の前提。現状の試験用モードは Gateway の世代ごとに別の Core を起動する）。(c) Gateway の再起動が Core の起動・復元を伴わず、軽くなること。**botd が落ちた場合は何も変わらない**（後述 §4.1。孤児の再採用が未実装で、systemd の `KillMode=mixed` が cgroup を終わらせるため、Core も一緒に失われる）。したがって永続化（`core_state.rs`）は分離後も必要である。
4. **Core を botd が 2 本持つには `ChildKey` の拡張が要る。** 既定の Worker 2 本構成は Manager の内部構造（`workers: Vec<…>`、`core_state.rs` の `Binding.workers`）に深く入っている。一方 botd の `ChildRecovery` の鍵は `(bot_id, shard_id, role)` で、同じ役割の枠を 1 Shard に複数置けない（`child_recovery.rs:36-41`）。Core の台数は設計の分岐点で、§3(f) と §6 で判断を求める。
5. **既存の Core に再接続する Gateway は、Core の状態を古いファイルで上書きしてはならない。** 現在の起動手順は、`core-state.json` を読んで Core に Restore する（`worker.rs:1613-1623`）。生きている Core に再接続した場合にこれを行うと、**メモリ内の新しい状態が最大約 2.5 秒前のファイルで巻き戻る**。接続時に Core の鮮度を判定する新しい合意（capability）が必須である。同様に、受信カーソル（`restore_event_stream`）は「最初のイベントより前に一度だけ」設定できる仕様で（`ipc-protocol.md:82-83`）、Core のプロセス寿命に対して一度である。**接続ごとのリセットが必要**である。

推奨する第 1 段階（詳細は §5）: 本番経路の既定は変えず、**opt-in の `separate` モード**を足す。Linux のみ、Unix domain socket で、Core を botd が起動し、Gateway が接続する。第 1 段階は模擬 Discord の自動試験と、WSL の実機（実 Discord の 1 Shard）で検証できる範囲に絞る。Core の更新・永続化の移設・Resume は含めない。

---

## 1. 現状の整理

### 1.1 比較表

| 観点 | 通常経路（live・embedded） | 試験用モード（`--allow-test-gateway-switch`、模擬 Discord のみ） |
|---|---|---|
| プロセス木 | `systemd → botd → Gateway(src) → Core×2(src --worker)` | `botd → Gateway(世代ごと, 旧新が並存し得る) → Core×1(botd --shard-core)`。Gateway の世代ごとに Core が別プロセス。 |
| Core を起動する者 | Gateway の `Manager`。既定は `env::current_exe()` の `--worker`（`main.rs:827`、`worker.rs:683-705`） | **同じく Gateway の `Manager`。** `Launch` 契約の `core.program` を使うだけ（`worker.rs:1545-1548`、`worker.rs:693-695`）。botd は Core を起動しない（`test_gateway_switch.rs:570-604` は Gateway のみ起動）。 |
| Core の数 | 2（`main.rs:827` 固定。キーで振り分け） | 1（`worker.rs:1547`） |
| 所有者・回収者 | Gateway が子ハンドルを持つ。`kill_on_drop(true)`（`worker.rs:704`）。Core の復旧は Manager 内の `CoreRecovery`（`worker.rs:1092-1257`）。botd は Core の存在を知らない。 | 同じ。回収は Gateway。botd は Gateway の世代だけを `ChildRecovery` で監督する（`daemon.rs:2598-2601` の `role: Gateway`）。 |
| 環境・token | Gateway: token あり（`daemon.rs:2877-2910`）。Core: `env_clear` し identity だけ渡す（`worker.rs:690-692,700-701`。`isolated` 方針） | Core は `credential_free_environment()` を要求（`worker.rs:3146,3176-3180`）。token は元々渡らない。 |
| IPC の媒体と向き | Gateway → Core の匿名パイプ。**Gateway が親で、要求を送り応答を受ける。** listener は存在しない。 | Gateway ↔ Core は同じ匿名パイプ。**さらに** botd ↔ Gateway に 2 本の loopback TCP: (1) 制御: **botd が listen、Gateway が接続**（`test_gateway_switch.rs:548-549,609-615`、`worker.rs:1871-1892`）。(2) 模擬 Discord/collector: **botd が listen、Gateway が接続**（`main.rs:810-816`）。 |
| 認証・identity | Hello で Bot/Shard/`generation_id`/`ownership_epoch` の一致を検査。**認証ではない**（`ipc-protocol.md:27`）。identity は環境変数 `BOT_WORKER_IPC_IDENTITY`（`worker.rs:51,700`）。 | 同じ Hello に加え、botd ↔ Gateway の制御は匿名 stdin 経由の 32 byte 秘密で認証（`test_gateway_switch.rs:609-623`）。**Gateway と Core の世代 ID は同一**（`shard_runtime.rs:350-359` の `gateway != core.identity` 拒否）。 |
| 世代 | Core の世代は Gateway の `GENERATION_ID` から Manager が採番（`worker.rs:135-148`） | Gateway と Core が同じ `generation_id`、`ownership_epoch` は 0 固定（`shard_runtime.rs:282-286`） |
| Core の状態の置き場所 | Gateway の Manager のメモリ（確認済みチェックポイント）。さらに `gateway-state/core-state.json`（`core_state.rs:27`。Gateway が書き、起動時に Restore） | メモリのみ。**永続化は無効**（`worker.rs:1580-1583` が `core_launch.is_none()` で永続化を外す）。 |
| Gateway が落ちたとき | Core も道連れ（stdin EOF で終了、または kill_on_drop）。再起動で新規 Core を起動し、`core-state.json` から復元（実機確認済み: `progress.md:262` (2)(3)） | Gateway 世代ごとに Core が失われる。状態は引き継がれない（模擬の無状態処理のみで成立）。 |
| Core だけが落ちたとき | Manager が補充（実機確認済み）。Gateway は不変。 | 同じ（`CoreRecovery`、`worker.rs:1722-`）。Gateway の admission が Active/Standby のときだけ補充（`worker.rs:1709-1720`）。 |
| botd の通常停止 | `--stop-children-on-exit`: Gateway に stdin の Shutdown を送る → Gateway が Core を drain・回収 → 終了（`daemon.rs:3141-3164`、`botd.rs:794-807`）。 | 同様。Gateway の drain で Core も止まる。 |
| botd の異常終了 | 実機で、botd `kill -9` の後に Gateway は残らず、古い予約が整理された（`progress.md:262` (4)）。systemd は `KillMode=mixed`（`botd.service:69`）。 | 推測: 同じ。試験用の guardian が lease の期限まで生きる設計はある（`shard-runtime-design.md:115`）。 |
| Core の更新 | `worker-path.txt` を Gateway の `Manager::watch` が監視し、Gateway の PID を変えずに入れ替え（実機確認済み）。 | 同じ仕組みだが、試験用 Core 1 本のみ。 |
| Identify / Resume | 再起動のたびに Identify を 1 回消費。ローカル許可（30 秒間隔・1 時間 10 回）で自動復旧（`local_identify.rs:14-27`）。Resume は未実装（`daemon.rs:2364-2366`、`worker.rs:1229`）。 | 模擬の共有予算（`gateway_switch.rs`）。実 Discord は対象外。 |

### 1.2 「分離」の試験用実装が実際に持っているもの

- 起動契約: `CoreLaunch`（`shard_runtime.rs:237-339`）。`shard_runtime::plan` が `--bot-id` `--shard-id` `--total-shards` `--ipc-path` `--state-path` `--management-path` を生成し、`--shard-core`、`--generation-id`、`--ownership-epoch` を足した argv が identity と一致することを `run_shard_core` が検査する（`worker.rs:3113-3174`）。検査の後は通常の Worker ループ `run()` を呼ぶだけである。
- 実プロセス試験: `tests/shard_core_launch.rs`（argv と identity の一致、環境の隔離、改ざんの拒否。親は試験自身が stdin/stdout で話す）、`tests/e2e_gateway_process_switch.rs`（Gateway の無停止切替、Active は 1 つ、旧 Core の REST 結果の帰属など）。
- つまり**ここで検証されたのは「Core の起動契約」と「Gateway の世代切替の状態機械」であって、「Gateway が既存の Core に接続する」ことではない。** 後者は未検証であり、コードも存在しない（推測ではなく、`UnixListener`/`UnixStream` 相当の使用が `worker.rs` に無いことを確認した。ipc への言及は `worker.rs:3131,3167` のみ）。

### 1.3 計画上すでに予約されているもの

`shard_runtime::plan` は Shard ごとに `ipc`、`gateway-state`、`core-state`、`gateway-management`、`core-management` を割り当てている（`shard_runtime.rs:144-153`、`shard-runtime-design.md:33-44`）。Core 欄は `credential_refs` が空、`clear_inherited: true` で、token を持たない前提が計画の型に既に入っている（`shard_runtime.rs:171-186`）。ただし `ipc`、`core-state`、`*-management` は割当だけで、実装はない（`shard-runtime-design.md:44`）。実際の `core-state.json` は `core-state` ではなく `gateway-state` の下にある（`main.rs:798-802`）。

---

## 2. 目標像

### 2.1 構成

```
systemd
└─ botd (所有者・監督者)                         ← KillMode=mixed の主プロセス
   ├─ Core(bot A, shard 0)  [listen: shard-0/ipc]   env_clear, token なし, 世代は botd が採番
   └─ Gateway(bot A, shard 0) ── connect ──► Core   token あり, Discord の WS/REST を所有
```

- **所有者は botd。** Core も Gateway も `ChildRecovery`（`child_recovery.rs`）で監督する。`ChildRole::Core` と `StartMode::Core` は既にエンジンに存在する（`child_recovery.rs:31-34,108-114`）が、daemon は Gateway の枠だけを登録している（`daemon.rs:2598-2601`）。
- **Core が listen し、Gateway が接続する。** 要求・応答の向きは現状と同じ（Gateway が要求し、Core が応答する。Core の側からの自発的な送信は現行のワイヤーには無い）ので、Gateway = クライアント、Core = サーバーとするのが最小の変更である。
- **起動順序は Core が先、Gateway が後。** 停止順序は逆（Gateway を drain してから Core）。
- Gateway の再起動は Core に影響しない。Core の再起動は Gateway に影響しない（Gateway は WS を維持したまま Core へ再接続する）。

### 2.2 分離で得られるもの・得られないもの

誇張を避けるため、すでに得られている性質と分けて表にする。

| 性質 | 現状（embedded + 永続化） | 分離後 | 備考 |
|---|---|---|---|
| Core のみ落ちても Gateway の WS が切れない | **達成済み**（実機: Worker `kill -9`、`progress.md:262`） | 同じ（再起動の担当が botd になる） | 追加で得るものはない |
| Core のホットスワップが Gateway を切らない | **達成済み**（実機、PID 不変） | 担当が botd に移る（§3(f)、第 1 段階では未対応） | 分離は逆に実装を増やす |
| Gateway 再起動後の `!count`・`!job` の継続 | **達成済み**（永続化。実機: 3→4→5→6） | 同じ | `kill -9` では約 2.5 秒分を失う窓がある（`core-state` の仕様）。分離後は Core が生きていればこの窓がない |
| Gateway 再起動後の snapshot 外の状態（StateMap、Cog、View、`wait_for`、実行中ハンドラー） | **失われる**（`known-limitations.md:39`、`ipc-protocol.md:61`） | **Core が生きていれば残る** | **分離の主な追加価値** |
| Gateway 再起動で Core の起動と Restore が不要 | 毎回 Worker 2 本を起動・復元 | 不要 | 再起動の短縮。**数値は未計測。** |
| 新旧 Gateway が同じ Core を共有 | 不可（Core は Gateway の子） | 可能（Core の受け入れ側の改修後） | Gateway 無停止更新の前提。現状の試験用モードは Gateway 世代ごとに別 Core |
| botd が落ちても Core の状態が残る | 残らない | **残らない**（孤児の再採用が未実装。`KillMode=mixed`） | 永続化は必要なまま。再採用を作れば別の設計になる（§4） |
| Gateway 再起動で Discord のイベントを取りこぼさない | 取りこぼす | **取りこぼす** | Resume が別途必要（§4.4）。分離は無関係 |
| Gateway 再起動で Identify を消費しない | 消費する | **消費する** | 同上 |
| Bot token が Core に渡らない | 達成（`env_clear`、`credential_free`） | 同じ | 変化なし。分離でも劣化させない |
| Core の障害と Gateway の障害の再起動予算が独立 | Core は Manager 内、Gateway は botd 内で別々 | botd 内で一元化・観測可能 | 運用上の見通しが良くなる（`botctl status` に Core を出せる） |

**結論: 分離は「Gateway の無停止更新の前提」を作る工事であり、単独で運用上の障害耐性を大きく上げるものではない。** 価値の大きさはロードマップ（Gateway ローリング更新、Resume）を実際にやるかどうかに依存する。やらないなら、永続化の現状で十分である、という判断も妥当である（判断点 J1）。

---

## 3. 移行案（段階的）

各段階で、既存の試験と実機の動作が保たれる順序にする。**旧経路（embedded）は既定のまま維持し、新経路は opt-in のフラグ**で選ぶ（(h)）。

### (a) Core を botd が起動する経路の追加

`shard_core_launch` の契約（`CoreLaunch`）を本番経路へ昇格させるときの差分:

| 項目 | 試験用モードの現状 | 本番経路で必要な変更 |
|---|---|---|
| 起動者 | Gateway の Manager | **botd の daemon**。`daemon.rs:2790-2936`（`Daemon::launch`）はプログラム 1 つ（Gateway）前提なので、Core 用の起動関数と `Entry` の拡張が要る。 |
| 実行ファイル | `botd --shard-core`（`botd.rs:1197`）。Gateway の `src` には無い | 本番の `src` Gateway と同じ成果物を使うか、`botd` を使うかを決める。Worker の中身（`bot_framework::commands()`、`worker.rs:3238`）は `src` クレートにあるので、どちらでも動くはず（推測。要確認）。ホットスワップの成果物（`worker-path.txt` の指す実行ファイル）と同一の契約になるよう、**Core の成果物は `src --worker` と同じ実体にする**のが自然。 |
| 入出力 | stdin/stdout が IPC | stdin は **botd からの制御（bootstrap と Shutdown）専用**、stdout/stderr は `core.log`（`gateway.log` と同様、`daemon.rs:2890-2899`）。**IPC は別の経路**（(b)）。 |
| 世代 | Gateway と同一（`Launch::validate`、`shard_runtime.rs:354`） | **Core 世代と Gateway 世代を分ける。** botd が Core 起動ごとに採番（`LaunchGeneration`、`daemon.rs:2825-2828` と同じ仕組み）。`validate` の同一性要求は separate モードでは使わない。 |
| 起動予約 | Gateway のみ（`daemon.rs:2833-2876`、`.botd-reservation`） | Core にも同様の予約を持たせる。孤児の判定（`scan_gateway_liveness`、`daemon.rs:2006`）は `/proc` の cmdline 走査なので、Core 用の判定を追加する。 |
| 環境 | `env_clear` + identity | 同じ。identity の渡し方は、環境変数ではなく stdin の bootstrap にする方が望ましい（`/proc/PID/environ` が同一ユーザーに読める。`known-limitations.md:82`）。 |
| 復旧 | Manager 内の `CoreRecovery` | botd の `ChildRecovery` に `ChildRole::Core` の枠を登録（`StartMode::Core`）。Core 復旧は Identify を消費しない（`worker.rs:1232-1235` と同じ方針）。 |

### (b) Gateway の live 経路が「子を起動する」代わりに「既存の Core へ接続する」モード

現在の起動: `Manager::start_gateway_pool(env::current_exe()?, 2, deployment)`（`main.rs:827`）。

- **Process の抽象化**: 現在の `Process`（`worker.rs:672-731`）は `ChildStdin`/`ChildStdout` と `ChildOwnership`（子ハンドル）に結びついている。接続型にするには、読み書きを `AsyncRead + AsyncWrite` の組へ一般化し、子ハンドルを持たない変種（`Attached`）を作る。**ここが最も大きい改修で、`worker.rs`（約 3,900 行）の中心部に触る。** 既存の試験（`hot_swap_scenarios`、`worker_*`、`event_seq_ipc` など）が通ることを各コミットで確認する。
- **新しいコンストラクタ**: `Manager::attach_gateway_pool(endpoints, deployment)`（名称は仮）。`start_configured_pool` が行う「spawn → prepare → restore → confirm」のうち、spawn を「connect」に置き換える。
- **`CoreRecovery` の代替**: 接続型では「子の死」ではなく「接続の切断」を検出する。再接続は有界のバックオフで試み、Core の再起動は botd が担う。Gateway は Core を起動しない。
- **`shutdown()` の変更**: 接続型では Core を止めない。接続を閉じるだけ（`worker.rs:2501-` は Worker の stdin を閉じて終了を待つ前提）。Core の停止は botd が行う。
- **ワイヤーの変更（新 capability `core-attach-v1`、推測を含む設計案）**:
  1. Hello に **Core 側の** `boot_id`（Core プロセスの起動ごとの乱数）と、状態の鮮度（`fresh: bool`、または状態の単調増加番号）を載せる。
  2. Gateway 世代と Core 世代を別フィールドにする（現行 Hello は `identity` の完全一致を要求する。`protocol.rs:84-86`）。
  3. **接続ごとの状態の初期化**: `run_with_backend` のローカル変数（`sequenced_events`、`replay_events`、`api_results`、`interaction_state`、`application_started`、`drain_enabled` など。`worker.rs:3249-3257`）は現状プロセス寿命である。接続寿命に変える必要がある。
  4. **イベントストリームのカーソル**: 接続ごとにリセットを許す（現状は最初のイベント前に一度だけ。`ipc-protocol.md:82-83`）。
  5. **未決の外部応答（`api-result-v1` の未決枠、最大 1024。`ipc-protocol.md:49`）**: 前の接続が残したものは、新しい接続のときに「結果不明」として確定させて解放する（解放しないと枠が埋まって新しい接続の応答が詰まる）。
  6. **切断時**: 実行中のハンドラーは継続してよいが、結果の送り先がない。結果は捨てて固定の診断を残す（副作用が起きたかどうかは Core のハンドラー次第で、Gateway からは分からない。現状の「Gateway が落ちれば全部失われる」より悪くはならないが、**Core 側の副作用が実行済みで REST の返信だけが失われる**ケースが残る。embedded でも「結果返却前のクラッシュ」は仕様上の限界として扱われている: `progress.md:182,192`）。
- **接続時のふるまい**:
  - Core が `fresh`（空の状態）→ 従来どおり `core-state.json` を Restore。
  - Core が `fresh` でない → **ファイルを Restore しない。** `snapshot` を要求してチェックポイントを採用する。
  - Gateway が接続できない → Discord にログインする前に有界に再試行し、失敗したら終了する（Identify の無駄な消費を避ける。§3(e)）。

### (c) 状態の置き場所（`core_state.rs` の永続化を Core 側へ移すか）

現状は **Gateway 側の Manager が確認済みチェックポイントを保存**している（`worker.rs:1688-1707`、`core_state.rs:199-297`）。段階を分ける。

| 段階 | 置き場所 | 利点 | 欠点・リスク |
|---|---|---|---|
| 第 1 段階（推奨） | **変更しない**（`gateway-state/core-state.json`、Gateway が書く） | 旧経路と同一のファイル・形式。モードを行き来しても状態が失われない。変更が小さい | Core が単独で落ち、Gateway も同時に落ちたときの鮮度は従来どおり |
| 第 2 段階以降 | Core が自分の状態を `core-state`（計画で予約済み）に書く | Core の再起動を botd が完結できる。Gateway が Restore を持つ必要がなくなる。「Core の状態は Core が持つ」という仕様に合う | 「確認済み」の定義が変わる（Core が書く状態は、返信が Gateway に届く前の状態を含む）。`Binding.workers`、形式、旧ファイルの読み込み互換（旧位置 `gateway-state/` からの読み込み）が要る |

注意（第 1 段階でも重要）: `core_state.rs` の `Binding.workers` が現在の台数（2）と一致しないファイルは**黙って無視される**（`core_state.rs:93-100`）。embedded（2 本）から separate（1 本）へ切り替えると、状態は空で起動する。Core を 2 本のままにすれば避けられる（§3(f)）。

### (d) IPC の認証・identity 検証

要件: 未認証の接続を受け付けない。Bot/Shard/世代を検証する。Core に Bot token を渡さない（AGENTS.md）。

- **媒体**: Unix domain socket（Linux のみ。Windows は対象外）。ディレクトリは `shard-N/` を `0700`、socket を `0600`、所有者は実行ユーザー（systemd の `StateDirectoryMode=0700`、`UMask=0077` と整合: `botd.service:53,57`）。`fs_perms` の検証を使う。Windows 用の Named Pipe は作らない（`cfg(unix)` で隔離する）。
- **接続元の確認**: `SO_PEERCRED`（tokio の `UnixStream::peer_cred()`）で接続元の uid が自分と同じであることを確認する。**ただしこれは同一ユーザーの別プロセス（botd、Core、Gateway、他の手元のプロセス）を区別できない。** 区別の根拠は次の能力秘密である。
- **能力秘密**: botd が Core 起動ごとに 32 byte の乱数を作り、**Core の stdin（匿名パイプ）で渡す**。Gateway へも **Gateway の stdin の bootstrap** で渡す（botd がメモリに保持）。環境変数・argv・ファイルには置かない（`local_control.rs:85-91` と `test_gateway_switch.rs:609` の既存方針と同じ）。比較は `subtle` の定数時間比較（既に依存にある）。
  - Gateway の stdin は現在 Shutdown の 1 行専用である（`main.rs:344-367`、`shard_runtime.rs:13-14`）。**bootstrap を 1 行目、Shutdown を 2 行目と契約する**必要がある。試験用モードの先例: `worker.rs:3034-3060`（BufReader の先読みで後続フレームを奪わないよう 1 byte ずつ読む）。
  - 秘密を生で送るか、チャレンジ・レスポンスにするか: 現在の依存には HMAC 実装が直接ない（`Cargo.toml` は `subtle`、`rustls`+ring feature、`getrandom`。`ring` を直接使うには依存追加が要る。要確認）。**第 1 段階は既存方針（0600 の socket 上で秘密を最初のフレームに載せ、定数時間比較）に合わせる**。チャレンジ・レスポンスは判断点 J6。
- **identity の検証**: Hello に Bot/Shard/total、Gateway 世代、Core 世代、`ownership_epoch` を載せる。Core は **Hello の Bot/Shard が自分の起動引数と一致**しなければ拒否する。Core は受理した最大の Gateway 世代を記憶し、**それより古い Gateway 世代の接続を拒否**する（フェンシングの下地）。`ownership_epoch` は第 1 段階では 0 固定のまま（`worker.rs:135-148` と同じ）。
- **同時接続**: 第 1 段階は Core が**同時に 1 接続のみ**受け付け、新しい（世代が大きい）接続が来たら古い接続を切って置き換える。ローリング更新（新旧 Gateway が同時に接続して Active を切り替える）は後段。
- **Core の token**: 変更なし。`env_clear`、`credential_free_environment()`（`worker.rs:3176-3180`）の検査を維持する。botd が Core に渡すものは、identity と能力秘密と socket パスだけである。
- **同一ユーザーの攻撃者は対象外**（`/proc/PID/environ`、ptrace など）。これは現状の embedded と同じ前提で、この設計で新たに悪化しないが、改善もしない。

### (e) 起動順序と再接続

```
botd 起動
  1. Core を起動 (Core が listen を開始し Ready を stdout に1行 or socket の接続可否で通知)
  2. botd が Core の Ready を確認 (有界。失敗なら Gateway は起動しない = Identify を消費しない)
  3. Gateway を起動 (bootstrap に Core の秘密・socket パス)
  4. Gateway は Discord にログインする「前に」Core へ接続し Hello を済ませる
     (Core が使えないまま Identify を消費しないため)
  5. Gateway が Discord にログイン
```

- **Gateway 再起動**: botd が `ChildRecovery` で Gateway だけを再起動する（Identify はローカル許可に従う。`daemon.rs:2367-2412`）。Core は触らない。Gateway は 4 で既存の Core に接続し、(b) の鮮度判定で状態を引き継ぐ。
- **Core 再起動**: botd が Core だけを再起動する。**Gateway は落とさない。** Gateway は接続断を検出し、有界のバックオフで再接続する。再接続は Core の新しい `boot_id` を見て `fresh` と判断し、自分のチェックポイント（メモリ、またはファイル）から Restore する。再接続できるまでの間の入力は、バッファせず、Interaction には固定の ephemeral 応答（M2 の枠組み: `main.rs` の `reject_interaction`）を返し、prefix は破棄して記録する。
- **Core が Degraded（再起動予算切れ）**: Gateway は WS を維持し続けるのか、終了して Shard を解放するのか。分散 failover との関係（Gateway が接続し続けて Active に見えるが処理できない）を含め、判断点 J8。
- **接続の再試行がバックオフで暴走しない**ことと、再接続が Identify を消費しないことを試験で確認する。

### (f) 既存の Worker 2 本構成（ホットスワップの根拠）との関係

2 本構成は Manager の内部構造に埋まっている: `workers: Vec<RwLock<Arc<Generation>>>`（`worker.rs:1365`）、キーによる振り分け、`core_state.rs` の `Binding.workers` と `states: Vec<…>`、各枠が独立した `CoreRecovery` を持つこと（`worker.rs:1109-1116` のコメント）。ホットスワップ（`Manager::replace`、`worker.rs:2002-`）は枠ごとに「候補を起動 → Hello → 状態復元 → 配送先切替 → 旧世代 drain」を行う。**2 本であることが可用性に必須かどうかは、読んだ範囲では断定できない**（旧世代は候補の準備中も動き続けるので、1 本でも切替中に落ちない構造に見える。推測。要確認）。ただし 2 本は処理の並行度を提供している（Worker の要求処理は直列。`ipc-protocol.md:55`）。1 本にすると**並行度が半減する**可能性がある。

選択肢:

| 案 | 内容 | 利点 | 欠点 |
|---|---|---|---|
| **A（推奨）** | botd が Shard あたり Core を **N プロセス**（現状どおり 2）持つ。socket は `ipc-0`、`ipc-1`。`ChildKey` に `slot` を足す | Manager の振り分け・永続化・ホットスワップの構造を保てる。embedded との状態ファイル互換。並行度維持 | `ChildKey` の全構築箇所を更新（grep で 8 ファイル 13 箇所: `botd.rs`、`child_recovery.rs`、`daemon.rs`、`daemon/test_gateway_switch.rs`、`worker.rs`、`tests/child_recovery*.rs`）。architecture.md の「Core 1 プロセス」と文言が異なる（§2 の `Worker` は Core 内の機能とされている: `architecture.md:96`） |
| B | 1 Core プロセスが N 本の実行枠を持つ（1 接続で内部分配、または N 接続） | architecture.md の記述に近い。botd の監督は 1 枠 | `run_with_backend` の直列ループの大改造。ホットスワップの単位が変わる。状態ファイル形式の変更 |
| C | Core 1 プロセス・1 実行枠（並行度 1） | 最も単純 | 並行度の低下（未計測）。状態ファイルの台数不一致で空起動になる |

**第 1 段階は A**。architecture.md への注記（Worker 2 本は Shard 内の処理分担であり、別 Shard ではない: `shard-runtime-design.md:3`）は既存と整合する。

ホットスワップ（Core 更新）の扱い:
- embedded では従来どおり Gateway が行う。
- **separate では第 1 段階は未対応**とし、`worker-path.txt` の監視を無効にして警告する。Core の更新は「botd が新しい Core 群を起動し、Gateway が新しい endpoint へ切替・旧を drain」という別の設計が要る（後段。方向性の案: botd が `core-endpoints.json` のような宣言を原子的に更新し、Gateway が `Manager::replace` と同様に候補へ接続→Restore→切替→旧の drain、旧 Core は drain 完了を自分で検知して終了し botd が回収。ただし**未設計・未検証**）。

### (g) systemd / botd の停止順序と `--stop-children-on-exit` の整合

現状: `shutdown_children` は全 Shard の Gateway に graceful stop を送り、全体 30 秒（`STOP_CHILDREN_DEADLINE`、`botd.rs:812`）で回収を待つ。強制終了はしない（`daemon.rs:1238-1261`）。unit は `KillSignal=SIGTERM`、`KillMode=mixed`、`TimeoutStopSec=60`（`botd.service:68-70`）。

separate モードでの順序:
1. 全 Shard の Gateway へ Shutdown（drain。受け付け済みの REST の結果を Core に返す）。
2. Gateway の終了を回収。
3. Core へ Shutdown（stdin の同様のフレーム）。回収。
4. 30 秒の予算を 2 段で配分する（例: Gateway 20 秒・Core 10 秒。**Gateway の drain 上限は 30 秒（`shard-runtime-design.md:72`）なので、現状の 30 秒予算に収まらない可能性がある。** `TimeoutStopSec` の見直しが要るかは要確認）。
5. Gateway が停止に失敗したら Core は止めない（Gateway が生きている間に Core を止めると、drain 中の応答が失われる）。

停止の単位:
- `botctl stop <bot>` は Shard の Gateway → Core の順に止める。
- 新規に **「Gateway だけ再起動」**（運用上の価値が最も高い操作）を足す。Core のみの再起動は別操作。

`KillMode=mixed` との関係（**推測を含む**）: botd 主プロセスが異常終了すると、systemd は cgroup の残りのプロセス（Gateway と Core）を終了させる。実機でも botd `kill -9` の後に Gateway は残っていなかった（`progress.md:262` (4)）ので、Core も同様になると推測する。**botd の再起動を跨いで Core を生かすには、(1) 孤児の再採用の実装（`known-limitations.md:21` で未実装。PID・世代・OS 開始時刻・認証済み IPC による再照合: `shard-runtime-design.md:76`）、(2) `KillMode` の変更（`process` など）、(3) 再採用時の世代・epoch の永続化が必要**で、この設計の範囲外とする。今回の目標像では、**Core の寿命は botd の寿命を超えない**。

### (h) 旧経路の併存と切り替えフラグ

- botd に **`--core-mode embedded|separate`**（既定 `embedded`）を足す。`USAGE`（`botd.rs:25`）とパース（`botd.rs:50-143`）の更新が要る。`--stop-children-on-exit` はこのモードで拡張する（試験用モードとの併用は現状どおり拒否: `botd.rs:255-256`）。
- Gateway へは `--core-endpoint <ABS>`（slot ごとに繰り返し）を渡し、**bootstrap（秘密）は stdin**。`GatewayOptions::parse` は `launch.len() == 7` を強制している（`main.rs:262-267`）ので更新が要る。引数がなければ従来どおり `start_gateway_pool`（embedded）を使う。
- Bot ごとの切替（設定ファイル項目）は第 1 段階では作らない。botd 全体のフラグ 1 つにする。
- **維持期間**は判断点 J2。目安: separate が実機で十分に検証され、ローリング更新に使われるようになるまで embedded を残す。
- 試験用モード（`--allow-test-gateway-switch`）は触らない。その `CoreLaunch` の契約は separate の Core 起動で再利用できる部分（argv と identity の検査）が多いが、統合は後段。

---

## 4. リスクと未解決

### 4.1 障害時の挙動

| 障害 | embedded（現状） | separate（目標。**推測を含む設計上の期待**） |
|---|---|---|
| Core だけ落ちる | Manager が補充。Gateway 不変（確認済み） | botd が Core を再起動。Gateway は WS を維持し再接続。切断中の入力は拒否・破棄。状態は Gateway のチェックポイントから Restore |
| Gateway だけ落ちる | Core も失われる。再起動で Core を新規起動・永続化から復元（確認済み） | Core は生きる。Gateway が再起動し、既存の Core の状態を採用（Restore しない）。Identify は 1 回消費（Resume は未実装） |
| Core と Gateway が同時に落ちる | 全て失われる。永続化から復元 | Core が botd に再起動され `fresh`。Gateway はチェックポイント（メモリが無ければファイル）から Restore |
| botd が落ちる（`kill -9` など） | systemd が botd を再起動（約 6 秒、確認済み）。Gateway も Core も失われる | 同じ。**Core も失われる**（再採用が未実装）。永続化が必要。起動時の古い予約の掃除は Core にも必要 |
| `systemctl stop` | 確認済み（残りプロセス 0、`core-state.json` が残る） | Gateway → Core の順の graceful。順序と期限の試験が要る |
| Core が無応答（ハング） | Manager の timeout で破損扱い・補充 | 接続型は切断を検知するのみ。**ヘルスチェックの責務**（botd か Gateway か）を決める必要がある（J7） |
| ソケットが残る（古い socket ファイル） | 該当なし | Core 起動時に、既存の socket に接続できるなら起動を拒否（別の Core が生きている）、接続できないなら削除して再作成。シンボリックリンクは辿らない |
| 孤児の Core（botd が手動で止められ systemd 外） | 該当なし（Gateway と一緒に終了） | Core が単独で残り得る。起動予約と `/proc` 走査で新しい botd が検知する（Gateway と同様）。自動 kill はしない（既存方針） |

### 4.2 互換性

- **ワイヤー**: 新 capability `core-attach-v1`（仮）を足す。未交渉の相手には従来の挙動（embedded のパイプ）を維持する。`Hello` の `identity` 完全一致（`protocol.rs:84-86`）を変更するため、`ipc_handshake` 系の試験を拡張する。`protocol.rs` の `Envelope/Control/Data`（MessagePack）は現行ワイヤーではない（`ipc-protocol.md:9`）ので、今回も JSON 行を維持する。
- **状態ファイル**: 第 1 段階は変更しない。台数の不一致は無視される（§3(c)）。
- **ホットスワップ成果物**: Core の実行ファイルは `src --worker` と同じ契約で作る。capability の最小集合（`api-result-v1` など。`shard-runtime-design.md:60`）の検査は接続型でも維持する。
- **Windows は対象外**。接続型は `cfg(unix)` に隔離し、Windows 向けの Named Pipe は作らない。

### 4.3 運用上のリスク

- **Identify の無駄な消費**: Core が使えないまま Gateway が Discord にログインし、すぐ終了すると、再起動のたびに Identify を消費する（ローカル許可は 1 時間 10 回。超えると `Degraded`: `local_identify.rs:27`）。§3(e) の順序（Core に接続してからログイン）で避ける。
- **状態の巻き戻り**: §0 の 5。必須の試験項目にする。
- **未決の外部応答の枠詰まり**: §3(b)-5。
- **WSL の注意**: WSL2 はアイドルで VM ごと停止する（`progress.md:263`）。実機検証中は端末を開いたままにする。
- **ログ**: `core.log` を新設する場合、秘密を書かない（既存の `redact`/診断の方針を踏襲）。Core の stdout を IPC に使わないので、`gateway.log` と同じ扱いができる。

### 4.4 Identify・Resume（分離とは別だが、分離の最終目的に関わる）

- 分離そのものは Identify の消費を変えない（再起動のたびに 1 回: `known-limitations.md:83-84`）。
- Gateway の無停止更新（`progress.md:231-234` のロードマップ）には、旧 Gateway から新 Gateway へ `session_id`・`seq`・`resume_gateway_url` を保護して渡し、新 Gateway が Resume することが必要。twilight-gateway 0.18 は `Shard::session()`（`shard.rs:408`）、`Shard::resume_url()`（`shard.rs:400`）で取得でき、`ConfigBuilder::session`/`resume_url`（`config.rs:385,400`）で再開でき、`Session` は serde 対応と文書化されている（`session.rs`）。**これは読んだだけで、実機では未検証。** 実 Discord での Resume の成否、旧接続が生きている間の新接続の Resume の挙動は**未確認**。
- 保存の順序（推測を含む設計上の注意）: `seq` を保存するタイミングが Core の受理（`event_accepted`）より前だと取りこぼし、後だと Resume 時に重複が出る（at-least-once）。保存は「Core が受理した後」に寄せ、重複は `event_key` による冪等化とアプリ側に委ねる。**`session_id` は秘密として扱い（0600）、ログに出さない。**
- 分離との関係: 新旧 Gateway が同じ Core を共有できることが前提であり、第 2 の接続を受け付ける Core の改修（§3(d) の「同時接続」）が必要。これは後段。

### 4.5 試験の増やし方

1. **単体**: 接続型 `Process` のフレーム読み書き（`tokio::io::duplex` で socket を使わずに）。Hello の拒否（秘密不一致、Bot/Shard 不一致、古い Gateway 世代、世代の分離）。接続ごとの状態の初期化（カーソル、未決枠、capability）。Core の鮮度の判定と「Restore しない」の分岐。stale socket の処理。ファイルモード（0700/0600）。
2. **実プロセス（Core だけ）**: `tests/core_attach.rs`（新規）。実際の Core を起動し、試験側が Gateway 役で接続・切断・再接続する。切断後も Core の PID と状態が保たれること、秘密なしの接続が拒否されること、2 接続目が 1 接続目を置き換えること。
3. **E2E（botd + 模擬 Discord）**: 既存の `e2e_shard_runtime.rs` の構成を `--core-mode separate` で再実行（Bot A の Shard 0/1、Bot B の Shard 0）。模擬の `Crash` 入力（`main.rs:732`、`exit(86)`）で Gateway を落とし、Core の PID が不変であることを確認する。Core の `kill -9` で Gateway の PID が不変であること。順序つきの停止。停止後に子プロセス・予約・socket が残らないこと。
4. **CI の分類**: 実プロセス試験は `.github/scripts/test-shard.sh` の `PROCESS` 群（`test-shard.sh:12`）に追加しないと rest に入る。反復 stress（`stress.yml`）に `process`/`switch` スイートがあるので、新試験も追加して 20 回程度の反復で安定性を確認する（過去の不安定の経緯: `known-limitations.md:61-66`）。
5. **systemd smoke**: `deploy/dev/wsl-systemd-smoke.sh` と `.github/workflows/systemd-smoke.yml` に separate モードの手順を追加（unit は `--core-mode` を含む drop-in で）。
6. **障害注入**: Core のハング、Core の起動失敗の連続（Degraded）、socket の削除、botd `kill -9`、接続の途中切断、フレーム過大、秘密不一致の連続（接続枯渇）。

---

## 5. 第 1 段階の提案

### 5.1 範囲

**入れる**
- botd の `--core-mode separate`（opt-in。既定は embedded のまま）。Linux のみ。
- Core の socket サーバー化（接続型の Core）と、Gateway 側の接続型 Manager。**1 接続のみ、能力秘密、Bot/Shard/世代の検査、接続ごとの状態初期化、鮮度判定（生きている Core には Restore しない）。**
- botd による Core の起動・監督（`ChildRole::Core` 枠、起動予約、`core.log`）と起動・停止の順序。
- Core 数は現状の 2（案 A。`ChildKey` に `slot` を追加）。
- 状態ファイルは変更なし（Gateway 側の永続化を継続）。

**入れない**（理由つき）
- Core のホットスワップの separate 対応（別設計が要る。embedded では引き続き動く）。
- 永続化の Core 側への移設（段階 2。互換の議論が要る）。
- 同時 2 接続（Gateway ローリング）、Resume、`ownership_epoch` の運用（段階 3 以降）。
- 孤児の再採用、botd 死亡時の Core 生存（設計外）。
- Windows。
- 設定ファイルによる Bot ごとの切替。

### 5.2 触るファイル（予定。調査に基づく見積もりで、実装時に確定）

| ファイル | 変更 |
|---|---|
| `src/src/worker.rs` | `Process` の読み書きの一般化と接続型の変種、`Manager::attach_gateway_pool`、接続断の扱い、`shutdown` の接続型分岐、Core のサーバーモード（`run_with_backend` の接続ごとの状態の初期化、listener、`run_shard_core` の拡張または新エントリ） |
| `src/src/protocol.rs` | Hello の拡張（`core-attach-v1`）。既存の `negotiate` の後方互換 |
| `src/src/shard_runtime.rs` | separate 用の Core/Gateway 起動仕様（slot、endpoint、bootstrap）。`test_switch` は変更しない |
| `src/src/child_recovery.rs` | `ChildKey` に `slot`（既定 0）。構築箇所の更新 |
| `src/src/daemon.rs` | Core 用の `Entry`・起動・予約・停止の順序・`shutdown_children` の 2 段化、`ProcessRecoveryHost` の role 分岐 |
| `src/src/bin/botd.rs` | `--core-mode`、`USAGE`、停止の期限配分 |
| `src/src/main.rs` | `--core-endpoint` と stdin の bootstrap、接続→ログインの順序、`GatewayOptions::parse` |
| `src/src/core_state.rs` | 変更は最小（接続型の起動で Restore を省略する分岐は `worker.rs` 側） |
| `src/tests/core_attach.rs`（新規）、`src/tests/e2e_shard_runtime.rs` 等 | §4.5 |
| `.github/scripts/test-shard.sh`、`stress.yml`、`systemd-smoke.yml`、`deploy/dev/wsl-systemd-smoke.sh` | 試験群・smoke の追加 |
| `docs/*` | `ipc-protocol.md`、`shard-runtime-design.md`、`operator-guide.md`、`known-limitations.md`、`progress.md` の更新は実装時 |

### 5.3 実装の分け方（各コミットで既存の試験が通る）

1. **1-A（振る舞い不変のリファクタリング）**: `Process` の読み書きの一般化。embedded のみ。既存の全試験が通ること。
2. **1-B（Core サーバーと接続型 Manager、botd なし）**: `core_attach` の実プロセス試験と単体試験。
3. **1-C（botd の配線）**: `ChildKey.slot`、Core の起動・監督、停止の順序、`--core-mode`。模擬 Discord の E2E。
4. **1-D（運用の整備）**: systemd smoke、WSL の手順書、`core.log`、`botctl status` への Core の表示（最小）。

### 5.4 検証

- **ローカルの重い全体試験は回さない**（運用方針）。WSL（Ubuntu 24.04）では対象の試験だけを `-j 6` 程度でビルド・実行し、反復と全体は GitHub Actions（`ci.yml` の 5 シャード、`stress.yml` の `process`/`switch` 反復、`systemd-smoke.yml`）で確認する。
- 成功の基準: 既存の全試験が従来どおり成功すること（embedded の回帰がない）、新試験が 20 回反復で安定すること。
- **実機（WSL2、実 Discord の 1 Shard）で確認すること**（`docs/real-operations-verification.md` の手順を基にする）:
  1. `systemctl start` → ログイン成功、`/ping`・`!ping`・`!count`。
  2. Gateway `kill -9` → 自動復旧（Identify はローカル許可）。**Core の PID が不変**。`!count` が継続し、**Restore のログが出ない**（巻き戻りがない）こと。
  3. Core（2 本のうち 1 本）`kill -9` → botd が再起動。**Gateway の PID が不変**。`!count` の継続。
  4. Core 2 本とも `kill -9` → Gateway は不変、再接続。切断中の入力は拒否される。
  5. `botctl stop` / `systemctl stop` → Gateway → Core の順で止まり、残りプロセス 0、予約 0、`core-state.json` が残る。
  6. botd `kill -9` → systemd が再起動。**Core も入れ替わる**（期待どおり。記録する）。
  7. 記録する数値: Gateway 再起動の所要時間（embedded と比較）。
  8. 確認できないこと（記録する）: 長時間稼働、複数 Shard、Resume、レート制限。
- 受け入れ後に `progress.md` に結果を記録し、`known-limitations.md` と `shard-runtime-design.md` の記述（「Core 欄は予約」「実 Core は Gateway 所有の Worker プール」）を更新する。

---

## 6. ユーザーに確認すべき判断点

| # | 論点 | 推奨 | 影響 |
|---|---|---|---|
| J1 | そもそも分離を進めるか。永続化の現状で足りるなら見送る選択がある。分離の追加価値は主に「snapshot 外の状態の保持」と「Gateway ローリング更新の前提」（§2.2） | Gateway ローリング更新を実際にやるなら進める | 作業量は大（`worker.rs` の中心部の改修） |
| J2 | 旧経路（embedded）の維持期間 | separate が実機で十分に検証され、ローリング更新に使われるまで維持 | 2 経路の保守コスト |
| J3 | Core の台数: 案 A（N プロセス、`ChildKey.slot`）／B（1 プロセス内 N 枠）／C（1 本） | 第 1 段階は A | architecture.md の「Core 1 プロセス」との文言差、並行度、状態ファイル互換 |
| J4 | 第 1 段階の範囲（§5.1）。特に Core ホットスワップを separate で後回しにしてよいか | 後回し（embedded では動く） | separate の間は Core 更新が再起動（短い欠落）になる |
| J5 | botd 死亡時に Core も失う前提でよいか（孤児の再採用と `KillMode` の変更は設計外） | 設計外 | 永続化は必要なまま |
| J6 | IPC 認証: 0600 socket 上で秘密を最初のフレームに載せる（既存方針）か、チャレンジ・レスポンス（`ring` の直接依存を追加）か | 第 1 段階は既存方針 | 依存追加には事前の `cargo add`/`cargo fetch` が必要 |
| J7 | Core のハングの検知の責務（botd のヘルスチェックか、Gateway の timeout か） | Gateway の timeout で切断→botd が再起動要求、の案を詳細化してから | 誤検知による再起動ループの設計 |
| J8 | Core が Degraded のときの Gateway の挙動（WS を維持するか終了するか） | 当面は維持して固定の応答を返す。failover との関係は別途 | 分散時の誤った Active 表示 |
| J9 | Core の成果物: `src --worker` と同一にするか、`botd --shard-core` を使うか | `src --worker` と同一（ホットスワップの成果物契約と揃う） | デプロイ物の構成 |
| J10 | `core.log` の新設と `botctl status` への Core の表示を第 1 段階に含めるか | 含める（最小） | 運用の見通し |
| J11 | 実 Discord の実機検証（§5.4）の実施者と時期 | ユーザー（WSL） | 完成の宣言の条件 |

---

## 付録 A: 主な根拠の一覧（読んだ場所）

- `src/src/main.rs:262-267,344-367,782-867`（起動引数の制約、shutdown_input、live 経路の Manager 起動）
- `src/src/worker.rs:51,56-148,672-731,1092-1257,1360-1374,1534-1682,1688-1707,1709-1722,1865-1942,2501-2547,2993-3060,3113-3180,3232-3257`
- `src/src/shard_runtime.rs:90-210,212-413`
- `src/src/daemon.rs:1234-1261,2006-2049,2346-2433,2598-2601,2790-2936,3141-3164`
- `src/src/daemon/test_gateway_switch.rs:539-707`
- `src/src/child_recovery.rs:1-215,567-600`
- `src/src/core_state.rs`（全体）、`src/src/local_control.rs:1-120`、`src/src/local_identify.rs:1-70`、`src/src/protocol.rs:70-100`
- `src/src/bin/botd.rs:25,742-812`、`deploy/systemd/botd.service:30-80`
- `src/tests/shard_core_launch.rs`、`.github/scripts/test-shard.sh:1-14`
- twilight-gateway 0.18.0（`~/.cargo/registry` の `shard.rs`、`config.rs`、`session.rs`）
- `docs/architecture.md` §2・§6・§7・§8、`docs/shard-runtime-design.md`、`docs/ipc-protocol.md`、`docs/local-response-management.md`、`docs/known-limitations.md`、`docs/handoff.md`、`docs/operator-guide.md`、`docs/progress.md`（2026-11-14〜15）

## 付録 B: この調査で確認していないこと

- 何も実行していない（cargo、試験、実機のいずれも）。したがって、上記の「達成済み」は既存文書の記録を引いたものである。
- 1 Core 構成で可用性が保てるか（§3(f)）、`src --worker` が Core として振る舞うために必要な差分、HMAC 依存の有無の最終確認、`TimeoutStopSec` の要否、`KillMode` の cgroup 挙動（推測）は、実装前に確認する。
- 実 Discord での Resume、Gateway ローリング更新、複数 Shard での separate モードは未検討または未検証である。
