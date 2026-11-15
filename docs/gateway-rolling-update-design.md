# Gateway の無停止更新（ローリング）の本番化：設計

状態：**設計案（未実装）**。V1（同一 Shard の 2 セッションが同時に有効で両方にイベントが届く）は実 Discord で確認済み（progress.md）。前提となる分離（`botd --core-mode separate`）は実装・実機確認済み（[progress.md](progress.md)、[real-separate-verification.md](real-separate-verification.md)）。この文書は、コードと既存の設計文書を読んだ結果と、**読んだだけで実機・実 Discord では確かめていないこと**を分けて書く。AGENTS.md の方針：独自のコンセンサスは実装しない／Core に Bot token を渡さない／未認証の遠隔管理を許可しない／未検証の保証を完成と主張しない。

## 1. 目的と範囲

Gateway のバイナリを、Discord の WebSocket を長く切らずに（できれば 1 回の再接続も無駄にせず）新版へ入れ替える。Core は botd が持ち続けるので、Gateway の更新とは独立に生き続ける。

**入れる**：単一ホット（単一 botd）、Linux、separate モード、Shard 単位の更新。
**入れない**：複数ホストの failover、Raft（本番は起動拒否のまま）、Core 自体の更新、embedded モードでの Gateway 更新。

## 2. 既にあるもの（読んで確認した）

- `src/src/gateway_switch.rs`：純粋な状態機械（`Stable → Spawning → Warming → PausingOld → AwaitingCandidateAuthority → ActivatingCandidate → DrainingOld → Stable`、失敗側に `Quiesce…/RecoveryRequired`）。候補（Standby）は「コマンド実行を無効にした READY 済みの Gateway」。epoch は `OwnershipCoordinator` からだけ得る。REST の結果は元の Core 経路を保つ。
- `src/src/daemon/test_gateway_switch.rs`：模擬 Discord でこの機械を botd に結線した試験専用の経路。通常の daemon は実更新を安全に拒否する。
- `src/src/ownership.rs`：`StandaloneCoordinator`（単一ホストの lease/epoch）。
- `src/src/local_identify.rs`：同一ホスト内の Identify 許可（30 秒間隔・1 時間 10 回）。プロセス間の調整は **なし**。
- 分離済みの Core：Gateway と独立に生き、再接続でき、状態を保つ。ただし **1 接続ずつ**しか受けない（`core_server::serve` は逐次）。Hello の identity は固定の generation（1）。

## 3. 方式：Resume ではなく「二重セッション＋待機側のバッファ」

進捗のロードマップ（`progress.md`）は Discord の Resume を前提にしていた。ここでは、**まず Resume を使わない方式**を第一候補にする。

- 新 Gateway は、旧 Gateway が接続したまま、**新しく Identify して独自のセッション**を持つ（Identify を 1 回消費。ローカル許可の枠内）。Discord は同じ Shard の複数セッションを許すと理解しているが、**実 Discord では未確認**（要検証 V1）。
- 新 Gateway は READY 後 Standby：イベントを受けるが Core へ配送せず、**一定量バッファ**する（または捨てる）。旧 Gateway は Active のまま処理する。
- 切替：旧を Pause（新規受付を止め、処理中の仕事を完了／drain）→ epoch を進める → 新を Active にする → バッファ済みイベントを **event_key で重複排除しながら**配送する。旧を drain して終了。
- 利点：Resume のための session_id／seq／resume_url の秘密の引き渡しが要らない（`session_id` は秘密として扱う必要があり、これは設計上の大きな負担だった）。切替中のイベントは、新旧どちらかのセッションが必ず受け取る（at-least-once。重複は event_key で除去）。
- 代償：Identify を毎回 1 回消費する（1 日の上限 `session_start_limit` に注意）。両セッションが同時に生きる短い間、Discord 側では 2 接続。

**Resume は第 2 候補**（Identify の消費を避けたい場合）。`twilight-gateway 0.18` は `Shard::session()` と `ConfigBuilder::session/resume_url` を持つと設計書 §4.4 に記録されているが、実機では未検証（V2）。

## 4. 必要な変更

### 4.1 Core：同時 2 接続と fencing
- `core_server::serve` を、**複数の接続を同時に保持**できるように変更する。アプリの状態（`CoreLife`）は共有だが、**状態を変える要求は Active の接続だけ**が出せる（Standby は Hello／Health／読み取りのみ）。
- Hello の identity に `ownership_epoch`（と接続ごとの generation）を使い、Core は **より小さい epoch の接続からの変更要求を拒否**する（遅れて届いた旧 Gateway の要求が新しい状態を壊さない）。今は generation を固定（1）にしているので、Gateway ごとの generation を botd が採番して渡す必要がある（Core 起動時の identity ではなく、接続時に提示する形に変える）。
- 接続ごとの交渉状態（capability・保留中の API 結果・イベントカーソル・drain）は既に接続ごと。共有するのはアプリ状態だけ。

### 4.2 botd：更新の指揮
- `botctl update gateway <shard>`（仮）：新しい Gateway バイナリのパスを受け、`gateway_switch` の状態機械を **本番の経路**で駆動する。試験専用の `test_gateway_switch` を土台にするが、模擬 Discord／模擬 Identify 予算は外す。
- 候補の起動：旧 Gateway と同じ引数＋`--standby`（仮）で第 2 の Gateway を起動。Core の **別スロットではなく同じ Core** に接続する（Standby として）。
- 切替の実行は、Pause ACK／AuthorityLost などの既存の効果列をそのまま使う。失敗時は旧 Gateway に戻す（`RecoveryRequired` を含む）。
- Identify はローカル許可を通す（プロセス内）。**botd と Gateway が別プロセス**なので、候補の Identify 前に botd が許可を取り、結果を Gateway に渡す（既存の `try_acquire_identify` の延長）。

### 4.3 Gateway（候補）
- `--standby`：READY までは通常どおり。READY 後は Core へ **配送せず**、受信イベントを上限付きでバッファする。Active 指示（botd から）で配送を開始する。
- バッファの上限を超えたら、**更新を中止**する（欠落を黙って許さない）。

### 4.4 Interaction（スラッシュコマンド）
- Interaction の初期応答は 3 秒以内。切替の瞬間に届いた Interaction は、(a) 旧がまだ処理する／(b) 新がバッファから処理する、のどちらかに **一意に**決める（event_key で重複排除）。3 秒を超えるバッファは使えない（要検証 V4）。

## 5. 実装の分け方（各段階で既存の試験が通る）

1. **R-1 Core の同時 2 接続と fencing**（Core のみ、botd なし）：実プロセス試験で、Standby の接続は変更要求を拒否される、より小さい epoch は拒否される、旧接続の切断で状態が壊れない。
2. **R-2 接続時 identity**（generation／epoch を接続時に提示。Core 起動時の固定をやめる）：既存の `core_attach`／`botd_separate_cores` を更新。
3. **R-3 候補 Gateway（`--standby`）とバッファ**：模擬 Discord で、READY 後に配送されない、Active 指示で配送される、上限超過で中止。
4. **R-4 botd の本番経路**：`gateway_switch` を駆動。模擬 Discord の E2E（`e2e_gateway_process_switch` の本番版）と、失敗の注入（候補の READY 失敗、Pause のタイムアウト、旧の途中死亡）。
5. **R-5 実機**：WSL＋実 Discord で、`!count` が更新をまたいで続く、更新中に `!ping` を連打して欠落・重複がない、Gateway の PID が入れ替わり Core の PID は不変、Identify の消費が 1 回。

## 6. 検証が必要な未確認事項

| # | 内容 | 方法 |
|---|---|---|
| V1 | 同じ Shard で 2 セッションが同時に有効か。両方にイベントが届くか | 実 Discord（テスト Bot、1 Shard）。Gateway 単体を 2 つ起動して `!ping` を受け取る |
| V2 | Resume の成否（第 2 候補を取る場合） | 実 Discord。旧を切ってから `session()` で再開 |
| V3 | 切替中のイベント欠落・重複の実際の数 | R-5 の連打試験 |
| V4 | Interaction の 3 秒内応答が切替で破れないか | R-5 の `/ping` 連打、レイテンシ記録 |
| V5 | Identify の消費が 1 回で収まるか、上限の余裕 | `session_start_limit` の残りを前後で記録 |

## 7. ユーザーに確認したい判断点

| # | 論点 | 推奨 | 影響 |
|---|---|---|---|
| R1 | 方式：二重セッション（Identify を 1 回消費）か、Resume か | まず二重セッション。V1 が通らなければ Resume | 秘密の引き渡しの有無、Identify の消費 |
| R2 | 更新の起動：`botctl update gateway` のみか、バイナリ変更の自動検知（`worker-path.txt` 方式）か | `botctl` のみ（明示操作） | 誤更新の防止 |
| R3 | バッファ上限超過時：中止して旧を維持か、欠落を許して続行か | 中止して旧を維持 | 更新が失敗し得る |
| R4 | 実 Discord での V1 を、実装の前に先に試すか | 先に試す（30 分程度。方式が決まる） | 設計の手戻りを避ける |

## 8. 範囲外・注意

- 複数ホストの failover と Raft の本番化は、この設計に含まない。
- 長時間稼働、複数 Shard、レート制限は未検証のまま。
- この文書は設計案であり、ここに書いた保証はどれも実装・検証後にだけ主張する。

## 9. R-3 着手前の調査結果（2026-11-15）：待機側に Core の「待機接続」は要らない

- R-1／R-2 で、Core 側は「高い epoch が来たら古い接続を止めて引き継ぐ」ことができる。したがって待機の Gateway は、**活性化まで Core に attach しなくてよい**（Discord にログインしてイベントをバッファするだけ）。活性化の合図で `attach(epoch+1)` すれば、その瞬間に旧 Gateway の Core 接続が止まる。Core に読み取り専用の待機モードを作る必要はない（§4.1 の「Standby は Hello／Health のみ」は不要になった）。
- 実装の形：`--standby` の Gateway は `Manager` を作らず、`live_events` の前段でイベントを上限付きで溜め、制御入力（stdin の `GatewayControl::Activate { epoch }`）が来たら `attach_gateway_pool` → 溜めたイベントを通常の経路で先に処理 → 通常動作。`GatewayControl` に `Activate` を足し、`shutdown_input`（1 フレームだけ読んで終了する）を連続読みに直す必要がある。
- **未解決の核心（V3）：切替境界の重複と欠落。** 旧と新は同じ Discord イベントを両方受ける。新が溜めたイベントのうち、旧が既に処理したものを再生すると**二重処理**（`!count` が 2 つ進む）になる。逆に境界を厳しく切ると欠落する。候補：(a) 旧が処理した最大のメッセージ ID（snowflake）を境界として Core に記録し、新は境界より後の ID だけを再生する（at-least-once に近い。ID の順序が崩れる稀なケースは残る）。(b) Core が直近のメッセージ ID／interaction ID の窓で重複排除する（`Request::Dispatch` に ID を足す必要があり、プロトコルが広がる）。いずれも、実 Discord での重複・欠落の実測（V3）なしに「保証」とは言えない。
- 判断を仰ぎたい点：境界の方式を (a) と (b) のどちらにするか。推奨は **(b)（Core 側の ID 窓による重複排除）**：旧・新の Gateway 間に追加の連絡経路が要らず、旧が突然死しても働く。ただし `Dispatch`／`Command`／`Interaction` 要求に Discord の ID を載せる変更が入る。
