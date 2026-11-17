# P1 の設計案：停滞の診断を daemon から照会できるようにする

- 日付: 2026-11-17
- 状態: **設計案（方向は DDR-0002・DDR-0006 で了承済み。以下の具体的な方式は、ユーザーの確認前の提案）。未実装。**
- 関連: [DDR-0002](discussion-records/0002-stalled-core-management.md)、[DDR-0003](discussion-records/0003-old-core-timeout-diagnostics.md)、[DDR-0006](discussion-records/0006-proposals-accepted-2026-11-17.md)、[proposals-2026-11-17.md](proposals-2026-11-17.md)（提案 2 の P1）、[01-stuck-handler.md](01-stuck-handler.md)

## 目的

Core の中で長引いている要求（B3 の観測が検知するもの）を、**`core-N.log` の 1 行ではなく、daemon（botd）から `botctl status` などで照会できる**ようにする。Core が固まっているときにも、照会できること（DDR-0002：「障害中の Bot 自身に依存しない管理経路」）。

## 現状（コードで確認済み）

- Core は、要求ごとの経過時間を `RequestObservations`（`src/src/worker.rs`）で追跡し、30 秒を超えたら標準エラーに 1 行を出す。**照会する手段はない。**
- `Response::Ready`（Core → Gateway の応答）に `#[serde(default)]` の任意フィールドがあり、`Response` の型は `deny_unknown_fields` ではない。つまり、**任意フィールドを足しても、古い Gateway は無視して読める**（後方互換。ただし、これだけでは botd に届かない）。
- botd が Core について見ているのは、`running` と `failures` だけ（`cores_view`、`src/src/daemon.rs`）。これは botd 自身が Core のプロセスを管理しているため。
- Gateway は、自分が持つ Core の情報（`WorkerIdentity`）を管理用の状態として持つが、**botd が Gateway から読む経路は、Core の停滞の照会には使えない**（Gateway が止まっていると届かない）。

## 検討した方式と、採用案

| 方式 | 内容 | 評価 |
|---|---|---|
| A. botd が Core の IPC ソケットへ接続して問い合わせる | — | **不採用（危険）**。Core は、**高い epoch の接続が来ると、いま接続中の Gateway を締め出す**（fence）。botd の接続が、Gateway を誤って締め出し得る |
| B. Core が Health の応答に、停滞の要約を載せ、Gateway 経由で botd へ | `Ready` に任意フィールドを足す | 後方互換だが、Gateway が定期的に問い合わせ、さらに botd が Gateway から読む経路が要る。**Gateway が止まっていると届かない** |
| **C. Core が、状態を小さなファイルに定期的に書き、botd が読む（採用案）** | IPC の変更が要らない。Gateway に依存しない。**Core が完全に固まると、更新が止まる（古さが「応答なし」の合図になる）** | **採用案** |

## 採用案（C）の設計

### 1. Core 側（書く）
- Core は、`<ipc>/core-N.status.json` に、状態を**原子的に**（一時ファイルへ書いて名前を付け替える）書く。権限 0600。`ipc` のディレクトリは、既に Core の所有者だけが入れる（0700）。
- 更新の契機：5 秒ごとの観測の tick（`observe_request_with` の `check_every`）に合わせる（未完了の要求が**ないとき**も、生存の印として、約 5 秒おきに更新する。ただし変化がなければ内容は同じで、書き込みは軽い）。
- 内容（**要求の内容・ID・ユーザー情報・秘密は含めない**）：
  ```json
  {"v":1,"updated_at_ms":1794890000000,"pending":2,"oldest_age_s":47,"warned":1,"threshold_s":30}
  ```
  - `pending`：未完了の要求の数。`oldest_age_s`：最も古い未完了の要求の経過秒。`warned`：警告を出した件数。`threshold_s`：警告の閾値（いまは定数の 30。P2 で設定可能に）。
- **重要な性質：** Core の非同期の実行基盤が、同期処理（ブロッキング）で完全に固まると、この書き込みも止まる。これは**欠点ではなく、検知の手がかり**（下記）。

### 2. botd 側（読む）
- `cores_view`（`botctl status`／`inspect` の Core 表示）に、各 Core の状態ファイルを読んで、次を足す：
  ```json
  {"running": true, "failures": 0,
   "stalled": {"pending": 2, "oldest_age_s": 47, "warned": 1, "threshold_s": 30, "status_age_s": 3}}
  ```
- `status_age_s` ＝ いまの時刻 − `updated_at_ms`。**`status_age_s` が大きい（例：15 秒以上。5 秒おきに書くので、3 回以上抜けた）なら、`"unresponsive": true`** と表示する（Core の実行基盤が固まっている疑い）。
- 読み方は**安全に**：通常のファイルか（シンボリックリンクでない）、所有者、サイズ上限（例：4 KiB）、JSON の形式を検証。読めない・古い・壊れているときは、`stalled: null` と理由を返し、**botd の動作に影響させない**（読み取りは失敗しても無視）。
- 既存の `running`／`failures` の出力は変えない（追加のみ）。

### 3. 後方互換・安全性
- **IPC・Hello・capability を変えない。** 古い Core はファイルを書かないので、`stalled: null`（対応していない Core）と表示する。
- botd は Core に**接続しない**（fence の危険がない）。
- 書き込みの失敗（ディスク満杯など）は、Core の処理に影響させない（最善努力。B3 の警告の出力と同じ方針：失敗を無視する）。
- 認可：`botctl status` の既存の認可（role・scope）に従う。Core の状態は、シャードごとの表示なので、scope 外のシャードには出さない（既存の `process_views` と同じ経路）。

## 試験の計画
1. 単体：状態ファイルの書き込み（原子的、0600、内容の形式、更新の間隔）。読み取り（壊れたファイル、古いファイル、巨大なファイル、シンボリックリンク、所有者の違いを、すべて安全に拒否）。
2. 統合（separate モード、模擬 Discord、`botd_separate_cores`）：
   - 意図的に止まるハンドラ（試験用の Worker）で、`botctl status` に `pending > 0`、`oldest_age_s` が増える。完了後に 0 に戻る。
   - Core の実行基盤を固める（`SIGSTOP` で Core を停止する）と、`status_age_s` が増え、`unresponsive: true` になる。`SIGCONT` で戻る。
   - 古い形式（状態ファイルなし）の Core で、`stalled: null`。
3. 実機（WSL・実 Discord）：Core に `SIGSTOP` を送り、`botctl status` の表示を確認する（要・ユーザーの確認）。

## 未決定・要確認（ユーザーに聞くこと）
- `unresponsive` の判定の秒数（15 秒の案）。
- 状態ファイルの名前と置き場所（`<ipc>/core-N.status.json` の案）。
- 表示の項目名（`stalled`、`unresponsive` は仮）。
- 診断に処理名・登録位置を載せるか（P1 では載せない。DDR-0003 の注意：非同期の「いま止まっている行」は特定できない場合がある。秘密・内容を出さない方針との両立が要る）。

## 作業量・リスク
- 作業量：小〜中（Core 側の書き込みと、botd 側の読み取りと、試験）。
- リスク：状態ファイルの書き込み頻度（5 秒おき。軽い）。`updated_at_ms` の時刻の扱い（Core と botd は同じホストなので、同じ時計を使う。時計の逆行に注意）。
- 範囲外：停止・再起動の管理操作（P3）、閾値の設定（P2）、自動の対処（保留）。
