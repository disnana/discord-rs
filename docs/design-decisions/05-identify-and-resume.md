# 05. Identify と Resume

## 用語

- **Identify**：Discord に接続するときの「ログイン」。Bot の token を渡して、新しい接続（セッション）を始める。Discord は回数を制限している。
- **Resume**：前の接続の続き。セッションの番号を伝え、切れていた間のイベントを受け取る。回数の制限は（一般には）ない。

## いまの状態（確認済み）

- **同一プロセスの中での再接続は、Resume で復帰する。** Oracle の記録：`Gateway 再接続の要求を受信` → `close code 4000` → `Gateway 再開（Resume 成功）`（約 1 秒、Identify を使わない）。出典：`docs/progress.md`（2026-11-16 夜）。
- **プロセスを再起動したときの Resume は、できない。** セッション情報がメモリだけにあり、再起動のたびに新しい Identify を使う。
- **Identify を使うのは、Gateway が新しく起動するとき**：プロセスの再起動、`gateway-update`（シャードごとに 1 回）、クラッシュからの復旧。
- **自分たちで課している制限：同じ Bot で、直近の起動から 30 秒以上空ける、1 時間に 10 回まで。** 超えると `IdentifyUnavailable` で `Degraded`（出典：`docs/known-limitations.md`）。初回起動と手動の起動は、常に許可して記録する。botd の再起動で履歴は消える。

## どれくらい頻繁に起きるか

- 通常の運用では、ほとんど起きない。Oracle の最初の約 18 時間は、Identify は起動時の 1 回だけだった。
- 頻繁に更新する、またはクラッシュを繰り返すときに、制限に当たり得る。

## Discord 側の制限（**私の記憶。公式で未確認**）

「1 日 1000 回程度」「シャードごとに数秒に 1 回程度」。実際の値は、公式ドキュメント（Gateway の Session Start Limit）で確認する（[unverified-items.md](unverified-items.md)）。

## 再起動をまたぐ Resume を作る場合の論点

- セッション情報（セッション ID と最後の番号）を、安全に保存する（秘匿の扱い）。
- 古くなったセッションの扱い（Discord が無効と返したら、Identify に切り替える）。
- `gateway-update` では、旧 Gateway と新 Gateway が同じセッションを同時には使えないので、更新の Identify を節約する効果はない。節約できるのは、**プロセスの再起動**のときだけ。

## 評価・推奨

- **この規模（数シャード）では、影響が小さい。後回しを推奨。** 大きな Bot で再起動が頻繁になったときに、再検討する。

## 決めること

[decisions-needed.md](decisions-needed.md) の D6。
