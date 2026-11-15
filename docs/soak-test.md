# 本番サーバーでの放置試験（長時間稼働）

目的：数時間〜1 日、実サーバーで botd を動かしたままにして、**増え続ける値**（メモリ、fd、ログ、再起動、Identify の消費）がないかを見る。これは本番相当の配置の最初の試験で、**実 Discord に接続する**ので、テスト用 Bot とテスト用ギルドだけを使う。手順は [real-operations-verification.md](real-operations-verification.md) の §0 と [linux-deployment.md](linux-deployment.md)、分離は [real-separate-verification.md](real-separate-verification.md)。

## 0. 先に確認すること（サーバーで）

```bash
uname -m; lsb_release -d; getconf GNU_LIBC_VERSION
```
- **x86_64 で Ubuntu 24.04（glibc 2.39 以上）**：開発機（WSL）でビルドした `target/release/{botd,botctl,src}` をそのまま使える。
- **aarch64（Oracle の Ampere など）や別の Ubuntu**：サーバーでビルドする（`git clone` → `cargo build --release --locked --bin botd --bin botctl --bin src`。Rust 1.90 と `build-essential pkg-config` が要る）。バイナリのコピーは使えない。

## 1. 配置（docs の §0 のとおり）

専用ユーザー `discord-rs`、`/opt/discord-rs/bin/{botd,botctl,src}`、`/etc/discord-rs/{bots.toml,botd.secret,botd.env}`、`deploy/systemd/botd.service`。token は `botd.env` にだけ置く（チャットやコマンドに貼らない）。

分離で放置する場合は drop-in（実機試験と同じ）：

```bash
sudo systemctl edit botd
```
```ini
[Service]
ExecStart=
ExecStart=/opt/discord-rs/bin/botd --file /etc/discord-rs/bots.toml --runtime-root /var/lib/discord-rs/runtime --gateway-program /opt/discord-rs/bin/src --secret-file /etc/discord-rs/botd.secret --control-port 47321 --stop-children-on-exit --core-mode separate
```
```bash
sudo systemctl enable --now botd
```

## 2. 放置中の記録

`deploy/dev/soak-report.sh` を数時間おきに実行して追記する（読み取り専用。token・secret・パスは出さない）。

```bash
sudo install -m 0755 deploy/dev/soak-report.sh /usr/local/bin/soak-report
sudo soak-report | tee -a ~/soak.log          # 開始時
sudo soak-report | tee -a ~/soak.log          # 数時間後、翌日
```
cron で自動化する場合（1 時間おき）：`0 * * * * /usr/local/bin/soak-report >> /var/log/soak.log 2>&1`

## 3. 何を見るか（合格の目安。数字は目安で、保証ではない）

| 項目 | 見方 | 異常の目安 |
|---|---|---|
| 再起動 | `NRestarts`、`generations`（gateway.log）、`restarts` | 放置中に増え続ける（理由のない再起動） |
| メモリ | 各プロセスの `rss_kB` | 時間とともに単調に増える（数 MB/時間以上） |
| fd | 各プロセスの `fds` | 増え続ける（リーク） |
| ログ | `files` の大きさ | `gateway.log`／`core-*.log` が急に大きくなる |
| 残骸 | sockets/secret・reservations の数 | 稼働中は socket 2 + secret 1、予約 1。それ以外が増える |
| 接続 | `closes`、`worker_unavailable` | 切断が頻発（WSL と違い、通常は少ないはず） |
| Core | `cores` の `running`／`failures` | `failures` が増える |
| 応答 | 時々 `!count` を送る | 返信がない／数が飛ぶ |

## 4. 終わり方

`sudo systemctl stop botd`（`Result=success`、残りプロセス 0、ipc に sock・secret なし）。結果は `docs/progress.md` に、日付・環境（サーバーの機種）・上の表の数値で書く。**確認できないこと**（今回の範囲外）：複数 Shard、複数ホスト、Resume、レート制限の高負荷。
