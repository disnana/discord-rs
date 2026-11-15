#!/usr/bin/env bash
# One-shot check for a single separate-mode shard on Ubuntu; run as root.
set -u
set -o pipefail
USER_NAME=discord-rs
ROOT=/var/lib/discord-rs/runtime
SECRET=/etc/discord-rs/botd.secret
BOTCTL=/opt/discord-rs/bin/botctl
PORT=47321
bot=test
repeat=1
bot_set=0
while [ "$#" -gt 0 ]; do
  case "$1" in
    --repeat)
      [ "$#" -ge 2 ] || { echo 'Missing repeat count' >&2; exit 2; }
      repeat=$2; shift 2 ;;
    --*) echo 'Unknown option' >&2; exit 2 ;;
    *) [ "$bot_set" -eq 0 ] || { echo 'Too many bot arguments' >&2; exit 2; }
       bot=$1; bot_set=1; shift ;;
  esac
done
case "$repeat" in ''|*[!0-9]*) echo 'Repeat count must be a positive integer' >&2; exit 2;; esac
[[ "$repeat" =~ ^0*[1-9][0-9]*$ ]] || { echo 'Repeat count must be positive' >&2; exit 2; }
repeat=$((10#$repeat))
[ "$(id -u)" -eq 0 ] || { echo 'Run as root via sudo' >&2; exit 2; }

results=()
failed=0
record() {
  local result=$1 label=$2 reason=$3
  printf '%-4s | %-25s | %s\n' "$result" "$label" "$reason"
  results+=("$(printf '%-4s | %-25s | %s' "$result" "$label" "$reason")")
  [ "$result" = PASS ] || failed=1
}
ctl() { timeout 8 sudo -u "$USER_NAME" "$BOTCTL" "$@" --timeout 5s --secret-file "$SECRET" --port "$PORT" --json 2>/dev/null; }
json_string() {
  local key=$1 input=$2
  if command -v jq >/dev/null 2>&1; then
    printf '%s' "$input" | jq -r --arg key "$key" '.. | objects | .[$key] // empty | select(type == "string")' 2>/dev/null | head -n 1
  else
    printf '%s\n' "$input" | tr '\n' ' ' | sed -nE "s/.*\"$key\"[[:space:]]*:[[:space:]]*\"([^\"]*)\".*/\\1/p"
  fi
}
warnings() {
  local output
  output=$(journalctl -u botd -b -p warning --no-pager 2>/dev/null) || return 1
  printf '%s\n' "$output" | awk 'NF && $0 !~ /^-- / {n++} END {print n+0}'
}
snapshot() {
  # A Gateway that an update promoted still has --standby in its command line, so --standby
  # is not a role: every process with --core-dir is "the Gateway" and there must be exactly one.
  cores=(); gateways=(); daemons=()
  local pid arg prev cmd role ipc epoch rss
  for pid in $(pgrep -u "$USER_NAME" -f 'bin/(botd|src)' 2>/dev/null | sort -n); do
    [ -r "/proc/$pid/cmdline" ] || continue
    args=(); mapfile -d '' -t args < "/proc/$pid/cmdline" || continue
    role=; ipc=; epoch=absent; prev=; cmd=${args[0]:-}
    for arg in "${args[@]}"; do
      [ "$prev" = --core-dir ] && ipc=$arg
      [ "$prev" = --core-epoch ] && epoch=$arg
      case "$arg" in --core-listen) role=core;; --core-dir) role=gateway;; esac
      prev=$arg
    done
    case "$role" in
      core) cores+=("$pid") ;;
      gateway) gateways+=("$pid"); gateway_ipc=$ipc; gateway_epoch=$epoch ;;
      *) case "$cmd" in */botd) daemons+=("$pid");; esac ;;
    esac
    rss=$(awk '/^VmRSS:/ {print $2; found=1} END {if (!found) print "unknown"}' "/proc/$pid/status" 2>/dev/null)
    printf '  pid=%s role=%s rss_kB=%s' "$pid" "${role:-botd}" "${rss:-unknown}"
    case "$role" in gateway) printf ' core_epoch=%s' "$epoch";; esac
    printf '\n'
  done
}

baseline_rss=
for ((round=1; round<=repeat; round++)); do
  echo "=== Round $round/$repeat PRE ==="
  gateway_ipc=; gateway_epoch=
  snapshot
  before_cores="${cores[*]}"; old=${gateways[0]:-}; ipc=$gateway_ipc
  if [ "$(systemctl is-active botd 2>/dev/null)" = active ] && [ "${#daemons[@]}" -eq 1 ] && [ "${#cores[@]}" -eq 2 ] && [ "${#gateways[@]}" -eq 1 ] && [ -n "$ipc" ]; then
    record PASS "R$round preconditions" 'botd, two cores and one gateway found'
  else
    record FAIL "R$round preconditions" 'expected one botd, two cores and one gateway; update skipped'
    break
  fi
  before_warnings=$(warnings) || before_warnings=unknown
  start=$SECONDS
  response=$(ctl gateway-update "$bot") || :
  id=$(json_string operation_id "$response")
  state=unknown
  if [[ "$id" =~ ^[a-fA-F0-9]{32}$ ]]; then
    while [ $((SECONDS-start)) -lt 180 ]; do
      response=$(ctl operation "$id") || :
      state=$(json_string state "$response" | tr '[:upper:]' '[:lower:]')
      case "$state" in succeeded|failed) break;; esac
      sleep 0.5
    done
  fi
  echo "Operation elapsed: $((SECONDS-start)) s"
  if [ "$state" = succeeded ]; then record PASS "R$round update" 'operation succeeded'; else record FAIL "R$round update" "operation state: ${state:-unknown}"; fi
  sleep 5
  echo "=== Round $round/$repeat POST ==="
  snapshot
  if [ "${#cores[@]}" -eq 2 ] && [ "${cores[*]}" = "$before_cores" ]; then record PASS "R$round cores" "unchanged: $before_cores"; else record FAIL "R$round cores" 'core count or PIDs changed'; fi
  new=${gateways[0]:-}
  if [ "${#gateways[@]}" -eq 1 ] && [ "$new" != "$old" ]; then record PASS "R$round gateway PID" "$old -> $new"; else record FAIL "R$round gateway PID" 'replacement not uniquely identified'; fi
  if [ "${#gateways[@]}" -eq 1 ]; then record PASS "R$round gateway count" 'exactly one Gateway process (no standby or retiring one left)'; else record FAIL "R$round gateway count" "gateways=${#gateways[@]}"; fi
  deadline=$((SECONDS+35))
  while kill -0 "$old" 2>/dev/null && [ "$SECONDS" -lt "$deadline" ]; do sleep 1; done
  if kill -0 "$old" 2>/dev/null; then record FAIL "R$round old gateway" "PID $old still exists"; else record PASS "R$round old gateway" "PID $old is gone"; fi
  sockets=$(find "$ipc" -maxdepth 1 -type s -name '*.sock' -printf '.' 2>/dev/null | wc -c)
  secrets=$(find "$ipc" -maxdepth 1 -type f -name core.secret -printf '.' 2>/dev/null | wc -c)
  if [ "$sockets" -eq 2 ] && [ "$secrets" -eq 1 ] && [ -S "$ipc/core-0.sock" ] && [ -S "$ipc/core-1.sock" ]; then record PASS "R$round IPC" 'two expected sockets and core.secret'; else record FAIL "R$round IPC" "sockets=$sockets core.secret=$secrets"; fi
  markers=$(find "$ROOT" \( -name standby-ready -o -name standby-active \) -print 2>/dev/null) || markers=unavailable
  if [ -z "$markers" ] && [ -d "$ROOT" ]; then record PASS "R$round markers" 'no standby markers'; else record FAIL "R$round markers" 'markers found or runtime root unreadable'; fi
  events=
  if [[ "$id" =~ ^[a-fA-F0-9]{32}$ ]] && [ -r "$ROOT/gateway-update-audit.jsonl" ]; then
    events=$(grep -F -- "\"operation_id\":\"$id\"" "$ROOT/gateway-update-audit.jsonl" | tail -n 2 | sed -nE 's/.*"event"[[:space:]]*:[[:space:]]*"([^"]*)".*/\1/p' | tr '\n' ' ')
  fi
  if [ "$events" = 'requested succeeded ' ]; then record PASS "R$round audit" 'requested then succeeded'; else record FAIL "R$round audit" 'expected audit events missing or out of order'; fi
  after_warnings=$(warnings) || after_warnings=unknown
  if [ "$before_warnings" != unknown ] && [ "$after_warnings" != unknown ] && [ "$before_warnings" = "$after_warnings" ]; then record PASS "R$round warnings" "count unchanged: $after_warnings"; else record FAIL "R$round warnings" "count: $before_warnings -> $after_warnings"; fi
  if [ "$repeat" -gt 1 ]; then
    rss=$(awk '/^VmRSS:/ {print $2}' "/proc/$new/status" 2>/dev/null)
    echo "Gateway RSS after round $round: ${rss:-unknown} kB"
    if [[ "$rss" =~ ^[0-9]+$ ]] && [ "$rss" -gt 0 ]; then
      [ "$round" -ne 1 ] || baseline_rss=$rss
      if [ -n "$baseline_rss" ] && awk -v current="$rss" -v base="$baseline_rss" 'BEGIN {exit !(current <= base * 1.2)}'; then record PASS "R$round RSS" "${rss} kB, baseline ${baseline_rss} kB"; else record FAIL "R$round RSS" "${rss} kB exceeds baseline by over 20% or baseline unavailable"; fi
    else record FAIL "R$round RSS" 'gateway RSS unavailable'; fi
  fi
done
echo '=== Summary ==='
printf '%s\n' "${results[@]}"
echo 'Discord で !count を数回送り、数が続くこと・返信が1件ずつであることを確認してください'
exit "$failed"
