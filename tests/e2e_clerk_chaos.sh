#!/usr/bin/env bash
# Copyright 2026 OpenS3 Contributors
# SPDX-License-Identifier: Apache-2.0
#
# Strengthened Clerk-only fault drill (local processes, no K8s required):
#   1. scale-out   : bring up 3-voter Raft gradually (0 → 1 → 2)
#   2. scale-in    : stop one voter; quorum of 2 still serves; then re-join
#   3. node-crash  : kill leader / follower; restart from intact raft-dir
#   4. disk-damage : corrupt or wipe a voter's raft-dir; survivors keep assign
#   5. majority-loss (negative): 1/3 voters → assign unavailable
#
# Usage: bash tests/e2e_clerk_chaos.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${ROOT}/target/debug/opens3"
DATA="${TMPDIR:-/tmp}/opens3-clerk-chaos-$$"
P0=58331
P1=58332
P2=58333
VP=28088
PEERS="http://127.0.0.1:${P0},http://127.0.0.1:${P1},http://127.0.0.1:${P2}"
ALL_PORTS=("$P0" "$P1" "$P2")

PASS=0
PHASE=""

log()  { printf '[clerk-chaos] %s\n' "$*"; }
ok()   { PASS=$((PASS + 1)); log "PASS [$PHASE] $*"; }
die()  { log "FAIL [$PHASE] $*"; exit 1; }
step() { PHASE="$1"; log "==== $PHASE ===="; }

cleanup() {
  pkill -f "opens3 clerk --port ${P0}" 2>/dev/null || true
  pkill -f "opens3 clerk --port ${P1}" 2>/dev/null || true
  pkill -f "opens3 clerk --port ${P2}" 2>/dev/null || true
  pkill -f "opens3 volume --port ${VP}" 2>/dev/null || true
  rm -rf "$DATA"
}
trap cleanup EXIT

mkdir -p "$DATA"/{c0,c1,c2,vol}
cargo build -q -p opens3

dir_for_port() {
  case "$1" in
    "$P0") echo "$DATA/c0" ;;
    "$P1") echo "$DATA/c1" ;;
    "$P2") echo "$DATA/c2" ;;
    *) return 1 ;;
  esac
}

# STS-style --ip + IP advertise-url: Raft id follows advertise-url only
# (ghost-voter / failover-panic regression).
start_clerk() {
  local port="$1"
  local dir ord
  dir=$(dir_for_port "$port")
  case "$port" in
    "$P0") ord=0 ;;
    "$P1") ord=1 ;;
    "$P2") ord=2 ;;
  esac
  mkdir -p "$dir"
  "$BIN" clerk \
    --port "$port" \
    --ip "opens3-clerk-${ord}" \
    --peers "$PEERS" \
    --raft-dir "$dir" \
    --advertise-url "http://127.0.0.1:${port}" &
}

kill_clerk_port() {
  local port="$1"
  pkill -f "opens3 clerk --port ${port}" 2>/dev/null || true
  for _ in $(seq 1 50); do
    if ! curl -sf --max-time 0.3 "http://127.0.0.1:${port}/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
}

find_leader() {
  local p body
  for p in "$@"; do
    body=$(curl -sf --max-time 1 "http://127.0.0.1:${p}/raft/status" 2>/dev/null || true)
    if echo "$body" | grep -q '"role":"Leader"'; then
      echo "$p"
      return 0
    fi
  done
  return 1
}

wait_leader() {
  local timeout="$1"
  shift
  local i leader
  for i in $(seq 1 "$timeout"); do
    if leader=$(find_leader "$@"); then
      echo "$leader"
      return 0
    fi
    sleep 0.25
  done
  return 1
}

wait_ports() {
  local p
  for _ in $(seq 1 40); do
    local ok_all=1
    for p in "$@"; do
      if ! curl -sf --max-time 1 "http://127.0.0.1:${p}/raft/status" >/dev/null 2>&1; then
        ok_all=0
        break
      fi
    done
    [[ "$ok_all" -eq 1 ]] && return 0
    sleep 0.25
  done
  return 1
}

wait_assign() {
  local leader="$1" tries="${2:-40}"
  local i
  for i in $(seq 1 "$tries"); do
    if curl -sf --max-time 2 "http://127.0.0.1:${leader}/dir/assign" 2>/dev/null | grep -q fid; then
      return 0
    fi
    sleep 0.4
  done
  return 1
}

assign_or_die() {
  local leader="$1" msg="$2"
  if wait_assign "$leader" 40; then
    ok "$msg (leader=$leader)"
  else
    die "$msg — assign failed on leader=$leader"
  fi
}

online_volume_node() {
  local leader="$1" node_id="$2"
  curl -sf -X POST "http://127.0.0.1:${leader}/cluster/server/online" \
    -H 'content-type: application/json' \
    -d "{\"node_id\":\"${node_id}\",\"online\":true}" >/dev/null
  local vids
  vids=$(curl -sf "http://127.0.0.1:${leader}/vol/list" \
    | python3 -c '
import sys, json
raw = json.load(sys.stdin)
vols = raw if isinstance(raw, list) else (raw.get("volumes") or [])
ids = []
for v in vols:
    if isinstance(v, dict):
        ids.append(str(v.get("id") or v.get("volume_id") or ""))
    else:
        ids.append(str(v))
print(" ".join(i for i in ids if i))
' 2>/dev/null || true)
  local vid
  for vid in $vids; do
    curl -sf -X POST "http://127.0.0.1:${leader}/vol/online" \
      -H 'content-type: application/json' \
      -d "{\"volume_id\":${vid},\"online\":true}" >/dev/null 2>&1 || true
  done
}

# ---------------------------------------------------------------------------
step "0-build"
log "data=$DATA peers=$PEERS"
ok "binary ready"

# ---------------------------------------------------------------------------
step "1-scale-out"
log "start clerk-0 alone (3-member config → no quorum yet)"
start_clerk "$P0"
sleep 1

log "start clerk-1 → expect quorum 2/3"
start_clerk "$P1"
LEADER=$(wait_leader 80 "$P0" "$P1") || die "no leader after scale-out to 2"
ok "leader after 2 voters: $LEADER"

log "start clerk-2 → full 3-voter cluster"
start_clerk "$P2"
LEADER=$(wait_leader 80 "${ALL_PORTS[@]}") || die "no leader after scale-out to 3"
wait_ports "${ALL_PORTS[@]}" || die "not all 3 clerks answering /raft/status"
ok "3 clerks alive; leader=$LEADER"

"$BIN" volume \
  --port "$VP" \
  --dir "$DATA/vol" \
  --clerk "http://127.0.0.1:${LEADER}" \
  --clerk-peers "$PEERS" \
  --node-id vs-chaos \
  --max-volumes 4 \
  --max-volume-size-mb 8 &
for _ in $(seq 1 30); do
  vc=$(curl -sf "http://127.0.0.1:${LEADER}/cluster/status" 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('volume_server_count',0))" 2>/dev/null || echo 0)
  [[ "${vc:-0}" -ge 1 ]] && break
  sleep 0.4
done
online_volume_node "$LEADER" "vs-chaos"
sleep 1
assign_or_die "$LEADER" "assign after scale-out"

# ---------------------------------------------------------------------------
step "2-scale-in"
FOLLOWER=""
for p in "${ALL_PORTS[@]}"; do
  if [[ "$p" != "$LEADER" ]]; then
    FOLLOWER="$p"
    break
  fi
done
test -n "$FOLLOWER"
log "stop follower :$FOLLOWER (scale-in → 2/3)"
kill_clerk_port "$FOLLOWER"
REMOVED="$FOLLOWER"

SURV=()
for p in "${ALL_PORTS[@]}"; do
  [[ "$p" == "$REMOVED" ]] && continue
  SURV+=("$p")
done
LEADER=$(wait_leader 60 "${SURV[@]}") || die "lost leader after scale-in"
assign_or_die "$LEADER" "assign after scale-in (2/3 voters)"

log "re-join scaled-in clerk :$REMOVED"
start_clerk "$REMOVED"
LEADER=$(wait_leader 80 "${ALL_PORTS[@]}") || die "no leader after re-join"
wait_ports "${ALL_PORTS[@]}" || die "re-joined clerk not answering"
ok "re-joined :$REMOVED; leader=$LEADER"
assign_or_die "$LEADER" "assign after scale back to 3"

# ---------------------------------------------------------------------------
step "3-node-crash-leader"
OLD_LEADER="$LEADER"
log "kill leader :$OLD_LEADER"
kill_clerk_port "$OLD_LEADER"
SURV=()
for p in "${ALL_PORTS[@]}"; do
  [[ "$p" == "$OLD_LEADER" ]] && continue
  SURV+=("$p")
done
LEADER=$(wait_leader 60 "${SURV[@]}") || die "no new leader after leader crash"
[[ "$LEADER" != "$OLD_LEADER" ]] || die "leader did not change"
assign_or_die "$LEADER" "assign after leader crash"

log "restart crashed leader from intact raft-dir"
start_clerk "$OLD_LEADER"
wait_ports "$OLD_LEADER" || die "restarted leader port not answering"
LEADER=$(wait_leader 60 "${ALL_PORTS[@]}") || die "no leader after leader restart"
assign_or_die "$LEADER" "assign after crashed leader rejoined"

# ---------------------------------------------------------------------------
step "4-node-crash-follower"
FOLLOWER=""
for p in "${ALL_PORTS[@]}"; do
  if [[ "$p" != "$LEADER" ]]; then
    FOLLOWER="$p"
    break
  fi
done
log "kill follower :$FOLLOWER"
kill_clerk_port "$FOLLOWER"
LEADER=$(wait_leader 40 "${ALL_PORTS[@]}") || die "lost leader after follower crash"
assign_or_die "$LEADER" "assign while one follower down"
start_clerk "$FOLLOWER"
wait_ports "$FOLLOWER" || die "follower restart failed"
ok "follower :$FOLLOWER restarted from intact disk"

# ---------------------------------------------------------------------------
step "5-disk-damage-corrupt-log"
TARGET=""
for p in "${ALL_PORTS[@]}"; do
  if [[ "$p" != "$LEADER" ]]; then
    TARGET="$p"
    break
  fi
done
TDIR=$(dir_for_port "$TARGET")
LOG_FILE="$TDIR/openraft-log.wal"
test -f "$LOG_FILE" || die "missing $LOG_FILE"
log "corrupt $LOG_FILE and restart :$TARGET"
kill_clerk_port "$TARGET"
cp "$LOG_FILE" "${LOG_FILE}.bak"
# Overwrite magic / records with garbage (not a valid WAL).
printf 'NOT-VALID-RAFT-WAL{{{{' >"$LOG_FILE"

set +e
start_clerk "$TARGET"
sleep 2
CORRUPT_ALIVE=0
curl -sf --max-time 1 "http://127.0.0.1:${TARGET}/raft/status" >/dev/null 2>&1 && CORRUPT_ALIVE=1
kill_clerk_port "$TARGET"
set -e

ALIVE=()
for p in "${ALL_PORTS[@]}"; do
  [[ "$p" == "$TARGET" ]] && continue
  ALIVE+=("$p")
done
LEADER=$(wait_leader 40 "${ALIVE[@]}") || die "survivors lost leader after disk corrupt"
assign_or_die "$LEADER" "assign after follower log corruption (corrupt_alive=$CORRUPT_ALIVE)"

cp "${LOG_FILE}.bak" "$LOG_FILE"
start_clerk "$TARGET"
LEADER=$(wait_leader 60 "${ALL_PORTS[@]}") || die "cluster did not recover after restoring log"
ok "restored corrupted node from backup"

# ---------------------------------------------------------------------------
step "6-disk-damage-wipe-raft-dir"
TARGET=""
for p in "${ALL_PORTS[@]}"; do
  if [[ "$p" != "$LEADER" ]]; then
    TARGET="$p"
    break
  fi
done
TDIR=$(dir_for_port "$TARGET")
log "wipe raft-dir $TDIR and restart :$TARGET"
kill_clerk_port "$TARGET"
rm -rf "$TDIR"
mkdir -p "$TDIR"
start_clerk "$TARGET"
sleep 2

ALIVE=()
for p in "${ALL_PORTS[@]}"; do
  [[ "$p" == "$TARGET" ]] && continue
  ALIVE+=("$p")
done
LEADER=$(wait_leader 40 "${ALIVE[@]}") || die "survivors lost leader after raft-dir wipe"
assign_or_die "$LEADER" "assign after follower raft-dir wipe"

WIPED_ROLE=$(curl -sf --max-time 1 "http://127.0.0.1:${TARGET}/raft/status" 2>/dev/null || echo '{}')
if echo "$WIPED_ROLE" | grep -q '"role":"Leader"'; then
  SURV_LEADER=$(find_leader "${ALIVE[@]}" || true)
  if [[ -n "${SURV_LEADER:-}" ]]; then
    die "split-brain: wiped node and survivor both claim Leader (wiped=$TARGET surv=$SURV_LEADER)"
  fi
fi
ok "no split-brain after empty raft-dir restart (wiped=$TARGET)"

# ---------------------------------------------------------------------------
step "7-majority-loss-negative"
# Leave a single *follower* alive (kill leader + one peer). Without quorum the
# survivor must not win an election; assign must fail. (A lone *former leader*
# may still serve in-memory assign while Raft commits stall — that is a
# different failure mode and not asserted here.)
FOLLOWER=""
for p in "${ALL_PORTS[@]}"; do
  if [[ "$p" != "$LEADER" ]]; then
    FOLLOWER="$p"
    break
  fi
done
OTHER=""
for p in "${ALL_PORTS[@]}"; do
  [[ "$p" == "$LEADER" || "$p" == "$FOLLOWER" ]] && continue
  OTHER="$p"
done
log "stop leader :$LEADER and peer :$OTHER; keep follower :$FOLLOWER"
kill_clerk_port "$LEADER"
kill_clerk_port "$OTHER"

# Wait past election timeouts; lone follower must not become Leader.
BECAME_LEADER=0
for _ in $(seq 1 20); do
  sleep 0.5
  body=$(curl -sf --max-time 1 "http://127.0.0.1:${FOLLOWER}/raft/status" 2>/dev/null || true)
  if echo "$body" | grep -q '"role":"Leader"'; then
    BECAME_LEADER=1
    break
  fi
done
if [[ "$BECAME_LEADER" -eq 1 ]]; then
  die "lone follower :$FOLLOWER became Leader without quorum"
fi
ok "lone follower :$FOLLOWER did not elect itself Leader"

ASSIGN_OK=0
if curl -sf --max-time 2 "http://127.0.0.1:${FOLLOWER}/dir/assign" 2>/dev/null | grep -q fid; then
  ASSIGN_OK=1
fi
if [[ "$ASSIGN_OK" -eq 1 ]]; then
  die "assign succeeded on non-leader after majority loss"
fi
ok "assign unavailable after majority loss (no quorum)"

# ---------------------------------------------------------------------------
step "summary"
log "assertions_passed=$PASS"
echo "e2e_clerk_chaos OK"
