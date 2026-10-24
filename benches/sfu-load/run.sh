#!/bin/bash
# usage: run.sh <rust|pion> <subscribers> [duration]
# Servers must be built and copied outside ~/Documents (macOS delays network delivery to binaries there):
#   /private/tmp/rust-broadcast  (cargo build --release --example broadcast)
#   /private/tmp/pion-broadcast  (go build ./broadcast in pion/examples)
#   ${BIN:-/private/tmp}/sfu-loadgen     (go build in benches/sfu-load)
KIND=$1; N=$2; DUR=${3:-20s}
LOG=${BIN:-/private/tmp}/sfu-$KIND.log
pkill -f "$KIND-broadcast" 2>/dev/null; sleep 1
"${BIN:-/private/tmp}/$KIND-broadcast" $EXTRA_ARGS > "$LOG" 2>&1 &
SPID=$!
sleep 2
if [ -n "$PERF_OUT" ]; then
  # Profile the server for 15 s in the middle of the measurement window (PERF_OUT is the file prefix).
  ( sleep 20; perf record -F 499 -g -p $SPID -o "$PERF_OUT.data" -- sleep 15 >/dev/null 2>&1 ) &
  PERFPID=$!
fi
${BIN:-/private/tmp}/sfu-loadgen -log "$LOG" -subs "$N" -duration "$DUR" -pid $SPID 2>&1 | grep -v "^$"
[ -n "$PERFPID" ] && wait $PERFPID
kill $SPID 2>/dev/null
