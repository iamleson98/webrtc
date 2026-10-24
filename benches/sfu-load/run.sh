#!/bin/bash
# usage: run.sh <rust|pion> <subscribers> [duration]
# Servers must be built and copied outside ~/Documents (macOS delays network delivery to binaries there):
#   /private/tmp/rust-broadcast  (cargo build --release --example broadcast)
#   /private/tmp/pion-broadcast  (go build ./broadcast in pion/examples)
#   /private/tmp/sfu-loadgen     (go build in benches/sfu-load)
KIND=$1; N=$2; DUR=${3:-20s}
LOG=/private/tmp/sfu-$KIND.log
pkill -f "$KIND-broadcast" 2>/dev/null; sleep 1
"/private/tmp/$KIND-broadcast" > "$LOG" 2>&1 &
SPID=$!
sleep 2
/private/tmp/sfu-loadgen -log "$LOG" -subs "$N" -duration "$DUR" -pid $SPID 2>&1 | grep -v "^$"
kill $SPID 2>/dev/null
