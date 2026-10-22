#!/bin/bash
# Cross-implementation TURN interop: pion (Go) <-> rtc-turn (Rust), over real UDP sockets.
#
#   1. rtc-turn server <- pion client    (Allocate, CreatePermission, ChannelBind, data both ways)
#   2. pion server     <- rtc-turn client
#
# Plus same-stack sanity pairs to prove the harness itself is sound.
#
# Usage: interop/run-turn.sh
set -u

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
GO_DIR="$REPO_ROOT/interop/pion-turn"
RUST_BIN_DIR="$REPO_ROOT/rtc/target/debug/examples"
LOG="$REPO_ROOT/interop/interop-turn-results.log"
PION_BIN="${TMPDIR:-/tmp}/pion-turn"
PASSES=0
FAILS=0

[ -f "$HOME/.cargo/env" ] && . "$HOME/.cargo/env"
echo "=== PION <-> RTC-TURN INTEROP $(date) ===" | tee "$LOG"

( cd "$GO_DIR" && go build -o "$PION_BIN" . ) || { echo "HARNESS FAIL: go build"; exit 1; }
( cd "$REPO_ROOT/rtc" && cargo build -p rtc-turn --example turn_server_udp --example turn_echo_check ) \
    || { echo "HARNESS FAIL: cargo build"; exit 1; }

run_pair() {
    local name="$1" server_cmd="$2" client_cmd="$3"
    echo "--- $name ---" | tee -a "$LOG"
    ( $server_cmd ) >>"$LOG" 2>&1 &
    local server_pid=$!
    sleep 1
    local output
    if output=$( $client_cmd 2>>"$LOG" ); then
        echo "$output" | tee -a "$LOG"
        if echo "$output" | grep -q "status=PASS"; then
            PASSES=$((PASSES+1))
        else
            FAILS=$((FAILS+1))
        fi
    else
        echo "${output:-CLIENT ERROR (see $LOG)}" | tee -a "$LOG"
        FAILS=$((FAILS+1))
    fi
    kill "$server_pid" 2>/dev/null
    wait "$server_pid" 2>/dev/null
    sleep 0.5
}

run_pair "pion client -> rtc-turn server" \
    "$RUST_BIN_DIR/turn_server_udp --public-ip 127.0.0.1 --port 34791 --users user=pass" \
    "$PION_BIN --mode client --addr 127.0.0.1:34791 --user user=pass"

run_pair "rtc-turn client -> pion server" \
    "$PION_BIN --mode server --addr 127.0.0.1:34792 --user user=pass" \
    "$RUST_BIN_DIR/turn_echo_check --server 127.0.0.1:34792 --user user=pass"

run_pair "rtc-turn client -> rtc-turn server (sanity)" \
    "$RUST_BIN_DIR/turn_server_udp --public-ip 127.0.0.1 --port 34793 --users user=pass" \
    "$RUST_BIN_DIR/turn_echo_check --server 127.0.0.1:34793 --user user=pass"

run_pair "pion client -> pion server (sanity)" \
    "$PION_BIN --mode server --addr 127.0.0.1:34794 --user user=pass" \
    "$PION_BIN --mode client --addr 127.0.0.1:34794 --user user=pass"

echo "=== TURN INTEROP DONE: $PASSES passed, $FAILS failed ===" | tee -a "$LOG"
[ "$FAILS" -eq 0 ] || exit 1
