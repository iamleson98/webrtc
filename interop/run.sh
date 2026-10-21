#!/bin/bash
# Cross-implementation interop: pion (Go) <-> webrtc-rs (Rust), over real sockets.
#
# Both directions traverse full ICE (STUN checks), DTLS and SCTP on loopback:
#
#   1. webrtc-rs server  <- pion client    (pion offers, webrtc-rs answers)
#   2. pion server       <- webrtc-rs client (webrtc-rs offers, pion answers)
#
# Plus same-stack sanity pairs to prove the harness itself is sound.
#
# Usage: interop/run.sh
set -u

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
GO_DIR="$REPO_ROOT/interop/pion-interop"
RUST_BIN_DIR="$REPO_ROOT/target/debug/examples"
LOG="$REPO_ROOT/interop/interop-results.log"
PASSES=0
FAILS=0

[ -f "$HOME/.cargo/env" ] && . "$HOME/.cargo/env"
echo "=== PION <-> WEBRTC-RS INTEROP $(date) ===" | tee "$LOG"

# Build both halves first: a compile failure is a harness failure, not an interop verdict.
if [ -n "${PION_CHECKOUT:-}" ] && [ -f "${PION_CHECKOUT}/go.mod" ]; then
    # Test against a local pion checkout instead of the published module.
    ( cd "$GO_DIR" && go mod edit -replace "github.com/pion/webrtc/v4=${PION_CHECKOUT}" && go mod tidy ) || exit 1
fi
( cd "$GO_DIR" && go build -o /tmp/pion-interop . ) || { echo "HARNESS FAIL: go build"; exit 1; }
( cd "$REPO_ROOT" && cargo build --example pion-interop --all-features ) || { echo "HARNESS FAIL: cargo build"; exit 1; }

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
        echo "CLIENT ERROR (see $LOG)" | tee -a "$LOG"
        FAILS=$((FAILS+1))
    fi
    kill "$server_pid" 2>/dev/null
    wait "$server_pid" 2>/dev/null
    sleep 0.5
}

# Cross direction 1: pion client offers -> webrtc-rs server answers.
run_pair "pion client -> webrtc-rs server" \
    "$RUST_BIN_DIR/pion-interop --mode server --http-port 9090" \
    "/tmp/pion-interop --mode client --url http://127.0.0.1:9090"

# Cross direction 2: webrtc-rs client offers -> pion server answers.
run_pair "webrtc-rs client -> pion server" \
    "/tmp/pion-interop --mode server --http_port 9091" \
    "$RUST_BIN_DIR/pion-interop --mode client --url http://127.0.0.1:9091"

# Same-stack sanity pairs: prove the harness works before trusting cross verdicts.
run_pair "webrtc-rs client -> webrtc-rs server (sanity)" \
    "$RUST_BIN_DIR/pion-interop --mode server --http-port 9092" \
    "$RUST_BIN_DIR/pion-interop --mode client --url http://127.0.0.1:9092"

run_pair "pion client -> pion server (sanity)" \
    "/tmp/pion-interop --mode server --http_port 9093" \
    "/tmp/pion-interop --mode client --url http://127.0.0.1:9093"

echo "=== INTEROP DONE: $PASSES passed, $FAILS failed ===" | tee -a "$LOG"
[ "$FAILS" -eq 0 ] || exit 1
