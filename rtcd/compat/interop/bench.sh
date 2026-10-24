#!/bin/bash
# Measures a server under classroom load: compat/interop/bench.sh <path to rtcd binary> [load args]
#
# The same RTCD_* environment configures the Go and the Rust server, which is the point of the
# comparison. The load generator and the server share the machine, so absolute numbers say little;
# the same run against each binary is what compares.
#
#   RTCD_GO_SRC=~/Documents/class-management/rtcd compat/interop/bench.sh /path/to/rtcd -calls 3
set -euo pipefail
BIN=$1; shift
HERE=$(cd "$(dirname "$0")" && pwd)
RUN=$(mktemp -d)
trap 'kill "$PID" 2>/dev/null || true; [ -s "$RUN/server.log" ] && { echo "--- server log (tail)"; tail -20 "$RUN/server.log"; }; rm -rf "$RUN"' EXIT
# A fresh port per run: a SIGTERMed server drains its sessions and keeps the old port for a while.
PORT=${BENCH_PORT:-$((20000 + RANDOM % 20000))}
(
  cd "$RUN"
  RTCD_STORE_DATASOURCE="$RUN/db" RTCD_LOGGER_ENABLEFILE=false RTCD_LOGGER_CONSOLELEVEL=ERROR \
  RTCD_API_SECURITY_ALLOWSELFREGISTRATION=true RTCD_API_HTTP_LISTENADDRESS=127.0.0.1:$PORT \
  RTCD_RTC_ICEPORTUDP=$((PORT+400)) RTCD_RTC_ICEPORTTCP=$((PORT+400)) \
  RTCD_RTC_ICEADDRESSUDP=127.0.0.1 RTCD_RTC_ICEADDRESSTCP=127.0.0.1 \
  exec "$BIN" > "$RUN/server.log" 2>&1
) &
PID=$!
sleep 2
kill -0 "$PID" 2>/dev/null || { echo "server exited at startup"; exit 1; }
"$HERE/run.sh" -url http://127.0.0.1:$PORT -pid "$PID" "$@"
