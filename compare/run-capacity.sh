#!/bin/bash
# How a server holds up under bigger and longer classroom load than the suite's, on the shape of a
# small VPS (the OVH box rtcd runs on: 4 CPUs, 8 GB):
#
#   compare/run-capacity.sh go|rust <output dir> [classes...]     (30 participants a class)
#
# Environment:
#   SECONDS_PER_RUN   length of each run's measured window (default 120)
#   SERVER_CPUS       CPUs the server is pinned to (default 0-3, so it sees four and, as on the VPS,
#                     picks the shards it would pick there)
#   LOADGEN_CPUS      CPUs for the load generator (default: those the server does not have, when
#                     there are any; on a four-CPU runner it shares them with the server, which is
#                     harsher than a VPS whose clients are remote)
#   MIN_DELIVERY      voice delivery (%) below which the run fails (default 85)
#   MAX_WORST_LOSS    worst single receiver's loss (%) above which the run fails (default 30)
#                     (measured on a four-CPU VM with the load generator on two other CPUs: 900
#                     participants deliver 90-96 % and 600 deliver 98-100 %; a single shard at
#                     900 delivered 60 % with a receiver losing 98 %. A runner shares its four CPUs
#                     with the load generator, so the bounds catch a collapse, not a few points.)
#   MAX_MEMORY_MIB    resident memory above which the run fails (default 4096: half the VPS)
#   MAX_GROWTH_MIB    growth of resident memory over the measured window above which the run fails
#                     (default 64: a leak per connection shows as growth with the clients steady)
#
# One text file per size (capacity-<n>.txt) and summary.md, which is also appended to the job
# summary. A run that misses a bound is recorded and fails the script at the end.
set -uo pipefail
SERVER=${1:?go or rust}
mkdir -p "${2:?output dir}"
OUT=$(cd "$2" && pwd)
shift 2
CLASSES=${*:-20 30}
ROOT=$(cd "$(dirname "$0")" && pwd)
H="$ROOT/harness/interop"
SECONDS_PER_RUN=${SECONDS_PER_RUN:-120}
SERVER_CPUS=${SERVER_CPUS:-0-3}
MIN_DELIVERY=${MIN_DELIVERY:-85}
MAX_WORST_LOSS=${MAX_WORST_LOSS:-30}
MAX_MEMORY_MIB=${MAX_MEMORY_MIB:-4096}
MAX_GROWTH_MIB=${MAX_GROWTH_MIB:-64}

ulimit -n 1048576 2>/dev/null || ulimit -n "$(ulimit -Hn)" 2>/dev/null || true

PIN=""
LOADGEN_PIN=""
if [ "$(uname)" = Linux ] && command -v taskset >/dev/null; then
  PIN="taskset -c $SERVER_CPUS"
  if [ -n "${LOADGEN_CPUS:-}" ]; then
    LOADGEN_PIN="taskset -c $LOADGEN_CPUS"
  elif [ "$(nproc --all)" -gt 4 ]; then
    LOADGEN_PIN="taskset -c 4-$(($(nproc --all) - 1))"
  fi
fi

echo "::group::$SERVER: build"
case $SERVER in
  go) (cd "$ROOT/rtcd-go" && go build -o "$OUT/rtcd.bin" ./cmd/rtcd) ;;
  rust) (cd "$ROOT/rtcd-rust" && cargo build --release -p rtcd && cp target/release/rtcd "$OUT/rtcd.bin") ;;
  *) echo "unknown server $SERVER"; exit 2 ;;
esac || { echo "build failed" > "$OUT/build-failed"; exit 1; }
# bench.sh starts the server by exec'ing this path, so the pinning goes in a wrapper that execs the
# binary: the server keeps the pid bench.sh measures.
printf '#!/bin/sh\nexec %s "%s"\n' "$PIN" "$OUT/rtcd.bin" > "$OUT/rtcd"
chmod +x "$OUT/rtcd"
echo "::endgroup::"

# The first number after a label on a line of the load generator's report.
field() { grep -E "$2" "$1" | head -1 | grep -oE "$3" | head -1; }

status=0
{
  echo "### $SERVER capacity (server on CPUs ${PIN:+$SERVER_CPUS}${PIN:-any}, ${SECONDS_PER_RUN}s per run)"
  echo
  echo "| participants | joined | join p95 | voice delivery | worst receiver loss | camera delivery | server CPU | memory | growth |"
  echo "|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
} > "$OUT/summary.md"

for classes in $CLASSES; do
  n=$((classes * 30))
  f="$OUT/capacity-$n.txt"
  echo "::group::$SERVER: $n participants ($classes classes), ${SECONDS_PER_RUN}s"
  $LOADGEN_PIN "$H/bench.sh" "$OUT/rtcd" -calls "$classes" -seconds "$SECONDS_PER_RUN" > "$f" 2>&1
  grep -E "joined|join time|voice|camera|server" "$f"

  joined=$(field "$f" "^joined" "[0-9]+" )
  failures=$(grep -E "^joined" "$f" | grep -oE "\([0-9]+ failures\)" | grep -oE "[0-9]+")
  p95=$(grep -E "^join time" "$f" | grep -oE "p95 [0-9.]+s" | grep -oE "[0-9.]+")
  voice=$(grep -E "^voice delivery" "$f" | grep -oE "[0-9.]+%" | head -1 | tr -d %)
  worst=$(grep -E "^voice delivery" "$f" | grep -oE "worst receiver [0-9.]+%" | grep -oE "[0-9.]+")
  camera=$(grep -E "^camera" "$f" | grep -oE "[0-9.]+%" | head -1 | tr -d %)
  cpu=$(grep -E "^server cpu" "$f" | grep -oE "[0-9.]+%" | head -1 | tr -d %)
  mem=$(grep -E "^server memory" "$f" | grep -oE "[0-9]+ MiB resident" | grep -oE "[0-9]+")
  mem0=$(grep -E "^server memory" "$f" | grep -oE "\([0-9]+ MiB at the start" | grep -oE "[0-9]+")
  growth=$(( ${mem:-0} - ${mem0:-0} ))

  verdict=""
  fail() { verdict="$verdict $1;"; status=1; }
  [ "${failures:-1}" = 0 ] || fail "joins failed"
  [ -n "$voice" ] || fail "no voice figure"
  awk -v v="${voice:-0}" -v m="$MIN_DELIVERY" 'BEGIN { exit !(v < m) }' && fail "voice delivery under $MIN_DELIVERY%"
  awk -v v="${worst:-100}" -v m="$MAX_WORST_LOSS" 'BEGIN { exit !(v > m) }' && fail "a receiver lost over $MAX_WORST_LOSS%"
  [ "${mem:-999999}" -le "$MAX_MEMORY_MIB" ] || fail "memory over $MAX_MEMORY_MIB MiB"
  [ "$growth" -le "$MAX_GROWTH_MIB" ] || fail "memory grew over $MAX_GROWTH_MIB MiB"
  echo "| $n | ${joined:-?} | ${p95:-?}s | ${voice:-?}% | ${worst:-?}% | ${camera:-?}% | ${cpu:-?}% of a core | ${mem:-?} MiB | ${growth} MiB |${verdict:+ **FAIL:$verdict**}" >> "$OUT/summary.md"
  echo "::endgroup::"
done

cat "$OUT/summary.md"
cat "$OUT/summary.md" >> "${GITHUB_STEP_SUMMARY:-/dev/null}"
rm -f "$OUT/rtcd" "$OUT/rtcd.bin"
exit $status
