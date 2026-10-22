#!/usr/bin/env python3
"""Whole-connection benchmarks, webrtc-rs against pion, side by side.

    python3 benches/pion/compare.py                 # 5 runs each, print a table, enforce the gates
    python3 benches/pion/compare.py --runs 9 --no-gate
    python3 benches/pion/compare.py --summary "$GITHUB_STEP_SUMMARY"

What is measured (the Go twin in main.go mirrors the Rust benches in ../*.rs):

* data-channel throughput — 256 MiB of 16 KiB messages between two peer connections in one
  process over loopback, with application-level back-pressure at 8 MiB buffered; reported with the
  CPU time (user + system) and peak resident memory the whole process used;
* connect latency — first API call to an open data channel, median of 30 rounds;
* media forwarding — 60,000 RTP packets of 1200 bytes, paced at 12,000 per second, through SRTP and
  the default interceptors between two peer connections; CPU time and peak memory of the process
  (the number that decides how many streams a media server carries per core), and delivery;
* memory — 200 idle connected peer connections (100 pairs, one data channel each) in one process,
  reported as the peak resident memory and CPU time of the whole process.

Rust is measured written both ways an application writes it: `throughput_send` waits in
`send().await` against a send-buffer limit, `throughput` polls `outstanding_bytes()` in a loop
(pion's own bench polls its buffered amount too, so that is the closest like-for-like).

Gates: Rust must reach at least `--min-throughput-ratio` of pion's throughput, use no more CPU
than pion and no more than `--max-rss-ratio` of its memory, hold 200 idle connections in no more
memory than pion, and connect no slower than pion.
Shared CI runners are noisy, so medians of several runs are compared with generous margins.
"""
import argparse
import json
import os
import re
import resource
import statistics
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))


def run(cmd, **kw):
    print("+", " ".join(cmd), flush=True)
    return subprocess.run(cmd, check=True, **kw)


def bench_executable(name):
    """Build the named cargo bench and return the path of its executable."""
    out = subprocess.run(
        ["cargo", "bench", "--no-run", "--bench", name, "--message-format=json"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    for line in out.splitlines():
        try:
            msg = json.loads(line)
        except ValueError:
            continue
        if msg.get("reason") == "compiler-artifact" and msg.get("target", {}).get("name") == name:
            if msg.get("executable"):
                return msg["executable"]
    sys.exit(f"could not find the executable for bench {name}")


def measure(cmd, pattern):
    """Run `cmd` once; return (regex groups from its output, cpu seconds, peak RSS MiB)."""
    with tempfile.TemporaryFile("w+") as out:
        proc = subprocess.Popen(cmd, stdout=out, stderr=subprocess.STDOUT)
        _, status, usage = os.wait4(proc.pid, 0)
        proc.returncode = os.waitstatus_to_exitcode(status)
        out.seek(0)
        text = out.read()
    if proc.returncode != 0:
        sys.exit(f"{cmd[0]} failed:\n{text}")
    m = re.search(pattern, text)
    if not m:
        sys.exit(f"no result line from {cmd[0]}:\n{text}")
    rss = usage.ru_maxrss / (1024 * 1024 if sys.platform == "darwin" else 1024)
    return m.groups(), usage.ru_utime + usage.ru_stime, rss


def many(cmd, pattern, runs):
    samples = [measure(cmd, pattern) for _ in range(runs)]
    return samples


def median(xs):
    return statistics.median(xs)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--no-gate", action="store_true")
    ap.add_argument("--summary", help="append a markdown table to this file")
    ap.add_argument("--min-throughput-ratio", type=float, default=0.9)
    ap.add_argument("--max-rss-ratio", type=float, default=1.0)
    args = ap.parse_args()

    run(["cargo", "bench", "--no-run", "--bench", "throughput", "--bench", "throughput_send", "--bench", "latency", "--bench", "memory", "--bench", "media"], cwd=ROOT)
    exe = {n: bench_executable(n) for n in ("throughput", "throughput_send", "latency", "memory", "media")}
    with tempfile.TemporaryDirectory() as tmp:
        go_bin = os.path.join(tmp, "pion-bench")
        run(["go", "build", "-o", go_bin, "."], cwd=HERE)

        results = {}
        thr = r"throughput_mibs=([0-9.]+)"
        for label, cmd, pat in [
            ("webrtc-rs (send().await)", [exe["throughput_send"], "--nocapture"], thr),
            ("webrtc-rs (poll outstanding_bytes)", [exe["throughput"], "--nocapture"], thr),
            ("pion", [go_bin, "-mode", "b1"], thr),
        ]:
            samples = many(cmd, pat, args.runs)
            results[label] = {
                "mibs": median(float(s[0][0]) for s in samples),
                "cpu": median(s[1] for s in samples),
                "rss": median(s[2] for s in samples),
            }

        lat = r"median_ms=([0-9.]+)"
        rust_lat = median(float(s[0][0]) for s in many([exe["latency"], "30"], lat, args.runs))
        pion_lat = median(float(s[0][0]) for s in many([go_bin, "-mode", "b3", "-n", "30"], lat, args.runs))

        media = {}
        for label, cmd in [("webrtc-rs", [exe["media"]]), ("pion", [go_bin, "-mode", "b4"])]:
            samples = [measure(cmd, r"received=(\d+)") for _ in range(args.runs)]
            media[label] = {
                "cpu": median(s[1] for s in samples),
                "rss": median(s[2] for s in samples),
                "received": min(int(s[0][0]) for s in samples),
            }

        # Idle connections: 100 pairs = 200 peer connections on each side. The Rust bench takes the
        # pair count as its first argument (so no `--nocapture` before it).
        mem = {}
        for label, cmd, pat in [
            ("webrtc-rs", [exe["memory"], "100"], r"connections=(\d+)"),
            ("pion", [go_bin, "-mode", "b2", "-n", "100"], r"connections=(\d+)"),
        ]:
            samples = many(cmd, pat, args.runs)
            mem[label] = {"rss": median(s[2] for s in samples), "cpu": median(s[1] for s in samples)}

    pion = results["pion"]
    rows, failures = [], []
    print()
    print(f"{'':36} {'MiB/s':>8} {'CPU s':>7} {'RSS MiB':>8}")
    for label, r in results.items():
        print(f"{label:36} {r['mibs']:>8.1f} {r['cpu']:>7.2f} {r['rss']:>8.1f}")
        rows.append((label, r))
    print(f"{'connect latency (median ms)':36} rust {rust_lat:.1f}   pion {pion_lat:.1f}")
    for label, m in media.items():
        print(f"{'media 60k pkts: ' + label:36} {m['cpu']:>7.2f} CPU-s {m['rss']:>7.1f} MiB  delivered {m['received']}")
    print(f"{'200 idle connections: RSS MiB':36} rust {mem['webrtc-rs']['rss']:.1f}   pion {mem['pion']['rss']:.1f}")
    print(f"{'200 idle connections: CPU s':36} rust {mem['webrtc-rs']['cpu']:.2f}   pion {mem['pion']['cpu']:.2f}")

    for label, r in results.items():
        if label == "pion":
            continue
        if r["mibs"] < pion["mibs"] * args.min_throughput_ratio:
            failures.append(f"{label}: {r['mibs']:.1f} MiB/s is below {args.min_throughput_ratio:.0%} of pion's {pion['mibs']:.1f}")
        if r["cpu"] > pion["cpu"]:
            failures.append(f"{label}: used {r['cpu']:.2f} CPU-s against pion's {pion['cpu']:.2f}")
        if r["rss"] > pion["rss"] * args.max_rss_ratio:
            failures.append(f"{label}: peak RSS {r['rss']:.1f} MiB exceeds pion's {pion['rss']:.1f}")
    if rust_lat > max(pion_lat * 1.2, pion_lat + 1):
        failures.append(f"connect latency {rust_lat:.1f} ms against pion's {pion_lat:.1f}")
    if media["webrtc-rs"]["cpu"] > media["pion"]["cpu"]:
        failures.append(f"media: {media['webrtc-rs']['cpu']:.2f} CPU-s against pion's {media['pion']['cpu']:.2f}")
    if media["webrtc-rs"]["rss"] > media["pion"]["rss"] * args.max_rss_ratio:
        failures.append(f"media: peak RSS {media['webrtc-rs']['rss']:.1f} MiB exceeds pion's {media['pion']['rss']:.1f}")
    if media["webrtc-rs"]["received"] < 0.99 * 60000:
        failures.append(f"media: only {media['webrtc-rs']['received']} of 60000 packets delivered")
    if mem["webrtc-rs"]["rss"] > mem["pion"]["rss"] * args.max_rss_ratio:
        failures.append(
            f"200 idle connections: peak RSS {mem['webrtc-rs']['rss']:.1f} MiB exceeds pion's {mem['pion']['rss']:.1f}"
        )

    if args.summary:
        import platform

        with open(args.summary, "a") as f:
            f.write(f"### Data channel, webrtc-rs vs pion — {platform.system()} {platform.machine()}\n\n")
            f.write("| | MiB/s | CPU s | peak RSS MiB |\n|---|--:|--:|--:|\n")
            for label, r in rows:
                f.write(f"| {label} | {r['mibs']:.1f} | {r['cpu']:.2f} | {r['rss']:.1f} |\n")
            f.write(f"\nConnect latency (median): webrtc-rs {rust_lat:.1f} ms, pion {pion_lat:.1f} ms\n\n")
            f.write("| media, 60k packets at 12k/s | CPU s | peak RSS MiB | delivered |\n|---|--:|--:|--:|\n")
            for label, m in media.items():
                f.write(f"| {label} | {m['cpu']:.2f} | {m['rss']:.1f} | {m['received']} |\n")
            f.write("\n")
            f.write("| 200 idle connections | peak RSS MiB | CPU s |\n|---|--:|--:|\n")
            for label, m in mem.items():
                f.write(f"| {label} | {m['rss']:.1f} | {m['cpu']:.2f} |\n")
            f.write("\n")
            if failures:
                f.write("**Gate failures**\n\n" + "\n".join(f"- {x}" for x in failures) + "\n\n")

    if failures and not args.no_gate:
        print("\nGATE FAILURES:", file=sys.stderr)
        for x in failures:
            print(" -", x, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
