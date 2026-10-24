#!/usr/bin/env python3
"""Runs the sfu-load matrix (pion and webrtc-rs at several subscriber counts and tokio worker
counts), repeats each cell, and prints a markdown table of medians.

    BIN=/path/to/binaries python3 matrix.py [--subs 50,200] [--repeats 3] [--duration 30s]
"""
import argparse, os, re, statistics, subprocess, sys

HERE = os.path.dirname(os.path.abspath(__file__))

def run(kind, subs, duration, workers):
    env = dict(os.environ)
    if workers:
        env["TOKIO_WORKER_THREADS"] = str(workers)
    out = subprocess.run([os.path.join(HERE, "run.sh"), kind, str(subs), duration],
                         env=env, capture_output=True, text=True, timeout=900).stdout
    m = re.search(r"subs=(\d+) active=(\d+) .*loss=(-?[\d.]+)%", out)
    c = re.search(r"cpu=([\d.]+)s over ([\d.]+)s \(([\d.]+)% of one core\) rss=(\d+) MiB", out)
    if not (m and c):
        return None
    return dict(active=int(m.group(2)), loss=float(m.group(3)), cpu=float(c.group(3)), rss=int(c.group(4)))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--subs", default="50,200")
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--duration", default="30s")
    args = ap.parse_args()
    configs = [("pion", None, "pion v4.2.24")]
    for w in (1, 2, 4, None):
        configs.append(("rust", w, f"webrtc-rs, tokio workers={w if w else 'default'}"))
    print(f"cores: {os.cpu_count()}\n")
    print("| subscribers | server | CPU % of one core (median) | RSS MiB | delivery | runs |")
    print("|---|---|---|---|---|---|")
    for subs in [int(x) for x in args.subs.split(",")]:
        for kind, workers, label in configs:
            runs = [r for r in (run(kind, subs, args.duration, workers) for _ in range(args.repeats)) if r]
            if not runs:
                print(f"| {subs} | {label} | failed | | | 0 |")
                continue
            cpu = statistics.median(r["cpu"] for r in runs)
            rss = statistics.median(r["rss"] for r in runs)
            loss = statistics.median(r["loss"] for r in runs)
            active = min(r["active"] for r in runs)
            print(f"| {subs} | {label} | {cpu:.1f} | {rss} | {100-loss:.2f}% ({active}/{subs} subscribers active) | {len(runs)} |")
            sys.stdout.flush()

main()
