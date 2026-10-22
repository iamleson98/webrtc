#!/usr/bin/env python3
"""Pion test parity ledger.

Every `Test*` function in the pion releases pinned in `go.mod` has one row in `ledger/<module>.tsv`:

    <pion file>\t<pion test>\t<status>\t<ref>\t<note>

status
    rust    a Rust test reproduces the scenario; `ref` is that test's function name and must exist
            somewhere in this repository (`fn <ref>`).
    golden  the test is a pure function of its input (parse, marshal, verdict); the differential
            corpus replays pion's own inputs and verdicts through this stack. `ref` is the
            `pion_golden` test that does it.
    named   a Rust test of the same (snake-cased) name exists; matched by `seed`, not yet read
            against the pion test. Counted separately and to be promoted to `rust` or demoted.
    na      the test exercises something that has no counterpart here (Go-only API, GopherJS, the
            vnet network simulator, ORTC); `note` says why.
    gap     a behaviour pion tests and this stack does not (yet) have. Listed, counted, ratcheted:
            the number of gaps in a module may only go down (`gaps.max`).
    todo    not yet triaged. Allowed only up to the ceiling in `gaps.max` (`<module>:todo`), which
            only ever goes down.

    python3 tools/pion-parity/parity.py check            # CI: fails on missing/stale/unresolved rows
    python3 tools/pion-parity/parity.py seed             # add rows for new pion tests (name-matched or todo)
    python3 tools/pion-parity/parity.py report [--md F]  # coverage table
"""
import argparse
import collections
import json
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
LEDGER = os.path.join(HERE, "ledger")
STATUSES = ("rust", "named", "golden", "na", "gap", "todo")

SKIP_DIRS = {"target", ".git", "node_modules", "fuzz", "examples", "test-wasm", "e2e"}


def modules():
    out = []
    for line in open(os.path.join(HERE, "modules.tsv")):
        if line.startswith("#") or not line.strip():
            continue
        name, path, crates, dirs = line.rstrip("\n").split("\t")
        out.append((name, path, crates, dirs.split()))
    return out


def module_dir(path):
    """Directory of the pinned module in the Go module cache (downloading it if needed)."""
    out = subprocess.run(
        ["go", "mod", "download", "-json", path],
        cwd=HERE, check=True, capture_output=True, text=True,
    ).stdout
    return json.loads(out)["Dir"]


def pion_tests(base):
    """(relative file, test name) for each `Test*` function a normal `go test` would run."""
    found = []
    for dp, dn, fn in os.walk(base):
        dn[:] = sorted(d for d in dn if d not in SKIP_DIRS and not d.startswith("."))
        for f in sorted(fn):
            if not f.endswith("_test.go"):
                continue
            text = open(os.path.join(dp, f), errors="ignore").read()
            head = text[:600]
            # Files for GopherJS / WebAssembly never run under a native `go test`.
            if re.search(r"//go:build[^\n]*\b(js|wasm)\b", head) and "!js" not in head:
                continue
            if f.endswith(("_js_test.go", "_wasm_test.go")):
                continue
            rel = os.path.relpath(os.path.join(dp, f), base)
            for m in re.finditer(r"^func (Test\w*)\(", text, re.M):
                found.append((rel, m.group(1)))
    return found


def snake(name):
    n = re.sub(r"^Test_?", "", name)
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", n)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    return re.sub(r"_+", "_", s).lower().strip("_")


_rust = {}

# A function carrying a test attribute: #[test], #[tokio::test(flavor = ...)], #[rstest], ...
TEST_FN = re.compile(
    r"#\[(?:\w+::)*(?:test|rstest)\b[^\]]*\]\s*(?:#\[[^\]]*\]\s*)*(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+(\w+)"
)


def rust_fns(dirs=None):
    """Names of the test functions outside comments in `dirs` (relative to the repository root)."""
    key = tuple(dirs or ["."])
    if key not in _rust:
        found = set()
        for d in key:
            for dp, dn, fn in os.walk(os.path.join(ROOT, d)):
                dn[:] = [x for x in dn if x not in {"target", ".git", "node_modules", "pion"}]
                if d in ("src", "tests") and dp == os.path.join(ROOT, d):
                    dn[:] = [x for x in dn if x != "rtc"]
                for f in fn:
                    if f.endswith(".rs"):
                        text = open(os.path.join(dp, f), errors="ignore").read()
                        # Commented-out tests (ported once, disabled since) do not count.
                        text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
                        text = re.sub(r"^\s*//.*$", "", text, flags=re.M)
                        found.update(TEST_FN.findall(text))
        _rust[key] = found
    return _rust[key]


def read_ledger(name):
    path = os.path.join(LEDGER, name + ".tsv")
    rows = collections.OrderedDict()
    if os.path.exists(path):
        for n, line in enumerate(open(path), 1):
            if line.startswith("#") or not line.strip():
                continue
            parts = line.rstrip("\n").split("\t")
            parts += [""] * (5 - len(parts))
            f, test, status, ref, note = parts[:5]
            rows[(f, test)] = (status, ref, note, n)
    return rows


def write_ledger(name, rows):
    os.makedirs(LEDGER, exist_ok=True)
    with open(os.path.join(LEDGER, name + ".tsv"), "w") as fh:
        fh.write("# file\ttest\tstatus\tref\tnote   (see tools/pion-parity/parity.py)\n")
        for (f, t), (status, ref, note, _) in rows.items():
            fh.write(f"{f}\t{t}\t{status}\t{ref}\t{note}\n")


def guess(test, dirs):
    """The Rust test with this pion test's name, snake-cased, in the crate that answers to it."""
    want = snake(test)
    fns = rust_fns(dirs)
    for cand in (want, "test_" + want):
        if cand in fns:
            return cand
    return None


def load_gap_limits():
    limits = {}
    p = os.path.join(HERE, "gaps.max")
    if os.path.exists(p):
        for line in open(p):
            if line.strip() and not line.startswith("#"):
                k, v = line.split()
                limits[k] = int(v)
    return limits


def cmd_seed(args):
    for name, path, _, dirs in modules():
        if args.module and name != args.module:
            continue
        tests = pion_tests(module_dir(path))
        rows = read_ledger(name)
        keep = collections.OrderedDict()
        for key in tests:
            if key in rows:
                keep[key] = rows[key]
            else:
                g = guess(key[1], dirs)
                keep[key] = ("named", g, "", 0) if g else ("todo", "", "", 0)
        write_ledger(name, keep)
        c = collections.Counter(v[0] for v in keep.values())
        print(f"{name:12} {len(keep):4} tests  {dict(c)}")


def check_module(name, path, dirs, errors, todos):
    tests = pion_tests(module_dir(path))
    rows = read_ledger(name)
    fns = rust_fns()  # a ref may live anywhere in the repository
    counts = collections.Counter()
    seen = set()
    for key in tests:
        seen.add(key)
        if key not in rows:
            errors.append(f"{name}: no ledger row for {key[0]}::{key[1]}")
            continue
        status, ref, note, line = rows[key]
        counts[status] += 1
        if status not in STATUSES:
            errors.append(f"{name}.tsv:{line}: unknown status {status!r}")
        elif status == "todo":
            todos.append(f"{name}.tsv:{line}: {key[1]} is not triaged")
        elif status in ("rust", "named", "golden") and ref not in fns:
            errors.append(f"{name}.tsv:{line}: {key[1]} -> `fn {ref}` does not exist")
        elif status in ("na", "gap") and not note.strip():
            errors.append(f"{name}.tsv:{line}: {key[1]} is {status} without a note")
    for key, (_, _, _, line) in rows.items():
        if key not in seen:
            errors.append(f"{name}.tsv:{line}: {key[1]} is no longer in the pinned pion release")
    return len(tests), counts


def cmd_check(args):
    errors = []
    limits = load_gap_limits()
    total = collections.Counter()
    for name, path, _, dirs in modules():
        if args.module and name != args.module:
            continue
        todos = []
        n, c = check_module(name, path, dirs, errors, todos)
        total.update(c)
        # Untriaged rows may not grow: `triage.max` is the ceiling, lowered as the ledger is worked
        # through, until it reaches zero.
        if len(todos) > limits.get(name + ":todo", 0):
            errors.append(
                f"{name}: {len(todos)} untriaged pion tests, more than the {limits.get(name + ':todo', 0)} allowed"
                " (gaps.max); triage the new ones"
            )
            errors.extend(todos[:20])
        gaps = c.get("gap", 0)
        if gaps > limits.get(name, 0):
            errors.append(f"{name}: {gaps} gaps, more than the {limits.get(name, 0)} allowed (gaps.max)")
        print(f"{name:12} {n:4} pion tests  " + "  ".join(f"{k}={v}" for k, v in sorted(c.items())))
    print("total:", dict(total))
    for e in errors[:200]:
        print("ERROR", e)
    if len(errors) > 200:
        print(f"... and {len(errors) - 200} more")
    return 1 if errors else 0


def cmd_report(args):
    lines = ["| pion module | tests | rust | named | golden | n/a | gap |", "|---|--:|--:|--:|--:|--:|--:|"]
    tot = collections.Counter()
    for name, path, crates, dirs in modules():
        rows = read_ledger(name)
        c = collections.Counter(v[0] for v in rows.values())
        tot.update(c)
        lines.append(f"| {name} (`{crates}`) | {len(rows)} | {c['rust']} | {c['named']} | {c['golden']} | {c['na']} | {c['gap']} |")
    lines.append(
        f"| **total** | {sum(tot.values())} | {tot['rust']} | {tot['named']} | {tot['golden']} | {tot['na']} | {tot['gap']} |"
    )
    out = "\n".join(lines)
    print(out)
    if args.md:
        with open(args.md, "a") as fh:
            fh.write("### pion test parity\n\n" + out + "\n")
    return 0


def cmd_baseline(args):
    """Rewrite gaps.max from the ledgers as they stand (to lower the ceilings after triage work)."""
    lines = ["# <module>[:todo]  ceiling. May only ever be lowered; `parity.py baseline` rewrites it."]
    for name, _, _, _ in modules():
        c = collections.Counter(v[0] for v in read_ledger(name).values())
        lines.append(f"{name} {c['gap']}")
        lines.append(f"{name}:todo {c['todo']}")
    open(os.path.join(HERE, "gaps.max"), "w").write("\n".join(lines) + "\n")
    return 0


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    for n, fn in (("check", cmd_check), ("seed", cmd_seed), ("report", cmd_report), ("baseline", cmd_baseline)):
        p = sub.add_parser(n)
        p.add_argument("--module")
        if n == "report":
            p.add_argument("--md")
        p.set_defaults(fn=fn)
    args = ap.parse_args()
    sys.exit(args.fn(args))


if __name__ == "__main__":
    main()
