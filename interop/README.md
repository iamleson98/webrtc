# Interop: pion ↔ webrtc-rs

This directory is the cross-implementation harness: it connects **pion (Go)** and
**webrtc-rs (Rust)** peer connections to each other over real sockets, both directions,
and asserts byte-for-byte data-channel echo through the full stack — ICE connectivity
checks, DTLS, SCTP, and the data-channel protocol, each spoken by two independent
implementations that must agree on every byte.

Community interop work (the historic webrtc-rs `testrtc`, browser test pages, WHIP/WHEP
deployments) is exactly this shape: two stacks, one wire. Passing both directions here is
the strongest cross-verification available short of a browser.

## What runs

| # | offering side | answering side | proves |
|---|---------------|----------------|--------|
| 1 | pion client   | webrtc-rs server | pion's offer/answer, ICE, DTLS, SCTP parse by webrtc-rs, and vice versa |
| 2 | webrtc-rs client | pion server | the same, initiated by webrtc-rs |
| 3 | webrtc-rs ↔ webrtc-rs | (sanity) | the harness itself is sound |
| 4 | pion ↔ pion | (sanity) | the harness itself is sound |

Each client sends 100 messages and requires every one echoed back verbatim.

## Run

```sh
interop/run.sh
```

Requirements: Go 1.27+ and a Rust toolchain. The Go module fetches pion v4; to test
against a local pion checkout instead, `go mod edit -replace
github.com/pion/webrtc/v4=<path-to-checkout>` and rebuild.

The Rust half is also usable standalone:

```sh
cargo build --example pion-interop --all-features
target/debug/examples/pion-interop --mode server --http_port 9090
target/debug/examples/pion-interop --mode client --url http://127.0.0.1:9090
```

Results print as `*_CLIENT result: echoed=100/100 mismatched=0 status=PASS` lines and are
logged to `interop/interop-results.log`.
