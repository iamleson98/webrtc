# rtcd-go vs rtcd-rust

```
rtcd-go/     the Go rtcd as the LMS runs it (its logging dependency vendored in third_party/)
rtcd-rust/   the Rust rtcd that replaces it (a submodule: github.com/iamleson98/rtcd-rs, with rtc inside it)
harness/     what runs against either one
  interop/   Go service client + pion: lookup, voice, camera, screen, reconnect; the browser bridge;
             the classroom load generator
  browser/   the LMS web app's call client (lms/) bundled for headless Chrome
run-suite.sh   build one server and run everything against it
report.py      compare the two servers' results
```

CI (`.github/workflows/compare.yml`) runs `run-suite.sh` for both servers on Ubuntu and macOS and then
`report.py`, which fails if the Rust service fails a check, delivers less than 99% of the media, or
uses more CPU or memory than Go at 300 participants.

## Run it locally

```bash
compare/run-suite.sh go   out/go
compare/run-suite.sh rust out/rust
mkdir results && mv out/go results/local-go && mv out/rust results/local-rust
compare/report.py results
```

Needs Go, a Rust toolchain, [bun](https://bun.sh) and Chrome. The load generator and the server share the
machine, so absolute numbers say little; the same run against each binary is what compares.
