# sfu-load

One publisher, N subscribers, against the `broadcast` example of each stack, with the same load
generator (pion/webrtc client) for both. It answers the question that matters for a media server:
what does one forwarded stream cost in CPU and memory.

```sh
cargo build --release --example broadcast          # webrtc-rs
(cd <pion>/examples && go build -o /private/tmp/pion-broadcast ./broadcast)
(cd benches/sfu-load && go build -o /private/tmp/sfu-loadgen .)
cp target/release/examples/broadcast /private/tmp/rust-broadcast
benches/sfu-load/run.sh rust 50 30s      # or: pion
TOKIO_WORKER_THREADS=1 benches/sfu-load/run.sh rust 200 30s
```

**Run the binaries from outside `~/Documents` (here `/private/tmp`).** On macOS a binary under
`~/Documents` receives its first UDP datagrams 2-3 s late; the same binary elsewhere is 50x
faster. It is the OS, not the library.

The publisher sends 120 packets/s of 1100 bytes (about 1 Mbit/s); each subscriber receives the
whole stream. The server prints each answer on stdout, so the generator reads it from the log.
It reports delivery per subscriber and the server's CPU time and resident memory over the window.
Everything runs on one machine over loopback, so absolute numbers include the load generator's
share of kernel work; compare the two servers, not the figures to anything else.

## Results (macOS, 10 cores, release builds, 30 s windows, 120 pps x 1100 B)

| subscribers | server | CPU (% of one core) | RSS | delivery |
|---|---|---|---|---|
| 50 | pion v4.2.24 | 20 to 22 | 168 MiB | 99.98 % |
| 50 | webrtc-rs, default tokio (10 workers) | 31 to 32 | 37 MiB | 100 % |
| 50 | webrtc-rs, `TOKIO_WORKER_THREADS=1` | 14 | 37 MiB | 100 % |
| 50 | webrtc-rs, `TOKIO_WORKER_THREADS=2` | 21 | 37 MiB | 100 % |
| 200 | pion v4.2.24 | 28 | 641 MiB | 96.9 % (6 subscribers got nothing) |
| 200 | webrtc-rs, `TOKIO_WORKER_THREADS=1` | 29 | 102 MiB | 100 % |
| 200 | webrtc-rs, `TOKIO_WORKER_THREADS=4` | 51 | 102 MiB | 100 % |

What the numbers say: per stream the Rust stack uses 4.5 to 6 times less memory and delivers
everything; its CPU is at parity or better **only if the runtime does not spread tiny work over
many worker threads**. Every forwarded packet wakes a task, and with 10 work-stealing workers each
wake becomes a futex round trip (about half of the 50-subscriber profile was system time and lock
contention), so more workers cost more CPU here. A media server should run few worker threads per
room (or one runtime per core with rooms pinned to it) rather than the default of one per core.


## Results (Linux, GitHub Actions `ubuntu-latest`, 4 cores, release builds, 30 s windows, 2 runs, medians)

Run by `.github/workflows/sfu-load.yml`. "Packets per frame" is how the publisher sends its 120 packets/s:
1 = evenly spaced, 8 = a video frame as a back-to-back burst (what an encoder does; it lets the sender
coalesce datagrams with UDP GSO). Delivery was 99.9 to 100 % in every cell.

| subscribers | packets/frame | pion v4.2.24 | webrtc-rs async (best config) | sfu-core, sendmmsg + GSO |
|---|---|---|---|---|
| 50 | 1 | 13.1 % / 149 MiB | 12.1 % / 32 MiB | **7.6 % / 23 MiB** (6.0 % without batching) |
| 50 | 8 | 9.3 % / 150 MiB | 7.1 % / 32 MiB | **3.5 % / 24 MiB** |
| 200 | 1 | 39.9 % / 562 MiB | 42.3 % / 96 MiB | **23.9 % / 66 MiB** |
| 200 | 8 | 32.8 % / 562 MiB | 26.9 % / 94 MiB | **10.8 % / 70 MiB** |

`sfu-core` (`benches/sfu-load/sfu-core`) is a prototype of the architecture for a production SFU: the sans-IO
`rtc` crate driven by **one thread that owns every peer connection and the shared UDP socket**. A datagram is
read, handed to its connection, the publisher's RTP is rewritten and written to every viewer's sender in the
same loop turn, and all resulting datagrams leave in one `sendmmsg` with equal-size runs per viewer coalesced
by UDP GSO. No packet crosses a task or a thread.

Why the async examples cost more: a Linux `perf` profile at 200 subscribers shows 45 % of the async example's
CPU in user space (task queues, event notifications, wakeups, per-hop pipeline work) against 27 % for pion,
whose cost is mostly the kernel (56 to 65 %). `sfu-core` removes the user-space hops and also batches the
kernel work. `.github/workflows/sfu-profile.yml` produces those profiles; `sfu-soak.yml` is the leak check
(viewers keep leaving and being replaced for a long time; memory must stay flat).

Memory per viewer, measured with `heap`: about 290 KB, of which the NACK send buffer is 128 KB at pion's default
size of 1024 packets (`sfu-core --nack-size 256`, rtcd's default, cuts it to 32 KB) and about 670 small allocations
are the per-connection copy of the codec list.
