# rtcd-rust

A Rust replacement for the Go `rtcd` (the Mattermost Calls SFU, as the LMS runs it), built on the
sans-I/O [`rtc`](vendor/rtc) crate. It speaks the same wire protocol, reads the same configuration, and
can be swapped in for the Go service without changing the LMS server or the web client.

## Compatibility with the Go rtcd

| | |
|---|---|
| Control plane | the same HTTP API (`/version`, `/register`, `/login`, `/unregister`, `/system/info`, `/calls/...`, `/metrics`) and the same msgpack websocket messages. Checked against golden vectors produced by the Go code (`crates/rtcd-proto/tests/golden`) and by the original Go service client |
| Configuration | the same `config.toml` keys and the same `RTCD_*` environment variables, including `:8045`-style addresses, `ice_host_override` NAT maps, STUN discovery, TURN credential generation, TLS |
| Container | `Dockerfile` reuses the LMS image's entrypoint and config template, so the environment, ports (8045, 8443), user (2001) and `/data` volume are unchanged |
| Data | a data directory the Go rtcd used is opened as is: its registered clients are taken over on the first start (`crates/rtcd/src/legacy.rs`) |
| Media | ICE-lite over UDP and ICE-TCP, DTLS-SRTP, the `calls-dc` data channel (lock protocol, zlib SDP, media map), Opus voice with VAD events, VP8 camera and screen share with simulcast layer switching that needs no renegotiation, `video_subscribe`, keyframe relay, BWE-driven screen layer choice |
| Metrics | the Prometheus metric names are the Go service's |

The LMS web app's own call client (`harness/browser/lms`) runs against it in headless Chrome in CI,
next to the Go service, with the same checks.

## Measuring network usage

The metrics are Prometheus counters, meant for a time-series database that measures traffic and
predicts the network bill (`increase(...[1d])` is GB per day):

| metric | what it counts |
|---|---|
| `rtcd_net_bytes_total{direction,transport}` | everything that crossed the media sockets (RTP, RTCP, STUN, DTLS), in and out, UDP and TCP. IP and UDP headers add 28 bytes per IPv4 datagram on top |
| `rtcd_net_packets_total{direction,transport}` | the same in packets, for the header overhead |
| `rtcd_net_dropped_packets_total` | datagrams the kernel refused to send (a full send buffer) |
| `rtcd_rtc_rtp_bytes_sent_total{type}` | RTP bytes sent to participants by `voice`, `video`, `screen`, `screen-audio` (the Go service's metric) |
| `rtcd_rtc_rtp_bytes_received_total{type}` | the uplink, by the same types |
| `rtcd_rtc_rtp_tracks_writes_time{groupID,type}` | time to write one packet to one receiver (a sample of the writes) |

## Layout

```
crates/rtcd-proto   wire formats (msgpack messages, data channel framing, session config, TURN)
crates/rtcd-net     batched UDP (recvmmsg/sendmmsg/GSO on Linux), RFC 4571 framing, interfaces, STUN
crates/rtcd-sfu     the engine (one per shard thread, no locks) and the runtime around real sockets
crates/rtcd         the service: config, auth, store, HTTP and websocket API, TLS, metrics, logging
crates/rtcd-testkit an emulated browser used by the engine tests
vendor/rtc          the WebRTC stack, pinned (git submodule)
compat/golden       regenerates the golden vectors from the Go sources
```

## Build and test

```bash
git submodule update --init --recursive
cargo build --release -p rtcd
cargo test --workspace
```

Run with the same arguments as the Go binary: `rtcd -config config.toml`.

## Not done yet

- One shard only: the runtime refuses `shards > 1` until datagrams and TCP streams can be routed
  between shard threads. One shard carried 300 participants at about 57% of a core in the load test.
- AV1.
