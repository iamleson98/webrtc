# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Security

- A UDP mux and an ordinary connection no longer create per-SSRC state for unauthenticated SRTP and
  SRTCP packets (inherited from `rtc`; see its changelog).

### Added

- **CI on Linux and macOS, x86-64 and arm64**, debug and release, tokio and smol, ring and aws-lc-rs;
  pion interop in both directions on every PR; nightly soak, stress and long-fuzz runs; a release
  workflow that verifies, then publishes behind an approval gate. See `rtc/docs/ci.md`.

### Changed

- **UDP sockets ask the kernel for 4 MiB send and receive buffers.** The Linux default (about
  208 KiB) is too small for a sender that emits whole congestion windows with GSO: one 64-datagram
  burst occupies roughly 150 KiB of the receive queue, a second before the reader runs overflows
  it, and every dropped datagram is a loss for SCTP to recover from. The kernel clamps the
  request to `net.core.rmem_max`/`wmem_max`, so asking is harmless; raise those sysctls on a busy
  server to get the full benefit.
- The driver flushes writes again after delivering messages to the application, so the window
  update described in `rtc`'s changelog leaves immediately rather than on the next wake-up.
- `DataChannel::outstanding_bytes()` and the wait inside `send().await` read the counter through
  `rtc`'s lock-free handle instead of locking the core on every call, so polling for send room no
  longer competes with the driver for the lock. The first call per channel still takes the lock once
  to fetch the handle.

### Added

- `benches/media.rs` and a media mode in the pion twin: RTP forwarding through SRTP and the default
  interceptors at a fixed rate, compared on CPU time, peak memory and delivery.
- `benches/pion/compare.py` also compares 200 idle connections (peak RSS and CPU) with pion, and
  gates on webrtc-rs using no more memory.
- `benches/throughput_send`: the data-channel transfer written the way most applications write
  it, with `send().await` against a send-buffer limit instead of polling `outstanding_bytes()`.

### Fixed

- `test_wildcard_mux_uses_arrival_interface` is skipped, rather than failing, on a host that cannot
  deliver a datagram to its own non-loopback address (a firewalled developer machine); and
  `test_ephemeral_ports_honor_the_range` draws its port window at random instead of using fixed ports
  that collide when runs overlap.

### Added

- **UDP mux — one socket for every connection**, pion's `SetICEUDPMux` + `ice.NewMultiUDPMuxFromPort` (the ice-single-port example). `UdpMux::bind` claims one UDP port once; `PeerConnectionBuilder::with_udp_mux` serves any number of connections from it — the SFU shape, a constant number of file descriptors, kernel receive buffers and wakeups instead of one per connection. A datagram finds its connection by the ICE username fragment of the first connectivity check and stays pinned to that peer's address; a wildcard-bound mux advertises one host candidate per interface and attributes each received datagram to the interface it actually arrived on. With a mux set the connection binds no UDP sockets of its own, re-registers its fragment on every ICE-restart rebind, and stray datagrams are dropped and counted without taxing anyone's connection. The [ice-single-port] example mirrors pion's: one signaling port, many PeerConnections, all answering on one UDP port.
- **SCTP zero checksum ([RFC 9653])**, pion's `SetEnableSCTPZeroChecksum`. `SettingEngineBuilder::with_sctp_enable_zero_checksum` opts the connection in; once both ends advertise the DTLS error-detection method, every data-channel packet drops the CRC-32C pass — one less full pass over every byte in both directions. The wire format matches pion/sctp byte-for-byte (parameter 0x8001, length 8, EDMID 1).
- **DTLS key logging**, pion's `SetDTLSKeyLogWriter`. `SettingEngineBuilder::with_dtls_key_logger` receives the session's `CLIENT_RANDOM` line (NSS key log format) once per handshake — point Wireshark at the file and the capture's DTLS and SRTP decrypt. `webrtc::peer_connection::KeyLogger` is re-exported so applications implement it without a direct rtc dependency.
- **Ephemeral UDP port range**, pion's `SetEphemeralUDPPortRange`. `PeerConnectionBuilder::with_udp_port_range(min, max)` makes `:0` UDP binds draw their port from `[min, max]` — probing from a random start and wrapping once — so a fleet of connections can be pinned inside a firewall's allowance. Concrete ports bind verbatim.

- **Crypto provider selection** ([webrtc#839](https://github.com/webrtc-rs/webrtc/issues/839),
  [rtc#128](https://github.com/webrtc-rs/rtc/issues/128)). New `crypto-ring` (default) and `crypto-aws-lc-rs`
  Cargo features forward to `rtc`. They are additive: enabling both compiles both providers and
  `ring` remains the resolved default, so a dependency enabling `crypto-aws-lc-rs` cannot silently change
  what an application runs. Building with neither compiles no provider, and the application
  supplies its own.
- `webrtc::peer_connection::crypto` re-exports the provider API. Without it
  `SettingEngine::set_crypto_provider` was uncallable from this crate — its signature names
  `Arc<dyn RTCCryptoProvider>`, which a user had no way to spell.
- Provider selection is per peer connection, so two connections in one process can use different
  providers. `tests/crypto_provider_integration.rs` covers same-provider, cross-provider, and
  application-supplied pairings end to end — ICE/STUN integrity, the DTLS handshake, and SRTP
  media over a live connection.
- Document pre-1.0 API stability / extensibility policy (see `docs/semver.md`).
### Changed

- No cryptography happens in this crate. The TURN client now takes its provider from the peer
  connection (`RTCPeerConnection::crypto_provider()`) rather than constructing one, so a
  connection uses exactly one provider throughout. A CI check asserts `webrtc` depends on no
  crypto implementation.
- **Bind addresses are resolved on every bind, and a wildcard means "every interface"**
  ([webrtc#874](https://github.com/webrtc-rs/webrtc/issues/874)). `with_udp_addrs` /
  `with_tcp_addrs` values are kept as configured instead of being resolved once at construction,
  so the ICE-restart rebind added in [webrtc#868](https://github.com/webrtc-rs/webrtc/issues/868)
  re-resolves them: a host name follows its DNS record, and `0.0.0.0` / `[::]` re-enumerates the
  local interfaces. A wildcard is no longer bound verbatim — one socket is bound per interface
  address (skipping loopback and link-local), which is what makes its host candidates usable, and
  what lets an ICE restart after a Wi-Fi/cellular handover pick up the interfaces the device has
  now. On a host with no usable interface the wildcard is bound as before. Because the configured
  addresses now outlive `build()`, `PeerConnectionBuilder::build` requires `A: Send + 'static` —
  owned addresses (`String`, `SocketAddr`, `&'static str`) are unaffected.
- **An address that cannot be bound is skipped rather than fatal**
  ([webrtc#874](https://github.com/webrtc-rs/webrtc/issues/874)). The failure is logged — `warn`
  for an enumerated interface address, `error` for one the application configured — and binding
  continues with the rest; only binding nothing at all is still an error. An address left behind
  by a network handover (`EADDRNOTAVAIL`) therefore no longer costs the connection the interfaces
  that are still there.

-

### Deprecated

-

### Removed

-

### Fixed

- **UDP sends refused before transmission are retried instead of left to the protocol's
  own retransmit timer.** On loopback, both ends of a fresh exchange send their first
  packets to each other in the same microsecond, confirming the same conntrack flow entry
  from two CPUs at once; the kernel drops the loser and reports `EPERM` to its `sendmsg`.
  Every packet is safe to re-send — the refused datagram provably never left the host — so
  the send funnel now retries twice after such refusals (`PermissionDenied`, and the
  `ConnectionRefused` a pending ICMP error surfaces as), parking a millisecond between
  attempts. A refusal that persists (a firewall REJECT) still surfaces as an error after
  the bounded retries. Measured effect on the connect-latency stress: the tail round that
  lost its ICE nomination to the clash and waited out the ~200 ms check interval
  (~1 round in 30, max 207 ms over 200 rounds) is gone — 500 rounds now run median 3 ms,
  **max 10 ms**.
- **Connect latency: 607 ms → 2 ms median.** Three stacked fixes, each a bug family the
  measurement kept unmasking: (1) the driver drained writes before events, but events
  are what *produce* writes — ICE selection starts DTLS, DTLS completing starts SCTP —
  so each handshake step queued behind a loop park whose only wake was the ~200 ms tick;
  the loop now drains events first, then writes, and every cascade step leaves in the
  same iteration. (2) The first DTLS ClientHello was buffered inside the endpoint when
  `connect` returned and nothing drained it in that event, so it rode one more tick;
  the rtc DTLS handler now pumps it into its write queue the moment `connect` returns.
  (3) Flushing the ClientHello early let it overtake the ICE nomination ahead of it on
  the wire — reaching a peer whose pair was not yet selected, getting dropped, and
  costing a full 1 s DTLS retransmit cycle; the rtc ICE handler now front-inserts its
  agent's transmits so the control plane can never be overtaken.
  `tests/connect_latency.rs` trip-wires both failure shapes: the median against the
  ~200 ms tick quantization, every round against the 1 s retransmit cycle.

### Security

-

## [0.20.0] - 2026-07-31

### Added

- The async `webrtc` v0.20.0 is a clean, ergonomic, runtime-agnostic rewrite on top of a Sans-I/O core `rtc`;
- It ships with Tokio and smol runtime backends, and any other runtime can be plugged in by implementing one trait.

[Unreleased]: https://github.com/webrtc-rs/webrtc/compare/0.20.0...HEAD

[0.20.1]: https://github.com/webrtc-rs/webrtc/compare/0.20.0...0.20.1

[0.20.0]: https://github.com/webrtc-rs/webrtc/releases/tag/0.20.0
