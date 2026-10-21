# ice-single-port

ice-single-port demonstrates webrtc-rs serving many PeerConnections on a single UDP port,
the Rust counterpart of Pion's [ice-single-port](https://github.com/pion/webrtc/tree/master/examples/ice-single-port)
example.

One `UdpMux` — one UDP socket — fronts every answering PeerConnection the signaling
endpoint creates. An SFU terminating thousands of subscribers this way holds a constant
number of file descriptors, kernel receive buffers and wakeups, instead of one of each per
connection.

Incoming datagrams are attributed to their connection by ICE username fragment (the first
STUN check a peer sends names the connection it is trying to reach) and then pinned to that
peer's address, so everything that follows — DTLS, SCTP, RTP — flows to the right
connection without another parse.

## Instructions

The signaling server listens on `:8080` (HTTP) and the shared WebRTC port on `:8443` (UDP):

```sh
cargo run --example ice-single-port
```

Open [http://localhost:8080](http://localhost:8080). Each press of **Start** creates a
fresh PeerConnection served by the same socket; the page prints the selected candidate
pair of each. Press it a few times and note they all answer on port 8443.

`--udp-port` and `--http-port` override the two ports.
