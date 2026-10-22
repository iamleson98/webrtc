module pion-parity

go 1.24.0

// The exact pion releases this repository is held to. The differential corpora in
// `rtc/rtc-*/tests/data/pion_*_golden.jsonl` were recorded against these versions, and the
// parity ledger (`ledger/*.tsv`) lists every `Test*` function they ship.
require (
	github.com/pion/datachannel v1.6.3
	github.com/pion/dtls/v3 v3.1.15
	github.com/pion/ice/v4 v4.4.8
	github.com/pion/interceptor v0.1.49
	github.com/pion/mdns/v2 v2.2.2
	github.com/pion/rtcp v1.2.20
	github.com/pion/rtp v1.10.6
	github.com/pion/sctp v1.12.0
	github.com/pion/sdp/v3 v3.0.20
	github.com/pion/srtp/v3 v3.1.3
	github.com/pion/stun/v4 v4.0.1
	github.com/pion/transport/v5 v5.1.1
	github.com/pion/turn/v5 v5.1.2
	github.com/pion/webrtc/v4 v4.2.24
)
