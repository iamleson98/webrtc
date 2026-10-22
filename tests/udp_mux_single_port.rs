//! UDP mux end-to-end: many peer connections served from one shared UDP socket.
//!
//! The mux is the Rust counterpart of pion's `ice.NewMultiUDPMuxFromPort` +
//! `SettingEngine.SetICEUDPMux` (the ice-single-port example). These tests drive it through
//! real sockets, not the mux's own unit-level routing harness:
//!
//! * one answerer on a mux-bound loopback port exchanges data-channel traffic with a
//!   normally-bound offerer, and the port its candidates advertise is the mux's one port —
//!   the whole point of the feature;
//! * **two** answerers share one mux while two offerers talk to both at once, proving the
//!   demultiplexing by ICE username fragment keeps connections separate on the shared
//!   socket: the same (mux port) serves both, and no message crosses over;
//! * a datagram from a stranger is dropped and counted, without disturbing the connected
//!   pair — a single misbehaving peer must not tax the others;
//! * on a host with non-loopback interfaces, a wildcard-bound mux works too. That path is
//!   the one that cannot advertise its bind address: every received datagram's local
//!   address is rebuilt from the interface it actually arrived on, which is what lets the
//!   ICE core match it to the advertised candidate. It is skipped where no usable
//!   interface exists (loopback-only CI), matching `wildcard_bind_expands_interfaces`.

use anyhow::Result;
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::sync::Arc;
use std::time::Duration;
use webrtc::data_channel::{DataChannel, DataChannelEvent, RTCDataChannelState};
use webrtc::peer_connection::udp_mux::UdpMux;
use webrtc::peer_connection::{
    PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCIceGatheringState,
    RTCPeerConnectionState,
};
use webrtc::runtime::{Receiver, Runtime, Sender, channel};

mod common;
use common::{block_on, runtime, sleep, timeout};

use rtc::shared::ifaces::ifaces;

const GATHER_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

/// The next message on `rx`, or a labelled error for the two ways it cannot arrive.
async fn recv_echoed(rx: &mut Receiver<String>, what: &str) -> Result<String> {
    match timeout(CONNECT_TIMEOUT, rx.recv()).await {
        Ok(Some(msg)) => Ok(msg),
        Ok(None) => Err(anyhow::anyhow!(
            "{what}: data channel closed before the echo"
        )),
        Err(_) => Err(anyhow::anyhow!("{what}: echo never arrived")),
    }
}

/// Waits for the data channel to report `Open` — `send_text` refuses before that.
async fn wait_open(dc: &Arc<dyn DataChannel>, what: &str) -> Result<()> {
    timeout(CONNECT_TIMEOUT, async {
        loop {
            if matches!(dc.ready_state().await, Ok(RTCDataChannelState::Open)) {
                return;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("{what}: data channel never opened"))
}

/// Offerer-side handler: signals gathering completion and connection.
struct OfferHandler {
    gather_tx: Sender<()>,
    connected_tx: Sender<()>,
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for OfferHandler {
    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        if state == RTCIceGatheringState::Complete {
            let _ = self.gather_tx.try_send(());
        }
    }

    async fn on_connection_state_change(&self, state: RTCPeerConnectionState) {
        if state == RTCPeerConnectionState::Connected {
            let _ = self.connected_tx.try_send(());
        }
    }
}

/// Answerer-side handler: signals gathering/connection and echoes data-channel traffic.
struct AnswerHandler {
    runtime: Arc<dyn Runtime>,
    gather_tx: Sender<()>,
    connected_tx: Sender<()>,
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for AnswerHandler {
    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        if state == RTCIceGatheringState::Complete {
            let _ = self.gather_tx.try_send(());
        }
    }

    async fn on_connection_state_change(&self, state: RTCPeerConnectionState) {
        if state == RTCPeerConnectionState::Connected {
            let _ = self.connected_tx.try_send(());
        }
    }

    async fn on_data_channel(&self, dc: Arc<dyn DataChannel>) {
        self.runtime.spawn(Box::pin(async move {
            while let Some(event) = dc.poll().await {
                match event {
                    DataChannelEvent::OnMessage(msg) => {
                        let data = String::from_utf8(msg.data.to_vec()).unwrap_or_default();
                        if let Err(err) = dc.send_text(&data).await {
                            log::error!("mux answerer failed to echo: {err}");
                        }
                    }
                    DataChannelEvent::OnClose => break,
                    _ => {}
                }
            }
        }));
    }
}

/// The UDP ports of the `host` candidates in an SDP.
fn udp_host_candidate_ports(sdp: &str) -> Vec<u16> {
    sdp.lines()
        .filter_map(|line| line.trim().strip_prefix("a=candidate:"))
        .filter_map(|line| {
            let fields: Vec<&str> = line.split_whitespace().collect();
            let [
                _foundation,
                _component,
                transport,
                _priority,
                _address,
                port,
                "typ",
                "host",
                ..,
            ] = fields[..]
            else {
                return None;
            };
            if !transport.eq_ignore_ascii_case("udp") {
                return None;
            }
            port.parse::<u16>().ok()
        })
        .collect()
}

/// One offerer↔answerer pair: the offerer bound the ordinary way, the answerer served by
/// `mux`.
struct MuxPair {
    offerer: Arc<dyn PeerConnection>,
    answerer: Arc<dyn PeerConnection>,
    data_channel: Arc<dyn DataChannel>,
}

impl MuxPair {
    async fn connect(mux: &Arc<UdpMux>, label: &str, offerer_addrs: &[&str]) -> Result<Self> {
        let runtime = runtime();

        let (offer_gather_tx, mut offer_gather_rx) = channel::<()>(1);
        let (offer_connected_tx, mut offer_connected_rx) = channel::<()>(1);
        let (answer_gather_tx, mut answer_gather_rx) = channel::<()>(1);
        let (answer_connected_tx, mut answer_connected_rx) = channel::<()>(1);

        let offerer: Arc<dyn PeerConnection> = Arc::new(
            PeerConnectionBuilder::new()
                .with_handler(Arc::new(OfferHandler {
                    gather_tx: offer_gather_tx,
                    connected_tx: offer_connected_tx,
                }))
                .with_runtime(runtime.clone())
                .with_udp_addrs(offerer_addrs.iter().map(|a| a.to_string()).collect())
                .build()
                .await?,
        );

        let answerer: Arc<dyn PeerConnection> = Arc::new(
            PeerConnectionBuilder::<SocketAddr>::new()
                .with_handler(Arc::new(AnswerHandler {
                    runtime: runtime.clone(),
                    gather_tx: answer_gather_tx,
                    connected_tx: answer_connected_tx,
                }))
                .with_runtime(runtime.clone())
                .with_udp_mux(mux.clone())
                .build()
                .await?,
        );

        let data_channel = offerer.create_data_channel(label, None).await?;

        // Offerer gathers, answerer answers with its own gathered description.
        let offer = offerer.create_offer(None).await?;
        offerer.set_local_description(offer).await?;
        timeout(GATHER_TIMEOUT, offer_gather_rx.recv())
            .await
            .map_err(|_| anyhow::anyhow!("offerer gathering did not complete"))?;
        let offer = offerer.local_description().await.expect("offer");

        answerer.set_remote_description(offer.clone()).await?;
        let answer = answerer.create_answer(None).await?;
        answerer.set_local_description(answer).await?;
        timeout(GATHER_TIMEOUT, answer_gather_rx.recv())
            .await
            .map_err(|_| anyhow::anyhow!("answerer gathering did not complete"))?;
        let answer = answerer.local_description().await.expect("answer");

        offerer.set_remote_description(answer).await?;

        timeout(CONNECT_TIMEOUT, offer_connected_rx.recv())
            .await
            .map_err(|_| anyhow::anyhow!("offerer never connected"))?;
        timeout(CONNECT_TIMEOUT, answer_connected_rx.recv())
            .await
            .map_err(|_| anyhow::anyhow!("answerer never connected"))?;

        Ok(Self {
            offerer,
            answerer,
            data_channel,
        })
    }

    /// The port the answerer's host candidates advertise — the mux's, in these tests.
    async fn advertised_port(&self) -> Result<u16> {
        let sdp = self.answerer.local_description().await.expect("answer").sdp;
        let ports = udp_host_candidate_ports(&sdp);
        assert!(
            !ports.is_empty(),
            "answerer advertised no UDP host candidates"
        );
        let port = ports[0];
        assert!(
            ports.iter().all(|p| *p == port),
            "host candidates disagree on port: {ports:?} — a mux answers on one port"
        );
        Ok(port)
    }
}

/// One answerer on a mux-bound loopback port: connects, echoes, and advertises the mux's
/// port — never a port of its own, because it bound no socket.
async fn single_pair_on_one_port() -> Result<()> {
    let runtime = runtime();
    let mux = UdpMux::bind("127.0.0.1:0")?;

    let pair = MuxPair::connect(&mux, "single", &["127.0.0.1:0"]).await?;
    assert_eq!(pair.advertised_port().await?, mux.local_addr().port());
    wait_open(&pair.data_channel, "single").await?;

    // Echo through the shared socket, one round trip each way.
    let (received_tx, mut received_rx) = channel::<String>(8);
    let dc = pair.data_channel.clone();
    runtime.spawn(Box::pin(async move {
        while let Some(event) = dc.poll().await {
            match event {
                DataChannelEvent::OnMessage(msg) => {
                    let _ = received_tx
                        .try_send(String::from_utf8(msg.data.to_vec()).unwrap_or_default());
                }
                DataChannelEvent::OnClose => break,
                _ => {}
            }
        }
    }));

    pair.data_channel.send_text("ping-through-mux").await?;
    let echoed = recv_echoed(&mut received_rx, "first mux echo").await?;
    assert_eq!(echoed, "ping-through-mux");

    // A datagram from a stranger is dropped and counted, and the live pair does not care.
    let before = mux.unrouted_datagrams();
    let stranger = UdpSocket::bind("127.0.0.1:0")?;
    stranger.send_to(b"not-stun-at-all", mux.local_addr())?;
    // Give the demux task a moment to read and drop it.
    sleep(Duration::from_millis(200)).await;
    assert!(
        mux.unrouted_datagrams() > before,
        "stray datagram must be counted, was {} -> {}",
        before,
        mux.unrouted_datagrams()
    );

    pair.data_channel.send_text("still-alive").await?;
    let echoed = recv_echoed(&mut received_rx, "echo after a stray datagram").await?;
    assert_eq!(echoed, "still-alive");

    pair.offerer.close().await?;
    pair.answerer.close().await?;
    mux.close();
    Ok(())
}

/// Two answerers share one mux; two offerers talk to both at once. Every connection is a
/// slice of the one socket, and the slices must not bleed into each other.
async fn two_pairs_share_one_port() -> Result<()> {
    let mux = UdpMux::bind("127.0.0.1:0")?;
    let mux_port = mux.local_addr().port();

    let pair_a = MuxPair::connect(&mux, "alpha", &["127.0.0.1:0"]).await?;
    let pair_b = MuxPair::connect(&mux, "beta", &["127.0.0.1:0"]).await?;

    assert_eq!(pair_a.advertised_port().await?, mux_port);
    assert_eq!(pair_b.advertised_port().await?, mux_port);
    wait_open(&pair_a.data_channel, "alpha").await?;
    wait_open(&pair_b.data_channel, "beta").await?;

    // A receiver per offerer, then send on both before asserting either: the connections
    // are up simultaneously and traffic is interleaved on the shared socket by design.
    let (rx_a_tx, mut rx_a) = channel::<String>(8);
    let (rx_b_tx, mut rx_b) = channel::<String>(8);

    let dc_a = pair_a.data_channel.clone();
    let dc_b = pair_b.data_channel.clone();
    runtime().spawn(Box::pin(async move {
        while let Some(event) = dc_a.poll().await {
            if let DataChannelEvent::OnMessage(msg) = event {
                let _ = rx_a_tx.try_send(String::from_utf8(msg.data.to_vec()).unwrap_or_default());
            }
        }
    }));
    runtime().spawn(Box::pin(async move {
        while let Some(event) = dc_b.poll().await {
            if let DataChannelEvent::OnMessage(msg) = event {
                let _ = rx_b_tx.try_send(String::from_utf8(msg.data.to_vec()).unwrap_or_default());
            }
        }
    }));

    pair_a.data_channel.send_text("alpha-1").await?;
    pair_b.data_channel.send_text("beta-1").await?;
    pair_a.data_channel.send_text("alpha-2").await?;
    pair_b.data_channel.send_text("beta-2").await?;

    assert_eq!(
        recv_echoed(&mut rx_a, "alpha's first echo").await?,
        "alpha-1",
        "alpha's first echo"
    );
    assert_eq!(
        recv_echoed(&mut rx_a, "alpha's second echo").await?,
        "alpha-2",
        "alpha's second echo — no beta traffic may cross over"
    );
    assert_eq!(
        recv_echoed(&mut rx_b, "beta's first echo").await?,
        "beta-1",
        "beta's first echo"
    );
    assert_eq!(
        recv_echoed(&mut rx_b, "beta's second echo").await?,
        "beta-2",
        "beta's second echo — no alpha traffic may cross over"
    );

    pair_a.offerer.close().await?;
    pair_a.answerer.close().await?;
    pair_b.offerer.close().await?;
    pair_b.answerer.close().await?;
    mux.close();
    Ok(())
}

/// Whether a datagram sent to this host's own `ip` is actually delivered to a socket bound to
/// the wildcard address.
///
/// Having an interface is not the same as being able to use it: a host firewall (macOS's
/// Application Firewall holds back unsigned binaries such as test executables), a container
/// network or a VPN can leave an interface up and addressed yet unreachable from the machine
/// itself. The test below is about how a mux handles traffic that *does* arrive, so it needs an
/// address that works; where there is none it is skipped rather than reported as a failure of
/// the code under test.
fn host_delivers_to_own_address(ip: IpAddr) -> bool {
    use std::net::UdpSocket;
    use std::time::Duration;

    let Ok(receiver) = UdpSocket::bind("0.0.0.0:0") else {
        return false;
    };
    let Ok(sender) = UdpSocket::bind("0.0.0.0:0") else {
        return false;
    };
    let _ = receiver.set_read_timeout(Some(Duration::from_millis(500)));
    let Ok(port) = receiver.local_addr().map(|a| a.port()) else {
        return false;
    };
    if sender.send_to(b"probe", (ip, port)).is_err() {
        return false;
    }
    let mut buf = [0u8; 16];
    matches!(receiver.recv_from(&mut buf), Ok((5, _)))
}

/// A wildcard-bound mux advertises per-interface candidates and rebuilds each datagram's
/// local address from the interface it arrived on. Only meaningful on a host with a
/// dialable non-loopback interface; skipped where there is none.
async fn wildcard_mux_uses_arrival_interface() -> Result<()> {
    let usable: Vec<IpAddr> = ifaces()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|interface| interface.addr.map(|addr| addr.ip()))
        .filter(|ip| {
            matches!(ip, IpAddr::V4(v4) if !v4.is_loopback() && !v4.is_unspecified() && !v4.is_link_local())
        })
        .filter(|ip| host_delivers_to_own_address(*ip))
        .collect();
    if usable.is_empty() {
        log::warn!("skipping wildcard mux test: no usable non-loopback interface");
        return Ok(());
    }

    let mux = UdpMux::bind("0.0.0.0:0")?;
    let pair = MuxPair::connect(&mux, "wildcard", &["0.0.0.0:0"]).await?;
    wait_open(&pair.data_channel, "wildcard").await?;

    // The advertised candidates are the mux port on each usable interface — never the
    // wildcard itself, which no peer could dial.
    let sdp = pair.answerer.local_description().await.expect("answer").sdp;
    let ports = udp_host_candidate_ports(&sdp);
    assert!(
        !ports.is_empty(),
        "wildcard mux advertised no host candidates"
    );
    assert!(
        ports.iter().all(|p| *p == mux.local_addr().port()),
        "all host candidates must carry the mux's port: {ports:?}"
    );

    // And the connection carries traffic — the arrival-interface rewrite is what let ICE
    // match the peer's checks to the advertised candidate.
    let (received_tx, mut received_rx) = channel::<String>(8);
    let dc = pair.data_channel.clone();
    runtime().spawn(Box::pin(async move {
        while let Some(event) = dc.poll().await {
            if let DataChannelEvent::OnMessage(msg) = event {
                let _ =
                    received_tx.try_send(String::from_utf8(msg.data.to_vec()).unwrap_or_default());
            }
        }
    }));
    pair.data_channel.send_text("wildcard-echo").await?;
    let echoed = recv_echoed(&mut received_rx, "wildcard mux echo").await?;
    assert_eq!(echoed, "wildcard-echo");

    pair.offerer.close().await?;
    pair.answerer.close().await?;
    mux.close();
    Ok(())
}

#[test]
fn test_single_pair_on_one_port() {
    block_on(async {
        single_pair_on_one_port()
            .await
            .expect("one pair served by one shared port");
    });
}

#[test]
fn test_two_pairs_share_one_port() {
    block_on(async {
        two_pairs_share_one_port()
            .await
            .expect("two pairs share one shared port without cross-talk");
    });
}

#[test]
fn test_wildcard_mux_uses_arrival_interface() {
    block_on(async {
        wildcard_mux_uses_arrival_interface()
            .await
            .expect("wildcard-bound mux works");
    });
}
