//! SCTP zero checksum (RFC 9653) through a full peer connection — pion's
//! `SetEnableSCTPZeroChecksum` equivalent.
//!
//! `with_sctp_enable_zero_checksum` opts a connection into the negotiated error-detection
//! method: once both ends advertise it in the handshake, every data-channel packet skips
//! the CRC-32C pass — one less full pass over every byte, in both directions. The
//! negotiation itself is property-tested at the `rtc-sctp` endpoint level (both-agree →
//! on, one-sided → off, default → off); what these tests add is the **plumbing**: the
//! public `SettingEngine` option must reach the transport, a pair built with it must come
//! up and carry correct traffic, and a pair that disagrees must still connect — falling
//! back to checksums rather than failing.

use anyhow::Result;
use bytes::BytesMut;
use std::sync::Arc;
use std::time::Duration;
use webrtc::data_channel::{DataChannel, DataChannelEvent, RTCDataChannelInit, RTCDataChannelState};
use webrtc::peer_connection::{
    PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCIceGatheringState,
    RTCSessionDescription, SettingEngineBuilder,
};
use webrtc::runtime::{Runtime, Sender, channel};

mod common;
use common::{block_on, runtime, sleep, timeout};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const GATHER_TIMEOUT: Duration = Duration::from_secs(10);

/// 1 MiB in 16 KiB chunks: enough volume that a broken checksum path (packets corrupted
/// in flight and accepted, or dropped wholesale) could not pass by luck.
const PAYLOAD_BYTES: usize = 1 << 20;
const CHUNK_SIZE: usize = 16 << 10;

struct Handler {
    runtime: Arc<dyn Runtime>,
    gather_tx: Sender<()>,
    connected_tx: Sender<()>,
    bytes: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for Handler {
    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        if state == RTCIceGatheringState::Complete {
            let _ = self.gather_tx.try_send(());
        }
    }

    async fn on_connection_state_change(
        &self,
        state: webrtc::peer_connection::RTCPeerConnectionState,
    ) {
        if state == webrtc::peer_connection::RTCPeerConnectionState::Connected {
            let _ = self.connected_tx.try_send(());
        }
    }

    /// Echoes verbatim, counting what it saw so the sender can assert conservation.
    async fn on_data_channel(&self, dc: Arc<dyn DataChannel>) {
        let runtime = self.runtime.clone();
        let bytes = Arc::clone(&self.bytes);
        runtime.spawn(Box::pin(async move {
            while let Some(event) = dc.poll().await {
                match event {
                    DataChannelEvent::OnMessage(msg) => {
                        bytes.fetch_add(msg.data.len(), std::sync::atomic::Ordering::Relaxed);
                        let _ = dc.send(msg.data).await;
                    }
                    DataChannelEvent::OnClose => break,
                    _ => {}
                }
            }
        }));
    }
}

/// One offerer→answerer pair with per-end zero-checksum opt-in flags.
async fn connect_pair(
    offerer_zero_checksum: bool,
    answerer_zero_checksum: bool,
) -> Result<(
    Arc<dyn PeerConnection>,
    Arc<dyn PeerConnection>,
    Arc<dyn DataChannel>,
    Arc<std::sync::atomic::AtomicUsize>,
)> {
    let runtime = runtime();

    let build = |handler: Handler, zero: bool| -> PeerConnectionBuilder<String> {
        let builder = PeerConnectionBuilder::new()
            .with_handler(Arc::new(handler))
            .with_runtime(runtime.clone())
            .with_udp_addrs(vec!["127.0.0.1:0".to_string()]);
        if zero {
            builder.with_setting_engine(
                SettingEngineBuilder::new()
                    .with_sctp_enable_zero_checksum(true)
                    .build(),
            )
        } else {
            builder
        }
    };

    let (o_gather_tx, mut o_gather_rx) = channel::<()>(1);
    let (o_connected_tx, mut o_connected_rx) = channel::<()>(1);
    let offerer: Arc<dyn PeerConnection> = Arc::new(
        build(
            Handler {
                runtime: runtime.clone(),
                gather_tx: o_gather_tx,
                connected_tx: o_connected_tx,
                bytes: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            },
            offerer_zero_checksum,
        )
        .build()
        .await?,
    );

    let (a_gather_tx, mut a_gather_rx) = channel::<()>(1);
    let (a_connected_tx, mut a_connected_rx) = channel::<()>(1);
    let answerer_bytes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let answerer: Arc<dyn PeerConnection> = Arc::new(
        build(
            Handler {
                runtime: runtime.clone(),
                gather_tx: a_gather_tx,
                connected_tx: a_connected_tx,
                bytes: Arc::clone(&answerer_bytes),
            },
            answerer_zero_checksum,
        )
        .build()
        .await?,
    );

    let dc = offerer
        .create_data_channel(
            "zero",
            Some(RTCDataChannelInit {
                ordered: true,
                ..Default::default()
            }),
        )
        .await?;

    let offer = offerer.create_offer(None).await?;
    offerer.set_local_description(offer).await?;
    timeout(GATHER_TIMEOUT, o_gather_rx.recv())
        .await
        .map_err(|_| anyhow::anyhow!("offerer gathering timed out"))?;
    let offer = offerer.local_description().await.expect("offer");

    answerer.set_remote_description(offer).await?;
    let answer = answerer.create_answer(None).await?;
    answerer.set_local_description(answer).await?;
    timeout(GATHER_TIMEOUT, a_gather_rx.recv())
        .await
        .map_err(|_| anyhow::anyhow!("answerer gathering timed out"))?;
    let answer: RTCSessionDescription = answerer.local_description().await.expect("answer");

    offerer.set_remote_description(answer).await?;
    timeout(CONNECT_TIMEOUT, o_connected_rx.recv())
        .await
        .map_err(|_| anyhow::anyhow!("offerer never connected"))?;
    timeout(CONNECT_TIMEOUT, a_connected_rx.recv())
        .await
        .map_err(|_| anyhow::anyhow!("answerer never connected"))?;

    Ok((offerer, answerer, dc, answerer_bytes))
}

/// Waits for the channel to open — `send` refuses before that.
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

async fn push_payload(dc: &Arc<dyn DataChannel>) -> Result<()> {
    let chunk = vec![0xa5u8; CHUNK_SIZE];
    let payload = BytesMut::from(&chunk[..]);
    let mut sent = 0;
    while sent < PAYLOAD_BYTES {
        dc.send(payload.clone()).await?;
        sent += CHUNK_SIZE;
    }
    Ok(())
}

/// Both ends opt in → the association comes up with zero checksums and carries a full
/// mebibyte both ways, byte-conserved: the receiver saw exactly what was sent, and the
/// sender saw every echo. Integrity survives because DTLS already authenticated the
/// channel — that substitution is the whole basis of the feature.
#[test]
fn test_both_ends_enabled_carries_a_mebibyte() {
    block_on(async {
        let (offerer, answerer, dc, answerer_bytes) = connect_pair(true, true)
            .await
            .expect("zero-checksum pair connects");

        wait_open(&dc, "both-enabled").await.unwrap();
        push_payload(&dc).await.unwrap();

        // Counters do the asserting: the answerer received the full payload...
        timeout(CONNECT_TIMEOUT, async {
            loop {
                if answerer_bytes.load(std::sync::atomic::Ordering::Relaxed) >= PAYLOAD_BYTES {
                    return;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("answerer received the full payload");
        // ...and its echoes returned to the offerer's channel: read them through `poll`.
        let mut echoed = 0usize;
        let dc_poll = dc.clone();
        timeout(CONNECT_TIMEOUT, async {
            loop {
                match dc_poll.poll().await {
                    Some(DataChannelEvent::OnMessage(msg)) => {
                        echoed += msg.data.len();
                        if echoed >= PAYLOAD_BYTES {
                            return;
                        }
                    }
                    Some(_) => {}
                    None => return,
                }
            }
        })
        .await
        .expect("echoes came back");
        assert_eq!(
            answerer_bytes.load(std::sync::atomic::Ordering::Relaxed),
            PAYLOAD_BYTES,
            "the receiver saw exactly the payload, no more, no less"
        );
        assert_eq!(echoed, PAYLOAD_BYTES, "the sender saw exactly its echoes");

        offerer.close().await.unwrap();
        answerer.close().await.unwrap();
    });
}

/// One end opts in, the other does not: the negotiation must fall back to checksums and
/// the connection must still come up and carry traffic — the feature is opt-in on both
/// ends precisely so an upgraded peer never strands a stock one.
#[test]
fn test_one_sided_opt_in_still_connects() {
    block_on(async {
        let (offerer, answerer, dc, answerer_bytes) = connect_pair(true, false)
            .await
            .expect("one-sided pair connects (negotiation falls back to checksums)");

        wait_open(&dc, "one-sided").await.unwrap();
        dc.send_text("fallback-carries-traffic").await.unwrap();

        timeout(CONNECT_TIMEOUT, async {
            loop {
                if answerer_bytes.load(std::sync::atomic::Ordering::Relaxed)
                    >= "fallback-carries-traffic".len()
                {
                    return;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("traffic flows despite the fallback");

        offerer.close().await.unwrap();
        answerer.close().await.unwrap();
    });
}
