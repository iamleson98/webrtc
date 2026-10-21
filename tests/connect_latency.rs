//! Connect-latency regression: the offer → answer → ICE → DTLS → SCTP → open cascade
//! must never wait on a timer.
//!
//! Two bug families used to make every connect cost a protocol timer:
//!
//! * The driver drained writes before events, so each handshake step that produces a
//!   write (ICE selection starts DTLS, whose ClientHello is a write; DTLS completing
//!   starts SCTP, whose INIT is a write) sat queued behind a loop park whose only wake
//!   was the ~200 ms tick — three timer-quantized stalls per connect.
//! * The first DTLS ClientHello was buffered inside the endpoint until a later drain
//!   (one more stall), and flushing it *without* keeping the ICE nomination ahead of
//!   it on the wire let it overtake the USE-CANDIDATE request, so the peer — its pair
//!   not yet selected — dropped it, and the flight came back only on the 1 s DTLS
//!   retransmit.
//!
//! Both are fixed (driver events-before-writes; the DTLS handler pumps the first flight
//! into the same iteration's write drain; the ICE handler front-inserts its control
//! plane so it can never be overtaken). A healthy in-process loopback connect lands in
//! single-digit milliseconds. This test trip-wires exactly the failure shapes above:
//! a median anywhere near the ~200 ms tick quantization, or any single round near the
//! 1 s retransmit cycle, fails it.
//!
//! The thresholds carry ~30-50x headroom over the healthy case so a busy CI box does
//! not flake it: they are calibrated to catch the bug families, not to measure.

use anyhow::Result;
use std::sync::Arc;
use std::time::{Duration, Instant};
use webrtc::data_channel::{DataChannelEvent, RTCDataChannelInit};
use webrtc::peer_connection::{
    PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCIceGatheringState,
};
use webrtc::runtime::{Runtime, Sender, channel};

mod common;
use common::{block_on, runtime, timeout};

/// Median of the seven rounds must stay far below the ~200 ms tick-quantized stall.
const MEDIAN_BUDGET: Duration = Duration::from_millis(150);
/// No single round may approach the 1 s first-flight retransmit cycle.
const MAX_BUDGET: Duration = Duration::from_millis(500);
/// Enough rounds that an every-connect stall moves the median, and an intermittent
/// one is likely to show up in the max at all.
const ROUNDS: usize = 7;

struct GatherWaiter {
    gather_tx: Sender<()>,
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for GatherWaiter {
    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        if state == RTCIceGatheringState::Complete {
            let _ = self.gather_tx.try_send(());
        }
    }
}

/// One full connect cycle: both peers built from scratch, offer/answer exchanged with
/// gathered candidates, data channel driven to OnOpen. Returns the wall time of the
/// whole cycle, matching how a user experiences "connect latency".
async fn connect_once(rt: Arc<dyn Runtime>) -> Result<Duration> {
    let start = Instant::now();

    let (o_gather_tx, mut o_gather_rx) = channel::<()>(1);
    let offerer = PeerConnectionBuilder::new()
        .with_handler(Arc::new(GatherWaiter {
            gather_tx: o_gather_tx,
        }))
        .with_runtime(rt.clone())
        .with_udp_addrs(vec!["127.0.0.1:0".to_string()])
        .build()
        .await?;
    let dc = offerer
        .create_data_channel("latency", Some(RTCDataChannelInit::default()))
        .await?;

    let offer = offerer.create_offer(None).await?;
    offerer.set_local_description(offer.clone()).await?;
    timeout(Duration::from_secs(10), o_gather_rx.recv()).await?;
    let offer_sdp = offerer.local_description().await.expect("offer");

    let (a_gather_tx, mut a_gather_rx) = channel::<()>(1);
    let answerer = PeerConnectionBuilder::new()
        .with_handler(Arc::new(GatherWaiter {
            gather_tx: a_gather_tx,
        }))
        .with_runtime(rt.clone())
        .with_udp_addrs(vec!["127.0.0.1:0".to_string()])
        .build()
        .await?;
    answerer.set_remote_description(offer_sdp).await?;
    let answer = answerer.create_answer(None).await?;
    answerer.set_local_description(answer.clone()).await?;
    timeout(Duration::from_secs(10), a_gather_rx.recv()).await?;
    let answer_sdp = answerer.local_description().await.expect("answer");

    offerer.set_remote_description(answer_sdp).await?;

    loop {
        match dc.poll().await {
            Some(DataChannelEvent::OnOpen) => break,
            Some(_) => continue,
            None => rt.yield_now().await,
        }
    }

    let elapsed = start.elapsed();

    offerer.close().await?;
    answerer.close().await?;
    Ok(elapsed)
}

#[test]
fn connect_cascade_is_not_timer_quantized() {
    let rt = runtime();
    let samples = block_on(async {
        let mut samples = Vec::with_capacity(ROUNDS);
        for _ in 0..ROUNDS {
            samples.push(connect_once(rt.clone()).await?);
        }
        anyhow::Ok(samples)
    })
    .unwrap();

    let mut sorted = samples.clone();
    sorted.sort();
    let median = sorted[sorted.len() / 2];
    let max = sorted[sorted.len() - 1];

    // Report what was measured before any assertion fires: the numbers are the whole
    // point of this test, and a failure should read as a latency report first.
    eprintln!(
        "connect latency over {ROUNDS} rounds: median {median:?}, min {:?}, max {max:?}",
        sorted[0],
    );

    assert!(
        median <= MEDIAN_BUDGET,
        "connect median {median:?} exceeds {MEDIAN_BUDGET:?}: the handshake cascade is \
         waiting on a timer again (tick-quantized stall, see module docs)"
    );
    assert!(
        max <= MAX_BUDGET,
        "connect max {max:?} exceeds {MAX_BUDGET:?}: some round lost its first flight \
         and fell back to a retransmit cycle (see module docs)"
    );
}
