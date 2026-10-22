//! Bench 4 (webrtc-rs): media forwarding — a sender writes RTP at a fixed rate to a receiver over
//! loopback, through SRTP and the default interceptors, and the receiver counts what arrives.
//!
//! The metric that matters for a media server is CPU per packet, so the rate is fixed (paced
//! 1 ms ticks) and the process's CPU time and peak memory are what `compare.py` reports. Run
//! standalone: cargo bench --bench media -- --nocapture

use anyhow::Result;
use bytes::Bytes;
use rtc::interceptor::Registry;
use rtc::media_stream::MediaStreamTrack;
use rtc::peer_connection::configuration::interceptor_registry::register_default_interceptors;
use rtc::peer_connection::configuration::media_engine::{MIME_TYPE_VP8, MediaEngine};
use rtc::rtp;
use rtc::rtp_transceiver::rtp_sender::{
    RTCRtpCodec, RTCRtpCodecParameters, RTCRtpCodingParameters, RTCRtpEncodingParameters,
    RtpCodecKind,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use webrtc::media_stream::track_local::TrackLocal;
use webrtc::media_stream::track_local::static_rtp::TrackLocalStaticRTP;
use webrtc::media_stream::track_remote::{TrackRemote, TrackRemoteEvent};
use webrtc::peer_connection::{
    PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCIceGatheringState,
    RTCPeerConnectionState,
};
use webrtc::runtime::{Runtime, Sender, channel, default_runtime};

const PACKETS: usize = 60_000;
const PER_TICK: usize = 12; // per millisecond: 12,000 packets/s
const PAYLOAD: usize = 1188; // 1200 bytes with the 12-byte RTP header
const SSRC: u32 = 0x1234_5678;

fn block_on<T>(rt: &Arc<dyn Runtime>, future: impl Future<Output = T>) -> T {
    let mut out = None;
    let slot = &mut out;
    rt.block_on(Box::pin(async move {
        *slot = Some(future.await);
    }));
    out.expect("block_on future did not run")
}

struct Sender2 {
    gather_tx: Sender<()>,
    connected_tx: Sender<()>,
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for Sender2 {
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

struct Receiver {
    gather_tx: Sender<()>,
    runtime: Arc<dyn Runtime>,
    received: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for Receiver {
    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        if state == RTCIceGatheringState::Complete {
            let _ = self.gather_tx.try_send(());
        }
    }

    async fn on_track(&self, track: Arc<dyn TrackRemote>) {
        let received = self.received.clone();
        self.runtime.spawn(Box::pin(async move {
            while let Some(event) = track.poll().await {
                if let TrackRemoteEvent::OnRtpPacket(_) = event {
                    received.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }
}

fn media_engine() -> Result<MediaEngine> {
    let mut media_engine = MediaEngine::default();
    media_engine.register_codec(
        RTCRtpCodecParameters {
            rtp_codec: vp8(),
            payload_type: 96,
            ..Default::default()
        },
        RtpCodecKind::Video,
    )?;
    Ok(media_engine)
}

fn vp8() -> RTCRtpCodec {
    RTCRtpCodec {
        mime_type: MIME_TYPE_VP8.to_owned(),
        clock_rate: 90000,
        channels: 0,
        sdp_fmtp_line: "".to_owned(),
        rtcp_feedback: vec![],
    }
}

fn main() -> Result<()> {
    let rt = default_runtime().ok_or_else(|| anyhow::anyhow!("no runtime feature enabled"))?;
    let rt2 = rt.clone();
    block_on(&rt, async move { run(rt2).await })
}

async fn run(rt: Arc<dyn Runtime>) -> Result<()> {
    let received = Arc::new(AtomicUsize::new(0));

    let (offerer_gather_tx, mut offerer_gather_rx) = channel::<()>(1);
    let (connected_tx, mut connected_rx) = channel::<()>(1);
    let mut offerer_media = media_engine()?;
    let offerer_registry = register_default_interceptors(Registry::new(), &mut offerer_media)?;
    let offerer = PeerConnectionBuilder::new()
        .with_media_engine(offerer_media)
        .with_interceptor_registry(offerer_registry)
        .with_handler(Arc::new(Sender2 {
            gather_tx: offerer_gather_tx,
            connected_tx,
        }))
        .with_runtime(rt.clone())
        .with_udp_addrs(vec!["127.0.0.1:0".to_string()])
        .build()
        .await?;

    let track: Arc<TrackLocalStaticRTP> =
        Arc::new(TrackLocalStaticRTP::new(MediaStreamTrack::new(
            "bench-stream".to_owned(),
            "bench-track".to_owned(),
            "bench-label".to_owned(),
            RtpCodecKind::Video,
            vec![RTCRtpEncodingParameters {
                rtp_coding_parameters: RTCRtpCodingParameters {
                    ssrc: Some(SSRC),
                    ..Default::default()
                },
                codec: vp8(),
                ..Default::default()
            }],
        )));
    offerer
        .add_track(Arc::clone(&track) as Arc<dyn TrackLocal>)
        .await?;

    let offer = offerer.create_offer(None).await?;
    offerer.set_local_description(offer).await?;
    webrtc::runtime::timeout(&*rt, Duration::from_secs(10), offerer_gather_rx.recv())
        .await
        .map_err(|_| anyhow::anyhow!("offerer gathering timed out"))?;
    let offer_sdp = offerer.local_description().await.expect("offer");

    let (answerer_gather_tx, mut answerer_gather_rx) = channel::<()>(1);
    let mut answerer_media = media_engine()?;
    let answerer_registry = register_default_interceptors(Registry::new(), &mut answerer_media)?;
    let answerer = PeerConnectionBuilder::new()
        .with_media_engine(answerer_media)
        .with_interceptor_registry(answerer_registry)
        .with_handler(Arc::new(Receiver {
            gather_tx: answerer_gather_tx,
            runtime: rt.clone(),
            received: received.clone(),
        }))
        .with_runtime(rt.clone())
        .with_udp_addrs(vec!["127.0.0.1:0".to_string()])
        .build()
        .await?;
    answerer.set_remote_description(offer_sdp).await?;
    let answer = answerer.create_answer(None).await?;
    answerer.set_local_description(answer).await?;
    webrtc::runtime::timeout(&*rt, Duration::from_secs(10), answerer_gather_rx.recv())
        .await
        .map_err(|_| anyhow::anyhow!("answerer gathering timed out"))?;
    let answer_sdp = answerer.local_description().await.expect("answer");
    offerer.set_remote_description(answer_sdp).await?;

    webrtc::runtime::timeout(&*rt, Duration::from_secs(10), connected_rx.recv())
        .await
        .map_err(|_| anyhow::anyhow!("connect timed out"))?;

    let payload = Bytes::from(vec![0x55u8; PAYLOAD]);
    let start = Instant::now();
    let mut sent = 0usize;
    let mut tick = 0u64;
    while sent < PACKETS {
        tick += 1;
        let due = start + Duration::from_millis(tick);
        let now = Instant::now();
        if due > now {
            rt.sleep(due - now).await;
        }
        for _ in 0..PER_TICK {
            let packet = rtp::packet::Packet {
                header: rtp::header::Header {
                    version: 2,
                    payload_type: 96,
                    sequence_number: sent as u16,
                    timestamp: (sent as u32).wrapping_mul(3000),
                    ssrc: SSRC,
                    ..Default::default()
                },
                payload: payload.clone(),
            };
            track.write_rtp(packet).await?;
            sent += 1;
        }
    }

    // Let the tail drain: stop once everything arrived or nothing new has for a while.
    let mut last = received.load(Ordering::Relaxed);
    let mut quiet = 0;
    while quiet < 6 && last < sent {
        rt.sleep(Duration::from_millis(50)).await;
        let now = received.load(Ordering::Relaxed);
        quiet = if now == last { quiet + 1 } else { 0 };
        last = now;
    }
    let elapsed = start.elapsed();
    println!(
        "RS_B4 result: sent={} received={} elapsed_ms={}",
        sent,
        received.load(Ordering::Relaxed),
        elapsed.as_millis()
    );
    drop(answerer);
    drop(offerer);
    Ok(())
}
