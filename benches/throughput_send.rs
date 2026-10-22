//! Bench 1b (webrtc-rs): the same transfer as `throughput`, written the way most applications
//! write it — `send().await` against a configured send-buffer limit, so the call itself waits
//! for room — rather than polling `outstanding_bytes()` in a loop.
//!
//! Run: cargo bench --bench throughput_send -- --nocapture

use anyhow::Result;
use bytes::BytesMut;
use std::sync::Arc;
use std::time::Instant;
use webrtc::data_channel::{DataChannel, DataChannelEvent, RTCDataChannelInit};
use webrtc::peer_connection::{PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler};
use webrtc::runtime::{Runtime, Sender, channel, default_runtime};

const TOTAL_BYTES: usize = 256 << 20; // 256 MiB
const CHUNK_SIZE: usize = 16 << 10; // 16 KiB messages
const BUFFER_HIGH: usize = 8 << 20; // 8 MiB buffered-amount ceiling, same as pion's bench

/// Drives `future` to completion on the shared runtime; `block_on` is `()`-typed.
fn block_on<T>(rt: &Arc<dyn Runtime>, future: impl Future<Output = T>) -> T {
    let mut out = None;
    let slot = &mut out;
    rt.block_on(Box::pin(async move {
        *slot = Some(future.await);
    }));
    out.expect("block_on future did not run")
}

struct GatherWaiter {
    gather_tx: Sender<()>,
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for GatherWaiter {
    async fn on_ice_gathering_state_change(
        &self,
        state: webrtc::peer_connection::RTCIceGatheringState,
    ) {
        if state == webrtc::peer_connection::RTCIceGatheringState::Complete {
            let _ = self.gather_tx.try_send(());
        }
    }
}

/// Receiver side: counts bytes until the target, then signals completion.
struct Receiver {
    gather_tx: Sender<()>,
    runtime: Arc<dyn Runtime>,
    done_tx: Sender<()>,
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for Receiver {
    async fn on_ice_gathering_state_change(
        &self,
        state: webrtc::peer_connection::RTCIceGatheringState,
    ) {
        if state == webrtc::peer_connection::RTCIceGatheringState::Complete {
            let _ = self.gather_tx.try_send(());
        }
    }

    async fn on_data_channel(&self, dc: Arc<dyn DataChannel>) {
        let done_tx = self.done_tx.clone();
        self.runtime.spawn(Box::pin(async move {
            let mut received = 0usize;
            while let Some(event) = dc.poll().await {
                if let DataChannelEvent::OnMessage(msg) = event {
                    received += msg.data.len();
                    if received >= TOTAL_BYTES {
                        let _ = done_tx.try_send(());
                        break;
                    }
                }
            }
        }));
    }
}

fn main() -> Result<()> {
    let rt = default_runtime().ok_or_else(|| anyhow::anyhow!("no runtime feature enabled"))?;
    let rt2 = rt.clone();
    block_on(&rt, async move { run(rt2).await })
}

async fn run(rt: Arc<dyn Runtime>) -> Result<()> {
    let (offerer_gather_tx, mut offerer_gather_rx) = channel::<()>(1);
    let offerer = PeerConnectionBuilder::new()
        .with_data_channel_send_buffer_limit(BUFFER_HIGH)
        .with_handler(Arc::new(GatherWaiter {
            gather_tx: offerer_gather_tx,
        }))
        .with_runtime(rt.clone())
        .with_udp_addrs(vec!["127.0.0.1:0".to_string()])
        .build()
        .await?;

    let dc = offerer
        .create_data_channel("bench", Some(RTCDataChannelInit::default()))
        .await?;

    let offer = offerer.create_offer(None).await?;
    offerer.set_local_description(offer.clone()).await?;
    webrtc::runtime::timeout(
        &*rt.clone(),
        std::time::Duration::from_secs(10),
        offerer_gather_rx.recv(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("offerer gathering timed out"))?;
    let offer_sdp = offerer
        .local_description()
        .await
        .expect("offerer local description");

    let (answerer_gather_tx, mut answerer_gather_rx) = channel::<()>(1);
    let (done_tx, mut done_rx) = channel::<()>(1);
    let answerer = PeerConnectionBuilder::new()
        .with_handler(Arc::new(Receiver {
            gather_tx: answerer_gather_tx,
            runtime: rt.clone(),
            done_tx,
        }))
        .with_runtime(rt.clone())
        .with_udp_addrs(vec!["127.0.0.1:0".to_string()])
        .build()
        .await?;

    answerer.set_remote_description(offer_sdp).await?;
    let answer = answerer.create_answer(None).await?;
    answerer.set_local_description(answer.clone()).await?;
    webrtc::runtime::timeout(
        &*rt.clone(),
        std::time::Duration::from_secs(10),
        answerer_gather_rx.recv(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("answerer gathering timed out"))?;
    let answer_sdp = answerer
        .local_description()
        .await
        .expect("answerer local description");

    offerer.set_remote_description(answer_sdp).await?;

    // Wait for the sender's channel to open.
    loop {
        match dc.poll().await {
            Some(DataChannelEvent::OnOpen) => break,
            Some(_) => continue,
            None => rt.yield_now().await,
        }
    }

    let payload = BytesMut::from(vec![0u8; CHUNK_SIZE].as_slice());
    let start = Instant::now();

    let mut sent = 0usize;
    while sent < TOTAL_BYTES {
        dc.send(payload.clone()).await?;
        sent += CHUNK_SIZE;
    }

    webrtc::runtime::timeout(
        &*rt.clone(),
        std::time::Duration::from_secs(300),
        done_rx.recv(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("receiver did not see all bytes"))?;
    let elapsed = start.elapsed();

    println!(
        "RS_B1S result: bytes={} elapsed_ms={} throughput_mibs={:.2} rss_mib={}",
        TOTAL_BYTES,
        elapsed.as_millis(),
        TOTAL_BYTES as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64(),
        rss_mib(),
    );
    offerer.close().await?;
    answerer.close().await?;
    Ok(())
}

fn rss_mib() -> i64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: i64 = rest
                .trim()
                .trim_end_matches("kB")
                .trim()
                .parse()
                .unwrap_or(0);
            return kb / 1024;
        }
    }
    -1
}
