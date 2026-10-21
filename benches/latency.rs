//! Bench 3 (webrtc-rs): end-to-end connect latency — first API call to an open
//! data channel, R rounds, median/mean/min/max.
//!
//! Run: cargo bench --bench latency -- --nocapture [rounds]

use anyhow::Result;
use std::sync::Arc;
use std::time::{Duration, Instant};
use webrtc::data_channel::{DataChannelEvent, RTCDataChannelInit};
use webrtc::peer_connection::{
    PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler,
    RTCIceGatheringState,
};
use webrtc::runtime::{Runtime, Sender, channel, default_runtime};

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

fn block_on<T>(rt: &Arc<dyn Runtime>, future: impl Future<Output = T>) -> T {
    let mut out = None;
    let slot = &mut out;
    rt.block_on(Box::pin(async move {
        *slot = Some(future.await);
    }));
    out.expect("block_on future did not run")
}

async fn connect_once(rt: Arc<dyn Runtime>) -> Result<Duration> {
    let start = Instant::now();

    let (o_gather_tx, mut o_gather_rx) = channel::<()>(1);
    let offerer = PeerConnectionBuilder::new()
        .with_handler(Arc::new(GatherWaiter { gather_tx: o_gather_tx }))
        .with_runtime(rt.clone())
        .with_udp_addrs(vec!["127.0.0.1:0".to_string()])
        .build()
        .await?;
    let dc = offerer
        .create_data_channel("latency", Some(RTCDataChannelInit::default()))
        .await?;

    let offer = offerer.create_offer(None).await?;
    offerer.set_local_description(offer.clone()).await?;
    webrtc::runtime::timeout(&*rt.clone(), Duration::from_secs(10), o_gather_rx.recv())
        .await
        .map_err(|_| anyhow::anyhow!("gather timeout"))?;
    let offer_sdp = offerer.local_description().await.expect("offer");

    let (a_gather_tx, mut a_gather_rx) = channel::<()>(1);
    let answerer = PeerConnectionBuilder::new()
        .with_handler(Arc::new(GatherWaiter { gather_tx: a_gather_tx }))
        .with_runtime(rt.clone())
        .with_udp_addrs(vec!["127.0.0.1:0".to_string()])
        .build()
        .await?;
    answerer.set_remote_description(offer_sdp).await?;
    let answer = answerer.create_answer(None).await?;
    answerer.set_local_description(answer.clone()).await?;
    webrtc::runtime::timeout(&*rt.clone(), Duration::from_secs(10), a_gather_rx.recv())
        .await
        .map_err(|_| anyhow::anyhow!("gather timeout"))?;
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

fn main() -> Result<()> {
    run()
}

fn run() -> Result<()> {
    let rounds: usize = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(20);
    let rt = default_runtime().ok_or_else(|| anyhow::anyhow!("no runtime feature enabled"))?;
    let rt2 = rt.clone();
    block_on(&rt, async move {
        let mut samples = Vec::with_capacity(rounds);
        for _ in 0..rounds {
            samples.push(connect_once(rt2.clone()).await?);
        }
        samples.sort();
        let median = samples[samples.len() / 2];
        let mean = samples.iter().sum::<Duration>() / samples.len() as u32;
        println!(
            "RS_B3 result: rounds={} median_ms={} mean_ms={:.1} min_ms={} max_ms={}",
            rounds,
            median.as_millis(),
            mean.as_secs_f64() * 1000.0,
            samples[0].as_millis(),
            samples[samples.len() - 1].as_millis(),
        );
        Ok::<(), anyhow::Error>(())
    })
}
