//! Bench 2 (webrtc-rs): memory per idle connected pair — N pairs (2N connections)
//! connected over loopback, then RSS / per-connection after settling.
//!
//! Run: cargo bench --bench memory -- --nocapture [N]

use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;
use webrtc::data_channel::RTCDataChannelInit;
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

#[allow(clippy::vec_box)]
async fn run(rt: Arc<dyn Runtime>, n: usize) -> Result<()> {
    let mut pcs: Vec<Arc<dyn PeerConnection>> = Vec::with_capacity(n * 2);

    for _ in 0..n {
        let (o_gather_tx, mut o_gather_rx) = channel::<()>(1);
        let offerer = PeerConnectionBuilder::new()
            .with_handler(Arc::new(GatherWaiter { gather_tx: o_gather_tx }))
            .with_runtime(rt.clone())
            .with_udp_addrs(vec!["127.0.0.1:0".to_string()])
            .build()
            .await?;
        offerer
            .create_data_channel("keepalive", Some(RTCDataChannelInit::default()))
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

        let offerer: Arc<dyn PeerConnection> = Arc::new(offerer);
        let answerer: Arc<dyn PeerConnection> = Arc::new(answerer);
        pcs.push(offerer);
        pcs.push(answerer);
    }

    // Let the handshakes finish.
    rt.sleep(Duration::from_secs(8)).await;
    println!(
        "RS_B2 result: connections={} rss_mib={} rss_per_conn_kib={}",
        pcs.len(),
        rss_mib(),
        rss_kib() / pcs.len() as i64,
    );

    for pc in pcs {
        pc.close().await?;
    }
    Ok(())
}

fn rss_kib() -> i64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            return rest.trim().trim_end_matches("kB").trim().parse().unwrap_or(0);
        }
    }
    -1
}

fn rss_mib() -> i64 {
    rss_kib() / 1024
}

fn main() -> Result<()> {
    let n: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);
    let rt = default_runtime().ok_or_else(|| anyhow::anyhow!("no runtime feature enabled"))?;
    let rt2 = rt.clone();
    block_on(&rt, async move { run(rt2, n).await })
}
