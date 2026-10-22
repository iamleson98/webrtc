//! Bench 3 (webrtc-rs): end-to-end connect latency — first API call to an open
//! data channel, R rounds, median/mean/min/max.
//!
//! Run: cargo bench --bench latency -- --nocapture [rounds]

use anyhow::Result;
use std::sync::Arc;
use std::time::{Duration, Instant};
use webrtc::data_channel::{DataChannelEvent, RTCDataChannelInit};
use webrtc::peer_connection::{
    PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCIceGatheringState,
    SettingEngineBuilder,
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

/// Phase trace for the connect-latency bench: set `LATENCY_TRACE=1` to print per-phase
/// timestamps (`t+ms  phase`) of one round — where the milliseconds actually go.
fn trace(start: &Instant, what: &str) {
    if std::env::var_os("LATENCY_TRACE").is_some() {
        eprintln!("t+{:>5}ms  {what}", start.elapsed().as_millis());
    }
}

async fn connect_once(rt: Arc<dyn Runtime>) -> Result<Duration> {
    let start = Instant::now();

    // `CHECK_MS=<n>` overrides the ICE check interval for this round — the A/B knob used
    // to find how much of the connect latency is check-interval quantization.
    let check_ms: Option<u64> = std::env::var("CHECK_MS").ok().and_then(|v| v.parse().ok());
    let setting_engine = check_ms.map(|ms| {
        SettingEngineBuilder::new()
            .with_ice_connection_attempts(Some(Duration::from_millis(ms)), None)
            .build()
    });

    let (o_gather_tx, mut o_gather_rx) = channel::<()>(1);
    let mut offerer_builder = PeerConnectionBuilder::new()
        .with_handler(Arc::new(GatherWaiter {
            gather_tx: o_gather_tx,
        }))
        .with_runtime(rt.clone())
        .with_udp_addrs(vec!["127.0.0.1:0".to_string()]);
    if let Some(se) = setting_engine.clone() {
        offerer_builder = offerer_builder.with_setting_engine(se);
    }
    let offerer = offerer_builder.build().await?;
    trace(&start, "offerer built");
    let dc = offerer
        .create_data_channel("latency", Some(RTCDataChannelInit::default()))
        .await?;
    trace(&start, "data channel created");

    let offer = offerer.create_offer(None).await?;
    offerer.set_local_description(offer.clone()).await?;
    trace(&start, "offer: created + local set");
    webrtc::runtime::timeout(&*rt.clone(), Duration::from_secs(10), o_gather_rx.recv())
        .await
        .map_err(|_| anyhow::anyhow!("gather timeout"))?;
    trace(&start, "offer: gathering complete");
    let offer_sdp = offerer.local_description().await.expect("offer");

    let (a_gather_tx, mut a_gather_rx) = channel::<()>(1);
    let mut answerer_builder = PeerConnectionBuilder::new()
        .with_handler(Arc::new(GatherWaiter {
            gather_tx: a_gather_tx,
        }))
        .with_runtime(rt.clone())
        .with_udp_addrs(vec!["127.0.0.1:0".to_string()]);
    if let Some(se) = setting_engine {
        answerer_builder = answerer_builder.with_setting_engine(se);
    }
    let answerer = answerer_builder.build().await?;
    answerer.set_remote_description(offer_sdp).await?;
    trace(&start, "answerer built + offer applied");
    let answer = answerer.create_answer(None).await?;
    answerer.set_local_description(answer.clone()).await?;
    webrtc::runtime::timeout(&*rt.clone(), Duration::from_secs(10), a_gather_rx.recv())
        .await
        .map_err(|_| anyhow::anyhow!("gather timeout"))?;
    trace(&start, "answer: created + local set + gathered");
    let answer_sdp = answerer.local_description().await.expect("answer");
    offerer.set_remote_description(answer_sdp).await?;
    trace(&start, "answer applied on offerer");

    loop {
        match dc.poll().await {
            Some(DataChannelEvent::OnOpen) => break,
            Some(_) => continue,
            None => rt.yield_now().await,
        }
    }
    trace(&start, "data channel open");
    let elapsed = start.elapsed();

    offerer.close().await?;
    answerer.close().await?;
    Ok(elapsed)
}

fn main() -> Result<()> {
    // With `LATENCY_TRACE=1`, honor `RUST_LOG` so one traced round can show the whole
    // connect sequence with millisecond timestamps (no logger without this: `RUST_LOG`
    // alone does nothing in a bench binary).
    if std::env::var_os("LATENCY_TRACE").is_some() {
        env_logger::builder()
            .format_timestamp_millis()
            .parse_env("RUST_LOG")
            .try_init()
            .ok();
    }
    run()
}

fn run() -> Result<()> {
    let rounds: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);
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
