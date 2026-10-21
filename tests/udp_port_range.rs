//! `with_udp_port_range` restricts which ephemeral port a `:0` UDP bind draws — pion's
//! `SetEphemeralUDPPortRange`.
//!
//! Without a range, port `0` defers to the operating system's machine-wide ephemeral window,
//! which may collide with other services or fall outside a firewall's allowances. With a range,
//! every bound host-candidate socket must land inside `[min, max]` — asserted here against the
//! offer's own host candidates — while a concrete port outside the range binds verbatim,
//! because the range only ever speaks for "pick one for me".

use anyhow::Result;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use webrtc::data_channel::RTCDataChannelInit;
use webrtc::peer_connection::{
    PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCIceGatheringState,
};
use webrtc::runtime::{Sender, channel};

mod common;
use common::{block_on, runtime, timeout};

struct Handler {
    gather_tx: Sender<()>,
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for Handler {
    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        if state == RTCIceGatheringState::Complete {
            let _ = self.gather_tx.try_send(());
        }
    }
}

fn udp_host_candidate_ports(sdp: &str) -> BTreeSet<u16> {
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

async fn gather_ports(udp_addrs: Vec<String>, range: Option<(u16, u16)>) -> Result<BTreeSet<u16>> {
    let (gather_tx, mut gather_rx) = channel::<()>(1);

    let mut builder = PeerConnectionBuilder::new()
        .with_handler(Arc::new(Handler { gather_tx }))
        .with_runtime(runtime())
        .with_udp_addrs(udp_addrs);
    if let Some((min, max)) = range {
        builder = builder.with_udp_port_range(min, max);
    }
    let pc = builder.build().await?;

    let _dc = pc
        .create_data_channel("range", Some(RTCDataChannelInit::default()))
        .await?;
    let offer = pc.create_offer(None).await?;
    pc.set_local_description(offer).await?;
    timeout(Duration::from_secs(10), gather_rx.recv())
        .await
        .map_err(|_| anyhow::anyhow!("gathering did not complete"))?;

    let sdp = pc.local_description().await.expect("offer").sdp;
    let ports = udp_host_candidate_ports(&sdp);
    pc.close().await?;
    Ok(ports)
}

/// Every ephemeral bind must land inside the configured range.
async fn ephemeral_ports_honor_the_range() -> Result<()> {
    let (min, max) = (20010u16, 20040u16);
    let ports = gather_ports(vec!["127.0.0.1:0".to_string()], Some((min, max))).await?;
    assert!(
        !ports.is_empty(),
        "a loopback ephemeral bind must succeed at all"
    );
    for port in &ports {
        assert!(
            *port >= min && *port <= max,
            "port {port} outside the configured range {min}..={max}"
        );
    }
    Ok(())
}

/// A concrete port is bound verbatim; the range never rewrites an explicit choice.
async fn explicit_ports_ignore_the_range() -> Result<()> {
    let explicit = 20999u16;
    let ports = gather_ports(
        vec![format!("127.0.0.1:{explicit}")],
        Some((20010, 20040)),
    )
    .await?;
    assert_eq!(
        ports.iter().copied().collect::<Vec<_>>(),
        vec![explicit],
        "a concrete port must bind verbatim, range notwithstanding"
    );
    Ok(())
}

#[test]
fn test_ephemeral_ports_honor_the_range() {
    block_on(async {
        ephemeral_ports_honor_the_range()
            .await
            .expect("ephemeral ports honor the range");
        explicit_ports_ignore_the_range()
            .await
            .expect("explicit ports ignore the range");
    });
}
