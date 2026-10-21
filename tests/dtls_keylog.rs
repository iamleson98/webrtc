//! DTLS key logging through a full peer connection — pion's `SetKeyLogWriter` equivalent.
//!
//! The `SettingEngine`'s `with_dtls_key_logger` is the debugging hook that makes a capture
//! decryptable: it receives the session's `CLIENT_RANDOM` line in the NSS key log format
//! once per handshake. These tests drive it end to end, through the async crate's public
//! API, over real sockets:
//!
//! * a collecting logger on **both** ends receives identical lines — the client random
//!   names the session, so whatever end logs it, the line is the same, exactly what
//!   `SSLKEYLOGFILE` tooling needs to decrypt traffic from either side of the capture;
//! * the file-backed [`FileKeyLogger`] lands the same line on disk, in append mode, so
//!   the file can be handed straight to Wireshark.
//!
//! The two `SettingEngine` re-exports (`SettingEngineBuilder`, `KeyLogger`) are part of
//! the point: an application configures this without taking a direct `rtc` dependency.

use anyhow::Result;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use webrtc::data_channel::{DataChannel, DataChannelEvent, RTCDataChannelState};
use webrtc::peer_connection::{
    KeyLogger, PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler,
    RTCIceGatheringState, RTCSessionDescription, SettingEngineBuilder,
};
use webrtc::runtime::{Runtime, Sender, channel};

mod common;
use common::{block_on, runtime, sleep, timeout};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const GATHER_TIMEOUT: Duration = Duration::from_secs(10);

/// A [`KeyLogger`] that collects lines for inspection.
#[derive(Default)]
struct CollectingLogger {
    lines: Mutex<Vec<String>>,
}

impl KeyLogger for CollectingLogger {
    fn write_line(&self, line: &str) {
        self.lines.lock().unwrap().push(line.to_owned());
    }
}

impl CollectingLogger {
    /// The collected lines, for assertions.
    fn snapshot(&self) -> Vec<String> {
        self.lines.lock().unwrap().clone()
    }
}

struct Handler {
    runtime: Arc<dyn Runtime>,
    gather_tx: Sender<()>,
    connected_tx: Sender<()>,
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

    async fn on_data_channel(&self, dc: Arc<dyn DataChannel>) {
        self.runtime.spawn(Box::pin(async move {
            while let Some(event) = dc.poll().await {
                match event {
                    DataChannelEvent::OnMessage(msg) => {
                        let _ = dc.send(msg.data).await;
                    }
                    DataChannelEvent::OnClose => break,
                    _ => {}
                }
            }
        }));
    }
}

/// One offerer→answerer pair; `offerer_logger`/`answerer_logger` are injected through the
/// public `SettingEngine`, exactly as an application would.
async fn connect_pair(
    offerer_logger: Option<Arc<dyn KeyLogger>>,
    answerer_logger: Option<Arc<dyn KeyLogger>>,
) -> Result<(
    Arc<dyn PeerConnection>,
    Arc<dyn PeerConnection>,
    Arc<dyn DataChannel>,
)> {
    let runtime = runtime();

    let (o_gather_tx, mut o_gather_rx) = channel::<()>(1);
    let (o_connected_tx, mut o_connected_rx) = channel::<()>(1);
    let mut offerer_builder = PeerConnectionBuilder::new()
        .with_handler(Arc::new(Handler {
            runtime: runtime.clone(),
            gather_tx: o_gather_tx,
            connected_tx: o_connected_tx,
        }))
        .with_runtime(runtime.clone())
        .with_udp_addrs(vec!["127.0.0.1:0".to_string()]);
    if let Some(logger) = offerer_logger {
        offerer_builder = offerer_builder.with_setting_engine(
            SettingEngineBuilder::new().with_dtls_key_logger(Some(logger)).build(),
        );
    }
    let offerer: Arc<dyn PeerConnection> = Arc::new(offerer_builder.build().await?);

    let (a_gather_tx, mut a_gather_rx) = channel::<()>(1);
    let (a_connected_tx, mut a_connected_rx) = channel::<()>(1);
    let mut answerer_builder = PeerConnectionBuilder::new()
        .with_handler(Arc::new(Handler {
            runtime: runtime.clone(),
            gather_tx: a_gather_tx,
            connected_tx: a_connected_tx,
        }))
        .with_runtime(runtime.clone())
        .with_udp_addrs(vec!["127.0.0.1:0".to_string()]);
    if let Some(logger) = answerer_logger {
        answerer_builder = answerer_builder.with_setting_engine(
            SettingEngineBuilder::new().with_dtls_key_logger(Some(logger)).build(),
        );
    }
    let answerer: Arc<dyn PeerConnection> = Arc::new(answerer_builder.build().await?);

    let dc = offerer.create_data_channel("keylog", None).await?;

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

    Ok((offerer, answerer, dc))
}

/// Every `CLIENT_RANDOM` line is one space-separated label and two hex fields (32-byte
/// client random, 48-byte master secret), with the trailing newline the format requires.
fn assert_valid_lines(collector: &CollectingLogger) {
    let lines = collector.snapshot();
    assert!(
        !lines.is_empty(),
        "a completed handshake must produce at least one CLIENT_RANDOM line"
    );
    for line in lines.iter() {
        let fields: Vec<&str> = line.trim_end().split(' ').collect();
        assert_eq!(fields.len(), 3, "line must be label + client random + secret: {line:?}");
        assert_eq!(fields[0], "CLIENT_RANDOM", "the NSS format label: {line:?}");
        assert_eq!(fields[1].len(), 64, "client random is 32 bytes of hex: {line:?}");
        assert_eq!(fields[2].len(), 96, "master secret is 48 bytes of hex: {line:?}");
        assert!(line.ends_with('\n'), "the newline is part of the format: {line:?}");
        assert!(
            fields[1].chars().chain(fields[2].chars()).all(|c| c.is_ascii_hexdigit()),
            "both fields are hex: {line:?}"
        );
    }
}

/// Both ends log the same line — the client random names the session, not the logging
/// end — so one capture decrypts with either side's key log.
#[test]
fn test_both_ends_log_identical_client_random_lines() {
    block_on(async {
        let offerer_collector = Arc::new(CollectingLogger::default());
        let answerer_collector = Arc::new(CollectingLogger::default());
        let offerer_logger: Arc<dyn KeyLogger> = offerer_collector.clone();
        let answerer_logger: Arc<dyn KeyLogger> = answerer_collector.clone();

        let (offerer, answerer, dc) = connect_pair(
            Some(offerer_logger),
            Some(answerer_logger),
        )
        .await
        .expect("pair with key loggers connects");

        // A message round trip proves the handshake the lines came from was real.
        timeout(CONNECT_TIMEOUT, async {
            loop {
                if matches!(dc.ready_state().await, Ok(RTCDataChannelState::Open)) {
                    return;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("data channel opened");
        dc.send_text("through-the-logged-session").await.unwrap();

        assert_valid_lines(&offerer_collector);
        assert_valid_lines(&answerer_collector);
        assert_eq!(
            offerer_collector.snapshot(),
            answerer_collector.snapshot(),
            "both ends must produce identical lines: the client random is the session's, not the end's"
        );

        offerer.close().await.unwrap();
        answerer.close().await.unwrap();
    });
}

/// The file-backed logger — the `SSLKEYLOGFILE` workflow: point Wireshark at the file.
#[test]
fn test_file_key_logger_writes_the_nss_format_to_disk() {
    block_on(async {
        let dir = std::env::temp_dir().join(format!("webrtc-keylog-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("session-keys.log");
        std::fs::remove_file(&path).ok();

        let logger: Arc<dyn KeyLogger> = Arc::new(
            rtc::dtls::config::FileKeyLogger::create(&path).expect("key log file opens"),
        );

        let (offerer, answerer, dc) = connect_pair(Some(logger), None)
            .await
            .expect("pair with a file key logger connects");

        timeout(CONNECT_TIMEOUT, async {
            loop {
                if matches!(dc.ready_state().await, Ok(RTCDataChannelState::Open)) {
                    return;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("data channel opened");
        dc.send_text("logged-to-disk").await.unwrap();

        let contents = std::fs::read_to_string(&path).expect("the key log file is readable");
        let mut valid = 0;
        for line in contents.lines().filter(|l| !l.is_empty()) {
            let fields: Vec<&str> = line.split(' ').collect();
            assert_eq!(fields.len(), 3, "on-disk line is the NSS format: {line:?}");
            assert_eq!(fields[0], "CLIENT_RANDOM", "the label: {line:?}");
            assert_eq!(fields[1].len(), 64, "client random hex length: {line:?}");
            assert_eq!(fields[2].len(), 96, "master secret hex length: {line:?}");
            valid += 1;
        }
        assert!(valid >= 1, "the file must hold at least one valid line");

        offerer.close().await.unwrap();
        answerer.close().await.unwrap();
        std::fs::remove_dir_all(&dir).ok();
    });
}
