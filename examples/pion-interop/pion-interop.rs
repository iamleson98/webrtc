//! pion-interop: cross-implementation data-channel echo, the webrtc-rs half of the
//! harness that proves pion (Go) and this crate interoperate over real sockets.
//!
//! Two modes, one binary:
//!
//! * `--mode server` — an HTTP signaling endpoint at `--http` plus an answering
//!   PeerConnection per request. Every data-channel message a peer sends is echoed back
//!   verbatim. Non-trickle ICE: one HTTP round trip carries both descriptions.
//! * `--mode client` — builds an offer, POSTs it to `--url`, applies the answer, then
//!   sends `--messages` messages and asserts each is echoed back exactly. Prints one
//!   `RUST_CLIENT result:` line and exits 0 only on success.
//!
//! The Go half lives in `interop/pion-interop`; `interop/run.sh` drives both directions:
//!
//! ```text
//!   pion client  -> webrtc-rs server   (pion offers, this crate answers)
//!   rust client  -> pion server        (this crate offers, pion answers)
//! ```
//!
//! Both directions traverse full ICE (STUN checks), DTLS and SCTP on loopback — the same
//! wire a browser would speak, cross-checked between two independent implementations.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Client, Method, Request, Response, Server, StatusCode};
use rtc::interceptor::Registry;
use rtc::peer_connection::configuration::RTCConfigurationBuilder;
use rtc::peer_connection::configuration::interceptor_registry::register_default_interceptors;
use rtc::peer_connection::configuration::media_engine::MediaEngine;
use rtc::peer_connection::state::RTCIceGatheringState;
use webrtc::data_channel::{DataChannel, DataChannelEvent, RTCDataChannelState};
use webrtc::peer_connection::{
    PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCSessionDescription,
};
use webrtc::runtime::{Runtime, Sender, channel};

#[path = "../common/mod.rs"]
mod common;
use common::{block_on, runtime, sleep, timeout};

const DEFAULT_HTTP_PORT: u16 = 9090;
const DEFAULT_MESSAGES: usize = 100;
const MSG_PREFIX: &str = "interop";

#[derive(Parser)]
#[command(
    name = "pion-interop",
    about = "Cross-implementation data-channel echo (pion <-> webrtc-rs)"
)]
struct Cli {
    #[arg(long)]
    mode: String, // "server" | "client"
    #[arg(long, default_value_t = DEFAULT_HTTP_PORT)]
    http_port: u16,
    /// Client: base URL of the signaling endpoint, e.g. http://127.0.0.1:9090
    #[arg(long, default_value = "http://127.0.0.1:9090")]
    url: String,
    /// Client: how many messages to send and expect echoes for.
    #[arg(long, default_value_t = DEFAULT_MESSAGES)]
    messages: usize,
    #[arg(short, long)]
    debug: bool,
}

/// Signaling handler for server mode: answer one offer, echo its data channel.
struct EchoAnswerHandler {
    runtime: Arc<dyn Runtime>,
    gather_tx: Sender<()>,
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for EchoAnswerHandler {
    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        if state == RTCIceGatheringState::Complete {
            let _ = self.gather_tx.try_send(());
        }
    }

    async fn on_data_channel(&self, dc: Arc<dyn DataChannel>) {
        self.runtime.spawn(Box::pin(async move {
            while let Some(event) = dc.poll().await {
                match event {
                    DataChannelEvent::OnMessage(msg) => {
                        // Echo verbatim: the sender asserts byte-for-byte round trips.
                        if let Err(err) = dc.send(msg.data).await {
                            eprintln!("echo send failed: {err}");
                        }
                    }
                    DataChannelEvent::OnClose => break,
                    _ => {}
                }
            }
        }));
    }
}

async fn answer_offer(offer: RTCSessionDescription) -> Result<String> {
    let runtime = runtime();

    let mut media = MediaEngine::default();
    media.register_default_codecs()?;
    let registry = register_default_interceptors(Registry::new(), &mut media)?;

    let (gather_tx, mut gather_rx) = channel::<()>(1);

    let pc: Arc<dyn PeerConnection> = Arc::new(
        PeerConnectionBuilder::new()
            .with_configuration(RTCConfigurationBuilder::new().build())
            .with_media_engine(media)
            .with_interceptor_registry(registry)
            .with_handler(Arc::new(EchoAnswerHandler {
                runtime: runtime.clone(),
                gather_tx,
            }))
            .with_runtime(runtime.clone())
            .with_udp_addrs(vec!["127.0.0.1:0".to_string()])
            .build()
            .await?,
    );

    pc.set_remote_description(offer).await?;
    let answer = pc.create_answer(None).await?;
    pc.set_local_description(answer).await?;

    // Non-trickle: the one HTTP response must carry every candidate.
    timeout(Duration::from_secs(10), gather_rx.recv())
        .await
        .map_err(|_| anyhow::anyhow!("answerer gathering timed out"))?;

    let desc = pc
        .local_description()
        .await
        .context("answerer has no local description")?;
    Ok(serde_json::to_string(&desc)?)
}

async fn server_mode(cli: &Cli) -> Result<()> {
    let runtime = runtime();
    let addr: SocketAddr = format!("0.0.0.0:{}", cli.http_port).parse()?;

    runtime.spawn(Box::pin(async move {
        let make_svc = make_service_fn(move |_| async move {
            Ok::<_, hyper::Error>(service_fn(|req: Request<Body>| async move {
                Ok::<Response<Body>, std::convert::Infallible>(
                    match (req.method(), req.uri().path()) {
                        (&Method::POST, "/doSignaling") => {
                            let body_bytes = hyper::body::to_bytes(req.into_body())
                                .await
                                .unwrap_or_default();
                            let offer: Result<RTCSessionDescription> =
                                serde_json::from_slice(&body_bytes).context("bad SDP JSON");
                            match offer {
                                Ok(offer) => match answer_offer(offer).await {
                                    Ok(answer_json) => Response::builder()
                                        .status(StatusCode::OK)
                                        .header("content-type", "application/json")
                                        .body(Body::from(answer_json))
                                        .unwrap(),
                                    Err(e) => Response::builder()
                                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                                        .body(Body::from(e.to_string()))
                                        .unwrap(),
                                },
                                Err(e) => Response::builder()
                                    .status(StatusCode::BAD_REQUEST)
                                    .body(Body::from(e.to_string()))
                                    .unwrap(),
                            }
                        }
                        _ => Response::builder()
                            .status(StatusCode::NOT_FOUND)
                            .body(Body::from("not found"))
                            .unwrap(),
                    },
                )
            }))
        });
        if let Err(e) = Server::bind(&addr).serve(make_svc).await {
            eprintln!("HTTP server error: {e}");
        }
    }));

    println!(
        "interop rust server: POST /doSignaling on http://0.0.0.0:{}",
        cli.http_port
    );
    // Serve until killed — run.sh manages the lifetime.
    loop {
        sleep(Duration::from_secs(3600)).await;
    }
}

struct ClientGather {
    gather_tx: Sender<()>,
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for ClientGather {
    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        if state == RTCIceGatheringState::Complete {
            let _ = self.gather_tx.try_send(());
        }
    }
}

async fn client_mode(cli: &Cli) -> Result<()> {
    let runtime = runtime();
    let (gather_tx, mut gather_rx) = channel::<()>(1);

    let pc: Arc<dyn PeerConnection> = Arc::new(
        PeerConnectionBuilder::new()
            .with_configuration(RTCConfigurationBuilder::new().build())
            .with_handler(Arc::new(ClientGather { gather_tx }))
            .with_runtime(runtime.clone())
            .with_udp_addrs(vec!["127.0.0.1:0".to_string()])
            .build()
            .await?,
    );

    let dc = pc.create_data_channel("interop", None).await?;

    let offer = pc.create_offer(None).await?;
    pc.set_local_description(offer).await?;
    common::timeout(Duration::from_secs(10), gather_rx.recv())
        .await
        .map_err(|_| anyhow::anyhow!("offerer gathering timed out"))?;
    let offer = pc
        .local_description()
        .await
        .context("offerer local description")?;

    // POST the offer, hand back the answer.
    let url = format!("{}/doSignaling", cli.url.trim_end_matches('/'));
    let body = serde_json::to_string(&offer)?;
    let request = Request::builder()
        .method(Method::POST)
        .uri(&url)
        .header("content-type", "application/json")
        .body(Body::from(body))?;

    let client = Client::new();
    let response = client.request(request).await?;
    if response.status() != StatusCode::OK {
        let bytes = hyper::body::to_bytes(response.into_body()).await?;
        anyhow::bail!(
            "signaling failed: {} {}",
            StatusCode::OK,
            String::from_utf8_lossy(&bytes)
        );
    }
    let bytes = hyper::body::to_bytes(response.into_body()).await?;
    let answer: RTCSessionDescription = serde_json::from_slice(&bytes)?;
    pc.set_remote_description(answer).await?;

    // Wait for the channel to open, then send and expect echoes.
    timeout(Duration::from_secs(15), async {
        loop {
            if matches!(dc.ready_state().await, Ok(RTCDataChannelState::Open)) {
                return;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("data channel never opened"))?;

    let (echo_tx, mut echo_rx) = channel::<String>(cli.messages.max(1));
    let dc_poll = dc.clone();
    runtime.spawn(Box::pin(async move {
        while let Some(event) = dc_poll.poll().await {
            if let DataChannelEvent::OnMessage(msg) = event {
                let _ = echo_tx.try_send(String::from_utf8(msg.data.to_vec()).unwrap_or_default());
            }
        }
    }));

    for i in 0..cli.messages {
        let message = format!("{MSG_PREFIX}-{i}");
        dc.send_text(&message).await?;
    }

    let mut echoed = 0usize;
    let mut mismatched = 0usize;
    for i in 0..cli.messages {
        match timeout(Duration::from_secs(15), echo_rx.recv()).await {
            Ok(Some(msg)) => {
                if msg == format!("{MSG_PREFIX}-{i}") {
                    echoed += 1;
                } else {
                    mismatched += 1;
                }
            }
            _ => break,
        }
    }

    let ok = echoed == cli.messages && mismatched == 0;
    println!(
        "RUST_CLIENT result: echoed={}/{} mismatched={} status={}",
        echoed,
        cli.messages,
        mismatched,
        if ok { "PASS" } else { "FAIL" }
    );
    pc.close().await?;
    if !ok {
        std::process::exit(1);
    }
    Ok(())
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    if cli.debug {
        env_logger::Builder::new()
            .filter_level(log::LevelFilter::Debug)
            .init();
    }
    block_on(async {
        match cli.mode.as_str() {
            "server" => server_mode(&cli).await,
            "client" => client_mode(&cli).await,
            other => anyhow::bail!("unknown mode {other:?} (server | client)"),
        }
    })
}
