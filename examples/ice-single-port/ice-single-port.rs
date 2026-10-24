//! ice-single-port demonstrates the ability to serve many PeerConnections on a single port.
//!
//! This is the Rust counterpart of pion's ice-single-port example. Every answering
//! PeerConnection the HTTP signaling endpoint creates is served by one shared UDP socket —
//! a [`UdpMux`] bound once at startup — instead of one socket per connection. That is the
//! deployment shape an SFU wants: a constant number of file descriptors, kernel receive
//! buffers and wakeups, no matter how many subscribers terminate on this host.
//!
//! ## Run
//!
//! ```sh
//! cargo run --example ice-single-port
//! ```
//!
//! Then open http://localhost:8080 in a browser and press "Start". Each browser session
//! gets its own PeerConnection; all of them answer on the same UDP port.
//!
//! Key concepts:
//! - Sharing one listening socket across connections with `UdpMux`
//! - Building every connection with `PeerConnectionBuilder::with_udp_mux`
//! - Non-trickle ICE over one HTTP round trip per connection

use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use rtc::interceptor::Registry;
use rtc::peer_connection::configuration::RTCConfigurationBuilder;
use rtc::peer_connection::configuration::interceptor_registry::register_default_interceptors;
use rtc::peer_connection::configuration::media_engine::MediaEngine;
use rtc::peer_connection::state::RTCIceGatheringState;
use webrtc::data_channel::DataChannel;
use webrtc::peer_connection::udp_mux::UdpMux;
use webrtc::peer_connection::{
    PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCSessionDescription,
};
use webrtc::runtime::{Runtime, Sender, channel};

#[path = "../common/mod.rs"]
mod common;
use common::{block_on, runtime, sleep};

const UDP_MUX_PORT: u16 = 8443;
const HTTP_PORT: u16 = 8080;
static INDEX_HTML: &str = "examples/ice-single-port/index.html";

#[derive(Parser)]
#[command(
    name = "ice-single-port",
    about = "Serve many PeerConnections on one UDP port"
)]
struct Cli {
    #[arg(short, long)]
    debug: bool,
    #[arg(long, default_value_t = UDP_MUX_PORT)]
    udp_port: u16,
    #[arg(long, default_value_t = HTTP_PORT)]
    http_port: u16,
}

/// Per-connection event handler: one per answering PeerConnection.
///
/// The gather channel is per connection — unlike a one-connection server, every signaling
/// request builds its own connection and waits for its own gathering to finish before the
/// single HTTP response carries the answer.
struct SinglePortHandler {
    runtime: Arc<dyn Runtime>,
    gather_tx: Sender<()>,
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for SinglePortHandler {
    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        println!("ICE gathering state has changed: {state:?}");
        if state == RTCIceGatheringState::Complete {
            let _ = self.gather_tx.try_send(());
        }
    }

    async fn on_connection_state_change(
        &self,
        state: webrtc::peer_connection::RTCPeerConnectionState,
    ) {
        println!("Peer Connection State has changed: {state:?}");
    }

    /// The browser is the offerer and creates the data channel; this side sends the server
    /// time on it every three seconds, pion's example behavior.
    async fn on_data_channel(&self, dc: Arc<dyn DataChannel>) {
        let runtime = self.runtime.clone();
        runtime.spawn(Box::pin(async move {
            // The channel reports `Open` once the SCTP association is up — sends before
            // that are refused, so wait for it.
            loop {
                if matches!(
                    dc.ready_state().await,
                    Ok(webrtc::data_channel::RTCDataChannelState::Open)
                ) {
                    break;
                }
                sleep(Duration::from_millis(50)).await;
            }

            loop {
                let message = chrono::Local::now().to_string();
                println!("Sending message: '{message}'");
                if dc.send_text(&message).await.is_err() {
                    break;
                }
                sleep(Duration::from_secs(3)).await;
            }
        }));
    }
}

/// Builds a fresh answering PeerConnection served by `mux`, and returns the answer JSON for
/// the browser once its own ICE gathering has completed (non-trickle: one HTTP round trip
/// carries both descriptions).
async fn do_signaling(offer: RTCSessionDescription, mux: &Arc<UdpMux>) -> Result<String> {
    let runtime = runtime();

    let mut media = MediaEngine::default();
    media.register_default_codecs()?;
    let registry = register_default_interceptors(Registry::new(), &mut media)?;

    let (gather_tx, mut gather_rx) = channel::<()>(1);

    let pc: Arc<dyn PeerConnection> = Arc::new(
        PeerConnectionBuilder::<SocketAddr>::new()
            .with_configuration(RTCConfigurationBuilder::new().build())
            .with_media_engine(media)
            .with_interceptor_registry(registry)
            .with_handler(Arc::new(SinglePortHandler {
                runtime: runtime.clone(),
                gather_tx,
            }))
            .with_runtime(runtime.clone())
            .with_udp_mux(mux.clone())
            .build()
            .await?,
    );

    pc.set_remote_description(offer).await?;
    let answer = pc.create_answer(None).await?;
    pc.set_local_description(answer).await?;

    // Block until ICE gathering is complete (non-trickle ICE): the single HTTP response
    // must carry the candidates with it.
    gather_rx.recv().await;

    match pc.local_description().await {
        Some(desc) => Ok(serde_json::to_string(&desc)?),
        None => Err(anyhow::anyhow!("no local description")),
    }
}

async fn remote_handler(
    req: Request<Body>,
    mux: Arc<UdpMux>,
) -> Result<Response<Body>, hyper::Error> {
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") | (&Method::GET, "/index.html") => {
            match std::fs::read_to_string(INDEX_HTML) {
                Ok(content) => Ok(Response::new(Body::from(content))),
                Err(_) => Ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::from("index.html not found"))
                    .unwrap()),
            }
        }

        (&Method::POST, "/doSignaling") => {
            let body_bytes = hyper::body::to_bytes(req.into_body()).await?;
            let offer_str = match std::str::from_utf8(&body_bytes) {
                Ok(s) => s.to_owned(),
                Err(_) => {
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from("invalid utf-8"))
                        .unwrap());
                }
            };
            let offer = match serde_json::from_str(&offer_str) {
                Ok(sdp) => sdp,
                Err(e) => {
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from(format!("bad SDP: {e}")))
                        .unwrap());
                }
            };

            match do_signaling(offer, &mux).await {
                Ok(payload) => Ok(Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/json")
                    .body(Body::from(payload))
                    .unwrap()),
                Err(e) => Ok(Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::from(e.to_string()))
                    .unwrap()),
            }
        }

        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("not found"))
            .unwrap()),
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    if cli.debug {
        env_logger::Builder::new()
            .format(|buf, record| {
                writeln!(
                    buf,
                    "{}:{} [{}] {} - {}",
                    record.file().unwrap_or("unknown"),
                    record.line().unwrap_or(0),
                    record.level(),
                    chrono::Local::now().format("%H:%M:%S.%6f"),
                    record.args()
                )
            })
            .filter(None, log::LevelFilter::Trace)
            .init();
    }
    block_on(async_main(cli))
}

async fn async_main(cli: Cli) -> Result<()> {
    let (ctrlc_tx, mut ctrlc_rx) = channel::<()>(1);
    ctrlc::set_handler(move || {
        let _ = ctrlc_tx.try_send(());
    })?;

    // The one socket every answering connection serves from. Every browser session gets
    // its own PeerConnection; every one of them answers on this port.
    let mux = UdpMux::bind(format!("[::]:{}", cli.udp_port))?;
    println!("Listening for WebRTC traffic at {}", cli.udp_port);

    let runtime = runtime();
    let addr: SocketAddr = format!("0.0.0.0:{}", cli.http_port).parse()?;
    let mux_srv = mux.clone();
    runtime.spawn(Box::pin(async move {
        let make_svc = make_service_fn(move |_| {
            let mux = mux_srv.clone();
            async move {
                Ok::<_, hyper::Error>(service_fn(move |req| remote_handler(req, mux.clone())))
            }
        });
        if let Err(e) = Server::bind(&addr).serve(make_svc).await {
            eprintln!("HTTP server error: {e}");
        }
    }));

    println!(
        "Open http://localhost:{} to access this demo",
        cli.http_port
    );
    println!("Press ctrl-c to stop");
    ctrlc_rx.recv().await;
    println!();
    mux.close();
    Ok(())
}
