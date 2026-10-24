//! A virtual-time world for the engine: a network, the engine, and browsers that behave like the
//! lms-web call client (`call-client.ts`): a real `rtc` peer connection that offers a data channel
//! and a microphone, answers the SFU's offers, and talks on the signaling data channel.
//!
//! Nothing here sleeps or opens a socket; time is a number the world advances to the next deadline,
//! so a whole call runs in milliseconds and every run is the same.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use rtc::interceptor::Registry;
use rtc::media_stream::MediaStreamTrack;
use rtc::peer_connection::configuration::RTCConfigurationBuilder;
use rtc::peer_connection::configuration::interceptor_registry::register_default_interceptors;
use rtc::peer_connection::configuration::media_engine::MediaEngine;
use rtc::peer_connection::event::{RTCDataChannelEvent, RTCPeerConnectionEvent, RTCTrackEvent};
use rtc::peer_connection::message::RTCMessage;
use rtc::peer_connection::sdp::{RTCSdpType, RTCSessionDescription};
use rtc::peer_connection::state::{RTCPeerConnectionState, RTCSignalingState};
use rtc::peer_connection::transport::{CandidateConfig, CandidateHostConfig, RTCIceCandidate, RTCIceCandidateInit};
use rtc::peer_connection::{RTCPeerConnection, RTCPeerConnectionBuilder};
use rtc::rtp_transceiver::RTCRtpSenderId;
use rtc::rtp_transceiver::rtp_sender::{
    RTCRtpCodec, RTCRtpCodingParameters, RTCRtpEncodingParameters, RTCRtpHeaderExtensionCapability, RtpCodecKind,
};
use rtc::sansio::Protocol;
use rtc::shared::{TaggedBytesMut, TransportContext, TransportProtocol};
use rtcd_proto::dc::{DcMessage, TrackInfo};
use rtcd_proto::{Message, MessageType, SessionConfig};
use rtcd_sfu::config::{EngineConfig, LocalAddress};
use rtcd_sfu::engine::{CloseReason, Command, Engine, Event, Proto};
use rtcd_sfu::ids::Rng;
use rtcd_sfu::metrics::NoMetrics;

pub const AUDIO_LEVEL_URI: &str = "urn:ietf:params:rtp-hdrext:ssrc-audio-level";
pub const SERVER: &str = "10.0.0.1:8443";

/// A track a browser receives.
#[derive(Debug, Clone)]
pub struct RemoteTrack {
    pub track_id: String,
    pub stream_ids: Vec<String>,
    pub ssrc: u32,
    pub rid: Option<String>,
}

/// An emulated browser.
pub struct Browser {
    pub name: String,
    pub session_id: String,
    pub pc: RTCPeerConnection,
    pub addr: SocketAddr,
    pub dc: Option<usize>,
    /// The verdict of the last lock request.
    pub lock_answer: Option<bool>,
    pub tracks: Vec<RemoteTrack>,
    pub ended_tracks: Vec<String>,
    /// Every RTP packet received: (track id, packet).
    pub rtp: Vec<(String, rtc::rtp::Packet)>,
    pub media_maps: Vec<BTreeMap<String, TrackInfo>>,
    pub dc_frames: Vec<DcMessage>,
    pub offers_received: usize,
    pub answers_sent: usize,
    pub mic: RTCRtpSenderId,
    pub mic_ssrc: u32,
    pub mic_seq: u16,
    pub connected: bool,
    pub signaling_conflicts: usize,
    /// Descriptions the SFU sent that the browser has not applied yet (it was mid-offer).
    pub remote_candidates_seen: usize,
}

impl Browser {
    pub fn stream_of(&self, track_id: &str) -> Option<&RemoteTrack> {
        self.tracks.iter().find(|t| t.track_id == track_id)
    }

    /// Packets received on the tracks whose id starts with `prefix`.
    pub fn received_on(&self, prefix: &str) -> usize {
        self.rtp.iter().filter(|(t, _)| t.starts_with(prefix)).count()
    }

    pub fn track_ids(&self) -> Vec<String> {
        self.tracks.iter().map(|t| t.track_id.clone()).collect()
    }
}

pub struct World {
    pub now: Instant,
    pub engine: Engine,
    pub server: SocketAddr,
    pub browsers: Vec<Browser>,
    pub closed: Vec<(String, CloseReason)>,
    pub voice_events: Vec<(String, bool)>,
    pub ice_messages: usize,
    next_browser: u16,
    pub group: String,
    /// Sends from the engine to the browsers at this address are lost while set.
    pub blackhole: bool,
}

pub fn engine_config() -> EngineConfig {
    EngineConfig {
        addresses: vec![LocalAddress {
            bind: "10.0.0.1".parse().unwrap(),
            public: None,
            udp_port: 8443,
            advertised_udp_port: None,
            tcp_port: None,
            advertised_tcp_port: None,
        }],
        ..Default::default()
    }
}

fn host_candidate(addr: SocketAddr) -> RTCIceCandidateInit {
    let c = CandidateHostConfig {
        base_config: CandidateConfig {
            network: "udp".into(),
            address: addr.ip().to_string(),
            port: addr.port(),
            component: 1,
            ..Default::default()
        },
        ..Default::default()
    }
    .new_candidate_host()
    .unwrap();
    RTCIceCandidate::from(&c).to_json().unwrap()
}

impl World {
    pub fn new() -> World {
        World::with_config(engine_config())
    }

    pub fn with_config(cfg: EngineConfig) -> World {
        // `RUST_LOG=rtcd_sfu=debug cargo test -- --nocapture` shows what the engine decides.
        let _ = tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::from_default_env()).with_test_writer().try_init();
        cfg.validate().unwrap();
        World {
            now: Instant::now(),
            engine: Engine::new(Arc::new(cfg), Arc::new(NoMetrics), Rng::seeded(7)),
            server: SERVER.parse().unwrap(),
            browsers: Vec::new(),
            closed: Vec::new(),
            voice_events: Vec::new(),
            ice_messages: 0,
            next_browser: 1,
            group: "group".into(),
            blackhole: false,
        }
    }

    // ----------------------------------------------------------------------------- browsers

    /// Starts a browser in `call` and waits for it to connect and open its data channel.
    pub fn join(&mut self, name: &str, call: &str) -> usize {
        let i = self.start(name, call);
        let ok = self.run_until(Duration::from_secs(20), |w| w.browsers[i].connected && w.browsers[i].dc.is_some());
        assert!(ok, "{name} did not connect");
        i
    }

    /// Starts a browser: asks the engine for a session and sends the first offer, without waiting.
    pub fn start(&mut self, name: &str, call: &str) -> usize {
        let session_id = format!("session-{name}");
        let cfg = SessionConfig {
            group_id: self.group.clone(),
            call_id: call.to_owned(),
            user_id: format!("user-{name}"),
            session_id: session_id.clone(),
            props: SessionConfig::from_join(
                "",
                &rtcd_proto::JoinRequest { call_id: call.into(), user_id: "u".into(), session_id: "s".into(), channel_id: "ch".into(), av1_support: false, dc_signaling: true },
            )
            .props,
        };
        self.engine.handle_command(self.now, Command::Join(cfg));

        let addr: SocketAddr = format!("10.1.0.{}:5000", self.next_browser).parse().unwrap();
        self.next_browser += 1;
        let mut me = MediaEngine::default();
        me.register_default_codecs().unwrap();
        me.register_header_extension(RTCRtpHeaderExtensionCapability { uri: AUDIO_LEVEL_URI.into() }, RtpCodecKind::Audio, None).unwrap();
        let registry = register_default_interceptors(Registry::new(), &mut me).unwrap();
        let mut pc = RTCPeerConnectionBuilder::new()
            .with_configuration(RTCConfigurationBuilder::new().build())
            .with_media_engine(me)
            .with_interceptor_registry(registry)
            .build(self.now)
            .unwrap();
        pc.create_data_channel("calls-dc", None).unwrap();
        let mic_ssrc = 1000 + self.next_browser as u32;
        let mic = pc
            .add_track(MediaStreamTrack::new(
                format!("stream-{name}"),
                format!("mic-{name}"),
                format!("mic-{name}"),
                RtpCodecKind::Audio,
                vec![RTCRtpEncodingParameters {
                    rtp_coding_parameters: RTCRtpCodingParameters { ssrc: Some(mic_ssrc), ..Default::default() },
                    codec: RTCRtpCodec { mime_type: "audio/opus".into(), clock_rate: 48000, channels: 2, sdp_fmtp_line: "minptime=10;useinbandfec=1".into(), rtcp_feedback: vec![] },
                    ..Default::default()
                }],
            ))
            .unwrap();
        pc.add_local_candidate(host_candidate(addr)).unwrap();
        let offer = pc.create_offer(None).unwrap();
        pc.set_local_description(self.now, offer.clone()).unwrap();

        self.browsers.push(Browser {
            name: name.to_owned(),
            session_id: session_id.clone(),
            pc,
            addr,
            dc: None,
            lock_answer: None,
            tracks: vec![],
            ended_tracks: vec![],
            rtp: vec![],
            media_maps: vec![],
            dc_frames: vec![],
            offers_received: 0,
            answers_sent: 0,
            mic,
            mic_ssrc,
            mic_seq: 0,
            connected: false,
            signaling_conflicts: 0,
            remote_candidates_seen: 0,
        });
        let json = serde_json::to_vec(&offer).unwrap();
        self.signal(self.browsers.len() - 1, MessageType::Sdp, json);
        self.browsers.len() - 1
    }

    /// Delivers a message from browser `i` to the engine, as the service would relay it.
    pub fn signal(&mut self, i: usize, kind: MessageType, data: Vec<u8>) {
        let b = &self.browsers[i];
        let msg = Message { group_id: self.group.clone(), user_id: format!("user-{}", b.name), session_id: b.session_id.clone(), call_id: String::new(), kind, data };
        self.engine.handle_command(self.now, Command::Signal(msg));
    }

    pub fn leave(&mut self, i: usize) {
        let id = self.browsers[i].session_id.clone();
        self.engine.handle_command(self.now, Command::Leave { session_id: id });
    }

    pub fn dc_send(&mut self, i: usize, msg: DcMessage) {
        let b = &mut self.browsers[i];
        let id = b.dc.expect("data channel is open");
        b.pc.data_channel(id).unwrap().send(self.now, BytesMut::from(&msg.encode()[..])).unwrap();
    }

    /// The browser asks the SFU for the signaling lock and returns its answer.
    pub fn request_lock(&mut self, i: usize) -> bool {
        self.browsers[i].lock_answer = None;
        self.dc_send(i, DcMessage::Lock(None));
        assert!(self.run_until(Duration::from_secs(2), |w| w.browsers[i].lock_answer.is_some()), "no lock answer");
        self.browsers[i].lock_answer.unwrap()
    }

    pub fn unlock(&mut self, i: usize) {
        self.dc_send(i, DcMessage::Unlock);
        self.run_for(Duration::from_millis(100));
    }

    /// Sends `frames` 20 ms Opus-like packets from browser `i`'s microphone, `level` giving each
    /// frame's audio level, and lets them travel.
    pub fn speak(&mut self, i: usize, frames: usize, level: impl Fn(usize) -> u8) {
        for f in 0..frames {
            self.send_audio(i, level(f));
            self.run_for(Duration::from_millis(20));
        }
    }

    pub fn send_audio(&mut self, i: usize, level: u8) {
        let now = self.now;
        let b = &mut self.browsers[i];
        let (ext_id, pt) = {
            let mut s = b.pc.rtp_sender(b.mic).expect("mic sender");
            let p = s.get_parameters();
            (
                p.rtp_parameters.header_extensions.iter().find(|e| e.uri == AUDIO_LEVEL_URI).map(|e| e.id),
                p.rtp_parameters.codecs.first().map_or(111, |c| c.payload_type),
            )
        };
        let mut header = rtc::rtp::header::Header {
            version: 2,
            payload_type: pt,
            sequence_number: b.mic_seq,
            timestamp: u32::from(b.mic_seq) * 960,
            ssrc: b.mic_ssrc,
            ..Default::default()
        };
        if let Some(id) = ext_id {
            header.set_extension(id as u8, Bytes::from(vec![level])).unwrap();
        }
        b.mic_seq = b.mic_seq.wrapping_add(1);
        b.pc.rtp_sender(b.mic)
            .unwrap()
            .write_rtp(now, rtc::rtp::packet::Packet { header, payload: Bytes::from_static(&[0xfc, 1, 2, 3, 4, 5, 6, 7]) })
            .expect("write_rtp");
    }

    // --------------------------------------------------------------------------- the network

    /// Moves everything that can move right now.
    fn settle(&mut self) {
        for _ in 0..100_000 {
            let mut moved = false;
            // The engine's output to the browsers.
            while let Some(t) = self.engine.poll_transmit() {
                moved = true;
                if self.blackhole {
                    continue;
                }
                if let Some(b) = self.browsers.iter_mut().find(|b| b.addr == t.peer) {
                    let _ = b.pc.handle_read(TaggedBytesMut {
                        now: self.now,
                        transport: TransportContext { local_addr: b.addr, peer_addr: t.local, ecn: None, transport_protocol: TransportProtocol::UDP },
                        message: t.data,
                    });
                }
            }
            // The engine's reports to the service, which relays them to the right browser.
            while let Some(e) = self.engine.poll_event() {
                moved = true;
                self.on_engine_event(e);
            }
            // The browsers.
            for i in 0..self.browsers.len() {
                moved |= self.pump_browser(i);
            }
            if !moved {
                return;
            }
        }
        panic!("the world never went quiet");
    }

    fn on_engine_event(&mut self, e: Event) {
        match e {
            Event::SessionClosed { session_id, reason, .. } => self.closed.push((session_id, reason)),
            Event::Message(m) => match m.kind {
                MessageType::VoiceOn => self.voice_events.push((m.session_id, true)),
                MessageType::VoiceOff => self.voice_events.push((m.session_id, false)),
                MessageType::Ice => {
                    self.ice_messages += 1;
                    if let Some(i) = self.browsers.iter().position(|b| b.session_id == m.session_id) {
                        let v: serde_json::Value = serde_json::from_slice(&m.data).unwrap();
                        assert_eq!(v["type"], "candidate");
                        let init: RTCIceCandidateInit = serde_json::from_value(v["candidate"].clone()).unwrap();
                        if !init.candidate.is_empty() {
                            self.browsers[i].remote_candidates_seen += 1;
                            let _ = self.browsers[i].pc.add_remote_candidate(init);
                        }
                    }
                }
                MessageType::Sdp => {
                    if let Some(i) = self.browsers.iter().position(|b| b.session_id == m.session_id) {
                        self.browser_receives_description(i, &m.data, false);
                    }
                }
                other => panic!("unexpected message to a browser: {other:?}"),
            },
        }
    }

    /// A browser applies a description from the SFU and, for an offer, answers it.
    fn browser_receives_description(&mut self, i: usize, json: &[u8], over_dc: bool) {
        let desc: RTCSessionDescription = serde_json::from_slice(json).unwrap();
        let now = self.now;
        let b = &mut self.browsers[i];
        match desc.sdp_type {
            RTCSdpType::Answer => {
                b.pc.set_remote_description(now, desc).expect("the SFU's answer applies");
            }
            RTCSdpType::Offer => {
                b.offers_received += 1;
                if b.pc.signaling_state() != RTCSignalingState::Stable {
                    b.signaling_conflicts += 1;
                    return;
                }
                b.pc.set_remote_description(now, desc).expect("the SFU's offer applies");
                let answer = b.pc.create_answer(None).unwrap();
                b.pc.set_local_description(now, answer.clone()).unwrap();
                b.answers_sent += 1;
                let json = serde_json::to_vec(&answer).unwrap();
                if over_dc && b.dc.is_some() {
                    let frame = DcMessage::Sdp(json).encode();
                    let id = b.dc.unwrap();
                    b.pc.data_channel(id).unwrap().send(now, BytesMut::from(&frame[..])).unwrap();
                } else {
                    self.signal(i, MessageType::Sdp, json);
                }
            }
            other => panic!("unexpected description {other:?}"),
        }
    }

    fn pump_browser(&mut self, i: usize) -> bool {
        let mut moved = false;
        let now = self.now;
        // What the browser sends the SFU.
        loop {
            let b = &mut self.browsers[i];
            let Some(w) = b.pc.poll_write() else { break };
            moved = true;
            if w.transport.peer_addr == self.server && !self.blackhole {
                self.engine.handle_datagram(now, Proto::Udp, self.server, b.addr, w.message);
            }
        }
        loop {
            let b = &mut self.browsers[i];
            let Some(ev) = b.pc.poll_event() else { break };
            moved = true;
            match ev {
                RTCPeerConnectionEvent::OnConnectionStateChangeEvent(s) => b.connected = s == RTCPeerConnectionState::Connected,
                RTCPeerConnectionEvent::OnIceCandidateEvent(e) => {
                    // The browser trickles its candidates to the SFU through the service.
                    let init = e.candidate.to_json().unwrap();
                    let data = serde_json::to_vec(&init).unwrap();
                    self.signal(i, MessageType::Ice, data);
                }
                RTCPeerConnectionEvent::OnDataChannel(RTCDataChannelEvent::OnOpen(id)) => b.dc = Some(id),
                RTCPeerConnectionEvent::OnTrack(RTCTrackEvent::OnOpen(init)) => b.tracks.push(RemoteTrack {
                    track_id: init.track_id.to_string(),
                    stream_ids: init.stream_ids.iter().map(|s| s.to_string()).collect(),
                    ssrc: init.ssrc,
                    rid: init.rid.map(|r| r.to_string()),
                }),
                RTCPeerConnectionEvent::OnTrack(RTCTrackEvent::OnClose(id)) => b.ended_tracks.push(id.to_string()),
                _ => {}
            }
        }
        loop {
            let b = &mut self.browsers[i];
            let Some(m) = b.pc.poll_read() else { break };
            moved = true;
            match m.message {
                RTCMessage::RtpPacket(track, packet) => b.rtp.push((track.to_string(), packet)),
                RTCMessage::DataChannelMessage(_, m) => {
                    let frame = DcMessage::decode(&m.data).expect("a frame the SFU wrote decodes");
                    b.dc_frames.push(frame.clone());
                    match frame {
                        DcMessage::Lock(Some(granted)) => b.lock_answer = Some(granted),
                        DcMessage::MediaMap(map) => b.media_maps.push(map),
                        DcMessage::Sdp(json) => self.browser_receives_description(i, &json, true),
                        _ => {}
                    }
                }
                _ => {}
            }
        }
        moved
    }

    /// Runs for `span` of virtual time, waking each side at its deadlines.
    pub fn run_for(&mut self, span: Duration) {
        let end = self.now + span;
        loop {
            self.settle();
            let next = self.next_wake().filter(|&t| t <= end).unwrap_or(end);
            if next > self.now {
                self.now = next;
            }
            self.engine.handle_timeout(self.now);
            for b in &mut self.browsers {
                let _ = b.pc.handle_timeout(self.now);
            }
            if self.now >= end {
                self.settle();
                return;
            }
        }
    }

    /// Runs until `done` holds or `limit` of virtual time passes; returns whether it held.
    pub fn run_until(&mut self, limit: Duration, mut done: impl FnMut(&mut World) -> bool) -> bool {
        let end = self.now + limit;
        loop {
            self.settle();
            if done(self) {
                return true;
            }
            if self.now >= end {
                return false;
            }
            let next = self.next_wake().filter(|&t| t <= end).unwrap_or(end);
            self.now = next.max(self.now);
            self.engine.handle_timeout(self.now);
            for b in &mut self.browsers {
                let _ = b.pc.handle_timeout(self.now);
            }
        }
    }

    fn next_wake(&mut self) -> Option<Instant> {
        let mut next = self.engine.poll_timeout();
        for b in &mut self.browsers {
            if let Some(t) = b.pc.poll_timeout() {
                next = Some(next.map_or(t, |n| n.min(t)));
            }
        }
        next
    }

    pub fn browser(&self, name: &str) -> usize {
        self.browsers.iter().position(|b| b.name == name).expect("no such browser")
    }
}
