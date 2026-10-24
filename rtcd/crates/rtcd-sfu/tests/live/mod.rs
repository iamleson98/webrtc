//! A real-time harness: emulated browsers on loopback UDP sockets against a running [`Sfu`].
#![allow(dead_code)]

use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::sync::mpsc::{Receiver, channel};
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
use rtc::peer_connection::transport::{
    CandidateConfig, CandidateHostConfig, RTCIceCandidate, RTCIceCandidateInit,
};
use rtc::peer_connection::{RTCPeerConnection, RTCPeerConnectionBuilder};
use rtc::rtp_transceiver::RTCRtpSenderId;
use rtc::rtp_transceiver::rtp_sender::{
    RTCRtpCodec, RTCRtpCodingParameters, RTCRtpEncodingParameters, RTCRtpHeaderExtensionCapability,
    RtpCodecKind,
};
use rtc::sansio::Protocol;
use rtc::shared::{TaggedBytesMut, TransportContext, TransportProtocol};
use rtcd_proto::dc::DcMessage;
use rtcd_proto::{Message, MessageType, SessionConfig};
use rtcd_sfu::config::{EngineConfig, LocalAddress};
use rtcd_sfu::engine::{CloseReason, Event};
use rtcd_sfu::metrics::NoMetrics;
use rtcd_sfu::runtime::{Sfu, SfuConfig};

const AUDIO_LEVEL_URI: &str = "urn:ietf:params:rtp-hdrext:ssrc-audio-level";

pub struct LiveBrowser {
    pub name: String,
    pub session_id: String,
    pc: RTCPeerConnection,
    socket: UdpSocket,
    server: SocketAddr,
    mic: RTCRtpSenderId,
    mic_ssrc: u32,
    mic_seq: u16,
    dc: Option<usize>,
    pub connected: bool,
    rtp: Vec<String>,
}

impl LiveBrowser {
    pub fn received(&self, prefix: &str) -> usize {
        self.rtp.iter().filter(|t| t.starts_with(prefix)).count()
    }
}

pub struct Live {
    pub sfu: Arc<Sfu>,
    events: Receiver<Event>,
    pub browsers: Vec<LiveBrowser>,
    pub closed: Vec<(String, CloseReason)>,
    pub voice: Vec<(String, bool)>,
    pending: Vec<(usize, Message)>,
}

impl Live {
    pub fn start(shards: usize) -> Live {
        let engine = EngineConfig {
            addresses: vec![LocalAddress {
                bind: "127.0.0.1".parse().unwrap(),
                public: None,
                udp_port: 0,
                advertised_udp_port: None,
                tcp_port: None,
                advertised_tcp_port: None,
            }],
            ..Default::default()
        };
        let cfg = SfuConfig {
            engine,
            shards,
            udp_recv_buffer: 1 << 20,
            udp_send_buffer: 1 << 20,
            ..Default::default()
        };
        let (tx, rx) = channel();
        let tx = std::sync::Mutex::new(tx);
        let sfu = Sfu::start(
            cfg,
            Arc::new(NoMetrics),
            Arc::new(move |e: Event| {
                let _ = tx.lock().unwrap().send(e);
            }),
        )
        .expect("the SFU starts");
        Live {
            sfu: Arc::new(sfu),
            events: rx,
            browsers: vec![],
            closed: vec![],
            voice: vec![],
            pending: vec![],
        }
    }

    fn server_addr(&self) -> SocketAddr {
        self.sfu.addresses()[0].advertised_udp()
    }

    pub fn join(&mut self, name: &str, call: &str) -> usize {
        let i = self.start_browser(name, call);
        assert!(
            self.run_until(Duration::from_secs(15), |l| l.browsers[i].connected
                && l.browsers[i].dc.is_some()),
            "{name} did not connect"
        );
        i
    }

    fn start_browser(&mut self, name: &str, call: &str) -> usize {
        let session_id = format!("session-{name}");
        let mut props = rtcd_proto::SessionConfig::from_join(
            "",
            &rtcd_proto::JoinRequest {
                dc_signaling: true,
                ..Default::default()
            },
        )
        .props;
        let _ = &mut props;
        self.sfu
            .join(SessionConfig {
                group_id: "group".into(),
                call_id: call.into(),
                user_id: format!("user-{name}"),
                session_id: session_id.clone(),
                props,
            })
            .expect("join");

        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        socket.set_nonblocking(true).unwrap();
        let addr = socket.local_addr().unwrap();
        let mut me = MediaEngine::default();
        me.register_default_codecs().unwrap();
        me.register_header_extension(
            RTCRtpHeaderExtensionCapability {
                uri: AUDIO_LEVEL_URI.into(),
            },
            RtpCodecKind::Audio,
            None,
        )
        .unwrap();
        let registry = register_default_interceptors(Registry::new(), &mut me).unwrap();
        let mut pc = RTCPeerConnectionBuilder::new()
            .with_configuration(RTCConfigurationBuilder::new().build())
            .with_media_engine(me)
            .with_interceptor_registry(registry)
            .build(Instant::now())
            .unwrap();
        pc.create_data_channel("calls-dc", None).unwrap();
        let mic_ssrc = 5000 + self.browsers.len() as u32;
        let mic = pc
            .add_track(MediaStreamTrack::new(
                format!("stream-{name}"),
                format!("mic-{name}"),
                format!("mic-{name}"),
                RtpCodecKind::Audio,
                vec![RTCRtpEncodingParameters {
                    rtp_coding_parameters: RTCRtpCodingParameters {
                        ssrc: Some(mic_ssrc),
                        ..Default::default()
                    },
                    codec: RTCRtpCodec {
                        mime_type: "audio/opus".into(),
                        clock_rate: 48000,
                        channels: 2,
                        sdp_fmtp_line: "minptime=10;useinbandfec=1".into(),
                        rtcp_feedback: vec![],
                    },
                    ..Default::default()
                }],
            ))
            .unwrap();
        let cand = CandidateHostConfig {
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
        pc.add_local_candidate(RTCIceCandidate::from(&cand).to_json().unwrap())
            .unwrap();
        let offer = pc.create_offer(None).unwrap();
        pc.set_local_description(Instant::now(), offer.clone())
            .unwrap();
        let server = self.server_addr();
        self.browsers.push(LiveBrowser {
            name: name.into(),
            session_id,
            pc,
            socket,
            server,
            mic,
            mic_ssrc,
            mic_seq: 0,
            dc: None,
            connected: false,
            rtp: vec![],
        });
        let i = self.browsers.len() - 1;
        self.send_signal(i, MessageType::Sdp, serde_json::to_vec(&offer).unwrap());
        i
    }

    fn send_signal(&self, i: usize, kind: MessageType, data: Vec<u8>) {
        let b = &self.browsers[i];
        let _ = self.sfu.signal(Message {
            group_id: "group".into(),
            user_id: format!("user-{}", b.name),
            session_id: b.session_id.clone(),
            call_id: String::new(),
            kind,
            data,
        });
    }

    pub fn speak(&mut self, i: usize, frames: usize) {
        for f in 0..frames {
            self.send_audio(i, if f % 2 == 0 { 20 } else { 70 });
            self.run(Duration::from_millis(20));
        }
    }

    fn send_audio(&mut self, i: usize, level: u8) {
        let b = &mut self.browsers[i];
        let (ext, pt) = {
            let mut s = b.pc.rtp_sender(b.mic).unwrap();
            let p = s.get_parameters();
            (
                p.rtp_parameters
                    .header_extensions
                    .iter()
                    .find(|e| e.uri == AUDIO_LEVEL_URI)
                    .map(|e| e.id),
                p.rtp_parameters
                    .codecs
                    .first()
                    .map_or(111, |c| c.payload_type),
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
        if let Some(id) = ext {
            header
                .set_extension(id as u8, Bytes::from(vec![level]))
                .unwrap();
        }
        b.mic_seq = b.mic_seq.wrapping_add(1);
        let _ = b.pc.rtp_sender(b.mic).unwrap().write_rtp(
            Instant::now(),
            rtc::rtp::packet::Packet {
                header,
                payload: Bytes::from_static(&[0xfc, 1, 2, 3, 4]),
            },
        );
    }

    /// One pass over everything: the SFU's reports, each browser's sockets, events and timers.
    pub fn pump(&mut self) {
        self.drain_events();
        let now = Instant::now();
        for i in 0..self.browsers.len() {
            let mut buf = [0u8; 2048];
            loop {
                let b = &mut self.browsers[i];
                match b.socket.recv_from(&mut buf) {
                    Ok((n, from)) => {
                        let local = b.socket.local_addr().unwrap();
                        let _ = b.pc.handle_read(TaggedBytesMut {
                            now,
                            transport: TransportContext {
                                local_addr: local,
                                peer_addr: from,
                                ecn: None,
                                transport_protocol: TransportProtocol::UDP,
                            },
                            message: BytesMut::from(&buf[..n]),
                        });
                    }
                    Err(_) => break,
                }
            }
            let _ = self.browsers[i].pc.handle_timeout(now);
            self.pump_browser(i);
        }
        for (i, msg) in std::mem::take(&mut self.pending) {
            self.send_signal(i, msg.kind, msg.data);
        }
    }

    fn pump_browser(&mut self, i: usize) {
        let mut outgoing: Vec<(MessageType, Vec<u8>)> = Vec::new();
        let mut dc_out: Vec<Vec<u8>> = Vec::new();
        {
            let b = &mut self.browsers[i];
            while let Some(w) = b.pc.poll_write() {
                let _ = b.socket.send_to(&w.message, b.server);
            }
            while let Some(ev) = b.pc.poll_event() {
                match ev {
                    RTCPeerConnectionEvent::OnConnectionStateChangeEvent(s) => {
                        b.connected = s == RTCPeerConnectionState::Connected
                    }
                    RTCPeerConnectionEvent::OnIceCandidateEvent(e) => outgoing.push((
                        MessageType::Ice,
                        serde_json::to_vec(&e.candidate.to_json().unwrap()).unwrap(),
                    )),
                    RTCPeerConnectionEvent::OnDataChannel(RTCDataChannelEvent::OnOpen(id)) => {
                        b.dc = Some(id)
                    }
                    RTCPeerConnectionEvent::OnTrack(RTCTrackEvent::OnOpen(_)) => {}
                    _ => {}
                }
            }
            while let Some(m) = b.pc.poll_read() {
                match m.message {
                    RTCMessage::RtpPacket(track, _) => b.rtp.push(track.to_string()),
                    RTCMessage::DataChannelMessage(_, m) => {
                        if let Ok(DcMessage::Sdp(json)) = DcMessage::decode(&m.data)
                            && let Ok(desc) = serde_json::from_slice::<RTCSessionDescription>(&json)
                        {
                            if desc.sdp_type == RTCSdpType::Offer
                                && b.pc.signaling_state() == RTCSignalingState::Stable
                            {
                                b.pc.set_remote_description(Instant::now(), desc).unwrap();
                                let answer = b.pc.create_answer(None).unwrap();
                                b.pc.set_local_description(Instant::now(), answer.clone())
                                    .unwrap();
                                dc_out.push(
                                    DcMessage::Sdp(serde_json::to_vec(&answer).unwrap()).encode(),
                                );
                            } else if desc.sdp_type == RTCSdpType::Answer {
                                b.pc.set_remote_description(Instant::now(), desc).unwrap();
                            }
                        }
                    }
                    _ => {}
                }
            }
            if let Some(id) = b.dc {
                for frame in dc_out.drain(..) {
                    let _ =
                        b.pc.data_channel(id)
                            .unwrap()
                            .send(Instant::now(), BytesMut::from(&frame[..]));
                }
            }
            while let Some(w) = b.pc.poll_write() {
                let _ = b.socket.send_to(&w.message, b.server);
            }
        }
        for (kind, data) in outgoing {
            self.send_signal(i, kind, data);
        }
    }

    pub fn drain_events(&mut self) {
        while let Ok(e) = self.events.try_recv() {
            match e {
                Event::SessionClosed {
                    session_id, reason, ..
                } => self.closed.push((session_id, reason)),
                Event::Message(m) => {
                    let Some(i) = self
                        .browsers
                        .iter()
                        .position(|b| b.session_id == m.session_id)
                    else {
                        continue;
                    };
                    match m.kind {
                        MessageType::VoiceOn => self.voice.push((m.session_id, true)),
                        MessageType::VoiceOff => self.voice.push((m.session_id, false)),
                        MessageType::Ice => {
                            let v: serde_json::Value = serde_json::from_slice(&m.data).unwrap();
                            if let Ok(init) = serde_json::from_value::<RTCIceCandidateInit>(
                                v["candidate"].clone(),
                            ) && !init.candidate.is_empty()
                            {
                                let _ = self.browsers[i].pc.add_remote_candidate(init);
                            }
                        }
                        MessageType::Sdp => {
                            let desc: RTCSessionDescription =
                                serde_json::from_slice(&m.data).unwrap();
                            let b = &mut self.browsers[i];
                            if desc.sdp_type == RTCSdpType::Answer {
                                b.pc.set_remote_description(Instant::now(), desc).unwrap();
                            } else if b.pc.signaling_state() == RTCSignalingState::Stable {
                                b.pc.set_remote_description(Instant::now(), desc).unwrap();
                                let answer = b.pc.create_answer(None).unwrap();
                                b.pc.set_local_description(Instant::now(), answer.clone())
                                    .unwrap();
                                self.pending.push((
                                    i,
                                    Message {
                                        group_id: "group".into(),
                                        user_id: String::new(),
                                        session_id: m.session_id.clone(),
                                        call_id: String::new(),
                                        kind: MessageType::Sdp,
                                        data: serde_json::to_vec(&answer).unwrap(),
                                    },
                                ));
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    pub fn run(&mut self, span: Duration) {
        let end = Instant::now() + span;
        while Instant::now() < end {
            self.pump();
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    pub fn run_until(&mut self, limit: Duration, mut done: impl FnMut(&mut Live) -> bool) -> bool {
        let end = Instant::now() + limit;
        while Instant::now() < end {
            self.pump();
            if done(self) {
                return true;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        false
    }
}
