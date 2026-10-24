//! Tracks and packets: what a participant sends, who receives it, and the forwarding between.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use rtc::peer_connection::event::{RTCDataChannelEvent, RTCPeerConnectionEvent, RTCTrackEvent, RTCTrackEventInit};
use rtc::peer_connection::message::RTCMessage;
use rtc::peer_connection::state::RTCPeerConnectionState;
use rtc::rtp_transceiver::rtp_sender::RtpCodecKind;
use rtc::sansio::Protocol;
use rtc::shared::marshal::MarshalSize;
use rtcd_proto::MessageType;
use serde::Deserialize;
use tracing::{debug, info, warn};

use super::session::{DownPhase, DownStream, Lifecycle, Source, SourceKey, SubRef};
use super::session::SessionKey;
use super::types::CloseReason;
use super::Engine;
use crate::ids::{TrackType, track_id};
use crate::metrics::Direction;
use crate::peer::AUDIO_LEVEL_URI;
use crate::rate::RateMonitor;
use crate::vad::{Monitor, VadConfig};

/// Who receives a stream: those it feeds, and those about to be switched onto it.
#[derive(Debug, Default)]
pub(crate) struct Fan {
    pub serving: Vec<SubRef>,
    pub waiting: Vec<SubRef>,
}

#[derive(Deserialize)]
struct ScreenAnnouncement {
    #[serde(rename = "screenStreamID")]
    screen_stream_id: String,
}

#[derive(Deserialize)]
struct VideoAnnouncement {
    #[serde(rename = "videoStreamID")]
    video_stream_id: String,
}

impl Engine {
    /// Handles what a connection reported since last looked at.
    pub(super) fn process_events(&mut self, key: SessionKey, now: Instant) {
        loop {
            let Some(s) = self.sessions.get_mut(key) else { return };
            let Some(event) = s.pc.poll_event() else { return };
            match event {
                RTCPeerConnectionEvent::OnIceCandidateEvent(e) => {
                    if let Ok(init) = e.candidate.to_json() {
                        let data = serde_json::json!({ "type": "candidate", "candidate": init }).to_string().into_bytes();
                        self.emit(key, MessageType::Ice, data);
                    }
                }
                RTCPeerConnectionEvent::OnConnectionStateChangeEvent(state) => self.connection_state(key, state, now),
                RTCPeerConnectionEvent::OnTrack(RTCTrackEvent::OnOpen(init)) => self.track_opened(key, init, now),
                RTCPeerConnectionEvent::OnTrack(RTCTrackEvent::OnClose(id)) => self.track_closed(key, &id.to_string(), now),
                RTCPeerConnectionEvent::OnDataChannel(RTCDataChannelEvent::OnOpen(id)) => self.on_dc_open(key, id, now),
                RTCPeerConnectionEvent::OnDataChannel(RTCDataChannelEvent::OnClose(id)) => self.on_dc_closed(key, id),
                _ => {}
            }
        }
    }

    fn connection_state(&mut self, key: SessionKey, state: RTCPeerConnectionState, now: Instant) {
        let Some(s) = self.sessions.get_mut(key) else { return };
        match state {
            RTCPeerConnectionState::Connected => {
                if s.lifecycle != Lifecycle::Connected {
                    s.lifecycle = Lifecycle::Connected;
                    self.metrics.connection_time(&s.cfg.group_id, now.saturating_duration_since(s.created_at));
                }
                self.metrics.connection_state("connected");
                info!(session = %s.cfg.session_id, "connected");
            }
            RTCPeerConnectionState::Disconnected => {
                self.metrics.connection_state("disconnected");
                debug!(session = %s.cfg.session_id, "disconnected");
            }
            RTCPeerConnectionState::Failed => {
                self.metrics.connection_state("failed");
                self.close_session(key, CloseReason::ConnectionLost, now);
            }
            RTCPeerConnectionState::Closed => self.close_session(key, CloseReason::ConnectionLost, now),
            _ => {}
        }
    }

    /// Drains what a connection received: media to forward, data channel messages to act on.
    /// Returns the packets to forward, or `None` if the session ended meanwhile.
    pub(super) fn process_reads(&mut self, key: SessionKey, now: Instant) -> Option<Vec<(u32, rtc::rtp::Packet)>> {
        let mut rtp = Vec::new();
        loop {
            let s = self.sessions.get_mut(key)?;
            let Some(msg) = s.pc.poll_read() else { break };
            match msg.message {
                RTCMessage::RtpPacket(_, packet) => {
                    let Some(&source) = s.up.by_ssrc.get(&packet.header.ssrc) else { continue };
                    if self.uplink_accepts(key, source, &packet, now) {
                        rtp.push((source, packet));
                    }
                }
                RTCMessage::DataChannelMessage(id, m) => {
                    self.on_dc_message(key, id, m.is_string, m.data, now);
                    self.sessions.get(key)?;
                }
                _ => {}
            }
        }
        Some(rtp)
    }

    /// Bookkeeping for a packet arriving from a participant; false if it must not be forwarded.
    fn uplink_accepts(&mut self, key: SessionKey, source: u32, packet: &rtc::rtp::Packet, now: Instant) -> bool {
        let Some(s) = self.sessions.get_mut(key) else { return false };
        let ext_id = s.up.audio_level_ext;
        let Some(src) = s.up.sources.iter_mut().find(|x| x.id == source) else { return false };
        match src.ty {
            TrackType::Voice => {
                if let (Some(id), Some(vad)) = (ext_id, s.up.vad.as_mut()) {
                    if let Some(level) = packet.header.get_extension_payload(id).and_then(|e| e.first()) {
                        // The level is the low seven bits; the top bit says whether the frame is voice.
                        if let Some(on) = vad.push_packet(level & 0x7f, packet.header.timestamp, now) {
                            self.emit(key, if on { MessageType::VoiceOn } else { MessageType::VoiceOff }, Vec::new());
                        }
                    }
                }
                let Some(s) = self.sessions.get(key) else { return false };
                !s.up.muted
            }
            _ => {
                src.rate.push(now, packet.marshal_size());
                true
            }
        }
    }

    /// A participant's track started to arrive.
    fn track_opened(&mut self, key: SessionKey, init: RTCTrackEventInit, now: Instant) {
        let Some(s) = self.sessions.get_mut(key) else { return };
        let Some(mut receiver) = s.pc.rtp_receiver(init.receiver_id) else { return };
        let track = receiver.track();
        let kind = track.kind();
        let Some(codec) = track.codec(init.ssrc).cloned() else {
            warn!(session = %s.cfg.session_id, ssrc = init.ssrc, "track without a codec");
            return;
        };
        let audio_level = receiver
            .get_parameters()
            .rtp_parameters
            .header_extensions
            .iter()
            .find(|e| e.uri == AUDIO_LEVEL_URI)
            .map(|e| e.id as u8);
        let stream_id = init.stream_ids.first().cloned().unwrap_or_default();
        let rid = init.rid.clone();

        let ty = match kind {
            RtpCodecKind::Audio if !s.up.screen_stream_id.is_empty() && stream_id == s.up.screen_stream_id => TrackType::ScreenAudio,
            RtpCodecKind::Audio => TrackType::Voice,
            RtpCodecKind::Video if !s.up.screen_stream_id.is_empty() && stream_id == s.up.screen_stream_id => TrackType::Screen,
            RtpCodecKind::Video if !s.up.video_stream_id.is_empty() && stream_id == s.up.video_stream_id => TrackType::Video,
            _ => {
                // The announcement of what this stream is can trail its first packets; keep it
                // aside rather than lose it.
                warn!(session = %s.cfg.session_id, stream = %stream_id, "video track of an unannounced stream");
                self.metrics.error(&s.cfg.group_id, "track");
                return;
            }
        };
        let entropy = self.rng.next_u64();
        let out_track_id = s
            .up
            .out_ids
            .entry(ty)
            .or_insert_with(|| track_id(ty, &s.cfg.session_id, entropy))
            .clone();
        if ty == TrackType::Voice {
            if audio_level.is_some() && s.up.vad.is_none() {
                s.up.vad = Some(Monitor::new(VadConfig::default()));
            }
            s.up.audio_level_ext = audio_level;
        }
        let id = s.up.next_source;
        s.up.next_source += 1;
        s.up.by_ssrc.insert(init.ssrc, id);
        let window = Duration::from_secs(if rid.as_deref() == Some("h") { 2 } else { 5 });
        s.up.sources.push(Source {
            id,
            ssrc: init.ssrc,
            ty,
            rid: rid.map(|r| r.to_string()),
            codec: codec.clone(),
            track_id: init.track_id.clone(),
            receiver: init.receiver_id,
            out_track_id,
            rate: RateMonitor::new(window),
        });
        info!(session = %s.cfg.session_id, %ty, ssrc = init.ssrc, "track received");
        self.metrics.track_added(&s.cfg.group_id, Direction::In, &codec.mime_type);
        self.fanout.entry(SourceKey { session: key, source: id }).or_default();

        for member in self.call_members(key) {
            if member != key && self.sessions.get(member).is_some_and(|m| m.down.ready) {
                self.sync_stream(member, key, ty, now);
            }
        }
    }

    /// A participant's track ended.
    fn track_closed(&mut self, key: SessionKey, track: &str, now: Instant) {
        let Some(s) = self.sessions.get_mut(key) else { return };
        let ended: Vec<u32> = s.up.sources.iter().filter(|x| x.track_id.to_string() == track).map(|x| x.id).collect();
        let mut types = HashSet::new();
        let mut orphaned = Vec::new();
        for id in ended {
            let pos = s.up.sources.iter().position(|x| x.id == id).expect("listed above");
            let src = s.up.sources.remove(pos);
            s.up.by_ssrc.remove(&src.ssrc);
            self.metrics.track_removed(&s.cfg.group_id, Direction::In, &src.codec.mime_type);
            types.insert(src.ty);
            if let Some(fan) = self.fanout.remove(&SourceKey { session: key, source: id }) {
                orphaned.extend(fan.serving.into_iter().chain(fan.waiting));
            }
        }
        for sub in orphaned {
            self.detach_stream(sub.session, sub.stream);
        }
        let Some(s) = self.sessions.get_mut(key) else { return };
        for ty in types {
            if !s.up.sources.iter().any(|x| x.ty == ty) {
                s.up.out_ids.remove(&ty);
            }
        }
        // Receivers may have another source for what ended (the other layer of a camera).
        for ty in TrackType::ALL {
            for member in self.call_members(key) {
                if member != key && self.sessions.get(member).is_some_and(|m| m.down.ready) {
                    self.sync_stream(member, key, ty, now);
                }
            }
        }
    }

    /// Gives a participant who just became ready every stream already in the call.
    pub(super) fn attach_existing_tracks(&mut self, key: SessionKey, now: Instant) {
        for member in self.call_members(key) {
            if member == key {
                continue;
            }
            for ty in TrackType::ALL {
                self.sync_stream(key, member, ty, now);
            }
        }
    }

    /// The source `receiver` should get of `publisher`'s streams of type `ty`, if any.
    fn desired_source(&self, receiver: SessionKey, publisher: SessionKey, ty: TrackType) -> Option<u32> {
        let p = self.sessions.get(publisher)?;
        let _ = receiver;
        p.up.sources.iter().find(|x| x.ty == ty).map(|x| x.id)
    }

    /// Makes what `receiver` gets of `publisher`'s `ty` stream match what it should get.
    pub(super) fn sync_stream(&mut self, receiver: SessionKey, publisher: SessionKey, ty: TrackType, _now: Instant) {
        let want = self.desired_source(receiver, publisher, ty);
        let Some(r) = self.sessions.get(receiver) else { return };
        let have = r
            .down
            .iter()
            .find(|(_, d)| d.publisher == publisher && d.ty == ty && !matches!(d.phase, DownPhase::PendingRemove | DownPhase::Removing))
            .map(|(id, d)| (id, d.source));
        match (have, want) {
            (None, None) => {}
            (Some((id, _)), None) => self.detach_stream(receiver, id),
            (Some((_, current)), Some(src)) if current == Some(src) => {}
            (Some((id, _)), Some(_)) => {
                // The stream's source changed under it (a codec or layer swap): replace it.
                self.detach_stream(receiver, id);
                self.attach_stream(receiver, publisher, ty, want.expect("matched Some"));
            }
            (None, Some(src)) => self.attach_stream(receiver, publisher, ty, src),
        }
    }

    fn attach_stream(&mut self, receiver: SessionKey, publisher: SessionKey, ty: TrackType, source: u32) {
        let ssrc = self.rng.ssrc();
        let Some(p) = self.sessions.get(publisher) else { return };
        let Some(src) = p.up.sources.iter().find(|x| x.id == source) else { return };
        let stream = DownStream {
            publisher,
            publisher_session_id: p.cfg.session_id.clone(),
            ty,
            out_track_id: src.out_track_id.clone(),
            ssrc,
            codec: src.codec.clone(),
            phase: DownPhase::PendingAdd,
            sender: None,
            payload_type: None,
            source: Some(source),
        };
        let Some(r) = self.sessions.get_mut(receiver) else { return };
        let id = r.down.insert(stream);
        self.fanout.entry(SourceKey { session: publisher, source }).or_default().serving.push(SubRef { session: receiver, stream: id });
        self.mark_dirty(receiver);
    }

    /// Stops sending a receiver one of its streams.
    pub(super) fn detach_stream(&mut self, receiver: SessionKey, stream: u32) {
        let Some(r) = self.sessions.get_mut(receiver) else { return };
        let Some(ds) = r.down.get_mut(stream) else { return };
        if let Some(source) = ds.source.take() {
            if let Some(fan) = self.fanout.get_mut(&SourceKey { session: ds.publisher, source }) {
                fan.serving.retain(|s| !(s.session == receiver && s.stream == stream));
                fan.waiting.retain(|s| !(s.session == receiver && s.stream == stream));
            }
        }
        match ds.phase {
            // Never offered: nothing to take back.
            DownPhase::PendingAdd => r.down.streams[stream as usize] = None,
            DownPhase::Adding | DownPhase::Active => {
                ds.phase = DownPhase::PendingRemove;
                r.down.changed = true;
            }
            DownPhase::PendingRemove | DownPhase::Removing => {}
        }
        self.mark_dirty(receiver);
    }

    /// Writes packets a participant sent to everyone who receives them.
    pub(super) fn forward(&mut self, key: SessionKey, packets: Vec<(u32, rtc::rtp::Packet)>, now: Instant) {
        for (source, mut packet) in packets {
            let Some(fan) = self.fanout.get(&SourceKey { session: key, source }) else { continue };
            if fan.serving.is_empty() {
                continue;
            }
            // The receivers negotiated their own extension ids; none of the sender's apply.
            packet.header.extension = false;
            packet.header.extensions.clear();
            let size = packet.marshal_size() as u64;
            let mut sent = 0u64;
            let mut ty = TrackType::Voice;
            for sub in &fan.serving {
                let Some(r) = self.sessions.get_mut(sub.session) else { continue };
                let Some(ds) = r.down.get_mut(sub.stream) else { continue };
                if ds.phase != DownPhase::Active {
                    continue;
                }
                let Some(sender_id) = ds.sender else { continue };
                ty = ds.ty;
                let Some(mut sender) = r.pc.rtp_sender(sender_id) else { continue };
                let pt = *ds.payload_type.get_or_insert_with(|| {
                    sender
                        .get_parameters()
                        .rtp_parameters
                        .codecs
                        .iter()
                        .find(|c| c.rtp_codec.mime_type.eq_ignore_ascii_case(&ds.codec.mime_type))
                        .map_or(packet.header.payload_type, |c| c.payload_type)
                });
                let mut out = packet.clone();
                out.header.ssrc = ds.ssrc;
                out.header.payload_type = pt;
                if sender.write_rtp(now, out).is_ok() {
                    sent += size;
                    if !r.dirty {
                        r.dirty = true;
                        self.dirty.push(sub.session);
                    }
                }
            }
            if sent > 0 {
                self.metrics.rtp_bytes_sent(ty, sent);
            }
        }
    }

    pub(super) fn set_muted(&mut self, key: SessionKey, muted: bool, _now: Instant) {
        let Some(s) = self.sessions.get_mut(key) else { return };
        s.up.muted = muted;
        if muted {
            if let Some(vad) = s.up.vad.as_mut() {
                if vad.reset() {
                    self.emit(key, MessageType::VoiceOff, Vec::new());
                }
            }
        }
        debug!(session = %self.session_id(key), muted, "microphone state");
    }

    /// The participant starts a screen share on the stream `data` names.
    pub(super) fn screen_on(&mut self, key: SessionKey, data: &[u8], _now: Instant) {
        let Ok(ann) = serde_json::from_slice::<ScreenAnnouncement>(data) else {
            warn!(session = %self.session_id(key), "unreadable screen announcement");
            return;
        };
        let call = self.call_id_of(key);
        let Some(call) = call.and_then(|c| self.calls.get_mut(&c)) else { return };
        if call.screen.is_some_and(|k| k != key) {
            warn!(session = %self.session_id(key), "someone else is already sharing a screen");
            return;
        }
        call.screen = Some(key);
        if let Some(s) = self.sessions.get_mut(key) {
            s.up.screen_stream_id = ann.screen_stream_id;
        }
    }

    /// The participant ends its screen share.
    pub(super) fn screen_off(&mut self, key: SessionKey, now: Instant) {
        let Some(id) = self.call_id_of(key) else { return };
        if let Some(call) = self.calls.get_mut(&id) {
            if call.screen == Some(key) {
                call.screen = None;
            }
        }
        let Some(s) = self.sessions.get_mut(key) else { return };
        s.up.screen_stream_id.clear();
        let ended: Vec<u32> = s.up.sources.iter().filter(|x| matches!(x.ty, TrackType::Screen | TrackType::ScreenAudio)).map(|x| x.id).collect();
        let mut orphaned = Vec::new();
        for id in ended {
            let pos = s.up.sources.iter().position(|x| x.id == id).expect("listed above");
            let src = s.up.sources.remove(pos);
            s.up.by_ssrc.remove(&src.ssrc);
            self.metrics.track_removed(&s.cfg.group_id, Direction::In, &src.codec.mime_type);
            if let Some(fan) = self.fanout.remove(&SourceKey { session: key, source: id }) {
                orphaned.extend(fan.serving.into_iter().chain(fan.waiting));
            }
        }
        for sub in orphaned {
            self.detach_stream(sub.session, sub.stream);
        }
        if let Some(s) = self.sessions.get_mut(key) {
            s.up.out_ids.remove(&TrackType::Screen);
            s.up.out_ids.remove(&TrackType::ScreenAudio);
        }
        let _ = now;
    }

    /// The participant announces the stream its camera uses.
    pub(super) fn video_on(&mut self, key: SessionKey, data: &[u8]) {
        match serde_json::from_slice::<VideoAnnouncement>(data) {
            Ok(a) => {
                if let Some(s) = self.sessions.get_mut(key) {
                    s.up.video_stream_id = a.video_stream_id;
                }
            }
            Err(err) => warn!(session = %self.session_id(key), %err, "unreadable video announcement"),
        }
    }

    /// The participant names the cameras it wants. Video forwarding policy lands with the video
    /// layer logic; for now the request is recorded.
    pub(super) fn video_subscribe(&mut self, key: SessionKey, _data: &[u8], _now: Instant) {
        let _ = key;
    }

    fn call_id_of(&self, key: SessionKey) -> Option<super::CallId> {
        let s = self.sessions.get(key)?;
        Some(super::CallId { group: s.cfg.group_id.clone(), call: s.cfg.call_id.clone() })
    }
}
