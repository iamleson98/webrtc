//! A forwarder where one thread owns every peer connection and the one UDP socket they share.
//!
//! The `broadcast` examples of the async crate and of pion move each packet through several tasks
//! (publisher driver, fan-out loop, one task per viewer, one driver per viewer), and every hop can
//! be a cross-thread wakeup. Here the sans-IO `rtc` crate makes none necessary: a datagram is read,
//! handed to its connection, the publisher's RTP is rewritten and written to every viewer's sender
//! in the same loop turn, and the resulting datagrams leave in one batch.
//!
//! Same signaling as the other `broadcast` examples (base64 offers POSTed to `/sdp`, answers printed
//! on stdout, the first offer is the publisher), so `benches/sfu-load` drives it unchanged.

use std::collections::HashMap;
use std::net::{SocketAddr, UdpSocket};
use std::os::fd::AsRawFd;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use anyhow::Result;
use bytes::BytesMut;
use clap::Parser;
use rtc::interceptor::Registry;
use rtc::media_stream::MediaStreamTrack;
use rtc::peer_connection::configuration::RTCConfigurationBuilder;
use rtc::peer_connection::configuration::interceptor_registry::register_default_interceptors;
use rtc::peer_connection::configuration::media_engine::MediaEngine;
use rtc::peer_connection::configuration::setting_engine::SettingEngineBuilder;
use rtc::peer_connection::event::{RTCPeerConnectionEvent, RTCTrackEvent};
use rtc::peer_connection::message::RTCMessage;
use rtc::peer_connection::sdp::RTCSessionDescription;
use rtc::peer_connection::state::RTCPeerConnectionState;
use rtc::peer_connection::transport::{
    CandidateConfig, CandidateHostConfig, RTCDtlsRole, RTCIceCandidate,
};
use rtc::peer_connection::{RTCPeerConnection, RTCPeerConnectionBuilder};
use rtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication;
use rtc::rtp_transceiver::rtp_sender::{
    RTCRtpCodec, RTCRtpCodingParameters, RTCRtpEncodingParameters, RtpCodecKind,
};
use rtc::rtp_transceiver::{RTCRtpReceiverId, RTCRtpSenderId};
use rtc::sansio::Protocol;
use rtc::shared::{TaggedBytesMut, TransportContext, TransportProtocol};

#[derive(Parser)]
struct Cli {
    #[arg(long, default_value_t = 8080)]
    port: u16,
    /// Send with sendmmsg and UDP GSO (Linux) instead of one sendto per datagram.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    batch: bool,
}

enum Role {
    Publisher { receivers: Vec<(RTCRtpReceiverId, u32)> },
    Viewer { sender: RTCRtpSenderId, ssrc: u32, payload_type: Option<u8> },
}

struct Conn {
    pc: RTCPeerConnection,
    role: Role,
    /// Earliest instant the connection wants `handle_timeout`, as of its last turn.
    deadline: Option<Instant>,
    dirty: bool,
    gone: bool,
}

struct Out {
    data: BytesMut,
    to: SocketAddr,
}

struct Server {
    socket: UdpSocket,
    local: SocketAddr,
    conns: Vec<Option<Conn>>,
    free: Vec<usize>,
    by_addr: HashMap<SocketAddr, usize>,
    by_ufrag: HashMap<String, usize>,
    publisher: Option<usize>,
    codec: Option<RTCRtpCodec>,
    out: Vec<Out>,
    batch: bool,
    /// UDP GSO works on this host (cleared the first time the kernel refuses it).
    gso: bool,
    forwarded: u64,
    last_pli: Instant,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let (offer_tx, offer_rx) = mpsc::channel::<String>();
    let port = cli.port;
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async move {
            let mut rx = rtc_signal::http_sdp_server(port).await;
            println!("Waiting for broadcaster offer on port {port}");
            while let Some(line) = rx.recv().await {
                if offer_tx.send(line).is_err() {
                    break;
                }
            }
        });
    });

    let ip = rtc_signal::get_local_ip();
    let socket = UdpSocket::bind(SocketAddr::new(ip, 0))?;
    socket.set_nonblocking(true)?;
    let local = socket.local_addr()?;
    let mut server = Server {
        socket,
        local,
        conns: vec![],
        free: vec![],
        by_addr: HashMap::new(),
        by_ufrag: HashMap::new(),
        publisher: None,
        codec: None,
        out: Vec::with_capacity(256),
        batch: cli.batch,
        gso: true,
        forwarded: 0,
        last_pli: Instant::now(),
    };
    server.run(offer_rx)
}

impl Server {
    fn run(&mut self, offers: mpsc::Receiver<String>) -> Result<()> {
        let mut buf = vec![0u8; 2048];
        // Viewer offers that arrived before the publisher's track: held, in order, until it has.
        let mut waiting: std::collections::VecDeque<String> = Default::default();
        loop {
            while let Ok(line) = offers.try_recv() {
                waiting.push_back(line);
            }
            while let Some(line) = waiting.front() {
                if self.publisher.is_some() && self.codec.is_none() {
                    break;
                }
                let line = line.clone();
                waiting.pop_front();
                if let Err(err) = self.accept(&line) {
                    println!("offer rejected: {err}");
                }
            }

            // Wait for a datagram or the next deadline, whichever is first (and look at the
            // signaling channel at least every 10 ms).
            let now = Instant::now();
            let wake = self
                .conns
                .iter()
                .flatten()
                .filter_map(|c| c.deadline)
                .min()
                .map_or(Duration::from_millis(10), |t| t.saturating_duration_since(now))
                .min(Duration::from_millis(10));
            poll_readable(&self.socket, wake);

            // Everything that has arrived, then one pass over what it touched.
            let mut received = 0;
            loop {
                match self.socket.recv_from(&mut buf) {
                    Ok((n, from)) => {
                        self.on_datagram(&buf[..n], from);
                        received += 1;
                        if received >= 512 {
                            break;
                        }
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(err) => return Err(err.into()),
                }
            }

            let now = Instant::now();
            for i in 0..self.conns.len() {
                let due = self.conns[i]
                    .as_ref()
                    .is_some_and(|c| c.deadline.is_some_and(|t| t <= now));
                if due {
                    let c = self.conns[i].as_mut().unwrap();
                    let _ = c.pc.handle_timeout(now);
                    c.dirty = true;
                }
            }
            if let Some(p) = self.publisher
                && now.duration_since(self.last_pli) > Duration::from_secs(3)
            {
                self.last_pli = now;
                self.request_keyframe(p, now);
            }
            self.pump(now);
            self.flush();
        }
    }

    fn request_keyframe(&mut self, p: usize, now: Instant) {
        let Some(c) = self.conns[p].as_mut() else { return };
        let Role::Publisher { receivers } = &c.role else { return };
        for (id, ssrc) in receivers.clone() {
            if let Some(mut r) = c.pc.rtp_receiver(id) {
                let _ = r.write_rtcp(
                    now,
                    vec![Box::new(PictureLossIndication { sender_ssrc: 0, media_ssrc: ssrc })],
                );
            }
        }
        c.dirty = true;
    }

    fn accept(&mut self, line: &str) -> Result<()> {
        let offer: RTCSessionDescription = serde_json::from_str(&rtc_signal::decode(line)?)?;
        let is_publisher = self.publisher.is_none();
        if !is_publisher && self.codec.is_none() {
            anyhow::bail!("the publisher has not sent its track yet");
        }

        let mut media_engine = MediaEngine::default();
        media_engine.register_default_codecs()?;
        let registry = register_default_interceptors(Registry::new(), &mut media_engine)?;
        let mut pc = RTCPeerConnectionBuilder::new()
            .with_configuration(RTCConfigurationBuilder::new().build())
            .with_setting_engine(
                SettingEngineBuilder::new().with_answering_dtls_role(RTCDtlsRole::Server).build(),
            )
            .with_media_engine(media_engine)
            .with_interceptor_registry(registry)
            .build(Instant::now())?;

        let role = if is_publisher {
            pc.add_transceiver_from_kind(RtpCodecKind::Video, None)?;
            Role::Publisher { receivers: vec![] }
        } else {
            let ssrc = rand::random_ssrc();
            let codec = self.codec.clone().unwrap();
            let sender = pc.add_track(MediaStreamTrack::new(
                format!("sfu-core-{ssrc}"),
                format!("sfu-core-track-{ssrc}"),
                format!("sfu-core-video-{ssrc}"),
                RtpCodecKind::Video,
                vec![RTCRtpEncodingParameters {
                    rtp_coding_parameters: RTCRtpCodingParameters { ssrc: Some(ssrc), ..Default::default() },
                    codec,
                    ..Default::default()
                }],
            ))?;
            Role::Viewer { sender, ssrc, payload_type: None }
        };

        pc.set_remote_description(Instant::now(), offer)?;
        let candidate = CandidateHostConfig {
            base_config: CandidateConfig {
                network: "udp".to_owned(),
                address: self.local.ip().to_string(),
                port: self.local.port(),
                component: 1,
                ..Default::default()
            },
            ..Default::default()
        }
        .new_candidate_host()?;
        pc.add_local_candidate(RTCIceCandidate::from(&candidate).to_json()?)?;
        let answer = pc.create_answer(None)?;
        pc.set_local_description(Instant::now(), answer)?;
        let local = pc.local_description().ok_or_else(|| anyhow::anyhow!("no local description"))?;
        let ufrag = local
            .sdp
            .lines()
            .find_map(|l| l.strip_prefix("a=ice-ufrag:"))
            .ok_or_else(|| anyhow::anyhow!("no ice-ufrag"))?
            .trim()
            .to_owned();
        println!("{} answer:\n{}", if is_publisher { "Broadcaster" } else { "Viewer" }, rtc_signal::encode(&serde_json::to_string(&local)?));

        let conn = Conn { deadline: pc.poll_timeout(), pc, role, dirty: true, gone: false };
        let idx = if let Some(i) = self.free.pop() {
            self.conns[i] = Some(conn);
            i
        } else {
            self.conns.push(Some(conn));
            self.conns.len() - 1
        };
        self.by_ufrag.insert(ufrag, idx);
        if is_publisher {
            self.publisher = Some(idx);
        }
        Ok(())
    }

    fn on_datagram(&mut self, data: &[u8], from: SocketAddr) {
        let idx = match self.by_addr.get(&from) {
            Some(&i) => i,
            None => match stun_local_ufrag(data).and_then(|u| self.by_ufrag.get(&u).copied()) {
                Some(i) => {
                    self.by_addr.insert(from, i);
                    i
                }
                None => return,
            },
        };
        let Some(c) = self.conns[idx].as_mut() else { return };
        let _ = c.pc.handle_read(TaggedBytesMut {
            now: Instant::now(),
            transport: TransportContext {
                local_addr: self.local,
                peer_addr: from,
                ecn: None,
                transport_protocol: TransportProtocol::UDP,
            },
            message: BytesMut::from(data),
        });
        c.dirty = true;
    }

    /// Drains every connection that was touched: events first, then what it received (the
    /// publisher's RTP fans out to the viewers, which dirties them), then what it wants to send.
    fn pump(&mut self, now: Instant) {
        // Rounds, because forwarding dirties other connections.
        for _ in 0..4 {
            let mut again = false;
            for i in 0..self.conns.len() {
                if !self.conns[i].as_ref().is_some_and(|c| c.dirty) {
                    continue;
                }
                again = true;
                let mut forward: Vec<rtc::rtp::Packet> = vec![];
                {
                    let c = self.conns[i].as_mut().unwrap();
                    c.dirty = false;
                    while let Some(event) = c.pc.poll_event() {
                        match event {
                            RTCPeerConnectionEvent::OnConnectionStateChangeEvent(state) => {
                                if matches!(state, RTCPeerConnectionState::Failed | RTCPeerConnectionState::Closed) {
                                    c.gone = true;
                                }
                            }
                            RTCPeerConnectionEvent::OnTrack(RTCTrackEvent::OnOpen(init)) => {
                                if let Role::Publisher { receivers } = &mut c.role
                                    && let Some(r) = c.pc.rtp_receiver(init.receiver_id)
                                {
                                    let track = r.track();
                                    if let Some(ssrc) = track.ssrcs().next() {
                                        if self.codec.is_none() {
                                            self.codec = track.codec(ssrc).cloned();
                                        }
                                        receivers.push((init.receiver_id, ssrc));
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    while let Some(m) = c.pc.poll_read() {
                        if let (Role::Publisher { .. }, RTCMessage::RtpPacket(_, packet)) = (&c.role, m.message) {
                            forward.push(packet);
                        }
                    }
                }
                if !forward.is_empty() {
                    self.fan_out(forward, now);
                }
                let c = self.conns[i].as_mut().unwrap();
                while let Some(msg) = c.pc.poll_write() {
                    self.out.push(Out { data: msg.message, to: msg.transport.peer_addr });
                }
                c.deadline = c.pc.poll_timeout();
            }
            if !again {
                break;
            }
        }

        // Retire what has failed or closed.
        for i in 0..self.conns.len() {
            if self.conns[i].as_ref().is_some_and(|c| c.gone) {
                let mut c = self.conns[i].take().unwrap();
                let _ = c.pc.close();
                self.by_addr.retain(|_, v| *v != i);
                self.by_ufrag.retain(|_, v| *v != i);
                if self.publisher == Some(i) {
                    println!("Broadcaster disconnected");
                    self.publisher = None;
                }
                self.free.push(i);
                let live = self.conns.iter().flatten().count();
                println!("Total active connections: {live}");
            }
        }
    }

    fn fan_out(&mut self, packets: Vec<rtc::rtp::Packet>, now: Instant) {
        for packet in packets {
            self.forwarded += 1;
            for c in self.conns.iter_mut().flatten() {
                let Role::Viewer { sender, ssrc, payload_type } = &mut c.role else { continue };
                if c.gone || c.pc.connection_state() != RTCPeerConnectionState::Connected {
                    continue;
                }
                let mut s = match c.pc.rtp_sender(*sender) {
                    Some(s) => s,
                    None => continue,
                };
                let pt = *payload_type.get_or_insert_with(|| {
                    s.get_parameters()
                        .rtp_parameters
                        .codecs
                        .first()
                        .map_or(packet.header.payload_type, |c| c.payload_type)
                });
                let mut p = packet.clone();
                p.header.ssrc = *ssrc;
                p.header.payload_type = pt;
                p.header.extension = false;
                p.header.extensions.clear();
                if s.write_rtp(now, p).is_ok() {
                    c.dirty = true;
                }
            }
        }
    }

    fn flush(&mut self) {
        if self.out.is_empty() {
            return;
        }
        #[cfg(target_os = "linux")]
        if self.batch {
            send_batch(&self.socket, &mut self.out, &mut self.gso);
            self.out.clear();
            return;
        }
        let _ = self.batch;
        for o in self.out.drain(..) {
            let _ = self.socket.send_to(&o.data, o.to);
        }
    }
}

mod rand {
    pub fn random_ssrc() -> u32 {
        use std::time::{SystemTime, UNIX_EPOCH};
        static SEQ: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(1);
        let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().subsec_nanos();
        t.wrapping_mul(2654435761).wrapping_add(n.wrapping_mul(40503)) | 1
    }
}

/// Blocks until the socket is readable or `timeout` passes.
fn poll_readable(socket: &UdpSocket, timeout: Duration) {
    let mut fd = libc::pollfd { fd: socket.as_raw_fd(), events: libc::POLLIN, revents: 0 };
    let ms = timeout.as_millis().min(i32::MAX as u128) as i32;
    // SAFETY: one valid pollfd, count 1.
    unsafe { libc::poll(&mut fd, 1, ms.max(if timeout.is_zero() { 0 } else { 1 })) };
}

/// The local ufrag of an ICE binding request: the USERNAME attribute is `local:remote`.
fn stun_local_ufrag(data: &[u8]) -> Option<String> {
    if data.len() < 20 || data[0] > 1 || data[4..8] != [0x21, 0x12, 0xa4, 0x42] {
        return None;
    }
    let mut at = 20;
    while at + 4 <= data.len() {
        let kind = u16::from_be_bytes([data[at], data[at + 1]]);
        let len = u16::from_be_bytes([data[at + 2], data[at + 3]]) as usize;
        let value = data.get(at + 4..at + 4 + len)?;
        if kind == 0x0006 {
            let name = std::str::from_utf8(value).ok()?;
            return name.split(':').next().map(str::to_owned);
        }
        at += 4 + len.div_ceil(4) * 4;
    }
    None
}

/// Sends `out` in as few syscalls as the kernel allows: datagrams are grouped by destination (the
/// order within a destination is kept), each run of equal-size datagrams becomes one UDP GSO
/// message (the kernel splits it again, but walks the stack once), and up to 64 messages go in a
/// single `sendmmsg`. Falls back to plain sends if the kernel or the NIC refuses GSO.
#[cfg(target_os = "linux")]
fn send_batch(socket: &UdpSocket, out: &mut [Out], gso: &mut bool) {
    use std::mem::{size_of, zeroed};

    const MAX_SEGMENTS: usize = 64;
    const MAX_GSO_BYTES: usize = 65_000;
    const UDP_SEGMENT: libc::c_int = 103;

    #[repr(C, align(8))]
    struct Control([u8; 32]);

    // Group by destination, keeping each destination's order.
    let mut ids: HashMap<SocketAddr, usize> = HashMap::new();
    for o in out.iter() {
        let next = ids.len();
        ids.entry(o.to).or_insert(next);
    }
    out.sort_by_key(|o| ids[&o.to]);

    // Runs: (start, end, segment size); a run of one is a plain datagram.
    let mut runs: Vec<(usize, usize, usize)> = Vec::new();
    let mut i = 0;
    while i < out.len() {
        let size = out[i].data.len();
        let mut j = i + 1;
        let mut total = size;
        if *gso {
            while j < out.len()
                && j - i < MAX_SEGMENTS
                && out[j].to == out[i].to
                && out[j].data.len() <= size
                && total + out[j].data.len() <= MAX_GSO_BYTES
            {
                let len = out[j].data.len();
                total += len;
                j += 1;
                if len < size {
                    break; // a shorter datagram can only be the last segment
                }
            }
        }
        runs.push((i, j, size));
        i = j;
    }

    for chunk in runs.chunks(64) {
        let mut addrs: Vec<libc::sockaddr_storage> = Vec::with_capacity(chunk.len());
        let mut lens: Vec<libc::socklen_t> = Vec::with_capacity(chunk.len());
        let mut iovs: Vec<libc::iovec> = Vec::new();
        let mut spans: Vec<(usize, usize)> = Vec::with_capacity(chunk.len());
        let mut controls: Vec<Control> = Vec::with_capacity(chunk.len());
        for &(start, end, size) in chunk {
            // SAFETY: zeroed sockaddr_storage is valid; the fields set below fit its layout.
            let mut ss: libc::sockaddr_storage = unsafe { zeroed() };
            let len = match out[start].to {
                SocketAddr::V4(a) => {
                    let sin = &mut ss as *mut _ as *mut libc::sockaddr_in;
                    unsafe {
                        (*sin).sin_family = libc::AF_INET as _;
                        (*sin).sin_port = a.port().to_be();
                        (*sin).sin_addr = libc::in_addr { s_addr: u32::from_ne_bytes(a.ip().octets()) };
                    }
                    size_of::<libc::sockaddr_in>()
                }
                SocketAddr::V6(a) => {
                    let sin6 = &mut ss as *mut _ as *mut libc::sockaddr_in6;
                    unsafe {
                        (*sin6).sin6_family = libc::AF_INET6 as _;
                        (*sin6).sin6_port = a.port().to_be();
                        (*sin6).sin6_addr = libc::in6_addr { s6_addr: a.ip().octets() };
                    }
                    size_of::<libc::sockaddr_in6>()
                }
            };
            addrs.push(ss);
            lens.push(len as libc::socklen_t);
            let first_iov = iovs.len();
            for o in &out[start..end] {
                iovs.push(libc::iovec { iov_base: o.data.as_ptr() as *mut _, iov_len: o.data.len() });
            }
            spans.push((first_iov, end - start));
            let mut control = Control([0; 32]);
            if end - start > 1 {
                // SAFETY: the buffer is 8-aligned and CMSG_SPACE(2) <= 32 bytes.
                unsafe {
                    let cm = control.0.as_mut_ptr() as *mut libc::cmsghdr;
                    (*cm).cmsg_level = libc::SOL_UDP;
                    (*cm).cmsg_type = UDP_SEGMENT;
                    (*cm).cmsg_len = libc::CMSG_LEN(2) as _;
                    std::ptr::write_unaligned(libc::CMSG_DATA(cm) as *mut u16, size as u16);
                }
            }
            controls.push(control);
        }
        let mut msgs: Vec<libc::mmsghdr> = (0..chunk.len())
            .map(|k| {
                // SAFETY: zeroed mmsghdr is valid; the pointers stay valid until the call returns.
                let mut m: libc::mmsghdr = unsafe { zeroed() };
                m.msg_hdr.msg_name = &mut addrs[k] as *mut _ as *mut _;
                m.msg_hdr.msg_namelen = lens[k];
                m.msg_hdr.msg_iov = &mut iovs[spans[k].0];
                m.msg_hdr.msg_iovlen = spans[k].1 as _;
                if spans[k].1 > 1 {
                    m.msg_hdr.msg_control = controls[k].0.as_mut_ptr() as *mut _;
                    m.msg_hdr.msg_controllen = unsafe { libc::CMSG_SPACE(2) } as _;
                }
                m
            })
            .collect();
        let mut sent = 0;
        while sent < msgs.len() {
            // SAFETY: msgs[sent..] are initialised and point at live buffers.
            let n = unsafe {
                libc::sendmmsg(socket.as_raw_fd(), msgs[sent..].as_mut_ptr(), (msgs.len() - sent) as u32, 0)
            };
            if n > 0 {
                sent += n as usize;
                continue;
            }
            let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
            let (start, end, _) = chunk[sent];
            if end - start > 1 && (errno == libc::EIO || errno == libc::EINVAL || errno == libc::EOPNOTSUPP || errno == libc::ENOPROTOOPT) {
                // The kernel or NIC does not do GSO: stop using it and send this run one by one.
                *gso = false;
                for o in &out[start..end] {
                    let _ = socket.send_to(&o.data, o.to);
                }
                sent += 1;
                continue;
            }
            // Anything else (a full socket buffer, an unreachable peer): drop this message.
            sent += 1;
        }
    }
}
