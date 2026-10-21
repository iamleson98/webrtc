//! UDP mux: serving many peer connections from one shared UDP socket.
//!
//! WebRTC is connectionless at the wire — a peer connection is identified by the ICE username
//! fragment it listens with, not by a dialled transport. That is what makes multiplexing
//! possible: one UDP socket can front any number of peer connections, as long as an incoming
//! datagram can be attributed to the connection it belongs to. An SFU that terminates ten
//! thousand subscribers needs exactly one listening socket this way, instead of ten thousand —
//! one per peer connection — which is the difference between `N` file descriptors, `N` kernel
//! receive buffers and `N` wakeups and a constant amount of each.
//!
//! This is the Rust counterpart of pion's `ice.NewMultiUDPMuxFromPort` +
//! `SettingEngine.SetICEUDPMux` (the [ice-single-port] example). Applications share a mux by
//! building every peer connection with [`with_udp_mux`](crate::PeerConnectionBuilder::with_udp_mux):
//!
//! ```no_run
//! # use webrtc::peer_connection::udp_mux::UdpMux;
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mux = UdpMux::bind("0.0.0.0:8443")?;
//! // Every connection built with this mux answers on 8443, whatever its own
//! // ports would have been:
//! // PeerConnectionBuilder::new().with_udp_mux(mux.clone())...
//! # Ok(())
//! # }
//! ```
//!
//! # How a datagram finds its connection
//!
//! Two rules, checked in order, attribute every incoming datagram:
//!
//! 1. **By peer address.** Each connection's handle claims the remote address the first time
//!    it *sends* there (`poll_send` does this lazily) and, on the receive side, the first time
//!    a routed packet *arrives* from one. From then on every datagram from that address —
//!    STUN, DTLS or SRTP — is delivered to that connection and to no other.
//! 2. **By ICE username fragment.** A datagram from an address nobody has claimed is parsed
//!    as STUN, and if it is a request carrying a `USERNAME` attribute of the form
//!    `local-fragment:remote-fragment` (RFC 8445 §7.2.2 — every ICE connectivity check is one),
//!    the local fragment identifies the connection uniquely. The address is then entered
//!    into the address map, so rule 1 covers everything that follows.
//!
//! Anything else — a datagram from a stranger that is not an ICE check — is dropped and
//! counted. This is the same demultiplexing strategy pion's `UDPMuxDefault` uses.
//!
//! # Delivering to the driver
//!
//! Each connection gets a [`MuxSocketHandle`] implementing
//! [`AsyncUdpSocket`](crate::runtime::AsyncUdpSocket), so the driver's existing event loop —
//! recv futures, burst drain, GSO flush — runs unmodified over it. The handle receives from a
//! bounded per-connection queue filled by one demux task that owns the real socket; it sends
//! directly through that socket, because a UDP send names its destination and needs no
//! coordination with the other connections. The queue is bounded so one stalled connection
//! cannot make the shared socket drop traffic for the healthy ones — its loss is its own.
//!
//! # Limitations, shared with pion
//!
//! * A TURN allocation is a 5-tuple resource: local address, local port, server address,
//!   server port, protocol. Two connections on one mux reach the same TURN server with the
//!   *same* 5-tuple, so the second is answered 437 (Allocation Mismatch, RFC 8656 §7.2). Give
//!   each mux'd connection using TURN its own `turn:` server address — the common single-port
//!   SFU deployment uses host candidates on a public address instead and needs no relay.
//! * The same reasoning applies to shared STUN servers for server-reflexive gathering: routing
//!   by address sends a shared server's replies to whichever connection claimed it first.
//!   Distinct servers per connection are fine.
//!
//! [ice-single-port]: https://github.com/pion/webrtc/tree/master/examples/ice-single-port

use crate::peer_connection::driver::resolve_bind_addrs;
use crate::peer_connection::transport::{
    UDP_RECV_BUF_LEN, gro_recv_buf_len, is_retryable_socket_recv_error,
};
use crate::runtime::{AsyncUdpSocket, EcnCodepoint, RecvMeta, Runtime, Sender, Transmit, channel};
use futures::future::poll_fn;
use futures::task::AtomicWaker;
use futures::FutureExt;
use log::{debug, error, trace};
use rtc::shared::error::{Error, Result};
use rtc::stun::attributes::ATTR_USERNAME;
use rtc::stun::message::{Message as StunMessage, is_stun_message};
use std::collections::{HashMap, VecDeque};
use std::io::{self, IoSliceMut};
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::task::{Context, Poll};

/// Per-connection receive queue cap, in datagrams.
///
/// The queue is what isolates connections sharing a mux: a driver that stops draining its
/// handle (a stalled application, a parked event loop) keeps holding at most this many
/// datagrams, and packets *beyond* it are dropped on that connection alone. The number is
/// sized to ride out a scheduling hiccup of tens of milliseconds at full media rate without
/// ever making a healthy connection pay for a sick one.
const MAX_QUEUE_DATAGRAMS: usize = 4096;

/// How many datagrams the demux task routes between sweeps of dead registrations.
///
/// Map entries are held weakly (a dropped connection's registration reclaims itself), so the
/// sweep is only hygiene: it reclaims the keys of connections whose peer addresses never
/// appear again instead of letting them accumulate behind long-lived strangers.
const SWEEP_INTERVAL: u64 = 4096;

/// Lock a [`std::sync::Mutex`] surviving a poisoned predecessor.
///
/// The mux's locks guard plain data structures and are never held across an await, so a panic
/// in one critical section cannot leave the structure itself broken — the next locker
/// recovers the intact state.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// A UDP socket shared by many peer connections.
///
/// Created with [`bind`](Self::bind) on the address the deployment wants to *listen* on —
/// typically one fixed public port, the same socket for every incoming peer. Build each
/// [`PeerConnectionBuilder`](crate::PeerConnectionBuilder) with
/// [`with_udp_mux`](crate::PeerConnectionBuilder::with_udp_mux) pointing here and the
/// connections it builds answer on this one port. See the [module docs](self) for how
/// attribution works and what the limitations are.
pub struct UdpMux {
    runtime: Arc<dyn Runtime>,
    socket: Arc<dyn AsyncUdpSocket>,
    /// The address the socket is bound to, exactly as the kernel sees it — possibly a
    /// wildcard, which is why candidate addresses are derived separately (see
    /// [`candidate_addrs`](Self::candidate_addrs)).
    bound_addr: SocketAddr,
    /// The addresses to advertise as host candidates: the bind address with a wildcard
    /// expanded into one entry per usable local interface, mirroring what the driver does for
    /// its own sockets. Peers cannot dial `0.0.0.0`, so a wildcard bind must not advertise
    /// itself as a candidate.
    candidate_addrs: Vec<SocketAddr>,
    state: Mutex<MuxState>,
    /// Wake for [`close`](Self::close): the one channel to the demux task's `select`.
    close_tx: Mutex<Option<Sender<u8>>>,
    closed: AtomicBool,
    /// Datagrams received but not attributable to any connection. Queue-overflow drops are
    /// *not* included — those are charged to the overflowing connection.
    unrouted: AtomicU64,
}

struct MuxState {
    /// ICE username fragment -> the connection listening with it.
    by_ufrag: HashMap<String, Weak<MuxQueue>>,
    /// Peer address -> the connection that owns traffic with that peer.
    by_addr: HashMap<SocketAddr, Weak<MuxQueue>>,
    /// Datagrams routed since the last dead-registration sweep.
    routed: u64,
}

/// One demultiplexed datagram, as captured by the demux task.
struct Datagram {
    data: Vec<u8>,
    from: SocketAddr,
    /// The destination IP the kernel recorded for the packet — what a wildcard-bound socket
    /// reconstructs its local address from, so the connection sees the candidate address the
    /// peer actually dialled rather than `0.0.0.0`.
    dst: IpAddr,
    ecn: Option<EcnCodepoint>,
}

/// The per-connection receive side of a mux: a queue the demux task fills and the connection's
/// driver drains through [`MuxSocketHandle`].
struct MuxQueue {
    local_addr: SocketAddr,
    deque: Mutex<VecDeque<Datagram>>,
    waker: AtomicWaker,
    /// Datagrams dropped because this queue was at capacity — the stalled-connection tax,
    /// charged to the connection that stalled.
    overflows: AtomicU64,
}

impl std::fmt::Debug for UdpMux {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UdpMux")
            .field("bound_addr", &self.bound_addr)
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish()
    }
}

/// A connection's handle onto the shared socket.
///
/// Implements [`AsyncUdpSocket`], so the driver treats it as just another socket: receives
/// drain a queue filled by the mux's demux task, sends go straight out the real socket (a UDP
/// send names its destination and needs nothing from the other connections). The handle is
/// also a registration — while it is alive the mux attributes both its username fragment and,
/// once established, its peers' addresses to this connection, and dropping it unregisters.
pub struct MuxSocketHandle {
    mux: Arc<UdpMux>,
    queue: Arc<MuxQueue>,
    ufrag: String,
}

impl std::fmt::Debug for MuxSocketHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MuxSocketHandle")
            .field("ufrag", &self.ufrag)
            .field("local_addr", &self.queue.local_addr)
            .finish()
    }
}

impl UdpMux {
    /// Binds the shared socket on `addrs` and starts its demux task on the default runtime.
    ///
    /// Same as [`bind_on`](Self::bind_on) with the runtime that
    /// [`PeerConnectionBuilder::build`](crate::PeerConnectionBuilder::build) would pick — the
    /// built-in one for the enabled runtime feature. Connections built on a *different*
    /// runtime must use [`bind_on`](Self::bind_on) and pass it, or the demux task will never
    /// be polled.
    pub fn bind<A: ToSocketAddrs>(addrs: A) -> Result<Arc<Self>> {
        let runtime = crate::runtime::default_runtime()
            .ok_or_else(|| Error::Other("no async runtime feature enabled".to_owned()))?;
        Self::bind_on(runtime, addrs)
    }

    /// Binds the shared socket on `addrs` and starts its demux task on `runtime`.
    ///
    /// Pass the **same** runtime that the peer connections sharing this mux are built with —
    /// both the demux task and the connections' drivers are spawned on it, and a socket is
    /// driven by the reactor that created it.
    ///
    /// `addrs` names the address to listen on, verbatim: `0.0.0.0:8443` binds the wildcard and
    /// receives on every interface of that family, exactly as an application expects of a
    /// listening socket. (Host candidates are still advertised per interface — see
    /// [`candidate_addrs`](Self::candidate_addrs).) A list is tried in order; the first
    /// address that binds becomes the one true socket, so a list is a fallback, not a set of
    /// listeners.
    pub fn bind_on<A: ToSocketAddrs>(runtime: Arc<dyn Runtime>, addrs: A) -> Result<Arc<Self>> {
        let mut last_err = None;
        let mut bound = None;
        for addr in addrs.to_socket_addrs()? {
            match std::net::UdpSocket::bind(addr) {
                Ok(socket) => {
                    socket.set_nonblocking(true)?;
                    bound = Some(socket);
                    break;
                }
                Err(err) => last_err = Some(err),
            }
        }
        let socket = match bound {
            Some(socket) => socket,
            None => {
                return Err(Error::Other(format!(
                    "udp mux: no address bound{}",
                    last_err.map(|err| format!(": {err}")).unwrap_or_default()
                )))
            }
        };
        let bound_addr = socket.local_addr()?;
        let socket = runtime.wrap_udp_socket(socket)?;

        // The addresses to advertise as host candidates. A wildcard is expanded to one entry
        // per usable interface — the same rule the driver applies to its own sockets — so a
        // peer always gets an address it can dial. Both paths advertise identically.
        let candidate_addrs = resolve_bind_addrs(&[bound_addr])?
            .into_iter()
            .map(|bind| bind.addr)
            .collect();

        let mux = Arc::new(Self {
            runtime,
            socket,
            bound_addr,
            candidate_addrs,
            state: Mutex::new(MuxState {
                by_ufrag: HashMap::new(),
                by_addr: HashMap::new(),
                routed: 0,
            }),
            close_tx: Mutex::new(None),
            closed: AtomicBool::new(false),
            unrouted: AtomicU64::new(0),
        });

        // The single reader of the real socket. Every connection's handle receives what this
        // task hands it; nothing else ever polls the socket, which is what keeps queue order
        // and waker bookkeeping single-producer and race-free.
        let task = Self::demux_task(Arc::clone(&mux));
        mux.runtime.spawn(Pin::from(Box::pin(task)));

        debug!(
            "udp mux listening on {} (host candidates: {:?})",
            mux.bound_addr, mux.candidate_addrs
        );
        Ok(mux)
    }

    /// The address the shared socket is bound to, verbatim — possibly a wildcard.
    pub fn local_addr(&self) -> SocketAddr {
        self.bound_addr
    }

    /// The addresses a peer connection sharing this mux should advertise as host candidates:
    /// one per usable local interface when the bind address is a wildcard, the bind address
    /// itself when it is concrete. These are what the connection's STUN gathering turns into
    /// host candidates, so a wildcard bind still yields candidates a peer can dial.
    pub fn candidate_addrs(&self) -> Vec<SocketAddr> {
        self.candidate_addrs.clone()
    }

    /// Datagrams the mux received but could not attribute to any connection. Queue-overflow
    /// drops are *not* here — they are charged to the overflowing connection.
    pub fn unrouted_datagrams(&self) -> u64 {
        self.unrouted.load(Ordering::Relaxed)
    }

    /// Registers a connection listening with ICE username fragment `ufrag`, returning its
    /// socket handle.
    ///
    /// Called by the driver at bind time — and again on every ICE-restart rebind, since a
    /// restart replaces the credentials — with the fragment the core reports through
    /// [`local_ice_parameters`](rtc::PeerConnection::local_ice_parameters). While the returned
    /// handle is alive, the mux attributes both `ufrag` and (once established) the connection's
    /// peers to it; dropping the handle unregisters.
    pub(crate) fn register(self: Arc<Self>, ufrag: &str) -> Result<Arc<MuxSocketHandle>> {
        // The guard is scoped to the map update: constructing the handle below moves
        // `self`, and the two must not overlap.
        let queue = {
            let mut state = lock(&self.state);
            if self.closed.load(Ordering::Acquire) {
                return Err(Error::Other(format!(
                    "udp mux on {} is closed",
                    self.bound_addr
                )));
            }
            if state
                .by_ufrag
                .get(ufrag)
                .is_some_and(|weak| weak.strong_count() > 0)
            {
                return Err(Error::Other(format!(
                    "udp mux: username fragment {ufrag:?} already registered"
                )));
            }

            let queue = Arc::new(MuxQueue {
                local_addr: self.bound_addr,
                deque: Mutex::new(VecDeque::new()),
                waker: AtomicWaker::new(),
                overflows: AtomicU64::new(0),
            });
            state.by_ufrag.insert(ufrag.to_owned(), Arc::downgrade(&queue));
            queue
        };
        Ok(Arc::new(MuxSocketHandle {
            mux: self,
            queue,
            ufrag: ufrag.to_owned(),
        }))
    }

    /// Stops the demux task. Queued datagrams are kept until their handles drain or drop.
    ///
    /// The mux does not free the socket on `Drop` — the demux task holds the mux alive from
    /// the inside, so an explicit `close` is the only exit. The socket itself closes when the
    /// last connection handle drops after it. Idempotent.
    pub fn close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            if let Some(tx) = lock(&self.close_tx).take() {
                let _ = tx.try_send(0);
            }
            debug!("udp mux on {} closed", self.bound_addr);
        }
    }

    /// Routes one received datagram to the connection it belongs to.
    ///
    /// The demux task's whole per-packet decision, extracted so tests can exercise the
    /// routing table without a socket. Ownership in order: a claimed peer address wins;
    /// failing that, an ICE connectivity check's username fragment bootstraps the mapping;
    /// failing that, the datagram is dropped and counted.
    pub(crate) fn route_datagram(
        &self,
        from: SocketAddr,
        data: &[u8],
        dst: IpAddr,
        ecn: Option<EcnCodepoint>,
    ) {
        let mut state = lock(&self.state);
        state.routed += 1;

        // 1. Known peer: this address is already owned by a connection.
        if let Some(queue) = take_live(&mut state.by_addr, &from) {
            drop(state);
            push_routed(&queue, Datagram {
                data: data.to_vec(),
                from,
                dst,
                ecn,
            });
            return;
        }

        // 2. Unknown peer speaking ICE: the username fragment says whose check this is. Only
        //    requests carry USERNAME (RFC 8445 §7.2.2); a response is a reply to something
        //    *we* sent, which rule 1 already owns via the send-side claim.
        if let Some(ufrag) = local_ufrag_from_stun_request(data) {
            if let Some(queue) = take_live(&mut state.by_ufrag, &ufrag) {
                // Pin the address: every datagram that follows — DTLS, SCTP, SRTP — is this
                // connection's, and must not pay the STUN parse again.
                state.by_addr.insert(from, Arc::downgrade(&queue));
                drop(state);
                push_routed(&queue, Datagram {
                    data: data.to_vec(),
                    from,
                    dst,
                    ecn,
                });
                return;
            }
        }

        // 3. Nobody's: drop, count, move on. Reclaim dropped-connection keys while here.
        self.unrouted.fetch_add(1, Ordering::Relaxed);
        trace!("udp mux {}: unrouted datagram from {}", self.bound_addr, from);
        if state.routed % SWEEP_INTERVAL == 0 {
            state.by_addr.retain(|_, weak| weak.strong_count() > 0);
            state.by_ufrag.retain(|_, weak| weak.strong_count() > 0);
        }
    }

    /// The demux task: the one reader of the real socket.
    ///
    /// Reads, de-coalesces (a GRO read may deliver several datagrams — split by `stride`
    /// before routing, since consecutive coalesced datagrams are as likely to belong to
    /// different connections as the same one), routes, and loops — until
    /// [`close`](Self::close) or a fatal socket error.
    ///
    /// The receive buffer is threaded through the read future rather than recreated: each
    /// read's `async` block takes ownership of it and hands it back in its output, the same
    /// move-in/move-out the driver's own recv futures use.
    async fn demux_task(self: Arc<Self>) {
        let buf_len = gro_recv_buf_len(self.socket.max_gro_segments()).max(UDP_RECV_BUF_LEN);
        let mut buf = vec![0u8; buf_len];

        // The close wake. Registered before the first read so a `close` racing task startup
        // still lands in the `select` below rather than between iterations.
        let (close_tx, mut close_rx) = channel::<u8>(1);
        *lock(&self.close_tx) = Some(close_tx);

        loop {
            if self.closed.load(Ordering::Acquire) {
                return;
            }

            let socket = Arc::clone(&self.socket);
            let read = {
                let mut buf = buf;
                async move {
                    let mut meta = [RecvMeta::default(); 1];
                    let res = poll_fn(|cx| {
                        let mut bufs = [IoSliceMut::new(&mut buf)];
                        socket.poll_recv(cx, &mut bufs, &mut meta)
                    })
                    .await;
                    (res, meta[0], buf)
                }
            };

            futures::select! {
                (res, meta, read_buf) = read.fuse() => {
                    buf = read_buf;
                    match res {
                        Ok(_) => {
                            // One GRO read may hold several datagrams; split by `stride` and
                            // route each, the same de-segmentation the driver's burst path
                            // performs. `dst_ip` is `None` where the platform does not
                            // report the datagram's destination — the bind address is then
                            // the best available answer, and exactly right for a concrete
                            // bind.
                            let dst_ip = meta.dst_ip.unwrap_or_else(|| self.bound_addr.ip());
                            let step = meta.stride.max(1);
                            let mut off = 0;
                            while off < meta.len {
                                let end = (off + step).min(meta.len);
                                self.route_datagram(meta.addr, &buf[off..end], dst_ip, meta.ecn);
                                off = end;
                            }
                        }
                        // Transient (an ICMP port-unreachable surfacing here, a spurious
                        // EAGAIN): the read is retried, exactly as the driver's own receive
                        // loop does. Anything else ends the mux.
                        Err(err) if is_retryable_socket_recv_error(&err) => continue,
                        Err(err) => {
                            error!("udp mux {}: socket read failed: {err}", self.bound_addr);
                            self.closed.store(true, Ordering::Release);
                            return;
                        }
                    }
                }
                _ = close_rx.recv().fuse() => return,
            }
        }
    }
}

/// The live queue a map entry points at, clearing the entry first when it has died.
///
/// Registrations are [`Weak`] so a dropped connection un-registers itself without a goodbye;
/// the flip side is that a lookup can race a drop, and clearing the dead entry right there
/// keeps a re-connecting peer from being wedged behind it.
fn take_live<K>(map: &mut HashMap<K, Weak<MuxQueue>>, key: &K) -> Option<Arc<MuxQueue>>
where
    K: std::hash::Hash + Eq + Clone,
{
    match map.get(key) {
        Some(weak) => match weak.upgrade() {
            Some(queue) => Some(queue),
            None => {
                map.remove(key);
                None
            }
        },
        None => None,
    }
}

/// Deliver a routed datagram to a connection's queue, waking its driver.
///
/// The overflow accounting is charged to the queue that overflowed, not to the mux, because
/// the health of the *other* connections sharing the socket is exactly what the bound must
/// protect. The waker is signalled after the lock: the consumer registers its waker under the
/// same lock it pops from, so a wake published either side of its critical section cannot be
/// lost.
fn push_routed(queue: &MuxQueue, datagram: Datagram) {
    {
        let mut deque = lock(&queue.deque);
        if deque.len() >= MAX_QUEUE_DATAGRAMS {
            queue.overflows.fetch_add(1, Ordering::Relaxed);
            return;
        }
        deque.push_back(datagram);
    }
    queue.waker.wake();
}

/// The local ICE username fragment from a STUN request, if `data` is one.
///
/// An ICE connectivity check is a Binding request whose `USERNAME` attribute is
/// `local-fragment:remote-fragment` (RFC 8445 §7.2.2) — the local fragment names the
/// addressee, which is the whole demultiplexing key. Only requests qualify: responses are
/// owned by rule 1 through the send-side claim, and indications carry no username at all.
fn local_ufrag_from_stun_request(data: &[u8]) -> Option<String> {
    if !is_stun_message(data) {
        return None;
    }
    let mut message = StunMessage::default();
    message.raw = data.to_vec();
    message.decode().ok()?;

    // Only requests bootstrap an address mapping. A success response carrying the same
    // username bytes must not: it belongs to a check this connection itself sent, and
    // routing it to whichever connection's fragment appears there would misattribute
    // traffic between connections sharing the socket.
    if message.typ.class != rtc::stun::message::CLASS_REQUEST {
        return None;
    }

    let username = message.get(ATTR_USERNAME).ok()?;
    let username = String::from_utf8(username).ok()?;
    let ufrag = username.split(':').next()?.to_owned();
    if ufrag.is_empty() {
        return None;
    }
    Some(ufrag)
}

impl AsyncUdpSocket for MuxSocketHandle {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.queue.local_addr)
    }

    fn poll_send(&self, cx: &mut Context<'_>, transmit: &Transmit<'_>) -> Poll<io::Result<usize>> {
        // Claim the destination on first send: replies — STUN binding responses, TURN — come
        // back from the address we sent to, and an address-map entry is what routes them home
        // without a username fragment to go by. An entry left behind by a dropped connection
        // is re-claimed rather than obeyed.
        {
            let mut state = lock(&self.mux.state);
            match state.by_addr.entry(transmit.destination) {
                std::collections::hash_map::Entry::Vacant(vacant) => {
                    vacant.insert(Arc::downgrade(&self.queue));
                }
                std::collections::hash_map::Entry::Occupied(mut occupied) => {
                    if occupied.get().strong_count() == 0 {
                        occupied.insert(Arc::downgrade(&self.queue));
                    }
                }
            }
        }

        self.mux.socket.poll_send(cx, transmit)
    }

    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        let mut deque = lock(&self.queue.deque);
        if let Some(datagram) = deque.pop_front() {
            drop(deque);

            let cap = bufs[0].len();
            let len = datagram.data.len().min(cap);
            if len < datagram.data.len() {
                trace!(
                    "udp mux: datagram from {} truncated to {len} of {} bytes",
                    datagram.from,
                    datagram.data.len()
                );
            }
            bufs[0][..len].copy_from_slice(&datagram.data[..len]);

            // Built field-by-field: `RecvMeta` is `#[non_exhaustive]`, so a literal will not
            // compile. `stride == len`: the queue holds de-coalesced datagrams, never a
            // GRO batch, so there is nothing to de-segment.
            let mut filled = RecvMeta::default();
            filled.addr = datagram.from;
            filled.len = len;
            filled.stride = len;
            filled.dst_ip = Some(datagram.dst);
            filled.ecn = datagram.ecn;
            meta[0] = filled;

            return Poll::Ready(Ok(1));
        }

        // Register inside the queue's lock: a producer must take the same lock to push, so
        // the waker cannot be lost to a push that lands between the empty check and the
        // registration.
        self.queue.waker.register(cx.waker());
        Poll::Pending
    }

    /// GSO passes straight through to the real socket: a segmented transmit is still one
    /// peer's traffic, and the kernel does the splitting.
    fn max_gso_segments(&self) -> usize {
        self.mux.socket.max_gso_segments()
    }

    /// Reports the real socket's capacity so the driver sizes receive buffers for the largest
    /// datagram the kernel can deliver, even though the handle itself hands out one
    /// de-coalesced datagram at a time.
    fn max_gro_segments(&self) -> usize {
        self.mux.socket.max_gro_segments()
    }
}

impl Drop for MuxSocketHandle {
    fn drop(&mut self) {
        // Unregister exactly what this handle registered. A restart may have replaced the
        // ufrag entry with a newer queue — that entry must survive — and the address map may
        // hold entries of other connections. Pointer identity decides.
        let queue = Arc::downgrade(&self.queue);
        let mut state = lock(&self.mux.state);
        if state
            .by_ufrag
            .get(&self.ufrag)
            .is_some_and(|weak| Weak::ptr_eq(weak, &queue))
        {
            state.by_ufrag.remove(&self.ufrag);
        }
        state.by_addr.retain(|_, weak| !Weak::ptr_eq(weak, &queue));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::task::noop_waker;
    use rtc::stun::message::MAGIC_COOKIE;
    use std::net::{IpAddr, Ipv4Addr};

    /// A STUN Binding request carrying `username`, hand-built to keep the test independent
    /// of the encoder's API surface: type 0x0001, magic cookie, one USERNAME attribute,
    /// RFC 5389 padding to 4 bytes.
    fn stun_binding_request(username: &str, transaction: u8) -> Vec<u8> {
        let username = username.as_bytes();
        let padded = (username.len() + 3) & !3;
        let total = 4 + padded;
        let mut message = Vec::with_capacity(20 + total);
        message.extend_from_slice(&0x0001u16.to_be_bytes()); // Binding Request
        message.extend_from_slice(&(total as u16).to_be_bytes());
        message.extend_from_slice(&MAGIC_COOKIE.to_be_bytes());
        message.extend_from_slice(&[transaction; 12]); // transaction id
        message.extend_from_slice(&ATTR_USERNAME.0.to_be_bytes());
        message.extend_from_slice(&(username.len() as u16).to_be_bytes());
        message.extend_from_slice(username);
        message.resize(20 + total, 0);
        message
    }

    fn localhost(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    /// Every datagram currently queued on `handle`, drained synchronously.
    ///
    /// The demux task is not involved — these tests drive `route_datagram` directly — so a
    /// no-op waker and repeated `poll_recv`s see exactly what was routed.
    fn drain(handle: &MuxSocketHandle) -> Vec<(usize, SocketAddr)> {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut out = Vec::new();
        loop {
            let mut buf = [0u8; 2048];
            let mut meta = [RecvMeta::default(); 1];
            let mut bufs = [IoSliceMut::new(&mut buf)];
            match handle.poll_recv(&mut cx, &mut bufs, &mut meta) {
                Poll::Ready(Ok(_)) => out.push((meta[0].len, meta[0].addr)),
                Poll::Ready(Err(err)) => panic!("poll_recv failed: {err}"),
                Poll::Pending => return out,
            }
        }
    }

    fn test_mux() -> Arc<UdpMux> {
        let runtime = crate::runtime::default_runtime().expect("runtime feature required");
        // Socket creation needs a live runtime context (`from_std` and friends), so enter
        // one for the bind; the mux keeps the runtime handle alive after, which keeps the
        // demux task's reactor running for the tests that rely on it.
        let mut out: Option<Arc<UdpMux>> = None;
        {
            let slot = &mut out;
            let rt = runtime.clone();
            runtime.block_on(Box::pin(async move {
                *slot = Some(UdpMux::bind_on(rt, localhost(0)).expect("bind"));
            }));
        }
        out.expect("mux bound")
    }

    #[test]
    fn ufrag_extraction_reads_username_local_part() {
        let request = stun_binding_request("localfrag:remotefrag", 1);
        assert_eq!(
            local_ufrag_from_stun_request(&request).as_deref(),
            Some("localfrag")
        );
    }

    #[test]
    fn ufrag_extraction_ignores_non_request_classes() {
        // A success response (0x0101) carrying the same username bytes is not a bootstrap
        // candidate: responses are routed by address, never by ufrag.
        let mut request = stun_binding_request("localfrag:remotefrag", 2);
        request[0] = 0x01;
        request[1] = 0x01;
        assert_eq!(local_ufrag_from_stun_request(&request), None);
    }

    #[test]
    fn ufrag_extraction_ignores_garbage() {
        assert_eq!(local_ufrag_from_stun_request(&[]), None);
        assert_eq!(local_ufrag_from_stun_request(&[0u8; 64]), None); // no magic cookie
    }

    #[test]
    fn routes_by_ufrag_then_pins_the_remote_address() {
        let mux = test_mux();
        let handle_a = Arc::clone(&mux).register("ufragA").expect("register A");
        let handle_b = Arc::clone(&mux).register("ufragB").expect("register B");

        let stranger = localhost(55_000);
        let dst = IpAddr::V4(Ipv4Addr::LOCALHOST);

        // An ICE check from an address nobody claimed: the ufrag in the username says B's.
        mux.route_datagram(
            stranger,
            &stun_binding_request("ufragB:remote", 3),
            dst,
            None,
        );
        assert_eq!(drain(&handle_b).len(), 1);
        assert!(drain(&handle_a).is_empty());

        // The follow-up traffic from the same peer — DTLS bytes, no username — is B's
        // without paying the STUN parse: the address was pinned by the check.
        mux.route_datagram(stranger, &[0x16, 0xfe, 0xfd, 0, 1, 2, 3], dst, None);
        let received = drain(&handle_b);
        assert_eq!(received, vec![(7, stranger)]);
        assert!(drain(&handle_a).is_empty());

        mux.close();
    }

    #[test]
    fn unrouted_datagrams_are_dropped_and_counted() {
        let mux = test_mux();
        let handle = Arc::clone(&mux).register("ufragA").expect("register");
        let stranger = localhost(55_001);
        let dst = IpAddr::V4(Ipv4Addr::LOCALHOST);

        // Not STUN, from nobody we know: dropped, and the mux — not the connection — is
        // charged, because no connection ever saw it.
        mux.route_datagram(stranger, &[9, 9, 9, 9], dst, None);
        assert_eq!(mux.unrouted_datagrams(), 1);
        assert!(drain(&handle).is_empty());

        // STUN, but for a ufrag nobody registered (a stale peer's check): same outcome.
        mux.route_datagram(
            stranger,
            &stun_binding_request("ghost:remote", 4),
            dst,
            None,
        );
        assert_eq!(mux.unrouted_datagrams(), 2);
        assert!(drain(&handle).is_empty());

        mux.close();
    }

    #[test]
    fn duplicate_live_registration_is_refused_but_replacement_after_drop_is_allowed() {
        let mux = test_mux();
        let handle = Arc::clone(&mux).register("ufrag").expect("register");
        assert!(Arc::clone(&mux).register("ufrag").is_err());

        // Drop = unregister (a restart's rebind, where the old handle goes away before the
        // new one registers the same ufrag again).
        drop(handle);
        let replacement = Arc::clone(&mux).register("ufrag").expect("re-register");
        drop(replacement);
        mux.close();
    }

    #[test]
    fn dropped_handle_unregisters_its_addresses() {
        let mux = test_mux();
        let handle = Arc::clone(&mux).register("ufragA").expect("register");
        let peer = localhost(55_002);

        // Claim the peer by sending to it (the send-side registration), then drop the handle.
        {
            let waker = noop_waker();
            let mut cx = Context::from_waker(&waker);
            let payload = [7u8; 16];
            let transmit = Transmit {
                destination: peer,
                ecn: None,
                contents: &payload,
                segment_size: None,
                src_ip: None,
            };
            let _ = handle.poll_send(&mut cx, &transmit);
            let entries = lock(&mux.state).by_addr.len();
            assert_eq!(entries, 1, "send must claim the destination");
        }
        drop(handle);

        // The stale claim must not wedge the port: a different connection claiming the same
        // peer (or nobody at all) is the correct successor, not the dropped one.
        let successor = Arc::clone(&mux).register("ufragB").expect("register B");
        let state = lock(&mux.state);
        let claimed_by_dropped = state
            .by_addr
            .values()
            .any(|weak| weak.strong_count() == 0);
        drop(state);
        assert!(
            !claimed_by_dropped,
            "drop must clear the address claim, not leave a dead registration"
        );
        drop(successor);
        mux.close();
    }
}
