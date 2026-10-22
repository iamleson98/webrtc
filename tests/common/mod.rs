//! Shared helpers for the integration tests.
//!
//! The test suite is deliberately runtime-agnostic: the same sources run under
//! `runtime-tokio` and under `runtime-smol`, selected by Cargo feature. These helpers
//! resolve [`webrtc::runtime::default_runtime`] — the compiled-in built-in — so a test
//! body does not have to thread a handle through every call.
//!
//! This lives in the test tree on purpose. The library itself has **no** ambient runtime:
//! every internal call site takes an explicit `&dyn Runtime`, and a `PeerConnection`'s
//! runtime is injected per connection via `with_runtime`. Tests that exercise a specific
//! runtime (including `MockRuntime`) should build it themselves and call its methods
//! directly rather than using these helpers.

#![allow(dead_code)]

use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use webrtc::runtime::{Elapsed, Runtime, default_runtime};

/// The one runtime instance shared by every helper in this module.
///
/// `default_runtime()` *constructs* a runtime on each call, so resolving it per operation
/// would allocate a fresh `Arc<dyn Runtime>` for every `sleep` / `timeout` / `interval` /
/// `yield_now`. Beyond the waste, that is semantically wrong: it hands out a distinct
/// runtime each time, which is harmless for the stateless built-ins but would silently
/// break any runtime carrying state (a `MockRuntime` would get a different virtual clock
/// per call). Resolve once, then clone the handle.
static RUNTIME: OnceLock<Arc<dyn Runtime>> = OnceLock::new();

/// The runtime under test, i.e. whichever built-in the enabled feature provides.
///
/// Cheap to call repeatedly: clones a shared `Arc` rather than building a new runtime.
/// **Every test and example should obtain its runtime from here** rather than calling
/// `default_runtime()` directly, so a single process-wide instance backs both the timer
/// helpers and each `PeerConnection`.
pub fn runtime() -> Arc<dyn Runtime> {
    Arc::clone(RUNTIME.get_or_init(|| {
        default_runtime().expect("no runtime feature enabled (runtime-tokio or runtime-smol)")
    }))
}

/// Drive `future` to completion on the runtime under test, returning its output.
///
/// [`Runtime::block_on`] is restricted to a `()` output so the trait stays object-safe;
/// this helper recovers the generic form for tests by moving the value out through a
/// local slot, as the trait documentation suggests.
pub fn block_on<F, T>(future: F) -> T
where
    F: Future<Output = T>,
{
    let mut out: Option<T> = None;
    {
        let slot = &mut out;
        runtime().block_on(Box::pin(async move {
            *slot = Some(future.await);
        }));
    }
    out.expect("block_on future did not run to completion")
}

/// Sleep on the runtime under test.
pub async fn sleep(duration: Duration) {
    runtime().sleep(duration).await;
}

/// Cooperatively yield on the runtime under test.
pub async fn yield_now() {
    runtime().yield_now().await;
}

/// Run `future` with a deadline on the runtime under test.
pub async fn timeout<T>(duration: Duration, future: impl Future<Output = T>) -> Result<T, Elapsed> {
    webrtc::runtime::timeout(&*runtime(), duration, future).await
}

/// A repeating timer on the runtime under test.
pub fn interval(period: Duration) -> Box<dyn webrtc::runtime::AsyncInterval> {
    runtime().interval(period)
}

/// Whether this machine delivers an IPv4 multicast datagram to a listener on the loopback
/// interface, which is what multicast DNS needs to resolve a peer running on the same host.
///
/// Virtualised CI hosts (notably macOS runners) can send multicast without ever delivering it back;
/// a test of mDNS there would time out for a reason that has nothing to do with the code under test.
/// Tests that need it call this first and say so when they skip.
#[allow(dead_code)]
pub fn multicast_loopback_works() -> bool {
    use std::net::{Ipv4Addr, UdpSocket};
    let group = Ipv4Addr::new(224, 0, 0, 251);
    let probe = || -> std::io::Result<bool> {
        let rx = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))?;
        rx.join_multicast_v4(&group, &Ipv4Addr::LOCALHOST)?;
        rx.set_read_timeout(Some(std::time::Duration::from_secs(2)))?;
        let port = rx.local_addr()?.port();
        let tx = socket2::Socket::new(
            socket2::Domain::IPV4,
            socket2::Type::DGRAM,
            Some(socket2::Protocol::UDP),
        )?;
        tx.bind(&std::net::SocketAddr::from((Ipv4Addr::LOCALHOST, 0)).into())?;
        tx.set_multicast_if_v4(&Ipv4Addr::LOCALHOST)?;
        tx.set_multicast_loop_v4(true)?;
        tx.send_to(b"probe", &std::net::SocketAddr::from((group, port)).into())?;
        let mut buf = [0u8; 16];
        Ok(rx.recv_from(&mut buf).is_ok())
    };
    probe().unwrap_or(false)
}
