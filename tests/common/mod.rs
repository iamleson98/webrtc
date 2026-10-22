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
    use socket2::{Domain, Protocol, Socket, Type};
    use std::net::{Ipv4Addr, SocketAddr};
    let group = Ipv4Addr::new(224, 0, 0, 251);
    // Exactly what multicast DNS does: the well-known port, shared with whoever else listens on it,
    // joined on the loopback interface.
    let probe = || -> std::io::Result<bool> {
        let rx = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        rx.set_reuse_address(true)?;
        #[cfg(all(unix, not(any(target_os = "solaris", target_os = "illumos"))))]
        rx.set_reuse_port(true)?;
        rx.bind(&SocketAddr::from((Ipv4Addr::UNSPECIFIED, 5353)).into())?;
        rx.join_multicast_v4(&group, &Ipv4Addr::LOCALHOST)?;
        rx.set_read_timeout(Some(std::time::Duration::from_secs(2)))?;

        let tx = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        tx.bind(&SocketAddr::from((Ipv4Addr::LOCALHOST, 0)).into())?;
        tx.set_multicast_if_v4(&Ipv4Addr::LOCALHOST)?;
        tx.set_multicast_loop_v4(true)?;
        tx.send_to(b"probe", &SocketAddr::from((group, 5353)).into())?;

        // Anything else listening on 5353 may be heard too; only our own datagram counts.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        let mut buf = [std::mem::MaybeUninit::<u8>::uninit(); 512];
        while std::time::Instant::now() < deadline {
            match rx.recv_from(&mut buf) {
                Ok((n, _)) => {
                    let got: Vec<u8> = buf[..n]
                        .iter()
                        .map(|b| unsafe { b.assume_init() })
                        .collect();
                    if got == b"probe" {
                        return Ok(true);
                    }
                }
                Err(_) => return Ok(false),
            }
        }
        Ok(false)
    };
    probe().unwrap_or(false)
}
