//! What the media plane reports. The service implements this over Prometheus; the plane itself only
//! calls it, so it carries no metrics dependency and tests can count calls.

use std::time::Duration;

use crate::ids::TrackType;

/// Direction of a track as seen by the SFU.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// From a participant to the SFU.
    In,
    /// From the SFU to a participant.
    Out,
}

impl Direction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::In => "in",
            Self::Out => "out",
        }
    }
}

/// Sink for the media plane's measurements. Every method must be cheap and must not block: they are
/// called from the forwarding loop.
pub trait Metrics: Send + Sync + 'static {
    fn session_started(&self, group: &str) {
        let _ = group;
    }
    fn session_ended(&self, group: &str) {
        let _ = group;
    }
    /// A connection state change: `connected`, `disconnected`, `failed` or `closed`.
    fn connection_state(&self, state: &str) {
        let _ = state;
    }
    fn error(&self, group: &str, kind: &str) {
        let _ = (group, kind);
    }
    fn track_added(&self, group: &str, direction: Direction, mime: &str) {
        let _ = (group, direction, mime);
    }
    fn track_removed(&self, group: &str, direction: Direction, mime: &str) {
        let _ = (group, direction, mime);
    }
    fn connection_time(&self, group: &str, took: Duration) {
        let _ = (group, took);
    }
    fn data_channel_open_time(&self, group: &str, took: Duration) {
        let _ = (group, took);
    }
    fn signaling_lock_grab_time(&self, group: &str, took: Duration) {
        let _ = (group, took);
    }
    fn signaling_lock_held_time(&self, group: &str, took: Duration) {
        let _ = (group, took);
    }
    fn client_loss_rate(&self, group: &str, v: f64) {
        let _ = (group, v);
    }
    fn client_rtt(&self, group: &str, v: f64) {
        let _ = (group, v);
    }
    fn client_jitter(&self, group: &str, v: f64) {
        let _ = (group, v);
    }
    /// RTP bytes (header and payload) sent to receivers.
    fn rtp_bytes_sent(&self, kind: TrackType, bytes: u64) {
        let _ = (kind, bytes);
    }
}

/// Discards everything.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoMetrics;

impl Metrics for NoMetrics {}
