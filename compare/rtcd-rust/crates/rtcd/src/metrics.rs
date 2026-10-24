//! Prometheus metrics, under the names the Go service exports so existing dashboards and alerts
//! keep working.

use std::time::Duration;

use prometheus::{
    CounterVec, Encoder, GaugeVec, HistogramOpts, HistogramVec, Opts, Registry, TextEncoder,
};
use rtcd_sfu::ids::TrackType;
use rtcd_sfu::metrics::{Direction, Metrics as SfuMetrics};

const NAMESPACE: &str = "rtcd";
const LATENCY_BUCKETS: [f64; 14] = [
    0.001, 0.005, 0.0075, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.3, 0.4, 0.5, 0.75, 1.0,
];
const LOSS_BUCKETS: [f64; 12] = [
    0.001, 0.005, 0.0075, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0,
];
/// Handshakes and signaling take longer than packet latencies: seconds, not milliseconds.
const SETUP_BUCKETS: [f64; 14] = [
    0.001, 0.005, 0.0075, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.3, 0.4, 0.5, 0.75, 1.0,
];

/// The service's metrics.
pub struct Metrics {
    registry: Registry,
    rtp_tracks: GaugeVec,
    rtp_bytes_sent: CounterVec,
    rtc_sessions: GaugeVec,
    conn_states: CounterVec,
    rtc_errors: CounterVec,
    connection_time: HistogramVec,
    dc_open_time: HistogramVec,
    lock_grab_time: HistogramVec,
    lock_held_time: HistogramVec,
    client_loss: HistogramVec,
    client_rtt: HistogramVec,
    client_jitter: HistogramVec,
    ws_connections: GaugeVec,
    ws_messages: CounterVec,
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Metrics")
    }
}

fn gauge(r: &Registry, subsystem: &str, name: &str, help: &str, labels: &[&str]) -> GaugeVec {
    let g = GaugeVec::new(
        Opts::new(name, help)
            .namespace(NAMESPACE)
            .subsystem(subsystem),
        labels,
    )
    .expect("valid metric");
    r.register(Box::new(g.clone())).expect("unique metric");
    g
}

fn counter(r: &Registry, subsystem: &str, name: &str, help: &str, labels: &[&str]) -> CounterVec {
    let c = CounterVec::new(
        Opts::new(name, help)
            .namespace(NAMESPACE)
            .subsystem(subsystem),
        labels,
    )
    .expect("valid metric");
    r.register(Box::new(c.clone())).expect("unique metric");
    c
}

fn histogram(
    r: &Registry,
    subsystem: &str,
    name: &str,
    help: &str,
    buckets: &[f64],
    labels: &[&str],
) -> HistogramVec {
    let h = HistogramVec::new(
        HistogramOpts::new(name, help)
            .namespace(NAMESPACE)
            .subsystem(subsystem)
            .buckets(buckets.to_vec()),
        labels,
    )
    .expect("valid metric");
    r.register(Box::new(h.clone())).expect("unique metric");
    h
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();
        #[cfg(target_os = "linux")]
        {
            // Process CPU, memory and file descriptors, prefixed like the Go process collector.
            let collector = prometheus::process_collector::ProcessCollector::for_self();
            let _ = registry.register(Box::new(collector));
        }
        let r = &registry;
        Self {
            rtp_tracks: gauge(
                r,
                "rtc",
                "rtp_tracks_total",
                "Total number of active RTP tracks",
                &["groupID", "direction", "type"],
            ),
            rtp_bytes_sent: counter(
                r,
                "rtc",
                "rtp_bytes_sent_total",
                "RTP bytes (header and payload) sent to receivers, by track type",
                &["type"],
            ),
            rtc_sessions: gauge(
                r,
                "rtc",
                "sessions_total",
                "Total number of active RTC sessions",
                &["groupID"],
            ),
            conn_states: counter(
                r,
                "rtc",
                "conn_states_total",
                "Total number of RTC connection state changes",
                &["type"],
            ),
            rtc_errors: counter(
                r,
                "rtc",
                "errors_total",
                "Total number of RTC related errors",
                &["groupID", "type"],
            ),
            connection_time: histogram(
                r,
                "rtc",
                "connection_time",
                "Time taken to establish the connection",
                &SETUP_BUCKETS,
                &["groupID"],
            ),
            dc_open_time: histogram(
                r,
                "rtc",
                "dc_open_time",
                "Time taken to open the data channel",
                &SETUP_BUCKETS,
                &["groupID"],
            ),
            lock_grab_time: histogram(
                r,
                "rtc",
                "signaling_lock_grab_time",
                "Time taken to acquire the signaling lock",
                &LATENCY_BUCKETS,
                &["groupID"],
            ),
            lock_held_time: histogram(
                r,
                "rtc",
                "signaling_lock_locked_time",
                "Time the signaling lock was held",
                &LATENCY_BUCKETS,
                &["groupID"],
            ),
            client_loss: histogram(
                r,
                "rtc_client",
                "loss_rate",
                "Client loss rate",
                &LOSS_BUCKETS,
                &["groupID"],
            ),
            client_rtt: histogram(
                r,
                "rtc_client",
                "rtt",
                "Client round trip time",
                &LATENCY_BUCKETS,
                &["groupID"],
            ),
            client_jitter: histogram(
                r,
                "rtc_client",
                "jitter",
                "Client latency jitter",
                &LATENCY_BUCKETS,
                &["groupID"],
            ),
            ws_connections: gauge(
                r,
                "ws",
                "connections_total",
                "Total number of active WebSocket sessions",
                &["clientID"],
            ),
            ws_messages: counter(
                r,
                "ws",
                "messages_total",
                "Total number of sent/received WebSocket messages",
                &["clientID", "type", "direction"],
            ),
            registry,
        }
    }

    pub fn ws_connected(&self, client: &str) {
        self.ws_connections.with_label_values(&[client]).inc();
    }

    pub fn ws_disconnected(&self, client: &str) {
        self.ws_connections.with_label_values(&[client]).dec();
    }

    pub fn ws_message(&self, client: &str, kind: &str, direction: &str) {
        self.ws_messages
            .with_label_values(&[client, kind, direction])
            .inc();
    }

    /// The metrics in the Prometheus text exposition format.
    pub fn render(&self) -> String {
        let mut out = Vec::new();
        if TextEncoder::new()
            .encode(&self.registry.gather(), &mut out)
            .is_err()
        {
            return String::new();
        }
        String::from_utf8(out).unwrap_or_default()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl SfuMetrics for Metrics {
    fn session_started(&self, group: &str) {
        self.rtc_sessions.with_label_values(&[group]).inc();
    }

    fn session_ended(&self, group: &str) {
        self.rtc_sessions.with_label_values(&[group]).dec();
    }

    fn connection_state(&self, state: &str) {
        self.conn_states.with_label_values(&[state]).inc();
    }

    fn error(&self, group: &str, kind: &str) {
        self.rtc_errors.with_label_values(&[group, kind]).inc();
    }

    fn track_added(&self, group: &str, direction: Direction, mime: &str) {
        self.rtp_tracks
            .with_label_values(&[group, direction.as_str(), mime])
            .inc();
    }

    fn track_removed(&self, group: &str, direction: Direction, mime: &str) {
        self.rtp_tracks
            .with_label_values(&[group, direction.as_str(), mime])
            .dec();
    }

    fn connection_time(&self, group: &str, took: Duration) {
        self.connection_time
            .with_label_values(&[group])
            .observe(took.as_secs_f64());
    }

    fn data_channel_open_time(&self, group: &str, took: Duration) {
        self.dc_open_time
            .with_label_values(&[group])
            .observe(took.as_secs_f64());
    }

    fn signaling_lock_grab_time(&self, group: &str, took: Duration) {
        self.lock_grab_time
            .with_label_values(&[group])
            .observe(took.as_secs_f64());
    }

    fn signaling_lock_held_time(&self, group: &str, took: Duration) {
        self.lock_held_time
            .with_label_values(&[group])
            .observe(took.as_secs_f64());
    }

    fn client_loss_rate(&self, group: &str, v: f64) {
        self.client_loss.with_label_values(&[group]).observe(v);
    }

    fn client_rtt(&self, group: &str, v: f64) {
        self.client_rtt.with_label_values(&[group]).observe(v);
    }

    fn client_jitter(&self, group: &str, v: f64) {
        self.client_jitter.with_label_values(&[group]).observe(v);
    }

    fn rtp_bytes_sent(&self, kind: TrackType, bytes: u64) {
        self.rtp_bytes_sent
            .with_label_values(&[kind.as_str()])
            .inc_by(bytes as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_names_are_the_ones_the_go_service_exports() {
        let m = Metrics::new();
        m.session_started("g");
        m.connection_state("connected");
        m.error("g", "rtp");
        m.track_added("g", Direction::In, "audio/opus");
        m.connection_time("g", Duration::from_millis(80));
        m.data_channel_open_time("g", Duration::from_millis(120));
        m.signaling_lock_grab_time("g", Duration::from_millis(2));
        m.signaling_lock_held_time("g", Duration::from_millis(40));
        m.client_loss_rate("g", 0.01);
        m.client_rtt("g", 0.05);
        m.client_jitter("g", 0.004);
        m.rtp_bytes_sent(TrackType::Voice, 1200);
        m.ws_connected("client");
        m.ws_message("client", "join", "in");
        let text = m.render();
        for name in [
            "rtcd_rtc_sessions_total{groupID=\"g\"} 1",
            "rtcd_rtc_conn_states_total{type=\"connected\"} 1",
            "rtcd_rtc_errors_total{groupID=\"g\",type=\"rtp\"} 1",
            "rtcd_rtc_rtp_tracks_total{direction=\"in\",groupID=\"g\",type=\"audio/opus\"} 1",
            "rtcd_rtc_connection_time_count{groupID=\"g\"} 1",
            "rtcd_rtc_dc_open_time_count",
            "rtcd_rtc_signaling_lock_grab_time_count",
            "rtcd_rtc_signaling_lock_locked_time_count",
            "rtcd_rtc_client_loss_rate_count",
            "rtcd_rtc_client_rtt_count",
            "rtcd_rtc_client_jitter_count",
            "rtcd_rtc_rtp_bytes_sent_total{type=\"voice\"} 1200",
            "rtcd_ws_connections_total{clientID=\"client\"} 1",
            "rtcd_ws_messages_total{clientID=\"client\",direction=\"in\",type=\"join\"} 1",
        ] {
            assert!(text.contains(name), "missing {name} in\n{text}");
        }
    }

    #[test]
    fn gauges_come_back_down() {
        let m = Metrics::new();
        m.session_started("g");
        m.session_ended("g");
        m.ws_connected("c");
        m.ws_disconnected("c");
        let text = m.render();
        assert!(text.contains("rtcd_rtc_sessions_total{groupID=\"g\"} 0"));
        assert!(text.contains("rtcd_ws_connections_total{clientID=\"c\"} 0"));
    }
}
