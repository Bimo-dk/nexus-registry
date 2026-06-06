use std::sync::Arc;

use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};
use chrono::Utc;
use once_cell::sync::OnceCell;
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry, TextEncoder,
};

use crate::state::AppState;

pub struct PromMetrics {
    pub registry: Registry,
    pub remotes_total: IntGaugeVec,
    pub ws_clients_connected: IntGauge,
    pub ws_messages_sent_total: IntCounterVec,
    pub http_requests_total: IntCounterVec,
    pub http_request_duration_ms: HistogramVec,
    pub health_check_duration_ms: HistogramVec,
    pub health_check_status: IntGaugeVec,
    pub circuit_breaker_state: IntGaugeVec,
    pub rate_limit_rejected_total: IntCounterVec,
}

static METRICS: OnceCell<Arc<PromMetrics>> = OnceCell::new();

const HTTP_BUCKETS: &[f64] = &[1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0];
const HEALTH_BUCKETS: &[f64] = &[5.0, 25.0, 100.0, 500.0, 1000.0, 1500.0, 3000.0];

pub fn init() -> Arc<PromMetrics> {
    METRICS
        .get_or_init(|| {
            let registry = Registry::new();
            let remotes_total = IntGaugeVec::new(
                Opts::new("nexus_registry_remotes_total", "Number of remotes"),
                &["enabled"],
            )
            .unwrap();
            let ws_clients_connected = IntGauge::new(
                "nexus_registry_ws_clients_connected",
                "Number of connected WS clients",
            )
            .unwrap();
            let ws_messages_sent_total = IntCounterVec::new(
                Opts::new("nexus_registry_ws_messages_sent_total", "WS messages sent"),
                &["type"],
            )
            .unwrap();
            let http_requests_total = IntCounterVec::new(
                Opts::new("nexus_registry_http_requests_total", "HTTP requests"),
                &["method", "path", "status"],
            )
            .unwrap();
            let http_request_duration_ms = HistogramVec::new(
                HistogramOpts::new("nexus_registry_http_request_duration_ms", "HTTP request duration")
                    .buckets(HTTP_BUCKETS.to_vec()),
                &["method", "path"],
            )
            .unwrap();
            let health_check_duration_ms = HistogramVec::new(
                HistogramOpts::new("nexus_registry_health_check_duration_ms", "Health-check duration")
                    .buckets(HEALTH_BUCKETS.to_vec()),
                &["remote"],
            )
            .unwrap();
            let health_check_status = IntGaugeVec::new(
                Opts::new(
                    "nexus_registry_health_check_status",
                    "Health-check status (1 active, 0 inactive)",
                ),
                &["remote", "status"],
            )
            .unwrap();
            let circuit_breaker_state = IntGaugeVec::new(
                Opts::new(
                    "nexus_registry_circuit_breaker_state",
                    "Circuit breaker state (1 active, 0 inactive)",
                ),
                &["remote", "state"],
            )
            .unwrap();
            let rate_limit_rejected_total = IntCounterVec::new(
                Opts::new(
                    "nexus_registry_rate_limit_rejected_total",
                    "Requests rejected by the rate limiter",
                ),
                &["by"],
            )
            .unwrap();

            registry.register(Box::new(remotes_total.clone())).unwrap();
            registry.register(Box::new(ws_clients_connected.clone())).unwrap();
            registry
                .register(Box::new(ws_messages_sent_total.clone()))
                .unwrap();
            registry.register(Box::new(http_requests_total.clone())).unwrap();
            registry
                .register(Box::new(http_request_duration_ms.clone()))
                .unwrap();
            registry
                .register(Box::new(health_check_duration_ms.clone()))
                .unwrap();
            registry.register(Box::new(health_check_status.clone())).unwrap();
            registry
                .register(Box::new(circuit_breaker_state.clone()))
                .unwrap();
            registry
                .register(Box::new(rate_limit_rejected_total.clone()))
                .unwrap();

            Arc::new(PromMetrics {
                registry,
                remotes_total,
                ws_clients_connected,
                ws_messages_sent_total,
                http_requests_total,
                http_request_duration_ms,
                health_check_duration_ms,
                health_check_status,
                circuit_breaker_state,
                rate_limit_rejected_total,
            })
        })
        .clone()
}

#[allow(dead_code)]
pub fn get() -> Option<Arc<PromMetrics>> {
    METRICS.get().cloned()
}

pub fn encode(custom_labels: &std::collections::BTreeMap<String, String>) -> String {
    let Some(m) = METRICS.get() else {
        return String::new();
    };
    let families = m.registry.gather();
    let encoder = TextEncoder::new();
    let mut buf = Vec::new();
    let _ = encoder.encode(&families, &mut buf);
    let raw = String::from_utf8(buf).unwrap_or_default();
    if custom_labels.is_empty() {
        raw
    } else {
        apply_custom_labels(&raw, custom_labels)
    }
}

/// Splice the `custom_labels` map into every metric line of a Prometheus
/// text-format payload. Lines beginning with `#` (HELP/TYPE) and empty lines
/// are passed through untouched.
fn apply_custom_labels(text: &str, labels: &std::collections::BTreeMap<String, String>) -> String {
    let extras: String = labels
        .iter()
        .map(|(k, v)| format!("{}=\"{}\"", k, v.replace('"', "\\\"")))
        .collect::<Vec<_>>()
        .join(",");

    let mut out = String::with_capacity(text.len() + extras.len() * 16);
    for line in text.split_inclusive('\n') {
        let trimmed = line.trim_end_matches(['\n', '\r']);
        if trimmed.is_empty() || trimmed.starts_with('#') {
            out.push_str(line);
            continue;
        }
        if let Some(brace_pos) = trimmed.find('{') {
            // metric{a="b"} 42  →  metric{a="b",extras} 42
            let (left, rest) = trimmed.split_at(brace_pos + 1);
            let inside_close = rest.find('}').unwrap_or(rest.len());
            let (inside, tail) = rest.split_at(inside_close);
            let needs_comma = !inside.is_empty();
            out.push_str(left);
            out.push_str(inside);
            if needs_comma {
                out.push(',');
            }
            out.push_str(&extras);
            out.push_str(tail);
            if line.ends_with('\n') {
                out.push('\n');
            }
        } else if let Some(space_pos) = trimmed.find(' ') {
            // metric 42  →  metric{extras} 42
            let (name, value) = trimmed.split_at(space_pos);
            out.push_str(name);
            out.push('{');
            out.push_str(&extras);
            out.push('}');
            out.push_str(value);
            if line.ends_with('\n') {
                out.push('\n');
            }
        } else {
            out.push_str(line);
        }
    }
    out
}

pub fn record_rate_limit_rejected(by: &str) {
    if let Some(m) = METRICS.get() {
        m.rate_limit_rejected_total.with_label_values(&[by]).inc();
    }
}

pub fn record_http_request(method: &str, path: &str, status: u16, duration_ms: f64) {
    if let Some(m) = METRICS.get() {
        let status_s = status.to_string();
        m.http_requests_total
            .with_label_values(&[method, path, &status_s])
            .inc();
        m.http_request_duration_ms
            .with_label_values(&[method, path])
            .observe(duration_ms);
    }
}

pub fn record_health_check(remote: &str, duration_ms: f64, status: &str) {
    if let Some(m) = METRICS.get() {
        m.health_check_duration_ms
            .with_label_values(&[remote])
            .observe(duration_ms);
        for s in ["healthy", "degraded", "down", "unknown"] {
            let v: i64 = if s == status { 1 } else { 0 };
            m.health_check_status.with_label_values(&[remote, s]).set(v);
        }
    }
}

pub fn record_circuit_state(remote: &str, state: &str) {
    if let Some(m) = METRICS.get() {
        for s in ["closed", "open", "half_open"] {
            let v: i64 = if s == state { 1 } else { 0 };
            m.circuit_breaker_state.with_label_values(&[remote, s]).set(v);
        }
    }
}

pub fn set_ws_clients(count: usize) {
    if let Some(m) = METRICS.get() {
        m.ws_clients_connected.set(count as i64);
    }
}

pub fn record_ws_message(message_type: &str) {
    if let Some(m) = METRICS.get() {
        m.ws_messages_sent_total.with_label_values(&[message_type]).inc();
    }
}

pub fn set_remotes_count(enabled: usize, disabled: usize) {
    if let Some(m) = METRICS.get() {
        m.remotes_total.with_label_values(&["true"]).set(enabled as i64);
        m.remotes_total.with_label_values(&["false"]).set(disabled as i64);
    }
}

/// Middleware that intercepts the dynamically-configured Prometheus scrape path.
/// Runs as one of the outermost layers so it fires for paths that aren't otherwise
/// registered as routes (e.g. `/metrics` when no static route exists for it).
pub async fn scrape_middleware(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let cfg = state.config_store.metrics();
    if !cfg.prometheus_enabled || req.uri().path() != cfg.prometheus_path {
        return next.run(req).await;
    }
    if cfg.require_auth {
        let presented = req
            .headers()
            .get("x-nexus-token")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let stored = state.config_store.token();
        let ok = crate::features::token::verify_token(
            stored.as_ref(),
            presented,
            &state.env.nexus_token_pepper,
            Utc::now(),
        );
        if !ok {
            return (StatusCode::UNAUTHORIZED, "Missing or invalid X-Nexus-Token").into_response();
        }
    }
    let body = encode(&cfg.custom_labels);
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
        .into_response()
}
