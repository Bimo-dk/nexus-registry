use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::{MatchedPath, Request, State},
    middleware::Next,
    response::Response,
};
use parking_lot::RwLock;
use serde::Serialize;

use crate::state::AppState;
use crate::time::iso_now;

#[derive(Default, Debug, Clone)]
pub struct RouteStats {
    pub count: u64,
    pub errors: u64,
    pub total_duration_ms: u64,
    pub last_duration_ms: u64,
    pub min_duration_ms: u64,
    pub max_duration_ms: u64,
    pub by_status: HashMap<String, u64>,
}

#[derive(Default)]
struct Inner {
    routes: HashMap<String, RouteStats>,
    counters: HashMap<String, u64>,
}

pub struct Metrics {
    inner: RwLock<Inner>,
    started_at: Instant,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(Inner::default()),
            started_at: Instant::now(),
        })
    }

    pub fn observe(&self, route: &str, status: u16, dur_ms: u64) {
        let mut inner = self.inner.write();
        let entry = inner.routes.entry(route.to_string()).or_default();
        entry.count += 1;
        entry.total_duration_ms += dur_ms;
        entry.last_duration_ms = dur_ms;
        entry.min_duration_ms = if entry.count == 1 {
            dur_ms
        } else {
            entry.min_duration_ms.min(dur_ms)
        };
        entry.max_duration_ms = entry.max_duration_ms.max(dur_ms);
        *entry.by_status.entry(status.to_string()).or_insert(0) += 1;
        if status >= 400 {
            entry.errors += 1;
        }
    }

    #[allow(dead_code)]
    pub fn increment(&self, name: &str, by: u64) {
        let mut inner = self.inner.write();
        *inner.counters.entry(name.to_string()).or_insert(0) += by;
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let inner = self.inner.read();
        let mut routes: Vec<RouteEntry> = inner
            .routes
            .iter()
            .map(|(route, s)| RouteEntry {
                route: route.clone(),
                count: s.count,
                errors: s.errors,
                total_duration_ms: s.total_duration_ms,
                last_duration_ms: s.last_duration_ms,
                min_duration_ms: s.min_duration_ms,
                max_duration_ms: s.max_duration_ms,
                avg_duration_ms: if s.count > 0 {
                    s.total_duration_ms as f64 / s.count as f64
                } else {
                    0.0
                },
                by_status: s.by_status.clone(),
            })
            .collect();
        routes.sort_by_key(|r| std::cmp::Reverse(r.count));

        MetricsSnapshot {
            timestamp: iso_now(),
            uptime_sec: self.started_at.elapsed().as_secs(),
            routes,
            counters: inner.counters.clone(),
            process: ProcessInfo {
                node_version: format!("rust-{}", env!("CARGO_PKG_VERSION")),
            },
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RouteEntry {
    pub route: String,
    pub count: u64,
    pub errors: u64,
    pub total_duration_ms: u64,
    pub last_duration_ms: u64,
    pub min_duration_ms: u64,
    pub max_duration_ms: u64,
    pub avg_duration_ms: f64,
    pub by_status: HashMap<String, u64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessInfo {
    pub node_version: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricsSnapshot {
    pub timestamp: String,
    pub uptime_sec: u64,
    pub routes: Vec<RouteEntry>,
    pub counters: HashMap<String, u64>,
    pub process: ProcessInfo,
}

pub async fn middleware(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let start = Instant::now();
    let method = req.method().clone();
    // Use a fixed bucket for unmatched routes — prevents unbounded HashMap growth
    // when arbitrary paths hit the 404 fallback.
    let route_template = req
        .extensions()
        .get::<MatchedPath>()
        .map(|m| m.as_str().to_string())
        .unwrap_or_else(|| "<not_matched>".to_string());

    let res = next.run(req).await;
    let dur_ms = start.elapsed().as_millis() as u64;
    let key = format!("{} {}", method, route_template);
    state.metrics.observe(&key, res.status().as_u16(), dur_ms);
    crate::features::metrics::record_http_request(
        method.as_str(),
        &route_template,
        res.status().as_u16(),
        dur_ms as f64,
    );
    res
}
