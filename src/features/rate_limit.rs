use std::net::IpAddr;
use std::num::NonZeroU32;
use std::sync::Arc;

use std::net::SocketAddr;

use axum::{
    extract::{ConnectInfo, Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use governor::{
    clock::{DefaultClock, QuantaInstant},
    middleware::NoOpMiddleware,
    state::keyed::DashMapStateStore,
    Quota, RateLimiter,
};
use parking_lot::RwLock;
use serde_json::json;

use crate::config::types::RateLimitingConfig;
use crate::correlation::CorrelationId;
use crate::state::AppState;

type Limiter = RateLimiter<String, DashMapStateStore<String>, DefaultClock, NoOpMiddleware<QuantaInstant>>;

pub struct RateLimitState {
    limiter: RwLock<Arc<Limiter>>,
}

impl RateLimitState {
    pub fn new(cfg: &RateLimitingConfig) -> Arc<Self> {
        Arc::new(Self {
            limiter: RwLock::new(Arc::new(build_limiter(cfg))),
        })
    }

    pub fn rebuild(&self, cfg: &RateLimitingConfig) {
        *self.limiter.write() = Arc::new(build_limiter(cfg));
    }

    fn current(&self) -> Arc<Limiter> {
        self.limiter.read().clone()
    }
}

fn build_limiter(cfg: &RateLimitingConfig) -> Limiter {
    let rps = NonZeroU32::new(cfg.requests_per_second.max(1)).unwrap();
    let burst = NonZeroU32::new(cfg.burst_size.max(1)).unwrap();
    let quota = Quota::per_second(rps).allow_burst(burst);
    RateLimiter::keyed(quota)
}

pub async fn middleware(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let cfg = state.config_store.rate_limiting();
    if !cfg.enabled {
        return next.run(req).await;
    }

    let key = match cfg.by.as_str() {
        "ip" => extract_client_ip(&req).unwrap_or_else(|| "unknown".into()),
        "token" => extract_token(&req).unwrap_or_else(|| "anonymous".into()),
        _ => return next.run(req).await,
    };

    let limiter = state.rate_limit.current();
    if limiter.check_key(&key).is_err() {
        crate::features::metrics::record_rate_limit_rejected(&cfg.by);
        let cid = req
            .extensions()
            .get::<CorrelationId>()
            .map(|c| c.as_str().to_string())
            .unwrap_or_default();
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({
                "error": "rate_limit_exceeded",
                "message": "Too many requests",
                "correlationId": cid,
            })),
        )
            .into_response();
    }
    next.run(req).await
}

fn extract_client_ip(req: &Request) -> Option<String> {
    if let Some(fwd) = req.headers().get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
        if let Some(last) = fwd.split(',').next_back() {
            let trimmed = last.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    if let Some(real) = req.headers().get("x-real-ip").and_then(|v| v.to_str().ok()) {
        if !real.is_empty() {
            return Some(real.to_string());
        }
    }
    req.extensions().get::<ConnectInfo<SocketAddr>>().map(|ci| {
        let ip: IpAddr = ci.0.ip();
        ip.to_string()
    })
}

fn extract_token(req: &Request) -> Option<String> {
    req.headers()
        .get("x-nexus-token")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}
