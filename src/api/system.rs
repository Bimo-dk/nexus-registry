use std::collections::HashMap;

use axum::{
    extract::{Query, State},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;

use axum::{http::StatusCode, routing::post};

use crate::observability::log_buffer::LogLevel;
use crate::state::AppState;
use crate::system_health::run_cycle;
use crate::ws::connection_count;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/health", get(health))
        .route("/config", get(config))
        .route("/logs", get(logs))
        .route("/metrics", get(metrics))
        .route("/shutdown", post(shutdown))
}

async fn shutdown(State(state): State<AppState>) -> Response {
    state.shutdown.trigger();
    (
        StatusCode::ACCEPTED,
        Json(json!({
            "accepted": true,
            "message": "shutdown sequence initiated",
        })),
    )
        .into_response()
}

#[derive(Deserialize)]
struct HealthQuery {
    fresh: Option<String>,
}

async fn health(State(state): State<AppState>, Query(q): Query<HealthQuery>) -> Response {
    if q.fresh.as_deref() == Some("true") {
        let snapshot = run_cycle(&state).await;
        return Json(snapshot).into_response();
    }
    if let Some(cached) = state.health_cache.read().clone() {
        return Json(cached).into_response();
    }
    let snapshot = run_cycle(&state).await;
    Json(snapshot).into_response()
}

async fn config(State(state): State<AppState>) -> Response {
    let c = &state.env;
    let allowed_origins: Vec<String> = if c.allowed_origins.is_empty() {
        vec!["*".to_string()]
    } else {
        c.allowed_origins.clone()
    };
    let system_services: Vec<String> = c.system_services.iter().map(|s| s.name.clone()).collect();

    Json(json!({
        "nodeEnv": c.node_env,
        "bindAddress": c.bind_address,
        "port": c.port,
        "databaseUrl": c.database_url,
        "dataDir": c.data_dir.display().to_string(),
        "healthCheckIntervalMs": c.health_interval_ms,
        "logBufferCapacity": c.log_buffer_capacity,
        "allowedOrigins": allowed_origins,
        "systemServices": system_services,
        "nexusTokenConfigured": !c.nexus_token.is_empty(),
        "wsClients": connection_count(),
        "nodeVersion": format!("rust-{}", env!("CARGO_PKG_VERSION")),
        "uptimeSec": state.started_at.elapsed().as_secs(),
    }))
    .into_response()
}

#[derive(Deserialize)]
struct LogsQuery {
    since: Option<String>,
    limit: Option<String>,
    level: Option<String>,
}

async fn logs(State(state): State<AppState>, Query(q): Query<LogsQuery>) -> Response {
    let limit = q
        .limit
        .as_deref()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(100)
        .clamp(1, state.log_buffer.capacity());
    let level = q.level.as_deref().and_then(LogLevel::from_str);
    let entries = state.log_buffer.snapshot(q.since.as_deref(), level, limit);
    Json(json!({ "entries": entries })).into_response()
}

async fn metrics(State(state): State<AppState>) -> Response {
    let mut snapshot = state.metrics.snapshot();
    let (capacity, size, total_appended) = state.log_buffer.stats();
    let mut extra = HashMap::new();
    extra.insert("logBufferCapacity".to_string(), capacity as u64);
    extra.insert("logBufferSize".to_string(), size as u64);
    extra.insert("logBufferTotalAppended".to_string(), total_appended);
    extra.insert("dbPoolSize".to_string(), state.db.size() as u64);
    extra.insert("dbPoolIdle".to_string(), state.db.num_idle() as u64);
    snapshot.counters.extend(extra);
    Json(snapshot).into_response()
}
