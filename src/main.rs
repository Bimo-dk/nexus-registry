mod api;
mod config;
mod correlation;
mod features;
mod http_error;
mod observability;
mod state;
mod store;
mod system_health;
mod time;
mod types;
mod validators;
mod ws;

#[cfg(test)]
mod tests;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::{DefaultBodyLimit, Extension, Request, State},
    http::{HeaderName, HeaderValue, Method, StatusCode},
    middleware,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use parking_lot::RwLock;
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::trace::{DefaultOnRequest, DefaultOnResponse, MakeSpan, TraceLayer};
use tracing::{info, warn, Level, Span};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::config::store::ConfigStore;
use crate::config::{DatabaseConfig, EnvConfig};
use crate::correlation::CorrelationId;
use crate::features::circuit::CircuitBreakerRegistry;
use crate::features::rate_limit::RateLimitState;
use crate::features::shutdown::ShutdownController;
use crate::observability::log_buffer::{LogBuffer, RingBufferLayer};
use crate::observability::metrics::Metrics;
use crate::state::AppState;
use crate::ws::connection_count;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env = EnvConfig::from_env();

    let log_buffer = LogBuffer::new(env.log_buffer_capacity);
    init_tracing(log_buffer.clone());

    if env.nexus_token.is_empty() {
        warn!("[auth] NEXUS_TOKEN is not set — no active token will be bootstrapped, all authenticated endpoints will reject all requests until /api/config/token/rotate is called");
    }
    if env.nexus_token_pepper == "nexus-registry-default-pepper" {
        warn!("[auth] NEXUS_TOKEN_PEPPER is not set — using the default pepper, set this in production");
    }

    let db_cfg = DatabaseConfig::resolve(&env, &env.data_dir)
        .map_err(|e| format!("[registry] database configuration error: {e}"))?;
    info!("[registry] Connecting to {} database", db_cfg.dialect.as_str());
    let db = store::init(&db_cfg, &env.data_dir).await?;
    let initial = store::list(&db).await?;
    info!(
        "[registry] Loaded {} remote(s) from {} ({})",
        initial.len(),
        db_cfg.dialect.as_str(),
        sanitise_db_url(&db_cfg.url)
    );

    let config_store = ConfigStore::hydrate(db.clone()).await?;
    features::token::init_from_env(&config_store, &env.nexus_token, &env.nexus_token_pepper).await?;
    features::metrics::init();

    let circuit_breaker = CircuitBreakerRegistry::new(config_store.clone());
    let rate_limit = RateLimitState::new(&config_store.rate_limiting());
    let shutdown = ShutdownController::new();

    let (broadcast_tx, _) = broadcast::channel(256);

    let state = AppState {
        db: db.clone(),
        env: Arc::new(env.clone()),
        config_store: config_store.clone(),
        circuit_breaker: circuit_breaker.clone(),
        rate_limit: rate_limit.clone(),
        shutdown: shutdown.clone(),
        metrics: Metrics::new(),
        log_buffer,
        broadcast_tx: broadcast_tx.clone(),
        health_cache: Arc::new(RwLock::new(None)),
        started_at: Arc::new(Instant::now()),
    };

    features::token::start_expiry_loop(config_store.clone(), broadcast_tx.clone());
    shutdown
        .clone()
        .spawn_orchestrator(config_store.clone(), broadcast_tx.clone());

    spawn_signal_listener(shutdown.clone());

    let cors = build_cors(&env);
    let app = build_router(state.clone(), cors);

    system_health::start_loop(state.clone());

    let ip: std::net::IpAddr = env.bind_address.parse().map_err(|e| {
        format!(
            "BIND_ADDRESS \"{}\" is not a valid IP address: {}",
            env.bind_address, e
        )
    })?;
    let addr = SocketAddr::from((ip, env.port));
    let listener = TcpListener::bind(addr).await?;
    info!("[registry] Listening on http://{}:{}", env.bind_address, env.port);
    info!(
        "[registry] WebSocket on ws://{}:{}/api/ws",
        env.bind_address, env.port
    );
    let origins_label = if env.allowed_origins.is_empty() {
        "(any)".to_string()
    } else {
        env.allowed_origins.join(", ")
    };
    info!("[registry] Allowed CORS origins: {}", origins_label);

    let shutdown_clone = shutdown.clone();
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
        .with_graceful_shutdown(async move { shutdown_clone.wait_for_drain().await })
        .await?;

    info!(step = 5, "[shutdown] closing DB pool");
    db.close().await;
    info!(step = 6, "[shutdown] exiting cleanly");
    Ok(())
}

fn spawn_signal_listener(shutdown: Arc<ShutdownController>) {
    tokio::spawn(async move {
        let ctrl_c = async {
            let _ = tokio::signal::ctrl_c().await;
        };

        #[cfg(unix)]
        let terminate = async {
            if let Ok(mut sig) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                sig.recv().await;
            }
        };

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => info!("[registry] Received SIGINT"),
            _ = terminate => info!("[registry] Received SIGTERM"),
        }
        shutdown.trigger();
    });
}

fn init_tracing(buffer: std::sync::Arc<LogBuffer>) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_ansi(false);
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .with(RingBufferLayer::new(buffer))
        .init();
}

pub fn build_router(state: AppState, cors: CorsLayer) -> Router {
    let api_router = Router::new()
        .nest("/remotes", api::remotes::router())
        .nest("/hosts", api::hosts::router())
        .nest("/gates", api::gates::router())
        .nest("/system", api::system::router())
        .nest("/config", config::routes::router())
        .layer(middleware::from_fn_with_state(
            state.clone(),
            features::token::middleware,
        ));

    let trace_layer = TraceLayer::new_for_http()
        .make_span_with(CorrelationSpan)
        .on_request(DefaultOnRequest::new().level(Level::INFO))
        .on_response(DefaultOnResponse::new().level(Level::INFO));

    Router::new()
        .route("/health", get(public_health))
        .route("/api/ws", get(ws::upgrade))
        .nest("/api", api_router)
        .fallback(not_found)
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            features::rate_limit::middleware,
        ))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            observability::metrics::middleware,
        ))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            features::metrics::scrape_middleware,
        ))
        .layer(trace_layer)
        .layer(middleware::from_fn(correlation::middleware))
        .layer(cors)
        .with_state(state)
}

fn build_cors(env: &EnvConfig) -> CorsLayer {
    let methods = [
        Method::GET,
        Method::POST,
        Method::PUT,
        Method::DELETE,
        Method::OPTIONS,
    ];
    let allowed_headers = [
        HeaderName::from_static("content-type"),
        HeaderName::from_static("x-nexus-token"),
        HeaderName::from_static("x-request-id"),
    ];
    let exposed = [HeaderName::from_static("x-request-id")];

    let origins_layer = if env.allowed_origins.is_empty() || env.allowed_origins.iter().any(|o| o == "*") {
        AllowOrigin::any()
    } else {
        let parsed: Vec<HeaderValue> = env
            .allowed_origins
            .iter()
            .filter_map(|o| HeaderValue::from_str(o).ok())
            .collect();
        AllowOrigin::list(parsed)
    };

    CorsLayer::new()
        .allow_origin(origins_layer)
        .allow_methods(methods)
        .allow_headers(allowed_headers)
        .expose_headers(exposed)
}

/// Strip credentials from a connection URL before it goes anywhere user-visible
/// (logs, /api/system/config, errors). Returns the URL unchanged for SQLite
/// (no embedded credentials).
fn sanitise_db_url(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_string();
    };
    let Some((_credentials, host_part)) = rest.split_once('@') else {
        return url.to_string();
    };
    format!("{scheme}://***@{host_part}")
}

async fn public_health(State(state): State<AppState>) -> Response {
    let db_ok = sqlx::query("SELECT 1").execute(state.db.pool()).await.is_ok();
    let status = if db_ok { "ok" } else { "degraded" };
    let code = if db_ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        code,
        Json(json!({
            "status": status,
            "timestamp": crate::time::iso_now(),
            "service": "nexus-registry",
            "wsClients": connection_count(),
            "db": if db_ok { "ok" } else { "down" },
        })),
    )
        .into_response()
}

#[derive(Clone)]
struct CorrelationSpan;

impl<B> MakeSpan<B> for CorrelationSpan {
    fn make_span(&mut self, request: &Request<B>) -> Span {
        let cid = request
            .extensions()
            .get::<CorrelationId>()
            .map(|c| c.as_str().to_string())
            .unwrap_or_else(|| "-".to_string());
        tracing::info_span!(
            "request",
            method = %request.method(),
            uri = %request.uri(),
            cid = %cid,
        )
    }
}

async fn not_found(ext: Option<Extension<CorrelationId>>) -> Response {
    let cid = ext
        .map(|e| e.0 .0.clone())
        .unwrap_or_else(|| "<no-correlation-id>".to_string());
    (
        StatusCode::NOT_FOUND,
        Json(json!({
            "error": "not_found",
            "message": "Route not found",
            "correlationId": cid,
        })),
    )
        .into_response()
}
