use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Extension, Json, Router,
};
use chrono::{Duration, Utc};
use serde_json::json;
use tracing::{error, info};

use crate::config::types::{
    CircuitBreakerConfig, GatewayProtectionConfig, GracefulShutdownConfig, MetricsConfig, PartialConfig,
    RateLimitingConfig, TokenRotateRequest, TokenRotationStored, WsReconnectConfig,
};
use crate::correlation::CorrelationId;
use crate::features::token;
use crate::http_error::error_response;
use crate::state::AppState;
use crate::time::iso_now;
use crate::ws::hub;
use crate::ws::messages::ServerMessage;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(get_all).put(put_all))
        .route("/rate-limiting", get(get_rate_limiting).put(put_rate_limiting))
        .route("/ws-reconnect", get(get_ws_reconnect).put(put_ws_reconnect))
        .route(
            "/circuit-breaker",
            get(get_circuit_breaker).put(put_circuit_breaker),
        )
        .route("/circuit-breaker/state", get(get_circuit_state))
        .route("/circuit-breaker/reset", post(reset_all_circuits))
        .route("/circuit-breaker/reset/{remote_name}", post(reset_one_circuit))
        .route(
            "/graceful-shutdown",
            get(get_graceful_shutdown).put(put_graceful_shutdown),
        )
        .route("/metrics", get(get_metrics).put(put_metrics))
        .route("/token", get(get_token))
        .route("/token/rotate", post(rotate_token))
        .route("/token/previous", delete(delete_previous_token))
        // Gateway-facing config endpoint: GET /api/config/gateway
        .route("/gateway", get(get_gateway_config))
        .route(
            "/gateway/protection",
            get(get_gateway_protection).put(put_gateway_protection),
        )
}

// ---- Unified ----

async fn get_all(State(state): State<AppState>) -> Response {
    Json(state.config_store.snapshot()).into_response()
}

async fn put_all(
    State(state): State<AppState>,
    Extension(cid): Extension<CorrelationId>,
    Json(body): Json<PartialConfig>,
) -> Response {
    if let Some(ref c) = body.rate_limiting {
        if let Err(e) = c.validate() {
            return validation_err(&cid, e);
        }
    }
    if let Some(ref c) = body.ws_reconnect {
        if let Err(e) = c.validate() {
            return validation_err(&cid, e);
        }
    }
    if let Some(ref c) = body.circuit_breaker {
        if let Err(e) = c.validate() {
            return validation_err(&cid, e);
        }
    }
    if let Some(ref c) = body.graceful_shutdown {
        if let Err(e) = c.validate() {
            return validation_err(&cid, e);
        }
    }
    if let Some(ref c) = body.metrics {
        if let Err(e) = c.validate() {
            return validation_err(&cid, e);
        }
    }

    if let Some(c) = body.rate_limiting {
        if let Err(e) = apply_rate_limiting(&state, c).await {
            return server_err(&cid, e);
        }
    }
    if let Some(c) = body.ws_reconnect {
        if let Err(e) = apply_ws_reconnect(&state, c).await {
            return server_err(&cid, e);
        }
    }
    if let Some(c) = body.circuit_breaker {
        if let Err(e) = apply_circuit_breaker(&state, c).await {
            return server_err(&cid, e);
        }
    }
    if let Some(c) = body.graceful_shutdown {
        if let Err(e) = apply_graceful_shutdown(&state, c).await {
            return server_err(&cid, e);
        }
    }
    if let Some(c) = body.metrics {
        if let Err(e) = apply_metrics(&state, c).await {
            return server_err(&cid, e);
        }
    }

    Json(state.config_store.snapshot()).into_response()
}

// ---- Rate limiting ----

async fn get_rate_limiting(State(state): State<AppState>) -> Response {
    Json((*state.config_store.rate_limiting()).clone()).into_response()
}

async fn put_rate_limiting(
    State(state): State<AppState>,
    Extension(cid): Extension<CorrelationId>,
    Json(new): Json<RateLimitingConfig>,
) -> Response {
    if let Err(e) = new.validate() {
        return validation_err(&cid, e);
    }
    match apply_rate_limiting(&state, new.clone()).await {
        Ok(out) => Json(out).into_response(),
        Err(e) => server_err(&cid, e),
    }
}

async fn apply_rate_limiting(
    state: &AppState,
    new: RateLimitingConfig,
) -> Result<RateLimitingConfig, sqlx::Error> {
    let saved = state.config_store.update_rate_limiting(new).await?;
    state.rate_limit.rebuild(&saved);
    hub::broadcast_config_changed(
        state,
        "rate_limiting",
        serde_json::to_value(&saved).unwrap_or_default(),
    );
    Ok(saved)
}

// ---- WS reconnect ----

async fn get_ws_reconnect(State(state): State<AppState>) -> Response {
    Json((*state.config_store.ws_reconnect()).clone()).into_response()
}

async fn put_ws_reconnect(
    State(state): State<AppState>,
    Extension(cid): Extension<CorrelationId>,
    Json(new): Json<WsReconnectConfig>,
) -> Response {
    if let Err(e) = new.validate() {
        return validation_err(&cid, e);
    }
    match apply_ws_reconnect(&state, new.clone()).await {
        Ok(out) => Json(out).into_response(),
        Err(e) => server_err(&cid, e),
    }
}

async fn apply_ws_reconnect(
    state: &AppState,
    new: WsReconnectConfig,
) -> Result<WsReconnectConfig, sqlx::Error> {
    let saved = state.config_store.update_ws_reconnect(new).await?;
    hub::broadcast_config_changed(
        state,
        "ws_reconnect",
        serde_json::to_value(&saved).unwrap_or_default(),
    );
    hub::broadcast_reconnect_policy(state, saved.clone());
    Ok(saved)
}

// ---- Circuit breaker ----

async fn get_circuit_breaker(State(state): State<AppState>) -> Response {
    Json((*state.config_store.circuit_breaker()).clone()).into_response()
}

async fn put_circuit_breaker(
    State(state): State<AppState>,
    Extension(cid): Extension<CorrelationId>,
    Json(new): Json<CircuitBreakerConfig>,
) -> Response {
    if let Err(e) = new.validate() {
        return validation_err(&cid, e);
    }
    match apply_circuit_breaker(&state, new.clone()).await {
        Ok(out) => Json(out).into_response(),
        Err(e) => server_err(&cid, e),
    }
}

async fn apply_circuit_breaker(
    state: &AppState,
    new: CircuitBreakerConfig,
) -> Result<CircuitBreakerConfig, sqlx::Error> {
    let saved = state.config_store.update_circuit_breaker(new).await?;
    hub::broadcast_config_changed(
        state,
        "circuit_breaker",
        serde_json::to_value(&saved).unwrap_or_default(),
    );
    Ok(saved)
}

async fn get_circuit_state(State(state): State<AppState>) -> Response {
    Json(state.circuit_breaker.snapshot()).into_response()
}

async fn reset_one_circuit(State(state): State<AppState>, Path(remote_name): Path<String>) -> Response {
    state.circuit_breaker.reset(&remote_name);
    info!(remote = %remote_name, "[circuit] manual reset");
    Json(json!({ "remote": remote_name, "state": "closed" })).into_response()
}

async fn reset_all_circuits(State(state): State<AppState>) -> Response {
    state.circuit_breaker.reset_all();
    info!("[circuit] reset all");
    Json(json!({ "reset": true })).into_response()
}

// ---- Graceful shutdown ----

async fn get_graceful_shutdown(State(state): State<AppState>) -> Response {
    Json((*state.config_store.graceful_shutdown()).clone()).into_response()
}

async fn put_graceful_shutdown(
    State(state): State<AppState>,
    Extension(cid): Extension<CorrelationId>,
    Json(new): Json<GracefulShutdownConfig>,
) -> Response {
    if let Err(e) = new.validate() {
        return validation_err(&cid, e);
    }
    match apply_graceful_shutdown(&state, new.clone()).await {
        Ok(out) => Json(out).into_response(),
        Err(e) => server_err(&cid, e),
    }
}

async fn apply_graceful_shutdown(
    state: &AppState,
    new: GracefulShutdownConfig,
) -> Result<GracefulShutdownConfig, sqlx::Error> {
    let saved = state.config_store.update_graceful_shutdown(new).await?;
    hub::broadcast_config_changed(
        state,
        "graceful_shutdown",
        serde_json::to_value(&saved).unwrap_or_default(),
    );
    Ok(saved)
}

// ---- Metrics ----

async fn get_metrics(State(state): State<AppState>) -> Response {
    Json((*state.config_store.metrics()).clone()).into_response()
}

async fn put_metrics(
    State(state): State<AppState>,
    Extension(cid): Extension<CorrelationId>,
    Json(new): Json<MetricsConfig>,
) -> Response {
    if let Err(e) = new.validate() {
        return validation_err(&cid, e);
    }
    match apply_metrics(&state, new.clone()).await {
        Ok(out) => Json(out).into_response(),
        Err(e) => server_err(&cid, e),
    }
}

async fn apply_metrics(state: &AppState, new: MetricsConfig) -> Result<MetricsConfig, sqlx::Error> {
    let saved = state.config_store.update_metrics(new).await?;
    hub::broadcast_config_changed(state, "metrics", serde_json::to_value(&saved).unwrap_or_default());
    Ok(saved)
}

// ---- Token rotation ----

async fn get_token(State(state): State<AppState>) -> Response {
    let stored = state.config_store.token();
    match stored.as_ref() {
        Some(t) => Json(t).into_response(),
        None => Json(json!({
            "activeTokenHash": "",
            "previousTokenHash": null,
            "previousTokenExpiresAt": null,
        }))
        .into_response(),
    }
}

async fn rotate_token(
    State(state): State<AppState>,
    Extension(cid): Extension<CorrelationId>,
    Json(req): Json<TokenRotateRequest>,
) -> Response {
    if let Err(e) = req.validate() {
        return validation_err(&cid, e);
    }
    let pepper = state.env.nexus_token_pepper.as_str();
    let new_hash = token::hash_token(&req.new_token, pepper);
    let current = state.config_store.token().as_ref().clone();

    let (previous_hash, previous_expiry) = match current {
        Some(c) => {
            let exp = Utc::now() + Duration::seconds(req.previous_token_ttl_seconds as i64);
            (Some(c.active_token_hash), Some(exp.to_rfc3339()))
        }
        None => (None, None),
    };

    let stored = TokenRotationStored {
        active_token_hash: new_hash,
        previous_token_hash: previous_hash,
        previous_token_expires_at: previous_expiry,
    };

    match state.config_store.update_token(stored.clone()).await {
        Ok(saved) => {
            let _ = state.broadcast_tx.send(ServerMessage::TokenRotated {
                timestamp: iso_now(),
                previous_token_expired: false,
            });
            info!(
                "[token] rotated; previous token kept for {}s",
                req.previous_token_ttl_seconds
            );
            Json(saved).into_response()
        }
        Err(e) => server_err(&cid, e),
    }
}

async fn delete_previous_token(
    State(state): State<AppState>,
    Extension(cid): Extension<CorrelationId>,
) -> Response {
    let current = state.config_store.token().as_ref().clone();
    let Some(current) = current else {
        return error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            "no token rotation state exists",
            cid.as_str(),
        );
    };
    let cleared = TokenRotationStored {
        active_token_hash: current.active_token_hash,
        previous_token_hash: None,
        previous_token_expires_at: None,
    };
    match state.config_store.update_token(cleared).await {
        Ok(_) => {
            let _ = state.broadcast_tx.send(ServerMessage::TokenRotated {
                timestamp: iso_now(),
                previous_token_expired: true,
            });
            info!("[token] previous token invalidated by request");
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => server_err(&cid, e),
    }
}

// ---- Gateway config (read by nexus-gateway on startup and via WS) ----

async fn get_gateway_config(State(state): State<AppState>) -> Response {
    let protection = (*state.config_store.gateway_protection()).clone();
    Json(serde_json::json!({ "protection": protection })).into_response()
}

async fn get_gateway_protection(State(state): State<AppState>) -> Response {
    Json((*state.config_store.gateway_protection()).clone()).into_response()
}

async fn put_gateway_protection(
    State(state): State<AppState>,
    Extension(cid): Extension<CorrelationId>,
    Json(new): Json<GatewayProtectionConfig>,
) -> Response {
    if let Err(e) = new.validate() {
        return validation_err(&cid, e);
    }
    match state.config_store.update_gateway_protection(new.clone()).await {
        Ok(saved) => {
            hub::broadcast_config_changed(
                &state,
                "gateway_protection",
                serde_json::to_value(&saved).unwrap_or_default(),
            );
            Json(saved).into_response()
        }
        Err(e) => server_err(&cid, e),
    }
}

// ---- Helpers ----

fn validation_err(cid: &CorrelationId, message: String) -> Response {
    error_response(
        StatusCode::BAD_REQUEST,
        "validation_failed",
        message,
        cid.as_str(),
    )
}

fn server_err(cid: &CorrelationId, err: sqlx::Error) -> Response {
    error!("[config] [{}] DB error: {}", cid.as_str(), err);
    error_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal_server_error",
        err.to_string(),
        cid.as_str(),
    )
}
