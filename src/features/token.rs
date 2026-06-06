use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::Response,
};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;
use tokio::sync::broadcast;
use tracing::{error, info};

use crate::config::store::ConfigStore;
use crate::config::types::TokenRotationStored;
use crate::correlation::CorrelationId;
use crate::http_error::error_response;
use crate::state::AppState;
use crate::time::iso_now;
use crate::ws::messages::ServerMessage;

type HmacSha256 = Hmac<Sha256>;

/// Hash a plaintext token with HMAC-SHA256 using the configured pepper.
/// Output is lowercase hex. Pepper rotation invalidates all existing hashes.
pub fn hash_token(plaintext: &str, pepper: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(pepper.as_bytes()).expect("HMAC-SHA256 accepts any key length");
    mac.update(plaintext.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Constant-time comparison of two hex-encoded hashes.
pub fn verify_hash(presented_plaintext: &str, expected_hash: &str, pepper: &str) -> bool {
    let candidate = hash_token(presented_plaintext, pepper);
    candidate.as_bytes().ct_eq(expected_hash.as_bytes()).into()
}

/// Verifies a presented token against the active hash, then the previous hash
/// if it is still within its expiry window.
pub fn verify_token(
    stored: &Option<TokenRotationStored>,
    presented: &str,
    pepper: &str,
    now: DateTime<Utc>,
) -> bool {
    let Some(stored) = stored else { return false };
    if presented.is_empty() {
        return false;
    }
    if verify_hash(presented, &stored.active_token_hash, pepper) {
        return true;
    }
    if let (Some(prev_hash), Some(expires_at)) = (
        stored.previous_token_hash.as_deref(),
        stored.previous_token_expires_at.as_deref(),
    ) {
        if let Ok(expiry) = DateTime::parse_from_rfc3339(expires_at) {
            if expiry.with_timezone(&Utc) > now && verify_hash(presented, prev_hash, pepper) {
                return true;
            }
        }
    }
    false
}

/// Initialize the active token from the NEXUS_TOKEN env-var if no active token
/// exists in the database. Idempotent — never overwrites an existing hash.
pub async fn init_from_env(config: &ConfigStore, plaintext: &str, pepper: &str) -> Result<(), sqlx::Error> {
    if plaintext.is_empty() {
        return Ok(());
    }
    if let Some(existing) = config.token().as_ref() {
        if !existing.active_token_hash.is_empty() {
            return Ok(());
        }
    }
    let hash = hash_token(plaintext, pepper);
    config
        .update_token(TokenRotationStored {
            active_token_hash: hash,
            previous_token_hash: None,
            previous_token_expires_at: None,
        })
        .await?;
    info!("[token] active token hash bootstrapped from NEXUS_TOKEN env-var");
    Ok(())
}

pub async fn middleware(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let cid = req
        .extensions()
        .get::<CorrelationId>()
        .map(|c| c.as_str().to_string())
        .unwrap_or_else(|| "<no-correlation-id>".to_string());

    let presented = req
        .headers()
        .get("x-nexus-token")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if presented.is_empty() {
        error!(
            "[auth] [{}] Missing X-Nexus-Token on {} {}",
            cid,
            req.method(),
            req.uri().path()
        );
        return error_response(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Missing X-Nexus-Token header",
            &cid,
        );
    }

    let stored = state.config_store.token();
    let ok = verify_token(
        stored.as_ref(),
        presented,
        &state.env.nexus_token_pepper,
        Utc::now(),
    );
    if !ok {
        error!(
            "[auth] [{}] Invalid X-Nexus-Token on {} {}",
            cid,
            req.method(),
            req.uri().path()
        );
        return error_response(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Invalid X-Nexus-Token",
            &cid,
        );
    }
    next.run(req).await
}

/// Background task that nulls out expired previous-token hashes once per minute
/// and broadcasts a `token_rotated` message when it does.
pub fn start_expiry_loop(config: Arc<ConfigStore>, broadcast_tx: broadcast::Sender<ServerMessage>) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(60));
        loop {
            tick.tick().await;
            let Some(current) = config.token().as_ref().clone() else {
                continue;
            };
            let Some(expires_at) = current.previous_token_expires_at.as_deref() else {
                continue;
            };
            let Ok(expiry) = DateTime::parse_from_rfc3339(expires_at) else {
                continue;
            };
            if expiry.with_timezone(&Utc) > Utc::now() {
                continue;
            }
            let cleared = TokenRotationStored {
                active_token_hash: current.active_token_hash.clone(),
                previous_token_hash: None,
                previous_token_expires_at: None,
            };
            if config.update_token(cleared).await.is_ok() {
                info!("[token] previous token expired and removed");
                let _ = broadcast_tx.send(ServerMessage::TokenRotated {
                    timestamp: iso_now(),
                    previous_token_expired: true,
                });
            }
        }
    });
}
