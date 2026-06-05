use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use chrono::Utc;
use futures_util::{sink::SinkExt, stream::StreamExt};
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use serde_json::Value;
use tokio::sync::broadcast;
use tracing::{debug, warn};

use crate::features::metrics as prom;
use crate::features::token;
use crate::observability::log_buffer::LogEntry;
use crate::state::AppState;
use crate::store;
use crate::time::iso_now;
use crate::types::{GateWithHost, Host};
use crate::ws::messages::{ClientMessage, ServerMessage};

static CONN_COUNT: AtomicUsize = AtomicUsize::new(0);
static CONN_ID_NEXT: AtomicU64 = AtomicU64::new(1);

/// Maps connection-id → gate name the client is interested in. Wired today
/// only so the field is populated for future per-gate broadcast filtering.
static GATE_SUBSCRIPTIONS: Lazy<RwLock<HashMap<u64, String>>> = Lazy::new(|| RwLock::new(HashMap::new()));

pub fn connection_count() -> usize {
    CONN_COUNT.load(Ordering::Relaxed)
}

#[allow(dead_code)]
pub fn gate_subscription_for(conn_id: u64) -> Option<String> {
    GATE_SUBSCRIPTIONS.read().get(&conn_id).cloned()
}

pub async fn upgrade(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    let header_token = headers.get("x-nexus-token").and_then(|v| v.to_str().ok());
    let query_token = query.get("token").map(|s| s.as_str());
    let presented = header_token.or(query_token).unwrap_or("");

    let stored = state.config_store.token();
    let authorized = token::verify_token(
        stored.as_ref(),
        presented,
        &state.env.nexus_token_pepper,
        Utc::now(),
    );

    if !authorized {
        warn!("[ws] rejected upgrade: missing or invalid X-Nexus-Token");
        return (StatusCode::UNAUTHORIZED, "Missing or invalid X-Nexus-Token").into_response();
    }

    ws.on_upgrade(move |socket| handle_connection(socket, state))
}

async fn handle_connection(socket: WebSocket, state: AppState) {
    let total = CONN_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
    let conn_id = CONN_ID_NEXT.fetch_add(1, Ordering::Relaxed);
    prom::set_ws_clients(total);

    let (mut sender, mut receiver) = socket.split();
    let mut rx_broadcast: broadcast::Receiver<ServerMessage> = state.broadcast_tx.subscribe();
    let mut rx_log: broadcast::Receiver<LogEntry> = state.log_buffer.subscribe();
    let mut log_subscribed = false;

    let welcome = ServerMessage::Welcome {
        timestamp: iso_now(),
        clients: total,
        reconnect_policy: (*state.config_store.ws_reconnect()).clone(),
    };
    if !send(&mut sender, &welcome).await {
        CONN_COUNT.fetch_sub(1, Ordering::Relaxed);
        prom::set_ws_clients(CONN_COUNT.load(Ordering::Relaxed));
        return;
    }

    loop {
        tokio::select! {
            incoming = receiver.next() => {
                let Some(Ok(msg)) = incoming else { break };
                match msg {
                    Message::Text(text) => {
                        match serde_json::from_str::<ClientMessage>(&text) {
                            Ok(ClientMessage::Ping) => {
                                let pong = ServerMessage::Pong { timestamp: iso_now() };
                                if !send(&mut sender, &pong).await { break; }
                            }
                            Ok(ClientMessage::Subscribe { subscribe }) if subscribe == "logs" => {
                                log_subscribed = true;
                            }
                            Ok(ClientMessage::Unsubscribe { subscribe }) if subscribe == "logs" => {
                                log_subscribed = false;
                            }
                            Ok(ClientMessage::SubscribeGate { gate_name }) => {
                                GATE_SUBSCRIPTIONS.write().insert(conn_id, gate_name);
                            }
                            _ => {}
                        }
                    }
                    Message::Close(_) => break,
                    Message::Ping(p) => {
                        if sender.send(Message::Pong(p)).await.is_err() { break; }
                    }
                    _ => {}
                }
            }
            recv = rx_broadcast.recv() => {
                match recv {
                    Ok(msg) => {
                        if !send(&mut sender, &msg).await { break; }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        debug!("[ws] broadcast lagged by {} messages", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            recv = rx_log.recv() => {
                match recv {
                    Ok(entry) => {
                        if log_subscribed {
                            let msg = ServerMessage::Log { entry };
                            if !send(&mut sender, &msg).await { break; }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }

    GATE_SUBSCRIPTIONS.write().remove(&conn_id);
    CONN_COUNT.fetch_sub(1, Ordering::Relaxed);
    prom::set_ws_clients(CONN_COUNT.load(Ordering::Relaxed));
}

async fn send(sender: &mut futures_util::stream::SplitSink<WebSocket, Message>, msg: &ServerMessage) -> bool {
    let Ok(json) = serde_json::to_string(msg) else {
        return false;
    };
    prom::record_ws_message(message_kind(msg));
    sender.send(Message::Text(json.into())).await.is_ok()
}

fn message_kind(msg: &ServerMessage) -> &'static str {
    match msg {
        ServerMessage::Welcome { .. } => "welcome",
        ServerMessage::RemotesChanged { .. } => "remotes_changed",
        ServerMessage::SystemHealth { .. } => "system_health",
        ServerMessage::Log { .. } => "log",
        ServerMessage::Pong { .. } => "pong",
        ServerMessage::ConfigChanged { .. } => "config_changed",
        ServerMessage::ReconnectPolicyChanged { .. } => "reconnect_policy_changed",
        ServerMessage::RegistryShuttingDown { .. } => "registry_shutting_down",
        ServerMessage::TokenRotated { .. } => "token_rotated",
        ServerMessage::HostChanged { .. } => "host_changed",
        ServerMessage::GateChanged { .. } => "gate_changed",
    }
}

pub fn broadcast_host_changed(state: &AppState, host: Host, trigger: impl Into<String>) {
    let _ = state.broadcast_tx.send(ServerMessage::HostChanged {
        timestamp: iso_now(),
        host,
        trigger: trigger.into(),
    });
}

pub fn broadcast_gate_changed(
    state: &AppState,
    gate: GateWithHost,
    trigger: impl Into<String>,
    old_host_id: Option<String>,
    new_host_id: Option<String>,
) {
    let _ = state.broadcast_tx.send(ServerMessage::GateChanged {
        timestamp: iso_now(),
        gate,
        trigger: trigger.into(),
        old_host_id,
        new_host_id,
    });
}

pub async fn broadcast_remotes_changed(state: &AppState, trigger: impl Into<String>) {
    let tx = &state.broadcast_tx;
    if tx.receiver_count() == 0 {
        return;
    }
    let remotes = match store::list(&state.db).await {
        Ok(r) => r,
        Err(_) => return,
    };
    let msg = ServerMessage::RemotesChanged {
        timestamp: iso_now(),
        remotes,
        trigger: trigger.into(),
    };
    let _ = tx.send(msg);
}

pub fn broadcast_system_health(state: &AppState, snapshot_value: Value) {
    let tx = &state.broadcast_tx;
    if tx.receiver_count() == 0 {
        return;
    }
    let msg = ServerMessage::SystemHealth {
        timestamp: iso_now(),
        snapshot: snapshot_value,
    };
    let _ = tx.send(msg);
}

pub fn broadcast_config_changed(state: &AppState, section: &str, value: Value) {
    let _ = state.broadcast_tx.send(ServerMessage::ConfigChanged {
        timestamp: iso_now(),
        section: section.to_string(),
        value,
    });
}

pub fn broadcast_reconnect_policy(state: &AppState, policy: crate::config::types::WsReconnectConfig) {
    let _ = state.broadcast_tx.send(ServerMessage::ReconnectPolicyChanged {
        timestamp: iso_now(),
        policy,
    });
}
