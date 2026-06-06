use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::types::WsReconnectConfig;
use crate::observability::log_buffer::LogEntry;
use crate::types::{GateWithHost, Host, RemoteConfig};

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    Welcome {
        timestamp: String,
        clients: usize,
        reconnect_policy: WsReconnectConfig,
    },
    RemotesChanged {
        timestamp: String,
        remotes: Vec<RemoteConfig>,
        trigger: String,
    },
    SystemHealth {
        timestamp: String,
        snapshot: Value,
    },
    Log {
        entry: LogEntry,
    },
    Pong {
        timestamp: String,
    },
    ConfigChanged {
        timestamp: String,
        section: String,
        value: Value,
    },
    ReconnectPolicyChanged {
        timestamp: String,
        policy: WsReconnectConfig,
    },
    RegistryShuttingDown {
        timestamp: String,
        resume_in_ms: u64,
    },
    TokenRotated {
        timestamp: String,
        previous_token_expired: bool,
    },
    HostChanged {
        timestamp: String,
        host: Host,
        trigger: String,
    },
    GateChanged {
        timestamp: String,
        gate: GateWithHost,
        trigger: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        old_host_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        new_host_id: Option<String>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    Ping,
    Subscribe { subscribe: String },
    Unsubscribe { subscribe: String },
    SubscribeGate { gate_name: String },
}
