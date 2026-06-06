use std::sync::Arc;
use std::time::Instant;

use parking_lot::RwLock;
use tokio::sync::broadcast;

use crate::config::store::ConfigStore;
use crate::config::EnvConfig;
use crate::features::circuit::CircuitBreakerRegistry;
use crate::features::rate_limit::RateLimitState;
use crate::features::shutdown::ShutdownController;
use crate::observability::log_buffer::LogBuffer;
use crate::observability::metrics::Metrics;
use crate::store::Db;
use crate::system_health::SystemHealthSnapshot;
use crate::ws::messages::ServerMessage;

#[derive(Clone)]
pub struct AppState {
    pub db: Db,
    pub env: Arc<EnvConfig>,
    pub config_store: Arc<ConfigStore>,
    pub circuit_breaker: Arc<CircuitBreakerRegistry>,
    pub rate_limit: Arc<RateLimitState>,
    pub shutdown: Arc<ShutdownController>,
    pub metrics: Arc<Metrics>,
    pub log_buffer: Arc<LogBuffer>,
    pub broadcast_tx: broadcast::Sender<ServerMessage>,
    pub health_cache: Arc<RwLock<Option<SystemHealthSnapshot>>>,
    pub started_at: Arc<Instant>,
}
