use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;
use tracing::info;

use crate::config::store::ConfigStore;
use crate::time::iso_now;
use crate::ws::messages::ServerMessage;

/// Coordinates the orchestrated shutdown sequence. `trigger()` fires the
/// sequence; `shutdown_signal()` is what `axum::serve` awaits on.
pub struct ShutdownController {
    trigger: Notify,
    proceed: Notify,
}

impl ShutdownController {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            trigger: Notify::new(),
            proceed: Notify::new(),
        })
    }

    /// Fires the shutdown sequence — usable from a signal handler or from
    /// `POST /api/system/shutdown`.
    pub fn trigger(&self) {
        self.trigger.notify_waiters();
        // notify_one in case no waiter yet; tokio Notify coalesces these.
        self.trigger.notify_one();
    }

    /// The future that `axum::serve(...).with_graceful_shutdown(...)` should
    /// await. Resolves when the orchestrator has finished broadcasting and
    /// the WS notice window has elapsed.
    pub async fn wait_for_drain(&self) {
        self.proceed.notified().await;
    }

    /// Background task that watches for the trigger, broadcasts the
    /// `registry_shutting_down` WS message, waits the configured notice
    /// window, then signals `wait_for_drain()`.
    pub fn spawn_orchestrator(
        self: Arc<Self>,
        config: Arc<ConfigStore>,
        broadcast_tx: tokio::sync::broadcast::Sender<ServerMessage>,
    ) {
        tokio::spawn(async move {
            self.trigger.notified().await;
            let cfg = config.graceful_shutdown();

            info!(step = 1, "[shutdown] received trigger, beginning sequence");
            info!(step = 2, "[shutdown] broadcasting registry_shutting_down");
            let _ = broadcast_tx.send(ServerMessage::RegistryShuttingDown {
                timestamp: iso_now(),
                resume_in_ms: cfg.ws_notice_ms,
            });

            info!(
                step = 3,
                ms = cfg.ws_notice_ms,
                "[shutdown] waiting for WS clients to disconnect"
            );
            tokio::time::sleep(Duration::from_millis(cfg.ws_notice_ms)).await;

            info!(
                step = 4,
                "[shutdown] signalling HTTP drain (axum will reject new connections, finish in-flight)"
            );
            self.proceed.notify_one();
        });
    }
}
