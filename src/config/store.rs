use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use sqlx::Row;

use crate::config::database::Dialect;
use crate::config::types::{
    AllConfig, CircuitBreakerConfig, GatewayProtectionConfig, GracefulShutdownConfig, MetricsConfig,
    RateLimitingConfig, TokenRotationStored, WsReconnectConfig,
};
use crate::store::Db;

// Cross-dialect schema. Each statement uses syntax accepted by SQLite,
// Postgres and MySQL alike. INTEGER PRIMARY KEY works everywhere. VARCHAR
// rather than TEXT for PK / short-string columns so MySQL can index them
// without `key length` errors. Single-row config tables intentionally do not
// declare a CHECK constraint here — MySQL 5.x silently ignored CHECK anyway,
// and we control every INSERT to use id = 1.
const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS rate_limiting_config (
    id INTEGER PRIMARY KEY,
    enabled INTEGER NOT NULL,
    requests_per_second INTEGER NOT NULL,
    burst_size INTEGER NOT NULL,
    by_field VARCHAR(32) NOT NULL
);
CREATE TABLE IF NOT EXISTS ws_reconnect_config (
    id INTEGER PRIMARY KEY,
    initial_delay_ms INTEGER NOT NULL,
    max_delay_ms INTEGER NOT NULL,
    backoff_multiplier REAL NOT NULL,
    jitter_ms INTEGER NOT NULL,
    max_attempts INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS circuit_breaker_config (
    id INTEGER PRIMARY KEY,
    enabled INTEGER NOT NULL,
    failure_threshold INTEGER NOT NULL,
    success_threshold INTEGER NOT NULL,
    open_duration_ms INTEGER NOT NULL,
    half_open_max_calls INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS graceful_shutdown_config (
    id INTEGER PRIMARY KEY,
    timeout_ms INTEGER NOT NULL,
    ws_notice_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS metrics_config (
    id INTEGER PRIMARY KEY,
    prometheus_enabled INTEGER NOT NULL,
    prometheus_path VARCHAR(255) NOT NULL,
    require_auth INTEGER NOT NULL,
    custom_labels TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS token_rotation (
    id INTEGER PRIMARY KEY,
    active_token_hash VARCHAR(255) NOT NULL,
    previous_token_hash VARCHAR(255),
    previous_token_expires_at VARCHAR(64)
);
CREATE TABLE IF NOT EXISTS gateway_protection_config (
    id INTEGER PRIMARY KEY,
    config_json TEXT NOT NULL
);
"#;

pub struct ConfigStore {
    db: Db,
    rate_limiting: RwLock<Arc<RateLimitingConfig>>,
    ws_reconnect: RwLock<Arc<WsReconnectConfig>>,
    circuit_breaker: RwLock<Arc<CircuitBreakerConfig>>,
    graceful_shutdown: RwLock<Arc<GracefulShutdownConfig>>,
    metrics: RwLock<Arc<MetricsConfig>>,
    token: RwLock<Arc<Option<TokenRotationStored>>>,
    gateway_protection: RwLock<Arc<GatewayProtectionConfig>>,
}

impl ConfigStore {
    pub async fn hydrate(db: Db) -> Result<Arc<Self>, sqlx::Error> {
        for stmt in SCHEMA.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            sqlx::query(stmt).execute(db.pool()).await?;
        }

        let rate_limiting = load_rate_limiting(&db).await?;
        let ws_reconnect = load_ws_reconnect(&db).await?;
        let circuit_breaker = load_circuit_breaker(&db).await?;
        let graceful_shutdown = load_graceful_shutdown(&db).await?;
        let metrics = load_metrics(&db).await?;
        let token = load_token(&db).await?;
        let gateway_protection = load_gateway_protection(&db).await?;

        Ok(Arc::new(Self {
            db,
            rate_limiting: RwLock::new(Arc::new(rate_limiting)),
            ws_reconnect: RwLock::new(Arc::new(ws_reconnect)),
            circuit_breaker: RwLock::new(Arc::new(circuit_breaker)),
            graceful_shutdown: RwLock::new(Arc::new(graceful_shutdown)),
            metrics: RwLock::new(Arc::new(metrics)),
            token: RwLock::new(Arc::new(token)),
            gateway_protection: RwLock::new(Arc::new(gateway_protection)),
        }))
    }

    pub fn rate_limiting(&self) -> Arc<RateLimitingConfig> {
        self.rate_limiting.read().clone()
    }

    pub fn ws_reconnect(&self) -> Arc<WsReconnectConfig> {
        self.ws_reconnect.read().clone()
    }

    pub fn circuit_breaker(&self) -> Arc<CircuitBreakerConfig> {
        self.circuit_breaker.read().clone()
    }

    pub fn graceful_shutdown(&self) -> Arc<GracefulShutdownConfig> {
        self.graceful_shutdown.read().clone()
    }

    pub fn metrics(&self) -> Arc<MetricsConfig> {
        self.metrics.read().clone()
    }

    pub fn token(&self) -> Arc<Option<TokenRotationStored>> {
        self.token.read().clone()
    }

    pub fn gateway_protection(&self) -> Arc<GatewayProtectionConfig> {
        self.gateway_protection.read().clone()
    }

    pub async fn update_gateway_protection(
        &self,
        new: GatewayProtectionConfig,
    ) -> Result<GatewayProtectionConfig, sqlx::Error> {
        save_gateway_protection(&self.db, &new).await?;
        *self.gateway_protection.write() = Arc::new(new.clone());
        Ok(new)
    }

    pub async fn update_rate_limiting(
        &self,
        new: RateLimitingConfig,
    ) -> Result<RateLimitingConfig, sqlx::Error> {
        save_rate_limiting(&self.db, &new).await?;
        *self.rate_limiting.write() = Arc::new(new.clone());
        Ok(new)
    }

    pub async fn update_ws_reconnect(
        &self,
        new: WsReconnectConfig,
    ) -> Result<WsReconnectConfig, sqlx::Error> {
        save_ws_reconnect(&self.db, &new).await?;
        *self.ws_reconnect.write() = Arc::new(new.clone());
        Ok(new)
    }

    pub async fn update_circuit_breaker(
        &self,
        new: CircuitBreakerConfig,
    ) -> Result<CircuitBreakerConfig, sqlx::Error> {
        save_circuit_breaker(&self.db, &new).await?;
        *self.circuit_breaker.write() = Arc::new(new.clone());
        Ok(new)
    }

    pub async fn update_graceful_shutdown(
        &self,
        new: GracefulShutdownConfig,
    ) -> Result<GracefulShutdownConfig, sqlx::Error> {
        save_graceful_shutdown(&self.db, &new).await?;
        *self.graceful_shutdown.write() = Arc::new(new.clone());
        Ok(new)
    }

    pub async fn update_metrics(&self, new: MetricsConfig) -> Result<MetricsConfig, sqlx::Error> {
        save_metrics(&self.db, &new).await?;
        *self.metrics.write() = Arc::new(new.clone());
        Ok(new)
    }

    pub async fn update_token(&self, new: TokenRotationStored) -> Result<TokenRotationStored, sqlx::Error> {
        save_token(&self.db, &new).await?;
        *self.token.write() = Arc::new(Some(new.clone()));
        Ok(new)
    }

    pub fn snapshot(&self) -> AllConfig {
        AllConfig {
            rate_limiting: (*self.rate_limiting()).clone(),
            ws_reconnect: (*self.ws_reconnect()).clone(),
            circuit_breaker: (*self.circuit_breaker()).clone(),
            graceful_shutdown: (*self.graceful_shutdown()).clone(),
            metrics: (*self.metrics()).clone(),
            token: self
                .token()
                .as_ref()
                .clone()
                .unwrap_or_else(|| TokenRotationStored {
                    active_token_hash: String::new(),
                    previous_token_hash: None,
                    previous_token_expires_at: None,
                }),
        }
    }
}

// ---- Upsert helper ----
//
// SQLite and Postgres share `INSERT ... ON CONFLICT(id) DO UPDATE SET col = excluded.col`.
// MySQL uses `INSERT ... ON DUPLICATE KEY UPDATE col = VALUES(col)`. This helper
// builds the right tail clause so each save_* function only writes the column
// list once and lets the dialect pick the upsert syntax.
fn upsert(dialect: Dialect, table: &str, columns: &[&str]) -> String {
    let placeholders = std::iter::repeat("?")
        .take(columns.len())
        .collect::<Vec<_>>()
        .join(", ");
    let col_list = columns.join(", ");
    let updatable: Vec<&&str> = columns.iter().filter(|c| **c != "id").collect();
    let tail = match dialect {
        Dialect::Sqlite | Dialect::Postgres => {
            let assigns: Vec<String> = updatable.iter().map(|c| format!("{c} = excluded.{c}")).collect();
            format!("ON CONFLICT(id) DO UPDATE SET {}", assigns.join(", "))
        }
        Dialect::MySql => {
            let assigns: Vec<String> = updatable.iter().map(|c| format!("{c} = VALUES({c})")).collect();
            format!("ON DUPLICATE KEY UPDATE {}", assigns.join(", "))
        }
    };
    format!("INSERT INTO {table} ({col_list}) VALUES ({placeholders}) {tail}")
}

// ---- Section loaders / savers ----

async fn load_rate_limiting(db: &Db) -> Result<RateLimitingConfig, sqlx::Error> {
    let sql = db.dialect.render(
        "SELECT enabled, requests_per_second, burst_size, by_field FROM rate_limiting_config WHERE id = 1",
    );
    let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
        .fetch_optional(db.pool())
        .await?;
    let cfg = match row {
        Some(r) => RateLimitingConfig {
            enabled: r.try_get::<i64, _>("enabled")? != 0,
            requests_per_second: r.try_get::<i64, _>("requests_per_second")? as u32,
            burst_size: r.try_get::<i64, _>("burst_size")? as u32,
            by: r.try_get("by_field")?,
        },
        None => {
            let cfg = RateLimitingConfig::default();
            save_rate_limiting(db, &cfg).await?;
            cfg
        }
    };
    Ok(cfg)
}

async fn save_rate_limiting(db: &Db, cfg: &RateLimitingConfig) -> Result<(), sqlx::Error> {
    let raw = upsert(
        db.dialect,
        "rate_limiting_config",
        &["id", "enabled", "requests_per_second", "burst_size", "by_field"],
    );
    let sql = db.dialect.render(&raw);
    sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
        .bind(1_i64)
        .bind(cfg.enabled as i64)
        .bind(cfg.requests_per_second as i64)
        .bind(cfg.burst_size as i64)
        .bind(&cfg.by)
        .execute(db.pool())
        .await?;
    Ok(())
}

async fn load_ws_reconnect(db: &Db) -> Result<WsReconnectConfig, sqlx::Error> {
    let sql = db.dialect.render(
        "SELECT initial_delay_ms, max_delay_ms, backoff_multiplier, jitter_ms, max_attempts \
         FROM ws_reconnect_config WHERE id = 1",
    );
    let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
        .fetch_optional(db.pool())
        .await?;
    let cfg = match row {
        Some(r) => WsReconnectConfig {
            initial_delay_ms: r.try_get::<i64, _>("initial_delay_ms")? as u64,
            max_delay_ms: r.try_get::<i64, _>("max_delay_ms")? as u64,
            backoff_multiplier: r.try_get::<f64, _>("backoff_multiplier")?,
            jitter_ms: r.try_get::<i64, _>("jitter_ms")? as u64,
            max_attempts: r.try_get::<i64, _>("max_attempts")? as u32,
        },
        None => {
            let cfg = WsReconnectConfig::default();
            save_ws_reconnect(db, &cfg).await?;
            cfg
        }
    };
    Ok(cfg)
}

async fn save_ws_reconnect(db: &Db, cfg: &WsReconnectConfig) -> Result<(), sqlx::Error> {
    let raw = upsert(
        db.dialect,
        "ws_reconnect_config",
        &[
            "id",
            "initial_delay_ms",
            "max_delay_ms",
            "backoff_multiplier",
            "jitter_ms",
            "max_attempts",
        ],
    );
    let sql = db.dialect.render(&raw);
    sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
        .bind(1_i64)
        .bind(cfg.initial_delay_ms as i64)
        .bind(cfg.max_delay_ms as i64)
        .bind(cfg.backoff_multiplier)
        .bind(cfg.jitter_ms as i64)
        .bind(cfg.max_attempts as i64)
        .execute(db.pool())
        .await?;
    Ok(())
}

async fn load_circuit_breaker(db: &Db) -> Result<CircuitBreakerConfig, sqlx::Error> {
    let sql = db.dialect.render(
        "SELECT enabled, failure_threshold, success_threshold, open_duration_ms, half_open_max_calls \
         FROM circuit_breaker_config WHERE id = 1",
    );
    let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
        .fetch_optional(db.pool())
        .await?;
    let cfg = match row {
        Some(r) => CircuitBreakerConfig {
            enabled: r.try_get::<i64, _>("enabled")? != 0,
            failure_threshold: r.try_get::<i64, _>("failure_threshold")? as u32,
            success_threshold: r.try_get::<i64, _>("success_threshold")? as u32,
            open_duration_ms: r.try_get::<i64, _>("open_duration_ms")? as u64,
            half_open_max_calls: r.try_get::<i64, _>("half_open_max_calls")? as u32,
        },
        None => {
            let cfg = CircuitBreakerConfig::default();
            save_circuit_breaker(db, &cfg).await?;
            cfg
        }
    };
    Ok(cfg)
}

async fn save_circuit_breaker(db: &Db, cfg: &CircuitBreakerConfig) -> Result<(), sqlx::Error> {
    let raw = upsert(
        db.dialect,
        "circuit_breaker_config",
        &[
            "id",
            "enabled",
            "failure_threshold",
            "success_threshold",
            "open_duration_ms",
            "half_open_max_calls",
        ],
    );
    let sql = db.dialect.render(&raw);
    sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
        .bind(1_i64)
        .bind(cfg.enabled as i64)
        .bind(cfg.failure_threshold as i64)
        .bind(cfg.success_threshold as i64)
        .bind(cfg.open_duration_ms as i64)
        .bind(cfg.half_open_max_calls as i64)
        .execute(db.pool())
        .await?;
    Ok(())
}

async fn load_graceful_shutdown(db: &Db) -> Result<GracefulShutdownConfig, sqlx::Error> {
    let sql = db
        .dialect
        .render("SELECT timeout_ms, ws_notice_ms FROM graceful_shutdown_config WHERE id = 1");
    let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
        .fetch_optional(db.pool())
        .await?;
    let cfg = match row {
        Some(r) => GracefulShutdownConfig {
            timeout_ms: r.try_get::<i64, _>("timeout_ms")? as u64,
            ws_notice_ms: r.try_get::<i64, _>("ws_notice_ms")? as u64,
        },
        None => {
            let cfg = GracefulShutdownConfig::default();
            save_graceful_shutdown(db, &cfg).await?;
            cfg
        }
    };
    Ok(cfg)
}

async fn save_graceful_shutdown(db: &Db, cfg: &GracefulShutdownConfig) -> Result<(), sqlx::Error> {
    let raw = upsert(
        db.dialect,
        "graceful_shutdown_config",
        &["id", "timeout_ms", "ws_notice_ms"],
    );
    let sql = db.dialect.render(&raw);
    sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
        .bind(1_i64)
        .bind(cfg.timeout_ms as i64)
        .bind(cfg.ws_notice_ms as i64)
        .execute(db.pool())
        .await?;
    Ok(())
}

async fn load_metrics(db: &Db) -> Result<MetricsConfig, sqlx::Error> {
    let sql = db.dialect.render(
        "SELECT prometheus_enabled, prometheus_path, require_auth, custom_labels \
         FROM metrics_config WHERE id = 1",
    );
    let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
        .fetch_optional(db.pool())
        .await?;
    let cfg = match row {
        Some(r) => {
            let labels_json: String = r.try_get("custom_labels")?;
            let custom_labels: BTreeMap<String, String> =
                serde_json::from_str(&labels_json).unwrap_or_default();
            MetricsConfig {
                prometheus_enabled: r.try_get::<i64, _>("prometheus_enabled")? != 0,
                prometheus_path: r.try_get("prometheus_path")?,
                require_auth: r.try_get::<i64, _>("require_auth")? != 0,
                custom_labels,
            }
        }
        None => {
            let cfg = MetricsConfig::default();
            save_metrics(db, &cfg).await?;
            cfg
        }
    };
    Ok(cfg)
}

async fn save_metrics(db: &Db, cfg: &MetricsConfig) -> Result<(), sqlx::Error> {
    let labels_json = serde_json::to_string(&cfg.custom_labels).unwrap_or_else(|_| "{}".into());
    let raw = upsert(
        db.dialect,
        "metrics_config",
        &[
            "id",
            "prometheus_enabled",
            "prometheus_path",
            "require_auth",
            "custom_labels",
        ],
    );
    let sql = db.dialect.render(&raw);
    sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
        .bind(1_i64)
        .bind(cfg.prometheus_enabled as i64)
        .bind(&cfg.prometheus_path)
        .bind(cfg.require_auth as i64)
        .bind(labels_json)
        .execute(db.pool())
        .await?;
    Ok(())
}

async fn load_token(db: &Db) -> Result<Option<TokenRotationStored>, sqlx::Error> {
    let sql = db.dialect.render(
        "SELECT active_token_hash, previous_token_hash, previous_token_expires_at \
         FROM token_rotation WHERE id = 1",
    );
    let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
        .fetch_optional(db.pool())
        .await?;
    Ok(row.map(|r| TokenRotationStored {
        active_token_hash: r.get("active_token_hash"),
        previous_token_hash: r.get("previous_token_hash"),
        previous_token_expires_at: r.get("previous_token_expires_at"),
    }))
}

async fn save_token(db: &Db, t: &TokenRotationStored) -> Result<(), sqlx::Error> {
    let raw = upsert(
        db.dialect,
        "token_rotation",
        &[
            "id",
            "active_token_hash",
            "previous_token_hash",
            "previous_token_expires_at",
        ],
    );
    let sql = db.dialect.render(&raw);
    sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
        .bind(1_i64)
        .bind(&t.active_token_hash)
        .bind(t.previous_token_hash.as_deref())
        .bind(t.previous_token_expires_at.as_deref())
        .execute(db.pool())
        .await?;
    Ok(())
}

async fn load_gateway_protection(db: &Db) -> Result<GatewayProtectionConfig, sqlx::Error> {
    let sql = db
        .dialect
        .render("SELECT config_json FROM gateway_protection_config WHERE id = 1");
    let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
        .fetch_optional(db.pool())
        .await?;
    let cfg = match row {
        Some(r) => {
            let json: String = r.try_get("config_json")?;
            serde_json::from_str(&json).unwrap_or_default()
        }
        None => {
            let cfg = GatewayProtectionConfig::default();
            save_gateway_protection(db, &cfg).await?;
            cfg
        }
    };
    Ok(cfg)
}

async fn save_gateway_protection(db: &Db, cfg: &GatewayProtectionConfig) -> Result<(), sqlx::Error> {
    let json = serde_json::to_string(cfg).unwrap_or_else(|_| "{}".into());
    let raw = upsert(db.dialect, "gateway_protection_config", &["id", "config_json"]);
    let sql = db.dialect.render(&raw);
    sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
        .bind(1_i64)
        .bind(json)
        .execute(db.pool())
        .await?;
    Ok(())
}
