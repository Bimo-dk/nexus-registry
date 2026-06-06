use std::path::{Path, PathBuf};
use std::str::FromStr;

use serde::Deserialize;
use sqlx::{
    any::{install_default_drivers, AnyConnectOptions, AnyPoolOptions},
    Any, Pool, Row,
};
use thiserror::Error;
use tracing::{info, warn};

use crate::config::database::{DatabaseConfig, Dialect};
use crate::types::{RemoteConfig, RemoteHealthStatus, UpdateRemoteRequest};

/// Database handle threaded through the registry. Wraps a sqlx `AnyPool`
/// (driver-dispatched at runtime by URL scheme) and the resolved `Dialect`
/// so query sites can rewrite placeholders + dispatch upsert syntax without
/// inspecting the URL every call.
#[derive(Clone)]
pub struct Db {
    pool: Pool<Any>,
    pub dialect: Dialect,
}

impl Db {
    pub fn pool(&self) -> &Pool<Any> {
        &self.pool
    }

    pub async fn close(&self) {
        self.pool.close().await
    }

    pub fn size(&self) -> u32 {
        self.pool.size()
    }

    pub fn num_idle(&self) -> usize {
        self.pool.num_idle()
    }
}

// ---- Per-dialect schema ----
//
// Each block is the minimum schema the registry needs to operate. Kept inline
// for now so a fresh boot against any supported database succeeds without an
// external migration step. Full migration files under `migrations/<dialect>/`
// land in the next refactor; until then `CREATE TABLE IF NOT EXISTS` keeps
// re-runs idempotent on SQLite + Postgres + MySQL.

const SCHEMA_SQLITE: &str = r#"
CREATE TABLE IF NOT EXISTS remotes (
    name              TEXT PRIMARY KEY,
    url               TEXT NOT NULL,
    exposed_module    TEXT NOT NULL,
    route_path        TEXT NOT NULL,
    enabled           INTEGER NOT NULL,
    added_at          TEXT NOT NULL,
    upstream_url      TEXT,
    health_status     TEXT,
    last_health_check TEXT,
    visibility        TEXT NOT NULL DEFAULT 'global'
);
CREATE INDEX IF NOT EXISTS idx_remotes_visibility ON remotes(visibility);

CREATE TABLE IF NOT EXISTS hosts (
    id             TEXT PRIMARY KEY,
    name           TEXT NOT NULL UNIQUE,
    url            TEXT NOT NULL,
    framework      TEXT NOT NULL CHECK (framework IN ('angular','vue','react')),
    remote_entry   TEXT NOT NULL,
    exposed_module TEXT NOT NULL,
    enabled        INTEGER NOT NULL DEFAULT 1,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS gates (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL UNIQUE,
    domain     TEXT NOT NULL UNIQUE,
    host_id    TEXT REFERENCES hosts(id) ON DELETE SET NULL,
    enabled    INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_gates_host_id ON gates(host_id);
"#;

const SCHEMA_POSTGRES: &str = r#"
CREATE TABLE IF NOT EXISTS remotes (
    name              VARCHAR(255) PRIMARY KEY,
    url               TEXT NOT NULL,
    exposed_module    TEXT NOT NULL,
    route_path        VARCHAR(255) NOT NULL,
    enabled           INTEGER NOT NULL,
    added_at          TEXT NOT NULL,
    upstream_url      TEXT,
    health_status     VARCHAR(32),
    last_health_check TEXT,
    visibility        VARCHAR(255) NOT NULL DEFAULT 'global'
);
CREATE INDEX IF NOT EXISTS idx_remotes_visibility ON remotes(visibility);

CREATE TABLE IF NOT EXISTS hosts (
    id             VARCHAR(64)  PRIMARY KEY,
    name           VARCHAR(255) NOT NULL UNIQUE,
    url            TEXT NOT NULL,
    framework      VARCHAR(32)  NOT NULL CHECK (framework IN ('angular','vue','react')),
    remote_entry   TEXT NOT NULL,
    exposed_module TEXT NOT NULL,
    enabled        INTEGER NOT NULL DEFAULT 1,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS gates (
    id         VARCHAR(64)  PRIMARY KEY,
    name       VARCHAR(255) NOT NULL UNIQUE,
    domain     VARCHAR(255) NOT NULL UNIQUE,
    host_id    VARCHAR(64)  REFERENCES hosts(id) ON DELETE SET NULL,
    enabled    INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_gates_host_id ON gates(host_id);
"#;

const SCHEMA_MYSQL: &str = r#"
CREATE TABLE IF NOT EXISTS remotes (
    name              VARCHAR(255) PRIMARY KEY,
    url               TEXT NOT NULL,
    exposed_module    TEXT NOT NULL,
    route_path        VARCHAR(255) NOT NULL,
    enabled           INT NOT NULL,
    added_at          VARCHAR(64) NOT NULL,
    upstream_url      TEXT,
    health_status     VARCHAR(32),
    last_health_check VARCHAR(64),
    visibility        VARCHAR(255) NOT NULL DEFAULT 'global',
    INDEX idx_remotes_visibility (visibility)
);

CREATE TABLE IF NOT EXISTS hosts (
    id             VARCHAR(64)  PRIMARY KEY,
    name           VARCHAR(255) NOT NULL UNIQUE,
    url            TEXT NOT NULL,
    framework      VARCHAR(32)  NOT NULL,
    remote_entry   TEXT NOT NULL,
    exposed_module TEXT NOT NULL,
    enabled        INT NOT NULL DEFAULT 1,
    created_at     VARCHAR(64) NOT NULL,
    updated_at     VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS gates (
    id         VARCHAR(64)  PRIMARY KEY,
    name       VARCHAR(255) NOT NULL UNIQUE,
    domain     VARCHAR(255) NOT NULL UNIQUE,
    host_id    VARCHAR(64),
    enabled    INT NOT NULL DEFAULT 1,
    created_at VARCHAR(64) NOT NULL,
    updated_at VARCHAR(64) NOT NULL,
    INDEX idx_gates_host_id (host_id),
    CONSTRAINT fk_gates_host FOREIGN KEY (host_id) REFERENCES hosts(id) ON DELETE SET NULL
);
"#;

fn schema_for(dialect: Dialect) -> &'static str {
    match dialect {
        Dialect::Sqlite => SCHEMA_SQLITE,
        Dialect::Postgres => SCHEMA_POSTGRES,
        Dialect::MySql => SCHEMA_MYSQL,
    }
}

/// Append `?mode=rwc` to a SQLite URL so sqlx creates the database file when
/// missing. `:memory:` and already-parameterised URLs are passed through.
fn sqlite_url_with_create_mode(url: &str) -> String {
    if url.contains("memory") {
        return url.to_string();
    }
    if url.contains("mode=") {
        return url.to_string();
    }
    if url.contains('?') {
        format!("{url}&mode=rwc")
    } else {
        format!("{url}?mode=rwc")
    }
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("remote \"{0}\" already exists")]
    Conflict(String),
    #[error(transparent)]
    Db(#[from] sqlx::Error),
}

pub async fn init(cfg: &DatabaseConfig, data_dir: &Path) -> Result<Db, StoreError> {
    install_default_drivers();

    std::fs::create_dir_all(data_dir).map_err(|e| {
        StoreError::Db(sqlx::Error::Io(std::io::Error::other(format!(
            "cannot create data dir {}: {}",
            data_dir.display(),
            e
        ))))
    })?;

    // sqlx::Any does not expose driver-specific options like
    // `create_if_missing`. For SQLite we add `mode=rwc` to the URL — equivalent
    // to create_if_missing(true) — and apply WAL + NORMAL sync via PRAGMAs once
    // connected. Postgres + MySQL connect directly from the URL.
    let connect_url = match cfg.dialect {
        Dialect::Sqlite => sqlite_url_with_create_mode(&cfg.url),
        Dialect::Postgres | Dialect::MySql => cfg.url.clone(),
    };

    let any_opts = AnyConnectOptions::from_str(&connect_url).map_err(StoreError::Db)?;
    // sqlite::memory: gives every pool connection its own private database,
    // so a pool of size N has N disjoint in-memory DBs. Pin to 1 to keep the
    // schema visible across queries during tests and ephemeral runs.
    let max_connections = if matches!(cfg.dialect, Dialect::Sqlite) && connect_url.contains("memory") {
        1
    } else {
        8
    };
    let pool: Pool<Any> = AnyPoolOptions::new()
        .max_connections(max_connections)
        .connect_with(any_opts)
        .await?;

    let db = Db {
        pool,
        dialect: cfg.dialect,
    };

    if matches!(cfg.dialect, Dialect::Sqlite) {
        // Foreign-key enforcement is opt-in on SQLite. Postgres + MySQL enforce
        // by default.
        sqlx::query("PRAGMA foreign_keys = ON").execute(db.pool()).await?;
        // Best-effort journal mode + sync — failures are non-fatal because
        // these PRAGMAs are no-ops for in-memory databases and some sqlx Any
        // wrappers reject the result rows.
        let _ = sqlx::query("PRAGMA journal_mode = WAL").execute(db.pool()).await;
        let _ = sqlx::query("PRAGMA synchronous = NORMAL")
            .execute(db.pool())
            .await;
    }

    for stmt in schema_for(cfg.dialect)
        .split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        sqlx::query(stmt).execute(db.pool()).await?;
    }

    if matches!(cfg.dialect, Dialect::Sqlite) {
        ensure_visibility_column(&db).await?;
        import_legacy_json(&db, data_dir).await?;
    }

    Ok(db)
}

/// Idempotent ALTER TABLE for upgrades from pre-visibility SQLite databases.
/// CREATE TABLE handles fresh installs; Postgres + MySQL deployments start
/// fresh under the multi-DB rewrite so they do not need this hook.
async fn ensure_visibility_column(db: &Db) -> Result<(), StoreError> {
    let rows: Vec<(i64, String, String, i64, Option<String>, i64)> =
        sqlx::query_as("PRAGMA table_info(remotes)")
            .fetch_all(db.pool())
            .await?;
    let has_visibility = rows.iter().any(|(_, name, _, _, _, _)| name == "visibility");
    if !has_visibility {
        sqlx::query("ALTER TABLE remotes ADD COLUMN visibility TEXT NOT NULL DEFAULT 'global'")
            .execute(db.pool())
            .await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_remotes_visibility ON remotes(visibility)")
            .execute(db.pool())
            .await?;
        info!("[store] added remotes.visibility column to existing database");
    }
    Ok(())
}

#[derive(Deserialize)]
struct LegacyFile {
    remotes: Vec<RemoteConfig>,
}

async fn import_legacy_json(db: &Db, data_dir: &Path) -> Result<(), StoreError> {
    let json_path: PathBuf = data_dir.join("registry.json");
    if !json_path.exists() {
        return Ok(());
    }
    let raw = match std::fs::read_to_string(&json_path) {
        Ok(s) => s,
        Err(e) => {
            warn!("[store] cannot read legacy registry.json: {}", e);
            return Ok(());
        }
    };
    let parsed: LegacyFile = match serde_json::from_str(&raw) {
        Ok(p) => p,
        Err(e) => {
            warn!(
                "[store] legacy registry.json is not valid JSON, skipping import: {}",
                e
            );
            return Ok(());
        }
    };

    let mut imported = 0usize;
    let mut tx = db.pool().begin().await?;
    for r in parsed.remotes {
        let res = sqlx::query(
            "INSERT OR IGNORE INTO remotes \
             (name, url, exposed_module, route_path, enabled, added_at, upstream_url, health_status, last_health_check) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&r.name)
        .bind(&r.url)
        .bind(&r.exposed_module)
        .bind(&r.route_path)
        .bind(r.enabled as i64)
        .bind(&r.added_at)
        .bind(r.upstream_url.as_deref())
        .bind(r.health_status.map(|h| h.as_str().to_string()))
        .bind(r.last_health_check.as_deref())
        .execute(&mut *tx)
        .await?;
        if res.rows_affected() > 0 {
            imported += 1;
        }
    }
    tx.commit().await?;

    let archived = data_dir.join("registry.json.imported");
    if let Err(e) = std::fs::rename(&json_path, &archived) {
        warn!("[store] could not archive legacy registry.json: {}", e);
    }
    info!(
        "[store] imported {} remote(s) from legacy registry.json",
        imported
    );
    Ok(())
}

// ---- Remote CRUD ----

fn row_to_remote(r: &sqlx::any::AnyRow) -> Result<RemoteConfig, sqlx::Error> {
    Ok(RemoteConfig {
        name: r.try_get("name")?,
        url: r.try_get("url")?,
        exposed_module: r.try_get("exposed_module")?,
        route_path: r.try_get("route_path")?,
        enabled: r.try_get::<i64, _>("enabled")? != 0,
        added_at: r.try_get("added_at")?,
        upstream_url: r.try_get("upstream_url")?,
        health_status: r
            .try_get::<Option<String>, _>("health_status")?
            .as_deref()
            .and_then(RemoteHealthStatus::from_str),
        last_health_check: r.try_get("last_health_check")?,
        visibility: r.try_get("visibility")?,
    })
}

pub async fn list(db: &Db) -> Result<Vec<RemoteConfig>, StoreError> {
    let sql = db.dialect.render(
        "SELECT name, url, exposed_module, route_path, enabled, added_at, upstream_url, \
         health_status, last_health_check, visibility FROM remotes ORDER BY added_at",
    );
    let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
        .fetch_all(db.pool())
        .await?;
    rows.iter()
        .map(row_to_remote)
        .collect::<Result<_, _>>()
        .map_err(Into::into)
}

/// Returns global remotes plus host-specific remotes for the given host id.
pub async fn list_for_host(db: &Db, host_id: &str) -> Result<Vec<RemoteConfig>, StoreError> {
    let host_visibility = format!("host:{}", host_id);
    let sql = db.dialect.render(
        "SELECT name, url, exposed_module, route_path, enabled, added_at, upstream_url, \
         health_status, last_health_check, visibility FROM remotes \
         WHERE visibility = 'global' OR visibility = ? ORDER BY added_at",
    );
    let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
        .bind(&host_visibility)
        .fetch_all(db.pool())
        .await?;
    rows.iter()
        .map(row_to_remote)
        .collect::<Result<_, _>>()
        .map_err(Into::into)
}

pub async fn get(db: &Db, name: &str) -> Result<Option<RemoteConfig>, StoreError> {
    let sql = db.dialect.render(
        "SELECT name, url, exposed_module, route_path, enabled, added_at, upstream_url, \
         health_status, last_health_check, visibility FROM remotes WHERE name = ?",
    );
    let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
        .bind(name)
        .fetch_optional(db.pool())
        .await?;
    match row {
        Some(r) => Ok(Some(row_to_remote(&r)?)),
        None => Ok(None),
    }
}

pub async fn insert(db: &Db, remote: &RemoteConfig) -> Result<(), StoreError> {
    let sql = db.dialect.render(
        "INSERT INTO remotes \
         (name, url, exposed_module, route_path, enabled, added_at, upstream_url, health_status, last_health_check, visibility) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    );
    let res = sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
        .bind(&remote.name)
        .bind(&remote.url)
        .bind(&remote.exposed_module)
        .bind(&remote.route_path)
        .bind(remote.enabled as i64)
        .bind(&remote.added_at)
        .bind(remote.upstream_url.as_deref())
        .bind(remote.health_status.map(|h| h.as_str().to_string()))
        .bind(remote.last_health_check.as_deref())
        .bind(&remote.visibility)
        .execute(db.pool())
        .await;

    match res {
        Ok(_) => Ok(()),
        Err(sqlx::Error::Database(db_err)) if is_unique_violation(&*db_err) => {
            Err(StoreError::Conflict(remote.name.clone()))
        }
        Err(e) => Err(StoreError::Db(e)),
    }
}

fn is_unique_violation(err: &dyn sqlx::error::DatabaseError) -> bool {
    // SQLite: error code 2067 (extended UNIQUE).
    // Postgres: SQLSTATE 23505 (unique_violation).
    // MySQL: code 1062 (ER_DUP_ENTRY).
    matches!(err.code().as_deref(), Some("2067") | Some("23505") | Some("1062"))
        || err.message().to_ascii_lowercase().contains("unique")
        || err.message().to_ascii_lowercase().contains("duplicate")
}

/// Re-exported for `store::entities` — both modules need to detect this.
pub(crate) fn is_unique_violation_pub(err: &dyn sqlx::error::DatabaseError) -> bool {
    is_unique_violation(err)
}

pub async fn update(
    db: &Db,
    name: &str,
    patch: UpdateRemoteRequest,
) -> Result<Option<RemoteConfig>, StoreError> {
    let mut tx = db.pool().begin().await?;

    let select_sql = db.dialect.render(
        "SELECT name, url, exposed_module, route_path, enabled, added_at, upstream_url, \
         health_status, last_health_check, visibility FROM remotes WHERE name = ?",
    );
    let existing = sqlx::query(sqlx::AssertSqlSafe(select_sql.as_ref()))
        .bind(name)
        .fetch_optional(&mut *tx)
        .await?;
    let Some(existing) = existing else {
        return Ok(None);
    };
    let existing = row_to_remote(&existing)?;

    let merged = RemoteConfig {
        name: existing.name.clone(),
        url: patch.url.unwrap_or(existing.url),
        exposed_module: patch.exposed_module.unwrap_or(existing.exposed_module),
        route_path: patch.route_path.unwrap_or(existing.route_path),
        enabled: patch.enabled.unwrap_or(existing.enabled),
        added_at: existing.added_at,
        upstream_url: patch.upstream_url.or(existing.upstream_url),
        health_status: patch.health_status.or(existing.health_status),
        last_health_check: patch.last_health_check.or(existing.last_health_check),
        visibility: patch.visibility.unwrap_or(existing.visibility),
    };

    let update_sql = db.dialect.render(
        "UPDATE remotes SET url = ?, exposed_module = ?, route_path = ?, enabled = ?, \
         upstream_url = ?, health_status = ?, last_health_check = ?, visibility = ? WHERE name = ?",
    );
    sqlx::query(sqlx::AssertSqlSafe(update_sql.as_ref()))
        .bind(&merged.url)
        .bind(&merged.exposed_module)
        .bind(&merged.route_path)
        .bind(merged.enabled as i64)
        .bind(merged.upstream_url.as_deref())
        .bind(merged.health_status.map(|h| h.as_str().to_string()))
        .bind(merged.last_health_check.as_deref())
        .bind(&merged.visibility)
        .bind(name)
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    Ok(Some(merged))
}

pub async fn delete(db: &Db, name: &str) -> Result<bool, StoreError> {
    let sql = db.dialect.render("DELETE FROM remotes WHERE name = ?");
    let res = sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
        .bind(name)
        .execute(db.pool())
        .await?;
    Ok(res.rows_affected() > 0)
}

pub async fn toggle(db: &Db, name: &str) -> Result<Option<RemoteConfig>, StoreError> {
    let mut tx = db.pool().begin().await?;
    let select_sql = db.dialect.render("SELECT enabled FROM remotes WHERE name = ?");
    let existing: Option<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(select_sql.as_ref()))
        .bind(name)
        .fetch_optional(&mut *tx)
        .await?;
    let Some(current) = existing else {
        return Ok(None);
    };
    let next = if current == 0 { 1i64 } else { 0i64 };
    let update_sql = db.dialect.render("UPDATE remotes SET enabled = ? WHERE name = ?");
    sqlx::query(sqlx::AssertSqlSafe(update_sql.as_ref()))
        .bind(next)
        .bind(name)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    get(db, name).await
}
