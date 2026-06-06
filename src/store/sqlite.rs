use std::path::{Path, PathBuf};
use std::str::FromStr;

use serde::Deserialize;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    Pool, Sqlite,
};
use thiserror::Error;
use tracing::{info, warn};

use crate::types::{RemoteConfig, RemoteHealthStatus, UpdateRemoteRequest};

pub type Db = Pool<Sqlite>;

const SCHEMA: &str = r#"
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

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("remote \"{0}\" already exists")]
    Conflict(String),
    #[error(transparent)]
    Db(#[from] sqlx::Error),
}

pub async fn init(database_url: &str, data_dir: &Path) -> Result<Db, StoreError> {
    // Ensure the data dir exists — used for legacy JSON import and for the
    // default SQLite file location.
    std::fs::create_dir_all(data_dir).map_err(|e| {
        StoreError::Db(sqlx::Error::Io(std::io::Error::other(format!(
            "cannot create data dir {}: {}",
            data_dir.display(),
            e
        ))))
    })?;

    let opts = SqliteConnectOptions::from_str(database_url)
        .map_err(StoreError::Db)?
        .create_if_missing(true)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .synchronous(sqlx::sqlite::SqliteSynchronous::Normal);

    let pool = SqlitePoolOptions::new()
        .max_connections(8)
        .connect_with(opts)
        .await?;
    // Enable foreign-key enforcement — required for gates→hosts ON DELETE RESTRICT.
    sqlx::query("PRAGMA foreign_keys = ON").execute(&pool).await?;
    for stmt in SCHEMA.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        sqlx::query(stmt).execute(&pool).await?;
    }

    ensure_visibility_column(&pool).await?;
    import_legacy_json(&pool, data_dir).await?;

    Ok(pool)
}

/// Idempotent ALTER TABLE for upgrades from pre-visibility databases.
/// CREATE TABLE handles fresh installs.
async fn ensure_visibility_column(db: &Db) -> Result<(), StoreError> {
    let rows: Vec<(i64, String, String, i64, Option<String>, i64)> =
        sqlx::query_as("PRAGMA table_info(remotes)").fetch_all(db).await?;
    let has_visibility = rows.iter().any(|(_, name, _, _, _, _)| name == "visibility");
    if !has_visibility {
        sqlx::query("ALTER TABLE remotes ADD COLUMN visibility TEXT NOT NULL DEFAULT 'global'")
            .execute(db)
            .await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_remotes_visibility ON remotes(visibility)")
            .execute(db)
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
    let mut tx = db.begin().await?;
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
        .bind(r.enabled)
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

#[derive(sqlx::FromRow)]
struct RemoteRow {
    name: String,
    url: String,
    exposed_module: String,
    route_path: String,
    enabled: bool,
    added_at: String,
    upstream_url: Option<String>,
    health_status: Option<String>,
    last_health_check: Option<String>,
    visibility: String,
}

impl From<RemoteRow> for RemoteConfig {
    fn from(r: RemoteRow) -> Self {
        RemoteConfig {
            name: r.name,
            url: r.url,
            exposed_module: r.exposed_module,
            route_path: r.route_path,
            enabled: r.enabled,
            added_at: r.added_at,
            upstream_url: r.upstream_url,
            health_status: r.health_status.as_deref().and_then(RemoteHealthStatus::from_str),
            last_health_check: r.last_health_check,
            visibility: r.visibility,
        }
    }
}

pub async fn list(db: &Db) -> Result<Vec<RemoteConfig>, StoreError> {
    let rows: Vec<RemoteRow> =
        sqlx::query_as("SELECT name, url, exposed_module, route_path, enabled, added_at, upstream_url, health_status, last_health_check, visibility FROM remotes ORDER BY added_at")
            .fetch_all(db)
            .await?;
    Ok(rows.into_iter().map(Into::into).collect())
}

/// Returns global remotes plus host-specific remotes for the given host id.
pub async fn list_for_host(db: &Db, host_id: &str) -> Result<Vec<RemoteConfig>, StoreError> {
    let host_visibility = format!("host:{}", host_id);
    let rows: Vec<RemoteRow> = sqlx::query_as(
        "SELECT name, url, exposed_module, route_path, enabled, added_at, upstream_url, health_status, last_health_check, visibility FROM remotes WHERE visibility = 'global' OR visibility = ? ORDER BY added_at"
    )
    .bind(&host_visibility)
    .fetch_all(db)
    .await?;
    Ok(rows.into_iter().map(Into::into).collect())
}

pub async fn get(db: &Db, name: &str) -> Result<Option<RemoteConfig>, StoreError> {
    let row: Option<RemoteRow> = sqlx::query_as(
        "SELECT name, url, exposed_module, route_path, enabled, added_at, upstream_url, health_status, last_health_check, visibility FROM remotes WHERE name = ?"
    )
    .bind(name)
    .fetch_optional(db)
    .await?;
    Ok(row.map(Into::into))
}

pub async fn insert(db: &Db, remote: &RemoteConfig) -> Result<(), StoreError> {
    let res = sqlx::query(
        "INSERT INTO remotes \
         (name, url, exposed_module, route_path, enabled, added_at, upstream_url, health_status, last_health_check, visibility) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&remote.name)
    .bind(&remote.url)
    .bind(&remote.exposed_module)
    .bind(&remote.route_path)
    .bind(remote.enabled)
    .bind(&remote.added_at)
    .bind(remote.upstream_url.as_deref())
    .bind(remote.health_status.map(|h| h.as_str().to_string()))
    .bind(remote.last_health_check.as_deref())
    .bind(&remote.visibility)
    .execute(db)
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
    err.code().as_deref() == Some("2067") || err.message().contains("UNIQUE")
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
    let mut tx = db.begin().await?;

    let existing: Option<RemoteRow> = sqlx::query_as(
        "SELECT name, url, exposed_module, route_path, enabled, added_at, upstream_url, health_status, last_health_check, visibility FROM remotes WHERE name = ?"
    )
    .bind(name)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(existing) = existing else {
        return Ok(None);
    };

    let merged = RemoteConfig {
        name: existing.name.clone(),
        url: patch.url.unwrap_or(existing.url),
        exposed_module: patch.exposed_module.unwrap_or(existing.exposed_module),
        route_path: patch.route_path.unwrap_or(existing.route_path),
        enabled: patch.enabled.unwrap_or(existing.enabled),
        added_at: existing.added_at,
        upstream_url: patch.upstream_url.or(existing.upstream_url),
        health_status: patch.health_status.or_else(|| {
            existing
                .health_status
                .as_deref()
                .and_then(RemoteHealthStatus::from_str)
        }),
        last_health_check: patch.last_health_check.or(existing.last_health_check),
        visibility: patch.visibility.unwrap_or(existing.visibility),
    };

    sqlx::query(
        "UPDATE remotes SET url = ?, exposed_module = ?, route_path = ?, enabled = ?, \
         upstream_url = ?, health_status = ?, last_health_check = ?, visibility = ? WHERE name = ?",
    )
    .bind(&merged.url)
    .bind(&merged.exposed_module)
    .bind(&merged.route_path)
    .bind(merged.enabled)
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
    let res = sqlx::query("DELETE FROM remotes WHERE name = ?")
        .bind(name)
        .execute(db)
        .await?;
    Ok(res.rows_affected() > 0)
}

pub async fn toggle(db: &Db, name: &str) -> Result<Option<RemoteConfig>, StoreError> {
    let mut tx = db.begin().await?;
    let existing: Option<bool> = sqlx::query_scalar("SELECT enabled FROM remotes WHERE name = ?")
        .bind(name)
        .fetch_optional(&mut *tx)
        .await?;
    let Some(current) = existing else {
        return Ok(None);
    };
    let next = !current;
    sqlx::query("UPDATE remotes SET enabled = ? WHERE name = ?")
        .bind(next)
        .bind(name)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    get(db, name).await
}
