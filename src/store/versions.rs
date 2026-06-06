use serde::Serialize;
use sqlx::Row;
use tracing::warn;
use ulid::Ulid;

use crate::store::sqlite::{get, Db, StoreError};
use crate::time::iso_now;
use crate::types::RemoteConfig;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteVersion {
    pub id: String,
    pub remote_name: String,
    pub version: u32,
    pub url: String,
    pub exposed_module: String,
    pub route_path: String,
    pub enabled: bool,
    pub upstream_url: Option<String>,
    pub visibility: String,
    pub recorded_at: String,
}

fn row_to_version(r: &sqlx::any::AnyRow) -> Result<RemoteVersion, sqlx::Error> {
    Ok(RemoteVersion {
        id: r.try_get("id")?,
        remote_name: r.try_get("remote_name")?,
        version: r.try_get::<i64, _>("version")? as u32,
        url: r.try_get("url")?,
        exposed_module: r.try_get("exposed_module")?,
        route_path: r.try_get("route_path")?,
        enabled: r.try_get::<i64, _>("enabled")? != 0,
        upstream_url: r.try_get("upstream_url")?,
        visibility: r.try_get("visibility")?,
        recorded_at: r.try_get("recorded_at")?,
    })
}

/// Snapshots `remote`'s current config. Non-fatal: failures are logged so
/// the calling handler still returns the correct response.
pub async fn record(db: &Db, remote: &RemoteConfig) {
    let id = Ulid::new().to_string();
    let recorded_at = iso_now();

    let max: i64 = match sqlx::query_scalar(db.dialect.prep(
        "SELECT COALESCE(MAX(version), 0) FROM remote_versions WHERE remote_name = ?",
    ))
    .bind(&remote.name)
    .fetch_one(db.pool())
    .await
    {
        Ok(v) => v,
        Err(e) => {
            warn!("[versions] failed to get max version for {}: {}", remote.name, e);
            return;
        }
    };

    let sql = db.dialect.prep(
        "INSERT INTO remote_versions \
         (id, remote_name, version, url, exposed_module, route_path, enabled, \
          upstream_url, visibility, recorded_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    );
    if let Err(e) = sqlx::query(sql)
        .bind(&id)
        .bind(&remote.name)
        .bind(max + 1)
        .bind(&remote.url)
        .bind(&remote.exposed_module)
        .bind(&remote.route_path)
        .bind(remote.enabled as i64)
        .bind(remote.upstream_url.as_deref())
        .bind(&remote.visibility)
        .bind(&recorded_at)
        .execute(db.pool())
        .await
    {
        warn!("[versions] failed to record version for {}: {}", remote.name, e);
    }
}

pub async fn list_for_remote(db: &Db, remote_name: &str) -> Result<Vec<RemoteVersion>, StoreError> {
    let sql = db.dialect.render(
        "SELECT id, remote_name, version, url, exposed_module, route_path, enabled, \
         upstream_url, visibility, recorded_at \
         FROM remote_versions WHERE remote_name = ? ORDER BY version DESC",
    );
    let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
        .bind(remote_name)
        .fetch_all(db.pool())
        .await?;
    rows.iter()
        .map(row_to_version)
        .collect::<Result<_, _>>()
        .map_err(StoreError::Db)
}

pub async fn get_version(db: &Db, remote_name: &str, version: u32) -> Result<Option<RemoteVersion>, StoreError> {
    let sql = db.dialect.render(
        "SELECT id, remote_name, version, url, exposed_module, route_path, enabled, \
         upstream_url, visibility, recorded_at \
         FROM remote_versions WHERE remote_name = ? AND version = ? LIMIT 1",
    );
    let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
        .bind(remote_name)
        .bind(version as i64)
        .fetch_optional(db.pool())
        .await?;
    match row {
        Some(r) => Ok(Some(row_to_version(&r)?)),
        None => Ok(None),
    }
}

/// Restores the remote to the state captured in `version`. Issues a direct
/// UPDATE (not a patch-merge) so fields like `upstream_url` that were None
/// in the snapshot are explicitly cleared. Returns the full updated
/// `RemoteConfig` on success, or `None` if the remote or version does not
/// exist.
pub async fn restore(
    db: &Db,
    remote_name: &str,
    version: u32,
) -> Result<Option<(RemoteConfig, u32)>, StoreError> {
    let Some(ver) = get_version(db, remote_name, version).await? else {
        return Ok(None);
    };

    let sql = db.dialect.render(
        "UPDATE remotes SET url = ?, exposed_module = ?, route_path = ?, enabled = ?, \
         upstream_url = ?, visibility = ? WHERE name = ?",
    );
    let res = sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
        .bind(&ver.url)
        .bind(&ver.exposed_module)
        .bind(&ver.route_path)
        .bind(ver.enabled as i64)
        .bind(ver.upstream_url.as_deref())
        .bind(&ver.visibility)
        .bind(remote_name)
        .execute(db.pool())
        .await?;

    if res.rows_affected() == 0 {
        return Ok(None);
    }

    match get(db, remote_name).await? {
        Some(remote) => Ok(Some((remote, ver.version))),
        None => Ok(None),
    }
}
