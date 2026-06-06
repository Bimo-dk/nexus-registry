use serde::Serialize;
use sqlx::Row;
use tracing::warn;
use ulid::Ulid;

use crate::store::sqlite::{Db, StoreError};
use crate::time::iso_now;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditEntry {
    pub id: String,
    pub entity_type: String,
    pub entity_id: String,
    pub action: String,
    pub actor: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<serde_json::Value>,
    pub created_at: String,
}

/// Fire-and-forget: spawns a task that inserts one row into audit_log.
/// Caller does not need to await. Failures are logged as warnings only.
pub fn append(
    db: Db,
    entity_type: &str,
    entity_id: &str,
    action: &str,
    actor: &str,
    meta: Option<serde_json::Value>,
) {
    let id = Ulid::new().to_string();
    let created_at = iso_now();
    let entity_type = entity_type.to_string();
    let entity_id = entity_id.to_string();
    let action = action.to_string();
    let actor = actor.to_string();
    let meta_str = meta.map(|v| v.to_string());

    tokio::spawn(async move {
        let sql = db.dialect.prep(
            "INSERT INTO audit_log \
             (id, entity_type, entity_id, action, actor, meta, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        );
        if let Err(e) = sqlx::query(sql)
            .bind(&id)
            .bind(&entity_type)
            .bind(&entity_id)
            .bind(&action)
            .bind(&actor)
            .bind(meta_str.as_deref())
            .bind(&created_at)
            .execute(db.pool())
            .await
        {
            warn!("[audit] failed to append entry: {}", e);
        }
    });
}

#[derive(Debug, Default)]
pub struct AuditQuery {
    pub entity_type: Option<String>,
    pub entity_id: Option<String>,
    pub action: Option<String>,
    pub before: Option<String>,
    pub limit: u64,
}

pub async fn query(db: &Db, q: &AuditQuery) -> Result<Vec<AuditEntry>, StoreError> {
    let mut where_parts: Vec<&str> = Vec::new();
    if q.entity_type.is_some() {
        where_parts.push("entity_type = ?");
    }
    if q.entity_id.is_some() {
        where_parts.push("entity_id = ?");
    }
    if q.action.is_some() {
        where_parts.push("action = ?");
    }
    if q.before.is_some() {
        where_parts.push("created_at < ?");
    }

    let where_clause = if where_parts.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", where_parts.join(" AND "))
    };

    let raw = format!(
        "SELECT id, entity_type, entity_id, action, actor, meta, created_at \
         FROM audit_log{where_clause} ORDER BY created_at DESC LIMIT ?"
    );
    let sql = db.dialect.render(&raw);
    let mut qb = sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()));
    if let Some(ref v) = q.entity_type {
        qb = qb.bind(v.as_str());
    }
    if let Some(ref v) = q.entity_id {
        qb = qb.bind(v.as_str());
    }
    if let Some(ref v) = q.action {
        qb = qb.bind(v.as_str());
    }
    if let Some(ref v) = q.before {
        qb = qb.bind(v.as_str());
    }
    qb = qb.bind(q.limit as i64);

    let rows = qb.fetch_all(db.pool()).await?;
    let mut entries = Vec::with_capacity(rows.len());
    for r in &rows {
        let meta_raw: Option<String> = r.try_get("meta")?;
        let meta = meta_raw.as_deref().and_then(|s| serde_json::from_str(s).ok());
        entries.push(AuditEntry {
            id: r.try_get("id")?,
            entity_type: r.try_get("entity_type")?,
            entity_id: r.try_get("entity_id")?,
            action: r.try_get("action")?,
            actor: r.try_get("actor")?,
            meta,
            created_at: r.try_get("created_at")?,
        });
    }
    Ok(entries)
}
